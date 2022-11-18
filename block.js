const mongoose = require('mongoose');
const lodash = require('lodash');
const cache = require('memory-cache');
const async = require('async');

const {mpapi} = require('./js-rpcapi');
const payment = require('./payment');
const config = require('./config');
const constants = require('./constants');

const Settings = require('./models/settings')();
const BakerCycle = require('./models/bakerCycle')();
const Reward = require('./models/reward')();

mpapi.node.setProvider(config.NODE_RPC);
mpapi.node.setDebugMode(false);

const PRESERVES_CYCLE = 5 + 2;
const BLOCKS_IN_CYCLE = 1440;
const TIME_BETWEEN_BLOCKS = 60;
const STEP_PROCESS_CYCLE = 500;

const blocksCache = new cache.Cache();
const blockConstantsCache = new cache.Cache();
const cycleInfoCache = new cache.Cache();

const getBlock = async (level = 'head') => {
    const cachedBlock = blocksCache.get(level);

    if (!cachedBlock) {
        const block = await mpapi.rpc.getHead(level);
        blocksCache.put(block.header.level, block, BLOCKS_IN_CYCLE * PRESERVES_CYCLE * TIME_BETWEEN_BLOCKS);

        return block;
    }

    return cachedBlock;
}

const getBlockEndorsers = (operations) => {
    const findEndorsers = (operations) => {
        return operations.filter(operation => {
            if (Array.isArray(operation)) {
                return findEndorsers(operation).length > 0 ? true : false;
            } else {
                if (operation.contents)
                    return findEndorsers(operation.contents).length > 0 ? true : false;
                else {
                    return operation.kind === 'endorsement' ? true : false;
                }
            }
        })
    }

    const endorserOperations = lodash.flattenDeep(findEndorsers(operations));
    return endorserOperations.map(operation => ({
        address: operation.contents[0].metadata.delegate,
        slots: operation.contents[0].metadata.slots.length,
        level: operation.contents[0].level,
    }))
}

const getRewardsForBaker = async (block, bakerAddress, endorsers) => {
    return await getRewards(block, constants.REWARD_TYPES.FOR_BAKING, bakerAddress, {endorsers});
}

const getRewardsForEndorser = async (block, endorserAddress, slots) => {
    return await getRewards(block, constants.REWARD_TYPES.FOR_ENDORSING, endorserAddress, {slots});
}

const getCycleInfo = async (cycle) => {
    if (!lodash.isNumber(cycle)) {
        throw new Error('Cycle must be a number');
    }

    const cachedCycleInfo = cycleInfoCache.get(cycle);

    if (!cachedCycleInfo) {
        const cycleInfo = await mpapi.rpc.getLevelsInCurrentCycle(BLOCKS_IN_CYCLE * cycle + 1)
        cycleInfoCache.put(cycle, cycleInfo, BLOCKS_IN_CYCLE * PRESERVES_CYCLE * TIME_BETWEEN_BLOCKS);

        return cycleInfo;
    }

    return cachedCycleInfo;
}

const getDelegatedAddresses = async (baker, level) => {
    const delegatedAddresses = await mpapi.rpc.getDelegatedAddresses(baker, level);

    return await async.mapLimit(
        delegatedAddresses.filter(address => address !== baker),
        2,
        async (address) => ({
            address,
            balance: mpapi.utility.totez(
                await mpapi.rpc.getMineBalance(address, level)
            )
        })
    )
}


const getBakerCycle = async (baker, cycle) => {
    const bakerCycle = await BakerCycle.findOne({
        address: baker,
        cycle: cycle,
    });

    if (bakerCycle) {
        return bakerCycle;
    }

    const cycleInfo = await getCycleInfo(cycle);

    let minDelegatorsBalances = [];
    let minFullStakingBalance = 0;
    let minOwnBalance = 0;
    let minDelegatedBalance = 0;

    for (let level = cycleInfo.first; level <= cycleInfo.last; level += STEP_PROCESS_CYCLE) {
        console.log(`Start checking for ${baker} in ${level}`);

        const gettingData = async (attemp) => {
            try {
                const levelDelegatorsBalances = await getDelegatedAddresses(baker, level);
                const fullStakingBalance = mpapi.utility.totez(await mpapi.rpc.getStakingMineBalance(baker, level));
                const ownBalance = mpapi.utility.totez(await mpapi.rpc.getOwnStakingMineBalance(baker, level));
                const delegatedBalance = mpapi.utility.totez(await mpapi.rpc.getDelegatedBalance(baker, level));

                return {
                    levelDelegatorsBalances,
                    fullStakingBalance,
                    ownBalance,
                    delegatedBalance
                };
            } catch (error) {
                console.log(`There is an error ${error} at getting data, attemp ${attemp}`);
                console.log('Repeat for getting data');
                return await gettingData(++attemp);
            }
        }

        const {levelDelegatorsBalances, fullStakingBalance, ownBalance, delegatedBalance} = await gettingData(0);

        if (level == cycleInfo.first) {
            minDelegatorsBalances = levelDelegatorsBalances;
            minFullStakingBalance = fullStakingBalance;
            minOwnBalance = ownBalance;
            minDelegatedBalance = delegatedBalance;
        }

        minFullStakingBalance = lodash.min([minFullStakingBalance, fullStakingBalance]);
        minOwnBalance = lodash.min([minOwnBalance, ownBalance]);
        minDelegatedBalance = lodash.min([minDelegatedBalance, delegatedBalance]);

        const stableLevelDelegators = lodash.intersectionBy(levelDelegatorsBalances, minDelegatorsBalances, 'address');
        if (!stableLevelDelegators.length) {
            minDelegatorsBalances = [];
            break;
        }

        if (stableLevelDelegators.length !== minDelegatorsBalances.length) {
            minDelegatorsBalances = lodash.intersectionBy(minDelegatorsBalances, stableLevelDelegators, 'address');
        }

        minDelegatorsBalances = lodash.zipWith(stableLevelDelegators, minDelegatorsBalances, (levelDelegator, cycleDelegator) => {
            return {
                address: levelDelegator.address,
                balance: lodash.min([cycleDelegator.balance, levelDelegator.balance])
            };
        });
    }
}

const getRewards = async (block, type = constants.REWARD_TYPES.FOR_BAKING, baker, {endorsers = [], slots = 0}) => {
    const level = block.metadata.level.level;
    const cycle = block.metadata.level.cycle;
    const priority = block.header.priority;
    const {baking_reward_per_endorsement, endorsement_reward} = await getBlockConstants(level);

    const bakerCycle = await getBakerCycle(baker, cycle - PRESERVES_CYCLE);
    if (!bakerCycle) {
        return [];
    }

    let totalReward = 0;
    switch (type) {
        case constants.REWARD_TYPES.FOR_BAKING:
            const countEndorsers = endorsers.reduce((count, endorser) => count + endorser.slots, 0)
            if (priority === 0)
                totalReward = baking_reward_per_endorsement[0] * countEndorsers;
            else
                totalReward = baking_reward_per_endorsement[1] * countEndorsers;
            break;
        case constants.REWARD_TYPES.FOR_ENDORSING:
            if (priority === 0)
                totalReward = endorsement_reward[0] * slots;
            else
                totalReward = endorsement_reward[1] * slots;
            break;
    }
    totalReward = mpapi.utility.totez(totalReward);

    let rewardOfAddresses = [];
    if (bakerCycle.fullCycleDelegators.length) {
        rewardOfAddresses = bakerCycle.fullCycleDelegators.map(delegator => ({
            address: delegator.address,
            reward: lodash.floor(totalReward / bakerCycle.minFullStakingBalance * delegator.minDelegatedBalance, 7),
            type,
            metadata: {
                priority,
                level,
                totalReward,
                countEndorsers: endorsers.length,
                countSlots: slots,
                bakingRewardConstant: baking_reward_per_endorsement,
                endorsementRewardConstant: endorsement_reward,
                minDelegatedBalance: delegator.minDelegatedBalance
            }
        }));
    }

    return rewardOfAddresses;
}

mongoose.connect(config.MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    useFindAndModify: false
}, async (error) => {
    if (error) throw error;

    await _rewards();
});

const getBlockConstants = async (level) => {
    if (!lodash.isNumber(level)) {
        throw new Error('Level must be a number');
    }

    const cachedBlockConstants = blockConstantsCache.get(level);

    if (!cachedBlockConstants) {
        const blockConstants = await mpapi.rpc.getConstants(level);
        blockConstantsCache.put(level, blockConstants, BLOCKS_IN_CYCLE * PRESERVES_CYCLE * TIME_BETWEEN_BLOCKS);

        return blockConstants;
    }

    return cachedBlockConstants;
}



const _rewards = async () => {
    const block = await getBlock(1111921);
    const level = block.header.level;
    const nextBlock = await getBlock(level + 1);
    // const baker = block.metadata.baker;
    const  baker  = config.BAKER_LIST[0]
    const blockEndorsers = getBlockEndorsers(nextBlock.operations);
    console.log(blockEndorsers.length)
    const rewardsB = await getRewardsForBaker(block, baker, blockEndorsers);
    const rewardsE = await getRewardsForEndorser(block, baker, 1);
}

// const _payment = async () => {
//     const lastLevel =
// }

//mp1CGpXtA9b74HpQ9fgyDA8W3pkUh6FMK2h4