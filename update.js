const async = require('async');

const Reward = require('./models/reward')();

const _update = async () => {
    const update = await Reward.updateMany(
        {level: {$gt: 532800}},
        {
            $set: {paymentOperationHash: null}
        });
};

module.exports = {
    _update
}
