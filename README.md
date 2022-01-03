401
401 * 1440 - 6 * 1440 =  

395: PAYED
396: 570240
397:
398:
399:
400:  
401: 577440
402: 578880

Before starting be sure that a blockchain node has been installed 

Clone existing repository
<pre><code>git clone https://github.com/mineplexio/Pool-Script.git</code></pre>

Add submodule js-rpcapi 
<pre><code>git submodule add https://github.com/mineplexio/js-rpcapi.git</code></pre>
<pre><code>git submodule update --remote</code></pre>

Install dependencies to js-rpcapi
<pre><code>cd js-rpcapi; npm install; cd ../</code></pre>

Copy from config-example.json to config.json and change this file up to you
<pre><code>cp config-example.json config.json</code></pre>

Run node process
<pre><code>npm run start</code></pre>
