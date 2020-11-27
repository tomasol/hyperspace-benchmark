/*
Execute all hyperspace benchmarks:
`npm run benchmark-hyperspace`
Filtering is possible, each argument must be matched by benchmark name:
`npm run benchmark-hyperspace ram 1000`
*/
const why = require('why-is-node-running') // should be your first require
const Benchmark = require('benchmark');
const { Client, Server } = require('hyperspace');
const ram = require('random-access-memory')
const hdr = require("hdr-histogram-js")
const microtime = require('microtime')
const tmp = require('tmp-promise')
const { createDHTServer } = require('../helpers/utils.js')

const useWasmForHistograms = false
const outputHistogramsToPlotter = false
/*
options:
readerCount - optional, number of parallel reader clients. Defaults to 1.
blocks - optional, how many blocks to read and write. Defaults to 1.
writeFun - (feed, client) - what to read / write
readFun - (feed, blockIdx, client, discoveryKey) - what to read
writerClientFun - optional. Writer client factory. Defaults to `() => new Client()`.
readerClientFun - optional. Readers client factory. Defaults to `(idx) => new Client()`.
*/
async function readAndWrite(histograms, server, options) {
    options.readerCount = options.readerCount ?? 1
    options.blocks = options.blocks ?? 1
    options.writerClientFun = options.writerClientFun ?? (() => new Client())
    options.readerClientFun = options.readerClientFun ?? ((idx) => new Client())

    await record(histograms, 'server.ready', async () => await server.ready())
    const writerClient = options.writerClientFun()
    await record(histograms, 'writerClient.ready', async () => await writerClient.ready())
    const readerClients = []
    for (let r = 0; r < options.readerCount; r++) {
        readerClients[r] = options.readerClientFun(r)
    }
    await record(histograms, 'readerClients.ready', async () => {
        await Promise.all(readerClients.map(client => client.ready()))
    })
    const cleanup = async () => {
        await record(histograms, 'readAndWrite.closeAll', async () => {
            await record(histograms, 'server.close', async () => await server.close())
            await record(histograms, 'writerClient.close', async () => await writerClient.close())
            await record(histograms, 'readerClients.close', async () => {
                await Promise.all(readerClients.map(client => client.close()))
            })
        })
    }
    // write on writerClient
    let key = null, discoveryKey = null
    {
        const corestore = writerClient.corestore()
        const feed = corestore.get()
        await record(histograms, 'writerClient.core.ready()', async () => await feed.ready())
        key = feed.key
        discoveryKey = feed.discoveryKey
        await record(histograms, 'writerClient.writeAll', async () => {
            for (let blockIdx = 0; blockIdx < options.blocks; blockIdx++) {
                await record(histograms, 'writerClient.write single block',
                    async () => await options.writeFun(feed, writerClient))
            }
        })
    }
    // read on readerClients
    {
        const corestores = await record(histograms, 'readerClients.corestore()',
            () => readerClients.map(client => {
                return {
                    client, corestore: client.corestore()
                }
            }))
        const feeds = await record(histograms, 'readerClients.corestores.get(key)',
            () => corestores.map(clientAndCorestore => {
                return {
                    client: clientAndCorestore.client, feed: clientAndCorestore.corestore.get(key)
                }
            }))
        // wait until all clients are ready
        await record(histograms, 'readerClients.feed.ready()',
            async () => await Promise.all(feeds.map(feedAndClient => feedAndClient.feed.ready())))
        // reading is parallel
        await record(histograms, 'readerClients.readAll', async () => {
            const readAll = async function (feed, client) {
                await record(histograms, 'readerClient.readAll', async () => {
                    for (let blockIdx = 0; blockIdx < options.blocks; blockIdx++) {
                        await record(histograms, 'readerClient.read single block',
                            async () => await options.readFun(feed, blockIdx, client, discoveryKey))
                    }
                })
            }
            await Promise.all(feeds.map(feedAndClient => readAll(feedAndClient.feed, feedAndClient.client)))
        })
    }
    await cleanup()
}

async function writeMessage(core, message) {
    await core.append(Buffer.from(message, 'utf8'))
    const block = await core.get(0)
    if (block.toString('utf8') != message) {
        throw new Error('Not same')
    }
}
async function readMessage(core, idx, message) {
    const block = await core.get(idx)
    if (block.toString('utf8') != message) {
        throw new Error('Not same')
    }
}

function outputHistograms(histograms) {
    const summaries = {}
    const encodedSummaries = {}
    for (key in histograms) {
        const h = histograms[key]
        summaries[key] = {
            mean: h.mean,
            ...h.summary
        }
        if (outputHistogramsToPlotter) {
            encodedSummaries[key] = hdr.encodeIntoCompressedBase64(h)
        }
        h.destroy()
    }
    console.log(summaries)
    if (outputHistogramsToPlotter) {
        console.log(encodedSummaries) // view using https://hdrhistogram.github.io/HdrHistogramJSDemo/plotFiles.html
    }
}

function newHist() {
    return hdr.build({ useWebAssembly: useWasmForHistograms })
}

async function record(histograms, key, fn) {
    if (!histograms[key]) {
        histograms[key] = newHist()
    }
    const start = microtime.now()
    const result = await fn()
    histograms[key].recordValue(microtime.now() - start)
    return result
}

function executeBenchmark(name, histograms, fn) {
    for (const word of process.argv.slice(2)) {
        const pattern = new RegExp(word, 'i')
        if (!name.match(pattern)) {
            console.log(`Skipping '${name}'`)
            return
        }
    }
    console.log(`Starting '${name}'`)
    const suite = new Benchmark.Suite
    return new Promise(function (resolve, reject) {
        const histogram = hdr.build();
        suite
            .add(name, (deferred) => {
                record(histograms, 'total', async () => fn(deferred, histograms))
            }, { defer: true })
            .on('cycle', (event) => {
                console.log(String(event.target));
                outputHistograms(histograms)
                resolve();

            })
            .run();
    })
}

function allocMessage(recordSize) {
    return Buffer.allocUnsafe(recordSize).fill(Math.floor(Math.random() * 10))
}

async function runAll() {

    const message10B = allocMessage(10)
    const message1KB = allocMessage(1000)
    const message1MB = allocMessage(1000000)

    await executeBenchmark('Start and stop single RAM server',
        {},
        async function (deferred, histograms) {
            const server = new Server({
                storage: ram,
                network: { bootstrap: [] },
                noMigrate: true
            })
            await record(histograms, 'serverReady', async () => server.ready())
            await record(histograms, 'serverClose', async () => server.close())
            deferred.resolve()
        })

    await executeBenchmark('Start and stop single RAM server and single client',
        {},
        async function (deferred, histograms) {
            const server = new Server({
                storage: ram,
                network: { bootstrap: [] },
                noMigrate: true
            })
            await record(histograms, 'serverReady', async () => await server.ready())
            const client = new Client()
            await record(histograms, 'clientReady', async () => await client.ready())
            await record(histograms, 'clientClose', async () => await client.close())
            await record(histograms, 'serverClose', async () => await server.close())
            deferred.resolve()
        })

    await executeBenchmark('Create single RAM server, write 1 block with 10B and read from another client',
        {},
        async function (deferred, histograms) {
            const server = new Server({
                storage: ram,
                network: { bootstrap: [] },
                noMigrate: true
            })
            await record(histograms, 'readAndWrite', async () => await readAndWrite(histograms, server, {
                writeFun: (core) => writeMessage(core, 'helloworld'),
                readFun: (core, idx) => readMessage(core, idx, 'helloworld'),
                blocks: 1
            }))
            deferred.resolve()
        })

    await executeBenchmark('Create single FS server, write 1 block with 10B and read from another client',
        {},
        async function (deferred, histograms) {
            const tmpDir = await record(histograms, 'mkdir', async () => await tmp.dir({ unsafeCleanup: true }))
            const server = new Server({
                storage: tmpDir.path,
                network: { bootstrap: [] },
                noMigrate: true
            })
            await record(histograms, 'readAndWrite', async () => await readAndWrite(histograms, server, {
                writeFun: (core) => writeMessage(core, 'helloworld'),
                readFun: (core, idx) => readMessage(core, idx, 'helloworld'),
                blocks: 1
            }))
            deferred.resolve()
            await record(histograms, 'cleanup dir', async () => await tmpDir.cleanup())
        })

    await executeBenchmark('Create single RAM server, write 1000 blocks with 10B and read from another client',
        {},
        async function (deferred, histograms) {
            const server = new Server({
                storage: ram,
                network: { bootstrap: [] },
                noMigrate: true
            })
            await record(histograms, 'readAndWrite', async () => await readAndWrite(histograms, server, {
                writeFun: (core) => writeMessage(core, 'helloworld'),
                readFun: (core, idx) => readMessage(core, idx, 'helloworld'),
                blocks: 1000
            }))
            deferred.resolve()
        })

    await executeBenchmark('Create single FS server, write 1000 blocks with 10B and read from another client',
        {},
        async function (deferred, histograms) {
            const tmpDir = await record(histograms, 'mkdir', async () => await tmp.dir({ unsafeCleanup: true }))
            const server = new Server({
                storage: tmpDir.path,
                network: { bootstrap: [] },
                noMigrate: true
            })
            await record(histograms, 'readAndWrite', async () => await readAndWrite(histograms, server, {
                writeFun: (core) => writeMessage(core, 'helloworld'),
                readFun: (core, idx) => readMessage(core, idx, 'helloworld'),
                blocks: 1000
            }))
            deferred.resolve()
            await record(histograms, 'cleanup dir', async () => await tmpDir.cleanup())
        })

    await executeBenchmark('Create single FS server, write 1000 blocks with 10B and read from another 10 clients',
        {},
        async function (deferred, histograms) {
            const tmpDir = await record(histograms, 'mkdir', async () => await tmp.dir({ unsafeCleanup: true }))
            const server = new Server({
                storage: tmpDir.path,
                network: { bootstrap: [] },
                noMigrate: true
            })
            await record(histograms, 'readAndWrite', async () => await readAndWrite(histograms, server, {
                writeFun: (core) => writeMessage(core, 'helloworld'),
                readFun: (core, idx) => readMessage(core, idx, 'helloworld'),
                blocks: 1000,
                readerCount: 10,
            }))
            deferred.resolve()
            await record(histograms, 'cleanup dir', async () => await tmpDir.cleanup())
        })

    const benchmarkDHTFn = function (message, blocks) {
        return async function (deferred, histograms) {
            let cleanups = {}
            const { bootstrap, dhtCleanup } = await record(histograms, 'dht.init',
                async () => await createDHTServer())
            cleanups.dht = async () => dhtCleanup()
            const writerServer = new Server({
                storage: ram,
                network: { bootstrap },
                noMigrate: true,
                host: 'hyperspace-writer'
            })
            // ready() and close() are called in readAndWrite
            const readerServer = new Server({
                storage: ram,
                network: {
                    bootstrap
                },
                noMigrate: true,
                host: 'hyperspace-reader'
            })
            cleanups.readerServer = async () => readerServer.close()
            await record(histograms, 'readerServer.init', async () => await readerServer.ready())
            await record(histograms, 'readAndWrite', async () => await readAndWrite(histograms, writerServer, {
                blocks,
                writerClientFun: () => new Client({ host: 'hyperspace-writer' }),
                readerClientFun: (idx) => new Client({ host: 'hyperspace-reader' }),
                writeFun: async (feed, client) => {
                    await writeMessage(feed, message)
                    await client.network.configure(feed.discoveryKey, { announce: true, lookup: true, flush: true })
                },
                readFun: async (feed, blockIdx, client, discoveryKey) => {
                    await client.network.configure(discoveryKey, { announce: false, lookup: true })
                    await readMessage(feed, blockIdx, message)
                }
            }))
            // cleanup
            for (const key in cleanups) {
                await record(histograms, 'close.' + key, async () => await cleanups[key]())
            }
            deferred.resolve()
        }
    }

    await executeBenchmark('Create DHT, two RAM servers, write 1 block with 10B and read using another server and client',
        {},
        benchmarkDHTFn(message10B, 1))


    await executeBenchmark('Create DHT, two RAM servers, write 1000 blocks with 1KB and read using another server and client',
        {},
        benchmarkDHTFn(message1KB, 1000))

    await executeBenchmark('Create DHT, two RAM servers, write 100 blocks with 1MB and read using another server and client',
        {},
        benchmarkDHTFn(message1MB, 100))
}

require('events').EventEmitter.defaultMaxListeners = 20; // avoid warning when registering 10 reader clients
if (useWasmForHistograms) hdr.initWebAssemblySync() // faster hdr metrics
runAll() // execute benchmarks
