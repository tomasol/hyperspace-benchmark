const { Client, Server } = require('hyperspace')
const ram = require('random-access-memory')
const test = require('tape')
const { createDHTServer } = require('../helpers/utils.js')

test('write & read a block with two servers', async t => {
    let cleanups = []
    try {
        const { bootstrap, dhtCleanup } = await createDHTServer()
        cleanups.push(dhtCleanup)

        const server1 = new Server({
            storage: ram,
            network: {
                bootstrap
            },
            noMigrate: true,
            host: 'hyperspace-1'
        })
        cleanups.push(async () => server1.close())
        await server1.ready()

        const server2 = new Server({
            storage: ram,
            network: {
                bootstrap
            },
            noMigrate: true,
            host: 'hyperspace-2'
        })
        cleanups.push(async () => server2.close())
        await server2.ready()

        const client1 = new Client({ host: 'hyperspace-1' })
        cleanups.push(async () => client1.close())
        await client1.ready()
        const client2 = new Client({ host: 'hyperspace-2' })
        cleanups.push(async () => client2.close())
        await client2.ready()
        // write on client1
        let key = null, discoveryKey = null
        {
            const corestore = client1.corestore()
            const core = corestore.get()
            await core.ready()
            key = core.key
            discoveryKey = core.discoveryKey
            await core.append(Buffer.from('hello world', 'utf8'))
            await client1.network.configure(discoveryKey, { announce: true, lookup: true, flush: true })
        }
        // read on client2
        {
            const corestore = client2.corestore()
            const core = corestore.get(key)
            await core.ready()
            await client2.network.configure(discoveryKey, { announce: false, lookup: true })
            const block = await core.get(0)
            t.same(block.toString('utf8'), 'hello world')
        }
    } catch (err) {
        t.error(err)
    } finally {
        for (const cleanup of cleanups) {
            await cleanup()
        }
        t.end()
    }
})
