const { Client, Server } = require('hyperspace')
const ram = require('random-access-memory')
const test = require('tape')

test('write & read a block with two clients', async t => {
    const server = new Server({
        storage: ram,
        network: {
            bootstrap: [], // no external dht
            preferredPort: 0
        },
        noMigrate: true
    })
    await server.ready()
    const client1 = new Client()
    await client1.ready()
    const client2 = new Client()
    await client2.ready()
    const cleanup = async () => {
        await server.close()
        await client1.close()
        await client2.close()
    }
    // write on client1
    let key = null
    {
        const corestore = client1.corestore()
        const core = corestore.get()
        await core.ready()
        key = core.key
        await core.append(Buffer.from('hello world', 'utf8'))
        const block = await core.get(0)
        t.same(block.toString('utf8'), 'hello world')
    }
    // read on client2
    {
        const corestore = client2.corestore()
        const core = corestore.get(key)
        await core.ready()
        const block = await core.get(0)
        t.same(block.toString('utf8'), 'hello world')
    }

    cleanup()
    t.end()
})
