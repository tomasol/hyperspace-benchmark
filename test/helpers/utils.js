const dht = require('@hyperswarm/dht')

createDHTServer = async function() {
    const bootstrapper = dht({
        bootstrap: false
    })
    bootstrapper.listen()
    await new Promise(resolve => {
        return bootstrapper.once('listening', resolve)
    })
    const bootstrapPort = bootstrapper.address().port
    const dhtCleanup = async () => { await bootstrapper.destroy() }
    return { bootstrap: [`localhost:${bootstrapPort}}`], dhtCleanup }
}

module.exports = {
    createDHTServer
}
