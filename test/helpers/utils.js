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


function drive_ready(drive) {
    return new Promise((resolve, reject) => {
        drive.ready(err => {
            if (err) {
                reject(err)
            } else {
                resolve()
            }
        })
    })
}

function drive_writeFile(drive, path, content) {
    return new Promise((resolve, reject) => {
        drive.writeFile(path, content, (err) => {
            if (err) {
                reject(err)
            } else {
                resolve()
            }
        })
    })
}

function drive_readFile(drive, path, encoding) {
    return new Promise((resolve, reject) => {
        drive.readFile(path, encoding, (err, content) => {
            if (err) {
                reject(err)
            } else {
                resolve(content)
            }
        })
    })
}

module.exports = {
    createDHTServer,
    drive_ready,
    drive_writeFile,
    drive_readFile
}
