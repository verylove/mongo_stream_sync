const MongoClient = require('mongodb')
const ElasticManager = require('./elasticManager')
const CollectionManager = require('./collectionManager')

const logger = new (require('service-logger'))(__filename)

class MongoStream {
  constructor (elasticManager, db, resumeTokenInterval = 60000) {
    this.elasticManager = elasticManager
    this.db = db
    this.collectionManagers = {}

    // after reconnect to mongo, restart all change streams
    db.on('reconnect', () => {
      logger.info('connect reestablished with mongoDB')
      const collectionManagers = Object.values(this.collectionManagers)
      collectionManagers.forEach(async (manager) => {
        await manager.resumeToken.get()
        await manager.resetChangeStream({ dump: false, ignoreResumeToken: false })
      })
    })

    // write resume tokens to file on an interval
    setInterval(() => {
      this.writeAllResumeTokens()
    }, resumeTokenInterval)
  }

  // contructs and return new MongoStream
  static async init (options) {
    const client = MongoClient.connect(options.url, options.mongoOpts)
    const db = client.db(options.db)
    // log any db events emitted
    db.on('close', (log) => { logger.info(`close`, log) })
    db.on('error', (err) => { logger.error(`db Error`, err) })
    db.on('parseError', (err) => { logger.error(`db parse Error`, err) })
    db.on('timeout', (err) => {
      logger.error(`db timeout`, err)
      this.writeAllResumeTokens()
      process.exit()
    })

    await db.createCollection('init')
    await db.dropCollection('init')
    const elasticManager = new ElasticManager(options.elasticOpts, options.mappings, options.bulkSize, options.parentChildRelations)
    const resumeTokenInterval = options.resumeTokenInterval
    const mongoStream = new MongoStream(elasticManager, db, resumeTokenInterval)
    const managerOptions = {
      dump: options.dumpOnStart,
      ignoreResumeTokens: options.ignoreResumeTokensOnStart,
      watch: true
    }

    CollectionManager.initializeStaticVariable({
      db,
      elasticManager,
      dumpProgress: options.dumpProgressCollection,
      resumeToken: options.resumeTokenCollection
    })

    await mongoStream.addCollectionManager(options.collections, managerOptions)
    return mongoStream
  }

  writeAllResumeTokens () {
    const collectionManagers = Object.values(this.collectionManagers)
    collectionManagers.forEach(manager => {
      manager.resumeToken.write()
    })
  }

  // accept single collection or array
  async addCollectionManager (collections, options) {
    if (!Array.isArray(collections)) collections = [collections]
    await this.removeCollectionManager(collections)

    for (const collection of collections) {
      const collectionManager = new CollectionManager(collection)
      if (options.dump) {
        await collectionManager.dumpProgress.get()

        if (options.ignoreDumpProgress || collectionManager.dumpProgress.count === 0) {
          await this.elasticManager.deleteElasticCollection(collection)
          await collectionManager.dumpProgress.reset()
        }
        await collectionManager.dumpCollection()
      }

      if (options.watch) { await collectionManager.watch(options.ignoreResumeTokens) }

      this.collectionManagers[collection] = collectionManager
    }
  }

  async removeCollectionManager (collections) {
    if (!Array.isArray(collections)) collections = [collections]
    for (const collection of collections) {
      if (this.collectionManagers[collection]) {
        this.collectionManagers[collection].removeChangeStream()
        delete this.collectionManagers[collection]
      }
    }

    return Object.keys(this.collectionManagers)
  }
}

module.exports = MongoStream
