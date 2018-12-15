import MongoClient from 'mongodb'
import ElasticManager from './elasticManager'
import CollectionManager from './collectionManager'

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
      ignoreResumeTokens: options.ignoreResumeTokensOnStart
    }
  }

}
