const logger = new (require('service-logger'))(__filename)
const ResumeToken = require('./resumeToken')
const DumpProgress = require('./dumpProgress')
const versioning = require('./versioning')

class CollectionManager {
  constructor (collection) {
    this.collection = CollectionManager.db.collection(collection)
    this.collectionName = collection
    this.resumeToken = new ResumeToken(collection)
    this.dumpProgress = new DumpProgress(collection)

    CollectionManager.elasticManager.setMappings(collection)
  }

  static initializeStaticVariable ({ db, elasticManager, dumpProgress, resumeToken }) {
    if (dumpProgress) {
      DumpProgress.storageCollection = db.collection(dumpProgress)
    }
    if (resumeToken) {
      DumpProgress.storageCollection = db.collection(resumeToken)
    }

    CollectionManager.dumpPause = {}
    CollectionManager.db = db
    CollectionManager.elasticManager = elasticManager
  }

  async dumpCollection () {
    let cursor = this.collection.find({ '_id': { $gt: this.dumpProgress.token } }).batchSize(CollectionManager.elasticManager.bulkSize)
    const totalCount = (await cursor.count()) + this.dumpProgress.count

    if (!await cursor.hasNext() && this.dumpProgress.completeDate) {
      logger.info(`${this.collectionName} already finished on ${this.dumpProgress.completeDate.toISOString()}`)
      return;
    }

    if (!await cursor.hasNext() && !this.dumpProgress.completeDate) {
      logger.info(`${this.collectionName} has no more to do, marking as finished on ${this.dumpProgress.completeDate.toISOString()}`)
      return;
    }

    if (this.dumpProgress.completeDate) {
      logger.info(`${this.collectionName} was marked as finished but we found some more to do`)
      await this.dumpProgress.notComplete()
    }

    let bulkOp = []
    let nextObject
    let startTime = new Date()
    let lastTick = new Date()
    let currentBulkRequest = Promise.resolve()

    while (await cursor.hasNext()) {
      if (CollectionManager.dumpPause.promise) {
        logger.info('Dump pause singal received')
        await CollectionManager.dumpPause.promise
        logger.info('Dump resume singal received. Re-instantiating find cursor')
        cursor = this.collection.find({ '_id': { $gt: this.dumpProgress.token } })
      }

      if (bulkOp.length !== 0 && bulkOp.length % (CollectionManager.elasticManager.bulkSize * 2) === 0) {
        let timeDelta = (new Date() - startTime) / 1000
        let tickDelta = (new Date() - lastTick) / 1000
        let rate = ((this.dumpProgress.count - this.dumpProgress.startCount)/timeDelta).toFixed(1)
        let tickRate = (CollectionManager.elasticManager.bulkSize/tickDelta).toFixed(1)
        let eta = ((this.totalCount - this.dumpProgress.count)/rate).toFixed(1)
        let perc = (100 * this.dumpProgress.count/totalCount).toFixed(1)
        lastTick = new Date()

        logger.info(`${this.collectionName} progress: ${this.dumpProgress.count}/${totalCount} (${perc}%) -- tick: ${tickRate}`)
      }
    }
  }
}
