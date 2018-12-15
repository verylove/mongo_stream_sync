import elasticsearch from 'elasticsearch'
import jsonpatch from 'json-patch'

const logger = new (require('service-logger'))(__filename)
const versioning = require('./versioning')

class ElasticManager {
  constructor (elasticOpts, mappings, bulkSize) {
    this.esClient = new elasticsearch.Client(elasticOpts)
    this.mappings = mappings
    this.bulkSize = bulkSize
    this.bulkOp = []
    this.interval = null
  }

  // call the appropriate replication function based on the change object parsed from a change stream
  replicate (change) {
    if (!this.interval) {
      this.interval = setInterval(() => {
        clearInterval(this.interval)
        this.interval = null
        this.sendBulkRequest(this.bulkOp)
        this.bulkOp = []
      }, 500)
    }

    const replicationFunctions = {
      'insert': this.insertDoc,
      'update': this.insertDoc,
      'replace': this.insertDoc,
      'delete': this.deleteDoc
    }

    if (replicationFunctions.hasOwnProperty(change.operationType)) {
      logger.info(`- ${change.documentKey._id.toString()}: ${change.ns.coll} ${change.operationType}`)
      return replicationFunctions[change.operationType].call(this, change)
    } else {
      logger.error(`REPLICATION ERROR: ${change.operationType} is not a support function`)
    }
  }

  // insert event format https://docs.mongodb.com/manual/reference/change-events/#insert-event
  insertDoc (changeStreamObj) {
    if (changeStreamObj.fullDocument === null) return
    const esId = changeStreamObj.fullDocument._id.toString() // convert mongo ObjectId to string
    delete changeStreamObj.fullDocument._id
    const esReadyDoc = changeStreamObj.fullDocument

    this.bulkOp.push({
      index: {
        _index: this.mappings[changeStreamObj.ns.coll].index,
        _type: this.mappings[changeStreamObj.ns.coll].type,
        _id: esId,
        _parent: esReadyDoc[this.mappings[changeStreamObj.ns.coll].parentId],
        _versionType: this.mappings[changeStreamObj.ns.coll].versionType,
        _version: versioning.getVersionAsInteger(esReadyDoc[this.mappings[changeStreamObj.ns.coll].versionField])
      }
    })

    const transformedDoc = this.transformDoc(changeStreamObj.ns.coll, esReadyDoc)

    this.bulkOp.push(transformedDoc)
  }

  transformDoc (collName, esReadyDoc) {
    const transformFunc = this.mappings[collName].transformFunc
    const transformations = this.mappings[collName].transformations
    if (transformFunc) {
      return transformFunc(esReadyDoc)
    } else if (transformations) {
      return jsonpatch.apply(esReadyDoc, transformations)
    } else {
      return esReadyDoc
    }
  }

  async deleteDoc (changeStreamObj) {
    const esId = changeStreamObj.documentKey._id.toString() // convert mongo ObjectId to string

    const { parentId, version } = await this.getExistingDoc(this.mappings[changeStreamObj.ns.coll], esId).catch((err) => {
      logger.error(`error finding existing document in delete: ${err}`)
    })

    this.bulkOp.push({
      delete: {
        _index: this.mappings[changeStreamObj.ns.coll].index,
        _type: this.mappings[changeStreamObj.ns.coll].type,
        _id: esId,
        _parent: this.mappings[changeStreamObj.ns.coll].parentId,
        _versionType: this.mappings[changeStreamObj.ns.coll].vserionType,
        _version: versioning.incrementVersionForDeletion(version)
      }
    })
  }

  async getExistingDoc (collection, id) {
    try {
      const doc = await this.esClient.search({
        index: collection.index,
        type: collection.type,
        q: `_id:${id}`,
        size: 1,
        version: Boolean(collection.versionType)
      })

      return {
        parentId: doc.hits.hits[0]._parent || null,
        version: doc.hits.hits[0]._version || null
      }
    } catch (err) {
      logger.error(`cannot find item of type ${collection.type} with id ${id}`)
    }
  }

  // delete all docs in ES before dumping the new doc into it
  async deleteElasticCollection (collectionName) {
    let searchResponse
    try {
      // first get a count for all ES docs of the specified type
      searchResponse = await this.esClient.search({
        index: this.mappings[collectionName].index,
        type: this.mappings[collectionName].type,
        size: this.bulkSize,
        scroll: '1m',
        version: Boolean(this.mappings[collectionName].versionType)
      })
    } catch (err) {
      // if the search query failed, the index or type does not exist
      searchResponse = { hits: { total: 0 } }
    }

    // loop through all existing esdocs in increments of bulksize, then delete them
    let numDeleted = 0
    for (let i = 0; i < Math.ceil(searchResponse.hits.total / this.bulkSize); i++) {
      const bulkDelete = []
      const dumpDocs = searchResponse.hits.hits
      for (let j = 0; j < dumpDocs.length; j++) {
        bulkDelete.push({
          delete: {
            _index: this.mappings[collectionName].index,
            _type: this.mappings[collectionName].type,
            _id: dumpDocs[j]._id,
            _parent: dumpDocs[j]._parent,
            _versionType: this.mappings[collectionName].versionType,
            _version: versioning.incrementVersionForDeletion(dumpDocs[j]._version)
          }
        })
      }
      numDeleted += dumpDocs.length
      logger.info(`${collectionName} delete progress: ${numDeleted}/${searchResponse.hits.total}`)
      searchResponse = await this.esClient.scroll({
        scrollId: searchResponse._scroll_id,
        scroll: '1m'
      })
      await this.sendBulkRequest(bulkDelete)
    }
    return numDeleted
  }

  setMappings (collection) {
    // set mappings between mongo and elasticsearch if they do not yet exist
    if (!this.mappings[collection]) {
      this.mappings[collection] = {}
    }

    if (!this.mappings[collection].index) {
      this.mappings[collection].index = this.mappings.default.index
      if (this.mappings[collection].index === '$self') {
        this.mappings[collection].index = collection
      }
    }

    if (!this.mappings[collection].type) {
      this.mappings[collection].type = this.mappings.default.type
      if (this.mappings[collection.type] === '$self') {
        this.mappings[collection].type = collection
      }
    }

    if (this.mappings[collection].transformations) {
      this.mappings[collection].transformFunc = jsonpatch.compile(this.mappings[collection].transformations)
    }
  }

  async sendBulkRequest (bulkOp) {
    if (bulkOp.length === 0) {
      return
    }

    const response = await this.esClient.bulk({
      refresh: false,
      body: bulkOp
    })

    if (!response.errors) { return }

    try {
      response.items.forEach(item => {
        let erroredItem
        if (item.delete && item.delete.error) {
          erroredItem = item.delete
        } else if (item.index && item.index.error) {
          erroredItem = item.index
        }

        if (erroredItem) {
          logger.error(`bulk request error:`, erroredItem)
          if (erroredItem.error.type === 'routing_missing_exception') {
            logger.debug('This is most likely to a missing child parent relationship in the config. See default config file for reference.')
          }
        }
      })
    } catch (err) {
      logger.error(`Bulk Error`, err)
    }
  }
}

module.exports = ElasticManager
