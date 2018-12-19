const fs = require('fs')
const logger = new (require('service-logger'))(__filename)
const BSON = require('bson')

class ResumeToken {
  constructor (collection) {
    this.collection = collection
    this.token = null
  }

  async get () {
    if (!this.token) {
      if (ResumeToken.storageCollection) {
        await this.getFromCollection()
      } else {
        this.getFromFile()
      }
    }

    return this.token
  }

  async getFromCollection () {
    try {
      const dbToken = await ResumeToken.storageCollection.findOne({ _id: this.collection })
      if (!dbToken) {
        this.token = null
      } else {
        this.token = dbToken.token
      }
    } catch (err) {
      logger.err(`resumeToken for ${this.collection} could not be retrieved from database`)
      logger.debug(err)
      this.token = null
    }
  }

  getFromFile () {
    try {
      const base64Buffer = fs.readFileSync(`./resumeTokens/${this.collection}`)
      this.token = BSON.deserialize(base64Buffer)
    } catch (err) {
      logger.err(`resumeToken for ${this.collection} could not be retrieved from file`)
      logger.debug(err)
      this.token = null
    }
  }

  async write () {
    if (ResumeToken.storageCollection) {
      await this.writeToCollection()
    } else {
      this.writeToFile()
    }
  }

  writeToFile () {
    if (!this.token) {
      fs.unlink(`./resumeToken/${this.collection}`)
      return
    }

    const b64String = BSON.serialize(this.token).toString('base64')
    fs.writeFileSync(`./resumeTokens/${this.collection}`, b64String, 'base64')
    logger.debug(`resumeToken for collection ${this.collection} saved to disk`)
  }

  async writeToCollection () {
    try {
      if (!this.token) {
        await ResumeToken.storageCollection.removeOne({ _id: this.collection })
        return
      }

      await ResumeToken.storageCollection.updateOne(
        { _id: this.collection },
        { $set: { token: this.token } },
        { upsert: true }
      )
      logger.debug(`resumeToken for collection ${this.collection} saved to database`)
    } catch (err) {
      logger.debug(`resumeToken for collection ${this.collection} could not be saved to database`)
      logger.debug(err)
    }
  }

  async reset () {
    this.token = null
    await this.write()
  }
}

module.exports = ResumeToken
