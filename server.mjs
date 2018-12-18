import express from 'express'
import { json } from 'body-parser'
import MongoStream from './lib/mongo-stream'
import CollectionManager from './lib/collectionManager'
import config from './lib/configParser'

const logger = new (require('service-logger'))(__filename)
let mongoStream

const app = express()
app.use(json()) // for parsing application/json

// return the status of all collectionManagers currently running
app.get('/', (request, response) => {
  const collectionManagers = Object.values(mongoStream.collectionManagers)
  const responseBody = { total: collectionManagers.length }
  collectionManagers.forEach(manager => {
    if (manager.changeStream) {
      responseBody[manager.collection] = 'Listening'
    } else {
      responseBody[manager.collection] = 'Not Listening'
    }
  })

  response.send(responseBody)
})

// return mappings of all collectionManagers currently running
app.get('/mappings', (request, response) => {
  response.send(mongoStream.elasticManager.mappings)
})

app.post('/collection-manager?', (request, response) => {
  logger.info(request.body)
  const collections = request.body.collections
  const managerOptions = {
    dump: request.body.dump,
    ignoreResumeToken: request.body.ignoreResumeToken,
    ignoreDumpProgress: request.body.ignoreDumpProgress,
    watch: request.body.watch
  }

  return mongoStream.addCollectionManager(collections, managerOptions)
    .then((results) => {
      response.send(results)
    }).catch(err => {
      logger.error(`Error posting collection-manager:`, err)
      response.send(err)
    })
})

// toggle dump progress
app.put('/dump/:toggle', (request, response) => {
  switch (request.params.toggle) {
    case 'pause':
      CollectionManager.pauseDump()
      response.send('Dump paused. To resume, use "/dump/resume"')
      break
    case 'resume':
      CollectionManager.resumeDump()
      response.send('Dump resumed.')
      break
    default:
      response.send(`ERROR: unknown dump option "${request.params.toggle}"`)
      break
  }
})

// triggers a remove for the specified collections
app.delete('/collection-manager/:collections?', (request, response) => {
  const collections = request.params.collections.split(',')
  logger.info(`Deleting collections: ${collections}`)

  return mongoStream.removeCollectionManager(collections)
    .then((results) => {
      logger.info(`Remaining collections after Delete: ${results}`)
      response.send(results)
    }).catch(err => {
      response.send(err)
    })
})

app.listen(config.adminPort, (err) => {
  if (err) {
    return logger.error(`Error listening on ${config.adminPort}:`, err)
  }

  MongoStream.init(config)
    .then((stream) => {
      logger.info('connected')
      mongoStream = stream
    }).catch((err) => {
      logger.error(`Error Creating MongoStream:`, err)
      process.exit()
    })

  logger.info(`server is listening on port ${config.adminPort}`)
})
