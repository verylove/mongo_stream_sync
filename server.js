import express from 'express';
import { json } from 'body-parser';
import MongoStream from './lib/mongo-stream';
import CollectionManager from './lib/collectionManager';
import config from './lib/configParser';

const logger = new (require('service-logger'))(__filename);
let mongoStream;

const app = express();
app.use(json());  //for parsing application/json

// return the status of all collectionManagers currently running
app.get('/', (request, response) => {
    
})
