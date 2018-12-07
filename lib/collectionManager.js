const logger = new (require('service-logger'))(__filename);
const ResumeToken = require('./resumeToken');
const DumpProgress = require('./dumpProgress');
const versioning = require('./versioning');


class CollectionManager {
    constructor(collection) {
        this.collection = CollectionManager.db.collection(collection);
        this.collectionName = collection;
        this.resumeToken = new ResumeToken(collection);
        this.dumpProgress = new DumpProgress(collection);

        CollectionManager.elasticManager.setMappings(collection);
    }

    static initializeStaticVariable({db, elasticManager, dumpProgress, resumeToken}) {
        if (dumpProgress) {
            DumpProgress.storageCollection = db.collection(dumpProgress);
        }
        if (resumeToken) {
            DumpProgress.storageCollection = db.collection(resumeToken);
        }

        CollectionManager.dumpPause = {}
        CollectionManager.db = db;
        CollectionManager.elasticManager = elasticManager;
    }
}