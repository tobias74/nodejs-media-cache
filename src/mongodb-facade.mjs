import {createRequire} from 'module';
const require = createRequire(import.meta.url);

export class MongoDbFacade {

  constructor(url, databaseName) {
    this.url = url;
    this.databaseName = databaseName;

    this.mongodb = require('mongodb');
    this.gridFsBucket = require('mongodb').GridFSBucket;

    this.isConnected = new Promise((resolve, reject) => {
      this.isConnectedResolver = resolve;
      this.isConnectedRejector = reject;
    });

    this.connect();

  }

  connect() {
    let MongoClient = this.mongodb.MongoClient;
    let client = new MongoClient(this.url, { useUnifiedTopology: true });
    client.connect(err => {
      if (err) {
        console.log(err);
      }

      this.db = client.db(this.databaseName);
      this.myGfsBucket = new this.gridFsBucket(this.db);

      this.isConnectedResolver(true);
    });
  }

  async getGfsBucket() {
    return this.isConnected.then((isConnected) => {
      if (isConnected) {
        return this.myGfsBucket;
      }
    });
  }

  async storeFileByStream(stream, contentType, metadata) {
    return this.isConnected.then((isConnected) => {
      if (isConnected) {
        return new Promise((resolve, reject) => {
          let writeStream = this.myGfsBucket.openUploadStream('unndedded-file-name', {
            contentType: contentType,
            metadata: metadata
          });
          stream.pipe(writeStream);
          writeStream.on('finish', function(file) {
            if (file._id !== writeStream.id) {
              console.log('ERR: GridFS-Misunderstanding!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
            }
            resolve(writeStream.id);
          });
        });
      }
    });
  }

  async getGridFile(fileId) {

    if (await this.isConnected) {
    
      var objectId = new this.mongodb.ObjectID(fileId);

      var cursor = this.myGfsBucket.find({
        '_id': objectId
      });
      
      var results = await cursor.toArray();

      var myResult = results[0];
  
      if (!myResult) {
        return Promise.reject("in getGridFile, we could not get the gridfile with id " + fileId);
      } 
      
      return myResult;
    }

  }

  async getGridFileStream(fileId, options) {
    if (await this.isConnected) {
        var objectId = new this.mongodb.ObjectID(fileId);
        var readstream = this.myGfsBucket.openDownloadStream(objectId, options);
        return readstream;
    }
  }
    
  async deleteGridFile(fileId) {
    return this.isConnected.then((isConnected) => {
      if (isConnected) {
        return new Promise((resolve, reject) => {
            this.myGfsBucket.delete(new this.mongodb.ObjectID(fileId), function(err) {
              if (err) {
                console.log(err);
                reject('did not find file to delete?');
              }
              else {
                  resolve('delete seems ok');
              }
            });
        });
      }
    });
  }

  
  async getCollection(collectionName) {
    return this.isConnected.then((isConnected) => {
      if (isConnected) {
          return this.db.collection(collectionName);
      }
    });
  }


}

export default MongoDbFacade;
