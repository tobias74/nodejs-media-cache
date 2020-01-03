import {createRequire} from 'module';
const require = createRequire(import.meta.url);

export class MongoDbFacade {

  constructor(url, databaseName) {
    this.url = url;
    this.databaseName = databaseName;

    this.fs = require('fs');
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
    let client = new MongoClient(this.url);
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
            resolve(file);
          });
        });
      }
    });
  }

  async storeFileByPath(filePath, data) {
    return this.isConnected.then((isConnected) => {
      if (isConnected) {
        return new Promise((resolve, reject) => {

        	var fileId = new this.mongodb.ObjectID();
        	var gridStore = new this.mongodb.GridStore(this.db, fileId, 'w', data);
        	var fileHandle = this.fs.openSync(filePath, 'r', '0666');
        	
        	gridStore.open(function(err, gridStore){
        	    if (err) {
        	        console.log(err);
        	    }
        		gridStore.writeFile(fileHandle, function(err, doc){
            	    if (err) {
            	        console.log(err);
            	    }
        			resolve(fileId);
        		});
        	});
        });
      }
    });
  }
  
  async getGridFile(fileId, mode) {
    return this.isConnected.then((isConnected) => {
      if (isConnected) {
        return new Promise((resolve, reject) => {

            var objectId = new this.mongodb.ObjectID(fileId);
        
            var gridStore = new this.mongodb.GridStore(this.db, objectId, mode);
        
            gridStore.open(function(err, gridStore) {
              if (err) {
                console.log(err);
                reject('could not get gridfile');
              }
              if (gridStore.length > 0) {
                resolve(gridStore);
              } else {
                console.log('the file had length zero...');
                reject('filelength zero??');
              }
        
            });

        });
      }
    });
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
