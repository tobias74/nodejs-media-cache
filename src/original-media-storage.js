module.exports = function(options){

    var fs = require('fs');
    var mongo = require('mongodb');
    var Grid = require('gridfs-stream');
    var GridFSBucket = require('mongodb').GridFSBucket;
    
    
    var mongoDbNameOriginals = options.mongoDbNameOriginals || 'ems_original_media';

    var db = new mongo.Db(mongoDbNameOriginals, new mongo.Server(options.mongoDbUrl, 27017));
    var gfs; // = Grid(db, mongo);
    var bucket = undefined;
    
    // make sure the db instance is open before passing into `Grid` 
    db.open(function (err) {
      console.log('we have the gridstream open');
      gfs = Grid(db, mongo);
      bucket = new GridFSBucket(db);
    });
    



    var storeOriginalMediaByStream = function(stream,contentType,callback){

      var writestream = gfs.createWriteStream({
          content_type: contentType,
          metadata: {
              content_type: contentType
          }
      });
      stream.pipe(writestream);
      writestream.on('close', function (file) {
        callback(null, file);
      });
    	
    };



    
    var storeOriginalMedia = function(filePath,contentType,callback){

    	var fileId = new mongo.ObjectID();
    	var gridStore = new mongo.GridStore(db, fileId, 'w', {content_type: contentType});
    	
    	var fileHandle = fs.openSync(filePath, 'r', 0666);
    	
    	gridStore.open(function(err, gridStore){
    		gridStore.writeFile(fileHandle, function(err, doc){
    			callback({
    				fileId: fileId
    			});
    		});
    	});
    	
    };
    
    var getMedia = function(stringFileId, callback){

    	var fileId = new mongo.ObjectID(stringFileId);
    	
    	var gridStore = new mongo.GridStore(db, fileId, 'r');
    	
    	gridStore.open(function(err, gridStore){
    	  if (err) {
    	    console.log('could not get media');
    	    console.log(err);
    	  }
    	  if (gridStore.length > 0){
      		callback(null, gridStore);
    	  }
    	  else {
    	    console.log('the file had length zero...');
    	    callback('filelength zero??');
    	  }

    	});
    	
    };
    
      
    var deleteMedia = function(mediaId){
      bucket.delete(new mongo.ObjectID(mediaId), function(err){
        if (err){
          console.log('did not find file to delete?');
          console.log(err);
        }
      });
    };  
  
    
    var getMediaStream = function(stringFileId, callback){
      getMedia(stringFileId, function(err, gridStore){
        var stream = gridStore.stream(true);
        callback(null, stream);
      });
      
    };
    
    
    var getRangedMediaStream = function(stringFileId, rangeData){
      console.log('rnagedata start ' + rangeData.start);
      var readstream = gfs.createReadStream({
        _id: stringFileId,
        range: {
          startPos: rangeData.start,
          endPos: rangeData.end
        }   
      });
      return readstream;
    };

    return {
        storeOriginalMedia: storeOriginalMedia,
        getMedia: getMedia,
        deleteMedia: deleteMedia,
        getRangedMediaStream: getRangedMediaStream,
        getMediaStream: getMediaStream,
        storeOriginalMediaByStream: storeOriginalMediaByStream

    };

};


