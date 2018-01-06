module.exports = function(options){


  var originalMediaStorage = require('./original-media-storage')(options);
  var mongo = require('mongodb');
  var GridFSBucket = require('mongodb').GridFSBucket;
  
  var GridStream = require('gridfs-stream');
  var gm = require('gm');
  
  var imageMagick = gm.subClass({
    imageMagick: true
  });

  var async = require('async');

  var mongoDbNameTranscoded = options.mongoDbNameTranscoded || 'ems_cached_media';

  var db = new mongo.Db(mongoDbNameTranscoded, new mongo.Server(options.mongoDbUrl, 27017));
  var gfs = undefined;
  var bucket = undefined;

  var {ObjectId} = require('mongodb'); // or ObjectID
  var safeObjectId = s => ObjectId.isValid(s) ? new ObjectId(s) : null;

  db.open(function (err, db) {
      if (err){
        console.log(err);
      }
      
      gfs = GridStream(db, mongo);
      bucket = new GridFSBucket(db);
      console.log('this is the bucket:');
      console.log(bucket);
  });

  var executeTranscodingJob = function(mediaId,mainCallback){
    
    originalMediaStorage.getMedia(mediaId, function(err, mediaFile){
        //console.log(mediaFile);
        if (!mediaFile){
          mainCallback('error, media not found for transcoding');
          return;
        }
        var simpleType = mediaFile.contentType.substring(0,5).toLowerCase();
        
    		var commandFunctions = [{pixel:200,name:'small'},{pixel:600,name:'medium'},{pixel:1200,name:'big'},{pixel:false,name:'original'}].map(function(imageSize){
    		  return function(callback){
    		    console.log('executing for ' + imageSize.pixel);

            originalMediaStorage.getMediaStream(mediaId, function(err, image){

          		var newImage = imageMagick(image);
              if (imageSize.pixel) {
                newImage.resize(imageSize.pixel, imageSize.pixel);
              }
              // @see http://www.imagemagick.org/Usage/thumbnails/#cut
              newImage.autoOrient().stream(function(err, stdout, stderr) {
                if (err) {
                    console.log('error in hreer');
                }
                stderr.pipe(process.stderr);
        
                var writestream = gfs.createWriteStream({
                    metadata: {
                        id: mediaId,
                        size: imageSize.name
                    }
                });
                stdout.pipe(writestream);
                writestream.on('close', function (file) {
                  introduceCachedImage({
                    imageId: mediaId,
                    imageSize: imageSize.name, 
                    fileId: file._id
                  }, function(){
                    callback(null, 'tobias');
                  });
                });
              });
      		  });
      		};
        });


    		commandFunctions.push(function(callback){
            mainCallback();
            callback(null, 'tobias');
    		});
    		
    		async.series(commandFunctions);


    });


  };
    
    
  var deleteCachedImage = function(mediaId){
    findAllCachedImages(mediaId, function(imageDataSet){
      imageDataSet.forEach(function(imageData){
        console.log(imageData);
        db.collection('resized-images').remove({'imageId': imageData.imageId, 'imageSize': imageData.imageSize, 'fileId': imageData.fileId}, function(err, countRemoved){
          console.log('we have ' + countRemoved);
          console.log(err);
        });
        bucket.delete(new mongo.ObjectID(imageData.fileId), function(err){
          if (err){
            console.log('did not find file to delete?');
            console.log(err);
          }
        });
      });
    });
  };  

  var findCachedImage = function(imageId, imageSize, callback){
    db.collection('resized-images').findOne({imageId: safeObjectId(imageId), imageSize:imageSize}, function(err, doc){
      if(err)
      {
        console.log('in find cahced image we have error: ' + err);
        console.log(err);
      }
      console.log('this is what we found in findcachedimage---------------------' + imageId + ' --- ' +imageSize);
      console.log(doc);
      callback(doc);
    });
  };

  var findAllCachedImages = function(imageId, callback){
    db.collection('resized-images').find({imageId: safeObjectId(imageId)}, function(err, doc){
      if(err)
      {
        console.log(err);
      }
      callback(doc);
    });
  };
  

  var introduceCachedImage = function(imageData, callback){
    db.collection('resized-images').insert([{
      imageId: imageData.imageId,
      imageSize: imageData.imageSize,
      fileId: imageData.fileId,
      status: 'completed'
    }], function(err, result){
      callback(result);
      //console.log(result);
    });
  };
  

  var getImageStreamFromCache = function(imageId, imageSize, callback){

    findCachedImage(imageId, imageSize, function(imageData){
      if (imageData === null){
        console.log('image not found');
        callback('error');
      }
      else {
        console.log('we are searching based on this');
        console.log(imageData);
        gfs.findOne({ 
            'metadata.id': imageData.imageId,
            'metadata.size': imageData.imageSize,
            '_id': imageData.fileId
        }, 
        function (err, file) {
          if (err){
            console.log(err);
          }
          if (file){
              var readstream = gfs.createReadStream({
                  _id: file._id
              });
              callback(readstream);
          }
          else{
            console.log('this is strange, we should have a file');
          }
        });
      }
    });
    
  };
    

  return {
      executeTranscodingJob: executeTranscodingJob,
      deleteCachedImage: deleteCachedImage,
      getImageStreamFromCache: getImageStreamFromCache
  };
    
    
    
    
    
    
    


};