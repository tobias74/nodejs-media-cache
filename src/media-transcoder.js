module.exports = function(options){


  var originalMediaStorage = require('./original-media-storage')(options);
  var mongo = require('mongodb');
  var GridFSBucket = require('mongodb').GridFSBucket;
  
  var GridStream = require('gridfs-stream');
  var gm = require('gm');
  
  var imageMagick = gm.subClass({
    imageMagick: true
  });

  var temp = require('temp');
  var fs = require('fs');
  var child_process = require('child_process');
  var async = require('async');

  var amqp = require('amqplib/callback_api');
  
  var amqpVideoConverterChannel;
  var amqpImageConverterChannel;

  if (options.enableVideo){
    
    amqp.connect('amqp://' + options.rabbitMqUser + ':' + options.rabbitMqPassword + '@' + options.rabbitMqUrl, function(err, conn) {
      if (err){
        console.log(err);
      }
      
      var myChannels = [options.converterQueueNameVideos, options.converterQueueNameImages].map(function(queueName){
        return conn.createChannel(function(err, ch) {
          if (err){
            console.log(err);
          }
          ch.assertQueue(queueName, {durable: true});
        });
      });
      amqpVideoConverterChannel = myChannels[0];
      amqpImageConverterChannel = myChannels[1];
  
    });
  }
  
  
  
  
  var announceMediaForTranscoding = function(mediaId, bundleData){
      originalMediaStorage.getMedia(mediaId, function(err, mediaFile){
        var simpleType = mediaFile.contentType.substring(0,5).toLowerCase();
        if (simpleType === 'video') {
          amqpVideoConverterChannel.sendToQueue(options.converterQueueNameVideos, new Buffer(JSON.stringify({
              'mediaId': mediaId,
              'bundleData': bundleData
          })), {persistent: true});
          console.log(" [x] Sent video-message to announce for converting!'");  
        }
        else if (simpleType === 'image') {
          amqpImageConverterChannel.sendToQueue(options.converterQueueNameImages, new Buffer(JSON.stringify({
              'mediaId': mediaId,
              'bundleData': bundleData
          })), {persistent: true});
          console.log(" [x] Sent image-message to announce for converting!'");  
        }
        else {
          console.log('no file Type for transcoding?');
        }

        
      });
  };

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



  var getVideo = function(stringFileId, callback){
    var fileId = new mongo.ObjectID(stringFileId);
    var gridStore = new mongo.GridStore(db, fileId, 'r');
    gridStore.open(callback);
  };
  
  var getRangedVideoStream = function(stringFileId, rangeData){
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

  var storeVideoByFile = function(filePath,contentType,callback){

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
  
  
  
  var introduceTranscodedVideo = function(payloadId, videoData,  callback){
    db.collection('transcoded-videos').insert([{
      payloadId: payloadId,
      mp4: videoData.mp4,
      ogv: videoData.ogv,
      webm: videoData.webm,
      jpg: videoData.jpg,
      status: 'completed'
    }], function(err, result){
      callback(null);
    });
  };
  

  var findTranscodedVideo = function(payloadId, callback){
    db.collection('transcoded-videos').findOne({payloadId: payloadId}, function(err, transcodedVideoData){
      callback(transcodedVideoData);
    });
  };
  
  var deleteTranscodedVideos = function(payloadId){
    
    findTranscodedVideo(payloadId, function(transcodedVideoData){
      
      var deleteChecker = function(err){
        if (err){
          console.log('------------------------------------------------------------------did not find the file to delete');
          console.log(err);
        }
      };
      
      bucket.delete(transcodedVideoData.mp4, deleteChecker);
      bucket.delete(transcodedVideoData.ogv, deleteChecker);
      bucket.delete(transcodedVideoData.webm, deleteChecker);
      bucket.delete(transcodedVideoData.jpg, deleteChecker);

      db.collection('transcoded-videos').remove({payloadId: payloadId}, function(err, countDoc){
        console.log('deleted: ' + countDoc);
      });
      
    });
    
    
  };

  var executeTranscodingJob = function(mediaId,mainCallback){
    
    originalMediaStorage.getMedia(mediaId, function(err, mediaFile){
        //console.log(mediaFile);
        if (!mediaFile){
          mainCallback('error, media not found for transcoding');
          return;
        }
        var simpleType = mediaFile.contentType.substring(0,5).toLowerCase();
        
        if (simpleType === 'video') {
          originalMediaStorage.getMediaStream(mediaId, function(err, mediaStream){
            if (err) {
              console.log(err);
              mainCallback(err);
              return;
            }
            var tempStream = temp.createWriteStream();
            var pathToTempFile = tempStream.path;
            mediaStream.pipe(tempStream);
            mediaStream.on('end', function(){
              var targetStream = temp.createWriteStream();
              var commandPath = __dirname + '/buzzconverter.sh ' + pathToTempFile + ' ' + targetStream.path;
              console.log(commandPath);
              child_process.exec(commandPath, function(error,stdout,stderr){
                console.log('finished transcoding.');
                async.series({
                  'mp4': function(callback){
                    storeVideoByFile(targetStream.path+'.mp4', 'video/mp4', function(fileData){
                      callback(null, fileData.fileId);
                    });
                  },
                  'ogv': function(callback){
                    storeVideoByFile(targetStream.path+'.ogv', 'video/ogv', function(fileData){
                      callback(null, fileData.fileId);
                    });
                  },
                  'webm': function(callback){
                    storeVideoByFile(targetStream.path+'.webm', 'video/webm', function(fileData){
                      callback(null, fileData.fileId);
                    });
                  },
                  'jpg': function(callback){
                    storeVideoByFile(targetStream.path+'.jpg', 'image/jpg', function(fileData){
                      callback(null, fileData.fileId);
                    });
                  }
                }, function(err, results){
                    introduceTranscodedVideo(mediaId, results, function(){
                      mainCallback();
                    });
                });      
              });
            });
          });
        }
        else if (simpleType === 'image') {

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

        }
        else {
          console.log('no file type?');
        }
      
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
    db.collection('resized-images').findOne({imageId: imageId, imageSize:imageSize}, function(err, doc){
      if(err)
      {
        console.log('in find cahced image we have error: ' + err);
        console.log(err);
      }
      callback(doc);
    });
  };

  var findAllCachedImages = function(imageId, callback){
    db.collection('resized-images').find({imageId: imageId}, function(err, doc){
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
      announceMediaForTranscoding: announceMediaForTranscoding,
      executeTranscodingJob: executeTranscodingJob,
      findTranscodedVideo: findTranscodedVideo,
      getRangedVideoStream: getRangedVideoStream,
      getVideo: getVideo,
      deleteCachedImage: deleteCachedImage,
      deleteTranscodedVideos: deleteTranscodedVideos,
      getImageStreamFromCache: getImageStreamFromCache

  };
    
    
    
    
    
    
    


};