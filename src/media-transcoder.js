

module.exports = function(options) {


  var mongoDbFacade;

  (async () => {
    var es = await import('./mongodb-facade.mjs');
    var mongoDbNameTranscoded = options.mongoDbNameTranscoded || 'ems_cached_media';
    mongoDbFacade = new es.MongoDbFacade(options.mongoDbUrl, mongoDbNameTranscoded);
  })();



  var originalMediaStorage = require('./original-media-storage')(options);
  var mongo = require('mongodb');
  var GridFSBucket = require('mongodb').GridFSBucket;

  var GridStream = require('gridfs-stream');
  var gm = require('gm');

  var imageMagick = gm.subClass({
    imageMagick: true
  });

  var temp = require('temp');
  var child_process = require('child_process');
  var async = require('async');

  var amqp = require('amqplib/callback_api');

  var amqpVideoConverterChannel;
  var amqpImageConverterChannel;

  if (options.enableVideo) {

    amqp.connect('amqp://' + options.rabbitMqUser + ':' + options.rabbitMqPassword + '@' + options.rabbitMqUrl, function(err, conn) {
      if (err) {
        console.log(err);
      }

      var myChannels = [options.converterQueueNameVideos, options.converterQueueNameImages].map(function(queueName) {
        return conn.createChannel(function(err, ch) {
          if (err) {
            console.log(err);
          }
          ch.assertQueue(queueName, {
            durable: true
          });
        });
      });
      amqpVideoConverterChannel = myChannels[0];
      amqpImageConverterChannel = myChannels[1];

    });
  }




  var announceMediaForTranscoding = function(mediaId, bundleData) {
    originalMediaStorage.getMedia(mediaId, function(err, mediaFile) {
      if (err) {
        console.log(err);
      }
      var simpleType = mediaFile.contentType.substring(0, 5).toLowerCase();
      if (simpleType === 'video') {
        amqpVideoConverterChannel.sendToQueue(options.converterQueueNameVideos, new Buffer(JSON.stringify({
          'mediaId': mediaId,
          'bundleData': bundleData
        })), {
          persistent: true
        });
        console.log(" [x] Sent video-message to announce for converting!'");
      } else if (simpleType === 'image') {
        amqpImageConverterChannel.sendToQueue(options.converterQueueNameImages, new Buffer(JSON.stringify({
          'mediaId': mediaId,
          'bundleData': bundleData
        })), {
          persistent: true
        });
        console.log(" [x] Sent image-message to announce for converting!'");
      } else {
        console.log('no file Type for transcoding?');
      }


    });
  };


  var getVideo = function(stringFileId, callback) {
    return mongoDbFacade.getGridFile(stringFileId, 'r').then((gridFile) => {
      callback(null, gridFile);
    }, (err) => {
      callback(err);
    });
  };

  var getVideoStream = function(stringFileId, options) {
    return mongoDbFacade.getGridFileStream(stringFileId, options);
  };

  var storeVideoByFile = function(filePath, contentType, callback) {
    mongoDbFacade.storeFileByPath(filePath, {
      content_type: contentType
    }).then((fileId) => {
      callback({
        fileId: fileId
      });
    });
  };



  var introduceTranscodedVideo = function(payloadId, videoData, callback) {
    mongoDbFacade.getCollection('transcoded-videos').then((collection) => {
      collection.insert([{
        payloadId: payloadId,
        mp4: videoData.mp4,
        ogv: videoData.ogv,
        webm: videoData.webm,
        jpg: videoData.jpg,
        status: 'completed'
      }], function(err, result) {
        callback(null);
      });
    });
  };


  var findTranscodedVideo = function(payloadId, callback) {
    mongoDbFacade.getCollection('transcoded-videos').then((collection) => {
      collection.findOne({
        payloadId: payloadId
      }, function(err, transcodedVideoData) {
        if (err) {
          console.log(err);
        }
        callback(transcodedVideoData);
      });
    });
  };

  var deleteTranscodedVideos = function(payloadId) {

    findTranscodedVideo(payloadId, function(transcodedVideoData) {

      mongoDbFacade.deleteGridFile(transcodedVideoData.mp4);
      mongoDbFacade.deleteGridFile(transcodedVideoData.ogv);
      mongoDbFacade.deleteGridFile(transcodedVideoData.webm);
      mongoDbFacade.deleteGridFile(transcodedVideoData.jpg);

      mongoDbFacade.getCollection('transcoded-videos').then((collection) => {
        collection.remove({
          payloadId: payloadId
        }, function(err, countDoc) {
          if (err) {
            console.log(err);
          }
          console.log('deleted: ' + countDoc);
        });
      });
    });
  };

  var executeTranscodingJob = function(mediaId, mainCallback) {

    originalMediaStorage.getMedia(mediaId, function(err, mediaFile) {
      //console.log(mediaFile);
      if (!mediaFile) {
        mainCallback('error, media not found for transcoding');
        return;
      }
      var simpleType = mediaFile.contentType.substring(0, 5).toLowerCase();

      if (simpleType === 'video') {
        originalMediaStorage.getMediaStream(mediaId, function(err, mediaStream) {
          if (err) {
            console.log(err);
            mainCallback(err);
            return;
          }
          var tempStream = temp.createWriteStream();
          var pathToTempFile = tempStream.path;
          mediaStream.pipe(tempStream);
          mediaStream.on('end', function() {
            var targetStream = temp.createWriteStream();
            var commandPath = __dirname + '/buzzconverter.sh ' + pathToTempFile + ' ' + targetStream.path;
            console.log(commandPath);
            child_process.exec(commandPath, function(error, stdout, stderr) {
              console.log('finished transcoding.');
              async.series({
                'mp4': function(callback) {
                  storeVideoByFile(targetStream.path + '.mp4', 'video/mp4', function(fileData) {
                    callback(null, fileData.fileId);
                  });
                },
                'ogv': function(callback) {
                  storeVideoByFile(targetStream.path + '.ogv', 'video/ogv', function(fileData) {
                    callback(null, fileData.fileId);
                  });
                },
                'webm': function(callback) {
                  storeVideoByFile(targetStream.path + '.webm', 'video/webm', function(fileData) {
                    callback(null, fileData.fileId);
                  });
                },
                'jpg': function(callback) {
                  storeVideoByFile(targetStream.path + '.jpg', 'image/jpg', function(fileData) {
                    callback(null, fileData.fileId);
                  });
                }
              }, function(err, results) {
                introduceTranscodedVideo(mediaId, results, function() {
                  mainCallback();
                });
              });
            });
          });
        });
      } else if (simpleType === 'image') {

        var commandFunctions = [{
          pixel: 200,
          name: 'small'
        }, {
          pixel: 600,
          name: 'medium'
        }, {
          pixel: 1200,
          name: 'big'
        }, {
          pixel: false,
          name: 'original'
        }].map(function(imageSize) {
          return function(callback) {
            console.log('executing for ' + imageSize.pixel);

            originalMediaStorage.getMediaStream(mediaId, function(err, image) {

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


                mongoDbFacade.storeFileByStream(stdout, null, {
                    id: mediaId,
                    size: imageSize.name
                }).then((file) => {
                  introduceCachedImage({
                    imageId: mediaId,
                    imageSize: imageSize.name,
                    fileId: file._id
                  }, function() {
                    callback(null, 'tobias');
                  });
                });

              });
            });
          };
        });


        commandFunctions.push(function(callback) {
          mainCallback();
          callback(null, 'tobias');
        });

        async.series(commandFunctions);

      } else {
        console.log('no file type?');
      }

    });


  };


  var deleteCachedImage = function(mediaId) {
    findAllCachedImages(mediaId, function(imageDataSet) {
      imageDataSet.forEach(function(imageData) {
        console.log(imageData);
        mongoDbFacade.getCollection('resized-images').then((collection) => {
          collection.remove({
            'imageId': imageData.imageId,
            'imageSize': imageData.imageSize,
            'fileId': imageData.fileId
          }, function(err, countRemoved) {
            console.log('we have ' + countRemoved);
            console.log(err);
          });
          mongoDbFacade.deleteGridFile(imageData.fileId).then((ok) => {
            console.log('delete of cahce image seems to be ok.', ok);
          }, (err) => {
            console.log('did not find file to delete?');
            console.log(err);
          });
        });
      });
    });
  };

  var findCachedImage = async function(imageId, imageSize) {
    
    let collection = await mongoDbFacade.getCollection('resized-images');
    
    let doc = await collection.findOne({
      imageId: imageId,
      imageSize: imageSize
    });
    
    return doc;
  };

  var findAllCachedImages = function(imageId, callback) {
    mongoDbFacade.getCollection('resized-images').then((collection) => {
      collection.find({
        imageId: imageId
      }, function(err, doc) {
        if (err) {
          console.log(err);
        }
        callback(doc);
      });
    });
  };


  var introduceCachedImage = function(imageData, callback) {
    mongoDbFacade.getCollection('resized-images').then((collection) => {
      collection.insert([{
        imageId: imageData.imageId,
        imageSize: imageData.imageSize,
        fileId: imageData.fileId,
        status: 'completed'
      }], function(err, result) {
        callback(result);
      //console.log(result);
      });
    });
  };


  var getImageStreamFromCache = async function(imageId, imageSize) {
    let imageData = await findCachedImage(imageId, imageSize);
    
    if (imageData === null) {
      console.log('image not found');
      callback('error');
    } else {
      console.log('we are searching based on this');
      console.log(imageData);

      let bucket = await mongoDbFacade.getGfsBucket();

      var cursor = bucket.find({
        'metadata.id': imageData.imageId,
        'metadata.size': imageData.imageSize,
        '_id': imageData.fileId
      });
      
      var results = await cursor.toArray();
      
      console.log('there are my result in ++++++++++++++++++++++++++++++++++++++++++++++++++');
      console.log(results);
      
      var myResult = results[0];
      
      var readstream = bucket.openDownloadStream(myResult._id);
      return readstream;
    }
  };


  return {
    announceMediaForTranscoding: announceMediaForTranscoding,
    executeTranscodingJob: executeTranscodingJob,
    findTranscodedVideo: findTranscodedVideo,
    getVideoStream: getVideoStream,
    getVideo: getVideo,
    deleteCachedImage: deleteCachedImage,
    deleteTranscodedVideos: deleteTranscodedVideos,
    getImageStreamFromCache: getImageStreamFromCache

  };









};