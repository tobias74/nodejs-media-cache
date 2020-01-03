var amqp = require('amqplib/callback_api');
var fs = require('fs');
var gm = require('gm');
var imageMagick = gm.subClass({imageMagick: true});
var temp = require('temp');
var child_process = require('child_process');
var async = require('async');


var amqpVideoConverterChannel;
var amqpImageConverterChannel;

var originalMediaStorage;
var transcodedMediaStorage;


var self = {};



var initializeRabbitMQ = async function(){

  return new Promise((resolve, reject) => {
    amqp.connect('amqp://' + self.options.rabbitMqUser + ':' + self.options.rabbitMqPassword + '@' + self.options.rabbitMqUrl, function(err, conn) {
      if (err) {
        console.log(err);
      }

      var myChannels = [self.options.converterQueueNameVideos, self.options.converterQueueNameImages].map(function(queueName) {
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
      
      resolve();
    });
    
  });
};


let startListeningForTranscodingJobs = function(callback) {


  amqp.connect('amqp://' + self.options.rabbitMqUser + ':' + self.options.rabbitMqPassword + '@' + self.options.rabbitMqUrl, function(err, conn) {
    if (err) {
      console.log(err);
    }

    [self.options.converterQueueNameImages, self.options.converterQueueNameVideos].forEach(function(queueName) {
      conn.createChannel(function(err, ch) {
        if (err) {
          console.log(err);
        }
        ch.assertQueue(queueName, {
          durable: true
        });
        ch.prefetch(1);

        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queueName);

        ch.consume(queueName, function(msg) {
          console.log(" [x] Received %s", msg.content.toString());
          var rabbitData = JSON.parse(msg.content.toString());
          executeTranscodingJob(rabbitData.mediaId, function() {
            callback(rabbitData.bundleData);
            ch.ack(msg);
          });
        }, {
          noAck: false
        });
      });
    });

  });
};

var announceMediaForTranscoding = function(mediaId, bundleData) {
  originalMediaStorage.getGridFile(mediaId).then(function(mediaFile) {
    var simpleType = mediaFile.contentType.substring(0, 5).toLowerCase();
    if (simpleType === 'video') {
      amqpVideoConverterChannel.sendToQueue(self.options.converterQueueNameVideos, new Buffer(JSON.stringify({
        'mediaId': mediaId,
        'bundleData': bundleData
      })), {
        persistent: true
      });
      console.log(" [x] Sent video-message to announce for converting!'");
    } else if (simpleType === 'image') {
      amqpImageConverterChannel.sendToQueue(self.options.converterQueueNameImages, new Buffer(JSON.stringify({
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
  return transcodedMediaStorage.getGridFile(stringFileId).then((gridFile) => {
    callback(null, gridFile);
  }, (err) => {
    callback(err);
  });
};

var getVideoStream = function(stringFileId, options) {
  return transcodedMediaStorage.getGridFileStream(stringFileId, options);
};

var storeVideoByFile = async function(filePath, contentType) {
  var readStream = fs.createReadStream(filePath);
  let fileId = await transcodedMediaStorage.storeFileByStream(readStream, contentType, {
    content_type: contentType
  });
  return fileId;
};



var introduceTranscodedVideo = function(payloadId, videoData, callback) {
  transcodedMediaStorage.getCollection('transcoded-videos').then((collection) => {
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
  transcodedMediaStorage.getCollection('transcoded-videos').then((collection) => {
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

    transcodedMediaStorage.deleteGridFile(transcodedVideoData.mp4);
    transcodedMediaStorage.deleteGridFile(transcodedVideoData.ogv);
    transcodedMediaStorage.deleteGridFile(transcodedVideoData.webm);
    transcodedMediaStorage.deleteGridFile(transcodedVideoData.jpg);

    transcodedMediaStorage.getCollection('transcoded-videos').then((collection) => {
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

  originalMediaStorage.getGridFile(mediaId).then(function(mediaFile) {
    //console.log(mediaFile);
    if (!mediaFile) {
      mainCallback('error, media not found for transcoding');
      return;
    }
    var simpleType = mediaFile.contentType.substring(0, 5).toLowerCase();

    if (simpleType === 'video') {
      originalMediaStorage.getGridFileStream(mediaId).then(function(mediaStream) {
        var tempStream = temp.createWriteStream();
        var pathToTempFile = tempStream.path;
        mediaStream.pipe(tempStream);
        mediaStream.on('end', function() {
          var targetStream = temp.createWriteStream();
          var commandPath = __dirname + '/src/buzzconverter.sh ' + pathToTempFile + ' ' + targetStream.path;
          console.log(commandPath);
          child_process.exec(commandPath, function(error, stdout, stderr) {
            console.log('finished transcoding.');
            async.series({
              'mp4': function(callback) {
                storeVideoByFile(targetStream.path + '.mp4', 'video/mp4').then(function(fileId) {
                  callback(null, fileId);
                });
              },
              'ogv': function(callback) {
                storeVideoByFile(targetStream.path + '.ogv', 'video/ogv').then(function(fileId) {
                  callback(null, fileId);
                });
              },
              'webm': function(callback) {
                storeVideoByFile(targetStream.path + '.webm', 'video/webm').then(function(fileId) {
                  callback(null, fileId);
                });
              },
              'jpg': function(callback) {
                storeVideoByFile(targetStream.path + '.jpg', 'image/jpg').then(function(fileId) {
                  callback(null, fileId);
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

          originalMediaStorage.getGridFileStream(mediaId).then(function(image) {

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


              transcodedMediaStorage.storeFileByStream(stdout, null).then((fileId) => {
                introduceCachedImage({
                  imageId: mediaId,
                  imageSize: imageSize.name,
                  fileId: fileId
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
      transcodedMediaStorage.getCollection('resized-images').then((collection) => {
        collection.remove({
          'imageId': imageData.imageId,
          'imageSize': imageData.imageSize,
          'fileId': imageData.fileId
        }, function(err, countRemoved) {
          console.log('we have ' + countRemoved);
          console.log(err);
        });
        transcodedMediaStorage.deleteGridFile(imageData.fileId).then((ok) => {
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
  
  let collection = await transcodedMediaStorage.getCollection('resized-images');
  
  let doc = await collection.findOne({
    imageId: imageId,
    imageSize: imageSize
  });
  
  return doc;
};

var findAllCachedImages = function(imageId, callback) {
  transcodedMediaStorage.getCollection('resized-images').then((collection) => {
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
  transcodedMediaStorage.getCollection('resized-images').then((collection) => {
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
  console.log('we are searching based on this');
  console.log(imageData);
  return transcodedMediaStorage.getGridFileStream(imageData.fileId);
};


module.exports = {
  initialize: async function(options) {
      self.options = options;
      
      var es = await import('./src/mongodb-facade.mjs');
  
      var mongoDbNameOriginals = options.mongoDbNameOriginals || 'ems_original_media';
      originalMediaStorage = new es.MongoDbFacade(options.mongoDbUrl, mongoDbNameOriginals);
  
      var mongoDbNameTranscoded = options.mongoDbNameTranscoded || 'ems_cached_media';
      transcodedMediaStorage = new es.MongoDbFacade(options.mongoDbUrl, mongoDbNameTranscoded);

      await initializeRabbitMQ();
  },
  getImageStreamFromCache: getImageStreamFromCache,
  announceMediaForTranscoding: announceMediaForTranscoding,
  findTranscodedVideo: findTranscodedVideo,
  getVideo: getVideo,
  getVideoStream: getVideoStream,
  executeTranscodingJob: executeTranscodingJob,
  startListeningForTranscodingJobs: startListeningForTranscodingJobs,


  storeOriginalMediaByStream: async function(stream, contentType) {
    let fileId = await originalMediaStorage.storeFileByStream(stream, contentType, {
        content_type: contentType
    });
    return fileId;
  },




  deleteMedia: function(mediaId) {
    originalMediaStorage.getGridFile(mediaId).then(function(mediaFile) {

      originalMediaStorage.deleteGridFile(mediaId);

      var simpleType = mediaFile.contentType.substring(0, 5).toLowerCase();

      if (simpleType === 'image') {
        deleteCachedImage(mediaId);
      } else if (simpleType === 'video') {
        deleteTranscodedVideos(mediaId);
      } else {
        console.log('wrong file type?');
      }

    });
  },
  
};


