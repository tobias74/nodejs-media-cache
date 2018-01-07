
module.exports = function(options){


  var mediaTranscoder=require('./src/media-transcoder')(options);

  var originalMediaStorage = require('./src/original-media-storage')(options);
  
  return {
    
    getImageStreamFromCache: mediaTranscoder.getImageStreamFromCache,
    announceMediaForTranscoding: mediaTranscoder.announceMediaForTranscoding,
    storeOriginalMedia: originalMediaStorage.storeOriginalMedia,
    findTranscodedVideo: mediaTranscoder.findTranscodedVideo,
    getVideo: mediaTranscoder.getVideo,
    getRangedVideoStream:mediaTranscoder.getRangedVideoStream,
    executeTranscodingJob: mediaTranscoder.executeTranscodingJob,
    storeOriginalMediaByStream: originalMediaStorage.storeOriginalMediaByStream,

    
    startListeningForTranscodingJobs: function(callback){
      var amqp = require('amqplib/callback_api');
      
      
      amqp.connect('amqp://' + options.rabbitMqUser + ':' + options.rabbitMqPassword + '@' + options.rabbitMqUrl, function(err, conn) {
        if (err){
          console.log(err);
        }
        
        [options.converterQueueNameImages, options.converterQueueNameVideos].forEach(function(queueName){
          conn.createChannel(function(err, ch) {
            if (err){
              console.log(err);
            }
            ch.assertQueue(queueName, {durable: true});
            ch.prefetch(1);
            
            console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queueName);
  
            ch.consume(queueName, function(msg) {
              console.log(" [x] Received %s", msg.content.toString());
              var rabbitData = JSON.parse(msg.content.toString());
              mediaTranscoder.executeTranscodingJob(rabbitData.mediaId, function(){
                callback(rabbitData.bundleData);
                ch.ack(msg);
              });
            }, {noAck: false});    
          });
        });

      });
    },
    
    deleteMedia: function(mediaId){
      originalMediaStorage.getMedia(mediaId, function(err, mediaFile){
        console.log('this should be deleted');
        console.log(mediaFile);
        if (!mediaFile){
          console.log('datbase icositency,....');
          return;
        }
        
        originalMediaStorage.deleteMedia(mediaId);
        
        var simpleType = mediaFile.contentType.substring(0,5).toLowerCase();
        
        
        if (simpleType === 'image'){
          mediaTranscoder.deleteCachedImage(mediaId);
        }
        else if (simpleType === 'video') {
          mediaTranscoder.deleteTranscodedVideos(mediaId);
        }
        else {
          console.log('wrong file type?');
        }
        
        
      });
    }
  };


  
};
  
