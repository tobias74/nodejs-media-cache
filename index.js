var env = process.env.NODE_ENV

var options = {};
options.mongoDbUrl = "localhost";
options.mongoDbNameOriginals = 'original_images_' + env;
options.mongoDbNameTranscoded = 'cached_images_' + env;


var mediaTranscoder=require('./src/media-transcoder')(options);
var originalMediaStorage = require('./src/original-media-storage')(options);







module.exports = {
  
  getImageStreamFromCache: mediaTranscoder.getImageStreamFromCache,
  storeOriginalMediaByStream: originalMediaStorage.storeOriginalMediaByStream,
  executeTranscodingJob: mediaTranscoder.executeTranscodingJob,

  deleteMedia: function(mediaId){
    originalMediaStorage.getMedia(mediaId, function(err, mediaFile){
      console.log('this should be deleted');
      console.log(mediaFile);
      if (!mediaFile){
        console.log('datbase icositency,....');
        return;
      }
      
      originalMediaStorage.deleteMedia(mediaId);
      
      mediaTranscoder.deleteCachedImage(mediaId);

    });
  }
};


  
