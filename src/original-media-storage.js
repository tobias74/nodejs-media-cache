
module.exports = function(options) {

  var mongoDbFacade;


  (async () => {
    var es = await import('./mongodb-facade.mjs');
    var mongoDbNameOriginals = options.mongoDbNameOriginals || 'ems_original_media';
    mongoDbFacade = new es.MongoDbFacade(options.mongoDbUrl, mongoDbNameOriginals);
  })();



  var storeOriginalMediaByStream = async function(stream, contentType) {
    let fileId = await mongoDbFacade.storeFileByStream(stream, contentType, {
        content_type: contentType
    });
    return fileId;
  };


  var getMedia = function(stringFileId, callback) {
    mongoDbFacade.getGridFile(stringFileId).then((gridFile) => {
      callback(null, gridFile);
    }, (err) => {
      console.log(err);
      callback(err);
    });
  };


  var deleteMedia = function(mediaId) {
    mongoDbFacade.deleteGridFile(mediaId).then((ok) => {
      console.log(ok);
    }, (err) => {
      console.log('did not find file to delete?');
      console.log(err);
    });
  };


  var getMediaStream = function(stringFileId, callback) {
    mongoDbFacade.getGridFileStream(stringFileId).then((stream) => {
      callback(null, stream);
    });
  };


  var getRangedMediaStream = function(stringFileId, start, end) {
    return mongoDbFacade.getRangedGridFileStream(stringFileId, start, end);
  };

  return {
    getMedia: getMedia,
    deleteMedia: deleteMedia,
    getRangedMediaStream: getRangedMediaStream,
    getMediaStream: getMediaStream,
    storeOriginalMediaByStream: storeOriginalMediaByStream

  };

};


