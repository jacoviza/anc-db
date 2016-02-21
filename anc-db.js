var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;

function AncClient() {
  this.upsert = AncClient.upsert;
  this.upsertSeries = AncClient.upsertSeries;
}

function connect() {
  if(AncClient.db === undefined) {
    MongoClient.connect('mongodb://localhost:27017/anc', function(err, db) {

      if(err) {
        console.error('Error while connecting to anc db');
      }

      AncClient.db = db;
    });
  }
}

AncClient.upsert = function(program, callback) {

  console.log("ancClient.upsert(): entering....");

  var programs  = this.db.collection('programs');
  var series    = this.db.collection('series');

  programs.update({date:program.date}, {'$set':program}, {upsert:true}, function(err,data){
    if(err) {
      console.log(err);
    } else {
      console.log('program succesfully upserted');
    }
  });

  series.update({title:program.series},{
    $set: {
      image       : program.imageUrl,
      smallImage  : program.smallImageUrl
    },
    $addToSet:{
      'programs':program.date
    }
  }, {upsert:true}, function(err, data){
    if(err) {
      console.log(err);
    } else {
      console.log('series succesfully upserted');
    }
  });

};

AncClient.upsertSeries = function(seriesData, callback) {

  console.log("ancClient.upsertSeries(): entering....");

  var series    = this.db.collection('series');
  console.log("series title: " + seriesData.title);
  console.log(seriesData);

  series.update({title:seriesData.title},{
    $set: {
      //image       : program.imageUrl,
      //smallImage  : program.smallImageUrl,
      description : seriesData.description
    }
  }, {upsert: false}, function(err, data) {
    if(err) {
      console.log(err);
    } else {
     // console.log(data);
      console.log('series successfully upserted');
    }

    callback(err, data);
  });


};

connect();

module.exports.AncClient = AncClient;
