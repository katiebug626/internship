const aws = require('aws-sdk');
var mysql = require('mysql');
var rekognition = new aws.Rekognition();
let s3 = new aws.S3({ apiVersion: '2006-03-01' });

aws.config.apiVersions = {
  rekognition: '2016-06-27',
  // other service API versions
};

aws.config.update({region:'us-west-2'});

function getObject(event){
    return new Promise(function(resolve, reject) {
        s3.getObject(event,(err,data) => {
            console.log("got in");
            if(err){
                const message = `Error getting object from ${event.Key} from bucket ${event.Bucket}. Make sure they exist and your bucket is in the same region as this function`;
                reject(message);
            }
            else{
                //shows content to user 
                console.log(`Content Type:`, data.ContentType);
                console.log(`Event received. Bucket: ${event.Bucket}, Key: ${event.Key}.`);
                resolve(event);
            }
        });
    });
}

function getRekLabels (event) {
    return  new Promise(function(resolve,reject){
        var param = {
                Image: {
                S3Object: {
                Bucket: event.Bucket, 
                Name: event.Key
                }
            },
            MaxLabels: 10,
            MinConfidence: 50
            };
            
         rekognition.detectLabels(param,(err,data) => {
                if (err) {
                    reject(err);
                }else {
                    var label = data.Labels;
                    var all_labels = [];
                    //compiles all labels into a single array
                    for(var i=0;i<label.length; i++){
                        all_labels.push(label[i].Name);
                    }
                    var csv_labels = all_labels.join(); //joins all the labels
                    resolve(csv_labels);
                }
        });
    });
}

function insertQuery(dbconn,labels){
    return new Promise(function(resolve,reject){
        var info = {
                        description: "some image description",
                        object_key: "",
                        labels: "labels pending"
                    };
        dbconn.query('INSERT INTO photo SET ?', info , function(err, reuslts, fields){
            if(err){
                dbconn.destroy();
                reject(err);
            }
            else{
                resolve(labels);
            }
        });
    });
}

function selectQuery(dbconn, key,labels){
    return new Promise(function(resolve,reject){
        dbconn.query('SELECT `object_key`,`labels` FROM photo WHERE object_key = ?',[key], function(err, reuslts, fields){
            if(err){
                dbconn.destroy();
                reject(err);
            }
            else{
                resolve(labels);
            }
        });
    });
}

function updateQuery(dbconn, key, labels){
    return new Promise(function(resolve,reject){
        console.log(`Updating key: ${key} with labels: ${labels}`);
        dbconn.query('UPDATE photo SET labels = ? WHERE object_key = ?', [labels,key], function(err, reuslts, fields){
            if(err){
                dbconn.destroy();
                reject(err);
            }
            else{
                resolve();
            }
        });
    });
}

function lambda_function(event, context){
    
    //console.log('Received event:', JSON.stringify(event, null, 2));

    // Get the object from the event and show its content type
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const params = {
        Bucket: bucket,
        Key: key,
    };
    
    //connects to DB instance 
    var conn = mysql.createConnection({
        host: process.env.DATABASE_HOST,
        user: process.env.DATABASE_USER,
        password: process.env.DATABASE_PASSWORD,
        port: process.env.DATABASE_PORT, 
        database: process.env.DATABASE_DB_NAME,
    });

        getObject(params)
        .then(
            function(result){
                return getRekLabels(result);
        })
        .then(function(labels){
            console.log(`Detect_labels finished. ${labels}.`); 
            return insertQuery(conn,labels);
        })
        .then(function(labels){
            return selectQuery(conn,key,labels);
        })
        .then(function(labels){
            return updateQuery(conn,key,labels);
        }).then(function(){
            conn.end();
            console.log("Finished");
        })
        .catch(function(err){
            console.log(err);
        });
}
        
        
     

var fake_s3_event = {
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws.s3",
      "awsRegion": "us-west-2",
      "eventTime": "...",
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": {
          "name": "imagekgp"
        },
        "object": {
          "key": "photos/542de1a5b1faeec8.png"
        }
      }
    }
  ]
};

var fake_s3_context = [];

lambda_function(fake_s3_event, fake_s3_context);
