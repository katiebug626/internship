const aws = require('aws-sdk');
var mysql = require('mysql');
var rekognition = new aws.Rekognition();
let s3 = new aws.S3({ apiVersion: '2006-03-01' });

aws.config.apiVersions = {
  rekognition: '2016-06-27',
  // other service API versions
};


exports.handler = (event, context, callback) => {
    
    //console.log('Received event:', JSON.stringify(event, null, 2));

    // Get the object from the event and show its content type
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const params = {
        Bucket: bucket,
        Key: key,
    };

    // Get the object from the event and show its content type
    s3.getObject(params, (err, data) => {
        if (err) {
            console.log(err);
            const message = `Error getting object ${key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
            console.log(message);
            callback(message);
        } else {
            //shows content to user
            console.log(`Content Type:`, data.ContentType);
            console.log(`Event received. Bucket: ${bucket}, Key: ${key}.`);
            var param = {
                Image: {
                S3Object: {
                Bucket: bucket, 
                Name: key
                }
            },
            MaxLabels: 10,
            MinConfidence: 50
            };
            
            //gets labels from s3 object
            rekognition.detectLabels(param,(err,data) => {
                if (err) {
                    console.log(err);
                    const message = `Error getting object ${key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
                    console.log(message);
                    callback(message);
                }else {
                    var label = data.Labels;
                    var all_labels = [];
                    //compiles all labels into a single array
                    for(var i=0;i<label.length; i++){
                        all_labels.push(label[i].Name);
                    }
                    var csv_labels = all_labels.join(); //joins all the labels
                    console.log(`Detect_labels finished. ${csv_labels}.`); //return labels to user
                    
                    var info = {
                        description: "some image description",
                        object_key: "",
                        labels: "labels pending"
                    };
                    
                    //inserts columns and content into photo table on the database
                    connection.query('INSERT INTO photo SET ?', info, function(error, results, fields){
                        if(error){
                            connection.destroy();
                            console.log("Error with inserting object_key, labels, and description");
                            throw error;
                        } else{
                            console.log("content inserted into table"); //delete later
                        }
                    });
                    
                    //executes a query to retrieve information about object from database
                    connection.query('SELECT `object_key`,`labels` FROM photo WHERE object_key = ?', [key],function(err, results, fields){
                        if (err){
                            connection.destroy();
                            callback(null, 'error' + err.stack);
                        }
                        else{
                            //updates labels info in database
                            console.log(`Updating key: ${key} wth labels: ${csv_labels}`);
                            connection.query('UPDATE photo SET labels = ? WHERE object_key = ?',[csv_labels, key], function(err, results, fields){
                                if (err){
                                    connection.destroy();
                                    callback(null, 'error' + err.stack);
                                }
                                else{
                                    connection.end();
                                    callback(null,"Finished");
                                }
                                
                            });
                            callback(null,results);

                        }
                
                     });
                    
                }
        
            });
    
        }
    });

    //connects to DB instance 
    var connection = mysql.createConnection({
        host: process.env.DATABASE_HOST,
        user: process.env.DATABASE_USER,
        password: process.env.DATABASE_PASSWORD,
        port: process.env.DATABASE_PORT, 
        database: process.env.DATABASE_DB_NAME,
    });
    
    /*
    connection.connect(function(err) {
        if (err){
            connection.destroy();
            callback(null, 'error' + err.stack);
        }
        else{
            console.log("Database connection successful!");
        }
    });
    */


};
    
      
    







  
 



