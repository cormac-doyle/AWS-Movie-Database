const https = require('https');
const express = require('express')
const app = express()
const port = 3000
const path = require("path");
const AWS = require("aws-sdk");

let publicPath = path.resolve(__dirname, "public")
app.use(express.static(publicPath))

app.listen(port, () => console.log(`Listening on the port ${port}`))

app.get('/movies/:movie_name/:movie_year', queryDB)
app.get('/create/:create_bool',CreateDestroyDB)


app.get('/', (req, res) => {
    res.sendFile('public/client.html', {root: __dirname})
})


app.use(express.static('public')); // serves html page 


// database config
AWS.config.update({
    region: "us-east-1",

})


let dynamodb = new AWS.DynamoDB();
let s3 = new AWS.S3();


function CreateDestroyDB (req, res){ //creates OR destroys database
    console.log("createDestroyDB called");



    let create_bool = req.params.create_bool; //get bool from client


    console.log("DataBase Status = " + create_bool);

 
    

    //if bool true: create DB and load data
    //if bool fasle: delete DB
    if(create_bool == 'true'){
        console.log("Creating Table. Please Wait...")

        //table parameters
        var params = {
            TableName : "Movies",
            KeySchema: [       
                { AttributeName: "year", KeyType: "HASH"},  //Partition key
                { AttributeName: "title", KeyType: "RANGE" }  //Sort key
            ],
            AttributeDefinitions: [       
                { AttributeName: "year", AttributeType: "N" },
                { AttributeName: "title", AttributeType: "S" }
            ],
            ProvisionedThroughput: {       
                ReadCapacityUnits: 1, 
                WriteCapacityUnits: 5
            }
        };
        
        dynamodb.createTable(params, function(err, data) {
        
            
            if (err) {
                console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                //console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
                
            }
        });

       
        wait(5000); // wait 5 seconds so table can be created
        console.log("Table created.");
        console.log("Loading Data. Please Wait...");

        const s3 = new AWS.S3();
        //let bucketData = null;
        
        s3.getObject( //get bucket data
            { Bucket: "csu44000assign2useast20", Key: "moviedata.json" },
            function (error, data) {
              if (error != null) {
                console.log("Failed to retrieve an object: " + error);
                return res.status(400).json(err)
              } else {
               

                //bucketData = data.Body.toString('utf-8');
                let bucketData = JSON.parse(data.Body);
                //console.log(jsonData)
                console.log("Bucket data loaded. Loaded "+ data.ContentLength + " bytes")
                
                var docClient = new AWS.DynamoDB.DocumentClient();

                console.log("Importing data into DynamoDB. Please wait...");

                //import data into dynamo DB table
                //var allMovies = JSON.parse(fs.readFileSync('moviedata.json', 'utf8'));
                bucketData.forEach(function(movie) {
                    var params = {
                        TableName: "Movies",
                        Item: {
                            "year":  movie.year,
                            "title": movie.title,
                            "rank":  movie.info.rank,
                            "release":  movie.info.release_date
                        }
                    };

                    docClient.put(params, function(err, data) {
                    if (err) {
                        //console.error("Unable to add movie", movie.title, ". Error JSON:", JSON.stringify(err, null, 2));
                    } else {
                        //console.log("PutItem succeeded:", movie.title);
                    }
                    });

                });
                console.log("Created table and loaded data.")
                return res.json("Created table and loaded data")
                


              }
            }
        );   
    }
    else{
        
        console.log("Deleting Table..");

        var params = {
            TableName : "Movies"
        };

        dynamodb.deleteTable(params, function(err, data) {
            if (err) {
                console.error("Unable to delete table. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                //console.log("Deleted table. Table description JSON:", JSON.stringify(data, null, 2));
                console.log("Table Deleted")
                return res.json("Deleted table")
                
            }
        });

    }
    
    //res.json(create_bool);


}

function queryDB(req, res){


    console.log("Querying Database. Please Wait.")
    let title = req.params.movie_name;
    let year = req.params.movie_year;
    
    
    var docClient = new AWS.DynamoDB.DocumentClient();

    var table = "Movies";


    //query params
    var params = {
        TableName : "Movies",
        KeyConditionExpression: "#yr = :yyyy and begins_with(title, :t)",
        ExpressionAttributeNames:{
            "#yr": "year",
        },
        ExpressionAttributeValues: {
            ":yyyy": parseInt(year),
            ":t": title
        }
    };


    //query database with parameters
    docClient.query(params, function(err, data) {
        if (err) {
            console.log(err)
            return res.status(400).json(err);
        } else {
            console.log("Query succeeded.");
            
            var result = [];
            data.Items.forEach(function(Item) {
                //console.log(Item)
                
                result.push({
                    "title": Item.title,
                    "year": Item.year,
                    "rank": Item.rank,
                    "release": Item.release
                })
            });
            //return res.status(200).json(results);
            console.log(result);
            res.json(result); //respond with array of movies
        }
    });

}

//function to wait a number of ms before executing next line
function wait(ms){
    var start = new Date().getTime();
    var end = start;
    while(end < start + ms) {
      end = new Date().getTime();
   }
 }