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
//app.get(destroyDB)

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






function CreateDestroyDB (req, res){
    console.log("createDestroyDB called");



    let create_bool = req.params.create_bool; //get bool from client


    console.log("DataBase Status = " + create_bool);

 
    

    //if bool true: create DB and load data
    //if bool fasle: delete DB
    if(create_bool == 'true'){

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
                console.log("Creating Table. Please Wait...")
            }
        });

       
        //await sleep(5000); // wait 5 seconds so table can be created
        console.log("Table created.");
        console.log("Loading Data. Please Wait...");

        const s3 = new AWS.S3();
        //let bucketData = null;
        
        s3.getObject(
            { Bucket: "csu44000assign2useast20", Key: "moviedata.json" },
            function (error, data) {
              if (error != null) {
                console.log("Failed to retrieve an object: " + error);
                return res.status(400).json(err)
              } else {
                //console.log("Loaded " + data.ContentLength + " bytes");
                //bucketData = data.Body.toString('utf-8');
                let bucketData = JSON.parse(data.Body);
                //console.log(jsonData)
                console.log("Bucket data loaded. Loaded "+ data.ContentLength + " bytes")
                
                var docClient = new AWS.DynamoDB.DocumentClient();

                console.log("Importing data into DynamoDB. Please wait...");

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



    if(!title || !year){
        res.status(400).send('Please provide title AND year');
    }

    if(!dynamodb){
        res.status(400).send('Unable to query as table does not exist');
    }


    

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


    
    docClient.query(params, function(err, data) {
        if (err) {
            console.log(err)
            return res.status(400).json(err);
        } else {
            console.log("Query succeeded.");
            var result = [];
            data.Items.forEach(function(item) {
                //console.log(item)
                
                result.push({
                    "title": item.title,
                    "year": item.year,
                    "rank": item.rank,
                    "release": item.release
                })
            });
            //return res.status(200).json(results);
            console.log(result);
            res.json(result);
        }
    });

}

/*
function sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
}
*/