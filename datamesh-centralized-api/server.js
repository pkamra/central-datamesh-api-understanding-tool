const express = require('express');
const cors = require('cors');
const path = require('path');
const AWS = require('aws-sdk');
var sleep = require('system-sleep');
const AthenaExpress = require("athena-express");
const app = express(),
      bodyParser = require("body-parser");
      port = 3080;

// place holder for the data
const users = [];

//configuring the AWS producer environment

app.use(cors());
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, '../datamesh-frontend/dist')));
sleep = function sleep(millis) {
  return new Promise(resolve => setTimeout(resolve, millis));
}

//Input producer accountId, S3 bucket name , centraldatamesh acoount id 
//Login to the Producer UI by a Lakeformation Admin Iam User who can make changes to S3 bucket permissions and 
//Lakeformation

app.post('/api/modifyS3BucketPermissionInProducerAccount',(req,res)=>{
  AWS.config.update({
    accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });
  // Create S3 service object
  s3 = new AWS.S3({apiVersion: '2006-03-01'});
  var centralDataMeshAccessPolicy = {
    Version: "2012-10-17",
    Statement: [
      {
        Sid: "AddCentralDataAccountAccess",
        Effect: "Allow",
        Principal:  {
         "AWS": "*"
        },
        Action: [
          "s3:*"
        ],
        Resource: [
          ""
        ]
      }
    ]
  };
  // create selected bucket resource string for bucket policy
var bucketResource = "arn:aws:s3:::" + req.body.producerbucketname 
var centraldatameshaccount = req.body.centraldatameshaccount ;
centralDataMeshAccessPolicy.Statement[0].Resource[0] = bucketResource;
centralDataMeshAccessPolicy.Statement[0].Resource[1] = bucketResource+ "/*";
var principal =  "arn:aws:iam::"+req.body.centraldatameshaccount+":root";
centralDataMeshAccessPolicy.Statement[0].Principal.AWS = principal
console.log("Bucket name is ::",req.body);
console.log("Central Data Mesh Account is ::",centraldatameshaccount);
console.log("Policy is ::",JSON.stringify(centralDataMeshAccessPolicy));
res.json(centralDataMeshAccessPolicy);

// convert policy JSON into string and assign into params
var bucketPolicyParams = {Bucket: req.body.producerbucketname , Policy: JSON.stringify(centralDataMeshAccessPolicy)};

// set the new policy on the selected bucket
  s3.putBucketPolicy(bucketPolicyParams, function(err, data) {
    if (err) {
      // display error message
      console.log("Error", err);
    } else {
      console.log("Success", data);
    }
  });

});

app.post('/api/registerDataLakeLocationInCentralDataMesh',(req,res)=>{
  AWS.config.update({
    accessKeyId: "ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });
  var lakeformation = new AWS.LakeFormation();
  var bucketResource = "arn:aws:s3:::" + req.body.producerbucketname 
  var params = {
    ResourceArn: '', /* required */
    // RoleArn: 'STRING_VALUE',
    UseServiceLinkedRole: true 
  };
  params.ResourceArn = bucketResource;
  lakeformation.registerResource(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
      res.json("Error");
    }
    else {
      console.log(data);           // successful response
      res.json("Ok");
    }
  });
});

//https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Glue.html#createDatabase-property
app.post('/api/createDataBaseInCentralAccount',(req,res)=>{
  AWS.config.update({
    accessKeyId: "ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });
  var glue = new AWS.Glue();
  var bucketdatabasename = req.body.bucketdatabasename; 
  var locationuri = req.body.locationuri;
  var params = {
    DatabaseInput: { /* required */
      Name: '', /* required */
      CreateTableDefaultPermissions: [],
      Description: 'Centralised database in the Dentral Data Mesh account',
      LocationUri: '',
      // Parameters: {
      //   '<KeyString>': 'STRING_VALUE',
      //   /* '<KeyString>': ... */
      // },
      // TargetDatabase: {
      //   CatalogId: '226124923270',
      //   DatabaseName: 'abcpkdatabase'
      // }
    },
    // CatalogId: 'STRING_VALUE',
    // Tags: {
    //   '<TagKey>': 'STRING_VALUE',
    //   /* '<TagKey>': ... */
    // }
  };
  params.DatabaseInput.Name = bucketdatabasename;
  params.DatabaseInput.LocationUri = locationuri;
  console.log("Params is :: ",JSON.stringify(params));
  glue.createDatabase(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
      res.json("Error");
    }
    else {
      console.log(data);           // successful response
      res.json("Ok");
    }
  });
});

//the centralised account is 226124923270 which provides access to the Producer account 774767717530 for create_table,alter and describe to the database
//if needed can be used for consumer account as well
app.post('/api/grantDBPermissionsOtherAccounts', (req, res) => {
/*credentials of the central account which is giving access to the producer account*/
   AWS.config.update({ 
    accessKeyId: "ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL", 
    secretAccessKey: "SECRET_ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
   });
   var lakeformation = new AWS.LakeFormation();
   var bucketdatabasename = req.body.bucketdatabasename; 
   var principaltobegivenaccess =  req.body.principaltobegivenaccess;
   var sourceaccountgivingaccess =  req.body.sourceaccountgivingaccess;
   var permissions =  req.body.permissions;
   var params = {
     Entries: [ /* required */
       {
         Id: '1', /* required */
         Permissions: [
           '',
           /* more items */
         ],
         PermissionsWithGrantOption: [
           '',
           /* more items */
         ],
         Principal: {
           DataLakePrincipalIdentifier: '' /* principal to be granted permission  , producer/consumer account in this case*/
         },
         Resource: {
           Database: {
             Name: '', /* required */
             CatalogId: '' /*caller account who is execiuting this API , central account in this case*/
           },
         }
       },
       /* more items */
     ],
     CatalogId: '' /*The identifier for the Data Catalog. By default, the account ID of teh account which will provide access to the other account. */
   };
 
   params.Entries[0].Principal.DataLakePrincipalIdentifier = principaltobegivenaccess;
   params.Entries[0].Resource.Database.Name = bucketdatabasename;
   params.Entries[0].Resource.Database.CatalogId = sourceaccountgivingaccess;
   params.Entries[0].Permissions = permissions;
   params.Entries[0].PermissionsWithGrantOption= permissions;
  
   params.CatalogId = sourceaccountgivingaccess;
   console.log(permissions[0]);
   lakeformation.batchGrantPermissions(params, function(err, data) {
     if (err) console.log(err, JSON.stringify(err.stack)); // an error occurred
     else  console.log(data);           // successful response
   });
   res.json("target account granted persmission to access the LFN table");
 });

//the centralised account is 226124923270 which provides access to the Comnsumer account  137311287507 for select permissions to the table
app.post('/api/grantTablePermissionsOtherAccounts', (req, res) => {
  /*credentials of the central account which is giving access to the producer account*/
     AWS.config.update({ 
      accessKeyId: "ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL", 
      secretAccessKey: "SECRET_ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
      region:"us-east-1"
     });
     var lakeformation = new AWS.LakeFormation();
     var bucketdatabasename = req.body.bucketdatabasename; 
     var principaltobegivenaccess =  req.body.principaltobegivenaccess;
     var sourceaccountgivingaccess =  req.body.sourceaccountgivingaccess;
     var permissions =  req.body.permissions;
     var params = {
       Entries: [ /* required */
         {
           Id: '1', /* required */
           Permissions: [
             "",
             /* more items */
           ],
           PermissionsWithGrantOption: [
            ""
             /* more items */
           ],
           Principal: {
             DataLakePrincipalIdentifier: '' /* principal to be granted permission  , producer/consumer account in this case*/
           },
           Resource: {
             Table: {
              DatabaseName: '', /* required */
              CatalogId: '',
              TableWildcard: {}
            }
           }
         },
         /* more items */
       ],
       CatalogId: '' /*The identifier for the Data Catalog. By default, the account ID of teh account which will provide access to the other account. */
     };
   
     params.Entries[0].Principal.DataLakePrincipalIdentifier = principaltobegivenaccess;
     params.Entries[0].Resource.Table.DatabaseName = bucketdatabasename;
     params.Entries[0].Resource.Table.CatalogId = sourceaccountgivingaccess;
     params.Entries[0].Permissions = permissions;
     params.Entries[0].PermissionsWithGrantOption= permissions;
    params.CatalogId = sourceaccountgivingaccess;

     console.log(permissions[0]);
     lakeformation.batchGrantPermissions(params, function(err, data) {
       if (err) console.log(err, JSON.stringify(err.stack)); // an error occurred
       else  console.log(data);           // successful response
     });
     res.json("target account granted persmission to access the LFN table");
   });

   //accept resource shares in accounts
   //in this case accept resource shares in producer and consumer accounts
   app.post('/api/acceptresourcesharesProducer', (req, res) => {
    AWS.config.update({
      accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
      secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
      region:"us-east-1"
    });
    var ram = new AWS.RAM({apiVersion: '2018-01-04'}); //lock the API version 
    var params = {
      // resourceOwner: "OTHER-ACCOUNTS", /* required */
      maxResults: 20,
      
      // resourceShareStatus: "PENDING"
    };
    ram.getResourceShareInvitations(params, function(err, data) {
      if (err) console.log(err, err.stack); // an error occurred
      else     {
        console.log(data);           // successful response
        data["resourceShareInvitations"].forEach((resourceShareInvitation)=>{
          if (resourceShareInvitation.status == 'PENDING'){
            console.log("Resource pending is :: " ,resourceShareInvitation.resourceShareInvitationArn);
            var params = {
              resourceShareInvitationArn: resourceShareInvitation.resourceShareInvitationArn, /* required */
  
            };
            ram.acceptResourceShareInvitation(params, function(err, data) {
              if (err) console.log(err, err.stack); // an error occurred
              else     console.log(data);           // successful response
            });
            res.json("target account accepted resource share");
          }
         
        })
  
      }
    });
  });



  //accept resource shares in accounts
  //in this case accept resource shares in producer and consumer accounts
  app.post('/api/acceptresourcesharesProducertest', (req, res) => {
    AWS.config.update({
      accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
      secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
      region:"us-east-1"
    });
    var ram = new AWS.RAM({apiVersion: '2018-01-04'}); //lock the API version 
    var params = {
      // resourceOwner: "OTHER-ACCOUNTS", /* required */
      maxResults: 20,
      nextToken: req.body.nextToken
      // resourceShareStatus: "PENDING"
    };
    let retry = 0;
    while(retry <= 3){
      console.log("While..... ");
      // sleep(1*1000); // sleep for 10 seconds
      sleep(10*1000).then((response) =>{
      console.log("Sleep.... ", response);
      ram.getResourceShareInvitations(params, function(err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
      }
      else {
          console.log(data);           // successful response
          data["resourceShareInvitations"].forEach((resourceShareInvitation)=>{
              if (resourceShareInvitation.status == 'PENDING'){
                console.log("Resource pending is :: " ,resourceShareInvitation.resourceShareInvitationArn);
                var params = {
                  resourceShareInvitationArn: resourceShareInvitation.resourceShareInvitationArn, /* required */
      
                };
                ram.acceptResourceShareInvitation(params, function(err, data) {
                  if (err) console.log(err, err.stack); // an error occurred
                  else     console.log(data);           // successful response
                });
                // sleep(10*1000).then((response) =>{console.log("Pending changed to accepted ::  Response is ", response);}) 
              }
          })
        } 
        params.nextToken = data.nextToken;
      });
      })//sleep
      retry = retry +1;
    }
    res.json("target account accepted resource share");
  });

  app.post('/api/acceptresourcesharesConsumer', (req, res) => {
    AWS.config.update({
      accessKeyId: "ACCESS_KEY_CONSUMER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
      secretAccessKey: "SECRET_KEY_CONSUMER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
      region:"us-east-1"
    });
    var ram = new AWS.RAM({apiVersion: '2018-01-04'}); //lock the API version 
    var params = {
      // resourceOwner: "OTHER-ACCOUNTS", /* required */
      maxResults: 20,
      nextToken: req.body.nextToken
      // resourceShareStatus: "PENDING"
    };
    let retry = 0;
    while(retry <= 3){
      console.log("While..... ");
      // sleep(1*1000); // sleep for 10 seconds
      sleep(10*1000).then((response) =>{
      console.log("Sleep.... ", response);
      ram.getResourceShareInvitations(params, function(err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
      }
      else {
          console.log(data);           // successful response
          data["resourceShareInvitations"].forEach((resourceShareInvitation)=>{
              if (resourceShareInvitation.status == 'PENDING'){
                console.log("Consumer Resource pending is :: " ,resourceShareInvitation.resourceShareInvitationArn);
                var params = {
                  resourceShareInvitationArn: resourceShareInvitation.resourceShareInvitationArn, /* required */
      
                };
                ram.acceptResourceShareInvitation(params, function(err, data) {
                  if (err) console.log(err, err.stack); // an error occurred
                  else     console.log(data);           // successful response
                });
                // sleep(10*1000).then((response) =>{console.log("Pending changed to accepted ::  Response is ", response);}) 
              }
          })
        } 
        params.nextToken = data.nextToken;
      });
      })//sleep
      retry = retry +1;
    }
    res.json("target consumer account accepted resource share");
  });

//create resource link in producer account and then the consumer account
app.post('/api/createResourceLinksInTheAccount', (req, res) => {
  AWS.config.update({ //admin in account who can create resource links
    accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });
  var resourcelinkname = req.body.resourcelinkname; 
  var databasename =  req.body.databasename;
  var databaseowningaccount =  req.body.databaseowningaccount;
  var glue = new AWS.Glue({apiVersion: '2017-03-31'});
  var params = {
    DatabaseInput: { /* required */
      Name: '', /* required */
      TargetDatabase: {
        CatalogId: '',/*Source database account , in central DM this is the central DM account */
        DatabaseName: ''
      }
    },
  };
  params.DatabaseInput.Name = resourcelinkname;
  params.DatabaseInput.TargetDatabase.CatalogId = databaseowningaccount;
  params.DatabaseInput.TargetDatabase.DatabaseName = databasename;

  glue.createDatabase(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
      res.json("glue db resource link creation failed ...");
    }
    else{
      console.log(data);           // successful response
      res.json("glue db resource link creation succesful ...");
    }
  });

});

app.post('/api/createResourceLinksInConsumerAccount', (req, res) => {
  AWS.config.update({
    accessKeyId: "ACCESS_KEY_CONSUMER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_KEY_CONSUMER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });
  var resourcelinkname = req.body.resourcelinkname; 
  var databasename =  req.body.databasename;
  var databaseowningaccount =  req.body.databaseowningaccount;
  var glue = new AWS.Glue({apiVersion: '2017-03-31'});
  var params = {
    DatabaseInput: { /* required */
      Name: '', /* required */
      TargetDatabase: {
        CatalogId: '',/*Source database account , in central DM this is the central DM account */
        DatabaseName: ''
      }
    },
  };
  params.DatabaseInput.Name = resourcelinkname;
  params.DatabaseInput.TargetDatabase.CatalogId = databaseowningaccount;
  params.DatabaseInput.TargetDatabase.DatabaseName = databasename;

  glue.createDatabase(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
      res.json("glue db resource link creation failed ...");
    }
    else{
      console.log(data);           // successful response
      res.json("glue db resource link creation succesful ...");
    }
  });

});


//setup glue crawler with IAM role
//update Glue crawler IAM role to give it access in Lakeformation
//run glue crawler to create the tables
//give access to specific iam user for athena access
//use athena in producer/central/consumer account to check access 
app.post('/api/createIAMRoleForGlueCrawler', (req, res) => { //TODO
  AWS.config.update({ //admin in account who can create resource links
    accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });

  var gluecrawlerrolename = req.body.gluecrawlerrolename;
  var s3bucketaccessforglue =  req.body.s3bucketaccessforglue;
  var gluecrawleraccount = req.body.gluecrawleraccount;
 
  //https://docs.aws.amazon.com/IAM/latest/UserGuide/example_iam_CreatePolicy_section.html
  // Create the IAM service object
  var iam = new AWS.IAM({apiVersion: '2010-05-08'});
  var myAssumeRolePolicy = {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "glue.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  };

  var assumerolePolicyDocument = {
    AssumeRolePolicyDocument: JSON.stringify(myAssumeRolePolicy),
    RoleName: gluecrawlerrolename
  };

  var awsmanagedpolicyforglueparams = {
    PolicyArn: "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    RoleName: gluecrawlerrolename
  };

  var customerdefinedpolicy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": ["arn:aws:s3:::","arn:aws:s3:::"]
        }
    ]
  };
  customerdefinedpolicy.Statement[0].Resource[0] = customerdefinedpolicy.Statement[0].Resource[0]+s3bucketaccessforglue;
  customerdefinedpolicy.Statement[0].Resource[1] = customerdefinedpolicy.Statement[0].Resource[1]+s3bucketaccessforglue+"/*";

  var gluecrawlers3bucketinlinepolicyparams = {
    PolicyDocument: JSON.stringify(customerdefinedpolicy),
    PolicyName: 'CustomPolicy_',
  };
  gluecrawlers3bucketinlinepolicyparams.PolicyName =  gluecrawlers3bucketinlinepolicyparams.PolicyName +gluecrawlerrolename;


  var createds3bucketpolicyparams = {
    PolicyArn: '',
    RoleName: gluecrawlerrolename
  };
  createds3bucketpolicyparams.PolicyArn = "arn:aws:iam::"+gluecrawleraccount+":policy/"+gluecrawlers3bucketinlinepolicyparams.PolicyName;

  iam.createPolicy(gluecrawlers3bucketinlinepolicyparams, function(err, data) {
    if (err) {
      console.log("Error creating the initial policy for the glue crawler to be able to crawl the s3 location", err);
      res.json(err);
    } else {
      console.log("Success creating the initial policy for the glue crawler to be able to crawl the s3 location", data);
      iam.createRole(assumerolePolicyDocument, function(err, data) {
        if (err) {
            console.log(err, err.stack); // an error occurred
            console.log("Glue IAM Role creation failed");
            res.json(err);
        } else {
            console.log("Role ARN is", data.Role.Arn);           // successful response
            iam.attachRolePolicy(awsmanagedpolicyforglueparams , function(err, data) {
            if (err) {
                console.log(err, err.stack);
                console.log("Error associating AWS managed policy with Glue Crawler",err);
                res.json(err);
            } else {
                console.log("Success", data);
                iam.attachRolePolicy(createds3bucketpolicyparams , function(err, data) {
                if (err) {
                    console.log(err, err.stack);
                    console.log("Error associating Customer managed policy with Glue Crawler",err);
                    res.json(err);
                } else {
                    console.log("Customer managed Policy attached");
                    console.log("Step 1 -  Glue IAM Role created");
                    res.json(data);
                    
                }
              })
            }
          });
        }
      })
    }
  })

})

app.post('/api/createLakeFormationAccessInProducer', (req,res) => {

  AWS.config.update({ //admin in account who can create resource links
    accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });

  var gluecrawlerrolename = req.body.gluecrawlerrolename;
  var databasename = req.body.databasename;
  var accountownerofdatabase = req.body.accountownerofdatabase;
  var gluecrawleraccount = req.body.gluecrawleraccount;

  //give access to the shared database to the glue iam role in lakeformation
  var params = {
    Entries: [ /* required */
      {
        Id: '1', /* required */
        Permissions: [
          "ALTER","DESCRIBE", "CREATE_TABLE"
          /* more items */
        ],
        PermissionsWithGrantOption: [
          "ALTER","DESCRIBE", "CREATE_TABLE"
          /* more items */
        ],
        Principal: {
          DataLakePrincipalIdentifier: '' /*glue iam role */
        },
        Resource: {
          
          Database: {
            Name: '', /* required  shared databse name*/
            CatalogId: '' /*source owning database id*/
          },        
        }
      },
      /* more items */
    ],
    CatalogId: '' /*producer account id*/
  };

  params.Entries[0].Principal.DataLakePrincipalIdentifier = 'arn:aws:iam::'+gluecrawleraccount+':role/'+gluecrawlerrolename;
  params.Entries[0].Resource.Database.Name = databasename;
  params.Entries[0].Resource.Database.CatalogId = accountownerofdatabase;
  params.CatalogId =  gluecrawleraccount;

  
  sleep(60*1000).then((response) => {
      var lakeformation = new AWS.LakeFormation({apiVersion: '2017-03-31'});
      lakeformation.batchGrantPermissions(params, function(err, data) {
        if (err) {
          console.log(err, err.stack); // an error occurred
          console.log("Lake formation grant permission ALTER , create, describe to glue IAM role failed");
          res.json(err);
        }
        else {
          console.log("Lake formation grant permission ALTER , create, describe  to glue IAM role succesful :: ",data);
          console.log(response);           // successful response
          console.log("Lake formation grant permission describe, drop to glue IAM role succesful after sleep");
          res.json(data);
        } //end of else
      })
  })
})
  
 // give access to the resource link to the glue iam role in lakeformation
app.post('/api/createAdditionalLakeFormationAccessInProducer', (req,res) => {

  AWS.config.update({ //admin in account who can create resource links
    accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });

  var gluecrawlerrolename = req.body.gluecrawlerrolename;
  var gluecrawleraccount = req.body.gluecrawleraccount;

  // give access to the resource link to the glue iam role in lakeformation
  var params = {
    Entries: [ /* required */
      {
        Id: '2', /* required */
        Permissions: [
          "DESCRIBE", "DROP"
          /* more items */
        ],
        PermissionsWithGrantOption: [
          "DESCRIBE", "DROP"
          /* more items */
        ],
        Principal: {
          DataLakePrincipalIdentifier: '' /*glue iam role */
        },
        Resource: {
          
          Database: {
            Name: '', /* required  shared databse name*/
            CatalogId: '' /* source owning resource link*/
          },        
        }
      },
      /* more items */
    ],
    CatalogId: '' /*producer account id*/
  };

  params.Entries[0].Principal.DataLakePrincipalIdentifier = 'arn:aws:iam::'+gluecrawleraccount+':role/'+gluecrawlerrolename;
  params.Entries[0].Resource.Database.Name = req.body.resourcelinkname;
  params.Entries[0].Resource.Database.CatalogId = gluecrawleraccount;
  params.CatalogId =  gluecrawleraccount;

  sleep(60*1000).then((response) => {
      var lakeformation = new AWS.LakeFormation({apiVersion: '2017-03-31'});
      lakeformation.batchGrantPermissions(params, function(err, data) {
        console.log(response);   
            if (err) {
              console.log(err, err.stack); // an error occurred
              console.log("Lake formation grant permission describe, drop to glue IAM role failed after sleep");
              res.json(err);
            }
            else {
                console.log("Lake formation grant permission describe, drop to glue IAM role succesful after sleep");
                res.json(data);
            }
      })//batchgrantpermissions
  })
})

app.post('/api/createGlueCrawler', (req,res) => {

  AWS.config.update({ //admin in account who can create resource links
    accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });
    // setup the glue crawler
    var params = {
      Name: req.body.crawlername, /* required */
      Role: "arn:aws:iam::"+req.body.gluecrawleraccount+":role/"+req.body.gluecrawlerrolename, /* required */
      DatabaseName: req.body.resourcelinkname, /*resource link name*/
      Targets: { /* required */
        DeltaTargets: [
          
        ],
        DynamoDBTargets: [
          
        ],
        JdbcTargets: [
         
        ],
        MongoDBTargets: [
          
        ],
        S3Targets: [
          {
            ConnectionName: '',
            DlqEventQueueArn: '',
            Exclusions: [
            ],
            Path: '', /*Path of S3 bucket in producer account*/
          },
          /* more items */
        ]
      },
      Classifiers: [
        
      ],
      Configuration: "{\"Version\":1}",
      RecrawlPolicy: {
        RecrawlBehavior: "CRAWL_EVERYTHING"
      },
      Schedule: '',
      SchemaChangePolicy: {
        DeleteBehavior: "DEPRECATE_IN_DATABASE",
        UpdateBehavior: "UPDATE_IN_DATABASE"
      },
      TablePrefix: '',
    };
    params.Targets.S3Targets[0].Path = 's3://'+req.body.s3bucketaccessforglue+"/"

        var glue = new AWS.Glue({apiVersion: '2017-03-31'});
          sleep(10*1000).then((response) => {
          glue.createCrawler(params, function(err, data) {
            if (err) {
              console.log(err, err.stack); // an error occurred
              res.json("Create Crawler failed");
            }
            else{
              console.log(data);           // successful response
              console.log("Create Crawler succesful");
              //execute the glue crawler
              var params = {
                Name: req.body.crawlername /* required name of crawler example CentralMeshCrawler */
              };
              // var glue = new AWS.Glue({apiVersion: '2017-03-31'});
              glue.startCrawler(params, function(err, data) {
                if (err) {
                  console.log(err, err.stack); // an error occurred
                  res.json("Glue crawler failed to start .....")
                }
                else  {  
                  console.log(data);           // successful response
                  res.json("Glue crawler running .....")
                }
              });
             console.log("Step 4 -  Crawler  created  and executed ");
            }//end of else
          })
        });
})

app.post('/api/deleteFromEnterpriseDataLake',(req,res) => {
  //delete the data location
  AWS.config.update({
    accessKeyId: "ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });
  var lakeformation = new AWS.LakeFormation();
  var bucketResource = "arn:aws:s3:::" + req.body.producerbucketname 
  var params = {
    ResourceArn: '', /* required */
  };
  params.ResourceArn = bucketResource;
  lakeformation.deregisterResource(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
    }
      console.log("Lakeformation Deregistering the S3 Data Location succesful ",data);           // successful response
        var lakeformation = new AWS.LakeFormation();
        var bucketdatabasename = req.body.bucketdatabasename; 
        var principaltobegivenaccess =  req.body.principaltobegivenaccess;
        var sourceaccountgivingaccess =  req.body.sourceaccountgivingaccess; /*central EDP account */
        var permissions =  req.body.permissions;
        var params = {
              Permissions: [
                "",
                /* more items */
              ],
              PermissionsWithGrantOption: [
              ""
                /* more items */
              ],
              Principal: {
                DataLakePrincipalIdentifier: '' /* principal to be granted permission  , producer/consumer account in this case*/
              },
              Resource: {
                
                Database: {
                  Name: '', /* required  shared databse name*/
                  CatalogId: '' /*source owning database id*/
                },        
              },
        };

        params.Principal.DataLakePrincipalIdentifier = principaltobegivenaccess;
        params.Resource.Database.Name = bucketdatabasename;
        params.Resource.Database.CatalogId = sourceaccountgivingaccess;
        params.Permissions = permissions;
        params.PermissionsWithGrantOption= permissions;

        lakeformation.revokePermissions(params, function(err, data) {
          if (err) {
            console.log(err, JSON.stringify(err.stack)); // an error occurred
            console.log("revoke LFN permissions given to the producer account from centyral account failed");
          }
            console.log("revoke LFN permissions given to the producer account from centyral account succesful :: ", data);  
            //delete the database
            var glue = new AWS.Glue({apiVersion: '2017-03-31'});
            var params = {
              Name: '', /* required */
              CatalogId: ''
            };
            params.Name = req.body.bucketdatabasename;
            params.CatalogId = req.body.sourceaccountgivingaccess;
            glue.deleteDatabase(params, function(err, data) {
              if (err) {
                console.log(err, err.stack); // an error occurred
                console.log("delete the database in EDP failed");
              }
                  console.log("delete the database in EDP succesful :: ", data);           // successful response
                  //go to producer account and delete the resourcelink
                  AWS.config.update({ //admin in account who can create resource links
                    accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
                    secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
                    region:"us-east-1"
                  });
                  var glue = new AWS.Glue({apiVersion: '2017-03-31'});
                  var params = {
                    Name: '', /* required */
                    CatalogId: ''
                  };
                  params.Name = req.body.resourcelinkname;
                  params.CatalogId = req.body.produceraccountnumber;
                  glue.deleteDatabase(params, function(err, data) {
                    if (err) {
                      console.log(err, err.stack); // an error occurred
                      console.log("Delete glue database resource link in Producer account failed");
                    }
                      console.log("Delete glue database resource link in Producer account successful:: ", data);           // successful response
                      //go to producer account and revoke the permissions for the Glue Crawler
                      var lakeformation = new AWS.LakeFormation();
                      var bucketdatabasename = req.body.bucketdatabasename; 
                      var principaltobegivenaccess =  'arn:aws:iam::'+req.body.gluecrawleraccount+':role/'+req.body.gluecrawlerrolename;
                      var sourceaccountgivingaccess =  req.body.sourceaccountgivingaccess; /*central EDP account */
                      var permissions =  req.body.permissions;
                      var params = {
                            Permissions: [
                              "",
                              /* more items */
                            ],
                            PermissionsWithGrantOption: [
                            ""
                              /* more items */
                            ],
                            Principal: {
                              DataLakePrincipalIdentifier: '' /* principal to be granted permission  , producer/consumer account in this case*/
                            },
                            Resource: {
                              
                              Database: {
                                Name: '', /* required  shared databse name*/
                                CatalogId: '' /*source owning database id*/
                              },        
                            },
                      };

                      params.Principal.DataLakePrincipalIdentifier = principaltobegivenaccess;
                      params.Resource.Database.Name = bucketdatabasename;
                      params.Resource.Database.CatalogId = sourceaccountgivingaccess;
                      params.Permissions = permissions;
                      params.PermissionsWithGrantOption= permissions;

                      lakeformation.revokePermissions(params, function(err, data) {
                        if (err) {
                          console.log(err, JSON.stringify(err.stack)); // an error occurred
                          console.log("Revoke the permissions for the Glue Crawler for ALTER/CREATE TABLE /DESCRIBE for resourcelink in producer failed");
                        }
                            //go to producer account and revoke the permissions for the Glue Crawler for DESCRIBE/DROP for resourcelink
                            console.log("Revoke the permissions for the Glue Crawler for ALTER/CREATE TABLE /DESCRIBE for resourcelink in producer successful");
                            var lakeformation = new AWS.LakeFormation();
                            var resourcelinkname = req.body.resourcelinkname; 
                            var principaltobegivenaccess =  'arn:aws:iam::'+req.body.gluecrawleraccount+':role/'+req.body.gluecrawlerrolename;
                            var produceraccountnumber =  req.body.produceraccountnumber; /*central EDP account */
                            var params = {
                                  Permissions: [
                                    "DESCRIBE", "DROP"
                                    /* more items */
                                  ],
                                  PermissionsWithGrantOption: [
                                    "DESCRIBE", "DROP"
                                    /* more items */
                                  ],
                                  Principal: {
                                    DataLakePrincipalIdentifier: '' /* principal to be granted permission  , producer/consumer account in this case*/
                                  },
                                  Resource: {
                                    
                                    Database: {
                                      Name: '', /* required  shared databse name*/
                                      CatalogId: '' /*source owning database id*/
                                    },        
                                  },
                            };
      
                            params.Principal.DataLakePrincipalIdentifier = principaltobegivenaccess;
                            params.Resource.Database.Name = resourcelinkname;
                            params.Resource.Database.CatalogId = produceraccountnumber;
                            // params.Permissions = permissions;
                            // params.PermissionsWithGrantOption= permissions;
      
                            lakeformation.revokePermissions(params, function(err, data) {
                              if (err) {
                                console.log(err, JSON.stringify(err.stack)); // an error occurred
                                console.log("Revoke the permissions for the Glue Crawler for DROP/DESCRIBE for resourcelink in producer failed");
                              }
                                  console.log("Revoke the permissions for the Glue Crawler for DROP/DESCRIBE for resourcelink in producer successful");
                                  var params = {
                                    Name: '' /* required */
                                  };
                                  params.Name = req.body.crawlername;
                                  glue.deleteCrawler(params, function(err, data) {
                                    if (err){
                                      console.log(err, err.stack); // an error occurred
                                      console.log("Delete crawler failed");
                                    }
                                    console.log("Delete crawler succesful :: ", data);           // successful response
                                    //delete the Glue IAM Role 
                                      var iam = new AWS.IAM({apiVersion: '2010-05-08'}); 
                                      console.log("Delete crawler role successful :: ", data);           // successful response
                                      var params = {
                                        PolicyArn: '', /* required */
                                        RoleName: '' /* required */
                                      };
                                      params.PolicyArn = "arn:aws:iam::"+req.body.produceraccountnumber+":policy/"+"CustomPolicy_" +req.body.gluecrawlerrolename;
                                      params.RoleName = req.body.gluecrawlerrolename;
                                      iam.detachRolePolicy(params, function(err, data) {
                                      if (err){
                                          console.log(err, err.stack); // an error occurred
                                          console.log("Detach IAM Policy from Glue Role failed");
                                      } 
                                      console.log("Detach IAM Policy from Glue Role successful");
                                      var params = {
                                        PolicyArn: '', /* required */
                                        RoleName: '' /* required */
                                      };
                                      params.PolicyArn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole";
                                      params.RoleName = req.body.gluecrawlerrolename;
                                      iam.detachRolePolicy(params, function(err, data) {
                                      if (err){
                                          console.log(err, err.stack); // an error occurred
                                          console.log("Detach Managed IAM Policy from Glue Role failed");
                                      } 
                                      console.log("Detach Managed IAM Policy from Glue Role successful");
                                      //delete the Glue IAM policy
                                      var params = {
                                        PolicyArn: '' /* required */
                                      };
                                      params.PolicyArn =  "arn:aws:iam::"+req.body.produceraccountnumber+":policy/"+"CustomPolicy_" +req.body.gluecrawlerrolename;
                                      iam.deletePolicy(params, function(err, data) {
                                            if (err) {
                                              console.log(err, err.stack); // an error occurred
                                              console.log("Delete crawler role policy failed");
                                            }
                                            console.log("Delete crawler role policy successful:: ", data);           // successful response
                                            var params = {
                                            RoleName: ''
                                            };
                                            params.RoleName = req.body.gluecrawlerrolename;
                                            iam.deleteRole(params, function(err, data) {
                                              if (err) {
                                                console.log(err, err.stack); // an error occurred
                                                console.log("Delete crawler role failed");
                                              }
                                              console.log("Delete crawler role successful");  
                                            });//iam deleterole
                                          })
                                      });//iam deletepolicy
                                    });//iam detach role policy                          
                                  });//delete crawler
                            })
                      })//lakeformation.revokepermissions in producer for gluecrawlerrole
                  });
            });
        });
  });

  res.json("Deleted from EDP");

})


//Central Data Lake account exploration options
app.get('/api/getDatabases',(req,res) =>{
  /*credentials of the central account */
  AWS.config.update({ 
    accessKeyId: "ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL", 
    secretAccessKey: "SECRET_ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
   });

   var params = {
    MaxResults: 100,
    NextToken: req.query.NextToken ,
    ResourceShareType: 'ALL'
  };
  var glue = new AWS.Glue({apiVersion: '2017-03-31'});
  glue.getDatabases(params, function(err, data) {
    if (err){
      console.log(err, err.stack); // an error occurred
      res.json(err);
    } 
    else {
      console.log(data);           // successful response
      res.json(data)
    }  
  });
})

// search tables in a specific database in teh centralised catalog account
app.get('/api/searchTables',(req,res) => {
  AWS.config.update({ 
    accessKeyId: "ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL", 
    secretAccessKey: "SECRET_ACCESS_KEY_CENTRAL_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
   });
   console.log(req.query.CatalogId);
   console.log(req.query.databasename);
   console.log(req.query.NextToken);
  var params = {
    CatalogId: req.query.CatalogId,
    Filters: [
      {
    
        Key: 'databasename',
        Value: req.query.databasename
      },
      /* more items */
    ],
    MaxResults: 10,
    NextToken: req.query.NextToken,
    ResourceShareType: 'ALL',
    SortCriteria: [
      {
        FieldName: 'UpdateTime',
        Sort: "DESC"
      },
      /* more items */
    ]
  };
  // params.NextToken = req.body.NextToken;
  // params.CatalogId = req.body.CatalogId;
  // params.Filters[0].Value =  req.body.databasename;
  var glue = new AWS.Glue({apiVersion: '2017-03-31'});
  glue.searchTables(params, function(err, data) {
    if (err){
      console.log(err, err.stack); // an error occurred
      res.json(err);
    } 
    else {
      console.log(data);           // successful response
      res.json(data)
    }
  });
})


// https://aws.amazon.com/blogs/apn/using-athena-express-to-simplify-sql-queries-on-amazon-athena/
app.get('/api/getAthenaResults', (req, res) => {
  AWS.config.update({ //producer account
    accessKeyId: "ACCESS_ID_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    secretAccessKey: "SECRET_KEY_PRODUCER_ACCOUNT_IAM_ADMIN_WHO_RUNS_THIS_TOOL",
    region:"us-east-1"
  });
 
  var dbname  = req.query.databasename+"link";
  var tablename = req.query.tablename;
  var repairquery = "MSCK REPAIR TABLE "+tablename
  var previewquery = "SELECT * FROM "+dbname+"."+tablename+ " limit 10;"
  const athenaExpressConfig = { aws: AWS }; //configuring athena-express with aws sdk object
  const athenaExpress = new AthenaExpress(athenaExpressConfig);
  let query = {
    sql: repairquery /* required */,
    db: dbname
  };
  
  athenaExpress
    .query(query)
    .then(results => {
      console.log(results);

      let query = {
         sql: previewquery,
        db: dbname
      };
      athenaExpress
      .query(query)
      .then(results => {
        res.json(results);
      })
      .catch(error => {
        console.log(error);
        res.json(error);
      })
    })
    .catch(error => {
      console.log(error);
      res.json(error);
    });
})


app.get('/api/users', (req, res) => {
  console.log('api/users called!')
  res.json(users);
});

app.post('/api/user', (req, res) => {
  const user = req.body.user;
  console.log('Adding user:::::', user);
  users.push(user);
  res.json("user addedd");
});

app.get('/', (req,res) => {
  res.sendFile(path.join(__dirname, '../datamesh-ui/dist/index.html'));
});

app.listen(port, () => {
    console.log(`Server listening on the port::${port}`);
});