<template>
<div class="container-fluid">
  <h1>Producer Onboarding</h1>

  
  <div class="mb-3 mt-3">
    <label for="produceraccountnumber" class="form-label">Data Producer Account Number:</label>
    <input type="text" class="form-control" id="produceraccountnumber" placeholder="Enter AWS Data Producer Account Number" v-model="produceraccountnumber">
  </div>
  <div class="mb-3 mt-3">
    <label for="producerbucketlocation" class="form-label">Bucket Location:</label>
    <input type="text" class="form-control" id="producerbucketlocation" placeholder="Enter S3 Bucket Location" v-model="producerbucketlocation" >
  </div>
  <div class="mb-3 mt-3">
    <label for="centraldataaccount" class="form-label">Enterprise Data Account:</label>
    <input type="text" class="form-control" id="centraldataaccount" placeholder="Enterprise Data Account" v-model="centraldataaccount" >
  </div>
   <div class="mb-3 mt-3">
    <label for="bucketdatabasename" class="form-label">Database Name To Be Used For Registration:</label>
    <input type="text" class="form-control" id="bucketdatabasename" placeholder="Database Name To Be Used For Registration" v-model="bucketdatabasename" >
  </div>
  
  <div class="mb-3 mt-3"> 
  <button  @click="addtoEDP()" class="btn btn-md btn-primary" >Add to Enterprise Data Platform</button>
  </div>
   <div class="mb-3 mt-3">
  <button  @click="deletefromEDP()" class="btn btn-md btn-primary" >Delete from Enterprise Data Platform</button>
  </div>
  
</div>

</template>

<script >
import axios from 'axios'
export default {
  data: () =>({
        fetchingFacts: false,
        produceraccountnumber: "",
        producerbucketlocation: "",
        centraldataaccount: "",
        bucketdatabasename: ""
  }),
  created() {
   
  },
  methods: {
    loadMoreFacts() {
   
    },
    deletefromEDP(){
      const data = { 
        producerbucketname: this.producerbucketlocation , 
        centraldatameshaccount: this.centraldataaccount,
        produceraccountnumber: this.produceraccountnumber,
        bucketdatabasename: this.bucketdatabasename,
        locationuri: "s3://"+this.producerbucketlocation,
        principaltobegivenaccess:  this.produceraccountnumber,
        sourceaccountgivingaccess: this.centraldataaccount,
        permissions: ["ALTER","CREATE_TABLE","DESCRIBE"],
        resourcelinkname: this.bucketdatabasename+"link",
        databasename: this.bucketdatabasename,
        databaseowningaccount: this.centraldataaccount,
        gluecrawlerrolename: "GlueCrawlerRole"+this.bucketdatabasename,
        s3bucketaccessforglue: this.producerbucketlocation,
        gluecrawleraccount: this.produceraccountnumber,
        accountownerofdatabase: this.centraldataaccount,
        crawlername: "GlueCrawler"+this.bucketdatabasename,
        nextToken: null
        };
            axios.post("http://localhost:3080/api/deleteFromEnterpriseDataLake", data)
      .then((response) => {
        console.log("Deleting from EDP done " , response);
      },(error) => {
        console.log("Error deleting from EDP");
        console.log(error);
      })
    },
    addtoEDP(){
        //api/modifyS3BucketPermissionInProducerAccount
        //api/registerDataLakeLocationInCentralDataMesh
        //createDataBaseInCentralAccount
        //grantDBPermissionsOtherAccounts
        //acceptresourcesharesProducer
        //createResourceLinksInTheAccount
        //createIAMRoleForGlueCrawler
        //createLakeFormationAccessInProducer
        //createGlueCrawler

        //go to producer and put in the db name and do 
        //getAthenaResults
      const data = { 
        producerbucketname: this.producerbucketlocation , 
        centraldatameshaccount: this.centraldataaccount,
        produceraccountnumber: this.produceraccountnumber,
        bucketdatabasename: this.bucketdatabasename,
        locationuri: "s3://"+this.producerbucketlocation,
        principaltobegivenaccess:  this.produceraccountnumber,
        sourceaccountgivingaccess: this.centraldataaccount,
        permissions: ["ALTER","CREATE_TABLE","DESCRIBE"],
        resourcelinkname: this.bucketdatabasename+"link",
        databasename: this.bucketdatabasename,
        databaseowningaccount: this.centraldataaccount,
        gluecrawlerrolename: "GlueCrawlerRole"+this.bucketdatabasename,
        s3bucketaccessforglue: this.producerbucketlocation,
        gluecrawleraccount: this.produceraccountnumber,
        accountownerofdatabase: this.centraldataaccount,
        crawlername: "GlueCrawler"+this.bucketdatabasename,
        nextToken: null
        };
      axios.post("http://localhost:3080/api/modifyS3BucketPermissionInProducerAccount", data)
      .then((response) => {
        console.log("modifyS3BucketPermissionInProducerAccount Done" , response);
         axios.post("http://localhost:3080/api/registerDataLakeLocationInCentralDataMesh", data)
         .then((response) => {
            console.log("registerDataLakeLocationInCentralDataMesh Done" , response);
            axios.post("http://localhost:3080/api/createDataBaseInCentralAccount",data)
            .then((response) => {
              console.log("createDataBaseInCentralAccount Done " ,response);
              axios.post("http://localhost:3080/api/grantDBPermissionsOtherAccounts",data)
              .then((response) =>{
                console.log("grantDBPermissionsOtherAccounts done " , response);
                axios.post("http://localhost:3080/api/acceptresourcesharesProducertest")
                .then((response) => { 
                  console.log("acceptresourcesharesProducertest Done " , response);
                  axios.post("http://localhost:3080/api/createResourceLinksInTheAccount",data)
                  .then((response) => {
                    console.log(" createResourceLinksInTheAccount Done ", response);
                    axios.post("http://localhost:3080/api/createIAMRoleForGlueCrawler",data)
                    .then((response) => {
                      console.log(" createIAMRoleForGlueCrawler DONE" ,response);
                      axios.post("http://localhost:3080/api/createAdditionalLakeFormationAccessInProducer",data)
                      .then((response) => {
                        console.log("createAdditionalLakeFormationAccessInProducer DONE " , response);
                         axios.post("http://localhost:3080/api/createLakeFormationAccessInProducer",data)
                         .then((response) =>{
                           console.log("createLakeFormationAccessInProducer DONE " , response);
                           axios.post("http://localhost:3080/api/createGlueCrawler",data)
                            .then((response) => {
                              console.log("createGlueCrawler DONE" ,response);
                            },(error) => {
                              console.log("Error in setting up Glue Crawler");
                              console.log(error);
                            })
                         },(error) => {
                            console.log("Error in setting up Additional Lakeformation access in Producer account for Database and Resource link");
                            console.log(error);
                         })      
                      },(error) => {
                        console.log("Error in setting up Lakeformation access in Producer account for Database and Resource link");
                        console.log(error);
                      })
                    },(error) => {
                      console.log("Error creating the IAM Role for Glue");
                      console.log(error);
                    }) 
                  },(error) => {
                  console.log("Error creating resource links in the producer account");
                  console.log(error);
                  }) 
                },(error) => {
                  console.log("Error accepting resource share in producer account");
                  console.log(error);
                }) 
              },(error) => {
                console.log("Error in granting database alter,create_table,describe access to producer account ");
                console.log(error);
              })
            },(error) => {
              console.log("Error while creating Database in Central Account");
              console.log(error);
            })
         },(error) => {
           console.log("Error in registering Data Location in Central Data Account");
            console.log(error);
         })
      }, (error) => {
        console.log("Error in updating bucket policy giving access to Central Data Account");
        console.log(error);
      })
    }
  }

}
</script>

