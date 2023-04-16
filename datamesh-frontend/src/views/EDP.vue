<template>
    <div class="container text-center  mt-5 mb-5">
      <h1 class="mt-5 fw-bolder text-success ">Enterprise Central Database Catalogs</h1>
      <div class="table-responsive my-5">
        <table id="tableComponent" class="table table-bordered table-hover">
          <thead>
              <tr>
              <!-- loop through each value of the fields to get the table header -->
              <th  v-for="field in catalogfields" :key='field' @click="sortTable(field)" > 
                  {{field}} <i class="bi bi-sort-alpha-down" aria-label='Sort Icon'></i>
              </th>
              </tr>
          </thead>
          <tbody>
              <!-- Loop through the list get the each table data for the specific catalog-->
              <tr v-for="item in catalogData" :key='item' v-on:click="retrieveTables(item)">
              <td v-for="field in catalogfields" :key='field'>{{item[field]}}</td>
              </tr>
          </tbody>
        </table> 
      </div>
       <div class="mb-3 mt-3"> 
        <button  @click="retrieveCatalogues()" class="btn btn-md btn-primary" >Fetch More Databases</button> 
      </div>
    
    </div>

    <h1 class="mt-5 fw-bolder text-success ">Glue Tables</h1>
    <div id="table-container"></div>
   

   <!-- <div class="form-outline w-50 " >
      <textarea class="form-control" id="textAreaExample2" rows="8"></textarea>
      <label class="form-label" for="Table Data Preview ">Message</label>
    </div> -->

    <div class="mb-3 mt-3">
      <label for="consumeraccount" class="form-label">Consumer Account ID </label>
      <input type="text" class="form-control" id="consumeraccount" placeholder="Consumer Account ID To be shared with" v-model="consumeraccount" >
    </div>

     <div class="mb-3 mt-3"> 
      <button  @click="giveReadOnlyAccessToConsumerAccount()" class="btn btn-md btn-primary" >Share with Account</button>
     </div>
</template>
<script>
import axios from 'axios'
export default {
  data: () =>({
       tableData :[],
       tablefields: [],
       catalogData:  [],
       catalogfields : ['Name','Description','LocationUri','CatalogId'],
       consumeraccount : null,
       NextToken : '',
       NextTokenTables : '',
       dataToBeRegisteredInConsumer : ''
  
       
  }),
   created() {
      axios.get("http://localhost:3080/api/getDatabases?NextToken="+this.NextToken)
      .then((response)  =>  {
        console.log(response)
        this.catalogData = response.data.DatabaseList
        this.NextToken =  response.data.NextToken
      }, (error)  =>  {
        //this.loading = false;
        console.log(error);
      })
  },
  methods: {
    retrieveCatalogues() {
      axios.get("http://localhost:3080/api/getDatabases?NextToken="+this.NextToken)
      .then((response)  =>  {
        console.log(response)
        this.catalogData = response.data.DatabaseList
        this.NextToken =  response.data.NextToken
        if (response.data.DatabaseList.length == 0 ) {
          this.NextToken = ''

        }
        console.log(this.NextToken);
      }, (error)  =>  {
        //this.loading = false;
        console.log(error);
      })
    },
    retrieveTables(item){
      console.log("Selected table details from catalog :: ", item)
      console.log(item.Name);
      console.log(item.CatalogId);
      this.dataToBeRegisteredInConsumer = item;
      document.getElementById('table-container').innerHTML = '';
      axios.get("http://localhost:3080/api/searchTables?NextToken="+this.NextTokenTables+"&databasename="+item.Name+"&CatalogId="+item.CatalogId)
      .then((response)  =>  {
        console.log(response);
        // Define table field names
        const tableFields = ['Name','DatabaseName','Column Descriptions','PartitionKeys'];  

        // Extract table data from API response
        const tableData = response.data.TableList.map((table) => {
          const columns = table.StorageDescriptor.Columns.map((column) => column.Name);
          const partitionKeys = table.PartitionKeys.map((partitionKey) => partitionKey.Name);
          return {
            Name: table.Name,
            DatabaseName: table.DatabaseName,
            'Column Descriptions': columns,
            PartitionKeys: partitionKeys
          };
        });

        // Get reference to HTML container element
        const container = document.getElementById('table-container');

        // Create HTML table element
        const table = document.createElement('table');

        // Create table header row
        const headerRow = document.createElement('tr');
        tableFields.forEach((field) => {
          const headerCell = document.createElement('th');
          headerCell.textContent = field;
          headerRow.appendChild(headerCell);
        });
        table.appendChild(headerRow);

        // Create table data rows
        tableData.forEach((data) => {
          const dataRow = document.createElement('tr');
          tableFields.forEach((field) => {
            const dataCell = document.createElement('td');
            dataCell.textContent = data[field];
            dataRow.appendChild(dataCell);
          });
          table.appendChild(dataRow);
        });

        //Add table to HTML container element
        container.appendChild(table);


      },(error)  =>  {
        console.log(error);
      })
    } , //retrieveTables
    giveReadOnlyAccessToConsumerAccount(){
      console.log(this.consumeraccount);
      var data = {
        "bucketdatabasename": this.dataToBeRegisteredInConsumer.Name, //database name registered in central governance account
        "principaltobegivenaccess" : this.consumeraccount,
        "sourceaccountgivingaccess" : this.dataToBeRegisteredInConsumer.CatalogId, //central governance account , where the catalog is registered
        "permissions" : ["SELECT"]
      }
      axios.post("http://localhost:3080/api/registerDataLakeLocationInCentralDataMesh", data)
      .then((response)  =>  { 
        console.log("Consumer Account registered in Central governance Account.");
        console.log(response);
        axios.post("http://localhost:3080/api/acceptresourcesharesConsumer") //the API executes in consumer account with credentials of consumer acount user profile or Admin Role for ease , who can accept Resource shares
            .then((response)  =>  {
                  console.log("Resource Share (RAM)  accept within the consumer account succesful");
                  console.log(response);
                  var data = {
                  "resourcelinkname": this.dataToBeRegisteredInConsumer.Name+"link", //database name registered in central governance account
                  "databasename" : this.dataToBeRegisteredInConsumer.Name,
                  "databaseowningaccount" : this.dataToBeRegisteredInConsumer.CatalogId //central governance account , where the catalog is registered
                  }
              axios.post("http://localhost:3080/api/createResourceLinksInConsumerAccount",data)
                .then((response)  =>  {
                  console.log("Resource link created in consumer account so that consumer account can query Athena");
                  console.log(response);
                },(error) => {
                  console.log("Error creating resource sharelinks in consumer account or Resource share link already exists ");
                  console.log(error);
                })
            },(error) => {
              console.log("Error in accepting Resource Shares via RAM in consumer account or Resource share already exists ....");
              console.log(error);
            })
      },(error) => {
        console.log("Error in giving access to consumer account from central governance account");
        console.log(error);
      }
      )
     }//giveReadOnlyAccessToConsumerAccount()

  }
}
</script>