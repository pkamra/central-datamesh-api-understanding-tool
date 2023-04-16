<template>
  <h1>Consumer Page</h1>
  <div class="mb-3 mt-3">
      <label for="databasename" class="form-label">Database Name</label>
      <input type="text" class="form-control" id="databasename" placeholder="Database Name That was shared with Consumer" v-model="databasename" >
  </div>
  <div class="mb-3 mt-3">
      <label for="tablename" class="form-label">Table Name In Database</label>
      <input type="text" class="form-control" id="tablename" placeholder="Database Name That was shared with Consumer" v-model="tablename" >
  </div>
  <div class="mb-3 mt-3"> 
        <button  @click="retrieveTableData()" class="btn btn-md btn-primary" >Preview Table Data</button> 
  </div>
  <div id="table-container"></div>
      
</template>
<script>
import axios from 'axios'
export default {
  data: () =>({
    databasename : null,
    tablename :  null  
  }),
  methods: {
    retrieveTableData() {
      axios.get("http://localhost:3080/api/getAthenaResults?databasename="+this.databasename+"&tablename="+this.tablename)
      .then((response)  =>  {      
              console.log(response);
              // Extract table data from API response
              const tableData = response.data.Items;

              // Define table field names
              const tableFields = Object.keys(tableData[0]);

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

              // Add table to HTML container element
              container.appendChild(table);
      },
      (error)  =>  {
        console.log(error);
      })
    }
  }
}
</script>