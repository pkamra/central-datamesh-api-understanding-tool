<template>
   <Header></Header>
   <div class="container fluid">
    <router-view />
    </div>
</template>
<script>
//importing bootstrap 5 Modules
import "bootstrap/dist/css/bootstrap.min.css";
import "bootstrap/dist/js/bootstrap.min.js";
import Header from './components/Header'
export default {
  
components: {
    Header
  }
}
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}

table {
  border-collapse: collapse;
}

td, th {
  border: 1px solid black;
  padding: 5px;
  word-wrap: break-word;
}
</style>
