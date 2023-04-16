<template>
    <nav class="navbar navbar-expand-lg navbar-light bg-light">
    <div class="container px-4 px-lg-5">
      <router-link class="navbar-brand" to="/"></router-link>
      
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation"><span class="navbar-toggler-icon"></span></button>
        <div class="collapse navbar-collapse" id="navbarSupportedContent" style="background-color: #e3f2fd;">
            <ul class="navbar-nav me-auto mb-2 mb-lg-0 ms-lg-4">
               
                <li class="nav-item"><router-link class="nav-link active" to="/">Producer</router-link></li>
                <li class="nav-item"><router-link class="nav-link" to="/edp">EDP</router-link></li>
                <li class="nav-item"><router-link class="nav-link" to="/consumer">Consumer</router-link></li>
             
            </ul>
           
        </div>
    </div>
  </nav>
</template>