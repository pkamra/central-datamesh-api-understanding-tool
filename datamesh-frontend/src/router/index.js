import { createWebHistory, createRouter } from "vue-router";
import Producer from "@/views/Producer.vue";
import EDP from "@/views/EDP.vue";
import Consumer from "@/views/Consumer.vue";
const routes = [
  {
    path: "/",
    name: "Producer",
    component: Producer,
  },
  {
    path: "/edp",
    name: "EDP",
    component: EDP,
  },
  {
    path: "/consumer",
    name: "Consumer",
    component: Consumer,
  },
];
const router = createRouter({
  history: createWebHistory(),
  routes,
});
export default router;