import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/jobs": "http://localhost:3000",
      "/health": "http://localhost:3000",
      "/ready": "http://localhost:3000",
      "/metrics": "http://localhost:3000",
    },
  },
});
