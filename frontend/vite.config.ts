import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// https://vite.dev/config/
export default defineConfig({
  cacheDir: "node_modules/.vite",
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api": {
        target: "https://actorgraph-production.up.railway.app",
        changeOrigin: true,
      },
    },
  },
});
