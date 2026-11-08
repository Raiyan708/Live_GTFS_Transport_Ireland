import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: the browser talks to Vite (5173); /api is proxied to FastAPI (8000), so no CORS setup is needed.
export default defineConfig({
  // GitHub Pages serves the demo from /<repo>/, so the build takes its base path from VITE_BASE (default "/").
  base: process.env.VITE_BASE ?? "/",
  plugins: [react()],
  server: { port: 5173, proxy: { "/api": { target: "http://localhost:8000", changeOrigin: false } } },
  test: { environment: "node" },
});
