import { defineConfig } from "vite";

// The WASM module is produced by `npm run build:wasm` (wasm-bindgen --target web)
// into ./pkg. Vite serves it as a native ES module; no custom plugin needed.
//
// Two ways to run the demo:
//   npm run dev          -> points at a REAL tpt-keystone node (default 5435),
//                          override with TPT_HTTP_ADDR_BASE / TPT_FLUX_WS_ADDR.
//   npm run dev:mock     -> points at `npm run mock` (a zero-dep seeded
//                          Canvas HTTP bridge on :5435) so the dashboard renders
//                          with sample data and no database required.
export default defineConfig(({ mode }) => {
  const isMock = mode === "mock";
  return {
    server: {
      port: 5173,
      // The dashboard fetches SQL from the running tpt-keystone Canvas HTTP
      // endpoint (default TPT_HTTP_ADDR = 5435). Allow that origin to be
      // overridden for remote nodes.
      envPrefix: ["TPT_"],
    },
    // In mock mode, default the base URLs to the local mock server.
    define: isMock
      ? {
          "import.meta.env.TPT_HTTP_ADDR_BASE": JSON.stringify("http://localhost:5435"),
          "import.meta.env.TPT_FLUX_WS_ADDR": JSON.stringify("ws://localhost:5435"),
        }
      : {},
    build: {
      target: "es2020",
      outDir: "dist",
    },
  };
});
