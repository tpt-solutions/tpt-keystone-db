import assert from "node:assert/strict";
import { test } from "node:test";
import { createServer } from "node:http";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";

import { buildArtifact, writeArtifact, publishToRegistry } from "./plugin-publish.js";

async function withTempPlugin<T>(fn: (manifestPath: string, entryPath: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "tpt-plugin-publish-"));
  try {
    const entryPath = join(dir, "index.mjs");
    await writeFile(
      entryPath,
      "export default { name: 'heatmap-overlay', setup(registry) { registry.registerComponent?.(); } };\n",
    );
    const manifestPath = join(dir, "tpt-plugin.json");
    await writeFile(
      manifestPath,
      JSON.stringify({ name: "heatmap-overlay", version: "1.0.0", description: "test plugin", entry: "./index.mjs" }),
    );
    return await fn(manifestPath, entryPath);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("buildArtifact packages a valid plugin with a matching checksum", async () => {
  await withTempPlugin(async (manifestPath, entryPath) => {
    const artifact = await buildArtifact(manifestPath);

    assert.equal(artifact.manifest.name, "heatmap-overlay");
    assert.equal(artifact.entryFilename, "index.mjs");
    assert.equal(artifact.sizeBytes > 0, true);

    const { readFile } = await import("node:fs/promises");
    const code = await readFile(entryPath);
    assert.equal(artifact.sha256, createHash("sha256").update(code).digest("hex"));
    assert.equal(Buffer.from(artifact.codeBase64, "base64").toString("utf8"), code.toString("utf8"));
  });
});

test("buildArtifact rejects an entry that doesn't export a CanvasPlugin shape", async () => {
  const dir = await mkdtemp(join(tmpdir(), "tpt-plugin-publish-bad-"));
  try {
    await writeFile(join(dir, "index.mjs"), "export const notAPlugin = 42;\n");
    const manifestPath = join(dir, "tpt-plugin.json");
    await writeFile(manifestPath, JSON.stringify({ name: "bad-plugin", version: "1.0.0", entry: "./index.mjs" }));

    await assert.rejects(buildArtifact(manifestPath), /does not export a CanvasPlugin/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("writeArtifact writes a <name>-<version>.tptplugin.json file", async () => {
  await withTempPlugin(async (manifestPath) => {
    const artifact = await buildArtifact(manifestPath);
    const outDir = await mkdtemp(join(tmpdir(), "tpt-plugin-out-"));
    try {
      const outPath = await writeArtifact(artifact, outDir);
      assert.equal(outPath.endsWith("heatmap-overlay-1.0.0.tptplugin.json"), true);

      const { readFile } = await import("node:fs/promises");
      const written = JSON.parse(await readFile(outPath, "utf8"));
      assert.equal(written.manifest.name, "heatmap-overlay");
      assert.equal(written.sha256, artifact.sha256);
    } finally {
      await rm(outDir, { recursive: true, force: true });
    }
  });
});

test("publishToRegistry POSTs the artifact and surfaces non-2xx responses as errors", async () => {
  await withTempPlugin(async (manifestPath) => {
    const artifact = await buildArtifact(manifestPath);

    let received: unknown;
    const server = createServer((req, res) => {
      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        received = JSON.parse(body);
        if (req.url === "/plugins") {
          res.writeHead(201, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: true }));
        } else {
          res.writeHead(404);
          res.end();
        }
      });
    });

    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("expected a bound TCP address");
    const baseUrl = `http://127.0.0.1:${address.port}`;

    try {
      await publishToRegistry(artifact, baseUrl);
      assert.deepEqual((received as { manifest: { name: string } }).manifest.name, "heatmap-overlay");

      await assert.rejects(publishToRegistry(artifact, `${baseUrl}/wrong-base`), /Registry publish failed: 404/);
    } finally {
      server.close();
    }
  });
});
