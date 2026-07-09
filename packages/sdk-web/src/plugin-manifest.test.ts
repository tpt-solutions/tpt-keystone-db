import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { validateManifest, loadManifest } from "./plugin-manifest.js";

test("validateManifest requires name, version, and entry", () => {
  assert.deepEqual(validateManifest({}), [
    "manifest.name is required",
    "manifest.version is required",
    "manifest.entry is required (path to the plugin's built ESM entry file)",
  ]);
});

test("validateManifest rejects a non-kebab-case name and a non-semver version", () => {
  const errors = validateManifest({ name: "Heat Map", version: "v1", entry: "./index.js" });
  assert.equal(errors.length, 2);
  assert.match(errors[0], /must be lowercase alphanumeric with hyphens/);
  assert.match(errors[1], /must be a semver string/);
});

test("validateManifest accepts a well-formed manifest", () => {
  assert.deepEqual(validateManifest({ name: "heatmap-overlay", version: "1.2.3", entry: "./dist/index.js" }), []);
});

test("loadManifest parses a valid manifest file", async () => {
  const dir = await mkdtemp(join(tmpdir(), "tpt-plugin-manifest-"));
  try {
    const path = join(dir, "tpt-plugin.json");
    await writeFile(path, JSON.stringify({ name: "heatmap-overlay", version: "1.0.0", entry: "./index.js" }));

    const manifest = await loadManifest(path);
    assert.equal(manifest.name, "heatmap-overlay");
    assert.equal(manifest.version, "1.0.0");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("loadManifest throws with the accumulated errors for an invalid manifest", async () => {
  const dir = await mkdtemp(join(tmpdir(), "tpt-plugin-manifest-"));
  try {
    const path = join(dir, "tpt-plugin.json");
    await writeFile(path, JSON.stringify({ version: "1.0.0" }));

    await assert.rejects(loadManifest(path), /manifest.name is required/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
