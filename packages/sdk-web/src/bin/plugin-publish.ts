#!/usr/bin/env node
// Marketplace publishing toolchain for Canvas plugins (TODO.md Phase 14,
// "Marketplace publishing toolchain"). Packages a plugin manifest + its
// built ESM entry file into a self-contained `.tptplugin.json` artifact
// (manifest + base64 code + sha256 checksum) and, optionally, POSTs it to
// a registry endpoint. There is no hosted TPT plugin registry yet, so
// `--registry` is a caller-supplied URL — exercised against a local HTTP
// server in `plugin-publish.test.ts`, not a real marketplace.
//
// Usage:
//   npx tpt-plugin-publish ./tpt-plugin.json [--out dist-plugin] [--registry https://example.com]

import { createHash } from "node:crypto";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname, resolve, basename } from "node:path";
import { pathToFileURL } from "node:url";

import { loadManifest } from "../plugin-manifest.js";
import type { PluginManifest } from "../plugin-manifest.js";

export interface PublishedArtifact {
  manifest: PluginManifest;
  entryFilename: string;
  sizeBytes: number;
  sha256: string;
  createdAt: string;
  codeBase64: string;
}

/** Loads + validates the manifest, then packages its entry file into a publishable artifact. */
export async function buildArtifact(manifestPath: string): Promise<PublishedArtifact> {
  const manifest = await loadManifest(manifestPath);
  const entryPath = resolve(dirname(manifestPath), manifest.entry);
  const code = await readFile(entryPath);

  // Catches the most common packaging mistake — pointing `entry` at
  // source .ts or a file that doesn't export a CanvasPlugin shape at all —
  // before the artifact ever reaches a registry.
  const mod = (await import(pathToFileURL(entryPath).href)) as Record<string, unknown>;
  const candidate = (mod.default ?? mod.plugin) as { name?: unknown; setup?: unknown } | undefined;
  if (!candidate || typeof candidate.name !== "string" || typeof candidate.setup !== "function") {
    throw new Error(
      `${manifest.entry} does not export a CanvasPlugin as \`default\` or \`plugin\` (expected { name: string, setup(registry) })`,
    );
  }

  return {
    manifest,
    entryFilename: basename(entryPath),
    sizeBytes: code.byteLength,
    sha256: createHash("sha256").update(code).digest("hex"),
    createdAt: new Date().toISOString(),
    codeBase64: code.toString("base64"),
  };
}

export async function writeArtifact(artifact: PublishedArtifact, outDir: string): Promise<string> {
  await mkdir(outDir, { recursive: true });
  const outPath = resolve(outDir, `${artifact.manifest.name}-${artifact.manifest.version}.tptplugin.json`);
  await writeFile(outPath, JSON.stringify(artifact, null, 2));
  return outPath;
}

export async function publishToRegistry(artifact: PublishedArtifact, registryUrl: string): Promise<void> {
  const res = await fetch(`${registryUrl.replace(/\/$/, "")}/plugins`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(artifact),
  });
  if (!res.ok) {
    throw new Error(`Registry publish failed: ${res.status} ${res.statusText}`);
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const manifestPath = args[0];
  if (!manifestPath || manifestPath.startsWith("--")) {
    console.error("usage: tpt-plugin-publish <manifest.json> [--out <dir>] [--registry <url>]");
    process.exitCode = 1;
    return;
  }

  const outIdx = args.indexOf("--out");
  const outDir = resolve(outIdx >= 0 ? args[outIdx + 1] : "dist-plugin");
  const registryIdx = args.indexOf("--registry");
  const registryUrl = registryIdx >= 0 ? args[registryIdx + 1] : undefined;

  const artifact = await buildArtifact(resolve(manifestPath));
  const outPath = await writeArtifact(artifact, outDir);
  console.log(
    `Packaged ${artifact.manifest.name}@${artifact.manifest.version} -> ${outPath} ` +
      `(${artifact.sizeBytes} bytes, sha256:${artifact.sha256.slice(0, 12)}...)`,
  );

  if (registryUrl) {
    await publishToRegistry(artifact, registryUrl);
    console.log(`Published to ${registryUrl}`);
  }
}

if (import.meta.url === pathToFileURL(resolve(process.argv[1] ?? "")).href) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
