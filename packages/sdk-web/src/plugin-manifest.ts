// Manifest format consumed by the marketplace publishing toolchain
// (`bin/plugin-publish.ts`, TODO.md Phase 14, "Marketplace publishing
// toolchain"). Kept separate from `plugin.ts` so it has no dependency on
// browser globals and is importable from Node-only tooling.

export interface PluginManifest {
  /** Lowercase, hyphenated, globally unique within a registry (e.g. "heatmap-overlay"). */
  name: string;
  /** Semver, e.g. "1.0.0". */
  version: string;
  description?: string;
  author?: string;
  /** Path to the built ESM entry file, relative to the manifest, exporting a `CanvasPlugin` as `default` or `plugin`. */
  entry: string;
  /** Minimum `tpt-keystone` server version this plugin was built against, if known. */
  keystoneVersion?: string;
  /** `@tpt/sdk-web` version this plugin was built against, if known. */
  sdkWebVersion?: string;
  keywords?: string[];
}

const NAME_RE = /^[a-z0-9][a-z0-9-]*$/;
const SEMVER_RE = /^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/;

export function validateManifest(manifest: Partial<PluginManifest>): string[] {
  const errors: string[] = [];

  if (!manifest.name) {
    errors.push("manifest.name is required");
  } else if (!NAME_RE.test(manifest.name)) {
    errors.push(`manifest.name "${manifest.name}" must be lowercase alphanumeric with hyphens, starting with a letter or digit`);
  }

  if (!manifest.version) {
    errors.push("manifest.version is required");
  } else if (!SEMVER_RE.test(manifest.version)) {
    errors.push(`manifest.version "${manifest.version}" must be a semver string (e.g. "1.0.0")`);
  }

  if (!manifest.entry) {
    errors.push("manifest.entry is required (path to the plugin's built ESM entry file)");
  }

  return errors;
}

export async function loadManifest(path: string): Promise<PluginManifest> {
  const { readFile } = await import("node:fs/promises");
  const raw = await readFile(path, "utf8");
  const parsed = JSON.parse(raw) as Partial<PluginManifest>;
  const errors = validateManifest(parsed);
  if (errors.length > 0) {
    throw new Error(`Invalid plugin manifest at ${path}:\n  ${errors.join("\n  ")}`);
  }
  return parsed as PluginManifest;
}
