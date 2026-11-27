/**
 * Cumulative GDI founder-review functionality manifest (machine-readable).
 * Grows as enhancements integrate — never replace with an outdated subset.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../group-demand-intelligence/publication-package/index.js";
import { resolveDurableGdiPublicationPackageRoot } from "./canonical-port-8080-ownership-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const MANIFEST_PATH = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "gdi-cumulative-functionality-manifest-v1.json",
);

export function loadGdiCumulativeFunctionalityManifest(options = {}) {
  const p = options.manifestPath || MANIFEST_PATH;
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  return { ...raw, _path: p };
}

export function validateGdiCumulativeFunctionalityManifest(options = {}) {
  const root = options.root || ROOT;
  const manifest = options.manifest || loadGdiCumulativeFunctionalityManifest(options);
  const errors = [];
  const htmlPath = path.join(root, "public/group-demand-intelligence.html");
  if (!fs.existsSync(htmlPath)) {
    errors.push("missing public/group-demand-intelligence.html");
  } else {
    const html = fs.readFileSync(htmlPath, "utf8");
    for (const script of manifest.requiredHtmlScripts || []) {
      if (!html.includes(script)) errors.push(`html missing script ${script}`);
    }
    if (!/executive-summary-tab\.js\?v=/.test(html)) {
      errors.push("html missing executive-summary-tab cache bust");
    }
  }

  const uiPath = path.join(root, "public/js/group-demand-intelligence/dealality-gdi-ui.js");
  if (!fs.existsSync(uiPath)) errors.push("missing dealality-gdi-ui.js");
  else {
    const ui = fs.readFileSync(uiPath, "utf8");
    if (!/executive-summary/.test(ui)) errors.push("ui missing executive-summary tab id");
    if (!/GDI_CUSTOMER_TABS|GDI_EXEC_SUMMARY_TAB/.test(ui)) {
      errors.push("ui missing four-tab / exec summary registry markers");
    }
  }

  for (const rel of [
    "public/js/group-demand-intelligence/executive-summary-tab.js",
    "public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js",
  ]) {
    if (!fs.existsSync(path.join(root, rel))) errors.push(`missing ${rel}`);
  }

  const durable = resolveDurableGdiPublicationPackageRoot({
    registryRoot: root,
    env: options.env || process.env,
  });
  if (!durable.ok) {
    errors.push(`durable package root unavailable: ${durable.error}`);
  } else {
    const store = createGdiPublicationPackageStore({ root: durable.root });
    const staged = store.readPointer(manifest.wyndhamHotelId, "STAGED");
    const current = store.readPointer(manifest.wyndhamHotelId, "CURRENT");
    const ver = String(staged?.publicationVersion || "");
    if (!ver) errors.push("Wyndham STAGED pointer missing");
    if (
      manifest.minStagedPublicationVersionPrefix &&
      !ver.startsWith(manifest.minStagedPublicationVersionPrefix)
    ) {
      errors.push(`STAGED version ${ver} below required prefix`);
    }
    for (const bad of manifest.forbiddenStagedPublicationSubstrings || []) {
      if (ver.includes(bad)) errors.push(`STAGED version contains forbidden ${bad}`);
    }
    if (current?.publicationVersion) {
      errors.push(
        `Wyndham CURRENT unexpectedly set (${current.publicationVersion}) — customer isolation breach`,
      );
    }
  }

  if (!Array.isArray(manifest.capabilities) || manifest.capabilities.length < 15) {
    errors.push("manifest capabilities too short — outdated baseline refused");
  }

  return {
    ok: errors.length === 0,
    errors,
    manifest,
    contract: manifest.contract,
  };
}
