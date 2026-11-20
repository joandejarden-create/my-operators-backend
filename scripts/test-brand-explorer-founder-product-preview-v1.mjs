#!/usr/bin/env node
/**
 * Founder product preview: Current Saved / Proposed Update wiring + registry parity.
 *
 *   node scripts/test-brand-explorer-founder-product-preview-v1.mjs
 *   npm run test:brand-explorer-founder-product-preview-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

function loadVm(rel, sandbox) {
  vm.runInContext(read(rel), sandbox, { filename: rel });
}

const sandbox = {
  URLSearchParams,
  console,
  window: {},
  globalThis: {},
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
vm.createContext(sandbox);

loadVm("public/js/brand-explorer-route-state.js", sandbox);
loadVm("public/js/brand-explorer-staged-candidate-registry.js", sandbox);

const RS = sandbox.BrandExplorerRouteState;
const Reg = sandbox.BrandExplorerStagedCandidateRegistry;
assert.ok(RS);
assert.ok(Reg);

const curio = Reg.byId("receQkxgjlezsc1xg");
const ascend = Reg.byId("reclkgOzvAcBheUSo");
assert.ok(curio);
assert.ok(ascend);
assert.match(curio.overlayUrl, /phase2h-candidate\.json$/);
assert.match(ascend.overlayUrl, /phase2h-candidate\.json$/);
assert.equal(curio.candidateVersion, "phase2h-component-parity");
assert.equal(ascend.founderApprovalStatus, "NOT_APPROVED");

assert.equal(
  Reg.resolveForBrand({ id: "receQkxgjlezsc1xg" }).overlayUrl,
  curio.overlayUrl
);
assert.equal(Reg.resolveForBrand({ name: "Ascend Hotel Collection" }).id, ascend.id);

for (const c of [curio, ascend]) {
  const abs = path.join(root, "public", c.overlayUrl.replace(/^\//, ""));
  assert.ok(fs.existsSync(abs), `missing overlay ${c.overlayUrl}`);
  // Refuse silent older candidates as primary overlay paths.
  assert.doesNotMatch(c.overlayUrl, /phase2[dfg]-/i);
  assert.doesNotMatch(c.overlayUrl, /curio-candidate\.json$/);
}

const combined = read("public/brand-explorer-combined.html");
assert.match(combined, /brand-explorer-route-state\.js/);
assert.match(combined, /brand-explorer-cumulative-proposal-assembler\.js/);
assert.match(combined, /brand-explorer-staged-candidate-registry\.js/);
assert.match(combined, /brand-explorer-founder-profile-mode\.js/);
assert.match(combined, /brand-explorer-founder-profile-mode\.css/);

const atelier = read("public/js/brand-explorer-atelier-from-api.js");
assert.match(atelier, /buildCumulativeProposedBrandAsync/);
assert.match(atelier, /isProposedProfileModeRequest/);
assert.match(atelier, /getProfileMode/);
assert.match(atelier, /Silent Phase 2F\/2G\/2D fallback is forbidden/);
assert.doesNotMatch(
  atelier.slice(atelier.indexOf("function candidateOverlayUrl"), atelier.indexOf("function applyCandidateOverlayToBrand")),
  /phase2d-preview\/curio-candidate\.json/
);

const appJs = read("public/app.js");
assert.match(appJs, /beProfileMode/);
assert.match(appJs, /beCandidateOverlay/);
assert.match(appJs, /beInternalPreview/);
assert.match(appJs, /founderProductPreview:\s*true/);
assert.match(appJs, /shouldEnableBeFounderProductPreview/);
assert.match(appJs, /mapped\.founderProductPreview/);
assert.match(appJs, /founderPreviewInjected/);
assert.match(appJs, /expectedBePreview/);
assert.match(
  appJs,
  /\/brand-explorer-combined'[\s\S]*?founderProductPreview:\s*true/
);

const founderMode = read("public/js/brand-explorer-founder-profile-mode.js");
assert.match(founderMode, /CURRENT SAVED/);
assert.match(founderMode, /PROPOSED UPDATE/);
assert.match(founderMode, /app#\/brand-explorer-combined/);

const compare = read("public/brand-explorer-compare.html");
assert.match(compare, /brand-explorer-staged-candidate-registry\.js/);

const st = RS.getBrandExplorerRouteState({
  location: {
    pathname: "/brand-explorer-combined.html",
    search: "?embed=1&appShell=1&id=receQkxgjlezsc1xg",
    hash: "",
  },
  parentLocation: {
    pathname: "/app",
    search: "",
    hash: "#/brand-explorer-combined?id=receQkxgjlezsc1xg&beInternalPreview=1&beProfileMode=proposed",
  },
});
assert.equal(st.isProposedProfileMode, true);
assert.equal(st.beProfileMode, "proposed");

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "BE_FOUNDER_PRODUCT_PREVIEW_V1",
      brands: [curio.slug, ascend.slug],
      ready: "founder_product_preview_wiring_pass",
    },
    null,
    2
  )
);
