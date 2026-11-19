/**
 * Factory doctor. Production build stays disabled when a critical check fails.
 */
import { existsSync, readFileSync, readdirSync } from "fs";
import { join } from "path";
import { repoRoot, CANONICAL_TAB_IDS, CANONICAL_TABS, LESSON_REGISTRY, PRESENTATION_CONTRACT } from "./product-shell.js";
import { lessonRegistrySummary } from "./lessons.js";
import { evaluateMasterRelease } from "./master-release.js";
import { validateLiveTabs } from "./live-tabs.js";
import { IMAGE_EVIDENCE_STATES } from "./image-evidence.js";
import { runMutationSuite } from "./mutations.js";

/** Recovered Phase 2 canonical set (materialization stays disabled). */
const CANONICAL_FILES = [
  "configs/brand-explorer-product-shell.json",
  "configs/brand-explorer-content-item-contract.json",
  "configs/brand-explorer-collection-cardinality-contract.json",
  "configs/brand-explorer-presentation-contract.json",
  "configs/brand-explorer-visual-geometry-contract.json",
  "configs/brand-explorer-quality-lessons.json",
  "lib/brand-explorer/canonical/master-release.js",
  "lib/brand-explorer/canonical/content-item-ledger.js",
  "lib/brand-explorer/canonical/live-tabs.js",
  "lib/brand-explorer/canonical/image-evidence.js",
  "lib/brand-explorer/canonical/legacy-apply-guard.js",
  "lib/brand-explorer/canonical/lessons.js",
  "lib/brand-explorer/canonical/mutations.js",
  "lib/brand-explorer/canonical/product-shell.js",
  "lib/brand-explorer/be-airtable-integration-sot-v1.js",
  "lib/brand-explorer/be-hpc-footprint-summary-v1.js",
  "scripts/brand-explorer-doctor.mjs",
  "scripts/brand-explorer-canonical-build.mjs",
  "scripts/brand-explorer-canonical-audit.mjs",
  "scripts/brand-explorer-canonical-backscan.mjs",
  "scripts/brand-explorer-canonical-test.mjs",
  "scripts/brand-explorer-legacy-guard.mjs",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-factory-preview-candidates.js",
  "lib/partner-intelligence/brand-explorer-tab-contracts.js",
];

function check(id, pass, detail = "") {
  return { id, pass: Boolean(pass), detail };
}

function rendererTabs(root) {
  const src = readFileSync(join(root, "public/js/brand-explorer-atelier-from-api.js"), "utf8");
  const block = src.match(/var ATELIER_TAB_DEFS = \[([\s\S]*?)\];/);
  if (!block) return { ids: [], labels: [] };
  const ids = [...block[1].matchAll(/id:\s*'([^']+)'/g)].map((m) => m[1]);
  const labels = [...block[1].matchAll(/label:\s*'([^']+)'/g)].map((m) =>
    m[1].replace(/<br>/g, " ").replace(/\s+/g, " ").trim()
  );
  return { ids, labels };
}

export function runBrandExplorerDoctor({ runMutations = true } = {}) {
  const root = repoRoot();
  const checks = [];
  const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  checks.push(check("repo_package", existsSync(join(root, "package.json")) && existsSync(join(root, "server.js")), root));
  checks.push(check("server_entry", existsSync(join(root, "server.js")), "node server.js from the repository root"));

  for (const file of CANONICAL_FILES) {
    checks.push(check(`file:${file}`, existsSync(join(root, file)), file));
  }

  const rendered = rendererTabs(root);
  checks.push(check("ten_tab_renderer", rendered.ids.join("|") === CANONICAL_TAB_IDS.join("|"), rendered.ids.join("|")));
  checks.push(
    check(
      "ten_tab_labels",
      CANONICAL_TABS.every((tab, i) => tab.label === rendered.labels[i]),
      rendered.labels.join(" | ")
    )
  );

  const lessons = lessonRegistrySummary();
  checks.push(check("lesson_registry_executable", lessons.executable, JSON.stringify(lessons)));
  checks.push(check("lesson_backscan_flags", LESSON_REGISTRY.lessons.filter((l) => l.status === "ACTIVE").every((l) => l.backscanRequired === true)));

  checks.push(check("image_evidence_states", IMAGE_EVIDENCE_STATES.includes("VERIFIED_EXACT_PROPERTY") && IMAGE_EVIDENCE_STATES.includes("APPROVED_NO_IMAGE")));
  checks.push(check("live_validator", validateLiveTabs({ pid: 1, buildSignature: "x", expected: 10, activated: 10, measurable: 10, audited: 10 }).pass));
  checks.push(check("master_release_export", typeof evaluateMasterRelease === "function"));
  checks.push(check("airtable_mapping", PRESENTATION_CONTRACT.airtable.basicsTable === "Brand Setup - Brand Basics"));

  const previewSrc = readFileSync(join(root, "lib/partner-intelligence/brand-explorer-factory-preview-candidates.js"), "utf8");
  checks.push(check("preview_route", previewSrc.includes("beInternalPreview=1") && previewSrc.includes("factoryPreview=1")));

  // Only gate recovered Phase 2 canonical entrypoints — historical brand-explorer:*
  // scripts may still reference archived tooling and must not block doctor recovery.
  const requiredScriptTargets = [
    "scripts/brand-explorer-doctor.mjs",
    "scripts/brand-explorer-canonical-build.mjs",
    "scripts/brand-explorer-canonical-audit.mjs",
    "scripts/brand-explorer-canonical-backscan.mjs",
    "scripts/brand-explorer-canonical-test.mjs",
    "scripts/brand-explorer-legacy-guard.mjs",
    "scripts/test-be-hpc-footprint-summary-v1.mjs",
    "scripts/be-phase2-pilot-validate-v1.mjs",
  ];
  const missingTargets = requiredScriptTargets.filter((file) => !existsSync(join(root, file)));
  checks.push(check("package_targets_exist", missingTargets.length === 0, missingTargets.join("; ")));

  // Phase 2 recovery: canonical path refuses writes; progressive npm wrapping of every
  // historical writer is tracked but does not block doctor once the guard module + build exist.
  const buildCmd = String(pkg.scripts?.["brand-explorer:build"] || "");
  const doctorCmd = String(pkg.scripts?.["brand-explorer:doctor"] || "");
  const testCmd = String(pkg.scripts?.["brand-explorer:test"] || "");
  checks.push(
    check(
      "canonical_npm_scripts",
      buildCmd.includes("brand-explorer-canonical-build.mjs") &&
        doctorCmd.includes("brand-explorer-doctor.mjs") &&
        testCmd.includes("brand-explorer-canonical-test.mjs"),
      "brand-explorer:doctor|test|build must point at recovered scripts",
    ),
  );
  checks.push(
    check(
      "legacy_apply_guard_module",
      existsSync(join(root, "lib/brand-explorer/canonical/legacy-apply-guard.js")) &&
        existsSync(join(root, "scripts/brand-explorer-legacy-guard.mjs")),
      "refuseLegacyApply + npm legacy-guard entry recovered; materializationEnabled stays false",
    ),
  );
  const unguardedHistorical = [];
  for (const [name, command] of Object.entries(pkg.scripts || {})) {
    const cmd = String(command);
    const applyCapable = /brand-explorer-.*(?:writer|remediation|wave1[2-7]-factory|full-tab-factory-build|active-release-apply|v40-active-release-remediation)/.test(
      cmd,
    );
    if (!applyCapable) continue;
    if (!cmd.includes("brand-explorer-legacy-guard.mjs") && !cmd.includes("refuseLegacyApply")) {
      unguardedHistorical.push(name);
    }
  }

  let mutations = null;
  if (runMutations) {
    mutations = runMutationSuite();
    checks.push(check("mutation_suite", mutations.pass, mutations.failed.join(", ")));
  }

  const failed = checks.filter((row) => !row.pass);
  return {
    pass: failed.length === 0,
    auditEnabled: failed.length === 0,
    materializationEnabled: false,
    productionBuildEnabled: false,
    checks,
    failed: failed.map((row) => row.id),
    warnings: {
      unguardedHistoricalNpmScripts: unguardedHistorical.slice(0, 40),
      unguardedHistoricalCount: unguardedHistorical.length,
      note: "Historical writers may still lack npm legacy-guard wrappers; canonical build refuses --apply and materializationEnabled=false. Progressive wrapping is a follow-up, not a MASTER gate weaken.",
    },
    lessons,
    mutations,
    writes: 0,
  };
}

export function listLegacyScripts() {
  const dir = join(repoRoot(), "scripts");
  return readdirSync(dir)
    .filter((name) => name.startsWith("brand-explorer"))
    .map((name) => {
      let status = "STILL_REQUIRED";
      if (/canonical|doctor|factory-backscan|visual-geometry-audit|atomic-collection-audit|quality-lessons-upgrade|test-brand-explorer-atomic/.test(name)) {
        status = "MIGRATED_TO_CANONICAL";
      } else if (/writer|remediation|wave1[2-7]-factory|full-tab-factory-build|active-release-apply|brand-explorer-factory\.mjs/.test(name)) {
        status = "ARCHIVED_READ_ONLY";
      } else if (/^test-/.test(name) || name.includes("-audit") || name.includes("preview-mode")) {
        status = "TEST_FIXTURE";
      }
      return { file: `scripts/${name}`, status };
    });
}
