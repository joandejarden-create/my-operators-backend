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
  "scripts/brand-explorer-doctor.mjs",
  "scripts/brand-explorer-canonical-build.mjs",
  "scripts/brand-explorer-canonical-audit.mjs",
  "scripts/brand-explorer-canonical-backscan.mjs",
  "scripts/brand-explorer-canonical-test.mjs",
  "scripts/brand-explorer-factory-backscan.mjs",
  "scripts/brand-explorer-visual-geometry-audit.mjs",
  "scripts/brand-explorer-atomic-collection-audit.mjs",
  "scripts/brand-explorer-quality-lessons-upgrade.mjs",
  "scripts/test-brand-explorer-atomic-collection.mjs",
  "scripts/brand-explorer-atomic-collection-remediation-apply.mjs",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-factory-preview-candidates.js",
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

  const missingTargets = [];
  for (const [name, command] of Object.entries(pkg.scripts || {})) {
    if (!String(name).includes("brand-explorer") && !String(command).includes("brand-explorer")) continue;
    const files = [...String(command).matchAll(/scripts\/[A-Za-z0-9_./-]+\.mjs/g)].map((m) => m[0]);
    for (const file of files) {
      if (!existsSync(join(root, file))) missingTargets.push(`${name} -> ${file}`);
    }
  }
  checks.push(check("package_targets_exist", missingTargets.length === 0, missingTargets.join("; ")));

  const unguarded = [];
  for (const [name, command] of Object.entries(pkg.scripts || {})) {
    const cmd = String(command);
    const applyCapable = /brand-explorer-.*(?:writer|remediation|wave1[2-7]-factory|full-tab-factory-build|active-release-apply|v40-active-release-remediation)/.test(cmd);
    if (!applyCapable) continue;
    if (!cmd.includes("brand-explorer-legacy-guard.mjs") && !cmd.includes("refuseLegacyApply")) {
      unguarded.push(name);
    }
  }
  checks.push(check("legacy_npm_apply_guarded", unguarded.length === 0, unguarded.slice(0, 12).join(", ")));

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
