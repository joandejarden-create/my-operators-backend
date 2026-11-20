/**
 * BE_LOCAL_PRODUCT_PARITY_GATE — permanent pre-publication quality gate.
 *
 * Before any Brand Explorer production Airtable writeback or customer promotion,
 * the exact approved immutable candidate must pass full content / presentation /
 * functional parity between:
 *   A) Brand Explorer development preview
 *   B) Canonical local Dealality application
 *
 * HTTP 200, matching Git commits alone, or unit-test PASS are not sufficient.
 * Evidence must be tied to candidateHash + applicationCommit and retained in
 * the publication audit store.
 *
 * Contract: BE_LOCAL_PRODUCT_PARITY_GATE_V1
 */
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { BE_CANONICAL_TAB_IDS } from "./be-airtable-integration-sot-v1.js";

export const BE_LOCAL_PRODUCT_PARITY_GATE = "BE_LOCAL_PRODUCT_PARITY_GATE";
export const BE_LOCAL_PRODUCT_PARITY_GATE_V1 = "BE_LOCAL_PRODUCT_PARITY_GATE_V1";

export const PARITY_STATUSES = Object.freeze({
  PASS: "PASS",
  BLOCK: "BLOCK",
});

/** Default max age for a reusable parity report (6 hours). */
export const DEFAULT_PARITY_REPORT_MAX_AGE_MS = 6 * 60 * 60 * 1000;

export const REQUIRED_ASSET_PATHS = Object.freeze([
  "/js/brand-explorer-atelier-from-api.js",
  "/css/brand-explorer-atelier-oe-panels.css",
  "/css/brand-explorer-gold-surface.css",
  "/brand-explorer-combined.html",
  "/brand-explorer-renderer-parity.html",
]);

export const REQUIRED_FUNCTIONAL_CHECKS = Object.freeze([
  "ten_tab_navigation",
  "view_recent_openings_jump",
  "footprint_region_country_archetype_toggles",
  "responsive_layout_no_clip",
  "compare_studio_accessible",
]);

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..");

export function parityAuditDir(root = REPO_ROOT) {
  return join(root, "reports", "brand-explorer", "comparison-studio", "local-product-parity");
}

/** Same normalization as founder publication lock hash (avoid circular imports). */
export function hashCandidateBlocks(blocks) {
  const normalized = (Array.isArray(blocks) ? blocks : [])
    .map((b) => ({
      slotKey: String(b.slotKey || "").trim(),
      title: String(b.title || "").trim(),
      body: String(b.body || "").trim(),
      sort: typeof b.sort === "number" ? b.sort : Number(b.sort) || 0,
      imageUrl: b.imageUrl || null,
      active: b.active === false ? false : true,
    }))
    .filter((b) => b.slotKey)
    .sort((a, b) => a.slotKey.localeCompare(b.slotKey) || a.sort - b.sort);
  return createHash("sha256").update(JSON.stringify(normalized)).digest("hex");
}

function pushFail(failures, code, message, extra = {}) {
  failures.push({ code, message, ...extra });
}

function normalizeTabId(id) {
  return String(id || "").trim();
}

function isApprovedException(exceptions, code) {
  const list = Array.isArray(exceptions) ? exceptions : [];
  return list.some((e) => e && (e.code === code || e === code) && e.founderApproved !== false);
}

/**
 * Build a machine-readable parity evidence object (PASS or BLOCK).
 * Used by live runners and defective/valid fixtures.
 */
export function buildLocalProductParityEvidence(input = {}) {
  const brandId = String(input.brandId || "").trim();
  const candidateHash = String(input.candidateHash || "").trim();
  const applicationCommit = String(input.applicationCommit || "").trim();
  const previewCommit = String(input.previewCommit || applicationCommit || "").trim();
  const validatedAt = input.validatedAt || new Date().toISOString();
  const tabs = input.tabs || {};
  const assets = input.assets || {};
  const functional = input.functional || {};
  const content = input.content || {};
  const visual = input.visual || {};
  const approvedExceptions = input.approvedExceptions || [];
  const renderer = input.renderer || {
    module: "public/js/brand-explorer-atelier-from-api.js",
    productMount: "BrandExplorerAtelierFromApi",
  };

  const failures = [];
  const tabResults = {};

  for (const tabId of BE_CANONICAL_TAB_IDS) {
    const row = tabs[tabId] || null;
    const tabFail = [];
    if (!row) tabFail.push("TAB_MISSING");
    else {
      if (row.pass === false) tabFail.push(...(row.failures || ["TAB_FAIL"]));
      if (row.missingSubsection) tabFail.push("REQUIRED_SUBSECTION_MISSING");
      if (row.contentLost) tabFail.push("CONTENT_LOST");
      if (row.cardsDiffer) tabFail.push("CARDS_DIFFER");
      if (row.imagesDiffer || row.missingImages) tabFail.push("IMAGES_DIFFER_OR_MISSING");
      if (row.tablesDiffer) tabFail.push("TABLES_DIFFER");
      if (row.kpisDiffer || row.invalidKpis) tabFail.push("KPIS_DIFFER_OR_INVALID");
      if (row.styleRegression) tabFail.push("STYLE_REGRESSION");
      if (row.navigationFailed) tabFail.push("NAVIGATION_FAILED");
    }
    const pass = tabFail.length === 0;
    tabResults[tabId] = {
      tabId,
      pass,
      failures: tabFail,
      ...(row || {}),
    };
    if (!pass) {
      for (const code of tabFail) {
        if (!isApprovedException(approvedExceptions, code)) {
          pushFail(failures, `TAB_${tabId}_${code}`, `Tab ${tabId}: ${code}`);
        }
      }
    }
  }

  const presentTabIds = Object.keys(tabResults).filter((id) => tabResults[id].pass || tabs[id]);
  if (presentTabIds.length < BE_CANONICAL_TAB_IDS.length) {
    pushFail(failures, "CANONICAL_TABS_INCOMPLETE", "Not all ten canonical tabs present in parity evidence.");
  }

  for (const assetPath of REQUIRED_ASSET_PATHS) {
    const row = assets[assetPath];
    if (!row || row.match !== true) {
      if (!isApprovedException(approvedExceptions, "ASSET_MISMATCH")) {
        pushFail(failures, "OUTDATED_OR_MISMATCHED_ASSETS", `Asset parity failed for ${assetPath}`, {
          assetPath,
        });
      }
    }
  }

  if (applicationCommit && previewCommit && applicationCommit !== previewCommit) {
    if (!isApprovedException(approvedExceptions, "COMMIT_MISMATCH")) {
      pushFail(failures, "PREVIEW_APP_COMMIT_MISMATCH", "Preview and local app commits differ.", {
        applicationCommit,
        previewCommit,
      });
    }
  }

  if (content.payloadVersionsMatch === false) {
    pushFail(failures, "PAYLOAD_VERSION_MISMATCH", "Preview and local application used different payload versions.");
  }
  if (content.materialContentLost) {
    pushFail(failures, "MATERIAL_CONTENT_LOST", "Material content was lost between environments.");
  }
  if (content.alteredUnexpectedly) {
    pushFail(failures, "CONTENT_ALTERED", "Content differed unexpectedly for identical candidate.");
  }
  if (content.titleCaseOk === false) {
    pushFail(failures, "TITLE_CASE_REGRESSION", "Title Case / heading editorial rules failed.");
  }
  if (content.ampersandOk === false) {
    pushFail(failures, "AMPERSAND_REGRESSION", "Heading ampersand conventions failed.");
  }
  if (content.numericFieldRolesOk === false) {
    pushFail(failures, "NUMERIC_FIELD_ROLE_REGRESSION", "Numeric field semantic roles failed.");
  }
  if (content.hpcAirtableDivergenceUnreconciled) {
    pushFail(
      failures,
      "HPC_AIRTABLE_DIVERGENCE_UNRECONCILED",
      "HPC/Airtable source differences were not identified and reconciled.",
    );
  }

  if (visual.materialStyleRegression) {
    pushFail(failures, "STYLE_LAYOUT_REGRESSION", "Styling or layout materially regressed.");
  }
  if (visual.screenshotsMissing && !isApprovedException(approvedExceptions, "SCREENSHOTS_OPTIONAL")) {
    pushFail(failures, "SCREENSHOTS_MISSING", "Matching-viewport screenshots are required evidence.");
  }

  for (const check of REQUIRED_FUNCTIONAL_CHECKS) {
    const row = functional[check];
    if (!row || row.pass !== true) {
      if (!isApprovedException(approvedExceptions, check)) {
        pushFail(failures, `FUNCTIONAL_${check.toUpperCase()}`, `Functional check failed: ${check}`);
      }
    }
  }

  if (!brandId) pushFail(failures, "BRAND_ID_REQUIRED", "Brand ID is required.");
  if (!candidateHash || candidateHash.length < 16) {
    pushFail(failures, "CANDIDATE_HASH_REQUIRED", "Approved candidate hash is required.");
  }
  if (!applicationCommit) {
    pushFail(failures, "APPLICATION_COMMIT_REQUIRED", "Application commit is required on the parity report.");
  }

  const status = failures.length === 0 ? PARITY_STATUSES.PASS : PARITY_STATUSES.BLOCK;
  return {
    contract: BE_LOCAL_PRODUCT_PARITY_GATE_V1,
    gate: BE_LOCAL_PRODUCT_PARITY_GATE,
    status,
    ok: status === PARITY_STATUSES.PASS,
    brandId,
    candidateHash,
    applicationCommit,
    previewCommit,
    validatedAt,
    renderer,
    assets,
    assetVersions: assets,
    tabs: tabResults,
    content,
    visual,
    functional,
    approvedExceptions,
    failures,
    tenTabPassCount: BE_CANONICAL_TAB_IDS.filter((id) => tabResults[id]?.pass).length,
    tenTabRequired: BE_CANONICAL_TAB_IDS.length,
    note:
      "Publication must fail closed unless status=PASS for the exact approved candidate hash on the intended application commit.",
  };
}

/**
 * Validate an existing report against the candidate about to publish.
 * Fail closed on stale / missing / wrong-hash / BLOCK reports.
 */
export function evaluateLocalProductParityGate(input = {}) {
  const failures = [];
  const expectedHash = String(input.expectedCandidateHash || input.candidateHash || "").trim();
  const expectedCommit = String(input.expectedApplicationCommit || input.applicationCommit || "").trim();
  const maxAgeMs =
    typeof input.maxAgeMs === "number" ? input.maxAgeMs : DEFAULT_PARITY_REPORT_MAX_AGE_MS;
  const nowMs = input.nowMs != null ? Number(input.nowMs) : Date.now();

  let report = input.report || input.localProductParity || null;
  if (!report && input.reportPath) {
    const path = input.reportPath;
    if (!existsSync(path)) {
      pushFail(failures, "PARITY_REPORT_MISSING", `Parity report file not found: ${path}`);
    } else {
      report = JSON.parse(readFileSync(path, "utf8"));
    }
  }

  if (!report) {
    // Allow synthesizing from comparison fragments (fixtures / live adapter)
    if (input.comparisons || input.tabs) {
      report = buildLocalProductParityEvidence(input);
    } else {
      pushFail(
        failures,
        "LOCAL_PRODUCT_PARITY_REQUIRED",
        "BE_LOCAL_PRODUCT_PARITY_GATE evidence is required before publication.",
      );
      return {
        step: "LOCAL_PRODUCT_PARITY",
        gate: BE_LOCAL_PRODUCT_PARITY_GATE,
        ok: false,
        status: PARITY_STATUSES.BLOCK,
        failures,
        report: null,
      };
    }
  }

  if (report.contract && report.contract !== BE_LOCAL_PRODUCT_PARITY_GATE_V1) {
    pushFail(failures, "PARITY_CONTRACT_MISMATCH", "Parity report contract version mismatch.", {
      observed: report.contract,
    });
  }

  if (report.status !== PARITY_STATUSES.PASS || report.ok === false) {
    pushFail(failures, "PARITY_STATUS_BLOCK", "Parity report status is not PASS.", {
      observed: report.status,
      nestedFailures: report.failures || [],
    });
  }

  if (expectedHash && report.candidateHash && report.candidateHash !== expectedHash) {
    pushFail(
      failures,
      "PARITY_REPORT_WRONG_CANDIDATE_HASH",
      "Parity report belongs to a different candidate hash.",
      { expectedHash, observed: report.candidateHash },
    );
  }
  if (expectedHash && !report.candidateHash) {
    pushFail(failures, "PARITY_REPORT_MISSING_CANDIDATE_HASH", "Parity report missing candidateHash.");
  }

  if (expectedCommit && report.applicationCommit && report.applicationCommit !== expectedCommit) {
    pushFail(
      failures,
      "PARITY_REPORT_WRONG_APPLICATION_COMMIT",
      "Parity report application commit does not match the publishing application tip.",
      { expectedCommit, observed: report.applicationCommit },
    );
  }

  if (report.validatedAt) {
    const age = nowMs - Date.parse(report.validatedAt);
    if (!Number.isFinite(age) || age < 0 || age > maxAgeMs) {
      pushFail(failures, "PARITY_REPORT_STALE", "Parity report is stale or has an invalid timestamp.", {
        validatedAt: report.validatedAt,
        maxAgeMs,
        ageMs: age,
      });
    }
  } else {
    pushFail(failures, "PARITY_REPORT_MISSING_TIMESTAMP", "Parity report missing validatedAt.");
  }

  if (Number(report.tenTabPassCount || 0) < Number(report.tenTabRequired || BE_CANONICAL_TAB_IDS.length)) {
    pushFail(failures, "TEN_TAB_PARITY_INCOMPLETE", "Not all ten tabs passed parity.");
  }

  // Re-evaluate nested evidence for structural integrity (fixtures may set status PASS incorrectly)
  if (input.reevaluateEvidence !== false && (report.tabs || report.assets || report.assetVersions || report.functional)) {
    const rebuilt = buildLocalProductParityEvidence({
      ...report,
      brandId: report.brandId || input.brandId,
      candidateHash: report.candidateHash || expectedHash,
      applicationCommit: report.applicationCommit || expectedCommit,
      previewCommit: report.previewCommit,
      tabs: report.tabs,
      assets: report.assets || report.assetVersions,
      functional: report.functional,
      content: report.content,
      visual: report.visual,
      approvedExceptions: report.approvedExceptions || input.approvedExceptions,
      validatedAt: report.validatedAt,
      renderer: report.renderer,
    });
    if (!rebuilt.ok) {
      for (const f of rebuilt.failures) {
        if (!failures.some((x) => x.code === f.code && x.message === f.message)) {
          failures.push(f);
        }
      }
    }
  }

  const ok = failures.length === 0;
  return {
    step: "LOCAL_PRODUCT_PARITY",
    gate: BE_LOCAL_PRODUCT_PARITY_GATE,
    ok,
    status: ok ? PARITY_STATUSES.PASS : PARITY_STATUSES.BLOCK,
    failures,
    report,
    expectedCandidateHash: expectedHash || null,
    expectedApplicationCommit: expectedCommit || null,
  };
}

/**
 * Persist parity evidence into the publication audit store.
 */
export function persistLocalProductParityEvidence(input = {}) {
  const root = input.root || REPO_ROOT;
  const evidence = input.evidence || input.report;
  if (!evidence) throw new Error("persistLocalProductParityEvidence requires evidence");
  const dir = parityAuditDir(root);
  mkdirSync(dir, { recursive: true });
  const brandId = String(evidence.brandId || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
  const hash = String(evidence.candidateHash || "nohash").slice(0, 16);
  const stamp = String(evidence.validatedAt || new Date().toISOString()).replace(/[:.]/g, "-");
  const file = `${brandId}-${evidence.status || "UNKNOWN"}-${hash}-${stamp}.json`;
  const path = join(dir, file);
  writeFileSync(path, JSON.stringify(evidence, null, 2));
  return {
    ok: true,
    path,
    relativePath: `reports/brand-explorer/comparison-studio/local-product-parity/${file}`,
  };
}

/** Helper: build a valid PASS evidence template for tests (ten tabs + assets + functional). */
export function buildValidParityEvidenceFixture(overrides = {}) {
  const tabs = {};
  for (const id of BE_CANONICAL_TAB_IDS) {
    tabs[id] = { pass: true, failures: [] };
  }
  const assets = {};
  for (const p of REQUIRED_ASSET_PATHS) {
    assets[p] = { match: true, appSha: "a".repeat(64), previewSha: "a".repeat(64) };
  }
  const functional = {};
  for (const c of REQUIRED_FUNCTIONAL_CHECKS) {
    functional[c] = { pass: true };
  }
  return buildLocalProductParityEvidence({
    brandId: overrides.brandId || "receQkxgjlezsc1xg",
    candidateHash: overrides.candidateHash || "c".repeat(64),
    applicationCommit: overrides.applicationCommit || "d".repeat(40),
    previewCommit: overrides.previewCommit || overrides.applicationCommit || "d".repeat(40),
    validatedAt: overrides.validatedAt || new Date().toISOString(),
    tabs: overrides.tabs || tabs,
    assets: overrides.assets || assets,
    functional: overrides.functional || functional,
    content: {
      payloadVersionsMatch: true,
      materialContentLost: false,
      alteredUnexpectedly: false,
      titleCaseOk: true,
      ampersandOk: true,
      numericFieldRolesOk: true,
      hpcAirtableDivergenceUnreconciled: false,
      ...(overrides.content || {}),
    },
    visual: {
      materialStyleRegression: false,
      screenshotsMissing: false,
      ...(overrides.visual || {}),
    },
    approvedExceptions: overrides.approvedExceptions || [],
    renderer: overrides.renderer,
  });
}

export function sha256Json(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}
