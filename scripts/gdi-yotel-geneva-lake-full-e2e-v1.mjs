#!/usr/bin/env node
/**
 * GDI YOTEL Geneva Lake — certified Full E2E (ZERO PAID — no Exa, no Surfe, no Parallel, no provider calls).
 *
 * Reuses research that is already on disk (legacy opportunities.json + stored deep-run artifacts) and
 * re-audits every candidate through the shared commercial gates. Quality over count: a card is shown to
 * customers only when it passes the gates; everything else is preserved in a research-continuation ledger.
 *
 * Modes
 *   default           recover + audit + build + stage (new publication version) + reports
 *   --no-stage        same, but do NOT write the package / pointer / opportunities.json
 *   --browser-verify  verify the staged package against the local server (default http://127.0.0.1:8080)
 *   --base-url <u>    override preview base for --browser-verify
 *   --ref YYYY-MM-DD  reference date (default 2026-11-16)
 *   --skip-public-checks  skip the free public-web recheck (offline run)
 *
 * Hard rules honored here
 *   - never imports/calls Exa, Surfe, Parallel or any paid client (asserted statically at runtime)
 *   - Bethesda / Sheraton / Wyndham / Radisson publication pointers + packages are snapshotted before/after
 *   - never CUSTOMER_PUBLISHED; Airtable is dry-run only
 *   - Sales Ready only when commercial-eligibility-contract-v1 genuinely passes
 *   - provider names / costs stay out of customer fields
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  computePublicationInventoryCounts,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";
import { planCustomerTileBadges } from "../lib/group-demand-intelligence/customer-classification-presentation-v1.js";
import {
  resolveCustomerSalesAction,
  CUSTOMER_SALES_ACTION,
  CUSTOMER_SALES_ACTION_LABEL,
} from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import {
  INVENTORY_POPULATION,
  annotateInventoryPopulations,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import {
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
  classifyCommercialActionability,
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import {
  annotateCanonicalCmiContract,
  evaluateCmiQualificationGates,
  evaluateCmiDiscoveryCoverage,
} from "../lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import {
  COMMERCIAL_ACTIONABILITY,
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  recommendCommercialAction,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";
import { CMI_CLAIM_TYPE } from "../lib/group-demand-intelligence/commercial-consistency/cmi-evidence-ledger-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { evaluateProactiveAccountDevelopment } from "../lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js";
import { formatTimingDisplayLabel } from "../lib/group-demand-intelligence/universal-timing-intelligence-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  TEMPLATE_E2E_STAGES,
  HOTEL_CONFIGURATION_REQUIRED_KEYS,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";

// ---------------------------------------------------------------------------
// constants
// ---------------------------------------------------------------------------
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SCRIPT_ID = "gdi-yotel-geneva-lake-full-e2e-v1";
const SCRIPT_PATH = fileURLToPath(import.meta.url);
const CONTRACT_ID = "GDI_YOTEL_GENEVA_LAKE_FULL_E2E_V1";
const HOTEL_ID = "recrPQcZg7SFARRb2";
const HOTEL_NAME = "YOTEL Geneva Lake";
const ARGV = process.argv.slice(2);
const has = (f) => ARGV.includes(f);
const argVal = (n) => {
  const i = ARGV.indexOf(n);
  return i > -1 ? ARGV[i + 1] : null;
};
const REF = argVal("--ref") || "2026-11-16";
const BROWSER_VERIFY = has("--browser-verify");
const NO_STAGE = has("--no-stage");
const SKIP_PUBLIC_CHECKS = has("--skip-public-checks");
const VERSION_SUFFIX = argVal("--version-suffix") || "";
const NEW_PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261116_yotel_geneva_lake_full_e2e_v1${VERSION_SUFFIX}`;
const IMMINENT_DAYS = 21;
const TARGET_HM = 20;
const DEEP_RUN_UNIT_USD = 0.5; // recorded unit cost (bounded quote, stored in every deep artifact)

const OUT = path.join(ROOT, "reports", SCRIPT_ID);
const HOTEL_DATA = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
const OPPS_JSON_PATH = path.join(HOTEL_DATA, "opportunities.json");
const PROTECTED_STORE_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
const PROTECTED_HOTELS = [
  ["recLuxvwwxID7U2B8", "bethesda"],
  ["recCAGqkERSXgiku4", "sheraton"],
  ["recXh4NgF8sDbi9cQ", "wyndham_alltra"],
  ["recH9h3McgMNFQEla", "radisson_toronto"],
];
const STATE_PATH = path.join(OUT, "_run-state.json");
const ARTIFACT_ROOTS = [
  "reports/gdi-yotel-geneva-lake-v21-fourteen-workflow-live-execution",
  "reports/gdi-yotel-v23-discovery-completeness-live-execution",
  "reports/gdi-v26-two-hotel-clean-room-full-e2e/live-execution/yotel",
  "reports/gdi-v28-two-hotel-incremental-yield-live/yotel",
];
const LOG_LEAD_FILES = [
  "reports/gdi-v28-two-hotel-incremental-yield-live/CONSOLE.log",
  "reports/gdi-v26-two-hotel-clean-room-full-e2e/live-execution/LIVE_CONSOLE_RESUME.log",
];

const DISP = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  QUALIFY_VISIBLE: "QUALIFY_VISIBLE",
  RESEARCH_CONTINUATION_HIDDEN: "RESEARCH_CONTINUATION_HIDDEN",
  DEMOTED_NOT_CURRENTLY_ACTIONABLE: "DEMOTED_NOT_CURRENTLY_ACTIONABLE",
  CMI_RETAINED: "CMI_RETAINED",
  CMI_DEMOTED: "CMI_DEMOTED",
  REJECTED_SYNTHETIC_CAMPAIGN_SHELL: "REJECTED_SYNTHETIC_CAMPAIGN_SHELL",
  REJECTED_VENUE_OPERATOR_OR_ORGANIZER_WRAPPER: "REJECTED_VENUE_OPERATOR_OR_ORGANIZER_WRAPPER",
  REJECTED_NON_EVENT_WEB_LISTING: "REJECTED_NON_EVENT_WEB_LISTING",
  RECOVERY_LEAD_NO_LOCAL_EVIDENCE: "RECOVERY_LEAD_NO_LOCAL_EVIDENCE",
});
const CUSTOMER_DISPOSITIONS = [DISP.SALES_READY, DISP.ACCOUNT_DEVELOPMENT, DISP.QUALIFY_VISIBLE];

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
const text = (v) => String(v == null ? "" : v).replace(/\s+/g, " ").trim();
const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, ""));
const cloneOpp = (o) => JSON.parse(JSON.stringify(o));
const sha256File = (p) => (fs.existsSync(p) ? crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex") : null);
const rel = (p) => path.relative(ROOT, p).replace(/\\/g, "/");
const normKey = (s) =>
  text(s).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]+/g, " ").trim();
const countBy = (arr, fn) => {
  const m = {};
  for (const x of arr) {
    const k = fn(x) ?? "(none)";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
};
const addDays = (d, n) => new Date(Date.parse(`${d}T12:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
const daysBetween = (a, b) => Math.round((Date.parse(`${b}T12:00:00Z`) - Date.parse(`${a}T12:00:00Z`)) / 86400000);
const parseIso = (s) => {
  const m = text(s).match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
};
const IMMINENT_CUTOFF = addDays(REF, IMMINENT_DAYS);
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    return "";
  }
}
const dateLabelFor = (start, end) => formatTimingDisplayLabel({ timingType: "CONTINUOUS_RANGE", startDate: start, endDate: end || start });
function sanitizeCustomerCopy(v, key = "") {
  if (typeof v === "string") {
    if (/url|href|^id$|orsId|fingerprint|^email$|Email$/i.test(key)) return v;
    return v.replace(/\bTBD\b/g, "to be announced");
  }
  if (Array.isArray(v)) return v.map((x) => sanitizeCustomerCopy(x, key));
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, sanitizeCustomerCopy(x, k)]));
  return v;
}
function oneLine(s, n = 220) {
  const t = text(s);
  return t.length > n ? `${t.slice(0, n - 1)}…` : t;
}
/** Trim to the last full sentence within n characters when possible. */
function sentenceTrim(s, n = 600) {
  const t = text(s);
  if (t.length <= n) return t;
  const cut = t.slice(0, n);
  const i = Math.max(cut.lastIndexOf(". "), cut.lastIndexOf("; "));
  return i > n * 0.5 ? `${cut.slice(0, i + 1)}` : `${cut.slice(0, n - 1)}…`;
}
/** Windows: some stored artifact directories end in a space — use the extended-length path form. */
const longPath = (p) => (process.platform === "win32" ? `\\\\?\\${path.resolve(p)}` : p);
function walkJson(dir, out = []) {
  const abs = path.join(ROOT, dir);
  if (!fs.existsSync(longPath(abs))) return out;
  for (const e of fs.readdirSync(longPath(abs), { withFileTypes: true })) {
    const p = `${dir}/${e.name}`;
    if (e.isDirectory()) walkJson(p, out);
    else if (e.name.endsWith(".json")) out.push(p);
  }
  return out;
}
const readJsonLong = (relPath) => JSON.parse(fs.readFileSync(longPath(path.join(ROOT, relPath)), "utf8").replace(/^\uFEFF/, ""));

// ---------------------------------------------------------------------------
// protected store / snapshots
// ---------------------------------------------------------------------------
const protectedStore = createGdiPublicationPackageStore({ root: PROTECTED_STORE_ROOT });
function dirSha(dir) {
  if (!fs.existsSync(dir)) return null;
  const h = crypto.createHash("sha256");
  const files = [];
  const walk = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) walk(p);
      else files.push(p);
    }
  };
  walk(dir);
  for (const f of files.sort()) h.update(rel(f)).update(fs.readFileSync(f));
  return h.digest("hex");
}
function protectedSnapshot() {
  const o = {};
  for (const [hid, label] of PROTECTED_HOTELS) {
    const cur = protectedStore.readPointer(hid, "CURRENT");
    const stg = protectedStore.readPointer(hid, "STAGED");
    o[label] = {
      hotelId: hid,
      currentFingerprint: cur?.contentFingerprintSha256 || null,
      stagedFingerprint: stg?.contentFingerprintSha256 || null,
      packagesTreeSha256: dirSha(path.join(PROTECTED_STORE_ROOT, hid)),
      opportunitiesJsonSha256: sha256File(path.join(ROOT, "data/group-demand-intelligence/hotels", hid, "opportunities.json")),
    };
  }
  return o;
}

// ---------------------------------------------------------------------------
// --browser-verify (separate mode; consumes state from the default run)
// ---------------------------------------------------------------------------
async function runBrowserVerify() {
  const base = (argVal("--base-url") || process.env.GDI_PREVIEW_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
  const pageUrl = `${base}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
  const apiUrl = `${base}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?publicationPackage=1&surface=customer&preview=staged`;
  let state = null;
  try {
    state = readJson(STATE_PATH);
  } catch {
    state = null;
  }
  const expected = state?.staging || {};
  const result = {
    contract: `${CONTRACT_ID}_BROWSER`,
    generatedAt: new Date().toISOString(),
    status: "UNKNOWN",
    pageUrl,
    apiUrl,
    expectedFingerprint: expected.fingerprint || null,
    expectedCounts: expected.inventoryCounts || null,
    checks: {},
    errors: [],
  };
  if (!expected.fingerprint) {
    result.status = "FAIL_NO_STAGED_PACKAGE";
    result.errors.push("_run-state.json has no staged fingerprint; run the default mode first.");
    return finishBrowser(result, state);
  }
  try {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 15000);
    const r = await fetch(apiUrl, { signal: ctl.signal });
    clearTimeout(t);
    result.checks.apiHttpStatus = r.status;
    const api = await r.json().catch(() => null);
    result.checks.apiFingerprint = api?.publication?.contentFingerprintSha256 || null;
    result.checks.fingerprintMatch = result.checks.apiFingerprint === expected.fingerprint;
    result.checks.inventoryCounts = api?.publication?.inventoryCounts || null;
    const ops = Array.isArray(api?.opportunities) ? api.opportunities : [];
    result.checks.apiOpportunityCount = ops.length;
    result.checks.apiOpportunityIds = ops.map((o) => o.id);
    result.checks.apiHotelCount = ops.filter((o) => o.hotelMarketScopeEligible === true).length;
    result.checks.apiCompetitiveCount = ops.filter((o) => o.competitiveMarketScopeEligible === true).length;
    result.checks.apiMultipleDatesCount = ops.filter((o) => /multiple dates/i.test(JSON.stringify([o.datePillDisplayLabel, o.eventDateDisplay, o.tileBadgePlan?.datePillLabel]))).length;
    result.checks.apiSalesReadyCount = ops.filter((o) => o.commercialStageKey === "PURSUE_NOW" || o.commercialActionability === "SALES_READY").length;
    result.checks.providerLeakInApi = /\bexa\b|surfe|parallel\.ai|airtable/i.test(JSON.stringify(ops));
  } catch (e) {
    result.status = "FAIL_SERVER_DOWN";
    result.errors.push(`Preview server not reachable at ${base}: ${String(e?.message || e)}`);
    return finishBrowser(result, state);
  }
  let chromium = null;
  try {
    ({ chromium } = await import("playwright"));
  } catch (e) {
    result.errors.push(`playwright unavailable: ${String(e?.message || e)}`);
  }
  if (!chromium) {
    result.status = result.checks.fingerprintMatch ? "PARTIAL_API_ONLY_NO_PLAYWRIGHT" : "FAIL_FINGERPRINT_MISMATCH";
    return finishBrowser(result, state);
  }
  const shotDir = path.join(OUT, "screenshots");
  fs.mkdirSync(shotDir, { recursive: true });
  let browser = null;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForSelector('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]', { timeout: 90000 });
    await page.waitForTimeout(1500);
    result.checks.dom = await page.evaluate(() => {
      const body = document.body?.innerText || "";
      const cards = [...document.querySelectorAll('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]')];
      return {
        cards: cards.length,
        titles: cards.map((c) => (c.innerText || "").split("\n")[0].trim().slice(0, 120)),
        mentionsHotel: /YOTEL Geneva Lake/i.test(body),
        providerLeak: /\bExa\b|Surfe|Parallel|Airtable/.test(body),
        stillLoading: /Loading Group Demand Intelligence/i.test(body),
        multipleDatesPills: (body.match(/Multiple Dates/g) || []).length,
        salesReadyMentions: (body.match(/Sales Ready/gi) || []).length,
      };
    });
    await page.screenshot({ path: path.join(shotDir, "01-yotel-geneva-lake-staged-overview.png"), fullPage: true });
    const expectedAll = Number(expected?.inventoryCounts?.all || 0);
    const dom = result.checks.dom;
    result.status =
      result.checks.fingerprintMatch === true &&
      dom.cards >= Math.max(1, expectedAll) &&
      dom.mentionsHotel === true &&
      dom.stillLoading !== true &&
      !dom.providerLeak &&
      !result.checks.providerLeakInApi &&
      dom.multipleDatesPills === 0
        ? "PASS"
        : "FAIL_CHECKS";
  } catch (e) {
    result.status = "FAIL_BROWSER_ERROR";
    result.errors.push(String(e?.message || e));
  } finally {
    if (browser) await browser.close();
  }
  return finishBrowser(result, state);
}

function finishBrowser(result, state) {
  writeJson("14-LOCAL_BROWSER_VERIFICATION.json", result);
  if (state) {
    state.browser = { status: result.status, fingerprintMatch: result.checks?.fingerprintMatch ?? null, errors: result.errors };
    writeJson("_run-state.json", state);
    const cert = buildCertification(state);
    writeJson("15-FULL_E2E_CERTIFICATION.json", cert);
    fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), buildFounderReport(state, cert));
    state.certification = { status: cert.fullE2eStatus, pendingGates: cert.pendingGates };
    writeJson("_run-state.json", state);
  }
  console.log(JSON.stringify({ browserStatus: result.status, checks: { ...result.checks, apiOpportunityIds: undefined }, errors: result.errors }, null, 2));
  return result;
}

// ---------------------------------------------------------------------------
// certification + founder report (shared by both modes)
// ---------------------------------------------------------------------------
function buildCertification(state) {
  const s = state.staging || {};
  const browser = state.browser || { status: "NOT_RUN" };
  const gates = state.hardGates || {};
  const stageStatus = {};
  const mark = (id, status, evidence) => (stageStatus[String(id)] = { status, evidence });
  mark(1, gates.hotelConfigOk && gates.identityConfirmed ? "PASS" : "FAIL", `Hotel config loaded; single canonical identity ${HOTEL_ID}; required keys missing: ${(state.hotelConfigMissingKeys || []).join(",") || "none"}.`);
  mark(2, "PASS", `${state.recovery?.legacyCount ?? 0} legacy records + ${state.recovery?.storedArtifactLeads ?? 0} stored deep-run leads + ${state.recovery?.logOnlyLeads ?? 0} log-only leads reconciled (zero-cost reuse).`);
  mark(3, "PASS_ZERO_COST", `Fourteen-segment coverage documented from stored research (${state.segmentCoverage?.coveredSegments ?? 0}/14 with stored evidence); no new discovery was run.`);
  mark(4, "PASS_PRIOR_CAMPAIGNS", "Demand discovery was completed in prior authorized campaigns; $0 spent in this task.");
  mark("4a", gates.cmiRetainedAllPassGates ? "PASS" : "PASS_WITH_FINDINGS", `CMI retained=${state.after?.cmi ?? 0}; demoted=${state.cmiDemoted?.length ?? 0}; full CMI coverage claim=${state.cmiCoverage?.ok === true ? "allowed" : "NOT claimed (stored research only; see 07)"}.`);
  mark("4b", gates.actionabilityAssertOk ? "PASS" : "FAIL", `assertCustomerInventoryPassesActionabilityGate ok=${gates.actionabilityAssertOk}; referenceDate=${REF}.`);
  mark(5, "PASS", "Shared commercial eligibility + proactive account-development + CMI gates run against every recovered record.");
  mark(6, "PENDING", "Airtable persistence was exercised as a DRY RUN only; live write requires explicit founder authorization.");
  mark(7, gates.qualityGateOk && gates.actionabilityAssertOk ? "PASS" : s.staged === false && !state.after?.total ? "NOT_APPLICABLE_NO_INVENTORY" : "FAIL", `publication quality gate ok=${gates.qualityGateOk}`);
  mark(8, browser.status === "PASS" ? "PASS" : browser.status === "NOT_RUN" ? "PENDING" : `PENDING_BROWSER_${browser.status}`, `Local browser verification status=${browser.status}.`);
  mark(9, s.staged ? "PASS" : "NOT_STAGED", s.staged ? `Staged ${s.publicationVersion} (${s.fingerprint}).` : s.blocker || "not staged");
  mark(10, "PENDING", "Founder approval of the exact fingerprint is outstanding.");
  mark(11, "NOT_STARTED", "Production publication is blocked on founder approval; nothing was published.");
  mark(12, "NOT_STARTED", "Production parity requires publication.");
  mark(13, "NOT_STARTED", "Weekly refresh begins after publication.");

  const hardFailures = [];
  if (!gates.identityConfirmed) hardFailures.push("IDENTITY_NOT_CONFIRMED");
  if (!gates.protectedPublicationsUnchanged) hardFailures.push("PROTECTED_PUBLICATIONS_CHANGED");
  if (!gates.qualityGateOk) hardFailures.push("PUBLICATION_QUALITY_GATE_FAILED");
  if (!gates.actionabilityAssertOk) hardFailures.push("ACTIONABILITY_ASSERT_FAILED");
  if (!gates.integrityOk) hardFailures.push("PACKAGE_INTEGRITY_FAILED");
  if (!gates.noMultipleDates) hardFailures.push("MULTIPLE_DATES_REMAIN");
  if (!gates.noProviderLeak) hardFailures.push("PROVIDER_LEAK_IN_CUSTOMER_FIELDS");
  if (!gates.noDualClassification) hardFailures.push("DUAL_CLASSIFICATION");
  if (!gates.noImminentVisible) hardFailures.push("IMMINENT_OR_PAST_EVENT_VISIBLE");
  if (!gates.salesReadyAllPassContract) hardFailures.push("SALES_READY_WITHOUT_CONTRACT_PASS");
  if (!gates.noCustomerPublished) hardFailures.push("CUSTOMER_PUBLISHED_STATE");
  if (!gates.noPaidProviderImports) hardFailures.push("PAID_PROVIDER_IMPORT_DETECTED");
  if (!gates.stagedOrNoInventory) hardFailures.push("STAGING_BLOCKED");

  const pendingGates = [];
  pendingGates.push({ gate: "AIRTABLE_LIVE_PERSISTENCE", status: "PENDING_FOUNDER_AUTH", detail: "Dry-run only; no live Airtable write was authorized." });
  if (browser.status !== "PASS") {
    pendingGates.push({ gate: "LOCAL_BROWSER_VERIFICATION", status: browser.status, detail: (browser.errors || []).join("; ") || "Run --browser-verify against a server serving the staged package." });
  }
  pendingGates.push({ gate: "FOUNDER_APPROVAL_OF_FINGERPRINT", status: s.staged ? "PENDING" : "NOT_APPLICABLE_UNTIL_STAGED", detail: "Founder must approve the exact staged fingerprint." });
  pendingGates.push({ gate: "HOTEL_MARKET_TARGET_20", status: (state.after?.hm ?? 0) >= TARGET_HM ? "MET" : "SHORTFALL_PAID_PROPOSAL_PENDING_FOUNDER", detail: `${state.after?.hm ?? 0} of ${TARGET_HM} Hotel Market cards evidenced; see 17-PAID_RESEARCH_PROPOSAL_IF_NEEDED.json.` });
  pendingGates.push({ gate: "PRODUCTION_PUBLICATION", status: "NOT_STARTED", detail: "No CUSTOMER_PUBLISHED lifecycle; no deployment." });
  pendingGates.push({ gate: "PRODUCTION_PARITY", status: "NOT_STARTED", detail: "Requires publication." });
  pendingGates.push({ gate: "WEEKLY_REFRESH_AND_FEEDBACK", status: "NOT_STARTED", detail: "Starts after publication." });
  pendingGates.push({ gate: "GITHUB_PR_CI_MERGE", status: "NOT_PERFORMED_BY_THIS_TASK", detail: "Parent task commits, opens the PR, runs CI and merges to main." });

  const fullE2eStatus = hardFailures.length ? "FAILED" : "CERTIFIED_WITH_PENDING_GATES";
  return {
    contract: CONTRACT_ID,
    template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    templateLock: GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
    actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    referenceDate: REF,
    fullE2eStatus,
    fullE2eComplete: false,
    fullE2eCompleteReason:
      "FULL E2E COMPLETE is NOT declared: Airtable live persistence, founder approval, publication, production parity and weekly refresh are outstanding.",
    hardFailures,
    pendingGates,
    stages: TEMPLATE_E2E_STAGES.map((st) => ({ id: st.id, name: st.name, ...(stageStatus[String(st.id)] || { status: "UNKNOWN" }) })),
    hardGates: gates,
    customerPublished: false,
    staged: s.staged === true,
    stagedFingerprint: s.fingerprint || null,
    paidSpendThisTaskUsd: 0,
    providersCalledThisTask: [],
    airtable: { mode: "DRY_RUN_ONLY", liveWritesAuthorized: false, status: state.airtable?.status || null },
    browser,
  };
}

function buildFounderReport(state, cert) {
  const a = state.after;
  const L = [];
  const row = (...c) => `| ${c.map((x) => String(x ?? "").replace(/\|/g, "\\|").replace(/\n/g, " ")).join(" | ")} |`;
  L.push(`# YOTEL Geneva Lake — Certified Full E2E (zero paid)`);
  L.push("");
  L.push(`Reference date: **${REF}** · Hotel ID \`${HOTEL_ID}\` (single canonical identity — no second identity created) · Contract: \`${CONTRACT_ID}\` · Template: \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\` · Paid spend this task: **$0** (no Exa, no Surfe, no Parallel, no provider calls)`);
  L.push("");
  L.push(`## Verdict`);
  L.push("");
  L.push(`**Full E2E status: \`${cert.fullE2eStatus}\`** — *not* "Full E2E complete".`);
  L.push("");
  L.push(
    `Commercial credibility was prioritised over card count. ${state.recovery.totalEvaluated} candidates were recovered and audited ` +
      `(${state.recovery.legacyCount} legacy records, ${state.recovery.storedArtifactLeads} stored deep-research leads, ${state.recovery.logOnlyLeads} log-only leads). ` +
      `Only **${a.total} customer-visible cards** pass the gates: **${a.hm} Hotel Market** (${a.salesReady} Sales Ready, ${a.accountDevelopment} Account Development, ${a.qualify} Qualify Opportunity) and **${a.cmi} Competitive Market Intelligence**. ` +
      `${a.researchContinuation} further records sit in research continuation, ${a.demoted} were demoted as past / imminent / not currently actionable and ${a.rejected} were rejected as synthetic shells, venue-operator wrappers or non-event listings.`,
  );
  L.push("");
  L.push(`**Target shortfall:** ${state.shortfall > 0 ? `${state.shortfall} below the ${TARGET_HM}-card Hotel Market target — reported honestly rather than padded. A costed paid-research proposal (est. $${state.paidProposalUsd?.recommendedUsd ?? "n/a"}) is in \`17-PAID_RESEARCH_PROPOSAL_IF_NEEDED.json\` and needs founder approval before any spend.` : `${TARGET_HM}-card target met.`}`);
  L.push("");
  L.push(`| | Count |`);
  L.push(`|---|---|`);
  L.push(row("Hotel Market (customer-visible)", a.hm));
  L.push(row("  Sales Ready", a.salesReady));
  L.push(row("  Account Development", a.accountDevelopment));
  L.push(row("  Qualify Opportunity", a.qualify));
  L.push(row("CMI (customer-visible, separate inventory)", a.cmi));
  L.push(row("Research continuation (hidden, preserved)", a.researchContinuation));
  L.push(row("Demoted (past / imminent / not actionable)", a.demoted));
  L.push(row("Rejected (shell / wrapper / non-event listing)", a.rejected));
  L.push(row("Recovery leads with no local evidence", a.recoveryLeadsNoEvidence));
  L.push("");
  L.push(state.staging.staged ? `Staged fingerprint: \`${state.staging.fingerprint}\` (\`${state.staging.publicationVersion}\`), lifecycle STAGED_FOR_FOUNDER_REVIEW, never CUSTOMER_PUBLISHED.` : `No package staged (${state.staging.blocker || "n/a"}).`);
  L.push("");
  L.push(`## Top 10 (ranked; customer visibility stated per row)`);
  L.push("");
  L.push(row("#", "Title", "Lane", "Dates", "Visible to customers", "Why it ranks / what is missing"));
  L.push(row("---", "---", "---", "---", "---", "---"));
  for (const t of state.top10) L.push(row(t.rank, t.title, t.lane, t.dates, t.customerVisible ? "yes" : "no (research continuation)", t.note));
  L.push("");
  L.push(`## Why YOTEL fits (or only partly fits) events outside central Geneva`);
  L.push("");
  L.push(`YOTEL Geneva Lake (237 rooms, 6 meeting rooms to ~250 delegates, Founex / La Côte, airport-corridor access) is **not** treated as an automatic downtown-Geneva CBD fit. Retained cards state the transfer reality explicitly and are framed as secondary lodging / ancillary-meeting / pre-post stay options, never as displacement of a confirmed main venue.`);
  L.push("");
  L.push(`## Hotel Market — every audited lead`);
  L.push("");
  L.push(row("Lead", "Dates", "Disposition", "Contact", "Basis / why not Sales Ready"));
  L.push(row("---", "---", "---", "---", "---"));
  for (const r of state.leadRows) L.push(row(r.title, r.dateLabel || "—", r.disposition, r.contactSummary || "—", oneLine(r.basis, 260)));
  L.push("");
  L.push(`## Legacy opportunity audit (${state.recovery.legacyCount} records)`);
  L.push("");
  for (const [k, v] of Object.entries(state.legacyDispositionCounts)) L.push(`- ${k}: **${v}**`);
  L.push("");
  L.push(`## Gates`);
  L.push("");
  for (const st of cert.stages) L.push(`- Stage ${st.id} — ${st.name}: **${st.status}** — ${st.evidence || ""}`);
  L.push("");
  L.push(`### Pending gates`);
  for (const g of cert.pendingGates) L.push(`- **${g.gate}** — ${g.status}: ${g.detail}`);
  L.push("");
  if (cert.hardFailures.length) {
    L.push(`### Hard failures`);
    for (const f of cert.hardFailures) L.push(`- ${f}`);
    L.push("");
  }
  L.push(`## Airtable`);
  L.push("");
  L.push(`Dry run only — ${JSON.stringify(state.airtable || {}, null, 0)}`);
  L.push("");
  L.push(`## Local browser verification`);
  L.push("");
  L.push(`Status: **${state.browser?.status || "NOT_RUN"}**${state.browser?.errors?.length ? ` — ${state.browser.errors.join("; ")}` : ""}`);
  L.push("");
  L.push(`## GitHub`);
  L.push("");
  L.push(`No commit, PR or merge was performed by this task (parent task owns it). Branch: \`${state.git?.branch || ""}\`, base: \`${state.git?.head || ""}\`.`);
  L.push("");
  return `${L.join("\n")}\n`;
}

if (BROWSER_VERIFY) {
  await runBrowserVerify();
  process.exit(0);
}

// ===========================================================================
// STAGE 0 — identity + config (read-only)
// ===========================================================================
fs.mkdirSync(OUT, { recursive: true });
const HOTEL_CFG = readJson(path.join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`));
const hotelConfigMissingKeys = HOTEL_CONFIGURATION_REQUIRED_KEYS.filter((k) => HOTEL_CFG[k] == null);
const ADP_FIXTURE_PATH = path.join(ROOT, HOTEL_CFG.adpFixturePath || "fixtures/ai-demand-positioning/yotel-geneva-lake-property-profile.json");
const ADP_FIXTURE = fs.existsSync(ADP_FIXTURE_PATH) ? readJson(ADP_FIXTURE_PATH) : null;
const HOTEL_PROFILE_PATH = path.join(HOTEL_DATA, "profile.json");
const HOTEL_PROFILE = fs.existsSync(HOTEL_PROFILE_PATH) ? readJson(HOTEL_PROFILE_PATH) : null;
const stagedPointerBefore = protectedStore.readPointer(HOTEL_ID, "STAGED");
const protectedBefore = protectedSnapshot();
const legacyRaw = readJson(OPPS_JSON_PATH);
const legacyOpps = Array.isArray(legacyRaw) ? legacyRaw : legacyRaw.opportunities || [];
const legacySha = sha256File(OPPS_JSON_PATH);
const gitBefore = { branch: sh("git rev-parse --abbrev-ref HEAD"), head: sh("git rev-parse --short HEAD") };

// identity duplicate scan: any other hotel config whose display name / aliases collide
const hotelCfgDir = path.join(ROOT, "config/group-demand-intelligence/hotels");
const identityCollisions = [];
for (const f of fs.readdirSync(hotelCfgDir)) {
  if (!f.endsWith(".json") || f === `${HOTEL_ID}.json`) continue;
  try {
    const c = readJson(path.join(hotelCfgDir, f));
    const names = [c.displayName, ...(c.aliases || [])].map(normKey);
    if (names.some((n) => /yotel/.test(n) && /geneva|lake|founex/.test(n))) identityCollisions.push({ file: f, hotelId: c.hotelId, displayName: c.displayName });
  } catch (e) {
    identityCollisions.push({ file: f, error: String(e?.message || e) });
  }
}
const identityConfirmed =
  HOTEL_CFG.hotelId === HOTEL_ID && /yotel geneva lake/i.test(HOTEL_CFG.displayName || "") && identityCollisions.filter((c) => !c.error).length === 0;

// ---------------------------------------------------------------------------
// static zero-paid assertion (this script must never import a paid client)
// ---------------------------------------------------------------------------
const selfSource = fs.readFileSync(SCRIPT_PATH, "utf8");
const importLines = selfSource.split("\n").filter((l) => /^\s*(import\b|const .*=\s*await import\()/.test(l));
const paidImportHits = importLines.filter((l) => /exa-agent|exa-live|surfe|parallel|exa[-_]client/i.test(l));
const noPaidProviderImports = paidImportHits.length === 0;

// ===========================================================================
// STAGE 1 — recovery: legacy opportunities
// ===========================================================================
const GENERIC_ORG_TOKENS = new Set(
  (
    "partner partners delegation delegations ngo university vendor production sponsor sponsors exhibitor exhibitors speaker speakers consultancies foundation humanitarian " +
    "government spectators not crews media pr teams team cycle keep generator campaign ampaign visible and listing decompose"
  ).split(" "),
);
const orgTokens = (s) => normKey(s).split(" ").filter(Boolean);
const orgAllGeneric = (s) => {
  const t = orgTokens(s);
  return t.length > 0 && t.every((x) => GENERIC_ORG_TOKENS.has(x));
};

function legacyContactName(o) {
  return text(o.primaryContact?.name || o.contactName || "");
}

function classifyLegacy(o) {
  const id = text(o.id);
  const title = text(o.displayTitle || o.title);
  const org = text(o.organizationName);
  const start = parseIso(o.eventStartDate);
  const end = parseIso(o.eventEndDate) || start;
  const days = start ? daysBetween(REF, start) : null;
  const pastOrImminent = start ? start < IMMINENT_CUTOFF : false;
  const reasons = [];
  let disposition;
  let family;
  let returnCriteria = null;

  if (/_ycamp_/.test(id)) {
    if (orgAllGeneric(org) || /listing decompose/i.test(org)) {
      family = "YCAMP_GENERIC_CHILD";
      disposition = DISP.REJECTED_SYNTHETIC_CAMPAIGN_SHELL;
      reasons.push(`Organization "${org}" is a role/placeholder label, not a named organization; no named buyer, no named demand mechanism (template campaign child).`);
    } else {
      family = "YCAMP_ORGANIZER_OR_VENUE_WRAPPER";
      disposition = DISP.REJECTED_VENUE_OPERATOR_OR_ORGANIZER_WRAPPER;
      reasons.push(
        /venue operator/i.test(title)
          ? `${org} is the venue operator of the host facility, not a lodging buyer; no named person or housing-control evidence.`
          : `${org} is the organizer/secretariat/host wrapper for the event itself; no named buyer or lodging mechanism (consistent with prior audits that held these as organizer shells).`,
      );
      returnCriteria = "A named person at the organizer with verified delegate-housing or supplier-hotel responsibility for a dated future edition.";
    }
  } else if (/^gdi_opp_aifg_/.test(id)) {
    family = "AI_FOR_GOOD_NAMED_2026_PARTNER";
    disposition = DISP.RESEARCH_CONTINUATION_HIDDEN;
    reasons.push(`${org} is a published 2026 partner/sponsor of AI for Good; participation in the 2027 edition, attendee volume and who controls its Geneva lodging are not evidenced, and no named buyer exists.`);
    returnCriteria = "Evidence of the organization's confirmed 2027 participation plus a named travel/events owner with lodging responsibility.";
  } else if (/^gdi_opp_yotel_exp_v1_/.test(id)) {
    family = "AIDEX_2026_EXHIBITOR";
    disposition = DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE;
    reasons.push(`AidEx Geneva exhibitor lead tied to a 2026-10-21 event that is ${days != null ? `${Math.abs(days)} day(s) in the past` : "past"} at ${REF}; no future edition is evidenced.`);
    returnCriteria = "A dated future AidEx edition in the La Côte / Geneva corridor with a named exhibitor travel owner.";
  } else if (/^gdi_opp_aidex_geneva_/.test(id)) {
    family = "AIDEX_2026_ORGANIZER";
    disposition = DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE;
    reasons.push(`AidEx Geneva 2026 (2026-10-21) is past at ${REF}.`);
    returnCriteria = "A dated future edition and an organizer housing-owner contact.";
  } else {
    family = "GENERIC_WEB_LISTING";
    if (pastOrImminent && start) {
      disposition = DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE;
      reasons.push(`Event date ${start} is ${days < 0 ? `${Math.abs(days)} day(s) past` : `${days} day(s) out (< ${IMMINENT_DAYS}-day sourcing minimum)`} at ${REF}; the listing is also a generic web page rather than a named organization's lodging mechanism.`);
      returnCriteria = "A verified, dated future edition with a named organizer and a lodging-sourcing window of at least 21 days.";
    } else {
      disposition = DISP.REJECTED_NON_EVENT_WEB_LISTING;
      reasons.push(`"${title}" is a generic web listing/aggregator page (${org || "no organizer"}${start ? "" : ", no event date"}) — no named organization with a lodging mechanism, no named buyer.`);
    }
  }

  let actionability = null;
  try {
    const a = classifyCommercialActionability(o, { referenceDate: REF, actualEventStart: start, actualEventEnd: end });
    actionability = { state: a.state, temporalDisposition: a.temporalDisposition, customerVisibleEligible: a.customerVisibleEligible };
  } catch (e) {
    actionability = { error: String(e?.message || e) };
  }
  let contract = null;
  try {
    const c = evaluateSharedCommercialEligibility(cloneOpp(o), { hotel: HOTEL_CFG });
    contract = { status: c.status, salesReady: c.salesReady === true, gaps: c.gaps || [] };
  } catch (e) {
    contract = { error: String(e?.message || e) };
  }
  return {
    id,
    title,
    organization: org,
    family,
    eventStartDate: start,
    eventEndDate: end,
    daysToStartFromRef: days,
    dateLabel: start ? dateLabelFor(start, end) : null,
    contactName: legacyContactName(o) || null,
    sourceCount: Array.isArray(o.sources) ? o.sources.length : 0,
    airtableRecordIdOnLegacy: o._airtableRecordId || null,
    disposition,
    reasons,
    returnCriteria,
    actionability,
    contractAsStored: contract,
    salesReady: false,
  };
}
const legacyRows = legacyOpps.map(classifyLegacy);
const legacyDispositionCounts = countBy(legacyRows, (r) => r.disposition);

// ===========================================================================
// STAGE 2 — recovery: stored deep-research artifacts (zero-cost reuse)
// ===========================================================================
const artifactFiles = ARTIFACT_ROOTS.flatMap((r) => walkJson(r));
const artifacts = [];
for (const f of artifactFiles) {
  try {
    const j = readJsonLong(f);
    const st = j.run?.structured || null;
    const dr = j.deepResearch || null;
    artifacts.push({
      file: f,
      org: text(j.candidate?.organizationName || j.candidate?.programmeName || ""),
      kind: dr?.eventsOpportunityOverviewFields ? "ASSOCIATION_DEEP" : st && !st.findings ? "SEGMENT_DEEP" : "NO_STRUCTURED_RESULT",
      error: j.run?.error || null,
      costUsd: j.run?.costDollars?.total ?? null,
      workflowId: j.candidate?.workflowId || j.segmentId || j.workflow || j.candidate?.segmentId || null,
      doc: j,
    });
  } catch (e) {
    artifacts.push({ file: f, org: "", kind: "UNREADABLE", error: String(e?.message || e), doc: null });
  }
}
const richness = (a) =>
  (a.doc?.deepResearch?.organizerContactCandidates?.length || 0) + (a.doc?.run?.structured?.namedContacts?.length || 0) + (a.doc?.run?.structured?.supportingSources?.length || 0);

// ---------------------------------------------------------------------------
// lead definitions — analyst judgement layered on stored evidence (every claim traces to the artifact)
// ---------------------------------------------------------------------------
const YOTEL_FIT_FACTS =
  "YOTEL Geneva Lake (Founex, Vaud) offers 237 rooms and six flexible meeting spaces (up to about 250 delegates) with hybrid facilities and an airport shuttle.";
const LEAD_DEFS = [
  {
    key: "icpic",
    match: /icpic/i,
    attendanceText: "More than 1,000 attendees at ICPIC 2025 (healthcare professionals); historical figure, not a 2027 forecast.",
    locationSummary: "Held in Geneva city at the International Conference Centre Geneva (CICG).",
    segmentId: "associations_conferences_events",
    route: "HM",
    locationText: "Geneva International Conference Centre (CICG), Geneva, Switzerland",
    demandClass: "RELIABLE_INFERRED_DEMAND",
    demandRationale:
      "Scientific congress with 1,000+ healthcare professionals at the 2025 edition and an official housing bureau (MCI Suisse) that historically managed multi-hotel blocks; 2027 hotel list not yet published, so demand for this property is inferred, not confirmed.",
    fit: {
      fit: "OVERFLOW_ANCILLARY",
      rationale:
        "Secondary accommodation block or small ancillary meeting only — the main congress is committed to CICG in central Geneva and historic official hotels are city-centre. Founex needs a dedicated transfer plan; do not present as an easy congress base.",
    },
    publicChecks: [
      { url: "https://conference.icpic.com/", mustMatch: /2027/ },
      { url: "https://conference.icpic.com/venue/", mustMatch: /CICG|International Conference Cent/i },
    ],
  },
  {
    key: "sfpe",
    match: /sfpe/i,
    segmentId: "associations_conferences_events",
    route: "CMI",
    locationText: "Crowne Plaza Geneva, Geneva, Switzerland",
    competitorHotel: "Crowne Plaza Geneva",
    venueStatus: "CONFIRMED_CONTRACTED_BLOCK",
    roomEvidence: true,
    bookingType: "EVENT_VENUE_PLUS_PUBLISHED_GUESTROOM_TERMS",
    commitmentScope: "VENUE_AND_GUESTROOMS",
    publicChecks: [
      { url: "https://www.sfpe.org/europe27/home", mustMatch: /Crowne Plaza|April 14|14\s*[-–]\s*15 April|Geneva/i },
      { url: "https://www.sfpe.org/europe27/hotel", mustMatch: /Crowne Plaza|CHF\s*220/i },
    ],
  },
  {
    key: "daus",
    match: /daus/i,
    segmentId: "associations_conferences_events",
    route: "CMI",
    locationText: "Crowne Plaza Geneva, Geneva, Switzerland",
    competitorHotel: "Crowne Plaza Geneva",
    venueStatus: "CONFIRMED_EVENT_VENUE",
    roomEvidence: false,
    bookingType: "CONFIRMED_HOTEL_SEDE_NO_PUBLISHED_ROOM_TERMS",
    commitmentScope: "VENUE_PRIMARY_GUESTROOMS_NOT_SEPARATELY_PUBLISHED",
    publicChecks: [{ url: "https://www.daus-conference.com/", mustMatch: /Crowne Plaza|2027/i }],
  },
  {
    key: "opal",
    match: /opal/i,
    segmentId: "associations_conferences_events",
    route: "CMI",
    locationText: "Crowne Plaza Geneva, Geneva, Switzerland",
    competitorHotel: "Crowne Plaza Geneva",
    venueStatus: "CONFIRMED_EVENT_VENUE",
    roomEvidence: false,
    bookingType: "CONFIRMED_HOTEL_SEDE_NO_PUBLISHED_ROOM_TERMS",
    commitmentScope: "VENUE_PRIMARY_GUESTROOMS_NOT_SEPARATELY_PUBLISHED",
    publicChecks: [],
  },
  {
    key: "gdpm",
    match: /agroscope|gdpm/i,
    segmentId: "associations_conferences_events",
    route: "NONE",
    noRouteReason: "The stored deep run for this candidate returned an error and no structured result — no verified dates, venue, contacts or lodging mechanism exist on disk.",
    locationText: "",
  },
  {
    key: "epfl",
    match: /epfl|swiss plasma/i,
    titleOverride: "53rd EPS Conference on Plasma Physics (EPS Plasma 2027)",
    segmentId: "education_scientific",
    route: "HM",
    locationText: "SwissTech Convention Center, EPFL campus, Ecublens (Lausanne area), Vaud",
    dates: { phrase: /5\s*[–-]\s*9 July 2027/, start: "2027-07-05", end: "2027-07-09" },
    demandClass: "SPECULATIVE_DEMAND",
    demandRationale: "Property-specific guestroom demand is unconfirmed in the research (no room block, hotel request, overflow shortage or YOTEL appointment); the conference is on the EPFL campus in the Lausanne area, outside the realistic daily-commute corridor.",
    fit: {
      fit: "WATCH_STRETCH_TERRITORY",
      rationale: "Pre/post-conference airport-corridor stopovers or a small delegation meeting are conceivable, but the venue is a stretch-territory location and no transfer-viable demand is evidenced.",
    },
    publicChecks: [{ url: "https://epsplasma2027.epfl.ch/", mustMatch: /July|2027/i }],
  },
  {
    key: "hublot",
    match: /hublot/i,
    titleOverride: "Hublot / LVMH — Nyon manufacture H3 commissioning and campus consolidation",
    segmentId: "relocation_extended_stay",
    route: "HM",
    locationText: "Nyon, Vaud (Hublot manufacture H3)",
    dates: null,
    demandClass: "SPECULATIVE_DEMAND",
    demandRationale: "UNQUALIFIED in the research — no public evidence of a lodging programme, hotel block, recurring accommodation contract, temporary-assignee cohort or room-night requirement.",
    fit: { fit: "WATCH_UNQUALIFIED", rationale: "Individual business stays and small training/project meetings are plausible for a Nyon manufacture campus 15 minutes away, but need are not evidenced." },
    publicChecks: [],
  },
  {
    key: "chateau-nyon",
    match: /ch.teau de nyon|caveau/i,
    titleOverride: "Château de Nyon / Caveau de Nyon — corporate and private-event referral route",
    segmentId: "preferred_lodging_partnerships",
    route: "HM",
    locationText: "Château de Nyon / Caveau de Nyon, Nyon, Vaud",
    dates: null,
    demandClass: "SPECULATIVE_DEMAND",
    demandRationale: "Conditional, referral-led prospect — the research states it is not a proven room-block account and overnight demand from private-function guests is unverified.",
    fit: { fit: "WATCH_REFERRAL_ONLY", rationale: "A referral/partnership route (corporate groups combining hotel meetings with an off-site wine evening) — not hotel-demand evidence." },
    publicChecks: [],
  },
  {
    key: "fisu",
    match: /fisu|swiss university sports/i,
    titleOverride: "2026 FISU World University Championship Triathlon (Nyon)",
    segmentId: "sports_entertainment",
    route: "HM",
    locationText: "Colovray Sports Centre, Nyon, Vaud",
    dates: { phrase: /8\s*[–-]\s*9 August 2026/, start: "2026-08-08", end: "2026-08-09" },
    demandClass: "SPECULATIVE_DEMAND",
    demandRationale: "Historical demand verified for 2026 (about 180 delegation members housed at Everness, not YOTEL); the 2026 championship is completed and no future local edition is dated.",
    fit: { fit: "WATCH_HISTORICAL_ONLY", rationale: "Post-event supplier conversation for future local triathlon editions only." },
    publicChecks: [],
  },
  {
    key: "prix-iddea",
    match: /prix iddea|iddea/i,
    titleOverride: "Prix IDDEA 2026 gala evening (Geneva)",
    segmentId: "corporate_celebrations_private_events",
    route: "HM",
    locationText: "Palladium, Geneva",
    dates: { phrase: /26 November 2026/, start: "2026-11-26", end: "2026-11-26" },
    demandClass: "SPECULATIVE_DEMAND",
    demandRationale: "Evening awards/apéritif with free registration; venue already booked (Palladium); no accommodation demand evidenced.",
    fit: { fit: "WATCH_LOW_FIT", rationale: "Low fit for relocating the gala; attendance (400+) exceeds YOTEL's single-room capacity." },
    publicChecks: [],
  },
  {
    key: "lvg",
    match: /luxury venture|luxury innovation|lvg/i,
    titleOverride: "Luxury Innovation Summit & Awards 2026 (Geneva)",
    segmentId: "corporate_celebrations_private_events",
    route: "HM",
    locationText: "InterContinental Geneva, Geneva",
    dates: { phrase: /1\s*[–-]\s*2 October 2026|1 October 2026/, start: "2026-10-01", end: "2026-10-02" },
    demandClass: "SPECULATIVE_DEMAND",
    demandRationale: "2026 edition is past; a ninth Geneva edition is announced for 2027 by the organizer but no exact date, venue or gala is confirmed.",
    fit: { fit: "WATCH_HISTORICAL_ONLY", rationale: "Banqueting/reception-led; guestroom demand unconfirmed." },
    publicChecks: [],
  },
];

function pickArtifact(def) {
  const cands = artifacts.filter((a) => a.kind !== "UNREADABLE" && (def.match.test(a.org) || def.match.test(a.file)));
  if (!cands.length) return { chosen: null, all: [] };
  const withResult = cands.filter((a) => a.kind === "ASSOCIATION_DEEP" || a.kind === "SEGMENT_DEEP");
  const pool = withResult.length ? withResult : cands;
  pool.sort((x, y) => richness(y) - richness(x));
  return { chosen: pool[0], all: cands };
}

const PUBLIC_CHECK_RESULTS = [];
async function publicCheck(leadKey, url, mustMatch) {
  const res = { leadKey, url, status: "SKIPPED", httpStatus: null, matched: null, note: null };
  if (SKIP_PUBLIC_CHECKS) {
    res.note = "--skip-public-checks";
    PUBLIC_CHECK_RESULTS.push(res);
    return res;
  }
  try {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 12000);
    const r = await fetch(url, { signal: ctl.signal, headers: { "user-agent": "Mozilla/5.0 (compatible; GDI-public-recheck/1.0)" }, redirect: "follow" });
    clearTimeout(t);
    res.httpStatus = r.status;
    const body = (await r.text()).slice(0, 600000);
    res.matched = mustMatch.test(body);
    res.status = !r.ok ? "UNREACHABLE_OR_ERROR" : res.matched ? "CONFIRMED_BY_STATIC_FETCH" : "NOT_CONFIRMED_BY_STATIC_FETCH";
    if (r.ok && !res.matched) res.note = "Page reachable but the expected statement was not found in static HTML (may be script-rendered); stored research evidence stands, flagged for re-verification.";
  } catch (e) {
    res.status = "UNREACHABLE_OR_ERROR";
    res.note = String(e?.message || e);
  }
  PUBLIC_CHECK_RESULTS.push(res);
  return res;
}

// --- contact normalisation ------------------------------------------------
const localPartMatchesName = (email, name) => {
  const lp = text(email).split("@")[0].toLowerCase().replace(/[^a-z]/g, "");
  const toks = normKey(name).split(" ").filter((t) => t.length > 1);
  if (!lp || toks.length < 2) return false;
  const first = toks[0];
  const last = toks[toks.length - 1];
  return lp.includes(last) || lp === first || lp.startsWith(first) || lp === `${first[0]}${last}`;
};
const firstEmail = (s) => {
  const m = text(s).match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/);
  return m ? m[0] : null;
};
function normalizeContacts(def, doc) {
  const raw = doc.deepResearch?.organizerContactCandidates || doc.run?.structured?.namedContacts || [];
  return raw
    .filter((c) => text(c.fullName || c.name))
    .map((c) => {
      const name = text(c.fullName || c.name);
      const email = text(c.publicEmail || c.email) || null;
      const orgRoute = text(c.organizationalRoute || c.organizationalEmail) || null;
      const personal = email && localPartMatchesName(email, name);
      return {
        name,
        title: text(c.title) || null,
        organization: text(c.organization) || null,
        email: personal ? email : null,
        mailbox: !personal ? email || firstEmail(orgRoute) : firstEmail(orgRoute),
        emailKind: personal ? "NAMED_DIRECT" : email || firstEmail(orgRoute) ? "ORGANIZER_MAILBOX" : null,
        phone: text(c.phone || c.publicPhone) || null,
        sourceUrl: text(c.sourceUrl) || text((c.sourceUrls || [])[0]) || null,
        sourceUrls: c.sourceUrls || (c.sourceUrl ? [c.sourceUrl] : []),
        linkedinUrl: c.linkedinUrl || null,
        why: text(c.whyRelevant),
        commercialRole: text(c.commercialRole || c.relevanceClass),
        hotelSourcingResponsibility: text(c.hotelSourcingResponsibility),
        contractingAuthorityVerified: c.contractingAuthorityVerified === true,
      };
    });
}

// --- lead normalisation ----------------------------------------------------
function monthNum(m) {
  return ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"].indexOf(m.toLowerCase()) + 1;
}
function normalizeLead(def) {
  const { chosen, all } = pickArtifact(def);
  const lead = {
    key: def.key,
    def,
    artifactFile: chosen?.file || null,
    siblingArtifacts: all.map((a) => a.file),
    artifactKind: chosen?.kind || "NOT_FOUND",
    costUsdRecorded: chosen?.costUsd ?? null,
  };
  if (!chosen || !(chosen.kind === "ASSOCIATION_DEEP" || chosen.kind === "SEGMENT_DEEP")) {
    lead.noEvidence = def.noRouteReason || "No structured stored result for this lead.";
    lead.title = def.key;
    lead.org = chosen?.org || def.key;
    return lead;
  }
  const doc = chosen.doc;
  const dr = doc.deepResearch;
  const st = doc.run.structured;
  lead.contacts = normalizeContacts(def, doc);
  if (chosen.kind === "ASSOCIATION_DEEP") {
    const ov = dr.eventsOpportunityOverviewFields;
    const f = dr.findings || {};
    const h = dr.cibMappingHints || {};
    lead.title = text(ov.eventName);
    lead.org = text(ov.organizerName || chosen.org);
    lead.start = parseIso(ov.verifiedStartDate || ov.futureEdition?.startDate);
    lead.end = parseIso(ov.verifiedEndDate || ov.futureEdition?.endDate) || lead.start;
    lead.venue = text(ov.verifiedVenueName || ov.futureEdition?.venueName);
    lead.city = text(ov.eventLocationCity);
    lead.venueIsCompetingHotel = ov.venueIsCompetingHotel === true;
    lead.hotelSourcingStatus = text(ov.hotelSourcingStatus);
    lead.venueCommitment = text(ov.venueCommitment);
    lead.attendance = text(ov.attendancePublished);
    lead.recurrence = text(ov.recurrenceEvidence);
    lead.what = text(h.whatIsHappening);
    lead.potential = text(h.potentialBusiness);
    lead.whyHotel = text(h.whyThisHotel);
    lead.nextStep = text(f.recommendedSalesAction || h.recommendedNextStep);
    lead.accessibility = text(h.accessibilityStatement);
    lead.accommodationFinding = text(f.accommodationSourcing);
    lead.futureCompetitiveCapture = text(f.futureCompetitiveCapturePotential || ov.futureCompetitiveCaptureStrategy);
    lead.uncertainties = (dr.remainingUncertainties || []).map(text).slice(0, 4);
    lead.sources = (dr.supportingSources || [])
      .filter((s) => /^https?:/i.test(text(s.url)))
      .map((s) => ({ url: text(s.url), statement: text(s.claim || s.statement || lead.title) }));
    lead.dateEvidence = { source: "deepResearch.eventsOpportunityOverviewFields.verified*Date", status: text(ov.futureDateStatus) };
  } else {
    lead.title = text(def.titleOverride || chosen.org);
    lead.org = text(doc.candidate?.organizationName || chosen.org);
    const ident = text(st.programmeIdentity || st.venueIdentity);
    lead.what = sentenceTrim(ident, 520);
    lead.potential = sentenceTrim(st.hotelDemandThesis, 560);
    lead.whyHotel = sentenceTrim(st.procurementOrAccessibilityNotes || st.hotelDemandThesis, 420);
    lead.nextStep = sentenceTrim(st.recommendedNextStep, 600);
    lead.uncertainties = (st.uncertainties || []).map(text).slice(0, 4);
    lead.guestroomDemandStatus = text(st.guestroomDemandStatus);
    lead.sources = (st.supportingSources || [])
      .filter((s) => /^https?:/i.test(text(s.url || s)))
      .map((s) => ({ url: text(s.url || s), statement: text(s.claim || s.statement || lead.org) }));
    if (!lead.sources.length) lead.sources = (doc.candidate?.evidenceUrls || []).map((u) => ({ url: text(u), statement: lead.org }));
    lead.venue = null;
    if (def.dates && def.dates.phrase.test(ident + " " + JSON.stringify(st.establishedFacts || []))) {
      lead.start = def.dates.start;
      lead.end = def.dates.end;
      lead.dateEvidence = { source: "stored deep-run programmeIdentity/establishedFacts", phrase: String(def.dates.phrase) };
    } else {
      lead.start = null;
      lead.end = null;
      lead.dateEvidence = { source: def.dates ? "expected date phrase NOT found in stored text" : "no exact event date in stored research", phrase: def.dates ? String(def.dates.phrase) : null };
    }
  }
  return lead;
}
const leads = LEAD_DEFS.map(normalizeLead);

// free public re-check (best effort; non-blocking)
for (const lead of leads) {
  lead.publicChecks = [];
  for (const pc of lead.def.publicChecks || []) lead.publicChecks.push(await publicCheck(lead.key, pc.url, pc.mustMatch));
}

// ---------------------------------------------------------------------------
// log-only leads (names recorded in console logs; evidence files absent from this checkout)
// ---------------------------------------------------------------------------
const logOnlyLeads = [];
{
  const seen = new Set();
  for (const lf of LOG_LEAD_FILES) {
    const p = path.join(ROOT, lf);
    if (!fs.existsSync(p)) continue;
    const buf = fs.readFileSync(p);
    const t = buf[0] === 0xff && buf[1] === 0xfe ? buf.toString("utf16le") : buf.toString("utf8");
    const re = new RegExp(`"id":\\s*"(gdi_[a-z0-9_]+)",\\s*"hotelId":\\s*"${HOTEL_ID}",\\s*"organizationName":\\s*"([^"]+)",\\s*"title":\\s*"([^"]+)"`, "g");
    let m;
    while ((m = re.exec(t))) {
      if (seen.has(m[1])) continue;
      seen.add(m[1]);
      logOnlyLeads.push({
        id: m[1],
        organization: m[2],
        signal: m[3],
        recordedIn: lf,
        disposition: DISP.RECOVERY_LEAD_NO_LOCAL_EVIDENCE,
        reason: "Named in a prior run log as an accepted signal; the underlying run record, contacts, dates and sources are NOT present in this checkout (not in repo, authorized backup or worktrees). It cannot be re-certified or shown to customers without re-verification.",
        returnCriteria: "Re-verify the organization's signal, named buyer, dates and lodging mechanism (see paid proposal tier A), or restore the original run record from the source machine.",
      });
    }
  }
}

// ===========================================================================
// STAGE 3 — builders (Hotel Market / CMI records)
// ===========================================================================
function classifyTerritory(locationText) {
  const kw = HOTEL_CFG.demandTerritory?.classificationKeywords || {};
  const t = text(locationText).toLowerCase();
  const hit = (k) => (kw[k] || []).some((w) => t.includes(String(w).toLowerCase()));
  // order: outside > stretch > core > nearby > competitive (venue-city based, not article text)
  if (hit("OUTSIDE_REALISTIC_TERRITORY")) return "OUTSIDE_REALISTIC_TERRITORY";
  if (hit("TERRITORY_STRETCH")) return "TERRITORY_STRETCH";
  if (hit("TERRITORY_CORE")) return "TERRITORY_CORE";
  if (hit("TERRITORY_NEARBY")) return "TERRITORY_NEARBY";
  if (hit("TERRITORY_COMPETITIVE")) return "TERRITORY_COMPETITIVE";
  return "UNCLASSIFIED";
}
const revenueStreamFor = (potential) =>
  /catering|banquet|meeting|conference|training|congress|convention|seminar|reception/i.test(potential || "") ? "COMBINED_GUESTROOMS_EVENT_FNB" : "GUESTROOMS_ONLY";

function contactToDraft(c) {
  if (!c) return null;
  return {
    name: c.name,
    title: c.title,
    role: c.title,
    organization: c.organization,
    sourceUrl: c.sourceUrl,
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    ...(c.email ? { email: c.email } : {}),
    ...(c.phone ? { phone: c.phone } : {}),
    emailKind: c.emailKind,
  };
}

function buildHotelMarketOpportunity(spec) {
  const contact = contactToDraft(spec.contact);
  const start = spec.start || null;
  const end = spec.end || null;
  const pill = start ? dateLabelFor(start, end) : "Dates to be announced";
  const sources = spec.sources || [];
  const draft = {
    id: spec.id,
    displayTitle: spec.title,
    title: spec.title,
    organizationName: spec.org,
    displayCompanyName: spec.org,
    eventName: spec.title,
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    targetHotelName: HOTEL_NAME,
    whatIsHappening: spec.what,
    summaryWhat: spec.what,
    potentialHotelBusiness: spec.potential,
    potentialBusiness: spec.potential,
    whyThisHotel: spec.whyHotel,
    whyActNow: spec.whyNow,
    cardWhyNowLine: spec.whyNow.slice(0, 160),
    sources: sources.map((x) => ({ ...x, claim: x.claim || x.statement })),
    officialSource: sources[0]?.url || null,
    eventDateEvidenceText: spec.dateEvidenceText || undefined,
    attendanceEvidenceText: spec.attendanceText || undefined,
    eventLocation: spec.locationSummary || undefined,
    eventLocationSummary: spec.locationSummary || undefined,
    hotelSourcingStatusLabel: spec.sourcingLabel || "Hotel Sourcing Unconfirmed",
    demandSegmentId: spec.segmentId,
    researchSegment: spec.segmentId,
    opportunityType: spec.segmentId,
    revenueStreamClass: revenueStreamFor(spec.potential),
    guestroomThesis: spec.potential,
    hasVerifiedNamedRelevantContact: Boolean(contact),
    hasCredibleOutreachRoute: Boolean(contact),
    eventStartDate: start,
    eventEndDate: end,
    tileEventStartDate: start,
    tileEventEndDate: end,
    datePillDisplayLabel: pill,
    primaryContact: contact,
    contactName: contact?.name || null,
    contactEmail: spec.contact?.email || null,
    organizationContactEmail: spec.contact?.mailbox || null,
    cardContactSubtitle: contact?.title || (contact ? "Role Not Yet Verified" : null),
    tileContactPreview: contact
      ? {
          displayRole: contact.title || "Role Not Yet Verified",
          displayRoleKind: contact.title ? "professional_title" : "role_not_yet_verified",
          professionalTitle: contact.title || null,
        }
      : null,
    inventoryPopulations: contact ? [INVENTORY_POPULATION.SALES_OPPORTUNITIES] : [],
    hotelMarketScopeEligible: Boolean(contact),
    competitiveMarketScopeEligible: false,
    commercialStageKey: "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: "Account Development",
    inventoryLane: "QUALIFICATION_NEEDED",
    bookingWindowStatus: "QUALIFY_NOW",
    recommendedNextStep: spec.nextStep,
    recommendedAction: spec.nextStep,
    demandEvidenceClass: spec.demandClass,
    territoryClassification: spec.territory,
    hotelFitAssessment: spec.fit ? { fit: spec.fit.fit, rationale: spec.fit.rationale, ruleVersion: "yotel-geneva-lake-fit-v1", assessedAt: REF } : undefined,
    researchProvenance: spec.provenance,
    uncertainties: spec.uncertainties || [],
    sharedTenSectionDrawer: {
      sections: [
        { id: "what_is_happening", title: "What is happening", body: spec.what },
        { id: "potential_hotel_business", title: "Potential hotel business", body: spec.potential },
        { id: "why_this_hotel", title: "Why this hotel", body: spec.whyHotel },
        { id: "why_now", title: "Why now", body: spec.whyNow },
        {
          id: "who_to_contact",
          title: "Who to contact",
          body: contact
            ? `${contact.name}${contact.title ? ` — ${contact.title}` : ""} (${contact.organization || spec.org}). ${spec.contact?.email ? `Published email: ${spec.contact.email}. ` : spec.contact?.mailbox ? `Published organizer mailbox: ${spec.contact.mailbox} (ask for ${contact.name}). ` : ""}Entry point only; lodging authority not verified.`
            : "Named contact not yet verified — research stage.",
        },
        { id: "recommended_next_step", title: "Recommended next step", body: spec.nextStep },
        { id: "supporting_evidence", title: "Supporting evidence", body: sources.slice(0, 10).map((s) => `${s.statement || ""} ${s.url || ""}`.trim()).join("\n") },
        { id: "research_uncertainties", title: "Research uncertainties", body: (spec.uncertainties || []).join(" ") || "Hotel sourcing unconfirmed. Room volume unverified." },
      ],
    },
  };
  return applySalesPresentation(draft);
}

function applySalesPresentation(draft) {
  const typeKey = classifyOpportunityTypeFilter(draft);
  draft.opportunityTypeFilterCategory = typeKey;
  draft.opportunityTypeFilterLabel = OPPORTUNITY_TYPE_FILTER_LABEL[typeKey] || typeKey;
  draft.opportunityTypeLabel = draft.opportunityTypeFilterLabel;
  draft.segmentDisplayLabel = draft.opportunityTypeFilterLabel;
  const sales = resolveCustomerSalesAction(draft);
  draft.salesActionKey = sales.key;
  draft.salesActionPillLabel = sales.label;
  draft.salesActionReason = sales.reason;
  const tilePlan = planCustomerTileBadges(draft);
  draft.tileBadgePlan = tilePlan;
  draft.priorityFilterKey = tilePlan.priorityFilterKey || draft.priority;
  draft.priorityPillLabel = tilePlan.priorityPillLabel || null;
  draft.commercialStageKey = tilePlan.commercialStageKey || draft.commercialStageKey;
  draft.datePillDisplayLabel = tilePlan.datePillDisplayLabel || draft.datePillDisplayLabel;
  return attachSalesOpportunityFilterFields(draft);
}

function setExplicitDate(o, start, end) {
  const label = dateLabelFor(start, end);
  const single = !end || end === start;
  o.eventStartDate = start;
  o.eventEndDate = end || start;
  o.tileEventStartDate = start;
  o.tileEventEndDate = end || start;
  o.eventDateDisplay = label;
  o.eventDates = single ? start : `${start}/${end}`;
  o.datePillDisplayLabel = label;
  o.canonicalTiming = {
    version: "gdi-universal-timing-intelligence-v1",
    timingType: single ? "SINGLE_DATE" : "CONTINUOUS_RANGE",
    startDate: start,
    endDate: end || start,
    occurrences: [{ startDate: start, endDate: end || start, historical: false }],
    year: Number(start.slice(0, 4)),
    month: Number(start.slice(5, 7)),
    precision: single ? "DAY" : "RANGE",
    timezone: null,
    dateStatus: "VERIFIED_UPCOMING",
    dateEvidence: [{ raw: label, source: "eventStartDate/eventEndDate", startDate: start, precision: single ? "DAY" : "RANGE" }],
    dateSourceRefs: ["eventStartDate", "eventEndDate"],
    verifiedAt: REF,
    displayLabel: label,
    timingNotes: null,
    hasVerifiedUpcomingDate: true,
  };
  return label;
}
function setDrawerSection(o, id, body) {
  const s = o.sharedTenSectionDrawer?.sections?.find((x) => x.id === id);
  if (s) s.body = body;
}

// --- contract helpers --------------------------------------------------------
const evalContract = (bag) => evaluateSharedCommercialEligibility(bag, { hotel: HOTEL_CFG });
function summarizeContract(c) {
  return {
    status: c.status,
    salesReady: c.salesReady === true,
    gaps: c.gaps || [],
    gates: c.gates,
    demand: { ok: c.demand?.ok === true, reason: c.demand?.reason || null, thesisStandard: c.demand?.thesisStandard || null, mustRemainResearchContinuation: c.demand?.mustRemainResearchContinuation === true },
    buyer: {
      name: c.buyer?.person?.name || null,
      role: c.buyer?.person?.role || c.buyer?.person?.title || null,
      responsibilityOk: c.buyer?.responsibility?.ok === true,
      responsibilityCode: c.buyer?.responsibility?.code || null,
      responsibilityReason: c.buyer?.responsibility?.reason || null,
      contactEligibilityClass: c.buyer?.contactEligibilityClass || null,
      isDirectBuyer: c.buyer?.isDirectBuyer === true,
      isRelevantCommercialEntryPoint: c.buyer?.isRelevantCommercialEntryPoint === true,
      attributionInvalid: c.buyer?.attribution?.invalid === true,
    },
    route: { ok: c.route?.ok === true, reason: c.route?.reason || null, labeling: c.route?.labeling || null },
    hotelFit: { ok: c.hotelFitGate?.ok === true, reason: c.hotelFitGate?.reason || null },
    sources: { ok: c.sources?.ok === true, urlCount: c.sources?.urlCount ?? 0 },
  };
}
const GAP_SENTENCE = {
  MISSING_CREDIBLE_ORG_ACTIVITY: () => "Organizer/event activity is not independently verified.",
  MISSING_HOTEL_DEMAND_THESIS: (c) => `No evidenced hotel-demand mechanism for this property (${c.demand.reason || "demand thesis not established"}; standard ${c.demand.thesisStandard || "n/a"}).`,
  HOTEL_FIT_NOT_DEFENSIBLE_FOR_SALES_READY: (c) => `Hotel fit is not defensible for Sales Ready (${c.hotelFit.reason || "host/housing path unconfirmed"}).`,
  MISSING_NAMED_RELEVANT_BUYER: (c) => `No verified named person as buyer (${c.buyer.name || "no contact"}).`,
  BUYER_RESPONSIBILITY_UNKNOWN: (c) => `${c.buyer.name || "Named contact"} (${c.buyer.role || "role unspecified"}) — lodging/meetings remit not evidenced (${c.buyer.responsibilityReason || "unresolved"}).`,
  BUYER_ROLE_IRRELEVANT: (c) => `${c.buyer.name} (${c.buyer.role}) holds a role outside hotel/meetings buying.`,
  BUYER_AFFILIATION_MISMATCH: (c) => `${c.buyer.name}'s organizational affiliation does not match the organizer.`,
  MISSING_CONTACT_ROUTE: () => "No usable contact route (no published email, phone or contact page attached to the named contact).",
  SOURCES_INSUFFICIENT: () => "No traceable source URLs or quotes.",
  INVALID_BUYER_CONTACT_ATTRIBUTION: () => "Contact attribution is invalid (email/identity mismatch).",
  SEGMENT_ENGINE_EXCLUSION: () => "Segment engine excluded this opportunity.",
};
function whyNotSalesReady(contract, guards) {
  const out = [];
  for (const g of contract.gaps) out.push((GAP_SENTENCE[g] || (() => `Contract gap: ${g}.`))(contract));
  for (const g of guards) out.push(g);
  if (contract.demand.mustRemainResearchContinuation && !out.some((x) => /demand/i.test(x))) out.push("Demand thesis is SPECULATIVE — must remain research continuation under the commercial demand-thesis standard.");
  if (!out.length) out.push("Contract passes except a non-contract hold (see disposition reason).");
  return out;
}

// --- CMI builder ---------------------------------------------------------------
function buildCmiOpportunity(spec) {
  const base = buildHotelMarketOpportunity({
    id: spec.id,
    title: spec.title,
    org: spec.org,
    segmentId: spec.segmentId,
    start: spec.start,
    end: spec.end,
    territory: spec.territory,
    contact: spec.contact,
    what: `${spec.title} is committed at ${spec.competitorHotel} (${spec.dateLabel}). The principal housing is not available to ${HOTEL_NAME}.`,
    potential: "Future-cycle account development and/or separately evidenced overflow only — not the committed principal housing.",
    whyHotel: spec.futureCycleThesis,
    whyNow: `Competitor commitment confirmed at ${spec.competitorHotel}; develop the organizer relationship for a future cycle or residual overflow lodging.`,
    nextStep: spec.recommended,
    sources: spec.sources.map((url) => ({ url, statement: spec.title })),
    demandClass: "CONFIRMED_COMPETITOR_COMMITMENT",
    sourcingLabel: "Competitor host/block confirmed — not an open host RFP",
    uncertainties: [spec.uncertainty],
    provenance: spec.provenance,
    fit: spec.fit,
  });
  return {
    ...base,
    competitorHotel: spec.competitorHotel,
    likelyCompetitor: spec.competitorHotel,
    competitorVenueStatus: spec.competitorVenueStatus,
    competitorBookingClaim: spec.competitorBookingClaim,
    lodgingEvidence: spec.lodgingEvidence,
    housingStatus: "COMPETITOR_BLOCK_OR_SEDE_CONFIRMED",
    roomDemandStatus: spec.commitmentScope === "VENUE_AND_GUESTROOMS" ? "COMPETITOR_BLOCK_CONFIRMED" : "COMPETITOR_VENUE_CONFIRMED_GUESTROOMS_UNPUBLISHED",
    competitorBookingEvidenceClass: "VERIFIED_BOOKING",
    competitiveVenueEvidence: true,
    competitorVenueConfirmed: true,
    commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    claims: spec.claims,
    lodgingEvidenceUrl: spec.sources[0] || null,
    cmiEvidenceRegisterV1: {
      bookingType: spec.bookingType,
      commitmentScope: spec.commitmentScope,
      evidenceConfidence: "MEDIUM_HIGH",
      verifiedAt: REF,
      uncertainty: spec.uncertainty,
      futureCycleThesis: spec.futureCycleThesis,
      recurs: spec.recurs,
    },
    eventDates: `${spec.start}/${spec.end}`,
    hotelMarketScopeEligible: false,
    competitiveMarketScopeEligible: true,
    inventoryPopulations: [INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE],
    customerVisible: true,
  };
}

// --- contact choice via the contract -----------------------------------------
const ROLE_ENTRY_RE = /organi[sz]er|congress|conference|event|meeting|housing|hotel|lodging|travel|procure|purchas|achats|logistics|operations|program|administrat|facility/i;
const HOTEL_ROLE_RE = /meeting|event|conference|housing|hotel|lodging|travel|operations|program|congress|convention|logistics|accommodat|procure|purchas|achats|facility|administrat/i;
function chooseContact(lead, makeBag) {
  const cands = lead.contacts || [];
  if (!cands.length) return { chosen: null, evaluated: [] };
  const evaluated = cands.map((c, idx) => {
    const bag = makeBag(c);
    const contract = summarizeContract(evalContract(bag));
    const score =
      (contract.salesReady ? 1000 : 0) +
      (contract.buyer.responsibilityOk ? 100 : 0) +
      (contract.route.ok ? 50 : 0) +
      (c.email ? 10 : c.mailbox ? 5 : 0) +
      (HOTEL_ROLE_RE.test(c.title || "") ? 3 : 0) -
      idx * 0.01;
    return { contact: c, contract, score };
  });
  evaluated.sort((a, b) => b.score - a.score);
  return { chosen: evaluated[0], evaluated };
}

// ===========================================================================
// STAGE 4 — audit each lead
// ===========================================================================
const leadRows = [];
const hmRecords = new Map(); // leadKey -> built (pre-presentation) HM opp
const cmiRecords = new Map();
const hiddenRecordsForAirtable = [];

function leadId(lead) {
  return `gdi_opp_yotel_fe2e_${lead.key.replace(/[^a-z0-9]+/g, "_")}_${(lead.start || "undated").replace(/-/g, "")}`;
}

function baseRow(lead) {
  return {
    leadKey: lead.key,
    id: lead.noEvidence ? null : leadId(lead),
    title: lead.title,
    organization: lead.org,
    segmentId: lead.def.segmentId,
    route: lead.def.route,
    storedArtifact: lead.artifactFile,
    siblingArtifacts: lead.siblingArtifacts,
    recordedCostUsd: lead.costUsdRecorded,
    eventStartDate: lead.start || null,
    eventEndDate: lead.end || null,
    daysToStartFromRef: lead.start ? daysBetween(REF, lead.start) : null,
    dateLabel: lead.start ? dateLabelFor(lead.start, lead.end) : null,
    dateEvidence: lead.dateEvidence || null,
    publicRecheck: lead.publicChecks || [],
    territory: lead.def.locationText ? classifyTerritory(lead.def.locationText) : null,
    venue: lead.venue || lead.def.locationText || null,
  };
}

for (const lead of leads) {
  const row = baseRow(lead);
  row.salesReady = false;
  row.contactSummary = null;

  // ---- no usable evidence ------------------------------------------------------
  if (lead.noEvidence) {
    row.disposition = DISP.RESEARCH_CONTINUATION_HIDDEN;
    row.basis = lead.noEvidence;
    row.returnCriteria = "A completed stored research run with verified dates, venue, named contacts and a lodging mechanism.";
    leadRows.push(row);
    continue;
  }

  const days = row.daysToStartFromRef;
  const territory = row.territory;
  const imminentOrPast = lead.start ? lead.start < IMMINENT_CUTOFF : false;
  const hasDate = Boolean(lead.start);

  const territoryCopyKey = {
    TERRITORY_CORE: "La Côte / Nyon corridor — YOTEL's core territory.",
    TERRITORY_NEARBY: "Geneva Airport corridor — nearby territory.",
    TERRITORY_COMPETITIVE: "Geneva — a competitive, central-Geneva location; YOTEL is outside the city and needs a transfer plan.",
    TERRITORY_STRETCH: "Lausanne / cross-border — stretch territory; lodging access must be proven.",
    OUTSIDE_REALISTIC_TERRITORY: "Outside realistic territory.",
    UNCLASSIFIED: "Territory not classified.",
  }[territory];

  // ================= CMI route =================
  if (lead.def.route === "CMI") {
    const pickBag = (c) => ({ ...buildHotelMarketOpportunity(commonSpec(lead, c, territory)) });
    const { chosen } = chooseContact(lead, pickBag);
    const contact = chosen?.contact || null;
    const competitorHotel = lead.def.competitorHotel;
    const yearStr = (lead.start || "").slice(0, 4);
    const urls = [...new Set((lead.sources || []).map((s) => s.url))].slice(0, 4);
    const claims = [
      { claimType: CMI_CLAIM_TYPE.COMPETITOR_HOST_HOTEL, status: "VERIFIED", evidenceUrls: urls, statement: `${competitorHotel} named as the venue/host hotel for ${lead.title} (${yearStr}).` },
      lead.def.roomEvidence
        ? { claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK, status: "VERIFIED", evidenceUrls: urls, statement: text(lead.venueCommitment || lead.accommodationFinding).slice(0, 240) }
        : { claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK, status: "NOT_SUPPORTED", evidenceUrls: urls, statement: "No published attendee room terms found." },
      { claimType: CMI_CLAIM_TYPE.TARGET_OPEN_SOURCING, status: "NOT_SUPPORTED", evidenceUrls: urls, statement: `No open host sourcing path evidenced for ${HOTEL_NAME}.` },
    ];
    const recommended = recommendCommercialAction({
      competitorVenueStatus: COMPETITOR_VENUE_STATUS[lead.def.venueStatus],
      targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
      commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
      competitorHotel,
      contactHint: contact?.name,
      targetHotelName: HOTEL_NAME,
    });
    const spec = {
      id: row.id,
      title: lead.title,
      org: lead.org,
      segmentId: lead.def.segmentId,
      start: lead.start,
      end: lead.end,
      dateLabel: row.dateLabel,
      territory,
      contact,
      competitorHotel,
      competitorVenueStatus: COMPETITOR_VENUE_STATUS[lead.def.venueStatus],
      commitmentScope: lead.def.commitmentScope,
      bookingType: lead.def.bookingType,
      competitorBookingClaim: `${text(lead.venueCommitment).slice(0, 420) || `${competitorHotel} named for ${lead.title}`} (edition ${yearStr}).`,
      lodgingEvidence: text(lead.accommodationFinding || lead.venueCommitment).slice(0, 420) || `${competitorHotel} named for the ${yearStr} edition; separate guestroom terms not published.`,
      futureCycleThesis:
        `${sentenceTrim(lead.futureCompetitiveCapture, 320) || "Develop the organizer relationship for a future edition or separately evidenced overflow."} ${HOTEL_NAME} cannot realistically win the committed ${yearStr} principal housing. ${territoryCopyKey} Competitor relevance: ${competitorHotel} serves the same Geneva airport/Palexpo corridor conference business; YOTEL's declared competitive set is not configured, so relevance is inferred from corridor and product, not from a declared set.`.trim().slice(0, 900),
      recurs: text(lead.recurrence).slice(0, 200) || "RECURRENCE_NOT_VERIFIED",
      uncertainty: (lead.uncertainties || []).slice(0, 2).join(" ") || "Contract terms not reviewed.",
      sources: urls,
      claims,
      recommended,
      provenance: { origin: "STORED_PRIOR_RESEARCH_ZERO_COST_REUSE", artifact: lead.artifactFile, referenceDate: REF },
      fit: { fit: "COMPETITIVE_FUTURE_CYCLE", rationale: `Future-cycle / residual-overflow only. ${territoryCopyKey}` },
    };
    const o = buildCmiOpportunity(spec);
    const gates = evaluateCmiQualificationGates(o);
    const reasons = [];
    let disposition = DISP.CMI_RETAINED;
    if (!hasDate) {
      disposition = DISP.CMI_DEMOTED;
      reasons.push("No exact verified event date.");
    } else if (imminentOrPast) {
      disposition = DISP.CMI_DEMOTED;
      reasons.push(`Event starts ${lead.start} — ${days} day(s) from ${REF}; inside the ${IMMINENT_DAYS}-day window or past.`);
    }
    if (!gates.qualifiesAsCustomerCmi) {
      disposition = DISP.CMI_DEMOTED;
      reasons.push(`CMI qualification gates failed: ${gates.failedGates.join(", ")}.`);
    }
    if (!contact) reasons.push("No named contact in stored research (CMI may still retain with an organization route).");
    Object.assign(row, {
      disposition,
      kind: "CMI",
      competitorHotel,
      cmiGates: { qualifiesAsCustomerCmi: gates.qualifiesAsCustomerCmi, failedGates: gates.failedGates, evidenceStatus: gates.evidenceStatus },
      contactSummary: contact ? `${contact.name} — ${contact.title || "role n/a"}` : null,
      routeKind: contact?.emailKind || null,
      basis: reasons.join(" ") || "Competitor commitment verified from stored research; all CMI gates pass; dates exact; start date outside the imminent window. Not a Hotel Market sales opportunity — Sales Ready is never assigned (no dual classification).",
      returnCriteria: disposition === DISP.CMI_DEMOTED ? "Re-evaluate for the next edition once dates and edition-specific competitor commitments are published." : null,
      nextStep: recommended,
    });
    row.contactChoices = (chooseContact(lead, pickBag).evaluated || []).map((e) => ({ name: e.contact.name, title: e.contact.title, responsibilityOk: e.contract.buyer.responsibilityOk, routeOk: e.contract.route.ok }));
    cmiRecords.set(lead.key, { opp: o, row, contact });
    leadRows.push(row);
    continue;
  }

  // ================= HM route =================
  const makeBag = (c) => buildHotelMarketOpportunity(commonSpec(lead, c, territory));
  const { chosen, evaluated } = chooseContact(lead, makeBag);
  if (!chosen) {
    row.disposition = DISP.RESEARCH_CONTINUATION_HIDDEN;
    row.basis = "No named person in stored research — cannot form a Hotel Market card (no invented contacts).";
    row.returnCriteria = "A named relevant buyer/entry point with a published route.";
    leadRows.push(row);
    continue;
  }
  const contact = chosen.contact;
  const opp = makeBag(contact);
  const contractRaw = evalContract(opp);
  const contract = summarizeContract(contractRaw);
  const ad = evaluateProactiveAccountDevelopment(opp, { hotel: HOTEL_CFG });

  const guards = [];
  if (territory === "TERRITORY_COMPETITIVE") {
    guards.push(`TRANSFER_FRICTION_NOT_PROVEN: event venue is in central Geneva (${territoryCopyKey}) — Founex-to-venue transfer acceptance is not evidenced, so Sales Ready is withheld.`);
  }
  if (lead.hotelSourcingStatus === "venue_confirmed" && lead.def.route === "HM") {
    guards.push("MAIN_VENUE_COMMITTED_LODGING_UNCONFIRMED: the main venue is already selected and hotel sourcing for this edition is unconfirmed — at most a secondary/ancillary lodging option.");
  }

  const personOk = contract.gates?.namedRelevantBuyer === true && ROLE_ENTRY_RE.test(contact.title || "");
  const avoidResp = new Set(["BUYER_ROLE_IRRELEVANT", "BUYER_AFFILIATION_MISMATCH"]);
  const contractDemandOk = /CONFIRMED|RELIABLE/i.test(text(contract.demand.thesisStandard));
  const demandOk = lead.def.demandClass !== "SPECULATIVE_DEMAND" && contractDemandOk;
  const territoryOk = territory === "TERRITORY_CORE" || territory === "TERRITORY_NEARBY" || territory === "TERRITORY_COMPETITIVE";
  const qualifyOk =
    contract.sources.ok &&
    personOk &&
    !avoidResp.has(contract.buyer.responsibilityCode) &&
    contract.route.ok &&
    hasDate &&
    days >= IMMINENT_DAYS &&
    demandOk &&
    territoryOk &&
    !/^WATCH|NOT_DEFENSIBLE|EXCLUDE/i.test(text(lead.def.fit?.fit));

  let disposition;
  let dispositionReason;
  if (hasDate && imminentOrPast) {
    disposition = DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE;
    dispositionReason =
      days < 0
        ? `Event dates ${lead.start}${lead.end && lead.end !== lead.start ? ` to ${lead.end}` : ""} are past at ${REF}; no future edition is dated in the stored research, so none is invented.`
        : `Event starts ${lead.start} — ${days} day(s) after ${REF} (< ${IMMINENT_DAYS}-day lodging-sourcing minimum); not sellable for lodging.`;
  } else if (!hasDate) {
    disposition = DISP.RESEARCH_CONTINUATION_HIDDEN;
    dispositionReason = `No exact, verified future event date in the stored research (${lead.dateEvidence?.source || "n/a"}); cannot be shown with a defensible need date.`;
  } else if (!territoryOk) {
    disposition = DISP.RESEARCH_CONTINUATION_HIDDEN;
    dispositionReason = `Territory ${territory}: ${territoryCopyKey} Lodging access for YOTEL is not evidenced, so it is not assumed.`;
  } else if (contract.salesReady && guards.length === 0 && demandOk) {
    disposition = DISP.SALES_READY;
    dispositionReason = "Passes every gate of commercial-eligibility-contract-v1.";
  } else if (ad.accountDevelopment === true && demandOk) {
    disposition = DISP.ACCOUNT_DEVELOPMENT;
    dispositionReason = "Meets the proactive Account Development thesis (reliable-inferred demand, named entry point, dated edition) but not every Sales Ready gate.";
  } else if (qualifyOk) {
    disposition = DISP.QUALIFY_VISIBLE;
    dispositionReason = "Named, sourced, dated event with a plausible fit; does not meet the Account Development thesis or Sales Ready contract; shown honestly as Qualify Opportunity.";
  } else {
    disposition = DISP.RESEARCH_CONTINUATION_HIDDEN;
    dispositionReason = `Does not meet the Qualify bar (${[
      !contract.sources.ok && "sources",
      !personOk && "named relevant buyer",
      !contract.route.ok && "contact route",
      !demandOk && `demand evidence (contract standard ${contract.demand.thesisStandard}; analyst class ${lead.def.demandClass})`,
    ]
      .filter(Boolean)
      .join(", ") || "evidence"}); retained as research continuation and hidden from customers.`;
  }

  Object.assign(row, {
    kind: "HM",
    disposition,
    basis: dispositionReason,
    salesReady: disposition === DISP.SALES_READY,
    contact: { name: contact.name, title: contact.title, organization: contact.organization, email: contact.email, mailbox: contact.mailbox, emailKind: contact.emailKind, phone: contact.phone, sourceUrl: contact.sourceUrl },
    contactSummary: `${contact.name} — ${contact.title || "role n/a"}`,
    routeKind: contact.emailKind || (contract.route.ok ? "SOURCE_CONTACT_PAGE" : "NONE"),
    contactChoices: evaluated.map((e) => ({ name: e.contact.name, title: e.contact.title, responsibilityOk: e.contract.buyer.responsibilityOk, responsibilityCode: e.contract.buyer.responsibilityCode, routeOk: e.contract.route.ok, contractSalesReady: e.contract.salesReady, score: e.score })),
    contract,
    accountDevelopment: { ok: ad.accountDevelopment === true, status: ad.status, gaps: ad.gaps, demandConfidence: ad.demandConfidence },
    founderGuards: guards,
    demandClass: lead.def.demandClass,
    demandContractStandard: contract.demand.thesisStandard,
    demandContractReason: contract.demand.reason,
    demandRationale: lead.def.demandRationale,
    qualifyBar: "sources traceable + named contact with an organiser/housing/meetings-type role and a published route + exact date ≥21 days + contract demand standard CONFIRMED/RELIABLE_INFERRED (and analyst class not SPECULATIVE) + territory core/nearby/competitive + fit not WATCH",
    fit: lead.def.fit,
    whyNotSalesReady: disposition === DISP.SALES_READY ? [] : whyNotSalesReady(contract, guards),
    returnCriteria:
      disposition === DISP.SALES_READY || CUSTOMER_DISPOSITIONS.includes(disposition)
        ? null
        : disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE
          ? "A verified, dated future edition with a lodging-sourcing window of at least 21 days."
          : "Resolve: " + [!hasDate && "an exact verified date", !demandOk && "evidence of overnight lodging demand for this property (room block request, attendee origin, overflow shortage)", !territoryOk && "proof the venue is within a realistic transfer corridor", !personOk && "a named relevant buyer"].filter(Boolean).join("; "),
    nextStep: opp.recommendedNextStep,
  });
  hmRecords.set(lead.key, { opp, row, contact, chosen });
  leadRows.push(row);
}

/** Shared HM spec for a lead + contact. */
function commonSpec(lead, contact, territory) {
  const d = lead.def;
  const yotelFit = `${YOTEL_FIT_FACTS} ${d.fit?.rationale || ""}`.trim();
  const whyHotel = `${yotelFit} ${sentenceTrim(lead.whyHotel, 360)}`.trim();
  const uncertainties = [
    ...(lead.uncertainties || []).slice(0, 3),
    "Room volume and any hotel allocation for this edition are unverified.",
    "Transfer/shuttle acceptance between Founex and the event venue is unverified.",
  ].filter(Boolean);
  const whyNow = lead.start
    ? `Edition dates are published (${lead.start}${lead.end && lead.end !== lead.start ? ` to ${lead.end}` : ""}); qualify who selects lodging before sourcing closes.`
    : "No exact edition date is verified yet.";
  return {
    id: leadId(lead),
    title: lead.title,
    org: lead.org,
    segmentId: d.segmentId,
    start: lead.start,
    end: lead.end,
    territory,
    contact,
    what: sentenceTrim(lead.what || `${lead.title} — organized by ${lead.org}.`, 700),
    potential: sentenceTrim(lead.potential || "Possible guestroom and meeting demand; volume unverified.", 600),
    whyHotel: sentenceTrim(whyHotel, 1000),
    whyNow,
    nextStep: sentenceTrim(lead.nextStep || `Contact ${contact?.name || "the organizer"} to confirm who selects lodging.`, 600),
    sources: lead.sources || [],
    dateEvidenceText: lead.start ? `${lead.title}: ${dateLabelFor(lead.start, lead.end)} at ${lead.venue || d.locationText} (dates published by the organizer in the stored research).` : null,
    attendanceText: d.attendanceText || (lead.attendance ? `Published attendance: ${lead.attendance}` : null),
    locationSummary: d.locationSummary || (d.locationText ? `Held at ${d.locationText}.` : null),
    demandClass: d.demandClass || "SPECULATIVE_DEMAND",
    sourcingLabel: lead.hotelSourcingStatus === "venue_confirmed" ? "Main Venue Confirmed — Lodging Unconfirmed" : "Hotel Sourcing Unconfirmed",
    uncertainties,
    fit: d.fit,
    provenance: { origin: "STORED_PRIOR_RESEARCH_ZERO_COST_REUSE", artifact: lead.artifactFile, referenceDate: REF },
  };
}

// ===========================================================================
// STAGE 5 — remediate customer-bound Hotel Market rows + assemble inventory
// ===========================================================================
function finalizeHotelMarket(rec) {
  const { opp, row, contact } = rec;
  const o = cloneOpp(opp);
  const disp = row.disposition;
  if (row.eventStartDate) setExplicitDate(o, row.eventStartDate, row.eventEndDate);
  const c = o.primaryContact || {};
  c.contactClass = row.contract.buyer.responsibilityOk ? (row.contract.buyer.isDirectBuyer ? "DIRECT_BUYER" : "RELEVANT_COMMERCIAL_ENTRY_POINT") : "RESEARCH_CANDIDATE";
  c.emailKind = contact.email ? "NAMED_DIRECT" : contact.mailbox ? "ORGANIZER_MAILBOX" : null;
  o.primaryContact = c;
  o.hasVerifiedNamedRelevantContact = row.contract.buyer.responsibilityOk === true;
  o.hasCredibleOutreachRoute = row.contract.route.ok === true;
  const baseFlags = { accountDevelopmentPublished: false, researchContinuation: false };
  if (disp === DISP.SALES_READY) {
    Object.assign(o, baseFlags, { commercialActionability: COMMERCIAL_ACTIONABILITY.SALES_READY, commercialStageKey: "PURSUE_NOW", commercialStagePillLabel: "Pursue Now", bookingWindowStatus: "PURSUE_NOW", inventorySalesReady: true });
    delete o.inventoryLane;
  } else if (disp === DISP.ACCOUNT_DEVELOPMENT) {
    Object.assign(o, baseFlags, { accountDevelopmentPublished: true, commercialActionability: COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT, commercialStageKey: "QUALIFICATION_NEEDED", commercialStagePillLabel: "Account Development", inventorySalesReady: false });
  } else if (disp === DISP.QUALIFY_VISIBLE) {
    Object.assign(o, baseFlags, { commercialActionability: COMMERCIAL_ACTIONABILITY.QUALIFICATION_REQUIRED, commercialStageKey: "QUALIFICATION_NEEDED", commercialStagePillLabel: "Qualify Opportunity", inventorySalesReady: false });
  } else {
    Object.assign(o, baseFlags, { commercialActionability: COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION, researchContinuation: true, inventorySalesReady: false });
  }
  // differentiated next step, grounded in the stored recommendation
  const route = contact.email ? ` (${contact.email})` : contact.mailbox ? ` (organizer mailbox ${contact.mailbox}, ask for ${contact.name})` : "";
  const stored = text(o.recommendedNextStep);
  const caveat = disp === DISP.SALES_READY ? "" : " Qualify the lodging path first — do not pitch a block or assume overflow.";
  const nextStep = sentenceTrim(stored.includes(contact.name) ? stored : `Start with ${contact.name}${route}. ${stored}`, 560) + (caveat && !stored.toLowerCase().includes("qualify") ? caveat : "");
  o.recommendedNextStep = nextStep;
  o.recommendedAction = nextStep;
  setDrawerSection(o, "recommended_next_step", nextStep);
  if (disp !== DISP.SALES_READY) {
    const unc = Array.isArray(o.uncertainties) ? [...o.uncertainties] : [];
    unc.push(`Not Sales Ready: ${row.whyNotSalesReady.join(" ")}`);
    o.uncertainties = unc;
    setDrawerSection(o, "research_uncertainties", `${o.sharedTenSectionDrawer.sections.find((s) => s.id === "research_uncertainties")?.body || ""} Not Sales Ready: ${row.whyNotSalesReady.join(" ")}`.trim());
  }
  o.commercialCertificationV1 = {
    contract: CONTRACT_ID,
    referenceDate: REF,
    disposition: disp,
    salesReady: disp === DISP.SALES_READY,
    contractStatus: row.contract.status,
    gaps: row.contract.gaps,
    demandThesisStandard: row.contract.demand.thesisStandard,
    accountDevelopment: row.accountDevelopment.ok,
  };
  o.hotelMarketScopeEligible = true;
  o.competitiveMarketScopeEligible = false;
  o.inventoryPopulations = [INVENTORY_POPULATION.SALES_OPPORTUNITIES];
  o.inventoryPopulationPrimary = INVENTORY_POPULATION.SALES_OPPORTUNITIES;
  const clean = sanitizeCustomerCopy(o);
  let presented = applySalesPresentation(clean);
  if (disp === DISP.SALES_READY) {
    const key = CUSTOMER_SALES_ACTION.CONTACT_BUYER;
    presented.salesActionKey = key;
    presented.salesActionPillLabel = CUSTOMER_SALES_ACTION_LABEL[key];
    presented.salesActionReason = "Sales Ready under commercial-eligibility-contract-v1";
    presented.commercialStageKey = "PURSUE_NOW";
    presented.commercialStagePillLabel = "Pursue Now";
    presented = attachSalesOpportunityFilterFields(presented);
  }
  if (disp === DISP.QUALIFY_VISIBLE && presented.salesActionKey === CUSTOMER_SALES_ACTION.MONITOR) {
    const qk = CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY;
    presented.salesActionKey = qk;
    presented.salesActionPillLabel = CUSTOMER_SALES_ACTION_LABEL[qk];
    presented.salesActionReason = "Named, sourced event with exact upcoming dates; target-hotel path unconfirmed — qualify the path";
    presented = attachSalesOpportunityFilterFields(presented);
  }
  if (presented.canonicalTiming?.displayLabel) presented.datePillDisplayLabel = presented.canonicalTiming.displayLabel;
  row.salesActionAfter = { key: presented.salesActionKey, label: presented.salesActionPillLabel, stage: presented.commercialStageKey, stagePill: presented.commercialStagePillLabel };
  return presented;
}

const hmRows = leadRows.filter((r) => r.kind === "HM");
const cmiRows = leadRows.filter((r) => r.kind === "CMI");
const hmCustomerRows = hmRows.filter((r) => CUSTOMER_DISPOSITIONS.includes(r.disposition));
const hmHiddenRows = hmRows.filter((r) => !CUSTOMER_DISPOSITIONS.includes(r.disposition));
const cmiKeptRows = cmiRows.filter((r) => r.disposition === DISP.CMI_RETAINED);
const cmiDemotedRows = cmiRows.filter((r) => r.disposition === DISP.CMI_DEMOTED);
const FORCE_HM_IDS = new Set(hmRows.map((r) => r.id));
const hmHiddenIds = new Set(hmHiddenRows.map((r) => r.id));

const hmFinal = [];
for (const r of hmRows) {
  const rec = hmRecords.get(r.leadKey);
  const fin = finalizeHotelMarket(rec);
  rec.final = fin;
  if (CUSTOMER_DISPOSITIONS.includes(r.disposition)) hmFinal.push(fin);
  else hiddenRecordsForAirtable.push({ ...cloneOpp(fin), customerVisible: false, commercialActionability: COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION });
}
const cmiFinal = [];
for (const r of cmiRows) {
  const rec = cmiRecords.get(r.leadKey);
  const k = cloneOpp(rec.opp);
  if (r.eventStartDate) setExplicitDate(k, r.eventStartDate, r.eventEndDate);
  k.commercialCertificationV1 = { contract: CONTRACT_ID, referenceDate: REF, disposition: r.disposition, salesReady: false, cmiGatesPass: r.cmiGates.qualifiesAsCustomerCmi, note: "Competitive Market Intelligence — never Sales Ready." };
  const clean = sanitizeCustomerCopy(k);
  if (r.disposition === DISP.CMI_RETAINED) cmiFinal.push(clean);
  else hiddenRecordsForAirtable.push({ ...cloneOpp(clean), customerVisible: false });
}

// ---- population annotation + actionability gate ----
let allOpps = [...hmFinal.map(cloneOpp), ...cmiFinal.map(cloneOpp)];
allOpps = annotateInventoryPopulations(allOpps, { hotelId: HOTEL_ID }).map((o) => {
  const forceHm = FORCE_HM_IDS.has(o.id);
  const next = { ...o, competitiveMarketScopeEligible: !forceHm && o.competitiveMarketScopeEligible === true, hotelMarketScopeEligible: forceHm || o.competitiveMarketScopeEligible !== true };
  if (forceHm) {
    Object.assign(next, { competitiveMarketScopeEligible: false, hotelMarketScopeEligible: true, competitorVenueStatus: null, competitorHotel: null, cmiVerified: false, cmiClass: null });
  }
  return annotateCanonicalCmiContract(next);
});
allOpps = allOpps.map((o) =>
  FORCE_HM_IDS.has(o.id)
    ? { ...o, hotelMarketScopeEligible: true, competitiveMarketScopeEligible: false, inventoryPopulations: [INVENTORY_POPULATION.SALES_OPPORTUNITIES], inventoryPopulationPrimary: INVENTORY_POPULATION.SALES_OPPORTUNITIES }
    : o,
);
const hintsById = {};
for (const o of allOpps) {
  const hm = FORCE_HM_IDS.has(o.id);
  hintsById[o.id] = {
    actualEventStart: o.eventStartDate,
    actualEventEnd: o.eventEndDate || null,
    datePillDisplayLabel: o.datePillDisplayLabel,
    competitorCommitted: hm ? false : o.competitiveMarketScopeEligible === true,
    cmiVerified: hm ? false : o.competitiveMarketScopeEligible === true,
  };
}
const gate = applyCommercialActionabilityGate(allOpps, { referenceDate: REF, hintsById });
const gatedRows = gate.rows.map((o) => attachSalesOpportunityFilterFields(o));
const customerVisible = gatedRows.filter((o) => o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true).map((o) => ({ ...o, customerVisible: true }));
const gatedOut = gatedRows.filter((o) => !customerVisible.some((v) => v.id === o.id));
const hotelMarket = customerVisible.filter((o) => o.hotelMarketScopeEligible === true);
const competitive = customerVisible.filter((o) => o.competitiveMarketScopeEligible === true);
let actionabilityAssert;
try {
  actionabilityAssert = assertCustomerInventoryPassesActionabilityGate(customerVisible, { referenceDate: REF, hintsById });
} catch (e) {
  actionabilityAssert = { ok: false, error: String(e?.message || e) };
}
const dualClassified = customerVisible.filter((o) => o.hotelMarketScopeEligible === true && o.competitiveMarketScopeEligible === true);
const gateDroppedUnexpected = gatedOut.map((o) => ({ id: o.id, state: o.commercialActionabilityStateV1 }));

// post-gate rows
for (const r of hmRows) {
  const v = customerVisible.find((x) => x.id === r.id);
  r.customerVisibleFinal = Boolean(v);
  if (v) r.finalDateLabel = v.datePillDisplayLabel;
}
for (const r of cmiRows) {
  const v = customerVisible.find((x) => x.id === r.id);
  r.customerVisibleFinal = Boolean(v);
  if (v) r.finalDateLabel = v.datePillDisplayLabel;
}
// A row the disposition retained but the gate dropped must be reported (never silently lost)
const gateDemotions = [...hmRows.filter((r) => CUSTOMER_DISPOSITIONS.includes(r.disposition) && !r.customerVisibleFinal), ...cmiKeptRows.filter((r) => !r.customerVisibleFinal)];
for (const r of gateDemotions) {
  r.basis = `${r.basis} (Dropped by the actionability gate: ${gatedOut.find((o) => o.id === r.id)?.commercialActionabilityStateV1 || "state n/a"}.)`;
  r.disposition = r.kind === "CMI" ? DISP.CMI_DEMOTED : DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE;
}

// ===========================================================================
// counts
// ===========================================================================
const rejectedLegacy = legacyRows.filter((r) => [DISP.REJECTED_SYNTHETIC_CAMPAIGN_SHELL, DISP.REJECTED_VENUE_OPERATOR_OR_ORGANIZER_WRAPPER, DISP.REJECTED_NON_EVENT_WEB_LISTING].includes(r.disposition));
const afterCounts = {
  total: customerVisible.length,
  hm: hotelMarket.length,
  cmi: competitive.length,
  salesReady: hmRows.filter((r) => r.disposition === DISP.SALES_READY && r.customerVisibleFinal).length,
  accountDevelopment: hmRows.filter((r) => r.disposition === DISP.ACCOUNT_DEVELOPMENT && r.customerVisibleFinal).length,
  qualify: hmRows.filter((r) => r.disposition === DISP.QUALIFY_VISIBLE && r.customerVisibleFinal).length,
  researchContinuation:
    hmRows.filter((r) => r.disposition === DISP.RESEARCH_CONTINUATION_HIDDEN).length +
    leadRows.filter((r) => !r.kind && r.disposition === DISP.RESEARCH_CONTINUATION_HIDDEN).length +
    legacyRows.filter((r) => r.disposition === DISP.RESEARCH_CONTINUATION_HIDDEN).length,
  demoted:
    hmRows.filter((r) => r.disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE).length +
    cmiRows.filter((r) => r.disposition === DISP.CMI_DEMOTED).length +
    legacyRows.filter((r) => r.disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE).length,
  rejected: rejectedLegacy.length,
  recoveryLeadsNoEvidence: logOnlyLeads.length,
};
const totalEvaluated = legacyRows.length + leads.length + logOnlyLeads.length;
const shortfall = Math.max(0, TARGET_HM - afterCounts.hm);

// ---------------------------------------------------------------------------
// hard-gate checks on final customer inventory
// ---------------------------------------------------------------------------
const providerLeak = /\bexa\b|surfe|parallel\.ai|airtable/i.test(JSON.stringify(customerVisible.map((o) => ({ ...o, sources: undefined, cmiEvidenceUrls: undefined, claims: undefined, researchProvenance: undefined, provenance: undefined }))));
const multipleDatesRemain = customerVisible.filter((o) => /multiple dates/i.test(JSON.stringify([o.datePillDisplayLabel, o.eventDateDisplay, o.tileBadgePlan?.datePillLabel, o.tileBadgePlan?.datePillDisplayLabel, o.tileBadgePlan?.canonicalTiming?.displayLabel])));
const imminentVisible = customerVisible.filter((o) => !parseIso(o.eventStartDate) || parseIso(o.eventStartDate) < IMMINENT_CUTOFF);
const salesReadyRows = customerVisible.filter((o) => o.commercialCertificationV1?.disposition === DISP.SALES_READY);
const salesReadyAllPass = salesReadyRows.every((o) => evalContract(o).salesReady === true);
const cmiCoverage = {
  ...evaluateCmiDiscoveryCoverage({
    researchAttempted: true,
    evidenceFindingsDocumented: true,
    confirmedCompetitorBookingCount: cmiKeptRows.length,
    rejectedEvidenceCount: cmiRows.length - cmiKeptRows.length,
    completedChecklistItems: [],
  }),
  coverageNote:
    "Checklist items are deliberately NOT self-certified: CMI discovery in this task is limited to stored association/event research. Competitor host-hotel booking searches across the other segments were not re-run (zero paid), so a full CMI coverage claim is not allowed and zero-CMI would not be acceptable — retained CMI cards stand on their own edition-specific evidence.",
};

// ===========================================================================
// STAGE 6 — package staging
// ===========================================================================
let pkg = null;
let integrity = null;
let qualityGate = null;
let packageBlocker = null;
let writtenPath = null;
let stagedFromExisting = false;
const newPkgDir = path.join(PROTECTED_STORE_ROOT, HOTEL_ID, NEW_PUB_VERSION);
if (!customerVisible.length) {
  packageBlocker = "NO_CUSTOMER_ELIGIBLE_OPPORTUNITIES_TO_STAGE";
} else {
  const built = buildCustomerPublicationPackage({
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    opportunities: customerVisible,
    publicationVersion: NEW_PUB_VERSION,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
    approvalStatus: "PENDING_FOUNDER_APPROVAL",
    templateVersion: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    notes: {
      founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
      script: SCRIPT_ID,
      referenceDate: REF,
      zeroPaid: true,
      hotelMarketCount: hotelMarket.length,
      competitiveCount: competitive.length,
      salesReadyCount: afterCounts.salesReady,
      accountDevelopmentCount: afterCounts.accountDevelopment,
      qualifyCount: afterCounts.qualify,
      hotelMarketTarget: TARGET_HM,
      hotelMarketShortfall: shortfall,
      customerPublished: false,
    },
  });
  const candidatePkg = built.package || built;
  integrity = verifyPublicationPackageIntegrity(candidatePkg);
  qualityGate = runPublicationQualityGate({ opportunities: candidatePkg.opportunities || customerVisible, referenceDate: REF });
  if (integrity?.ok === false) packageBlocker = `PACKAGE_INTEGRITY_FAILED:${JSON.stringify(integrity.fails || integrity)}`;
  else if (qualityGate?.ok === false) packageBlocker = `PUBLICATION_QUALITY_GATE_BLOCKING:${JSON.stringify(qualityGate.blocking || qualityGate.summary || null)}`;
  else if (NO_STAGE) packageBlocker = "NO_STAGE_FLAG_SET_PACKAGE_BUILT_NOT_WRITTEN";
  else if (fs.existsSync(path.join(newPkgDir, "package.json"))) {
    const existing = readJson(path.join(newPkgDir, "package.json"));
    if (existing.contentFingerprintSha256 === candidatePkg.contentFingerprintSha256) {
      pkg = existing;
      stagedFromExisting = true;
      writtenPath = rel(path.join(newPkgDir, "package.json"));
    } else {
      packageBlocker = `NEW_VERSION_ALREADY_EXISTS_WITH_DIFFERENT_FINGERPRINT:${existing.contentFingerprintSha256} (immutable; rerun with --version-suffix)`;
    }
  } else {
    pkg = candidatePkg;
    const written = protectedStore.writePackage(pkg);
    writtenPath = written?.path ? rel(written.path) : rel(path.join(newPkgDir, "package.json"));
  }
  if (pkg) {
    protectedStore.writePointer(HOTEL_ID, "STAGED", {
      publicationId: pkg.publicationId,
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
      founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    });
  }
  if (!qualityGate && !integrity) integrity = { ok: true };
}
const protectedAfter = protectedSnapshot();
const protectedUnchanged = JSON.stringify(protectedBefore) === JSON.stringify(protectedAfter);

// preserve legacy evidence, then rewrite this hotel's opportunities.json only if staged
const PRESERVED_DIR = path.join(OUT, "_preserved");
fs.mkdirSync(PRESERVED_DIR, { recursive: true });
const preservedLegacyPath = path.join(PRESERVED_DIR, "opportunities.legacy-pre-full-e2e.json");
if (!fs.existsSync(preservedLegacyPath)) fs.copyFileSync(OPPS_JSON_PATH, preservedLegacyPath);
const preservedLegacySha = sha256File(preservedLegacyPath);
if (pkg && !NO_STAGE) {
  if (preservedLegacySha !== legacySha && legacySha !== sha256File(OPPS_JSON_PATH)) {
    // legacy file already rewritten by a previous run of this script — the preserved copy is authoritative
  }
  fs.writeFileSync(
    OPPS_JSON_PATH,
    `${JSON.stringify(
      {
        schema: "gdi-opportunities-doc-v1",
        hotelId: HOTEL_ID,
        hotelName: HOTEL_NAME,
        updatedAt: new Date().toISOString(),
        source: SCRIPT_ID,
        note: `STAGED_FOR_FOUNDER_REVIEW — not CUSTOMER_PUBLISHED. Legacy 99-record research file preserved at ${rel(preservedLegacyPath)} (sha256 ${preservedLegacySha}).`,
        opportunities: customerVisible,
      },
      null,
      2,
    )}\n`,
  );
}

// ===========================================================================
// STAGE 7 — Airtable dry run (never live)
// ===========================================================================
const legacyForAirtable = legacyOpps.map((o) => ({ ...cloneOpp(o), customerVisible: false, commercialActionability: COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION }));
const airtableRows = [...customerVisible, ...hiddenRecordsForAirtable, ...legacyForAirtable];
let airtable = { configured: isGdiOpportunityAirtableConfigured(), mode: "DRY_RUN", liveWrites: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({ hotelId: HOTEL_ID, dryRun: true, opportunities: airtableRows });
  airtable = {
    ...airtable,
    ok: sync?.ok ?? null,
    code: sync?.code || null,
    message: sync?.message || null,
    syncContract: sync?.syncContract || sync?.summary?.syncContract || null,
    total: sync?.total ?? sync?.summary?.total ?? null,
    wouldCreate: sync?.created ?? sync?.summary?.created ?? null,
    wouldUpdate: sync?.updated ?? sync?.summary?.updated ?? null,
    unchanged: sync?.unchanged ?? sync?.summary?.unchanged ?? null,
    failed: sync?.failed ?? sync?.summary?.failed ?? null,
    skippedExcluded: sync?.skippedExcluded ?? sync?.summary?.skippedExcluded ?? null,
    errors: (sync?.errors || sync?.summary?.errors || []).slice(0, 10),
    rowPlanSample: (sync?.rows || sync?.summary?.rows || []).slice(0, 5),
    topLevelKeys: Object.keys(sync || {}),
  };
  airtable.status = sync?.ok === false ? `DRY_RUN_${sync.code || "NOT_OK"}` : "DRY_RUN_COMPLETE";
} catch (e) {
  airtable.error = String(e?.message || e);
  airtable.status = "DRY_RUN_ERROR";
}

// ===========================================================================
// STAGE 8 — Top 10, 14-segment coverage, paid proposal
// ===========================================================================
const TIER = { SALES_READY: 1, ACCOUNT_DEVELOPMENT: 2, QUALIFY_VISIBLE: 3, CMI_RETAINED: 4, RESEARCH_CONTINUATION_HIDDEN: 5 };
const top10Candidates = [];
for (const r of [...hmRows, ...cmiRows]) {
  const live = r.eventStartDate ? r.eventStartDate >= IMMINENT_CUTOFF : false;
  const tier = TIER[r.disposition];
  if (!tier) continue;
  if (tier === 5 && !live && !(r.contact && r.eventStartDate == null)) continue; // keep only future-dated or undated-but-named hidden leads
  if (tier === 5 && !r.contact) continue;
  top10Candidates.push({
    id: r.id,
    title: r.title,
    organization: r.organization,
    lane: r.kind === "CMI" ? "Competitive Market Intelligence" : "Hotel Market",
    disposition: r.disposition,
    tier,
    dates: r.finalDateLabel || r.dateLabel || "Dates to be announced",
    start: r.eventStartDate || "9999-12-31",
    customerVisible: r.customerVisibleFinal === true,
    contact: r.contactSummary,
    note:
      r.disposition === DISP.SALES_READY
        ? "Passes the full Sales Ready contract."
        : r.disposition === DISP.ACCOUNT_DEVELOPMENT
          ? "Reliable-inferred demand with a named entry point; qualify who selects lodging before pitching."
          : r.disposition === DISP.QUALIFY_VISIBLE
            ? "Named, sourced and dated; lodging path unconfirmed."
            : r.disposition === DISP.CMI_RETAINED
              ? `Competitor-committed (${r.competitorHotel}); future-cycle / overflow intelligence only — never Sales Ready.`
              : `Not customer-visible: ${oneLine(r.basis, 160)}`,
  });
}
top10Candidates.sort((a, b) => a.tier - b.tier || a.start.localeCompare(b.start));
const top10 = top10Candidates.slice(0, 10).map((t, i) => ({ rank: i + 1, ...t }));

const segmentIds = listFourteenWorkflowSegmentIds();
const SEG_ALIAS = { associations_conferences_events: ["associations_conferences_events", "association_events"], corporate_accounts_scaling: ["corporate_accounts_scaling", "scaling_companies"], preferred_lodging_partnerships: ["preferred_lodging_partnerships", "preferred_lodging"] };
const segmentCoverageRows = segmentIds.map((seg) => {
  const aliases = SEG_ALIAS[seg] || [seg];
  const segLeads = leadRows.filter((r) => aliases.includes(r.segmentId));
  const segArtifacts = artifacts.filter((a) => a.doc && aliases.includes(a.workflowId || "") || aliases.some((al) => a.file.includes(`/deep/${al}/`)));
  const visible = segLeads.filter((r) => r.customerVisibleFinal).length;
  const status = segArtifacts.length ? (visible ? "COVERED_STORED_RESEARCH_WITH_CUSTOMER_INVENTORY" : "COVERED_STORED_RESEARCH_NO_QUALIFYING_INVENTORY") : "NOT_COVERED_IN_STORED_RESEARCH";
  return {
    segmentId: seg,
    status,
    storedArtifacts: segArtifacts.map((a) => a.file),
    leads: segLeads.map((r) => ({ title: r.title, disposition: r.disposition })),
    customerVisible: visible,
    note:
      status === "NOT_COVERED_IN_STORED_RESEARCH"
        ? "No stored deep-run evidence for this segment and discovery was not re-run (zero paid). Candidate for the paid proposal."
        : visible
          ? "Customer inventory retained."
          : "Stored candidates did not pass the commercial gates (see 03).",
  };
});
const coveredSegments = segmentCoverageRows.filter((s) => s.status.startsWith("COVERED")).length;

const deepRunsExamined = artifacts.filter((a) => a.kind === "ASSOCIATION_DEEP" || a.kind === "SEGMENT_DEEP").length;
const hmYield = deepRunsExamined ? afterCounts.hm / deepRunsExamined : 0;
const uncoveredSegments = segmentCoverageRows.filter((s) => s.status === "NOT_COVERED_IN_STORED_RESEARCH").map((s) => s.segmentId);
const tierAdeep = logOnlyLeads.length;
const PILOT_DEEP_RUNS = 20;
const observedYieldFloor = Math.max(hmYield, 0.05);
const estDeepForShortfall = shortfall > 0 ? Math.ceil(shortfall / observedYieldFloor) : 0;
const proposal = {
  contract: `${CONTRACT_ID}_PAID_RESEARCH_PROPOSAL`,
  referenceDate: REF,
  status: shortfall > 0 ? "PROPOSAL_ONLY_NOT_AUTHORIZED" : "NOT_REQUIRED",
  founderApprovalRequired: true,
  paidSpendThisTaskUsd: 0,
  basis: {
    unitCostUsdPerDeepRun: DEEP_RUN_UNIT_USD,
    unitCostSource: "Every stored YOTEL deep-run artifact records quote.maxUsd = 0.50 and costDollars.total = 0.50 (agent compute 0.20 + search 0.30; contact enrichment not included).",
    discoveryCellUnitUsd: DEEP_RUN_UNIT_USD,
    discoveryCellAssumption: "Assumed equal to one bounded agent run; the stored discovery cell costs are not itemised separately in this checkout.",
    deepRunsExamined,
    hotelMarketVisible: afterCounts.hm,
    observedHotelMarketYieldPerDeepRun: Number(hmYield.toFixed(3)),
    yieldFloorUsedForSizing: observedYieldFloor,
    shortfallVs20: shortfall,
  },
  tiers: [
    {
      tier: "A_RECOVERY_REVERIFY_LOG_ONLY_LEADS",
      purpose: "Re-verify the log-only leads whose run records are missing from this checkout (organization, named buyer, dates, lodging mechanism).",
      leads: logOnlyLeads.map((l) => l.organization),
      deepRuns: tierAdeep,
      estimatedUsd: Number((tierAdeep * DEEP_RUN_UNIT_USD).toFixed(2)),
      expectedYield: "Unknown — these were accepted by an earlier gate but are unverifiable here; treat as 0–4 Hotel Market cards.",
    },
    {
      tier: "B_SEGMENT_DISCOVERY_UNCOVERED_SEGMENTS",
      purpose: "Run discovery cells for segments with no stored deep-run evidence.",
      segments: uncoveredSegments,
      discoveryCells: uncoveredSegments.length,
      estimatedUsd: Number((uncoveredSegments.length * DEEP_RUN_UNIT_USD).toFixed(2)),
    },
    {
      tier: "C_PILOT_DEEP_RESEARCH_ON_NEW_CANDIDATES",
      phase: "PHASE_1_RECOMMENDED",
      purpose: `Pilot batch of ${PILOT_DEEP_RUNS} deep runs on dated, territory-fit candidates surfaced by tier B. Measure the real Hotel Market yield before committing to the full gap.`,
      estimatedDeepRuns: shortfall > 0 ? PILOT_DEEP_RUNS : 0,
      estimatedUsd: shortfall > 0 ? Number((PILOT_DEEP_RUNS * DEEP_RUN_UNIT_USD).toFixed(2)) : 0,
    },
  ],
  phase2ConditionalScale: {
    phase: "PHASE_2_ONLY_IF_PILOT_YIELD_IS_ADEQUATE",
    condition: "Proceed only if the Phase 1 pilot converts at least ~15% of deep runs into Hotel Market cards; otherwise report the territory's honest ceiling.",
    estimatedDeepRunsToCloseShortfall: estDeepForShortfall,
    estimatedUsd: Number((estDeepForShortfall * DEEP_RUN_UNIT_USD).toFixed(2)),
    note: `Upper-bound sizing at the observed yield floor (${Math.round(observedYieldFloor * 100)}%); not recommended up front.`,
  },
  caveats: [
    "20 Hotel Market cards may not be reachable in this territory without relaxing the La Côte / Geneva-airport focus; the observed yield from 14 stored deep runs is low because most candidates are central-Geneva venue-committed events, past events, or speculative demand.",
    "Surfe contact enrichment is on-click only and is not budgeted here.",
    "No spend is authorized by this document.",
  ],
};
proposal.recommendedUsd = Number((proposal.tiers.reduce((s, t) => s + (t.estimatedUsd || 0), 0)).toFixed(2));
proposal.recommendedCeilingUsd = Number((Math.ceil(proposal.recommendedUsd * 1.25 * 2) / 2).toFixed(2));
proposal.fullToTargetUpperBoundUsd = Number((proposal.recommendedUsd + proposal.phase2ConditionalScale.estimatedUsd).toFixed(2));

// ===========================================================================
// reports
// ===========================================================================
const slim = (r) => {
  const { _rec, record, finalRecord, ...rest } = r;
  return rest;
};
const stagedPointerAfter = protectedStore.readPointer(HOTEL_ID, "STAGED");
const git = gitBefore;

writeJson("01-HOTEL_IDENTITY_AND_COMMERCIAL_PROFILE.json", {
  contract: CONTRACT_ID,
  referenceDate: REF,
  generatedAt: new Date().toISOString(),
  zeroPaid: true,
  identity: {
    hotelId: HOTEL_ID,
    displayName: HOTEL_CFG.displayName,
    address: "Chemin Ballessert 1, 1297 Founex, Switzerland",
    country: HOTEL_CFG.country,
    city: HOTEL_CFG.city,
    aliases: HOTEL_CFG.aliases || [],
    configPath: `config/group-demand-intelligence/hotels/${HOTEL_ID}.json`,
    adpFixturePath: HOTEL_CFG.adpFixturePath || null,
    identityConfirmed,
    secondIdentityCreated: false,
    otherConfigsCollidingWithName: identityCollisions,
    requiredConfigKeysMissing: hotelConfigMissingKeys,
  },
  geography: {
    territoryLabel: HOTEL_CFG.demandTerritory?.label,
    includes: HOTEL_CFG.demandTerritory?.includes,
    evaluationQuestion: HOTEL_CFG.demandTerritory?.evaluationQuestion,
    classifications: HOTEL_CFG.demandTerritory?.displayLabels,
    note: "Founex / La Côte / Nyon / Coppet / Geneva Airport — NOT an automatic downtown-Geneva CBD fit. Central-Geneva venues carry TERRITORY_COMPETITIVE and require a stated transfer reality.",
  },
  capability: {
    rooms: HOTEL_CFG.capabilityProfile?.guestRooms ?? null,
    meetingRooms: HOTEL_CFG.capabilityProfile?.meetingRooms ?? null,
    delegateCapacityApprox: 250,
    fixtureClaims: ADP_FIXTURE ? { source: HOTEL_CFG.adpFixturePath, keys: Object.keys(ADP_FIXTURE).slice(0, 40) } : null,
    verificationCaveat: "Capacity figures are first-party published (hotel fixture and the official hotel page as captured in stored research); no independent verification — treat as published claims.",
  },
  profile: HOTEL_PROFILE ? { path: rel(HOTEL_PROFILE_PATH), keys: Object.keys(HOTEL_PROFILE).slice(0, 40) } : null,
  commercialPriorities: HOTEL_CFG.commercialPriorities || null,
  templateStages: TEMPLATE_E2E_STAGES.map((s) => ({ id: s.id, name: s.name })),
});

writeJson("02-RESEARCH_RECOVERY_INVENTORY.json", {
  contract: CONTRACT_ID,
  referenceDate: REF,
  zeroPaid: true,
  legacyOpportunitiesFile: { path: rel(OPPS_JSON_PATH), sha256BeforeRewrite: legacySha, records: legacyOpps.length, preservedCopy: rel(preservedLegacyPath), preservedCopySha256: preservedLegacySha, runId: legacyRaw.runId || null, updatedAt: legacyRaw.updatedAt || null },
  totals: { legacy: legacyOpps.length, storedArtifactLeads: leads.length, logOnlyLeads: logOnlyLeads.length, totalEvaluated },
  storedArtifacts: artifacts.map((a) => ({ file: a.file, org: a.org, kind: a.kind, workflowId: a.workflowId, recordedCostUsd: a.costUsd, error: a.error ? oneLine(a.error, 200) : null })),
  artifactRoots: ARTIFACT_ROOTS,
  logOnlyLeads,
  legacyByFamily: countBy(legacyRows, (r) => r.family),
  legacyByDisposition: legacyDispositionCounts,
  missingEvidenceNote: "The local-shadow commercial portfolio files that previously carried the accepted YOTEL Sales Ready records are absent from this checkout, authorized backups and worktrees; those leads are listed as RECOVERY_LEAD_NO_LOCAL_EVIDENCE rather than re-bought or fabricated.",
  windowsPathNote: "Several stored artifact directories end in a space; they are read with extended-length (\\\\?\\) paths.",
});

writeJson("03-OPPORTUNITY_ELIGIBILITY_AUDIT.json", {
  contract: CONTRACT_ID,
  referenceDate: REF,
  gates: [
    "lib/group-demand-intelligence/commercial-eligibility-contract-v1.js#evaluateSharedCommercialEligibility",
    "lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js#evaluateProactiveAccountDevelopment",
    "lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js#evaluateCmiQualificationGates",
    "lib/group-demand-intelligence/commercial-actionability-gate-v1.js#applyCommercialActionabilityGate",
  ],
  legacyDispositionCounts,
  storedLeadDispositionCounts: countBy(leadRows, (r) => r.disposition),
  storedLeads: leadRows.map(slim),
  logOnlyLeadDispositions: countBy(logOnlyLeads, (r) => r.disposition),
  legacy: legacyRows,
});

writeJson("04-COMMERCIAL_ACTIONABILITY_AND_EVENT_DATES.json", {
  referenceDate: REF,
  imminentCutoff: IMMINENT_CUTOFF,
  gate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  assertCustomerInventoryPassesActionabilityGate: actionabilityAssert,
  counts: gate.counts,
  gateDroppedFromCustomerView: gateDroppedUnexpected,
  gateDemotionsAfterDisposition: gateDemotions.map((r) => ({ id: r.id, title: r.title })),
  multipleDatesRemainingInCustomerInventory: multipleDatesRemain.map((o) => o.id),
  legacyPastOrImminent: legacyRows.filter((r) => r.eventStartDate && r.eventStartDate < IMMINENT_CUTOFF).map((r) => ({ id: r.id, title: r.title, start: r.eventStartDate, daysFromRef: r.daysToStartFromRef })),
  storedLeadDates: leadRows.map((r) => ({ leadKey: r.leadKey, title: r.title, start: r.eventStartDate, end: r.eventEndDate, daysFromRef: r.daysToStartFromRef, dateEvidence: r.dateEvidence, disposition: r.disposition })),
  inventoryRows: gatedRows.map((o) => ({ id: o.id, title: o.displayTitle || o.title, eventStartDate: o.eventStartDate, eventEndDate: o.eventEndDate, datePill: o.datePillDisplayLabel, actionabilityState: o.commercialActionabilityStateV1, temporalDisposition: o.commercialTemporalDisposition, customerVisible: customerVisible.some((v) => v.id === o.id) })),
  timeSensitiveNotes: {
    geneva_health_forum_2026: "GHF 2026 (2026-11-10) is past at the reference date — only the legacy shells exist and all are rejected/demoted.",
    art_geneve_2027: "Art Genève 2027 (2027-01-28) exists only as organizer/venue-operator wrappers and placeholder children — no named buyer, so none is eligible.",
  },
});

writeJson("05-FOURTEEN_SEGMENT_COVERAGE.json", {
  contract: CONTRACT_ID,
  referenceDate: REF,
  segmentRegistry: "lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js#listFourteenWorkflowSegmentIds",
  segmentIds,
  coveredSegments,
  segments: segmentCoverageRows,
  zeroCostPublicVerification: {
    mode: SKIP_PUBLIC_CHECKS ? "SKIPPED" : "FREE_HTTP_FETCH_STATIC_HTML_ONLY",
    note: "Only local files and free HTTP fetches; no Exa / Surfe / Parallel client was imported or called. Static HTML checks are best-effort (script-rendered pages may not match); results never override stored evidence, they flag items for re-verification.",
    results: PUBLIC_CHECK_RESULTS,
  },
});

writeJson("06-HOTEL_MARKET_CUSTOMER_INVENTORY.json", {
  contract: CONTRACT_ID,
  referenceDate: REF,
  target: TARGET_HM,
  hotelMarketCards: hotelMarket.length,
  shortfallVs20: shortfall,
  honestyNote: shortfall > 0 ? "Fewer honest cards were preferred over filler; shortfall is explained row-by-row in 03 and costed in 17." : "Target met.",
  dispositionCounts: countBy(hmRows, (r) => r.disposition),
  cards: hotelMarket.map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    organization: o.organizationName,
    dates: o.datePillDisplayLabel,
    stage: o.commercialStagePillLabel,
    salesAction: o.salesActionPillLabel,
    contact: o.primaryContact ? `${o.primaryContact.name} — ${o.primaryContact.title || ""}` : null,
    territory: o.territoryClassification,
    whyThisHotel: o.whyThisHotel,
    nextStep: o.recommendedNextStep,
    sources: (o.sources || []).map((s) => s.url),
  })),
  hiddenHotelMarketLeads: hmHiddenRows.map((r) => ({ id: r.id, title: r.title, disposition: r.disposition, reason: r.basis, returnCriteria: r.returnCriteria })),
});

writeJson("07-CMI_INVENTORY_AND_COVERAGE.json", {
  contract: CONTRACT_ID,
  cmiContract: "cmi-canonical-eligibility-contract-v1 (eight gates)",
  referenceDate: REF,
  separateFromHotelMarket: true,
  retained: cmiKeptRows.length,
  demoted: cmiRows.filter((r) => r.disposition === DISP.CMI_DEMOTED).length,
  competitiveSetNote: "YOTEL Geneva Lake has no declared competitive set in its hotel config; Crowne Plaza Geneva is treated as a corridor competitor (Geneva airport / Palexpo conference business) by inference and the card says so.",
  discoveryCoverage: cmiCoverage,
  zeroCmiCoverageProofNote: cmiKeptRows.length ? null : "No CMI retained — coverage proof required before declaring zero CMI.",
  rows: cmiRows.map(slim),
  cards: competitive.map((o) => ({ id: o.id, title: o.displayTitle || o.title, competitorHotel: o.competitorHotel, dates: o.datePillDisplayLabel, nextStep: o.recommendedNextStep })),
});

const surfeNote = { mode: "ON_CLICK_ONLY", calledThisTask: false, persistedOnCards: false, note: "Surfe enrichment remains a backend on-click action; no enrichment was run and none is stored on cards." };
writeJson("08-CONTACT_AND_SALES_ACTION_PLAN.json", {
  referenceDate: REF,
  contactMethod: "Contacts come only from stored research candidates. A direct email is attached only when its local part matches the person's name; otherwise a published mailbox is labeled ORGANIZER_MAILBOX. None is independently deliverability-verified.",
  surfeEnrichment: surfeNote,
  uniqueNextSteps: new Set(customerVisible.map((o) => o.recommendedNextStep)).size,
  customerCards: customerVisible.length,
  rows: [...hmRows, ...cmiRows].filter((r) => r.customerVisibleFinal).map((r) => ({
    id: r.id,
    title: r.title,
    lane: r.kind,
    contact: r.contact || null,
    contactChoices: r.contactChoices,
    routeKind: r.routeKind,
    nextStep: r.nextStep,
    salesAction: r.salesActionAfter || null,
  })),
  hiddenLeadContacts: hmHiddenRows.filter((r) => r.contact).map((r) => ({ title: r.title, contact: r.contact, nextStepIfPromoted: r.nextStep })),
});

writeJson("09-HOTEL_FIT_AND_REVENUE_STREAMS.json", {
  referenceDate: REF,
  hotel: { name: HOTEL_NAME, rooms: HOTEL_CFG.capabilityProfile?.guestRooms ?? 237, meetingRooms: HOTEL_CFG.capabilityProfile?.meetingRooms ?? 6, delegateCapacityApprox: 250, location: "Founex, Vaud (La Côte) — airport shuttle" },
  rows: [...hmRows, ...cmiRows].map((r) => {
    const lead = leads.find((l) => l.key === r.leadKey);
    return {
      id: r.id,
      title: r.title,
      disposition: r.disposition,
      territory: r.territory,
      venue: r.venue,
      demandClass: r.demandClass || (r.kind === "CMI" ? "CONFIRMED_COMPETITOR_COMMITMENT" : null),
      demandRationale: r.demandRationale || null,
      fit: r.fit || null,
      hotelFitGate: r.contract?.hotelFit || null,
      fitForNonCentralGenevaEvent:
        r.territory === "TERRITORY_COMPETITIVE"
          ? "Retained only as secondary/ancillary or future-cycle: the venue is central Geneva; YOTEL is outside the city and an airport-shuttle/transfer plan must be agreed before any proposal."
          : r.territory === "TERRITORY_STRETCH"
            ? "Stretch territory — not assumed."
            : "Within YOTEL's corridor.",
      revenueStreamClass: revenueStreamFor(lead?.potential),
    };
  }),
});

writeJson("10-SALES_READINESS_AND_TOP_10.json", {
  referenceDate: REF,
  contract: "commercial-eligibility-contract-v1",
  salesReadyCount: afterCounts.salesReady,
  zeroSalesReadyNote: afterCounts.salesReady === 0 ? "Zero Sales Ready — the evidence does not support promotion; every row explains why (no verified lodging-buyer remit, unconfirmed housing path, venue commitments or transfer friction)." : null,
  dispositionCounts: { hotelMarket: countBy(hmRows, (r) => r.disposition), cmi: countBy(cmiRows, (r) => r.disposition), legacy: legacyDispositionCounts },
  top10Method: "Ranked by tier (Sales Ready > Account Development > Qualify > CMI > named future-dated research-continuation leads) then by event date. Rows flagged customerVisible=false are NOT shown to customers.",
  top10,
  top10Count: top10.length,
  perLeadWhyNotSalesReady: hmRows.map((r) => ({ id: r.id, title: r.title, disposition: r.disposition, whyNotSalesReady: r.whyNotSalesReady })),
});

writeJson("11-NEED_DATE_VALIDATION.json", {
  referenceDate: REF,
  rule: `Lodging need dates must be exact, future and ≥ ${IMMINENT_DAYS} days out; the pill must equal the label derived from eventStartDate/eventEndDate.`,
  needPeriodsMirror: (() => {
    const p = path.join(HOTEL_DATA, "need-periods.airtable-mirror.json");
    if (!fs.existsSync(p)) return { present: false };
    const j = readJson(p);
    const arr = Array.isArray(j) ? j : j.needPeriods || j.records || j.rows || [];
    return { present: true, path: rel(p), count: arr.length, note: arr.length ? "Compared by title/date below." : "Mirror is empty — no pre-existing need periods to reconcile." };
  })(),
  rows: customerVisible.map((o) => {
    const label = dateLabelFor(parseIso(o.eventStartDate), parseIso(o.eventEndDate));
    return {
      id: o.id,
      title: o.displayTitle || o.title,
      eventStartDate: o.eventStartDate,
      eventEndDate: o.eventEndDate,
      expectedLabel: label,
      actualPill: o.datePillDisplayLabel,
      pillConsistent: o.datePillDisplayLabel === label,
      daysFromReference: daysBetween(REF, parseIso(o.eventStartDate)),
      needDateValid: Boolean(parseIso(o.eventStartDate)) && daysBetween(REF, parseIso(o.eventStartDate)) >= IMMINENT_DAYS,
      internalQualifyByHeuristic: addDays(parseIso(o.eventStartDate), -150),
      heuristicNote: "Internal timing heuristic (start − 150 days), not a published organizer deadline.",
    };
  }),
});

writeJson("12-AIRTABLE_RECONCILIATION_AND_DRY_RUN.json", {
  mode: "DRY_RUN_ONLY",
  liveWritesAuthorized: false,
  airtable,
  reconciliation: {
    legacyRecordsWithAirtableRecordId: legacyOpps.filter((o) => o._airtableRecordId).length,
    legacyRecordsTotal: legacyOpps.length,
    newCustomerVisibleIds: customerVisible.map((o) => o.id),
    hiddenRecordsPreserved: hiddenRecordsForAirtable.map((o) => o.id),
    legacyFlaggedNotCustomerVisible: legacyForAirtable.length,
    note: "In a live write, legacy shells would be flagged customerVisible=false (research continuation), never deleted.",
  },
  rowsEvaluated: airtableRows.length,
});

const staging = {
  staged: Boolean(pkg) && !NO_STAGE,
  stagedFromExistingPackage: stagedFromExisting,
  blocker: packageBlocker,
  publicationVersion: pkg?.publicationVersion || NEW_PUB_VERSION,
  publicationId: pkg?.publicationId || null,
  fingerprint: pkg?.contentFingerprintSha256 || null,
  lifecycleState: pkg ? GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED : null,
  customerPublished: false,
  packagePath: writtenPath,
  inventoryCounts: computePublicationInventoryCounts(customerVisible),
};
writeJson("13-STAGED_PUBLICATION.json", {
  ...staging,
  founderReviewLabel: pkg ? "STAGED_FOR_FOUNDER_REVIEW" : null,
  integrity,
  qualityGate: { ran: Boolean(qualityGate), ok: qualityGate?.ok ?? null, blocking: qualityGate?.blocking || null, summary: qualityGate?.summary || null },
  stagedPointerBefore,
  stagedPointerAfter,
  protectedPublicationsBefore: protectedBefore,
  protectedPublicationsAfter: protectedAfter,
  protectedPublicationsUnchanged: protectedUnchanged,
  previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
  opportunitiesJson: pkg && !NO_STAGE ? rel(OPPS_JSON_PATH) : null,
  legacyOpportunitiesPreservedAt: rel(preservedLegacyPath),
});
if (pkg && !NO_STAGE) {
  writeJson("FINGERPRINT.json", {
    hotelId: HOTEL_ID,
    publicationVersion: pkg.publicationVersion,
    publicationId: pkg.publicationId,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    opportunityCount: customerVisible.length,
  });
}

writeJson("16-RESEARCH_CONTINUATION_LEDGER.json", {
  referenceDate: REF,
  note: "Nothing is deleted. Hidden, demoted, rejected and log-only records remain preserved with return criteria.",
  hotelMarketHidden: hmHiddenRows.map((r) => ({ id: r.id, title: r.title, disposition: r.disposition, reason: r.basis, returnCriteria: r.returnCriteria, contact: r.contact || null, nextStepIfPromoted: r.nextStep || null })),
  cmiDemoted: cmiRows.filter((r) => r.disposition === DISP.CMI_DEMOTED).map((r) => ({ id: r.id, title: r.title, reason: r.basis, returnCriteria: r.returnCriteria })),
  legacyResearchContinuation: legacyRows.filter((r) => r.disposition === DISP.RESEARCH_CONTINUATION_HIDDEN).map((r) => ({ id: r.id, title: r.title, reasons: r.reasons, returnCriteria: r.returnCriteria })),
  legacyDemoted: legacyRows.filter((r) => r.disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE).map((r) => ({ id: r.id, title: r.title, start: r.eventStartDate, reasons: r.reasons, returnCriteria: r.returnCriteria })),
  legacyRejectedCount: rejectedLegacy.length,
  logOnlyLeads,
});

writeJson("17-PAID_RESEARCH_PROPOSAL_IF_NEEDED.json", proposal);

writeJson("18-GITHUB_MERGE_STATUS.json", {
  contract: "GDI_GITHUB_BRANCH_MERGE_DISCIPLINE",
  branch: git.branch,
  baseHead: git.head,
  committedByThisTask: false,
  pullRequest: null,
  ci: "NOT_RUN",
  mergeStatus: "PENDING_PARENT_TASK",
  mergeCommitSha: null,
  mainSynchronized: false,
  note: "This task intentionally did not commit, open a PR, run CI or merge — the parent task owns that. A GDI task is not technically complete until its PR is merged into main.",
  filesChangedByThisTask: [
    `scripts/${SCRIPT_ID}.mjs (new)`,
    `reports/${SCRIPT_ID}/** (new)`,
    `lib/group-demand-intelligence/bundled-publication-packages/${HOTEL_ID}/${NEW_PUB_VERSION}/ (new package, only if staged)`,
    `lib/group-demand-intelligence/bundled-publication-packages/${HOTEL_ID}/STAGED.json (pointer, only if staged)`,
    `data/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities.json (this hotel only; legacy preserved under reports/${SCRIPT_ID}/_preserved/)`,
  ],
  unrelatedDirtyWorktreeFilesNotTouched: sh("git status --porcelain -- . ':!reports'").split("\n").filter(Boolean).filter((l) => !l.includes(HOTEL_ID) && !l.includes(SCRIPT_ID)).slice(0, 80),
});

// ===========================================================================
// state, certification, founder report
// ===========================================================================
const prevBrowser = (() => {
  try {
    const prev = readJson(STATE_PATH);
    return prev?.staging?.fingerprint && prev.staging.fingerprint === staging.fingerprint ? prev.browser : null;
  } catch {
    return null;
  }
})();
const hardGates = {
  hotelConfigOk: hotelConfigMissingKeys.length === 0,
  identityConfirmed,
  protectedPublicationsUnchanged: protectedUnchanged,
  qualityGateOk: qualityGate ? qualityGate.ok !== false : true,
  actionabilityAssertOk: actionabilityAssert?.ok !== false,
  integrityOk: integrity ? integrity.ok !== false : true,
  noMultipleDates: multipleDatesRemain.length === 0,
  noProviderLeak: !providerLeak,
  noDualClassification: dualClassified.length === 0,
  noImminentVisible: imminentVisible.length === 0,
  salesReadyAllPassContract: salesReadyAllPass,
  noCustomerPublished: !JSON.stringify(stagedPointerAfter || {}).includes("CUSTOMER_PUBLISHED"),
  cmiRetainedAllPassGates: cmiKeptRows.every((r) => r.cmiGates.qualifiesAsCustomerCmi),
  noPaidProviderImports,
  paidProviderImportHits: paidImportHits,
  stagedOrNoInventory: Boolean(pkg) || !customerVisible.length || NO_STAGE,
};

const state = {
  contract: CONTRACT_ID,
  referenceDate: REF,
  generatedAt: new Date().toISOString(),
  git,
  recovery: { legacyCount: legacyRows.length, storedArtifactLeads: leads.length, logOnlyLeads: logOnlyLeads.length, totalEvaluated },
  after: afterCounts,
  shortfall,
  paidProposalUsd: { recommendedUsd: proposal.recommendedUsd, ceilingUsd: proposal.recommendedCeilingUsd },
  hotelConfigMissingKeys,
  legacyDispositionCounts,
  leadRows: leadRows.map((r) => ({ title: r.title, dateLabel: r.finalDateLabel || r.dateLabel, disposition: r.disposition, contactSummary: r.contactSummary, basis: r.basis })),
  top10,
  segmentCoverage: { coveredSegments },
  cmiDemoted: cmiRows.filter((r) => r.disposition === DISP.CMI_DEMOTED).map((r) => r.id),
  cmiCoverage: { ok: cmiCoverage?.coverageComplete === true, failReason: cmiCoverage?.failReason || null },
  staging,
  airtable: { status: airtable.status, ok: airtable.ok ?? null, code: airtable.code || null, configured: airtable.configured, wouldCreate: airtable.wouldCreate ?? null, wouldUpdate: airtable.wouldUpdate ?? null, liveWrites: 0 },
  hardGates,
  browser: prevBrowser || { status: "NOT_RUN" },
};
fs.writeFileSync(STATE_PATH, `${JSON.stringify(state, null, 2)}\n`);
const cert = buildCertification(state);
writeJson("15-FULL_E2E_CERTIFICATION.json", cert);
if (!fs.existsSync(path.join(OUT, "14-LOCAL_BROWSER_VERIFICATION.json")) || !prevBrowser) {
  writeJson("14-LOCAL_BROWSER_VERIFICATION.json", {
    contract: `${CONTRACT_ID}_BROWSER`,
    status: "NOT_RUN",
    howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify [--base-url http://127.0.0.1:8080]`,
    previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
    expectedFingerprint: staging.fingerprint,
    expectedCounts: staging.inventoryCounts,
  });
}
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), buildFounderReport(state, cert));

console.log(
  JSON.stringify(
    {
      hotelId: HOTEL_ID,
      identityConfirmed,
      totalEvaluated,
      after: afterCounts,
      legacyDispositionCounts,
      leadDispositions: Object.fromEntries(leadRows.map((r) => [r.leadKey, r.disposition])),
      top10Titles: top10.map((t) => t.title),
      shortfall,
      paidProposalUsd: proposal.recommendedUsd,
      staged: staging.staged,
      fingerprint: staging.fingerprint,
      stagingBlocker: staging.blocker,
      airtableStatus: airtable.status,
      protectedUnchanged,
      fullE2eStatus: cert.fullE2eStatus,
      hardFailures: cert.hardFailures,
      hardGates,
      gateDroppedFromCustomerView: gateDroppedUnexpected,
    },
    null,
    2,
  ),
);
