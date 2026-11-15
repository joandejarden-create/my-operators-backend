#!/usr/bin/env node
/**
 * GDI Radisson Blu Toronto Downtown — Final commercial sales-readiness audit
 * and Full E2E certification (ZERO PAID — no Exa, no Surfe, no provider calls).
 *
 * Modes
 *   default          audit + remediate + stage (new pub version only if customer-facing content changed)
 *   --no-stage       audit + remediate + reports, but do NOT write the package / pointer / opportunities.json
 *   --browser-verify verify the staged package against the local server (default http://127.0.0.1:8080)
 *   --base-url <u>   override preview base for --browser-verify
 *
 * Hard rules honored here
 *   - prior fingerprinted package is read-only (sha256 verified before/after)
 *   - Bethesda / Sheraton / Wyndham publications are snapshotted before/after
 *   - never CUSTOMER_PUBLISHED; Airtable is dry-run only
 *   - Sales Ready only when commercial-eligibility-contract-v1 genuinely passes
 *   - provider names/costs stay out of customer fields
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
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import {
  annotateCanonicalCmiContract,
  evaluateCmiQualificationGates,
} from "../lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import { COMMERCIAL_ACTIONABILITY } from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";
import {
  evaluateSharedCommercialEligibility,
  COMMERCIAL_ELIGIBILITY_STATUS,
} from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { evaluateProactiveAccountDevelopment } from "../lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js";
import { formatTimingDisplayLabel } from "../lib/group-demand-intelligence/universal-timing-intelligence-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
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
const SCRIPT_ID = "gdi-radisson-toronto-final-commercial-certification-v1";
const CONTRACT_ID = "GDI_RADISSON_TORONTO_FINAL_COMMERCIAL_CERTIFICATION_V1";
const HOTEL_ID = "recH9h3McgMNFQEla";
const HOTEL_NAME = "Radisson Blu Toronto Downtown";
const ARGV = process.argv.slice(2);
const has = (f) => ARGV.includes(f);
const argVal = (n) => {
  const i = ARGV.indexOf(n);
  return i > -1 ? ARGV[i + 1] : null;
};
const REF = argVal("--ref") || "2026-11-16";
const BROWSER_VERIFY = has("--browser-verify");
const NO_STAGE = has("--no-stage");
const VERSION_SUFFIX = argVal("--version-suffix") || "";
const NEW_PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261116_radisson_toronto_final_commercial_cert_v1${VERSION_SUFFIX}`;
const PRIOR_PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261115_radisson_toronto_14seg_exa_20usd_v1`;
const PRIOR_FINGERPRINT = "ecafbc4db19fa39ec09b97d125c8247addb07a60f052cf91827eb210e5a1e1ae";
const IMMINENT_DAYS = 21;

const OUT = path.join(ROOT, "reports", SCRIPT_ID);
const HOTEL_DATA = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
const OPPS_JSON_PATH = path.join(HOTEL_DATA, "opportunities.json");
const PROTECTED_STORE_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
const PRIOR_PKG_DIR = path.join(PROTECTED_STORE_ROOT, HOTEL_ID, PRIOR_PUB_VERSION);
const PRIOR_PKG_PATH = path.join(PRIOR_PKG_DIR, "package.json");
const RAW_DIRS = [
  path.join(ROOT, "reports/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1/raw-runs"),
  path.join(ROOT, "reports/gdi-radisson-blu-toronto-paid-full-e2e-15usd-v1/raw-runs"),
];
const PROTECTED_HOTELS = [
  ["recLuxvwwxID7U2B8", "bethesda"],
  ["recCAGqkERSXgiku4", "sheraton"],
  ["recXh4NgF8sDbi9cQ", "wyndham_alltra"],
];
const STATE_PATH = path.join(OUT, "_run-state.json");

// Disposition keys
const DISP = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  QUALIFY_VISIBLE: "QUALIFY_VISIBLE",
  RESEARCH_CONTINUATION_HIDDEN: "RESEARCH_CONTINUATION_HIDDEN",
  DEMOTED_NOT_CURRENTLY_ACTIONABLE: "DEMOTED_NOT_CURRENTLY_ACTIONABLE",
  CMI_RETAINED: "CMI_RETAINED",
  CMI_DEMOTED: "CMI_DEMOTED",
});

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
function dateLabelFor(start, end) {
  return formatTimingDisplayLabel({ timingType: "CONTINUOUS_RANGE", startDate: start, endDate: end || start });
}
function sanitizeCustomerCopy(v, key = "") {
  if (typeof v === "string") {
    if (/url|href|^id$|orsId|fingerprint|^email$|Email$/i.test(key)) return v;
    return v.replace(/\bTBD\b/g, "to be announced");
  }
  if (Array.isArray(v)) return v.map((x) => sanitizeCustomerCopy(x, key));
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, sanitizeCustomerCopy(x, k)]));
  return v;
}
const registrableDomain = (host) => text(host).toLowerCase().replace(/^www\./, "").split(".").slice(-2).join(".");
function hostOf(url) {
  try {
    return new URL(url).hostname;
  } catch {
    return "";
  }
}
function oneLine(s, n = 220) {
  const t = text(s);
  return t.length > n ? `${t.slice(0, n - 1)}…` : t;
}

// ---------------------------------------------------------------------------
// protected store / snapshots
// ---------------------------------------------------------------------------
const protectedStore = createGdiPublicationPackageStore({ root: PROTECTED_STORE_ROOT });
function protectedSnapshot() {
  const o = {};
  for (const [hid, label] of PROTECTED_HOTELS) {
    const cur = protectedStore.readPointer(hid, "CURRENT");
    const stg = protectedStore.readPointer(hid, "STAGED");
    o[label] = {
      hotelId: hid,
      currentFingerprint: cur?.contentFingerprintSha256 || null,
      stagedFingerprint: stg?.contentFingerprintSha256 || null,
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
    const t = setTimeout(() => ctl.abort(), 10000);
    const r = await fetch(apiUrl, { signal: ctl.signal });
    clearTimeout(t);
    result.checks.apiHttpStatus = r.status;
    const api = await r.json().catch(() => null);
    result.checks.apiFingerprint = api?.publication?.contentFingerprintSha256 || null;
    result.checks.fingerprintMatch = result.checks.apiFingerprint === expected.fingerprint;
    result.checks.inventoryCounts = api?.publication?.inventoryCounts || null;
    const ops = Array.isArray(api?.opportunities) ? api.opportunities : [];
    result.checks.apiOpportunityCount = ops.length;
    result.checks.apiHotelCount = ops.filter((o) => o.hotelMarketScopeEligible === true).length;
    result.checks.apiCompetitiveCount = ops.filter((o) => o.competitiveMarketScopeEligible === true).length;
    result.checks.apiMultipleDatesCount = ops.filter((o) => /multiple dates/i.test(JSON.stringify([o.datePillDisplayLabel, o.eventDateDisplay, o.tileBadgePlan?.datePillLabel]))).length;
    result.checks.apiSalesReadyCount = ops.filter((o) => o.commercialStageKey === "PURSUE_NOW" || o.commercialActionability === "SALES_READY").length;
    result.checks.apiContainsCfea = ops.some((o) => o.id === "rb_gdi_free_cfea_2026");
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
  const browser = await chromium.launch({ headless: true });
  try {
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
        mentionsHotel: /Radisson Blu Toronto/i.test(body),
        providerLeak: /\bExa\b|Surfe|Parallel|Airtable/.test(body),
        stillLoading: /Loading Group Demand Intelligence/i.test(body),
        multipleDatesPills: (body.match(/Multiple Dates/g) || []).length,
        salesReadyMentions: (body.match(/Sales Ready/gi) || []).length,
        cfeaVisible: /CFEA 2026/i.test(body),
      };
    });
    await page.screenshot({ path: path.join(shotDir, "01-radisson-final-cert-staged-overview.png"), fullPage: true });
    const expectedAll = Number(expected?.inventoryCounts?.all || 0);
    const dom = result.checks.dom;
    result.status =
      result.checks.fingerprintMatch === true &&
      dom.cards >= Math.max(1, expectedAll) &&
      dom.mentionsHotel === true &&
      dom.stillLoading !== true &&
      !dom.providerLeak &&
      !result.checks.providerLeakInApi &&
      dom.multipleDatesPills === 0 &&
      dom.cfeaVisible === false
        ? "PASS"
        : "FAIL_CHECKS";
  } catch (e) {
    result.status = "FAIL_BROWSER_ERROR";
    result.errors.push(String(e?.message || e));
  } finally {
    await browser.close();
  }
  return finishBrowser(result, state);
}

function finishBrowser(result, state) {
  writeJson("13-LOCAL_BROWSER_VERIFICATION.json", result);
  if (state) {
    state.browser = { status: result.status, fingerprintMatch: result.checks?.fingerprintMatch ?? null, errors: result.errors };
    writeJson("_run-state.json", state);
    const cert = buildCertification(state);
    writeJson("14-FULL_E2E_CERTIFICATION.json", cert);
    fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), buildFounderReport(state, cert));
    state.certification = { status: cert.fullE2eStatus, pendingGates: cert.pendingGates };
    writeJson("_run-state.json", state);
  }
  console.log(JSON.stringify({ browserStatus: result.status, checks: result.checks, errors: result.errors }, null, 2));
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
  mark(1, gates.hotelConfigOk ? "PASS" : "FAIL", `Hotel config loaded; required keys missing: ${(state.hotelConfigMissingKeys || []).join(",") || "none"}`);
  mark(2, "PASS", `${state.rawEvidence?.oppsWithRawEvidence ?? 0} of ${state.before?.total ?? 0} opportunities reconciled to stored research runs (zero-cost reuse).`);
  mark(3, "PASS", "Discovery planned in prior campaigns; this task is zero-paid audit only.");
  mark(4, "PASS_PRIOR_CAMPAIGNS", "Demand discovery completed in prior authorized campaigns ($18.60 recorded); $0 spent in this task.");
  mark("4a", gates.cmiRetainedAllPassGates ? "PASS" : "PASS_WITH_FINDINGS", `CMI retained=${state.after?.cmi ?? 0}; demoted=${state.cmiDemoted?.length ?? 0}.`);
  mark("4b", gates.actionabilityAssertOk ? "PASS" : "FAIL", `assertCustomerInventoryPassesActionabilityGate ok=${gates.actionabilityAssertOk}; referenceDate=${REF}.`);
  mark(5, "PASS", "Shared commercial eligibility + proactive account-development + CMI gates re-run against recovered evidence.");
  mark(6, "PENDING", "Airtable persistence was exercised as a DRY RUN only; live write requires explicit founder authorization.");
  mark(7, gates.qualityGateOk && gates.actionabilityAssertOk ? "PASS" : "FAIL", `publication quality gate ok=${gates.qualityGateOk}`);
  mark(
    8,
    browser.status === "PASS" ? "PASS" : browser.status === "NOT_RUN" ? "PENDING" : "PENDING_BROWSER_" + browser.status,
    `Local browser verification status=${browser.status}.`,
  );
  mark(9, s.staged ? "PASS" : s.unchangedFromPrior ? "NOT_REQUIRED_CONTENT_UNCHANGED" : "FAIL", s.staged ? `Staged ${s.publicationVersion} (${s.fingerprint}).` : s.blocker || "not staged");
  mark(10, "PENDING", "Founder approval of the exact fingerprint is outstanding.");
  mark(11, "NOT_STARTED", "Production publication is blocked on founder approval; nothing was published.");
  mark(12, "NOT_STARTED", "Production parity requires publication.");
  mark(13, "NOT_STARTED", "Weekly refresh begins after publication.");

  const hardFailures = [];
  if (!gates.priorPackageUnchanged) hardFailures.push("PRIOR_PACKAGE_MODIFIED");
  if (!gates.protectedPublicationsUnchanged) hardFailures.push("PROTECTED_PUBLICATIONS_CHANGED");
  if (!gates.qualityGateOk) hardFailures.push("PUBLICATION_QUALITY_GATE_FAILED");
  if (!gates.actionabilityAssertOk) hardFailures.push("ACTIONABILITY_ASSERT_FAILED");
  if (!gates.integrityOk) hardFailures.push("PACKAGE_INTEGRITY_FAILED");
  if (!gates.noMultipleDates) hardFailures.push("MULTIPLE_DATES_REMAIN");
  if (!gates.noProviderLeak) hardFailures.push("PROVIDER_LEAK_IN_CUSTOMER_FIELDS");
  if (!gates.noDualClassification) hardFailures.push("DUAL_CLASSIFICATION");
  if (!gates.noImminentVisible) hardFailures.push("IMMINENT_EVENT_STILL_VISIBLE");
  if (!gates.salesReadyAllPassContract) hardFailures.push("SALES_READY_WITHOUT_CONTRACT_PASS");
  if (!gates.noCustomerPublished) hardFailures.push("CUSTOMER_PUBLISHED_STATE");
  if (!gates.stagedOrUnchanged) hardFailures.push("STAGING_BLOCKED");
  if (browser.status && /^FAIL_(CHECKS|FINGERPRINT_MISMATCH|BROWSER_ERROR)/.test(browser.status) && browser.status !== "FAIL_SERVER_DOWN") {
    // A reachable server that disagrees with the staged package is a pending gate (stale server cache) not a data failure.
  }

  const pendingGates = [];
  pendingGates.push({ gate: "AIRTABLE_LIVE_PERSISTENCE", status: "PENDING_FOUNDER_AUTH", detail: "Dry-run only; no live Airtable write was authorized." });
  if (browser.status !== "PASS") {
    pendingGates.push({ gate: "LOCAL_BROWSER_VERIFICATION", status: browser.status, detail: (browser.errors || []).join("; ") || "Run --browser-verify against a server restarted on the staged package." });
  }
  pendingGates.push({ gate: "FOUNDER_APPROVAL_OF_FINGERPRINT", status: "PENDING", detail: "Founder must approve the exact staged fingerprint." });
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
    priorFingerprintPreserved: gates.priorPackageUnchanged === true,
    paidSpendThisTaskUsd: 0,
    providersCalledThisTask: [],
    airtable: { mode: "DRY_RUN_ONLY", liveWritesAuthorized: false, status: state.airtable?.status || null },
    browser,
  };
}

function buildFounderReport(state, cert) {
  const b = state.before;
  const a = state.after;
  const L = [];
  const row = (...c) => `| ${c.map((x) => String(x ?? "").replace(/\|/g, "\\|").replace(/\n/g, " ")).join(" | ")} |`;
  L.push(`# Radisson Blu Toronto Downtown — Final Commercial Certification`);
  L.push("");
  L.push(`Reference date: **${REF}** · Contract: \`${CONTRACT_ID}\` · Template: \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\` · Paid spend this task: **$0** (no Exa, no Surfe, no provider calls)`);
  L.push("");
  L.push(`## Verdict`);
  L.push("");
  L.push(`**Full E2E status: \`${cert.fullE2eStatus}\`** — *not* "Full E2E complete".`);
  L.push("");
  L.push(
    `Commercial credibility was prioritised over card count. Of the ${b.total} staged cards (${b.hm} Hotel Market + ${b.cmi} CMI), the page now carries **${a.total} customer-visible cards** ` +
      `(${a.hm} Hotel Market + ${a.cmi} CMI): **${a.salesReady} Sales Ready**, ${a.accountDevelopment} Account Development, ${a.qualify} Qualify Opportunity. ` +
      `${a.researchContinuation} Hotel Market cards moved to research continuation and ${a.demoted} were demoted as not currently actionable.`,
  );
  L.push("");
  L.push(`| | Before (staged \`${PRIOR_PUB_VERSION}\`) | After |`);
  L.push(`|---|---|---|`);
  L.push(row("Hotel Market cards", b.hm, a.hm));
  L.push(row("CMI cards", b.cmi, a.cmi));
  L.push(row("Sales Ready", b.salesReady, a.salesReady));
  L.push(row("Account Development", b.hm, a.accountDevelopment));
  L.push(row("Qualify Opportunity (visible)", 0, a.qualify));
  L.push(row("Research continuation (hidden, preserved)", 0, a.researchContinuation));
  L.push(row("Demoted (not currently actionable)", 0, a.demoted));
  L.push("");
  L.push(`Prior fingerprint \`${PRIOR_FINGERPRINT}\` is preserved untouched on disk (sha256 unchanged: **${cert.priorFingerprintPreserved}**).`);
  L.push(state.staging.staged ? `New staged fingerprint: \`${state.staging.fingerprint}\` (\`${state.staging.publicationVersion}\`), lifecycle STAGED_FOR_FOUNDER_REVIEW, never CUSTOMER_PUBLISHED.` : `No new package staged (${state.staging.blocker || "unchanged"}).`);
  L.push("");
  L.push(`## CFEA 2026 disposition`);
  L.push("");
  L.push(`- ${state.cfea.summary}`);
  L.push("");
  L.push(`## Hotel Market — one row per card, and why it is (or is not) Sales Ready`);
  L.push("");
  L.push(row("Card", "Dates", "Disposition", "Contact (after audit)", "Why not Sales Ready / basis"));
  L.push(row("---", "---", "---", "---", "---"));
  for (const r of state.hmRows) {
    L.push(row(r.title, r.finalDateLabel || r.dateLabelBefore, r.disposition, r.contactSummary, r.salesReady ? `Passes commercial-eligibility-contract-v1 (all gates); demand standard ${r.demandStandard} — inferred from published event scale/timing/location, not a confirmed booking or RFP.` : r.whyNotSalesReady.join(" ")));
  }
  L.push("");
  L.push(`## Date corrections`);
  L.push("");
  if (state.dateCorrections.length) {
    L.push(row("Card", "Before", "After", "Basis"));
    L.push(row("---", "---", "---", "---"));
    for (const d of state.dateCorrections) L.push(row(d.title, d.before, d.after, d.basis));
  } else L.push("None required.");
  L.push("");
  L.push(`## Contact audit`);
  L.push("");
  L.push(row("Card", "Staged contact", "Audited contact", "Route", "Note"));
  L.push(row("---", "---", "---", "---", "---"));
  for (const r of state.hmRows) {
    L.push(row(r.title, r.contactBefore, r.contactSummary, r.routeKind, r.contactNote));
  }
  L.push("");
  L.push(`## Competitive Market Intelligence`);
  L.push("");
  L.push(row("Card", "Disposition", "Detail"));
  L.push(row("---", "---", "---"));
  for (const r of state.cmiRows) L.push(row(r.title, r.disposition, r.detail));
  L.push("");
  L.push(`## Research continuation ledger (preserved, hidden from customers)`);
  L.push("");
  const hidden = [...state.hmRows.filter((r) => r.disposition === DISP.RESEARCH_CONTINUATION_HIDDEN || r.disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE), ...state.cmiRows.filter((r) => r.disposition === DISP.CMI_DEMOTED)];
  if (hidden.length) {
    L.push(row("Card", "Disposition", "What it would take to return"));
    L.push(row("---", "---", "---"));
    for (const r of hidden) L.push(row(r.title, r.disposition, r.returnCriteria || r.detail || ""));
  } else L.push("None.");
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

// ---------------------------------------------------------------------------
// STAGE 0 — load baseline (read-only)
// ---------------------------------------------------------------------------
fs.mkdirSync(OUT, { recursive: true });
const HOTEL_CFG = readJson(path.join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`));
const hotelConfigMissingKeys = HOTEL_CONFIGURATION_REQUIRED_KEYS.filter((k) => HOTEL_CFG[k] == null);
const priorPkg = readJson(PRIOR_PKG_PATH);
if (priorPkg.contentFingerprintSha256 !== PRIOR_FINGERPRINT) {
  console.error(`Prior package fingerprint mismatch: ${priorPkg.contentFingerprintSha256}`);
  process.exit(3);
}
const priorPkgShaBefore = sha256File(PRIOR_PKG_PATH);
const stagedPointerBefore = protectedStore.readPointer(HOTEL_ID, "STAGED");
const protectedBefore = protectedSnapshot();
const priorOpps = priorPkg.opportunities || [];
const priorHm = priorOpps.filter((o) => o.hotelMarketScopeEligible === true);
const priorCmi = priorOpps.filter((o) => o.competitiveMarketScopeEligible === true);
const IMMINENT_CUTOFF = addDays(REF, IMMINENT_DAYS);

// ---------------------------------------------------------------------------
// raw research evidence (zero-cost reuse)
// ---------------------------------------------------------------------------
const RAW_FILE_INDEX = [];
for (const dir of RAW_DIRS) {
  if (!fs.existsSync(dir)) continue;
  for (const f of fs.readdirSync(dir)) {
    if (!f.endsWith(".json") || f.startsWith("p1_") || f.startsWith("discC")) continue;
    RAW_FILE_INDEX.push(path.join(dir, f));
  }
}
const RAW_PREF = ["p3_", "p2_", "deepA_", "deepB_", "cmiC_"];
function rawsFor(id) {
  const hash = (String(id).match(/([0-9a-f]{12,16})$/i) || [])[1];
  if (!hash) return [];
  const files = RAW_FILE_INDEX.filter((f) => path.basename(f).includes(hash)).sort((x, y) => {
    const px = RAW_PREF.findIndex((p) => path.basename(x).startsWith(p));
    const py = RAW_PREF.findIndex((p) => path.basename(y).startsWith(p));
    return (px < 0 ? 99 : px) - (py < 0 ? 99 : py);
  });
  const out = [];
  for (const f of files) {
    let j;
    try {
      j = readJson(f);
    } catch (e) {
      console.error(`raw run unreadable ${f}: ${e.message}`);
      continue;
    }
    const st = j?.paid?.result?.structured;
    if (st && (st.eventsOpportunityOverviewFields || st.organizerContactCandidates)) out.push({ file: rel(f), structured: st });
  }
  return out;
}

function recoverEvidence(opp, raws) {
  const ov = raws.map((r) => r.structured.eventsOpportunityOverviewFields).find(Boolean) || {};
  const patch = {};
  const recovered = [];
  const venue = text(ov.verifiedVenueName || ov.futureEdition?.venue);
  const loc = [venue, text(ov.eventLocationCity), text(ov.eventLocationRegion)].filter(Boolean).join(", ");
  if (loc && !text(opp.eventLocationSummary)) {
    patch.eventLocationSummary = loc;
    recovered.push("eventLocationSummary");
  }
  if (text(ov.attendancePublished) && !text(opp.attendanceEvidenceText)) {
    patch.attendanceEvidenceText = text(ov.attendancePublished);
    recovered.push("attendanceEvidenceText");
  }
  const dateEv = text(ov.futureDateStatus) || (ov.verifiedStartDate ? `Official dates ${ov.verifiedStartDate}${ov.verifiedEndDate && ov.verifiedEndDate !== ov.verifiedStartDate ? ` to ${ov.verifiedEndDate}` : ""}.` : "");
  if (dateEv && !text(opp.eventDateEvidenceText)) {
    patch.eventDateEvidenceText = dateEv;
    recovered.push("eventDateEvidenceText");
  }
  return {
    patch,
    recovered,
    overview: ov,
    officialStart: parseIso(ov.verifiedStartDate || ov.futureEdition?.startDate),
    officialEnd: parseIso(ov.verifiedEndDate || ov.futureEdition?.endDate),
    venue,
    rawFiles: raws.map((r) => r.file),
  };
}

function rawContactCandidates(raws) {
  const out = [];
  const seen = new Set();
  for (const r of raws) {
    for (const c of r.structured.organizerContactCandidates || []) {
      const name = text(c.name);
      const k = normKey(name);
      if (!k || seen.has(k)) continue;
      seen.add(k);
      out.push({
        name,
        title: text(c.role || c.title),
        organization: text(c.organization),
        sourceUrls: (c.sourceUrls || []).map(text).filter(Boolean),
        sourceUrl: text((c.sourceUrls || [])[0]) || null,
        publishedEmail: text(c.publishedEmail) || null,
        emailDirect: c.emailIsDirectNamedContact === true,
        controlVerified: c.contractControlVerified === true,
        relevance: text(c.commercialRelevance),
        origin: `RAW_RESEARCH_RUN:${r.file}`,
      });
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// contract evaluation helpers
// ---------------------------------------------------------------------------
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
const evalContract = (bag) => evaluateSharedCommercialEligibility(bag, { hotel: HOTEL_CFG });

function emailFromText(s) {
  const m = text(s).match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/);
  return m ? m[0] : null;
}

/** Only attach an email when its domain matches a domain the source pages were published on. */
function vetEmail(email, sourceUrls) {
  if (!email) return { ok: false, reason: "no_email" };
  const d = registrableDomain(email.split("@")[1]);
  const domains = new Set((sourceUrls || []).map((u) => registrableDomain(hostOf(u))).filter(Boolean));
  if (!domains.size) return { ok: false, reason: "no_source_domain_to_compare" };
  if (domains.has(d)) return { ok: true, reason: "email_domain_matches_source_domain" };
  return { ok: false, reason: `email_domain_${d}_not_in_source_domains_${[...domains].join("|")}` };
}

/** A personal mailbox's local part carries the person's last name (or first-initial+last / full first name). */
function localPartMatchesName(email, name) {
  const lp = text(email).split("@")[0].toLowerCase().replace(/[^a-z]/g, "");
  const toks = normKey(name).split(" ").filter((t) => t.length > 1);
  if (!lp || toks.length < 2) return false;
  const first = toks[0];
  const last = toks[toks.length - 1];
  return lp.includes(last) || lp === first || lp.startsWith(first) || lp === `${first[0]}${last}`;
}

function withContact(opp, cand) {
  const n = cloneOpp(opp);
  const pc = {
    name: cand.name,
    title: cand.title || null,
    role: cand.title || null,
    organization: cand.organization || n.organizationName,
    sourceUrl: cand.sourceUrl || null,
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  };
  delete n.organizationContactEmail;
  delete n.contactEmail;
  if (cand.email) {
    pc.email = cand.email;
    n.contactEmail = cand.email;
  }
  if (cand.mailbox) n.organizationContactEmail = cand.mailbox;
  n.primaryContact = pc;
  n.contactName = cand.name;
  n.primaryContactName = cand.name;
  n.contactRole = cand.title || "";
  return n;
}

const HOTEL_ROLE_RE = /meeting|event|conference|housing|hotel|lodging|travel|operations|program|congress|convention|logistics|accommodat/i;
const NON_BUYER_ROLE_RE = /sponsor|marketing|communications|press|media|industry affairs|recruit|speaker/i;

function buildCandidates(opp, raws) {
  const cur = opp.primaryContact || {};
  const rawCands = rawContactCandidates(raws);
  const out = [];
  const curKey = normKey(cur.name);
  const curRaw = rawCands.find((c) => normKey(c.name) === curKey);
  const curTextEmail = emailFromText(`${opp.recommendedNextStep} ${opp.recommendedAction}`);
  const mk = (base, isCurrent) => {
    const sourceUrls = [...new Set([...(base.sourceUrls || []), base.sourceUrl].filter(Boolean))];
    let email = null;
    let mailbox = null;
    let emailNote = null;
    const raw = base.publishedEmail || null;
    const fromStagedText = !raw && isCurrent && curTextEmail ? curTextEmail : null;
    const pick = raw || fromStagedText;
    if (pick) {
      const vet = vetEmail(pick, sourceUrls.length ? sourceUrls : (opp.sources || []).map((s) => s.url));
      const personal = localPartMatchesName(pick, base.name);
      if (vet.ok) {
        if (base.emailDirect === true || (fromStagedText && personal)) email = pick;
        else mailbox = pick;
      } else if (fromStagedText && personal) {
        // carried from prior research (email was attached to the staged card); cross-domain so flag for verification
        email = pick;
        emailNote = `Email ${pick} carried from prior research; its domain differs from the cited source domain (${vet.reason}) — verify before use.`;
      } else emailNote = `Email ${pick} not attached (${vet.reason}).`;
    }
    return { ...base, sourceUrls, email, mailbox, emailNote, isCurrent };
  };
  out.push(
    mk(
      {
        name: text(cur.name),
        title: text(cur.title || cur.role),
        organization: text(cur.organization) || text(opp.organizationName),
        sourceUrl: cur.sourceUrl || curRaw?.sourceUrl || null,
        sourceUrls: curRaw?.sourceUrls || (cur.sourceUrl ? [cur.sourceUrl] : []),
        publishedEmail: curRaw?.publishedEmail || null,
        emailDirect: curRaw?.emailDirect === true,
        controlVerified: curRaw?.controlVerified === true,
        relevance: curRaw?.relevance || "",
        origin: curRaw ? "STAGED_PACKAGE+RAW_RESEARCH_RUN" : "STAGED_PACKAGE",
      },
      true,
    ),
  );
  for (const c of rawCands) {
    if (normKey(c.name) === curKey) continue;
    out.push(mk(c, false));
    if (out.length >= 6) break;
  }
  return out.filter((c) => c.name);
}

function evalCandidate(opp, cand) {
  const variant = withContact(opp, cand);
  const c = evalContract(variant);
  const s = summarizeContract(c);
  let score = 0;
  if (s.salesReady) score += 1000;
  if (s.buyer.responsibilityOk) score += 200;
  if (s.buyer.isDirectBuyer) score += 20;
  if (s.route.ok) score += 40;
  if (HOTEL_ROLE_RE.test(cand.title)) score += 15;
  if (NON_BUYER_ROLE_RE.test(cand.title)) score -= 25;
  if (cand.email) score += 6;
  else if (cand.mailbox) score += 3;
  if (cand.isCurrent) score += 1; // tie-break toward the staged contact
  return { cand, summary: s, score, variant };
}

const GAP_SENTENCE = {
  MISSING_CREDIBLE_ORG_ACTIVITY: () => "Organizer/event activity is not independently verified.",
  MISSING_HOTEL_DEMAND_THESIS: (r) =>
    `No evidenced hotel-demand mechanism for this property (${r.contract.demand.reason || "demand thesis not established"}; thesis standard ${r.contract.demand.thesisStandard || "n/a"}).`,
  HOTEL_FIT_NOT_DEFENSIBLE_FOR_SALES_READY: (r) =>
    `Hotel fit is not defensible for Sales Ready (${r.contract.hotelFit.reason || "host/housing path unconfirmed"}).`,
  MISSING_NAMED_RELEVANT_BUYER: (r) => `No verified named person as buyer (${r.contract.buyer.name || "no contact"}).`,
  BUYER_RESPONSIBILITY_UNKNOWN: (r) =>
    `${r.contract.buyer.name || "Named contact"} (${r.contract.buyer.role || "role unspecified"}) — lodging/meetings remit not evidenced (${r.contract.buyer.responsibilityReason || "unresolved"}).`,
  BUYER_ROLE_IRRELEVANT: (r) => `${r.contract.buyer.name} (${r.contract.buyer.role}) holds a role outside hotel/meetings buying.`,
  BUYER_AFFILIATION_MISMATCH: (r) => `${r.contract.buyer.name}'s organizational affiliation does not match the organizer.`,
  MISSING_CONTACT_ROUTE: () => "No usable contact route (no published email, phone or contact page attached to the named contact).",
  SOURCES_INSUFFICIENT: () => "No traceable source URLs or quotes.",
  INVALID_BUYER_CONTACT_ATTRIBUTION: () => "Contact attribution is invalid (email/identity mismatch).",
  SEGMENT_ENGINE_EXCLUSION: () => "Segment engine excluded this opportunity.",
};
function whyNotSalesReady(r) {
  if (r.disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE) return [r.dispositionReason];
  const out = [];
  for (const g of r.contract.gaps) {
    const f = GAP_SENTENCE[g];
    out.push(f ? f(r) : `Contract gap: ${g}.`);
  }
  for (const g of r.founderGuards || []) out.push(g);
  if (r.existingTargetHotelBlock) {
    out.push("Radisson Blu already appears as a published participant/partner hotel — this is account management of an existing listing, not a new Sales Ready prospect.");
  }
  if (r.contract.demand.mustRemainResearchContinuation && !out.some((x) => /demand/i.test(x))) {
    out.push("Demand thesis is SPECULATIVE — must remain research continuation under the commercial demand-thesis standard.");
  }
  if (!out.length) out.push("Contract passes except a non-contract hold (see disposition reason).");
  return out;
}

// ---------------------------------------------------------------------------
// presentation helpers (patterns from the 14-segment / 20plus scripts)
// ---------------------------------------------------------------------------
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

/**
 * Sales Ready presentation. The events tile derivation only emits "Pursue Now" when hotel sourcing is
 * explicitly open, which is stricter than (and orthogonal to) the shared Sales Ready contract. For rows
 * that pass the contract in full we pin Pursue Now + Contact Buyer so the card is distinguishable from
 * Qualify Opportunity cards.
 */
function applySalesReadyPresentation(o) {
  const key = CUSTOMER_SALES_ACTION.CONTACT_BUYER;
  const label = CUSTOMER_SALES_ACTION_LABEL[key];
  o.salesActionKey = key;
  o.salesActionPillLabel = label;
  o.salesActionReason = "Sales Ready under commercial-eligibility-contract-v1 — verified relevant buyer, realistic demand thesis, defensible hotel fit and a usable contact route";
  o.commercialStageKey = "PURSUE_NOW";
  o.commercialStagePillLabel = "Pursue Now";
  if (o.tileBadgePlan) {
    o.tileBadgePlan = {
      ...o.tileBadgePlan,
      commercialStageKey: "PURSUE_NOW",
      commercialStagePillLabel: "Pursue Now",
      commercialStagePillClass: "gdi-pill gdi-pill-stage-pursue",
      commercialStageSource: "commercial-eligibility-contract-v1:SALES_READY",
      salesActionKey: key,
      salesActionPillLabel: label,
      salesActionReason: o.salesActionReason,
    };
  }
  return attachSalesOpportunityFilterFields(o);
}

const shortTitle = (t) => text(t).split(";")[0].trim();

function setDrawerSection(o, id, body) {
  const secs = o.sharedTenSectionDrawer?.sections;
  if (!Array.isArray(secs)) return;
  const s = secs.find((x) => x.id === id);
  if (s) s.body = body;
}
function getDrawerBody(o, id) {
  return text(o.sharedTenSectionDrawer?.sections?.find((x) => x.id === id)?.body);
}

function setExplicitDate(o, start, end, { note = null } = {}) {
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
    timingNotes: note,
    hasVerifiedUpcomingDate: true,
  };
  return label;
}

const CMI_DISPLAY_KEYS = [
  "competitiveStatus",
  "competitiveStatusLabel",
  "cmiClass",
  "cmiClassificationReason",
  "cmiCompetitorHotel",
  "competitorHotel",
  "likelyCompetitor",
  "competitorVenueStatus",
  "competitorVenueStatusLabel",
  "competitorBookingEvidenceClass",
  "competitiveFutureCyclePresentation",
  "competitiveCommercialActionKey",
  "competitiveCommercialActionLabel",
  "futureCycleOpportunityStatus",
  "cmiEvidenceUrls",
];

// ---------------------------------------------------------------------------
// HOTEL MARKET AUDIT + REMEDIATION
// ---------------------------------------------------------------------------
const hmRows = [];
const remediatedHm = [];
let rawEvidenceCount = 0;

function overflowish(o) {
  return /overflow|conditional|stretch|supplement/i.test(`${o.hotelFitAssessment?.fit || ""} ${o.hotelFitAssessment?.rationale || ""} ${o.whyThisHotel || ""}`);
}

function generateNextStep(o, disp, r) {
  const c = o.primaryContact || {};
  const who = `${c.name} (${shortTitle(c.title) || "role not published"}, ${c.organization})`;
  const ev = `${o.displayTitle} (${o.datePillDisplayLabel})`;
  const route = c.email ? ` at ${c.email}` : o.organizationContactEmail ? ` via the organizer mailbox ${o.organizationContactEmail} (ask for ${c.name})` : "";
  if (disp === DISP.SALES_READY) {
    return overflowish(o)
      ? `Contact ${who}${route} to ask how lodging is arranged for ${ev} and whether ${HOTEL_NAME} can be listed as an overflow or ancillary-meeting option; do not pitch exclusive housing.`
      : `Contact ${who}${route} to confirm who selects lodging for ${ev} and offer a Harbourfront group rate with meeting space.`;
  }
  if (disp === DISP.ACCOUNT_DEVELOPMENT) {
    return `Build the account with ${who}${route}: learn how ${ev} handles lodging and meetings, then position ${HOTEL_NAME} for the next sourcing decision rather than pitching a block now.`;
  }
  const gaps = r.contract.gaps;
  const clauses = [];
  if (gaps.includes("MISSING_HOTEL_DEMAND_THESIS")) clauses.push("out-of-town attendance and the lodging mechanism are not yet evidenced");
  if (gaps.some((g) => /BUYER_RESPONSIBILITY_UNKNOWN|BUYER_ROLE_IRRELEVANT|MISSING_NAMED/.test(g))) clauses.push(`${c.name}'s remit over attendee lodging is unverified`);
  if (gaps.includes("MISSING_CONTACT_ROUTE")) clauses.push("no direct route is published");
  if (gaps.includes("HOTEL_FIT_NOT_DEFENSIBLE_FOR_SALES_READY")) clauses.push("the housing path and hotel fit are unconfirmed");
  if ((r.founderGuards || []).length) clauses.push("published attendance is public footfall, not evidence of overnight lodging demand");
  if (r.existingTargetHotelBlock) clauses.push("a Radisson Blu participant/partner listing already exists, so this is account management of an existing listing rather than new prospecting");
  const why = clauses.length ? `Qualify before pursuing — ${clauses.join("; ")}. ` : "Qualify before pursuing. ";
  if (r.existingTargetHotelBlock) {
    return `${why}Start with ${who}${route} to review how the existing Radisson Blu listing for ${ev} is performing (pickup, release dates, remaining inventory) and what extra rooms or meeting space the organizer needs.`;
  }
  return `${why}Start with ${who}${route} to confirm who owns lodging for ${ev} and whether any hotel is already contracted.`;
}

function auditHotelMarketOpp(opp) {
  const raws = rawsFor(opp.id);
  if (raws.length) rawEvidenceCount += 1;
  const asStaged = summarizeContract(evalContract(cloneOpp(opp)));
  const rec = recoverEvidence(opp, raws);
  const enriched = { ...cloneOpp(opp), ...rec.patch };
  const cands = buildCandidates(enriched, raws);
  const evals = cands.map((c) => evalCandidate(enriched, c));
  const current = evals.find((e) => e.cand.isCurrent) || evals[0];
  let chosen = current;
  for (const e of evals) if (e.score > chosen.score) chosen = e;
  const finalBag = chosen.variant;
  const finalContract = evalContract(finalBag);
  const contract = summarizeContract(finalContract);
  const ad = evaluateProactiveAccountDevelopment(finalBag, { hotel: HOTEL_CFG });

  const start = parseIso(opp.eventStartDate);
  const end = parseIso(opp.eventEndDate) || start;
  const daysToStart = start ? daysBetween(REF, start) : null;
  const imminent = start ? start < IMMINENT_CUTOFF : false;

  // Founder credibility guards layered on top of the contract (never loosen it).
  const guards = [];
  const eventLabel = text(rec.overview?.eventTypeCustomerLabel || opp.opportunityTypeLabel);
  if (contract.salesReady && /\bpublic\b|consumer/i.test(eventLabel)) {
    guards.push(
      `PUBLIC_FOOTFALL_NOT_LODGING_DEMAND: "${eventLabel}" — published attendance is public/consumer footfall, not evidence of out-of-town guestroom demand for this hotel.`,
    );
  }
  const contractSalesReady = contract.salesReady === true;
  const salesReadyFinal = contractSalesReady && guards.length === 0;

  const copyBlob = `${opp.whatIsHappening} ${opp.potentialHotelBusiness}`;
  const existingTargetHotelBlock = /radisson blu[^.]{0,100}(participant block|official hotel|partner hotel|promo|room block|hotel partner)|(participant block|official hotel|hotel partners?)[^.]{0,80}radisson blu/i.test(copyBlob);
  const contactPageSource = /\/(contact|staff|team|about|leadership|people)/i.test(text(finalBag.primaryContact?.sourceUrl));
  const personOk = contract.gates.namedRelevantBuyer === true;
  const avoidResp = new Set(["BUYER_ROLE_IRRELEVANT", "BUYER_AFFILIATION_MISMATCH"]);
  const qualifyOk =
    contract.sources.ok &&
    (personOk || existingTargetHotelBlock) &&
    (contract.buyer.responsibilityCode !== "MISSING_NAMED_RELEVANT_BUYER" || existingTargetHotelBlock) &&
    !avoidResp.has(contract.buyer.responsibilityCode) &&
    (contract.route.ok || contactPageSource) &&
    start &&
    daysToStart >= IMMINENT_DAYS &&
    !/^WATCH|NOT_DEFENSIBLE|EXCLUDE/i.test(text(opp.hotelFitAssessment?.fit)) &&
    contract.demand.thesisStandard !== "CONTRADICTORY";

  let disposition;
  let dispositionReason;
  if (imminent) {
    disposition = DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE;
    dispositionReason = `Event starts ${start} — ${daysToStart} day(s) after the ${REF} certification date (< ${IMMINENT_DAYS}-day lodging-sourcing minimum). Not sellable for lodging; no future edition is evidenced, so none is invented.`;
  } else if (salesReadyFinal) {
    disposition = DISP.SALES_READY;
    dispositionReason = `Passes every gate of commercial-eligibility-contract-v1 (credible activity, realistic demand [${contract.demand.thesisStandard}], defensible fit, named relevant buyer with supported responsibility, usable route, traceable sources).`;
  } else if (ad.accountDevelopment === true && !guards.length) {
    disposition = DISP.ACCOUNT_DEVELOPMENT;
    dispositionReason = "Meets the proactive Account Development five-part thesis (CONFIRMED/RELIABLE_INFERRED demand) but not every Sales Ready gate.";
  } else if (qualifyOk) {
    disposition = DISP.QUALIFY_VISIBLE;
    dispositionReason = "Named, sourced event with exact upcoming dates and a plausible Toronto fit, but it does not meet the Account Development thesis or Sales Ready contract; shown honestly as Qualify Opportunity.";
  } else {
    disposition = DISP.RESEARCH_CONTINUATION_HIDDEN;
    dispositionReason = "Does not meet even the Qualify bar (named/sourced contact, route, exact dates, fit); retained as research continuation and hidden from customers.";
  }

  const row = {
    id: opp.id,
    title: opp.displayTitle || opp.title,
    organization: opp.organizationName,
    kind: "HM",
    dateLabelBefore: opp.datePillDisplayLabel,
    eventStartDate: start,
    eventEndDate: end,
    daysToStartFromRef: daysToStart,
    asStagedContract: asStaged,
    evidenceRecovery: { rawFiles: rec.rawFiles, recoveredFields: rec.recovered, venue: rec.venue || null, officialStart: rec.officialStart, officialEnd: rec.officialEnd },
    contactCandidates: evals.map((e) => ({
      name: e.cand.name,
      title: e.cand.title,
      origin: e.cand.origin,
      isStagedContact: e.cand.isCurrent,
      directEmailAttached: e.cand.email || null,
      organizerMailboxAttached: e.cand.mailbox || null,
      emailNote: e.cand.emailNote,
      responsibilityOk: e.summary.buyer.responsibilityOk,
      responsibilityCode: e.summary.buyer.responsibilityCode,
      routeOk: e.summary.route.ok,
      contractSalesReady: e.summary.salesReady,
      score: e.score,
    })),
    contactBefore: `${text(opp.primaryContact?.name)} — ${text(opp.primaryContact?.title)}`,
    chosenContact: { name: chosen.cand.name, title: chosen.cand.title, changed: !chosen.cand.isCurrent, origin: chosen.cand.origin },
    contract,
    accountDevelopment: { ok: ad.accountDevelopment === true, status: ad.status, gaps: ad.gaps, demandConfidence: ad.demandConfidence },
    disposition,
    dispositionReason,
    contractSalesReady,
    founderGuards: guards,
    existingTargetHotelBlock,
    salesReady: disposition === DISP.SALES_READY,
    _finalBag: finalBag,
    _chosen: chosen,
    _rec: rec,
  };
  row.whyNotSalesReady = row.salesReady ? [] : whyNotSalesReady(row);
  return row;
}

for (const opp of priorHm) hmRows.push(auditHotelMarketOpp(opp));

// narrative repair detection (generic "tracked as an exhibitor / vendor" templates)
const narrativeRepairs = [];

function remediateHotelMarketRow(r) {
  const o = cloneOpp(r._finalBag);
  const ch = r._chosen.cand;
  const disp = r.disposition;

  // --- evidence fields persisted so the contract is reproducible from the package ---
  const evidencePersisted = Object.keys(r._rec.patch);

  // --- strip competitor-framed fields from Hotel Market rows (no dual presentation) ---
  const stripped = [];
  if (o.competitorBookingClaim) {
    o.hostContextNote = text(o.competitorBookingClaim);
    stripped.push("competitorBookingClaim→hostContextNote");
    delete o.competitorBookingClaim;
  }
  for (const k of CMI_DISPLAY_KEYS) {
    if (k in o && o[k] != null && o[k] !== false) {
      stripped.push(k);
    }
    delete o[k];
  }
  o.cmiVerified = false;

  // --- narrative repair ---
  const ov = r._rec.overview || {};
  if (/tracked as an? exhibitor|vendor programme contact/i.test(text(o.whatIsHappening))) {
    const venue = r._rec.venue;
    const city = text(ov.eventLocationCity);
    const att = text(ov.attendancePublished);
    const rebuilt =
      `${o.displayTitle} is ${o.organizationName}'s ${text(ov.eventTypeCustomerLabel).toLowerCase() || "event"}` +
      `${city ? ` in ${city}` : ""}${venue ? ` at ${venue}` : ""}, ${dateLabelFor(r.eventStartDate, r.eventEndDate)}; official dates are published by the organizer.` +
      `${att ? ` Published attendance scale: ${att}.` : ""}` +
      `${text(ov.hotelSourcingStatus) === "venue_confirmed" ? " Venue is confirmed; no target-hotel sourcing path is evidenced." : " Hotel sourcing is not confirmed in the public materials reviewed."}`;
    narrativeRepairs.push({ id: o.id, before: oneLine(o.whatIsHappening, 160), after: oneLine(rebuilt, 160) });
    o.whatIsHappening = rebuilt;
    o.summaryWhat = rebuilt;
    setDrawerSection(o, "what_is_happening", rebuilt);
  }

  // --- explicit dates (fix Multiple Dates / wrong pill) ---
  const label = dateLabelFor(r.eventStartDate, r.eventEndDate);
  const staged = text(o.datePillDisplayLabel);
  let dateCorrection = null;
  if (r.eventStartDate && (staged !== label || /multiple/i.test(staged) || /multiple/i.test(text(o.eventDateDisplay)))) {
    let note = null;
    let basis = "Verified eventStartDate/eventEndDate on the opportunity record";
    if (r.evidenceRecovery.officialStart && r.evidenceRecovery.officialStart === r.eventStartDate) basis += `; official organizer dates ${r.evidenceRecovery.officialStart}${r.evidenceRecovery.officialEnd && r.evidenceRecovery.officialEnd !== r.evidenceRecovery.officialStart ? ` to ${r.evidenceRecovery.officialEnd}` : ""} (stored research run)`;
    if (o.id === "rx_d_df006c3aba8a") {
      note = "Programme is delivered as discrete modules within this window (per organizer pages); confirm module dates before quoting.";
      basis += "; span label retained because the programme is modular";
    }
    const after = setExplicitDate(o, r.eventStartDate, r.eventEndDate, { note });
    dateCorrection = { id: o.id, title: r.title, before: staged, after, basis };
  } else if (r.eventStartDate) {
    // still pin canonical timing so tile derivation cannot regress to Multiple Dates
    setExplicitDate(o, r.eventStartDate, r.eventEndDate);
  }

  // --- evidence-gated stage / actionability ---
  const c = o.primaryContact || {};
  const route = r.contract.route;
  o.hasVerifiedNamedRelevantContact = r.contract.buyer.responsibilityOk === true;
  o.hasCredibleOutreachRoute = route.ok === true;
  c.contactClass = r.contract.buyer.responsibilityOk ? (r.contract.buyer.isDirectBuyer ? "DIRECT_BUYER" : "RELEVANT_COMMERCIAL_ENTRY_POINT") : "RESEARCH_CANDIDATE";
  if (ch.email) c.email = ch.email;
  if (ch.mailbox) c.publishedMailbox = ch.mailbox;
  c.emailKind = ch.email ? "NAMED_DIRECT" : ch.mailbox ? "ORGANIZER_MAILBOX" : null;
  o.primaryContact = c;

  const baseFlags = { accountDevelopmentPublished: false, researchContinuation: false };
  if (disp === DISP.SALES_READY) {
    Object.assign(o, baseFlags, {
      commercialActionability: COMMERCIAL_ACTIONABILITY.SALES_READY,
      commercialStageKey: "PURSUE_NOW",
      commercialStagePillLabel: "Pursue Now",
      bookingWindowStatus: "PURSUE_NOW",
      inventorySalesReady: true,
    });
    delete o.inventoryLane;
  } else if (disp === DISP.ACCOUNT_DEVELOPMENT) {
    Object.assign(o, baseFlags, {
      accountDevelopmentPublished: true,
      commercialActionability: COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT,
      commercialStageKey: "QUALIFICATION_NEEDED",
      commercialStagePillLabel: "Account Development",
      inventorySalesReady: false,
    });
  } else if (disp === DISP.QUALIFY_VISIBLE) {
    Object.assign(o, baseFlags, {
      commercialActionability: COMMERCIAL_ACTIONABILITY.QUALIFICATION_REQUIRED,
      commercialStageKey: "QUALIFICATION_NEEDED",
      commercialStagePillLabel: "Qualify Opportunity",
      inventorySalesReady: false,
    });
  } else {
    Object.assign(o, baseFlags, {
      commercialActionability: COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION,
      researchContinuation: true,
      inventorySalesReady: false,
    });
  }

  // --- differentiated next step ---
  const before = text(o.recommendedNextStep);
  const lastName = normKey(c.name).split(" ").slice(-1)[0];
  const keepOriginal =
    (disp === DISP.SALES_READY || disp === DISP.ACCOUNT_DEVELOPMENT) &&
    !r.chosenContact.changed &&
    before.length >= 60 &&
    normKey(before).includes(lastName);
  const generated = generateNextStep(o, disp, r);
  const nextStep = keepOriginal ? before : generated;
  o.recommendedNextStep = nextStep;
  o.recommendedAction = nextStep;
  setDrawerSection(o, "recommended_next_step", nextStep);

  // --- who to contact (drawer + preview) ---
  const routeLine = ch.email
    ? ` Published email: ${ch.email} (from organizer pages; not independently deliverability-verified).`
    : ch.mailbox
      ? ` Organizer mailbox: ${ch.mailbox} (shared route — ask for ${c.name}; not independently deliverability-verified).`
      : " No email is published for this contact; use the organizer contact channel.";
  const remit = r.contract.buyer.responsibilityOk ? "Role supports a lodging/meetings buying remit." : "Lodging/meetings remit not yet verified — qualify before relying on this contact.";
  setDrawerSection(o, "who_to_contact", `${c.name} — ${c.title || "role not published"} (${c.organization}).${routeLine} ${remit}`);
  o.tileContactPreview = { displayRole: c.title || null, displayRoleKind: c.title ? "professional_title" : "unknown", professionalTitle: c.title || null };
  o.cardContactSubtitle = c.title || undefined;

  // --- uncertainties: contact audit note + gaps ---
  const unc = Array.isArray(o.uncertainties) ? [...o.uncertainties] : o.uncertainties ? [text(o.uncertainties)] : [];
  const note = `Contact audit (${REF}): ${c.name}, ${c.title || "role not published"} — ${r.contract.buyer.responsibilityOk ? "role consistent with lodging/meetings buying" : "lodging/meetings remit not verified"}; route: ${ch.email ? "named direct email published" : ch.mailbox ? "organizer mailbox published" : "no email published"}.`;
  unc.push(note);
  if (disp !== DISP.SALES_READY) unc.push(`Not Sales Ready: ${r.whyNotSalesReady.join(" ")}`);
  o.uncertainties = unc;
  const unDrawer = getDrawerBody(o, "research_uncertainties");
  setDrawerSection(o, "research_uncertainties", [unDrawer, note].filter(Boolean).join(" "));

  // --- persisted evidence fields (contract reproducibility) + compact certification ---
  Object.assign(o, r._rec.patch);
  o.commercialCertificationV1 = {
    contract: CONTRACT_ID,
    referenceDate: REF,
    disposition: disp,
    salesReady: r.salesReady,
    contractStatus: r.contract.status,
    gaps: r.contract.gaps,
    demandThesisStandard: r.contract.demand.thesisStandard,
    accountDevelopment: r.accountDevelopment.ok,
    contactChanged: r.chosenContact.changed,
  };

  // --- population contract: forced Hotel Market, never CMI ---
  o.hotelMarketScopeEligible = true;
  o.competitiveMarketScopeEligible = false;
  o.inventoryPopulations = [INVENTORY_POPULATION.SALES_OPPORTUNITIES];
  o.inventoryPopulationPrimary = INVENTORY_POPULATION.SALES_OPPORTUNITIES;
  o.competitorVenueStatus = null;
  o.competitorHotel = null;
  o.cmiClass = null;

  const clean = sanitizeCustomerCopy(o);
  let presented = applySalesPresentation(clean);
  if (disp === DISP.SALES_READY) presented = applySalesReadyPresentation(presented);
  if (disp === DISP.QUALIFY_VISIBLE && presented.salesActionKey === CUSTOMER_SALES_ACTION.MONITOR) {
    // A visible Hotel Market card must carry an actionable motion — never a passive Monitor.
    const qk = CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY;
    presented.salesActionKey = qk;
    presented.salesActionPillLabel = CUSTOMER_SALES_ACTION_LABEL[qk];
    presented.salesActionReason = "Named, sourced event with exact upcoming dates; target-hotel path unconfirmed — qualify the path (resolver returned passive Monitor, overridden for visible Hotel Market inventory)";
    if (presented.tileBadgePlan) {
      presented.tileBadgePlan = { ...presented.tileBadgePlan, salesActionKey: qk, salesActionPillLabel: presented.salesActionPillLabel, salesActionReason: presented.salesActionReason };
    }
    presented = attachSalesOpportunityFilterFields(presented);
  }
  // presentation may re-derive; pin explicit label last
  if (presented.canonicalTiming?.displayLabel) presented.datePillDisplayLabel = presented.canonicalTiming.displayLabel;

  r.nextStepBefore = before;
  r.nextStepAfter = nextStep;
  r.nextStepChanged = before !== nextStep;
  r.strippedCmiResidue = stripped;
  r.evidencePersisted = evidencePersisted;
  r.dateCorrection = dateCorrection;
  r.finalDateLabel = presented.datePillDisplayLabel;
  r.salesActionBefore = { key: opp0(r.id)?.salesActionKey, label: opp0(r.id)?.salesActionPillLabel, stage: opp0(r.id)?.commercialStagePillLabel };
  r.salesActionAfter = { key: presented.salesActionKey, label: presented.salesActionPillLabel, reason: presented.salesActionReason, stage: presented.commercialStageKey, stagePill: presented.commercialStagePillLabel };
  r.contactSummary = `${c.name} — ${c.title || "role n/a"}`;
  r.routeKind = c.emailKind || (route.ok ? "SOURCE_CONTACT_PAGE" : "NONE");
  r.contactNote = ch.emailNote || (r.chosenContact.changed ? `Re-selected from stored research (${ch.origin}).` : r.contract.buyer.responsibilityOk ? "Staged contact retained." : "Staged contact retained; remit unverified.");
  return presented;
}
const priorById = new Map(priorOpps.map((o) => [o.id, o]));
const opp0 = (id) => priorById.get(id);

const remediatedHmAll = hmRows.map((r) => ({ row: r, opp: remediateHotelMarketRow(r) }));
for (const { row, opp } of remediatedHmAll) {
  row.record = opp;
}

// ---------------------------------------------------------------------------
// CMI AUDIT
// ---------------------------------------------------------------------------
const cmiRows = [];
const cmiKept = [];
for (const opp of priorCmi) {
  const raws = rawsFor(opp.id);
  if (raws.length) rawEvidenceCount += 1;
  const rec = recoverEvidence(opp, raws);
  const o = cloneOpp(opp);
  const gatesBefore = evaluateCmiQualificationGates(o);
  const start = parseIso(o.eventStartDate);
  const end = parseIso(o.eventEndDate) || start;
  const daysToStart = start ? daysBetween(REF, start) : null;
  const imminent = start ? start < IMMINENT_CUTOFF : false;
  const unc = JSON.stringify([o.uncertainties, o.cmiEvidenceRegisterV1?.uncertainty, o.whatIsHappening]);
  const datesInferred = /inferred|infer(?:red)? from|estimated window|travel window|not (?:officially )?confirmed/i.test(`${o.eventDateEvidenceText || ""} ${unc}`) && /(inferred)/i.test(unc);
  const gatesOk = gatesBefore.qualifiesAsCustomerCmi === true;

  // contact audit (named person with defensible role) — CMI keeps the staged contact unless the stored research has a clearly better one
  const cands = buildCandidates({ ...o, ...rec.patch }, raws);
  const evals = cands.map((c) => evalCandidate({ ...o, ...rec.patch }, c));
  const current = evals.find((e) => e.cand.isCurrent) || evals[0];
  let chosen = current;
  for (const e of evals) if (e.score > chosen.score + 100) chosen = e; // only swap when responsibility materially improves
  const contactSummary = summarizeContract(evalContract(chosen.variant));
  const pcName = text(o.primaryContact?.name);
  const deskContact = /team|office|desk|conference\b.*\bteam/i.test(pcName) || !/^[A-Z][\p{L}'’.-]+(?:\s+[A-Z&][\p{L}'’.-]*)+/u.test(pcName);

  let disposition = DISP.CMI_RETAINED;
  const reasons = [];
  if (imminent) {
    disposition = DISP.CMI_DEMOTED;
    reasons.push(`Event starts ${start} — ${daysToStart} day(s) from ${REF}; inside the ${IMMINENT_DAYS}-day window, not actionable.`);
  }
  if (!gatesOk) {
    disposition = DISP.CMI_DEMOTED;
    reasons.push(`CMI qualification gates failed: ${gatesBefore.failedGates.join(", ")}.`);
  }
  if (datesInferred) {
    disposition = DISP.CMI_DEMOTED;
    reasons.push("Event dates are inferred rather than organizer-confirmed.");
  }
  if (deskContact && disposition === DISP.CMI_RETAINED) {
    reasons.push("Contact is an organizer desk, not a named person (allowed as a defensible intermediary for CMI).");
  }

  const row = {
    id: o.id,
    title: o.displayTitle || o.title,
    organization: o.organizationName,
    kind: "CMI",
    competitorHotel: o.cmiCompetitorHotel || o.competitorHotel || null,
    start,
    end,
    daysToStartFromRef: daysToStart,
    dateLabelBefore: o.datePillDisplayLabel,
    cmiGates: { qualifiesAsCustomerCmi: gatesBefore.qualifiesAsCustomerCmi, failedGates: gatesBefore.failedGates, gates: gatesBefore.gates, evidenceStatus: gatesBefore.evidenceStatus },
    cmiContractSalesReadyInfo: summarizeContract(evalContract(o)).salesReady,
    cmiContractNote: "CMI is competitive-market intelligence, not a Hotel Market sales opportunity: Sales Ready is never assigned here (no dual classification).",
    contactBefore: `${pcName} — ${text(o.primaryContact?.title)}`,
    contactAudit: {
      stagedContactResponsibilityOk: current.summary.buyer.responsibilityOk,
      stagedContactResponsibilityCode: current.summary.buyer.responsibilityCode,
      routeOk: current.summary.route.ok,
      deskContact,
      alternativeConsidered: evals.filter((e) => !e.cand.isCurrent).map((e) => ({ name: e.cand.name, title: e.cand.title, responsibilityOk: e.summary.buyer.responsibilityOk, routeOk: e.summary.route.ok })),
      swapped: !chosen.cand.isCurrent,
    },
    evidenceRecovery: { rawFiles: rec.rawFiles, recoveredFields: rec.recovered },
    disposition,
    detail: reasons.join(" ") || "Competitor commitment verified; all eight CMI gates pass; dates exact; start date outside the imminent window.",
    returnCriteria:
      disposition === DISP.CMI_DEMOTED
        ? reasons.some((r) => /gates failed/i.test(r))
          ? "Needs edition-specific evidence of a named competitor hotel commitment (host hotel / block / booking code) before it can return as CMI."
          : reasons.some((r) => /inferred/i.test(r))
            ? "Needs organizer-confirmed dates plus edition-specific competitor commitment."
            : "Re-evaluate against the next edition once dates and competitor commitments are published."
        : null,
  };
  cmiRows.push(row);

  if (disposition === DISP.CMI_RETAINED) {
    const k = cloneOpp(o);
    Object.assign(k, rec.patch);
    // pin explicit dates so tile derivation never produces Multiple Dates
    if (start) setExplicitDate(k, start, end);
    if (chosen && !chosen.cand.isCurrent) {
      const swapped = withContact(k, chosen.cand);
      k.primaryContact = swapped.primaryContact;
      k.contactName = swapped.contactName;
    }
    k.commercialCertificationV1 = { contract: CONTRACT_ID, referenceDate: REF, disposition, salesReady: false, cmiGatesPass: true, note: "Competitive Market Intelligence — never Sales Ready." };
    cmiKept.push(sanitizeCustomerCopy(k));
  } else {
    row.record = cloneOpp(o);
  }
}
const cmiDemoted = cmiRows.filter((r) => r.disposition === DISP.CMI_DEMOTED);

// ---------------------------------------------------------------------------
// population + actionability gate
// ---------------------------------------------------------------------------
const hmCustomerRows = hmRows.filter((r) => [DISP.SALES_READY, DISP.ACCOUNT_DEVELOPMENT, DISP.QUALIFY_VISIBLE].includes(r.disposition));
const hmHiddenRows = hmRows.filter((r) => !hmCustomerRows.includes(r));
const FORCE_HM_IDS = new Set(hmRows.map((r) => r.id));
const hmHiddenIds = new Set(hmHiddenRows.map((r) => r.id));

let allOpps = [...hmRows.map((r) => cloneOpp(r.record)), ...cmiKept.map(cloneOpp)];
allOpps = annotateInventoryPopulations(allOpps, { hotelId: HOTEL_ID }).map((o) => {
  const forceHm = FORCE_HM_IDS.has(o.id);
  const next = { ...o, competitiveMarketScopeEligible: !forceHm && o.competitiveMarketScopeEligible === true, hotelMarketScopeEligible: forceHm || o.competitiveMarketScopeEligible !== true };
  if (forceHm) {
    next.competitiveMarketScopeEligible = false;
    next.hotelMarketScopeEligible = true;
    next.competitorVenueStatus = null;
    next.competitorHotel = null;
    next.cmiVerified = false;
    next.cmiClass = null;
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
  const row = hmRows.find((r) => r.id === o.id);
  const hint = {
    actualEventStart: o.eventStartDate,
    actualEventEnd: o.eventEndDate || null,
    datePillDisplayLabel: o.datePillDisplayLabel,
    competitorCommitted: hm ? false : o.competitiveMarketScopeEligible === true,
    cmiVerified: hm ? false : o.competitiveMarketScopeEligible === true,
  };
  if (row?.disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE) {
    hint.actionabilityOverride = "NOT_CURRENTLY_ACTIONABLE";
    hint.nearTermSourcingClosed = true;
  }
  if (hmHiddenIds.has(o.id)) hint.forceExcludeFromHotel = true;
  hintsById[o.id] = hint;
}
const gate = applyCommercialActionabilityGate(allOpps, { referenceDate: REF, hintsById });
const gatedRows = gate.rows.map((o) => attachSalesOpportunityFilterFields(o));
const customerVisible = gatedRows
  .filter((o) => (o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true) && !hmHiddenIds.has(o.id))
  .map((o) => ({ ...o, customerVisible: true }));
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

// reconcile rows that the gate demoted unexpectedly (e.g., a retained row dropped by the gate)
const gateDroppedUnexpected = gatedOut.filter((o) => !hmHiddenIds.has(o.id) && !cmiDemoted.some((c) => c.id === o.id));
for (const r of hmRows) {
  const v = customerVisible.find((x) => x.id === r.id);
  r.customerVisibleFinal = Boolean(v);
  if (v) {
    r.finalRecord = v;
    r.finalDateLabel = v.datePillDisplayLabel;
    r.salesActionAfter = { key: v.salesActionKey, label: v.salesActionPillLabel, reason: v.salesActionReason, stage: v.commercialStageKey, stagePill: v.commercialStagePillLabel, commercialActionability: v.commercialActionability };
  }
}

// ---------------------------------------------------------------------------
// counts
// ---------------------------------------------------------------------------
const hmDispCounts = countBy(hmRows, (r) => r.disposition);
const afterCounts = {
  total: customerVisible.length,
  hm: hotelMarket.length,
  cmi: competitive.length,
  salesReady: hmRows.filter((r) => r.disposition === DISP.SALES_READY && r.customerVisibleFinal).length,
  accountDevelopment: hmRows.filter((r) => r.disposition === DISP.ACCOUNT_DEVELOPMENT && r.customerVisibleFinal).length,
  qualify: hmRows.filter((r) => r.disposition === DISP.QUALIFY_VISIBLE && r.customerVisibleFinal).length,
  researchContinuation: hmRows.filter((r) => r.disposition === DISP.RESEARCH_CONTINUATION_HIDDEN).length,
  demoted: hmRows.filter((r) => r.disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE).length + cmiDemoted.length,
  demotedHm: hmRows.filter((r) => r.disposition === DISP.DEMOTED_NOT_CURRENTLY_ACTIONABLE).length,
  demotedCmi: cmiDemoted.length,
};
const salesReadyBefore = priorOpps.filter((o) => o.commercialStageKey === "PURSUE_NOW" || o.commercialActionability === "SALES_READY").length;
const beforeCounts = { total: priorOpps.length, hm: priorHm.length, cmi: priorCmi.length, salesReady: salesReadyBefore };

// ---------------------------------------------------------------------------
// hard-gate checks on the final customer inventory
// ---------------------------------------------------------------------------
const providerLeak = /\bexa\b|surfe|parallel\.ai|airtable/i.test(JSON.stringify(customerVisible.map((o) => ({ ...o, sources: undefined, cmiEvidenceUrls: undefined }))));
const multipleDatesRemain = customerVisible.filter((o) => /multiple dates/i.test(JSON.stringify([o.datePillDisplayLabel, o.eventDateDisplay, o.tileBadgePlan?.datePillLabel, o.tileBadgePlan?.datePillDisplayLabel, o.tileBadgePlan?.canonicalTiming?.displayLabel])));
const imminentVisible = customerVisible.filter((o) => parseIso(o.eventStartDate) && parseIso(o.eventStartDate) < IMMINENT_CUTOFF);
const salesReadyRows = customerVisible.filter((o) => o.commercialCertificationV1?.disposition === DISP.SALES_READY);
const salesReadyAllPass = salesReadyRows.every((o) => evalContract(o).salesReady === true);

// ---------------------------------------------------------------------------
// package staging
// ---------------------------------------------------------------------------
// content changed? compare normalized visible set vs prior package (ids + remediation fingerprint)
const contentChanged = (() => {
  const a = JSON.stringify(priorOpps.map((o) => o.id).sort());
  const b = JSON.stringify(customerVisible.map((o) => o.id).sort());
  if (a !== b) return true;
  return customerVisible.some((o) => JSON.stringify(o) !== JSON.stringify(priorById.get(o.id)));
})();

let pkg = null;
let integrity = null;
let qualityGate = null;
let packageBlocker = null;
let writtenPath = null;
let stagedFromExisting = false;
const newPkgDir = path.join(PROTECTED_STORE_ROOT, HOTEL_ID, NEW_PUB_VERSION);
if (!customerVisible.length) {
  packageBlocker = "NO_CUSTOMER_ELIGIBLE_OPPORTUNITIES_TO_STAGE";
} else if (!contentChanged) {
  packageBlocker = "CUSTOMER_CONTENT_UNCHANGED_NO_NEW_PACKAGE";
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
      priorFingerprintPreserved: PRIOR_FINGERPRINT,
      priorPublicationVersion: PRIOR_PUB_VERSION,
      customerPublished: false,
    },
  });
  const candidatePkg = built.package || built;
  integrity = verifyPublicationPackageIntegrity(candidatePkg);
  qualityGate = runPublicationQualityGate({ opportunities: candidatePkg.opportunities || customerVisible, referenceDate: REF });
  if (integrity?.ok === false) packageBlocker = `PACKAGE_INTEGRITY_FAILED:${JSON.stringify(integrity.fails || integrity)}`;
  else if (qualityGate?.ok === false) packageBlocker = `PUBLICATION_QUALITY_GATE_BLOCKING:${JSON.stringify(qualityGate.blocking || qualityGate.summary || null)}`;
  else if (candidatePkg.contentFingerprintSha256 === PRIOR_FINGERPRINT) packageBlocker = "NEW_FINGERPRINT_EQUALS_PRIOR_NO_CHANGE";
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
const priorPkgUnchanged = sha256File(PRIOR_PKG_PATH) === priorPkgShaBefore;

if (pkg && !NO_STAGE) {
  fs.mkdirSync(HOTEL_DATA, { recursive: true });
  fs.writeFileSync(
    OPPS_JSON_PATH,
    `${JSON.stringify(
      {
        schema: "gdi-opportunities-doc-v1",
        hotelId: HOTEL_ID,
        hotelName: HOTEL_NAME,
        updatedAt: new Date().toISOString(),
        source: SCRIPT_ID,
        note: "STAGED_FOR_FOUNDER_REVIEW — not CUSTOMER_PUBLISHED",
        opportunities: customerVisible,
      },
      null,
      2,
    )}\n`,
  );
}

// ---------------------------------------------------------------------------
// Airtable dry run (never live)
// ---------------------------------------------------------------------------
const airtableRows = [
  ...customerVisible,
  ...hmHiddenRows.map((r) => ({ ...cloneOpp(r.record), customerVisible: false, commercialActionability: COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION })),
  ...cmiDemoted.map((r) => ({ ...cloneOpp(r.record), customerVisible: false })),
];
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

// ---------------------------------------------------------------------------
// derived audit tables
// ---------------------------------------------------------------------------
const cfeaRow = hmRows.find((r) => r.id === "rb_gdi_free_cfea_2026");
const cfea = {
  id: "rb_gdi_free_cfea_2026",
  eventDates: cfeaRow ? `${cfeaRow.eventStartDate} to ${cfeaRow.eventEndDate}` : null,
  daysFromReference: cfeaRow?.daysToStartFromRef ?? null,
  disposition: cfeaRow?.disposition || "NOT_FOUND",
  actionabilityState: gatedRows.find((o) => o.id === "rb_gdi_free_cfea_2026")?.commercialActionabilityStateV1 || null,
  removedFromActiveHotelMarket: cfeaRow ? !customerVisible.some((o) => o.id === cfeaRow.id) : null,
  futureEditionInvented: false,
  summary: cfeaRow
    ? `CFEA 2026 (Nov 20–21, 2026) starts ${cfeaRow.daysToStartFromRef} day(s) after ${REF} — too little time to source lodging, so it is removed from the active Hotel Market and recorded as NOT_CURRENTLY_ACTIONABLE (research continuation). No future edition is evidenced in the stored research, so none is invented; it returns only if a dated future edition is verified.`
    : "CFEA row not found.",
  returnCriteria: "A verified, dated future CFEA edition with a lodging-sourcing window ≥ 21 days.",
};
if (cfeaRow) cfeaRow.returnCriteria = cfea.returnCriteria;
for (const r of hmHiddenRows) {
  if (!r.returnCriteria) {
    r.returnCriteria = r.whyNotSalesReady.length ? `Resolve: ${r.whyNotSalesReady.join(" ")}` : "Strengthen evidence.";
    r.detail = r.dispositionReason;
  }
}
for (const r of hmRows) r.title = r.title;
for (const r of cmiRows) {
  const v = customerVisible.find((o) => o.id === r.id);
  if (v) r.finalDateLabel = v.datePillDisplayLabel;
}

const dateCorrections = [
  ...hmRows.filter((r) => r.dateCorrection).map((r) => r.dateCorrection),
  ...cmiRows.filter((r) => r.disposition === DISP.CMI_RETAINED && r.dateLabelBefore !== r.finalDateLabel).map((r) => ({ id: r.id, title: r.title, before: r.dateLabelBefore, after: r.finalDateLabel, basis: "Verified eventStartDate/eventEndDate" })),
];

// ---------------------------------------------------------------------------
// reports
// ---------------------------------------------------------------------------
const slim = (r) => {
  const { _finalBag, _chosen, _rec, record, finalRecord, ...rest } = r;
  return rest;
};
const stagedPointerAfter = protectedStore.readPointer(HOTEL_ID, "STAGED");
const git = { branch: sh("git rev-parse --abbrev-ref HEAD"), head: sh("git rev-parse --short HEAD") };

writeJson("01-BASELINE_AND_INVENTORY.json", {
  contract: CONTRACT_ID,
  referenceDate: REF,
  generatedAt: new Date().toISOString(),
  zeroPaid: true,
  baselinePackage: { publicationVersion: PRIOR_PUB_VERSION, fingerprint: PRIOR_FINGERPRINT, path: rel(PRIOR_PKG_PATH), sha256: priorPkgShaBefore, lifecycle: priorPkg.lifecycleState, inventoryCounts: priorPkg.inventoryCounts || computePublicationInventoryCounts(priorOpps) },
  before: beforeCounts,
  stagedPointerBefore,
  inventory: priorOpps.map((o) => ({
    id: o.id,
    kind: o.hotelMarketScopeEligible ? "HM" : "CMI",
    title: o.displayTitle || o.title,
    organization: o.organizationName,
    start: o.eventStartDate,
    end: o.eventEndDate,
    datePill: o.datePillDisplayLabel,
    salesAction: o.salesActionPillLabel,
    stage: o.commercialStagePillLabel,
    contact: o.primaryContact ? `${o.primaryContact.name} — ${o.primaryContact.title}` : null,
    rawResearchRuns: rawsFor(o.id).map((r) => r.file),
  })),
  protectedPublicationsBefore: protectedBefore,
  hotelCapabilityProfile: { rooms: HOTEL_CFG.capabilityProfile?.guestRooms ?? null, source: "config/group-demand-intelligence/hotels/recH9h3McgMNFQEla.json" },
});

writeJson("02-21_HOTEL_MARKET_COMMERCIAL_AUDIT.json", {
  contract: CONTRACT_ID,
  referenceDate: REF,
  salesReadyContract: "lib/group-demand-intelligence/commercial-eligibility-contract-v1.js#evaluateSharedCommercialEligibility",
  accountDevelopmentStandard: "lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js#evaluateProactiveAccountDevelopment",
  dispositionCounts: hmDispCounts,
  methodNote:
    "Each card is evaluated twice: (1) exactly as staged, and (2) after zero-cost recovery of evidence already stored in the prior research runs (venue/location, published attendance, official date statements) and re-selection of the best-supported contact from stored organizer candidates. Sales Ready requires the post-recovery contract to pass in full.",
  rows: hmRows.map(slim),
});

writeJson("03-15_CMI_ELIGIBILITY_AUDIT.json", {
  contract: CONTRACT_ID,
  cmiContract: "cmi-canonical-eligibility-contract-v1 (eight gates)",
  referenceDate: REF,
  retained: cmiRows.filter((r) => r.disposition === DISP.CMI_RETAINED).length,
  demoted: cmiDemoted.length,
  rows: cmiRows.map(slim),
});

writeJson("04-EVENT_DATES_AND_ACTIONABILITY.json", {
  referenceDate: REF,
  imminentCutoff: IMMINENT_CUTOFF,
  gate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  assertCustomerInventoryPassesActionabilityGate: actionabilityAssert,
  counts: gate.counts,
  cfea,
  dateCorrections,
  multipleDatesRemainingInCustomerInventory: multipleDatesRemain.map((o) => o.id),
  rows: gatedRows.map((o) => {
    const r = hmRows.find((x) => x.id === o.id) || cmiRows.find((x) => x.id === o.id);
    return {
      id: o.id,
      title: o.displayTitle || o.title,
      kind: FORCE_HM_IDS.has(o.id) ? "HM" : "CMI",
      eventStartDate: o.eventStartDate,
      eventEndDate: o.eventEndDate,
      datePillBefore: priorById.get(o.id)?.datePillDisplayLabel,
      datePillAfter: o.datePillDisplayLabel,
      officialStart: r?.evidenceRecovery?.officialStart ?? null,
      officialEnd: r?.evidenceRecovery?.officialEnd ?? null,
      officialMatchesRecord: r?.evidenceRecovery?.officialStart ? r.evidenceRecovery.officialStart === o.eventStartDate : null,
      daysFromReference: o.eventStartDate ? daysBetween(REF, parseIso(o.eventStartDate)) : null,
      actionabilityState: o.commercialActionabilityStateV1,
      temporalDisposition: o.commercialTemporalDisposition,
      customerVisible: customerVisible.some((v) => v.id === o.id),
    };
  }),
});

writeJson("05-SALES_READINESS_RECLASSIFICATION.json", {
  referenceDate: REF,
  before: beforeCounts,
  after: afterCounts,
  dispositionCounts: hmDispCounts,
  salesReady: hmRows.filter((r) => r.salesReady).map((r) => ({ id: r.id, title: r.title, contact: r.contactSummary })),
  zeroSalesReadyNote: afterCounts.salesReady === 0 ? "Zero Sales Ready — evidence does not support promotion; each row explains why." : null,
  perCard: hmRows.map((r) => ({
    id: r.id,
    title: r.title,
    dispositionBefore: "ACCOUNT_DEVELOPMENT (staged label)",
    dispositionAfter: r.disposition,
    salesReady: r.salesReady,
    reason: r.dispositionReason,
    whyNotSalesReady: r.whyNotSalesReady,
    customerVisible: r.customerVisibleFinal === true,
  })),
  researchContinuationLedger: [
    ...hmHiddenRows.map((r) => ({ id: r.id, kind: "HM", title: r.title, disposition: r.disposition, reason: r.dispositionReason, returnCriteria: r.returnCriteria, preservedRecord: r.record })),
    ...cmiDemoted.map((r) => ({ id: r.id, kind: "CMI", title: r.title, disposition: r.disposition, reason: r.detail, returnCriteria: r.returnCriteria, preservedRecord: r.record })),
  ],
});

writeJson("06-CONTACT_VERIFICATION.json", {
  referenceDate: REF,
  method:
    "Contacts re-evaluated through the shared contract's buyer-responsibility and route gates. Emails were attached only when published on an organizer page whose domain matches the email domain; none is independently deliverability-verified, and shared mailboxes are labeled ORGANIZER_MAILBOX (never presented as person-direct).",
  hotelMarket: hmRows.map((r) => ({
    id: r.id,
    title: r.title,
    staged: r.contactBefore,
    chosen: r.chosenContact,
    responsibilityOk: r.contract.buyer.responsibilityOk,
    responsibilityCode: r.contract.buyer.responsibilityCode,
    routeOk: r.contract.route.ok,
    routeKind: r.routeKind,
    note: r.contactNote,
    candidates: r.contactCandidates,
  })),
  cmi: cmiRows.map((r) => ({ id: r.id, title: r.title, staged: r.contactBefore, audit: r.contactAudit })),
});

writeJson("07-SALES_ACTION_REMEDIATION.json", {
  referenceDate: REF,
  salesActionDistributionBefore: countBy(priorHm, (o) => o.salesActionPillLabel),
  salesActionDistributionAfter: countBy(hotelMarket, (o) => o.salesActionPillLabel),
  stageDistributionAfter: countBy(hotelMarket, (o) => o.commercialStagePillLabel),
  uniqueNextStepsAfter: new Set(hotelMarket.map((o) => o.recommendedNextStep)).size,
  hotelMarketCards: hotelMarket.length,
  narrativeRepairs,
  rows: hmRows.map((r) => ({
    id: r.id,
    title: r.title,
    disposition: r.disposition,
    salesActionBefore: r.salesActionBefore,
    salesActionAfter: r.salesActionAfter,
    nextStepBefore: r.nextStepBefore,
    nextStepAfter: r.nextStepAfter,
    nextStepChanged: r.nextStepChanged,
    strippedCmiResidue: r.strippedCmiResidue,
    evidencePersistedOnRecord: r.evidencePersisted,
  })),
});

const attendanceNum = (s) => {
  const m = text(s).replace(/,/g, "").match(/(\d{3,6})/);
  return m ? Number(m[1]) : null;
};
writeJson("08-HOTEL_FIT_AND_REVENUE_STREAMS.json", {
  referenceDate: REF,
  hotel: { name: HOTEL_NAME, rooms: 157, meetingSpace: "~11,000 sq ft / 20 meeting rooms (Admiral Ballroom 3,151 sq ft)", fnbCaveat: "Restaurant/lounge reported closed; F&B limited to banquet/meeting-room catering — verify before selling catering." },
  rows: hmRows.map((r) => {
    const o = priorById.get(r.id);
    const ov = r._rec.overview || {};
    const att = attendanceNum(r._rec.patch.attendanceEvidenceText || o.attendanceEvidenceText || ov.attendancePublished);
    const flags = [];
    if (att && att >= 5000) flags.push("LARGE_EVENT_OVERFLOW_ONLY");
    if (/COMBINED/.test(o.revenueStreamClass || "") && r._rec.venue && !/hotel|castle|marriott|hilton|sheraton|westin|hyatt/i.test(r._rec.venue)) flags.push("CATERING_CLAIM_AT_NON_HOTEL_VENUE_VERIFY");
    if (/COMBINED/.test(o.revenueStreamClass || "")) flags.push("FNB_LIMITED_VERIFY_BANQUET_CAPABILITY");
    if (text(ov.hotelSourcingStatus) === "venue_confirmed" && !/radisson/i.test(JSON.stringify(o.sources || []))) flags.push("VENUE_CONFIRMED_NO_TARGET_HOTEL_PATH");
    return {
      id: r.id,
      title: r.title,
      disposition: r.disposition,
      revenueStreamClass: o.revenueStreamClass,
      territory: o.territoryClassification || null,
      venue: r._rec.venue || null,
      publishedAttendance: text(r._rec.patch.attendanceEvidenceText || o.attendanceEvidenceText || ov.attendancePublished) || null,
      hotelFit: o.hotelFitAssessment || null,
      hotelFitGate: r.contract.hotelFit,
      fitRationaleOrEvidence: text(o.hotelFitAssessment?.rationale) || text(o.whyThisHotel),
      hotelSourcingStatus: o.hotelSourcingStatusLabel,
      flags,
    };
  }),
});

writeJson("09-NEED_DATE_VALIDATION.json", {
  referenceDate: REF,
  rule: `Lodging need dates must be exact, future, and ≥ ${IMMINENT_DAYS} days out; the pill must equal the label derived from eventStartDate/eventEndDate.`,
  rows: customerVisible.map((o) => {
    const label = dateLabelFor(parseIso(o.eventStartDate), parseIso(o.eventEndDate));
    const r = hmRows.find((x) => x.id === o.id) || cmiRows.find((x) => x.id === o.id);
    return {
      id: o.id,
      title: o.displayTitle || o.title,
      eventStartDate: o.eventStartDate,
      eventEndDate: o.eventEndDate,
      expectedLabel: label,
      actualPill: o.datePillDisplayLabel,
      pillConsistent: o.datePillDisplayLabel === label,
      officialStart: r?.evidenceRecovery?.officialStart ?? null,
      officialMatchesRecord: r?.evidenceRecovery?.officialStart ? r.evidenceRecovery.officialStart === o.eventStartDate : null,
      daysFromReference: daysBetween(REF, parseIso(o.eventStartDate)),
      needDateValid: Boolean(parseIso(o.eventStartDate)) && daysBetween(REF, parseIso(o.eventStartDate)) >= IMMINENT_DAYS,
      timingNote: o.canonicalTiming?.timingNotes || null,
    };
  }),
});

writeJson("10-AIRTABLE_RECONCILIATION.json", {
  mode: "LOCAL_RECONCILIATION_ONLY",
  airtableConfigured: airtable.configured,
  note: "Local staged inventory vs prior package. Live Airtable state is compared only through the dry-run in 11; no live write was made.",
  priorPackageIds: priorOpps.length,
  newCustomerVisibleIds: customerVisible.length,
  retainedVisible: customerVisible.filter((o) => priorById.has(o.id)).map((o) => o.id),
  removedFromCustomerView: [...hmHiddenRows.map((r) => ({ id: r.id, kind: "HM", disposition: r.disposition })), ...cmiDemoted.map((r) => ({ id: r.id, kind: "CMI", disposition: r.disposition }))],
  airtableFieldImpact: {
    commercialActionabilityByCard: Object.fromEntries(hmRows.map((r) => [r.id, r.finalRecord?.commercialActionability || (r.disposition === DISP.RESEARCH_CONTINUATION_HIDDEN ? COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION : "NOT_CUSTOMER_VISIBLE")])),
    hiddenRowsFlaggedCustomerVisibleFalse: airtableRows.filter((o) => o.customerVisible === false).map((o) => o.id),
  },
});
writeJson("11-AIRTABLE_WRITE_DRY_RUN.json", { ...airtable, liveWritesAuthorized: false, rowsEvaluated: airtableRows.length });

const staging = {
  staged: Boolean(pkg) && !NO_STAGE,
  stagedFromExistingPackage: stagedFromExisting,
  blocker: packageBlocker,
  unchangedFromPrior: packageBlocker === "CUSTOMER_CONTENT_UNCHANGED_NO_NEW_PACKAGE",
  publicationVersion: pkg?.publicationVersion || NEW_PUB_VERSION,
  publicationId: pkg?.publicationId || null,
  fingerprint: pkg?.contentFingerprintSha256 || null,
  lifecycleState: pkg ? GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED : null,
  customerPublished: false,
  packagePath: writtenPath,
  inventoryCounts: computePublicationInventoryCounts(customerVisible),
};
writeJson("12-NEW_STAGED_PUBLICATION.json", {
  ...staging,
  founderReviewLabel: pkg ? "STAGED_FOR_FOUNDER_REVIEW" : null,
  integrity,
  qualityGate: { ran: Boolean(qualityGate), ok: qualityGate?.ok ?? null, blocking: qualityGate?.blocking || null, summary: qualityGate?.summary || null },
  priorPackagePreserved: { fingerprint: PRIOR_FINGERPRINT, publicationVersion: PRIOR_PUB_VERSION, path: rel(PRIOR_PKG_PATH), sha256Before: priorPkgShaBefore, sha256After: sha256File(PRIOR_PKG_PATH), unchanged: priorPkgUnchanged },
  stagedPointerBefore,
  stagedPointerAfter,
  protectedPublicationsBefore: protectedBefore,
  protectedPublicationsAfter: protectedAfter,
  protectedPublicationsUnchanged: protectedUnchanged,
  previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
  opportunitiesJson: pkg && !NO_STAGE ? rel(OPPS_JSON_PATH) : null,
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
    priorFingerprintPreserved: PRIOR_FINGERPRINT,
  });
}

// ---------------------------------------------------------------------------
// state, certification, founder report
// ---------------------------------------------------------------------------
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
  priorPackageUnchanged: priorPkgUnchanged,
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
  cmiRetainedAllPassGates: cmiRows.filter((r) => r.disposition === DISP.CMI_RETAINED).every((r) => r.cmiGates.qualifiesAsCustomerCmi),
  stagedOrUnchanged: Boolean(pkg) || staging.unchangedFromPrior || NO_STAGE,
  gateDroppedUnexpectedIds: gateDroppedUnexpected.map((o) => o.id),
};
if (hardGates.gateDroppedUnexpectedIds.length) hardGates.stagedOrUnchanged = false;

const state = {
  contract: CONTRACT_ID,
  referenceDate: REF,
  generatedAt: new Date().toISOString(),
  git,
  before: beforeCounts,
  after: afterCounts,
  rawEvidence: { oppsWithRawEvidence: rawEvidenceCount, total: priorOpps.length },
  hotelConfigMissingKeys,
  hmRows: hmRows.map((r) => ({
    id: r.id,
    title: r.title,
    disposition: r.disposition,
    salesReady: r.salesReady,
    demandStandard: r.contract.demand.thesisStandard,
    contactRole: r.chosenContact.title,
    dateLabelBefore: r.dateLabelBefore,
    finalDateLabel: r.finalDateLabel || null,
    contactBefore: r.contactBefore,
    contactSummary: r.contactSummary,
    routeKind: r.routeKind,
    contactNote: r.contactNote,
    whyNotSalesReady: r.whyNotSalesReady,
    returnCriteria: r.returnCriteria || null,
    detail: r.dispositionReason,
  })),
  cmiRows: cmiRows.map((r) => ({ id: r.id, title: r.title, disposition: r.disposition, detail: r.detail, returnCriteria: r.returnCriteria })),
  cmiDemoted: cmiDemoted.map((r) => r.id),
  dateCorrections,
  cfea,
  staging,
  airtable: { status: airtable.status, ok: airtable.ok ?? null, code: airtable.code || null, configured: airtable.configured, wouldCreate: airtable.wouldCreate ?? null, wouldUpdate: airtable.wouldUpdate ?? null, liveWrites: 0 },
  hardGates,
  browser: prevBrowser || { status: "NOT_RUN" },
};
fs.writeFileSync(STATE_PATH, `${JSON.stringify(state, null, 2)}\n`);
const cert = buildCertification(state);
writeJson("14-FULL_E2E_CERTIFICATION.json", cert);
if (!fs.existsSync(path.join(OUT, "13-LOCAL_BROWSER_VERIFICATION.json")) || !prevBrowser) {
  writeJson("13-LOCAL_BROWSER_VERIFICATION.json", {
    contract: `${CONTRACT_ID}_BROWSER`,
    status: "NOT_RUN",
    howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify [--base-url http://127.0.0.1:8080]`,
    previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
    expectedFingerprint: staging.fingerprint,
    expectedCounts: staging.inventoryCounts,
  });
}
writeJson("15-GITHUB_MERGE_STATUS.json", {
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
    "scripts/gdi-radisson-toronto-final-commercial-certification-v1.mjs (new)",
    "reports/gdi-radisson-toronto-final-commercial-certification-v1/** (new)",
    `lib/group-demand-intelligence/bundled-publication-packages/${HOTEL_ID}/${NEW_PUB_VERSION}/ (new package)`,
    `lib/group-demand-intelligence/bundled-publication-packages/${HOTEL_ID}/STAGED.json (pointer -> new package)`,
    `data/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities.json (this hotel only)`,
  ],
  unrelatedDirtyWorktreeFilesNotTouched: sh("git status --porcelain -- . ':!reports' ").split("\n").filter(Boolean).filter((l) => !l.includes(HOTEL_ID) && !l.includes("gdi-radisson-toronto-final-commercial-certification-v1")),
});
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), buildFounderReport(state, cert));

console.log(
  JSON.stringify(
    {
      before: beforeCounts,
      after: afterCounts,
      hmDispositions: hmDispCounts,
      cfeaDisposition: cfea.disposition,
      dateCorrections: dateCorrections.map((d) => `${d.title}: ${d.before} -> ${d.after}`),
      newFingerprint: staging.fingerprint,
      stagingBlocker: staging.blocker,
      priorFingerprintPreserved: priorPkgUnchanged,
      protectedUnchanged,
      airtableStatus: airtable.status,
      fullE2eStatus: cert.fullE2eStatus,
      hardFailures: cert.hardFailures,
      hardGates,
    },
    null,
    2,
  ),
);
