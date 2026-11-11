/**
 * Read-only measurement of the existing GDI Full E2E engine.
 *
 * Exit 0 means the measurement finished. It does not certify research quality,
 * commercial qualification, customer delivery, or full end-to-end sign-off.
 *
 * Does not call paid providers, publish, or write hotel stores.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "../lib/group-demand-intelligence/research/research-constants-v1.js";
import { FOURTEEN_WORKFLOWS } from "../lib/group-demand-intelligence/research/discovery-to-decision/pipeline-trace-v1.js";
import { FULL_E2E_CONTRACT_VERSION } from "../lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js";
import { COMMERCIAL_ELIGIBILITY_CONTRACT_V1, evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { classifyCommercialDemandThesis, DEMAND_THESIS_STANDARD } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import { classifyContactEligibility } from "../lib/group-demand-intelligence/contact-eligibility-standard-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { findCustomerInternalIdLeaks } from "../lib/group-demand-intelligence/enrich-gdi-opportunity-for-customer.js";
import { mayAutoPublish } from "../lib/group-demand-intelligence/workflow/weekly-refresh-contract-v1.js";
import { AUTHORIZED_NEW_SPEND_USD } from "../lib/group-demand-intelligence/workflow/paid-multisegment-research-v1.js";
import {
  SPICE_ISLAND_HOTEL_ID,
  SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
} from "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js";
import { resolveGdiPublishedInventoryRoot } from "../lib/group-demand-intelligence/durable-publication/resolve-gdi-published-registry-root-v1.js";
import {
  createGdiPublishedInventoryRegistry,
  mergePublishedInventoryIntoOpportunities,
} from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTELS = [
  { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
  { id: "recG66DQJKP2c0UNh", name: "Renaissance New York Times Square Hotel" },
  { id: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside" },
  { id: SPICE_ISLAND_HOTEL_ID, name: "Spice Island Beach Resort", international: true },
];

function git(args) {
  return execFileSync("git", args, { cwd: ROOT, encoding: "utf8" }).trim();
}

function readJson(file, fallback = null) {
  if (!fs.existsSync(file)) return fallback;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function inc(bag, key) {
  const name = key == null || key === "" ? "UNSET" : String(key);
  bag[name] = (bag[name] || 0) + 1;
}

function loadOpportunities(hotelId) {
  const file = path.join(ROOT, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json");
  const doc = readJson(file, null);
  const opportunities = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  return { file, present: Boolean(doc), count: opportunities.length, opportunities };
}

function sourceUrls(opportunity) {
  const urls = [];
  const push = (value) => {
    const text = String(value || "").trim();
    if (/^https?:\/\//i.test(text)) urls.push(text);
  };
  push(opportunity.officialSource);
  push(opportunity.discoverySource);
  push(opportunity.sourceUrl);
  push(opportunity.evidenceUrl);
  if (Array.isArray(opportunity.sources)) {
    for (const source of opportunity.sources) {
      if (typeof source === "string") push(source);
      else push(source?.url || source?.href);
    }
  }
  return [...new Set(urls)];
}

function personFrom(opportunity) {
  const contact = opportunity.primaryContact || opportunity.contact || {};
  return {
    name: contact.name || opportunity.primaryContactName || opportunity.contactName || "",
    role: contact.role || contact.title || opportunity.primaryContactRole || "",
    title: contact.title || opportunity.primaryContactTitle || "",
  };
}

function evaluateOpportunity(opportunity) {
  let eligibility = null;
  let eligibilityError = null;
  try {
    eligibility = evaluateSharedCommercialEligibility(opportunity);
  } catch (err) {
    eligibilityError = err?.message || String(err);
  }
  let thesis = null;
  try {
    thesis = classifyCommercialDemandThesis(opportunity);
  } catch (err) {
    thesis = { thesisStandard: "EVALUATION_ERROR", error: err?.message || String(err) };
  }
  let contact = null;
  try {
    contact = classifyContactEligibility(personFrom(opportunity), opportunity);
  } catch (err) {
    contact = { class: "EVALUATION_ERROR", error: err?.message || String(err) };
  }
  let readiness = null;
  try {
    readiness = isGdiCustomerOpportunityReady(opportunity);
  } catch (err) {
    readiness = { ok: false, state: "EVALUATION_ERROR", error: err?.message || String(err) };
  }
  const leaks = findCustomerInternalIdLeaks(opportunity);
  return { eligibility, eligibilityError, thesis, contact, readiness, leaks };
}

function measureHotel(hotel) {
  const loaded = loadOpportunities(hotel.id);
  const thesis = {};
  const eligibility = {};
  const contactClass = {};
  const readiness = {};
  const segments = {};
  const demandTypes = {};
  const eventDateStatus = {};
  const gates = {
    realisticHotelDemand: 0,
    namedRelevantBuyer: 0,
    usableContactRoute: 0,
    sourcesTraceable: 0,
    credibleOrganizationActivity: 0,
  };
  let salesReady = 0;
  let salesReadySpeculative = 0;
  let withSourceUrl = 0;
  let withNeedDate = 0;
  let withRecommendedAction = 0;
  let internalLeaks = 0;
  let evaluationErrors = 0;
  const ids = new Set();
  const titleKeys = new Map();
  const sample = [];

  for (const opportunity of loaded.opportunities) {
    const id = String(opportunity.id || opportunity.opportunityId || "");
    if (id) ids.add(id);
    const titleKey = `${opportunity.organizationName || opportunity.company || ""}|${opportunity.title || opportunity.opportunityName || ""}`
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
    if (titleKey !== "|") titleKeys.set(titleKey, (titleKeys.get(titleKey) || 0) + 1);

    const result = evaluateOpportunity(opportunity);
    if (result.eligibilityError) evaluationErrors += 1;
    inc(thesis, result.thesis?.thesisStandard);
    inc(eligibility, result.eligibility?.status);
    inc(contactClass, result.contact?.class);
    inc(readiness, result.readiness?.state);
    inc(segments, opportunity.segment || opportunity.demandFamily);
    inc(demandTypes, opportunity.demandType || opportunity.demandSignalType);
    inc(eventDateStatus, opportunity.eventDateStatus);
    if (opportunity.eventDateStatus || opportunity.eventDateDisplay || opportunity.eventStartDate) withNeedDate += 1;
    if (result.eligibility?.salesReady) {
      salesReady += 1;
      if (result.thesis?.thesisStandard === "SPECULATIVE_DEMAND") salesReadySpeculative += 1;
    }
    for (const gate of Object.keys(gates)) {
      if (result.eligibility?.gates?.[gate] === true) gates[gate] += 1;
    }
    if (sourceUrls(opportunity).length) withSourceUrl += 1;
    if (opportunity.recommendedAction || opportunity.salesActionKey || result.eligibility?.nextActions?.length) {
      withRecommendedAction += 1;
    }
    if (result.leaks?.length) internalLeaks += 1;
    if (sample.length < 8) {
      sample.push({
        id,
        thesis: result.thesis?.thesisStandard || null,
        eligibility: result.eligibility?.status || null,
        contactClass: result.contact?.class || null,
        readiness: result.readiness?.state || null,
        eventDateStatus: opportunity.eventDateStatus || null,
        segment: opportunity.segment || opportunity.demandFamily || null,
        salesReady: result.eligibility?.salesReady === true,
      });
    }
  }

  const duplicateGroups = [...titleKeys.values()].filter((count) => count > 1).length;
  const workflowHits = FOURTEEN_WORKFLOWS.filter((row) =>
    loaded.opportunities.some((opportunity) =>
      [opportunity.segment, opportunity.demandFamily, opportunity.demandType, opportunity.workflowId]
        .map((value) => String(value || ""))
        .includes(row.id)
    )
  ).map((row) => row.id);

  return {
    hotel: hotel.name,
    hotelId: hotel.id,
    international: hotel.international === true,
    storePresent: loaded.present,
    researchStoreCount: loaded.count,
    uniqueIds: ids.size,
    duplicateIdRows: loaded.count - ids.size,
    duplicateTitleGroups: duplicateGroups,
    thesis,
    eligibility,
    contactClass,
    readiness,
    segments,
    demandTypes,
    eventDateStatus,
    gates,
    salesReady,
    salesReadySpeculative,
    withSourceUrl,
    withNeedDate,
    withRecommendedAction,
    internalLeaks,
    evaluationErrors,
    workflowIdsPresentInStore: workflowHits,
    sample,
  };
}

function readPublishedRegistry() {
  const root = resolveGdiPublishedInventoryRoot(process.env);
  const file = path.join(root, "registry", "current-registry-v1.json");
  const doc = readJson(file, null);
  const byHotel = {};
  if (doc?.hotels) {
    for (const [hotelId, bucket] of Object.entries(doc.hotels)) {
      const rows = Object.values(bucket?.opportunities || {});
      byHotel[hotelId] = {
        published: rows.filter((row) => row.publicationState === "PUBLISHED").map((row) => ({
          id: row.opportunityId,
          classification: row.commercialClassification || null,
          version: row.publishedVersion || null,
        })),
        otherStates: rows.filter((row) => row.publicationState !== "PUBLISHED").length,
      };
    }
  }
  return { rootName: path.basename(root), registryPresent: Boolean(doc), hotelCount: Object.keys(byHotel).length, byHotel };
}

function isolationProbe() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-cert-isolation-"));
  const registry = createGdiPublishedInventoryRegistry({ root });
  const blocked = registry.assertCannotPromoteFromResearchAlone({ hotelId: "rec-cert-probe" });
  const merged = mergePublishedInventoryIntoOpportunities(
    "rec-cert-probe",
    [{ id: "research-only-row" }],
    { root, strictPublishedOnly: true }
  );
  return {
    researchRowLeaked: merged.length !== 0,
    promotionBlocked: blocked.blocked === true,
  };
}

async function probeAuth() {
  if (process.env.GDI_CERT_SKIP_NETWORK === "1") {
    return [{ route: "skipped", status: null, error: "network_skipped" }];
  }
  const routes = [
    "/api/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities",
    "/api/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/research-runs",
    "/api/ai-demand-positioning/review/admin-publication-control",
    "/app/admin/ai-demand-admin.html",
  ];
  const results = [];
  for (const route of routes) {
    try {
      const response = await fetch(`http://127.0.0.1:8080${route}`, { signal: AbortSignal.timeout(20000) });
      results.push({ route, status: response.status });
    } catch (err) {
      results.push({ route, status: null, error: err?.name || "unavailable" });
    }
  }
  return results;
}

async function probeSampleUrls(hotels) {
  if (process.env.GDI_CERT_SKIP_NETWORK === "1") return [];
  const checks = [];
  for (const hotel of hotels) {
    const { opportunities } = loadOpportunities(hotel.id);
    const picked = [];
    for (const opportunity of opportunities) {
      const url = sourceUrls(opportunity).find((candidate) => !/[?&](token|key|sig)=/i.test(candidate));
      if (!url) continue;
      picked.push(url);
      if (picked.length === 2) break;
    }
    for (const url of picked) {
      let status = null;
      let error = null;
      try {
        const response = await fetch(url, { method: "GET", redirect: "follow", signal: AbortSignal.timeout(8000) });
        status = response.status;
      } catch (err) {
        error = err?.name || "fetch_failed";
      }
      checks.push({
        hotelId: hotel.id,
        host: (() => {
          try { return new URL(url).host; } catch { return null; }
        })(),
        status,
        error,
      });
    }
    if (!picked.length) checks.push({ hotelId: hotel.id, host: null, status: null, error: "no_http_source_on_sampled_rows" });
  }
  return checks;
}

function loadWorkflowSummary() {
  const file = process.env.GDI_CERT_WORKFLOW_SUMMARY;
  if (!file) return null;
  return readJson(file, null);
}

function decide(input) {
  const eventsMissing = !fs.existsSync(
    path.join(ROOT, "lib/group-demand-intelligence/research/exa-agent/fixtures/gdi-events-mock-provider-responses-v1.js")
  );
  const workflow = input.workflow;
  const localDeliveryFailed = Boolean(
    workflow?.hotels?.some((row) => row.localDelivery === "failed")
  );
  const engineRan = Boolean(
    workflow?.hotels?.length &&
      workflow.hotels.every((row) => Number(row.exitCode) === 0 && Number(row.paidCalls || 0) === 0 && row.localDelivery === "passed")
  );
  const spice = input.registry.byHotel[SPICE_ISLAND_HOTEL_ID];
  const spiceIds = (spice?.published || []).map((row) => row.id).sort();
  const expectedSpice = [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS].sort();
  const spiceExact = spiceIds.length === expectedSpice.length && spiceIds.every((id, index) => id === expectedSpice[index]);
  const bethesdaPublished = input.registry.byHotel.recLuxvwwxID7U2B8?.published?.length || 0;
  const publicationSafety = input.isolation.researchRowLeaked || input.isolation.promotionBlocked !== true
    ? "FAIL"
    : spiceExact
      ? "PASS"
      : "NOT VERIFIED";
  const commercialQualification = input.hotels.some((hotel) => hotel.salesReadySpeculative > 0)
    ? "FAIL"
    : "NOT VERIFIED";
  const engineExecution = !input.workflow ? "NOT VERIFIED" : localDeliveryFailed ? "FAIL" : engineRan ? "PASS" : "NOT VERIFIED";
  const customerDelivery = !input.workflow ? "NOT VERIFIED" : localDeliveryFailed ? "FAIL" : "NOT VERIFIED";

  const measured = [engineExecution, customerDelivery, commercialQualification, publicationSafety];
  const fullE2e = measured.includes("FAIL") ? "FAIL" : "NOT VERIFIED";

  return {
    engineExecution,
    eventsOrchestrator: eventsMissing ? "NOT VERIFIED" : "PASS",
    researchQuality: "NOT VERIFIED",
    commercialQualification,
    customerDelivery,
    publicationSafety,
    fullE2e,
    eventsFixture: eventsMissing ? "MISSING" : "PRESENT",
    spicePublishedExact: spiceExact,
    bethesdaPublishedCount: bethesdaPublished,
    paidCalls: workflow?.hotels?.reduce((sum, row) => sum + Number(row.paidCalls || 0), 0) ?? null,
  };
}

function markdown(report) {
  const lines = [];
  lines.push("# GDI Full E2E certification report");
  lines.push("");
  lines.push(`Measured commit: \`${report.commit}\`. Contract: \`${report.contract}\`.`);
  lines.push("");
  lines.push("This report records a zero-paid, no-publication measurement. A completed script is not commercial certification.");
  lines.push("");
  lines.push("## Scorecard");
  lines.push("");
  lines.push("| Decision | Status |");
  lines.push("| --- | --- |");
  for (const [key, value] of Object.entries(report.decisions)) {
    if (typeof value === "string" && ["PASS", "FAIL", "NOT VERIFIED"].includes(value)) {
      lines.push(`| ${key} | ${value} |`);
    }
  }
  lines.push("");
  lines.push(`Events fixture: ${report.decisions.eventsFixture}. Spice published set matches the certified seven: ${report.decisions.spicePublishedExact}. Bethesda published rows: ${report.decisions.bethesdaPublishedCount}.`);
  lines.push("");
  lines.push("## Catalog");
  lines.push("");
  lines.push(`Fourteen workflows: ${report.workflows.join(", ")}.`);
  lines.push("");
  lines.push(`Eleven segments: ${report.segments.join(", ")}.`);
  lines.push("");
  lines.push("## Hotels");
  lines.push("");
  for (const hotel of report.hotels) {
    lines.push(`### ${hotel.hotel}`);
    lines.push("");
    lines.push(`Research store ${hotel.researchStoreCount}. Unique ids ${hotel.uniqueIds}. Duplicate title groups ${hotel.duplicateTitleGroups}. Sales Ready by the existing evaluator ${hotel.salesReady}/${hotel.researchStoreCount}. Source URL present ${hotel.withSourceUrl}/${hotel.researchStoreCount}. Need-date field present ${hotel.withNeedDate}/${hotel.researchStoreCount}. Customer-ready ${hotel.readiness.READY || 0}/${hotel.researchStoreCount}. Internal-label leaks ${hotel.internalLeaks}. Evaluation errors ${hotel.evaluationErrors}.`);
    lines.push("");
    lines.push(`Thesis: ${JSON.stringify(hotel.thesis)}.`);
    lines.push("");
    lines.push(`Eligibility: ${JSON.stringify(hotel.eligibility)}.`);
    lines.push("");
    lines.push(`Contact class: ${JSON.stringify(hotel.contactClass)}.`);
    lines.push("");
    lines.push(`Workflow ids found on stored rows: ${hotel.workflowIdsPresentInStore.join(", ") || "none"}.`);
    lines.push("");
  }
  lines.push("## Publication and authentication");
  lines.push("");
  lines.push(`Registry present: ${report.registry.registryPresent}. Isolation leak: ${report.isolation.researchRowLeaked}. Promotion without approval blocked: ${report.isolation.promotionBlocked}.`);
  lines.push("");
  lines.push(`Anonymous route statuses: ${JSON.stringify(report.auth)}. Signed-in Memberstack delivery was not available.`);
  lines.push("");
  lines.push("## Independent source sample");
  lines.push("");
  lines.push("Host and HTTP status only. Reachability does not confirm the demand thesis.");
  lines.push("");
  lines.push(JSON.stringify(report.urlSample));
  lines.push("");
  lines.push("## Operational controls");
  lines.push("");
  lines.push(`Weekly auto-publish default: ${JSON.stringify(report.autoPublishDefault)}. Existing paid envelope constant: ${report.authorizedNewSpendUsd} USD. This run did not spend it.`);
  lines.push("");
  lines.push("## Workflow runs");
  lines.push("");
  lines.push(report.workflow ? JSON.stringify(report.workflow) : "NOT RUN in this measurement.");
  lines.push("");
  lines.push("## Exceptions");
  lines.push("");
  lines.push("- The events mock fixture `gdi-events-mock-provider-responses-v1.js` is not in the repository or the local Dealality tree. It was not reconstructed.");
  lines.push("- Explicit `--dry-run` on the research-to-page coordinator still sets weekly-refresh state persistence. Certification runs must use an isolated `GDI_DATA_ROOT`.");
  lines.push("- PR #166 was open at measurement time, so this commit is the stabilization branch head rather than a merged main commit.");
  lines.push("- Authenticated hotel and admin sessions were not available. Feedback was not posted.");
  lines.push("- The coordinator synthesizes hotel relevance for at most 60 stored opportunities in one run.");
  lines.push("- Zero-paid discovery stays plan-only. Local and production delivery verification failed, and budget reconcile reported the ledger missing from the isolated root until the existing ledger was copied. No spend was recorded.");
  lines.push("- Thesis class CONFIRMED_DEMAND is assigned from accommodation language in the stored thesis text. It is not an independent confirmation.");
  lines.push("- Stored rows use segment labels. They do not carry the fourteen workflow ids, so an empty workflow-id match is not a missing catalog.");
  lines.push("- A paid-provider validation was not run. It needs a separate authorization. The historical envelope must not be treated as a fresh allowance.");
  lines.push("");
  lines.push("## Regression commands");
  lines.push("");
  lines.push("```bash");
  lines.push("npm run test:gdi-full-e2e-certification-readiness-v1");
  lines.push("npm run test:gdi-full-e2e-certification-execute-v1");
  lines.push("npm run gdi:full-e2e-certification-execute");
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}

const hotels = HOTELS.map(measureHotel);
const registry = readPublishedRegistry();
const isolation = isolationProbe();
const auth = await probeAuth();
const urlSample = await probeSampleUrls(HOTELS);
const workflow = loadWorkflowSummary();
const decisions = decide({ registry, isolation, workflow, hotels });
const report = {
  ok: true,
  measurementComplete: true,
  certificationAwarded: false,
  commit: git(["rev-parse", "HEAD"]),
  branch: git(["branch", "--show-current"]),
  contract: FULL_E2E_CONTRACT_VERSION,
  eligibilityContract: COMMERCIAL_ELIGIBILITY_CONTRACT_V1,
  workflows: FOURTEEN_WORKFLOWS.map((row) => row.id),
  segments: [...SUPERVISOR_EXPECTED_SEGMENTS],
  hotels,
  registry: {
    registryPresent: registry.registryPresent,
    hotelCount: registry.hotelCount,
    publishedCounts: Object.fromEntries(
      Object.entries(registry.byHotel).map(([hotelId, bucket]) => [hotelId, bucket.published.length])
    ),
    spiceClassifications: (registry.byHotel[SPICE_ISLAND_HOTEL_ID]?.published || []).map((row) => row.classification),
  },
  isolation,
  auth,
  urlSample,
  autoPublishDefault: mayAutoPublish({}),
  authorizedNewSpendUsd: AUTHORIZED_NEW_SPEND_USD,
  workflow,
  decisions,
  thesisEnum: DEMAND_THESIS_STANDARD,
};

const reportPath = path.join(ROOT, "docs/gdi-full-e2e-certification-report-20261111.md");
if (process.env.GDI_CERT_WRITE_REPORT === "1") {
  fs.writeFileSync(reportPath, markdown(report));
}
console.log(JSON.stringify({
  ok: true,
  certificationAwarded: false,
  commit: report.commit,
  decisions: report.decisions,
  hotels: report.hotels.map((hotel) => ({
    hotel: hotel.hotel,
    researchStoreCount: hotel.researchStoreCount,
    salesReady: hotel.salesReady,
    ready: hotel.readiness.READY || 0,
    thesis: hotel.thesis,
    eligibility: hotel.eligibility,
    contactClass: hotel.contactClass,
    gates: hotel.gates,
    salesReadySpeculative: hotel.salesReadySpeculative,
    withSourceUrl: hotel.withSourceUrl,
    withNeedDate: hotel.withNeedDate,
  })),
  publishedCounts: report.registry.publishedCounts,
  isolation: report.isolation,
  auth: report.auth,
  urlSample: report.urlSample,
  workflow: report.workflow,
}, null, 2));
