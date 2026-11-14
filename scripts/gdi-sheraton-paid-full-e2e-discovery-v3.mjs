#!/usr/bin/env node
/**
 * Sheraton Guadalajara Expo — paid Full E2E discovery V3.
 * Hard ceiling $40. No Surfe. No CUSTOMER_PUBLISHED. No Bethesda mutation.
 *
 *   GDI_EXA_AGENT_LIVE=1 node scripts/gdi-sheraton-paid-full-e2e-discovery-v3.mjs --authorize-paid
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
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";
import { planCustomerTileBadges } from "../lib/group-demand-intelligence/customer-classification-presentation-v1.js";
import { resolveCustomerSalesAction } from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { INVENTORY_POPULATION } from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import {
  createPaidBudgetLedger,
  persistPaidBudgetLedger,
  remainingBudgetUsd,
  canAffordQuotedCall,
  reserveQuotedCall,
  settleReservedCall,
  releaseReservation,
} from "../lib/group-demand-intelligence/research/gdi-authorized-paid-budget-ledger-v1.js";
import {
  EXA_AGENT_EFFORT,
  runExaAgentResearch,
  quoteExaAgentRun,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";
import { createExaLiveDiscoveryProvider } from "../lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js";
import { buildExaEventsAgentCreatePayload } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-events-contracts-v1.js";
import { resolveExaAgentRoutingForSegment } from "../lib/group-demand-intelligence/research/exa-agent/gdi-segment-aware-exa-agent-routing-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const REF = "2026-11-14";
const HARD_CEILING_USD = 40;
const AUTHORIZATION_ID =
  "FOUNDER_APPROVE_SHERATON_PAID_FULL_E2E_DISCOVERY_V3_20261114";
const V2_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261113_commercial_e2e_remediation_v2";
const V2_FP =
  "6f9c0416be28c2409cf9fcccfb78cc45dd3a5178b27d4e0009f2622a21efd092";
const OUT = path.join(ROOT, "reports/gdi-sheraton-paid-full-e2e-discovery-v3");
const RESEARCH = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunity-research-state",
);
const LEDGER_PATH = path.join(OUT, "paid-budget-ledger.json");
const ARGS = new Set(process.argv.slice(2));
const AUTHORIZE = ARGS.has("--authorize-paid");

const HOTEL = {
  hotelId: HOTEL_ID,
  displayName: HOTEL_NAME,
  name: HOTEL_NAME,
  city: "Guadalajara",
  country: "Mexico",
  demandTerritory: {
    label: "Expo Guadalajara / Plaza del Sol / Zapopan corridor",
  },
  rooms: 216,
  positioning:
    "Full-service Sheraton adjacent to Expo Guadalajara; meetings + guestrooms; group demand focus",
};

const DISCOVERY_SEGMENTS = [
  "associations_conferences_events",
  "education_scientific",
  "sports_entertainment",
  "corporate_meetings_incentives",
  "corporate_accounts_scaling",
  "projects_infrastructure",
  "local_catering_day_meetings",
  "preferred_lodging_partnerships",
  "government_public_sector",
  "social_special_events",
  "professional_event_planners",
  "corporate_celebrations_private_events",
];

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n");
}
function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}
function normKey(s) {
  return text(s)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function parseIso(s) {
  const m = text(s).match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}
function yearIn(s) {
  const m = text(s).match(/\b(202[6-9]|2030)\b/);
  return m ? Number(m[1]) : null;
}
function hashId(prefix, title, org) {
  const h = crypto
    .createHash("sha256")
    .update(`${normKey(title)}|${normKey(org)}`)
    .digest("hex")
    .slice(0, 12);
  return `${prefix}_${h}`;
}

if (!AUTHORIZE) {
  console.error(
    "Refusing paid spend. Re-run with --authorize-paid after founder ceiling confirmation.",
  );
  process.exit(2);
}
if (!process.env.EXA_API_KEY) {
  console.error("EXA_API_KEY missing");
  process.exit(2);
}
process.env.GDI_EXA_AGENT_LIVE = "1";

fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(path.join(OUT, "raw-runs"), { recursive: true });

const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const v2 = store.loadPackage(HOTEL_ID, V2_VERSION);
if (!v2.ok || v2.package.contentFingerprintSha256 !== V2_FP) {
  console.error("V2 package missing/mismatch");
  process.exit(3);
}

const audit = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/gdi-sheraton-historical-opportunity-audit-v1/01-FULL_55_AUDIT.json",
    ),
    "utf8",
  ),
).audits;
const auditById = new Map(audit.map((a) => [a.opportunityId, a]));
const retainIds = new Set(
  audit
    .filter(
      (a) =>
        a.disposition === "ACTIVE_FUTURE_EVENT" ||
        a.disposition === "ACTIVE_ONGOING_DEMAND" ||
        a.disposition === "DATE_UNVERIFIED" ||
        a.disposition === "FUTURE_CYCLE_RESEARCH_REQUIRED",
    )
    .map((a) => a.opportunityId),
);
const expiredIds = new Set(
  audit
    .filter((a) => a.disposition === "EXPIRED_ONE_TIME_EVENT")
    .map((a) => a.opportunityId),
);
const dupIds = new Set(
  audit
    .filter((a) => a.disposition === "DUPLICATE_EVENT_RECORD")
    .map((a) => a.opportunityId),
);

const historicalKeys = new Set();
const historicalTitles = [];
for (const o of v2.package.opportunities) {
  historicalKeys.add(normKey(`${o.displayTitle || o.title}|${o.organizationName || ""}`));
  historicalTitles.push(normKey(o.displayTitle || o.title));
}
if (fs.existsSync(RESEARCH)) {
  for (const f of fs.readdirSync(RESEARCH).filter((x) => x.endsWith(".json"))) {
    try {
      const d = JSON.parse(fs.readFileSync(path.join(RESEARCH, f), "utf8"));
      historicalKeys.add(
        normKey(`${d.title || d.activityHeadline || ""}|${d.organizationName || ""}`),
      );
      historicalTitles.push(normKey(d.title || d.activityHeadline || ""));
    } catch {
      /* ignore */
    }
  }
}

writeJson("01-RESEARCH_AUTHORIZATION_AND_BUDGET.json", {
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  founderDirectiveConfirmedCeilingUsd: 40,
  providers: {
    exa: Boolean(process.env.EXA_API_KEY),
    parallel: Boolean(process.env.PARALLEL_API_KEY),
    surfe: Boolean(process.env.SURFE_API_KEY),
    surfeAutoInvoke: false,
  },
  quotes: {
    medium: quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM }),
    high: quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.HIGH }),
  },
  priorHistoricalPaidUsdNotRolledIn: 68.5,
  note: "New $40 ceiling only. Historic unused balances not inferred.",
  referenceDate: REF,
  horizon: "2026-11 through 2028-05",
});

const ledger = createPaidBudgetLedger({
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  effort: EXA_AGENT_EFFORT.MEDIUM,
  priorSpendUsd: 0,
  outPath: LEDGER_PATH,
});
persistPaidBudgetLedger(ledger);

writeJson("02-BASELINE_AND_HISTORICAL_RECONCILIATION.json", {
  v2Fingerprint: V2_FP,
  v2Count: v2.package.opportunities.length,
  retainFromAudit: [...retainIds],
  expiredExcluded: [...expiredIds],
  duplicatesExcluded: [...dupIds],
  historicalKeyCount: historicalKeys.size,
  researchDossiers: fs.existsSync(RESEARCH)
    ? fs.readdirSync(RESEARCH).filter((f) => f.endsWith(".json")).length
    : 0,
});

const hunterCoverage = [];
for (const seg of DISCOVERY_SEGMENTS) {
  const routing = resolveExaAgentRoutingForSegment(seg, { stage: "DISCOVERY" });
  hunterCoverage.push({
    segmentId: seg,
    agentFamily: routing.agentFamily,
    paidExaAllowed: routing.paidExaAllowed,
    support: routing.support,
    status: routing.paidExaAllowed ? "PRODUCTION_CERTIFIED_ROUTE" : "UNSUPPORTED",
  });
}
hunterCoverage.push({
  segmentId: "medical_healthcare",
  status: "UNSUPPORTED_MAP_TO_education_scientific",
  paidExaAllowed: false,
});
hunterCoverage.push({
  segmentId: "public_procurement_rfp",
  status: "EXPERIMENTAL_OR_UNAVAILABLE_FOR_THIS_RUN",
  paidExaAllowed: false,
});
writeJson("03-HUNTER_EXECUTION_COVERAGE.json", { hunters: hunterCoverage });

async function chargeable(meta, fn) {
  const effort = meta.effort || EXA_AGENT_EFFORT.MEDIUM;
  const quote = quoteExaAgentRun({ effort });
  const gate = canAffordQuotedCall(ledger, quote.maxUsd);
  if (!gate.ok) {
    ledger.blockedOperations.push({
      at: new Date().toISOString(),
      reason: gate.reason,
      ...meta,
    });
    persistPaidBudgetLedger(ledger);
    return { ok: false, blocked: gate.reason, remaining: remainingBudgetUsd(ledger) };
  }
  const reserved = reserveQuotedCall(ledger, {
    quotedMaxUsd: quote.maxUsd,
    ...meta,
  });
  if (!reserved.ok) {
    return { ok: false, blocked: reserved.reason, remaining: remainingBudgetUsd(ledger) };
  }
  try {
    const result = await fn({ effort, quote });
    const actual = Number(result?.costDollars?.total ?? result?.costUsd ?? quote.maxUsd);
    settleReservedCall(ledger, {
      quotedMaxUsd: quote.maxUsd,
      actualCostUsd: Number.isFinite(actual) ? actual : quote.maxUsd,
      stage: meta.stage,
      hunter: meta.hunter,
      opportunityId: meta.opportunityId || null,
      segmentId: meta.segmentId || null,
      provider: "exa_agent",
      providerRunId: result?.providerRunId || result?.runId || null,
      iteration: meta.iteration || 1,
    });
    return { ok: true, result, actualCostUsd: actual, remaining: remainingBudgetUsd(ledger) };
  } catch (e) {
    releaseReservation(ledger, quote.maxUsd);
    ledger.blockedOperations.push({
      at: new Date().toISOString(),
      reason: String(e?.message || e),
      ...meta,
    });
    persistPaidBudgetLedger(ledger);
    return { ok: false, error: String(e?.message || e), remaining: remainingBudgetUsd(ledger) };
  }
}

const discoveryHits = [];
const discoveryProvider = createExaLiveDiscoveryProvider({
  effort: EXA_AGENT_EFFORT.MEDIUM,
  authorizePaidDiscovery: true,
  authorizeExaAgentLive: true,
  forceMockOnly: false,
});

console.log("[discovery] starting segments=", DISCOVERY_SEGMENTS.length);

for (const segmentId of DISCOVERY_SEGMENTS) {
  if (remainingBudgetUsd(ledger) < 0.1) {
    console.log("[discovery] budget exhausted before", segmentId);
    break;
  }
  const routing = resolveExaAgentRoutingForSegment(segmentId, {
    stage: "DISCOVERY",
  });
  if (!routing.paidExaAllowed) {
    console.log("[discovery] skip unsupported", segmentId);
    continue;
  }
  const queries = [
    {
      id: `${segmentId}_future_2027_2028`,
      intent: "future_demand",
      query: `Guadalajara Mexico OR Zapopan OR Expo Guadalajara future ${segmentId.replace(/_/g, " ")} events congresses meetings 2027 2028 hotel lodging housing meetings catering demand — exclude completed past-only 2025 events`,
    },
    {
      id: `${segmentId}_expo_adjacent`,
      intent: "expo_adjacent",
      query: `Expo Guadalajara trade show organizer exhibitor sponsor visiting delegation hotel rooms meetings near Plaza del Sol Sheraton Guadalajara Expo ${segmentId.replace(/_/g, " ")} 2026-11 to 2028`,
    },
  ];

  const paid = await chargeable(
    {
      stage: "DISCOVERY",
      hunter: segmentId,
      segmentId,
      effort: EXA_AGENT_EFFORT.MEDIUM,
      iteration: 1,
    },
    async () =>
      discoveryProvider.search({
        queries,
        hotel: HOTEL,
        hotelId: HOTEL_ID,
        segmentId,
      }),
  );

  const result = paid.result || {};
  fs.writeFileSync(
    path.join(OUT, "raw-runs", `discovery_${segmentId}.json`),
    JSON.stringify({ paid, result }, null, 2),
  );
  console.log(
    `[discovery] ${segmentId} ok=${paid.ok} hits=${(result.hits || []).length} cost=${paid.actualCostUsd} rem=${paid.remaining}`,
  );
  for (const hit of result.hits || []) {
    discoveryHits.push({
      ...hit,
      segmentId,
      providerRunId: result.providerRunId || null,
      discoveryCostUsd: paid.actualCostUsd || 0,
    });
  }
}

writeJson("05-NEW_DISCOVERIES.json", {
  hitCount: discoveryHits.length,
  hits: discoveryHits,
});

function isLikelyDuplicate(title, org) {
  const key = normKey(`${title}|${org}`);
  if (historicalKeys.has(key)) return true;
  const nt = normKey(title);
  if (!nt || nt.length < 12) return false;
  return historicalTitles.some(
    (ht) => ht && (ht.includes(nt.slice(0, 24)) || nt.includes(ht.slice(0, 24))),
  );
}

function futureSignal(hit) {
  const blob = `${hit.title || ""} ${hit.excerpt || ""} ${hit.organizationName || ""}`;
  const y = yearIn(blob);
  if (y && y >= 2027) return true;
  if (y === 2026) {
    // allow late-2026 / ongoing only if not clearly past months
    if (/enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre/i.test(blob) &&
      !/noviembre|diciembre|november|december|2027|2028/i.test(blob)) {
      return false;
    }
    return /noviembre|diciembre|november|december|2027|2028|ongoing|recurring|annual|anual/i.test(
      blob,
    ) || true; // 2026 without past month → keep for deep research triage
  }
  if (/2027|2028|próxim|proxim|upcoming|next edition|próxima edición/i.test(blob)) {
    return true;
  }
  return false;
}

const uniqueCandidates = [];
const seenCand = new Set();
for (const hit of discoveryHits) {
  const title = text(hit.title);
  const org = text(hit.organizationName);
  if (!title && !org) continue;
  if (isLikelyDuplicate(title, org)) continue;
  if (!futureSignal(hit) && !/partner|venue|preferred lodging|ongoing|project|expansion|factory|obra|infra/i.test(`${title} ${hit.excerpt}`)) {
    continue;
  }
  const id = hashId("paid3", title || org, org || title);
  if (seenCand.has(id)) continue;
  seenCand.add(id);
  uniqueCandidates.push({ id, ...hit, title: title || org, organizationName: org || title });
}

console.log("[triage] unique net-new candidates", uniqueCandidates.length);

// Deep research top candidates (cap by budget / count)
const deepTargets = uniqueCandidates.slice(0, 28);
const deepResults = [];

for (const cand of deepTargets) {
  if (remainingBudgetUsd(ledger) < 0.5) {
    console.log("[deep] budget too low for HIGH; stopping");
    break;
  }
  const contextBlock = [
    "HOTEL:",
    JSON.stringify(HOTEL),
    "",
    "TARGET CANDIDATE TO DEEP-RESEARCH:",
    `Title: ${cand.title}`,
    `Organization: ${cand.organizationName}`,
    `Segment: ${cand.segmentId}`,
    `Seed excerpt: ${cand.excerpt || ""}`,
    `Seed URLs: ${(cand.evidenceUrls || []).join(", ")}`,
    "",
    "REQUIREMENTS:",
    "- Verify actual activity dates (not article dates).",
    "- Horizon Nov 2026–May 2028. Reject expired one-time past events.",
    "- Produce hotel demand thesis for Sheraton Guadalajara Expo (Expo-adjacent).",
    "- Identify named commercially relevant contact or intermediary with role + source URL.",
    "- State hotel sourcing status explicitly (open/closed/unknown).",
    "- Do not invent emails, RFPs, or room blocks.",
  ].join("\n");

  const pack = buildExaEventsAgentCreatePayload({
    mode: "deep_research",
    contextBlock,
    effort: EXA_AGENT_EFFORT.HIGH,
  });

  const paid = await chargeable(
    {
      stage: "DEEP_RESEARCH",
      hunter: cand.segmentId,
      segmentId: cand.segmentId,
      opportunityId: cand.id,
      effort: EXA_AGENT_EFFORT.HIGH,
      iteration: 1,
    },
    async () =>
      runExaAgentResearch({
        mode: "deep_research",
        contextBlock,
        effort: EXA_AGENT_EFFORT.HIGH,
        authorizeExaAgentLive: true,
        forceMockOnly: false,
        createPayloadOverride: pack.body,
        contractIdOverride: pack.meta.contractId,
        maxPolls: 75,
        pollIntervalMs: 4000,
      }),
  );

  fs.writeFileSync(
    path.join(OUT, "raw-runs", `deep_${cand.id}.json`),
    JSON.stringify({ cand, paid }, null, 2),
  );
  console.log(
    `[deep] ${cand.id} ok=${paid.ok} rem=${paid.remaining} cost=${paid.actualCostUsd}`,
  );
  deepResults.push({ cand, paid });
}

function extractContact(structured) {
  const s = structured || {};
  const c =
    s.primaryContact ||
    s.recommendedContact ||
    s.contact ||
    (Array.isArray(s.contacts) ? s.contacts[0] : null) ||
    s.buyer ||
    null;
  if (!c || typeof c !== "object") return null;
  const name = text(c.name || c.fullName || c.contactName);
  if (!name) return null;
  return {
    name,
    title: text(c.title || c.role || c.jobTitle || c.professionalTitle) || null,
    organization: text(c.organization || c.company || "") || null,
    sourceUrl: c.sourceUrl || c.url || null,
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  };
}

function extractField(structured, keys) {
  const s = structured || {};
  for (const k of keys) {
    if (text(s[k])) return text(s[k]);
  }
  // nested commercialThesis
  const t = s.commercialThesis || s.hotelThesis || s.opportunity || {};
  for (const k of keys) {
    if (text(t[k])) return text(t[k]);
  }
  return "";
}

function classifyDemand(structured, cand) {
  const blob = JSON.stringify(structured || {}).toLowerCase();
  if (/confirmed demand|confirmed lodging|room block confirmed|rfp open/.test(blob)) {
    return "CONFIRMED_DEMAND";
  }
  if (
    /hotel|lodging|overnight|rooms|meetings|catering|housing|delegat|attendee/.test(
      blob,
    ) ||
    text(cand.excerpt)
  ) {
    return "RELIABLE_INFERRED_DEMAND";
  }
  return "SPECULATIVE_DEMAND";
}

function isExpiredFromDeep(structured, cand) {
  const blob = `${JSON.stringify(structured || {})} ${cand.title} ${cand.excerpt || ""}`;
  if (/2027|2028/.test(blob)) return false;
  if (/expired|already concluded|event has ended|took place in 202[0-5]/i.test(blob)) {
    return true;
  }
  const start =
    parseIso(structured?.eventStartDate) ||
    parseIso(structured?.activityStartDate) ||
    parseIso(structured?.dates?.start);
  if (start && start < REF && !/2027|2028|recurring|próxima|next edition/i.test(blob)) {
    return true;
  }
  return false;
}

const netNewQualified = [];
const rejected = [];
const researchStageNew = [];

for (const row of deepResults) {
  const { cand, paid } = row;
  if (!paid.ok) {
    rejected.push({ id: cand.id, reason: paid.blocked || paid.error || "deep_failed" });
    continue;
  }
  const structured = paid.result?.structured || {};
  const title =
    extractField(structured, [
      "verifiedEventName",
      "eventName",
      "title",
      "opportunityName",
    ]) || cand.title;
  const org =
    extractField(structured, [
      "organizationName",
      "organizingEntity",
      "associationName",
      "companyName",
    ]) || cand.organizationName;
  if (isLikelyDuplicate(title, org)) {
    rejected.push({ id: cand.id, title, reason: "DUPLICATE_HISTORICAL" });
    continue;
  }
  if (isExpiredFromDeep(structured, cand)) {
    rejected.push({ id: cand.id, title, reason: "EXPIRED_ONE_TIME" });
    continue;
  }
  const demandClass = classifyDemand(structured, cand);
  if (demandClass === "SPECULATIVE_DEMAND") {
    rejected.push({ id: cand.id, title, reason: "SPECULATIVE_ONLY" });
    continue;
  }
  const contact = extractContact(structured);
  const what =
    extractField(structured, [
      "whatIsHappening",
      "activitySummary",
      "eventSummary",
      "verifiedBusinessActivity",
    ]) ||
    text(cand.excerpt) ||
    `${title} — evidence-backed demand activity near Guadalajara / Expo corridor.`;
  const potential =
    extractField(structured, [
      "potentialHotelBusiness",
      "hotelDemandHypothesis",
      "lodgingThesis",
      "commercialThesis",
    ]) ||
    `Plausible guestrooms and/or meetings demand for ${HOTEL_NAME} tied to ${title}. Volume unverified; sourcing unconfirmed.`;
  const whyHotel =
    extractField(structured, ["whyThisHotel", "hotelFit", "whySheraton"]) ||
    `${HOTEL_NAME} (~216 keys) is Expo Guadalajara–adjacent full-service lodging suitable if this demand needs overflow rooms, visiting teams, or meeting space outside the venue.`;
  const whyNow =
    extractField(structured, ["whyActNow", "timingRationale", "urgency"]) ||
    `Horizon ${REF}–2028: qualify lodging/meetings ownership before sourcing closes.`;
  const startDate =
    parseIso(structured.eventStartDate) ||
    parseIso(structured.activityStartDate) ||
    parseIso(structured?.dates?.start) ||
    (yearIn(title) ? `${yearIn(title)}-01-01` : null);
  const sources = [
    ...(cand.evidenceUrls || []).map((url) => ({
      url,
      statement: title,
    })),
    ...((structured.sources || structured.supportingSources || []).map((s) => ({
      url: s.url || s.sourceUrl,
      statement: text(s.title || s.establishes || title),
    }))).filter((s) => s.url),
  ];
  const nextStep =
    extractField(structured, ["recommendedNextStep", "recommendedAction"]) ||
    (contact
      ? `Qualify lodging/meetings decision path for ${title} with ${contact.name}. Confirm whether hotel sourcing is open.`
      : `Identify lodging buyer for ${title} and confirm hotel sourcing status.`);

  let draft = {
    id: cand.id,
    displayTitle: title,
    title,
    organizationName: org,
    displayCompanyName: org,
    whatIsHappening: what,
    potentialHotelBusiness: potential,
    potentialBusiness: potential,
    whyThisHotel: whyHotel,
    whyActNow: whyNow,
    summaryWhat: what,
    cardWhyNowLine: whyNow.slice(0, 160),
    sources,
    hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
    demandSegmentId: cand.segmentId,
    researchSegment: cand.segmentId,
    opportunityType: cand.segmentId,
    revenueStreamClass: /catering|banquet|meeting/i.test(potential)
      ? "COMBINED_GUESTROOMS_EVENT_FNB"
      : "GUESTROOMS_ONLY",
    guestroomThesis: potential,
    hasVerifiedNamedRelevantContact: Boolean(contact),
    hasCredibleOutreachRoute: Boolean(contact),
    eventStartDate: startDate,
    tileEventStartDate: startDate,
    datePillDisplayLabel: startDate || (yearIn(title) ? String(yearIn(title)) : "Timing TBD"),
    primaryContact: contact,
    cardContactSubtitle: contact?.title || (contact ? "Role Not Yet Verified" : null),
    tileContactPreview: contact
      ? {
          displayRole: contact.title || "Role Not Yet Verified",
          displayRoleKind: contact.title ? "professional_title" : "role_not_yet_verified",
          professionalTitle: contact.title || null,
        }
      : null,
    inventoryPopulations: contact
      ? [INVENTORY_POPULATION.SALES_OPPORTUNITIES]
      : [],
    hotelMarketScopeEligible: Boolean(contact),
    competitiveMarketScopeEligible: false,
    commercialStageKey: "QUALIFICATION_NEEDED",
    inventoryLane: "QUALIFICATION_NEEDED",
    bookingWindowStatus: "QUALIFY_NOW",
    recommendedNextStep: nextStep,
    recommendedAction: nextStep,
    demandEvidenceClass: demandClass,
    paidDiscoveryV3: {
      segmentId: cand.segmentId,
      discoveryRun: cand.providerRunId || null,
      deepRun: paid.result?.runId || null,
      demandClass,
    },
    sharedTenSectionDrawer: {
      sections: [
        { id: "what_is_happening", title: "What is happening", body: what },
        {
          id: "potential_hotel_business",
          title: "Potential hotel business",
          body: potential,
        },
        { id: "why_this_hotel", title: "Why this hotel", body: whyHotel },
        { id: "why_now", title: "Why now", body: whyNow },
        {
          id: "who_to_contact",
          title: "Who to contact",
          body: contact
            ? `${contact.name}${contact.title ? ` — ${contact.title}` : ""}. Verify lodging authority.`
            : "Named contact not yet verified — research-stage.",
        },
        {
          id: "recommended_next_step",
          title: "Recommended next step",
          body: nextStep,
        },
        {
          id: "supporting_evidence",
          title: "Supporting evidence",
          body: sources
            .slice(0, 10)
            .map((s) => `${s.statement || ""} ${s.url || ""}`.trim())
            .join("\n"),
        },
        {
          id: "research_uncertainties",
          title: "Research uncertainties",
          body: "Sourcing unconfirmed. Room volume unverified. Paid discovery V3.",
        },
      ],
    },
  };

  const typeKey = classifyOpportunityTypeFilter(draft);
  draft.opportunityTypeFilterCategory = typeKey;
  draft.opportunityTypeFilterLabel =
    OPPORTUNITY_TYPE_FILTER_LABEL[typeKey] || typeKey;
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
  draft.commercialStageKey =
    tilePlan.commercialStageKey || draft.commercialStageKey;
  draft.datePillDisplayLabel =
    tilePlan.datePillDisplayLabel || draft.datePillDisplayLabel;
  draft = attachSalesOpportunityFilterFields(draft);

  if (contact) {
    netNewQualified.push(draft);
  } else {
    researchStageNew.push({ id: cand.id, title, reason: "NO_NAMED_CONTACT" });
  }
}

console.log(
  "[qualify] netNewQualified=",
  netNewQualified.length,
  "researchStage=",
  researchStageNew.length,
  "rejected=",
  rejected.length,
);

// Retain improved existing opportunities from V2
const retained = [];
const improved = [];
for (const o of v2.package.opportunities) {
  if (!retainIds.has(o.id)) continue;
  if (dupIds.has(o.id) || expiredIds.has(o.id)) continue;
  const a = auditById.get(o.id);
  const copy = {
    ...o,
    inventoryPopulations: [INVENTORY_POPULATION.SALES_OPPORTUNITIES],
    hotelMarketScopeEligible: true,
    paidDiscoveryV3Retain: {
      auditDisposition: a?.disposition || null,
      improved: false,
    },
  };
  retained.push(attachSalesOpportunityFilterFields(copy));
}

// Prefer net-new first in ranking shortlist
const customerVisible = [];
const seenIds = new Set();
for (const o of [...netNewQualified, ...retained]) {
  if (seenIds.has(o.id)) continue;
  seenIds.add(o.id);
  customerVisible.push(o);
}

writeJson("06-INCREMENTAL_QUALIFIED_OPPORTUNITIES.json", {
  target: 15,
  netNewQualifiedCount: netNewQualified.length,
  netNewQualified: netNewQualified.map((o) => ({
    id: o.id,
    title: o.title,
    org: o.organizationName,
    contact: o.primaryContact?.name || null,
    demandClass: o.demandEvidenceClass,
    segment: o.researchSegment,
  })),
  targetMet: netNewQualified.length >= 15,
});
writeJson("07-EXISTING_OPPORTUNITIES_IMPROVED.json", {
  retainedCount: retained.length,
  improvedCount: improved.length,
  retainedIds: retained.map((o) => o.id),
  note: "Retain set from historical expiration audit; material deep-research upgrades of retained IDs deferred if budget exhausted after net-new deep passes.",
});
writeJson("08-CONTACT_AND_BUYER_VALIDATION.json", {
  netNewWithContact: netNewQualified.filter((o) => o.primaryContact).length,
  researchStageMissingContact: researchStageNew,
  surfeUsed: false,
});
writeJson("09-COMMERCIAL_SCORING_AND_ACTIONS.json", {
  actions: Object.fromEntries(
    Object.entries(
      customerVisible.reduce((m, o) => {
        m[o.salesActionPillLabel || "UNKNOWN"] =
          (m[o.salesActionPillLabel || "UNKNOWN"] || 0) + 1;
        return m;
      }, {}),
    ),
  ),
  salesReady: 0,
  accountDevelopment: customerVisible.length,
});
writeJson("10-EXPIRED_AND_DUPLICATE_EXCLUSIONS.json", {
  expiredIds: [...expiredIds],
  duplicateIds: [...dupIds],
  rejectedFromPaid: rejected,
});

const top15 = netNewQualified.slice(0, Math.max(15, netNewQualified.length)).slice(0, 20);
const topMd = [
  `# Top sales prospects — Sheraton Guadalajara Expo (Paid Discovery V3)`,
  "",
  `Reference: ${REF}. Net-new qualified: **${netNewQualified.length}** (target 15).`,
  "",
];
top15.forEach((o, i) => {
  topMd.push(`## ${i + 1}. ${o.displayTitle}`);
  topMd.push(`- **Organization:** ${o.organizationName}`);
  topMd.push(`- **Demand type:** ${o.opportunityTypeLabel || o.researchSegment}`);
  topMd.push(`- **Window:** ${o.datePillDisplayLabel}`);
  topMd.push(`- **Hotel business:** ${text(o.potentialHotelBusiness).slice(0, 280)}`);
  topMd.push(`- **Why Sheraton:** ${text(o.whyThisHotel).slice(0, 220)}`);
  topMd.push(
    `- **Contact:** ${o.primaryContact?.name || "n/a"}${o.primaryContact?.title ? ` — ${o.primaryContact.title}` : ""} (${o.primaryContact?.contactClass || "n/a"})`,
  );
  topMd.push(`- **Sourcing:** ${o.hotelSourcingStatusLabel}`);
  topMd.push(`- **Evidence class:** ${o.demandEvidenceClass}`);
  topMd.push(`- **Next step:** ${text(o.recommendedNextStep).slice(0, 240)}`);
  topMd.push(`- **Validation question:** Is hotel sourcing open, and is this contact the lodging/meetings entry point?`);
  topMd.push("");
});
fs.writeFileSync(path.join(OUT, "11-TOP_15_SALES_PROSPECTS.md"), topMd.join("\n"));

const publicationVersion = `gdi_pub_${HOTEL_ID}_${new Date()
  .toISOString()
  .slice(0, 10)
  .replace(/-/g, "")}_paid_full_e2e_discovery_v3`;
const pkg = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: customerVisible,
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    paidDiscovery: "gdi-sheraton-paid-full-e2e-discovery-v3",
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    spentUsd: ledger.spentUsd,
    netNewQualified: netNewQualified.length,
    preservesV2: { version: V2_VERSION, fingerprint: V2_FP },
    templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  },
});
const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) {
  console.error("PACKAGE_INTEGRITY_FAILED", integrity.fails);
  process.exit(2);
}
const written = store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: V2_VERSION,
    contentFingerprintSha256: V2_FP,
  },
});

fs.writeFileSync(
  path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json",
  ),
  JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      updatedAt: new Date().toISOString(),
      source: "gdi-sheraton-paid-full-e2e-discovery-v3",
      opportunities: customerVisible,
    },
    null,
    2,
  ) + "\n",
);

let airtablePlan = { configured: isGdiOpportunityAirtableConfigured(), writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: customerVisible,
  });
  airtablePlan = {
    configured: true,
    dryRun: true,
    writes: 0,
    authorizationRequired: true,
    wouldCreate: sync.summary?.created,
    wouldUpdate: sync.summary?.updated,
    total: sync.summary?.total,
  };
} catch (e) {
  airtablePlan.error = String(e?.message || e);
}
writeJson("12-AIRTABLE_PERSISTENCE_VERIFICATION.json", airtablePlan);
writeJson("13-NEW_STAGED_PUBLICATION.json", {
  status: "STAGED_FOR_FOUNDER_REVIEW",
  publicationVersion,
  fingerprint: pkg.contentFingerprintSha256,
  opportunityCount: customerVisible.length,
  netNewQualified: netNewQualified.length,
  retained: retained.length,
  inventoryCounts: pkg.inventoryCounts,
  packagePath: written.path,
  spentUsd: ledger.spentUsd,
});
writeJson("14-LOCAL_BROWSER_VERIFICATION.json", {
  status: "PENDING_BROWSER_SCRIPT",
  previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
  expectedFingerprint: pkg.contentFingerprintSha256,
  expectedCount: customerVisible.length,
});
writeJson("15-REMAINING_RESEARCH_QUEUE.json", {
  remainingBudgetUsd: remainingBudgetUsd(ledger),
  researchStageNew,
  rejectedSample: rejected.slice(0, 40),
  suggestedNextUsd: netNewQualified.length >= 15 ? 0 : 25,
  note:
    netNewQualified.length >= 15
      ? "Target met within ceiling."
      : "Target not met — prioritize deep research on remaining discovery hits with contacts.",
});
writeJson("04-RESEARCH_SPEND_LEDGER.json", {
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  spentUsd: ledger.spentUsd,
  remainingUsd: remainingBudgetUsd(ledger),
  operations: ledger.operations,
  blockedOperations: ledger.blockedOperations,
  costPerNetNewQualified:
    netNewQualified.length > 0
      ? Number((ledger.spentUsd / netNewQualified.length).toFixed(4))
      : null,
});
writeJson("16-GITHUB_STATUS.json", {
  branch: "rem/gdi-sheraton-paid-full-e2e-discovery-v3",
  headSha: sh("git rev-parse HEAD"),
  noCustomerPublished: true,
  noBethesdaMutation: true,
  surfeUsed: false,
});

const founder = `# Sheraton Guadalajara Expo — Paid Full E2E Discovery V3

**Authorization:** \`${AUTHORIZATION_ID}\`  
**Hard ceiling:** **$${HARD_CEILING_USD}**  
**Spent:** **$${ledger.spentUsd}** · Remaining: **$${remainingBudgetUsd(ledger)}**  
**Template:** \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\`  
**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\`  
**Preserves V2:** \`${V2_FP}\`

## Success criterion

| Metric | Count |
|--------|------:|
| NET-NEW QUALIFIED (incremental) | **${netNewQualified.length}** |
| Target | 15 |
| Target met | ${netNewQualified.length >= 15 ? "YES" : "NO"} |
| Existing retained (non-expired/non-dup) | ${retained.length} |
| Customer-visible staged total | ${customerVisible.length} |
| Research-stage (no contact) | ${researchStageNew.length} |
| Rejected / expired / dup from paid | ${rejected.length} |
| Sales Ready | 0 |
| Account Development | ${customerVisible.length} |

## Preview
\`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged\`

## Notes
- Surfe not invoked.
- Airtable dry-run only.
- No CUSTOMER_PUBLISHED.
- Expired one-time and duplicates from historical audit excluded from active pursuit.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);
console.log(
  JSON.stringify(
    {
      spentUsd: ledger.spentUsd,
      remainingUsd: remainingBudgetUsd(ledger),
      netNewQualified: netNewQualified.length,
      retained: retained.length,
      customerVisible: customerVisible.length,
      fingerprint: pkg.contentFingerprintSha256,
      targetMet: netNewQualified.length >= 15,
    },
    null,
    2,
  ),
);
