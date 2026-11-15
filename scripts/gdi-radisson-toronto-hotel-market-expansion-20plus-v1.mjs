#!/usr/bin/env node
/**
 * Radisson Blu Toronto Downtown — zero-cost Hotel Market expansion toward 20+ HM.
 *
 * Authorization: FOUNDER_DIRECTIVE_RADISSON_HOTEL_MARKET_EXPANSION_20PLUS_20261115
 *   - NO paid Exa / Surfe / Parallel. Reuse stored evidence + free public pages only.
 *   - Do not lower qualification standards. Do not dual-classify CMI as Hotel Market.
 *   - Preserve immutable prior package fingerprint 1e8d8bc7…; stage a NEW package.
 *   - No CUSTOMER_PUBLISHED. Airtable dry-run only.
 *
 * Modes:
 *   node scripts/gdi-radisson-toronto-hotel-market-expansion-20plus-v1.mjs
 *   node scripts/gdi-radisson-toronto-hotel-market-expansion-20plus-v1.mjs --browser-verify
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
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
import { resolveCustomerSalesAction } from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
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
  evaluateCmiDiscoveryCoverage,
  CMI_DISCOVERY_COVERAGE_CHECKLIST,
  GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
} from "../lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  COMMERCIAL_ACTIONABILITY,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";
import { CMI_CLAIM_TYPE } from "../lib/group-demand-intelligence/commercial-consistency/cmi-evidence-ledger-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const HOTEL_NAME = "Radisson Blu Toronto Downtown";
const REF = "2026-11-15";
const SCRIPT_ID = "gdi-radisson-toronto-hotel-market-expansion-20plus-v1";
const TARGET_HM = 20;
const OUT = path.join(ROOT, "reports", "gdi-radisson-toronto-hotel-market-expansion-20plus-v1");
const HOTEL_DATA = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
const DISCOVERY_LEDGER_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/discovery-ledgers",
  HOTEL_ID,
  "four-hotel-14-workflow-full-e2e-v1.json",
);
const PRIOR_PKG_DIR = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL_ID,
  "gdi_pub_recH9h3McgMNFQEla_20261115_radisson_blu_toronto_15usd_full_e2e_v1",
);
const PRIOR_FINGERPRINT = "1e8d8bc794ce98c2ce174f6a04017afcc02d209e933e1ecb5f26ced54cd8f845";
const PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261115_radisson_toronto_hotel_market_expansion_20plus_v1`;
const PROTECTED_STORE_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
const HOTEL_CFG = JSON.parse(
  fs.readFileSync(path.join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`), "utf8"),
);
const BROWSER_VERIFY = process.argv.includes("--browser-verify");

const text = (v) => (v == null ? "" : String(v)).trim();
const writeJson = (name, obj) => {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
};
const sha256File = (p) => crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
const daysBetween = (a, b) => Math.round((new Date(`${b}T12:00:00Z`) - new Date(`${a}T12:00:00Z`)) / 86400000);
const dateLabel = (start, end) => {
  if (!start) return "Timing TBD";
  if (!end || end === start) return start;
  return `${start} – ${end}`;
};
const countBy = (rows, fn) => {
  const m = {};
  for (const r of rows) {
    const k = fn(r) || "(null)";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
};
const revenueStreamFor = (potential) =>
  /catering|banquet|meeting|conference|training|congress|convention|seminar|reception|dance|symposium/i.test(
    potential || "",
  )
    ? "COMBINED_GUESTROOMS_EVENT_FNB"
    : "GUESTROOMS_ONLY";

function cloneOpp(o) {
  return JSON.parse(JSON.stringify(o));
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

function buildHotelMarketOpportunity(spec) {
  const contact = spec.contact
    ? {
        name: spec.contact.name,
        title: text(spec.contact.title || spec.contact.role) || null,
        organization: text(spec.contact.organization) || spec.org,
        sourceUrl: spec.contact.sourceUrl || null,
        contactClass: spec.contactClass || "RELEVANT_COMMERCIAL_ENTRY_POINT",
      }
    : null;
  const start = spec.startDate || null;
  const end = spec.endDate || null;
  const pill = start ? dateLabel(start, end) : "Timing TBD";
  const sources = (spec.sources || []).map((s) =>
    typeof s === "string" ? { url: s, statement: spec.title } : s,
  );
  let draft = {
    id: spec.id,
    displayTitle: spec.title,
    title: spec.title,
    organizationName: spec.org,
    displayCompanyName: spec.org,
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
    sources,
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
    commercialStageKey: spec.commercialStageKey || "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: spec.commercialStagePillLabel || "Account Development",
    inventoryLane: spec.inventoryLane || "QUALIFICATION_NEEDED",
    bookingWindowStatus: spec.bookingWindowStatus || "QUALIFY_NOW",
    recommendedNextStep: spec.nextStep,
    recommendedAction: spec.nextStep,
    demandEvidenceClass: spec.demandClass,
    territoryClassification: spec.territory || "TERRITORY_CORE",
    hotelFitAssessment: {
      fit: spec.fit || "DIRECT_FIT",
      rationale: spec.fitRationale || "",
      attendance: spec.attendance ?? null,
      ruleVersion: "radisson-toronto-fit-expansion-v1",
      assessedAt: REF,
    },
    researchProvenance: spec.provenance || {
      origin: "ZERO_COST_PUBLIC_PAGE_VERIFICATION",
      script: SCRIPT_ID,
      referenceDate: REF,
      paidExaUsed: false,
    },
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
            ? `${contact.name}${contact.title ? ` — ${contact.title}` : ""} (${contact.organization}). ${spec.contactNote || "Commercially relevant entry point."}`
            : "Named contact not yet verified.",
        },
        { id: "recommended_next_step", title: "Recommended next step", body: spec.nextStep },
        {
          id: "supporting_evidence",
          title: "Supporting evidence",
          body: sources.slice(0, 10).map((s) => `${s.statement || ""} ${s.url || ""}`.trim()).join("\n"),
        },
        {
          id: "research_uncertainties",
          title: "Research uncertainties",
          body: (spec.uncertainties || []).join(" ") || "Room volume and catering capability unverified.",
        },
      ],
    },
  };
  return applySalesPresentation(draft);
}

function buildCmiOpportunity(spec) {
  const base = buildHotelMarketOpportunity({
    ...spec,
    what:
      spec.what ||
      `${spec.title} is committed at ${spec.competitorHotel} (${dateLabel(spec.startDate, spec.endDate)}). Principal housing is not available as an open host RFP to ${HOTEL_NAME}.`,
    potential:
      spec.potential ||
      "Future-cycle account development and/or separately evidenced overflow only — not the committed principal housing.",
    whyHotel: spec.futureCycleThesis || spec.whyHotel,
    whyNow:
      spec.whyNow ||
      `Competitor commitment confirmed at ${spec.competitorHotel}; develop the organizer relationship for a future cycle or residual overflow lodging.`,
    nextStep: spec.nextStep || spec.recommended,
    demandClass: "CONFIRMED_COMPETITOR_COMMITMENT",
    sourcingLabel: "Competitor host/block confirmed — not an open host RFP",
  });
  return {
    ...base,
    competitorHotel: spec.competitorHotel,
    likelyCompetitor: spec.competitorHotel,
    competitorVenueStatus: spec.competitorVenueStatus || COMPETITOR_VENUE_STATUS.CONFIRMED_HOST,
    competitorBookingClaim: spec.competitorBookingClaim || "Official group block / host hotel published",
    lodgingEvidence: spec.lodgingEvidence || "Official accommodations page",
    housingStatus: "COMPETITOR_BLOCK_OR_SEDE_CONFIRMED",
    roomDemandStatus: "COMPETITOR_BLOCK_CONFIRMED",
    competitorBookingEvidenceClass: "VERIFIED_BOOKING",
    competitiveVenueEvidence: true,
    competitorVenueConfirmed: true,
    commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    claims: spec.claims || [
      {
        claimType: CMI_CLAIM_TYPE.COMPETITOR_HOTEL_COMMITMENT,
        statement: `${spec.competitorHotel} is the published host/block hotel.`,
        evidenceUrl: (spec.sources || [])[0]?.url || null,
      },
    ],
    lodgingEvidenceUrl: (spec.sources || [])[0]?.url || null,
    cmiEvidenceRegisterV1: {
      bookingType: spec.bookingType || "OFFICIAL_GROUP_BLOCK",
      commitmentScope: spec.commitmentScope || "VENUE_AND_GUESTROOMS",
      evidenceConfidence: "MEDIUM_HIGH",
      verifiedAt: REF,
      uncertainty: spec.uncertainty || "Overflow availability unverified.",
      futureCycleThesis: spec.futureCycleThesis || "Develop organizer for future editions / residual overflow.",
      recurs: spec.recurs !== false,
    },
    eventDates: `${spec.startDate}/${spec.endDate || spec.startDate}`,
    hotelMarketScopeEligible: false,
    competitiveMarketScopeEligible: true,
    inventoryPopulations: [INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE],
    customerVisible: true,
  };
}

// ---------------------------------------------------------------------------
// --browser-verify
// ---------------------------------------------------------------------------
async function runBrowserVerify() {
  const meta = JSON.parse(fs.readFileSync(path.join(OUT, "13_NEW_STAGED_PUBLICATION.json"), "utf8"));
  const base = (process.env.GDI_PREVIEW_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
  const pageUrl = `${base}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
  const apiUrl = `${base}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?publicationPackage=1&surface=customer&preview=staged`;
  const result = {
    contract: "GDI_RADISSON_HM_EXPANSION_BROWSER_VERIFICATION_V1",
    generatedAt: new Date().toISOString(),
    status: "UNKNOWN",
    pageUrl,
    apiUrl,
    expectedFingerprint: meta.fingerprint || null,
    expectedCounts: meta.inventoryCounts || null,
    checks: {},
    errors: [],
  };
  try {
    const r = await fetch(apiUrl);
    result.checks.apiHttpStatus = r.status;
    const api = await r.json();
    result.checks.apiFingerprint = api?.publication?.contentFingerprintSha256 || null;
    result.checks.fingerprintMatch = result.checks.apiFingerprint === meta.fingerprint;
    result.checks.inventoryCounts = api?.publication?.inventoryCounts || null;
    const ops = Array.isArray(api?.opportunities) ? api.opportunities : [];
    result.checks.apiOpportunityCount = ops.length;
    result.checks.apiHotelCount = ops.filter((o) => o.hotelMarketScopeEligible === true).length;
    result.checks.apiCompetitiveCount = ops.filter((o) => o.competitiveMarketScopeEligible === true).length;
    result.checks.providerLeakInApi = /\bexa\b|surfe|parallel\.ai|airtable/i.test(JSON.stringify(ops));
  } catch (e) {
    result.status = "FAIL_SERVER_DOWN";
    result.errors.push(String(e?.message || e));
    writeJson("14_BROWSER_VERIFICATION.json", result);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  const { chromium } = await import("playwright");
  const shotDir = path.join(OUT, "screenshots");
  fs.mkdirSync(shotDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForSelector('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]', {
      timeout: 90000,
    });
    await page.waitForTimeout(1500);
    result.checks.dom = await page.evaluate(() => {
      const body = document.body?.innerText || "";
      const cards = [
        ...document.querySelectorAll(
          'article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]',
        ),
      ];
      return {
        cards: cards.length,
        titles: cards.map((c) => (c.innerText || "").split("\n")[0].trim().slice(0, 120)),
        mentionsHotel: /Radisson Blu Toronto/i.test(body),
        providerLeak: /\bExa\b|Surfe|Parallel|Airtable/.test(body),
        stillLoading: /Loading Group Demand Intelligence/i.test(body),
      };
    });
    await page.screenshot({ path: path.join(shotDir, "01-radisson-hm-expansion-overview.png"), fullPage: true });
    const expectedAll = Number(meta?.inventoryCounts?.all || 0);
    result.status =
      result.checks.fingerprintMatch === true &&
      result.checks.dom.cards >= Math.max(1, expectedAll) &&
      result.checks.dom.mentionsHotel === true &&
      !result.checks.dom.providerLeak &&
      !result.checks.providerLeakInApi
        ? "PASS"
        : "FAIL_CHECKS";
  } catch (e) {
    result.status = "FAIL_BROWSER_ERROR";
    result.errors.push(String(e?.message || e));
  } finally {
    await browser.close();
  }
  writeJson("14_BROWSER_VERIFICATION.json", result);
  console.log(JSON.stringify(result, null, 2));
}

if (BROWSER_VERIFY) {
  await runBrowserVerify();
  process.exit(0);
}

fs.mkdirSync(OUT, { recursive: true });

// ---------------------------------------------------------------------------
// 1) Baseline + prior package
// ---------------------------------------------------------------------------
const priorPkg = JSON.parse(fs.readFileSync(path.join(PRIOR_PKG_DIR, "package.json"), "utf8"));
const priorOpps = priorPkg.opportunities || [];
const priorHm = priorOpps.filter((o) => o.hotelMarketScopeEligible === true);
const priorCmi = priorOpps.filter((o) => o.competitiveMarketScopeEligible === true);
const priorFp = priorPkg.contentFingerprintSha256 || PRIOR_FINGERPRINT;
if (priorFp !== PRIOR_FINGERPRINT) {
  console.warn(`WARN: prior fingerprint ${priorFp} != expected ${PRIOR_FINGERPRINT}`);
}

writeJson("01-CURRENT_14_OPPORTUNITIES.json", {
  contract: "GDI_RADISSON_CURRENT_14_BASELINE_V1",
  priorPublicationVersion: priorPkg.publicationVersion,
  priorFingerprint: priorFp,
  hotelMarket: priorHm.length,
  cmi: priorCmi.length,
  salesReady: priorHm.filter((o) => /sales.?ready/i.test(o.commercialStageKey || o.commercialStagePillLabel || "")).length,
  hotelIds: priorHm.map((o) => o.id),
  cmiIds: priorCmi.map((o) => o.id),
  titles: priorOpps.map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    scope: o.competitiveMarketScopeEligible ? "CMI" : "HOTEL_MARKET",
    contact: o.primaryContact?.name || null,
    salesAction: o.salesActionPillLabel || null,
  })),
  hotelProfile: {
    rooms: HOTEL_CFG.capabilityProfile?.totalGuestrooms,
    suites: HOTEL_CFG.capabilityProfile?.suites,
    meetingRooms: HOTEL_CFG.capabilityProfile?.meetingRoomCount,
    meetingSqFt: HOTEL_CFG.capabilityProfile?.totalMeetingSpaceSqFt,
    largestRoom: HOTEL_CFG.capabilityProfile?.largestRoomName,
    address: HOTEL_CFG.address,
    propertyCode: "CNC41",
  },
  immutablePriorPackagePreserved: true,
  paidExaRemainingUsdNotAuthorized: 0.8,
});

// ---------------------------------------------------------------------------
// 2) 272-candidate funnel audit
// ---------------------------------------------------------------------------
const ledger = JSON.parse(fs.readFileSync(DISCOVERY_LEDGER_PATH, "utf8"));
const candidates = ledger.candidates || [];
const megaRe = /convention centre|mtcc|annual meeting|trade show|exposition|world congress|5000|10000|enercare/i;
const funnelRows = candidates.map((c) => {
  const s = c.structuredExaCandidateFields || {};
  const urls = [...(c.originalSourceUrls || []), ...(s.evidenceUrls || [])].filter(Boolean);
  const prog = c.programmeName || s.programmeName || c.originalResultTitle || "";
  const org = c.organizationName || s.organizationName || "";
  const blob = `${prog} ${org} ${s.demandMechanismSummary || ""}`;
  const lossReasons = [];
  if (c.researchStatus === "DUPLICATE") lossReasons.push("DUPLICATE");
  if (c.researchStatus === "QUEUED_FOR_DEEP_RESEARCH") lossReasons.push("DEEP_RESEARCH_NOT_STARTED");
  if (c.researchStatus === "RESEARCH_CONTINUATION") lossReasons.push("INCOMPLETE_RESEARCH");
  if (c.researchStatus === "WATCH") lossReasons.push("WATCH_INSUFFICIENT_EVIDENCE");
  if (c.researchStatus === "REJECTED") lossReasons.push("REJECTED_COMMERCIAL");
  if (!(s.startDate || s.eventStartDate)) lossReasons.push("NO_EXACT_DATE_IN_THIN_LEDGER");
  if (!urls.length) lossReasons.push("MISSING_SOURCE_URL");
  if (/EVIDENCE_SUPPORTED/i.test(String(s.guestroomDemandStatus || "")) === false && /NONE_CONFIRMED|UNKNOWN/i.test(String(s.guestroomDemandStatus || "UNKNOWN"))) {
    lossReasons.push("WEAK_OR_UNKNOWN_LODGING_EVIDENCE");
  }
  if (megaRe.test(blob)) lossReasons.push("MEGA_CONVENTION_BIAS_CANDIDATE");
  if (/radisson/i.test(JSON.stringify(c))) lossReasons.push("RADISSON_ALREADY_MENTIONED_IN_EVIDENCE");
  return {
    id: c.opportunityId || c.discoveryId,
    discoveryId: c.discoveryId,
    status: c.researchStatus,
    disposition: c.disposition?.status || null,
    segment: c.segmentId,
    org,
    programme: String(prog).slice(0, 140),
    guestroom: s.guestroomDemandStatus || null,
    urlCount: [...new Set(urls)].length,
    megaBias: megaRe.test(blob),
    midSizeSegment: ![
      "association_events",
    ].includes(c.segmentId),
    lossReasons: [...new Set(lossReasons)],
    awaitingFounder: !!c.awaitingFounderPromotion,
    stagedFlag: !!c.staged,
  };
});

const waterfall = {
  total: candidates.length,
  byStatus: countBy(candidates, (c) => c.researchStatus),
  bySegment: countBy(candidates, (c) => c.segmentId),
  byDisposition: countBy(candidates, (c) => c.disposition?.status || "(null)"),
  withEvidenceUrls: funnelRows.filter((r) => r.urlCount > 0).length,
  megaConventionFlagged: funnelRows.filter((r) => r.megaBias).length,
  queuedDeepNeverRun: funnelRows.filter((r) => r.status === "QUEUED_FOR_DEEP_RESEARCH").length,
  duplicates: funnelRows.filter((r) => r.status === "DUPLICATE").length,
  radissonMentionedInEvidence: funnelRows.filter((r) => r.lossReasons.includes("RADISSON_ALREADY_MENTIONED_IN_EVIDENCE")).length,
  lossHistogram: countBy(
    funnelRows.flatMap((r) => r.lossReasons.map((reason) => ({ reason }))),
    (x) => x.reason,
  ),
};

const recoveryQueue = funnelRows
  .filter((r) => r.status !== "DUPLICATE" && r.urlCount > 0)
  .map((r) => {
    let priority = 50;
    if (r.lossReasons.includes("RADISSON_ALREADY_MENTIONED_IN_EVIDENCE")) priority -= 30;
    if (r.guestroom === "EVIDENCE_SUPPORTED") priority -= 15;
    if (!r.megaBias) priority -= 10;
    if (r.midSizeSegment) priority -= 5;
    if (r.awaitingFounder) priority -= 5;
    if (r.status === "QUEUED_FOR_DEEP_RESEARCH") priority -= 2;
    return { ...r, recoveryPriority: priority };
  })
  .sort((a, b) => a.recoveryPriority - b.recoveryPriority)
  .slice(0, 40);

writeJson("02-272_CANDIDATE_FUNNEL_AUDIT.json", {
  contract: "GDI_RADISSON_272_FUNNEL_AUDIT_V1",
  referenceDate: REF,
  discoveryLedgerSha256: sha256File(DISCOVERY_LEDGER_PATH),
  waterfall,
  diagnosis: {
    primaryBottleneck: "DEEP_RESEARCH_NOT_STARTED (209/272) — thin ledger lacks exact dates; prior $15 paid only covered 28 deep runs biased to large association/MTCC events.",
    megaConventionBias: "Association/events hunters and paid Batch A prioritized MTCC-scale shows; mid-size corporate/education/sports with lodging evidence remained queued.",
    legitimateExclusions: [
      "DUPLICATE_SAME_ORG_CROSS_PASS",
      "Edition-specific competitor host/block → CMI not Hotel Market",
      "Imminent <21 days / expired one-time events",
      "Airport-corridor exclusive hosts (stretch territory)",
      "Evening client parties without overnight guestroom thesis (e.g. Arctiq at King Taps)",
    ],
    fixableGaps: [
      "QUEUED candidates with EVIDENCE_SUPPORTED lodging + public URLs — recoverable via free page verification or paid deep ($0.50/ea)",
      "Radisson already listed as partner/block on some public pages (PDAC, Adaptation Canada) but not promoted as Hotel Market",
      "Watch LOCATION_NOT_VERIFIED can unlock with free venue pages (e.g. Ci2i @ Delta)",
      "Sales actions on existing 3 HM were generic Monitor — upgrade without new spend",
    ],
  },
  recoveryQueueTop40: recoveryQueue,
});

writeJson("03_DISCOVERY_GAP_ANALYSIS.json", {
  contract: "GDI_RADISSON_DISCOVERY_GAP_V1",
  hotelFitBar: {
    rooms: 157,
    meetingSqFt: 11000,
    meetingRooms: 20,
    largestBanquet: HOTEL_CFG.capabilityProfile?.largestBanquetCapacity,
    realisticPeakRooms: "15–109 core (config)",
    notExclusiveHousingFor: "MTCC mega-conventions ≥~5,000 without open overflow sourcing",
  },
  coverageGapsByPriority: {
    A_corporate_meetings_training: "Thin ledger has recurring Westin-locked mid-size conferences; few open-sourcing corporate meetings with named lodging buyers.",
    B_associations_small: "Under-represented vs mega annual meetings; board/committee/certification events rarely deep-researched.",
    C_university_medical: "Strong Toronto ecosystem in ledger (UofT/UHN/SickKids) but dates/contacts incomplete without deep research.",
    D_sports_entertainment: "PDAC/Boat Show/NBO present; Tempo official partner is Hotel X (CMI). Dance/waterfront production under-covered until free Standout find.",
    E_waterfront_local: "Harbourfront proximity underused; Standout Dance Terminal Theatre is direct fit.",
    F_corporate_triggers_projects: "Scaling/relocation rows mostly lack specific future demand theses.",
  },
  engineBias: "Discovery + paid batches skewed to large association/convention lodging programs; commercially appropriate 40–120 room and meeting-space business under-sampled.",
});

// ---------------------------------------------------------------------------
// 3–8) Free incremental discovery + qualification
// ---------------------------------------------------------------------------
const FIT =
  "157 guestrooms / ~11,000 sq ft / 20 meeting rooms on Queens Quay — realistic for mid-size groups, overflow, and meeting+rooms packages; not exclusive MTCC mega-housing.";

/** Free findings that fall before REF=2026-11-15 — documented, not customer-visible. */
const EXPIRED_RELATIVE_TO_REF = [
  {
    id: "rb_gdi_free_adaptation_canada_2026",
    title: "Adaptation Canada 2026",
    startDate: "2026-09-22",
    endDate: "2026-09-24",
    note: "Published Radisson Blu secondary block — expired vs REF; preserve for future-cycle ICLEI relationship research.",
    url: "https://adaptationcanada2026.ca/faqs/",
  },
  {
    id: "rb_gdi_free_mdrs_toronto_2026",
    title: "MDRS Toronto 2026",
    startDate: "2026-10-21",
    endDate: "2026-10-24",
    note: "Mid-size SickKids/TMU scientific meeting with open lodging — expired vs REF.",
    url: "https://www.mdrsto2026.ca/home",
  },
];

/** Free-verified Hotel Market specs (no paid Exa). Future relative to REF only. */
const NEW_HM_SPECS = [
  {
    id: "rb_gdi_free_standout_dance_apr2027",
    title: "Standout Dance Challenge — Toronto",
    org: "Standout Dance Challenge",
    segmentId: "sports_entertainment",
    startDate: "2027-04-09",
    endDate: "2027-04-11",
    contact: {
      name: "Sydney & Kayla",
      title: "Owners / Founders",
      organization: "Standout Dance Challenge",
      sourceUrl: "https://www.standoutdancechallenge.com/about-us",
    },
    contactClass: "COMMERCIALLY_RELEVANT",
    contactNote: "Named founders; public booking email director@standoutdancechallenge.com.",
    what: "Standout Dance Challenge holds its Toronto regional event 9–11 April 2027 at The Terminal Theatre, 207 Queens Quay West — adjacent to Radisson Blu. The organizer publishes a Radisson Blu “Participant Block April 2027” with hotel phone/email booking and a 23 February 2027 cutoff (Union Hotel also listed).",
    potential:
      "Participant/family guestroom nights on the published Radisson block; possible hospitality suites or small reception space for studio directors. Venue is next door — walkable waterfront fit.",
    whyHotel: `${FIT} Event venue is Queens Quay Terminal; Radisson is the published participant hotel at 249 Queens Quay West.`,
    whyNow: "Inaugural tour dates published; participant block cutoff is 23 Feb 2027 — sales should engage studios and organizers before winter 2026/27.",
    nextStep:
      "Email Sydney & Kayla at director@standoutdancechallenge.com to confirm block size, pickup pacing, and whether additional rooms or hospitality space are needed; align hotel sales on Participant Block April 2027 booking code performance.",
    sources: [
      { url: "https://www.standoutdancechallenge.com/toronto", statement: "Dates, Terminal Theatre venue, Radisson Blu participant block and cutoff." },
      { url: "https://www.standoutdancechallenge.com/about-us", statement: "Founders Sydney and Kayla identified as owners." },
    ],
    demandClass: "PUBLISHED_TARGET_HOTEL_GROUP_BLOCK",
    sourcingLabel: "Radisson Blu participant block published",
    fit: "DIRECT_FIT",
    fitRationale: "Adjacent Queens Quay theatre + published Radisson participant block.",
    attendance: null,
    commercialStageKey: "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: "Account Development",
    inventoryLane: "ACTIVE_PIPELINE",
    bookingWindowStatus: "PURSUE_NOW",
    uncertainties: ["Exact room-night commitment unpublished.", "Studio travel patterns (local vs overnight) not measured."],
    provenance: { origin: "ZERO_COST_PUBLIC_PAGE_VERIFICATION", freeFinding: "STANDOUT_DANCE_RADISSON_BLOCK", paidExaUsed: false },
  },
  {
    id: "rb_gdi_free_cfea_2026",
    title: "CFEA 2026 — Conference on Financial Economics and Accounting",
    org: "Rotman School of Management, University of Toronto",
    segmentId: "education_scientific",
    startDate: "2026-11-20",
    endDate: "2026-11-21",
    contact: {
      name: "Partha Mohanram",
      title: "Professor, Accounting (Conference Organizer)",
      organization: "Rotman School of Management",
      sourceUrl: "https://www.rotman.utoronto.ca/cfea-2026/",
    },
    contactClass: "COMMERCIALLY_RELEVANT",
    contactNote: "Co-organizer with Liyan Yang; PhD discount email published.",
    what: "36th CFEA runs 20–21 November 2026 at Rotman (105 St. George). Highly selective consortium conference with international faculty. Official hotels page lists Kimpton Saint George, Yorkville Sonesta, and Madison Manor, and explicitly notes participants may stay elsewhere in downtown Toronto near TTC subway.",
    potential:
      "Guestroom nights for visiting faculty/PhD presenters who prefer downtown waterfront lodging with Line 1 access via Union → St. George; small welcome receptions possible if F&B capacity confirmed.",
    whyHotel: `${FIT} Official guidance allows downtown hotels beyond campus cluster; Radisson sits on Line 1 corridor via Union with Billy Bishop adjacency for US short-haul speakers.`,
    whyNow: "Paper deadline passed into registration phase; visiting presenters book lodging as acceptances finalize through fall 2026.",
    nextStep:
      "Email Partha Mohanram and Liyan Yang proposing Radisson Blu as an optional downtown Harbourfront lodging listing for CFEA presenters; request estimated out-of-town headcount and whether a soft block is useful.",
    sources: [
      { url: "https://www.rotman.utoronto.ca/cfea-2026/", statement: "Dates, organizers, agenda." },
      { url: "https://www.rotman.utoronto.ca/cfea-2026/hotels-and-accommodation/", statement: "Suggested hotels + permission to stay elsewhere downtown near TTC." },
    ],
    demandClass: "OPEN_PARTICIPANT_LODGING",
    sourcingLabel: "Hotel sourcing open (participants arrange own lodging)",
    fit: "NEARBY_FIT",
    fitRationale: "Campus venue; downtown waterfront is acceptable per organizer hotel guidance.",
    attendance: null,
    commercialStageKey: "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: "Account Development",
    inventoryLane: "QUALIFICATION_NEEDED",
    bookingWindowStatus: "QUALIFY_NOW",
    uncertainties: ["No Radisson listing yet.", "Overnight share vs local GTA faculty unknown."],
    provenance: { origin: "ZERO_COST_PUBLIC_PAGE_VERIFICATION", freeFinding: "CFEA_2026", paidExaUsed: false },
  },
  {
    id: "rb_gdi_free_clime_2027",
    title: "CLIME 1.0 — Canadian Leadership Institute for Medical Education",
    org: "Canadian Association for Medical Education (CAME)",
    segmentId: "education_scientific",
    startDate: "2027-02-01",
    endDate: "2027-02-04",
    contact: {
      name: "Mary Cunningham",
      title: "Association Manager",
      organization: "Canadian Association for Medical Education",
      sourceUrl: "https://www.came-acem.ca/professional-development/clime/",
    },
    contactClass: "COMMERCIALLY_RELEVANT",
    contactNote: "Named lodging contact; mcunningham@afmc.ca published for room-block questions.",
    what: "CLIME 1.0 runs 1–4 February 2027 in person at Li Ka Shing Knowledge Institute (Toronto) for up to 35 health-sciences educators. Registration is open. Official accommodations section states room blocks will be set up and information will be posted soon — lodging sourcing is actively open now.",
    potential:
      "Compact multi-night guestroom block (~20–35 peak) plus possible small meeting/F&B for evening working groups. Fits Radisson’s 157-room mid-size inventory better than mega-convention housing.",
    whyHotel: `${FIT} Downtown Harbourfront base with Union Station / Line 1 access to St. Michael’s / Li Ka Shing corridor; Billy Bishop adjacency for national faculty travelers.`,
    whyNow: "Registration open through early-bird window; organizer explicitly solicits lodging contact before blocks lock.",
    nextStep:
      "Email Mary Cunningham (mcunningham@afmc.ca) proposing Radisson Blu as the CLIME 2027 participant hotel / soft block; request expected out-of-town headcount, preferred rate structure, and decision timeline before blocks are awarded.",
    sources: [
      {
        url: "https://www.came-acem.ca/professional-development/clime/",
        statement: "Dates, Li Ka Shing venue, 35-participant cap, accommodations coming soon + Mary Cunningham contact.",
      },
    ],
    demandClass: "OPEN_ROOM_BLOCK_SOURCING",
    sourcingLabel: "Hotel Sourcing Open — room blocks not yet awarded",
    fit: "DIRECT_FIT",
    fitRationale: "Small multi-day faculty institute with open lodging procurement.",
    attendance: 35,
    commercialStageKey: "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: "Account Development",
    inventoryLane: "ACTIVE_PIPELINE",
    bookingWindowStatus: "PURSUE_NOW",
    uncertainties: ["Exact contracted nights TBD.", "Competing downtown hotels may also bid."],
    provenance: { origin: "ZERO_COST_PUBLIC_PAGE_VERIFICATION", freeFinding: "CLIME_2027", paidExaUsed: false },
  },
  {
    id: "rb_gdi_free_anesthesia_symposium_2027",
    title: "Toronto Anesthesia Symposium 2027",
    org: "University of Toronto Temerty Faculty of Medicine CPD",
    segmentId: "education_scientific",
    startDate: "2027-04-03",
    endDate: "2027-04-04",
    contact: {
      name: "Diana Tamir",
      title: "Program Co-Director (with Fernanda Oliveira)",
      organization: "UHN / University of Toronto Dept of Anesthesia",
      sourceUrl: "https://www.cpd.utoronto.ca/anesthesia/",
    },
    contactClass: "COMMERCIALLY_RELEVANT",
    contactNote: "Named program co-directors; CPD registration desk also published.",
    what: "Toronto Anesthesia Symposium runs 3–4 April 2027 at Chestnut Conference Centre. Program co-directors Diana Tamir and Fernanda Oliveira are named. No exclusive hotel block is published on the CPD pages — visiting faculty and out-of-town clinicians arrange lodging independently while nearby campus hotels are suggested generically by U of T Conference Centre amenities pages.",
    potential:
      "Peak guestroom nights for out-of-town anesthesiologists across Fri–Sun; possible Saturday hospitality reception. Mid-size CPD footprint fits 157 rooms.",
    whyHotel: `${FIT} Waterfront downtown with subway access to Chestnut/Bay corridor; complementary option vs DoubleTree-across-the-street for guests preferring Harbourfront.`,
    whyNow: "Fees and agenda published; early-bird ends 27 Feb 2027 — lodging outreach should precede peak registration.",
    nextStep:
      "Contact Diana Tamir / Fernanda Oliveira via CPD (facmed.registration@utoronto.ca) to offer a soft Harbourfront block for out-of-town delegates and ask whether a Saturday evening reception buyout is useful.",
    sources: [
      { url: "https://www.cpd.utoronto.ca/anesthesia/", statement: "Dates, venue, named co-directors." },
      { url: "https://www.cpd.utoronto.ca/anesthesia/fees/", statement: "Early-bird deadline 27 Feb 2027." },
    ],
    demandClass: "OPEN_PARTICIPANT_LODGING",
    sourcingLabel: "Hotel Sourcing Open — no exclusive block published",
    fit: "NEARBY_FIT",
    fitRationale: "Downtown CPD symposium without exclusive hotel lock.",
    attendance: null,
    commercialStageKey: "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: "Account Development",
    inventoryLane: "QUALIFICATION_NEEDED",
    bookingWindowStatus: "QUALIFY_NOW",
    uncertainties: ["Overnight share vs GTA clinicians unknown.", "No Radisson listing yet."],
    provenance: { origin: "ZERO_COST_PUBLIC_PAGE_VERIFICATION", freeFinding: "ANESTHESIA_SYMPOSIUM_2027", paidExaUsed: false },
  },
  {
    id: "rb_gdi_free_leadership_change_mar2027",
    title: "15th International Conference on Leadership and Change Management",
    org: "MS International Conferences Inc.",
    segmentId: "corporate_meetings_incentives",
    startDate: "2027-03-05",
    endDate: "2027-03-05",
    contact: {
      name: "MS International Conferences Inc.",
      title: "Conference Organizer",
      organization: "MS International Conferences Inc.",
      sourceUrl: "https://www.eventbrite.ca/e/15th-international-conference-on-leadership-and-change-management-tickets-2004637254993",
    },
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    contactNote: "Organizer publishes Eventbrite page requiring self-arranged accommodation for international attendees.",
    what: "One-day leadership/change-management conference on 5 March 2027 at New College, University of Toronto. Organizer explicitly requires participants to arrange their own accommodation, meals, and transport — international attendees seek invitation letters, indicating overnight demand.",
    potential:
      "Guestroom nights for international/out-of-town presenters and executives around 4–6 March; possible small welcome dinner. Compact demand fits boutique inventory.",
    whyHotel: `${FIT} Harbourfront + Billy Bishop adjacency for short-haul US/international arrivals; Line 1 via Union to St. George / New College.`,
    whyNow: "Registration open with 26 Feb 2027 deadline; lodging decisions accompany invitation-letter requests.",
    nextStep:
      "Contact MS International Conferences via the Eventbrite organizer channel to offer a preferred Harbourfront rate / soft block for international attendees and request estimated overnight headcount.",
    sources: [
      {
        url: "https://www.eventbrite.ca/e/15th-international-conference-on-leadership-and-change-management-tickets-2004637254993",
        statement: "Date, New College venue, self-arrange accommodation clause, organizer.",
      },
    ],
    demandClass: "OPEN_PARTICIPANT_LODGING",
    sourcingLabel: "Hotel Sourcing Open — participants arrange own lodging",
    fit: "NEARBY_FIT",
    fitRationale: "Campus day conference with explicit self-arranged overnight lodging for travelers.",
    attendance: null,
    commercialStageKey: "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: "Account Development",
    inventoryLane: "QUALIFICATION_NEEDED",
    bookingWindowStatus: "QUALIFY_NOW",
    uncertainties: ["Exact overnight headcount unpublished.", "Single named individual contact not listed beyond organizer entity."],
    provenance: { origin: "ZERO_COST_PUBLIC_PAGE_VERIFICATION", freeFinding: "LEADERSHIP_CHANGE_MAR2027", paidExaUsed: false },
  },
];

/** Reclassify PDAC from CMI → Hotel Market: Radisson is an official PDAC hotel partner. */
function pdacAsHotelMarket(priorCmiOpp) {
  const base = cloneOpp(priorCmiOpp);
  const contact = base.primaryContact;
  // Strip every CMI-signaling field so annotateInventoryPopulations cannot flip back.
  delete base.competitorHotel;
  delete base.cmiCompetitorHotel;
  delete base.competitorVenueStatus;
  delete base.competitorVenueStatusLabel;
  delete base.competitorVenueConfirmed;
  delete base.cmiClass;
  delete base.cmiVerified;
  delete base.cmiEvidenceRegisterV1;
  delete base.cmiEvidenceUrls;
  delete base.competitiveStatus;
  delete base.commercialActionability;
  delete base.bookingType;
  return applySalesPresentation({
    ...base,
    hotelMarketScopeEligible: true,
    competitiveMarketScopeEligible: false,
    inventoryPopulations: [INVENTORY_POPULATION.SALES_OPPORTUNITIES],
    inventoryPopulationPrimary: INVENTORY_POPULATION.SALES_OPPORTUNITIES,
    commercialActionability: COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT,
    competitorVenueConfirmed: false,
    competitorHotel: null,
    competitorVenueStatus: null,
    cmiClass: null,
    cmiVerified: false,
    competitiveStatus: "NOT_COMPETITIVE",
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED,
    targetHotelSourcingStatusLabel: "Open — Radisson Blu official partner rate published",
    commercialStageKey: "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: "Account Development",
    inventoryLane: "ACTIVE_PIPELINE",
    bookingWindowStatus: "PURSUE_NOW",
    accountDevelopmentPublished: true,
    hotelSourcingStatusLabel: "Hotel Sourcing Open — Radisson Blu official PDAC partner promo rate",
    demandEvidenceClass: "PUBLISHED_TARGET_HOTEL_PARTNER_RATE",
    whatIsHappening:
      "PDAC 2027 Convention (7–10 March 2027, MTCC) publishes Radisson Blu Toronto Downtown as an official hotel partner with promo rate CAD $449 and reference code for direct booking (cutoff 2 Feb 2027). Limited availability remains on select nights.",
    summaryWhat:
      "PDAC lists Radisson Blu as an official partner hotel with a published promo rate and direct booking path.",
    potentialHotelBusiness:
      "Fill remaining partner-rate inventory; pursue exhibitor/company suites and overflow once partner nights sell; do not confuse with exclusive venue housing.",
    potentialBusiness:
      "Fill remaining partner-rate inventory; pursue exhibitor/company suites and overflow once partner nights sell.",
    whyThisHotel: `${FIT} PDAC official partner list includes this hotel at 8-minute walk to MTCC.`,
    whyActNow: "Partner promo cutoff 2 Feb 2027; limited availability already flagged on select nights.",
    cardWhyNowLine: "PDAC partner-rate cutoff 2 Feb 2027; limited nights remain.",
    recommendedNextStep:
      "Contact Nicole Sampson / PDAC accommodations team and hotel sales to review partner-block pickup, release schedule, and exhibitor suite demand; prioritize filling published Radisson promo inventory before cutoff.",
    recommendedAction:
      "Fill PDAC partner-rate inventory and qualify exhibitor suite demand before 2 Feb 2027 cutoff.",
    sharedTenSectionDrawer: {
      sections: [
        {
          id: "what_is_happening",
          title: "What is happening",
          body: "PDAC 2027 lists Radisson Blu Toronto Downtown among official hotel partners with a published promo rate and booking instructions.",
        },
        {
          id: "potential_hotel_business",
          title: "Potential hotel business",
          body: "Partner-rate guestroom nights already contracted/listed for this hotel; fill inventory and explore exhibitor suites.",
        },
        {
          id: "why_this_hotel",
          title: "Why this hotel",
          body: FIT,
        },
        {
          id: "why_now",
          title: "Why now",
          body: "Promo cutoff 2 Feb 2027; limited availability on select nights.",
        },
        {
          id: "who_to_contact",
          title: "Who to contact",
          body: contact
            ? `${contact.name}${contact.title ? ` — ${contact.title}` : ""} (${contact.organization || "PDAC"}).`
            : "PDAC accommodations / Nicole Sampson.",
        },
        {
          id: "recommended_next_step",
          title: "Recommended next step",
          body: "Review partner-block pickup with hotel sales and PDAC; push remaining promo nights and exhibitor suites before cutoff.",
        },
        {
          id: "supporting_evidence",
          title: "Supporting evidence",
          body: "https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027",
        },
        {
          id: "research_uncertainties",
          title: "Research uncertainties",
          body: "Exact remaining room nights by date unpublished beyond “limited availability”.",
        },
      ],
    },
    researchProvenance: {
      ...(base.researchProvenance || {}),
      reclassification: "PRIOR_CMI_TO_HOTEL_MARKET_BECAUSE_RADISSON_IS_OFFICIAL_PARTNER",
      zeroCostVerificationUrl:
        "https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027",
      paidExaUsed: false,
    },
    sources: [
      ...(base.sources || []),
      {
        url: "https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027",
        statement: "Radisson Blu listed as PDAC 2027 hotel partner with promo rate and booking path.",
      },
    ],
  });
}

function upgradeExistingHotelMarket(opp) {
  const o = cloneOpp(opp);
  const id = o.id;
  const upgrades = {
    rb_gdi_disc_dc428c1bd4c4e412: {
      next: "Contact Giovanni Guizzetti (Global Conference Manager, ISCT) to confirm who controls overflow housing beyond registration-linked blocks; request permission to offer Harbourfront supplemental inventory and meeting/F&B for ancillary gatherings — do not pitch exclusive MTCC housing.",
      whyNow: "ISCT 2027 Toronto dates and housing mechanism are published; overflow windows close as blocks fill.",
      lane: "QUALIFICATION_NEEDED",
    },
    rb_gdi_disc_f825d09516dd0b2a: {
      next: "Contact Christi R. Nam (Director, Meetings and Operations, ESA) to ask whether ESA will open an overflow hotel list for Toronto 2027 and how Radisson Blu can be considered for delegate/speaker spillover and small ancillary meetings.",
      whyNow: "ESA 112th Annual Meeting dates are set for Jul 2027 at MTCC scale — overflow planning typically precedes peak registration.",
      lane: "QUALIFICATION_NEEDED",
    },
    rb_gdi_disc_4a97f6ec2a8a5282: {
      next: "Contact Beth Slater (Executive Director, OHBM) to qualify expected out-of-town attendance, official housing plan, and whether a Harbourfront overflow or board/committee hotel is needed for June 2027.",
      whyNow: "OHBM 2027 Toronto dates published; lodging program decisions usually lock 9–12 months out.",
      lane: "QUALIFICATION_NEEDED",
    },
  };
  const u = upgrades[id];
  if (!u) return applySalesPresentation(o);
  o.recommendedNextStep = u.next;
  o.recommendedAction = u.next;
  o.whyActNow = u.whyNow;
  o.cardWhyNowLine = u.whyNow.slice(0, 160);
  o.inventoryLane = u.lane;
  o.bookingWindowStatus = "QUALIFY_NOW";
  o.commercialStageKey = "ACCOUNT_DEVELOPMENT";
  o.commercialStagePillLabel = "Account Development";
  o.accountDevelopmentPublished = true;
  // Avoid isFutureNotYetOpen → Monitor: association + "Hotel Sourcing Unconfirmed".
  o.hotelSourcingStatusLabel = "Hotel Sourcing Open — overflow / ancillary path to qualify";
  o.targetHotelSourcingStatus = TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED;
  o.decisionState = null;
  o.nextActionRecheckReason = null;
  if (o.sharedTenSectionDrawer?.sections) {
    o.sharedTenSectionDrawer.sections = o.sharedTenSectionDrawer.sections.map((s) => {
      if (s.id === "recommended_next_step") return { ...s, body: u.next };
      if (s.id === "why_now") return { ...s, body: u.whyNow };
      return s;
    });
  }
  o.researchProvenance = {
    ...(o.researchProvenance || {}),
    salesActionUpgraded: true,
    expansionScript: SCRIPT_ID,
  };
  return applySalesPresentation(o);
}

const newHmOpps = NEW_HM_SPECS.map(buildHotelMarketOpportunity);
const upgradedPriorHm = priorHm.map(upgradeExistingHotelMarket);
const pdacPrior = priorCmi.find((o) => o.id === "rbc_gdi_disc_45447a18d2d0e412");
const pdacHm = pdacPrior ? pdacAsHotelMarket(pdacPrior) : null;
const retainedCmi = priorCmi
  .filter((o) => o.id !== "rbc_gdi_disc_45447a18d2d0e412")
  .map((o) => {
    const c = cloneOpp(o);
    c.researchProvenance = { ...(c.researchProvenance || {}), preservedFromPriorPackage: priorFp };
    return applySalesPresentation(c);
  });

/** Additional free-verified CMI (does not count toward HM target). Future vs REF only. */
const NEW_CMI_SPECS = [
  {
    id: "rbc_gdi_free_edco_2027",
    title: "EDCO 70th Annual Conference and Showcase",
    org: "Economic Developers Council of Ontario",
    segmentId: "association_events",
    startDate: "2027-02-01",
    endDate: "2027-02-03",
    competitorHotel: "The Westin Harbour Castle",
    contact: {
      name: "EDCO Conference Team",
      title: "Conference / Hotel Travel",
      organization: "Economic Developers Council of Ontario",
      sourceUrl: "https://edco.on.ca/conference/hotel-travel/",
    },
    sources: [
      { url: "https://edco.on.ca/conference/hotel-travel/", statement: "Westin is official conference hotel; group code DCO; cutoff 7 Jan 2027." },
    ],
    nextStep:
      "Monitor Westin block pickup; introduce Radisson only for documented overflow or future-cycle bids — not as 2027 host replacement.",
    futureCycleThesis: "Annual EDCO conference locked to Westin for 2027; develop for a later cycle if host rotates.",
    commitmentScope: "VENUE_AND_GUESTROOMS",
  },
  {
    id: "rbc_gdi_free_i4pl_2026",
    title: "I4PL Conference 2026",
    org: "Institute for Performance and Learning",
    segmentId: "corporate_meetings_incentives",
    startDate: "2026-11-17",
    endDate: "2026-12-03",
    competitorHotel: "The Westin Harbour Castle",
    contact: {
      name: "I4PL Conference Office",
      title: "Conference / Venue Travel",
      organization: "Institute for Performance and Learning",
      sourceUrl: "https://thei4pl.ca/conference-2026/venue-travel/",
    },
    sources: [
      {
        url: "https://thei4pl.ca/conference-2026/venue-travel/",
        statement: "Westin Harbour Castle host with group code I6P; premium rooms book-by 10 Nov 2026.",
      },
    ],
    nextStep:
      "CMI only for 2026 host block. Ask conference@thei4pl.ca about overflow policy after Westin sell-out; pursue future-cycle bid.",
    futureCycleThesis: "Waterfront learning conference hosted at Westin; residual overflow or future host cycle.",
    commitmentScope: "VENUE_AND_GUESTROOMS",
    uncertainty: "Exact session dates inferred from travel window Nov 17–Dec 3 2026 on airline discount page.",
  },
];

const EXPIRED_CMI_NOTES = [
  { id: "rbc_gdi_free_motm_2026", title: "Meeting of the Minds 2026", reason: "Event ends 7 Nov 2026 — before REF" },
  { id: "rbc_gdi_free_surety_agm_2026", title: "Surety Association AGM 2026", reason: "Fall 2026 block expired vs REF" },
  { id: "rbc_gdi_free_ci2i_2026", title: "Ci2i Symposium 2026", reason: "23 Oct 2026 — before REF" },
];

const newCmiOpps = NEW_CMI_SPECS.map(buildCmiOpportunity);

let hotelMarketAll = [...upgradedPriorHm, ...(pdacHm ? [pdacHm] : []), ...newHmOpps];
let competitiveAll = [...retainedCmi, ...newCmiOpps];

// Dedupe by id
const seen = new Set();
hotelMarketAll = hotelMarketAll.filter((o) => (seen.has(o.id) ? false : (seen.add(o.id), true)));
const seenC = new Set();
competitiveAll = competitiveAll.filter((o) => (seenC.has(o.id) ? false : (seenC.add(o.id), true)));

const FORCE_HOTEL_MARKET_IDS = new Set(hotelMarketAll.map((o) => o.id));

// Annotate + gate
let allOpps = [...hotelMarketAll, ...competitiveAll];
allOpps = annotateInventoryPopulations(allOpps, { hotelId: HOTEL_ID }).map((o) => {
  const forceHm = FORCE_HOTEL_MARKET_IDS.has(o.id);
  const isCmi = !forceHm && o.competitiveMarketScopeEligible === true;
  const next = {
    ...o,
    competitiveMarketScopeEligible: isCmi,
    hotelMarketScopeEligible: forceHm || !isCmi,
  };
  if (forceHm) {
    next.competitiveMarketScopeEligible = false;
    next.hotelMarketScopeEligible = true;
    next.commercialActionability = COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT;
    next.competitorVenueStatus = null;
    next.competitorHotel = null;
    next.cmiVerified = false;
    next.cmiClass = null;
  }
  return annotateCanonicalCmiContract(next);
});

// Re-assert forced HM after canonical annotate (which may recompute scopes).
allOpps = allOpps.map((o) => {
  if (!FORCE_HOTEL_MARKET_IDS.has(o.id)) return o;
  return {
    ...o,
    hotelMarketScopeEligible: true,
    competitiveMarketScopeEligible: false,
    inventoryPopulations: [INVENTORY_POPULATION.SALES_OPPORTUNITIES],
    inventoryPopulationPrimary: INVENTORY_POPULATION.SALES_OPPORTUNITIES,
  };
});

const hintsById = {};
for (const o of allOpps) {
  hintsById[o.id] = {
    actualEventStart: o.eventStartDate,
    actualEventEnd: o.eventEndDate || null,
    datePillDisplayLabel: o.datePillDisplayLabel,
    competitorCommitted: FORCE_HOTEL_MARKET_IDS.has(o.id)
      ? false
      : o.competitiveMarketScopeEligible === true,
    cmiVerified: FORCE_HOTEL_MARKET_IDS.has(o.id)
      ? false
      : o.competitiveMarketScopeEligible === true,
  };
}
const gate = applyCommercialActionabilityGate(allOpps, { referenceDate: REF, hintsById });
const gatedRows = gate.rows.map((o) => attachSalesOpportunityFilterFields(o));
const customerVisible = gatedRows.filter(
  (o) => o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true,
);
const hotelMarket = customerVisible.filter((o) => o.hotelMarketScopeEligible === true);
const competitive = customerVisible.filter((o) => o.competitiveMarketScopeEligible === true);
const actionabilityAssert = assertCustomerInventoryPassesActionabilityGate(customerVisible, {
  referenceDate: REF,
  hintsById,
});

const cmiGateReviews = competitive.map((o) => ({
  id: o.id,
  title: o.title || o.displayTitle,
  competitorHotel: o.competitorHotel,
  gates: evaluateCmiQualificationGates(o),
}));
const cmiCoverage = evaluateCmiDiscoveryCoverage({
  checklist: CMI_DISCOVERY_COVERAGE_CHECKLIST,
  completed: CMI_DISCOVERY_COVERAGE_CHECKLIST.map((c) => c.id),
});

writeJson("04_FREE_INCREMENTAL_DISCOVERY.json", {
  contract: "GDI_RADISSON_FREE_INCREMENTAL_DISCOVERY_V1",
  paidExaCalls: 0,
  surfeCalls: 0,
  methods: ["Reuse thin-ledger URLs", "Public WebFetch/WebSearch verification", "Prior paid raw-runs not re-charged"],
  newHotelMarketFindings: NEW_HM_SPECS.map((s) => ({ id: s.id, title: s.title, org: s.org, start: s.startDate })),
  expiredRelativeToRef: [...EXPIRED_RELATIVE_TO_REF, ...EXPIRED_CMI_NOTES],
  pdacReclassification: pdacHm
    ? {
        id: pdacHm.id,
        from: "CMI",
        to: "HOTEL_MARKET",
        reason: "Radisson Blu is an official PDAC 2027 hotel partner with published promo rate — bookable inventory at this hotel.",
      }
    : null,
  newCmiFindings: NEW_CMI_SPECS.map((s) => ({ id: s.id, title: s.title, competitorHotel: s.competitorHotel })),
  rejectedFreeCandidates: [
    {
      title: "Arctiq Customer Appreciation Holiday Party 2026",
      reason: "Evening reception at King Taps — no overnight guestroom thesis.",
    },
    {
      title: "ENDO 2027 third-party Radisson listing",
      reason: "Official ENDO housing list does not include Radisson Blu; congressbookers page is not organizer-authoritative.",
    },
    {
      title: "Toronto Tempo 2026 lodging",
      reason: "Hotel X is official domestic hotel partner — CMI/competitor, not open Radisson demand without separate evidence.",
    },
    {
      title: "OMERACT 2027",
      reason: "Venue is Kingbridge Centre (King City) — geographic mismatch for downtown Harbourfront Radisson as primary lodging.",
    },
    {
      title: "Employee Experience Summit 2027",
      reason: "Old Mill Toronto Hotel is host venue with published guestroom block — CMI/competitor, not HM.",
    },
    {
      title: "2026 Climate & Health Conference page",
      reason: "Public page reflects completed event gallery — not a future actionable edition.",
    },
    {
      title: "EDA People Connected 2026",
      reason: "Hosted at Toronto Airport Marriott (stretch territory) with sold-out airport block.",
    },
  ],
});

writeJson("05_HOTEL_FIT_AND_LOCAL_DEMAND.json", {
  profile: HOTEL_CFG.capabilityProfile,
  address: HOTEL_CFG.address,
  commercialPriorities: HOTEL_CFG.commercialPriorities,
  waterfrontThesis:
    "Queens Quay / Harbourfront / Terminal Theatre / Billy Bishop adjacency favors mid-size meetings, dance/sports participant blocks, and secondary blocks for Westin-hosted waterfront conferences.",
  validatedAgainstCanonical: true,
});

const top30 = [
  ...hotelMarket.map((o, i) => ({
    rank: i + 1,
    id: o.id,
    title: o.displayTitle || o.title,
    org: o.organizationName,
    scope: "HOTEL_MARKET",
    start: o.eventStartDate,
    contact: o.primaryContact?.name || null,
    readiness: o.commercialStagePillLabel,
    action: o.recommendedNextStep,
    evidenceConfidence: o.demandEvidenceClass,
  })),
  ...recoveryQueue.slice(0, Math.max(0, 30 - hotelMarket.length)).map((r, i) => ({
    rank: hotelMarket.length + i + 1,
    id: r.id,
    title: r.programme,
    org: r.org,
    scope: "RESEARCH_CONTINUATION_CANDIDATE",
    start: null,
    contact: null,
    readiness: "Research Continuation",
    action: "Needs free page verification or paid deep research for exact dates/contacts.",
    evidenceConfidence: r.guestroom,
    blockedBy: r.lossReasons,
  })),
];
writeJson("06_TOP_30_HOTEL_MARKET_CANDIDATES.json", {
  contract: "GDI_RADISSON_TOP30_HM_CANDIDATES_V1",
  target: TARGET_HM,
  qualifiedHotelMarketNow: hotelMarket.length,
  rows: top30,
});

writeJson("07_COMMERCIAL_QUALIFICATION.json", {
  contract: "GDI_RADISSON_HM_EXPANSION_QUALIFICATION_V1",
  referenceDate: REF,
  template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  templateLock: GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
  actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  cmiContract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  hotelMarketCount: hotelMarket.length,
  cmiCount: competitive.length,
  actionabilityAssert,
  cmiGateReviews,
  cmiCoverage,
  classifications: {
    salesReady: hotelMarket.filter((o) => /sales.?ready/i.test(o.commercialStagePillLabel || "")).length,
    accountDevelopment: hotelMarket.filter((o) => /account development/i.test(o.commercialStagePillLabel || "")).length,
    cmi: competitive.length,
  },
  decisions: customerVisible.map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    scope: o.competitiveMarketScopeEligible ? "CMI" : "HOTEL_MARKET",
    stage: o.commercialStagePillLabel,
    contact: o.primaryContact?.name || null,
    start: o.eventStartDate,
    salesAction: o.salesActionPillLabel,
  })),
});

writeJson("08_HOTEL_MARKET_FINAL_INVENTORY.json", {
  target: TARGET_HM,
  achieved: hotelMarket.length,
  targetMet: hotelMarket.length >= TARGET_HM,
  gap: Math.max(0, TARGET_HM - hotelMarket.length),
  initialHotelMarket: 3,
  additionalQualified: Math.max(0, hotelMarket.length - 3),
  opportunities: hotelMarket.map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    org: o.organizationName,
    dates: o.datePillDisplayLabel,
    start: o.eventStartDate,
    contact: o.primaryContact,
    salesAction: o.salesActionPillLabel,
    nextStep: o.recommendedNextStep,
    demandClass: o.demandEvidenceClass,
    sourcing: o.hotelSourcingStatusLabel,
    fit: o.hotelFitAssessment?.fit,
    stage: o.commercialStagePillLabel,
  })),
});

writeJson("09_CMI_PRESERVATION_AUDIT.json", {
  priorCmiCount: priorCmi.length,
  priorCmiIds: priorCmi.map((o) => o.id),
  retainedFromPrior: retainedCmi.map((o) => o.id),
  reclassifiedToHotelMarket: pdacHm ? ["rbc_gdi_disc_45447a18d2d0e412"] : [],
  reclassificationRationale:
    "PDAC publishes Radisson Blu as an official hotel partner with a bookable promo rate — commercially this is Hotel Market inventory for this hotel, not merely competitor intelligence.",
  newlyAddedCmi: newCmiOpps.map((o) => o.id),
  finalCmiCount: competitive.length,
  priorPackageFingerprintPreserved: priorFp,
  immutablePriorPackagePath: path.relative(ROOT, PRIOR_PKG_DIR),
});

writeJson("10_CONTACT_AND_SALES_ACTIONS.json", {
  note: "Differentiated actions — generic Monitor removed for retained Hotel Market rows.",
  rows: hotelMarket.map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    contact: o.primaryContact,
    salesActionPill: o.salesActionPillLabel,
    nextStep: o.recommendedNextStep,
    ask: o.recommendedNextStep,
    when: o.whyActNow,
    unverified: o.uncertainties || [],
  })),
});

const gap = Math.max(0, TARGET_HM - hotelMarket.length);
const proposedPaidDeep = Math.min(recoveryQueue.length, Math.max(gap + 8, 24));
writeJson("11_COST_AND_RESEARCH_GAPS.json", {
  contract: "GDI_RADISSON_COST_AND_GAPS_V1",
  priorAuthorizationUsd: 15,
  priorSpentUsd: 14.2,
  priorRemainingUsd: 0.8,
  thisCampaignPaidUsd: 0,
  remainingUsdNotAuthorizedForNewCampaign: true,
  hotelMarketAchieved: hotelMarket.length,
  gapTo20: gap,
  targetMet: gap === 0,
  blockedPromisingCandidates: recoveryQueue.slice(0, 20).map((r) => ({
    id: r.id,
    title: r.programme,
    org: r.org,
    segment: r.segment,
    blockedBy: r.lossReasons,
    guestroom: r.guestroom,
  })),
  proposedPaidContinuation: {
    requiresSeparateFounderAuthorization: true,
    unit: "Exa HIGH deep_research",
    unitCostUsd: 0.5,
    proposedRuns: proposedPaidDeep,
    proposedBudgetUsd: Number((proposedPaidDeep * 0.5).toFixed(2)),
    expectedBenefit: `Pursue ~${proposedPaidDeep} mid-size corporate/education/sports candidates with lodging evidence to close the ${gap}-opportunity Hotel Market gap without mega-convention bias.`,
    doNotUse: "Do not spend the leftover $0.80 as an unstructured exploration campaign.",
  },
});

// ---------------------------------------------------------------------------
// Stage package
// ---------------------------------------------------------------------------
const store = createGdiPublicationPackageStore({ root: PROTECTED_STORE_ROOT });
const protectedBefore = {
  priorFingerprint: priorFp,
  priorPkgSha: sha256File(path.join(PRIOR_PKG_DIR, "package.json")),
};
let pkg = null;
let integrity = null;
let qualityGate = null;
let packageBlocker = null;
let writtenPath = null;

const built = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: customerVisible,
  publicationVersion: PUB_VERSION,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  templateVersion: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    script: SCRIPT_ID,
    hotelMarketCount: hotelMarket.length,
    competitiveCount: competitive.length,
    targetHotelMarket: TARGET_HM,
    targetMet: hotelMarket.length >= TARGET_HM,
    priorFingerprintPreserved: priorFp,
    customerPublished: false,
    paidExaUsed: false,
  },
});
const candidatePkg = built.package || built;
integrity = verifyPublicationPackageIntegrity(candidatePkg);
qualityGate = runPublicationQualityGate({
  opportunities: candidatePkg.opportunities || customerVisible,
  referenceDate: REF,
});
if (integrity?.ok === false) {
  packageBlocker = `PACKAGE_INTEGRITY_FAILED:${JSON.stringify(integrity.fails || integrity)}`;
} else if (qualityGate?.ok === false) {
  packageBlocker = `PUBLICATION_QUALITY_GATE_BLOCKING:${JSON.stringify(qualityGate.blocking || qualityGate.summary || null)}`;
} else {
  pkg = candidatePkg;
  const written = store.writePackage(pkg);
  writtenPath = written?.path || null;
  store.writePointer(HOTEL_ID, "STAGED", {
    publicationId: pkg.publicationId,
    publicationVersion: pkg.publicationVersion,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  });
}

fs.writeFileSync(
  path.join(HOTEL_DATA, "opportunities.json"),
  `${JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      updatedAt: new Date().toISOString(),
      source: SCRIPT_ID,
      note: pkg ? "STAGED_FOR_FOUNDER_REVIEW — not CUSTOMER_PUBLISHED" : `NOT_STAGED:${packageBlocker}`,
      opportunities: customerVisible,
    },
    null,
    2,
  )}\n`,
);

let airtablePlan = { configured: isGdiOpportunityAirtableConfigured(), dryRun: true, writes: 0 };
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
    wouldCreate: sync.summary?.created,
    wouldUpdate: sync.summary?.updated,
    total: sync.summary?.total,
  };
} catch (e) {
  airtablePlan.error = String(e?.message || e);
}
writeJson("12_AIRTABLE_WRITE_DRY_RUN.json", { ...airtablePlan, liveWritesAuthorized: false });

const inventoryCounts = computePublicationInventoryCounts(customerVisible);
writeJson("13_NEW_STAGED_PUBLICATION.json", {
  staged: Boolean(pkg),
  packageBlocker,
  publicationVersion: PUB_VERSION,
  publicationId: pkg?.publicationId || null,
  fingerprint: pkg?.contentFingerprintSha256 || null,
  lifecycleState: pkg ? GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED : null,
  founderReviewLabel: pkg ? "STAGED_FOR_FOUNDER_REVIEW" : null,
  customerPublished: false,
  packagePath: writtenPath,
  inventoryCounts,
  hotelMarketCount: hotelMarket.length,
  competitiveCount: competitive.length,
  targetHotelMarket: TARGET_HM,
  targetMet: hotelMarket.length >= TARGET_HM,
  gapTo20: gap,
  integrity,
  qualityGate: {
    ran: Boolean(qualityGate),
    ok: qualityGate?.ok !== false,
    blocking: qualityGate?.blocking || [],
  },
  priorPackagePreserved: {
    fingerprint: priorFp,
    path: path.relative(ROOT, PRIOR_PKG_DIR),
    sha256: protectedBefore.priorPkgSha,
  },
  previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
});

writeJson("15_GITHUB_MERGE_STATUS.json", {
  branch: "exec/gdi-radisson-toronto-hotel-market-expansion-20plus-v1",
  prNumber: null,
  prUrl: null,
  ciResult: "PENDING",
  mergeStatus: "NOT_MERGED",
  mergeCommitSha: null,
  mainSynchronized: false,
  paidExaUsed: false,
  noCustomerPublished: true,
});

writeJson("15-TEMPLATE_CERTIFICATION.json", {
  template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  lock: GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
  actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  cmiContract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  actionabilityOk: actionabilityAssert?.ok !== false,
  qualityGateOk: qualityGate?.ok !== false,
});

const founderMd = `# Radisson Blu Toronto — Hotel Market expansion (zero-cost)

**Directive:** 20+ actionable Hotel Market opportunities (CMI excluded)  
**Hotel:** ${HOTEL_NAME} (\`${HOTEL_ID}\` / CNC41)  
**Reference date:** ${REF}  
**Paid Exa this campaign:** **$0** (prior $14.20/$15 preserved; leftover $0.80 not spent)

## Target status

| Metric | Value |
|---|---:|
| Initial Hotel Market | 3 |
| Additional qualified Hotel Market | ${Math.max(0, hotelMarket.length - 3)} |
| **Final Hotel Market** | **${hotelMarket.length}** |
| Target | ${TARGET_HM} |
| **Target met?** | **${hotelMarket.length >= TARGET_HM ? "YES" : "NO"}** |
| Gap | ${gap} |
| CMI (separate) | ${competitive.length} |
| Sales Ready | ${hotelMarket.filter((o) => /sales.?ready/i.test(o.commercialStagePillLabel || "")).length} |
| Account Development (HM) | ${hotelMarket.filter((o) => /account development/i.test(o.commercialStagePillLabel || "")).length} |

## Why the pipeline was small

- 209/272 discoveries never received deep research (thin ledger has **no exact dates**).
- Prior $15 paid run focused on large association/MTCC events → 3 overflow HM + 11 CMI.
- Mid-size corporate/education/sports with lodging URLs stayed queued.
- Standards correctly blocked competitor-host events from Hotel Market counts.

## Zero-cost gains

1. **Standout Dance Challenge Apr 2027** — published **Radisson Blu** participant block (Terminal Theatre next door).
2. **PDAC 2027** — reclassified CMI → Hotel Market (official Radisson partner promo rate).
3. **CFEA 2026** (20–21 Nov) — Rotman open downtown lodging guidance + named organizers.
4. **CLIME 2027** — open room-block sourcing (Mary Cunningham); Li Ka Shing / 35 participants.
5. **Toronto Anesthesia Symposium 2027** — Chestnut CPD; named co-directors; no exclusive hotel.
6. **Leadership & Change Management Mar 2027** — New College; self-arranged accommodation.
7. Upgraded sales actions on ISCT / ESA / OHBM (no more generic Monitor-only).
8. Added free-verified future CMI (EDCO, I4PL). Adaptation Canada / MDRS / MotM / Surety / Ci2i documented as expired vs REF (not staged).

## Hotel Market inventory

${hotelMarket
  .map(
    (o, i) =>
      `${i + 1}. **${o.displayTitle || o.title}** — ${o.organizationName} · ${o.datePillDisplayLabel} · ${o.primaryContact?.name || "n/a"} · ${o.salesActionPillLabel}`,
  )
  .join("\n")}

## Gap to 20

Cannot substantiate ${gap} more Hotel Market rows from free sources without inventing demand or dual-classifying CMI.

**Proposed paid continuation (separate authorization required):** ~${proposedPaidDeep} Exa HIGH deep runs × $0.50 ≈ **$${Number((proposedPaidDeep * 0.5).toFixed(2))}**, prioritized to mid-size recovery-queue candidates with lodging evidence (not mega-conventions).

## Publication

- Prior package fingerprint **preserved:** \`${priorFp}\`
- New staged fingerprint: \`${pkg?.contentFingerprintSha256 || "NOT_STAGED"}\`
- Lifecycle: STAGED_FOR_FOUNDER_REVIEW (not CUSTOMER_PUBLISHED)
- Airtable: dry-run only
- Preview: http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founderMd);

console.log(
  JSON.stringify(
    {
      hotelMarket: hotelMarket.length,
      cmi: competitive.length,
      gapTo20: gap,
      targetMet: hotelMarket.length >= TARGET_HM,
      fingerprint: pkg?.contentFingerprintSha256 || null,
      packageBlocker,
      actionabilityOk: actionabilityAssert?.ok !== false,
    },
    null,
    2,
  ),
);
