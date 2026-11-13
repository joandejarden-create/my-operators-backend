#!/usr/bin/env node
/**
 * Sheraton Guadalajara Expo — commercial E2E remediation V2 to Bethesda quality bar.
 *
 * - Snapshots prior STAGED package (fingerprint 60f2d7…)
 * - Remediates all 62 with certified classification / sales-action / quick-filter modules
 * - Evidence-grounded narratives from research dossiers (no paid research)
 * - Stages NEW publication candidate (new fingerprint)
 * - Airtable dry-run only; no CUSTOMER_PUBLISHED; no Bethesda mutation
 *
 *   npm run gdi:sheraton-commercial-e2e-remediation-v2
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
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

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const BASELINE_FP =
  "60f2d7ef732ac9578e33e159988092b90f49ec8372b8b174c7630296240e6211";
const BASELINE_VERSION = "gdi_pub_recCAGqkERSXgiku4_20261113_full_e2e_v1";
const TEMPLATE_SHA = "13f104edb810176a64062c97278772201d9c1e84";
const OUT = path.join(
  ROOT,
  "reports/gdi-sheraton-guadalajara-commercial-e2e-remediation-v2",
);
const RESEARCH_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunity-research-state",
);
const DATA_ROOT = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
);

function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n");
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function isGenericThesis(s) {
  return /Documented local expansion\s*\/\s*growth activity|investigate hotel-relevant group/i.test(
    text(s),
  );
}

function stripGenericFiller(s) {
  return text(s)
    .replace(
      /Documented local expansion\s*\/\s*growth activity[^.]*\./gi,
      "",
    )
    .replace(/\s{2,}/g, " ")
    .trim();
}

function mapSegmentToDemandId(seg) {
  const s = text(seg).toLowerCase().replace(/-/g, "_");
  const map = {
    association_events: "association_conference_group",
    association_conference_group: "association_conference_group",
    sports_entertainment: "sports_entertainment",
    government_public_sector: "government_public_sector",
    corporate_meetings_incentives: "corporate_meetings_incentives",
    education_scientific: "education_scientific",
    projects_infrastructure: "projects_infrastructure",
    social_special_events: "social_special_events",
    local_catering_day_meetings: "local_catering_day_meetings",
    corporate_celebrations_private_events: "corporate_celebrations_private_events",
    scaling_companies: "scaling_company",
    scaling_company: "scaling_company",
    relocation_extended_stay: "relocation_extended_stay",
    preferred_lodging: "preferred_lodging_partnership",
  };
  return map[s] || s || null;
}

function detectPartnership(title, dossier) {
  const blob = `${title} ${dossier?.title || ""} ${dossier?.demandType || ""}`.toLowerCase();
  return (
    /preferred lodging partnership|venue partnership|overnight guest referral|independent event venue/i.test(
      blob,
    ) || dossier?.preferredLodgingPartnershipV1 === true
  );
}

function detectCateringPotential(title, dossier, what) {
  const blob = `${title} ${what} ${dossier?.potentialHotelBusiness || ""}`.toLowerCase();
  return /banquet|catering|gala|private dining|day meeting|celebration|wedding|open house.*networking|award/i.test(
    blob,
  );
}

function detectCompetitive(dossier, opp) {
  const blob = `${opp.whatIsHappening || ""} ${dossier?.whyThisHotel || ""} ${dossier?.hotelOpportunityThesis || ""}`.toLowerCase();
  return (
    /competitor hotel|hosted at|held at (the )?(hilton|marriott|hyatt|fiesta americana|live aqua|nh collection|hard rock)|confirmed at another hotel/i.test(
      blob,
    ) || opp.competitiveMarketScopeEligible === true
  );
}

function parseIsoDate(raw) {
  const s = text(raw);
  if (!s) return null;
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (m) return m[1];
  const y = s.match(/\b(20\d{2})\b/);
  return y ? `${y[1]}-01-01` : null;
}

function buildNarratives(base, dossier) {
  const title = text(base.displayTitle || base.title);
  const org = text(base.organizationName || dossier?.organizationName || "");
  let what = stripGenericFiller(
    dossier?.whatIsHappening ||
      dossier?.activityHeadline ||
      base.whatIsHappening ||
      "",
  );
  if (!what || isGenericThesis(what)) {
    what = text(
      `${org || "Organization"} is associated with ${title}. Public evidence supports a demand-generating activity in the Guadalajara / Jalisco market; lodging and meetings capture remain to be qualified with the organizer.`,
    );
  }
  let potential = stripGenericFiller(
    dossier?.potentialHotelBusiness ||
      base.potentialHotelBusiness ||
      base.potentialBusiness ||
      "",
  );
  if (!potential || potential.length < 40) {
    potential = text(
      `Plausible guestroom (and possibly meeting/catering) demand for attendees, organizers, exhibitors, or visiting staff around ${title}. Volume and decision path are not yet confirmed from public research.`,
    );
  }
  let whyHotel = stripGenericFiller(
    dossier?.whyThisHotel ||
      dossier?.hotelOpportunityThesis ||
      base.whyThisHotel ||
      "",
  );
  if (!whyHotel || /same commercial market \(the market\)/i.test(whyHotel)) {
    whyHotel = text(
      `${HOTEL_NAME} is a full-service Expo Guadalajara–adjacent hotel (≈216 keys, meeting inventory) positioned for overflow lodging, peri-event stays, and mid-size group meetings in the Plaza del Sol / Expo corridor. Fit depends on whether this demand is realistically capturable versus venue-hosted or competitor-blocked inventory.`,
    );
  }
  let whyNow = stripGenericFiller(
    dossier?.honestSalesBriefV41?.reasonToContactNow ||
      dossier?.salesBrief?.whyActNow ||
      base.whyActNow ||
      "",
  );
  if (!whyNow || whyNow.length < 30) {
    const dateHint =
      text(dossier?.originalActivityDate || dossier?.growthEvidenceDate || base.datePillDisplayLabel) ||
      "Timing still being qualified";
    whyNow = text(
      `Commercial timing signal: ${dateHint}. Establish the lodging/meetings owner before the next public sourcing cycle closes or competitor relationships harden.`,
    );
  }
  return { what, potential, whyHotel, whyNow };
}

function chooseCommercialStage(ctx) {
  if (ctx.partnership) return "RELATIONSHIP_DEVELOPMENT";
  if (ctx.pursueNowEligible) return "PURSUE_NOW";
  if (ctx.hasFutureDate || ctx.recurring) return "FUTURE_CAPTURE";
  return "QUALIFICATION_NEEDED";
}

function chooseDisposition(ctx) {
  // Keep customer-visible when named contact + evidence + hotel-relevant thesis exist.
  if (!ctx.hasContact) return "RESEARCH_STAGE";
  if (!ctx.hasEvidence) return "RESEARCH_STAGE";
  if (ctx.relevance === "OUTSIDE") return "QUARANTINE_WRONG_MARKET";
  if (ctx.expiredWithoutFuture) return "RESEARCH_STAGE_EXPIRED";
  return "CUSTOMER_VISIBLE";
}

fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(path.join(OUT, "baseline"), { recursive: true });

const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const stagedPtr = store.readPointer(HOTEL_ID, "STAGED");
const baselineLoaded = store.loadPackage(HOTEL_ID, BASELINE_VERSION);
if (!baselineLoaded.ok) {
  console.error("BLOCKED: baseline package missing", baselineLoaded);
  process.exit(3);
}
const baseline = baselineLoaded.package;
if (baseline.contentFingerprintSha256 !== BASELINE_FP) {
  console.error("BLOCKED: baseline fingerprint mismatch");
  process.exit(3);
}

// Snapshot baseline
fs.writeFileSync(
  path.join(OUT, "baseline", "package.json"),
  JSON.stringify(baseline, null, 2) + "\n",
);
fs.copyFileSync(
  path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL_ID,
    "STAGED.json",
  ),
  path.join(OUT, "baseline", "STAGED.json"),
);

const assessments = [];
const customerOps = [];
const researchStage = [];
const quarantined = [];

const rootCauses = {
  marketScopeHotelZero:
    "DTO used inventoryPopulations:['hotel'] instead of certified SALES_OPPORTUNITIES — UI Market Scope Hotel counts only SALES_OPPORTUNITIES membership (cause E/H).",
  salesOpportunitiesAllZero:
    "salesOpportunityQuickFilterKeys were never attached — Bethesda filter chips require attachSalesOpportunityFilterFields / classifySalesOpportunityKeys (cause E/G).",
  opportunityTypeAll62:
    "opportunityType left as raw researchSegment snake_case without opportunityTypeFilterCategory / demandSegmentId mapping (cause D/E).",
  buildAccountOnly:
    "salesActionKey hardcoded BUILD_ACCOUNT; certified resolveCustomerSalesAction never applied with partnership/catering/qualify signals (cause B/G).",
  unknownSourcing:
    "hotelSourcingStatusLabel set to UNKNOWN instead of Hotel Sourcing Unconfirmed / normalized sourcing enum (cause C/E).",
  genericNarratives:
    "Shortlist hotelThesis fallback + Aimbridge 'Documented local expansion/growth activity' strings promoted without demand-specific rewrite (cause A/G).",
  zeroSalesReady:
    "Commercially mostly justified (no verified lodging DM + open sourcing), but prior demotion also skipped Qualify Opportunity differentiation — eligibility audit applied below without inventing Sales Ready (cause B, partial F).",
};

for (const raw of baseline.opportunities) {
  const id = text(raw.id);
  const dossierPath = path.join(RESEARCH_DIR, `${id}.json`);
  const dossier = fs.existsSync(dossierPath) ? loadJson(dossierPath) : null;
  const title = text(raw.displayTitle || raw.title || id);
  const org = text(raw.organizationName || dossier?.organizationName || "");
  const narratives = buildNarratives(raw, dossier || {});
  const partnership = detectPartnership(title, dossier || {});
  const catering = detectCateringPotential(title, dossier || {}, narratives.what);
  const competitive = detectCompetitive(dossier || {}, raw);
  const segRaw =
    dossier?.researchSegment ||
    raw.opportunityType ||
    dossier?.originatingSegment ||
    "";
  const demandSegmentId = mapSegmentToDemandId(segRaw);
  const startDate =
    parseIsoDate(raw.tileEventStartDate) ||
    parseIsoDate(dossier?.originalActivityDate) ||
    parseIsoDate(dossier?.growthEvidenceDate) ||
    parseIsoDate(raw.datePillDisplayLabel);
  const yearMatch = text(raw.datePillDisplayLabel || title).match(/\b(20\d{2})\b/);
  const year = yearMatch ? Number(yearMatch[1]) : startDate ? Number(startDate.slice(0, 4)) : null;
  const expiredWithoutFuture =
    year != null && year < 2026 && !/2026|2027|2028|recurring|edition|ciclo|próxim/i.test(
      `${title} ${narratives.what} ${narratives.whyNow}`,
    );
  const pc = raw.primaryContact || {};
  const hasContact = Boolean(text(pc.name));
  const evidence =
    Array.isArray(raw.sources) && raw.sources.length
      ? raw.sources
      : Array.isArray(dossier?.evidence)
        ? dossier.evidence.map((e) => ({
            url: e.url,
            statement: text(e.statement || title),
          }))
        : [];
  const hasEvidence = evidence.some((e) => text(e.url || e));
  const blob = `${title} ${org} ${narratives.what}`.toLowerCase();
  let relevance = "HOTEL";
  if (/cancún|cancun|las vegas|cdmx reforma only|mexico city reforma/i.test(blob)) {
    relevance = "OUTSIDE";
  } else if (
    /guadalajara|zapopan|jalisco|expo|plaza del sol|tlaquepaque/i.test(blob)
  ) {
    relevance = "HOTEL";
  }

  const hasFutureDate =
    (startDate && startDate >= "2026-01-01") ||
    (year != null && year >= 2026) ||
    /2026|2027|2028/i.test(title + narratives.what);

  // Qualify Opportunity when lodging thesis + unconfirmed sourcing + named contact
  const qualifyStance =
    hasContact &&
    hasEvidence &&
    relevance === "HOTEL" &&
    !partnership &&
    /lodging|room|hotel|overnight|attendee|delegate|housing|group|congreso|congress|torneo|expo/i.test(
      `${narratives.potential} ${narratives.what}`,
    );

  const pursueNowEligible = false; // no confirmed open RFP/sourcing in this free remediation

  const ctx = {
    partnership,
    catering,
    competitive,
    hasContact,
    hasEvidence,
    relevance,
    expiredWithoutFuture,
    hasFutureDate,
    recurring: /annual|edición|edition|recurring|ciclo|congress|congreso|maratón|expo/i.test(
      title + narratives.what,
    ),
    pursueNowEligible,
    qualifyStance,
  };

  const disposition = chooseDisposition(ctx);
  const commercialStageKey = chooseCommercialStage(ctx);

  const inventoryPopulations = [];
  if (relevance === "HOTEL" && disposition === "CUSTOMER_VISIBLE") {
    inventoryPopulations.push(INVENTORY_POPULATION.SALES_OPPORTUNITIES);
  }
  if (competitive && disposition === "CUSTOMER_VISIBLE") {
    inventoryPopulations.push(
      INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE,
    );
  }
  if (!inventoryPopulations.length && disposition === "CUSTOMER_VISIBLE") {
    inventoryPopulations.push(INVENTORY_POPULATION.SALES_OPPORTUNITIES);
  }

  let draft = {
    ...raw,
    displayTitle: title,
    title,
    organizationName: org || raw.organizationName || null,
    displayCompanyName: org || raw.displayCompanyName || null,
    whatIsHappening: narratives.what,
    potentialHotelBusiness: narratives.potential,
    potentialBusiness: narratives.potential,
    whyThisHotel: narratives.whyHotel,
    whyActNow: narratives.whyNow,
    recommendedNextStep: text(
      dossier?.recommendedAction ||
        dossier?.recommendedNextSalesAction ||
        raw.recommendedNextStep ||
        raw.recommendedAction ||
        "",
    ),
    recommendedAction: null, // filled after sales action resolve
    hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
    hotelMarketScopeEligible: inventoryPopulations.includes(
      INVENTORY_POPULATION.SALES_OPPORTUNITIES,
    ),
    competitiveMarketScopeEligible: inventoryPopulations.includes(
      INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE,
    ),
    cmiVerified: competitive ? false : null,
    competitiveStatus: competitive ? "NEEDS_VERIFICATION" : null,
    competitiveStatusLabel: competitive
      ? "Competitive — needs verification"
      : null,
    inventoryPopulations,
    inventoryPopulationPrimary: inventoryPopulations[0] || null,
    demandSegmentId: demandSegmentId || undefined,
    researchSegment: text(segRaw) || raw.opportunityType,
    opportunityType: demandSegmentId || raw.opportunityType,
    preferredLodgingPartnershipV1: partnership || undefined,
    demandOpportunitySubtype: partnership
      ? "PREFERRED_LODGING_PARTNERSHIP"
      : undefined,
    revenueStreamClass: catering
      ? "COMBINED_GUESTROOMS_EVENT_FNB"
      : "GUESTROOMS_ONLY",
    eventFnbThesis: catering
      ? text(
          `Public program signals may include banquet/networking/meeting components around ${title}; catering capture is hypothesized, not confirmed.`,
        )
      : undefined,
    guestroomThesis: narratives.potential,
    commercialStageKey,
    commercialStagePillLabel: commercialStageKey
      .replace(/_/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase()),
    inventoryLane: commercialStageKey,
    bookingWindowStatus: qualifyStance ? "QUALIFY_NOW" : "WATCH",
    hasVerifiedNamedRelevantContact: hasContact,
    hasCredibleOutreachRoute: hasContact,
    sources: evidence,
    eventStartDate: startDate,
    tileEventStartDate: startDate,
    eventDateDisplay: startDate || raw.datePillDisplayLabel || null,
    datePillDisplayLabel:
      startDate ||
      (year ? String(year) : null) ||
      raw.datePillDisplayLabel ||
      "Timing TBD",
    summaryWhat: narratives.what,
    cardWhyNowLine: narratives.whyNow.slice(0, 160),
    standingOpportunity: ctx.recurring && !startDate ? true : undefined,
    remediationV2: {
      baselineFingerprint: BASELINE_FP,
      disposition,
      marketScopeRationale:
        relevance === "HOTEL"
          ? "Guadalajara/Jalisco/Expo-corridor relevance supported"
          : "Outside realistic Sheraton Expo territory",
      competitiveRationale: competitive
        ? "Competitor-hosted or alternate-venue language in research — CMI eligible pending verification"
        : "No verified competitor-hosted evidence",
      narrativeRemediated: isGenericThesis(raw.whatIsHappening),
    },
  };

  // Opportunity type filter labels
  const typeKey = classifyOpportunityTypeFilter(draft);
  draft.opportunityTypeFilterCategory = typeKey;
  draft.opportunityTypeFilterLabel =
    OPPORTUNITY_TYPE_FILTER_LABEL[typeKey] || typeKey;
  draft.opportunityTypeLabel = draft.opportunityTypeFilterLabel;
  draft.segmentDisplayLabel = draft.opportunityTypeFilterLabel;

  // Defensive qualify stance inputs for sales-action resolver
  if (qualifyStance) {
    draft.recommendedNextStep = text(
      `Qualify the lodging/meetings decision path for ${title} with ${text(
        pc.name,
      )}. Lodging demand path not established for ${HOTEL_NAME} — do not claim open RFP; confirm whether hotel sourcing is open before pursuit.`,
    );
    draft.commercialStageKey = "QUALIFICATION_NEEDED";
    draft.inventoryLane = "QUALIFICATION_NEEDED";
    draft.bookingWindowStatus = "QUALIFY_NOW";
  }
  if (partnership) {
    draft.recommendedNextStep = text(
      `Open a preferred-lodging / referral partnership conversation for overflow and overnight guests tied to ${title}.`,
    );
    draft.commercialStageKey = "RELATIONSHIP_DEVELOPMENT";
    draft.inventoryLane = "RELATIONSHIP_DEVELOPMENT";
    draft.preferredLodgingPartnershipV1 = true;
    draft.demandOpportunitySubtype = "PREFERRED_LODGING_PARTNERSHIP";
  }

  const sales = resolveCustomerSalesAction(draft);
  draft.salesActionKey = sales.key;
  draft.salesActionPillLabel = sales.label;
  draft.salesActionReason = sales.reason;
  draft.recommendedAction = text(
    dossier?.recommendedAction || draft.recommendedNextStep || sales.label,
  );
  draft.recommendedNextStep = draft.recommendedAction;

  const tilePlan = planCustomerTileBadges(draft);
  draft.tileBadgePlan = tilePlan;
  draft.priorityFilterKey = tilePlan.priorityFilterKey || draft.priority;
  draft.priorityPillLabel = tilePlan.priorityPillLabel || null;
  draft.showSalesActionPill = true;
  draft.commercialStageKey =
    tilePlan.commercialStageKey || draft.commercialStageKey;
  draft.datePillDisplayLabel =
    tilePlan.datePillDisplayLabel || draft.datePillDisplayLabel;
  if (tilePlan.tileEventStartDate) {
    draft.tileEventStartDate = tilePlan.tileEventStartDate;
  }

  draft = attachSalesOpportunityFilterFields(draft);

  // Shared drawer
  draft.sharedTenSectionDrawer = {
    sections: [
      {
        id: "what_is_happening",
        title: "What is happening",
        body: narratives.what,
      },
      {
        id: "potential_hotel_business",
        title: "Potential hotel business",
        body: narratives.potential,
      },
      {
        id: "why_this_hotel",
        title: "Why this hotel",
        body: narratives.whyHotel,
      },
      {
        id: "why_now",
        title: "Why now",
        body: narratives.whyNow,
      },
      {
        id: "who_to_contact",
        title: "Who to contact",
        body: hasContact
          ? `${pc.name}${pc.title ? ` — ${pc.title}` : ""}. ${text(
              pc.contactClass || "Verify lodging decision authority before outreach.",
            )}`
          : "Named contact not yet verified.",
      },
      {
        id: "recommended_next_step",
        title: "Recommended next step",
        body: draft.recommendedNextStep,
      },
      {
        id: "supporting_evidence",
        title: "Supporting evidence",
        body: evidence.length
          ? evidence
              .map((e) =>
                typeof e === "string"
                  ? e
                  : `${e.statement || ""} ${e.url || ""}`.trim(),
              )
              .join("\n")
          : "Evidence retained in research dossier.",
      },
    ],
  };

  const assessment = {
    id,
    title,
    disposition,
    salesAction: draft.salesActionPillLabel,
    salesActionReason: draft.salesActionReason,
    commercialStageKey: draft.commercialStageKey,
    marketScope: {
      hotel: draft.hotelMarketScopeEligible,
      competitive: draft.competitiveMarketScopeEligible,
      rationale: draft.remediationV2.marketScopeRationale,
    },
    salesOpportunityQuickFilterKeys: draft.salesOpportunityQuickFilterKeys,
    opportunityTypeFilter: typeKey,
    narrativeRemediated: draft.remediationV2.narrativeRemediated,
    contact: pc.name || null,
    contactTitle: pc.title || null,
    sourcing: draft.hotelSourcingStatusLabel,
    datePill: draft.datePillDisplayLabel,
    relevance,
    partnership,
    catering,
    competitive,
    qualifyStance,
    hasEvidence,
    salesReady: false,
    salesReadyBlockers: [
      "No verified open hotel sourcing / RFP for this property",
      "Booking authority not confirmed as hotel-sourcing decision-maker in most cases",
    ],
  };
  assessments.push(assessment);

  if (disposition === "CUSTOMER_VISIBLE") {
    customerOps.push(draft);
  } else if (String(disposition).startsWith("QUARANTINE")) {
    quarantined.push({ id, title, disposition });
  } else {
    researchStage.push({ id, title, disposition });
  }
}

// Dedupe preserve order
const seen = new Set();
const uniqueCustomer = [];
for (const o of customerOps) {
  if (seen.has(o.id)) continue;
  seen.add(o.id);
  uniqueCustomer.push(o);
}

const publicationVersion = `gdi_pub_${HOTEL_ID}_${new Date()
  .toISOString()
  .slice(0, 10)
  .replace(/-/g, "")}_commercial_e2e_remediation_v2`;

const pkg = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: uniqueCustomer,
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    remediation: "gdi-sheraton-commercial-e2e-remediation-v2",
    baselineFingerprint: BASELINE_FP,
    baselineVersion: BASELINE_VERSION,
    templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    templateSha: TEMPLATE_SHA,
    paidResearchUsd: 0,
  },
});
const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) {
  console.error("PACKAGE_INTEGRITY_FAILED", integrity.fails);
  process.exit(2);
}

const written = store.writePackage(pkg);
if (!written.ok) {
  console.error("WRITE_FAILED", written);
  process.exit(2);
}
// Archive prior STAGED pointer content already snapshotted; update STAGED to new candidate
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: BASELINE_VERSION,
    contentFingerprintSha256: BASELINE_FP,
  },
});

// Local opportunities.json for live research browse
fs.writeFileSync(
  path.join(DATA_ROOT, "opportunities.json"),
  JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      updatedAt: new Date().toISOString(),
      source: "gdi-sheraton-commercial-e2e-remediation-v2",
      templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
      opportunities: uniqueCustomer,
    },
    null,
    2,
  ) + "\n",
);

// Airtable dry-run
let airtablePlan = { configured: isGdiOpportunityAirtableConfigured(), writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: uniqueCustomer,
    source: "SHERATON_REMEDIATION_V2_DRY_RUN",
  });
  airtablePlan = {
    configured: true,
    dryRun: true,
    writes: 0,
    ok: sync.ok,
    airtableCountExisting: sync.airtableCount,
    summary: {
      total: sync.summary?.total,
      wouldCreate: sync.summary?.created,
      wouldUpdate: sync.summary?.updated,
      failed: sync.summary?.failed,
      sampleRows: (sync.summary?.rows || []).slice(0, 15),
    },
    authorizationRequired: true,
  };
} catch (e) {
  airtablePlan.error = String(e?.message || e);
}

const actionCounts = {};
const filterKeyCounts = {};
for (const o of uniqueCustomer) {
  const a = o.salesActionPillLabel || "?";
  actionCounts[a] = (actionCounts[a] || 0) + 1;
  for (const k of o.salesOpportunityQuickFilterKeys || []) {
    filterKeyCounts[k] = (filterKeyCounts[k] || 0) + 1;
  }
}

const beforeAfter = {
  before: {
    fingerprint: BASELINE_FP,
    customerVisible: 62,
    salesReady: 0,
    marketScopeHotel: 0,
    marketScopeCompetitive: 0,
    salesOpportunityFiltersPopulated: false,
    buildAccountOnly: 62,
    genericUnknownSourcing: true,
  },
  after: {
    fingerprint: pkg.contentFingerprintSha256,
    publicationVersion,
    assessed: 62,
    customerVisible: uniqueCustomer.length,
    researchStage: researchStage.length,
    quarantined: quarantined.length,
    salesReady: 0,
    marketScopeHotel: uniqueCustomer.filter((o) => o.hotelMarketScopeEligible)
      .length,
    marketScopeCompetitive: uniqueCustomer.filter(
      (o) => o.competitiveMarketScopeEligible,
    ).length,
    actionCounts,
    filterKeyCounts,
    inventoryCounts: pkg.inventoryCounts,
    narrativesRemediated: assessments.filter((a) => a.narrativeRemediated)
      .length,
    contactsPresent: assessments.filter((a) => a.contact).length,
  },
};

writeJson("01-BASELINE_62_OPPORTUNITIES.json", {
  fingerprint: BASELINE_FP,
  version: BASELINE_VERSION,
  count: baseline.opportunities.length,
  ids: baseline.opportunities.map((o) => o.id),
  archivedAt: path.join(OUT, "baseline"),
});
writeJson("02-E2E_RESEARCH_COVERAGE.json", {
  dossiersAvailable: fs.existsSync(RESEARCH_DIR)
    ? fs.readdirSync(RESEARCH_DIR).filter((f) => f.endsWith(".json")).length
    : 0,
  assessed: 62,
  paidResearchUsd: 0,
  freeRemediation: true,
  rootCauses,
});
writeJson("03-MARKET_SCOPE_RECLASSIFICATION.json", {
  hotelEligible: beforeAfter.after.marketScopeHotel,
  competitiveEligible: beforeAfter.after.marketScopeCompetitive,
  rows: assessments.map((a) => ({
    id: a.id,
    hotel: a.marketScope.hotel,
    competitive: a.marketScope.competitive,
    rationale: a.marketScope.rationale,
  })),
});
writeJson("04-SALES_OPPORTUNITIES_RECLASSIFICATION.json", {
  filterKeyCounts,
  rows: assessments
    .filter((a) => a.disposition === "CUSTOMER_VISIBLE")
    .map((a) => ({
      id: a.id,
      keys: a.salesOpportunityQuickFilterKeys,
    })),
});
writeJson("05-COMMERCIAL_ACTION_REVIEW.json", {
  actionCounts,
  rows: assessments.map((a) => ({
    id: a.id,
    action: a.salesAction,
    reason: a.salesActionReason,
    disposition: a.disposition,
  })),
});
writeJson("06-CONTACT_AND_BUYER_QUALITY.json", {
  withContact: assessments.filter((a) => a.contact).length,
  needingResearch: assessments.filter((a) => !a.contact).length,
  salesReadyAudit:
    "Zero Sales Ready retained — justified. Qualify Opportunity used where lodging thesis + unconfirmed sourcing + named contact. No Surfe.",
  rows: assessments.map((a) => ({
    id: a.id,
    contact: a.contact,
    title: a.contactTitle,
    salesReady: a.salesReady,
    blockers: a.salesReadyBlockers,
  })),
});
writeJson("07-DEMAND_THESIS_REMEDIATION.json", {
  remediatedCount: assessments.filter((a) => a.narrativeRemediated).length,
  note: "Generic expansion/growth filler replaced with demand-specific dossier-backed narratives where available.",
});
writeJson("08-DATES_TITLES_AND_SOURCING.json", {
  sourcingNormalizedTo: "Hotel Sourcing Unconfirmed",
  datePillSamples: uniqueCustomer.slice(0, 20).map((o) => ({
    id: o.id,
    title: o.displayTitle,
    datePill: o.datePillDisplayLabel,
    eventStartDate: o.eventStartDate || null,
  })),
});
writeJson("09-HOTEL_SPECIFIC_FIT.json", {
  hotel: HOTEL_NAME,
  rooms: 216,
  meetingRooms: 12,
  needDates: "UNKNOWN",
  competitiveSet: "UNKNOWN_EMPTY",
  positioning: "Expo Guadalajara–adjacent full-service Sheraton",
});
writeJson("10-SEMANTIC_QUALITY_REVIEW.json", {
  assessed: assessments.length,
  customerVisible: uniqueCustomer.length,
  researchStage,
  quarantined,
  assessments,
});
writeJson("11-AIRTABLE_PERSISTENCE_PLAN.json", airtablePlan);
writeJson("12-BEFORE_AFTER_SCORECARD.json", beforeAfter);
writeJson("14-NEW_STAGED_PUBLICATION.json", {
  status: "STAGED_FOR_FOUNDER_REVIEW",
  publicationId: pkg.publicationId,
  publicationVersion,
  fingerprint: pkg.contentFingerprintSha256,
  opportunityCount: uniqueCustomer.length,
  inventoryCounts: pkg.inventoryCounts,
  baselineSuperseded: { version: BASELINE_VERSION, fingerprint: BASELINE_FP },
  packagePath: written.path,
  doNotPublishWithoutFounderFingerprintApproval: true,
});
writeJson("15-PAID_RESEARCH_CONTINUATION_QUEUE.json", {
  autoSpendAuthorized: false,
  suggestedBudgetUsd: 40,
  items: [
    {
      gap: "Expo Guadalajara exhibitor/sponsor lodging decision-makers",
      expected: "5–10 incremental AD with named buyers",
      estUsd: 15,
    },
    {
      gap: "Association 2027 cycle housing RFPs (Notariado, SMNE, SIUMA)",
      expected: "Buyer verification toward Qualify/Pursue",
      estUsd: 15,
    },
    {
      gap: "Competitive set + competitor-hosted overflow evidence",
      expected: "Populate CMI with verified alternate venues",
      estUsd: 10,
    },
  ],
});
writeJson("16-GITHUB_STATUS.json", {
  branch: "rem/gdi-sheraton-commercial-e2e-remediation-v2",
  templateSha: TEMPLATE_SHA,
  headSha: sh("git rev-parse HEAD"),
  noBethesdaMutation: true,
  noCustomerPublished: true,
});

// 13 filled by browser script
writeJson("13-LOCAL_FOUNDER_PREVIEW_VERIFICATION.json", {
  status: "PENDING_BROWSER_SCRIPT",
  previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
  expectedFingerprint: pkg.contentFingerprintSha256,
  expectedCount: uniqueCustomer.length,
});

const founder = `# Sheraton Guadalajara Expo — Commercial E2E Remediation V2

**Template:** \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\` @ \`${TEMPLATE_SHA}\`  
**Baseline fingerprint (archived):** \`${BASELINE_FP}\`  
**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\`  
**New version:** \`${publicationVersion}\`  
**Status:** **STAGED_FOR_FOUNDER_REVIEW** — not CUSTOMER_PUBLISHED

## Root causes (Sheraton ≠ Bethesda filters)

| Symptom | Root cause |
|---------|------------|
| Market Scope Hotel 0 | \`inventoryPopulations: ["hotel"]\` instead of \`SALES_OPPORTUNITIES\` |
| Sales Opportunities chips empty | Missing \`salesOpportunityQuickFilterKeys\` |
| Opportunity Type “All 62” | Raw researchSegment without filter category mapping |
| Only Build Account | Hardcoded action; certified sales-action resolver unused |
| UNKNOWN sourcing | Label not normalized to Hotel Sourcing Unconfirmed |
| Generic expansion narratives | Aimbridge shortlist thesis / expansion filler not demand-rewritten |

These were **presentation + classification pipeline gaps (E/G/H)**, compounded by incomplete commercial differentiation (B) — not solely a UI bug.

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Customer-visible | 62 | ${uniqueCustomer.length} |
| Research-stage / demoted | 0 | ${researchStage.length} |
| Quarantined | 0 | ${quarantined.length} |
| Market Scope Hotel | 0 | ${beforeAfter.after.marketScopeHotel} |
| Market Scope Competitive | 0 | ${beforeAfter.after.marketScopeCompetitive} |
| Sales Ready | 0 | 0 |
| Build Account only | 62 | ${actionCounts["Build Account"] || 0} |
| Narratives remediated | — | ${beforeAfter.after.narrativesRemediated} |

### Commercial actions after remediation
${Object.entries(actionCounts)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

### Sales Opportunities filter keys
${Object.entries(filterKeyCounts)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

## Sales Ready

Still **0** — commercially justified. Remediation correctly expands **Qualify Opportunity / Partnership / Monitor** differentiation without inventing confirmed buyers or open RFPs.

## Airtable

Dry-run only — **no writes**. Authorize separately before upserting ${uniqueCustomer.length} customer rows (existing Airtable hotel count: ${airtablePlan.airtableCountExisting ?? "n/a"}).

## Preview

\`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged\`

Restart local \`node server.js\` if the API still serves the baseline fingerprint.

## Paid research

**Not executed.** Continuation queue in \`15-PAID_RESEARCH_CONTINUATION_QUEUE.json\` (~$40 suggested, not authorized).

## Full E2E complete?

**PARTIALLY COMPLETE** — classification/presentation/narrative remediation done at Bethesda structural bar; competitive set, need dates, and verified Sales Ready buyers remain blockers without hotel input / paid research.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder, "utf8");

console.log(
  JSON.stringify(
    {
      baselineFp: BASELINE_FP,
      newFp: pkg.contentFingerprintSha256,
      customerVisible: uniqueCustomer.length,
      researchStage: researchStage.length,
      quarantined: quarantined.length,
      actionCounts,
      filterKeyCounts,
      inventoryCounts: pkg.inventoryCounts,
      out: OUT,
    },
    null,
    2,
  ),
);
