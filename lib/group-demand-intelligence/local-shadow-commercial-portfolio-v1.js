/**
 * Local shadow commercial portfolio overlay for founder sales workspace.
 * Waterstone + Chapultepec only. Never writes Airtable / never publishes.
 *
 * - Overlays commercialProspectTier / dosGrade / salesBrief onto store rows
 * - Merges FS-only shadow opportunities missing from the live read path
 * - Persists founder review decisions locally (ASSIGN_NOW etc.)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  DOS_GRADE,
  assessOutreachRoutes,
  CONTACT_PERSONHOOD,
} from "./commercial-sales-quality-v1.js";
import { hotelDir, loadOpportunities, saveOpportunities } from "./repository.js";
import { isGroupDemandIntelligenceLocalDev } from "./feature-flag.js";
import { applyChapultepecScalingActiveLocalInventory } from "./chapultepec-scaling-active-local-inventory-v1.js";
import { applyMexicoTwoHotelLocalArchiveRegistry } from "./mexico-two-hotel-local-archive-registry-v1.js";
import { applyUniversalTimingOverlay } from "./universal-timing-intelligence-v1.js";
import {
  applyNobuLondonCampaignVisibilityDisable,
  isShadowDocumentCustomerFacingEnabled,
} from "./nobu-london-campaign-visibility-disable-v1.js";
import { applyResearchToDrawerIntelligenceV2 } from "./research-to-drawer-intelligence-v2.js";
import { applyWaterstoneContactMaturationOverlay } from "./waterstone-contact-maturation-overlay-v1.js";
import { applyWaterstoneCommercialEligibilityOverlay } from "./waterstone-commercial-eligibility-overlay-v1.js";
import {
  applyWaterstoneCleanSalesInventory,
  mergeWaterstoneClean28ClientFacingPack,
} from "./waterstone-clean-sales-inventory-v1.js";
import { normalizeOpportunityForCommercialEligibilityEvaluation } from "./commercial-eligibility-field-normalization-v1.js";

export const LOCAL_SHADOW_COMMERCIAL_PORTFOLIO_V1 =
  "gdi-local-shadow-commercial-portfolio-v1";

export const LOCAL_SHADOW_HOTEL_IDS = Object.freeze([
  "recgMYovrrZDJMqzX", // Waterstone
  "recterKmNPdntCm0v", // Grand Fiesta Americana Chapultepec
  "recIEcSWuhSCoNLFN", // Fiesta Americana Condesa Cancún All Inclusive
  "recC2kbCaS48jFjFF", // Nobu Hotel London Portman Square
  "recLuxvwwxID7U2B8", // Bethesda Marriott
  "recrPQcZg7SFARRb2", // YOTEL Geneva Lake
  "recKRJjcPnb4tVDDS", // Spice Island Beach Resort
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

const NOBU_LONDON_HOTEL_ID = "recC2kbCaS48jFjFF";
const NOBU_LONDON_V2_OVERLAY = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/nobu-london-research-to-drawer-v2-overlay-v1.json",
);
/** Nobu London 14-workflow live Exa FullE2E local shadow (isolated; never mutates baseline store IDs). */
const NOBU_LONDON_14_WORKFLOW_LIVE_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/nobu-london-14-workflow-live-opportunities-v1.json",
);

const DEFAULT_SHADOW_PORTFOLIO = path.join(
  ROOT,
  "reports/gdi-2-review-loop/waterstone-chapultepec-expansion/shadow-portfolios.json",
);

/** Stage E matured Scaling opportunities (local preview only; never Airtable). */
const STAGE_E_SHADOW_OPPORTUNITIES = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/stage-e-chapultepec-opportunities.json",
);

/** Stage F sales-leader editorial briefs (prefer over Stage E when present). */
const STAGE_F_SHADOW_OPPORTUNITIES = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/stage-f-chapultepec-opportunities.json",
);

/** Live Mexico Scaling campaign V1 shadow (never overwrites gold three). */
const LIVE_CAMPAIGN_V1_SHADOW_OPPORTUNITIES = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/live-campaign-v1-chapultepec-opportunities.json",
);

/** Live Condesa Cancún Scaling campaign V1 shadow (hotel-scoped; never Chapultepec). */
const LIVE_CAMPAIGN_V1_CONDESA_SHADOW_OPPORTUNITIES = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/live-campaign-v1-condesa-cancun-opportunities.json",
);

/** Segments 4 / 9 / 12 live Exa FullE2E local shadow (isolated; never overwrites Scaling/Events). */
const SEGMENTS_4_9_12_LIVE_CHAPULTEPEC_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/segments-4-9-12-live-chapultepec-opportunities-v1.json",
);
const SEGMENTS_4_9_12_LIVE_CONDESA_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/segments-4-9-12-live-condesa-opportunities-v1.json",
);

/** Safe Condesa multi-segment reclass overlay (local preview only; originals preserved). */
const CONDESA_MULTISEGMENT_RECLASS_PREVIEW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/condesa-multisegment-reclass-preview-v1.json",
);

/** Professional Event Planner zero-paid cert fixture (Chapultepec local preview only). */
const PROFESSIONAL_EVENT_PLANNERS_CERT_CHAPULTEPEC_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/professional-event-planners-cert-chapultepec-v1.json",
);

/** Mexico Preferred Lodging live FullE2E shadows (isolated; local review only). */
const PREFERRED_LODGING_MEXICO_LIVE_CHAPULTEPEC_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/preferred-lodging-mexico-live-chapultepec-opportunities-v1.json",
);
const PREFERRED_LODGING_MEXICO_LIVE_CONDESA_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/preferred-lodging-mexico-live-condesa-opportunities-v1.json",
);

/** Mexico Professional Event Planners live FullE2E shadows (isolated; local review only). */
const PROFESSIONAL_EVENT_PLANNERS_MEXICO_LIVE_CHAPULTEPEC_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/professional-event-planners-mexico-live-chapultepec-opportunities-v1.json",
);
const PROFESSIONAL_EVENT_PLANNERS_MEXICO_LIVE_CONDESA_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/professional-event-planners-mexico-live-condesa-opportunities-v1.json",
);

/** Waterstone V7 Stage 2 live research → local preview (never Airtable / publication). */
const WATERSTONE_V7_STAGE2_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/waterstone-v7-stage2-opportunities-v1.json",
);

/** Waterstone V9 live deep discovery → local founder review (never Airtable / publication). */
const WATERSTONE_V9_LIVE_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/waterstone-v9-live-opportunities-v1.json",
);

/** Waterstone 14-workflow live FullE2E local shadow (isolated; never mutates protected 24). */
const WATERSTONE_14_WORKFLOW_LIVE_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/waterstone-14-workflow-live-opportunities-v1.json",
);

/** Bethesda V16 14-workflow live FullE2E local shadow (isolated; never auto-promotes facing). */
const BETHESDA_V16_FOURTEEN_WORKFLOW_LIVE_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/bethesda-v16-fourteen-workflow-live-opportunities-v1.json",
);

/** Bethesda V19 Need-Date activation — enrichments + selective local promotions. */
const BETHESDA_V19_NEED_DATE_ACTIVATION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/bethesda-v19-need-date-activation-v1.json",
);

/** Bethesda V20 incremental live research — internal staging only. */
const BETHESDA_V20_INCREMENTAL_LIVE_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/bethesda-v20-incremental-live-opportunities-v1.json",
);

/** YOTEL Geneva Lake V21 14-workflow live — internal staging only. */
const YOTEL_GENEVA_LAKE_V21_FOURTEEN_WORKFLOW_LIVE_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/yotel-geneva-lake-v21-fourteen-workflow-live-opportunities-v1.json",
);

/** YOTEL Geneva Lake V23 discovery completeness corrective — internal staging only. */
const YOTEL_GENEVA_LAKE_V23_DISCOVERY_COMPLETENESS_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/yotel-geneva-lake-v23-discovery-completeness-opportunities-v1.json",
);

/** YOTEL Geneva Lake V24 selective local promotion — founder-accepted Sales Ready only. */
const YOTEL_GENEVA_LAKE_V24_SELECTIVE_PROMOTION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/yotel-geneva-lake-v24-selective-promotion-opportunities-v1.json",
);

/** V28 two-hotel selective local promotion — exact founder-approved Sales Ready IDs only. */
const YOTEL_GENEVA_LAKE_V28_SELECTIVE_PROMOTION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/yotel-geneva-lake-v28-selective-promotion-opportunities-v1.json",
);
const SPICE_ISLAND_V28_SELECTIVE_PROMOTION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/spice-island-v28-selective-promotion-opportunities-v1.json",
);

/**
 * Internal staging shadows (customerFacingEnabled must remain false until founder GO).
 * Merged for inventory reconciliation / handoff audits only — never auto-promoted.
 */
const YOTEL_GENEVA_LAKE_V28_INCREMENTAL_YIELD_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/yotel-geneva-lake-v28-incremental-yield-opportunities-v1.json",
);
const SPICE_ISLAND_V28_INCREMENTAL_YIELD_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/spice-island-v28-incremental-yield-opportunities-v1.json",
);
const YOTEL_GENEVA_LAKE_V30_ADAPTIVE_CONTINUATION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/yotel-geneva-lake-v30-adaptive-continuation-opportunities-v1.json",
);
const SPICE_ISLAND_V30_ADAPTIVE_CONTINUATION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/spice-island-v30-adaptive-continuation-opportunities-v1.json",
);

/** V32 two-hotel selective local promotion — founder-approved V31 exact IDs (loads after internal staging). */
const YOTEL_GENEVA_LAKE_V32_SELECTIVE_PROMOTION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/yotel-geneva-lake-v32-selective-promotion-opportunities-v1.json",
);
const SPICE_ISLAND_V32_SELECTIVE_PROMOTION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/spice-island-v32-selective-promotion-opportunities-v1.json",
);

/** V33 adaptive continuation — internal staging only until founder promotion GO. */
const YOTEL_GENEVA_LAKE_V33_ADAPTIVE_CONTINUATION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/yotel-geneva-lake-v33-adaptive-continuation-opportunities-v1.json",
);
const SPICE_ISLAND_V33_ADAPTIVE_CONTINUATION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/spice-island-v33-adaptive-continuation-opportunities-v1.json",
);

/** V34 selective local promotion — Kenwood Travel only (loads after V33 internal staging). */
const SPICE_ISLAND_V34_SELECTIVE_PROMOTION_SHADOW = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/spice-island-v34-selective-promotion-opportunities-v1.json",
);

function isSelectivePromotionSticky(opp = {}) {
  return (
    opp?.gdiV34SelectivePromotion === true ||
    opp?.gdiV32SelectivePromotion === true ||
    opp?.gdiV28SelectivePromotion === true ||
    opp?.gdiV29SelectivePromotion === true ||
    /SELECTIVE_PROMOTION/i.test(String(opp?.localSalesReadyVisibilityStamp || ""))
  );
}

const CONDESA_HOTEL_ID = "recIEcSWuhSCoNLFN";

const PROTECTED_GOLD_SHADOW_IDS = new Set([
  "gdi_sc_stage_e_mendel_shadow",
  "gdi_sc_stage_e_r2_shadow",
  "gdi_sc_stage_e_clip_shadow",
]);

const FOUNDER_REVIEW_DECISIONS = Object.freeze([
  DOS_GRADE.ASSIGN_NOW,
  DOS_GRADE.ASSIGN_FOR_DISCOVERY,
  DOS_GRADE.RESEARCH_FIRST,
  DOS_GRADE.DO_NOT_PURSUE,
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Prefer scrubbed store sales copy over stale shadow Contact-<person> strings.
 * run-052 Issue 1: shadow portfolios were re-injecting rejected buyers after FS scrub.
 */
function preferStoreSalesActionOverShadow(storeVal, shadowVal, storeOpp = {}) {
  const store = text(storeVal);
  const shadow = text(shadowVal);
  if (!shadow) return storeVal || null;
  if (!store) return shadowVal || null;
  const storeScrubbed =
    Boolean(storeOpp.associationEventsPresentationScrubVersion) ||
    Boolean(storeOpp.associationEventsBuyerIntegrityVersion) ||
    Boolean(storeOpp.integrityRemediation?.at);
  if (storeScrubbed && store !== shadow) return storeVal;
  // If store already moved to org-route language, keep it.
  if (/^Identify the association meetings/i.test(store) && /^Contact\s+/i.test(shadow)) {
    return storeVal;
  }
  return shadowVal || storeVal || null;
}

function hotelKey(hotelId) {
  const id = String(hotelId || "").trim();
  if (id === "recgMYovrrZDJMqzX") return "waterstone";
  if (id === "recterKmNPdntCm0v") return "chapultepec";
  if (id === "recIEcSWuhSCoNLFN") return "condesa_cancun";
  if (id === NOBU_LONDON_HOTEL_ID) return "nobu_london";
  if (id === "recLuxvwwxID7U2B8") return "bethesda";
  if (id === "recrPQcZg7SFARRb2") return "yotel_geneva_lake";
  if (id === "recKRJjcPnb4tVDDS") return "spice_island_beach_resort";
  return null;
}

export function isLocalShadowCommercialHotel(hotelId = "", env = process.env) {
  const id = String(hotelId || "").trim();
  if (!LOCAL_SHADOW_HOTEL_IDS.includes(id)) return false;
  if (String(env.GDI_LOCAL_SHADOW_COMMERCIAL || "1").trim() === "0") return false;
  return true;
}

/**
 * Load Stage E matured Scaling opportunities for local drawer preview.
 * Isolated JSON; never production Airtable.
 */
export function loadStageEShadowOpportunities(hotelId = "", env = process.env) {
  const id = String(hotelId || "").trim();
  if (!isLocalShadowCommercialHotel(id, env)) return [];
  if (String(env.GDI_STAGE_E_SHADOW_PREVIEW || "1").trim() === "0") return [];
  const p =
    text(env.GDI_STAGE_E_SHADOW_OPPORTUNITIES_PATH) || STAGE_E_SHADOW_OPPORTUNITIES;
  if (!fs.existsSync(p)) return [];
  try {
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    const items = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
    return items.filter(
      (o) =>
        o &&
        String(o.hotelId || id) === id &&
        o.stageEShadowPreview !== false,
    );
  } catch {
    return [];
  }
}

export function stageEShadowOpportunitiesPath() {
  return STAGE_E_SHADOW_OPPORTUNITIES;
}

export function stageFShadowOpportunitiesPath() {
  return STAGE_F_SHADOW_OPPORTUNITIES;
}

/**
 * Load Stage F editorial shadow opportunities (prefer when merging).
 */
export function loadStageFShadowOpportunities(hotelId = "", env = process.env) {
  const id = String(hotelId || "").trim();
  if (!isLocalShadowCommercialHotel(id, env)) return [];
  if (String(env.GDI_STAGE_F_SHADOW_PREVIEW || "1").trim() === "0") return [];
  const p =
    text(env.GDI_STAGE_F_SHADOW_OPPORTUNITIES_PATH) || STAGE_F_SHADOW_OPPORTUNITIES;
  if (!fs.existsSync(p)) return [];
  try {
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    const items = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
    return items.filter(
      (o) => o && String(o.hotelId || id) === id && o.stageFEditorial !== false,
    );
  } catch {
    return [];
  }
}

export function shadowPortfolioPath(env = process.env) {
  const override = text(env.GDI_LOCAL_SHADOW_PORTFOLIO_PATH);
  if (override) return path.isAbsolute(override) ? override : path.join(ROOT, override);
  const localCopy = path.join(
    ROOT,
    "data/group-demand-intelligence/local-shadow/shadow-portfolios.json",
  );
  if (fs.existsSync(localCopy)) return localCopy;
  return DEFAULT_SHADOW_PORTFOLIO;
}

export function loadShadowPortfolioDoc(env = process.env) {
  const p = shadowPortfolioPath(env);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export function shadowProspectsForHotel(hotelId, env = process.env) {
  const doc = loadShadowPortfolioDoc(env);
  const key = hotelKey(hotelId);
  if (!doc || !key) return [];
  const rows = doc[key]?.prospects;
  return Array.isArray(rows) ? rows : [];
}

function founderReviewsPath(hotelId) {
  return path.join(hotelDir(hotelId), "founder-reviews.json");
}

export function loadFounderReviews(hotelId) {
  const p = founderReviewsPath(hotelId);
  if (!fs.existsSync(p)) {
    return { hotelId, items: [], updatedAt: null, version: LOCAL_SHADOW_COMMERCIAL_PORTFOLIO_V1 };
  }
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return { hotelId, items: [], updatedAt: null, version: LOCAL_SHADOW_COMMERCIAL_PORTFOLIO_V1 };
  }
}

export function saveFounderReview(hotelId, { opportunityId, decision, comment, actor } = {}) {
  const id = String(opportunityId || "").trim();
  const grade = String(decision || "").trim().toUpperCase();
  if (!id) {
    const err = new Error("opportunityId_required");
    err.code = "invalid_founder_review";
    throw err;
  }
  if (!FOUNDER_REVIEW_DECISIONS.includes(grade)) {
    const err = new Error("invalid_founder_review_decision");
    err.code = "invalid_founder_review";
    err.allowed = [...FOUNDER_REVIEW_DECISIONS];
    throw err;
  }
  const doc = loadFounderReviews(hotelId);
  const next = {
    opportunityId: id,
    decision: grade,
    comment: text(comment) || "",
    actor: text(actor) || "founder_local",
    updatedAt: new Date().toISOString(),
    localOnly: true,
    doesNotAffectProduction: true,
  };
  const idx = (doc.items || []).findIndex((x) => x.opportunityId === id);
  if (idx >= 0) doc.items[idx] = { ...doc.items[idx], ...next };
  else doc.items = [...(doc.items || []), next];
  doc.hotelId = hotelId;
  doc.updatedAt = next.updatedAt;
  doc.version = LOCAL_SHADOW_COMMERCIAL_PORTFOLIO_V1;
  fs.mkdirSync(path.dirname(founderReviewsPath(hotelId)), { recursive: true });
  const tmp = `${founderReviewsPath(hotelId)}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(doc, null, 2)}\n`);
  fs.rmSync(founderReviewsPath(hotelId), { force: true });
  fs.renameSync(tmp, founderReviewsPath(hotelId));
  return next;
}

function mapTierToProspectSection(tier) {
  const t = String(tier || "").toUpperCase();
  if (t === "PRIORITY_SALES_PROSPECT" || t === "PRIORITY") return "PRIORITY";
  if (
    t === "EXPLORATORY_SALES_PROSPECT" ||
    t === "EXPLORATORY" ||
    t === "RESEARCH_CONTINUATION"
  ) {
    return "EXPLORATORY";
  }
  return null;
}

/**
 * Re-evaluate contact personhood/routes on read so housing desks are never
 * labeled verified named people in the local UI.
 */
export function applyCommercialContactQualityOverlay(opp = {}) {
  // Align local-shadow contact/thesis fields with shared eligibility recognition.
  const { opportunity: bag } =
    normalizeOpportunityForCommercialEligibilityEvaluation(opp);
  const contact = bag.primaryContact || {};
  const evidenceUrls = [
    ...(Array.isArray(bag.evidenceUrls) ? bag.evidenceUrls : []),
    ...(Array.isArray(bag.sources) ? bag.sources : []),
  ];
  const routes = assessOutreachRoutes(
    {
      name: contact.name || contact.fullName,
      title: contact.title || contact.role,
      role: contact.role || contact.title,
      email: contact.email,
      phone: contact.phone,
      url: contact.url || contact.sourceUrl,
      organization: contact.organization || bag.organizationName,
      evidence: contact.evidence || contact.source || contact.sourceUrl || contact.emailSource,
      evidenceUrl: contact.evidenceUrl || contact.sourceUrl,
      sourceUrl: contact.sourceUrl,
    },
    {
      organizationName: bag.organizationName,
      evidenceUrls,
      contactPageUrl:
        bag.organizationContactUrl ||
        bag.publicContactPath ||
        bag.contactUrl ||
        contact.url ||
        contact.organizationContactRoute,
      organizationContactEmail: bag.organizationContactEmail || bag.functionalContactEmail,
    },
  );
  return {
    ...bag,
    contactPersonhood: routes.personhood,
    personEvidence: routes.personEvidence,
    hasVerifiedNamedRelevantContact: routes.hasVerifiedNamedRelevantContact,
    isFunctionalOrganizationRoute: routes.isFunctionalOrganizationRoute,
    hasCredibleOutreachRoute: routes.hasCredibleOutreachRoute,
    outreachRouteKinds: routes.routeKinds,
    surfeOnDemandEligible: routes.surfeOnDemandEligible,
    surfeAutoTriggerForbidden: true,
    surfePersistForbidden: true,
    // Never invent a verified person stamp from a housing desk label
    primaryContact:
      routes.personhood === CONTACT_PERSONHOOD.FUNCTIONAL_ORGANIZATION_ROUTE &&
      contact &&
      typeof contact === "object"
        ? {
            ...contact,
            contactKind:
              contact.contactKind === "PRIMARY" ? "FUNCTIONAL_ROUTE" : contact.contactKind,
            personhood: routes.personhood,
          }
        : contact,
  };
}

function overlayShadowFields(storeOpp, shadow) {
  if (!shadow) return storeOpp;
  // run-034+ remmediation owns commercial truth — do not let stale shadow
  // scorecards overwrite geography corrections / rebuilt salesBriefs.
  if (storeOpp.localRemediationRun) {
    return applyCommercialContactQualityOverlay({
      ...storeOpp,
      localShadowPortfolio: true,
      shadowCommercialTier:
        storeOpp.commercialProspectTier || shadow.commercialProspectTier || null,
    });
  }
  const section = mapTierToProspectSection(shadow.commercialProspectTier);
  // Prefer store hotel thesis over shadow when shadow still carries foreign-hotel copy.
  const thesisForBrief =
    storeOpp.hotelOpportunityThesis ||
    storeOpp.whyThisHotel ||
    shadow.hotelOpportunityThesis ||
    null;
  const salesBrief =
    storeOpp.salesBrief ||
    storeOpp.commercialIntelligenceBrief ||
    (thesisForBrief || shadow.salesActionRecommended
      ? {
          opportunityOverview:
            shadow.title ||
            `${shadow.organizationName || ""} — ${shadow.title || ""}`.trim(),
          companyAndLocalActivity: thesisForBrief || null,
          potentialBusinessForThisHotel:
            shadow.roomDemandRationale || thesisForBrief || null,
          whyThisHotel: thesisForBrief || null,
          recommendedSalesApproach: {
            nextAction: shadow.salesActionRecommended || null,
            qualificationQuestions: Array.isArray(shadow.unknowns)
              ? shadow.unknowns.slice(0, 5)
              : [],
            timingRationale: shadow.refreshRecommendation || null,
            conversationOpener: shadow.conversationOpener || null,
          },
          evidenceAndUncertainty: {
            uncertainties: shadow.unknowns || [],
            sources: shadow.evidenceUrls || [],
          },
          whoToContact: {
            primary: shadow.primaryContact || null,
          },
        }
      : null);

  // Store completion contract wins over stale shadow DOS when research is open.
  // Never let an older shadow ASSIGN_NOW override RESEARCH_FIRST / continuation.
  const storeDos = text(storeOpp.dosGrade);
  const shadowDos = text(shadow.dosGrade);
  const continuationOpen =
    storeOpp.commercialResearchCompletionState === "RESEARCH_CONTINUATION" ||
    storeOpp.commercialDrawerAcceptance === "CONTINUATION_OPEN" ||
    storeOpp.commerciallyResearchComplete === false;
  const dosRank = { ASSIGN_NOW: 3, ASSIGN_FOR_DISCOVERY: 2, RESEARCH_FIRST: 1, DO_NOT_PURSUE: 0 };
  let dosGrade = storeDos || shadowDos || null;
  if (continuationOpen && storeDos) {
    dosGrade = storeDos;
  } else if (storeDos && shadowDos && (dosRank[storeDos] ?? 99) < (dosRank[shadowDos] ?? 99)) {
    dosGrade = storeDos;
  } else if (!storeDos && shadowDos) {
    dosGrade = shadowDos;
  }

  const remediatedPriority = storeOpp.priorityRemediationV1
    ? storeOpp.priority
    : shadow.priorityRemediationV1
      ? shadow.priority
      : storeOpp.priority || shadow.priority || null;

  return applyCommercialContactQualityOverlay({
    ...storeOpp,
    priority: remediatedPriority,
    priorityRemediationV1:
      storeOpp.priorityRemediationV1 ||
      shadow.priorityRemediationV1 ||
      storeOpp.priorityRemediationV1 ||
      null,
    commercialProspectTier: shadow.commercialProspectTier || storeOpp.commercialProspectTier,
    shadowCommercialTier: shadow.commercialProspectTier || storeOpp.shadowCommercialTier,
    dosGrade,
    goldBriefStatus: shadow.goldBriefStatus || storeOpp.goldBriefStatus,
    goldBriefComplete: shadow.goldBriefComplete ?? storeOpp.goldBriefComplete,
    goldBriefActionable: shadow.goldBriefActionable ?? storeOpp.goldBriefActionable,
    goldBriefMissing: shadow.goldBriefMissing || storeOpp.goldBriefMissing,
    // run-052: never let a dirty shadow sales action override a scrubbed store action.
    // Prefer store when it has presentation-atomic scrub / integrity stamps or a
    // non-empty salesAction that no longer matches the shadow Contact-<rejected> copy.
    salesActionRecommended: preferStoreSalesActionOverShadow(
      storeOpp.salesActionRecommended,
      shadow.salesActionRecommended,
      storeOpp,
    ),
    conversationOpener: preferStoreSalesActionOverShadow(
      storeOpp.conversationOpener,
      shadow.conversationOpener,
      storeOpp,
    ),
    prospectReviewSection: section || storeOpp.prospectReviewSection,
    salesBrief: salesBrief || storeOpp.salesBrief,
    commercialIntelligenceBrief:
      storeOpp.commercialIntelligenceBrief || salesBrief || null,
    localShadowPortfolio: true,
    localShadowOnly: storeOpp.localShadowOnly === true,
    customerVisible: storeOpp.customerVisible !== false,
  });
}

/**
 * Union store opportunities with local FS + shadow portfolio overlays.
 * Hotel-scoped; never crosses hotels.
 */
export function mergeLocalShadowCommercialOpportunities(
  hotelId,
  opportunities = [],
  opts = {},
) {
  const env = opts.env || process.env;
  // V26 clean-room: never merge historical local shadows into research inventory.
  if (String(env.GDI_CLEAN_ROOM_NAMESPACE || "").trim()) {
    return (opportunities || []).map(applyCommercialContactQualityOverlay);
  }
  if (!isLocalShadowCommercialHotel(hotelId, env)) {
    return (opportunities || []).map(applyCommercialContactQualityOverlay);
  }

  const byId = new Map();
  for (const o of opportunities || []) {
    if (o?.id) byId.set(String(o.id), { ...o });
  }

  // Prefer FS document when it has additional shadow rows (Airtable-empty local mode)
  try {
    const fsDoc = loadOpportunities(hotelId);
    for (const o of fsDoc.opportunities || []) {
      if (!o?.id) continue;
      const id = String(o.id);
      if (!byId.has(id)) {
        byId.set(id, { ...o, localShadowOnly: true });
      } else {
        // Fill missing commercial brief fields from FS if live row is thin.
        // Prefer FS ownership / gold-brief updates so autonomous research
        // written to the local store is not overwritten by a thinner Airtable row.
        const cur = byId.get(id);
        const fsOwns =
          Boolean(o.ownershipRun) ||
          Boolean(o.salesBrief?.investigationHistory?.ownershipVersion) ||
          Boolean(o.localShadowPortfolio) ||
          Boolean(o.reresearchCampaign);
        // Local priority remediation must survive thinner Airtable/live null priority.
        const preferFsPriority =
          Boolean(o.priorityRemediationV1) ||
          (Boolean(o.priority) && !cur.priority);
        byId.set(id, {
          ...o,
          ...cur,
          ...(preferFsPriority
            ? {
                priority: o.priority,
                priorityRemediationV1:
                  o.priorityRemediationV1 || cur.priorityRemediationV1 || null,
              }
            : {}),
          ...(fsOwns
            ? {
                salesBrief: o.salesBrief || cur.salesBrief,
                commercialIntelligenceBrief:
                  o.commercialIntelligenceBrief || cur.commercialIntelligenceBrief,
                associationEventsGoldBrief:
                  o.associationEventsGoldBrief || cur.associationEventsGoldBrief,
                hotelOpportunityThesis:
                  o.hotelOpportunityThesis || cur.hotelOpportunityThesis,
                dosGrade: o.dosGrade || cur.dosGrade,
                ownershipRun: o.ownershipRun || cur.ownershipRun,
                ownershipUpdatedAt: o.ownershipUpdatedAt || cur.ownershipUpdatedAt,
                commercialProspectTier:
                  o.commercialProspectTier || cur.commercialProspectTier,
                commercialOutcomesAddressed:
                  o.commercialOutcomesAddressed || cur.commercialOutcomesAddressed,
                // Prefer local re-research identity + narrative over thinner/stale Airtable.
                // Use nullish coalescing so intentional empty organizer clears are preserved.
                eventName: o.eventName ?? cur.eventName,
                eventDisplayName: o.eventDisplayName ?? cur.eventDisplayName,
                title: o.title ?? cur.title,
                displayTitle: o.displayTitle ?? cur.displayTitle,
                organizationName: o.organizationName ?? cur.organizationName,
                organizer: o.organizer ?? cur.organizer,
                whatIsHappening: o.whatIsHappening ?? cur.whatIsHappening,
                eventSeriesId: o.eventSeriesId ?? cur.eventSeriesId,
                eventEditionId: o.eventEditionId ?? cur.eventEditionId,
                eventStartDate: o.eventStartDate ?? cur.eventStartDate,
                eventEndDate: o.eventEndDate ?? cur.eventEndDate,
                verifiedStartDate: o.verifiedStartDate ?? cur.verifiedStartDate,
                verifiedEndDate: o.verifiedEndDate ?? cur.verifiedEndDate,
                futureEditionDate: o.futureEditionDate ?? cur.futureEditionDate,
                futureEditionEndDate:
                  o.futureEditionEndDate ?? cur.futureEditionEndDate,
                hotelSourcingStatus: o.hotelSourcingStatus ?? cur.hotelSourcingStatus,
                venueSourcingStatus: o.venueSourcingStatus ?? cur.venueSourcingStatus,
                commercialOpportunityTrack:
                  o.commercialOpportunityTrack ?? cur.commercialOpportunityTrack,
                historicalEventOnly:
                  o.historicalEventOnly ?? cur.historicalEventOnly,
                reresearchCampaign: o.reresearchCampaign ?? cur.reresearchCampaign,
                // Do not let a thinner Airtable customerVisible:false /
                // prospectSuppressed hide local autonomous research on FS.
                customerVisible: o.customerVisible === false ? false : true,
                isTestData: o.isTestData === true,
                prospectSuppressed:
                  o.dosGrade === "DO_NOT_PURSUE"
                    ? true
                    : o.prospectSuppressed === true
                      ? true
                      : false,
                localShadowPortfolio: true,
              }
            : {
                salesBrief: cur.salesBrief || o.salesBrief,
                commercialIntelligenceBrief:
                  cur.commercialIntelligenceBrief || o.commercialIntelligenceBrief,
                hotelOpportunityThesis:
                  cur.hotelOpportunityThesis || o.hotelOpportunityThesis,
              }),
          summaryWhat: fsOwns
            ? o.summaryWhat || o.whatIsHappening || cur.summaryWhat
            : cur.summaryWhat || o.summaryWhat,
          summaryWhyHotel: fsOwns
            ? o.summaryWhyHotel || cur.summaryWhyHotel
            : cur.summaryWhyHotel || o.summaryWhyHotel,
          summaryWhyMatters: fsOwns
            ? o.summaryWhyMatters || cur.summaryWhyMatters
            : cur.summaryWhyMatters || o.summaryWhyMatters,
          sources: fsOwns
            ? o.sources?.length
              ? o.sources
              : cur.sources
            : cur.sources?.length
              ? cur.sources
              : o.sources,
        });
      }
    }
  } catch {
    /* FS optional */
  }

  const shadowById = new Map(
    shadowProspectsForHotel(hotelId, env).map((p) => [String(p.id), p]),
  );
  for (const [id, shadow] of shadowById) {
    if (!byId.has(id)) {
      // Materialize minimal UI row from shadow prospect when store lacks it
      byId.set(id, materializeShadowProspectAsOpportunity(shadow, hotelId));
    } else {
      byId.set(id, overlayShadowFields(byId.get(id), shadow));
    }
  }

  // Stage E matured Scaling Companies — isolated FS preview into real drawer path
  try {
    const stageE = loadStageEShadowOpportunities(hotelId, env);
    for (const o of stageE) {
      if (!o?.id) continue;
      const id = String(o.id);
      const stamped = {
        ...o,
        hotelId,
        localShadowOnly: true,
        localShadowPortfolio: true,
        stageEShadowPreview: true,
        customerVisible: o.customerVisible !== false,
        doesNotAffectProduction: true,
      };
      if (!byId.has(id)) {
        byId.set(id, stamped);
      } else {
        // Prefer Stage E matured CIB / contacts / research state over thinner rows
        const cur = byId.get(id);
        byId.set(id, {
          ...cur,
          ...stamped,
          sources: stamped.sources?.length ? stamped.sources : cur.sources,
          commercialIntelligenceBrief:
            stamped.commercialIntelligenceBrief || cur.commercialIntelligenceBrief,
          salesBrief: stamped.salesBrief || cur.salesBrief,
          primaryContact: stamped.primaryContact || cur.primaryContact,
          secondaryContact: stamped.secondaryContact || cur.secondaryContact,
        });
      }
    }
  } catch {
    /* Stage E shadow optional */
  }

  // Stage F sales-leader editorial — wins over Stage E for same opportunity IDs
  try {
    const stageF = loadStageFShadowOpportunities(hotelId, env);
    for (const o of stageF) {
      if (!o?.id) continue;
      const id = String(o.id);
      const stamped = {
        ...o,
        hotelId,
        localShadowOnly: true,
        localShadowPortfolio: true,
        stageEShadowPreview: true,
        stageFEditorial: true,
        customerVisible: o.customerVisible !== false,
        doesNotAffectProduction: true,
      };
      if (!byId.has(id)) {
        byId.set(id, stamped);
      } else {
        const cur = byId.get(id);
        byId.set(id, {
          ...cur,
          ...stamped,
          sources: stamped.sources?.length ? stamped.sources : cur.sources,
          sourcesFull: stamped.sourcesFull || cur.sourcesFull,
          commercialIntelligenceBrief:
            stamped.commercialIntelligenceBrief || cur.commercialIntelligenceBrief,
          salesBrief: stamped.salesBrief || cur.salesBrief,
          primaryContact: stamped.primaryContact || cur.primaryContact,
          secondaryContact: stamped.secondaryContact || cur.secondaryContact,
          displayCompanyName: stamped.displayCompanyName || cur.displayCompanyName,
          displayTitle: stamped.displayTitle || cur.displayTitle,
          title: stamped.title || cur.title,
          segmentDisplayLabel: stamped.segmentDisplayLabel || cur.segmentDisplayLabel,
        });
      }
    }
  } catch {
    /* Stage F shadow optional */
  }

  // Live campaign V1 shadows — add new companies; never overwrite protected gold IDs.
  // Hotel-scoped: Chapultepec + Condesa files both filtered by opportunity.hotelId.
  try {
    if (String(env.GDI_LIVE_CAMPAIGN_V1_SHADOW_PREVIEW || "1").trim() !== "0") {
      const livePaths = [
        LIVE_CAMPAIGN_V1_SHADOW_OPPORTUNITIES,
        LIVE_CAMPAIGN_V1_CONDESA_SHADOW_OPPORTUNITIES,
        SEGMENTS_4_9_12_LIVE_CHAPULTEPEC_SHADOW,
        SEGMENTS_4_9_12_LIVE_CONDESA_SHADOW,
        PROFESSIONAL_EVENT_PLANNERS_CERT_CHAPULTEPEC_SHADOW,
        PREFERRED_LODGING_MEXICO_LIVE_CHAPULTEPEC_SHADOW,
        PREFERRED_LODGING_MEXICO_LIVE_CONDESA_SHADOW,
        PROFESSIONAL_EVENT_PLANNERS_MEXICO_LIVE_CHAPULTEPEC_SHADOW,
        PROFESSIONAL_EVENT_PLANNERS_MEXICO_LIVE_CONDESA_SHADOW,
        NOBU_LONDON_14_WORKFLOW_LIVE_SHADOW,
        WATERSTONE_V7_STAGE2_SHADOW,
        WATERSTONE_V9_LIVE_SHADOW,
        WATERSTONE_14_WORKFLOW_LIVE_SHADOW,
        BETHESDA_V16_FOURTEEN_WORKFLOW_LIVE_SHADOW,
        BETHESDA_V19_NEED_DATE_ACTIVATION_SHADOW,
        BETHESDA_V20_INCREMENTAL_LIVE_SHADOW,
        YOTEL_GENEVA_LAKE_V21_FOURTEEN_WORKFLOW_LIVE_SHADOW,
        YOTEL_GENEVA_LAKE_V23_DISCOVERY_COMPLETENESS_SHADOW,
        YOTEL_GENEVA_LAKE_V24_SELECTIVE_PROMOTION_SHADOW,
        YOTEL_GENEVA_LAKE_V28_SELECTIVE_PROMOTION_SHADOW,
        SPICE_ISLAND_V28_SELECTIVE_PROMOTION_SHADOW,
        // Internal staging (CF disabled on document) — research-to-dashboard handoff
        YOTEL_GENEVA_LAKE_V28_INCREMENTAL_YIELD_SHADOW,
        SPICE_ISLAND_V28_INCREMENTAL_YIELD_SHADOW,
        YOTEL_GENEVA_LAKE_V30_ADAPTIVE_CONTINUATION_SHADOW,
        SPICE_ISLAND_V30_ADAPTIVE_CONTINUATION_SHADOW,
        YOTEL_GENEVA_LAKE_V33_ADAPTIVE_CONTINUATION_SHADOW,
        SPICE_ISLAND_V33_ADAPTIVE_CONTINUATION_SHADOW,
        // Founder selective promotions load last so internal CF-off staging cannot demote them.
        YOTEL_GENEVA_LAKE_V32_SELECTIVE_PROMOTION_SHADOW,
        SPICE_ISLAND_V32_SELECTIVE_PROMOTION_SHADOW,
        SPICE_ISLAND_V34_SELECTIVE_PROMOTION_SHADOW,
      ];
      // Also merge per-segment Nobu live shadow files when present.
      try {
        const shadowDir = path.join(
          ROOT,
          "data/group-demand-intelligence/local-shadow",
        );
        if (fs.existsSync(shadowDir)) {
          for (const name of fs.readdirSync(shadowDir)) {
            if (
              name.startsWith("nobu-london-") &&
              name.endsWith("-opportunities-v1.json")
            ) {
              livePaths.push(path.join(shadowDir, name));
            }
            if (
              name.startsWith("waterstone-v7-") &&
              name.endsWith("-opportunities-v1.json") &&
              name !== "waterstone-v7-stage2-opportunities-v1.json"
            ) {
              livePaths.push(path.join(shadowDir, name));
            }
            if (
              name.startsWith("waterstone-v9-") &&
              name.endsWith("-opportunities-v1.json") &&
              name !== "waterstone-v9-live-opportunities-v1.json"
            ) {
              livePaths.push(path.join(shadowDir, name));
            }
          }
        }
      } catch {
        /* optional */
      }
      for (const livePath of livePaths) {
        if (!fs.existsSync(livePath)) continue;
        const liveDoc = JSON.parse(fs.readFileSync(livePath, "utf8"));
        // Campaign-level founder disable: still merge for research retention, but do not
        // force customerVisible=true (tombstone overlay hides from facing lists).
        const docFacingEnabled = isShadowDocumentCustomerFacingEnabled(liveDoc);
        const liveItems = Array.isArray(liveDoc?.opportunities)
          ? liveDoc.opportunities
          : [];
        for (const o of liveItems) {
          if (!o?.id) continue;
          const id = String(o.id);
          if (PROTECTED_GOLD_SHADOW_IDS.has(id)) continue;
          if (String(o.hotelId || "") !== String(hotelId)) continue;
          const explicitHide =
            o.customerVisible === false ||
            o.campaignCustomerFacingDisabled === true ||
            o.founderCustomerVisibilityDisable === true ||
            docFacingEnabled === false;
          const stamped = {
            ...o,
            hotelId,
            localShadowOnly: true,
            localShadowPortfolio: true,
            liveCampaignV1Shadow: true,
            stageEShadowPreview: true,
            // Preserve certificationFixtureOnly from PEP/PLP zero-paid cert shadows.
            certificationFixtureOnly:
              o.certificationFixtureOnly === true ? true : undefined,
            // Local review inventory: keep researched live-campaign rows visible in the
            // founder local GDI UI unless explicitly / campaign-disabled.
            // doesNotAffectProduction remains true — this is not publication.
            customerVisible: explicitHide ? false : true,
            campaignCustomerFacingDisabled: explicitHide ? true : o.campaignCustomerFacingDisabled,
            founderCustomerVisibilityDisable:
              explicitHide ? true : o.founderCustomerVisibilityDisable,
            founderVisibilityDisableReason:
              explicitHide
                ? o.founderVisibilityDisableReason ||
                  liveDoc.founderVisibilityDisableReason ||
                  null
                : o.founderVisibilityDisableReason || null,
            doesNotAffectProduction: true,
          };
          if (!byId.has(id)) {
            byId.set(id, stamped);
          } else if (!PROTECTED_GOLD_SHADOW_IDS.has(id)) {
            const cur = byId.get(id);
            let next = {
              ...cur,
              ...stamped,
              sources: stamped.sources?.length ? stamped.sources : cur.sources,
              commercialIntelligenceBrief:
                stamped.commercialIntelligenceBrief || cur.commercialIntelligenceBrief,
              primaryContact: stamped.primaryContact || cur.primaryContact,
              secondaryContact: stamped.secondaryContact || cur.secondaryContact,
            };
            // Internal staging / CF-off overlays must not demote founder-approved
            // selective promotions already sticky in the merge. A later selective
            // stamp may still change visibility intentionally.
            if (
              isSelectivePromotionSticky(cur) &&
              !isSelectivePromotionSticky(o) &&
              (docFacingEnabled === false || explicitHide)
            ) {
              next = {
                ...next,
                customerVisible: true,
                campaignCustomerFacingDisabled: false,
                founderCustomerVisibilityDisable: false,
                gdiV34SelectivePromotion: cur.gdiV34SelectivePromotion === true,
                gdiV32SelectivePromotion: cur.gdiV32SelectivePromotion === true,
                gdiV28SelectivePromotion: cur.gdiV28SelectivePromotion === true,
                gdiV29SelectivePromotion: cur.gdiV29SelectivePromotion === true,
                localSalesReadyVisibilityStamp:
                  cur.localSalesReadyVisibilityStamp ||
                  next.localSalesReadyVisibilityStamp,
              };
            }
            byId.set(id, next);
          }
        }
      }
    }
  } catch {
    /* live campaign shadow optional */
  }

  // Condesa multi-segment reclass preview — overlays researchSegment for local UI only.
  // Does not mutate live-campaign-v1-condesa-cancun-opportunities.json on disk.
  try {
    if (
      String(hotelId) === CONDESA_HOTEL_ID &&
      String(env.GDI_CONDESA_RECLASS_PREVIEW || "1").trim() !== "0" &&
      fs.existsSync(CONDESA_MULTISEGMENT_RECLASS_PREVIEW)
    ) {
      const reclassDoc = JSON.parse(
        fs.readFileSync(CONDESA_MULTISEGMENT_RECLASS_PREVIEW, "utf8"),
      );
      if (reclassDoc?.applyLocalPreview !== false) {
        for (const row of reclassDoc.reclassifications || []) {
          const id = String(row.opportunityId || "");
          if (!id || !byId.has(id)) continue;
          const cur = byId.get(id);
          const toSeg = String(row.toResearchSegment || "").trim();
          if (!toSeg) continue;
          const isEvents = toSeg === "associations_government";
          byId.set(id, {
            ...cur,
            priorResearchSegment:
              cur.priorResearchSegment ||
              cur.researchSegment ||
              cur.segment ||
              row.fromResearchSegment ||
              null,
            priorOpportunityType: cur.priorOpportunityType || cur.opportunityType || null,
            researchSegment: toSeg,
            segment: isEvents
              ? "Association / Conference"
              : cur.segment || "scaling_companies",
            segmentDisplayLabel: isEvents
              ? "Association / Conference / Events"
              : "Scaling Companies",
            // Local preview only: surface event filter/type without mutating on-disk Scaling IDs.
            opportunityType: isEvents
              ? "PRIMARY_PURSUIT"
              : cur.opportunityType || "CORPORATE_ACCOUNT_RELATIONSHIP",
            eventSubtype: row.eventSubtype ?? cur.eventSubtype ?? null,
            demandSignalType: isEvents
              ? cur.demandSignalType || "association_conference_event"
              : cur.demandSignalType,
            // Evidence-backed overview fields from reclass rationale (local preview only).
            eventDisplayName: row.eventDisplayName ?? cur.eventDisplayName ?? null,
            historicalProgrammeLabel:
              row.historicalProgrammeLabel ?? cur.historicalProgrammeLabel ?? null,
            commercialOpportunityTrack:
              row.commercialOpportunityTrack ?? cur.commercialOpportunityTrack ?? null,
            demandCycleStatus: row.demandCycleStatus ?? cur.demandCycleStatus ?? null,
            hotelSourcingStatus: row.hotelSourcingStatus ?? cur.hotelSourcingStatus ?? null,
            eventLocationCity: row.eventLocationCity ?? cur.eventLocationCity ?? cur.eventCity ?? null,
            eventLocationCountry:
              row.eventLocationCountry ?? cur.eventLocationCountry ?? cur.eventCountry ?? null,
            verifiedVenue: row.verifiedVenue ?? cur.verifiedVenue ?? null,
            incumbentVenue: row.incumbentVenue ?? cur.incumbentVenue ?? null,
            historicalEventOnly:
              row.historicalEventOnly === true
                ? true
                : row.historicalEventOnly === false
                  ? false
                  : cur.historicalEventOnly === true,
            segmentReclassificationPreview: true,
            segmentReclassificationRationale: row.rationale || null,
            dispositionPreserved:
              row.dispositionPreserved || cur.commercialDisposition || null,
            doesNotAffectProduction: true,
            localShadowPortfolio: true,
          });
        }
      }
    }
  } catch {
    /* reclass preview optional */
  }

  // Apply contact quality even when no shadow row
  for (const [id, o] of byId) {
    if (!shadowById.has(id)) byId.set(id, applyCommercialContactQualityOverlay(o));
  }

  const reviews = loadFounderReviews(hotelId);
  const reviewById = new Map((reviews.items || []).map((r) => [r.opportunityId, r]));
  const withReviews = [...byId.values()].map((o) => {
    const rev = reviewById.get(o.id);
    return rev
      ? {
          ...o,
          founderReviewDecision: rev.decision,
          founderReviewComment: rev.comment || "",
          founderReviewUpdatedAt: rev.updatedAt,
          founderReviewLocalOnly: true,
        }
      : o;
  });

  // Chapultepec Scaling: active local cohort = gold three + FullE2E five.
  // Legacy Scaling remain in the merge set (history/detail) but archived for list.
  const withScalingInventory = applyChapultepecScalingActiveLocalInventory(
    withReviews,
    hotelId,
    { env },
  );

  // Mexico two-hotel durable local archive registry (founder-approved IDs).
  // Takes precedence for customer visibility; never deletes; detail-by-id retained.
  const withArchive = applyMexicoTwoHotelLocalArchiveRegistry(
    withScalingInventory,
    hotelId,
    { env },
  );

  // Nobu London campaign visibility disable (founder rejected second-wave staging).
  // Reversible tombstone — research retained; not permanent archive.
  const withNobuCampaignDisable = applyNobuLondonCampaignVisibilityDisable(
    withArchive,
    hotelId,
    { env },
  );

  // Nobu Research-to-Drawer Intelligence V2 overlay (zero paid; reversible).
  let withV2 = withNobuCampaignDisable;
  try {
    if (
      String(hotelId) === NOBU_LONDON_HOTEL_ID &&
      fs.existsSync(NOBU_LONDON_V2_OVERLAY)
    ) {
      const v2Doc = JSON.parse(fs.readFileSync(NOBU_LONDON_V2_OVERLAY, "utf8"));
      const byId = new Map(
        (v2Doc.opportunities || []).map((row) => [String(row.id), row]),
      );
      withV2 = withNobuCampaignDisable.map((o) => {
        const row = byId.get(String(o.id));
        if (!row?.researchToDrawerV2) return o;
        return applyResearchToDrawerIntelligenceV2({
          ...o,
          researchToDrawerV2: row.researchToDrawerV2,
        });
      });
    }
  } catch {
    withV2 = withNobuCampaignDisable;
  }

  // Universal Timing Intelligence V1 — recover verified dates from saved research
  // into structured timing for tiles/DTOs (zero paid; non-destructive overlay).
  const withTiming = withV2.map((o) => applyUniversalTimingOverlay(o));

  // Waterstone contact maturation — recover named people / org routes from
  // existing evidence for Priority A/B gaps (zero paid; reversible; no Airtable).
  const withMaturation = applyWaterstoneContactMaturationOverlay(
    withTiming,
    hotelId,
    { env },
  );

  // Commercial eligibility readiness — disposition stamps + Alliance/Prevue
  // honesty corrections. Does NOT auto-hide (founder must approve ID list).
  const withEligibility = applyWaterstoneCommercialEligibilityOverlay(
    withMaturation,
    hotelId,
    { env },
  );

  // Waterstone clean-28 client pack (shippable lib/) then clean inventory stamps.
  // Ensures production share parity without depending on lean-excluded local-shadow.
  const withClean28Pack = mergeWaterstoneClean28ClientFacingPack(
    withEligibility,
    hotelId,
    { env },
  );

  // Waterstone clean sales inventory — primary list = Sales Ready only;
  // RC/Watch → internal queues; NCR → reversible local archive.
  return applyWaterstoneCleanSalesInventory(withClean28Pack, hotelId, { env });
}

export function materializeShadowProspectAsOpportunity(shadow, hotelId) {
  const contact = shadow.primaryContact || {};
  const section = mapTierToProspectSection(shadow.commercialProspectTier);
  const sources = (shadow.evidenceUrls || [])
    .map((s) => (typeof s === "string" ? { url: s } : s))
    .filter((s) => text(s?.url));
  const base = {
    id: shadow.id,
    hotelId,
    title: shadow.title || shadow.organizationName,
    organizationName: shadow.organizationName,
    segment: shadow.segment || null,
    researchSegment: /scaling/i.test(String(shadow.segment || ""))
      ? "scaling_companies"
      : /association|conference/i.test(String(shadow.segment || ""))
        ? "association_events"
        : null,
    demandProductType: shadow.demandProductType || "EVENT_GROUP_OPPORTUNITY",
    eventDateDisplay: shadow.eventDateDisplay || null,
    eventLocationSummary: shadow.eventLocationSummary || null,
    hotelOpportunityThesis: shadow.hotelOpportunityThesis || null,
    roomDemandStatus: shadow.roomDemandStatus || null,
    roomDemandRationale: shadow.roomDemandRationale || null,
    opportunityQualification: shadow.opportunityQualification || "WATCHLIST",
    priority: "WATCHLIST",
    primaryContact: {
      name: contact.name || null,
      fullName: contact.name || null,
      title: contact.title || null,
      role: contact.title || null,
      email: contact.email || null,
      phone: contact.phone || null,
      organization: contact.organization || shadow.organizationName,
      source: contact.evidence || contact.url || null,
      sourceUrl: contact.evidence || contact.url || null,
      url: contact.url || contact.evidence || null,
    },
    sources,
    evidenceUrls: sources,
    summaryWhat: shadow.hotelOpportunityThesis || shadow.title,
    summaryWhyHotel: shadow.hotelOpportunityThesis || null,
    summaryWhyMatters: shadow.roomDemandRationale || shadow.hotelOpportunityThesis || null,
    recommendedAction: shadow.salesActionRecommended || null,
    commercialProspectTier: shadow.commercialProspectTier,
    prospectReviewSection: section,
    dosGrade: shadow.dosGrade || null,
    localShadowOnly: true,
    localShadowPortfolio: true,
    customerVisible: true,
    isTestData: false,
    language: shadow.language || null,
  };
  return overlayShadowFields(base, shadow);
}

/**
 * Persist a merged opportunity set to the local FS shadow store (no Airtable).
 */
export function writeLocalShadowOpportunityStore(hotelId, opportunities, meta = {}) {
  if (!isLocalShadowCommercialHotel(hotelId)) {
    const err = new Error("hotel_not_in_local_shadow_allowlist");
    err.code = "local_shadow_hotel_forbidden";
    throw err;
  }
  saveOpportunities(hotelId, {
    hotelId,
    opportunities,
    updatedAt: new Date().toISOString(),
    runId: meta.runId || null,
    localShadowCommercialPortfolio: true,
    sourceNote:
      meta.sourceNote ||
      "Local shadow commercial portfolio — not production / not customer publication",
  });
  return loadOpportunities(hotelId);
}

export function founderReviewAllowedDecisions() {
  return [...FOUNDER_REVIEW_DECISIONS];
}

export function localShadowEnabledInProcess(env = process.env) {
  return (
    isGroupDemandIntelligenceLocalDev(env) ||
    String(env.GDI_LOCAL_SHADOW_COMMERCIAL || "1").trim() !== "0"
  );
}
