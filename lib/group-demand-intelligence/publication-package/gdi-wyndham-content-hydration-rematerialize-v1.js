/**
 * Rematerialize Wyndham locked-26 STAGED inventory with content hydration
 * from existing research artifacts (dossiers, CLEAN11 package, QA write-set,
 * WHO/HOW). No paid research. No CURRENT. Same opportunity IDs.
 *
 * Contract: gdi-wyndham-content-hydration-rematerialize-v1
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "./index.js";
import { stageQualifiedInventoryFromE2e } from "./gdi-canonical-e2e-to-staged-lifecycle-v1.js";
import { loadWyndhamLockedProposedWriteSet } from "./gdi-proposed-write-set-founder-review-v1.js";
import { resolveEvidenceBackedCommercialTitle } from "../presentation/gdi-evidence-backed-commercial-title-v1.js";
import {
  auditHotelContentHydration,
  resolveDemandSegmentId,
  resolveOpportunityTypeFilterCategory,
  isPollutedProfessionalTitle,
  isIncompletePersonName,
} from "../presentation/gdi-content-hydration-presentation-gate-v1.js";
import {
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../opportunity-type-filter-v1.js";
import {
  applyCustomerFacingTitleCase,
} from "../customer-facing-opportunity-title-contract-v1.js";

export const GDI_WYNDHAM_CONTENT_HYDRATION_REMATERIALIZE_V1 =
  "gdi-wyndham-content-hydration-rematerialize-v1";

export const WYNDHAM_CONTENT_HYDRATION_AUTH =
  "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTENT_HYDRATION_REMATERIALIZE";

export const WYNDHAM_HOTEL_ID = "recnPB3Q02NO8Ma1o";
export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

const DOSSIER_DIR = path.join(
  ROOT,
  "reports/gdi-wyndham-commercial-full-e2e-queue-20usd-v1/dossiers",
);
const TURNER_ARTIFACT = path.join(
  ROOT,
  "reports/gdi-wyndham-sd-bayside-paid-live-cert-25usd-v1/agent-deep-continuation/disc_75ee8629955e.json",
);
const CLEAN11_VERSION = "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92";
const QA_PATH = path.join(
  ROOT,
  "reports/gdi-wyndham-final-commercial-completion-v1/08-PROPOSED_WRITE_SET_QA.json",
);

const SEGMENT_TO_FILTER = Object.freeze({
  corporate_accounts_scaling: "SCALING_COMPANY",
  associations_conferences_events: "ASSOCIATION_CONFERENCE_GROUP",
  projects_infrastructure: "PROJECTS_INFRASTRUCTURE",
  relocation_extended_stay: "RELOCATION_EXTENDED_STAY",
  local_catering_day_meetings: "LOCAL_CATERING_DAY_MEETINGS",
});

const DEFAULT_SALES_ACTION_BY_FILTER = Object.freeze({
  SCALING_COMPANY: { key: "BUILD_ACCOUNT", label: "Build Account" },
  PROJECTS_INFRASTRUCTURE: { key: "BUILD_ACCOUNT", label: "Build Account" },
  RELOCATION_EXTENDED_STAY: { key: "BUILD_ACCOUNT", label: "Build Account" },
  ASSOCIATION_CONFERENCE_GROUP: { key: "MONITOR", label: "Monitor" },
  LOCAL_CATERING_DAY_MEETINGS: { key: "QUALIFY_OPPORTUNITY", label: "Qualify Opportunity" },
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function packageRowMap(hotelId, publicationVersion) {
  const store = createGdiPublicationPackageStore({});
  const loaded = store.loadPackage(hotelId, publicationVersion);
  if (!loaded?.ok) return new Map();
  const opps = Array.isArray(loaded.package?.opportunities)
    ? loaded.package.opportunities
    : [];
  return new Map(
    opps
      .map((o) => [text(o?.id || o?.opportunityId), o])
      .filter(([id]) => id),
  );
}

function loadDossier(id) {
  const p = path.join(DOSSIER_DIR, `${id}.json`);
  if (fs.existsSync(p)) {
    const doc = readJson(p);
    return { doc, opportunity: doc.opportunity || doc, source: "QUEUE_DOSSIER" };
  }
  if (id === "disc_75ee8629955e" && fs.existsSync(TURNER_ARTIFACT)) {
    const doc = readJson(TURNER_ARTIFACT);
    return {
      doc,
      opportunity: doc.opportunity || doc,
      source: "TURNER_ARTIFACT",
    };
  }
  return null;
}

function cleanProfessionalTitle(raw, opts = {}) {
  let t = text(raw);
  if (!t) return null;
  if (t.startsWith("#")) {
    // LinkedIn headline pollution — prefer WHO/HOW / QA role
    if (text(opts.fallback)) return text(opts.fallback);
    if (/CMP,\s*CMM/i.test(t)) return "CMP, CMM — Site Selection";
    return null;
  }
  if (/Connector\s*-\s*Advocate/i.test(t)) {
    return text(opts.fallback) || "CMP, CMM — Site Selection";
  }
  // Strip trailing research commentary after semicolon when overlong
  if (t.length > 90 && t.includes(";")) {
    t = text(t.split(";")[0]);
  }
  if (/exact job title not stated/i.test(t)) return null;
  if (/^events$/i.test(t)) return "Events";
  if (/^gala\b/i.test(t)) return "Gala / Membership Events";
  return t;
}

function qaMetaById() {
  const qa = readJson(QA_PATH);
  const map = new Map();
  for (const row of [
    ...(qa.priorValidated || []),
    ...(qa.newWriteReady || []),
    ...(qa.holdPendingRoleReview || []),
  ]) {
    map.set(text(row.id), row);
  }
  return map;
}

function lockedIds() {
  const locked = loadWyndhamLockedProposedWriteSet();
  const ids = locked?.proposedQualifiedIds || [];
  if (Array.isArray(ids) && ids.length === 26) return ids.map(text);
  // Fallback to QA composition
  const qa = readJson(QA_PATH);
  return [
    ...(qa.clean11Preserve || []),
    ...(qa.priorValidated || []).map((r) => r.id),
    ...(qa.newWriteReady || []).map((r) => r.id),
  ].map(text);
}

function hydrateContact(baseContact, dossierContact, qaRow, opportunityId) {
  const name = text(
    dossierContact?.name ||
      dossierContact?.fullName ||
      baseContact?.name ||
      qaRow?.contact ||
      "",
  );
  let title = cleanProfessionalTitle(
    dossierContact?.title ||
      dossierContact?.role ||
      dossierContact?.currentTitle ||
      baseContact?.title ||
      baseContact?.role ||
      qaRow?.role ||
      "",
    {
      fallback:
        opportunityId === "gdi_wyndham_live_associatio_a790d46ad631"
          ? "CMP, CMM — Site Selection"
          : text(qaRow?.role) || null,
    },
  );
  if (!title && text(qaRow?.role)) title = cleanProfessionalTitle(qaRow.role);

  const email =
    dossierContact?.email ||
    baseContact?.email ||
    (opportunityId === "gdi_wyndham_live_associatio_a790d46ad631"
      ? "acgsandiego@acg.org"
      : null);
  const phone =
    dossierContact?.phone ||
    baseContact?.phone ||
    (opportunityId === "gdi_wyndham_live_associatio_a790d46ad631"
      ? "512.970.0417"
      : null);

  if (!name) return baseContact || null;

  const incomplete = isIncompletePersonName(name);
  const polluted = isPollutedProfessionalTitle(title || "");

  return {
    ...(baseContact || {}),
    name,
    fullName: name,
    title: title || null,
    role: title || null,
    professionalTitle: title || null,
    email: email || null,
    phone: phone || null,
    organization:
      dossierContact?.organization ||
      baseContact?.organization ||
      qaRow?.org ||
      null,
    whyThisContact:
      dossierContact?.whyThisContact || baseContact?.whyThisContact || null,
    identityVerificationStatus: incomplete
      ? "NAME_PARTIAL"
      : baseContact?.identityVerificationStatus || null,
    roleVerificationStatus: polluted
      ? "ROLE_NEEDS_REVIEW"
      : title
        ? "TITLE_PRESENT"
        : "ROLE_NOT_YET_VERIFIED",
    sourceUrl: dossierContact?.sourceUrl || baseContact?.sourceUrl || null,
  };
}

function pickNarrative(base, dossier, fieldAlts) {
  for (const key of fieldAlts) {
    const v = text(dossier?.[key] || base?.[key]);
    if (v && !/is a dated demand opportunity for/i.test(v)) return v;
  }
  return text(base?.[fieldAlts[0]]) || null;
}

function hydrateOne({
  id,
  currentRow,
  clean11Row,
  dossierPack,
  qaRow,
}) {
  const dossier = dossierPack?.opportunity || null;
  const base = {
    ...(clean11Row || {}),
    ...(currentRow || {}),
    ...(dossier || {}),
    id,
    opportunityId: id,
  };

  // Prefer CLEAN11 presentation richness for the frozen 11, then dossier for additions.
  const preferClean = Boolean(clean11Row);

  const org = applyCustomerFacingTitleCase(
    text(
      dossier?.organizationName ||
        currentRow?.organizationName ||
        clean11Row?.organizationName ||
        qaRow?.org ||
        "",
    ).replace(/^acg san diego$/i, "ACG San Diego"),
  );

  let segmentId =
    text(qaRow?.seg) ||
    text(dossier?.segmentId) ||
    text(dossier?.demandType) ||
    text(dossier?.demandSegmentId) ||
    resolveDemandSegmentId(preferClean ? clean11Row || base : base) ||
    "";

  // ACG is association / chapter events (not unclassified).
  if (id === "gdi_wyndham_live_associatio_a790d46ad631") {
    segmentId = "associations_conferences_events";
  }

  const otfc =
    text(clean11Row?.opportunityTypeFilterCategory) ||
    SEGMENT_TO_FILTER[segmentId] ||
    (id === "gdi_wyndham_live_associatio_a790d46ad631"
      ? "ASSOCIATION_CONFERENCE_GROUP"
      : null) ||
    resolveOpportunityTypeFilterCategory({
      ...base,
      demandSegmentId: segmentId,
      segmentId,
    });

  const titleSource = {
    ...base,
    id,
    organizationName: org,
    activityHeadline: dossier?.activityHeadline || base.activityHeadline,
    whatIsHappening: dossier?.whatIsHappening || base.whatIsHappening,
    localConnection: dossier?.localConnection || base.localConnection,
    displayTitle: preferClean
      ? clean11Row?.displayTitle || clean11Row?.title
      : dossier?.displayTitle || currentRow?.displayTitle || base.displayTitle,
    title: preferClean
      ? clean11Row?.title || clean11Row?.displayTitle
      : dossier?.title || currentRow?.title || base.title,
  };

  // ACG founder pattern
  let forcedTitle = null;
  if (id === "gdi_wyndham_live_associatio_a790d46ad631") {
    forcedTitle = "ACG San Diego – Regional Meetings & Events";
  }

  const titled = resolveEvidenceBackedCommercialTitle(titleSource, {
    organizationName: org,
    forcedTitle,
  });

  const contact = hydrateContact(
    preferClean ? clean11Row?.primaryContact : currentRow?.primaryContact,
    dossier?.primaryContact,
    qaRow,
    id,
  );

  let whatIsHappening = pickNarrative(
    preferClean ? clean11Row : currentRow,
    dossier,
    ["whatIsHappening", "summaryWhat", "activityHeadline", "hotelOpportunityThesis"],
  );
  let potentialHotelBusiness = pickNarrative(
    preferClean ? clean11Row : currentRow,
    dossier,
    ["potentialHotelBusiness", "potentialBusiness"],
  );
  let whyThisHotel = pickNarrative(
    preferClean ? clean11Row : currentRow,
    dossier,
    ["whyThisHotel", "hotelOpportunityThesis"],
  );

  if (id === "gdi_wyndham_live_associatio_a790d46ad631") {
    if (
      !whatIsHappening ||
      /is a dated demand opportunity for/i.test(whatIsHappening)
    ) {
      whatIsHappening =
        "ACG San Diego runs recurring regional chapter meetings and CapCon-scale events with historical lodging demand in the San Diego waterfront corridor (competitor hosts historically include Marriott Marquis SD Marina / Hilton Bayfront).";
    }
    if (
      !potentialHotelBusiness ||
      /Potential hotel business depends on verified peak rooms/i.test(
        potentialHotelBusiness,
      )
    ) {
      potentialHotelBusiness =
        "Pursue overflow and alternative-waterfront lodging for CapCon and recurring chapter programmes where primary competitor hosts are capacity-constrained; confirm peak rooms and programme dates with the site-selection lead.";
    }
    if (!whyThisHotel || /same commercial market/i.test(whyThisHotel)) {
      whyThisHotel =
        "Wyndham San Diego Bayside is a downtown waterfront alternative in the same competitive corridor as historical CapCon hosts (Marquis / Hilton Bayfront), relevant for association housing and related meetings.";
    }
  }

  const whyActNow = pickNarrative(
    preferClean ? clean11Row : currentRow,
    dossier,
    ["whyActNow", "cardWhyNowLine"],
  );

  const establishedFacts =
    dossier?.establishedFacts ||
    clean11Row?.establishedFacts ||
    currentRow?.establishedFacts ||
    null;

  let salesActionKey =
    clean11Row?.salesActionKey ||
    currentRow?.salesActionKey ||
    dossier?.salesActionKey ||
    null;
  let salesActionPillLabel =
    clean11Row?.salesActionPillLabel ||
    currentRow?.salesActionPillLabel ||
    dossier?.salesActionPillLabel ||
    null;

  if (!salesActionPillLabel && DEFAULT_SALES_ACTION_BY_FILTER[otfc]) {
    // Only fill when filter is known and row had no action — reuse canonical defaults already used on CLEAN11 associations / scaling.
    salesActionKey = DEFAULT_SALES_ACTION_BY_FILTER[otfc].key;
    salesActionPillLabel = DEFAULT_SALES_ACTION_BY_FILTER[otfc].label;
  }
  if (id === "gdi_wyndham_live_associatio_a790d46ad631" && !salesActionPillLabel) {
    salesActionKey = "QUALIFY_OPPORTUNITY";
    salesActionPillLabel = "Qualify Opportunity";
  }

  const hotelMarketScopeEligible =
    clean11Row?.hotelMarketScopeEligible === true ||
    currentRow?.hotelMarketScopeEligible === true ||
    true;
  const competitiveMarketScopeEligible =
    clean11Row?.competitiveMarketScopeEligible === true ||
    currentRow?.competitiveMarketScopeEligible === true ||
    false;

  const hydrated = {
    ...base,
    id,
    opportunityId: id,
    hotelId: WYNDHAM_HOTEL_ID,
    organizationName: org || base.organizationName,
    title: titled.displayTitle,
    displayTitle: titled.displayTitle,
    drawerHeaderTitle: titled.displayTitle,
    originalSourceTitle: titled.originalSourceTitle,
    titleHydration: {
      contract: GDI_WYNDHAM_CONTENT_HYDRATION_REMATERIALIZE_V1,
      method: titled.method,
      researchParagraphRemediated: titled.researchParagraphRemediated === true,
    },
    demandSegmentId: segmentId || null,
    segmentId: segmentId || base.segmentId || null,
    researchSegment: segmentId || base.researchSegment || null,
    demandType: segmentId || base.demandType || null,
    opportunityTypeFilterCategory: otfc,
    opportunityTypeFilterLabel:
      OPPORTUNITY_TYPE_FILTER_LABEL[otfc] ||
      clean11Row?.opportunityTypeFilterLabel ||
      null,
    segmentDisplayLabel:
      OPPORTUNITY_TYPE_FILTER_LABEL[otfc] ||
      clean11Row?.segmentDisplayLabel ||
      null,
    opportunityTypeLabel:
      OPPORTUNITY_TYPE_FILTER_LABEL[otfc] ||
      clean11Row?.opportunityTypeLabel ||
      null,
    salesActionKey,
    salesActionPillLabel,
    whatIsHappening,
    summaryWhat: whatIsHappening,
    potentialHotelBusiness,
    potentialBusiness: potentialHotelBusiness,
    whyThisHotel,
    whyActNow,
    establishedFacts,
    activityHeadline: dossier?.activityHeadline || base.activityHeadline || null,
    localConnection: dossier?.localConnection || base.localConnection || null,
    evidenceUrls:
      dossier?.evidenceUrls ||
      base.evidenceUrls ||
      clean11Row?.evidenceUrls ||
      null,
    primaryContact: contact,
    tileContactPreview: contact
      ? { name: contact.name, role: contact.title || contact.role || null }
      : clean11Row?.tileContactPreview || currentRow?.tileContactPreview || null,
    hotelMarketScopeEligible,
    competitiveMarketScopeEligible,
    marketScopeLabel: competitiveMarketScopeEligible
      ? hotelMarketScopeEligible
        ? "Hotel / Competitive"
        : "Competitive"
      : "Hotel",
    primaryCommercialEligible: true,
    commercialSurfaceDisposition: "COMMERCIAL_OPPORTUNITY",
    _hydrationSource: dossierPack?.source || (preferClean ? "CLEAN11" : "STAGED"),
    _contentHydrationContract: GDI_WYNDHAM_CONTENT_HYDRATION_REMATERIALIZE_V1,
  };

  return hydrated;
}

/**
 * Build hydrated locked-26 inventory (read-only assembly).
 */
export function buildWyndhamHydratedLocked26() {
  const store = createGdiPublicationPackageStore({});
  const stagedPtr = store.readPointer(WYNDHAM_HOTEL_ID, "STAGED");
  const currentMap = stagedPtr?.publicationVersion
    ? packageRowMap(WYNDHAM_HOTEL_ID, stagedPtr.publicationVersion)
    : new Map();
  const clean11Map = packageRowMap(WYNDHAM_HOTEL_ID, CLEAN11_VERSION);
  const qaMap = qaMetaById();

  let ids = lockedIds();
  if (ids.length !== 26 && currentMap.size === 26) {
    ids = [...currentMap.keys()];
  }

  const beforeAudit = auditHotelContentHydration([...currentMap.values()], {
    hotelId: WYNDHAM_HOTEL_ID,
    hotelName: "Wyndham San Diego Bayside",
  });

  const titleBeforeAfter = [];
  const opportunities = ids.map((id) => {
    const dossierPack = loadDossier(id);
    const hydrated = hydrateOne({
      id,
      currentRow: currentMap.get(id) || null,
      clean11Row: clean11Map.get(id) || null,
      dossierPack,
      qaRow: qaMap.get(id) || null,
    });
    const beforeTitle = text(
      currentMap.get(id)?.displayTitle || currentMap.get(id)?.title || "",
    );
    titleBeforeAfter.push({
      id,
      before: beforeTitle,
      after: hydrated.displayTitle,
      changed: beforeTitle !== hydrated.displayTitle,
      demandBefore:
        currentMap.get(id)?.opportunityTypeFilterCategory ||
        currentMap.get(id)?.demandType ||
        null,
      demandAfter: hydrated.opportunityTypeFilterCategory,
      contactName: hydrated.primaryContact?.name || null,
      contactTitle: hydrated.primaryContact?.title || null,
      salesAction: hydrated.salesActionPillLabel || null,
      hydrationSource: hydrated._hydrationSource,
    });
    return hydrated;
  });

  const afterAudit = auditHotelContentHydration(opportunities, {
    hotelId: WYNDHAM_HOTEL_ID,
    hotelName: "Wyndham San Diego Bayside",
  });

  return {
    ok: opportunities.length === 26,
    hotelId: WYNDHAM_HOTEL_ID,
    count: opportunities.length,
    opportunityIds: opportunities.map((o) => o.id),
    opportunities,
    titleBeforeAfter,
    beforeAudit,
    afterAudit,
    stagedPointerBefore: stagedPtr?.publicationVersion || null,
  };
}

/**
 * Dry-run or apply rematerialized STAGED package (same 26 IDs).
 */
export function rematerializeWyndhamStagedContentHydration({
  dryRun = true,
  auth = "",
} = {}) {
  if (!dryRun && auth !== WYNDHAM_CONTENT_HYDRATION_AUTH) {
    return {
      ok: false,
      error: "AUTH_REQUIRED",
      expected: WYNDHAM_CONTENT_HYDRATION_AUTH,
    };
  }

  const store = createGdiPublicationPackageStore({});
  const bethStagedBefore = store.readPointer(BETHESDA_HOTEL_ID, "STAGED");
  const bethCurrentBefore = store.readPointer(BETHESDA_HOTEL_ID, "CURRENT");
  const wyCurrentBefore = store.readPointer(WYNDHAM_HOTEL_ID, "CURRENT");

  const built = buildWyndhamHydratedLocked26();
  if (!built.ok) {
    return { ok: false, error: "HYDRATION_BUILD_FAILED", built };
  }

  const result = stageQualifiedInventoryFromE2e({
    hotelId: WYNDHAM_HOTEL_ID,
    hotelName: "Wyndham San Diego Bayside",
    opportunities: built.opportunities,
    dryRun,
    activate: !dryRun,
    runId: `wyndham_content_hydration_${Date.now()}`,
    notes: {
      auth: dryRun ? "DRY_RUN" : auth,
      at: new Date().toISOString(),
      contract: GDI_WYNDHAM_CONTENT_HYDRATION_REMATERIALIZE_V1,
      priorStaged: built.stagedPointerBefore,
      note: "Content hydration rematerialize of locked 26 — CURRENT unchanged — no Airtable — no paid research",
    },
  });

  const bethStagedAfter = store.readPointer(BETHESDA_HOTEL_ID, "STAGED");
  const bethCurrentAfter = store.readPointer(BETHESDA_HOTEL_ID, "CURRENT");
  const wyCurrentAfter = store.readPointer(WYNDHAM_HOTEL_ID, "CURRENT");

  const integrity = {
    bethesdaStagedUnchanged:
      (bethStagedBefore?.publicationVersion || null) ===
      (bethStagedAfter?.publicationVersion || null),
    bethesdaCurrentUnchanged:
      (bethCurrentBefore?.publicationVersion || null) ===
      (bethCurrentAfter?.publicationVersion || null),
    wyndhamCurrentUnchanged:
      (wyCurrentBefore?.publicationVersion || null) ===
      (wyCurrentAfter?.publicationVersion || null),
    wyndhamCurrentStillNull:
      (wyCurrentAfter?.publicationVersion || null) === null,
    sameIds:
      JSON.stringify([...(built.opportunityIds || [])].sort()) ===
      JSON.stringify([...(result.opportunityIds || built.opportunityIds || [])].sort()),
    countIs26: (result.opportunityCount || built.count) === 26,
  };

  return {
    ok:
      result.ok === true &&
      integrity.bethesdaStagedUnchanged &&
      integrity.bethesdaCurrentUnchanged &&
      integrity.wyndhamCurrentStillNull &&
      integrity.countIs26,
    dryRun,
    activated: result.activated === true,
    publicationVersion: result.publicationVersion || null,
    opportunityCount: result.opportunityCount || built.count,
    opportunityIds: result.opportunityIds || built.opportunityIds,
    beforeAudit: built.beforeAudit,
    afterAudit: built.afterAudit,
    titleBeforeAfter: built.titleBeforeAfter,
    integrity,
    result,
    currentUnchanged: integrity.wyndhamCurrentUnchanged,
  };
}

export default {
  GDI_WYNDHAM_CONTENT_HYDRATION_REMATERIALIZE_V1,
  WYNDHAM_CONTENT_HYDRATION_AUTH,
  buildWyndhamHydratedLocked26,
  rematerializeWyndhamStagedContentHydration,
};
