/**
 * GDI Customer Publication Package V1 — immutable approved customer-facing report.
 *
 * The package stores final approved customer content (DTO-shaped), not raw
 * research that production reinterprets independently.
 *
 * Local preview and production share must resolve to the same package version.
 */

import crypto from "node:crypto";
import {
  GDI_PUBLICATION_LIFECYCLE_STATE,
  GDI_PUBLICATION_LIFECYCLE_V1,
} from "./gdi-publication-lifecycle-v1.js";

export const GDI_CUSTOMER_PUBLICATION_PACKAGE_V1 =
  "gdi-customer-publication-package-v1";

export const GDI_PUBLICATION_PRESENTATION_SCHEMA_V1 =
  "gdi-publication-presentation-schema-v1";

/** Mutable operational fields excluded from immutable content fingerprint. */
export const FINGERPRINT_EXCLUDED_OPPORTUNITY_KEYS = Object.freeze([
  "shareValidation",
  "hotelValidation",
  "familiarityStatus",
  "commercialValue",
  "contactPersonAssessment",
  "emailAssessment",
  "phoneAssessment",
  "validatedAt",
  "hotelValidationUpdatedAt",
  "salesActionTaken",
  "salesOutcome",
  "actionTakenAt",
  "feedbackUpdatedAt",
  "requestTimestamp",
  "servedAt",
  "fetchedAt",
]);

/** Fields compared for content parity / frozen into package opportunities. */
export const PUBLICATION_CONTENT_FIELDS = Object.freeze([
  "id",
  "displayTitle",
  "title",
  "organizationName",
  "displayCompanyName",
  "whatIsHappening",
  "potentialHotelBusiness",
  "potentialBusiness",
  "whyThisHotel",
  "whyActNow",
  "recommendedNextStep",
  "recommendedAction",
  "recommendedNextAction",
  "hotelSourcingStatusLabel",
  "tileClassificationLine",
  "salesActionKey",
  "salesActionPillLabel",
  "priority",
  "priorityPillLabel",
  "commercialCategory",
  "commercialStageKey",
  "commercialStagePillLabel",
  "inventoryPopulations",
  "inventoryPopulationPrimary",
  "hotelMarketScopeEligible",
  "competitiveMarketScopeEligible",
  "competitiveStatus",
  "competitiveStatusLabel",
  "cmiVerified",
  "eventDateDisplay",
  "tileEventStartDate",
  "tileEventEndDate",
  "datePillDisplayLabel",
  "cardWhyNowLine",
  "summary",
  "summaryWhat",
  "establishedFacts",
  "uncertainties",
  "qualificationQuestions",
  "primaryContact",
  "secondaryContact",
  "sources",
  "drawerHeaderTitle",
  "segmentDisplayLabel",
  "opportunityType",
  "opportunityTypeLabel",
  "opportunityTypeFilterLabel",
  "opportunityTypeFilterCategory",
  "demandFamily",
  "demandFamilyLabel",
  // Drawer / overview presentation — must not be live-recomputed after approval.
  "eventsOpportunityOverview",
  "eventsDrawerModel",
  "sharedTenSectionDrawer",
  "commercialIntelligenceBrief",
  "associationEventsGoldBrief",
  "salesBrief",
  "watchCardCurrentStatus",
  "currentStatus",
  "statusLabel",
  // Hotel Need-Date Match (frozen at package build; timing ≠ open room block).
  "needDateMatchV1",
  "needDateIndicator",
  "tileNeedDatePreview",
  "needDateAssociations",
  "needDateRelevanceClass",
  "hotelPreferenceMatchLine",
  "hotelPreferenceMatchExplanations",
  "salesOpportunityQuickFilterKeys",
  // Local-master freeze fields (customer-facing presentation inputs)
  "tileContactPreview",
  "displayRole",
  "opportunityRole",
  "primaryContactName",
  "contactName",
  "contactRole",
  "revenueStreamClass",
  "revenueStreamIcons",
  "tileBadgePlan",
  "canonicalCustomerPresentation",
  "canonicalCustomerPresentationVersion",
  "showSalesActionPill",
  "priorityFilterKey",
  "showPriorityPill",
]);

function text(v) {
  if (v == null) return "";
  return String(v).replace(/\s+/g, " ").trim();
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value === undefined ? null : value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((v) => stableStringify(v)).join(",")}]`;
  }
  const keys = Object.keys(value).sort();
  return `{${keys
    .map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`)
    .join(",")}}`;
}

function sha256Utf8(s) {
  return crypto.createHash("sha256").update(String(s), "utf8").digest("hex");
}

function pickContentFields(opp = {}) {
  const out = {};
  for (const k of PUBLICATION_CONTENT_FIELDS) {
    if (k in opp) out[k] = opp[k];
  }
  // Always pin identity.
  out.id = text(opp.id);
  return out;
}

function stripExcluded(opp = {}) {
  const out = { ...opp };
  for (const k of FINGERPRINT_EXCLUDED_OPPORTUNITY_KEYS) {
    delete out[k];
  }
  return out;
}

function normalizePopulation(v) {
  return text(v).toUpperCase();
}

/**
 * Compute Market Scope membership counts from frozen opportunities.
 * Overlap is intentional (Hotel ∩ Competitive).
 */
export function computePublicationInventoryCounts(opportunities = []) {
  const ids = opportunities.map((o) => text(o.id)).filter(Boolean);
  const hotel = [];
  const competitive = [];
  for (const o of opportunities) {
    const id = text(o.id);
    if (!id) continue;
    const pops = Array.isArray(o.inventoryPopulations)
      ? o.inventoryPopulations.map(normalizePopulation)
      : [];
    const hotelEligible =
      o.hotelMarketScopeEligible === true ||
      pops.includes("SALES_OPPORTUNITIES") ||
      pops.includes("HOTEL");
    const competitiveEligible =
      o.competitiveMarketScopeEligible === true ||
      pops.includes("COMPETITIVE_MARKET_INTELLIGENCE") ||
      pops.includes("COMPETITIVE");
    if (hotelEligible) hotel.push(id);
    if (competitiveEligible) competitive.push(id);
  }
  return {
    all: ids.length,
    hotel: hotel.length,
    competitive: competitive.length,
    allIds: ids.slice().sort(),
    hotelIds: hotel.slice().sort(),
    competitiveIds: competitive.slice().sort(),
  };
}

/**
 * Deterministic content fingerprint over canonical serialized publication body.
 * Round-trip through JSON so undefined keys match durable on-disk packages
 * (JSON.stringify drops undefined; in-memory objects must not hash differently).
 */
export function fingerprintPublicationContent(body = {}) {
  const canonical = {
    schema: GDI_CUSTOMER_PUBLICATION_PACKAGE_V1,
    hotelId: text(body.hotelId),
    publicationVersion: text(body.publicationVersion),
    presentationSchemaVersion:
      text(body.presentationSchemaVersion) || GDI_PUBLICATION_PRESENTATION_SCHEMA_V1,
    applicationCompatibilityVersion: text(body.applicationCompatibilityVersion),
    sortOrder: Array.isArray(body.sortOrder) ? body.sortOrder.map(String) : [],
    opportunities: (Array.isArray(body.opportunities) ? body.opportunities : [])
      .map((o) => pickContentFields(stripExcluded(o)))
      .sort((a, b) => String(a.id).localeCompare(String(b.id))),
  };
  const durable = JSON.parse(JSON.stringify(canonical));
  const serialized = stableStringify(durable);
  return {
    contentFingerprintSha256: sha256Utf8(serialized),
    canonicalSerializedByteLength: Buffer.byteLength(serialized, "utf8"),
    opportunityCount: durable.opportunities.length,
  };
}

/**
 * Build an immutable customer publication package from approved DTOs.
 */
export function buildCustomerPublicationPackage(input = {}) {
  const hotelId = text(input.hotelId);
  const hotelName = text(input.hotelName) || null;
  const opportunitiesIn = Array.isArray(input.opportunities)
    ? input.opportunities
    : [];
  if (!hotelId) {
    throw new Error("buildCustomerPublicationPackage: hotelId required");
  }
  if (!opportunitiesIn.length) {
    throw new Error("buildCustomerPublicationPackage: opportunities required");
  }

  const sortOrder = Array.isArray(input.sortOrder)
    ? input.sortOrder.map(String)
    : opportunitiesIn.map((o) => text(o.id)).filter(Boolean);

  const byId = new Map();
  for (const o of opportunitiesIn) {
    const id = text(o?.id);
    if (!id) continue;
    if (byId.has(id)) {
      throw new Error(`buildCustomerPublicationPackage: duplicate opportunity id ${id}`);
    }
    byId.set(id, pickContentFields(stripExcluded(o)));
  }

  const ordered = [];
  for (const id of sortOrder) {
    const row = byId.get(id);
    if (!row) {
      throw new Error(`buildCustomerPublicationPackage: sortOrder id missing ${id}`);
    }
    ordered.push(row);
    byId.delete(id);
  }
  // Append any remaining in stable id order (should be empty when sortOrder complete).
  for (const id of [...byId.keys()].sort()) {
    ordered.push(byId.get(id));
  }

  const inventory = computePublicationInventoryCounts(ordered);
  const publicationVersion =
    text(input.publicationVersion) ||
    `gdi_pub_${hotelId}_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}_v1`;
  const publicationId =
    text(input.publicationId) ||
    `gdi_pubpkg_${hotelId}_${sha256Utf8(`${hotelId}|${publicationVersion}`).slice(0, 16)}`;

  const presentationSchemaVersion =
    text(input.presentationSchemaVersion) || GDI_PUBLICATION_PRESENTATION_SCHEMA_V1;
  const applicationCompatibilityVersion =
    text(input.applicationCompatibilityVersion) ||
    text(input.appBuildId) ||
    "gdi-customer-app-compat-v1";

  const fp = fingerprintPublicationContent({
    hotelId,
    publicationVersion,
    presentationSchemaVersion,
    applicationCompatibilityVersion,
    sortOrder: ordered.map((o) => o.id),
    opportunities: ordered,
  });

  const now = text(input.createdAt) || new Date().toISOString();
  const lifecycleState =
    text(input.lifecycleState) || GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED;

  return {
    schema: GDI_CUSTOMER_PUBLICATION_PACKAGE_V1,
    lifecycleSchema: GDI_PUBLICATION_LIFECYCLE_V1,
    hotelId,
    hotelName,
    publicationId,
    publicationVersion,
    presentationSchemaVersion,
    applicationCompatibilityVersion,
    lifecycleState,
    createdAt: now,
    approvedAt: input.approvedAt || null,
    approvalStatus: input.approvalStatus || "CANDIDATE",
    contentFingerprintSha256: fp.contentFingerprintSha256,
    opportunityCount: inventory.all,
    inventoryCounts: {
      all: inventory.all,
      hotel: inventory.hotel,
      competitive: inventory.competitive,
    },
    inventoryIds: {
      all: inventory.allIds,
      hotel: inventory.hotelIds,
      competitive: inventory.competitiveIds,
    },
    sortOrder: ordered.map((o) => o.id),
    opportunities: ordered,
    // Feedback is intentionally absent — operational and mutable.
    feedbackSeparation: {
      policy: "FEEDBACK_NOT_IN_IMMUTABLE_PACKAGE",
      note: "Hotel Validation / Action / Outcome remain in share validation store",
    },
    gates: input.gates || {},
    notes: input.notes || null,
  };
}

/**
 * Verify package fingerprint matches body (fail closed).
 */
export function verifyPublicationPackageIntegrity(pkg = {}) {
  const fails = [];
  if (pkg?.schema !== GDI_CUSTOMER_PUBLICATION_PACKAGE_V1) {
    fails.push("schema:mismatch");
  }
  if (!text(pkg.hotelId)) fails.push("hotelId:missing");
  if (!text(pkg.publicationId)) fails.push("publicationId:missing");
  if (!text(pkg.publicationVersion)) fails.push("publicationVersion:missing");
  if (!Array.isArray(pkg.opportunities) || !pkg.opportunities.length) {
    fails.push("opportunities:empty");
  }
  const fp = fingerprintPublicationContent({
    hotelId: pkg.hotelId,
    publicationVersion: pkg.publicationVersion,
    presentationSchemaVersion: pkg.presentationSchemaVersion,
    applicationCompatibilityVersion: pkg.applicationCompatibilityVersion,
    sortOrder: pkg.sortOrder,
    opportunities: pkg.opportunities,
  });
  if (
    text(pkg.contentFingerprintSha256).toLowerCase() !==
    fp.contentFingerprintSha256.toLowerCase()
  ) {
    fails.push("contentFingerprintSha256:mismatch");
  }
  const counts = computePublicationInventoryCounts(pkg.opportunities || []);
  if (Number(pkg.opportunityCount) !== counts.all) {
    fails.push("opportunityCount:mismatch");
  }
  return {
    ok: fails.length === 0,
    fails,
    recomputedFingerprintSha256: fp.contentFingerprintSha256,
    recomputedCounts: {
      all: counts.all,
      hotel: counts.hotel,
      competitive: counts.competitive,
    },
  };
}

export function publicationPackageServeMeta(pkg = {}) {
  return {
    publicationPackageSchema: GDI_CUSTOMER_PUBLICATION_PACKAGE_V1,
    publicationId: pkg.publicationId || null,
    publicationVersion: pkg.publicationVersion || null,
    contentFingerprintSha256: pkg.contentFingerprintSha256 || null,
    presentationSchemaVersion: pkg.presentationSchemaVersion || null,
    applicationCompatibilityVersion: pkg.applicationCompatibilityVersion || null,
    opportunityCount: pkg.opportunityCount ?? null,
    lifecycleState: pkg.lifecycleState || null,
    inventoryCounts: pkg.inventoryCounts || null,
  };
}
