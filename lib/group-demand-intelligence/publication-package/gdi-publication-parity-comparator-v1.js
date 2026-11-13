/**
 * Deterministic GDI publication parity comparator V1.
 *
 * Compares an approved publication package against a candidate serve payload
 * (local preview or production share). Does not broadly rewrite text to hide
 * mismatches — only insignificant transport normalization.
 */

import {
  PUBLICATION_CONTENT_FIELDS,
  computePublicationInventoryCounts,
  fingerprintPublicationContent,
  verifyPublicationPackageIntegrity,
} from "./gdi-customer-publication-package-v1.js";
import { runPublicationQualityGate } from "./gdi-publication-quality-gate-v1.js";

export const GDI_PUBLICATION_PARITY_COMPARATOR_V1 =
  "gdi-publication-parity-comparator-v1";

function text(v) {
  if (v == null) return "";
  // Normalize insignificant transport only: NBSP → space, collapse whitespace.
  return String(v)
    .replace(/\u00a0/g, " ")
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\s+/g, " ")
    .trim();
}

function normJson(v) {
  if (v == null) return null;
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
    return typeof v === "string" ? text(v) : v;
  }
  if (Array.isArray(v)) return v.map(normJson);
  if (typeof v === "object") {
    const out = {};
    for (const k of Object.keys(v).sort()) {
      out[k] = normJson(v[k]);
    }
    return out;
  }
  return text(v);
}

function fieldEqual(a, b) {
  return JSON.stringify(normJson(a)) === JSON.stringify(normJson(b));
}

function indexById(list = []) {
  const map = new Map();
  for (const o of list) {
    const id = text(o?.id);
    if (id) map.set(id, o);
  }
  return map;
}

/**
 * Inventory parity: id set, counts, market scope, sort, duplicates.
 */
export function comparePublicationInventory(approved = {}, candidate = {}) {
  const aOps = Array.isArray(approved.opportunities) ? approved.opportunities : [];
  const cOps = Array.isArray(candidate.opportunities) ? candidate.opportunities : [];
  const aIds = aOps.map((o) => text(o.id)).filter(Boolean);
  const cIds = cOps.map((o) => text(o.id)).filter(Boolean);
  const aSet = new Set(aIds);
  const cSet = new Set(cIds);
  const missing = [...aSet].filter((id) => !cSet.has(id)).sort();
  const extra = [...cSet].filter((id) => !aSet.has(id)).sort();
  const aDupes = aIds.filter((id, i) => aIds.indexOf(id) !== i);
  const cDupes = cIds.filter((id, i) => cIds.indexOf(id) !== i);

  const aCounts = computePublicationInventoryCounts(aOps);
  const cCounts = computePublicationInventoryCounts(cOps);

  const aSort = Array.isArray(approved.sortOrder)
    ? approved.sortOrder.map(String)
    : aIds;
  const cSort = Array.isArray(candidate.sortOrder)
    ? candidate.sortOrder.map(String)
    : cIds;

  const sortMismatch =
    aSort.length === cSort.length &&
    aSort.every((id, i) => id === cSort[i])
      ? []
      : aSort
          .map((id, i) => ({ index: i, approved: id, candidate: cSort[i] || null }))
          .filter((row) => row.approved !== row.candidate);

  const hotelMissing = aCounts.hotelIds.filter((id) => !cCounts.hotelIds.includes(id));
  const hotelExtra = cCounts.hotelIds.filter((id) => !aCounts.hotelIds.includes(id));
  const competitiveMissing = aCounts.competitiveIds.filter(
    (id) => !cCounts.competitiveIds.includes(id),
  );
  const competitiveExtra = cCounts.competitiveIds.filter(
    (id) => !aCounts.competitiveIds.includes(id),
  );

  const ok =
    missing.length === 0 &&
    extra.length === 0 &&
    aDupes.length === 0 &&
    cDupes.length === 0 &&
    sortMismatch.length === 0 &&
    hotelMissing.length === 0 &&
    hotelExtra.length === 0 &&
    competitiveMissing.length === 0 &&
    competitiveExtra.length === 0 &&
    aCounts.all === cCounts.all &&
    aCounts.hotel === cCounts.hotel &&
    aCounts.competitive === cCounts.competitive;

  return {
    ok,
    approvedCounts: {
      all: aCounts.all,
      hotel: aCounts.hotel,
      competitive: aCounts.competitive,
    },
    candidateCounts: {
      all: cCounts.all,
      hotel: cCounts.hotel,
      competitive: cCounts.competitive,
    },
    missingOpportunityIds: missing,
    extraOpportunityIds: extra,
    duplicateApprovedIds: [...new Set(aDupes)],
    duplicateCandidateIds: [...new Set(cDupes)],
    sortMismatches: sortMismatch,
    hotelScopeMissingIds: hotelMissing,
    hotelScopeExtraIds: hotelExtra,
    competitiveScopeMissingIds: competitiveMissing,
    competitiveScopeExtraIds: competitiveExtra,
  };
}

/**
 * Content parity across PUBLICATION_CONTENT_FIELDS for shared ids.
 */
export function comparePublicationContent(approved = {}, candidate = {}) {
  const aMap = indexById(approved.opportunities);
  const cMap = indexById(candidate.opportunities);
  const shared = [...aMap.keys()].filter((id) => cMap.has(id)).sort();
  const differences = [];

  for (const id of shared) {
    const a = aMap.get(id);
    const c = cMap.get(id);
    for (const field of PUBLICATION_CONTENT_FIELDS) {
      if (field === "id") continue;
      const av = a[field];
      const cv = c[field];
      // Treat undefined and null/"" equivalently only when both empty.
      const aEmpty = av == null || av === "" || (Array.isArray(av) && !av.length);
      const cEmpty = cv == null || cv === "" || (Array.isArray(cv) && !cv.length);
      if (aEmpty && cEmpty) continue;
      if (!fieldEqual(av, cv)) {
        differences.push({
          opportunityId: id,
          field,
          differenceType: "CONTENT_FIELD_MISMATCH",
          approvedValue: av ?? null,
          candidateValue: cv ?? null,
        });
      }
    }
  }

  return {
    ok: differences.length === 0,
    comparedOpportunityCount: shared.length,
    fieldDifferenceCount: differences.length,
    differences,
  };
}

/**
 * Rendered-text parity: concatenate key customer strings per opportunity.
 */
export function compareRenderedCustomerText(approved = {}, candidate = {}) {
  const renderFields = [
    "displayTitle",
    "organizationName",
    "whatIsHappening",
    "whyThisHotel",
    "whyActNow",
    "potentialHotelBusiness",
    "recommendedNextStep",
    "hotelSourcingStatusLabel",
    "tileClassificationLine",
    "salesActionPillLabel",
    "cardWhyNowLine",
  ];
  const aMap = indexById(approved.opportunities);
  const cMap = indexById(candidate.opportunities);
  const shared = [...aMap.keys()].filter((id) => cMap.has(id)).sort();
  const differences = [];

  for (const id of shared) {
    const a = aMap.get(id);
    const c = cMap.get(id);
    const aText = renderFields.map((f) => text(a[f])).filter(Boolean).join("\n");
    const cText = renderFields.map((f) => text(c[f])).filter(Boolean).join("\n");
    if (aText !== cText) {
      differences.push({
        opportunityId: id,
        field: "rendered_customer_text",
        differenceType: "RENDERED_TEXT_MISMATCH",
        approvedValue: aText,
        candidateValue: cText,
      });
    }
  }

  return {
    ok: differences.length === 0,
    renderedTextDifferenceCount: differences.length,
    differences,
  };
}

/**
 * Full parity certification between approved package and candidate serve.
 */
export function comparePublicationParity(approvedPkg, candidatePayload, opts = {}) {
  const integrity = verifyPublicationPackageIntegrity(approvedPkg);
  const inventory = comparePublicationInventory(approvedPkg, candidatePayload);
  const content = comparePublicationContent(approvedPkg, candidatePayload);
  const rendered = compareRenderedCustomerText(approvedPkg, candidatePayload);

  const candidateFp =
    candidatePayload?.contentFingerprintSha256 ||
    fingerprintPublicationContent({
      hotelId: approvedPkg.hotelId,
      publicationVersion: approvedPkg.publicationVersion,
      presentationSchemaVersion: approvedPkg.presentationSchemaVersion,
      applicationCompatibilityVersion: approvedPkg.applicationCompatibilityVersion,
      sortOrder: candidatePayload.sortOrder || approvedPkg.sortOrder,
      opportunities: candidatePayload.opportunities || [],
    }).contentFingerprintSha256;

  const fingerprintMatch =
    text(approvedPkg.contentFingerprintSha256).toLowerCase() ===
    text(candidateFp).toLowerCase();

  const frontendMatch =
    !opts.requireFrontendMatch ||
    text(opts.candidateFrontendBuild) ===
      text(approvedPkg.applicationCompatibilityVersion);

  const qualityApproved = runPublicationQualityGate({
    opportunities: approvedPkg.opportunities,
  });
  const qualityCandidate = runPublicationQualityGate({
    opportunities: candidatePayload.opportunities || [],
  });

  const ok =
    integrity.ok &&
    inventory.ok &&
    content.ok &&
    rendered.ok &&
    fingerprintMatch &&
    frontendMatch &&
    qualityApproved.ok &&
    qualityCandidate.ok;

  return {
    version: GDI_PUBLICATION_PARITY_COMPARATOR_V1,
    ok,
    publicationReady: ok,
    hotelId: approvedPkg.hotelId || null,
    publicationId: approvedPkg.publicationId || null,
    publicationVersion: approvedPkg.publicationVersion || null,
    contentFingerprintSha256: approvedPkg.contentFingerprintSha256 || null,
    candidateContentFingerprintSha256: candidateFp,
    fingerprintMatch,
    frontendCompatibilityOk: frontendMatch,
    integrity,
    inventory,
    content: {
      ok: content.ok,
      fieldDifferenceCount: content.fieldDifferenceCount,
      comparedOpportunityCount: content.comparedOpportunityCount,
    },
    rendered: {
      ok: rendered.ok,
      renderedTextDifferenceCount: rendered.renderedTextDifferenceCount,
    },
    quality: {
      approvedOk: qualityApproved.ok,
      candidateOk: qualityCandidate.ok,
      approvedBlockingCount: qualityApproved.blockingCount,
      candidateBlockingCount: qualityCandidate.blockingCount,
    },
    differences: [...content.differences, ...rendered.differences],
    inventoryAnomalies: {
      missingOpportunityIds: inventory.missingOpportunityIds,
      extraOpportunityIds: inventory.extraOpportunityIds,
    },
  };
}
