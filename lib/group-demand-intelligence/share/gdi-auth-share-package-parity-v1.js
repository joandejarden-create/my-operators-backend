/**
 * GDI_AUTH_SHARE_PACKAGE_PARITY_V1
 *
 * Compare authenticated founder-staged package inventory against the customer-share
 * publication package contract for the same hotel.
 *
 * - Auth path: resolvePreview… preferStaged
 * - Share path: resolveShare… CURRENT only (never silent staged)
 * - Simulated share: compare staged DTOs as if CURRENT pointed at the same package
 *   (used for pre-publication parity without activating CUSTOMER_PUBLISHED)
 *
 * Does not issue tokens, write Airtable, or set CUSTOMER_PUBLISHED.
 */

import {
  resolvePreviewOpportunitiesFromPublicationPackage,
  resolveShareOpportunitiesFromApprovedPackage,
  resolveOpportunityFromApprovedPackage,
} from "../publication-package/gdi-share-approved-package-resolver-v1.js";
import { attachPackageCustomerPresentation } from "../publication-package/gdi-package-customer-presentation-attach-v1.js";
import { classifyOpportunityTypeFilter } from "../opportunity-type-filter-v1.js";
import { createGdiPublicationPackageStore } from "../publication-package/gdi-publication-package-store-v1.js";
import { verifyPublicationPackageIntegrity } from "../publication-package/gdi-customer-publication-package-v1.js";

export const GDI_AUTH_SHARE_PACKAGE_PARITY_V1 =
  "gdi-auth-share-package-parity-v1";

const CUSTOMER_SAFE_FIELDS = Object.freeze([
  "id",
  "title",
  "displayTitle",
  "organizationName",
  "hotelMarketScopeEligible",
  "competitiveMarketScopeEligible",
  "segment",
  "opportunityTypeLabel",
  "demandSignalTypeLabel",
  "priority",
  "startDate",
  "endDate",
  "eventStartDate",
  "eventEndDate",
]);

/** Field-by-field commercial drawer contract compared for auth↔share parity. */
export const COMMERCIAL_DRAWER_PARITY_FIELDS = Object.freeze([
  "title",
  "organization",
  "whatIsHappening",
  "potentialHotelBusiness",
  "whyThisHotel",
  "whyActNow",
  "recommendedNextStep",
  "contactNames",
  "contactRelevance",
  "evidenceUrls",
  "opportunityType",
  "commercialPill",
  "startDate",
  "endDate",
]);

function text(v) {
  return String(v == null ? "" : v).trim();
}

function titleOf(o) {
  return text(o?.displayTitle || o?.title || o?.customerFacingTitle || "");
}

function orgOf(o) {
  return text(
    o?.organizationName ||
      o?.primaryOrganizationName ||
      o?.organizerName ||
      o?.canonicalCustomerPresentation?.organizationName ||
      "",
  );
}

function commercialPill(o) {
  return text(
    o?.canonicalCustomerPresentation?.commercialAction?.pillLabel ||
      o?.commercialAction?.pillLabel ||
      o?.tileCommercialActionLabel ||
      "",
  );
}

function present(opportunities, meta) {
  return attachPackageCustomerPresentation(opportunities || [], meta || {});
}

function inventorySummary(opportunities = []) {
  const typeCounts = {};
  const commercialPills = {};
  let hotelMarket = 0;
  let cmi = 0;
  let scaling = 0;
  let localCatering = 0;
  for (const o of opportunities) {
    if (o?.hotelMarketScopeEligible === true) hotelMarket += 1;
    if (o?.competitiveMarketScopeEligible === true) cmi += 1;
    const t = classifyOpportunityTypeFilter(o);
    typeCounts[t] = (typeCounts[t] || 0) + 1;
    if (t === "SCALING_COMPANY") scaling += 1;
    if (t === "LOCAL_CATERING_DAY_MEETINGS") localCatering += 1;
    const pill = commercialPill(o) || "UNKNOWN";
    commercialPills[pill] = (commercialPills[pill] || 0) + 1;
  }
  return {
    all: opportunities.length,
    hotelMarket,
    cmi,
    scaling,
    localCatering,
    contactBuyer: commercialPills["Contact Buyer"] || 0,
    typeCounts,
    commercialPills,
  };
}

function pickCommercial(o, ...keys) {
  const canon = o?.canonicalCustomerPresentation || {};
  for (const key of keys) {
    const v = text(o?.[key] || canon?.[key] || "");
    if (v) return v;
  }
  return "";
}

function contactNamesOf(o) {
  const contacts =
    o?.whoToContact ||
    o?.contacts ||
    o?.canonicalCustomerPresentation?.whoToContact ||
    [];
  if (!Array.isArray(contacts)) return "";
  return contacts
    .map((c) => text(c?.name || c?.fullName || c?.displayName || ""))
    .filter(Boolean)
    .sort()
    .join(" | ");
}

function contactRelevanceOf(o) {
  const contacts =
    o?.whoToContact ||
    o?.contacts ||
    o?.canonicalCustomerPresentation?.whoToContact ||
    [];
  if (!Array.isArray(contacts)) return "";
  return contacts
    .map((c) =>
      text(
        c?.relevance ||
          c?.whyRelevant ||
          c?.contactRelevance ||
          c?.roleRelevance ||
          "",
      ),
    )
    .filter(Boolean)
    .sort()
    .join(" | ");
}

function evidenceUrlsOf(o) {
  const evidence =
    o?.supportingEvidence ||
    o?.evidence ||
    o?.canonicalCustomerPresentation?.supportingEvidence ||
    [];
  if (!Array.isArray(evidence)) return text(o?.sourceUrl || "");
  return evidence
    .map((e) => text(e?.url || e?.href || e?.sourceUrl || ""))
    .filter(Boolean)
    .sort()
    .join(" | ");
}

function rowFingerprint(o) {
  return {
    id: text(o?.id),
    title: titleOf(o),
    organization: orgOf(o),
    hotelMarket: o?.hotelMarketScopeEligible === true,
    cmi: o?.competitiveMarketScopeEligible === true,
    opportunityType: classifyOpportunityTypeFilter(o),
    commercialPill: commercialPill(o),
    startDate: text(o?.startDate || o?.eventStartDate || ""),
    endDate: text(o?.endDate || o?.eventEndDate || ""),
    whatIsHappening: pickCommercial(
      o,
      "whatIsHappening",
      "demandThesis",
      "summary",
      "description",
    ),
    potentialHotelBusiness: pickCommercial(
      o,
      "potentialHotelBusiness",
      "potentialBusiness",
      "hotelPotentialBusiness",
    ),
    whyThisHotel: pickCommercial(o, "whyThisHotel", "hotelProductFit"),
    whyActNow: pickCommercial(o, "whyActNow", "timingRationale"),
    recommendedNextStep: pickCommercial(
      o,
      "recommendedNextStep",
      "nextSalesAction",
      "customerFacingRecommendedNextStep",
    ),
    contactNames: contactNamesOf(o),
    contactRelevance: contactRelevanceOf(o),
    evidenceUrls: evidenceUrlsOf(o),
  };
}

function compareRows(authRow, shareRow) {
  const mismatches = [];
  for (const key of Object.keys(authRow)) {
    if (String(authRow[key]) !== String(shareRow[key])) {
      mismatches.push({ field: key, auth: authRow[key], share: shareRow[key] });
    }
  }
  return mismatches;
}

/**
 * Build simulated share inventory from the staged package without writing CURRENT.
 */
export function simulateShareInventoryFromStagedPackage(hotelId, opts = {}) {
  const staged = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId,
    preferStaged: true,
    preview: "staged",
    root: opts.root,
    env: opts.env,
  });
  if (!(staged.ok && staged.mode === "PACKAGE")) {
    return {
      ok: false,
      error: staged.error || "STAGED_UNAVAILABLE",
      staged,
    };
  }
  const integrity = verifyPublicationPackageIntegrity(staged.package);
  if (!integrity.ok) {
    return {
      ok: false,
      error: "PACKAGE_INTEGRITY_FAILED",
      fails: integrity.fails,
    };
  }
  const opportunities = present(staged.opportunities, {
    ...staged.meta,
    lifecycleState: "CUSTOMER_PUBLISHED",
    previewSource: "CURRENT",
  });
  return {
    ok: true,
    fingerprint: staged.meta?.contentFingerprintSha256 || null,
    publicationVersion: staged.meta?.publicationVersion || null,
    publicationId: staged.meta?.publicationId || null,
    opportunities,
    inventory: inventorySummary(opportunities),
    package: staged.package,
    meta: staged.meta,
  };
}

/**
 * Full parity assessment for a hotel.
 */
export function assessAuthSharePackageParity(hotelId, opts = {}) {
  const store =
    opts.store ||
    createGdiPublicationPackageStore({ env: opts.env, root: opts.root });
  const stagedPtr = store.readPointer(hotelId, "STAGED");
  const currentPtr = store.readPointer(hotelId, "CURRENT");

  const authResolved = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId,
    preferStaged: true,
    preview: "staged",
    root: opts.root,
    env: opts.env,
  });
  const shareResolved = resolveShareOpportunitiesFromApprovedPackage({
    hotelId,
    root: opts.root,
    env: opts.env,
  });
  const simulated = simulateShareInventoryFromStagedPackage(hotelId, opts);

  const authOpps =
    authResolved.ok && authResolved.mode === "PACKAGE"
      ? present(authResolved.opportunities, authResolved.meta)
      : [];
  const liveShareOpps =
    shareResolved.ok && shareResolved.mode === "PACKAGE"
      ? present(shareResolved.opportunities, shareResolved.meta)
      : [];

  const authById = new Map(authOpps.map((o) => [text(o.id), o]));
  const simById = new Map(
    (simulated.opportunities || []).map((o) => [text(o.id), o]),
  );

  const missingInSim = [];
  const extraInSim = [];
  const fieldMismatches = [];
  for (const id of authById.keys()) {
    if (!simById.has(id)) missingInSim.push(id);
  }
  for (const id of simById.keys()) {
    if (!authById.has(id)) extraInSim.push(id);
  }
  for (const [id, authOpp] of authById) {
    const simOpp = simById.get(id);
    if (!simOpp) continue;
    const mismatches = compareRows(rowFingerprint(authOpp), rowFingerprint(simOpp));
    if (mismatches.length) {
      fieldMismatches.push({ id, title: titleOf(authOpp), mismatches });
    }
  }

  const detailFails = [];
  for (const id of authById.keys()) {
    const detail = resolveOpportunityFromApprovedPackage({
      hotelId,
      opportunityId: id,
      preview: "staged",
      preferStaged: true,
      root: opts.root,
      env: opts.env,
    });
    if (!(detail.ok && detail.opportunity && text(detail.opportunity.id) === id)) {
      detailFails.push({
        id,
        error: detail.error || "DETAIL_MISS",
      });
    }
  }

  const authFp = authResolved.meta?.contentFingerprintSha256 || null;
  const shareFp = shareResolved.meta?.contentFingerprintSha256 || null;
  const simulatedFp = simulated.fingerprint || null;

  const liveShareMatchesStaged =
    shareResolved.ok &&
    shareResolved.mode === "PACKAGE" &&
    authFp &&
    shareFp &&
    String(authFp).toLowerCase() === String(shareFp).toLowerCase() &&
    authOpps.length === liveShareOpps.length;

  const simulatedParityOk =
    simulated.ok === true &&
    missingInSim.length === 0 &&
    extraInSim.length === 0 &&
    fieldMismatches.length === 0 &&
    detailFails.length === 0 &&
    authFp &&
    simulatedFp &&
    String(authFp).toLowerCase() === String(simulatedFp).toLowerCase();

  return {
    contract: GDI_AUTH_SHARE_PACKAGE_PARITY_V1,
    hotelId,
    stagedPointer: stagedPtr,
    currentPointer: currentPtr,
    auth: {
      ok: authResolved.ok && authResolved.mode === "PACKAGE",
      mode: authResolved.mode,
      fingerprint: authFp,
      publicationVersion: authResolved.meta?.publicationVersion || null,
      inventory: inventorySummary(authOpps),
      opportunityIds: [...authById.keys()].sort(),
    },
    liveShare: {
      ok: shareResolved.ok,
      mode: shareResolved.mode,
      error: shareResolved.error || shareResolved.meta?.reason || null,
      fingerprint: shareFp,
      inventory: inventorySummary(liveShareOpps),
      matchesStagedFingerprint: liveShareMatchesStaged,
    },
    simulatedShareFromStaged: {
      ok: simulated.ok,
      fingerprint: simulatedFp,
      inventory: simulated.inventory || null,
      parityOk: simulatedParityOk,
      missingInSim,
      extraInSim,
      fieldMismatchCount: fieldMismatches.length,
      fieldMismatches: fieldMismatches.slice(0, 20),
      detailFailCount: detailFails.length,
      detailFails: detailFails.slice(0, 20),
    },
    customerSafeFieldsCompared: CUSTOMER_SAFE_FIELDS,
    commercialDrawerFieldsCompared: COMMERCIAL_DRAWER_PARITY_FIELDS,
    productionExternalReady: Boolean(
      liveShareMatchesStaged && currentPtr?.lifecycleState === "CUSTOMER_PUBLISHED",
    ),
    founderApprovalRequired: !(
      currentPtr?.lifecycleState === "CUSTOMER_PUBLISHED" &&
      authFp &&
      shareFp &&
      String(authFp).toLowerCase() === String(shareFp).toLowerCase()
    ),
    note:
      "Auth staged vs simulated share must match field-by-field before founder publishes STAGED → CURRENT. Live external share remains on CURRENT until approval.",
  };
}

export function assertAuthShareSimulatedParity(assessment) {
  if (!assessment?.auth?.ok) {
    throw new Error("AUTH_STAGED_PACKAGE_UNAVAILABLE");
  }
  if (!assessment?.simulatedShareFromStaged?.parityOk) {
    throw new Error(
      `SIMULATED_SHARE_PARITY_FAILED missing=${assessment.simulatedShareFromStaged.missingInSim.length} extra=${assessment.simulatedShareFromStaged.extraInSim.length} field=${assessment.simulatedShareFromStaged.fieldMismatchCount} detail=${assessment.simulatedShareFromStaged.detailFailCount}`,
    );
  }
  return true;
}
