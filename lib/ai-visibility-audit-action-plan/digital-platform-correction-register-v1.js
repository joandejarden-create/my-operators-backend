/**
 * Digital Platform Correction Register V1
 * Permanent AVA module: platform-specific corrections with ownership + evidence.
 */
export const DIGITAL_PLATFORM_CORRECTION_REGISTER_CONTRACT =
  "AVA_DIGITAL_PLATFORM_CORRECTION_REGISTER_V1";

export const PLATFORM_ASSESSMENT_STATUSES = Object.freeze({
  VERIFIED_CORRECT: "VERIFIED_CORRECT",
  VERIFIED_INCONSISTENCY: "VERIFIED_INCONSISTENCY",
  VERIFIED_MISSING_INFORMATION: "VERIFIED_MISSING_INFORMATION",
  IDENTIFIED_NOT_ASSESSED: "IDENTIFIED_NOT_ASSESSED",
  ACCESS_RESTRICTED: "ACCESS_RESTRICTED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  NOT_FOUND: "NOT_FOUND",
});

export const CORRECTION_EXECUTION_METHODS = Object.freeze({
  HOTEL_CMS: "Hotel CMS / website CMS",
  BRAND_REQUEST: "Brand content / franchise request",
  LISTING_OWNER_UPDATE: "Listing-owner profile update",
  EXTERNAL_OUTREACH: "External organization outreach",
  PLATFORM_SUPPORT: "Platform support ticket",
  TO_CONFIRM: "Execution path to confirm",
});

export const CORRECTION_CAPABILITY = Object.freeze({
  HOTEL_DIRECT: "HOTEL_CAN_CHANGE_DIRECTLY",
  BRAND_APPROVAL: "REQUIRES_BRAND_APPROVAL",
  THIRD_PARTY_OWNER: "REQUIRES_THIRD_PARTY_LISTING_OWNER",
  EXTERNAL_OUTREACH: "EXTERNAL_BACKLINK_OR_CITATION_OUTREACH",
});

/**
 * Normalize one correction row for the register and customer PDF.
 */
export function normalizePlatformCorrection(row = {}, ctx = {}) {
  const id =
    row.correctionId ||
    row.findingId ||
    `DCR-${String(ctx.propertyId || "hotel").slice(-6)}-${Math.random()
      .toString(36)
      .slice(2, 6)}`;
  return {
    correctionId: id,
    hotelPropertyId: ctx.propertyId || row.hotelPropertyId || null,
    hotelName: ctx.hotelName || row.hotelName || null,
    platform: row.platform || row.platformName || "Platform",
    exactUrl: row.exactUrl || row.url || null,
    issue: row.issue || row.discrepancyOrOpportunity || "",
    currentInformation: row.currentInformation || row.currentlyPublished || "",
    recommendedCorrection:
      row.recommendedCorrection || row.recommendedChange || "",
    supportingEvidenceUrl:
      row.supportingEvidenceUrl || row.evidenceUrl || row.exactUrl || row.url || null,
    commercialRelevance:
      row.commercialRelevance ||
      row.whyItMatters ||
      "Supports accurate discovery for the hotel’s target demand segments.",
    priority: row.priority || "MEDIUM",
    responsibleParty:
      row.responsibleParty ||
      row.responsibleOrganization ||
      "RESPONSIBLE_PARTY_TO_CONFIRM",
    executionMethod:
      row.executionMethod || CORRECTION_EXECUTION_METHODS.TO_CONFIRM,
    capabilityClass: row.capabilityClass || CORRECTION_CAPABILITY.HOTEL_DIRECT,
    status:
      row.status ||
      (row.hotelConfirmationRequired
        ? "REQUIRES_CONFIRMATION"
        : "VERIFIED"),
    assessmentStatus:
      row.assessmentStatus || PLATFORM_ASSESSMENT_STATUSES.IDENTIFIED_NOT_ASSESSED,
    linkedActionId: row.linkedActionId || row.actionId || null,
    hotelConfirmationRequired: row.hotelConfirmationRequired !== false,
  };
}

export function buildCorrectionRegisterFromDcr(payload) {
  const dcr = payload?.digitalContentReadiness || {};
  const propertyId = payload?.property?.propertyId;
  const hotelName = payload?.property?.name;
  const rows = (dcr.platformCorrections || []).map((c) =>
    normalizePlatformCorrection(c, { propertyId, hotelName })
  );
  return {
    contract: DIGITAL_PLATFORM_CORRECTION_REGISTER_CONTRACT,
    propertyId,
    hotelName,
    generatedAt: new Date().toISOString(),
    corrections: rows,
    counts: {
      total: rows.length,
      high: rows.filter((r) => /HIGH/i.test(r.priority)).length,
      requiresConfirmation: rows.filter((r) =>
        /CONFIRM/i.test(String(r.status))
      ).length,
    },
  };
}

export function mergeCorrectionRegister(existing = {}, incoming = {}) {
  const map = new Map();
  const keyOf = (r) =>
    `${String(r.platform || "").toLowerCase()}|${String(r.exactUrl || "").toLowerCase()}|${String(
      r.issue || ""
    )
      .toLowerCase()
      .slice(0, 80)}`;
  for (const r of [...(existing.corrections || []), ...(incoming.corrections || [])]) {
    if (!r) continue;
    const k = keyOf(r);
    if (map.has(k)) {
      const prev = map.get(k);
      // Preserve founder/hotel decisions on linked actions
      map.set(k, {
        ...prev,
        ...r,
        linkedActionId: prev.linkedActionId || r.linkedActionId,
        status: prev.status?.includes?.("APPROVED") ? prev.status : r.status,
      });
    } else {
      map.set(k, r);
    }
  }
  return {
    contract: DIGITAL_PLATFORM_CORRECTION_REGISTER_CONTRACT,
    propertyId: incoming.propertyId || existing.propertyId || null,
    hotelName: incoming.hotelName || existing.hotelName || null,
    generatedAt: new Date().toISOString(),
    corrections: [...map.values()],
    counts: {
      total: map.size,
      high: [...map.values()].filter((r) => /HIGH/i.test(r.priority)).length,
      requiresConfirmation: [...map.values()].filter((r) =>
        /CONFIRM/i.test(String(r.status))
      ).length,
    },
  };
}

export function attachCorrectionRegisterToPayload(payload, register) {
  const merged = mergeCorrectionRegister(
    payload.digitalPlatformCorrectionRegister || {},
    register
  );
  return {
    ...payload,
    digitalPlatformCorrectionRegister: merged,
    reporting: {
      ...(payload.reporting || {}),
      digitalPlatformCorrectionRegisterContract:
        DIGITAL_PLATFORM_CORRECTION_REGISTER_CONTRACT,
      digitalPlatformCorrectionRegisterAt: merged.generatedAt,
    },
  };
}
