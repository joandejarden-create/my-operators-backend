/**
 * Demand Anchors / Travel Infrastructure — verified fixture import gating.
 * Google metadata is allowed only at fixture envelope level, never on points.
 */

export const GOOGLE_METADATA_POINT_FIELDS = [
  "googlePlaceId",
  "googleName",
  "googleLatitude",
  "googleLongitude",
  "googleFormattedAddress",
  "googleMapsUri",
  "googleTypes",
  "googleBusinessStatus",
  "businessStatus",
  "googleSearchQuery",
];

export const VERIFICATION_METHOD_LABEL =
  "Google Maps / Google Places pre-import verification";

function normalizeBool(v) {
  if (v === true || v === 1 || v === "1") return true;
  if (typeof v === "string" && ["true", "yes"].includes(v.toLowerCase())) return true;
  return false;
}

export function parseRequireVerifiedFixtureFlag(argvOrBody) {
  if (Array.isArray(argvOrBody)) {
    return argvOrBody.some((a) =>
      [
        "--requireVerifiedFile",
        "--requireVerifiedFile=true",
        "--require-verified-fixture",
        "--require-verified-fixture=true",
      ].includes(a)
    );
  }
  return normalizeBool(argvOrBody?.requireVerifiedFile ?? argvOrBody?.requireVerifiedFixture);
}

function containsPlaceholderName(name) {
  return /(sample|placeholder|lorem ipsum)/i.test(String(name || ""));
}

export function pointHasGoogleMetadataFields(point) {
  if (!point || typeof point !== "object") return false;
  return GOOGLE_METADATA_POINT_FIELDS.some((field) => Object.hasOwn(point, field));
}

/** Remove Google QA-only fields before writing import-ready fixtures. */
export function stripGoogleMetadataFromPoint(point) {
  if (!point || typeof point !== "object") return point;
  const out = { ...point };
  for (const field of GOOGLE_METADATA_POINT_FIELDS) {
    delete out[field];
  }
  delete out.manuallyVerified;
  return out;
}

export function stripGoogleMetadataFromPoints(points) {
  return (points || []).map(stripGoogleMetadataFromPoint);
}

export function containsUnresolvedVerificationNotes(notes) {
  const text = String(notes || "");
  if (!/(unresolved|no match|ambiguous)/i.test(text)) return false;
  return !/(manually verified|manual override|manual review approved)/i.test(text);
}

/**
 * @param {object} body
 * @returns {string[]|null} errors or null when valid
 */
export function validateVerifiedFixtureRequirements(body) {
  const requireVerified = parseRequireVerifiedFixtureFlag(body);
  if (!requireVerified) return null;

  const verification = body?.verification || {};
  const points = Array.isArray(body?.points) ? body.points : [];
  const errors = [];

  if (!String(verification.method || "").trim()) {
    errors.push("verification.method is required when require-verified-fixture is enabled");
  }
  if (!String(verification.verifiedAt || "").trim()) {
    errors.push("verification.verifiedAt is required when require-verified-fixture is enabled");
  }
  if (!Number.isFinite(Number(verification.verifiedRecords))) {
    errors.push("verification.verifiedRecords is required when require-verified-fixture is enabled");
  }

  const googleFieldPoints = points.filter((p) => pointHasGoogleMetadataFields(p));
  if (googleFieldPoints.length) {
    errors.push("Points must not include Google-specific fields; keep Google metadata in the verification report only.");
  }

  const badNames = points.filter((p) =>
    containsPlaceholderName(p.name || p["Demand Anchor Name"])
  );
  if (badNames.length) errors.push("Fixture contains sample/placeholder point names.");

  const missingCoords = points.filter((p) => {
    const lat = p.latitude ?? p.Latitude ?? p.lat;
    const lng = p.longitude ?? p.Longitude ?? p.lng;
    return lat == null || lng == null;
  });
  if (missingCoords.length) errors.push("All points must include latitude and longitude.");

  const missingSourceRef = points.filter(
    (p) => !String(p.sourceReference || p["Source URL / Reference"] || p.sourceUrl || "").trim()
  );
  if (missingSourceRef.length) errors.push("All points must include Source URL / Reference.");

  const unresolvedNotes = points.filter((p) =>
    containsUnresolvedVerificationNotes(p.notes || p.Notes)
  );
  if (unresolvedNotes.length) {
    errors.push(
      "Points with unresolved / no match / ambiguous verification notes require explicit manual override before import."
    );
  }

  return errors.length ? errors : null;
}
