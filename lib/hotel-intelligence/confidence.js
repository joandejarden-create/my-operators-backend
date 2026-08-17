/**
 * Field-level confidence model (V1).
 * Deterministic, source-aware, documented — avoid fake precision.
 *
 * Tiers:
 *   0.95–1.00 Verified / authoritative
 *   0.85–0.94 High confidence
 *   0.70–0.84 Probable
 *   0.50–0.69 Needs review
 *   <0.50     Do not auto-accept
 */

export const CONFIDENCE_VERSION = "hotel-intelligence-confidence-v1";

export const CONFIDENCE_TIERS = Object.freeze({
  VERIFIED: { min: 0.95, max: 1.0, label: "verified" },
  HIGH: { min: 0.85, max: 0.94, label: "high" },
  PROBABLE: { min: 0.7, max: 0.84, label: "probable" },
  NEEDS_REVIEW: { min: 0.5, max: 0.69, label: "needs_review" },
  DO_NOT_AUTO_ACCEPT: { min: 0, max: 0.49, label: "do_not_auto_accept" },
});

/**
 * Source authority by field family.
 * Higher base_score = more trusted for that field.
 * Documented — not hidden magic.
 */
export const SOURCE_FIELD_AUTHORITY = Object.freeze({
  brand_name: {
    brand_directory: 0.96,
    official_site: 0.9,
    hotelbeds: 0.78,
    dealality_census: 0.88,
    giata_drive: 0.86, // chain names when present; not Brand Explorer SoT
    stayingapi: 0.45, // OTA listing brand unreliable for Dealality brand SoT
    serpapi: 0.4, // hotel_class/brands filter ≠ Dealality Current Brand
    google_places: 0.65,
    openstreetmap: 0.55,
    manual: 0.7,
  },
  room_count: {
    official_site: 0.96,
    brand_directory: 0.9,
    hotelbeds: 0.82,
    dealality_census: 0.85,
    room_count_research: 0.88, // evidence-backed public-source research (not auto-accept alone)
    stayingapi: 0.0, // NOT_SUPPORTED — never auto-accept
    serpapi: 0.0, // NOT_SUPPORTED — room types / VR bedrooms ≠ keys
    giata_drive: 0.0, // SUPPORTED_BUT_NOT_ENTITLED — roomTypes ≠ total keys
    google_places: 0.45,
    openstreetmap: 0.4,
    manual: 0.7,
  },
  latitude: {
    dealality_census: 0.9,
    hotelbeds: 0.8,
    giata_drive: 0.9, // Open Content geoCodes strong when matched
    stayingapi: 0.84, // useful geo when identity match is Exact/High
    serpapi: 0.88, // Google Hotels GPS when identity Exact/High
    google_places: 0.88,
    openstreetmap: 0.85,
    brand_directory: 0.75,
    official_site: 0.7,
    manual: 0.65,
  },
  longitude: {
    dealality_census: 0.9,
    hotelbeds: 0.8,
    giata_drive: 0.9,
    stayingapi: 0.84,
    serpapi: 0.88,
    google_places: 0.88,
    openstreetmap: 0.85,
    brand_directory: 0.75,
    official_site: 0.7,
    manual: 0.65,
  },
  website: {
    official_site: 0.98,
    brand_directory: 0.94,
    dealality_census: 0.9,
    giata_drive: 0.88,
    hotelbeds: 0.8,
    stayingapi: 0.72, // often Booking.com listing URL, not official site
    serpapi: 0.78, // non-Google link when present; often travel host
    google_places: 0.75,
    openstreetmap: 0.6,
    manual: 0.7,
  },
  phone: {
    official_site: 0.95,
    brand_directory: 0.9,
    dealality_census: 0.88,
    giata_drive: 0.86,
    hotelbeds: 0.82,
    stayingapi: 0.4, // typically not returned
    serpapi: 0.82, // Google Hotels property details often include phone
    google_places: 0.8,
    openstreetmap: 0.55,
    manual: 0.7,
  },
  official_name: {
    dealality_census: 0.92,
    brand_directory: 0.9,
    official_site: 0.9,
    giata_drive: 0.9,
    hotelbeds: 0.8,
    stayingapi: 0.78,
    serpapi: 0.84,
    google_places: 0.7,
    openstreetmap: 0.65,
    manual: 0.75,
  },
  address_line_1: {
    dealality_census: 0.9,
    brand_directory: 0.88,
    official_site: 0.9,
    giata_drive: 0.9,
    hotelbeds: 0.82,
    stayingapi: 0.86,
    serpapi: 0.88,
    google_places: 0.85,
    openstreetmap: 0.8,
    manual: 0.7,
  },
  default: {
    dealality_census: 0.85,
    brand_directory: 0.8,
    official_site: 0.85,
    giata_drive: 0.86,
    hotelbeds: 0.75,
    stayingapi: 0.7,
    serpapi: 0.78,
    google_places: 0.65,
    openstreetmap: 0.6,
    manual: 0.65,
  },
});

/**
 * StayingAPI confidence assumptions (V1):
 * - Strong for address/geo when identity match is Exact/High via existing matchCensusProperty
 * - Weak/zero for Rooms/Keys (capability NOT_SUPPORTED)
 * - Weak for brand/parent (OTA taxonomy ≠ Dealality Brand Setup)
 * - Website often Booking listing URL — probable, not official_site
 *
 * SerpApi Google Hotels confidence assumptions (V1):
 * - Strong for address/geo/phone when identity Exact/High
 * - Zero for Rooms/Keys (SERPAPI_ROOMS_CAPABILITY=NOT_SUPPORTED)
 * - Weak for brand/parent (hotel_class ≠ Dealality Current Brand)
 * - Website probable when non-Google host; not auto-verified official_site
 * - property_token is a durable external ID, not canonical hotel_id
 *
 * GIATA Drive Open Content confidence assumptions (V1):
 * - Strong for identity/geo/address/brand/website/phone when giataId matched
 * - Zero for Rooms/Keys (SUPPORTED_BUT_NOT_ENTITLED — roomTypes ≠ total keys)
 * - No Hotelbeds/Booking/Expedia supplier IDs (MultiCodes not entitled)
 * - giataId is external_id only — never replaces dhl_
 * - Complementary enrichment; not primary CALA universe discovery
 */

export function labelForConfidence(score) {
  const n = Number(score);
  if (!Number.isFinite(n)) return "unknown";
  if (n >= 0.95) return CONFIDENCE_TIERS.VERIFIED.label;
  if (n >= 0.85) return CONFIDENCE_TIERS.HIGH.label;
  if (n >= 0.7) return CONFIDENCE_TIERS.PROBABLE.label;
  if (n >= 0.5) return CONFIDENCE_TIERS.NEEDS_REVIEW.label;
  return CONFIDENCE_TIERS.DO_NOT_AUTO_ACCEPT.label;
}

/**
 * @param {string} field
 * @param {string} source
 * @param {{ completeness?: number, agreementBonus?: number }} [opts]
 */
export function scoreFieldConfidence(field, source, opts = {}) {
  const f = String(field || "default");
  const s = String(source || "").trim().toLowerCase();
  const table = SOURCE_FIELD_AUTHORITY[f] || SOURCE_FIELD_AUTHORITY.default;
  let base = table[s] ?? SOURCE_FIELD_AUTHORITY.default[s] ?? 0.5;
  const completeness = Number(opts.completeness);
  if (Number.isFinite(completeness)) {
    base = base * (0.85 + 0.15 * Math.min(1, Math.max(0, completeness)));
  }
  if (Number.isFinite(opts.agreementBonus)) {
    base = Math.min(1, base + Number(opts.agreementBonus));
  }
  const score = Math.round(Math.min(1, Math.max(0, base)) * 100) / 100;
  return {
    field: f,
    source: s,
    confidence: score,
    tier: labelForConfidence(score),
    auto_accept: score >= 0.85,
    explanation: `source_authority[${f}][${s}]=${score}`,
  };
}

/**
 * Prefer highest-confidence evidence; keep conflicts visible.
 * @param {Array<{ field: string, value: unknown, source: string, confidence?: number }>} evidenceRows
 */
export function preferCanonicalValue(evidenceRows) {
  const rows = [...(evidenceRows || [])].map((r) => {
    const scored =
      r.confidence != null
        ? {
            confidence: Number(r.confidence),
            tier: labelForConfidence(r.confidence),
            explanation: "provided",
          }
        : scoreFieldConfidence(r.field, r.source);
    return { ...r, ...scored };
  });
  rows.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
  const preferred = rows[0] || null;
  const conflicts = [];
  if (preferred) {
    for (const r of rows.slice(1)) {
      if (String(r.value) !== String(preferred.value)) {
        conflicts.push({
          field: preferred.field,
          preferred_value: preferred.value,
          preferred_source: preferred.source,
          preferred_confidence: preferred.confidence,
          alternate_value: r.value,
          alternate_source: r.source,
          alternate_confidence: r.confidence,
        });
      }
    }
  }
  return { preferred, all: rows, conflicts };
}
