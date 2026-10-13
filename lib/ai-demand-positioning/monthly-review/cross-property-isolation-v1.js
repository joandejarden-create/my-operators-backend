/**
 * Cross-property isolation helpers for Monthly Executive Review.
 * Ensures Cambridge reviews never contain NOHO entities and vice versa (all five).
 */

import { ADP_CERTIFIED_PROPERTY_IDS } from "../contracts/adp-certified-property-cohort-v1.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const KNOWN_IDENTITY_ALIASES = Object.freeze({
  adp_cambridge_beaches_bermuda: [
    "Cambridge Beaches",
    "Cambridge Beaches Resort",
    "Cambridge Beaches Resort & Spa",
  ],
  adp_now_now_noho: ["NOW NOW NOHO", "NOW NOW", "NoHo"],
  adp_waterstone_boca_raton: ["Waterstone", "Waterstone Resort", "Waterstone Resort & Marina"],
  adp_renaissance_times_square: [
    "Renaissance Times Square",
    "Renaissance New York Times Square",
    "Renaissance New York Times Square Hotel",
  ],
  adp_hotel_phillips_kansas_city: [
    "Hotel Phillips",
    "Hotel Phillips Kansas City",
    "Hotel Phillips Kansas City, Curio Collection by Hilton",
  ],
});

function loadPublishedPropertyName(propertyId) {
  try {
    const manifest = JSON.parse(
      fs.readFileSync(
        path.join(ROOT, "data/ai-demand-positioning/published", propertyId, "manifest.json"),
        "utf8"
      )
    );
    return manifest.propertyName || null;
  } catch {
    return null;
  }
}

/**
 * Allowed entity names for a subject property = subject aliases + competitors in its review.
 */
export function buildAllowedEntitySet(propertyId, review) {
  const allowed = new Set();
  for (const a of KNOWN_IDENTITY_ALIASES[propertyId] || []) allowed.add(a.toLowerCase());
  const name = review?.property?.name;
  if (name) allowed.add(String(name).toLowerCase());
  for (const d of review?.competitiveMovement?.rows || review?.lostDemand?.displacement || []) {
    if (d?.name) allowed.add(String(d.name).toLowerCase());
    if (d?.competitor) allowed.add(String(d.competitor).toLowerCase());
  }
  // Also scan displacement leaders from nested competitiveMovement object fields
  const cm = review?.competitiveMovement;
  if (cm && typeof cm === "object" && !Array.isArray(cm)) {
    for (const v of Object.values(cm)) {
      if (Array.isArray(v)) {
        for (const d of v) {
          if (d?.name) allowed.add(String(d.name).toLowerCase());
        }
      } else if (v && typeof v === "object" && v.name) {
        allowed.add(String(v.name).toLowerCase());
      }
    }
  }
  // Displacement from nested structures in assessment/evidence
  const raw = JSON.stringify(review || {});
  return { allowed, raw };
}

/**
 * Forbidden = other cohort hotels' identity aliases (not present as this property's competitors).
 */
export function buildForbiddenEntitySetForProperty(propertyId, review) {
  const { allowed } = buildAllowedEntitySet(propertyId, review);
  const forbidden = [];
  for (const otherId of ADP_CERTIFIED_PROPERTY_IDS) {
    if (otherId === propertyId) continue;
    const aliases = [
      ...(KNOWN_IDENTITY_ALIASES[otherId] || []),
      loadPublishedPropertyName(otherId),
    ].filter(Boolean);
    for (const alias of aliases) {
      const key = alias.toLowerCase();
      // If this hotel's own displacement evidence legitimately names another ADP cohort hotel, allow it
      if (allowed.has(key)) continue;
      forbidden.push({ propertyId: otherId, alias });
    }
  }

  function scanReviewText(rev) {
    const text = JSON.stringify(rev || {});
    const hits = [];
    for (const f of forbidden) {
      // Word-ish match; avoid tiny tokens
      if (f.alias.length < 5) continue;
      const re = new RegExp(f.alias.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
      if (re.test(text)) {
        hits.push({ leakedAlias: f.alias, fromPropertyId: f.propertyId });
      }
    }
    return hits;
  }

  return { forbidden, allowed: [...allowed], scanReviewText };
}

export function assertNoCrossPropertyEntityLeakage(propertyId, review) {
  const pack = buildForbiddenEntitySetForProperty(propertyId, review);
  const hits = pack.scanReviewText(review);
  return {
    gate: "ADP_MONTHLY_REVIEW_NO_CROSS_PROPERTY_ENTITY_LEAKAGE",
    pass: hits.length === 0,
    hits,
    propertyId,
  };
}
