/**
 * Filter parse artifacts from observed competitor entities for customer display.
 */

import { canonicalizeCompetitorName, matchesDeclaredComp } from "../intelligence/competitor-name-resolution.js";

const ARTIFACT_PATTERNS = [
  /^best hotel$/i,
  /^top hotel$/i,
  /^upscale hotel$/i,
  /^luxury hotel$/i,
  /^family resort$/i,
  /^friendly hotel$/i,
  /^waterfront hotel$/i,
  /^south florida hotel$/i,
  /^top meeting hotel$/i,
  /^top upscale hotel$/i,
  /^yacht club$/i,
  /^the yacht club$/i,
  /^country club$/i,
  /^best resort$/i,
  /^luxury resort$/i,
  /^oceanfront resort$/i,
  /^many (suites|resort|hotels?)$/i,
  /^oceanfront suites$/i,
  /^the main hotel$/i,
  /^with only \d+ suites$/i,
  /^hampton inn(&? suites)?$/i,
  /^this hotel$/i,
  /^this resort$/i,
  /^this boutique resort$/i,
  /^this iconic hotel$/i,
  /^this iconic boutique hotel$/i,
  /^choose hotel$/i,
  /^choose from\b/i,
  /^located in\b/i,
  /^situated (in|on|at)\b/i,
  /^although slightly north of downtown, this hotel$/i,
];

const HOTEL_SIGNAL = /\b(hotel|resort|suites|inn|collection|marriott|hilton|hyatt|waldorf|curio|renaissance|wyndham|four seasons|acqualina|embassy)\b/i;

export function isLikelyArtifactEntity(name) {
  const trimmed = String(name || "").trim();
  if (!trimmed || trimmed.length < 4) return true;
  if (ARTIFACT_PATTERNS.some((p) => p.test(trimmed))) return true;
  // Prose fragments that still contain a lodging noun.
  if (/^this\b/i.test(trimmed)) return true;
  if (/^(choose|formerly|located|situated)\s/i.test(trimmed)) return true;
  if (/^(the)\s/i.test(trimmed) && !HOTEL_SIGNAL.test(trimmed) && trimmed.split(/\s+/).length <= 3) {
    // Allow short branded names like "The Chatwal" only when they look proper-cased
    // and are not generic; still prefer registry resolution upstream.
    // Treat bare "The X" without lodging noun as artifact unless multi-word proper brand.
    if (!/^[A-Z][a-z]+(\s[A-Z][a-z]+)+$/.test(trimmed) && trimmed.split(/\s+/).length < 3) return true;
  }
  return false;
}

export function isCustomerSafeObservedAlternative(name, propertyProfile) {
  const declared = propertyProfile?.declaredCompSet || [];
  if (declared.some((d) => matchesDeclaredComp(name, d))) return true;
  if (isLikelyArtifactEntity(name)) return false;
  const canonical = canonicalizeCompetitorName(name, { market: propertyProfile?.market }) || name;
  if (isLikelyArtifactEntity(canonical)) return false;
  if (HOTEL_SIGNAL.test(canonical)) return true;
  const words = canonical.split(/\s+/).filter(Boolean);
  return words.length >= 2 && /^[A-Z]/.test(canonical);
}

export function classifyObservedEntityQuality(observedList, propertyProfile) {
  const raw = observedList || [];
  const customerSafe = raw.filter((o) => isCustomerSafeObservedAlternative(o.name, propertyProfile));
  return {
    rawEntityCount: raw.length,
    canonicalEntityCount: customerSafe.length,
    filteredArtifactCount: raw.length - customerSafe.length,
    customerSafeAlternatives: customerSafe,
    customerSafe: true,
  };
}
