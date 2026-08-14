/**
 * Factual project descriptors when no credible hotel proper name exists.
 * Never invent a fake property name.
 */
import { isUsableEntityName } from "./market-alerts-qualification-gate.js";
import { isGeographyOnlyLabel } from "./market-alerts-geo-keywords.js";

/**
 * @param {{ eventType?: string|null, rooms?: number|null, hotelProject?: string|null, title?: string, summary?: string }} input
 * @returns {string|null}
 */
export function buildProjectLabel(input = {}) {
  const hotel = String(input.hotelProject || "").trim();
  if (hotel && isUsableEntityName(hotel) && !isGeographyOnlyLabel(hotel)) {
    return null;
  }

  const text = `${input.title || ""} ${input.summary || ""}`.trim();
  const eventType = input.eventType || null;
  const rooms = Number.isFinite(input.rooms) ? input.rooms : null;
  const keys = rooms ? `${rooms}-key ` : "";

  if (eventType === "Adaptive Reuse Proposal" || /\b(adaptive reuse|convert(?:s|ed|ing)? .{0,40}(?:into|to) (?:a )?(?:hotel|resort)|office[- ]to[- ]hotel|office.{0,40}(?:hotel|resort|jw marriott))\b/i.test(text)) {
    if (/\boffice\b/i.test(text) && /\bresort\b/i.test(text) && !/\bhotel\b/i.test(text)) {
      return `Proposed ${keys}office-to-resort conversion`.replace(/\s+/g, " ").trim();
    }
    if (/\boffice\b/i.test(text)) return `Proposed ${keys}office-to-hotel conversion`.replace(/\s+/g, " ").trim();
    if (/\bwarehouse\b/i.test(text)) return `Proposed ${keys}warehouse-to-hotel conversion`.replace(/\s+/g, " ").trim();
    return `Proposed ${keys}adaptive reuse hotel`.replace(/\s+/g, " ").trim();
  }

  if (eventType === "Site Acquisition") {
    if (/\bwaterfront\b/i.test(text)) return "Waterfront hotel development site";
    return "Hotel development site";
  }

  if (/\bmixed[- ]use\b/i.test(text) && /\b(hotel|resort)\b/i.test(text)) {
    return rooms ? `Mixed-use project with ${rooms}-key hotel` : "Mixed-use hotel project";
  }

  if (eventType === "Planning Application" || eventType === "Development Proposal") {
    if (/\bresort\b/i.test(text) && !/\bhotel\b/i.test(text)) {
      return rooms ? `Proposed ${rooms}-key resort` : "Planned resort";
    }
    return rooms ? `Proposed ${rooms}-key hotel` : "Proposed hotel project";
  }

  if (rooms && /\b(hotel|resort|lodging)\b/i.test(text)) {
    return `Proposed ${rooms}-key hotel`;
  }

  return null;
}
