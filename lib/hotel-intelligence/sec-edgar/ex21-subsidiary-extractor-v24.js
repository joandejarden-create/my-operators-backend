/**
 * Exhibit 21 subsidiary / SPE extraction — property-owning vehicles and parents.
 * Does not infer that a subsidiary owns a specific hotel without additional evidence.
 */

import crypto from "node:crypto";

export const EX21_SUBSIDIARY_EXTRACTOR_V24 = "sec-edgar-ex21-subsidiary-extractor-v24";

function stripHtml(html) {
  return String(html || "")
    .replace(/<[^>]+>/g, "\n")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/\s+/g, " ")
    .trim();
}

const ENTITY_LINE_RE =
  /^\s*([A-Z][A-Za-z0-9&.,'\- ]{3,90}?(?:LLC|L\.?L\.?C\.?|L\.?P\.?|Inc\.?|Corp\.?|Corporation|Company|Ltd\.?|Limited|Trust))\s*$/i;

const JURISDICTION_RE = /\b(Delaware|Maryland|Virginia|California|New York|Texas|Florida|Nevada|Massachusetts|Illinois|District of Columbia|Cayman|Bermuda)\b/i;

/**
 * Extract subsidiary / SPE entity names from EX-21 text.
 */
export function extractSubsidiariesFromExhibit21(doc, meta = {}) {
  const text = stripHtml(doc?.text || doc?.html || "");
  const lines = String(doc?.text || doc?.html || "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/tr>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .split(/\n/)
    .map((l) => l.replace(/\s+/g, " ").trim())
    .filter(Boolean);

  const entities = [];
  const seen = new Set();
  for (const line of lines) {
    if (line.length < 5 || line.length > 120) continue;
    if (/^(name|jurisdiction|state of|exhibit|subsidiary|subsidiaries|list of)/i.test(line)) continue;
    const m = line.match(
      /([A-Z][A-Za-z0-9&.,'\- ]{2,80}?(?:LLC|L\.L\.C\.|L\.P\.|Inc\.|Corp\.|Corporation|Company|Ltd\.|Limited|Trust))\b/,
    );
    if (!m) continue;
    const name = m[1].replace(/\s+/g, " ").trim();
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    if (/^(the company|registrant|see exhibit)/i.test(name)) continue;
    seen.add(key);
    const jur = line.match(JURISDICTION_RE)?.[1] || null;
    const looksHotelSpe = /\b(hotel|inn|resort|suites|lodging|hospitality|marriott|hilton|hyatt)\b/i.test(name);
    entities.push({
      entityId: `org_${crypto.createHash("sha1").update(`${meta.cik}|${name}`).digest("hex").slice(0, 16)}`,
      entityName: name,
      roleHint: looksHotelSpe ? "PROPERTY_SPE_CANDIDATE" : "SUBSIDIARY",
      jurisdiction: jur,
      relationshipStatus: "HISTORICALLY_VERIFIED",
      historicalApplicability: "HISTORICALLY_VERIFIED",
      inferredOwnershipOfNamedHotel: false,
      note: "EX-21 discloses subsidiary existence — does not by itself prove ownership of a specific hotel property",
      snippet: line.slice(0, 240),
      source: {
        cik: meta.cik || null,
        accessionNumber: meta.accessionNumber || null,
        formType: meta.form || null,
        filingDate: meta.filingDate || null,
        documentUrl: meta.documentUrl || null,
        exhibitFileName: meta.exhibitFileName || null,
        companyName: meta.companyName || null,
      },
    });
    if (entities.length >= (meta.maxEntities || 200)) break;
  }

  return {
    ok: true,
    version: EX21_SUBSIDIARY_EXTRACTOR_V24,
    entityCount: entities.length,
    hotelSpeCandidates: entities.filter((e) => e.roleHint === "PROPERTY_SPE_CANDIDATE").length,
    entities,
    guarantee:
      "Subsidiary listing ≠ property-level legal ownership attribution without additional hotel-linked evidence.",
  };
}
