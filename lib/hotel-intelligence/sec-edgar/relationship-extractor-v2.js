/**
 * Owner / operator / franchisor relationship extraction from SEC text.
 * Never infers legal ownership solely from portfolio listing / registrant.
 */

import { ENTITY_ROLE } from "./entity-attribution-v2.js";
import { HISTORICAL_APPLICABILITY } from "./contract.js";
import { detectBrandFamily } from "./brand-families-v2.js";

export const RELATIONSHIP_EXTRACTOR_V2 = "sec-edgar-relationship-extractor-v2";

export const RELATIONSHIP_STATUS = Object.freeze({
  CURRENTLY_VERIFIED: "CURRENTLY_VERIFIED",
  HISTORICALLY_VERIFIED: "HISTORICALLY_VERIFIED",
  SUPERSEDED: "SUPERSEDED",
  CONFLICTED: "CONFLICTED",
  CURRENT_STATUS_UNKNOWN: "CURRENT_STATUS_UNKNOWN",
});

function stripHtml(html) {
  return String(html || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function windowAround(text, index, radius = 200) {
  return text.slice(Math.max(0, index - radius), Math.min(text.length, index + radius)).trim();
}

const PATTERNS = [
  {
    role: ENTITY_ROLE.LEGAL_OWNER,
    // Prefer explicit fee-simple / legal-owner language; avoid "brand owned by Marriott"
    re: /\b(?:fee simple owner(?:ship)?(?:\s+is|\s+of)?|legal owner(?:\s+is)?|the (?:hotel|property) is owned by|owned in fee(?:\s+simple)? by)\s+([A-Z][A-Za-z0-9&.,'\- ]{3,80}?)(?:\.|,|;|\s+and\s+)/gi,
    requiresExplicit: true,
  },
  {
    role: ENTITY_ROLE.MANAGEMENT_COMPANY,
    re: /\b(?:managed by|management company(?:\s+is)?|hotel manager(?:\s+is)?|operator(?:\s+is)?)\s+([A-Z][A-Za-z0-9&.,'\- ]{3,80}?)(?:\.|,|;|\s+under\s+)/gi,
    requiresExplicit: true,
  },
  {
    role: ENTITY_ROLE.FRANCHISOR,
    re: /\b(?:franchisor|franchised by|franchise agreement with)\s+([A-Z][A-Za-z0-9&.,'\- ]{3,60}?)(?:\.|,|;|\s+dated\s+)/gi,
    requiresExplicit: true,
  },
  {
    role: ENTITY_ROLE.FRANCHISEE,
    re: /\b(?:franchisee)\s+([A-Z][A-Za-z0-9&.,'\- ]{3,60}?)(?:\.|,|;)/gi,
    requiresExplicit: true,
  },
  {
    role: ENTITY_ROLE.PARENT_COMPANY,
    re: /\b(?:wholly owned subsidiary of|indirect subsidiary of|parent company of)\s+([A-Z][A-Za-z0-9&.,'\- ]{3,60}?)(?:\.|,|;)/gi,
    requiresExplicit: true,
  },
];

const DATE_RE =
  /\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b|\b\d{4}-\d{2}-\d{2}\b|\b(?:acquired|disposed|sold|commenced|terminated)\s+(?:on|in)\s+(\d{4})\b/gi;

function cleanEntityName(name) {
  return String(name || "")
    .replace(/\s+/g, " ")
    .replace(/^(the|our|its)\s+/i, "")
    .replace(/[,\s]+$/g, "")
    .trim();
}

function isNoiseEntity(name) {
  const n = cleanEntityName(name);
  if (n.length < 3 || n.length > 90) return true;
  if (/\b(agreement|exhibit|table|section|property|hotel|portfolio)\b/i.test(n) && n.split(/\s+/).length <= 2)
    return true;
  if (/^(corporations?|affiliates?|subsidiaries|divisions|successors|predecessors|officers|directors)\b/i.test(n))
    return true;
  if (/\band all of (its|their)\b/i.test(n)) return true;
  if (/^(a\s+trs|holders?\b|us\b|them\b)/i.test(n)) return true;
  if (n.split(/\s+/).length === 1 && n.length <= 4) return true;
  return false;
}

/**
 * Extract explicit relationship mentions. Portfolio listing alone ≠ ownership.
 */
export function extractRelationshipsFromDocument(doc, meta = {}) {
  const text = stripHtml(doc?.text || doc?.html || "");
  const findings = [];
  const seen = new Set();

  for (const p of PATTERNS) {
    p.re.lastIndex = 0;
    let m;
    while ((m = p.re.exec(text)) !== null) {
      const entityName = cleanEntityName(m[1]);
      if (isNoiseEntity(entityName)) continue;
      const snippet = windowAround(text, m.index);
      const dates = [...snippet.matchAll(DATE_RE)].map((d) => d[0]);
      const key = `${p.role}|${entityName.toLowerCase()}|${snippet.slice(0, 60)}`;
      if (seen.has(key)) continue;
      seen.add(key);

      const status = classifyRelationshipStatus(snippet, meta);
      findings.push({
        extractorVersion: RELATIONSHIP_EXTRACTOR_V2,
        role: p.role,
        entityName,
        relationshipStatus: status,
        historicalApplicability:
          status === RELATIONSHIP_STATUS.CURRENTLY_VERIFIED
            ? HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED
            : status === RELATIONSHIP_STATUS.HISTORICALLY_VERIFIED
              ? HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED
              : HISTORICAL_APPLICABILITY.CURRENT_STATUS_UNCONFIRMED,
        datesMentioned: dates.slice(0, 3),
        effectiveDate: dates[0] || null,
        snippet: snippet.slice(0, 500),
        explicitEvidence: true,
        inferredFromPortfolioListing: false,
        hotelNameHints: extractHotelHints(snippet),
        brandHint: detectBrandFamily(snippet).brandLabel,
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
      if (findings.length >= (meta.maxFindings || 40)) break;
    }
    if (findings.length >= (meta.maxFindings || 40)) break;
  }

  // Explicitly record registrant as FILING_REGISTRANT only — never as owner by default
  if (meta.companyName) {
    findings.push({
      extractorVersion: RELATIONSHIP_EXTRACTOR_V2,
      role: ENTITY_ROLE.FILING_REGISTRANT,
      entityName: meta.companyName,
      relationshipStatus: RELATIONSHIP_STATUS.CURRENTLY_VERIFIED,
      historicalApplicability: HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED,
      snippet: `SEC registrant ${meta.companyName}`,
      explicitEvidence: true,
      inferredFromPortfolioListing: false,
      note: "Registrant of filing — not automatically legal owner or manager",
      source: {
        cik: meta.cik || null,
        accessionNumber: meta.accessionNumber || null,
        formType: meta.form || null,
        filingDate: meta.filingDate || null,
        documentUrl: meta.documentUrl || null,
        companyName: meta.companyName || null,
      },
    });
  }

  return {
    ok: true,
    version: RELATIONSHIP_EXTRACTOR_V2,
    findingCount: findings.length,
    findings,
    guarantee:
      "Legal ownership is never inferred solely because a hotel appears in a registrant portfolio schedule.",
  };
}

function classifyRelationshipStatus(snippet, meta) {
  const s = String(snippet || "").toLowerCase();
  if (/formerly|previously|prior to|until\s+\d{4}|terminated|disposed|sold the/.test(s)) {
    return RELATIONSHIP_STATUS.HISTORICALLY_VERIFIED;
  }
  if (/currently|as of the date|presently|is managed by|is owned by/.test(s)) {
    return RELATIONSHIP_STATUS.CURRENTLY_VERIFIED;
  }
  const year = Number(String(meta.filingDate || "").slice(0, 4));
  if (Number.isFinite(year) && year <= new Date().getFullYear() - 3) {
    return RELATIONSHIP_STATUS.CURRENT_STATUS_UNKNOWN;
  }
  return RELATIONSHIP_STATUS.HISTORICALLY_VERIFIED;
}

function extractHotelHints(snippet) {
  const hints = [];
  const re =
    /\b((?:Courtyard|Residence Inn|Westin|Sheraton|Hilton|Embassy Suites|Hampton Inn|Marriott|Hyatt)[A-Za-z0-9&.'\- ]{0,40})\b/gi;
  let m;
  while ((m = re.exec(snippet)) !== null) {
    hints.push(m[1].trim());
    if (hints.length >= 3) break;
  }
  return hints;
}

/**
 * Merge relationship findings; mark conflicts when same role has different entities.
 */
export function reconcileRelationshipFindings(findings = []) {
  const byHotelRole = new Map();
  const out = [];
  for (const f of findings) {
    if (f.role === ENTITY_ROLE.FILING_REGISTRANT) {
      out.push(f);
      continue;
    }
    const hotel = (f.hotelNameHints && f.hotelNameHints[0]) || "UNATTRIBUTED";
    const key = `${hotel}|${f.role}`;
    const prev = byHotelRole.get(key);
    if (!prev) {
      byHotelRole.set(key, f);
      out.push(f);
      continue;
    }
    if (prev.entityName.toLowerCase() !== f.entityName.toLowerCase()) {
      out.push({
        ...f,
        relationshipStatus: RELATIONSHIP_STATUS.CONFLICTED,
        conflictsWith: prev.entityName,
      });
    } else {
      out.push({ ...f, relationshipStatus: RELATIONSHIP_STATUS.SUPERSEDED });
    }
  }
  return { version: RELATIONSHIP_EXTRACTOR_V2, findings: out, count: out.length };
}
