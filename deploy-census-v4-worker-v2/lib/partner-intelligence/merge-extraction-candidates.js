/**
 * Merge extraction candidates across sources — one best fact per fieldKey.
 */
import { PARTNER_INTELLIGENCE_GAP_COPY } from "../../api/lib/partner-intelligence-field-map.js";

const ROLE_WEIGHT = {
  overview_en: 100,
  regional_deck: 85,
  overview_es: 70,
  public_web: 45,
  any: 30,
};

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeForEvidenceMatch(s) {
  return nz(s)
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[""]/g, '"')
    .replace(/['']/g, "'");
}

export function evidenceAppearsInSource(evidenceQuote, sourceText) {
  const ev = normalizeForEvidenceMatch(evidenceQuote);
  const src = normalizeForEvidenceMatch(sourceText);
  if (!ev || ev === normalizeForEvidenceMatch(PARTNER_INTELLIGENCE_GAP_COPY)) return true;
  if (ev.length < 12) return src.includes(ev);
  if (src.includes(ev)) return true;
  // Allow truncated match for long quotes
  const short = ev.slice(0, Math.min(80, ev.length));
  return short.length >= 20 && src.includes(short);
}

function roleMatchScore(field, sourceRole) {
  const roles = field?.sourceRoles || ["any"];
  if (roles.includes(sourceRole)) return ROLE_WEIGHT[sourceRole] || 40;
  if (roles.includes("any")) return Math.round((ROLE_WEIGHT[sourceRole] || 40) * 0.35);
  return 0;
}

function isLowQualityExtraction(candidate) {
  const val = nz(candidate.extractedValue);
  if (!val || val === PARTNER_INTELLIGENCE_GAP_COPY) return false;
  if (/\|{2,}/.test(val) || /Conﬁden|Confidential\s*│/i.test(val)) return true;
  if (/Senior Vice President|galardonado|Drawing from his experience/i.test(val) && candidate.fieldKey !== "op.leadership.executives") {
    return true;
  }
  if (val.length > 450 && candidate.valueType !== "json") return true;
  return false;
}

function scoreCandidate(candidate, field) {
  if (candidate.dataGap === "Yes") return -1;
  if (isLowQualityExtraction(candidate)) return -1;
  if (candidate._evidenceValid === false) return -1;

  let score = Number(candidate.confidenceScore) || 50;
  score += roleMatchScore(field, candidate._sourceRole);
  if (candidate.extractionType === "Directly Stated") score += 12;
  if (candidate._extractor === "llm") score += 8;
  if (candidate._extractor === "structured") score += 15;
  return score;
}

/**
 * @param {Array<object>} taggedCandidates — each includes fieldKey + _sourceRole + _sourceId + _sourceTitle
 * @param {Array<object>} registryFields
 */
export function mergeExtractionCandidates(taggedCandidates, registryFields) {
  const fieldByKey = new Map(registryFields.map((f) => [f.fieldKey, f]));
  const best = new Map();

  for (const c of taggedCandidates) {
    const field = fieldByKey.get(c.fieldKey);
    if (!field) continue;
    const score = scoreCandidate(c, field);
    if (score < 0) continue;
    const prev = best.get(c.fieldKey);
    if (!prev || score > prev._mergeScore) {
      best.set(c.fieldKey, { ...c, _mergeScore: score, _field: field });
    }
  }

  return [...best.values()];
}

/**
 * Inject structured regional portfolio when deck parsed successfully.
 * @param {object|null} structured
 * @param {string} sourceId
 * @param {string} sourceTitle
 */
export function structuredRegionalFacts(structured, sourceId, sourceTitle) {
  if (!structured || !structured.hotelCount) return [];
  return [
    {
      fieldKey: "op.markets.regionalPortfolio",
      explorerSection: "Regional Portfolio",
      displayLabel: "Regional Portfolio (structured)",
      extractedValue: structured.portfolioJson,
      normalizedValue: structured.portfolioJson,
      evidenceText: structured.serializedHotels.slice(0, 500),
      pageSectionAnchor: sourceTitle,
      extractionType: "Structured Parse",
      confidenceLevel: "High",
      confidenceScore: 88,
      dataGap: "No",
      valueType: "json",
      _sourceRole: "regional_deck",
      _sourceId: sourceId,
      _sourceTitle: sourceTitle,
      _extractor: "structured",
      _evidenceValid: true,
    },
    {
      fieldKey: "op.markets.activeMarkets",
      explorerSection: "Regional Presence",
      displayLabel: "Active Markets",
      extractedValue: [...new Set(structured.hotels.map((h) => h.property))].slice(0, 40).join(", "),
      normalizedValue: "",
      evidenceText: structured.serializedHotels.slice(0, 400),
      pageSectionAnchor: sourceTitle,
      extractionType: "Structured Parse",
      confidenceLevel: "Medium",
      confidenceScore: 72,
      dataGap: "No",
      valueType: "text",
      _sourceRole: "regional_deck",
      _sourceId: sourceId,
      _sourceTitle: sourceTitle,
      _extractor: "structured",
      _evidenceValid: true,
    },
  ];
}

export { ROLE_WEIGHT };
