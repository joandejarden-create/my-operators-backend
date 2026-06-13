/**
 * Rule-based brand fact extraction from reference material text.
 * Facts include evidence quotes — no values without source support.
 */
import { PARTNER_INTELLIGENCE_GAP_COPY } from "../../api/lib/partner-intelligence-field-map.js";
import { listBrandFieldsForExtraction } from "./brand-explorer-registry-catalog.js";
import { BRAND_FIELD_EXTRACTION_HINTS } from "./brand-field-extraction-hints.js";
import { excerptAround } from "./extract-source-text.js";

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeExtracted(text) {
  return nz(text).replace(/\s+/g, " ").trim();
}

function factCandidate(field, payload) {
  return {
    fieldKey: field.fieldKey,
    explorerSection: field.explorerSection,
    displayLabel: field.displayLabel,
    ...payload,
  };
}

function gapFact(field, followUp) {
  return factCandidate(field, {
    extractedValue: PARTNER_INTELLIGENCE_GAP_COPY,
    normalizedValue: PARTNER_INTELLIGENCE_GAP_COPY,
    evidenceText: PARTNER_INTELLIGENCE_GAP_COPY,
    extractionType: "Needs Confirmation",
    confidenceLevel: "Low",
    confidenceScore: 20,
    dataGap: "Yes",
    followUpQuestion: followUp || `Confirm ${field.displayLabel} in brand reference materials.`,
  });
}

function tryExtractField(field, raw, anchor) {
  const hints = BRAND_FIELD_EXTRACTION_HINTS[field.fieldKey];
  if (!hints) return null;

  if (hints.fixedValue) {
    const val =
      typeof hints.fixedValue === "function" ? hints.fixedValue() : hints.fixedValue;
    const idx = raw.toLowerCase().indexOf(String(val).toLowerCase().slice(0, 12));
    return factCandidate(field, {
      extractedValue: val,
      normalizedValue: val,
      evidenceText:
        idx >= 0
          ? excerptAround(raw, idx, 140)
          : `Fixed from brand registry (${anchor})`,
      pageSectionAnchor: anchor,
      extractionType: "Directly Stated",
      confidenceLevel: idx >= 0 ? "High" : "Medium",
      confidenceScore: idx >= 0 ? 85 : 70,
      dataGap: "No",
    });
  }

  for (const pattern of hints.patterns || []) {
    const m = raw.match(pattern);
    if (!m) continue;
    let value = m[1] != null && hints.transform ? hints.transform(m) : normalizeExtracted(m[0]);
    if (hints.transform && m[1] == null && !hints.fixedValue) {
      value = hints.transform(m);
    }
    if (!value) continue;
    value = normalizeExtracted(String(value));
    if (value.length > 2000) value = value.slice(0, 1997) + "…";
    return factCandidate(field, {
      extractedValue: value,
      normalizedValue: value,
      evidenceText: excerptAround(raw, m.index ?? 0, 160),
      pageSectionAnchor: anchor,
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      confidenceScore: 82,
      dataGap: "No",
    });
  }
  return null;
}

/**
 * @param {string} sourceText
 * @param {import('./brand-explorer-registry-catalog.js').RegistryField[]} [fields]
 * @param {{ sourceTitle?: string, sourceRole?: string, localFilePath?: string }} meta
 */
export function extractBrandFactsFromText(sourceText, fields, meta = {}) {
  const registry = fields || listBrandFieldsForExtraction();
  const raw = nz(sourceText);
  const anchor = meta.localFilePath || meta.sourceTitle || "source";
  const applicable = registry.filter((f) => {
    const roles = f.sourceRoles || ["any"];
    const role = meta.sourceRole || "any";
    return roles.includes(role) || roles.includes("any");
  });

  const facts = [];
  for (const field of applicable) {
    const hit = tryExtractField(field, raw, anchor);
    facts.push(hit || gapFact(field));
  }
  return facts;
}

/**
 * Merge facts from multiple sources — prefer higher confidence, then FDD > brochure > web.
 */
const ROLE_PRIORITY = {
  fdd: 4,
  development_brochure: 3,
  brand_summary: 2,
  brand_web: 1,
  any: 0,
};

export function mergeBrandExtractionCandidates(taggedFacts, registryFields) {
  const byKey = new Map();
  const fields = registryFields || listBrandFieldsForExtraction();

  for (const c of taggedFacts) {
    if (c.dataGap === "Yes") continue;
    const prev = byKey.get(c.fieldKey);
    const roleScore = ROLE_PRIORITY[c._sourceRole] ?? 0;
    const prevScore = prev ? ROLE_PRIORITY[prev._sourceRole] ?? 0 : -1;
    const conf = c.confidenceScore || 0;
    const prevConf = prev?.confidenceScore || 0;
    if (!prev || roleScore > prevScore || (roleScore === prevScore && conf > prevConf)) {
      byKey.set(c.fieldKey, c);
    }
  }

  return fields.map((field) => {
    const hit = byKey.get(field.fieldKey);
    if (hit) return hit;
    return gapFact(field);
  });
}
