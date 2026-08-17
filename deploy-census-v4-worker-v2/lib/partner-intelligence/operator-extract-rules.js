/**
 * Rule-based operator fact extraction (MVP — no LLM).
 * Covers all publishable registry fields: specialized rules → keyword context → gap.
 */
import { PARTNER_INTELLIGENCE_GAP_COPY } from "../../api/lib/partner-intelligence-field-map.js";
import { listOperatorFieldsForExtraction } from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import {
  OPERATOR_FIELD_EXTRACTION_HINTS,
} from "./operator-field-extraction-hints.js";
import { excerptAround } from "./extract-source-text.js";

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function splitSentences(text) {
  return nz(text)
    .split(/(?<=[.!?])\s+|\n{2,}/)
    .map((s) => s.replace(/\s+/g, " ").trim())
    .filter((s) => s.length > 25);
}

function keywordHits(sentence, keywords) {
  const lower = sentence.toLowerCase();
  let hits = 0;
  for (const kw of keywords || []) {
    if (lower.includes(kw.toLowerCase())) hits++;
  }
  return hits;
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
    followUpQuestion: followUp || `Confirm ${field.displayLabel} with operator or additional source.`,
  });
}

function buildKeywordList(field) {
  const hints = OPERATOR_FIELD_EXTRACTION_HINTS[field.fieldKey] || {};
  const fromRegistry = [field.displayLabel, field.explorerSection, field.explorerTab]
    .join(" ")
    .toLowerCase()
    .split(/\W+/)
    .filter((w) => w.length > 3);
  const merged = [...(hints.keywords || []), ...fromRegistry];
  return [...new Set(merged)];
}

function tryPatternExtract(field, raw, anchor) {
  const hints = OPERATOR_FIELD_EXTRACTION_HINTS[field.fieldKey];
  if (!hints?.patterns?.length) return null;

  for (const pattern of hints.patterns) {
    const m = raw.match(pattern);
    if (!m) continue;
    const value = nz(m[1] || m[0]);
    if (!value) continue;
    return factCandidate(field, {
      extractedValue: value,
      normalizedValue: value,
      evidenceText: excerptAround(raw, m.index, 120),
      pageSectionAnchor: anchor,
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      confidenceScore: 78,
      dataGap: "No",
    });
  }
  return null;
}

function tryContextExtract(field, raw, anchor) {
  const hints = OPERATOR_FIELD_EXTRACTION_HINTS[field.fieldKey] || {};
  const keywords = buildKeywordList(field);
  const sentences = splitSentences(raw);
  let best = null;

  for (const sentence of sentences) {
    const hits = keywordHits(sentence, keywords);
    if (hits < 2) continue;
    if (!best || hits > best.hits) {
      best = { sentence, hits };
    }
  }

  if (!best) return null;

  let value = best.sentence;
  if (hints.capability) {
    if (/\b(yes|full|comprehensive|in-house|dedicated|strong)\b/i.test(value)) {
      value = "Yes — " + value.slice(0, 200);
    } else if (/\b(no|not available|limited|third party)\b/i.test(value)) {
      value = "Limited — " + value.slice(0, 200);
    }
  }

  if (value.length > 400) value = value.slice(0, 397) + "…";

  return factCandidate(field, {
    extractedValue: value,
    normalizedValue: value,
    evidenceText: value,
    pageSectionAnchor: anchor,
    extractionType: "Inferred from Context",
    confidenceLevel: best.hits >= 3 ? "Medium" : "Low",
    confidenceScore: Math.min(35 + best.hits * 12, 62),
    dataGap: "No",
  });
}

function trySpecializedExtract(field, raw, sourceMeta, anchor) {
  const key = field.fieldKey;

  if (key === "op.snapshot.companyName") {
    const m = raw.match(/Arbor Lodging(?:\s+Partners|\s+\(CALA\))?/i);
    if (m) {
      return factCandidate(field, {
        extractedValue: "Arbor Lodging",
        normalizedValue: "Arbor Lodging",
        evidenceText: excerptAround(raw, m.index, 80),
        pageSectionAnchor: anchor,
        extractionType: "Directly Stated",
        confidenceLevel: "High",
        confidenceScore: 85,
        dataGap: "No",
      });
    }
  }

  if (key === "op.snapshot.companyDescription" && sourceMeta.metaDescription) {
    return factCandidate(field, {
      extractedValue: sourceMeta.metaDescription,
      normalizedValue: sourceMeta.metaDescription,
      evidenceText: sourceMeta.metaDescription,
      pageSectionAnchor: anchor,
      extractionType: "Directly Stated",
      confidenceLevel: "Medium",
      confidenceScore: 72,
      dataGap: "No",
    });
  }

  if (key === "op.brand.familiesOperated") {
    const families = [];
    for (const name of ["Marriott", "Hilton", "Hyatt", "IHG", "Choice", "Wyndham"]) {
      if (new RegExp(`\\b${name}\\b`, "i").test(raw)) families.push(name);
    }
    if (families.length) {
      return factCandidate(field, {
        extractedValue: families.join(", "),
        normalizedValue: families.join(", "),
        evidenceText: `Mentioned brands: ${families.join(", ")}`,
        pageSectionAnchor: anchor,
        extractionType: "Directly Stated",
        confidenceLevel: "Medium",
        confidenceScore: 68,
        dataGap: "No",
      });
    }
  }

  if (key === "op.markets.activeCountries") {
    const hits = [];
    if (/mexico/i.test(raw)) hits.push("Mexico");
    if (/united states|u\.s\./i.test(raw)) hits.push("United States");
    if (/canada/i.test(raw)) hits.push("Canada");
    if (/caribbean/i.test(raw)) hits.push("Caribbean");
    if (/latin america|cala/i.test(raw)) hits.push("Latin America");
    if (hits.length) {
      const val = [...new Set(hits)].join(", ");
      return factCandidate(field, {
        extractedValue: val,
        normalizedValue: val,
        evidenceText: `Geography mentions: ${val}`,
        pageSectionAnchor: anchor,
        extractionType: "Directly Stated",
        confidenceLevel: "Medium",
        confidenceScore: 62,
        dataGap: "No",
      });
    }
  }

  return null;
}

/**
 * @param {string} text
 * @param {{ sourceTitle?: string, sourceUrl?: string, sourceType?: string, metaDescription?: string, localFilePath?: string }} sourceMeta
 */
export function extractOperatorFactsFromText(text, sourceMeta = {}) {
  const raw = nz(text);
  const fields = listOperatorFieldsForExtraction();
  const out = [];
  const anchor =
    sourceMeta.localFilePath ||
    sourceMeta.sourceUrl ||
    sourceMeta.sourceTitle ||
    "document";

  for (const field of fields) {
    let candidate =
      trySpecializedExtract(field, raw, sourceMeta, anchor) ||
      tryPatternExtract(field, raw, anchor) ||
      tryContextExtract(field, raw, anchor);

    if (!candidate) {
      candidate = gapFact(
        field,
        field.allowGapCopy === false
          ? `Source required for ${field.displayLabel}.`
          : undefined
      );
    }

    out.push(candidate);
  }

  return out;
}

export { PARTNER_INTELLIGENCE_GAP_COPY };
