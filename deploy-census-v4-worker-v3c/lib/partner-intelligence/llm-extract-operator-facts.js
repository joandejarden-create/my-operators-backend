/**
 * LLM-based operator fact extraction with mandatory evidence quotes.
 * Requires OPENAI_API_KEY. Facts remain Pending until human review.
 */
import { PARTNER_INTELLIGENCE_GAP_COPY } from "../../api/lib/partner-intelligence-field-map.js";
import { evidenceAppearsInSource } from "./merge-extraction-candidates.js";
import { extractOperatorFactsFromText } from "./operator-extract-rules.js";

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function isLlmExtractionEnabled() {
  return (
    process.env.PARTNER_INTELLIGENCE_LLM_EXTRACTION_ENABLED === "1" &&
    !!nz(process.env.OPENAI_API_KEY)
  );
}

function groupFieldsByTab(fields) {
  const groups = new Map();
  for (const f of fields) {
    const tab = f.explorerTab || "Other";
    if (!groups.has(tab)) groups.set(tab, []);
    groups.get(tab).push(f);
  }
  return groups;
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function truncateSource(text, max = 90000) {
  const t = nz(text);
  if (t.length <= max) return t;
  return t.slice(0, max) + "\n\n[…truncated for model context…]";
}

async function callOpenAiJson(system, user) {
  const model = process.env.PARTNER_INTELLIGENCE_LLM_MODEL || "gpt-4o-mini";
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      temperature: 0.1,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
    }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json.error?.message || `OpenAI API error (${res.status})`);
  }
  const content = json.choices?.[0]?.message?.content;
  if (!content) throw new Error("OpenAI returned empty response.");
  return JSON.parse(content);
}

const SYSTEM_PROMPT = `You extract hotel operator profile facts for Dealality Partner Intelligence.
RULES:
- Use ONLY information explicitly supported by SOURCE TEXT.
- For each field, return extractedValue or null if not clearly supported.
- evidenceQuote MUST be an exact contiguous substring copied from SOURCE TEXT (max 320 characters). Never paraphrase evidence.
- Do not invent numbers, brands, markets, or capabilities.
- For valueType "json", return valid JSON as a string in extractedValue.
- confidenceLevel: High = directly stated; Medium = clearly implied in one sentence; Low = weak mention.
- extractionType: "Directly Stated" or "Inferred from Context" (never guess).`;

/**
 * @param {string} sourceText
 * @param {object[]} fields — registry fields for this batch
 * @param {object} sourceMeta
 */
async function llmExtractBatch(sourceText, fields, sourceMeta) {
  const fieldSpec = fields.map((f) => ({
    fieldKey: f.fieldKey,
    displayLabel: f.displayLabel,
    explorerSection: f.explorerSection,
    valueType: f.valueType || "text",
  }));

  const userPrompt = `SOURCE: ${sourceMeta.sourceTitle || "document"}
SOURCE ROLE: ${sourceMeta.sourceRole || "any"}

FIELDS:
${JSON.stringify(fieldSpec, null, 2)}

SOURCE TEXT:
${truncateSource(sourceText)}

Return JSON:
{
  "facts": [
    {
      "fieldKey": "…",
      "extractedValue": "… or null",
      "evidenceQuote": "exact substring or empty if null",
      "confidenceLevel": "High|Medium|Low",
      "extractionType": "Directly Stated|Inferred from Context"
    }
  ]
}`;

  const parsed = await callOpenAiJson(SYSTEM_PROMPT, userPrompt);
  const facts = Array.isArray(parsed.facts) ? parsed.facts : [];
  const fieldByKey = new Map(fields.map((f) => [f.fieldKey, f]));
  const out = [];

  for (const row of facts) {
    const field = fieldByKey.get(row.fieldKey);
    if (!field) continue;
    const value = row.extractedValue == null ? "" : String(row.extractedValue).trim();
    const evidence = nz(row.evidenceQuote);
    if (!value || value.toLowerCase() === "null") {
      out.push(gapCandidate(field, sourceMeta));
      continue;
    }
    const evidenceValid = evidenceAppearsInSource(evidence, sourceText);
    if (!evidenceValid) {
      out.push(gapCandidate(field, sourceMeta, "Evidence quote not found in source — rejected by validator."));
      continue;
    }
    const confLevel = ["High", "Medium", "Low"].includes(row.confidenceLevel) ? row.confidenceLevel : "Medium";
    const confScore = confLevel === "High" ? 82 : confLevel === "Medium" ? 68 : 52;
    out.push({
      fieldKey: field.fieldKey,
      explorerSection: field.explorerSection,
      displayLabel: field.displayLabel,
      extractedValue: value,
      normalizedValue: value,
      evidenceText: evidence,
      pageSectionAnchor: sourceMeta.pageSectionAnchor || sourceMeta.sourceTitle || "",
      extractionType: row.extractionType === "Inferred from Context" ? "Inferred from Context" : "Directly Stated",
      confidenceLevel: confLevel,
      confidenceScore: confScore,
      dataGap: "No",
      valueType: field.valueType || "text",
      _extractor: "llm",
      _evidenceValid: true,
    });
  }

  for (const field of fields) {
    if (!out.some((f) => f.fieldKey === field.fieldKey)) {
      out.push(gapCandidate(field, sourceMeta));
    }
  }

  return out;
}

function gapCandidate(field, sourceMeta, followUp) {
  return {
    fieldKey: field.fieldKey,
    explorerSection: field.explorerSection,
    displayLabel: field.displayLabel,
    extractedValue: PARTNER_INTELLIGENCE_GAP_COPY,
    normalizedValue: PARTNER_INTELLIGENCE_GAP_COPY,
    evidenceText: PARTNER_INTELLIGENCE_GAP_COPY,
    pageSectionAnchor: sourceMeta.pageSectionAnchor || "",
    extractionType: "Needs Confirmation",
    confidenceLevel: "Low",
    confidenceScore: 20,
    dataGap: "Yes",
    followUpQuestion: followUp || `Confirm ${field.displayLabel} with operator or additional source.`,
    valueType: field.valueType || "text",
    _extractor: "llm",
    _evidenceValid: true,
  };
}

/**
 * @param {string} sourceText
 * @param {object[]} fields — filtered registry fields
 * @param {object} sourceMeta
 */
export async function extractOperatorFactsWithLlm(sourceText, fields, sourceMeta = {}) {
  if (!isLlmExtractionEnabled()) {
    const rules = extractOperatorFactsFromText(sourceText, sourceMeta);
    return {
      usedLlm: false,
      extractor: "rules",
      facts: rules.map((f) => ({ ...f, _extractor: "rules", _evidenceValid: true })),
    };
  }

  const groups = groupFieldsByTab(fields);
  const all = [];
  const batchSize = Number(process.env.PARTNER_INTELLIGENCE_LLM_BATCH_SIZE) || 12;

  for (const [, tabFields] of groups) {
    for (const batch of chunk(tabFields, batchSize)) {
      const batchFacts = await llmExtractBatch(sourceText, batch, sourceMeta);
      all.push(...batchFacts);
    }
  }

  return { usedLlm: true, extractor: "llm", facts: all };
}
