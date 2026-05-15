/**
 * FDD Terms & Obligations — parallel to fee rows (api/fdd-intelligence.js).
 * Non-fee legal/commercial terms; separate Airtable table + memory store.
 */

import axios from "axios";
import Airtable from "airtable";
import {
  segmentFddByItems,
  resolveFullTextForExtraction,
  splitSectionTextIntoChunks,
  loadFddDocumentByIdForTerms,
} from "./fdd-intelligence.js";
import {
  TERM_EXTRACTION_TARGET_SET,
  documentationReferenceFromSection,
  formatSectionPromptLabel,
} from "./fdd-intelligence-section-targets.js";
import {
  auditFddTerms,
  FDD_TERM_AUDIT_VERSION,
  termBulkApproveBlockedReason,
  TERM_LEGAL_SENSITIVE_CATEGORIES,
} from "./fdd-intelligence-term-audit.js";

const TERMS_TABLE = process.env.AIRTABLE_TABLE_FDD_TERMS || "";
const FDD_MODEL_API_KEY = process.env.FDD_INTELLIGENCE_MODEL_API_KEY || "";
const FDD_MODEL_API_URL = process.env.FDD_INTELLIGENCE_MODEL_API_URL || "https://api.openai.com/v1/chat/completions";
const FDD_MODEL_NAME = process.env.FDD_INTELLIGENCE_MODEL_NAME || "gpt-4o-mini";
const FDD_CHUNK_MAX_CHARS = parseInt(process.env.FDD_INTELLIGENCE_CHUNK_MAX_CHARS || "28000", 10) || 28000;
const FDD_CHUNK_OVERLAP_CHARS = parseInt(process.env.FDD_INTELLIGENCE_CHUNK_OVERLAP_CHARS || "800", 10) || 800;
const FDD_SECTION_TEXT_MAX_CHARS = 120000;

const TERM_CATEGORIES = new Set([
  "Territory / Area Protection",
  "Franchise Term",
  "Renewal Rights",
  "Transfer / Change of Ownership",
  "Termination / Default",
  "Liquidated Damages",
  "Post-Termination Obligations",
  "PIP / Renovation / Brand Standards",
  "Required Systems / Technology",
  "Training / Staffing / Operator Requirements",
  "Reporting / Audit / Records",
  "Approved Suppliers / Procurement",
  "Insurance / Indemnification",
  "Financial Performance Representation",
  "System Health / Outlets",
  "Dispute Resolution / Governing Law",
  "Other / Needs Review",
]);

const RISK_LEVELS = new Set(["Low", "Medium", "High", "Unclear"]);
const FLEX_LEVELS = new Set(["Low", "Medium", "High", "Unclear"]);
const NEGOTIABILITY = new Set(["Low", "Medium", "High", "Unclear"]);
const REVIEW_STATUSES = new Set(["Needs Review", "Approved", "Rejected"]);

const termsById = new Map();

function needsTermAuditBackfill(t) {
  const v = String((t && t.auditVersion) || "").trim();
  if (v !== FDD_TERM_AUDIT_VERSION) return true;
  if (String(t.normalizedTermBucket || "").trim() === "") return true;
  if (t.termAuditScore == null || Number.isNaN(Number(t.termAuditScore))) return true;
  return false;
}

/** Duplicate detection is per document + extraction run; run audit in document batches when the caller has mixed documents. */
function auditFddTermsForDocumentGroups(terms) {
  const m = new Map();
  for (const t of terms || []) {
    const k = String(t.fddDocumentId || "");
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(t);
  }
  for (const g of m.values()) auditFddTerms(g);
}

function numOrUndef(v) {
  if (v == null || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function summarizeTermAuditResults(terms) {
  const byStatus = {};
  let legalReviewRequired = 0;
  let autoApproveEligible = 0;
  let duplicate = 0;
  for (const t of terms || []) {
    const st = String(t.termAuditStatus || "").trim() || "(none)";
    byStatus[st] = (byStatus[st] || 0) + 1;
    if (t.legalReviewRequired) legalReviewRequired += 1;
    if (t.autoApproveEligible) autoApproveEligible += 1;
    if (t.possibleDuplicateTerm) duplicate += 1;
  }
  return { byStatus, legalReviewRequired, autoApproveEligible, duplicate };
}

async function persistAuditedTermsInPlace(terms) {
  if (!terms.length) return;
  if (isTermsAirtableEnabled()) {
    for (const t of terms) {
      await atUpdateTerm(t);
    }
  } else {
    memoryUpsertTerms(terms);
  }
}

function isTermsAirtableEnabled() {
  return !!(process.env.AIRTABLE_API_KEY && process.env.AIRTABLE_BASE_ID && TERMS_TABLE.trim());
}

function getBase() {
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

function escapeAtFormula(s) {
  if (s == null) return "";
  return String(s).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

function nowIso() {
  return new Date().toISOString();
}

function isPlaceholderSectionText(text) {
  const t = String(text || "");
  return !t.trim() || t.includes("[No extracted text yet");
}

function stripMarkdownJsonFence(text) {
  let t = String(text || "").trim();
  if (t.startsWith("```")) {
    t = t.replace(/^```(?:json)?\s*/i, "");
    const idx = t.lastIndexOf("```");
    if (idx !== -1) t = t.slice(0, idx).trim();
  }
  return t;
}

async function callTermsModelJson({ system, user }) {
  if (!FDD_MODEL_API_KEY.trim()) throw new Error("FDD_INTELLIGENCE_MODEL_API_KEY not set");
  const { status, data } = await axios.post(
    FDD_MODEL_API_URL,
    {
      model: FDD_MODEL_NAME,
      temperature: 0.1,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
    },
    {
      headers: {
        Authorization: `Bearer ${FDD_MODEL_API_KEY}`,
        "Content-Type": "application/json",
      },
      timeout: 180000,
      validateStatus: () => true,
    }
  );
  if (status < 200 || status >= 300) {
    const msg = (data && data.error && data.error.message) || `HTTP ${status} from chat API`;
    throw new Error(msg);
  }
  if (!data || !Array.isArray(data.choices) || !data.choices[0] || !data.choices[0].message) {
    throw new Error((data && data.error && data.error.message) || "Invalid chat completion response");
  }
  return data.choices[0].message.content || "";
}

function parseTermsJson(content) {
  const stripped = stripMarkdownJsonFence(content);
  const parsed = JSON.parse(stripped);
  if (parsed && Array.isArray(parsed.terms)) return parsed.terms;
  if (Array.isArray(parsed)) return parsed;
  throw new Error('Model JSON must be { "terms": [ ... ] }');
}

function buildTermsSystemPrompt() {
  return [
    "You extract non-fee legal and commercial franchise terms from franchise disclosure document text (including U.S. FTC FDD format and other national formats).",
    "Do NOT output ordinary recurring fees, initial fees, or royalty line items — those belong in a separate fee table.",
    "Return JSON ONLY — no prose, no markdown fences. Single JSON object with key \"terms\" whose value is an array.",
    "Each term object must use these keys:",
    "termObligationName, termCategory, termSummary, ownerImpact, requiredConditionalOptional, trigger, appliesWhen,",
    "riskLevel, flexibilityLevel, negotiability, legalReviewRequired (boolean), commercialReviewRequired (boolean),",
    "sourceTextExcerpt, documentationReference, documentationReferencePageNumber, confidence, reviewStatus.",
    "termCategory must be one of:",
    "Territory / Area Protection | Franchise Term | Renewal Rights | Transfer / Change of Ownership |",
    "Termination / Default | Liquidated Damages | Post-Termination Obligations | PIP / Renovation / Brand Standards |",
    "Required Systems / Technology | Training / Staffing / Operator Requirements | Reporting / Audit / Records |",
    "Approved Suppliers / Procurement | Insurance / Indemnification | Financial Performance Representation |",
    "System Health / Outlets | Dispute Resolution / Governing Law | Other / Needs Review.",
    "riskLevel, flexibilityLevel, negotiability must each be one of: Low | Medium | High | Unclear.",
    "reviewStatus must be Needs Review for every row (operator approves later).",
    "confidence must be High | Medium | Low | Unclear.",
    "requiredConditionalOptional should be Required, Conditional, Optional, or Unclear when possible.",
    "Use short verbatim excerpts for sourceTextExcerpt when possible.",
    "When uncertain about category or risk, use Unclear rather than guessing.",
    "Extract territory protections and carve-outs; renewal and transfer approval requirements; termination/default triggers;",
    "liquidated damages exposure; post-termination covenants; PIP/brand standards; required systems and training obligations;",
    "supplier restrictions; reporting/audit; Item 19 FPR presence and limitations; Item 20 outlet counts or trends when stated.",
  ].join(" ");
}

function buildTermsUserPrompt({ itemNumber, itemTitle, sectionText, document, chunkIndex, chunkCount, sourceFormat, extractionTarget, promptLabel }) {
  const isUsItemFormat =
    String(sourceFormat || "us_fdd_item") === "us_fdd_item" &&
    (/^\d{1,2}$/.test(String(itemNumber || "").trim()) || /^ex$/i.test(String(itemNumber || "").trim()));
  const head = isUsItemFormat
    ? `FDD Item ${itemNumber}: ${itemTitle}`
    : `Franchise disclosure section: ${promptLabel || `${itemNumber}: ${itemTitle}`}\nInternal extraction target: ${extractionTarget || "unclear"}`;
  const header = `${head}\nBrand context: brandName=${document.brandName}, fddYear=${document.fddYear}, country=${document.country || "US"}.`;
  const chunkNote =
    chunkCount > 1
      ? `\nSegment chunk ${chunkIndex + 1} of ${chunkCount}. Only extract terms evidenced in this chunk.`
      : "";
  const body = String(sectionText || "").slice(0, FDD_SECTION_TEXT_MAX_CHARS);
  return `${header}${chunkNote}\n\n--- SECTION TEXT START ---\n${body}\n--- SECTION TEXT END ---\n\nReturn strictly: { \"terms\": [ ... ] }`;
}

function defaultLegalReviewForTerm(termCategory, sourceItemNumber) {
  const cat = String(termCategory || "");
  const item = String(sourceItemNumber || "");
  if (item === "17" || /^xvii$/i.test(item)) return true;
  const legalCats = [
    "Termination / Default",
    "Liquidated Damages",
    "Insurance / Indemnification",
    "Dispute Resolution / Governing Law",
    "Post-Termination Obligations",
    "Transfer / Change of Ownership",
  ];
  if (legalCats.some((x) => cat.includes(x.split(" / ")[0]) || cat === x)) return true;
  if (/dispute|governing law|termination|liquidated|indemnif|insurance/i.test(cat)) return true;
  return false;
}

function normalizeTermCategory(raw) {
  const s = String(raw || "").trim();
  if (TERM_CATEGORIES.has(s)) return s;
  return "Other / Needs Review";
}

function normalizeEnum(raw, allowed, fallback) {
  const s = String(raw || "").trim();
  if (allowed.has(s)) return s;
  return fallback;
}

function normalizeTermFromModel(raw, document, meta) {
  const termCategory = normalizeTermCategory(raw.termCategory);
  const legalDefault = defaultLegalReviewForTerm(termCategory, meta.sourceItemNumber);
  const legal =
    typeof raw.legalReviewRequired === "boolean" ? raw.legalReviewRequired : legalDefault;
  const commercial =
    typeof raw.commercialReviewRequired === "boolean" ? raw.commercialReviewRequired : true;
  return {
    id: newId("fddterm"),
    fddDocumentId: document.id,
    parentCompany: document.parentCompany || "",
    brandName: document.brandName || "",
    fddYear: Number.isFinite(Number(document.fddYear)) ? Number(document.fddYear) : new Date().getFullYear(),
    country: document.country || "US",
    sourceItemNumber: meta.sourceItemNumber,
    sourceItemTitle: meta.sourceItemTitle,
    termObligationName: String(raw.termObligationName || "Unclear").slice(0, 500),
    termCategory,
    termSummary: String(raw.termSummary || "Unclear").slice(0, 8000),
    ownerImpact: String(raw.ownerImpact || "Unclear").slice(0, 8000),
    requiredConditionalOptional: String(raw.requiredConditionalOptional || "Unclear").slice(0, 200),
    trigger: String(raw.trigger || "Unclear").slice(0, 2000),
    appliesWhen: String(raw.appliesWhen || "Unclear").slice(0, 8000),
    riskLevel: normalizeEnum(raw.riskLevel, RISK_LEVELS, "Unclear"),
    flexibilityLevel: normalizeEnum(raw.flexibilityLevel, FLEX_LEVELS, "Unclear"),
    negotiability: normalizeEnum(raw.negotiability, NEGOTIABILITY, "Unclear"),
    legalReviewRequired: !!legal,
    commercialReviewRequired: commercial !== false,
    sourceTextExcerpt: String(raw.sourceTextExcerpt || "").slice(0, 8000),
    documentationReference: String(
      raw.documentationReference || meta.documentationReferenceHint || `Item ${meta.sourceItemNumber}`
    ).slice(0, 500),
    documentationReferencePageNumber: String(raw.documentationReferencePageNumber || "Unclear").slice(0, 200),
    confidence: normalizeEnum(raw.confidence, new Set(["High", "Medium", "Low", "Unclear"]), "Medium"),
    reviewStatus: REVIEW_STATUSES.has(String(raw.reviewStatus || "").trim())
      ? String(raw.reviewStatus).trim()
      : "Needs Review",
    reviewerNotes: String(raw.reviewerNotes || "").slice(0, 8000),
    extractionRunId: meta.extractionRunId,
    extractionUsedAi: meta.extractionUsedAi,
    modelNameUsed: meta.modelNameUsed || "",
    sourceChunkIndex: String(meta.sourceChunkIndex ?? 0),
    sourceChunkCount: String(meta.sourceChunkCount ?? 1),
    createdAt: nowIso(),
    updatedAt: nowIso(),
  };
}

function placeholderTerms(document, sectionList, extractionRunId) {
  const runId = extractionRunId || newId("fddtermext");
  const out = [];
  const pairs = [
    { target: "territory", fallbackNum: "12" },
    { target: "renewal_transfer_termination", fallbackNum: "17" },
  ];
  for (const { target, fallbackNum } of pairs) {
    const sec =
      sectionList.find((s) => s.extractionTarget === target) ||
      sectionList.find((s) => String(s.itemNumber) === fallbackNum);
    const excerpt = (sec && sec.sectionText ? sec.sectionText : "").slice(0, 400);
    const sourceNum = sec ? String(sec.sourceSectionLabel || sec.itemNumber) : fallbackNum;
    const sourceTitle = sec ? String(sec.sourceSectionHeading || sec.itemTitle || "") : `Item ${fallbackNum}`;
    const docRef = sec ? documentationReferenceFromSection(sec) : `Item ${fallbackNum}`;
    out.push({
      id: newId("fddterm"),
      fddDocumentId: document.id,
      parentCompany: document.parentCompany || "",
      brandName: document.brandName || "",
      fddYear: Number(document.fddYear) || new Date().getFullYear(),
      country: document.country || "US",
      sourceItemNumber: sourceNum,
      sourceItemTitle: sourceTitle || `Item ${fallbackNum}`,
      termObligationName:
        target === "territory" ? "Territory (placeholder — configure AI)" : "Renewal / termination (placeholder — configure AI)",
      termCategory: target === "territory" ? "Territory / Area Protection" : "Renewal Rights",
      termSummary: "AI not configured; add FDD_INTELLIGENCE_MODEL_API_KEY and re-run Extract terms.",
      ownerImpact: "Unclear",
      requiredConditionalOptional: "Unclear",
      trigger: "Unclear",
      appliesWhen: "Unclear",
      riskLevel: "Unclear",
      flexibilityLevel: "Unclear",
      negotiability: "Unclear",
      legalReviewRequired: target === "renewal_transfer_termination" || /^xvii$/i.test(sourceNum) || sourceNum === "17",
      commercialReviewRequired: true,
      sourceTextExcerpt: excerpt || "Unclear",
      documentationReference: docRef,
      documentationReferencePageNumber: "Unclear",
      confidence: "Low",
      reviewStatus: "Needs Review",
      reviewerNotes: "",
      extractionRunId: runId,
      extractionUsedAi: false,
      modelNameUsed: "placeholder",
      sourceChunkIndex: "0",
      sourceChunkCount: "1",
      createdAt: nowIso(),
      updatedAt: nowIso(),
    });
  }
  return { terms: out, usedAi: false, warnings: ["FDD_INTELLIGENCE_MODEL_API_KEY not set — placeholder terms only."] };
}

export async function runTermsAiExtraction(document, sectionList, extractionRunId, onProgress) {
  return extractTermsWithAi(document, sectionList, extractionRunId, onProgress);
}

async function extractTermsWithAi(document, sectionList, extractionRunId, onProgress) {
  const runId = extractionRunId || newId("fddtermext");
  const all = [];
  const warnings = [];
  if (!FDD_MODEL_API_KEY.trim()) {
    const termScope = (sectionList || []).filter(
      (s) => s && s.itemNumber !== "FULL" && s.extractionTarget && TERM_EXTRACTION_TARGET_SET.has(s.extractionTarget)
    );
    if (typeof onProgress === "function") {
      onProgress({
        stage: "terms",
        message:
          termScope.length > 0
            ? `Terms: placeholder mode (${termScope.length} term section(s) in scope)`
            : "Terms: placeholder mode (no term-target sections)",
        index: termScope.length ? 1 : 0,
        total: termScope.length ? 1 : 0,
        label: "Placeholder",
      });
    }
    return placeholderTerms(document, sectionList, runId);
  }

  const termSections = (sectionList || []).filter(
    (s) => s && s.itemNumber !== "FULL" && s.extractionTarget && TERM_EXTRACTION_TARGET_SET.has(s.extractionTarget)
  );

  for (let si = 0; si < termSections.length; si++) {
    const sec = termSections[si];
    const termSecIdx = si + 1;
    const sourceLabel = sec.sourceSectionLabel != null ? String(sec.sourceSectionLabel) : String(sec.itemNumber);
    const itemTitle = (sec.sourceSectionHeading || sec.itemTitle || "").trim() || `Section ${sourceLabel}`;
    const itemNumber = sourceLabel;
    const sectionText = sec.sectionText || "";
    const itemLabel = formatSectionPromptLabel(sec);
    const docRefHint = documentationReferenceFromSection(sec);
    if (typeof onProgress === "function") {
      onProgress({
        stage: "terms",
        message: `Terms: section ${termSecIdx}/${termSections.length} · ${itemLabel}`.slice(0, 300),
        index: termSecIdx,
        total: termSections.length,
        label: itemLabel,
      });
    }
    if (isPlaceholderSectionText(sectionText)) {
      warnings.push(`${itemLabel}: skipped (no segmented text).`);
      continue;
    }
    try {
      const chunks = splitSectionTextIntoChunks(sectionText, FDD_CHUNK_MAX_CHARS, FDD_CHUNK_OVERLAP_CHARS);
      for (const ch of chunks) {
        try {
          const content = await callTermsModelJson({
            system: buildTermsSystemPrompt(),
            user: buildTermsUserPrompt({
              itemNumber,
              itemTitle,
              sectionText: ch.chunkText,
              document,
              chunkIndex: ch.chunkIndex,
              chunkCount: ch.chunkCount,
              sourceFormat: sec.sourceFormat,
              extractionTarget: sec.extractionTarget,
              promptLabel: itemLabel,
            }),
          });
          const rawTerms = parseTermsJson(content);
          for (const raw of rawTerms) {
            if (!raw || typeof raw !== "object") continue;
            all.push(
              normalizeTermFromModel(raw, document, {
                sourceItemNumber: itemNumber,
                sourceItemTitle: itemTitle,
                extractionRunId: runId,
                extractionUsedAi: true,
                modelNameUsed: FDD_MODEL_NAME,
                sourceChunkIndex: ch.chunkIndex,
                sourceChunkCount: ch.chunkCount,
                documentationReferenceHint: docRefHint,
              })
            );
          }
        } catch (e) {
          warnings.push(`${itemLabel} chunk ${ch.chunkIndex + 1}/${ch.chunkCount}: ${e.message || e}`);
        }
      }
    } catch (e) {
      warnings.push(`${itemLabel}: ${e.message || e}`);
    }
  }

  if (!all.length) {
    const ph = placeholderTerms(document, sectionList, runId);
    return { terms: ph.terms, usedAi: true, warnings: [...warnings, "No AI terms produced; placeholders returned."] };
  }
  return { terms: all, usedAi: true, warnings };
}

function termToAirtableFields(t) {
  const o = {
    "FDD Document ID": t.fddDocumentId || "",
    "Parent Company": t.parentCompany || "",
    "Brand Name": t.brandName || "",
    "FDD Year": t.fddYear,
    Country: t.country || "US",
    "Source Item Number": t.sourceItemNumber || "",
    "Source Item Title": t.sourceItemTitle || "",
    "Term / Obligation Name": t.termObligationName || "",
    "Term Category": t.termCategory || "",
    "Term Summary": t.termSummary || "",
    "Owner Impact": t.ownerImpact || "",
    "Required / Conditional / Optional": t.requiredConditionalOptional || "",
    Trigger: t.trigger || "",
    "Applies When": t.appliesWhen || "",
    "Risk Level": t.riskLevel || "Unclear",
    "Flexibility Level": t.flexibilityLevel || "Unclear",
    Negotiability: t.negotiability || "Unclear",
    "Legal Review Required": !!t.legalReviewRequired,
    "Commercial Review Required": t.commercialReviewRequired !== false,
    "Source Text Excerpt": t.sourceTextExcerpt || "",
    "Documentation Reference": t.documentationReference || "",
    "Documentation Reference Page Number": t.documentationReferencePageNumber || "",
    Confidence: t.confidence || "Medium",
    "Review Status": t.reviewStatus || "Needs Review",
    "Reviewer Notes": t.reviewerNotes || "",
    "Extraction Run ID": t.extractionRunId || "",
    "Extraction Used AI": !!t.extractionUsedAi,
    "Model Name Used": t.modelNameUsed || "",
    "Source Chunk Index": t.sourceChunkIndex ?? "0",
    "Source Chunk Count": t.sourceChunkCount ?? "1",
    "Normalized Term Bucket": t.normalizedTermBucket || "",
    "Comparable Term Group": t.comparableTermGroup || "",
    "Possible Duplicate Term": !!t.possibleDuplicateTerm,
    "Duplicate Term Group Key": t.duplicateTermGroupKey || "",
    "Term Audit Score": numOrUndef(t.termAuditScore) ?? 0,
    "Term Audit Confidence": t.termAuditConfidence || "",
    "Term Audit Status": t.termAuditStatus || "",
    "Term Audit Issues": t.termAuditIssues || "",
    "Auto-Approve Eligible": !!t.autoApproveEligible,
    "Last Audited At": t.lastAuditedAt || "",
    "Audit Version": t.auditVersion || "",
    "Source Support Score": numOrUndef(t.sourceSupportScore) ?? 0,
    "Category Quality Score": numOrUndef(t.categoryQualityScore) ?? 0,
    "Risk Quality Score": numOrUndef(t.riskQualityScore) ?? 0,
    "Owner Impact Score": numOrUndef(t.ownerImpactScore) ?? 0,
    "Legal Sensitivity Score": numOrUndef(t.legalSensitivityScore) ?? 0,
    "Created At": t.createdAt || nowIso(),
    "Updated At": t.updatedAt || nowIso(),
  };
  return o;
}

function termFromAirtable(rec) {
  const f = rec.fields || {};
  return {
    id: rec.id,
    fddDocumentId: f["FDD Document ID"] || "",
    parentCompany: f["Parent Company"] || "",
    brandName: f["Brand Name"] || "",
    fddYear: typeof f["FDD Year"] === "number" ? f["FDD Year"] : parseInt(String(f["FDD Year"] || ""), 10) || new Date().getFullYear(),
    country: f["Country"] || "US",
    sourceItemNumber: f["Source Item Number"] != null ? String(f["Source Item Number"]) : "",
    sourceItemTitle: f["Source Item Title"] || "",
    termObligationName: f["Term / Obligation Name"] || "",
    termCategory: f["Term Category"] || "",
    termSummary: f["Term Summary"] || "",
    ownerImpact: f["Owner Impact"] || "",
    requiredConditionalOptional: f["Required / Conditional / Optional"] || "",
    trigger: f["Trigger"] || "",
    appliesWhen: f["Applies When"] || "",
    riskLevel: f["Risk Level"] || "Unclear",
    flexibilityLevel: f["Flexibility Level"] || "Unclear",
    negotiability: f["Negotiability"] || "Unclear",
    legalReviewRequired: !!f["Legal Review Required"],
    commercialReviewRequired:
      f["Commercial Review Required"] === undefined || f["Commercial Review Required"] === null
        ? true
        : !!f["Commercial Review Required"],
    sourceTextExcerpt: f["Source Text Excerpt"] || "",
    documentationReference: f["Documentation Reference"] || "",
    documentationReferencePageNumber: f["Documentation Reference Page Number"] || "",
    confidence: f["Confidence"] || "Medium",
    reviewStatus: f["Review Status"] || "Needs Review",
    reviewerNotes: f["Reviewer Notes"] || "",
    extractionRunId: f["Extraction Run ID"] || "",
    extractionUsedAi: !!f["Extraction Used AI"],
    modelNameUsed: f["Model Name Used"] || "",
    sourceChunkIndex: f["Source Chunk Index"] != null ? String(f["Source Chunk Index"]) : "0",
    sourceChunkCount: f["Source Chunk Count"] != null ? String(f["Source Chunk Count"]) : "1",
    normalizedTermBucket: f["Normalized Term Bucket"] || "",
    comparableTermGroup: f["Comparable Term Group"] || "",
    possibleDuplicateTerm: !!f["Possible Duplicate Term"],
    duplicateTermGroupKey: f["Duplicate Term Group Key"] || "",
    termAuditScore: typeof f["Term Audit Score"] === "number" ? f["Term Audit Score"] : parseInt(String(f["Term Audit Score"] || ""), 10) || 0,
    termAuditConfidence: f["Term Audit Confidence"] || "",
    termAuditStatus: f["Term Audit Status"] || "",
    termAuditIssues: f["Term Audit Issues"] || "",
    autoApproveEligible: !!f["Auto-Approve Eligible"],
    lastAuditedAt: f["Last Audited At"] || "",
    auditVersion: f["Audit Version"] || "",
    sourceSupportScore:
      typeof f["Source Support Score"] === "number" ? f["Source Support Score"] : parseInt(String(f["Source Support Score"] || ""), 10) || 0,
    categoryQualityScore:
      typeof f["Category Quality Score"] === "number" ? f["Category Quality Score"] : parseInt(String(f["Category Quality Score"] || ""), 10) || 0,
    riskQualityScore:
      typeof f["Risk Quality Score"] === "number" ? f["Risk Quality Score"] : parseInt(String(f["Risk Quality Score"] || ""), 10) || 0,
    ownerImpactScore:
      typeof f["Owner Impact Score"] === "number" ? f["Owner Impact Score"] : parseInt(String(f["Owner Impact Score"] || ""), 10) || 0,
    legalSensitivityScore:
      typeof f["Legal Sensitivity Score"] === "number"
        ? f["Legal Sensitivity Score"]
        : parseInt(String(f["Legal Sensitivity Score"] || ""), 10) || 0,
    createdAt: f["Created At"] || nowIso(),
    updatedAt: f["Updated At"] || nowIso(),
  };
}

async function atDeleteTermsForDoc(docId) {
  if (!isTermsAirtableEnabled()) return;
  const base = getBase();
  const idEsc = escapeAtFormula(docId);
  const recs = await base(TERMS_TABLE).select({ filterByFormula: `{FDD Document ID}='${idEsc}'`, pageSize: 100 }).all();
  for (const r of recs) await r.destroy();
}

async function atInsertTerms(rows) {
  const base = getBase();
  const out = [];
  for (const row of rows) {
    const [rec] = await base(TERMS_TABLE).create([{ fields: termToAirtableFields(row) }]);
    out.push(termFromAirtable(rec));
  }
  return out;
}

async function atListTermsForDoc(docId) {
  if (!isTermsAirtableEnabled()) return [];
  const base = getBase();
  const idEsc = escapeAtFormula(docId);
  const recs = await base(TERMS_TABLE).select({ filterByFormula: `{FDD Document ID}='${idEsc}'`, pageSize: 100 }).all();
  return recs.map(termFromAirtable);
}

async function atGetTerm(recordId) {
  if (!isTermsAirtableEnabled()) return null;
  try {
    const base = getBase();
    const rec = await base(TERMS_TABLE).find(recordId);
    return termFromAirtable(rec);
  } catch {
    return null;
  }
}

async function atUpdateTerm(row) {
  const base = getBase();
  row.updatedAt = nowIso();
  const [rec] = await base(TERMS_TABLE).update([{ id: row.id, fields: termToAirtableFields(row) }]);
  return termFromAirtable(rec);
}

function memoryListTermsForDoc(docId) {
  return [...termsById.values()].filter((t) => t.fddDocumentId === docId);
}

function memoryDeleteTermsForDoc(docId) {
  for (const [id, t] of termsById) {
    if (t.fddDocumentId === docId) termsById.delete(id);
  }
}

function memoryUpsertTerms(rows) {
  for (const r of rows) termsById.set(r.id, { ...r });
}

function memoryGetTerm(id) {
  return termsById.get(id) || null;
}

function memoryUpdateTerm(row) {
  row.updatedAt = nowIso();
  termsById.set(row.id, { ...row });
  return row;
}

async function listTermsForDocument(docId) {
  if (isTermsAirtableEnabled()) return atListTermsForDoc(docId);
  return memoryListTermsForDoc(docId);
}

/** GET /api/fdd-intelligence/documents/:id/terms */
export async function listFddDocumentTerms(req, res) {
  try {
    const { id } = req.params;
    const doc = await loadFddDocumentByIdForTerms(id);
    if (!doc) return res.status(404).json({ success: false, error: "Document not found" });
    const terms = await listTermsForDocument(id);
    if (terms.some(needsTermAuditBackfill)) {
      auditFddTermsForDocumentGroups(terms);
      await persistAuditedTermsInPlace(terms);
    }
    return res.json({
      success: true,
      storage: isTermsAirtableEnabled() ? "airtable" : "memory",
      documentId: id,
      terms,
    });
  } catch (e) {
    console.error("[fdd-intelligence-terms] list:", e);
    res.status(500).json({ success: false, error: e.message || "List failed" });
  }
}

/** Replace all terms for a document (same behavior as POST extract-terms persistence). */
export async function persistTermsRowsForDocument(docId, terms) {
  auditFddTermsForDocumentGroups(terms);
  if (isTermsAirtableEnabled()) {
    await atDeleteTermsForDoc(docId);
    return await atInsertTerms(terms);
  }
  memoryDeleteTermsForDoc(docId);
  memoryUpsertTerms(terms);
  return terms;
}

/** POST /api/fdd-intelligence/documents/:id/extract-terms */
export async function postFddExtractTerms(req, res) {
  const { id } = req.params;
  try {
    const doc = await loadFddDocumentByIdForTerms(id);
    if (!doc) return res.status(404).json({ success: false, error: "Document not found" });

    const { fullTextForExtraction } = await resolveFullTextForExtraction(doc);
    if (!String(fullTextForExtraction || "").trim()) {
      return res.status(400).json({ success: false, error: "No full text available for extraction" });
    }

    const sectionList = segmentFddByItems(fullTextForExtraction);
    const extractionRunId = newId("fddtermext");
    const { terms, usedAi, warnings } = await extractTermsWithAi(doc, sectionList, extractionRunId);

    if (isTermsAirtableEnabled()) {
      const saved = await persistTermsRowsForDocument(id, terms);
      return res.json({
        success: true,
        storage: "airtable",
        extractionRunId,
        usedAi,
        warnings,
        terms: saved,
        termCount: saved.length,
      });
    }

    await persistTermsRowsForDocument(id, terms);
    return res.json({
      success: true,
      storage: "memory",
      extractionRunId,
      usedAi,
      warnings,
      terms,
      termCount: terms.length,
    });
  } catch (e) {
    console.error("[fdd-intelligence-terms] extract:", e);
    res.status(500).json({ success: false, error: e.message || "Extract failed" });
  }
}

/** PATCH /api/fdd-intelligence/terms/:termId */
function applyPatchBodyToTerm(row, body) {
  const b = body || {};
  if (b.reviewStatus != null) {
    const rs = String(b.reviewStatus).trim();
    if (REVIEW_STATUSES.has(rs)) row.reviewStatus = rs;
  }
  if (b.reviewerNotes != null) row.reviewerNotes = String(b.reviewerNotes);
  if (b.legalReviewRequired != null) row.legalReviewRequired = !!b.legalReviewRequired;
  if (b.commercialReviewRequired != null) row.commercialReviewRequired = !!b.commercialReviewRequired;
  if (b.termObligationName != null) row.termObligationName = String(b.termObligationName).slice(0, 500);
  if (b.termCategory != null) row.termCategory = normalizeTermCategory(b.termCategory);
  if (b.termSummary != null) row.termSummary = String(b.termSummary).slice(0, 8000);
  if (b.ownerImpact != null) row.ownerImpact = String(b.ownerImpact).slice(0, 8000);
  if (b.requiredConditionalOptional != null) row.requiredConditionalOptional = String(b.requiredConditionalOptional).slice(0, 200);
  if (b.trigger != null) row.trigger = String(b.trigger).slice(0, 2000);
  if (b.appliesWhen != null) row.appliesWhen = String(b.appliesWhen).slice(0, 8000);
  if (b.riskLevel != null) row.riskLevel = normalizeEnum(b.riskLevel, RISK_LEVELS, row.riskLevel);
  if (b.flexibilityLevel != null) row.flexibilityLevel = normalizeEnum(b.flexibilityLevel, FLEX_LEVELS, row.flexibilityLevel);
  if (b.negotiability != null) row.negotiability = normalizeEnum(b.negotiability, NEGOTIABILITY, row.negotiability);
  if (b.sourceTextExcerpt != null) row.sourceTextExcerpt = String(b.sourceTextExcerpt).slice(0, 8000);
  if (b.sourceItemNumber != null) row.sourceItemNumber = String(b.sourceItemNumber).slice(0, 50);
  if (b.documentationReference != null) row.documentationReference = String(b.documentationReference).slice(0, 500);
  if (b.documentationReferencePageNumber != null) {
    row.documentationReferencePageNumber = String(b.documentationReferencePageNumber).slice(0, 200);
  }
  if (b.confidence != null) row.confidence = normalizeEnum(b.confidence, new Set(["High", "Medium", "Low", "Unclear"]), row.confidence);

  if (b.action === "approve") row.reviewStatus = "Approved";
  if (b.action === "reject") row.reviewStatus = "Rejected";
  if (b.action === "needs_review") row.reviewStatus = "Needs Review";
  if (b.action === "toggle_legal") row.legalReviewRequired = !row.legalReviewRequired;
  if (b.action === "toggle_commercial") row.commercialReviewRequired = !row.commercialReviewRequired;
}

function notAutoApproveReason(term) {
  if (String(term.reviewStatus || "").trim() !== "Needs Review") return "not_needs_review";
  if (term.legalReviewRequired) return "legal_review_required";
  if (term.commercialReviewRequired !== false) return "commercial_review_required";
  if (term.possibleDuplicateTerm) return "duplicate";
  const cat = String(term.termCategory || "").trim();
  if (TERM_LEGAL_SENSITIVE_CATEGORIES.has(cat)) return "legal_sensitive_category";
  if (String(term.normalizedTermBucket || "").trim() === "Other / Needs Mapping") return "bucket_other";
  const score = Number(term.termAuditScore ?? 0);
  if (score < 90) return "score_below_90";
  const risk = String(term.riskLevel || "").trim();
  if (risk !== "Low" && risk !== "Medium") return "risk_not_low_medium";
  const excerpt = String(term.sourceTextExcerpt || "").trim();
  if (excerpt.length < 40) return "source_excerpt_weak";
  return "summary_or_impact_weak";
}

export async function patchFddTerm(req, res) {
  const { termId } = req.params;
  try {
    const body = req.body || {};
    const row0 = isTermsAirtableEnabled() ? await atGetTerm(termId) : memoryGetTerm(termId);
    if (!row0) return res.status(404).json({ success: false, error: "Term not found" });
    const docId = row0.fddDocumentId;
    const all = await listTermsForDocument(docId);
    const idx = all.findIndex((t) => String(t.id) === String(termId));
    if (idx < 0) return res.status(404).json({ success: false, error: "Term not found" });
    applyPatchBodyToTerm(all[idx], body);
    auditFddTermsForDocumentGroups(all);
    await persistAuditedTermsInPlace(all);
    return res.json({ success: true, storage: isTermsAirtableEnabled() ? "airtable" : "memory", term: all[idx] });
  } catch (e) {
    console.error("[fdd-intelligence-terms] patch:", e);
    res.status(500).json({ success: false, error: e.message || "Update failed" });
  }
}

/** POST /api/fdd-intelligence/documents/:id/audit-terms */
export async function postFddDocumentAuditTerms(req, res) {
  const { id } = req.params;
  try {
    const doc = await loadFddDocumentByIdForTerms(id);
    if (!doc) return res.status(404).json({ success: false, error: "Document not found" });
    const terms = await listTermsForDocument(id);
    auditFddTermsForDocumentGroups(terms);
    await persistAuditedTermsInPlace(terms);
    const summary = summarizeTermAuditResults(terms);
    return res.json({
      success: true,
      storage: isTermsAirtableEnabled() ? "airtable" : "memory",
      documentId: id,
      summary,
      termCount: terms.length,
    });
  } catch (e) {
    console.error("[fdd-intelligence-terms] audit-terms:", e);
    res.status(500).json({ success: false, error: e.message || "Audit failed" });
  }
}

/** POST /api/fdd-intelligence/documents/:id/approve-auto-eligible-terms */
export async function postFddApproveAutoEligibleTerms(req, res) {
  const { id } = req.params;
  try {
    const doc = await loadFddDocumentByIdForTerms(id);
    if (!doc) return res.status(404).json({ success: false, error: "Document not found" });
    const terms = await listTermsForDocument(id);
    auditFddTermsForDocumentGroups(terms);
    let approvedCount = 0;
    const skipped = [];
    for (const t of terms) {
      if (t.autoApproveEligible) {
        t.reviewStatus = "Approved";
        approvedCount++;
      } else if (String(t.reviewStatus || "").trim() === "Needs Review") {
        skipped.push({ termId: t.id, reason: notAutoApproveReason(t) });
      }
    }
    auditFddTermsForDocumentGroups(terms);
    await persistAuditedTermsInPlace(terms);
    return res.json({
      success: true,
      storage: isTermsAirtableEnabled() ? "airtable" : "memory",
      documentId: id,
      approvedCount,
      skippedCount: skipped.length,
      skipped,
    });
  } catch (e) {
    console.error("[fdd-intelligence-terms] approve-auto-eligible-terms:", e);
    res.status(500).json({ success: false, error: e.message || "Approve failed" });
  }
}

/** POST /api/fdd-intelligence/documents/:id/bulk-update-terms */
export async function postFddBulkUpdateTerms(req, res) {
  const { id } = req.params;
  try {
    const body = req.body || {};
    const termIds = Array.isArray(body.termIds) ? body.termIds.map((x) => String(x)) : [];
    const action = String(body.action || "").trim();
    const reviewer = body.reviewer != null ? String(body.reviewer).trim() : "";
    const allowed = new Set([
      "clear_commercial_review",
      "mark_commercial_review_needed",
      "clear_legal_review",
      "mark_legal_review_needed",
      "approve",
      "reject",
      "needs_review",
    ]);
    if (!termIds.length) return res.status(400).json({ success: false, error: "termIds array required" });
    if (!allowed.has(action)) return res.status(400).json({ success: false, error: "Invalid action" });

    const doc = await loadFddDocumentByIdForTerms(id);
    if (!doc) return res.status(404).json({ success: false, error: "Document not found" });

    const all = await listTermsForDocument(id);
    const skipped = [];
    let updatedCount = 0;
    const uniq = [...new Set(termIds)];

    for (const tid of uniq) {
      const t = all.find((x) => String(x.id) === tid);
      if (!t) {
        skipped.push({ termId: tid, reason: "not_found" });
        continue;
      }
      if (String(t.fddDocumentId || "") !== String(id)) {
        skipped.push({ termId: tid, reason: "wrong_document" });
        continue;
      }
      const notePrefix = reviewer ? `[${reviewer}] ` : "";
      if (action === "clear_commercial_review") {
        t.commercialReviewRequired = false;
        updatedCount++;
      } else if (action === "mark_commercial_review_needed") {
        t.commercialReviewRequired = true;
        updatedCount++;
      } else if (action === "clear_legal_review") {
        t.legalReviewRequired = false;
        updatedCount++;
      } else if (action === "mark_legal_review_needed") {
        t.legalReviewRequired = true;
        updatedCount++;
      } else if (action === "approve") {
        const r = termBulkApproveBlockedReason(t);
        if (r) skipped.push({ termId: t.id, reason: r });
        else {
          t.reviewStatus = "Approved";
          if (reviewer) t.reviewerNotes = `${notePrefix}${String(t.reviewerNotes || "")}`.trim();
          updatedCount++;
        }
      } else if (action === "reject") {
        t.reviewStatus = "Rejected";
        if (reviewer) t.reviewerNotes = `${notePrefix}${String(t.reviewerNotes || "")}`.trim();
        updatedCount++;
      } else if (action === "needs_review") {
        t.reviewStatus = "Needs Review";
        if (reviewer) t.reviewerNotes = `${notePrefix}${String(t.reviewerNotes || "")}`.trim();
        updatedCount++;
      }
    }

    auditFddTermsForDocumentGroups(all);
    await persistAuditedTermsInPlace(all);
    return res.json({
      success: true,
      storage: isTermsAirtableEnabled() ? "airtable" : "memory",
      documentId: id,
      action,
      updatedCount,
      skippedCount: skipped.length,
      skipped,
    });
  } catch (e) {
    console.error("[fdd-intelligence-terms] bulk-update-terms:", e);
    res.status(500).json({ success: false, error: e.message || "Bulk update failed" });
  }
}

function normBrand(s) {
  return String(s || "")
    .trim()
    .toLowerCase();
}

/** GET /api/fdd-intelligence/brands/:brandName/terms
 *  Query: includeNeedsReview=1|true → Approved + Needs Review (excludes Rejected).
 *  Query: fddYear=<number> → restrict to terms from that FDD year (optional).
 *  Default: Approved only.
 */
export async function getFddBrandTerms(req, res) {
  try {
    const brandName = decodeURIComponent(req.params.brandName || "");
    const nb = normBrand(brandName);
    const includeNeeds = String(req.query.includeNeedsReview || "").trim() === "1" || String(req.query.includeNeedsReview || "").toLowerCase() === "true";
    const rawYear = req.query.fddYear;
    let fddYearFilter = null;
    if (rawYear != null && String(rawYear).trim() !== "") {
      const y = parseInt(String(rawYear).trim(), 10);
      if (Number.isFinite(y)) fddYearFilter = y;
    }

    let pool = [];
    if (isTermsAirtableEnabled()) {
      const base = getBase();
      const recs = await base(TERMS_TABLE)
        .select({
          filterByFormula: `LOWER(TRIM({Brand Name}))='${escapeAtFormula(nb)}'`,
          pageSize: 100,
        })
        .all()
        .catch(() => []);
      pool = recs.map(termFromAirtable);
    } else {
      pool = [...termsById.values()].filter((t) => normBrand(t.brandName) === nb);
    }

    if (fddYearFilter != null) {
      pool = pool.filter((t) => Number(t.fddYear) === fddYearFilter);
    }

    auditFddTermsForDocumentGroups(pool);

    const terms = pool.filter((t) => {
      const rs = String(t.reviewStatus || "").trim();
      if (includeNeeds) return rs === "Approved" || rs === "Needs Review";
      return rs === "Approved";
    });

    return res.json({
      success: true,
      storage: isTermsAirtableEnabled() ? "airtable" : "memory",
      brandName,
      includeNeedsReview: includeNeeds,
      fddYear: fddYearFilter,
      terms,
    });
  } catch (e) {
    console.error("[fdd-intelligence-terms] brand terms:", e);
    res.status(500).json({ success: false, error: e.message || "Read failed" });
  }
}
