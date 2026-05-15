/**
 * FDD Intelligence — Franchise Disclosure Document registry, segmentation, extraction, and review.
 *
 * Storage:
 * - When `AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID`, and the core three `AIRTABLE_TABLE_FDD_*` document/sections/fee names are set,
 *   documents, sections, and fee rows are read/written in Airtable (source of truth). PDFs stay on disk; `File Path` is local.
 * - Otherwise: in-memory Maps (reset on restart). Optional `airtable` string in JSON when env is partially set.
 *
 * Plain-text / PDF text: stored in Airtable under the field named by `AIRTABLE_FDD_DOCUMENT_FULL_TEXT_FIELD` (default `Full Text`).
 * Add that Long text column to **FDD Documents** if missing, or override the env to match your base.
 *
 * FDD Sections: `FDD Document ID` holds the parent document Airtable record id (text). Linked field `FDD Document` is left for a later enhancement.
 *
 * AI extraction: `extractFeeRowsForDocument()` — configure `FDD_INTELLIGENCE_MODEL_API_KEY` or placeholder rows are used.
 */

import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";
import multer from "multer";
import axios from "axios";
import Airtable from "airtable";
import { COST_BASIS_TEXT_KEYS, mergeCostBasisFromInference, parseBasisNeedsReviewFlag } from "./fdd-intelligence-cost-basis.js";
import { AUDIT_ROW_FIELD_KEYS, mergeAuditResultIntoRow, auditFddFeeRow, isAutoApproveEligible } from "./fdd-intelligence-row-audit.js";
import {
  collectRomanNumeralHeaderMatches,
  mergeBoundaryMarkers,
  inferExtractionTargetFromSection,
  US_ITEM_NUMBER_TO_EXTRACTION_TARGET,
  FEE_EXTRACTION_TARGET_SET,
  formatSectionPromptLabel,
  documentationReferenceFromSection,
} from "./fdd-intelligence-section-targets.js";
import {
  createFddExtractionJob,
  emitFddExtractionJobProgress,
  completeFddExtractionJob,
  failFddExtractionJob,
  getFddExtractionJobSnapshot,
  attachFddExtractionJobSse,
} from "./fdd-intelligence-extraction-jobs.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** @type {string | undefined} */
const AIRTABLE_DOCS_TABLE = process.env.AIRTABLE_TABLE_FDD_DOCUMENTS;
const AIRTABLE_SECTIONS_TABLE = process.env.AIRTABLE_TABLE_FDD_SECTIONS;
const AIRTABLE_ROWS_TABLE = process.env.AIRTABLE_TABLE_FDD_FEE_ROWS;

/** Airtable column name for fee-row workflow status (single select). Override if your base uses a different label. */
const AT_FDD_FEE_REVIEW_STATUS_FIELD = process.env.AIRTABLE_FDD_FEE_REVIEW_STATUS_FIELD || "Review Status";
const AT_RS_BRACE = `{${AT_FDD_FEE_REVIEW_STATUS_FIELD}}`;
const AT_REVIEW_STATUS_APPROVED_OR_NEEDS_FORMULA = `OR(${AT_RS_BRACE}='Approved',${AT_RS_BRACE}='Needs Review',${AT_RS_BRACE}='approved',${AT_RS_BRACE}='Needs review',${AT_RS_BRACE}='Need Review',${AT_RS_BRACE}='need review')`;
const AT_REVIEW_STATUS_APPROVED_ONLY_FORMULA = `OR(${AT_RS_BRACE}='Approved',${AT_RS_BRACE}='approved')`;

/** Server-side only. If unset, AI path is skipped and placeholder rows are used. */
const FDD_MODEL_API_KEY = process.env.FDD_INTELLIGENCE_MODEL_API_KEY || "";
/** Default: OpenAI Chat Completions. Swap URL + body in `callChatCompletionsJson()` for Anthropic, Azure OpenAI, etc. */
const FDD_MODEL_API_URL = process.env.FDD_INTELLIGENCE_MODEL_API_URL || "https://api.openai.com/v1/chat/completions";
const FDD_MODEL_NAME = process.env.FDD_INTELLIGENCE_MODEL_NAME || "gpt-4o-mini";
/** Max characters of section text sent per Item call (prompt + section must fit provider limits). */
const FDD_SECTION_TEXT_MAX_CHARS = Math.min(parseInt(process.env.FDD_INTELLIGENCE_SECTION_MAX_CHARS || "48000", 10) || 48000, 120000);
/** When a segmented Item exceeds this length, split into overlapping chunks for separate model calls. */
const FDD_CHUNK_MAX_CHARS = Math.max(2000, parseInt(process.env.FDD_INTELLIGENCE_CHUNK_MAX_CHARS || "18000", 10) || 18000);
/** Overlap between consecutive chunks (characters). */
const FDD_CHUNK_OVERLAP_CHARS = Math.max(
  0,
  Math.min(
    FDD_CHUNK_MAX_CHARS - 1,
    parseInt(process.env.FDD_INTELLIGENCE_CHUNK_OVERLAP_CHARS || "1200", 10) || 1200
  )
);

const AI_EXTRACTION_ITEM_NUMBERS = ["5", "6", "7", "11", "17"];

/** Prefer body slices at least this long when picking a real Item section over a TOC line. */
const SEGMENT_MIN_BODY_CHARS = 1000;
/** Minimum body length to accept a split-line ITEM N + title candidate (avoids false positives). */
const SEGMENT_MIN_SPLIT_BODY_CHARS = 200;

/**
 * Normalize an FDD title line for comparison (case, punctuation, ampersand).
 */
function normalizeTitleSignature(s) {
  return String(s || "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * True if a non-empty line after "ITEM N" matches the catalog title (exact / contains / strong word overlap).
 */
function catalogTitleMatchesExpectedTitle(catalogTitle, lineText) {
  const a = normalizeTitleSignature(catalogTitle);
  const b = normalizeTitleSignature(lineText);
  if (!a.length || !b.length) return false;
  if (a === b) return true;
  if (a.length >= 8 && b.length >= 8 && (a.includes(b) || b.includes(a))) return true;
  const aw = a.split(" ").filter((w) => w.length > 2);
  const bw = b.split(" ").filter((w) => w.length > 2);
  if (!aw.length || !bw.length) return false;
  const bset = new Set(bw);
  let inter = 0;
  for (const w of aw) if (bset.has(w)) inter++;
  const ratio = inter / Math.min(aw.length, bw.length);
  return ratio >= 0.85;
}

/** Regex for a line that is only "ITEM {n}" or "ITEM EX" (body heading, not "ITEM 5 INITIAL FEES ..."). */
function itemOnlyLineRegex(itemNumber) {
  if (String(itemNumber).toUpperCase() === "EX") return /^\s*ITEM\s+EX\s*\.?\s*$/i;
  const n = parseInt(String(itemNumber), 10);
  if (!Number.isFinite(n)) return null;
  return new RegExp(`^\\s*ITEM\\s+${n}\\s*\\.?\\s*$`, "i");
}

/** Up to `max` non-empty trimmed lines starting at `from` (index in text). */
function collectNonEmptyLinesAfter(text, from, max) {
  let p = from;
  const rows = [];
  while (rows.length < max && p < text.length) {
    const nl = text.indexOf("\n", p);
    const end = nl === -1 ? text.length : nl;
    const trimmed = text.slice(p, end).trim();
    if (trimmed.length) rows.push(trimmed);
    if (nl === -1) break;
    p = nl + 1;
  }
  return rows;
}

/**
 * Find body-style split headings: line is only ITEM N, and one of the next 1–5 non-empty lines matches catalog title.
 */
function collectSplitLineTitleCandidates(text) {
  const out = [];
  let p = 0;
  while (p < text.length) {
    const nl = text.indexOf("\n", p);
    const end = nl === -1 ? text.length : nl;
    const rawLine = text.slice(p, end);
    const trimmed = rawLine.trim();
    for (const def of ITEM_CATALOG) {
      const re = itemOnlyLineRegex(def.itemNumber);
      if (!re || !re.test(trimmed)) continue;
      const after = nl === -1 ? text.length : nl + 1;
      const nextLines = collectNonEmptyLinesAfter(text, after, 5);
      for (const titleLine of nextLines) {
        if (catalogTitleMatchesExpectedTitle(def.itemTitle, titleLine)) {
          out.push({
            index: p,
            rawLine: trimmed,
            itemNumber: def.itemNumber,
            matchedTitleLine: titleLine,
            candidateSourceType: "split-line-item-title",
            splitLine: true,
            toc: false,
          });
          break;
        }
      }
    }
    if (nl === -1) break;
    p = nl + 1;
  }
  return out;
}

/** Merge regex-based headers with split-line candidates (same index+item → prefer split metadata). */
function mergeItemHeaderMatches(regexMatches, splitCandidates) {
  const key = (m) => `${m.index}|${m.itemNumber}`;
  const map = new Map();
  for (const m of regexMatches) {
    map.set(key(m), { ...m });
  }
  for (const s of splitCandidates) {
    const k = key(s);
    const prev = map.get(k);
    if (prev) {
      map.set(k, {
        ...prev,
        ...s,
        toc: false,
        splitLine: true,
        rawLine: s.rawLine || prev.rawLine,
      });
    } else {
      map.set(k, { ...s });
    }
  }
  return [...map.values()].sort((a, b) => a.index - b.index || String(a.itemNumber).localeCompare(String(b.itemNumber)));
}

function countTrimmedLinesMatching(text, re) {
  let n = 0;
  for (const line of String(text || "").split("\n")) {
    if (re.test(line.trim())) n++;
  }
  return n;
}

function countSubstringOccurrences(text, needle) {
  const h = String(text || "");
  const n = String(needle);
  if (!n.length) return 0;
  let c = 0;
  let i = 0;
  while ((i = h.indexOf(n, i)) !== -1) {
    c++;
    i += n.length;
  }
  return c;
}

function countRegexGlobal(text, re) {
  const s = String(text || "");
  const g = re.global ? re : new RegExp(re.source, `${re.flags.replace("g", "")}g`);
  const m = s.match(g);
  return m ? m.length : 0;
}

/** Full text for segmentation / AI: prefer fresh PDF parse from disk; never rely on Airtable-truncated Full Text alone when a PDF exists. */
export async function resolveFullTextForExtraction(doc) {
  const stored = String(doc.fullText || "");
  if (doc.filePath && fs.existsSync(doc.filePath)) {
    try {
      const buf = fs.readFileSync(doc.filePath);
      const pdfText = await tryExtractPdfText(buf);
      if (pdfText && String(pdfText).trim().length) {
        return { fullTextForExtraction: String(pdfText), extractionTextSource: "pdf-file" };
      }
    } catch (_) {
      /* fall through to stored */
    }
    if (stored.trim()) {
      return { fullTextForExtraction: stored, extractionTextSource: "doc-fullText" };
    }
    return { fullTextForExtraction: "", extractionTextSource: "fallback-empty" };
  }
  if (stored.trim()) {
    const extractionTextSource = doc.sourceType === "manual" ? "pasted-text" : "doc-fullText";
    return { fullTextForExtraction: stored, extractionTextSource };
  }
  return { fullTextForExtraction: "", extractionTextSource: "fallback-empty" };
}

/**
 * After caller clears prior sections + fee rows for the document, resolves extraction text,
 * segments once, persists FDD Sections, and returns the lists for fee/terms extraction.
 */
async function fddResolveSegmentAndInsertSections(doc, docId) {
  const { fullTextForExtraction, extractionTextSource } = await resolveFullTextForExtraction(doc);
  doc.extractionNotes = null;
  appendExtractionPipelineDiagnostics(doc, fullTextForExtraction, extractionTextSource);
  const sectionList = segmentFddByItems(fullTextForExtraction);
  appendSegmentationFormatDiagnostics(doc, sectionList);
  appendSegmentationExtractionNotes(doc, sectionList, fullTextForExtraction);
  let createdSections;
  if (isFddAirtablePersistence()) {
    createdSections = await atInsertSections(docId, sectionList);
  } else {
    createdSections = [];
    for (const s of sectionList) {
      const rec = {
        id: newId("fddsec"),
        fddDocumentId: docId,
        itemNumber: s.itemNumber,
        itemTitle: s.itemTitle,
        sectionText: s.sectionText,
        pageStart: s.pageStart,
        pageEnd: s.pageEnd,
        extractionStatus: s.extractionStatus || "extracted",
        sourceFormat: s.sourceFormat || "us_fdd_item",
        sourceSectionLabel: s.sourceSectionLabel != null ? String(s.sourceSectionLabel) : String(s.itemNumber || ""),
        sourceSectionHeading: s.sourceSectionHeading || s.itemTitle || "",
        extractionTarget: s.extractionTarget || US_ITEM_NUMBER_TO_EXTRACTION_TARGET[String(s.itemNumber)] || "general_terms",
        candidateSourceType: s.candidateSourceType || "",
        segmentOrder: Number.isFinite(Number(s.segmentOrder)) ? Number(s.segmentOrder) : 0,
        createdAt: nowIso(),
        updatedAt: nowIso(),
      };
      sections.set(rec.id, rec);
      createdSections.push(rec);
    }
  }
  return { fullTextForExtraction, extractionTextSource, sectionList, createdSections };
}

function appendExtractionPipelineDiagnostics(doc, fullTextForExtraction, extractionTextSource) {
  const ext = String(fullTextForExtraction || "");
  const extLen = ext.length;
  const storageWriteLen = truncateAtLongText(ext).length;
  const appFees = countSubstringOccurrences(ext, "Application Fees and Related Fees");
  const newToSystem = countSubstringOccurrences(ext, "New-to-System AC Hotels");
  const item5Token = countRegexGlobal(ext, /\bITEM\s+5\b/gi);
  const item5Only = countTrimmedLinesMatching(ext, /^\s*ITEM\s+5\.?\s*$/i);
  const initialFeesOnly = countTrimmedLinesMatching(ext, /^\s*INITIAL\s+FEES\s*$/i);
  appendExtractionNote(
    doc,
    `Extraction text: source=${extractionTextSource} extractionLen=${extLen} storageWriteLen=${storageWriteLen} count_ApplicationFeesAndRelatedFees=${appFees} count_NewToSystemACHotels=${newToSystem} count_ITEM5_token=${item5Token} ITEM_5_only_lines=${item5Only} INITIAL_FEES_only_lines=${initialFeesOnly}`
  );
}

function appendSegmentationTocMismatchWarning(doc, sectionList, fullTextForExtraction) {
  const ext = String(fullTextForExtraction || "");
  const hasBody =
    ext.includes("Application Fees and Related Fees") || ext.includes("New-to-System AC Hotels");
  const i5 = sectionList.find((s) => String(s.itemNumber) === "5");
  const tocLike = i5 && i5.segmentationDiag && i5.segmentationDiag.chosenHeaderWasTocLike === true;
  if (hasBody && tocLike) {
    appendExtractionNote(doc, "Body text appears present but segmentation did not select it.");
  }
}

function appendFullTextStoragePolicyNote(doc, extractionTextSource, extractionLen, forAirtablePersistence) {
  const cap = AT_FDD_TEXT_TRUNCATE;
  if (!forAirtablePersistence) {
    appendExtractionNote(
      doc,
      `Document fullText updated after extraction (${extractionLen} characters, source=${extractionTextSource}).`
    );
    return;
  }
  if (extractionTextSource === "pdf-file" && extractionLen > 0) {
    appendExtractionNote(
      doc,
      `Airtable Full Text field stores at most ${cap} characters; segmentation and AI used full PDF text from disk (${extractionLen} characters).`
    );
  } else if (extractionLen > cap) {
    appendExtractionNote(
      doc,
      `Airtable Full Text field stores at most ${cap} characters; extraction used ${extractionLen} characters (truncated for storage only).`
    );
  } else {
    appendExtractionNote(doc, `Airtable Full Text storage length matches extraction length (${extractionLen} characters).`);
  }
}

export const FDD_UPLOAD_DIR = path.join(__dirname, "..", "uploads", "fdd-intelligence");

if (!fs.existsSync(FDD_UPLOAD_DIR)) {
  fs.mkdirSync(FDD_UPLOAD_DIR, { recursive: true });
}

/** @typedef {'Draft'|'Needs Review'|'Approved'|'Rejected'} ReviewStatus */
/** @typedef {'pending'|'extracting'|'extracted'|'extract_failed'|'registered'} ExtractionStatus */

/** In-memory stores */
const documents = new Map();
const sections = new Map();
const rows = new Map();

/**
 * Full FDD persistence: all three table names plus the same Airtable credentials as the rest of the app.
 * If any piece is missing, stay on in-memory Maps only (no half-sync).
 */
function isFddAirtablePersistence() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  return !!(apiKey && baseId && AIRTABLE_DOCS_TABLE && AIRTABLE_SECTIONS_TABLE && AIRTABLE_ROWS_TABLE);
}

/** When storage is memory, surface misconfiguration for operators (optional UI banner). */
function airtablePersistenceNote() {
  if (isFddAirtablePersistence()) return null;
  const hasAny = !!(AIRTABLE_DOCS_TABLE || AIRTABLE_SECTIONS_TABLE || AIRTABLE_ROWS_TABLE);
  if (hasAny) return "partial_airtable_env";
  return null;
}

function getFddAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
  return new Airtable({ apiKey }).base(baseId);
}

function escapeAtFormula(s) {
  if (s == null) return "";
  return String(s).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/**
 * Unwrap Airtable single-select / lookup shapes (string, `{ name }`, one-element array, `{ value }`).
 * @param {unknown} raw
 * @param {number} depth
 * @returns {unknown}
 */
function unwrapAirtableSelectLike(raw, depth = 0) {
  if (raw == null || depth > 8) return raw;
  if (typeof raw === "string") return raw;
  if (typeof raw === "number" || typeof raw === "boolean") return raw;
  if (Array.isArray(raw)) {
    if (!raw.length) return "";
    return unwrapAirtableSelectLike(raw[0], depth + 1);
  }
  if (typeof raw === "object") {
    if (typeof raw.name === "string") return raw.name;
    if (typeof raw.value === "string") return raw.value;
    if (typeof raw.text === "string") return raw.text;
  }
  return raw;
}

/**
 * Airtable single-select can return a string, `{ name }`, or a one-element array (lookup/multiselect edge cases).
 * Normalize so API filters and UI pills match Airtable.
 * @param {unknown} raw
 * @returns {string}
 */
function normalizeFddReviewStatusFromAirtable(raw) {
  let v = unwrapAirtableSelectLike(raw);
  if (v == null || v === "") return "Needs Review";
  if (typeof v !== "string") {
    v = String(v);
  }
  const s = v.replace(/\u00a0/g, " ").trim();
  if (!s) return "Needs Review";
  const lower = s.toLowerCase();
  if (lower === "approved" || lower === "approve") return "Approved";
  if (lower === "needs review" || lower === "need review" || lower === "needs-review") return "Needs Review";
  if (lower === "draft") return "Draft";
  if (lower === "rejected" || lower === "reject") return "Rejected";
  if (/needs?\s+review/i.test(s)) return "Needs Review";
  return s;
}

function isEconomicsScopeReviewStatus(status) {
  const n = normalizeFddReviewStatusFromAirtable(status);
  if (n === "Draft" || n === "Rejected") return false;
  if (n === "Approved" || n === "Needs Review") return true;
  const raw0 = unwrapAirtableSelectLike(status);
  const raw = String(raw0 ?? "")
    .toLowerCase()
    .replace(/\u00a0/g, " ")
    .trim();
  if (!raw || raw.includes("draft") || raw.includes("reject")) return false;
  if (/needs?\s+review/i.test(raw)) return true;
  if (raw === "approved" || raw === "approve") return true;
  return false;
}

/** Long text field on FDD Documents for extracted/plain text (not in original brief — add column or override via env). */
const AT_FDD_DOC_FULL_TEXT_FIELD = process.env.AIRTABLE_FDD_DOCUMENT_FULL_TEXT_FIELD || "Full Text";

function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

/** One id per POST /documents/:id/extract; copied onto every fee row for auditability. */
function newExtractionRunId() {
  return newId("fddext");
}

function nowIso() {
  return new Date().toISOString();
}

/** FTC-style item labels for segmentation hints */
const ITEM_CATALOG = [
  { itemNumber: "5", itemTitle: "Initial Fees" },
  { itemNumber: "6", itemTitle: "Other Fees" },
  { itemNumber: "7", itemTitle: "Estimated Initial Investment" },
  { itemNumber: "8", itemTitle: "Restrictions on Sources of Products and Services" },
  { itemNumber: "9", itemTitle: "Franchisee Obligations" },
  { itemNumber: "10", itemTitle: "Financing" },
  { itemNumber: "11", itemTitle: "Franchisor Assistance, Advertising, Computer Systems, and Training" },
  { itemNumber: "12", itemTitle: "Territory" },
  { itemNumber: "17", itemTitle: "Renewal, Termination, Transfer, and Dispute Resolution" },
  { itemNumber: "19", itemTitle: "Financial Performance Representations" },
  { itemNumber: "20", itemTitle: "Outlets and Franchisee Information" },
  { itemNumber: "EX", itemTitle: "Exhibits, Schedules, Addenda, and Footnotes" },
];

function itemTitleFromCatalog(itemNumber) {
  const c = ITEM_CATALOG.find((x) => String(x.itemNumber) === String(itemNumber));
  return c ? c.itemTitle : "Unclear";
}

/**
 * Extraction provenance / model metadata copied onto each fee row (not the rules-based audit score).
 */
function buildFeeRowExtractionMeta({
  extractionRunId,
  extractionUsedAi,
  modelNameUsed,
  sourceItemNumber,
  sourceItemTitle,
  document,
  extractionConfidence,
  sourceChunkIndex = 0,
  sourceChunkCount = 1,
  extractionTarget,
}) {
  const sidRaw = sourceItemNumber != null ? String(sourceItemNumber).trim() : "";
  const sid = sidRaw.length ? sidRaw : "Unclear";
  const stitle =
    sourceItemTitle != null && String(sourceItemTitle).trim().length
      ? String(sourceItemTitle).trim()
      : itemTitleFromCatalog(sid);
  const year = document.fddYear != null && document.fddYear !== "" ? Number(document.fddYear) : new Date().getFullYear();
  const needsLegalReview =
    extractionTarget === "renewal_transfer_termination" || sid === "17" || /^xvii$/i.test(sid);
  return {
    extractionRunId,
    extractionUsedAi: !!extractionUsedAi,
    modelNameUsed: extractionUsedAi ? modelNameUsed || FDD_MODEL_NAME : "placeholder",
    sourceItemNumber: sid,
    sourceItemTitle: stitle,
    sourceDocumentId: document.id,
    sourceDocumentBrandName: document.brandName || "Unclear",
    sourceDocumentFddYear: Number.isFinite(year) ? year : new Date().getFullYear(),
    extractionConfidence: extractionConfidence || "Unclear",
    needsLegalReview,
    needsCommercialReview: true,
    sourceChunkIndex: Number.isFinite(Number(sourceChunkIndex)) ? Number(sourceChunkIndex) : 0,
    sourceChunkCount: Number.isFinite(Number(sourceChunkCount)) ? Number(sourceChunkCount) : 1,
  };
}

/**
 * TOC-style lines often use dot leaders and a trailing page number (e.g. "ITEM 5 INITIAL FEES ........ 25").
 */
function isTocLikeItemLine(line) {
  const L = String(line || "").trim();
  if (!L) return true;
  if (/\.{4,}/.test(L)) return true;
  if (/\.{2,}\s*\d{1,4}\s*$/i.test(L)) return true;
  return false;
}

/** Text after the first line of a slice (the ITEM header line). */
function bodyAfterFirstLine(slice) {
  const s = String(slice || "").replace(/^\n+/, "");
  const i = s.indexOf("\n");
  if (i === -1) return "";
  return s.slice(i + 1);
}

function nextItemHeaderIndex(matches, fromIndex) {
  let best = null;
  for (const x of matches) {
    if (x.index > fromIndex && (best === null || x.index < best)) best = x.index;
  }
  return best == null ? null : best;
}

/**
 * Collect all ITEM / Item header lines (case-insensitive), with index for slicing.
 */
function collectItemHeaderMatches(text) {
  const headerRe = /(?:^|\n)\s*((?:ITEM|Item)\s+(\d{1,2}|EX)\b[.\s:–-]*[^\n]*)/gi;
  const matches = [];
  let m;
  while ((m = headerRe.exec(text)) !== null) {
    const rawLine = (m[1] || "").trim();
    const numRaw = m[2];
    const itemNumber = String(numRaw).toUpperCase() === "EX" ? "EX" : String(parseInt(numRaw, 10));
    matches.push({
      index: m.index,
      rawLine,
      itemNumber,
    });
  }
  return matches;
}

/**
 * For one item number, choose the best header match: skip TOC-like lines when possible,
 * prefer long body text and later occurrences (body sections after the table of contents).
 */
function pickBestSectionForItemNumber(itemNumber, matches, text) {
  const cand = matches.filter((x) => x.itemNumber === itemNumber);
  if (!cand.length) return null;

  const scored = cand.map((c) => {
    const end = nextItemHeaderIndex(matches, c.index) ?? text.length;
    const slice = text.slice(c.index, end);
    const body = bodyAfterFirstLine(slice);
    const bodyLen = body.trim().length;
    const splitLine = !!c.splitLine;
    const toc = !splitLine && isTocLikeItemLine(c.rawLine);
    return { c, slice, bodyLen, toc, index: c.index, splitLine };
  });

  let pool = scored.filter((x) => x.splitLine && x.bodyLen >= SEGMENT_MIN_SPLIT_BODY_CHARS);
  if (!pool.length) pool = scored.filter((x) => !x.toc && x.bodyLen >= SEGMENT_MIN_BODY_CHARS);
  if (!pool.length) pool = scored.filter((x) => !x.toc);
  if (!pool.length) pool = scored;

  pool.sort((a, b) => {
    const aOk = a.splitLine && a.bodyLen >= SEGMENT_MIN_SPLIT_BODY_CHARS;
    const bOk = b.splitLine && b.bodyLen >= SEGMENT_MIN_SPLIT_BODY_CHARS;
    if (aOk !== bOk) return aOk ? -1 : 1;
    if (b.bodyLen !== a.bodyLen) return b.bodyLen - a.bodyLen;
    return b.index - a.index;
  });

  const win = pool[0];
  const tocSkipped = scored.filter((x) => x.toc && x.c.index !== win.c.index).length;
  const sectionText = win.slice.trim();
  const chosenToc = win.splitLine ? false : win.toc;
  return {
    itemNumber,
    sectionText,
    segmentationDiag: {
      chosenStartIndex: win.c.index,
      sectionTextLength: sectionText.length,
      first500Chars: sectionText.slice(0, 500),
      tocLikeCandidatesSkipped: tocSkipped,
      totalCandidatesForItem: cand.length,
      chosenHeaderWasTocLike: chosenToc,
      candidateSourceType: win.c.candidateSourceType || (win.splitLine ? "split-line-item-title" : "regex-item-header"),
      rawLine: win.c.rawLine,
      matchedTitleLine: win.c.matchedTitleLine ?? null,
    },
  };
}

/** True when US-style Item 5 and 6 body segments look real (avoid TOC-only Mexico docs forcing catalog mode). */
function hasUsFeeCoreForCatalogMode(matches, text) {
  const p5 = pickBestSectionForItemNumber("5", matches, text);
  const p6 = pickBestSectionForItemNumber("6", matches, text);
  if (!p5 || !p6) return false;
  if (p5.sectionText.length < 350 || p6.sectionText.length < 350) return false;
  return true;
}

function enrichUsCatalogSection(def, picked) {
  const target = US_ITEM_NUMBER_TO_EXTRACTION_TARGET[String(def.itemNumber)] || "general_terms";
  return {
    itemNumber: def.itemNumber,
    itemTitle: def.itemTitle,
    sectionText: picked.sectionText,
    pageStart: null,
    pageEnd: null,
    extractionStatus: "segmented",
    segmentationDiag: picked.segmentationDiag,
    sourceSectionLabel: String(def.itemNumber),
    sourceSectionHeading: def.itemTitle,
    extractionTarget: target,
    sourceFormat: "us_fdd_item",
    candidateSourceType: picked.segmentationDiag?.candidateSourceType || "catalog-item",
  };
}

function buildBoundarySectionsFromMarkers(text, markers) {
  const out = [];
  for (let i = 0; i < markers.length; i++) {
    const mk = markers[i];
    const start = mk.index;
    const end = i + 1 < markers.length ? markers[i + 1].index : text.length;
    const slice = text.slice(start, end).trim();
    if (slice.length < 40) continue;
    const firstLine = slice.split("\n")[0] || "";
    const nl = slice.indexOf("\n");
    const bodyAfter = nl === -1 ? "" : slice.slice(nl + 1);
    let sourceSectionLabel;
    let sourceSectionHeading;
    let itemNumber;
    let itemTitle;
    let sourceFormat;
    const candidateSourceType = mk.kind === "item" ? "boundary-item-header" : "boundary-roman-header";

    if (mk.kind === "item") {
      sourceFormat = "us_fdd_item";
      sourceSectionLabel = String(mk.itemNumber);
      itemNumber = String(mk.itemNumber);
      itemTitle = itemTitleFromCatalog(itemNumber) || mk.rawLine || firstLine;
      sourceSectionHeading =
        (mk.matchedTitleLine && String(mk.matchedTitleLine).trim()) ||
        firstLine.replace(/^\s*(?:ITEM|Item)\s+\d{1,2}\b[.\s:–-]*/i, "").trim() ||
        itemTitle;
    } else {
      sourceFormat = "roman_numeral_disclosure";
      sourceSectionLabel = mk.romanLabel;
      itemNumber = mk.romanLabel;
      itemTitle = mk.heading || firstLine;
      sourceSectionHeading = mk.heading || firstLine;
    }

    const extractionTarget = inferExtractionTargetFromSection(firstLine, bodyAfter.slice(0, 1000));

    out.push({
      itemNumber,
      itemTitle,
      sectionText: slice,
      pageStart: null,
      pageEnd: null,
      extractionStatus: "segmented",
      sourceSectionLabel,
      sourceSectionHeading,
      extractionTarget,
      sourceFormat,
      candidateSourceType,
      segmentationDiag: {
        chosenStartIndex: start,
        sectionTextLength: slice.length,
        first500Chars: slice.slice(0, 500),
        tocLikeCandidatesSkipped: 0,
        totalCandidatesForItem: markers.length,
        chosenHeaderWasTocLike: false,
        candidateSourceType,
        rawLine: mk.rawLine,
        matchedTitleLine: mk.matchedTitleLine || null,
        boundaryMode: true,
      },
    });
  }
  return out;
}

function buildSegmentationDiagnostics({ detectedSourceFormat, romanHeadingCount, itemHeadingCount, sectionList, unmappedRomanHeadings }) {
  const mappedTargets = [...new Set(sectionList.map((x) => x.extractionTarget).filter(Boolean))];
  const mappedTargetLines = [];
  for (const s of sectionList) {
    if (!s.extractionTarget || s.itemNumber === "FULL") continue;
    const lab = s.sourceSectionLabel || s.itemNumber;
    const head = (s.sourceSectionHeading || s.itemTitle || "").slice(0, 140);
    mappedTargetLines.push(`Mapped section ${lab} → ${s.extractionTarget}: ${head}`);
  }
  return {
    detectedSourceFormat,
    romanHeadingCount,
    itemHeadingCount,
    targetSectionCount: sectionList.filter((s) => s.itemNumber !== "FULL").length,
    mappedTargets,
    mappedTargetLines,
    unmappedRomanHeadings: unmappedRomanHeadings || [],
  };
}

function appendSegmentationFormatDiagnostics(doc, sectionList) {
  const d = sectionList._segmentationDiagnostics;
  if (!d) return;
  appendExtractionNote(
    doc,
    `Detected section format: ${d.detectedSourceFormat} (romanHeadingCount=${d.romanHeadingCount}, itemHeadingCount=${d.itemHeadingCount}, targetSectionCount=${d.targetSectionCount})`
  );
  for (const line of (d.mappedTargetLines || []).slice(0, 60)) {
    appendExtractionNote(doc, line);
  }
  if (d.unmappedRomanHeadings && d.unmappedRomanHeadings.length) {
    appendExtractionNote(doc, `Unmapped headings: ${d.unmappedRomanHeadings.slice(0, 24).join(" | ")}`);
  }
}

/**
 * Split FDD plain text into Item sections using line-based ITEM headers, plus Roman numeral disclosure sections.
 * Preserves US catalog segmentation when Item 5/6 body slices look substantive; otherwise merges Roman + ITEM boundaries.
 */
export function segmentFddByItems(fullText) {
  const text = String(fullText || "").replace(/\r\n/g, "\n");
  if (!text.trim()) {
    const ph = ITEM_CATALOG.map((def) => ({
      itemNumber: def.itemNumber,
      itemTitle: def.itemTitle,
      sectionText: `[No extracted text yet — upload PDF or paste text, then run extraction.]`,
      pageStart: null,
      pageEnd: null,
      extractionStatus: "pending",
      sourceSectionLabel: String(def.itemNumber),
      sourceSectionHeading: def.itemTitle,
      extractionTarget: US_ITEM_NUMBER_TO_EXTRACTION_TARGET[String(def.itemNumber)] || "general_terms",
      sourceFormat: "us_fdd_item",
      candidateSourceType: "placeholder-empty-text",
    }));
    ph._segmentationDiagnostics = buildSegmentationDiagnostics({
      detectedSourceFormat: "unknown",
      romanHeadingCount: 0,
      itemHeadingCount: 0,
      sectionList: ph,
      unmappedRomanHeadings: [],
    });
    return ph;
  }

  const regexMatches = collectItemHeaderMatches(text);
  const splitCandidates = collectSplitLineTitleCandidates(text);
  const matches = mergeItemHeaderMatches(regexMatches, splitCandidates);
  const romanMatches = collectRomanNumeralHeaderMatches(text);
  const romanHeadingCount = romanMatches.length;
  const itemHeadingCount = matches.length;

  if (matches.length === 0 && romanMatches.length >= 2) {
    const markers = mergeBoundaryMarkers([], romanMatches);
    const out = buildBoundarySectionsFromMarkers(text, markers);
    let o = 0;
    for (const s of out) s.segmentOrder = o++;
    const unmapped = out
      .filter((s) => s.extractionTarget === "general_terms" && s.sourceFormat === "roman_numeral_disclosure")
      .map((s) => `${s.sourceSectionLabel} ${s.sourceSectionHeading}`.trim());
    out._segmentationDiagnostics = buildSegmentationDiagnostics({
      detectedSourceFormat: "roman_numeral_disclosure",
      romanHeadingCount,
      itemHeadingCount: 0,
      sectionList: out,
      unmappedRomanHeadings: unmapped,
    });
    return out;
  }

  if (matches.length === 0) {
    const full = [
      {
        itemNumber: "FULL",
        itemTitle: "Full document (unsegmented)",
        sectionText: text.slice(0, 500000),
        pageStart: null,
        pageEnd: null,
        extractionStatus: "heuristic_unsegmented",
        sourceSectionLabel: "FULL",
        sourceSectionHeading: "Full document (unsegmented)",
        extractionTarget: "general_terms",
        sourceFormat: "unknown",
        candidateSourceType: "unsegmented-full",
        segmentOrder: 0,
      },
    ];
    full._segmentationDiagnostics = buildSegmentationDiagnostics({
      detectedSourceFormat: "unknown",
      romanHeadingCount,
      itemHeadingCount: 0,
      sectionList: full,
      unmappedRomanHeadings: [],
    });
    return full;
  }

  const usFeeCore = hasUsFeeCoreForCatalogMode(matches, text);
  const useBoundaryMixed = romanMatches.length >= 2 && !usFeeCore;

  if (useBoundaryMixed) {
    const markers = mergeBoundaryMarkers(matches, romanMatches);
    const out = buildBoundarySectionsFromMarkers(text, markers);
    let o = 0;
    for (const s of out) s.segmentOrder = o++;
    const romanBodySections = out.filter((s) => s.sourceFormat === "roman_numeral_disclosure").length;
    const fmt =
      romanBodySections >= 2 && matches.length < 8
        ? "roman_numeral_disclosure"
        : romanMatches.length && matches.length
          ? "mixed"
          : "us_fdd_item";
    const unmapped = out
      .filter((s) => s.extractionTarget === "general_terms" && s.sourceFormat === "roman_numeral_disclosure")
      .map((s) => `${s.sourceSectionLabel} ${s.sourceSectionHeading}`.trim());
    out._segmentationDiagnostics = buildSegmentationDiagnostics({
      detectedSourceFormat: fmt,
      romanHeadingCount,
      itemHeadingCount,
      sectionList: out,
      unmappedRomanHeadings: unmapped,
    });
    return out;
  }

  const out = [];
  let o = 0;
  for (const def of ITEM_CATALOG) {
    const picked = pickBestSectionForItemNumber(def.itemNumber, matches, text);
    if (!picked) continue;
    const row = enrichUsCatalogSection(def, picked);
    row.segmentOrder = o++;
    out.push(row);
  }
  out._segmentationDiagnostics = buildSegmentationDiagnostics({
    detectedSourceFormat: "us_fdd_item",
    romanHeadingCount,
    itemHeadingCount,
    sectionList: out,
    unmappedRomanHeadings: [],
  });
  return out;
}

function isAiExtractionConfigured() {
  return typeof FDD_MODEL_API_KEY === "string" && FDD_MODEL_API_KEY.trim().length > 0;
}

/** Append timestamped extraction diagnostics (parse/API errors per Item). */
function appendExtractionNote(doc, line) {
  const entry = `[${nowIso()}] ${line}`;
  doc.extractionNotes = doc.extractionNotes ? `${doc.extractionNotes}\n${entry}` : entry;
}

/** Per-item / per-section segmentation diagnostics for Extraction Notes (fee-relevant US Items + Roman / mapped fee targets). */
function appendSegmentationExtractionNotes(doc, sectionList, fullText) {
  for (const s of sectionList) {
    const d = s.segmentationDiag;
    if (!d) continue;
    const n = String(s.itemNumber);
    const logUs = AI_EXTRACTION_ITEM_NUMBERS.includes(n);
    const logFeeTarget = s.extractionTarget && FEE_EXTRACTION_TARGET_SET.has(s.extractionTarget);
    const logRoman = s.sourceFormat === "roman_numeral_disclosure";
    if (!logUs && !logFeeTarget && !logRoman) continue;
    const raw = JSON.stringify(d.rawLine ?? "");
    const mtl = JSON.stringify(d.matchedTitleLine ?? "");
    const lab = s.sourceSectionLabel || s.itemNumber;
    appendExtractionNote(
      doc,
      `Segmentation section ${lab} target=${s.extractionTarget || ""}: source=${d.candidateSourceType || "unknown"} rawLine=${raw} matchedTitleLine=${mtl} startIndex=${d.chosenStartIndex} sectionTextLen=${d.sectionTextLength} candidates=${d.totalCandidatesForItem} tocLikeSkipped=${d.tocLikeCandidatesSkipped} chosenHeaderWasTocLike=${d.chosenHeaderWasTocLike}\nfirst500:\n${d.first500Chars}`
    );
  }
}

/**
 * Normalize a single scalar for row display: null, undefined, or blank string → "Unclear".
 * Numbers are stringified (amounts, page refs).
 */
function normalizeFddScalar(value) {
  if (value === null || value === undefined) return "Unclear";
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  const s = String(value).trim();
  return s.length ? s : "Unclear";
}

const ROW_TEXT_FIELDS = [
  "feeOrObligationName",
  "feeType",
  "amount",
  "amountType",
  "basis",
  "frequency",
  "dueTiming",
  "requiredOptional",
  "lifecyclePhase",
  "appliesWhen",
  "conditionalTrigger",
  "responsibleParty",
  "passThroughStatus",
  "bundledStatus",
  "canBeWaived",
  "estimatedCostImpact",
  "implementationRisk",
  "intakeMapping",
  "matchScoreImpact",
  "documentationReference",
  "documentationReferencePageNumber",
  "sourceTextExcerpt",
  "reviewerNotes",
];

/** Allowed `commercialCategory` values for FDD fee rows (Airtable + APIs). */
const COMMERCIAL_CATEGORY_VALUES = new Set([
  "Franchise Fee",
  "Recurring Brand Fee",
  "Estimated Initial Investment",
  "Required System / Technology Cost",
  "Training / Conference / Education",
  "Sales / Marketing / Loyalty / Reservation Program",
  "Transfer / Renewal / Relicensing",
  "Termination / Default / Penalty",
  "Legal / Operational Obligation",
  "Optional Program",
  "Pass-Through / Third-Party Cost",
  "Other / Needs Review",
]);

function amountLooksUnclearForObligation(row) {
  const a = String(row.amount != null ? row.amount : "").trim().toLowerCase();
  if (!a || a === "unclear" || a === "tbd" || a === "n/a" || a === "na") return true;
  if (/not determinable|cannot determine|entire cost\b/i.test(a)) return true;
  if (!/\d/.test(a)) return true;
  return false;
}

/**
 * Rules-based commercial classification for FDD fee/obligation rows (MVP).
 * May be replaced or supplemented by AI classification later.
 * Uses row-scoped text only (excludes catalog `sourceItemTitle`) so Item 17 headers like
 * "Renewal, Termination, Transfer…" do not false-trigger categories on unrelated fee names.
 * Priority order is intentional (e.g. transfer vs termination, recurring vs marketing, tech vs training).
 * @param {Record<string, unknown>} row
 */
function inferCommercialCategory(row) {
  const hayRow = [
    row.feeOrObligationName,
    row.feeType,
    row.basis,
    row.appliesWhen,
    row.conditionalTrigger,
    row.documentationReference,
    row.lifecyclePhase,
  ]
    .filter((x) => x != null && String(x).trim() !== "")
    .join(" ")
    .toLowerCase();

  const name = String(row.feeOrObligationName || "").trim().toLowerCase();
  const lc = String(row.lifecyclePhase || "").trim().toLowerCase();
  const itemNum = String(row.sourceItemNumber || "").trim();
  const passThroughField = String(row.passThroughStatus || "").toLowerCase();
  const reqOpt = String(row.requiredOptional || "").toLowerCase();
  const feeTypeLower = String(row.feeType || "").toLowerCase();

  const legalSpecificHit =
    /\b(indemnification|insurance requirements?|franchise term length)\b/i.test(hayRow) ||
    (/\bterm length\b/i.test(hayRow) && /\bfranchise\b/i.test(name)) ||
    /\bconstruction drawings review\b/i.test(hayRow) ||
    /\bcompliance assessment\b/i.test(hayRow) ||
    /\b(operational compliance|required assessment)\b/i.test(hayRow) ||
    /\bsecurity design plan review\b/i.test(hayRow) ||
    /\b(residential brand standard audit|brand standard audit|annual residential project assessment|residential project assessment)\b/i.test(hayRow) ||
    (/\bfire protection\b/i.test(hayRow) && /\b(audit|re-assessment|reassessment|inspection|compliance|fee)\b/i.test(hayRow)) ||
    (/\blife safety\b/i.test(hayRow) && /\b(audit|re-assessment|reassessment|inspection|compliance|fee|design)\b/i.test(hayRow));
  if (legalSpecificHit && !/\b(trademark non-compliance|pip non-compliance|best rate guarantee non-compliance|liquidated damages)\b/i.test(hayRow)) {
    return "Legal / Operational Obligation";
  }

  if (/\brelicensing\b/i.test(name)) {
    return "Transfer / Renewal / Relicensing";
  }

  const transferLifecycleStrong =
    /\b(transfer|change of ownership|relicensing|sale)\b/i.test(lc) ||
    (/\brenewal\b/i.test(lc) &&
      (itemNum === "17" || /^xvii$/i.test(itemNum)) &&
      /\b(application fee|property improvement plan|\bpip\b|transfer|relicensing|comfort letter|estoppel|prospectus|assignment|consent|change of ownership)\b/i.test(
        name
      ));
  const item17TransferName =
    (itemNum === "17" || /^xvii$/i.test(itemNum)) &&
    /\b(application fee|property improvement plan|\bpip\b|transfer|renewal|relicensing|comfort letter|estoppel|prospectus|assignment|consent|change of ownership)\b/i.test(
      name
    );
  if (transferLifecycleStrong || item17TransferName) {
    const transferSemantic =
      /\b(application fee|property improvement plan|\bpip\b|renewal|relicensing|comfort letter|estoppel|prospectus|assignment|consent|change of ownership)\b/i.test(hayRow) ||
      /\b(transfer fee|pip fee|pip refresh|pip revision|pip modification|new pip fee)\b/i.test(hayRow) ||
      (/\bapplication fee\b/i.test(name) && /\btransfer\b/i.test(lc)) ||
      /\bproperty improvement plan\b/i.test(name) ||
      /\bpip\b/i.test(name);
    if (transferSemantic) {
      return "Transfer / Renewal / Relicensing";
    }
  }

  const transferGeneral =
    /\b(transfer|renewal|relicensing|change of ownership|pip on transfer|outside counsel on transfer)\b/i.test(hayRow) ||
    /\b(prospectus review|comfort letter or estoppel|comfort letter|estoppel processing)\b/i.test(hayRow);
  if (transferGeneral) {
    return "Transfer / Renewal / Relicensing";
  }

  const terminationHit =
    /\b(termination|default|liquidated damages|non-compliance|no-show|penalty|damages|trademark non-compliance|pip non-compliance|best rate guarantee non-compliance|unauthorized electronic identifier)\b/i.test(
      hayRow
    ) || /\bremoval from system\b/i.test(hayRow);
  if (terminationHit) {
    return "Termination / Default / Penalty";
  }

  const techTokenRe =
    /\b(pms|pos|point-of-sale|point of sale|software|hardware|network|wi-?fi|server|firewall|tokenization|gateway|digital guest services|digital food|lobby pc|endpoint detection|managed detection|information security|mobile key|lock system|gxp|mdash|mcredit|technology)\b/i;
  const techPhraseRe =
    /\b(property management system|residential property management system|residential key control system|key control system|payment solution|payment device|merchant id|credit card|email solution|onesource|sfaweb|\bgpo\b|\bfmn\b|\blsp\b|\bgre\b|mobile device management|\bmdm\b|application management)\b/i;
  const techExtendedRe =
    /\b(yield management|opportunity management|property management,\s*reservation|management system implementation|new pms|pms transition|ongoing pms|pms cost|lock system infrastructure|credit card processing|credit card gateway|chip and pin|settlement transaction|tokenization implementation|information security managed|managed detection and response)\b/i;
  if (
    techTokenRe.test(hayRow) ||
    techPhraseRe.test(hayRow) ||
    techExtendedRe.test(hayRow) ||
    (/\bpayment\b/i.test(hayRow) && /\b(system|software|technology|vendor|implementation|solution|gateway|device)\b/i.test(hayRow)) ||
    (/\bcredit card\b/i.test(hayRow) && /\b(processing|gateway|fee|validation|device|transaction|solution)\b/i.test(hayRow))
  ) {
    return "Required System / Technology Cost";
  }

  const recurringExact =
    /^(franchise fees|royalty|continuing royalty|continuing fee|monthly franchise fee)\s*$/i.test(String(row.feeOrObligationName || "").trim()) ||
    /\bfranchise fees\b/i.test(name);
  const revenueBaseRe = /\b(gross room sales|gross rooms revenue|gross room revenue|room sales|gross revenue)\b/i;
  const franchiseRoyaltyContext = /\b(franchise fee|franchise fees|royalty|royalties|continuing fee|monthly franchise fee)\b/i;
  if (
    recurringExact ||
    (revenueBaseRe.test(hayRow) && franchiseRoyaltyContext.test(hayRow)) ||
    (/\b(royalty|royalties|continuing fee|monthly franchise fee)\b/i.test(hayRow) &&
      !/\b(initial franchise|application fee|franchise application)\b/i.test(hayRow))
  ) {
    return "Recurring Brand Fee";
  }

  const marketingHit =
    /\b(marketing fund|advertising|loyalty|reservation program|\bgso\b|global sales organization|sales organization|revenue management|group sales|national group sales|account sales|brand advocate|program services contribution|customer engagement center|\bcec\b|demand generation|digital support)\b/i.test(hayRow) ||
    /\b(participation in global sales organization|participation in sales and marketing)\b/i.test(hayRow) ||
    (/\bcustomer issue resolution\b/i.test(hayRow) &&
      /\b(guest|customer service|program|engagement|cec)\b/i.test(hayRow)) ||
    (/\breservation\b/i.test(hayRow) &&
      !/\b(property management|yield management|opportunity management|system|systems|pms|technology|hardware|software)\b/i.test(hayRow)) ||
    (/\bsales\b/i.test(hayRow) &&
      !/\bgross room sales\b/i.test(hayRow) &&
      !/\broom sales\b/i.test(hayRow) &&
      /\b(group|national|international|account|lead|intermediary)\b/i.test(hayRow));
  if (marketingHit) {
    return "Sales / Marketing / Loyalty / Reservation Program";
  }

  const trainingStrictRe =
    /\b(training|conference|learning|education|immersion|immersions|onboarding|fitm|fond|audit program|gss improvement program|general manager conference|executive orientation)\b/i;
  if (trainingStrictRe.test(hayRow)) {
    return "Training / Conference / Education";
  }

  const passThroughHay =
    passThroughField.includes("pass") ||
    passThroughField.includes("third") ||
    /\b(pass-through|pass through|third-party|third party)\b/i.test(hayRow);
  if (passThroughHay) {
    return "Pass-Through / Third-Party Cost";
  }

  if (reqOpt.includes("optional")) {
    return "Optional Program";
  }

  if (
    /\btrademark license\b/i.test(hayRow) &&
    !/\b(trademark non-compliance|penalty|non-compliance)\b/i.test(hayRow)
  ) {
    return "Franchise Fee";
  }

  const franchiseByPhrase =
    /\b(application fee|franchise application|initial franchise|enrollment fee|conversion fee|extension fee)\b/i.test(hayRow) ||
    /\b(property improvement plan|property improvement plan fee|pip fee|new pip fee|pip refresh|pip revision|pip modification|api enrollment)\b/i.test(hayRow) ||
    (/\bpip\b/i.test(hayRow) &&
      /\b(fee|plan|improvement|renovation|refresh|modification|non-compliance)\b/i.test(hayRow) &&
      !/\bpip on transfer\b/i.test(hayRow));
  if (franchiseByPhrase && !/\b(royalty|royalties|gross room sales|continuing fee|monthly franchise fee)\b/i.test(hayRow)) {
    return "Franchise Fee";
  }

  if (itemNum === "7") {
    const item7Exception =
      /\b(application fee|franchise application|initial franchise fee|initial franchise)\b/i.test(hayRow) ||
      trainingStrictRe.test(hayRow) ||
      techTokenRe.test(hayRow) ||
      techPhraseRe.test(hayRow) ||
      techExtendedRe.test(hayRow) ||
      (/\bpayment\b/i.test(hayRow) && /\b(system|software|technology|vendor|implementation|solution|gateway|device)\b/i.test(hayRow)) ||
      (/\bcredit card\b/i.test(hayRow) && /\b(processing|gateway|fee|validation|device|transaction|solution)\b/i.test(hayRow));
    if (!item7Exception) {
      return "Estimated Initial Investment";
    }
  }

  const obligationSignal =
    /\bobligation\b/i.test(hayRow) || /\bobligation\b/i.test(feeTypeLower);
  if (obligationSignal && amountLooksUnclearForObligation(row)) {
    return "Legal / Operational Obligation";
  }

  if (
    itemNum === "11" &&
    /\bconstruction\b/i.test(hayRow) &&
    /\b(cost|costs)\b/i.test(hayRow) &&
    !obligationSignal
  ) {
    return "Estimated Initial Investment";
  }

  return "Other / Needs Review";
}

/**
 * Ensures `row.commercialCategory` is set to a valid enum value, inferring when missing or invalid.
 * @param {Record<string, unknown>} row
 */
function ensureFeeRowCommercialCategory(row) {
  if (!row || typeof row !== "object") return row;
  const cur = String(row.commercialCategory || "").trim();
  if (COMMERCIAL_CATEGORY_VALUES.has(cur)) return row;
  row.commercialCategory = inferCommercialCategory(row);
  return row;
}

/** Value written to Airtable; does not mutate `row`. */
function commercialCategoryForPersistence(row) {
  const c = String(row.commercialCategory || "").trim();
  if (COMMERCIAL_CATEGORY_VALUES.has(c)) return c;
  return inferCommercialCategory(row);
}

/** Max length for Airtable single-line Duplicate Group Key. */
const DUPLICATE_GROUP_KEY_MAX_LEN = 900;

/**
 * Normalizes a fragment for duplicate grouping (lowercase, punctuation stripped, noisy words removed).
 */
function normalizeForDuplicateGroupKeyPart(s) {
  let t = String(s || "").toLowerCase().trim();
  t = t.replace(/\s+/g, " ");
  t = t.replace(/[$€£¢]/g, "");
  t = t.replace(/%/g, "pct");
  t = t.replace(/[^a-z0-9\s]/gi, " ");
  t = t.replace(/\b(fee|fees|cost|costs|charge|charges|program|service|services)\b/gi, " ");
  t = t.replace(/\s+/g, " ").trim();
  return t;
}

/**
 * Deterministic duplicate grouping key (same document + run + key => possible duplicate).
 * Caller should set commercialCategory before calling (e.g. via ensureFeeRowCommercialCategory).
 */
function buildDuplicateGroupKey(row) {
  const docId = String(row.fddDocumentId || "").trim();
  const brand = normBrand(row.brandName);
  const year = String(row.fddYear != null ? row.fddYear : "").trim();
  const cat = String(row.commercialCategory || "").trim();
  const nameKey = normalizeForDuplicateGroupKeyPart(row.feeOrObligationName || "");
  const amtKey = normalizeForDuplicateGroupKeyPart(row.amount != null ? String(row.amount) : "");
  const raw = `${docId}|${brand}|${year}|${cat}|${nameKey}|${amtKey}`;
  return raw.length > DUPLICATE_GROUP_KEY_MAX_LEN ? raw.slice(0, DUPLICATE_GROUP_KEY_MAX_LEN) : raw;
}

/**
 * Sets duplicateGroupKey and possibleDuplicate on each row (same fddDocumentId batch).
 * Flags when two+ rows share (extractionRunId, duplicateGroupKey). Does not delete or merge rows.
 * @returns {{ inputRows: number, duplicateGroups: number, duplicateRows: number }}
 */
function applyDuplicateFlagsToRows(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return { inputRows: 0, duplicateGroups: 0, duplicateRows: 0 };
  }
  for (const r of rows) {
    r.duplicateGroupKey = buildDuplicateGroupKey(r);
  }
  const buckets = new Map();
  for (const r of rows) {
    const run = String(r.extractionRunId || "").trim() || `noid:${r.id || "unknown"}`;
    const composite = `${run}::${r.duplicateGroupKey}`;
    if (!buckets.has(composite)) buckets.set(composite, []);
    buckets.get(composite).push(r);
  }
  let duplicateGroups = 0;
  let duplicateRows = 0;
  for (const [, arr] of buckets) {
    if (arr.length >= 2) {
      duplicateGroups += 1;
      duplicateRows += arr.length;
      for (const r of arr) r.possibleDuplicate = true;
    } else {
      for (const r of arr) r.possibleDuplicate = false;
    }
  }
  return { inputRows: rows.length, duplicateGroups, duplicateRows };
}

/**
 * Ensures category then duplicate flags for a list of rows (typically one document).
 */
function enrichFeeRowsListForApi(rowsIn) {
  const rows = Array.isArray(rowsIn) ? rowsIn.map((r) => ({ ...r })) : [];
  for (const r of rows) {
    ensureFeeRowCommercialCategory(r);
    mergeCostBasisFromInference(r);
  }
  const stats = applyDuplicateFlagsToRows(rows);
  for (const r of rows) {
    mergeAuditResultIntoRow(r);
  }
  return { rows, ...stats };
}

/**
 * Groups approved rows by fddDocumentId and applies duplicate detection per document.
 */
function enrichApprovedRowsWithDuplicateFlags(rowList) {
  const byDoc = new Map();
  for (const r of rowList) {
    const id = String(r.fddDocumentId || "");
    if (!byDoc.has(id)) byDoc.set(id, []);
    byDoc.get(id).push({ ...r });
  }
  const out = [];
  for (const [, group] of byDoc) {
    const { rows } = enrichFeeRowsListForApi(group);
    out.push(...rows);
  }
  return out;
}

/**
 * True when duplicate flags or persisted audit snapshot differs (Airtable PATCH fan-in).
 * @param {Record<string, unknown>|undefined} prev
 * @param {Record<string, unknown>} next
 */
function feeRowDupOrAuditDirty(prev, next) {
  if (!prev) return true;
  if (!!prev.possibleDuplicate !== !!next.possibleDuplicate) return true;
  if (String(prev.duplicateGroupKey || "") !== String(next.duplicateGroupKey || "")) return true;
  for (const k of AUDIT_ROW_FIELD_KEYS) {
    if (prev[k] !== next[k]) return true;
  }
  return false;
}

/** True when persisted fee row fields differ enough to write Airtable / memory. */
function feeRowPersistDirty(prev, next) {
  if (!prev) return true;
  if (feeRowDupOrAuditDirty(prev, next)) return true;
  if (normalizeFddReviewStatusFromAirtable(prev.reviewStatus) !== normalizeFddReviewStatusFromAirtable(next.reviewStatus)) {
    return true;
  }
  if (!!prev.needsCommercialReview !== !!next.needsCommercialReview) return true;
  if (!!prev.needsLegalReview !== !!next.needsLegalReview) return true;
  if (!!prev.basisNeedsReview !== !!next.basisNeedsReview) return true;
  return false;
}

const BULK_APPROVE_BLOCKED_CATEGORIES = new Set([
  "Other / Needs Review",
  "Legal / Operational Obligation",
  "Termination / Default / Penalty",
  "Transfer / Renewal / Relicensing",
]);

/**
 * @returns {string|null} skip reason code, or null if bulk approve is allowed
 */
function getBulkApproveSkipReason(row) {
  const rs = normalizeFddReviewStatusFromAirtable(row.reviewStatus);
  if (rs === "Approved") return "already_approved";
  if (rs === "Rejected") return "rejected_status";
  if (rs === "Draft") return "draft_status";
  if (row.needsLegalReview === true) return "legal_review_required";
  if (row.possibleDuplicate === true) return "possible_duplicate";
  if (row.basisNeedsReview === true) return "basis_needs_review";
  const cat = String(row.commercialCategory || "").trim();
  if (BULK_APPROVE_BLOCKED_CATEGORIES.has(cat)) return "blocked_category";
  const score = Number(row.auditScore);
  if (!Number.isFinite(score) || score < 80) return "audit_score_below_80";
  return null;
}

/**
 * Recomputes and persists duplicate flags for all rows in a document after a PATCH (bounded to one doc).
 * Future: optional scheduled job to refresh duplicates across the full base without per-PATCH fan-out.
 */
async function persistDuplicateFlagsForDocumentAfterPatch(docId) {
  if (!docId) return;
  if (isFddAirtablePersistence()) {
    const raw = await atListRowsForDoc(docId);
    const { rows: list } = enrichFeeRowsListForApi(raw);
    for (const r of list) {
      const prev = raw.find((x) => x.id === r.id);
      if (!prev) continue;
      if (!feeRowDupOrAuditDirty(prev, r)) continue;
      await atUpdateFeeRow(r);
    }
  } else {
    const raw = Array.from(rows.values()).filter((r) => r.fddDocumentId === docId);
    const { rows: list } = enrichFeeRowsListForApi(raw);
    for (const r of list) {
      rows.set(r.id, r);
    }
  }
}

/**
 * Map one model row object onto the persisted row shape; every listed text field becomes a non-empty string or "Unclear".
 */
function normalizeModelRowToSchema(raw, document, defaultItemLabel) {
  const brand = document.brandName || "Unknown Brand";
  const parent = document.parentCompany || "Unknown Parent";
  const year = document.fddYear || new Date().getFullYear();
  const out = {
    fddDocumentId: document.id,
    parentCompany: parent,
    brandName: brand,
    fddYear: year,
    country: document.country || "US",
    reviewStatus: "Needs Review",
  };
  for (const key of ROW_TEXT_FIELDS) {
    out[key] = normalizeFddScalar(raw && raw[key]);
  }
  for (const key of COST_BASIS_TEXT_KEYS) {
    out[key] = normalizeFddScalar(raw && raw[key]);
  }
  out.basisNeedsReview = parseBasisNeedsReviewFlag(raw && raw.basisNeedsReview);
  if (out.documentationReference === "Unclear") {
    out.documentationReference = defaultItemLabel;
  }
  out.reviewStatus = "Needs Review";
  return out;
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

function parseModelJsonRows(content) {
  const stripped = stripMarkdownJsonFence(content);
  let parsed;
  try {
    parsed = JSON.parse(stripped);
  } catch (e) {
    const err = new Error(`JSON.parse failed: ${e.message}`);
    err.cause = e;
    throw err;
  }
  if (Array.isArray(parsed)) return parsed;
  if (parsed && Array.isArray(parsed.rows)) return parsed.rows;
  if (parsed && Array.isArray(parsed.fees)) return parsed.fees;
  throw new Error('Model JSON must be an array or { "rows": [...] }');
}

/**
 * OpenAI-compatible chat completion returning message content.
 *
 * PROVIDER_SWAP:
 * - Anthropic: POST to `https://api.anthropic.com/v1/messages`, set `x-api-key`, body `{ model, max_tokens, messages }`.
 * - Azure OpenAI: set `FDD_INTELLIGENCE_MODEL_API_URL` to your deployment chat URL and use API key style required by Azure.
 * - Google Gemini: different endpoint/schema — implement a sibling function and branch on `FDD_INTELLIGENCE_PROVIDER` if needed.
 */
async function callChatCompletionsJson({ system, user }) {
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
    const msg = (data && data.error && data.error.message) || "Invalid chat completion response";
    throw new Error(msg);
  }
  return data.choices[0].message.content || "";
}

function buildExtractionSystemPrompt() {
  return [
    "You extract structured commercial obligations from hotel Franchise Disclosure Document (FDD) text.",
    "Return JSON ONLY — no prose, no markdown fences. The response must be a single JSON object.",
    'Shape: { "rows": [ { ... } ] }',
    "Each element of rows must include these keys (strings unless noted):",
    "feeOrObligationName, feeType, amount, amountType, basis, frequency, dueTiming, requiredOptional, lifecyclePhase,",
    "appliesWhen, conditionalTrigger, responsibleParty, passThroughStatus, bundledStatus, canBeWaived,",
    "estimatedCostImpact, implementationRisk, intakeMapping, matchScoreImpact, documentationReference,",
    "documentationReferencePageNumber, sourceTextExcerpt, reviewerNotes,",
    "normalizedCostBasis, rawCostBasisText, amountFormulaType, calculationUnit, revenueBase, unitRate, percentageRate,",
    "fixedAmount, formulaNotes, basisConfidence, basisNeedsReview (boolean).",
    "normalizedCostBasis must be one of: Gross Room Sales / Room Revenue | Gross Revenue / Total Revenue | F&B Revenue |",
    "Residential Revenue | Per Guestroom / Per Key | Per Occupied Room / Room Night | Per Reservation / Booking |",
    "Per Lead / Referral | Per Transaction | Per User / Device / Workstation | Per Property / Hotel | Per Month |",
    "Per Year / Annual | Lump Sum / One-Time | Greater-Of Formula | Mixed Formula | Actual Cost / Pass-Through |",
    "Variable / As Incurred | Other / Custom Basis | Not Stated / Unclear.",
    "When the FDD states a real fee basis that does not fit any standard category above, set normalizedCostBasis to Other / Custom Basis (do not force it into Gross Revenue, Per Guestroom, Lump Sum, etc.).",
    "For Other / Custom Basis: rawCostBasisText must quote the exact basis language from the FDD; calculationUnit should be the closest plain-English unit (e.g. per comfort letter, per audit, per person, per day, per request).",
    "Set basisNeedsReview to true unless the excerpt + basis together make the custom basis completely unambiguous; when in doubt, true.",
    "amountFormulaType must be one of: Fixed | Percentage | Per-Unit | Recurring Fixed | Greater-Of | Mixed Formula |",
    "Actual Cost | Variable | Unclear.",
    "basisConfidence must be one of: High | Medium | Low.",
    "Populate rawCostBasisText with the verbatim basis/amount phrasing from the section when possible.",
    "Populate percentageRate (e.g. 6.0%), unitRate (e.g. $500 per guestroom), fixedAmount when clearly stated.",
    "Use short literal excerpts from the provided section for sourceTextExcerpt when possible.",
    'If unknown, use the string "Unclear". Do not use null.',
    "Do not merge unrelated fees into one row. If a fee has multiple components, use separate rows.",
    "If amount is not stated, amount should be Unclear or describe the variable basis in amount and basis.",
    "Extract every fee, charge, payment, pass-through, penalty, required system, required program, optional program,",
    "training obligation, transfer cost, renewal cost, termination cost, and other commercial obligation in the section text.",
    "Extract from both narrative paragraphs and tables (preserve column roles such as type of fee, amount, due date, remarks).",
    "When a table has a fee name in the first column and amount in the second, emit one row per table row unless the text clearly merges rows.",
    "If a table cell references notes (e.g. 'See Notes 1 and 2') without a numeric amount, put that pointer in documentationReference or reviewerNotes and include any matching note text found in the same section.",
  ].join(" ");
}

function buildExtractionUserPrompt({
  itemNumber,
  itemTitle,
  sectionText,
  document,
  chunkIndex = 0,
  chunkCount = 1,
  sourceFormat,
  extractionTarget,
  promptLabel,
  documentationReferenceHint,
}) {
  const country = document.country || "US";
  const isUsItemFormat =
    String(sourceFormat || "us_fdd_item") === "us_fdd_item" &&
    (/^\d{1,2}$/.test(String(itemNumber || "").trim()) || /^ex$/i.test(String(itemNumber || "").trim()));
  const refHint =
    documentationReferenceHint ||
    documentationReferenceFromSection({
      sourceSectionLabel: itemNumber,
      sourceSectionHeading: itemTitle,
    });
  const nonUsTableNote =
    !isUsItemFormat || String(sourceFormat || "") === "roman_numeral_disclosure"
      ? " This section may follow a non-U.S. FTC disclosure layout. Do not pretend the source is U.S. Item 5/6/7 unless the text explicitly uses those labels; use the source reference line below for documentationReference when appropriate."
      : "";
  const head = isUsItemFormat
    ? `FDD Item ${itemNumber}: ${itemTitle}`
    : `Franchise disclosure section: ${promptLabel || `${itemNumber}: ${itemTitle}`}\nInternal extraction target: ${extractionTarget || "unclear"}\nSource reference: ${refHint}`;
  const header = `${head}\nBrand context (metadata only): brandName=${document.brandName}, fddYear=${document.fddYear}, country=${country}.${nonUsTableNote}`;
  const chunkNote =
    chunkCount > 1
      ? `\nThis is segment chunk ${chunkIndex + 1} of ${chunkCount} from the same section (overlapping windows). Extract obligations supported by this chunk only; do not invent fees not evidenced here.`
      : "";
  const body = String(sectionText || "").slice(0, FDD_SECTION_TEXT_MAX_CHARS);
  return `${header}${chunkNote}\n\n--- SECTION TEXT START ---\n${body}\n--- SECTION TEXT END ---\n\nReturn strictly: { "rows": [ ... ] }`;
}

/** Prefer chunk end at paragraph boundary, then line, then whitespace (avoid mid-word when possible). */
function findChunkEndBoundary(s, start, targetEnd, maxChars) {
  const low = start + Math.max(200, Math.floor(maxChars * 0.55));
  const end = Math.min(s.length, targetEnd);
  for (let p = end - 1; p >= low; p--) {
    if (p > 0 && s[p - 1] === "\n" && s[p] === "\n") return Math.min(s.length, p + 1);
  }
  for (let p = end - 1; p >= low; p--) {
    if (s[p] === "\n") return p + 1;
  }
  for (let p = end - 1; p >= low; p--) {
    if (/\s/.test(s[p])) return p + 1;
  }
  return end;
}

/**
 * Split long section text into overlapping chunks for separate model calls.
 * @returns {Array<{ chunkIndex: number, chunkCount: number, chunkText: string }>}
 */
export function splitSectionTextIntoChunks(sectionText, maxChars = FDD_CHUNK_MAX_CHARS, overlap = FDD_CHUNK_OVERLAP_CHARS) {
  const s = String(sectionText || "");
  const n = s.length;
  if (!n) return [];
  const mc = Math.max(1000, maxChars);
  const ov = Math.max(0, Math.min(mc - 1, overlap));
  if (n <= mc) return [{ chunkIndex: 0, chunkCount: 1, chunkText: s }];

  const pieces = [];
  let start = 0;
  while (start < n) {
    const targetEnd = Math.min(n, start + mc);
    let cut = targetEnd;
    if (cut < n) {
      cut = findChunkEndBoundary(s, start, cut, mc);
    }
    pieces.push(s.slice(start, cut));
    if (cut >= n) break;
    const nextStart = cut - ov;
    start = nextStart > start ? nextStart : cut;
  }
  const chunkCount = pieces.length;
  return pieces.map((chunkText, chunkIndex) => ({ chunkIndex, chunkCount, chunkText }));
}

function isPlaceholderSectionText(text) {
  const t = String(text || "");
  return !t.trim() || t.includes("[No extracted text yet");
}

/**
 * Runs fee extraction for every segmented section whose extractionTarget is in the fee set (US Items 5–7, 11, 17 plus mapped non-U.S. sections).
 * On JSON failure, logs note on document and continues.
 */
export async function extractFeeRowsForDocument(document, sectionList, extractionRunId, onProgress) {
  const runId = extractionRunId || newExtractionRunId();
  if (!isAiExtractionConfigured()) {
    const feeScope = (sectionList || []).filter(
      (s) =>
        s &&
        s.itemNumber !== "FULL" &&
        s.extractionTarget &&
        FEE_EXTRACTION_TARGET_SET.has(s.extractionTarget)
    );
    if (typeof onProgress === "function") {
      onProgress({
        stage: "fees",
        message:
          feeScope.length > 0
            ? `Fees: placeholder mode (${feeScope.length} fee section(s) in scope)`
            : "Fees: placeholder mode (no fee-target sections)",
        index: feeScope.length ? 1 : 0,
        total: feeScope.length ? 1 : 0,
        label: "Placeholder",
      });
    }
    const placeholder = placeholderExtractFeeRows(document, sectionList, runId);
    for (const r of placeholder) {
      ensureFeeRowCommercialCategory(r);
      mergeCostBasisFromInference(r);
    }
    const dup = applyDuplicateFlagsToRows(placeholder);
    for (const r of placeholder) mergeAuditResultIntoRow(r);
    appendExtractionNote(
      document,
      `Deduplication scan: inputRows=${dup.inputRows} duplicateGroups=${dup.duplicateGroups} duplicateRows=${dup.duplicateRows}`
    );
    return {
      rows: placeholder,
      usedAi: false,
      extractionWarnings: [],
    };
  }

  const allRows = [];
  const warnings = [];

  const feeSections = (sectionList || []).filter(
    (s) =>
      s &&
      s.itemNumber !== "FULL" &&
      s.extractionTarget &&
      FEE_EXTRACTION_TARGET_SET.has(s.extractionTarget)
  );

  for (let si = 0; si < feeSections.length; si++) {
    const sec = feeSections[si];
    const feeSecIdx = si + 1;
    const sourceLabel = sec.sourceSectionLabel != null ? String(sec.sourceSectionLabel) : String(sec.itemNumber);
    const sourceHeading = (sec.sourceSectionHeading || sec.itemTitle || "").trim();
    const itemNumber = sourceLabel;
    const itemTitle = sourceHeading || `Section ${itemNumber}`;
    const sectionText = sec.sectionText || "";
    const itemLabel = formatSectionPromptLabel(sec);
    const docRefDefault = documentationReferenceFromSection(sec);
    const feeSectionTotal = feeSections.length;
    if (typeof onProgress === "function") {
      onProgress({
        stage: "fees",
        message: `Fees: section ${feeSecIdx}/${feeSectionTotal} · ${itemLabel}`.slice(0, 300),
        index: feeSecIdx,
        total: feeSectionTotal,
        label: itemLabel,
      });
    }

    if (isPlaceholderSectionText(sectionText)) {
      const msg = `${itemLabel}: skipped (no segmented text for this section).`;
      warnings.push(msg);
      appendExtractionNote(document, msg);
      continue;
    }

    try {
      const chunks = splitSectionTextIntoChunks(sectionText, FDD_CHUNK_MAX_CHARS, FDD_CHUNK_OVERLAP_CHARS);
      if (!chunks.length) {
        const msg = `${itemLabel}: internal chunking produced no segments.`;
        warnings.push(msg);
        appendExtractionNote(document, msg);
        continue;
      }
      const rowsPerChunk = [];
      const chunkErrors = [];

      for (const ch of chunks) {
        try {
          const content = await callChatCompletionsJson({
            system: buildExtractionSystemPrompt(),
            user: buildExtractionUserPrompt({
              itemNumber,
              itemTitle,
              sectionText: ch.chunkText,
              document,
              chunkIndex: ch.chunkIndex,
              chunkCount: ch.chunkCount,
              sourceFormat: sec.sourceFormat,
              extractionTarget: sec.extractionTarget,
              promptLabel: itemLabel,
              documentationReferenceHint: docRefDefault,
            }),
          });
          const rawRows = parseModelJsonRows(content);
          rowsPerChunk.push(rawRows.length);
          for (const raw of rawRows) {
            if (!raw || typeof raw !== "object") continue;
            const normalized = normalizeModelRowToSchema(raw, document, docRefDefault);
            const audit = buildFeeRowExtractionMeta({
              extractionRunId: runId,
              extractionUsedAi: true,
              modelNameUsed: FDD_MODEL_NAME,
              sourceItemNumber: itemNumber,
              sourceItemTitle: itemTitle,
              document,
              extractionConfidence: "Medium",
              sourceChunkIndex: ch.chunkIndex,
              sourceChunkCount: ch.chunkCount,
              extractionTarget: sec.extractionTarget,
            });
            const feeRow = {
              id: newId("fddrow"),
              ...normalized,
              ...audit,
              createdAt: nowIso(),
              updatedAt: nowIso(),
            };
            ensureFeeRowCommercialCategory(feeRow);
            mergeCostBasisFromInference(feeRow);
            allRows.push(feeRow);
          }
        } catch (chunkErr) {
          const msg = chunkErr && chunkErr.message ? chunkErr.message : String(chunkErr);
          chunkErrors.push(`chunk ${ch.chunkIndex + 1}/${ch.chunkCount}: ${msg}`);
          rowsPerChunk.push(0);
          warnings.push(`${itemLabel} chunk ${ch.chunkIndex + 1}/${ch.chunkCount}: ${msg}`);
        }
      }

      appendExtractionNote(
        document,
        `${itemLabel} AI: target=${sec.extractionTarget} sectionLen=${sectionText.length} chunkCount=${chunks.length} chunkMax=${FDD_CHUNK_MAX_CHARS} overlap=${FDD_CHUNK_OVERLAP_CHARS} rowsPerChunk=[${rowsPerChunk.join(",")}] chunkErrors=${chunkErrors.length ? chunkErrors.join(" | ") : "none"}`
      );

      const totalFromItem = rowsPerChunk.reduce((a, b) => a + b, 0);
      if (!totalFromItem) {
        warnings.push(`${itemLabel}: model returned zero rows across all chunks.`);
        appendExtractionNote(document, `${itemLabel}: model returned zero rows across all chunks.`);
      }
    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      warnings.push(`${itemLabel}: ${msg}`);
      appendExtractionNote(document, `${itemLabel}: ${msg}`);
    }
  }

  if (!allRows.length) {
    const fallback = placeholderExtractFeeRows(document, sectionList, runId);
    for (const r of fallback) {
      ensureFeeRowCommercialCategory(r);
      mergeCostBasisFromInference(r);
    }
    const dup = applyDuplicateFlagsToRows(fallback);
    for (const r of fallback) mergeAuditResultIntoRow(r);
    appendExtractionNote(
      document,
      `Deduplication scan: inputRows=${dup.inputRows} duplicateGroups=${dup.duplicateGroups} duplicateRows=${dup.duplicateRows}`
    );
    return {
      rows: fallback,
      usedAi: true,
      extractionWarnings: [...warnings, "No AI rows produced; used placeholder fallback."],
    };
  }

  const dup = applyDuplicateFlagsToRows(allRows);
  for (const r of allRows) mergeAuditResultIntoRow(r);
  appendExtractionNote(
    document,
    `Deduplication scan: inputRows=${dup.inputRows} duplicateGroups=${dup.duplicateGroups} duplicateRows=${dup.duplicateRows}`
  );

  return { rows: allRows, usedAi: true, extractionWarnings: warnings };
}

/**
 * Placeholder structured extraction — used when `FDD_INTELLIGENCE_MODEL_API_KEY` is not set or AI yields no rows.
 * Row shape matches AI output; empty-like values use "Unclear" for consistency with normalization.
 * @param {string} [extractionRunId] — when omitted, a new run id is generated (prefer passing from POST /extract).
 */
export function placeholderExtractFeeRows(document, sectionList, extractionRunId) {
  const runId = extractionRunId || newExtractionRunId();
  const brand = document.brandName || "Unknown Brand";
  const parent = document.parentCompany || "Unknown Parent";
  const year = document.fddYear || new Date().getFullYear();
  const feeInit =
    sectionList.find((s) => s.extractionTarget === "initial_fees") || sectionList.find((s) => String(s.itemNumber) === "5");
  const feeOther =
    sectionList.find((s) => s.extractionTarget === "other_fees") || sectionList.find((s) => String(s.itemNumber) === "6");
  const excerpt5 = (feeInit && feeInit.sectionText ? feeInit.sectionText : document.fullTextForExtraction || document.fullText || "").slice(0, 280);
  const ref5 = feeInit ? documentationReferenceFromSection(feeInit) : "Item 5";

  const base = {
    fddDocumentId: document.id,
    parentCompany: parent,
    brandName: brand,
    fddYear: year,
    country: document.country || "US",
    reviewStatus: "Needs Review",
    reviewerNotes: "Unclear",
    documentationReference: ref5,
    documentationReferencePageNumber: "Unclear",
    sourceTextExcerpt: excerpt5 || "Unclear",
    intakeMapping: "Unclear",
    matchScoreImpact: "Unclear",
  };

  const defs = [
    {
      itemNumber: "5",
      itemTitle: itemTitleFromCatalog("5"),
      row: {
        ...base,
        feeOrObligationName: "Initial franchise fee",
        feeType: "Initial Franchise Fee",
        amount: "50000",
        amountType: "fixed_usd",
        basis: "Per new franchise agreement",
        frequency: "One-time",
        dueTiming: "Upon signing franchise agreement",
        requiredOptional: "Required",
        lifecyclePhase: "Application",
        appliesWhen: "New development or conversion",
        conditionalTrigger: "Unclear",
        responsibleParty: "Franchisee",
        passThroughStatus: "N/A",
        bundledStatus: "Not bundled",
        canBeWaived: "Rarely",
        estimatedCostImpact: "High",
        implementationRisk: "Low",
      },
    },
    {
      itemNumber: "6",
      itemTitle: itemTitleFromCatalog("6"),
      row: {
        ...base,
        feeOrObligationName: "Continuing royalty",
        feeType: "Royalty Fee",
        amount: "5.5",
        amountType: "percent_of_gross_room_revenue",
        basis: "Gross rooms revenue (as defined in franchise agreement)",
        frequency: "Monthly",
        dueTiming: "Monthly in arrears",
        requiredOptional: "Required",
        lifecyclePhase: "Operations",
        appliesWhen: "Hotel operating under brand",
        conditionalTrigger: "Unclear",
        responsibleParty: "Franchisee",
        passThroughStatus: "N/A",
        bundledStatus: "N/A",
        canBeWaived: "No",
        estimatedCostImpact: "High",
        implementationRisk: "Medium",
        documentationReference: feeOther ? documentationReferenceFromSection(feeOther) : "Item 6",
        sourceTextExcerpt: (feeOther && feeOther.sectionText ? feeOther.sectionText : document.fullTextForExtraction || document.fullText || "").slice(0, 280),
      },
    },
    {
      itemNumber: "11",
      itemTitle: itemTitleFromCatalog("11"),
      row: {
        ...base,
        feeOrObligationName: "Brand marketing fund contribution",
        feeType: "Marketing Fee",
        amount: "2",
        amountType: "percent_of_gross_room_revenue",
        basis: "Same revenue base as royalty unless FDD states otherwise",
        frequency: "Monthly",
        dueTiming: "With royalty reporting",
        requiredOptional: "Required",
        lifecyclePhase: "Operations",
        appliesWhen: "Open and operating",
        conditionalTrigger: "Unclear",
        responsibleParty: "Franchisee",
        passThroughStatus: "Collected by franchisor",
        bundledStatus: "Pooled fund",
        canBeWaived: "No",
        estimatedCostImpact: "Medium",
        implementationRisk: "Low",
        documentationReference: "Item 11",
        sourceTextExcerpt: "Placeholder row for marketing / advertising — replace via AI extraction.",
      },
    },
    {
      itemNumber: "EX",
      itemTitle: itemTitleFromCatalog("EX"),
      row: {
        ...base,
        feeOrObligationName: "Property management system / CRS fees",
        feeType: "PMS/CRS Fee",
        amount: "Unclear",
        amountType: "vendor_pass_through",
        basis: "Per vendor invoice as described in technology schedule",
        frequency: "Monthly",
        dueTiming: "Per vendor terms",
        requiredOptional: "Required",
        lifecyclePhase: "Operations",
        appliesWhen: "Participation in brand reservation / distribution systems",
        conditionalTrigger: "If mandated brand systems apply",
        responsibleParty: "Franchisee",
        passThroughStatus: "Pass-through",
        bundledStatus: "Technology bundle",
        canBeWaived: "No",
        estimatedCostImpact: "Medium",
        implementationRisk: "Medium",
        documentationReference: "Exhibits / technology schedule",
        sourceTextExcerpt: "Placeholder — map exhibit tables with AI or manual review.",
      },
    },
  ];

  return defs.map(({ itemNumber, itemTitle, row }) => {
    const feeRow = {
      id: newId("fddrow"),
      ...row,
      ...buildFeeRowExtractionMeta({
        extractionRunId: runId,
        extractionUsedAi: false,
        modelNameUsed: "placeholder",
        sourceItemNumber: itemNumber,
        sourceItemTitle: itemTitle,
        document,
        extractionConfidence: "Low",
        extractionTarget:
          itemNumber === "5"
            ? "initial_fees"
            : itemNumber === "6"
              ? "other_fees"
              : itemNumber === "11"
                ? "systems_training_assistance"
                : "general_terms",
      }),
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    ensureFeeRowCommercialCategory(feeRow);
    mergeCostBasisFromInference(feeRow);
    return feeRow;
  });
}

async function tryExtractPdfText(buffer) {
  try {
    const mod = await import("pdf-parse");
    const pdfParse = mod.default;
    const data = await pdfParse(buffer);
    return data && typeof data.text === "string" ? data.text : "";
  } catch (e) {
    console.warn("[fdd-intelligence] PDF text extraction unavailable:", e && e.message ? e.message : e);
    return "";
  }
}

function stripDocumentForList(doc) {
  const { fullText, fullTextForExtraction, ...rest } = doc;
  return {
    ...rest,
    hasExtractedText: !!(fullText && String(fullText).trim()),
    textPreviewChars: fullText ? Math.min(fullText.length, 240) : 0,
  };
}

export const fddIntelligenceUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, FDD_UPLOAD_DIR),
    filename: (_req, file, cb) => {
      const safe = (file.originalname || "fdd.pdf").replace(/[^a-zA-Z0-9.-]/g, "_");
      cb(null, `fdd_${Date.now().toString(36)}_${safe}`);
    },
  }),
  limits: { fileSize: 40 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /\.pdf$/i.test(file.originalname || "") || (file.mimetype || "").toLowerCase() === "application/pdf";
    if (ok) return cb(null, true);
    cb(new Error("Only PDF uploads are allowed for FDD documents in this MVP."));
  },
});

export function fddCreateDocumentMaybeMultipart(req, res, next) {
  const ct = req.headers["content-type"] || "";
  if (ct.includes("multipart/form-data")) {
    return fddIntelligenceUpload.single("file")(req, res, (err) => {
      if (err) {
        if (err.code === "LIMIT_FILE_SIZE") {
          return res.status(413).json({ success: false, error: "FDD PDF exceeds 40 MB limit." });
        }
        return res.status(400).json({ success: false, error: err.message || "Upload failed" });
      }
      next();
    });
  }
  next();
}

function readBodyField(req, key) {
  if (req.body && req.body[key] != null) return String(req.body[key]).trim();
  return "";
}

function buildDocumentFromRequest(req) {
  const parentCompany = readBodyField(req, "parentCompany");
  const brandName = readBodyField(req, "brandName");
  const fddYear = parseInt(readBodyField(req, "fddYear"), 10) || new Date().getFullYear();
  const country = readBodyField(req, "country") || "US";
  const jurisdiction = readBodyField(req, "jurisdiction");
  const documentType = readBodyField(req, "documentType") || "FDD";
  const sourceType = readBodyField(req, "sourceType") || (req.file ? "upload" : "url");
  const sourceUrl = readBodyField(req, "sourceUrl");
  const notes = readBodyField(req, "notes");
  let fileName = readBodyField(req, "fileName");
  let filePath = readBodyField(req, "filePath");
  let fullText = readBodyField(req, "pastedText") || readBodyField(req, "fullText");

  if (req.file) {
    fileName = req.file.originalname || fileName || "fdd.pdf";
    filePath = req.file.path;
  }

  if (!brandName) {
    const err = new Error("brandName is required");
    err.statusCode = 400;
    throw err;
  }

  const id = newId("fdddoc");
  const doc = {
    id,
    parentCompany: parentCompany || null,
    brandName,
    fddYear,
    country,
    jurisdiction: jurisdiction || null,
    documentType,
    sourceType,
    sourceUrl: sourceUrl || null,
    fileName: fileName || null,
    filePath: filePath || null,
    fullText: fullText || "",
    extractionStatus: "pending",
    extractedAt: null,
    reviewedAt: null,
    reviewer: null,
    notes: notes || null,
    extractionNotes: null,
    createdAt: nowIso(),
    updatedAt: nowIso(),
  };
  return doc;
}

const AT_FDD_TEXT_TRUNCATE = 95000;

function truncateAtLongText(s) {
  const t = String(s || "");
  return t.length > AT_FDD_TEXT_TRUNCATE ? t.slice(0, AT_FDD_TEXT_TRUNCATE) : t;
}

/**
 * Coerce values for Airtable singleLineText (and similar): finite numbers → string;
 * null / undefined / blank after trim → undefined so callers can omit or default.
 */
function textValueForAirtable(value) {
  if (value === null || value === undefined) return undefined;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  const s = String(value).trim();
  return s.length ? s : undefined;
}

function omitUndefinedFields(obj) {
  const o = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v !== undefined) o[k] = v;
  }
  return o;
}

/** Map Airtable FDD Documents record → API document (id = Airtable record id). */
function documentFromAirtable(rec) {
  const f = rec.fields || {};
  const fullText = f[AT_FDD_DOC_FULL_TEXT_FIELD] != null ? String(f[AT_FDD_DOC_FULL_TEXT_FIELD]) : "";
  return {
    id: rec.id,
    parentCompany: f["Parent Company"] ?? null,
    brandName: f["Brand Name"] || "",
    fddYear: typeof f["FDD Year"] === "number" ? f["FDD Year"] : parseInt(String(f["FDD Year"] || ""), 10) || new Date().getFullYear(),
    country: f["Country"] || "US",
    jurisdiction: f["Jurisdiction"] ?? null,
    documentType: f["Document Type"] || "FDD",
    sourceType: f["Source Type"] || "",
    sourceUrl: f["Source URL"] ?? null,
    fileName: f["File Name"] ?? null,
    filePath: f["File Path"] ?? null,
    fullText,
    extractionStatus: f["Extraction Status"] || "pending",
    extractionNotes: f["Extraction Notes"] ?? null,
    extractedAt: f["Extracted At"] || null,
    reviewedAt: f["Reviewed At"] || null,
    reviewer: f["Reviewer"] ?? null,
    notes: f["Notes"] ?? null,
    createdAt: f["Created At"] || rec._rawJson?.createdTime || nowIso(),
    updatedAt: f["Updated At"] || nowIso(),
  };
}

/** Map API document → Airtable FDD Documents fields. PDF stays local; File Path stores server path string. */
function documentToAirtableFields(doc, { isCreate } = { isCreate: false }) {
  const fields = {
    "Parent Company": doc.parentCompany || "",
    "Brand Name": doc.brandName || "",
    "FDD Year": Number.isFinite(Number(doc.fddYear)) ? Number(doc.fddYear) : new Date().getFullYear(),
    Country: doc.country || "US",
    Jurisdiction: doc.jurisdiction || "",
    "Document Type": doc.documentType || "FDD",
    "Source Type": doc.sourceType || "",
    "Source URL": doc.sourceUrl || "",
    "File Name": doc.fileName || "",
    "File Path": doc.filePath || "",
    "Extraction Status": doc.extractionStatus || "pending",
    "Extraction Notes": doc.extractionNotes || "",
    Reviewer: doc.reviewer || "",
    Notes: doc.notes || "",
    [AT_FDD_DOC_FULL_TEXT_FIELD]: truncateAtLongText(doc.fullText || ""),
    "Updated At": doc.updatedAt || nowIso(),
  };
  if (doc.extractedAt) fields["Extracted At"] = doc.extractedAt;
  if (doc.reviewedAt) fields["Reviewed At"] = doc.reviewedAt;
  if (isCreate) {
    fields["Created At"] = doc.createdAt || nowIso();
  }
  return fields;
}

/** Airtable Page Start / Page End are singleLineText: write non-blank scalars as strings; omit when unknown (same as prior undefined). */
function pageRefToAirtableText(value) {
  return textValueForAirtable(value);
}

/** Read page refs from Airtable (string, legacy number, or empty). */
function pageRefFromAirtableField(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  const s = String(value).trim();
  return s.length ? s : null;
}

function sectionFromAirtable(rec) {
  const f = rec.fields || {};
  const itemNum = f["Item Number"] != null ? String(f["Item Number"]) : "";
  const extTargetRaw = f["Extraction Target"] != null ? String(f["Extraction Target"]).trim() : "";
  const extractionTarget =
    extTargetRaw || US_ITEM_NUMBER_TO_EXTRACTION_TARGET[itemNum] || (itemNum === "FULL" ? "general_terms" : "");
  return {
    id: rec.id,
    fddDocumentId: f["FDD Document ID"] || "",
    itemNumber: itemNum,
    itemTitle: f["Item Title"] || "",
    sectionText: f["Section Text"] != null ? String(f["Section Text"]) : "",
    pageStart: pageRefFromAirtableField(f["Page Start"]),
    pageEnd: pageRefFromAirtableField(f["Page End"]),
    extractionStatus: f["Extraction Status"] || "",
    sourceFormat: f["Source Format"] || "us_fdd_item",
    sourceSectionLabel: f["Source Section Label"] != null ? String(f["Source Section Label"]) : itemNum,
    sourceSectionHeading: f["Source Section Heading"] || "",
    extractionTarget,
    candidateSourceType: f["Candidate Source Type"] || "",
    segmentOrder: typeof f["Segment Order"] === "number" ? f["Segment Order"] : parseInt(String(f["Segment Order"] || "0"), 10) || 0,
    createdAt: f["Created At"] || nowIso(),
    updatedAt: f["Updated At"] || nowIso(),
  };
}

/**
 * FDD Sections → Airtable.
 * Linked field "FDD Document" is intentionally omitted for MVP — store plain-text `FDD Document ID` (Airtable record id).
 * Later: switch to linked record array `[docRecordId]` once the column type is confirmed.
 */
function sectionToAirtableFields(docRecordId, sec) {
  return omitUndefinedFields({
    "FDD Document ID": textValueForAirtable(docRecordId) ?? "",
    "Item Number": textValueForAirtable(sec.itemNumber) ?? "",
    "Item Title": sec.itemTitle || "",
    "Section Text": truncateAtLongText(sec.sectionText || ""),
    "Page Start": pageRefToAirtableText(sec.pageStart),
    "Page End": pageRefToAirtableText(sec.pageEnd),
    "Extraction Status": sec.extractionStatus || "extracted",
    "Source Format": sec.sourceFormat || "us_fdd_item",
    "Source Section Label": sec.sourceSectionLabel != null ? String(sec.sourceSectionLabel).slice(0, 900) : "",
    "Source Section Heading": sec.sourceSectionHeading != null ? String(sec.sourceSectionHeading).slice(0, 9000) : "",
    "Extraction Target": sec.extractionTarget || "",
    "Candidate Source Type": sec.candidateSourceType != null ? String(sec.candidateSourceType).slice(0, 900) : "",
    "Segment Order": Number.isFinite(Number(sec.segmentOrder)) ? Number(sec.segmentOrder) : 0,
    "Created At": sec.createdAt || nowIso(),
    "Updated At": sec.updatedAt || nowIso(),
  });
}

function parseFeeRowAuditNumber(value) {
  if (value == null || value === "") return undefined;
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function parseFeeRowAuditBool(value) {
  if (value === true || value === false) return value;
  if (value == null) return false;
  const s = String(value).trim().toLowerCase();
  return s === "1" || s === "true" || s === "yes";
}

function feeRowAuditFieldsToAirtable(row) {
  if (!row || (!row.lastAuditedAt && !(row.auditVersion && String(row.auditVersion).trim()))) {
    return {};
  }
  return omitUndefinedFields({
    "Audit Score": parseFeeRowAuditNumber(row.auditScore),
    "Audit Confidence":
      row.auditConfidence != null && String(row.auditConfidence).trim()
        ? String(row.auditConfidence).slice(0, 900)
        : undefined,
    "Audit Status":
      row.auditStatus != null && String(row.auditStatus).trim() ? String(row.auditStatus).slice(0, 900) : undefined,
    "Audit Issues": row.auditIssues != null && String(row.auditIssues).trim() ? truncateAtLongText(String(row.auditIssues)) : undefined,
    "Auto-Approve Eligible": !!row.autoApproveEligible,
    "Last Audited At": row.lastAuditedAt || undefined,
    "Audit Version": row.auditVersion != null ? String(row.auditVersion).slice(0, 900) : undefined,
    "Source Support Score": parseFeeRowAuditNumber(row.sourceSupportScore),
    "Amount Quality Score": parseFeeRowAuditNumber(row.amountQualityScore),
    "Basis Quality Score": parseFeeRowAuditNumber(row.basisQualityScore),
    "Category Quality Score": parseFeeRowAuditNumber(row.categoryQualityScore),
    "Duplicate Risk Score": parseFeeRowAuditNumber(row.duplicateRiskScore),
    "Legal Risk Score": parseFeeRowAuditNumber(row.legalRiskScore),
  });
}

function feeRowFromAirtable(rec) {
  const f = rec.fields || {};
  const out = {
    id: rec.id,
    fddDocumentId: f["FDD Document ID"] || "",
    parentCompany: f["Parent Company"] || "",
    brandName: f["Brand Name"] || "",
    fddYear: typeof f["FDD Year"] === "number" ? f["FDD Year"] : parseInt(String(f["FDD Year"] || ""), 10) || new Date().getFullYear(),
    country: f["Country"] || "US",
    feeOrObligationName: f["Fee / Obligation Name"] || "",
    feeType: f["Fee Type"] || "",
    commercialCategory: f["Commercial Category"] != null ? String(f["Commercial Category"]).trim() : "",
    possibleDuplicate: !!f["Possible Duplicate"],
    duplicateGroupKey: f["Duplicate Group Key"] != null ? String(f["Duplicate Group Key"]).trim() : "",
    amount: f["Amount"] != null ? String(f["Amount"]) : "",
    amountType: f["Amount Type"] || "",
    basis: f["Basis"] || "",
    frequency: f["Frequency"] || "",
    dueTiming: f["Due Timing"] || "",
    requiredOptional: f["Required / Optional"] || "",
    lifecyclePhase: f["Lifecycle Phase"] || "",
    appliesWhen: f["Applies When"] || "",
    conditionalTrigger: f["Conditional Trigger"] || "",
    responsibleParty: f["Responsible Party"] || "",
    passThroughStatus: f["Pass-Through Status"] || "",
    bundledStatus: f["Bundled Status"] || "",
    canBeWaived: f["Can Be Waived"] || "",
    estimatedCostImpact: f["Estimated Cost Impact"] || "",
    implementationRisk: f["Implementation Risk"] || "",
    intakeMapping: f["Intake Mapping"] || "",
    matchScoreImpact: f["Match Score Impact"] || "",
    documentationReference: f["Documentation Reference"] || "",
    documentationReferencePageNumber:
      f["Documentation Reference Page Number"] != null ? String(f["Documentation Reference Page Number"]) : "",
    sourceTextExcerpt: f["Source Text Excerpt"] || "",
    reviewerNotes: f["Reviewer Notes"] || "",
    normalizedCostBasis: f["Normalized Cost Basis"] != null ? String(f["Normalized Cost Basis"]).trim() : "",
    rawCostBasisText: f["Raw Cost Basis Text"] != null ? String(f["Raw Cost Basis Text"]) : "",
    amountFormulaType: f["Amount Formula Type"] != null ? String(f["Amount Formula Type"]).trim() : "",
    calculationUnit: f["Calculation Unit"] != null ? String(f["Calculation Unit"]).trim() : "",
    revenueBase: f["Revenue Base"] != null ? String(f["Revenue Base"]).trim() : "",
    unitRate: f["Unit Rate"] != null ? String(f["Unit Rate"]).trim() : "",
    percentageRate: f["Percentage Rate"] != null ? String(f["Percentage Rate"]).trim() : "",
    fixedAmount: f["Fixed Amount"] != null ? String(f["Fixed Amount"]).trim() : "",
    formulaNotes: f["Formula Notes"] != null ? String(f["Formula Notes"]) : "",
    basisConfidence: f["Basis Confidence"] != null ? String(f["Basis Confidence"]).trim() : "",
    basisNeedsReview: !!f["Basis Needs Review"],
    reviewStatus: normalizeFddReviewStatusFromAirtable(f[AT_FDD_FEE_REVIEW_STATUS_FIELD]),
    extractionRunId: f["Extraction Run ID"] || "",
    extractionUsedAi: !!f["Extraction Used AI"],
    modelNameUsed: f["Model Name Used"] || "",
    sourceItemNumber: f["Source Item Number"] != null ? String(f["Source Item Number"]) : "",
    sourceItemTitle: f["Source Item Title"] || "",
    sourceDocumentId: f["Source Document ID"] || "",
    sourceDocumentBrandName: f["Source Document Brand Name"] || "",
    sourceDocumentFddYear:
      typeof f["Source Document FDD Year"] === "number"
        ? f["Source Document FDD Year"]
        : parseInt(String(f["Source Document FDD Year"] || ""), 10) || new Date().getFullYear(),
    extractionConfidence: f["Extraction Confidence"] || "",
    needsLegalReview: !!f["Needs Legal Review"],
    needsCommercialReview: f["Needs Commercial Review"] !== false && f["Needs Commercial Review"] !== undefined ? !!f["Needs Commercial Review"] : true,
    sourceChunkIndex:
      f["Source Chunk Index"] != null && String(f["Source Chunk Index"]).trim() !== ""
        ? parseInt(String(f["Source Chunk Index"]), 10) || 0
        : 0,
    sourceChunkCount:
      f["Source Chunk Count"] != null && String(f["Source Chunk Count"]).trim() !== ""
        ? parseInt(String(f["Source Chunk Count"]), 10) || 1
        : 1,
    createdAt: f["Created At"] || nowIso(),
    updatedAt: f["Updated At"] || nowIso(),
    auditScore: parseFeeRowAuditNumber(f["Audit Score"]),
    auditConfidence: f["Audit Confidence"] != null ? String(f["Audit Confidence"]).trim() : "",
    auditStatus: f["Audit Status"] != null ? String(f["Audit Status"]).trim() : "",
    auditIssues: f["Audit Issues"] != null ? String(f["Audit Issues"]) : "",
    autoApproveEligible: parseFeeRowAuditBool(f["Auto-Approve Eligible"]),
    lastAuditedAt: f["Last Audited At"] || "",
    auditVersion: f["Audit Version"] != null ? String(f["Audit Version"]).trim() : "",
    sourceSupportScore: parseFeeRowAuditNumber(f["Source Support Score"]),
    amountQualityScore: parseFeeRowAuditNumber(f["Amount Quality Score"]),
    basisQualityScore: parseFeeRowAuditNumber(f["Basis Quality Score"]),
    categoryQualityScore: parseFeeRowAuditNumber(f["Category Quality Score"]),
    duplicateRiskScore: parseFeeRowAuditNumber(f["Duplicate Risk Score"]),
    legalRiskScore: parseFeeRowAuditNumber(f["Legal Risk Score"]),
  };
  ensureFeeRowCommercialCategory(out);
  mergeCostBasisFromInference(out);
  return out;
}

function feeRowToAirtableFields(row) {
  return omitUndefinedFields({
    "FDD Document ID": textValueForAirtable(row.fddDocumentId) ?? "",
    "Parent Company": row.parentCompany || "",
    "Brand Name": row.brandName || "",
    "FDD Year": Number.isFinite(Number(row.fddYear)) ? Number(row.fddYear) : new Date().getFullYear(),
    Country: row.country || "US",
    "Fee / Obligation Name": row.feeOrObligationName || "",
    "Fee Type": row.feeType || "",
    "Commercial Category": commercialCategoryForPersistence(row),
    "Possible Duplicate": !!row.possibleDuplicate,
    "Duplicate Group Key": String(row.duplicateGroupKey || "").slice(0, DUPLICATE_GROUP_KEY_MAX_LEN),
    Amount: row.amount != null ? String(row.amount) : "",
    "Amount Type": row.amountType || "",
    Basis: row.basis || "",
    Frequency: row.frequency || "",
    "Due Timing": row.dueTiming || "",
    "Required / Optional": row.requiredOptional || "",
    "Lifecycle Phase": row.lifecyclePhase || "",
    "Applies When": row.appliesWhen || "",
    "Conditional Trigger": row.conditionalTrigger || "",
    "Responsible Party": row.responsibleParty || "",
    "Pass-Through Status": row.passThroughStatus || "",
    "Bundled Status": row.bundledStatus || "",
    "Can Be Waived": row.canBeWaived || "",
    "Estimated Cost Impact": row.estimatedCostImpact || "",
    "Implementation Risk": row.implementationRisk || "",
    "Intake Mapping": row.intakeMapping || "",
    "Match Score Impact": row.matchScoreImpact || "",
    "Documentation Reference": row.documentationReference || "",
    "Documentation Reference Page Number": textValueForAirtable(row.documentationReferencePageNumber) ?? "",
    "Source Text Excerpt": truncateAtLongText(row.sourceTextExcerpt || ""),
    "Reviewer Notes": row.reviewerNotes || "",
    ...{ [AT_FDD_FEE_REVIEW_STATUS_FIELD]: normalizeFddReviewStatusFromAirtable(row.reviewStatus) },
    "Normalized Cost Basis": String(row.normalizedCostBasis ?? "").slice(0, 900),
    "Raw Cost Basis Text": truncateAtLongText(row.rawCostBasisText || ""),
    "Amount Formula Type": String(row.amountFormulaType ?? "").slice(0, 900),
    "Calculation Unit": String(row.calculationUnit ?? "").slice(0, 900),
    "Revenue Base": String(row.revenueBase ?? "").slice(0, 900),
    "Unit Rate": String(row.unitRate ?? "").slice(0, 900),
    "Percentage Rate": String(row.percentageRate ?? "").slice(0, 900),
    "Fixed Amount": String(row.fixedAmount ?? "").slice(0, 900),
    "Formula Notes": truncateAtLongText(row.formulaNotes || ""),
    "Basis Confidence": String(row.basisConfidence ?? "").slice(0, 900),
    "Basis Needs Review": !!row.basisNeedsReview,
    "Extraction Run ID": row.extractionRunId || "",
    "Extraction Used AI": !!row.extractionUsedAi,
    "Model Name Used": row.modelNameUsed || "",
    "Source Item Number": textValueForAirtable(row.sourceItemNumber) ?? "",
    "Source Item Title": row.sourceItemTitle || "",
    "Source Document ID": textValueForAirtable(row.sourceDocumentId) ?? "",
    "Source Document Brand Name": row.sourceDocumentBrandName || "",
    "Source Document FDD Year": normalizeFddScalar(row.sourceDocumentFddYear ?? row.fddYear),
    "Extraction Confidence": row.extractionConfidence || "",
    "Source Chunk Index": textValueForAirtable(row.sourceChunkIndex ?? 0) ?? "0",
    "Source Chunk Count": textValueForAirtable(row.sourceChunkCount ?? 1) ?? "1",
    "Needs Legal Review": !!row.needsLegalReview,
    "Needs Commercial Review": row.needsCommercialReview !== false,
    "Created At": row.createdAt || nowIso(),
    "Updated At": row.updatedAt || nowIso(),
    ...feeRowAuditFieldsToAirtable(row),
  });
}

async function atListDocuments() {
  const base = getFddAirtableBase();
  const records = await base(AIRTABLE_DOCS_TABLE)
    .select({
      sort: [{ field: "Updated At", direction: "desc" }],
      pageSize: 100,
    })
    .all();
  return records.map(documentFromAirtable);
}

async function atGetDocument(recordId, { includeFullText } = { includeFullText: false }) {
  const base = getFddAirtableBase();
  try {
    const rec = await base(AIRTABLE_DOCS_TABLE).find(recordId);
    const doc = documentFromAirtable(rec);
    if (!includeFullText) delete doc.fullText;
    return doc;
  } catch {
    return null;
  }
}

/** Load document with full text for FDD Terms extraction (used by api/fdd-intelligence-terms.js). */
export async function loadFddDocumentByIdForTerms(id) {
  if (!id) return null;
  if (isFddAirtablePersistence()) {
    return await atGetDocument(id, { includeFullText: true });
  }
  const d = documents.get(id);
  if (d) return { ...d };
  if (id === "mock_doc_sample") {
    const m = getMockDocumentsList()[0];
    return { ...m };
  }
  return null;
}

async function atCreateDocument(doc) {
  const base = getFddAirtableBase();
  const [rec] = await base(AIRTABLE_DOCS_TABLE).create([{ fields: documentToAirtableFields(doc, { isCreate: true }) }]);
  return documentFromAirtable(rec);
}

async function atUpdateDocument(doc) {
  const base = getFddAirtableBase();
  const [rec] = await base(AIRTABLE_DOCS_TABLE).update([{ id: doc.id, fields: documentToAirtableFields(doc, { isCreate: false }) }]);
  return documentFromAirtable(rec);
}

async function atDeleteSectionsAndRowsForDoc(docRecordId) {
  const base = getFddAirtableBase();
  const idEsc = escapeAtFormula(docRecordId);
  const secFormula = `{FDD Document ID}='${idEsc}'`;
  const secs = await base(AIRTABLE_SECTIONS_TABLE).select({ filterByFormula: secFormula, pageSize: 100 }).all();
  for (const r of secs) {
    await r.destroy();
  }
  const rowFormula = `{FDD Document ID}='${idEsc}'`;
  const rowRecs = await base(AIRTABLE_ROWS_TABLE).select({ filterByFormula: rowFormula, pageSize: 100 }).all();
  for (const r of rowRecs) {
    await r.destroy();
  }
}

async function atInsertSections(docRecordId, sectionList) {
  const base = getFddAirtableBase();
  const created = [];
  for (const s of sectionList) {
    const fields = sectionToAirtableFields(docRecordId, s);
    const [rec] = await base(AIRTABLE_SECTIONS_TABLE).create([{ fields }]);
    created.push(sectionFromAirtable(rec));
  }
  return created;
}

async function atInsertFeeRows(rowsIn) {
  const base = getFddAirtableBase();
  const out = [];
  for (const row of rowsIn) {
    mergeAuditResultIntoRow(row);
    const payload = { ...feeRowToAirtableFields(row) };
    const [rec] = await base(AIRTABLE_ROWS_TABLE).create([{ fields: payload }]);
    out.push(feeRowFromAirtable(rec));
  }
  return out;
}

async function atListSectionsForDoc(docRecordId) {
  const base = getFddAirtableBase();
  const idEsc = escapeAtFormula(docRecordId);
  const records = await base(AIRTABLE_SECTIONS_TABLE)
    .select({
      filterByFormula: `{FDD Document ID}='${idEsc}'`,
      pageSize: 100,
    })
    .all();
  const out = records.map(sectionFromAirtable);
  out.sort((a, b) => (a.segmentOrder ?? 99999) - (b.segmentOrder ?? 99999) || String(a.itemNumber).localeCompare(String(b.itemNumber), undefined, { numeric: true }));
  return out;
}

async function atListRowsForDoc(docRecordId) {
  const base = getFddAirtableBase();
  const idEsc = escapeAtFormula(docRecordId);
  const records = await base(AIRTABLE_ROWS_TABLE)
    .select({
      filterByFormula: `{FDD Document ID}='${idEsc}'`,
      pageSize: 100,
    })
    .all();
  return records.map(feeRowFromAirtable);
}

async function atGetFeeRow(recordId) {
  const base = getFddAirtableBase();
  try {
    const rec = await base(AIRTABLE_ROWS_TABLE).find(recordId);
    return feeRowFromAirtable(rec);
  } catch {
    return null;
  }
}

async function atUpdateFeeRow(row) {
  const base = getFddAirtableBase();
  row.updatedAt = nowIso();
  const [rec] = await base(AIRTABLE_ROWS_TABLE).update([{ id: row.id, fields: feeRowToAirtableFields(row) }]);
  return feeRowFromAirtable(rec);
}

async function atListApprovedRowsByBrandNames(brandNamesLower) {
  const base = getFddAirtableBase();
  const set = new Set(brandNamesLower.map((b) => String(b).trim().toLowerCase()).filter(Boolean));
  if (!set.size) return [];
  const records = await base(AIRTABLE_ROWS_TABLE)
    .select({
      filterByFormula: AT_REVIEW_STATUS_APPROVED_ONLY_FORMULA,
      pageSize: 100,
    })
    .all();
  return records
    .map(feeRowFromAirtable)
    .filter((r) => set.has(String(r.brandName || "").trim().toLowerCase()) && r.reviewStatus === "Approved");
}

/**
 * Rows for brand economics: prefer all fee rows for that brand (no status in Airtable formula — avoids
 * single-select label mismatches), then keep Approved + Needs Review in app. Falls back to status-scoped
 * query if the brand formula returns nothing (e.g. unsupported field type).
 */
async function atListApprovedOrNeedsReviewRowsForBrand(brandName) {
  const base = getFddAirtableBase();
  const nb = normBrand(brandName);
  if (!nb) return [];

  const mapAndFilter = (records) =>
    records
      .map(feeRowFromAirtable)
      .filter((r) => normBrand(r.brandName) === nb && isEconomicsScopeReviewStatus(r.reviewStatus));

  const brandExactFormula = `{Brand Name}='${escapeAtFormula(String(brandName || "").trim())}'`;
  try {
    const exact = await base(AIRTABLE_ROWS_TABLE).select({ filterByFormula: brandExactFormula, pageSize: 100 }).all();
    if (exact.length) return mapAndFilter(exact);
  } catch (e) {
    console.warn("[fdd-intelligence] economics rows: exact brand formula failed:", e?.message || e);
  }

  const brandLowerFormula = `LOWER(TRIM({Brand Name}))='${escapeAtFormula(nb)}'`;
  try {
    const byBrand = await base(AIRTABLE_ROWS_TABLE).select({ filterByFormula: brandLowerFormula, pageSize: 100 }).all();
    if (byBrand.length) return mapAndFilter(byBrand);
  } catch (e) {
    console.warn("[fdd-intelligence] economics rows: brand LOWER(TRIM) formula failed:", e?.message || e);
  }

  const records = await base(AIRTABLE_ROWS_TABLE)
    .select({
      filterByFormula: AT_REVIEW_STATUS_APPROVED_OR_NEEDS_FORMULA,
      pageSize: 100,
    })
    .all();
  return mapAndFilter(records);
}

/** POST /api/fdd-intelligence/documents */
export async function postFddDocument(req, res) {
  try {
    const doc = buildDocumentFromRequest(req);
    if (doc.filePath && fs.existsSync(doc.filePath) && !doc.fullText) {
      const buf = fs.readFileSync(doc.filePath);
      doc.fullText = await tryExtractPdfText(buf);
      if (!doc.fullText.trim()) {
        doc.notes = [doc.notes, "PDF on disk; plain text empty (OCR or pdf-parse may be needed)."].filter(Boolean).join(" ");
      }
    }
    if (isFddAirtablePersistence()) {
      const saved = await atCreateDocument(doc);
      return res.status(201).json({
        success: true,
        storage: "airtable",
        airtable: null,
        document: stripDocumentForList(saved),
      });
    }
    documents.set(doc.id, doc);
    return res.status(201).json({
      success: true,
      storage: "memory",
      airtable: airtablePersistenceNote(),
      document: stripDocumentForList(doc),
    });
  } catch (e) {
    const code = e.statusCode || 500;
    res.status(code).json({ success: false, error: e.message || "Create failed" });
  }
}

/** GET /api/fdd-intelligence/documents */
export async function listFddDocuments(_req, res) {
  try {
    if (isFddAirtablePersistence()) {
      const list = (await atListDocuments()).map(stripDocumentForList);
      return res.json({
        success: true,
        storage: "airtable",
        airtable: null,
        documents: list,
      });
    }
    const list = Array.from(documents.values())
      .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
      .map(stripDocumentForList);
    return res.json({
      success: true,
      storage: "memory",
      airtable: airtablePersistenceNote(),
      documents: list.length ? list : getMockDocumentsList(),
    });
  } catch (e) {
    console.error("[fdd-intelligence] list documents:", e);
    res.status(500).json({ success: false, error: e.message || "List failed" });
  }
}

function getMockDocumentsList() {
  return [
    {
      id: "mock_doc_sample",
      parentCompany: "Sample Hotels Inc.",
      brandName: "Sample Brand",
      fddYear: new Date().getFullYear(),
      country: "US",
      jurisdiction: null,
      documentType: "FDD",
      sourceType: "mock",
      sourceUrl: null,
      fileName: null,
      filePath: null,
      extractionStatus: "extracted",
      extractedAt: nowIso(),
      reviewedAt: null,
      reviewer: null,
      notes: "Mock registry row — create a real document via POST to replace in-memory data.",
      createdAt: nowIso(),
      updatedAt: nowIso(),
      hasExtractedText: false,
      textPreviewChars: 0,
      _mock: true,
    },
  ];
}

/** GET /api/fdd-intelligence/documents/:id */
export async function getFddDocument(req, res) {
  const { id } = req.params;
  const includeText = String(req.query.includeText || "") === "1";
  try {
    if (isFddAirtablePersistence()) {
      const doc = await atGetDocument(id, { includeFullText: includeText });
      if (!doc) {
        return res.status(404).json({ success: false, error: "Document not found" });
      }
      return res.json({
        success: true,
        storage: "airtable",
        airtable: null,
        document: includeText ? doc : stripDocumentForList(doc),
        fullText: includeText ? doc.fullText : undefined,
      });
    }
    const doc = documents.get(id);
    if (!doc) {
      if (id === "mock_doc_sample") {
        return res.json({ success: true, storage: "memory", document: getMockDocumentsList()[0], mock: true });
      }
      return res.status(404).json({ success: false, error: "Document not found" });
    }
    return res.json({
      success: true,
      storage: "memory",
      airtable: airtablePersistenceNote(),
      document: includeText ? doc : stripDocumentForList(doc),
      fullText: includeText ? doc.fullText : undefined,
    });
  } catch (e) {
    console.error("[fdd-intelligence] get document:", e);
    res.status(500).json({ success: false, error: e.message || "Read failed" });
  }
}

/** POST /api/fdd-intelligence/documents/:id/extract */
export async function postFddExtract(req, res) {
  const { id } = req.params;

  if (isFddAirtablePersistence()) {
    let doc = null;
    try {
      doc = await atGetDocument(id, { includeFullText: true });
      if (!doc) return res.status(404).json({ success: false, error: "Document not found" });
      doc.extractionStatus = "extracting";
      doc.updatedAt = nowIso();
      await atUpdateDocument(doc);

      await atDeleteSectionsAndRowsForDoc(id);

      const { fullTextForExtraction, extractionTextSource, sectionList, createdSections } = await fddResolveSegmentAndInsertSections(
        doc,
        id
      );

      doc.fullTextForExtraction = fullTextForExtraction;
      const extractionRunId = newExtractionRunId();
      const { rows: newRows, usedAi, extractionWarnings } = await extractFeeRowsForDocument(doc, createdSections, extractionRunId);
      delete doc.fullTextForExtraction;

      const persistedRows = await atInsertFeeRows(newRows);

      appendSegmentationTocMismatchWarning(doc, sectionList, fullTextForExtraction);
      appendFullTextStoragePolicyNote(doc, extractionTextSource, fullTextForExtraction.length, true);

      doc.fullText = fullTextForExtraction;

      doc.extractionStatus = "extracted";
      doc.extractedAt = nowIso();
      doc.updatedAt = nowIso();
      await atUpdateDocument(doc);

      return res.json({
        success: true,
        storage: "airtable",
        airtable: null,
        document: stripDocumentForList(doc),
        sectionsCreated: createdSections.length,
        rowsCreated: persistedRows.length,
        extractionRunId,
        extractionUsedAi: usedAi,
        extractionWarnings: extractionWarnings && extractionWarnings.length ? extractionWarnings : undefined,
      });
    } catch (e) {
      console.error("[fdd-intelligence] extract error (airtable):", e);
      if (doc) {
        try {
          doc.extractionStatus = "extract_failed";
          doc.updatedAt = nowIso();
          await atUpdateDocument(doc);
        } catch (_) {
          /* ignore secondary write failure */
        }
      }
      return res.status(500).json({ success: false, error: e.message || "Extraction failed" });
    }
  }

  const doc = documents.get(id);
  if (!doc) return res.status(404).json({ success: false, error: "Document not found" });

  doc.extractionStatus = "extracting";
  doc.updatedAt = nowIso();

  try {
    for (const [sid, sec] of sections) {
      if (sec.fddDocumentId === id) sections.delete(sid);
    }
    for (const [rid, row] of rows) {
      if (row.fddDocumentId === id) rows.delete(rid);
    }

    const { fullTextForExtraction, extractionTextSource, sectionList, createdSections } = await fddResolveSegmentAndInsertSections(
      doc,
      id
    );

    const extractionRunId = newExtractionRunId();
    doc.fullTextForExtraction = fullTextForExtraction;
    const { rows: newRows, usedAi, extractionWarnings } = await extractFeeRowsForDocument(doc, createdSections, extractionRunId);
    delete doc.fullTextForExtraction;
    for (const r of newRows) mergeAuditResultIntoRow(r);
    for (const r of newRows) rows.set(r.id, r);

    appendSegmentationTocMismatchWarning(doc, sectionList, fullTextForExtraction);
    appendFullTextStoragePolicyNote(doc, extractionTextSource, fullTextForExtraction.length, false);

    doc.fullText = fullTextForExtraction;
    doc.extractionStatus = "extracted";
    doc.extractedAt = nowIso();
    doc.updatedAt = nowIso();
    documents.set(id, doc);

    return res.json({
      success: true,
      storage: "memory",
      airtable: airtablePersistenceNote(),
      document: stripDocumentForList(doc),
      sectionsCreated: createdSections.length,
      rowsCreated: newRows.length,
      extractionRunId,
      extractionUsedAi: usedAi,
      extractionWarnings: extractionWarnings && extractionWarnings.length ? extractionWarnings : undefined,
    });
  } catch (e) {
    doc.extractionStatus = "extract_failed";
    doc.updatedAt = nowIso();
    documents.set(id, doc);
    console.error("[fdd-intelligence] extract error:", e);
    return res.status(500).json({ success: false, error: e.message || "Extraction failed" });
  }
}

function summarizeFddFullFeeAudit(rows) {
  const byStatus = {};
  for (const r of rows || []) {
    const k = String(r.auditStatus || "").trim() || "(blank)";
    byStatus[k] = (byStatus[k] || 0) + 1;
  }
  return { byStatus, total: (rows || []).length };
}

function summarizeFddFullFeeDuplicates(rows) {
  const list = rows || [];
  let possibleDuplicateRows = 0;
  const gkeys = new Set();
  for (const r of list) {
    if (r.possibleDuplicate) possibleDuplicateRows += 1;
    const g = String(r.duplicateGroupKey || "").trim();
    if (g) gkeys.add(g);
  }
  return { totalRows: list.length, possibleDuplicateRows, duplicateGroupKeyCount: gkeys.size };
}

function summarizeFddFullTermsCategories(terms) {
  const byCategory = {};
  for (const t of terms || []) {
    const k = String(t.termCategory || "").trim() || "(blank)";
    byCategory[k] = (byCategory[k] || 0) + 1;
  }
  return { byCategory, total: (terms || []).length };
}

/**
 * Core full extraction (no Express). Returns { httpStatus, body }.
 * @param {{ id: string, fullExtractionRunId: string, onProgress?: (e: object) => void }} opts
 */
async function runFddExtractFullPipeline({ id, fullExtractionRunId, onProgress }) {
  const feeExtractionRunId = `${fullExtractionRunId}__fee`;
  const termsExtractionRunId = `${fullExtractionRunId}__terms`;
  const storageMain = isFddAirtablePersistence() ? "airtable" : "memory";
  const emptyFees = {
    ok: false,
    extractionRunId: feeExtractionRunId,
    rowsCreated: 0,
    warnings: [],
    auditSummary: { byStatus: {}, total: 0 },
    duplicateSummary: { totalRows: 0, possibleDuplicateRows: 0, duplicateGroupKeyCount: 0 },
  };
  const emptyTerms = {
    ok: false,
    extractionRunId: termsExtractionRunId,
    rowsCreated: 0,
    warnings: [],
    categorySummary: { byCategory: {}, total: 0 },
    legalReviewCount: 0,
    commercialReviewCount: 0,
  };

  const touch = typeof onProgress === "function" ? onProgress : () => {};

  let doc = null;
  try {
    touch({ stage: "prepare", message: "Loading document and preparing extraction…" });
    const termsMod = await import("./fdd-intelligence-terms.js");
    const { runTermsAiExtraction, persistTermsRowsForDocument } = termsMod;

    if (isFddAirtablePersistence()) {
      doc = await atGetDocument(id, { includeFullText: true });
      if (!doc) {
        return {
          httpStatus: 404,
          body: {
            ok: false,
            partialSuccess: false,
            error: "Document not found",
            documentId: id,
            storage: storageMain,
            fullExtractionRunId,
            fullTextSource: null,
            fullTextLength: 0,
            sectionsCount: 0,
            fees: emptyFees,
            terms: emptyTerms,
            warnings: ["Document not found"],
          },
        };
      }
      doc.extractionStatus = "extracting";
      doc.updatedAt = nowIso();
      await atUpdateDocument(doc);
      await atDeleteSectionsAndRowsForDoc(id);
    } else {
      doc = documents.get(id);
      if (!doc) {
        return {
          httpStatus: 404,
          body: {
            ok: false,
            partialSuccess: false,
            error: "Document not found",
            documentId: id,
            storage: storageMain,
            fullExtractionRunId,
            fullTextSource: null,
            fullTextLength: 0,
            sectionsCount: 0,
            fees: emptyFees,
            terms: emptyTerms,
            warnings: ["Document not found"],
          },
        };
      }
      doc.extractionStatus = "extracting";
      doc.updatedAt = nowIso();
      for (const [sid, sec] of sections) {
        if (sec.fddDocumentId === id) sections.delete(sid);
      }
      for (const [rid, row] of rows) {
        if (row.fddDocumentId === id) rows.delete(rid);
      }
    }

    touch({ stage: "segmenting", message: "Segmenting disclosure into sections…" });
    const { fullTextForExtraction, extractionTextSource, sectionList, createdSections } = await fddResolveSegmentAndInsertSections(
      doc,
      id
    );
    touch({
      stage: "segmenting",
      message: `Segmented ${createdSections.length} section(s)`,
      sectionsCount: createdSections.length,
    });

    doc.fullTextForExtraction = fullTextForExtraction;

    let feesRes = { ...emptyFees, extractionRunId: feeExtractionRunId };
    let persistedFeeRows = [];
    try {
      touch({ stage: "fees", message: "Starting fee extraction…" });
      const { rows: newRows, extractionWarnings } = await extractFeeRowsForDocument(
        doc,
        createdSections,
        feeExtractionRunId,
        touch
      );
      if (isFddAirtablePersistence()) {
        persistedFeeRows = await atInsertFeeRows(newRows);
      } else {
        for (const r of newRows) mergeAuditResultIntoRow(r);
        for (const r of newRows) rows.set(r.id, r);
        persistedFeeRows = newRows;
      }
      feesRes = {
        ok: true,
        extractionRunId: feeExtractionRunId,
        rowsCreated: persistedFeeRows.length,
        warnings: extractionWarnings || [],
        auditSummary: summarizeFddFullFeeAudit(persistedFeeRows),
        duplicateSummary: summarizeFddFullFeeDuplicates(persistedFeeRows),
      };
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      feesRes = { ...emptyFees, extractionRunId: feeExtractionRunId, warnings: [msg], ok: false };
      console.error("[fdd-intelligence] extract-full fee leg:", e);
    }
    delete doc.fullTextForExtraction;

    let termsRes = { ...emptyTerms, extractionRunId: termsExtractionRunId };
    let persistedTerms = [];
    try {
      touch({ stage: "terms", message: "Starting terms extraction…" });
      const { terms, warnings } = await runTermsAiExtraction(doc, sectionList, termsExtractionRunId, touch);
      persistedTerms = await persistTermsRowsForDocument(id, terms);
      termsRes = {
        ok: true,
        extractionRunId: termsExtractionRunId,
        rowsCreated: persistedTerms.length,
        warnings: warnings || [],
        categorySummary: summarizeFddFullTermsCategories(persistedTerms),
        legalReviewCount: persistedTerms.filter((t) => t.legalReviewRequired).length,
        commercialReviewCount: persistedTerms.filter((t) => t.commercialReviewRequired !== false).length,
      };
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      termsRes = { ...emptyTerms, extractionRunId: termsExtractionRunId, warnings: [msg], ok: false };
      console.error("[fdd-intelligence] extract-full terms leg:", e);
    }

    appendSegmentationTocMismatchWarning(doc, sectionList, fullTextForExtraction);
    appendFullTextStoragePolicyNote(doc, extractionTextSource, fullTextForExtraction.length, isFddAirtablePersistence());

    doc.fullText = fullTextForExtraction;
    const anyOk = feesRes.ok || termsRes.ok;
    const allOk = feesRes.ok && termsRes.ok;
    doc.extractionStatus = anyOk ? "extracted" : "extract_failed";
    if (anyOk) doc.extractedAt = nowIso();
    doc.updatedAt = nowIso();
    appendExtractionNote(
      doc,
      `[Full extraction ${fullExtractionRunId}] feesOk=${feesRes.ok} termsOk=${termsRes.ok} feeRows=${feesRes.rowsCreated} termRows=${termsRes.rowsCreated} source=${extractionTextSource}`
    );
    console.log(
      `[fdd-intelligence] extract-full ${fullExtractionRunId} doc=${id} feesOk=${feesRes.ok} termsOk=${termsRes.ok} sections=${createdSections.length}`
    );

    touch({ stage: "finalize", message: "Saving document…" });
    if (isFddAirtablePersistence()) {
      await atUpdateDocument(doc);
    } else {
      documents.set(id, doc);
    }

    const partialSuccess = anyOk && !allOk;
    const topWarnings = [];
    if (partialSuccess) {
      topWarnings.push("Partial success: one extraction leg failed; inspect fees.ok and terms.ok.");
    }

    const httpStatus = anyOk ? 200 : 500;
    return {
      httpStatus,
      body: {
        ok: anyOk,
        partialSuccess,
        storage: storageMain,
        documentId: id,
        fullExtractionRunId,
        fullTextSource: extractionTextSource,
        fullTextLength: fullTextForExtraction.length,
        sectionsCount: createdSections.length,
        fees: feesRes,
        terms: termsRes,
        warnings: topWarnings,
      },
    };
  } catch (e) {
    console.error("[fdd-intelligence] extract-full fatal:", e);
    if (doc) {
      try {
        doc.extractionStatus = "extract_failed";
        doc.updatedAt = nowIso();
        if (isFddAirtablePersistence()) await atUpdateDocument(doc);
        else documents.set(id, doc);
      } catch (_) {
        /* ignore secondary write failure */
      }
    }
    return {
      httpStatus: 500,
      body: {
        ok: false,
        partialSuccess: false,
        error: e.message || "Full extraction failed",
        documentId: id,
        fullExtractionRunId,
        storage: storageMain,
        fullTextSource: null,
        fullTextLength: 0,
        sectionsCount: 0,
        fees: emptyFees,
        terms: emptyTerms,
        warnings: [e.message || "Full extraction failed"],
      },
    };
  }
}

async function runFddExtractFullBackground(jobId, id, fullExtractionRunId) {
  const touch = (evt) => emitFddExtractionJobProgress(jobId, evt);
  try {
    const out = await runFddExtractFullPipeline({ id, fullExtractionRunId, onProgress: touch });
    completeFddExtractionJob(jobId, { httpStatus: out.httpStatus, body: out.body });
  } catch (e) {
    console.error("[fdd-intelligence] extract-full-async background:", e);
    failFddExtractionJob(jobId, e);
  }
}

/** True when client requests background job + SSE (same URL as sync extract-full). */
function wantsAsyncFullExtract(req) {
  const q = req.query && req.query.async;
  if (q === "1" || String(q || "").toLowerCase() === "true") return true;
  const b = req.body;
  if (!b || typeof b !== "object") return false;
  return b.async === true || b.async === "true" || String(b.mode || "").toLowerCase() === "async";
}

/**
 * POST /api/fdd-intelligence/documents/:id/extract-full
 * One pass: load document → resolve full text → segment → persist sections → fee extraction → terms extraction.
 *
 * Async mode (same URL — works behind proxies that only allow the legacy path): JSON body `{ "async": true }`
 * or query `?async=1` delegates to {@link postFddExtractFullAsync} (202 + jobId). Plain POST remains synchronous (200/500).
 */
export async function postFddExtractFull(req, res) {
  if (wantsAsyncFullExtract(req)) {
    return postFddExtractFullAsync(req, res);
  }
  const fullExtractionRunId = newId("fddfull");
  try {
    const out = await runFddExtractFullPipeline({ id: req.params.id, fullExtractionRunId, onProgress: null });
    return res.status(out.httpStatus).json(out.body);
  } catch (e) {
    console.error("[fdd-intelligence] extract-full:", e);
    return res.status(500).json({ ok: false, partialSuccess: false, error: e.message || "Full extraction failed" });
  }
}

/**
 * POST /api/fdd-intelligence/documents/:id/extract-full-async
 * Starts full extraction in the background; poll GET …/extract-jobs/:jobId or subscribe to …/events (SSE).
 */
export async function postFddExtractFullAsync(req, res) {
  const { id } = req.params;
  let exists = false;
  if (isFddAirtablePersistence()) {
    const d = await atGetDocument(id, { includeFullText: false });
    exists = !!d;
  } else {
    exists = documents.has(id) || id === "mock_doc_sample";
  }
  if (!exists) {
    return res.status(404).json({ ok: false, accepted: false, error: "Document not found" });
  }
  const fullExtractionRunId = newId("fddfull");
  const jobId = createFddExtractionJob(id, fullExtractionRunId);
  setImmediate(() => {
    runFddExtractFullBackground(jobId, id, fullExtractionRunId);
  });
  const base = "/api/fdd-intelligence";
  return res.status(202).json({
    ok: true,
    accepted: true,
    jobId,
    documentId: id,
    fullExtractionRunId,
    pollUrl: `${base}/extract-jobs/${encodeURIComponent(jobId)}`,
    eventsUrl: `${base}/extract-jobs/${encodeURIComponent(jobId)}/events`,
  });
}

/** GET /api/fdd-intelligence/extract-jobs/:jobId */
export async function getFddExtractionJob(req, res) {
  const snap = getFddExtractionJobSnapshot(req.params.jobId);
  if (!snap) return res.status(404).json({ ok: false, error: "Job not found" });
  return res.json({ ok: true, job: snap });
}

/** GET /api/fdd-intelligence/extract-jobs/:jobId/events — Server-Sent Events progress stream. */
export function streamFddExtractionJobEvents(req, res) {
  attachFddExtractionJobSse(req.params.jobId, req, res);
}

/** Optional ?debug=1 on GET …/sections — adds sectionText length + head preview (no secrets). */
function decorateSectionsListForDebug(list) {
  return list.map((s) => ({
    ...s,
    debug: {
      sectionTextLength: (s.sectionText || "").length,
      sectionTextHead500: (s.sectionText || "").slice(0, 500),
      sourceFormat: s.sourceFormat,
      sourceSectionLabel: s.sourceSectionLabel,
      sourceSectionHeading: s.sourceSectionHeading,
      extractionTarget: s.extractionTarget,
      candidateSourceType: s.candidateSourceType,
    },
  }));
}

/** GET /api/fdd-intelligence/documents/:id/sections */
export async function listFddSections(req, res) {
  const { id } = req.params;
  const debug = String(req.query.debug || "") === "1";
  try {
    if (isFddAirtablePersistence()) {
      const doc = await atGetDocument(id, { includeFullText: false });
      if (!doc) return res.status(404).json({ success: false, error: "Document not found" });
      let list = await atListSectionsForDoc(id);
      list.sort((a, b) => (a.segmentOrder ?? 99999) - (b.segmentOrder ?? 99999) || String(a.itemNumber).localeCompare(String(b.itemNumber), undefined, { numeric: true }));
      if (debug) list = decorateSectionsListForDebug(list);
      return res.json({
        success: true,
        storage: "airtable",
        airtable: null,
        sections: list,
        ...(debug ? { sectionsDebug: true } : {}),
      });
    }
    let list = Array.from(sections.values())
      .filter((s) => s.fddDocumentId === id)
      .sort((a, b) => (a.segmentOrder ?? 99999) - (b.segmentOrder ?? 99999) || String(a.itemNumber).localeCompare(String(b.itemNumber), undefined, { numeric: true }));
    if (!documents.get(id) && id !== "mock_doc_sample") {
      return res.status(404).json({ success: false, error: "Document not found" });
    }
    if (debug) list = decorateSectionsListForDebug(list);
    return res.json({
      success: true,
      storage: "memory",
      airtable: airtablePersistenceNote(),
      sections: list,
      ...(debug ? { sectionsDebug: true } : {}),
    });
  } catch (e) {
    console.error("[fdd-intelligence] list sections:", e);
    res.status(500).json({ success: false, error: e.message || "List failed" });
  }
}

/** GET /api/fdd-intelligence/documents/:id/rows */
export async function listFddRows(req, res) {
  const { id } = req.params;
  try {
    if (isFddAirtablePersistence()) {
      const doc = await atGetDocument(id, { includeFullText: false });
      if (!doc) return res.status(404).json({ success: false, error: "Document not found" });
      const raw = await atListRowsForDoc(id);
      const { rows: list } = enrichFeeRowsListForApi(raw);
      return res.json({ success: true, storage: "airtable", airtable: null, rows: list });
    }
    const rawMem = Array.from(rows.values()).filter((r) => r.fddDocumentId === id);
    if (!documents.get(id) && id !== "mock_doc_sample") {
      return res.status(404).json({ success: false, error: "Document not found" });
    }
    const { rows: list } = enrichFeeRowsListForApi(rawMem);
    return res.json({ success: true, storage: "memory", airtable: airtablePersistenceNote(), rows: list });
  } catch (e) {
    console.error("[fdd-intelligence] list rows:", e);
    res.status(500).json({ success: false, error: e.message || "List failed" });
  }
}

/** PATCH body boolean coercion for review toggles. */
function parsePatchBoolean(val) {
  if (val === true || val === false) return val;
  const s = String(val).trim().toLowerCase();
  if (s === "true" || s === "1") return true;
  if (s === "false" || s === "0") return false;
  return undefined;
}

/** PATCH /api/fdd-intelligence/rows/:rowId */
export async function patchFddRow(req, res) {
  const { rowId } = req.params;
  try {
    let row;
    if (isFddAirtablePersistence()) {
      row = await atGetFeeRow(rowId);
    } else {
      row = rows.get(rowId);
    }
    if (!row) return res.status(404).json({ success: false, error: "Row not found" });

    const allowed = [
      "reviewStatus",
      "reviewerNotes",
      "feeOrObligationName",
      "feeType",
      "amount",
      "amountType",
      "basis",
      "frequency",
      "dueTiming",
      "requiredOptional",
      "lifecyclePhase",
      "appliesWhen",
      "conditionalTrigger",
      "responsibleParty",
      "passThroughStatus",
      "bundledStatus",
      "canBeWaived",
      "estimatedCostImpact",
      "implementationRisk",
      "intakeMapping",
      "matchScoreImpact",
      "documentationReference",
      "documentationReferencePageNumber",
      "sourceTextExcerpt",
      "extractionConfidence",
      "normalizedCostBasis",
      "rawCostBasisText",
      "amountFormulaType",
      "calculationUnit",
      "revenueBase",
      "unitRate",
      "percentageRate",
      "fixedAmount",
      "formulaNotes",
      "basisConfidence",
    ];

    for (const k of allowed) {
      if (!Object.prototype.hasOwnProperty.call(req.body || {}, k)) continue;
      if (k === "extractionConfidence") {
        const t = String(req.body[k]).trim();
        if (t) row[k] = t;
        continue;
      }
      row[k] = req.body[k];
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "needsLegalReview")) {
      const b = parsePatchBoolean(req.body.needsLegalReview);
      if (b !== undefined) row.needsLegalReview = b;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "needsCommercialReview")) {
      const b = parsePatchBoolean(req.body.needsCommercialReview);
      if (b !== undefined) row.needsCommercialReview = b;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "basisNeedsReview")) {
      const b = parsePatchBoolean(req.body.basisNeedsReview);
      if (b !== undefined) row.basisNeedsReview = b;
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "commercialCategory")) {
      const t = String(req.body.commercialCategory).trim();
      row.commercialCategory = COMMERCIAL_CATEGORY_VALUES.has(t) ? t : inferCommercialCategory(row);
    } else {
      row.commercialCategory = inferCommercialCategory(row);
    }

    row.updatedAt = nowIso();

    let updatedRow = row;
    if (isFddAirtablePersistence()) {
      await atUpdateFeeRow(row);
      await persistDuplicateFlagsForDocumentAfterPatch(row.fddDocumentId);
      updatedRow = await atGetFeeRow(rowId);
    } else {
      rows.set(rowId, row);
      await persistDuplicateFlagsForDocumentAfterPatch(row.fddDocumentId);
      updatedRow = rows.get(rowId);
    }

    if (isFddAirtablePersistence()) {
      const doc = await atGetDocument(row.fddDocumentId, { includeFullText: true });
      if (doc) {
        if (Object.prototype.hasOwnProperty.call(req.body || {}, "reviewer")) {
          doc.reviewer = req.body.reviewer;
        }
        doc.reviewedAt = nowIso();
        doc.updatedAt = nowIso();
        await atUpdateDocument(doc);
      }
    } else {
      if (Object.prototype.hasOwnProperty.call(req.body || {}, "reviewer")) {
        const d = documents.get(row.fddDocumentId);
        if (d) {
          d.reviewer = req.body.reviewer;
          d.updatedAt = nowIso();
          documents.set(d.id, d);
        }
      }
      const doc = documents.get(row.fddDocumentId);
      if (doc) {
        doc.reviewedAt = nowIso();
        doc.updatedAt = nowIso();
        documents.set(doc.id, doc);
      }
    }

    return res.json({
      success: true,
      storage: isFddAirtablePersistence() ? "airtable" : "memory",
      airtable: isFddAirtablePersistence() ? null : airtablePersistenceNote(),
      row: updatedRow,
    });
  } catch (e) {
    console.error("[fdd-intelligence] patch row:", e);
    res.status(500).json({ success: false, error: e.message || "Update failed" });
  }
}

function normBrand(s) {
  return String(s || "")
    .trim()
    .toLowerCase();
}

/** GET /api/fdd-intelligence/brands/:brandName/economics — default Approved only; ?includeNeedsReview=1 adds Needs Review; ?fddYear= and ?country= filter scope (country from FDD / fee row metadata). */
export async function getFddBrandEconomics(req, res) {
  try {
    const brandName = decodeURIComponent(req.params.brandName || "");
    const nb = normBrand(brandName);
    const includeNeedsReview =
      String(req.query.includeNeedsReview || "").trim() === "1" ||
      String(req.query.includeNeedsReview || "").trim().toLowerCase() === "true";
    const yearQ = req.query.fddYear;
    const fddYearFilter =
      yearQ != null && String(yearQ).trim() !== "" && Number.isFinite(parseInt(String(yearQ), 10))
        ? parseInt(String(yearQ), 10)
        : null;
    const countryRaw = req.query.country;
    const countryFilter =
      countryRaw != null && String(countryRaw).trim() !== "" ? String(countryRaw).trim() : null;

    let scopeRows;
    if (isFddAirtablePersistence()) {
      scopeRows = await atListApprovedOrNeedsReviewRowsForBrand(brandName);
    } else {
      scopeRows = Array.from(rows.values()).filter(
        (r) => normBrand(r.brandName) === nb && isEconomicsScopeReviewStatus(r.reviewStatus)
      );
    }
    if (fddYearFilter != null) {
      scopeRows = scopeRows.filter((r) => Number(r.fddYear) === fddYearFilter);
    }
    if (countryFilter != null) {
      const want = String(countryFilter).trim().toLowerCase();
      scopeRows = scopeRows.filter((r) => String(r.country || "").trim().toLowerCase() === want);
    }
    scopeRows = enrichApprovedRowsWithDuplicateFlags(scopeRows);

    const approvedCount = scopeRows.filter((r) => r.reviewStatus === "Approved").length;
    const needsReviewCount = scopeRows.filter((r) => r.reviewStatus === "Needs Review").length;
    const possibleDuplicateCount = scopeRows.filter((r) => r.possibleDuplicate === true).length;
    const legalReviewCount = scopeRows.filter((r) => r.needsLegalReview === true).length;
    const commercialReviewCount = scopeRows.filter((r) => r.needsCommercialReview === true).length;

    const economics = includeNeedsReview
      ? scopeRows
      : scopeRows.filter((r) => r.reviewStatus === "Approved");

    return res.json({
      success: true,
      storage: isFddAirtablePersistence() ? "airtable" : "memory",
      brandName,
      includeNeedsReview,
      fddYear: fddYearFilter,
      country: countryFilter,
      summary: {
        approvedCount,
        needsReviewCount,
        possibleDuplicateCount,
        legalReviewCount,
        commercialReviewCount,
        scopeRowCount: scopeRows.length,
        economicsRowCount: economics.length,
      },
      economics,
      disclaimer:
        "Dealality FDD Intelligence is a commercial decision-support tool. It does not provide legal advice. Owners should review all franchise documents with qualified legal and financial advisors before making commitments.",
    });
  } catch (e) {
    console.error("[fdd-intelligence] brand economics:", e);
    res.status(500).json({ success: false, error: e.message || "Read failed" });
  }
}

function summarizeRows(rowList) {
  const costStack = {};
  const lifecycle = {};
  const riskFlags = [];
  for (const r of rowList) {
    const ft = r.feeType || "Other";
    costStack[ft] = (costStack[ft] || 0) + 1;
    const lc = r.lifecyclePhase || "Unknown";
    lifecycle[lc] = (lifecycle[lc] || 0) + 1;
    if (String(r.implementationRisk || "").toLowerCase() === "high") {
      riskFlags.push({ rowId: r.id, feeOrObligationName: r.feeOrObligationName, flag: "implementation_risk_high" });
    }
    if (String(r.passThroughStatus || "").toLowerCase().includes("pass")) {
      riskFlags.push({ rowId: r.id, feeOrObligationName: r.feeOrObligationName, flag: "pass_through_cost" });
    }
  }
  return { costStackSummary: costStack, lifecycleCostSummary: lifecycle, riskFlags };
}

/** GET /api/fdd-intelligence/compare?brands=A,B,C */
export async function getFddCompare(req, res) {
  try {
    const raw = req.query.brands || req.query.brand || "";
    const brandNames = String(raw)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (!brandNames.length) {
      return res.status(400).json({ success: false, error: "Query `brands` required (comma-separated)." });
    }

    let approvedPool = null;
    if (isFddAirtablePersistence()) {
      approvedPool = await atListApprovedRowsByBrandNames(brandNames);
    }

    const perBrand = brandNames.map((name) => {
      const nb = normBrand(name);
      const economics = enrichApprovedRowsWithDuplicateFlags(
        isFddAirtablePersistence()
          ? (approvedPool || []).filter((r) => normBrand(r.brandName) === nb)
          : Array.from(rows.values()).filter(
              (r) => normBrand(r.brandName) === nb && normalizeFddReviewStatusFromAirtable(r.reviewStatus) === "Approved"
            )
      );
      return {
        brandName: name,
        economics,
        ...summarizeRows(economics),
      };
    });

    return res.json({
      success: true,
      storage: isFddAirtablePersistence() ? "airtable" : "memory",
      brands: perBrand,
      disclaimer:
        "Dealality FDD Intelligence is a commercial decision-support tool. It does not provide legal advice. Owners should review all franchise documents with qualified legal and financial advisors before making commitments.",
    });
  } catch (e) {
    console.error("[fdd-intelligence] compare:", e);
    res.status(500).json({ success: false, error: e.message || "Compare failed" });
  }
}

function countAuditStatusSummary(list) {
  const c = {
    highConfidenceCount: 0,
    quickReviewCount: 0,
    needsReviewCount: 0,
    manualReviewRequiredCount: 0,
    autoApproveEligibleCount: 0,
  };
  for (const r of list) {
    const s = String(r.auditStatus || "");
    if (s === "High Confidence") c.highConfidenceCount++;
    else if (s === "Quick Review") c.quickReviewCount++;
    else if (s === "Needs Review") c.needsReviewCount++;
    else if (s === "Manual Review Required" || s === "Do Not Auto-Approve") c.manualReviewRequiredCount++;
    if (r.autoApproveEligible === true) c.autoApproveEligibleCount++;
  }
  return c;
}

/** POST /api/fdd-intelligence/documents/:id/audit */
export async function postFddDocumentAudit(req, res) {
  const { id } = req.params;
  try {
    if (isFddAirtablePersistence()) {
      const doc = await atGetDocument(id, { includeFullText: false });
      if (!doc) return res.status(404).json({ ok: false, error: "Document not found" });
      const raw = await atListRowsForDoc(id);
      const { rows: list } = enrichFeeRowsListForApi(raw);
      for (const r of list) await atUpdateFeeRow(r);
      return res.json({
        ok: true,
        storage: "airtable",
        documentId: id,
        auditedCount: list.length,
        ...countAuditStatusSummary(list),
      });
    }
    const doc = documents.get(id);
    if (!doc && id !== "mock_doc_sample") return res.status(404).json({ ok: false, error: "Document not found" });
    const rawMem = Array.from(rows.values()).filter((r) => r.fddDocumentId === id);
    const { rows: list } = enrichFeeRowsListForApi(rawMem);
    for (const r of list) rows.set(r.id, r);
    return res.json({
      ok: true,
      storage: "memory",
      documentId: id,
      auditedCount: list.length,
      ...countAuditStatusSummary(list),
    });
  } catch (e) {
    console.error("[fdd-intelligence] document audit:", e);
    res.status(500).json({ ok: false, error: e.message || "Audit failed" });
  }
}

/** POST /api/fdd-intelligence/documents/:id/approve-auto-eligible */
export async function postFddDocumentApproveAutoEligible(req, res) {
  const { id } = req.params;
  const reviewer = String(req.body?.reviewer || "").trim() || "FDD Audit Auto-Approval";
  const skippedReasonsSummary = {};
  try {
    const bumpSkip = (reason) => {
      skippedReasonsSummary[reason] = (skippedReasonsSummary[reason] || 0) + 1;
    };

    if (isFddAirtablePersistence()) {
      const doc = await atGetDocument(id, { includeFullText: false });
      if (!doc) return res.status(404).json({ ok: false, error: "Document not found" });
      const raw = await atListRowsForDoc(id);
      const { rows: list } = enrichFeeRowsListForApi(raw);
      const approvedRowIds = [];
      for (const r of list) {
        const audit = auditFddFeeRow(r);
        Object.assign(r, audit);
        const ok = isAutoApproveEligible(r, audit);
        if (ok) {
          r.reviewStatus = "Approved";
          approvedRowIds.push(r.id);
        } else {
          const rs = normalizeFddReviewStatusFromAirtable(r.reviewStatus);
          if (rs !== "Needs Review") bumpSkip("not_needs_review");
          else if (Number(audit.auditScore) < 90) bumpSkip("audit_score_below_90");
          else if (r.possibleDuplicate === true) bumpSkip("possible_duplicate");
          else if (r.needsLegalReview === true) bumpSkip("legal_review_required");
          else if (r.basisNeedsReview === true) bumpSkip("basis_needs_review");
          else if (r.needsCommercialReview === true) bumpSkip("commercial_review_required");
          else bumpSkip("failed_eligibility_rules");
        }
        r.updatedAt = nowIso();
        await atUpdateFeeRow(r);
      }
      doc.reviewer = reviewer;
      doc.reviewedAt = nowIso();
      doc.updatedAt = nowIso();
      await atUpdateDocument(doc);
      const skippedCount = list.length - approvedRowIds.length;
      return res.json({
        ok: true,
        approvedCount: approvedRowIds.length,
        skippedCount,
        approvedRowIds,
        skippedReasonsSummary,
      });
    }

    const doc = documents.get(id);
    if (!doc && id !== "mock_doc_sample") return res.status(404).json({ ok: false, error: "Document not found" });
    const rawMem = Array.from(rows.values()).filter((r) => r.fddDocumentId === id);
    const { rows: list } = enrichFeeRowsListForApi(rawMem);
    const approvedRowIds = [];
    for (const r of list) {
      const audit = auditFddFeeRow(r);
      Object.assign(r, audit);
      const ok = isAutoApproveEligible(r, audit);
      if (ok) {
        r.reviewStatus = "Approved";
        approvedRowIds.push(r.id);
      } else {
        const rs = normalizeFddReviewStatusFromAirtable(r.reviewStatus);
        if (rs !== "Needs Review") bumpSkip("not_needs_review");
        else if (Number(audit.auditScore) < 90) bumpSkip("audit_score_below_90");
        else if (r.possibleDuplicate === true) bumpSkip("possible_duplicate");
        else if (r.needsLegalReview === true) bumpSkip("legal_review_required");
        else if (r.basisNeedsReview === true) bumpSkip("basis_needs_review");
        else if (r.needsCommercialReview === true) bumpSkip("commercial_review_required");
        else bumpSkip("failed_eligibility_rules");
      }
      r.updatedAt = nowIso();
      rows.set(r.id, r);
    }
    if (doc) {
      doc.reviewer = reviewer;
      doc.reviewedAt = nowIso();
      doc.updatedAt = nowIso();
      documents.set(doc.id, doc);
    }
    const skippedCount = list.length - approvedRowIds.length;
    return res.json({
      ok: true,
      approvedCount: approvedRowIds.length,
      skippedCount,
      approvedRowIds,
      skippedReasonsSummary,
    });
  } catch (e) {
    console.error("[fdd-intelligence] approve auto-eligible:", e);
    res.status(500).json({ ok: false, error: e.message || "Approve failed" });
  }
}

/**
 * POST /api/fdd-intelligence/documents/:id/bulk-update-rows
 * Body: { rowIds: string[], action: string, reviewer?: string }
 */
export async function postFddBulkUpdateRows(req, res) {
  const { id: docId } = req.params;
  const rowIdsRaw = req.body && Array.isArray(req.body.rowIds) ? req.body.rowIds : [];
  const action = String(req.body?.action || "").trim();
  const reviewer = String(req.body?.reviewer || "").trim() || "Bulk update";

  const allowedActions = new Set([
    "clear_commercial_review",
    "mark_commercial_review_needed",
    "approve",
    "reject",
    "needs_review",
  ]);
  if (!allowedActions.has(action)) {
    return res.status(400).json({ ok: false, error: "Invalid or missing action" });
  }

  const rowIds = [...new Set(rowIdsRaw.map((x) => String(x || "").trim()).filter(Boolean))];
  const requestedCount = rowIds.length;
  if (!requestedCount) {
    return res.status(400).json({ ok: false, error: "rowIds required" });
  }

  const skipped = [];
  /** @type {Record<string, number>} */
  const summaryByReason = {};
  const bumpReason = (reason) => {
    summaryByReason[reason] = (summaryByReason[reason] || 0) + 1;
  };

  try {
    let raw;
    if (isFddAirtablePersistence()) {
      const doc = await atGetDocument(docId, { includeFullText: false });
      if (!doc) return res.status(404).json({ ok: false, error: "Document not found" });
      raw = await atListRowsForDoc(docId);
    } else {
      const doc = documents.get(docId);
      if (!doc && docId !== "mock_doc_sample") return res.status(404).json({ ok: false, error: "Document not found" });
      raw = Array.from(rows.values()).filter((r) => r.fddDocumentId === docId);
    }

    const prevSnap = new Map(raw.map((r) => [r.id, { ...r }]));
    const working = raw.map((r) => ({ ...r }));
    const byId = new Map(working.map((r) => [r.id, r]));

    /** @type {string[]} */
    const updatedRowIds = [];

    for (const rid of rowIds) {
      const r = byId.get(rid);
      if (!r) {
        skipped.push({ rowId: rid, reason: "row_not_found" });
        bumpReason("row_not_found");
        continue;
      }
      if (String(r.fddDocumentId || "") !== String(docId)) {
        skipped.push({ rowId: rid, reason: "wrong_document" });
        bumpReason("wrong_document");
        continue;
      }

      if (action === "clear_commercial_review") {
        if (r.needsCommercialReview !== true) {
          skipped.push({ rowId: rid, reason: "already_commercial_cleared" });
          bumpReason("already_commercial_cleared");
          continue;
        }
        r.needsCommercialReview = false;
        updatedRowIds.push(rid);
        continue;
      }

      if (action === "mark_commercial_review_needed") {
        if (r.needsCommercialReview === true) {
          skipped.push({ rowId: rid, reason: "already_needs_commercial" });
          bumpReason("already_needs_commercial");
          continue;
        }
        r.needsCommercialReview = true;
        updatedRowIds.push(rid);
        continue;
      }

      if (action === "approve") {
        mergeAuditResultIntoRow(r);
        const skipReason = getBulkApproveSkipReason(r);
        if (skipReason) {
          skipped.push({ rowId: rid, reason: skipReason });
          bumpReason(skipReason);
          continue;
        }
        r.reviewStatus = "Approved";
        r.updatedAt = nowIso();
        updatedRowIds.push(rid);
        continue;
      }

      if (action === "reject") {
        r.reviewStatus = "Rejected";
        r.updatedAt = nowIso();
        updatedRowIds.push(rid);
        continue;
      }

      if (action === "needs_review") {
        r.reviewStatus = "Needs Review";
        r.updatedAt = nowIso();
        updatedRowIds.push(rid);
        continue;
      }
    }

    const { rows: enriched } = enrichFeeRowsListForApi(working);

    for (const r of enriched) {
      const prev = prevSnap.get(r.id);
      if (!feeRowPersistDirty(prev, r)) continue;
      if (isFddAirtablePersistence()) {
        await atUpdateFeeRow(r);
      } else {
        rows.set(r.id, r);
      }
    }

    if (updatedRowIds.length && isFddAirtablePersistence()) {
      const doc = await atGetDocument(docId, { includeFullText: false });
      if (doc) {
        doc.reviewer = reviewer;
        doc.reviewedAt = nowIso();
        doc.updatedAt = nowIso();
        await atUpdateDocument(doc);
      }
    } else if (updatedRowIds.length) {
      const d = documents.get(docId);
      if (d) {
        d.reviewer = reviewer;
        d.reviewedAt = nowIso();
        d.updatedAt = nowIso();
        documents.set(docId, d);
      }
    }

    const updatedCount = updatedRowIds.length;
    const skippedCount = skipped.length;

    return res.json({
      ok: true,
      action,
      requestedCount,
      updatedCount,
      skippedCount,
      updatedRowIds,
      skipped,
      summaryByReason,
    });
  } catch (e) {
    console.error("[fdd-intelligence] bulk-update-rows:", e);
    res.status(500).json({ ok: false, error: e.message || "Bulk update failed" });
  }
}

/** GET /api/fdd-intelligence/documents/:id/file — optional download of uploaded PDF */
export async function getFddDocumentFile(req, res) {
  const { id } = req.params;
  try {
    let doc;
    if (isFddAirtablePersistence()) {
      doc = await atGetDocument(id, { includeFullText: false });
    } else {
      doc = documents.get(id);
    }
    if (!doc || !doc.filePath) return res.status(404).json({ success: false, error: "File not available" });
    const resolved = path.resolve(doc.filePath);
    const baseResolved = path.resolve(FDD_UPLOAD_DIR);
    if (!resolved.startsWith(baseResolved)) return res.status(403).json({ success: false, error: "Invalid path" });
    if (!fs.existsSync(resolved) || !fs.statSync(resolved).isFile()) {
      return res.status(404).json({ success: false, error: "Missing file on disk" });
    }
    res.sendFile(resolved);
  } catch (e) {
    console.error("[fdd-intelligence] file serve:", e);
    res.status(500).json({ success: false, error: e.message || "File error" });
  }
}
