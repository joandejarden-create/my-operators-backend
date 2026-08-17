/**
 * External owner-facing copy governance for Brand Explorer presentation rows.
 * Strips internal source citations / governance language while preserving traceability in reports.
 */
import { DESIGN_HOTELS_SOURCE_IDS } from "./brand-explorer-design-hotels-content-packages-v35F.js";

export const EXTERNAL_OWNER_AUDIT_PATTERNS = Object.freeze([
  { id: "sources_block", re: /\n\nSources:\s/i, severity: "critical" },
  { id: "source_line", re: /^\s*Sources?:\s/im, severity: "critical" },
  { id: "http_url", re: /https?:\/\//i, severity: "high", exceptSlots: ["footprint.openings", "footprint.momentum"] },
  { id: "source_library", re: /\bsource library\b/i, severity: "high" },
  { id: "approved_source", re: /\bapproved source\b/i, severity: "high" },
  { id: "evidence_note", re: /\bevidence\b/i, severity: "medium" },
  { id: "staging_run", re: /\bstaging run\b/i, severity: "high" },
  { id: "governance_impl", re: /\bgovernance implementation\b/i, severity: "high" },
  { id: "internal_ref", re: /\binternal\b/i, severity: "medium" },
  { id: "confirm_fdd", re: /\bconfirm in (the )?fdd\b/i, severity: "high" },
  { id: "loi", re: /\bloi\b/i, severity: "high" },
  { id: "franchise_disclosure", re: /\bfranchise disclosure\b/i, severity: "high" },
  { id: "item_19", re: /\bitem\s*19\b/i, severity: "high" },
  { id: "fee_schedule", re: /\bfee schedule\b/i, severity: "medium" },
  { id: "disclosure_document", re: /\bdisclosure document\b/i, severity: "high" },
  { id: "public_materials_no_publish", re: /\bpublic materials do not publish\b/i, severity: "medium" },
  { id: "not_recommendation", re: /\bnot a recommendation\b/i, severity: "medium" },
  { id: "compliance_determination", re: /\bcompliance determination\b/i, severity: "high" },
  { id: "confidential_economics", re: /\bconfidential economics\b/i, severity: "medium" },
  { id: "brand_verified", re: /\bbrand-verified content\b/i, severity: "high" },
  { id: "performance_rep", re: /\bperformance representation\b/i, severity: "high" },
  { id: "net_contribution", re: /\bnet contribution\b/i, severity: "high" },
  { id: "fee_stack", re: /\bfee stack\b/i, severity: "high" },
  { id: "franchise_license", re: /\bfranchise license fee\b/i, severity: "high" },
  { id: "qa_ref", re: /\bqa\b/i, severity: "medium" },
]);

export const AFFILIATION_SANITIZE_REPLACEMENTS = Object.freeze([
  { re: /\bnot a published franchise disclosure checklist\b/gi, replace: "not a published owner participation checklist in public materials" },
  { re: /\bnot a franchise disclosure schedule\b/gi, replace: "not a published commercial terms schedule" },
  { re: /\bfranchise disclosure document\b/gi, replace: "commercial agreement materials" },
  { re: /\bfranchise disclosure\b/gi, replace: "commercial agreement review" },
  { re: /\bperformance representation\b/gi, replace: "operating performance detail" },
  { re: /\bpublic materials do not publish a full fee schedule or performance representation\b/gi, replace: "Public materials do not publish a full participation cost schedule or property-level operating detail" },
  { re: /\bNo Dealality performance, ADR, RevPAR, or net-contribution claims on this page\.?\s*/gi, replace: "" },
  { re: /\bDealality does not publish ADR, RevPAR, rooms-from-loyalty, or repeat-demand guarantees[^.]*\.?\s*/gi, replace: "Dealality does not publish property-level loyalty performance guarantees on this page. " },
  { re: /\bconfirm every line in your disclosure document and LOI\b/gi, replace: "confirm participation terms directly with brand representatives" },
  { re: /\bConfirm categories, basis, and timing in your FDD and LOI\.?\b/gi, replace: "Confirm participation categories and timing directly with brand representatives." },
  { re: /\bInitial \/ franchise license fee\b/gi, replace: "Initial affiliation or membership entry costs" },
  { re: /\bfee stack\b/gi, replace: "participation cost categories" },
  { re: /\bnet contribution\b/gi, replace: "owner economics" },
  { re: /\bAI-Assisted from Official Public Sources · Curated by Dealality\. Confirm membership details directly with Design Hotels—not a company validation claim\.?\s*/gi, replace: "Curated by Dealality from official public brand materials." },
  { re: /\bwithout franchise prototype language\b/gi, replace: "without rigid chain-standardization language" },
  { re: /\bnot a pipeline disclosure\b/gi, replace: "not a development pipeline disclosure" },
  { re: /\bnot a conventional franchise flag conversion\b/gi, replace: "not a conventional chain-flag conversion" },
  { re: /\bwithout a rigid franchise prototype\b/gi, replace: "without a rigid chain prototype" },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function extractSourceFootnote(body, sourcesById = new Map()) {
  const blob = nz(body);
  const match = blob.match(/\n\nSources:\s*([\s\S]*)$/i);
  if (!match) return { mainBody: blob, sourceFootnote: "", sourceIds: [] };
  const sourceFootnote = match[1].trim();
  const mainBody = blob.slice(0, match.index).trim();
  const sourceIds = [];
  for (const [id, meta] of sourcesById.entries()) {
    const url = nz(meta?.sourceUrl);
    const title = nz(meta?.sourceTitle);
    if (sourceFootnote.includes(id) || (url && sourceFootnote.includes(url)) || (title && sourceFootnote.includes(title))) {
      sourceIds.push(id);
    }
  }
  for (const id of Object.values(DESIGN_HOTELS_SOURCE_IDS)) {
    if (sourceFootnote.includes(id) && !sourceIds.includes(id)) sourceIds.push(id);
  }
  return { mainBody, sourceFootnote, sourceIds };
}

export function stripInlineUrls(text, { allowTrailingPropertyUrl = false } = {}) {
  let s = nz(text);
  if (!s) return s;
  if (allowTrailingPropertyUrl) {
    const lines = s.split("\n");
    const last = lines[lines.length - 1]?.trim() || "";
    if (/^https?:\/\//i.test(last)) {
      return lines.slice(0, -1).join("\n").replace(/https?:\/\/\S+/gi, "").trim();
    }
  }
  return s.replace(/https?:\/\/\S+/gi, "").trim();
}

export function sanitizeAffiliationExternalCopy(text, { slotKey = "", sourcesById = new Map() } = {}) {
  let s = extractSourceFootnote(text, sourcesById).mainBody;
  s = stripInlineUrls(s, {
    allowTrailingPropertyUrl: slotKey === "footprint.openings" || slotKey === "footprint.momentum",
  });
  for (const rule of AFFILIATION_SANITIZE_REPLACEMENTS) {
    s = s.replace(rule.re, rule.replace);
  }
  // openings/momentum bodies use blank-line structure: date / summary / trailing URL
  if (slotKey === "footprint.openings" || slotKey === "footprint.momentum") {
    return s
      .split("\n")
      .map((line) => line.replace(/[ \t]{2,}/g, " ").trimEnd())
      .join("\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim();
  }
  return s
    .split("\n")
    .map((line) => line.replace(/[ \t]{2,}/g, " ").trim())
    .filter(Boolean)
    .join("\n")
    .trim();
}

export function auditExternalOwnerPhrase(text, slotKey = "") {
  const blob = `${text}`;
  const hits = [];
  for (const pat of EXTERNAL_OWNER_AUDIT_PATTERNS) {
    if (pat.exceptSlots?.includes(slotKey)) continue;
    if (pat.re.test(blob)) {
      const m = blob.match(pat.re);
      hits.push({
        patternId: pat.id,
        severity: pat.severity,
        phrase: m?.[0] || pat.id,
      });
    }
  }
  return hits;
}

export function auditPresentationRowExternalOwner(row, sourcesById = new Map()) {
  const slotKey = nz(row.slotKey);
  const combined = [row.title, row.body, row.caseSummaryOverview, row.caseSummaryOwnerObjective, row.caseSummaryBrandRelevance, row.caseSummaryInterpretation, row.caseSummaryTags]
    .filter(Boolean)
    .join("\n");
  const trace = extractSourceFootnote(row.body || "", sourcesById);
  const hits = auditExternalOwnerPhrase(combined, slotKey);
  return {
    recordId: row.recordId,
    slotKey,
    title: row.title,
    bodyExcerpt: nz(row.body).slice(0, 160),
    hits,
    hasSourceSupport: trace.sourceIds.length > 0 || trace.sourceFootnote.length > 0,
    internalSourceTrace: {
      sourceFootnote: trace.sourceFootnote,
      sourceIds: trace.sourceIds,
    },
  };
}

export function rowNeedsExternalOwnerPatch(row) {
  const audit = auditPresentationRowExternalOwner(row);
  if (audit.hits.length) return true;
  const sanitized = sanitizeAffiliationExternalCopy(row.body || "", { slotKey: row.slotKey });
  if (sanitized !== nz(row.body)) return true;
  if (row.slotKey === "footprint.openings") {
    const fields = ["caseSummaryOverview", "caseSummaryBrandRelevance", "caseSummaryOwnerObjective", "caseSummaryInterpretation", "caseSummaryTags"];
    return fields.some((f) => !nz(row[f]));
  }
  return false;
}
