/**
 * v40C — Residual owner-copy scrub for Presentation leftovers after v40.
 * Avoids mechanical phrases that still fail internal-preview owner-copy gates.
 */
import {
  auditExternalOwnerPhrase,
  extractSourceFootnote,
  sanitizeAffiliationExternalCopy,
  stripInlineUrls,
} from "./brand-explorer-external-owner-content-governance.js";
import { BRAND_MODEL_SCRUB_PROFILES } from "./brand-explorer-owner-copy-scrubber.js";
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
} from "./brand-explorer-v40b-copy-quality-patterns.js";

export const V40C_RESIDUAL_SCRUBBER_VERSION = "v40c";

export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

export const MAP_PRESENTATION_FIELDS = Object.freeze({
  title: "Title",
  body: "Body",
  caseSummaryOverview: "Case Summary Overview",
  caseSummaryBrandRelevance: "Case Summary Brand Relevance",
  caseSummaryOwnerObjective: "Case Summary Owner Objective",
  caseSummaryInterpretation: "Case Summary Interpretation",
  caseSummaryTags: "Case Summary Tags",
});

/** Ordered: longer / more specific first. */
export const V40C_RESIDUAL_REPLACEMENTS = Object.freeze([
  {
    re: /\bEvaluate whether the brand’s distribution, operating requirements, and owner obligations fit the asset’s demand mix and investment plan\.?/gi,
    replace:
      "Evaluate whether the brand’s distribution, operating requirements, and owner obligations fit the asset’s demand mix and investment plan.",
  },
  {
    re: /\bowner economics after brand-related costs\b/gi,
    replace: "whether brand economics fit the asset after program costs",
  },
  {
    re: /\bparticipation cost categories\b/gi,
    replace: "participation costs and program fees",
  },
  {
    re: /\bletter of intent or commercial proposal\b/gi,
    replace: "commercial proposal",
  },
  {
    re: /\bfranchise disclosure materials\b/gi,
    replace: "commercial agreement materials",
  },
  {
    re: /\bpublic franchise performance disclosures \(where applicable\)\b/gi,
    replace: "public performance materials (where applicable)",
  },
  {
    re: /\bConfirm in Item 7 and your LOI\.?/gi,
    replace:
      "Confirm participation costs and timing directly during brand engagement and legal review.",
  },
  {
    re: /\bConfirm every line in your disclosure document and LOI\.?/gi,
    replace:
      "Confirm participation costs, operating obligations, and agreement terms directly during brand engagement and legal review.",
  },
  {
    re: /\bconfirm every line in your disclosure document and LOI\b/gi,
    replace:
      "confirm participation costs, operating obligations, and agreement terms directly during brand engagement and legal review",
  },
  { re: /\bfee stack\b/gi, replace: "participation costs and program fees" },
  { re: /\bnet contribution\b/gi, replace: "contribution after program costs" },
  {
    re: /\bfranchise disclosure document\b/gi,
    replace: "commercial agreement materials",
  },
  { re: /\bfranchise disclosure\b/gi, replace: "commercial agreement review" },
  { re: /\bdisclosure document\b/gi, replace: "commercial agreement materials" },
  {
    re: /\bperformance representation\b/gi,
    replace: "operating performance detail",
  },
  {
    re: /\bItem\s*19\s*\/\s*FDD\.?/gi,
    replace:
      "Public materials do not provide property-level performance economics. Treat economics as a diligence item, not a forecast.",
  },
  { re: /\bItem\s*19\b/gi, replace: "public performance materials" },
  { re: /\bItem\s*7\b/gi, replace: "initial investment schedules" },
  { re: /\bFDD\b/g, replace: "commercial agreement materials" },
  { re: /\bLOI\b/g, replace: "commercial proposal" },
  { re: /\bADR\b/g, replace: "average daily rate" },
  { re: /\bRevPAR\b/g, replace: "revenue per available room" },
  { re: /\bSources?:\s*/gi, replace: "" },
  { re: /\bSource:\s*/gi, replace: "" },
  { re: /\bOutput Note\.?/gi, replace: "" },
  { re: /\binternal review\b/gi, replace: "owner diligence review" },
  {
    re: /\bsupports internal review\b/gi,
    replace: "supports owner diligence",
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function collapseWhitespace(s) {
  return nz(s)
    .split("\n")
    .map((line) => line.replace(/[ \t]{2,}/g, " ").trim())
    .filter(Boolean)
    .join("\n")
    .trim();
}

function collapseWhitespacePreserveParagraphs(s) {
  return nz(s)
    .split(/\n/)
    .map((line) => line.replace(/[ \t]{2,}/g, " ").trimEnd())
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/** Recent Momentum / Openings contract: trailing announcement URLs are required. */
function isOpeningsOrMomentumSlot(slotKey) {
  const k = nz(slotKey);
  return k === "footprint.openings" || k === "footprint.momentum";
}

function peelTrailingHttpUrl(text) {
  const lines = nz(text).split("\n");
  if (!lines.length) return { body: "", trailingUrl: "" };
  const last = (lines[lines.length - 1] || "").trim();
  if (/^https?:\/\//i.test(last)) {
    return { body: lines.slice(0, -1).join("\n").trim(), trailingUrl: last };
  }
  return { body: nz(text), trailingUrl: "" };
}

export function scrubResidualOwnerFacingCopy(text, { slotKey = "", brandSlug = "" } = {}) {
  const before = nz(text);
  const trace = extractSourceFootnote(before);
  let after = trace.mainBody || before;
  const preserveAnnouncementUrl = isOpeningsOrMomentumSlot(slotKey);

  after = sanitizeAffiliationExternalCopy(after, { slotKey });
  after = stripInlineUrls(after, {
    allowTrailingPropertyUrl: preserveAnnouncementUrl,
  });

  for (const rule of V40C_RESIDUAL_REPLACEMENTS) {
    after = after.replace(rule.re, rule.replace);
  }

  // Hard strip residual tokens that still fail internal-preview gates.
  // Preserve trailing announcement URLs on openings/momentum (PVQL + Recent Momentum contract).
  let trailingAnnouncementUrl = "";
  if (preserveAnnouncementUrl) {
    const peeled = peelTrailingHttpUrl(after);
    after = peeled.body;
    trailingAnnouncementUrl = peeled.trailingUrl;
  }
  after = after
    .replace(/\bLOI\b/g, "commercial proposal")
    .replace(/\bFDD\b/g, "commercial agreement materials")
    .replace(/\bItem\s*19\b/gi, "public performance materials")
    .replace(/\bItem\s*7\b/gi, "initial investment schedules")
    .replace(/https?:\/\/\S+/gi, "")
    .replace(/\bfranchise disclosure\b/gi, "commercial agreement review")
    .replace(/\bdisclosure document\b/gi, "commercial agreement materials")
    .replace(/\bfee stack\b/gi, "participation costs and program fees")
    .replace(/\bnet contribution\b/gi, "contribution after program costs")
    .replace(/\bparticipation cost categories\b/gi, "participation costs and program fees")
    .replace(/\bowner economics after brand-related costs\b/gi, "whether brand economics fit the asset after program costs");
  if (trailingAnnouncementUrl) {
    after = `${nz(after)}\n\n${trailingAnnouncementUrl}`;
  }

  after = preserveAnnouncementUrl
    ? collapseWhitespacePreserveParagraphs(after)
    : collapseWhitespace(after);

  if (!after && before) {
    const profile = BRAND_MODEL_SCRUB_PROFILES[brandSlug];
    after = profile
      ? `Confirm ${profile.focus} directly during brand engagement. Public materials on this page are orientation only—not commercial terms or a forecast.`
      : "Confirm participation costs, operating obligations, and agreement terms directly during brand engagement and legal review.";
  }

  const forbiddenAfter = scanForbiddenLanguage(after).filter((h) => {
    if (preserveAnnouncementUrl && h.id === "raw_url") return false;
    return true;
  });
  const mechanicalAfter = scanMechanicalCopy(after).filter((h) =>
    ["high", "medium"].includes(h.severity)
  );
  const governanceHits = auditExternalOwnerPhrase(after, slotKey).filter((h) =>
    ["critical", "high"].includes(h.severity)
  );

  const clean =
    forbiddenAfter.length === 0 &&
    mechanicalAfter.filter((h) => h.severity === "high").length === 0 &&
    governanceHits.length === 0;

  // Ignore whitespace-only / announcement-URL peel-restore diffs — they are not residual debt.
  const normalizeCompare = (s) =>
    nz(s)
      .replace(/https?:\/\/\S+/gi, " ")
      .replace(/\s+/g, " ")
      .trim();
  const meaningfulChange = normalizeCompare(after) !== normalizeCompare(before);
  if (!meaningfulChange) {
    after = before;
  }

  return {
    before,
    after,
    changed: meaningfulChange,
    forbiddenAfter,
    mechanicalAfter,
    governanceHits,
    remainingForbidden: [...forbiddenAfter, ...governanceHits],
    sourceFootnotePreserved: trace.sourceFootnote || "",
    brandModel: BRAND_MODEL_SCRUB_PROFILES[brandSlug]?.brandModelType || null,
    clean,
  };
}

export function scrubResidualPresentationRow(row, { brandSlug = "" } = {}) {
  const slotKey = nz(row.slotKey);
  const fields = {};
  const audits = {};
  const patches = [];

  for (const [apiKey, airtableKey] of Object.entries(MAP_PRESENTATION_FIELDS)) {
    const before = nz(row[apiKey]);
    if (!before) continue;
    const scrub = scrubResidualOwnerFacingCopy(before, { slotKey, brandSlug });
    audits[apiKey] = scrub;
    if (!scrub.changed) continue;
    fields[airtableKey] = scrub.after;
    const forbiddenPhrase =
      scrub.forbiddenAfter[0]?.label ||
      scrub.governanceHits[0]?.patternId ||
      scrub.mechanicalAfter[0]?.id ||
      "residual_owner_copy";
    patches.push({
      brandSlug,
      recordId: row.recordId,
      slotKey,
      field: airtableKey,
      before: scrub.before,
      after: scrub.after,
      reason: `v40C residual owner-copy scrub (${forbiddenPhrase})`,
      forbiddenPhraseRemoved: forbiddenPhrase,
      brandModelFitCheck: BRAND_MODEL_SCRUB_PROFILES[brandSlug] ? "pass" : "unknown",
      externalOwnerCopyCheck: scrub.clean ? "pass" : "fail",
      sourceSupportRetained: Boolean(scrub.sourceFootnotePreserved),
      safeForGenericApply: scrub.clean === true,
      codePatchRequired: false,
      airtablePatchRequired: true,
    });
  }

  return {
    recordId: row.recordId,
    slotKey,
    fields,
    changed: Object.keys(fields).length > 0,
    audits,
    patches,
    clean: patches.every((p) => p.externalOwnerCopyCheck === "pass"),
  };
}

/**
 * Build residual Presentation patch plan for one brand.
 */
export function buildResidualOwnerCopyPatchPlan({ brandSlug, presentationRows = [] } = {}) {
  const patches = [];
  const rowResults = [];
  for (const row of presentationRows) {
    if (/do not display|internal only/i.test(nz(row.externalDisplayStatus))) continue;
    const result = scrubResidualPresentationRow(row, { brandSlug });
    rowResults.push(result);
    patches.push(...result.patches);
  }

  const unsafe = patches.filter((p) => !p.safeForGenericApply);
  return {
    version: V40C_RESIDUAL_SCRUBBER_VERSION,
    brandSlug,
    table: PRESENTATION_TABLE,
    patches,
    rowResults,
    summary: {
      patchCount: patches.length,
      unsafeCount: unsafe.length,
      rowsTouched: rowResults.filter((r) => r.changed).length,
    },
  };
}
