/**
 * v40 — Brand Explorer owner-facing copy scrubber.
 *
 * Rewrites forbidden owner-visible language into diligence-safe copy.
 * Preserves meaning; does not expose Sources:/URLs; no generic filler.
 */
import {
  AFFILIATION_SANITIZE_REPLACEMENTS,
  auditExternalOwnerPhrase,
  extractSourceFootnote,
  sanitizeAffiliationExternalCopy,
  stripInlineUrls,
} from "./brand-explorer-external-owner-content-governance.js";

export const V40_OWNER_COPY_SCRUBBER_VERSION = "v40";

/** Extra replacements beyond affiliation sanitize — applies to all release candidates. */
export const V40_OWNER_SAFE_REPLACEMENTS = Object.freeze([
  { re: /\bConfirm in (the )?LOI\s*\/\s*FDD\.?/gi, replace: "Confirm participation costs, operating obligations, and agreement terms directly during brand engagement and legal review." },
  { re: /\bConfirm in (your |the )?LOI\.?/gi, replace: "Confirm agreement terms directly during brand engagement and legal review." },
  { re: /\bConfirm in (your |the )?FDD\.?/gi, replace: "Confirm participation costs and obligations directly during brand engagement and legal review." },
  { re: /\bconfirm every line in your disclosure document and LOI\b/gi, replace: "confirm participation terms directly with brand representatives and counsel" },
  { re: /\bConfirm categories, basis, and timing in your FDD and LOI\.?/gi, replace: "Confirm participation categories and timing directly with brand representatives." },
  { re: /\bItem\s*19\s*\/\s*FDD\.?/gi, replace: "Public materials do not provide property-level performance economics. Treat economics as a diligence item, not a forecast." },
  { re: /\bItem\s*19\b/gi, replace: "public franchise performance disclosures (where applicable)" },
  { re: /\bFDD\b/g, replace: "franchise disclosure materials" },
  { re: /\bLOI\b/g, replace: "letter of intent or commercial proposal" },
  { re: /\bfranchise disclosure document\b/gi, replace: "commercial agreement materials" },
  { re: /\bfranchise disclosure\b/gi, replace: "commercial agreement review" },
  { re: /\bdisclosure document\b/gi, replace: "commercial agreement materials" },
  { re: /\bfee stack\b/gi, replace: "participation cost categories" },
  { re: /\bnet contribution\b/gi, replace: "owner economics after brand-related costs" },
  { re: /\bEvaluate net contribution after (the )?fee stack\.?/gi, replace: "Evaluate whether the brand’s distribution, operating requirements, and owner obligations fit the asset’s demand mix and investment plan." },
  { re: /\bperformance representation\b/gi, replace: "operating performance detail" },
  { re: /\brooms from loyalty\b/gi, replace: "loyalty-driven occupancy contribution" },
  { re: /\bADR\b/g, replace: "average daily rate" },
  { re: /\bRevPAR\b/g, replace: "revenue per available room" },
  { re: /\bSources?:\s*/gi, replace: "" },
  { re: /\binternal review\b/gi, replace: "owner diligence review" },
  { re: /\bsupports internal review\b/gi, replace: "supports owner diligence" },
  { re: /\bOutput Note\.?/gi, replace: "" },
  { re: /\bstaging\b/gi, replace: "preparation" },
  { re: /\bmetadata\b/gi, replace: "reference detail" },
  { re: /\bAI-Assisted from Official Public Sources[^.]*\.?\s*/gi, replace: "Curated by Dealality from official public brand materials. " },
]);

export const BRAND_MODEL_SCRUB_PROFILES = Object.freeze({
  "everhome-suites": {
    brandModelType: "extended_stay_platform",
    focus: "extended-stay operating model, room product, demand fit, opening obligations, owner diligence",
    avoid: "generic franchise/legal boilerplate; unsupported fee/performance claims",
  },
  kimpton: {
    brandModelType: "lifestyle_full_brand",
    focus: "lifestyle positioning, operating complexity, F&B/experience expectations, IHG system context where source-supported",
    avoid: "FDD/Item 19 in owner UI; visible URLs; internal notes",
  },
  "radisson-individuals-by-choice": {
    brandModelType: "soft_brand_collection",
    focus: "conversion fit, owner flexibility, Choice system context where source-supported",
    avoid: "LOI / fee-stack boilerplate; unsupported performance or fee claims",
  },
  "design-hotels": {
    brandModelType: "affiliation_curation_platform",
    focus:
      "independent design-led member hotels, curation, architecture and local identity, affiliation and recognition value, Marriott Bonvoy context only where source-supported and caveated",
    avoid:
      "franchise flag / chain prototype / FDD / Item 19 / LOI / fee-stack / ADR-RevPAR / brand-verified / raw URLs / Sources notes",
  },
});

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

/**
 * Preserve blank-line paragraph breaks (date / summary / trailing URL) while
 * normalizing intra-line whitespace. Required for footprint.momentum / openings.
 */
function collapseWhitespacePreserveParagraphs(s) {
  return nz(s)
    .split(/\n/)
    .map((line) => line.replace(/[ \t]{2,}/g, " ").trimEnd())
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function isOpeningsOrMomentumSlot(slotKey) {
  const k = nz(slotKey);
  return k === "footprint.openings" || k === "footprint.momentum";
}

/** Peel a trailing https URL line so hard-strips do not destroy announcement links. */
function peelTrailingHttpUrl(text) {
  const lines = nz(text).split("\n");
  if (!lines.length) return { body: "", trailingUrl: "" };
  const last = (lines[lines.length - 1] || "").trim();
  if (/^https?:\/\//i.test(last)) {
    return { body: lines.slice(0, -1).join("\n").trim(), trailingUrl: last };
  }
  return { body: nz(text), trailingUrl: "" };
}

/**
 * Classify blocker phrase into v40 blocker types.
 */
export function classifyOwnerCopyBlockerType(patternId) {
  const id = nz(patternId).toLowerCase();
  if (id === "http_url" || id === "visible_url") return "visible_url";
  if (id === "fee_stack" || id === "fee_schedule" || id === "franchise_license") return "fee_stack_language";
  if (id === "item_19" || id === "confirm_fdd" || id === "franchise_disclosure") return "fdd_item19_language";
  if (id === "loi") return "loi_language";
  if (id === "net_contribution") return "net_contribution_language";
  if (
    id === "sources_block" ||
    id === "source_line" ||
    id === "source_library" ||
    id === "approved_source" ||
    id === "evidence_note" ||
    id === "staging_run" ||
    id === "governance_impl" ||
    id === "internal_ref" ||
    id === "qa_ref"
  ) {
    return "internal_source_language";
  }
  return "forbidden_owner_copy";
}

/**
 * Scrub owner-facing text for a slot.
 * @returns {{ before: string, after: string, changed: boolean, hitsBefore: object[], hitsAfter: object[], sourceFootnotePreserved: string }}
 */
export function scrubOwnerFacingCopy(text, { slotKey = "", brandSlug = "" } = {}) {
  const before = nz(text);
  const trace = extractSourceFootnote(before);
  let after = trace.mainBody || before;

  const preserveAnnouncementUrl = isOpeningsOrMomentumSlot(slotKey);

  after = sanitizeAffiliationExternalCopy(after, { slotKey });
  after = stripInlineUrls(after, {
    allowTrailingPropertyUrl: preserveAnnouncementUrl,
  });

  for (const rule of AFFILIATION_SANITIZE_REPLACEMENTS) {
    after = after.replace(rule.re, rule.replace);
  }
  for (const rule of V40_OWNER_SAFE_REPLACEMENTS) {
    after = after.replace(rule.re, rule.replace);
  }

  // Hard strip residual forbidden tokens that survived rewrite (keep sentence readable).
  // For openings/momentum, preserve a trailing announcement URL on its own line.
  let trailingAnnouncementUrl = "";
  if (preserveAnnouncementUrl) {
    const peeled = peelTrailingHttpUrl(after);
    after = peeled.body;
    trailingAnnouncementUrl = peeled.trailingUrl;
  }
  after = after
    .replace(/\bLOI\b/g, "commercial proposal")
    .replace(/\bFDD\b/g, "franchise disclosure materials")
    .replace(/\bItem\s*19\b/gi, "public performance disclosures")
    .replace(/https?:\/\/\S+/gi, "");
  if (trailingAnnouncementUrl) {
    after = `${nz(after)}\n\n${trailingAnnouncementUrl}`;
  }

  after = preserveAnnouncementUrl
    ? collapseWhitespacePreserveParagraphs(after)
    : collapseWhitespace(after);

  // Empty after scrub of a short forbidden-only string — use diligence placeholder by brand model
  if (!after && before) {
    const profile = BRAND_MODEL_SCRUB_PROFILES[brandSlug];
    after = profile
      ? `Confirm ${profile.focus} directly during brand engagement. Public materials on this page are orientation only—not commercial terms or a forecast.`
      : "Confirm participation costs, operating obligations, and agreement terms directly during brand engagement and legal review.";
  }

  const hitsBefore = auditExternalOwnerPhrase(before, slotKey);
  const hitsAfter = auditExternalOwnerPhrase(after, slotKey);

  return {
    before,
    after,
    changed: after !== before,
    hitsBefore,
    hitsAfter,
    remainingForbidden: hitsAfter.filter((h) => ["critical", "high"].includes(h.severity)),
    sourceFootnotePreserved: trace.sourceFootnote || "",
    brandModel: BRAND_MODEL_SCRUB_PROFILES[brandSlug]?.brandModelType || null,
  };
}

/**
 * Scrub a full presentation row (title, body, case summary fields).
 */
export function scrubPresentationRow(row, { brandSlug = "" } = {}) {
  const slotKey = nz(row.slotKey);
  const fields = {};
  const audits = {};

  const titleScrub = scrubOwnerFacingCopy(row.title || "", { slotKey, brandSlug });
  if (titleScrub.changed) fields.Title = titleScrub.after;
  audits.title = titleScrub;

  const bodyScrub = scrubOwnerFacingCopy(row.body || "", { slotKey, brandSlug });
  if (bodyScrub.changed) fields.Body = bodyScrub.after;
  audits.body = bodyScrub;

  const caseFields = [
    ["caseSummaryOverview", "Case Summary Overview"],
    ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
    ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
    ["caseSummaryInterpretation", "Case Summary Interpretation"],
    ["caseSummaryTags", "Case Summary Tags"],
  ];
  for (const [apiKey, airtableKey] of caseFields) {
    if (!nz(row[apiKey])) continue;
    const scrub = scrubOwnerFacingCopy(row[apiKey], { slotKey, brandSlug });
    if (scrub.changed) fields[airtableKey] = scrub.after;
    audits[apiKey] = scrub;
  }

  const allHitsBefore = [
    ...titleScrub.hitsBefore,
    ...bodyScrub.hitsBefore,
    ...Object.values(audits)
      .filter((a) => a && a !== titleScrub && a !== bodyScrub)
      .flatMap((a) => a.hitsBefore || []),
  ];
  const allHitsAfter = [
    ...((fields.Title != null ? scrubOwnerFacingCopy(fields.Title, { slotKey, brandSlug }) : titleScrub).hitsAfter),
    ...((fields.Body != null ? scrubOwnerFacingCopy(fields.Body, { slotKey, brandSlug }) : bodyScrub).hitsAfter),
  ];

  return {
    recordId: row.recordId,
    slotKey,
    fields,
    changed: Object.keys(fields).length > 0,
    audits,
    hitsBefore: allHitsBefore,
    hitsAfter: allHitsAfter,
    externalOwnerCopyClean: allHitsAfter.filter((h) => ["critical", "high"].includes(h.severity)).length === 0,
    sourceSupportRetained: Boolean(bodyScrub.sourceFootnotePreserved),
  };
}

export function forbiddenTermsRemain(text, slotKey = "") {
  return auditExternalOwnerPhrase(text, slotKey).filter((h) => ["critical", "high"].includes(h.severity));
}
