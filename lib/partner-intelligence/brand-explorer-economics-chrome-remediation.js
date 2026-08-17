/**
 * v40C — Economics chrome inventory + remediation orchestration helpers.
 *
 * Renderer patches live in public/js/brand-explorer-atelier-from-api.js.
 * This module inventories chrome points and projects post-remediation readiness.
 */
export const V40C_VERSION = "v40c";

export const V40C_DEFAULT_BRANDS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
]);

export const V40C_INCOMPLETE_CONTROL = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
]);

export const V40C_APPLY_FLAGS = Object.freeze({
  approve: "--approve-brand-explorer-v40C-economics-chrome-remediation",
  noCompanyValidation: "--confirm-no-company-validation-claim",
  noActiveApproval: "--confirm-no-active-profile-approval",
  noSourceLibrary: "--confirm-no-source-library-changes",
  noRegistry: "--confirm-no-registry-changes",
  noImageFields: "--confirm-no-image-field-changes",
  externalLocked: "--confirm-external-profiles-remain-locked",
  internalClean: "--confirm-internal-preview-owner-copy-clean",
  brandOnly: "--confirm-brand-only",
});

/** Inventory of economics / diligence chrome injection points (post-patch targets). */
export const ECONOMICS_CHROME_INVENTORY = Object.freeze([
  {
    id: "econ_fee_support_item7_loi",
    sourceFile: "public/js/brand-explorer-atelier-from-api.js",
    rendererFunction: "econFeeSupportFromApi",
    tab: "atelier-economics",
    section: "Typical Economics at a Glance · KPI support notes",
    hardcoded: true,
    fromBrandSetup: true,
    fromPresentation: false,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "Confirm in Item 7 and your LOI.",
    proposedReplacement:
      "Confirm participation costs and timing directly during brand engagement and legal review.",
    codePatchRequired: true,
    airtablePatchRequired: false,
    status: "patched_in_v40c",
  },
  {
    id: "econ_glance_hint_fdd_item7",
    sourceFile: "public/js/brand-explorer-atelier-from-api.js",
    rendererFunction: "renderAtelierEconomicsObligations",
    tab: "atelier-economics",
    section: "Typical Economics at a Glance · section hint",
    hardcoded: true,
    fromBrandSetup: false,
    fromPresentation: false,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "FDD Item 7 style—confirm every line in your disclosure document and LOI",
    proposedReplacement:
      "Typical ranges and fee-schedule notes from Brand Setup—confirm participation costs, operating obligations, and agreement terms directly during brand engagement and legal review",
    codePatchRequired: true,
    airtablePatchRequired: false,
    status: "patched_in_v40c",
  },
  {
    id: "econ_confirm_section_fdd_loi",
    sourceFile: "public/js/brand-explorer-atelier-from-api.js",
    rendererFunction: "renderAtelierEconomicsObligations",
    tab: "atelier-economics",
    section: "Confirm With Brand / Legal Counsel",
    hardcoded: true,
    fromBrandSetup: false,
    fromPresentation: false,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "Confirm in the FDD & LOI / franchise disclosure document, LOI",
    proposedReplacement: "Confirm With Brand / Legal Counsel + owner-safe diligence body",
    codePatchRequired: true,
    airtablePatchRequired: false,
    status: "patched_in_v40c",
  },
  {
    id: "econ_disclaimer_fdd_loi",
    sourceFile: "public/js/brand-explorer-atelier-from-api.js",
    rendererFunction: "renderAtelierEconomicsObligations",
    tab: "atelier-economics",
    section: "How to use this tab",
    hardcoded: true,
    fromBrandSetup: false,
    fromPresentation: true,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "franchise disclosure document, LOI, or your advisors",
    proposedReplacement: "agreement review with the brand and your advisors",
    codePatchRequired: true,
    airtablePatchRequired: "if Presentation economics.intro still dirty",
    status: "patched_in_v40c",
  },
  {
    id: "econ_fee_bucket_footnotes",
    sourceFile: "public/js/brand-explorer-atelier-from-api.js",
    rendererFunction: "econFeeBucketProofHtml / ECON_FEE_BUCKET_DEFS",
    tab: "atelier-economics",
    section: "Fee buckets To Join / To Operate",
    hardcoded: true,
    fromBrandSetup: false,
    fromPresentation: true,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "confirm in the FDD and LOI / net contribution after mandatory program costs",
    proposedReplacement: "owner-safe participation / program-cost diligence footnotes",
    codePatchRequired: true,
    airtablePatchRequired: "if Presentation fee-bucket bodies still dirty",
    status: "patched_in_v40c",
  },
  {
    id: "econ_fee_card_default_body",
    sourceFile: "public/js/brand-explorer-atelier-from-api.js",
    rendererFunction: "econFeeCardBodyFromApi",
    tab: "atelier-economics",
    section: "Fee type cards from Brand Setup",
    hardcoded: true,
    fromBrandSetup: true,
    fromPresentation: false,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "franchise disclosure document and LOI",
    proposedReplacement: "confirm basis and timing directly with the brand and legal counsel",
    codePatchRequired: true,
    airtablePatchRequired: false,
    status: "patched_in_v40c",
  },
  {
    id: "econ_cash_steady_fee_stack",
    sourceFile: "public/js/brand-explorer-atelier-from-api.js",
    rendererFunction: "ECON_CASH_PHASE_DEFS.steadystate",
    tab: "atelier-economics",
    section: "Cash & Capital Rhythm · Steady State",
    hardcoded: true,
    fromBrandSetup: false,
    fromPresentation: true,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "full recurring fee stack",
    proposedReplacement: "full set of recurring participation costs",
    codePatchRequired: true,
    airtablePatchRequired: "if Presentation cash body still dirty",
    status: "patched_in_v40c",
  },
  {
    id: "loyalty_net_contribution_default",
    sourceFile: "public/js/brand-explorer-atelier-from-api.js",
    rendererFunction: "renderAtelierLoyalty (implPnl default)",
    tab: "atelier-loyalty",
    section: "Loyalty implications · P&L",
    hardcoded: true,
    fromBrandSetup: false,
    fromPresentation: true,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "net contribution after costs",
    proposedReplacement: "contribution after program costs",
    codePatchRequired: true,
    airtablePatchRequired: "if Presentation loyalty.implications.pnl still dirty",
    status: "patched_in_v40c",
  },
  {
    id: "gold_detail_fdd_disclaimer",
    sourceFile: "public/js/brand-explorer-gold-detail.js",
    rendererFunction: "working-sample disclaimer",
    tab: "gold-detail panels (when attached)",
    section: "Working sample note",
    hardcoded: true,
    fromBrandSetup: false,
    fromPresentation: false,
    appearsExternal: false,
    appearsInternalPreview: true,
    beforeSnippet: "publicly available FDD materials",
    proposedReplacement: "publicly available brand materials",
    codePatchRequired: true,
    airtablePatchRequired: false,
    status: "patched_in_v40c",
  },
]);

export const BRAND_MODEL_ECONOMICS_COPY = Object.freeze({
  "everhome-suites": {
    model: "extended_stay_platform",
    use: [
      "owner diligence",
      "operating model",
      "room product / kitchen-equipped format",
      "opening obligations",
      "extended-stay demand fit",
    ],
    avoid: ["generic franchise disclosure language", "net contribution", "fee stack"],
  },
  kimpton: {
    model: "lifestyle_full_brand",
    use: [
      "operating complexity",
      "F&B / experience expectations",
      "brand standards and agreement terms",
      "IHG system participation where source-supported",
    ],
    avoid: ["FDD / Item 19", "visible URLs", "fee-stack boilerplate"],
  },
  "radisson-individuals-by-choice": {
    model: "soft_brand_collection",
    use: [
      "conversion / affiliation fit",
      "owner flexibility",
      "Choice system participation",
      "agreement and program requirements to confirm directly",
    ],
    avoid: ["LOI", "fee-stack", "franchise disclosure boilerplate"],
  },
  "design-hotels": {
    model: "affiliation_curation_platform",
    use: [
      "independent design-led member hotels",
      "curation / architecture / local identity",
      "owner individuality",
      "affiliation and recognition value",
      "Marriott Bonvoy / Marriott ecosystem context only where source-supported and carefully caveated",
    ],
    avoid: [
      "franchise flag",
      "chain prototype",
      "FDD / Item 19 / LOI",
      "fee stack / net contribution",
      "ADR / RevPAR",
      "brand-verified",
      "raw URLs / Sources notes",
    ],
  },
});

/** Forbidden strings for internal preview owner-copy gate (founder review path). */
export const V40C_INTERNAL_PREVIEW_FORBIDDEN = Object.freeze([
  { id: "loi", re: /\bLOI\b/, label: "LOI" },
  { id: "fdd", re: /\bFDD\b/, label: "FDD" },
  { id: "item_19", re: /\bItem\s*19\b/i, label: "Item 19" },
  { id: "item_7", re: /\bItem\s*7\b/i, label: "Item 7" },
  { id: "franchise_disclosure", re: /\bfranchise disclosure\b/i, label: "franchise disclosure" },
  { id: "fee_stack", re: /\bfee stack\b/i, label: "fee stack" },
  { id: "net_contribution", re: /\bnet contribution\b/i, label: "net contribution" },
  { id: "raw_url", re: /https?:\/\/\S+/i, label: "raw URLs" },
  { id: "sources_block", re: /\bSources:\s*/i, label: "Sources:" },
  { id: "source_line", re: /\bSource:\s*/i, label: "Source:" },
  { id: "disclosure_document", re: /\bdisclosure document\b/i, label: "disclosure document" },
  { id: "performance_representation", re: /\bperformance representation\b/i, label: "performance representation" },
  { id: "adr", re: /\bADR\b/, label: "ADR" },
  { id: "revpar", re: /\bRevPAR\b/, label: "RevPAR" },
  { id: "output_note", re: /\bOutput Note\b/i, label: "Output Note" },
  { id: "internal_review", re: /\binternal review\b/i, label: "internal review" },
  { id: "participation_cost_categories", re: /\bparticipation cost categories\b/i, label: "participation cost categories" },
  { id: "owner_economics_awkward", re: /\bowner economics after brand-related costs\b/i, label: "owner economics after brand-related costs" },
]);

export function scanInternalPreviewOwnerCopy(text) {
  const blob = text == null ? "" : String(text);
  const hits = [];
  for (const rule of V40C_INTERNAL_PREVIEW_FORBIDDEN) {
    const m = blob.match(rule.re);
    if (m) hits.push({ id: rule.id, label: rule.label, snippet: m[0].slice(0, 100) });
  }
  return hits;
}

export function buildV40CApplyDesign(brandSlugs = V40C_DEFAULT_BRANDS) {
  const brands = brandSlugs.join(",");
  return {
    command: [
      "npm run brand-explorer-v40c-economics-chrome-remediation --",
      `--brands ${brands}`,
      "--apply",
      V40C_APPLY_FLAGS.approve,
      V40C_APPLY_FLAGS.noCompanyValidation,
      V40C_APPLY_FLAGS.noActiveApproval,
      V40C_APPLY_FLAGS.noSourceLibrary,
      V40C_APPLY_FLAGS.noRegistry,
      V40C_APPLY_FLAGS.noImageFields,
      V40C_APPLY_FLAGS.externalLocked,
      V40C_APPLY_FLAGS.internalClean,
      V40C_APPLY_FLAGS.brandOnly,
    ].join(" \\\n  "),
    allowedWrites: [
      "Presentation Title",
      "Presentation Body",
      "Case Summary fields",
      "External Display Status (hide only)",
    ],
    forbiddenWrites: [
      "active approval",
      "Company Validated",
      "Source Library",
      "Registry",
      "image fields",
      "external unlock",
    ],
    note: "Renderer chrome is committed via code. Apply only patches Presentation residual copy.",
  };
}
