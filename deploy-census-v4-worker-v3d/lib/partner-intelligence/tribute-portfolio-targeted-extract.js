/**
 * Tribute Portfolio — targeted, source-backed extraction pass (dry-run default).
 *
 * WHY THIS EXISTS
 * The generic brand extractor (`run-extraction.runPartnerBrandExtraction`) is
 * pilot-keyed: `resolveBrandExtractionContext` only resolves brands present in
 * `PILOT_BRANDS`, and `getBrandFieldHints` returns null when there is no pilot
 * profile. Tribute Portfolio is not a pilot brand, so every registry field fell
 * through to a data-gap placeholder → 24 Pending facts, 0 approvable.
 *
 * This module applies a conservative, evidence-required Tribute (Marriott soft-
 * brand) pattern set to the ACTUAL loaded text of the approved Marriott-controlled
 * sources. It only emits a fact when a pattern matches real text (with an evidence
 * excerpt). No gap facts, no placeholder values, no title-only facts (except
 * identity), no economics/fees/Item 19/legal (those stay Internal Only / human
 * review and are never targeted here).
 *
 * Reuses existing factory primitives: extract-source-text (loader), airtable
 * source/fact helpers, the Brand Explorer registry. The pattern set is structured
 * as a reusable Marriott soft-brand profile so future Tribute-like brands can be
 * added by config.
 *
 * Dry-run default. Apply creates Pending facts only — never approves, never
 * publishes, never writes Brand Setup / hero / logo, never sets Company Validated.
 *
 * @see docs/data-intelligence/tribute-portfolio-package-pipeline-v1.md
 */
import { randomUUID } from "crypto";
import {
  MAP_PARTNER_FACT,
  PARTNER_INTELLIGENCE_GAP_COPY,
} from "../../api/lib/partner-intelligence-field-map.js";
import { getRegistryField } from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { listPartnerSources } from "./airtable-source.js";
import { listPartnerFacts, createPartnerFact } from "./airtable-facts.js";
import { loadSourceDocumentText, excerptAround } from "./extract-source-text.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME, PARENT_COMPANY } from "./tribute-portfolio-brand-package.js";

export const TARGETED_VERSION = "2";
export const V23_WAVE = "v23-evidence-readiness";
export const REPORT_JSON_NAME = "tribute-portfolio-targeted-extract.json";
export const REPORT_MD_NAME = "tribute-portfolio-targeted-extract.md";
export const RUN_ID_PREFIX = "tribute-targeted";
export const TARGETED_TAG = "tribute-targeted";

/** Registry keys we NEVER target here — held Internal Only / human review. */
export const HELD_FIELD_KEYS = new Set([
  "be.economics.royaltyPct",
  "be.economics.initialFranchiseFee",
  "be.economics.marketingFeePct",
]);

/**
 * Conservative Tribute (Marriott soft-brand) extraction profile.
 * Each rule emits a fact ONLY when a pattern matches loaded source text.
 *  - approvable: Directly Stated company copy → safe for source-backed stewardship.
 *  - aiInterpreted: derived/paraphrased → created as Inferred, flagged human review.
 * `preferRoles` orders which loaded sources to scan first (by classified role).
 */
export const TRIBUTE_RULES = [
  {
    fieldKey: "be.identity.brandName",
    kind: "identity",
    approvable: true,
    extractionType: "Directly Stated",
    confidenceLevel: "High",
    confidenceScore: 90,
    fixedValue: BRAND_NAME,
    patterns: [/\bTribute Portfolio\b/i],
    preferRoles: ["consumer_page", "brand_page", "development_page", "local_pdf"],
  },
  {
    fieldKey: "be.identity.parentCompany",
    kind: "identity",
    approvable: true,
    extractionType: "Directly Stated",
    confidenceLevel: "High",
    confidenceScore: 88,
    fixedValue: PARENT_COMPANY,
    patterns: [/Marriott International(?:,?\s*Inc\.?)?/i],
    preferRoles: ["brand_page", "development_page", "local_pdf", "consumer_page"],
  },
  {
    fieldKey: "be.loyalty.programName",
    kind: "value",
    approvable: true,
    extractionType: "Directly Stated",
    confidenceLevel: "High",
    confidenceScore: 88,
    fixedValue: "Marriott Bonvoy",
    patterns: [/Marriott Bonvoy/i],
    preferRoles: ["consumer_page", "brand_page", "development_page"],
  },
  {
    fieldKey: "be.positioning.summary",
    kind: "value",
    approvable: true,
    extractionType: "Directly Stated",
    confidenceLevel: "High",
    confidenceScore: 84,
    patterns: [
      /A family of independent boutique hotels bound by[^.]*\./i,
      /An exceptional collection of independent hotels[^.]*\.(?:\s*Stay independent\.?)?/i,
      /collection of (?:characterful,?\s*)?independent hotels[^.]*\./i,
      /Tribute Portfolio is[^.]*independent[^.]*\./i,
    ],
    preferRoles: ["consumer_page", "brand_page", "development_page"],
  },
  {
    fieldKey: "be.positioning.tagline",
    kind: "value",
    approvable: true,
    extractionType: "Directly Stated",
    confidenceLevel: "High",
    confidenceScore: 82,
    patterns: [/Stay independent\.?/i],
    transform: () => "Stay independent.",
    preferRoles: ["consumer_page", "brand_page", "development_page"],
  },
  {
    fieldKey: "be.positioning.guestPromise",
    kind: "value",
    approvable: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 74,
    patterns: [
      /bound by their Indie spirit and heart for connecting people and places[^.]*\.?/i,
      /independent hotels and resorts with unique personalities and spirit[^.]*\./i,
      /unique personalities and spirit[^.]*\./i,
    ],
    preferRoles: ["consumer_page", "brand_page", "development_page"],
  },
  {
    fieldKey: "be.overview.developmentModel",
    kind: "value",
    approvable: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 74,
    patterns: [
      /best-in-class operating model for independent hotels[^.]*\./i,
      /turnkey loyalty program[^.]*\./i,
    ],
    preferRoles: ["brand_page", "development_page", "consumer_page"],
  },
  {
    fieldKey: "be.overview.whyValue",
    kind: "value",
    approvable: false,
    aiInterpreted: true,
    extractionType: "Inferred",
    confidenceLevel: "Medium",
    confidenceScore: 62,
    patterns: [
      /best-in-class operating model for independent hotels[\s\S]{0,240}?financing flexibility\.?/i,
      /Lender confidence in Marriott-branded affiliation[^.]*\./i,
    ],
    preferRoles: ["brand_page", "development_page"],
  },
  {
    fieldKey: "be.overview.typicalUseCase",
    kind: "value",
    approvable: false,
    aiInterpreted: true,
    extractionType: "Inferred",
    confidenceLevel: "Low",
    confidenceScore: 45,
    patterns: [/best-in-class operating model for independent hotels/i, /independent hotels and resorts/i],
    transform: () =>
      "Independent and conversion hotels seeking Marriott distribution and Bonvoy access while retaining their own identity (AI-interpreted from company materials — human review).",
    preferRoles: ["brand_page", "development_page", "consumer_page"],
  },
];

/** Field keys eligible for auto-approval when created by targeted extraction. */
export const TARGETED_APPROVABLE_KEYS = new Set(
  TRIBUTE_RULES.filter((r) => r.approvable).map((r) => r.fieldKey)
);

/** Field keys that stay Pending / human-review (AI-interpreted). */
export const TARGETED_HUMAN_REVIEW_KEYS = new Set(
  TRIBUTE_RULES.filter((r) => r.aiInterpreted).map((r) => r.fieldKey)
);

/**
 * v23 evidence-readiness rules — Bonvoy mechanics, standards themes, governance vintage.
 * Never emits economics/fees/Item 19, KPI numerics, property proof, or case studies.
 */
export const V23_TRIBUTE_RULES = [
  {
    fieldKey: "be.loyalty.earnMechanics",
    targetExplorerSlots: ["loyalty.earn"],
    category: "safe_source_backed_candidate",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 72,
    patterns: [
      /Earn and redeem points that take you everywhere you want to go\.?/i,
      /Earn Free Nights, Discounted Member Rates & More With Marriott Bonvoy\.?/i,
    ],
    preferRoles: ["bonvoy_page", "consumer_page"],
  },
  {
    fieldKey: "be.loyalty.redeemMechanics",
    targetExplorerSlots: ["loyalty.redeem"],
    category: "safe_source_backed_candidate",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 72,
    patterns: [
      /Earn and redeem points that take you everywhere you want to go\.?/i,
      /Use points for dining, golf, spas, and more during a stay/i,
      /Redeeming Hotel Nights and More/i,
    ],
    preferRoles: ["bonvoy_page", "consumer_page"],
  },
  {
    fieldKey: "be.loyalty.eliteTierLadder",
    targetExplorerSlots: ["loyalty.elite"],
    category: "safe_source_backed_candidate",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 70,
    patterns: [/Member\s+Silver Elite\s+Gold Elite\s+Platinum Elite\s+Titanium Elite\s+Ambassador Elite/i],
    transform: (m) =>
      nz(m[0])
        .replace(/\s+/g, " ")
        .replace(/nextTab.*/i, "")
        .trim(),
    preferRoles: ["bonvoy_page"],
  },
  {
    fieldKey: "be.loyalty.memberRatesBenefit",
    targetExplorerSlots: ["loyalty.earn", "loyalty.proof"],
    category: "safe_source_backed_candidate",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 68,
    patterns: [/Enjoy exclusive discounted rates for Marriott Bonvoy Members\.?/i, /Member Rates Enjoy exclusive member room rates/i],
    preferRoles: ["bonvoy_page"],
  },
  {
    fieldKey: "be.loyalty.programScaleStatement",
    targetExplorerSlots: ["loyalty.proof"],
    category: "safe_source_backed_candidate",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Low",
    confidenceScore: 58,
    patterns: [/Rewards You at 7,000\+ Hotels Worldwide\.?/i, /Over 30 hotel brands and 10,000 global destinations/i],
    preferRoles: ["bonvoy_page", "consumer_page"],
  },
  {
    fieldKey: "be.standards.qualityAssuranceTheme",
    targetExplorerSlots: ["standards.requirement"],
    category: "existing_evidence_needs_human_review",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 66,
    fddLegal: true,
    patterns: [/quality assurance program\./i],
    preferRoles: ["local_pdf"],
  },
  {
    fieldKey: "be.standards.designStandardsDelivery",
    targetExplorerSlots: ["standards.requirement"],
    category: "existing_evidence_needs_human_review",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 66,
    fddLegal: true,
    patterns: [
      /receive the Design Standards from Franchisor within 10 days of the Effective Date/i,
      /Design Standards from Franchisor within 10 days of the Effective Date/i,
    ],
    preferRoles: ["local_pdf"],
  },
  {
    fieldKey: "be.meta.fddDocumentVintage",
    targetExplorerSlots: ["standards.last_reviewed"],
    category: "existing_evidence_needs_human_review",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "High",
    confidenceScore: 85,
    fixedValue:
      "Source document vintage: 2026 Tribute Portfolio FDD dated March 31, 2026 — governance reference only; not a Dealality or company review date.",
    patterns: [/March 31, 2026/i, /3\/31\/2026/i],
    preferRoles: ["local_pdf"],
  },
  {
    fieldKey: "be.positioning.independentCollectionStatement",
    targetExplorerSlots: ["overview.proof_operator"],
    category: "safe_source_backed_candidate",
    approvable: false,
    humanReview: true,
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    confidenceScore: 64,
    patterns: [
      /A family of independent boutique hotels bound by their Indie spirit and heart for connecting people and places\.?/i,
    ],
    preferRoles: ["consumer_page", "brand_page"],
  },
];

/** v23 target slots we track even when no rule matches. */
export const V23_TARGET_SLOTS = [
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
  "loyalty.kpi.hotels",
  "loyalty.kpi.markets",
  "loyalty.kpi.members",
  "loyalty.kpi.mix",
  "standards.last_reviewed",
  "standards.requirement",
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "overview.proof.5",
  "overview.proof.6",
  "overview.proof_operator",
];

export const V23_NEEDS_NEW_CAPTURE = [
  "loyalty.kpi.hotels",
  "loyalty.kpi.markets",
  "loyalty.kpi.members",
  "loyalty.kpi.mix",
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "overview.proof.5",
  "overview.proof.6",
];

export const V23_INTERNAL_ONLY_SLOTS = ["economics.*"];

export const V23_V23B_TARGET_SLOTS = [
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "standards.last_reviewed",
  "standards.requirement",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

/* ------------------------------------------------------------------ */
/* Source role classification (for scan ordering + reporting)          */
/* ------------------------------------------------------------------ */

export function classifyLoadedSourceRole(source) {
  const type = nz(source.sourceType).toLowerCase();
  const title = `${nz(source.sourceTitle)} ${nz(source.sourceUrl)} ${nz(source.localFilePath)}`.toLowerCase();
  if (type === "fdd") return "local_pdf";
  if (/bonvoy|loyalty/.test(title)) return "bonvoy_page";
  if (/development|brand-portfolio|development home/.test(title)) return "development_page";
  if (/brand page|premium|portfolio page|brand-page/.test(title)) return "brand_page";
  if (/tribute-portfolio\.marriott\.com|consumer/.test(title)) return "consumer_page";
  return "other";
}

const KEY_PHRASES = [
  "Tribute Portfolio",
  "Marriott International",
  "Marriott Bonvoy",
  "independent hotels",
  "Stay independent",
  "unique personalities and spirit",
  "operating model for independent hotels",
  "Lender confidence",
];

/* ------------------------------------------------------------------ */
/* Load source text (lazy cache)                                       */
/* ------------------------------------------------------------------ */

export async function loadApprovedTributeSources(recordId = TRIBUTE_RECORD_ID) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ brandId: recordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
}

/**
 * Build a source-text inventory (readable length, key phrases, role, eligibility).
 * Text is loaded eagerly here so the dry-run report can show real diagnostics.
 */
export async function buildSourceInventory(sources) {
  const inventory = [];
  for (const s of sources) {
    const role = classifyLoadedSourceRole(s);
    const approvedExtraction = nz(s.approvedForExtraction) === "Yes";
    const approvedExplorer = nz(s.approvedForExplorerUse) === "Yes";
    let text = "";
    let error = null;
    try {
      const doc = await loadSourceDocumentText(s);
      text = nz(doc.text);
    } catch (err) {
      error = err.message || String(err);
    }
    const lower = text.toLowerCase();
    const keyPhrasesPresent = KEY_PHRASES.filter((p) => lower.includes(p.toLowerCase()));
    inventory.push({
      id: s.id,
      title: s.sourceTitle,
      sourceType: s.sourceType,
      sourceUrl: nz(s.sourceUrl),
      localFilePath: nz(s.localFilePath),
      role,
      approvedForExtraction: approvedExtraction,
      approvedForExplorerUse: approvedExplorer,
      textLength: text.length,
      readable: text.length > 0,
      keyPhrasesPresent,
      extractionEligible: approvedExtraction && text.length > 0 && role !== "bonvoy_page" ? true : approvedExtraction && text.length > 0,
      error,
      _text: text,
    });
  }
  return inventory;
}

/* ------------------------------------------------------------------ */
/* Existing fact audit                                                 */
/* ------------------------------------------------------------------ */

export async function auditExistingFacts(recordId = TRIBUTE_RECORD_ID) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: recordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);

  const brandFacts = all.filter((f) => nz(f.fieldName).startsWith("be."));
  const dataGaps = brandFacts.filter((f) => nz(f.dataGap) === "Yes" || !nz(f.extractedValue));
  const heldInternal = brandFacts.filter((f) => nz(f.publicVisibility) === "Internal Only");
  const withValue = brandFacts.filter((f) => nz(f.dataGap) !== "Yes" && nz(f.extractedValue));
  const priorTargeted = brandFacts.filter(isTargetedFact);

  return {
    total: brandFacts.length,
    dataGapCount: dataGaps.length,
    heldInternalCount: heldInternal.length,
    withValueCount: withValue.length,
    priorTargetedCount: priorTargeted.length,
    dataGapFieldKeys: dataGaps.map((f) => f.fieldName),
    withValueFieldKeys: withValue.map((f) => f.fieldName),
    priorTargetedFieldKeys: priorTargeted.map((f) => f.fieldName),
    facts: brandFacts,
    _priorTargetedByField: new Set(priorTargeted.map((f) => nz(f.fieldName))),
    _valueByField: new Set(withValue.map((f) => nz(f.fieldName))),
  };
}

/* ------------------------------------------------------------------ */
/* Targeted extraction                                                 */
/* ------------------------------------------------------------------ */

function scanRuleAgainstInventory(rule, inventory) {
  const ordered = [...inventory].sort((a, b) => {
    const ai = rule.preferRoles.indexOf(a.role);
    const bi = rule.preferRoles.indexOf(b.role);
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
  });

  for (const src of ordered) {
    if (!src.readable || !src.approvedForExtraction) continue;
    const text = src._text || "";
    for (const pattern of rule.patterns) {
      const m = text.match(pattern);
      if (!m) continue;
      const matchIdx = m.index ?? 0;
      let value;
      if (rule.transform) value = rule.transform(m);
      else if (rule.fixedValue) value = rule.fixedValue;
      else value = nz(m[0]).replace(/\s+/g, " ");
      if (!value) continue;
      if (value.length > 1200) value = value.slice(0, 1197) + "…";
      return {
        value,
        evidence: excerptAround(text, matchIdx, 160),
        sourceId: src.id,
        sourceTitle: src.title,
        sourceRole: src.role,
      };
    }
  }
  return null;
}

/**
 * Build proposed targeted facts. Never returns gap/placeholder facts.
 * @returns {{ proposed: object[], notFound: object[], skippedDuplicate: object[] }}
 */
export function buildTargetedFacts(inventory, factAudit) {
  const proposed = [];
  const notFound = [];
  const skippedDuplicate = [];

  for (const rule of TRIBUTE_RULES) {
    if (HELD_FIELD_KEYS.has(rule.fieldKey)) continue;

    if (factAudit._priorTargetedByField.has(rule.fieldKey) || factAudit._valueByField.has(rule.fieldKey)) {
      skippedDuplicate.push({
        fieldKey: rule.fieldKey,
        reason: factAudit._priorTargetedByField.has(rule.fieldKey)
          ? "prior_targeted_fact_exists"
          : "existing_value_fact_exists",
      });
      continue;
    }

    const hit = scanRuleAgainstInventory(rule, inventory);
    if (!hit) {
      notFound.push({ fieldKey: rule.fieldKey, reason: "no_pattern_match_in_approved_sources" });
      continue;
    }

    proposed.push({
      fieldKey: rule.fieldKey,
      value: hit.value,
      evidence: hit.evidence,
      extractionType: rule.extractionType,
      confidenceLevel: rule.confidenceLevel,
      confidenceScore: rule.confidenceScore,
      approvable: Boolean(rule.approvable),
      aiInterpreted: Boolean(rule.aiInterpreted),
      humanReview: Boolean(rule.aiInterpreted),
      publicVisibility: "Public",
      sourceId: hit.sourceId,
      sourceTitle: hit.sourceTitle,
      sourceRole: hit.sourceRole,
    });
  }

  return { proposed, notFound, skippedDuplicate };
}

function sourceRefForInventoryEntry(hit, inventory) {
  const src = inventory.find((s) => s.id === hit.sourceId);
  return {
    sourceId: hit.sourceId,
    sourceTitle: hit.sourceTitle,
    sourceRole: hit.sourceRole,
    sourceUrl: nz(src?.sourceUrl),
    localFilePath: nz(src?.localFilePath),
  };
}

/**
 * Build v23 evidence-readiness candidate facts (never gap/placeholder facts).
 */
export function buildV23CandidateFacts(inventory, factAudit) {
  const proposed = [];
  const notFound = [];
  const skippedDuplicate = [];
  const internalOnly = [];
  const unsupported = [];

  for (const rule of V23_TRIBUTE_RULES) {
    if (HELD_FIELD_KEYS.has(rule.fieldKey)) {
      internalOnly.push({ fieldKey: rule.fieldKey, reason: "held_field_key" });
      continue;
    }

    if (factAudit._priorTargetedByField.has(rule.fieldKey) || factAudit._valueByField.has(rule.fieldKey)) {
      skippedDuplicate.push({
        fieldKey: rule.fieldKey,
        reason: factAudit._priorTargetedByField.has(rule.fieldKey)
          ? "prior_targeted_fact_exists"
          : "existing_value_fact_exists",
      });
      continue;
    }

    const hit = scanRuleAgainstInventory(rule, inventory);
    if (!hit) {
      notFound.push({
        fieldKey: rule.fieldKey,
        targetExplorerSlots: rule.targetExplorerSlots,
        reason: "no_pattern_match_in_approved_sources",
      });
      continue;
    }

    const sourceRef = sourceRefForInventoryEntry(hit, inventory);
    const displayEligibility = rule.fddLegal
      ? "pending_human_review_before_external_display"
      : rule.humanReview
        ? "pending_human_review"
        : "candidate_public_after_review";

    proposed.push({
      fieldKey: rule.fieldKey,
      value: hit.value,
      evidence: hit.evidence,
      evidenceNote: rule.fddLegal
        ? "FDD/legal theme — human review required; do not present as legal advice."
        : "Marriott-controlled source excerpt — not Marriott validation.",
      extractionType: rule.extractionType,
      confidenceLevel: rule.confidenceLevel,
      confidenceScore: rule.confidenceScore,
      approvable: Boolean(rule.approvable),
      aiInterpreted: Boolean(rule.aiInterpreted),
      humanReview: true,
      reviewStatus: "Pending Review",
      publicVisibility: rule.fddLegal ? "Internal Only" : "Public",
      displayEligibility,
      targetExplorerSlots: rule.targetExplorerSlots || [],
      category: rule.category,
      sourceId: hit.sourceId,
      sourceTitle: hit.sourceTitle,
      sourceRole: hit.sourceRole,
      sourceUrl: sourceRef.sourceUrl,
      localFilePath: sourceRef.localFilePath,
      v23Wave: V23_WAVE,
    });
  }

  for (const slot of V23_NEEDS_NEW_CAPTURE) {
    unsupported.push({
      targetExplorerSlot: slot,
      reason: "needs_new_source_capture",
      detail:
        slot.startsWith("loyalty.kpi.")
          ? "Numeric loyalty KPI requires approved extracted fact — not inferred from Bonvoy marketing copy."
          : "Property-level proof narrative requires hotel/PR source — consumer page is brand-level only.",
    });
  }

  return { proposed, notFound, skippedDuplicate, internalOnly, unsupported };
}

function slotsSupportedByV23Facts(v23Proposed) {
  const supported = new Set();
  for (const fact of v23Proposed) {
    for (const slot of fact.targetExplorerSlots || []) supported.add(slot);
  }
  return [...supported].sort();
}

function slotsStillLackingEvidence(v23Proposed) {
  const supported = new Set(slotsSupportedByV23Facts(v23Proposed));
  return V23_TARGET_SLOTS.filter((slot) => !supported.has(slot));
}

function assessV23BReviewPackageReady(v23Proposed, slotsLacking) {
  const v23bSlots = new Set(V23_V23B_TARGET_SLOTS);
  const proposedBySlot = new Map();
  for (const fact of v23Proposed) {
    for (const slot of fact.targetExplorerSlots || []) {
      if (!proposedBySlot.has(slot)) proposedBySlot.set(slot, []);
      proposedBySlot.get(slot).push(fact);
    }
  }
  const v23bCovered = V23_V23B_TARGET_SLOTS.filter((slot) => proposedBySlot.has(slot));
  const ready = v23bCovered.length >= 3 && v23Proposed.length >= 4;
  return {
    ready,
    v23bSlotsCovered: v23bCovered,
    v23bSlotsMissing: V23_V23B_TARGET_SLOTS.filter((slot) => !proposedBySlot.has(slot)),
    candidateFactCount: v23Proposed.length,
    slotsStillLackingEvidence: slotsLacking,
    note: ready
      ? "Enough source-backed candidate facts exist to draft a v23B human-review package (still no writer in this task)."
      : "Extend extraction / approve Pending facts before building v23B review package.",
  };
}

/* ------------------------------------------------------------------ */
/* Apply — create Pending facts only                                   */
/* ------------------------------------------------------------------ */

export async function applyTargetedFacts(proposed, { recordId = TRIBUTE_RECORD_ID, v23Wave = false } = {}) {
  const runId = `${RUN_ID_PREFIX}${v23Wave ? "-v23" : ""}-${randomUUID().slice(0, 8)}`;
  const today = new Date().toISOString().slice(0, 10);
  const created = [];
  const errors = [];

  for (const p of proposed) {
    const reg = getRegistryField(p.fieldKey, "Brand Explorer");
    const fields = {
      "Source Title": `${reg?.displayLabel || p.fieldKey} — ${runId}`,
      [MAP_PARTNER_FACT.profileType]: "Brand",
      [MAP_PARTNER_FACT.brand]: [recordId],
      [MAP_PARTNER_FACT.sourceRecord]: p.sourceId ? [p.sourceId] : [],
      [MAP_PARTNER_FACT.explorerType]: "Brand Explorer",
      [MAP_PARTNER_FACT.explorerSection]: reg?.explorerSection || "",
      [MAP_PARTNER_FACT.fieldName]: p.fieldKey,
      [MAP_PARTNER_FACT.extractedValue]: p.value,
      [MAP_PARTNER_FACT.normalizedValue]: p.value,
      [MAP_PARTNER_FACT.evidenceText]: p.evidence,
      [MAP_PARTNER_FACT.pageSectionAnchor]: p.sourceTitle || "",
      [MAP_PARTNER_FACT.sourceQuality]: p.confidenceLevel === "High" ? "High" : "Medium",
      [MAP_PARTNER_FACT.confidenceScore]: p.confidenceScore,
      [MAP_PARTNER_FACT.confidenceLevel]: p.confidenceLevel,
      [MAP_PARTNER_FACT.extractionType]: p.extractionType,
      [MAP_PARTNER_FACT.publicVisibility]: p.publicVisibility,
      [MAP_PARTNER_FACT.humanReviewStatus]: "Pending",
      [MAP_PARTNER_FACT.dataGap]: "No",
      [MAP_PARTNER_FACT.reviewerNotes]: p.aiInterpreted
        ? `${TARGETED_TAG}: AI-interpreted from company materials — human review before approval.`
        : p.v23Wave
          ? `${TARGETED_TAG} ${V23_WAVE}: source-backed candidate — Pending Review; not approved for display.`
          : `${TARGETED_TAG}: source-backed from Marriott-controlled materials.`,
      [MAP_PARTNER_FACT.followUpQuestion]: p.aiInterpreted
        ? "Confirm wording reflects company materials; not a Marriott endorsement."
        : "",
      [MAP_PARTNER_FACT.lastUpdated]: today,
      [MAP_PARTNER_FACT.extractionRunId]: runId,
    };
    try {
      const fact = await createPartnerFact(fields);
      created.push({ id: fact.id, fieldKey: p.fieldKey });
    } catch (err) {
      errors.push({ fieldKey: p.fieldKey, error: err.message || String(err) });
    }
  }

  return { runId, created, errors };
}

/* ------------------------------------------------------------------ */
/* Pipeline integration helpers                                        */
/* ------------------------------------------------------------------ */

/** True when fact was created by the targeted Tribute extraction pass. */
export function isTargetedFact(fact) {
  return nz(fact.extractionRunId).startsWith(RUN_ID_PREFIX);
}

/** Generic gap / placeholder fact from pilot-keyed extraction (not targeted, not held internal). */
export function isPlaceholderFact(fact) {
  if (!nz(fact.fieldName).startsWith("be.")) return false;
  if (isTargetedFact(fact)) return false;
  if (nz(fact.publicVisibility) === "Internal Only") return false;
  const val = nz(fact.extractedValue);
  const evidence = nz(fact.evidenceText);
  return (
    nz(fact.dataGap) === "Yes" ||
    !val ||
    val === PARTNER_INTELLIGENCE_GAP_COPY ||
    evidence === PARTNER_INTELLIGENCE_GAP_COPY ||
    nz(fact.extractionType) === "Needs Confirmation"
  );
}

/** Build fact audit shape from an in-memory fact list (pipeline live state). */
export function buildFactAuditFromList(facts) {
  const brandFacts = (facts || []).filter((f) => nz(f.fieldName).startsWith("be."));
  const dataGaps = brandFacts.filter((f) => isPlaceholderFact(f));
  const heldInternal = brandFacts.filter((f) => nz(f.publicVisibility) === "Internal Only");
  const withValue = brandFacts.filter((f) => !isPlaceholderFact(f) && nz(f.extractedValue));
  const priorTargeted = brandFacts.filter(isTargetedFact);

  return {
    total: brandFacts.length,
    dataGapCount: dataGaps.length,
    heldInternalCount: heldInternal.length,
    withValueCount: withValue.length,
    priorTargetedCount: priorTargeted.length,
    dataGapFieldKeys: dataGaps.map((f) => f.fieldName),
    withValueFieldKeys: withValue.map((f) => f.fieldName),
    priorTargetedFieldKeys: priorTargeted.map((f) => f.fieldName),
    placeholderFacts: dataGaps.map((f) => ({ id: f.id, fieldName: f.fieldName })),
    facts: brandFacts,
    _priorTargetedByField: new Set(priorTargeted.map((f) => nz(f.fieldName))),
    _valueByField: new Set(withValue.map((f) => nz(f.fieldName))),
  };
}

/**
 * True when generic extraction produced gap placeholders and targeted facts are
 * still missing — pipeline should run targeted extraction before stewardship.
 */
export function shouldUseTargetedExtraction(facts) {
  const audit = buildFactAuditFromList(facts);
  if (audit.dataGapCount === 0 && audit.priorTargetedCount > 0) return false;
  const targetRuleCount = TRIBUTE_RULES.filter((r) => !HELD_FIELD_KEYS.has(r.fieldKey)).length;
  const missingTargeted = targetRuleCount - audit.priorTargetedCount;
  const approved = (facts || []).filter(
    (f) =>
      nz(f.fieldName).startsWith("be.") &&
      (nz(f.humanReviewStatus) === "Approved" || nz(f.humanReviewStatus) === "Edited")
  );
  if (approved.length >= 3) return false;
  return audit.dataGapCount > 0 && missingTargeted > 0;
}

/**
 * Plan or apply targeted extraction using live pipeline state.
 * Dry-run: returns proposed facts only. Apply: creates Pending targeted facts.
 */
export async function runTargetedExtractionForPipeline({
  recordId = TRIBUTE_RECORD_ID,
  mode = "dry-run",
  liveSources = [],
  liveFacts = [],
} = {}) {
  const inventory = await buildSourceInventory(liveSources);
  const factAudit = buildFactAuditFromList(liveFacts);
  const { proposed, notFound, skippedDuplicate } = buildTargetedFacts(inventory, factAudit);

  let applyResult = null;
  if (mode === "apply" && proposed.length) {
    applyResult = await applyTargetedFacts(proposed, { recordId });
  }

  return {
    used: shouldUseTargetedExtraction(liveFacts) || proposed.length > 0 || factAudit.priorTargetedCount > 0,
    placeholderFactsDetected: factAudit.placeholderFacts,
    placeholderCount: factAudit.dataGapCount,
    priorTargetedCount: factAudit.priorTargetedCount,
    proposed,
    proposedCount: proposed.length,
    approvableCount: proposed.filter((p) => p.approvable).length,
    humanReviewCount: proposed.filter((p) => p.humanReview).length,
    notFound,
    skippedDuplicate,
    applyResult,
    sourceInventory: inventory.map(({ _text, ...rest }) => rest),
  };
}

/* ------------------------------------------------------------------ */
/* Report                                                              */
/* ------------------------------------------------------------------ */

export async function buildTributeTargetedExtractReport({ mode = "dry-run", recordId = TRIBUTE_RECORD_ID } = {}) {
  const sources = await loadApprovedTributeSources(recordId);
  const inventory = await buildSourceInventory(sources);
  const factAudit = await auditExistingFacts(recordId);
  const { proposed, notFound, skippedDuplicate } = buildTargetedFacts(inventory, factAudit);
  const v23 = buildV23CandidateFacts(inventory, factAudit);
  const slotsLacking = slotsStillLackingEvidence(v23.proposed);
  const v23bAssessment = assessV23BReviewPackageReady(v23.proposed, slotsLacking);

  let applyResult = null;
  let v23ApplyResult = null;
  if (mode === "apply" && proposed.length) {
    applyResult = await applyTargetedFacts(proposed, { recordId });
  }
  if (mode === "apply" && v23.proposed.length) {
    v23ApplyResult = await applyTargetedFacts(v23.proposed, { recordId, v23Wave: true });
  }

  const approvableCount = proposed.filter((p) => p.approvable).length;
  const humanReviewCount = proposed.filter((p) => p.humanReview).length;
  const v23SourceBacked = v23.proposed.filter((p) => p.category === "safe_source_backed_candidate");
  const v23InternalOnly = v23.proposed.filter((p) => p.publicVisibility === "Internal Only");

  // Governance eligibility projection: ≥3 approved publish-scope facts incl. identity + substantive.
  const identityKeys = proposed.filter((p) => p.approvable && /be\.identity\./.test(p.fieldKey)).length;
  const substantiveKeys = proposed.filter(
    (p) => p.approvable && /be\.(positioning|overview|loyalty)\./.test(p.fieldKey)
  ).length;
  const wouldEnableGovernance = approvableCount >= 3 && identityKeys >= 1 && substantiveKeys >= 1;

  return {
    targetedVersion: TARGETED_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified:
      (mode === "apply" && !!applyResult?.created?.length) || (mode === "apply" && !!v23ApplyResult?.created?.length),
    companyValidatedUntouched: true,
    brand: { name: BRAND_NAME, recordId, parentCompany: PARENT_COMPANY },
    targetedExtractionExtended: true,
    v23EvidenceReadiness: {
      wave: V23_WAVE,
      newCandidateFactsProposed: v23.proposed.length,
      sourceBackedCandidateFacts: v23SourceBacked.length,
      internalOnlyCandidateFacts: v23InternalOnly.length,
      unsupportedOrNotExtractable: v23.notFound.length + v23.unsupported.length,
      slotsSupported: slotsSupportedByV23Facts(v23.proposed),
      slotsStillLackingEvidence: slotsLacking,
      itemsNeedingNewSourceCapture: v23.unsupported,
      v23BReviewPackageCanBeBuilt: v23bAssessment.ready,
      v23BAssessment: v23bAssessment,
    },
    rootCause: {
      summary:
        "Generic brand extraction is pilot-keyed. resolveBrandExtractionContext only resolves PILOT_BRANDS; Tribute is not a pilot, so getBrandFieldHints returned null for every field and the extractor emitted registry-wide data-gap placeholders.",
      evidence: [
        "brand-extraction-context.js: resolveBrandExtractionContext → { resolved:false, pilotKey:null } for non-pilot brandId.",
        "brand-field-extraction-hints.js: getBrandFieldHints returns null when !pilotKey; BRAND_HINT_PROFILES only has kimptonHotels + curioCollection.",
        "brand-extract-rules.js: extractBrandFactsFromText pushes gapFact when tryExtractField returns null → all 24 fields became gaps.",
      ],
      notTheCause: [
        "Source text loading works: FDD ~1.3M chars, Bonvoy 13.5k, consumer 2.8k, brand page 3.6k, dev captures 3–6k.",
        "Source roles / extraction eligibility fine: 6/6 approved for Explorer + Extraction.",
        "Fact steward is correctly strict: it refused to approve gap/placeholder facts (protected governance).",
      ],
    },
    sourceInventory: inventory.map(({ _text, ...rest }) => rest),
    existingFactAudit: {
      total: factAudit.total,
      dataGapCount: factAudit.dataGapCount,
      heldInternalCount: factAudit.heldInternalCount,
      withValueCount: factAudit.withValueCount,
      priorTargetedCount: factAudit.priorTargetedCount,
      dataGapFieldKeys: factAudit.dataGapFieldKeys,
      note:
        "All non-held Pending facts are data-gap placeholders (dataGap=Yes). Leave them Pending for now; after targeted clean facts are approved, mark the placeholder rows Needs Review or Rejected in a later stewardship step (do not approve them).",
    },
    proposedFacts: proposed,
    proposedCount: proposed.length,
    approvableCount,
    humanReviewCount,
    v23CandidateFacts: v23.proposed,
    v23CandidateCount: v23.proposed.length,
    v23NotFound: v23.notFound,
    v23SkippedDuplicate: v23.skippedDuplicate,
    v23Unsupported: v23.unsupported,
    notCreated: {
      notFound,
      skippedDuplicate,
      heldFieldKeys: [...HELD_FIELD_KEYS],
      heldNote:
        "FDD economics/fees/Item 19/legal are never targeted here — they remain Internal Only / human review. be.footprint.geoIntro is not targeted (no Tribute-specific footprint statement in approved sources).",
      v23NotFound: v23.notFound,
      v23Unsupported: v23.unsupported,
    },
    supersessionPlan: {
      existingPlaceholders: factAudit.dataGapCount,
      recommendation: "leave_pending_then_needs_review",
      detail:
        `Do not delete or approve the ${factAudit.dataGapCount} existing placeholder facts (${factAudit.heldInternalCount} of which are held Internal Only). After the targeted clean facts are approved, set the matching placeholder rows to Needs Review (or Rejected) so they don't clutter publish scope. Targeted facts are tagged with extractionRunId 'tribute-targeted-*' and Reviewer Notes 'tribute-targeted' to prevent duplicate creation on rerun.`,
    },
    applyRecommended: mode === "dry-run" && (proposed.length > 0 || v23.proposed.length > 0),
    applyResult,
    v23ApplyResult,
    governanceProjection: {
      wouldEnableGovernanceAfterApproval: wouldEnableGovernance,
      approvableFacts: approvableCount,
      identityFacts: identityKeys,
      substantiveFacts: substantiveKeys,
      detail: wouldEnableGovernance
        ? "Approving the source-backed facts (identity + positioning/loyalty/development) meets the ≥3 approved + identity + substantive readiness bar → governance publish can proceed (Company Materials / AI-Assisted Profile)."
        : "Not enough approvable source-backed facts yet; strengthen sources or patterns before governance.",
    },
    nextCommand:
      mode === "dry-run" && v23.proposed.length > 0
        ? "npm run tribute-portfolio-targeted-extract -- --apply"
        : mode === "dry-run" && proposed.length > 0
          ? "npm run tribute-portfolio-targeted-extract -- --apply"
          : "npm run brand-explorer-evidence-required-slot-readiness-plan -- --brand tribute-portfolio --dry-run",
    doesNotDo: [
      "Approve facts (all created as Pending) or publish governance",
      "Create gap/placeholder facts or facts from empty source text",
      "Target FDD economics / fees / Item 19 / legal (kept Internal Only / human review)",
      "Write Brand Setup content / hero / image / logo fields",
      "Set Company Validated or Company Validation Date; imply Marriott validation",
      "Use third-party sources or change UI/scoring/BAS/OAS/OCS/Deal Readiness/schema",
    ],
  };
}

/* ------------------------------------------------------------------ */
/* Markdown                                                            */
/* ------------------------------------------------------------------ */

export function buildTributeTargetedExtractMarkdown(report) {
  const lines = [
    "# Tribute Portfolio — Targeted Source-Backed Extraction",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Brand: ${report.brand.name} \`${report.brand.recordId}\``,
    "",
    "## Root cause",
    "",
    `- ${report.rootCause.summary}`,
    "",
    "**Evidence:**",
    ...report.rootCause.evidence.map((e) => `- ${e}`),
    "",
    "**Not the cause:**",
    ...report.rootCause.notTheCause.map((e) => `- ${e}`),
    "",
    "## 1. Source inventory (readable text + key phrases)",
    "",
    "| Source | Role | Extraction? | Chars | Key phrases | Error |",
    "|--------|------|-------------|-------|-------------|-------|",
    ...report.sourceInventory.map(
      (s) =>
        `| ${nzShort(s.title)} | ${s.role} | ${s.approvedForExtraction ? "yes" : "no"} | ${s.textLength} | ${s.keyPhrasesPresent.length} | ${s.error || "—"} |`
    ),
    "",
    "## 2. Existing 24 Pending fact audit",
    "",
    `- Total brand facts: **${report.existingFactAudit.total}**`,
    `- Data-gap placeholders: **${report.existingFactAudit.dataGapCount}**`,
    `- Held Internal Only (FDD economics): **${report.existingFactAudit.heldInternalCount}**`,
    `- With real value: **${report.existingFactAudit.withValueCount}**`,
    `- Prior targeted facts: **${report.existingFactAudit.priorTargetedCount}**`,
    `- ${report.existingFactAudit.note}`,
    "",
    "## 3. Proposed targeted facts (source-backed)",
    "",
    `- Proposed (v1 rules): **${report.proposedCount}** · approvable: **${report.approvableCount}** · human-review (AI-interpreted): **${report.humanReviewCount}**`,
    "",
    "| Field | Type | Conf | Approvable | Source | Value |",
    "|-------|------|------|------------|--------|-------|",
    ...report.proposedFacts.map(
      (p) =>
        `| \`${p.fieldKey}\` | ${p.extractionType} | ${p.confidenceLevel} | ${p.approvable ? "yes" : "review"} | ${p.sourceRole} | ${nzShort(p.value, 70)} |`
    ),
    "",
    "## 3b. v23 evidence-readiness candidate facts",
    "",
    `- New candidates: **${report.v23EvidenceReadiness?.newCandidateFactsProposed ?? report.v23CandidateCount ?? 0}**`,
    `- Source-backed: **${report.v23EvidenceReadiness?.sourceBackedCandidateFacts ?? 0}** · internal-only: **${report.v23EvidenceReadiness?.internalOnlyCandidateFacts ?? 0}**`,
    `- Slots supported: ${(report.v23EvidenceReadiness?.slotsSupported || []).map((s) => `\`${s}\``).join(", ") || "none"}`,
    `- Slots still lacking evidence: ${(report.v23EvidenceReadiness?.slotsStillLackingEvidence || []).map((s) => `\`${s}\``).join(", ") || "none"}`,
    `- v23B review package can be built: **${report.v23EvidenceReadiness?.v23BReviewPackageCanBeBuilt ? "yes" : "no"}**`,
    "",
    "| Field | Slots | Eligibility | Source | Value |",
    "|-------|-------|-------------|--------|-------|",
    ...(report.v23CandidateFacts || []).map(
      (p) =>
        `| \`${p.fieldKey}\` | ${(p.targetExplorerSlots || []).join(", ")} | ${p.displayEligibility} | ${p.sourceRole} | ${nzShort(p.value, 60)} |`
    ),
    "",
    "### v23 evidence excerpts",
    "",
    ...(report.v23CandidateFacts || []).map((p) => `- \`${p.fieldKey}\`: “${nzShort(p.evidence, 180)}”`),
    "",
    "### v1 evidence excerpts",
    "",
    ...report.proposedFacts.map((p) => `- \`${p.fieldKey}\`: “${nzShort(p.evidence, 180)}”`),
    "",
    "## 4. Facts held / not created",
    "",
    `- Held field keys (Internal Only / human review, never targeted): ${[...report.notCreated.heldFieldKeys].map((k) => `\`${k}\``).join(", ")}`,
    `- Not found in approved sources: ${report.notCreated.notFound.map((n) => `\`${n.fieldKey}\``).join(", ") || "none"}`,
    `- Skipped (duplicate/existing): ${report.notCreated.skippedDuplicate.map((n) => `\`${n.fieldKey}\` (${n.reason})`).join(", ") || "none"}`,
    `- ${report.notCreated.heldNote}`,
    "",
    "## 5. Duplicate / supersession handling",
    "",
    `- Existing placeholders: ${report.supersessionPlan.existingPlaceholders}`,
    `- Recommendation: **${report.supersessionPlan.recommendation}**`,
    `- ${report.supersessionPlan.detail}`,
    "",
    "## 6. Governance projection",
    "",
    `- Would enable governance after approval: **${report.governanceProjection.wouldEnableGovernanceAfterApproval ? "yes" : "no"}**`,
    `- Approvable: ${report.governanceProjection.approvableFacts} (identity ${report.governanceProjection.identityFacts}, substantive ${report.governanceProjection.substantiveFacts})`,
    `- ${report.governanceProjection.detail}`,
    "",
    "## 7. Apply",
    "",
    `- Apply recommended: **${report.applyRecommended ? "yes" : "no"}**`,
    report.applyResult
      ? `- Applied: created ${report.applyResult.created.length} Pending facts (run ${report.applyResult.runId})${report.applyResult.errors.length ? `, ${report.applyResult.errors.length} errors` : ""}`
      : "- No facts created (dry-run).",
    "",
    "## 8. Exact next command",
    "",
    "```bash",
    report.nextCommand,
    "```",
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
    "",
  ];
  return lines.join("\n");
}

function nzShort(v, n = 40) {
  const s = String(v == null ? "" : v).replace(/\s+/g, " ").trim();
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
