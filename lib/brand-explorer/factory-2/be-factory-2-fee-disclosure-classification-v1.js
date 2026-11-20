/**
 * Franchise / affiliation fee disclosure classification (portfolio closure).
 *
 * Distinguishes research gaps from genuinely undisclosed or non-franchise models.
 * Never invents fee amounts.
 *
 * Contract: BE_FACTORY_2_0_FEE_DISCLOSURE_CLASSIFICATION_V1
 */
export const BE_FACTORY_2_0_FEE_DISCLOSURE_CLASSIFICATION_V1 =
  "BE_FACTORY_2_0_FEE_DISCLOSURE_CLASSIFICATION_V1";

export const FEE_DISCLOSURE_CLASS = Object.freeze({
  A_CURRENT_AUTHORITATIVE: "A_CURRENT_AUTHORITATIVE_AVAILABLE",
  B_HISTORICAL_ONLY: "B_HISTORICAL_AVAILABLE_CURRENT_UNVERIFIED",
  C_NOT_PUBLICLY_DISCLOSED: "C_NOT_PUBLICLY_DISCLOSED",
  D_NON_CONVENTIONAL_MODEL: "D_MANAGED_MEMBERSHIP_OR_NON_FRANCHISE",
  E_MATERIAL_FEE_MISSING: "E_MATERIAL_FEE_MISSING_DESPITE_EVIDENCE",
  UNKNOWN: "UNKNOWN_NEEDS_REVIEW",
});

function nz(s) {
  return s == null ? "" : String(s).trim();
}

/**
 * Parent / model hints for reusable classification (not fee amounts).
 * Expand as verified source registry grows.
 */
export const FEE_SOURCE_REGISTRY_V1 = Object.freeze({
  marriott: {
    parent: "Marriott International",
    typicalModel: "franchise_or_management",
    publicFddLikely: true,
    sourceNote: "U.S. FDD / franchise disclosure where applicable; confirm current exhibit.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  hilton: {
    parent: "Hilton",
    typicalModel: "franchise_or_management",
    publicFddLikely: true,
    sourceNote: "U.S. FDD / franchise disclosure where applicable.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  hyatt: {
    parent: "Hyatt",
    typicalModel: "franchise_or_management",
    publicFddLikely: true,
    sourceNote: "U.S. FDD / franchise disclosure where applicable.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  ihg: {
    parent: "IHG",
    typicalModel: "franchise_or_management",
    publicFddLikely: true,
    sourceNote: "U.S. FDD / franchise disclosure where applicable.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  accor: {
    parent: "Accor",
    typicalModel: "franchise_management_or_lease",
    publicFddLikely: false,
    sourceNote: "Often management/franchise by market; public U.S.-style FDD not always available.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  wyndham: {
    parent: "Wyndham",
    typicalModel: "franchise",
    publicFddLikely: true,
    sourceNote: "U.S. FDD commonly available for Wyndham franchise brands.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  choice: {
    parent: "Choice Hotels",
    typicalModel: "franchise",
    publicFddLikely: true,
    sourceNote: "U.S. FDD commonly available for Choice franchise brands.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  "best western": {
    parent: "BWH Hotels / Best Western",
    typicalModel: "membership",
    publicFddLikely: false,
    sourceNote: "Best Western / SureStay often membership rather than classic royalty franchise.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL,
  },
  sonesta: {
    parent: "Sonesta",
    typicalModel: "franchise_or_management",
    publicFddLikely: true,
    sourceNote: "Confirm current Sonesta franchise / management exhibits.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  preferred: {
    parent: "Preferred Hotels & Resorts",
    typicalModel: "membership_network",
    publicFddLikely: false,
    sourceNote: "Soft-brand / membership network — not a conventional franchise fee schedule.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL,
  },
  "small luxury": {
    parent: "SLH",
    typicalModel: "membership_network",
    publicFddLikely: false,
    sourceNote: "Membership network — not a conventional franchise fee schedule.",
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL,
  },
  melia: {
    parent: "Meliá",
    typicalModel: "management_or_franchise",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  barcelo: {
    parent: "Barceló",
    typicalModel: "management_or_franchise",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  minor: {
    parent: "Minor Hotels",
    typicalModel: "management_or_franchise",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  rotana: {
    parent: "Rotana",
    typicalModel: "management",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL,
  },
  dusit: {
    parent: "Dusit",
    typicalModel: "management_or_franchise",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  shangri: {
    parent: "Shangri-La",
    typicalModel: "management",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL,
  },
  ascott: {
    parent: "Ascott / CapitaLand",
    typicalModel: "management_or_franchise",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED,
  },
  "pan pacific": {
    parent: "Pan Pacific Hotels Group",
    typicalModel: "management",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL,
  },
  gha: {
    parent: "GHA alliance brands",
    typicalModel: "alliance_or_management",
    publicFddLikely: false,
    defaultClassWhenUnverified: FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL,
  },
});

const MEMBERSHIP_BRAND_RE =
  /surestay|best western|ascend|preferred|small luxury|slh|worldhotels|design hotels|bunkhouse/i;

const MANAGED_HINT_RE =
  /park hyatt|grand hyatt|andaz|waldorf|ritz.?carlton|st\.?\s*regis|edition|four seasons|mandarin|shangri|raffles|fairmont/i;

function resolveParentKey({ parentCompany, brandName, loyaltyProgram } = {}) {
  const blob = `${nz(parentCompany)} ${nz(brandName)} ${nz(loyaltyProgram)}`.toLowerCase();
  const keys = Object.keys(FEE_SOURCE_REGISTRY_V1).sort((a, b) => b.length - a.length);
  for (const k of keys) {
    if (blob.includes(k)) return k;
  }
  return null;
}

/**
 * Classify fee disclosure posture for a brand candidate.
 * Does NOT invent percentages. Uses body language + parent registry.
 */
export function classifyFeeDisclosure(input = {}) {
  const blocks = input.blocks || [];
  const feeBlocks = blocks.filter((b) =>
    /^economics\.fee/i.test(String(b.slotKey || b.slot_key || ""))
  );
  const blob = feeBlocks.map((b) => `${b.title || ""}\n${b.body || ""}`).join("\n");
  const parentKey = resolveParentKey(input);
  const reg = parentKey ? FEE_SOURCE_REGISTRY_V1[parentKey] : null;

  const hasNumericPct = /\b\d{1,2}(\.\d+)?\s*%/.test(blob);
  const notDisclosed =
    /not (publicly )?disclosed|confirm current .* (fee|disclosure|exhibit)|amounts are not staged|do not invent|pending verified disclosure/i.test(
      blob
    );
  const membership =
    MEMBERSHIP_BRAND_RE.test(`${input.brandName || ""} ${input.parentCompany || ""}`) ||
    reg?.typicalModel === "membership" ||
    reg?.typicalModel === "membership_network";
  const managed =
    MANAGED_HINT_RE.test(nz(input.brandName)) ||
    /management agreement|principally managed/i.test(blob) ||
    reg?.typicalModel === "management";

  let classification = FEE_DISCLOSURE_CLASS.UNKNOWN;
  let ownerFacingTreatment = "Confirm current fee / affiliation exhibits before IC.";
  let isResearchGap = true;
  let certifiableWithoutAmounts = false;

  if (hasNumericPct && !/rumor|illustrative only|peer rumor/i.test(blob)) {
    classification = FEE_DISCLOSURE_CLASS.A_CURRENT_AUTHORITATIVE;
    ownerFacingTreatment =
      "Numeric fee figures present — must be tied to a cited current disclosure exhibit before QUALITY_CERTIFIED.";
    isResearchGap = false;
    certifiableWithoutAmounts = false;
  } else if (membership || managed || reg?.typicalModel === "alliance_or_management") {
    classification = FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL;
    ownerFacingTreatment = membership
      ? "Membership / soft-brand network — conventional public franchise royalty schedule may not apply. Confirm affiliation / membership fee exhibit."
      : "Principally managed or non-standard affiliation model — confirm management / affiliation economics; do not invent franchise royalty percentages.";
    isResearchGap = false;
    certifiableWithoutAmounts = true;
  } else if (notDisclosed || feeBlocks.length > 0) {
    classification =
      reg?.defaultClassWhenUnverified || FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED;
    if (classification === FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL) {
      isResearchGap = false;
      certifiableWithoutAmounts = true;
      ownerFacingTreatment =
        reg?.sourceNote ||
        "Non-conventional fee model — Not Publicly Disclosed as a franchise schedule is an accurate posture when verified.";
    } else {
      // C: genuine not publicly disclosed — NOT the same as unfinished research when FDD unavailable
      isResearchGap = Boolean(reg?.publicFddLikely);
      certifiableWithoutAmounts = !reg?.publicFddLikely;
      ownerFacingTreatment =
        "Not Publicly Disclosed — confirm current franchise / development exhibit privately with brand development. Do not underwrite invented percentages.";
    }
  }

  return {
    contract: BE_FACTORY_2_0_FEE_DISCLOSURE_CLASSIFICATION_V1,
    classification,
    parentKey,
    parentRegistry: reg
      ? {
          parent: reg.parent,
          typicalModel: reg.typicalModel,
          publicFddLikely: reg.publicFddLikely,
          sourceNote: reg.sourceNote,
        }
      : null,
    feeBlockCount: feeBlocks.length,
    hasNumericPct,
    isResearchGap,
    certifiableWithoutAmounts,
    ownerFacingTreatment,
    // E reserved when we later attach exhibits missing a known mandatory line item
    materialFeeMissingDespiteEvidence: classification === FEE_DISCLOSURE_CLASS.E_MATERIAL_FEE_MISSING,
  };
}

export function summarizeFeeClassifications(rows) {
  const byClass = {};
  for (const r of rows || []) {
    const c = r.classification || FEE_DISCLOSURE_CLASS.UNKNOWN;
    byClass[c] = (byClass[c] || 0) + 1;
  }
  return {
    total: (rows || []).length,
    byClass,
    researchGaps: (rows || []).filter((r) => r.isResearchGap).length,
    certifiableWithoutAmounts: (rows || []).filter((r) => r.certifiableWithoutAmounts).length,
  };
}
