/**
 * Brand Explorer v36B — renderer-derived Full Tab Content Contract (read-only).
 *
 * Derived from public/js/brand-explorer-atelier-from-api.js + api/brand-library.js,
 * cross-checked against docs/brand-explorer-presentation-slots.md.
 */
export const FULL_TAB_CONTRACT_VERSION = "v36B";

export const ATELIER_RENDERER_FILE = "public/js/brand-explorer-atelier-from-api.js";
export const API_NORMALIZER = "api/brand-library.js normalizeBrandExplorerPresentationRecords";

export const HARDCODED_FALLBACK_SURFACES = Object.freeze([
  {
    id: "commercial_static_demand",
    tab: "Commercial Engine",
    renderer: "renderCommercialEngine → COMM_STATIC",
    detail: "Static demand matrix when commercial.demand slots empty",
  },
  {
    id: "loyalty_demand_matrix",
    tab: "Loyalty Program",
    renderer: "renderLoyaltyProgram → LOY_DEMAND",
    detail: "Hardcoded loyalty demand pairs when loyalty slots thin",
  },
  {
    id: "overview_scenario_defaults",
    tab: "Overview",
    renderer: "renderAtelierOverview scenario strip",
    detail: "Urban/Boutique/Resort default titles when overview.scenario.* missing",
  },
  {
    id: "overview_proof_fallbacks",
    tab: "Overview",
    renderer: "renderAtelierOverview proof grid",
    detail: "ATELIER_PROOF_FALLBACK_HEADS when overview.proof.* empty",
  },
  {
    id: "standards_owner_fallback",
    tab: "Owner Considerations",
    renderer: "renderStandardsOwnerConsiderations",
    detail: "Generic standards copy when standards.* slots empty",
  },
  {
    id: "economics_fee_templates",
    tab: "Economics & Obligations",
    renderer: "renderAtelierEconomicsObligations",
    detail: "FDD-oriented fee bucket language for affiliation brands when economics slots thin",
  },
]);

/** Slot URL display policy per family prefix. */
export const SLOT_URL_POLICY = Object.freeze({
  "footprint.openings": "trailing_property_url_allowed",
  "footprint.momentum": "announcement_url_allowed",
  default: "strip_http_urls",
});

const SLOT_DEFINITIONS = [
  {
    prefix: "overview.",
    tab: "Overview",
    renderer: "renderAtelierOverview",
    renders: true,
    documented: true,
    repeatable: "mixed",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body", "Image→imageUrl (scenario/proof)", "Sort Order", "Active"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "scenario/proof use hardcoded defaults when slots missing",
    docMismatch: "overview.proof.1–6 rendered but under-documented in slots doc",
  },
  {
    prefix: "overview.scenario.",
    tab: "Overview",
    renderer: "renderAtelierOverview scenario strip",
    renders: true,
    documented: true,
    repeatable: true,
    activeProfileRequired: true,
    expectedFields: ["Title", "Body", "Image→imageUrl", "Sort Order", "Active"],
    imageRequired: true,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "ATELIER_SCENARIO_FALLBACK_TITLES when no API blocks",
    docMismatch: null,
  },
  {
    prefix: "overview.proof.",
    tab: "Overview",
    renderer: "renderAtelierOverview proof grid",
    renders: true,
    documented: false,
    repeatable: true,
    activeProfileRequired: false,
    expectedFields: ["Title", "Body", "Sort Order", "Active"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "ATELIER_PROOF_FALLBACK_HEADS when slots empty",
    docMismatch: "Rendered in code; docs only mention overview.proof_operator",
    proofCompanyValidatedNote: "Proof labels may show brand-specific text; Company Validated does not gate proof grid render",
  },
  {
    prefix: "overview.differentiators.",
    tab: "Overview",
    renderer: "renderAtelierOverview Key Differentiators",
    renders: true,
    documented: true,
    repeatable: false,
    activeProfileRequired: true,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "section hidden when both identity/commercial slots empty",
    docMismatch: null,
  },
  {
    prefix: "overview.bestAt.",
    tab: "Overview",
    renderer: "renderAtelierOverview Best At cards",
    renders: true,
    documented: true,
    repeatable: true,
    activeProfileRequired: false,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "cards omitted when no rows",
    docMismatch: null,
  },
  {
    prefix: "valueOwners.",
    tab: "Value to Owners",
    renderer: "renderValueToOwners",
    renders: true,
    documented: true,
    repeatable: "mixed",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "watchouts section may render empty list",
    docMismatch: null,
  },
  {
    prefix: "operations.",
    tab: "Operating Model",
    renderer: "renderOperationsStandards",
    renders: true,
    documented: true,
    repeatable: "mixed",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "operations.model.* has no Brand Setup fallback",
    docMismatch: null,
  },
  {
    prefix: "standards.",
    tab: "Owner Considerations",
    renderer: "renderStandardsOwnerConsiderations",
    renders: true,
    documented: false,
    repeatable: "standards.requirement multi-row",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "generic standards fallback paragraph when slots empty",
    docMismatch: "Rendered in code but under-documented in presentation-slots.md",
  },
  {
    prefix: "commercial.",
    tab: "Commercial Engine",
    renderer: "renderCommercialEngine",
    renders: true,
    documented: true,
    repeatable: "mixed",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "COMM_STATIC demand matrix when commercial.demand empty",
    docMismatch: null,
  },
  {
    prefix: "economics.",
    tab: "Economics & Obligations",
    renderer: "renderAtelierEconomicsObligations",
    renders: true,
    documented: "partial",
    repeatable: "mixed",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "fee bucket templates when economics.metric/checklist thin",
    docMismatch: "economics.checklist/diligence/opening.financials documented but NOT wired in UI",
  },
  {
    prefix: "loyalty.",
    tab: "Loyalty Program",
    renderer: "renderLoyaltyProgram",
    renders: true,
    documented: true,
    repeatable: "mixed",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "LOY_DEMAND hardcoded matrix; KPI strip may be empty",
    docMismatch: null,
  },
  {
    prefix: "footprint.",
    tab: "Footprint & Growth",
    renderer: "renderFootprintGrowth + renderMomentumSection",
    renders: true,
    documented: true,
    repeatable: "openings/momentum/mix multi-row",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body", "Case Summary columns (openings)", "Image→imageUrl (openings)"],
    imageRequired: "footprint.openings",
    modalRequired: "footprint.openings",
    urlPolicy: "openings/momentum exception",
    fallbackBehavior: "4-paragraph body parser drops scenario/meta/teaser",
    docMismatch: null,
  },
  {
    prefix: "materials.gallery.",
    tab: "Brand Materials",
    renderer: "renderBrandMaterials gallery tiles",
    renders: true,
    documented: true,
    repeatable: true,
    activeProfileRequired: true,
    expectedFields: ["Title", "Image→imageUrl", "Sort Order", "Active"],
    imageRequired: true,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "galleryDefaultLabels caption when title empty",
    docMismatch: null,
  },
  {
    prefix: "materials.file",
    tab: "Brand Materials",
    renderer: "renderBrandMaterials file cards",
    renders: true,
    documented: true,
    repeatable: true,
    activeProfileRequired: false,
    expectedFields: ["Title", "Body", "Summary URL"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "summary_url_field_internal",
    fallbackBehavior: "cards omitted when no rows",
    docMismatch: null,
  },
  {
    prefix: "materials.caseStudy",
    tab: "Brand Materials",
    renderer: "modal JS only — cards NOT rendered",
    renders: false,
    documented: true,
    repeatable: true,
    activeProfileRequired: false,
    expectedFields: ["Title", "Body", "Case Summary columns"],
    imageRequired: false,
    modalRequired: true,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "data can exist with zero visible UI",
    docMismatch: "Documented and parsed in modal JS but not rendered in cards",
  },
  {
    prefix: "insight.",
    tab: "Dealality Insight",
    renderer: "renderDealalityInsight",
    renders: true,
    documented: true,
    repeatable: "insight.similar multi-row",
    activeProfileRequired: true,
    expectedFields: ["Title", "Body"],
    imageRequired: false,
    modalRequired: false,
    urlPolicy: "strip_http_urls",
    fallbackBehavior: "similar brands list empty when no rows",
    docMismatch: "dealalityInsight.* alias uses insight.* prefix in code",
  },
];

const DOCUMENTED_UNRENDERED = ["materials.caseStudy"];
const RENDERED_UNDERDOCUMENTED = [
  "overview.proof.1–6",
  "standards.*",
  "economics.checklist",
  "economics.diligence",
  "economics.opening.financials",
];

function familyForSlotKey(slotKey) {
  const key = String(slotKey || "");
  for (const def of SLOT_DEFINITIONS) {
    if (key === def.prefix.replace(/\.$/, "") || key.startsWith(def.prefix)) return def;
  }
  if (key.startsWith("dealalityInsight.")) {
    return SLOT_DEFINITIONS.find((d) => d.prefix === "insight.");
  }
  return null;
}

export function buildFullTabContentContract() {
  const families = SLOT_DEFINITIONS.map((def) => ({
    ...def,
    slotKeyPattern: `${def.prefix}*`,
  }));

  return {
    contractVersion: FULL_TAB_CONTRACT_VERSION,
    derivedFrom: [ATELIER_RENDERER_FILE, API_NORMALIZER, "docs/brand-explorer-presentation-slots.md"],
    families,
    hardcodedFallbackSurfaces: HARDCODED_FALLBACK_SURFACES,
    documentedButNotRendered: DOCUMENTED_UNRENDERED,
    renderedButUnderdocumented: RENDERED_UNDERDOCUMENTED,
    slotUrlPolicy: SLOT_URL_POLICY,
    summary: {
      familyCount: families.length,
      documentedMismatchCount: families.filter((f) => f.docMismatch).length,
      hardcodedFallbackCount: HARDCODED_FALLBACK_SURFACES.length,
    },
  };
}

export function classifySlotKey(slotKey) {
  const family = familyForSlotKey(slotKey);
  if (!family) {
    return {
      slotKey,
      tab: "Unknown",
      renders: false,
      documented: false,
      family: null,
    };
  }
  return {
    slotKey,
    tab: family.tab,
    renders: family.renders !== false,
    documented: family.documented !== false,
    family: family.prefix,
    activeProfileRequired: family.activeProfileRequired,
    urlPolicy: family.urlPolicy,
    fallbackBehavior: family.fallbackBehavior,
    docMismatch: family.docMismatch,
  };
}

export function auditPresentationRowsAgainstContract(presentationRows = [], apiBlocks = []) {
  const contract = buildFullTabContentContract();
  const rows = presentationRows || [];
  const blocks = apiBlocks || [];
  const bySlot = new Map();

  for (const row of rows) {
    const cls = classifySlotKey(row.slotKey);
    bySlot.set(row.slotKey, {
      ...cls,
      recordId: row.recordId,
      visible: row.visible !== false,
      hasTitle: Boolean(String(row.title || "").trim()),
      hasBody: Boolean(String(row.body || "").trim()),
      hasImageUrl: Boolean(String(row.imageUrl || "").trim()),
      inApi: blocks.some((b) => b.slotKey === row.slotKey),
      apiImageUrl: blocks.find((b) => b.slotKey === row.slotKey)?.imageUrl || "",
    });
  }

  const undocumentedRendered = [];
  const documentedUnrendered = [];
  for (const [slotKey, meta] of bySlot) {
    if (meta.docMismatch && meta.renders) undocumentedRendered.push(slotKey);
    if (DOCUMENTED_UNRENDERED.some((p) => slotKey.startsWith(p.replace(/\.$/, "")))) {
      documentedUnrendered.push(slotKey);
    }
  }

  return {
    contractVersion: contract.contractVersion,
    rowCount: rows.length,
    visibleRowCount: rows.filter((r) => r.visible !== false).length,
    slotAudit: [...bySlot.values()],
    documentedButNotRenderedRows: documentedUnrendered,
    renderedUnderdocumentedSlots: undocumentedRendered,
    contract,
  };
}
