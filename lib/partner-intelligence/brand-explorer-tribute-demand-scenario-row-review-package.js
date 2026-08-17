/**
 * Brand Explorer Tribute Demand Scenario Row Review Package v25C-5A.
 *
 * Founder-review package with polished demand-scenario drafts for Tribute Portfolio.
 * Read-only — no Airtable writes.
 *
 * @see docs/data-intelligence/brand-explorer-tribute-demand-scenario-row-review-package-v25C-5A.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { listPartnerSources } from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";

export const PACKAGE_VERSION = "25C-5A";
export const REPORT_JSON_NAME = "brand-explorer-tribute-demand-scenario-row-review-package.json";
export const REPORT_MD_NAME = "brand-explorer-tribute-demand-scenario-row-review-package.md";
export const DOC_MD_NAME = "brand-explorer-tribute-demand-scenario-row-review-package-v25C-5A.md";

export const NEXT_WRITER = "brand-explorer-tribute-demand-scenario-row-creation-writer";
export const NEXT_WRITER_VERSION = "25C-5B";

export const DEMAND_SLOT = "commercial.demand";
export const DEMAND_MINIMUM = 3;
export const DEMAND_TARGET = 6;

export const CONSUMER_SITE_URL = "https://tribute-portfolio.marriott.com/";
export const CONSUMER_SOURCE_ID = "recF0qS9JIZjM3qza";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const GOVERNANCE_LABELS = [
  "AI-drafted from official-source metadata",
  "Pending founder review",
  "Not company-validated",
  "Not Marriott-validated",
];

const REFERENCE_BRANDS = [
  { name: "Curio Collection by Hilton", id: "receQkxgjlezsc1xg" },
  { name: "Kimpton Hotels", id: "recCKuXCmGvxHPfb3" },
  { name: "Radisson Blu by Choice", id: "recWPEvxBQxVVzSq3" },
  { name: "Ascend Hotel Collection", id: "reclkgOzvAcBheUSo" },
];

/** Reference-brand demand titles — block exact copies (structure/density reference only). */
const FORBIDDEN_REFERENCE_TITLES = new Set([
  "Gateway Urban",
  "Regional & Secondary Upscale",
  "Corporate-Led Urban",
  "Resort / Coastal Leisure",
  "Conversion / Repositioning",
  "Pure Economy / Highway",
  "Independent / Soft-Brand Conversion",
  "Heritage & Experiential Repositioning",
]);

const FORBIDDEN_MARRIOTT_VALIDATION = /marriott\s+validated|validated\s+by\s+marriott|company-validated\s+demand/i;
const UNSUPPORTED_STATISTICS =
  /\b\d{1,3}(?:\.\d+)?%\b|\b\d[\d,]*\s*(?:million|billion|thousand|m\+|k\+)\b|\$\s?\d[\d,]+/i;
const GENERIC_FILLER =
  /^(strong|moderate|not a fit|tbd|n\/a|lorem|placeholder)$/i;

/** Tribute-specific demand scenarios — not copied from reference-brand titles. */
export const POLISHED_DEMAND_SCENARIO_ROWS = [
  {
    themeKey: "urban_lifestyle_conversion",
    title: "Urban Lifestyle Conversion",
    body: "Strong",
    sort: 0,
    demandScenarioDescription:
      "Independent urban boutique or lifestyle hotel converting into Tribute Portfolio while preserving local character, design narrative, and guest-facing identity—adding Marriott commercial systems and Bonvoy participation rather than a prototype-led reflag.",
    ownerImplication:
      "Underwrite conversion PIP scope, design latitude within collection standards, and whether urban ADR and operating complexity support a lifestyle collection affiliation—not select-service economics.",
    demandLogic:
      "Consumer-site urban examples (e.g., Hotel Rumbao in Old San Juan, Humano Lima on the Malecón) show Tribute anchoring city lifestyle assets; development captures describe an operating model for independent hotels seeking Marriott affiliation.",
    sourceBasis:
      "Tribute consumer site (recF0qS9JIZjM3qza) property map + Marriott development capture (recSLu3N7s84rIKS6); property URLs SJUTX, LIMTX as illustrative examples—not performance claims.",
    sourceIds: ["recF0qS9JIZjM3qza", "recSLu3N7s84rIKS6"],
    propertyExamples: ["Hotel Rumbao, a Tribute Portfolio Hotel (SJUTX)", "Humano, Lima, a Tribute Portfolio Hotel (LIMTX)"],
    riskLevel: "low",
    updatesExistingRow: false,
  },
  {
    themeKey: "resort_leisure_repositioning",
    title: "Resort / Leisure Repositioning",
    body: "Moderate–strong",
    sort: 1,
    demandScenarioDescription:
      "Resort or leisure-led independent asset repositioning under Tribute—Caribbean, Riviera Maya, or other leisure markets where owners want collection affiliation with resort-scale guest experience and Marriott network access.",
    ownerImplication:
      "Model seasonal demand, resort operating complexity, all-inclusive or F&B scope, and PIP investment before assuming Bonvoy distribution lift clears franchise and loyalty fees.",
    demandLogic:
      "Consumer-site resort listings (Crystal Cove Barbados, future Casa Nizuc listing) illustrate leisure-footprint use cases within the collection; directional only—no occupancy or RevPAR claims.",
    sourceBasis:
      "Tribute consumer site (recF0qS9JIZjM3qza) + property URLs BGITY, CUNAN; aligns with existing partial row title 'Resort & leisure conversion' pending owner-implication backfill.",
    sourceIds: ["recF0qS9JIZjM3qza"],
    propertyExamples: [
      "Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort (BGITY)",
      "Casa Nizuc, a Tribute Portfolio Resort (CUNAN — future listing example)",
    ],
    riskLevel: "low",
    updatesExistingRow: true,
    existingRowTitle: "Resort & leisure conversion",
  },
  {
    themeKey: "independent_marriott_distribution",
    title: "Independent Hotel Seeking Marriott Distribution",
    body: "Strong",
    sort: 2,
    demandScenarioDescription:
      "Owner-operated independent hotel that wants Bonvoy participation, global distribution, and Marriott sales/commercial support while keeping an independent sense of place and local programming latitude.",
    ownerImplication:
      "Compare fee stack, loyalty contribution assumptions, and retained design/programming flexibility against harder reflag options—Tribute fits when independence and character are strategic, not when a prototype select-service reflag is the goal.",
    demandLogic:
      "Brand page capture emphasizes 'Stay independent' and unique personalities; development sources frame Tribute as an operating model for independent hotels with lender-confidence context—founder-reviewed directional use case, not a performance guarantee.",
    sourceBasis:
      "Marriott brand page capture (recNvITV5HzuQburM) + development portfolio capture (recSLu3N7s84rIKS6); Bonvoy page (recBonvoy secondary context only).",
    sourceIds: ["recNvITV5HzuQburM", "recSLu3N7s84rIKS6"],
    propertyExamples: [],
    riskLevel: "low",
    updatesExistingRow: false,
  },
  {
    themeKey: "mixed_use_destination",
    title: "Mixed-Use Or Destination-Led Hospitality",
    body: "Moderate–strong",
    sort: 3,
    demandScenarioDescription:
      "Hospitality component within mixed-use or destination-led development where Tribute adds lifestyle collection positioning and Marriott ecosystem access without forcing a standardized chain prototype.",
    ownerImplication:
      "Validate that destination draw, non-room revenue, and guest-experience investment support collection standards—weak fit when the asset is primarily economy transient or highway-oriented.",
    demandLogic:
      "Development captures reference independent-hotel operating model and lender-confidence framing; approved positioning treats conversion/repositioning as a core Tribute use case when local character and experience investment are central to the deal thesis.",
    sourceBasis:
      "Marriott development home + portfolio captures (recZmeduOoM1PZEpT, recSLu3N7s84rIKS6) + founder-reviewed positioning from required-section source-capture package.",
    sourceIds: ["recZmeduOoM1PZEpT", "recSLu3N7s84rIKS6"],
    propertyExamples: [],
    riskLevel: "medium",
    updatesExistingRow: false,
  },
  {
    themeKey: "secondary_market_lifestyle",
    title: "Secondary Market Lifestyle Upgrade",
    body: "Moderate–strong",
    sort: 4,
    demandScenarioDescription:
      "Secondary or regional-market independent asset upgrading to lifestyle collection affiliation when local ADR depth and demand profile support design, F&B, and service investment through conversion and QA cycles.",
    ownerImplication:
      "Stress-test ADR ceiling, staffing model, and market demand depth—Tribute is a lifestyle/independent-character play, not an economy or limited-service highway reflag.",
    demandLogic:
      "Approved positioning emphasizes boutique, lifestyle, and resort assets where local character supports premium conversion; weaker where market ADR cannot support full-service or resort operating complexity.",
    sourceBasis:
      "Founder-reviewed AI draft from brand-explorer-required-section-source-capture-package + consumer/development source set; no market-statistics claims.",
    sourceIds: ["recF0qS9JIZjM3qza", "recSLu3N7s84rIKS6"],
    propertyExamples: [],
    riskLevel: "medium",
    updatesExistingRow: false,
  },
  {
    themeKey: "operator_led_reflag",
    title: "Operator-Led Reflag Or Repositioning",
    body: "Strong",
    sort: 5,
    demandScenarioDescription:
      "Third-party operator-led conversion or reflag where the owner wants Marriott soft-collection affiliation, with the operator executing PIP, opening support, and ongoing QA rhythm under collection standards.",
    ownerImplication:
      "Align operator capability with collection design, F&B, and service expectations; confirm franchise and loyalty economics in the pro forma after fees—not just brand logo lift.",
    demandLogic:
      "FDD and development sources describe franchise operating model context; Tribute's collection posture suits operator-led repositioning when independent identity and Bonvoy access are both deal requirements.",
    sourceBasis:
      "2026 Tribute FDD capture (recjVfKnl9q18MO5w — secondary factual) + development captures; directional franchise-model context only.",
    sourceIds: ["recjVfKnl9q18MO5w", "recSLu3N7s84rIKS6"],
    propertyExamples: [],
    riskLevel: "low",
    updatesExistingRow: false,
  },
  {
    themeKey: "economy_limited_service",
    title: "Economy / Limited-Service Reflag",
    body: "Not a fit",
    sort: 6,
    demandScenarioDescription:
      "Prototype select-service, economy, or highway-oriented limited-service reflag seeking lowest-cost affiliation—outside Tribute's independent lifestyle and resort collection positioning.",
    ownerImplication:
      "Route to Marriott select-service or limited-service flags if economics drive the decision; do not force Tribute when operating model, design scope, and guest experience cannot support collection standards.",
    demandLogic:
      "Approved positioning explicitly contrasts Tribute with prototype-led select-service reflags; collection fit requires design, F&B, and experience investment—not economy transient posture.",
    sourceBasis:
      "Founder-reviewed positioning from display-content-completion + required-section source-capture package; negative scenario for owner clarity (parity with reference brands' 'not a fit' row).",
    sourceIds: ["recF0qS9JIZjM3qza"],
    propertyExamples: [],
    riskLevel: "low",
    updatesExistingRow: false,
  },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.md",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "reports/brand-explorer-required-section-source-capture-package.json",
  "reports/tribute-portfolio-targeted-extract.md",
  "reports/tribute-portfolio-targeted-extract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Brand Explorer Presentation rows (API)",
  "live Tribute Partner Facts (API)",
  "live Tribute Source Library records (API)",
  "live Curio/Kimpton/Radisson/Ascend commercial.demand rows (API)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-tribute-demand-scenario-row-review-package.js",
  "scripts/brand-explorer-tribute-demand-scenario-row-review-package.mjs",
  "docs/data-intelligence/brand-explorer-tribute-demand-scenario-row-review-package-v25C-5A.md",
  "reports/brand-explorer-tribute-demand-scenario-row-review-package.md",
  "reports/brand-explorer-tribute-demand-scenario-row-review-package.json",
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

export function demandIsComplete(row) {
  const title = nz(row?.title);
  const body = nz(row?.body);
  const implication = nz(row?.caseSummaryOwnerObjective) || nz(row?.caseSummaryInterpretation);
  return [title, body, implication].every(hasVal);
}

function copiesReferenceBrandTitle(title) {
  return FORBIDDEN_REFERENCE_TITLES.has(nz(title));
}

function containsUnsupportedStatistics(text) {
  return UNSUPPORTED_STATISTICS.test(nz(text));
}

function impliesMarriottValidation(text) {
  return FORBIDDEN_MARRIOTT_VALIDATION.test(nz(text));
}

function isGenericFiller(text) {
  const t = nz(text);
  if (!t || t.length < 24) return GENERIC_FILLER.test(t);
  const lower = t.toLowerCase();
  return (
    lower.includes("lorem ipsum") ||
    lower === "tbd" ||
    lower.includes("approved source excerpt") ||
    (lower.includes("brand footprint setup shows") && lower.includes("do not treat"))
  );
}

function assessDemandRow(row) {
  const blockedReasons = [];
  const fields = [
    row.title,
    row.body,
    row.demandScenarioDescription,
    row.ownerImplication,
    row.demandLogic,
    row.sourceBasis,
  ];
  const combined = fields.join(" ");

  if (!hasVal(row.title)) blockedReasons.push("missing_title");
  if (!hasVal(row.body)) blockedReasons.push("missing_body");
  if (!hasVal(row.demandScenarioDescription)) blockedReasons.push("missing_demand_scenario_description");
  if (!hasVal(row.ownerImplication)) blockedReasons.push("missing_owner_implication");
  if (!hasVal(row.demandLogic)) blockedReasons.push("missing_demand_logic");
  if (!hasVal(row.sourceBasis)) blockedReasons.push("missing_source_basis");

  if (copiesReferenceBrandTitle(row.title)) blockedReasons.push("copies_reference_brand_title");
  if (containsUnsupportedStatistics(combined)) blockedReasons.push("unsupported_statistics");
  if (impliesMarriottValidation(combined)) blockedReasons.push("implies_marriott_validation");
  if (isGenericFiller(row.demandScenarioDescription) || isGenericFiller(row.ownerImplication)) {
    blockedReasons.push("generic_filler");
  }

  const contractComplete = demandIsComplete({
    title: row.title,
    body: row.body,
    caseSummaryOwnerObjective: row.ownerImplication,
  });

  const readyForFounderReview = blockedReasons.length === 0;
  const readyForRowCreation = readyForFounderReview && contractComplete;

  return { blockedReasons, readyForFounderReview, readyForRowCreation, contractComplete };
}

export function buildFlattenedDemandRowTargets(
  brandRecordId = TRIBUTE_RECORD_ID,
  brandName = BRAND_NAME,
  rows = POLISHED_DEMAND_SCENARIO_ROWS
) {
  return rows.map((row) => ({
    slotKey: DEMAND_SLOT,
    title: row.title,
    body: row.body,
    sort: row.sort,
    themeKey: row.themeKey,
    updatesExistingRow: Boolean(row.updatesExistingRow),
    fields: {
      "Slot Key": DEMAND_SLOT,
      Title: row.title,
      Body: row.body,
      "Case Summary Overview": row.demandScenarioDescription,
      "Case Summary Owner Objective": row.ownerImplication,
      "Case Summary Interpretation": row.demandLogic,
      Brand: [brandRecordId],
      "Brand Name": brandName,
      Active: true,
      "Sort Order": row.sort,
    },
  }));
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function diagnoseDemandRows(rows) {
  const complete = rows.filter(demandIsComplete);
  const incomplete = rows.filter((r) => !demandIsComplete(r));
  return {
    totalRows: rows.length,
    completeRows: complete.length,
    incompleteRows: incomplete.length,
    complete,
    incomplete: incomplete.map((r) => ({
      title: r.title,
      body: r.body,
      hasOwnerImplication: hasVal(r.caseSummaryOwnerObjective) || hasVal(r.caseSummaryInterpretation),
      missing: [
        !hasVal(r.title) ? "title" : null,
        !hasVal(r.body) ? "body" : null,
        !(hasVal(r.caseSummaryOwnerObjective) || hasVal(r.caseSummaryInterpretation))
          ? "owner_implication"
          : null,
      ].filter(Boolean),
    })),
    usesGenericFallback: rows.length === 0,
    contractMinimum: DEMAND_MINIMUM,
    meetsContractMinimum: complete.length >= DEMAND_MINIMUM,
  };
}

export function buildBrandExplorerTributeDemandScenarioRowReviewPackageMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Tribute Demand Scenario Row Review Package v${PACKAGE_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- v25C-5A exists: **${report.v25C5AReviewPackageExists ? "yes" : "no"}**`);
  lines.push(`- Demand rows ready for founder review: **${report.demandRowsReadyForFounderReview}/${report.proposedDemandScenarioRows.length}**`);
  lines.push(`- At least 3 complete rows available: **${report.atLeastThreeCompleteRowsAvailable ? "yes" : "no"}**`);
  lines.push(`- Meets minimum after v25C-5B: **${report.demandMeetsMinimumAfterV25C5B ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Current Demand Scenario diagnosis");
  lines.push(`- Existing rows: ${report.currentDemandDiagnosis.totalRows}`);
  lines.push(`- Contract-complete rows: ${report.currentDemandDiagnosis.completeRows}`);
  lines.push(`- Uses generic COMM_DEMAND fallback in UI: ${report.currentDemandDiagnosis.usesGenericFallback ? "yes (no rows)" : report.currentDemandDiagnosis.completeRows < 3 ? "partial — insufficient owner-facing rows" : "no"}`);
  lines.push("");
  lines.push("## Proposed demand scenario rows");
  for (const r of report.proposedDemandScenarioRows) {
    lines.push(
      `- **${r.title}** · ${r.body} · ready: ${r.readyForFounderReview ? "yes" : "no"} · row creation: ${r.readyForRowCreation ? "yes" : "no"}`
    );
  }
  lines.push("");
  lines.push("## Exact next writer (v25C-5B)");
  lines.push("```bash");
  lines.push(report.exactNextWriterCommandDryRun);
  lines.push("");
  lines.push(report.exactNextWriterCommandApply);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerTributeDemandScenarioRowReviewPackageReport(options = {}) {
  const brandRecordId = TRIBUTE_RECORD_ID;
  const brandBasics = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);
  const tribute = await fetchBrandApiShape(brandRecordId);

  let existingSources = [];
  try {
    let offset = "";
    do {
      const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
      existingSources.push(...(page.sources || []));
      offset = page.offset || "";
    } while (offset);
  } catch (err) {
    console.warn("[v25C-5A] source list warning:", err?.message || err);
  }

  const referenceRowStructure = [];
  for (const ref of REFERENCE_BRANDS) {
    const b = await fetchBrandApiShape(ref.id);
    if (!b) continue;
    const demand = blocksForSlot(b, DEMAND_SLOT);
    referenceRowStructure.push({
      brand: ref.name,
      demandCount: demand.length,
      slotKey: DEMAND_SLOT,
      fieldPattern: {
        title: "scenario name",
        body: "directional label (Strong, Moderate–strong, Not a fit)",
        caseSummaryOwnerObjective: "typically empty on live reference rows",
        caseSummaryInterpretation: "typically empty on live reference rows",
      },
      sampleRows: demand.slice(0, 6).map((r) => ({
        title: nz(r.title),
        body: nz(r.body),
        sort: r.sort ?? null,
        hasOwnerImplication:
          hasVal(r.caseSummaryOwnerObjective) || hasVal(r.caseSummaryInterpretation),
      })),
    });
  }

  const existingDemand = blocksForSlot(tribute, DEMAND_SLOT);
  const currentDemandDiagnosis = diagnoseDemandRows(existingDemand);

  const proposedDemandScenarioRows = POLISHED_DEMAND_SCENARIO_ROWS.map((row) => {
    const assessment = assessDemandRow(row);
    return {
      slotKey: DEMAND_SLOT,
      themeKey: row.themeKey,
      title: row.title,
      body: row.body,
      demandScenarioDescription: row.demandScenarioDescription,
      ownerImplication: row.ownerImplication,
      demandLogic: row.demandLogic,
      sourceBasis: row.sourceBasis,
      sourceIds: row.sourceIds,
      propertyExamples: row.propertyExamples,
      riskLevel: row.riskLevel,
      sort: row.sort,
      updatesExistingRow: Boolean(row.updatesExistingRow),
      existingRowTitle: row.existingRowTitle || null,
      founderReviewStatus: GOVERNANCE_LABELS.join("; "),
      governanceLabels: GOVERNANCE_LABELS,
      contractComplete: assessment.contractComplete,
      readyForFounderReview: assessment.readyForFounderReview,
      readyForRowCreation: assessment.readyForRowCreation,
      blockedReasons: assessment.blockedReasons,
      airtableFieldMapping: {
        "Slot Key": DEMAND_SLOT,
        Title: row.title,
        Body: row.body,
        "Case Summary Overview": row.demandScenarioDescription,
        "Case Summary Owner Objective": row.ownerImplication,
        "Case Summary Interpretation": row.demandLogic,
      },
    };
  });

  const demandReady = proposedDemandScenarioRows.filter((r) => r.readyForFounderReview);
  const demandBlocked = proposedDemandScenarioRows.filter((r) => !r.readyForFounderReview);
  const demandRowCreationReady = proposedDemandScenarioRows.filter((r) => r.readyForRowCreation);

  const proposedPayloads = buildFlattenedDemandRowTargets(brandRecordId, BRAND_NAME);

  const existingRowToUpdate = existingDemand.find(
    (r) => nz(r.title).toLowerCase() === "resort & leisure conversion"
  );

  const rowsWouldCreate = proposedDemandScenarioRows.filter((r) => !r.updatesExistingRow);
  const rowsWouldUpdate = proposedDemandScenarioRows.filter((r) => r.updatesExistingRow);

  const demandMeetsMinimumAfterV25C5B = demandRowCreationReady.length >= DEMAND_MINIMUM;
  const atLeastThreeCompleteRowsAvailable = demandRowCreationReady.length >= DEMAND_MINIMUM;

  return {
    packageVersion: PACKAGE_VERSION,
    v25C5AReviewPackageExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    protectedSectionsUntouched: [
      "loyalty",
      "footprint.openings",
      "footprint.momentum",
      "standards",
      "footprint.portfolio_mix",
      "branded residences",
    ],
    brand: {
      recordId: brandRecordId,
      name: BRAND_NAME,
      slug: "tribute-portfolio",
    },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    sourceInventoryCount: existingSources.length,
    sourceIdsReferenced: [
      CONSUMER_SOURCE_ID,
      "recNvITV5HzuQburM",
      "recSLu3N7s84rIKS6",
      "recZmeduOoM1PZEpT",
      "recjVfKnl9q18MO5w",
    ],
    referenceRowStructure,
    currentDemandDiagnosis,
    existingPresentationRows: {
      commercialDemand: existingDemand.map((r) => ({
        title: r.title,
        body: r.body,
        sort: r.sort,
        recordId: r.recordId || r.id || null,
        hasOwnerImplication:
          hasVal(r.caseSummaryOwnerObjective) || hasVal(r.caseSummaryInterpretation),
        contractComplete: demandIsComplete(r),
      })),
    },
    proposedDemandScenarioRows,
    demandRowsReadyForFounderReview: demandReady.length,
    demandRowsReadyForRowCreation: demandRowCreationReady.length,
    demandRowsBlocked: demandBlocked.map((r) => ({
      title: r.title,
      reasons: r.blockedReasons,
    })),
    atLeastThreeCompleteRowsAvailable,
    demandMeetsMinimumAfterV25C5B,
    targetParityRows: DEMAND_TARGET,
    exactProposedCopyByRow: proposedDemandScenarioRows.map((r) => ({
      title: r.title,
      body: r.body,
      demandScenarioDescription: r.demandScenarioDescription,
      ownerImplication: r.ownerImplication,
      demandLogic: r.demandLogic,
      sourceBasis: r.sourceBasis,
    })),
    exactProposedRowPayloads: proposedPayloads,
    rowsNeedingCreationV25C5B: rowsWouldCreate.length,
    rowsNeedingUpdateV25C5B: rowsWouldUpdate.length,
    existingRowUpdateTarget: existingRowToUpdate
      ? {
          recordId: existingRowToUpdate.recordId || existingRowToUpdate.id || null,
          currentTitle: existingRowToUpdate.title,
          proposedTitle: "Resort / Leisure Repositioning",
          reason: "Add owner implication + align title; retain Moderate–strong directional label",
        }
      : null,
    presentationTable: PRESENTATION_TABLE,
    companyValidatedBefore,
    companyValidatedAfter: companyValidatedBefore,
    exactNextWriter: NEXT_WRITER,
    exactNextWriterVersion: NEXT_WRITER_VERSION,
    exactNextWriterCommandDryRun: `npm run ${NEXT_WRITER} -- --brand tribute-portfolio --dry-run`,
    exactNextWriterCommandApply: `npm run ${NEXT_WRITER} -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-5B-demand-scenario-rows --founder-reviewed-demand-scenario-row-copy --approve-brand-explorer-v25C-5B-row-create`,
    v25C5BRowCreationReady:
      demandMeetsMinimumAfterV25C5B &&
      demandRowCreationReady.length >= DEMAND_MINIMUM &&
      demandBlocked.length === 0,
    doesNotDo: [
      "Create or update commercial.demand presentation rows in Airtable",
      "Change Company Validated or Company Validation Date",
      "Modify loyalty, openings, momentum, standards, portfolio mix, or branded residences fields",
      "Copy reference-brand demand scenario titles verbatim",
      "Invent market statistics or demand performance claims",
      "Imply Marriott validated anything",
    ],
  };
}
