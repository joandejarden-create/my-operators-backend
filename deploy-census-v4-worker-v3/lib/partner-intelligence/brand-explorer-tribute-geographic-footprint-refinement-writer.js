/**
 * Brand Explorer Tribute Geographic Footprint Refinement Writer v25C-5D.
 *
 * Refines existing footprint.region presentation rows for Tribute Portfolio with
 * brand-specific owner-facing regional context. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-tribute-geographic-footprint-refinement-writer-v25C-5D.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";

export const WRITER_VERSION = "25C-5D";
export const REPORT_JSON_NAME = "brand-explorer-tribute-geographic-footprint-refinement-writer.json";
export const REPORT_MD_NAME = "brand-explorer-tribute-geographic-footprint-refinement-writer.md";
export const DOC_MD_NAME = "brand-explorer-tribute-geographic-footprint-refinement-writer-v25C-5D.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v25C-5D-geographic-footprint-refinement";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-geographic-footprint-copy";
export const APPLY_FLAG_NO_COUNTS = "--confirm-no-unsupported-footprint-counts";

export const REGION_SLOTS = [
  "footprint.region.am",
  "footprint.region.cala",
  "footprint.region.eu",
  "footprint.region.mea",
  "footprint.region.apac",
];
export const MIN_REGION_ROWS = 5;

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const GENERIC_REGION_RE =
  /brand footprint setup shows|do not treat this card as|underwrite conversion scope, market tier, and operating complexity for your asset/i;

const UNSUPPORTED_STATISTICS =
  /\b\d{1,3}(?:\.\d+)?%\b|\b\d[\d,]*\s*(?:million|billion|thousand|m\+|k\+|hotels?|properties|markets?|countries)\b|\$\s?\d[\d,]+/i;

const MARRIOTT_VALIDATION_RE =
  /marriott\s+validated|validated\s+by\s+marriott|company-validated\s+footprint/i;

const PROTECTED_SLOT_PREFIXES = [
  "loyalty.",
  "standards.",
  "footprint.openings",
  "footprint.momentum",
  "footprint.portfolio_mix",
  "overview.portfolio_context",
  "commercial.demand",
  "branded.",
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.md",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/tribute-portfolio-targeted-extract.md",
  "reports/tribute-portfolio-targeted-extract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute footprint.region rows",
  "live Tribute Source Library records",
  "live Tribute Partner Facts",
  "live Curio/Kimpton/Radisson/Ascend footprint.region rows",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

export function buildRegionBody({
  statusLabel,
  regionalContext,
  ownerImplication,
  fitSignal,
  confirmationNote,
  sourceBasis,
}) {
  return [
    statusLabel,
    "",
    regionalContext,
    "",
    `Owner implication: ${ownerImplication}`,
    `Fit signal: ${fitSignal}`,
    `Confirmation note: ${confirmationNote}`,
    `Source/founder-review basis: ${sourceBasis}`,
  ].join("\n");
}

export const REFINED_REGION_ROWS = [
  {
    slotKey: "footprint.region.am",
    title: "Americas",
    sort: 260,
    statusLabel: "Independent lifestyle fit",
    regionalContext:
      "Tribute Portfolio in the Americas suits owners evaluating independent lifestyle, urban boutique, and resort-adjacent conversions who want Marriott affiliation and Bonvoy participation without a hard-brand prototype reflag.",
    ownerImplication:
      "Screen market tier, conversion PIP scope, and operating complexity before assuming collection economics clear fees—especially in secondary lifestyle markets.",
    fitSignal:
      "Strongest where local character and design narrative support premium conversion, not economy limited-service reflags.",
    confirmationNote:
      "Confirm current brand appetite and development priorities with Marriott before underwriting.",
    sourceBasis:
      "Tribute consumer site + development captures + founder-reviewed positioning (recF0qS9JIZjM3qza, recSLu3N7s84rIKS6); directional only.",
  },
  {
    slotKey: "footprint.region.cala",
    title: "CALA",
    sort: 280,
    statusLabel: "Strong lifestyle & resort context",
    regionalContext:
      "CALA is a meaningful Tribute context for resort, urban lifestyle, and character-led assets—illustrated by captured examples in Puerto Rico (Hotel Rumbao), Barbados (Crystal Cove), Mexico/Riviera Maya (Casa Nizuc listing), Colombia (Loma Medellín), and Peru (Humano Lima).",
    ownerImplication:
      "Underwrite resort operating complexity, seasonal demand, and F&B scope alongside Bonvoy distribution potential—not generic beach-market ADR assumptions.",
    fitSignal:
      "Directional fit for leisure and urban lifestyle conversions where independent identity and Marriott systems both matter.",
    confirmationNote:
      "Use this region as a fit-screening context rather than a count-based footprint claim; confirm project-specific approval with Marriott development.",
    sourceBasis:
      "Tribute openings/momentum property examples + consumer site (recF0qS9JIZjM3qza); illustrative examples only—not performance claims.",
  },
  {
    slotKey: "footprint.region.eu",
    title: "Europe",
    sort: 290,
    statusLabel: "Urban & boutique context",
    regionalContext:
      "Europe can be relevant for urban lifestyle, independent boutique, and destination-led properties where local identity, heritage, or design narrative matters and owners want soft-collection affiliation with Marriott distribution.",
    ownerImplication:
      "Validate conversion scope, preservation constraints, operating model, and fee stack before assuming Tribute is preferable to a select-service Marriott reflag.",
    fitSignal:
      "Useful owner-screening region for character-led assets—not a uniform prototype expansion story.",
    confirmationNote:
      "Confirm current brand appetite and development priorities with Marriott before underwriting.",
    sourceBasis:
      "Founder-reviewed positioning + Marriott development/context captures; no unsupported Europe count claims.",
  },
  {
    slotKey: "footprint.region.mea",
    title: "MEA",
    sort: 300,
    statusLabel: "Selective / case-by-case",
    regionalContext:
      "Middle East & Africa should be treated as a case-by-case fit screen for Tribute—dependent on market tier, asset quality, operating complexity, and Marriott development appetite rather than a broad regional footprint claim.",
    ownerImplication:
      "Do not underwrite from this card alone; confirm whether the asset supports lifestyle/resort collection standards and whether Marriott has active development interest for the address.",
    fitSignal:
      "Limited default signal—pursue only when local character, ADR depth, and operator capability align with collection posture.",
    confirmationNote:
      "Use this region as a fit-screening context rather than a count-based footprint claim.",
    sourceBasis:
      "Cautious founder-reviewed screening language; no sourced MEA property-example package in current Tribute capture set.",
  },
  {
    slotKey: "footprint.region.apac",
    title: "APAC",
    sort: 270,
    statusLabel: "Destination & urban lifestyle context",
    regionalContext:
      "Asia Pacific can support heritage, urban waterfront, and destination-led lifestyle opportunities where a soft collection preserves local identity while connecting to Marriott commercial systems and Bonvoy.",
    ownerImplication:
      "Stress-test operating complexity, design investment, and market ADR depth—Tribute is not a select-service highway play.",
    fitSignal:
      "Directional context for independent-character assets; confirm address-level interest rather than inferring regional density.",
    confirmationNote:
      "Confirm current brand appetite and development priorities with Marriott before underwriting.",
    sourceBasis:
      "Founder-reviewed positioning + approved global region framing; illustrative only without APAC count claims.",
  },
].map((row) => ({
  ...row,
  body: buildRegionBody(row),
}));

export function isGenericRegionCopy(row) {
  const b = nz(row?.body).toLowerCase();
  if (!b) return true;
  return GENERIC_REGION_RE.test(b);
}

export function scanRegionCopySafety(text) {
  const combined = nz(text);
  const issues = [];
  if (GENERIC_REGION_RE.test(combined.toLowerCase())) issues.push("generic_template_copy");
  if (UNSUPPORTED_STATISTICS.test(combined)) issues.push("unsupported_statistics");
  if (MARRIOTT_VALIDATION_RE.test(combined)) issues.push("marriott_validation_language");
  if (!combined || combined.length < 80) issues.push("blank_or_too_thin");
  return issues;
}

export function evaluateGeographicFootprintReadiness(brand, regionRows = null) {
  const rows =
    regionRows ||
    REGION_SLOTS.flatMap((slotKey) => {
      const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
      return blocks.filter((b) => b && nz(b.slotKey) === slotKey);
    });
  const genericCount = rows.filter(isGenericRegionCopy).length;
  const safetyIssues = rows.flatMap((r) =>
    scanRegionCopySafety(`${r.title}\n${r.body}`).map((issue) => `${r.title || r.slotKey}:${issue}`)
  );
  const blockers = [];
  if (rows.length < MIN_REGION_ROWS) blockers.push(`insufficient_region_rows:${rows.length}`);
  if (genericCount > 0) blockers.push(`generic_region_copy:${genericCount}`);
  if (safetyIssues.length) blockers.push(...safetyIssues);

  return {
    regionRowCount: rows.length,
    genericRegionCount: genericCount,
    copySafetyPassed: safetyIssues.length === 0,
    safetyIssues,
    ready: blockers.length === 0,
    blockers,
  };
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function isProtectedSlot(slotKey) {
  const key = nz(slotKey);
  if (REGION_SLOTS.includes(key)) return false;
  return PROTECTED_SLOT_PREFIXES.some((prefix) => key === prefix || key.startsWith(prefix));
}

function bodiesMatch(a, b) {
  return normalizeBody(a) === normalizeBody(b);
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-tribute-geographic-footprint-refinement-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_COUNTS}`;
}

export async function buildBrandExplorerTributeGeographicFootprintRefinementWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noUnsupportedCounts = false,
} = {}) {
  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-5D pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );

  const presentationRows = presentationRaw.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
  }));

  const regionRowsLive = presentationRows.filter((r) => REGION_SLOTS.includes(r.slotKey));
  const brandShape = {
    brandExplorer: {
      blocks: regionRowsLive.map((r) => ({
        recordId: r.recordId,
        slotKey: r.slotKey,
        title: r.title,
        body: r.body,
        sort: r.sortOrder,
      })),
    },
  };

  const currentReadiness = evaluateGeographicFootprintReadiness(brandShape);
  const thinGenericReasons = regionRowsLive
    .filter(isGenericRegionCopy)
    .map((r) => ({
      slotKey: r.slotKey,
      title: r.title,
      reason: "Template filler uses 'Brand Footprint setup shows' / 'do not treat this card as' language",
      bodyPreview: r.body.slice(0, 140),
    }));

  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const rowsMatched = [];
  const applyBlockers = [];

  for (const planned of REFINED_REGION_ROWS) {
    const safety = scanRegionCopySafety(`${planned.title}\n${planned.body}`);
    if (safety.length) {
      applyBlockers.push(`proposed_copy_unsafe:${planned.slotKey}:${safety.join(",")}`);
    }

    const live = regionRowsLive.find((r) => r.slotKey === planned.slotKey);
    if (!live) {
      rowsWouldCreate.push({
        slotKey: planned.slotKey,
        title: planned.title,
        sort: planned.sort,
        action: "create",
        fields: {
          "Slot Key": planned.slotKey,
          Title: planned.title,
          Body: planned.body,
          "Brand Name": BRAND_NAME,
          Brand: [brandRecordId],
          Active: true,
          "Sort Order": planned.sort,
        },
      });
      continue;
    }

    if (bodiesMatch(live.body, planned.body) && nz(live.title) === planned.title) {
      rowsMatched.push({
        recordId: live.recordId,
        slotKey: planned.slotKey,
        title: planned.title,
        action: "matched",
      });
      continue;
    }

    rowsWouldUpdate.push({
      recordId: live.recordId,
      slotKey: planned.slotKey,
      title: planned.title,
      sort: planned.sort,
      action: "update",
      currentBody: live.body,
      proposedBody: planned.body,
      fields: {
        "Slot Key": planned.slotKey,
        Title: planned.title,
        Body: planned.body,
        "Brand Name": BRAND_NAME,
        Active: true,
        "Sort Order": planned.sort,
      },
    });
  }

  if (regionRowsLive.length < MIN_REGION_ROWS) {
    applyBlockers.push(`insufficient_live_region_rows:${regionRowsLive.length}`);
  }
  for (const row of regionRowsLive) {
    if (!hasVal(row.body)) applyBlockers.push(`blank_region_row:${row.slotKey}`);
  }

  const projectedReadiness = evaluateGeographicFootprintReadiness({
    brandExplorer: {
      blocks: REFINED_REGION_ROWS.map((r) => ({
        slotKey: r.slotKey,
        title: r.title,
        body: r.body,
      })),
    },
  });

  const applyGatesReady = apply && approveBatch && founderReviewed && noUnsupportedCounts;
  const hasWork = rowsWouldCreate.length > 0 || rowsWouldUpdate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const created = [];
    const updated = [];
    const errors = [];

    for (const row of rowsWouldUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        },
        row.recordId
      );
      if (!res.ok) {
        errors.push({
          action: "update",
          slotKey: row.slotKey,
          recordId: row.recordId,
          message: json.error?.message || res.status,
        });
      } else {
        updated.push({ recordId: json.id, slotKey: row.slotKey, title: row.title });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const row of rowsWouldCreate) {
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields: row.fields, typecast: true }),
      });
      if (!res.ok) {
        errors.push({
          action: "create",
          slotKey: row.slotKey,
          message: json.error?.message || res.status,
        });
      } else {
        created.push({ recordId: json.id, slotKey: row.slotKey, title: row.title });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    airtableModified = (created.length > 0 || updated.length > 0) && errors.length === 0;
    applyResults = { created, updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(brandRecordId));
  } else if (apply) {
    applyResults = { created: [], updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C5DWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: { name: BRAND_NAME, recordId: brandRecordId, slug: "tribute-portfolio" },
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-tribute-geographic-footprint-refinement-writer.js",
      "scripts/brand-explorer-tribute-geographic-footprint-refinement-writer.mjs",
      "docs/data-intelligence/brand-explorer-tribute-geographic-footprint-refinement-writer-v25C-5D.md",
      "reports/brand-explorer-tribute-geographic-footprint-refinement-writer.md",
      "reports/brand-explorer-tribute-geographic-footprint-refinement-writer.json",
      "package.json",
      "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
    ],
    currentGeographicFootprintReadiness: {
      contractStatusBeforeApply:
        currentReadiness.ready ? "meets_minimum" : "thin_or_generic",
      ...currentReadiness,
    },
    thinGenericDiagnosis: {
      genericRegionCount: currentReadiness.genericRegionCount,
      reasons: thinGenericReasons,
      contractDetector:
        "isGenericRegionCopy matches 'Brand Footprint setup shows' and 'do not treat this card as' template filler",
    },
    footprintRowsInspected: regionRowsLive.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title,
      sortOrder: r.sortOrder,
      generic: isGenericRegionCopy(r),
      bodyPreview: r.body.slice(0, 160),
    })),
    proposedRefinedCopyByRegion: REFINED_REGION_ROWS.map((r) => ({
      slotKey: r.slotKey,
      title: r.title,
      sort: r.sort,
      statusLabel: r.statusLabel,
      regionalContext: r.regionalContext,
      ownerImplication: r.ownerImplication,
      fitSignal: r.fitSignal,
      confirmationNote: r.confirmationNote,
      sourceBasis: r.sourceBasis,
      body: r.body,
    })),
    rowsWouldUpdate,
    rowsWouldCreate,
    rowsMatched,
    unsupportedCountsExcluded: !REFINED_REGION_ROWS.some((r) =>
      scanRegionCopySafety(r.body).includes("unsupported_statistics")
    ),
    projectedGeographicFootprintReadyAfterApply: projectedReadiness.ready,
    projectedReadiness,
    loyaltyRowsUntouched: true,
    openingsRowsUntouched: true,
    momentumRowsUntouched: true,
    demandRowsUntouched: true,
    standardsRowsUntouched: true,
    portfolioMixContextRowsUntouched: true,
    brandedResidencesUntouched: true,
    imagesUntouched: true,
    protectedNonGeoRowsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      noUnsupportedCounts,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers: [...new Set(applyBlockers)],
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio"),
    idempotentAfterApply:
      rowsWouldCreate.length === 0 && rowsWouldUpdate.length === 0 && projectedReadiness.ready,
    doesNotDo: [
      "Invent property counts, pipeline numbers, country counts, or performance claims",
      "Use generic Brand Footprint setup template copy",
      "Modify loyalty, openings, momentum, demand, standards, portfolio mix/context, or branded residences rows",
      "Change images or Company Validated",
      "Imply Marriott validated footprint claims",
    ],
  };
}

export function buildBrandExplorerTributeGeographicFootprintRefinementWriterMarkdown(report) {
  return [
    `# Brand Explorer Tribute Geographic Footprint Refinement Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Writer exists: **${report.v25C5DWriterExists ? "yes" : "no"}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    "",
    "## Summary",
    "",
    `- Current status: **${report.currentGeographicFootprintReadiness.contractStatusBeforeApply}**`,
    `- Ready after apply: **${report.projectedGeographicFootprintReadyAfterApply ? "yes" : "no"}**`,
    `- Rows to update: **${report.rowsWouldUpdate.length}**`,
    `- Rows to create: **${report.rowsWouldCreate.length}**`,
    `- Unsupported counts excluded: **${report.unsupportedCountsExcluded ? "yes" : "no"}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    "",
    "## Apply command",
    "",
    "```bash",
    report.exactApplyCommand,
    "```",
  ].join("\n");
}
