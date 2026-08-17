/**
 * Match Score v2 — Brand Setup score-critical gap audit (Active/Live only).
 * Read-only against Airtable. Field keys match api/match-score-server.js (no invented names).
 *
 * Fill sources for remediation: founder knowledge (A) + existing Dealality docs (B) only.
 * Never invent Priority Markets / fees / standards to juice scores.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadActiveUniverse, ACTIVE_UNIVERSE_SOURCE } from "./partner-intelligence/brand-explorer-active-universe.js";
import { fetchBrandData } from "../api/match-score-server.js";
import { buildBrandSetupAirtableMappingInventory } from "../api/brand-library.js";

export const AUDIT_VERSION = "match-score-brand-setup-gap-audit-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "..");

/** Score-critical checks — aligned to Match Score v2 soft factors + hard gates. */
export const SCORE_CRITICAL_CHECKS = Object.freeze([
  {
    key: "hotelChainScale",
    label: "Hotel Chain Scale",
    table: "Brand Setup - Brand Basics",
    area: "basics",
    softFactor: "chainScaleProximity",
    gate: "chainScale",
    weightPct: 14,
    isFilled: (bd) => filledStr(bd?.brandBasics?.["Hotel Chain Scale"]),
  },
  {
    key: "hotelServiceModel",
    label: "Hotel Service Model",
    table: "Brand Setup - Brand Basics",
    area: "basics",
    softFactor: "serviceModelAlignment",
    gate: null,
    weightPct: 10,
    isFilled: (bd) => filledStr(bd?.brandBasics?.["Hotel Service Model"]),
  },
  {
    key: "marketsToAvoid",
    label: "Markets to Avoid (optional for soft; needed for avoid gate)",
    table: "Brand Setup - Brand Basics / Project Fit",
    area: "basics",
    softFactor: null,
    gate: "geographyAvoid",
    weightPct: 0,
    optional: true,
    isFilled: (bd) => {
      const basics = bd?.brandBasics?.["Markets to Avoid or Saturated"];
      const fit = bd?.brandFit?.["Markets to Avoid"];
      return filledAny(basics) || filledAny(fit);
    },
  },
  {
    key: "priorityMarkets",
    label: "Priority Markets",
    table: "Brand Setup - Project Fit",
    area: "projectFit",
    softFactor: "geographyPriority",
    gate: null,
    weightPct: 18,
    isFilled: (bd) => hasPriorityMarkets(bd?.brandFit || {}),
  },
  {
    key: "softCollectionBrand",
    label: "Soft/Collection Brand",
    table: "Brand Setup - Project Fit",
    area: "projectFit",
    softFactor: "softHardPreference",
    gate: null,
    weightPct: 8,
    isFilled: (bd) => filledStr(bd?.brandFit?.["Soft/Collection Brand"]),
  },
  {
    key: "acceptableProjectType",
    label: "Acceptable Project Type",
    table: "Brand Setup - Project Fit",
    area: "projectFit",
    softFactor: null,
    gate: "projectType",
    weightPct: 0,
    isFilled: (bd) => filledAny(bd?.brandFit?.["Acceptable Project Type"]),
  },
  {
    key: "acceptableBuildingTypes",
    label: "Acceptable Building Types (breakdown only)",
    table: "Brand Setup - Project Fit",
    area: "projectFit",
    softFactor: null,
    gate: null,
    weightPct: 0,
    optional: true,
    isFilled: (bd) => filledAny(bd?.brandFit?.["Acceptable Building Types"]),
  },
  {
    key: "acceptableProjectStages",
    label: "Acceptable Project Stages (breakdown only)",
    table: "Brand Setup - Project Fit",
    area: "projectFit",
    softFactor: null,
    gate: null,
    weightPct: 0,
    optional: true,
    isFilled: (bd) => filledAny(bd?.brandFit?.["Acceptable Project Stages"]),
  },
  {
    key: "roomCountRange",
    label: "Min/Max Room Count",
    table: "Brand Setup - Project Fit",
    area: "projectFit",
    softFactor: null,
    gate: "rooms",
    weightPct: 0,
    isFilled: (bd) => {
      const pf = bd?.brandFit || {};
      return pf["Min - Room Count"] != null && pf["Min - Room Count"] !== "" && pf["Max - Room Count"] != null && pf["Max - Room Count"] !== "";
    },
  },
  {
    key: "acceptableAgreementsType",
    label: "Acceptable Agreements Type",
    table: "Brand Setup - Project Fit",
    area: "projectFit",
    softFactor: "agreementsTypeCompatibility",
    gate: "agreementType",
    weightPct: 6,
    isFilled: (bd) => hasAcceptableAgreements(bd?.brandFit || {}),
  },
  {
    key: "additionalAmenities",
    label: "Additional Amenities (Brand Standards)",
    table: "Brand Setup - Brand Standards",
    area: "standards",
    softFactor: "brandStandardsCompatibility",
    gate: null,
    weightPct: 14,
    isFilled: (bd) => filledAny(bd?.brandStandards?.["Additional Amenities"]),
  },
  {
    key: "fbParkingStandards",
    label: "F&B Outlets Required / Parking Required",
    table: "Brand Setup - Brand Standards",
    area: "standards",
    softFactor: "brandStandardsCompatibility",
    gate: null,
    weightPct: 0,
    optional: true,
    isFilled: (bd) => {
      const st = bd?.brandStandards || {};
      return filledStr(st["F&B Outlets Required"]) || filledStr(st["Parking Required"]);
    },
  },
  {
    key: "feeRoyaltyRange",
    label: "Typical Royalty Fee Range (Min/Max)",
    table: "Brand Setup - Fee Structure",
    area: "fees",
    softFactor: "feesToleranceCompatibility",
    gate: null,
    weightPct: 12,
    isFilled: (bd) => {
      const fs = bd?.brandFeeStructure || {};
      return (fs["Min - Typical Royalty Fee Range"] != null && fs["Min - Typical Royalty Fee Range"] !== "") ||
        (fs["Max - Typical Royalty Fee Range"] != null && fs["Max - Typical Royalty Fee Range"] !== "");
    },
  },
  {
    key: "feeMarketingOrLoyalty",
    label: "Marketing and/or Loyalty fee ranges",
    table: "Brand Setup - Fee Structure",
    area: "fees",
    softFactor: "feesToleranceCompatibility",
    gate: null,
    weightPct: 0,
    optional: true,
    isFilled: (bd) => {
      const fs = bd?.brandFeeStructure || {};
      const mkt = fs["Min - Typical Marketing Fee Range"] != null || fs["Max - Typical Marketing Fee Range"] != null;
      const loy = fs["Min - Typical Loyalty Program Fee"] != null || fs["Max - Typical Loyalty Program Fee"] != null;
      return Boolean(mkt || loy);
    },
  },
  {
    key: "incentiveTypes",
    label: "Incentive Types (Operational Support)",
    table: "Brand Setup - Operational Support",
    area: "operationalSupport",
    softFactor: "keyMoneyWillingnessCompatibility",
    gate: "keyMoney",
    weightPct: 10,
    isFilled: (bd) => filledAny(bd?.brandOperationalSupport?.["Incentive Types"]),
  },
  {
    key: "willingToNegotiateIncentives",
    label: "Willing to Negotiate Incentives",
    table: "Brand Setup - Operational Support",
    area: "operationalSupport",
    softFactor: "incentivesMatchCompatibility",
    gate: null,
    weightPct: 8,
    isFilled: (bd) => {
      const op = bd?.brandOperationalSupport || {};
      return filledStr(op["Willing to Negotiate Incentives"]) || filledStr(op["Willing to Negotiate Incentives?"]);
    },
  },
]);

/** Soft-factor keys whose null drag is most common when Brand Setup is thin (~52% weight). */
export const NULL_DRAG_SOFT_FACTORS = Object.freeze([
  "geographyPriority",
  "brandStandardsCompatibility",
  "feesToleranceCompatibility",
  "softHardPreference",
]);

function filledStr(v) {
  if (v == null) return false;
  if (typeof v === "string") return v.trim() !== "" && v.trim() !== "—";
  if (typeof v === "number" && !Number.isNaN(v)) return true;
  if (typeof v === "boolean") return true;
  return String(v).trim() !== "";
}

function filledAny(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.some((x) => filledStr(typeof x === "object" && x != null ? x.name ?? x : x));
  return filledStr(v);
}

function hasPriorityMarkets(brandFit) {
  const cols = [
    "Global - Priority Markets",
    "United States - Priority Markets",
    "Canada - Priority Markets",
    "Western Europe - Priority Markets",
    "United Kingdom - Priority Markets",
    "Other - Priority Markets",
  ];
  for (const col of cols) {
    if (brandFit[col] === true || brandFit[col] === "Yes") return true;
  }
  return filledAny(brandFit["Priority Markets"]);
}

function hasAcceptableAgreements(brandFit) {
  if (filledAny(brandFit["Acceptable Agreements Type"])) return true;
  const boolCols = [
    "Franchise Only - Acceptable Agreements Type",
    "Third-Party Management Only - Acceptable Agreements Type",
    "Brand + Third-Party - Acceptable Agreements Type",
    "Brand-Managed - Acceptable Agreements Type",
    "Lease - Acceptable Agreements Type",
    "Joint Venture - Acceptable Agreements Type",
    "Flexible/Open - Acceptable Agreements Type",
  ];
  return boolCols.some((c) => brandFit[c] === true || brandFit[c] === "Yes" || brandFit[c] === "Acceptable");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {{ baseId?: string, apiKey?: string, brandGapMs?: number }} opts
 */
export async function runMatchScoreBrandSetupGapAudit(opts = {}) {
  const baseId = opts.baseId || process.env.AIRTABLE_BASE_ID;
  const apiKey = opts.apiKey || process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  }
  const brandGapMs = opts.brandGapMs ?? 350;

  const universe = await loadActiveUniverse({ includeDetails: false });
  const brands = [];
  let fetchFailures = 0;

  for (const row of universe.brands) {
    let brandData = null;
    try {
      brandData = await fetchBrandData(baseId, apiKey, row.name);
    } catch (err) {
      fetchFailures += 1;
      brandData = null;
      if (process.env.NODE_ENV !== "test") {
        console.warn("[gap-audit] fetchBrandData failed for", row.name, err?.message || err);
      }
    }

    const checks = SCORE_CRITICAL_CHECKS.map((c) => {
      const filled = brandData ? Boolean(c.isFilled(brandData)) : false;
      return {
        key: c.key,
        label: c.label,
        table: c.table,
        area: c.area,
        softFactor: c.softFactor,
        gate: c.gate,
        weightPct: c.weightPct,
        optional: Boolean(c.optional),
        filled,
        requiredForP1: !c.optional,
      };
    });

    const required = checks.filter((c) => c.requiredForP1);
    const requiredFilled = required.filter((c) => c.filled).length;
    const optional = checks.filter((c) => c.optional);
    const optionalFilled = optional.filter((c) => c.filled).length;
    const blankRequired = required.filter((c) => !c.filled).map((c) => c.key);
    const blankOptional = optional.filter((c) => !c.filled).map((c) => c.key);

    const nullDragWeightMissing = SCORE_CRITICAL_CHECKS.filter(
      (c) => c.softFactor && NULL_DRAG_SOFT_FACTORS.includes(c.softFactor) && c.weightPct > 0
    ).reduce((sum, c) => {
      const hit = checks.find((x) => x.key === c.key);
      return sum + (hit && !hit.filled ? c.weightPct : 0);
    }, 0);

    brands.push({
      recordId: row.recordId,
      name: row.name,
      slug: row.slug,
      status: row.status,
      brandDataFetched: Boolean(brandData),
      linkedTablesPresent: brandData
        ? {
            brandBasics: Boolean(brandData.brandBasics && Object.keys(brandData.brandBasics).length),
            brandFit: Boolean(brandData.brandFit && Object.keys(brandData.brandFit).length),
            brandStandards: Boolean(brandData.brandStandards && Object.keys(brandData.brandStandards).length),
            brandFeeStructure: Boolean(brandData.brandFeeStructure && Object.keys(brandData.brandFeeStructure).length),
            brandOperationalSupport: Boolean(brandData.brandOperationalSupport && Object.keys(brandData.brandOperationalSupport).length),
            brandDealTerms: Boolean(brandData.brandDealTerms && Object.keys(brandData.brandDealTerms).length),
            brandFootprint: Boolean(brandData.brandFootprint && Object.keys(brandData.brandFootprint).length),
          }
        : null,
      scoreCriticalRequiredTotal: required.length,
      scoreCriticalRequiredFilled: requiredFilled,
      scoreCriticalRequiredPct: required.length ? Math.round((requiredFilled / required.length) * 1000) / 10 : 0,
      scoreCriticalOptionalFilled: optionalFilled,
      scoreCriticalOptionalTotal: optional.length,
      blankRequiredKeys: blankRequired,
      blankOptionalKeys: blankOptional,
      nullDragWeightMissingPct: nullDragWeightMissing,
      p1Complete: requiredFilled === required.length && Boolean(brandData),
      checks,
    });

    if (brandGapMs > 0) await sleep(brandGapMs);
  }

  brands.sort((a, b) => a.scoreCriticalRequiredPct - b.scoreCriticalRequiredPct || a.name.localeCompare(b.name));

  const p1CompleteCount = brands.filter((b) => b.p1Complete).length;
  const avgRequiredPct =
    brands.length > 0
      ? Math.round((brands.reduce((s, b) => s + b.scoreCriticalRequiredPct, 0) / brands.length) * 10) / 10
      : 0;

  const inventory = buildBrandSetupAirtableMappingInventory();
  const scoreCriticalKeys = new Set(SCORE_CRITICAL_CHECKS.map((c) => c.key));
  const p2BacklogTables = [
    ...new Set((inventory.rows || []).map((r) => r.airtableTable).filter(Boolean)),
  ].sort();

  return {
    auditVersion: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    readOnly: true,
    fillPolicy: {
      allowedSources: ["A_founder_brand_knowledge", "B_existing_dealality_docs_fdd_research"],
      forbidden: ["web_ai_invent_without_approve", "explorer_marketing_as_operating_fact", "global_priority_defaults_to_juice_scores"],
      scoringRule:
        "Match Score v2 excludes null soft factors from the denominator; still complete Brand Setup so scores are trustworthy (min scored weight). Do not invent Global priority to juice scores.",
    },
    activeUniverse: {
      source: ACTIVE_UNIVERSE_SOURCE,
      totalCount: universe.totalCount,
      expectedNote: "Product docs often cite 24; always use live Airtable count — do not hardcode.",
      brands: universe.brands.map((b) => ({
        recordId: b.recordId,
        name: b.name,
        slug: b.slug,
        status: b.status,
      })),
    },
    summary: {
      activeLiveCount: universe.totalCount,
      fetchFailures,
      p1CompleteCount,
      p1IncompleteCount: brands.length - p1CompleteCount,
      avgScoreCriticalRequiredPct: avgRequiredPct,
      brandsWithHighNullDrag: brands.filter((b) => b.nullDragWeightMissingPct >= 40).length,
    },
    scoreCriticalChecks: SCORE_CRITICAL_CHECKS.map(({ isFilled, ...rest }) => rest),
    brands,
    p2FullCompleteness: {
      definition: "All Brand Setup tables used in product intake/library mapping — not only Match Score fields.",
      brandSetupTablesFromInventory: p2BacklogTables,
      inventoryRowCount: inventory.rowCount || (inventory.rows || []).length,
      note: "P2 continues brand-by-brand after P1 score-critical = 100%. Same A/B fill rule; blanks without source stay blank.",
      scoreCriticalKeysExcludedFromP2Focus: [...scoreCriticalKeys],
    },
  };
}

export function buildFounderWorksheet(report) {
  const rows = [];
  for (const brand of report.brands || []) {
    for (const key of brand.blankRequiredKeys || []) {
      const check = SCORE_CRITICAL_CHECKS.find((c) => c.key === key);
      rows.push({
        brandName: brand.name,
        brandSlug: brand.slug,
        brandRecordId: brand.recordId,
        fieldKey: key,
        fieldLabel: check?.label || key,
        airtableTable: check?.table || "",
        softFactor: check?.softFactor || "",
        gate: check?.gate || "",
        weightPct: check?.weightPct ?? 0,
        requiredForP1: true,
        proposedValue: "",
        sourceType: "",
        sourceRef: "",
        filledBy: "",
        filledDate: "",
        notes: "",
      });
    }
  }
  return {
    version: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    instructions: [
      "Fill proposedValue only from A (founder/brand knowledge) or B (existing Dealality docs/FDD/research).",
      "Set sourceType to A or B and sourceRef to person or doc path/date.",
      "Leave proposedValue blank if unknown — do not invent Global priority or Flexible defaults.",
      "Apply via: node scripts/apply-match-score-brand-setup-fills.mjs --worksheet <path> --dry-run then --apply after founder approve.",
    ],
    rowCount: rows.length,
    rows,
  };
}

function csvEscape(val) {
  const s = val == null ? "" : String(val);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function worksheetToCsv(worksheet) {
  const cols = [
    "brandName",
    "brandSlug",
    "brandRecordId",
    "fieldKey",
    "fieldLabel",
    "airtableTable",
    "softFactor",
    "gate",
    "weightPct",
    "proposedValue",
    "sourceType",
    "sourceRef",
    "filledBy",
    "filledDate",
    "notes",
  ];
  const lines = [cols.join(",")];
  for (const r of worksheet.rows || []) {
    lines.push(cols.map((c) => csvEscape(r[c])).join(","));
  }
  return lines.join("\r\n");
}

export function writeMatchScoreBrandSetupGapReports(report, worksheet, { outDir } = {}) {
  const reportsDir = outDir || path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, "match-score-brand-setup-gap-audit.json");
  const mdPath = path.join(reportsDir, "match-score-brand-setup-gap-audit.md");
  const listPath = path.join(reportsDir, "match-score-active-live-brand-list.json");
  const worksheetJsonPath = path.join(reportsDir, "match-score-brand-setup-founder-worksheet.json");
  const worksheetCsvPath = path.join(reportsDir, "match-score-brand-setup-founder-worksheet.csv");
  const p2Path = path.join(reportsDir, "match-score-brand-setup-p2-full-completeness-backlog.md");

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(listPath, JSON.stringify(report.activeUniverse, null, 2), "utf8");
  fs.writeFileSync(worksheetJsonPath, JSON.stringify(worksheet, null, 2), "utf8");
  fs.writeFileSync(worksheetCsvPath, worksheetToCsv(worksheet), "utf8");
  fs.writeFileSync(mdPath, renderGapAuditMarkdown(report), "utf8");
  fs.writeFileSync(p2Path, renderP2BacklogMarkdown(report), "utf8");

  return { jsonPath, mdPath, listPath, worksheetJsonPath, worksheetCsvPath, p2Path };
}

function renderGapAuditMarkdown(report) {
  const lines = [];
  lines.push(`# Match Score Brand Setup Gap Audit`);
  lines.push("");
  lines.push(`- **Version:** ${report.auditVersion}`);
  lines.push(`- **Generated:** ${report.generatedAt}`);
  lines.push(`- **Universe:** ${report.activeUniverse.source.name}`);
  lines.push(`- **Active/Live count (live):** **${report.summary.activeLiveCount}** (do not hardcode 26)`);
  lines.push(`- **P1 complete (score-critical required):** ${report.summary.p1CompleteCount} / ${report.summary.activeLiveCount}`);
  lines.push(`- **Avg score-critical required fill:** ${report.summary.avgScoreCriticalRequiredPct}%`);
  lines.push(`- **Brands with ≥40% null-drag weight missing:** ${report.summary.brandsWithHighNullDrag}`);
  lines.push("");
  lines.push(`## Fill policy`);
  lines.push("");
  lines.push(`- Allowed: A founder/brand knowledge; B existing Dealality docs/FDD/research`);
  lines.push(`- Forbidden: inventing web/AI values; Explorer marketing as fees/geography; Global-priority defaults to juice scores`);
  lines.push(`- Scoring: null soft factors are excluded from the denominator; still complete Brand Setup (min scored weight) for trustworthy published scores`);
  lines.push("");
  lines.push(`## Active/Live brand list`);
  lines.push("");
  for (const b of report.activeUniverse.brands) {
    lines.push(`- \`${b.slug}\` — ${b.name} (\`${b.recordId}\`, status=${b.status || "—"})`);
  }
  lines.push("");
  lines.push(`## Per-brand score-critical fill`);
  lines.push("");
  lines.push(`| Brand | Required % | Null-drag missing wt | Blank required keys | P1 done |`);
  lines.push(`|-------|------------|----------------------|---------------------|---------|`);
  for (const b of report.brands) {
    lines.push(
      `| ${b.name} | ${b.scoreCriticalRequiredPct}% | ${b.nullDragWeightMissingPct}% | ${(b.blankRequiredKeys || []).join(", ") || "—"} | ${b.p1Complete ? "yes" : "no"} |`
    );
  }
  lines.push("");
  lines.push(`## Next steps`);
  lines.push("");
  lines.push(`1. Fill \`reports/match-score-brand-setup-founder-worksheet.csv\` from A/B sources.`);
  lines.push(`2. \`npm run apply-match-score-brand-setup-fills -- --dry-run\``);
  lines.push(`3. Founder approve → \`--apply\``);
  lines.push(`4. Re-run \`npm run audit-match-score-brand-setup-gaps\``);
  lines.push(`5. \`npm run refresh-deal-brand-cache-active-brands\``);
  lines.push("");
  return lines.join("\n");
}

function renderP2BacklogMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Setup P2 — Full Completeness Backlog`);
  lines.push("");
  lines.push(`Definition of done: **full Brand Setup completeness** for Active/Live (not only Match Score score-critical fields).`);
  lines.push("");
  lines.push(`## Brand Setup tables (from library mapping inventory)`);
  lines.push("");
  for (const t of report.p2FullCompleteness.brandSetupTablesFromInventory || []) {
    lines.push(`- ${t}`);
  }
  lines.push("");
  lines.push(`Inventory rows: ${report.p2FullCompleteness.inventoryRowCount}`);
  lines.push("");
  lines.push(`## Rule`);
  lines.push("");
  lines.push(`Same as P1: fill only from A/B. Blank without source stays blank. Do not overwrite company-validated Explorer fields lightly (see DATA_VALIDATION_PROTOCOL).`);
  lines.push("");
  lines.push(`## Sequencing`);
  lines.push("");
  lines.push(`1. Finish P1 (score-critical required = 100% per brand).`);
  lines.push(`2. For each Active/Live brand, walk remaining Brand Setup tabs (Deal Terms, Footprint, remaining Fit/Standards/Basics fields).`);
  lines.push(`3. Founder sign-off per brand.`);
  lines.push("");
  return lines.join("\n");
}
