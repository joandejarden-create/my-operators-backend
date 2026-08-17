/**
 * Wave 13 SO/ — section-pattern failure extraction (report-only).
 * Reads latest tab-factory / PVQL / remediation artifacts. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    console.warn("[so-section-pattern-failures] read failed", rel, err?.message || err);
    return null;
  }
}

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function extractSoSectionPatternFailures() {
  const tabFactory = readJson("reports/brand-explorer-tab-factory-audit.json");
  const so =
    (tabFactory?.brandResults || []).find(
      (b) => nz(b.brandSlug).toLowerCase() === "so-hotels-and-resorts"
    ) || null;
  const sections = so?.sectionPatternParity?.sections || {};
  const momentum = sections.recent_momentum || {};
  const geo = sections.geographic_footprint || {};
  const growth = sections.growth_priorities || {};
  const hold = readJson("reports/brand-explorer-wave13-so-hold-remediation.json");
  const release = readJson("reports/brand-explorer-wave13-so-public-release.json");

  const issues = [
    {
      brand: "SO/",
      brandSlug: "so-hotels-and-resorts",
      recordIdBasics: "recTJdPlr4mDs9app",
      section: "Recent Momentum",
      slotKey: "footprint.momentum",
      recordId: "(live presentation rows — resolve at apply)",
      field: "Title / Body dateLine",
      failureType: "dated_cards_below_min",
      currentValue:
        "dateLine values Directory / Collection (structured but undated vs year/month gate); datedCount=0",
      requiredPattern:
        "≥2 cards with title + year/month dateLine (e.g. Mar 2026) + geography + owner summary + https source; no raw URL in prose",
      proposedFix:
        "Rebuild 2 momentum cards from source pack: Accor Brandbook Mar 2026 + SO/ Paris brand-site listing 2025 (International Reference)",
      sourceSupport:
        "reports/brand-explorer-wave13-source-pack-so-hotels-and-resorts.md Recent Momentum candidates",
      metrics: momentum.metrics || null,
      failures: momentum.failures || [],
    },
    {
      brand: "SO/",
      brandSlug: "so-hotels-and-resorts",
      recordIdBasics: "recTJdPlr4mDs9app",
      section: "Geographic Footprint",
      slotKey: "footprint.region.mea",
      recordId: "(empty MEA panel — resolve at apply)",
      field: "Body / Active / External Display Status",
      failureType: "empty_mea_region_panel",
      currentValue: "footprint.region.mea visible with empty body (words=0)",
      requiredPattern:
        "Fill with source-supported MEA copy OR suppress (Active=false + Do Not Display). No empty visible panel.",
      proposedFix:
        "No source-supported SO/ MEA operating inventory — suppress MEA panel cleanly",
      sourceSupport:
        "Source pack CALA/MEA: none_found; hold remediation geo package omitted MEA intentionally",
      metrics: geo.metrics || null,
      failures: geo.failures || [],
    },
    {
      brand: "SO/",
      brandSlug: "so-hotels-and-resorts",
      recordIdBasics: "recTJdPlr4mDs9app",
      section: "Geographic Footprint",
      slotKey: "footprint.geo_intro (+ regions)",
      recordId: "(geo_intro + region rows)",
      field: "Body",
      failureType: "footprint_not_brand_specific",
      currentValue:
        "brandSpecific() tokens for Basics name SO/ collapse to slug token 'resorts'; corpus often lacks 'resorts'",
      requiredPattern:
        "Brand-specific geo_intro + ≥3 filled regions; SO/ Hotels & Resorts / resorts language present",
      proposedFix:
        "Refresh geo_intro (+ keep filled EU/APAC/AM/CALA) with explicit SO/ Hotels & Resorts / resorts wording; suppress MEA",
      sourceSupport: "so-hotels.com Paris/Maldives + Accor SO/ brand page (International Reference)",
      metrics: geo.metrics || null,
      failures: geo.failures || [],
    },
    {
      brand: "SO/",
      brandSlug: "so-hotels-and-resorts",
      recordIdBasics: "recTJdPlr4mDs9app",
      section: "Growth Priorities",
      slotKey: "footprint.growth_editorial / footprint.growth_themes / footprint.growth_fit",
      recordId: "(growth slots — resolve at apply)",
      field: "Body",
      failureType: "growth_priorities_not_brand_specific",
      currentValue:
        "Themes present (5) + editorial ~32 words but brandSpecific fails (missing 'resorts' token match)",
      requiredPattern:
        "≥2 theme chips + ≥30-word SO/-specific growth editorial (fashion/design-led hotels and resorts, destination energy, F&B intensity)",
      proposedFix:
        "Rewrite growth_themes + growth_editorial/fit as SO/ Hotels & Resorts selective luxury lifestyle growth — distinguish from Mama Shelter / Fairmont / MGallery / generic Accor",
      sourceSupport: "Accor Brandbook + SO/ brand page positioning",
      metrics: growth.metrics || null,
      failures: growth.failures || [],
    },
  ];

  const report = {
    version: "wave13-so-section-pattern-failures-v1",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    brandSlug: "so-hotels-and-resorts",
    recordId: "recTJdPlr4mDs9app",
    sourcesRead: [
      "reports/brand-explorer-tab-factory-audit.json",
      "reports/brand-explorer-wave13-so-hold-remediation.json",
      "reports/brand-explorer-wave13-so-public-release.json",
      "reports/brand-explorer-wave13-source-pack-so-hotels-and-resorts.md",
    ],
    priorReadyStatement:
      release?.readyStatement ||
      "wave13_so_status_and_release_fields_applied_universe_46_pvql_blocked_on_section_pattern_parity",
    holdRemediationVersion: hold?.version || null,
    tabFactoryAuditPass: so?.auditPass === true,
    sectionPatternPass: so?.sectionPatternParity?.pass === true,
    issues,
    issueCount: issues.length,
    readyForCleanupPlan: true,
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-section-pattern-failures.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-section-pattern-failures.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);

  const md = [
    `# Wave 13 SO/ — Section Pattern Failures`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Brand: **SO/** (\`so-hotels-and-resorts\`, \`${report.recordId}\`)`,
    `Tab-factory auditPass: **${report.tabFactoryAuditPass}** · sectionPatternPass: **${report.sectionPatternPass}**`,
    ``,
    `| Brand | Section | Field | Failure Type | Required Pattern | Proposed Fix | Source Support |`,
    `| --- | --- | --- | --- | --- | --- | --- |`,
  ];
  for (const i of issues) {
    md.push(
      `| ${i.brand} | ${i.section} | ${i.field} | \`${i.failureType}\` | ${i.requiredPattern.replace(/\|/g, "/")} | ${i.proposedFix.replace(/\|/g, "/")} | ${i.sourceSupport.replace(/\|/g, "/")} |`
    );
  }
  md.push(
    ``,
    `## Details`,
    ``
  );
  for (const i of issues) {
    md.push(
      `### ${i.section} — \`${i.failureType}\``,
      ``,
      `- Slot: \`${i.slotKey}\``,
      `- Current: ${i.currentValue}`,
      `- Proposed: ${i.proposedFix}`,
      `- Failures: ${(i.failures || []).join(", ") || "—"}`,
      ``
    );
  }
  fs.writeFileSync(mdPath, `${md.join("\n")}\n`);
  return { ...report, paths: { jsonPath, mdPath } };
}

if (import.meta.url === `file://${process.argv[1]?.replace(/\\/g, "/")}` || process.argv[1]?.endsWith("brand-explorer-wave13-so-section-pattern-failures.js")) {
  const r = extractSoSectionPatternFailures();
  console.log(`Wrote ${r.paths.jsonPath}`);
  console.log(`Wrote ${r.paths.mdPath}`);
  console.log(`issues=${r.issueCount}`);
}
