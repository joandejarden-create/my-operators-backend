#!/usr/bin/env node
/**
 * READ-ONLY report generator for Operator Explorer architecture audit.
 * Consumes reports/operator-explorer-architecture-live-schema-dump.json
 * Does not call Airtable write APIs.
 */
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const dump = JSON.parse(
  readFileSync(join(root, "reports/operator-explorer-architecture-live-schema-dump.json"), "utf8")
);
const completeness = JSON.parse(
  readFileSync(join(root, "reports/operator-explorer-architecture-completeness-snapshot.json"), "utf8")
);

const branch = "app-shell-left-nav";
const commit = "3c88c0b4e22a35052e450d00c5e2f1b9e417c040";

mkdirSync(join(root, "reports"), { recursive: true });

const DUMMY_IDS = new Set([
  "recTUjuDxL96yWcQA",
  "recBReJUmxdOUvQzp",
  "recZPHT2zqc8K6itx",
  "recZgNR85WZKDItLF",
  "recbT3q8ApRIBu4j5",
  "reckO98E46sKTn3F3",
  "recq3NiRxOerg4kZU",
  "recwbyY4qfNP1bV3r",
  "recxAa86Qoc0nFRSt",
]);
const RESEARCH_STAGE_IDS = new Set([
  "recjgHXqTJktijFUR",
  "recHj56wpRLUnJ5Wx",
  "rec9JSyGQjvodsPSJ",
]);
const BASELINE_IDS = new Set(["recF5Z87OAqFgndoq", "recWPKu5laVZxsvpn"]);
const CALIBRATION_IDS = new Set([
  "recF5Z87OAqFgndoq",
  "recQ6Cf8O2z0tiqBz",
  "recWPKu5laVZxsvpn",
  "reciI2tYQBfMoMK9G",
  "rec3TUHT9Z4AnFp5P",
  "recGWxIJqnYHkJZFD",
]);
const WAVE2_IDS = new Set([
  "recLjxtxIIVJaGbXK",
  "recfwDdU5t9h4uFnZ",
  "recKVILWcRLqrQlWs",
  "reckyv9O0Y3auYpJJ",
]);

function classify(m) {
  const name = m.company_name || "";
  const status = m.submission_status || "";
  if (DUMMY_IDS.has(m.id)) {
    return {
      class: "Beta / Dummy",
      explorer: false,
      fit: false,
      action:
        "Mark Test Fixture / exclude from Explorer + Fit production; keep until code fixtures replace demos",
    };
  }
  if (RESEARCH_STAGE_IDS.has(m.id) || status === "Research Stage") {
    return {
      class: "Real — Research Stage",
      explorer: false,
      fit: "internal-only",
      action:
        "Keep Research Stage; enrich Argentina presence; do not graduate without founder criteria",
    };
  }
  if (/\(Managed\)$/.test(name)) {
    return {
      class: "Production Real",
      explorer: "partial-brand-managed",
      fit: "brand-managed-path",
      action: "Retain as brand-managed path; not third-party Explorer gold bar",
    };
  }
  if (BASELINE_IDS.has(m.id)) {
    return {
      class: "Production Real",
      explorer: true,
      fit: true,
      action: "Protected quality baseline — do not remediate without baseline revision task",
    };
  }
  if (CALIBRATION_IDS.has(m.id) || WAVE2_IDS.has(m.id)) {
    return {
      class: "Production Real",
      explorer: "eligible-with-gaps",
      fit: "partial",
      action: "Continue intelligence enrichment; promote Explorer only after gates",
    };
  }
  if (status === "Active") {
    return {
      class: "Real — Research Required",
      explorer: "not-yet",
      fit: "thin",
      action: "Queue for research wave; Explorer eligibility after MVOP + evidence",
    };
  }
  if (status === "In Review") {
    return {
      class: "Unclear",
      explorer: false,
      fit: false,
      action: "Confirm real vs dummy; Research Stage if real; Test Only if dummy",
    };
  }
  return {
    class: "Unclear",
    explorer: false,
    fit: false,
    action: "Manual review",
  };
}

const classified = dump.universe.masters.map((m) => ({
  company: m.company_name,
  id: m.id,
  status: m.submission_status,
  ...classify(m),
}));
const counts = {};
for (const c of classified) counts[c.class] = (counts[c.class] || 0) + 1;

const tablePurpose = {
  "Operator Setup - Master": {
    purpose: "Canonical operator/company record for Explorer + Fit",
    entity: "Operator",
    op: true,
    prod: true,
    explorer: true,
    fit: true,
    workflow: false,
  },
  "Operator Setup - Profile & Positioning": {
    purpose: "Scale, brands, service models, positioning",
    entity: "Operator child",
    op: true,
    prod: true,
    explorer: true,
    fit: true,
    workflow: false,
  },
  "Operator Setup - Platform & Markets": {
    purpose: "Geography, markets, platform JSON",
    entity: "Operator child",
    op: true,
    prod: true,
    explorer: true,
    fit: true,
    workflow: false,
  },
  "Operator Setup - Commercial Fit & Terms": {
    purpose: "Structures, openings, commercial/bf_*",
    entity: "Operator child",
    op: true,
    prod: true,
    explorer: true,
    fit: true,
    workflow: false,
  },
  "Operator Setup - Governance, Delivery & Diligence": {
    purpose: "Services, reporting, RM/F&B capability",
    entity: "Operator child",
    op: true,
    prod: true,
    explorer: true,
    fit: true,
    workflow: false,
  },
  "Operator Setup - Case Studies": {
    purpose: "Property proof / comparables (sparse structured)",
    entity: "Assignment-like",
    op: true,
    prod: true,
    explorer: true,
    fit: "narrative-only",
    workflow: false,
  },
  "Operator Setup - Explorer Materials": {
    purpose: "Media/presentation assets",
    entity: "Presentation",
    op: true,
    prod: true,
    explorer: true,
    fit: false,
    workflow: false,
  },
  "Operator Setup - Leadership Platform": {
    purpose: "Leadership tab platform rows",
    entity: "Presentation",
    op: true,
    prod: true,
    explorer: true,
    fit: false,
    workflow: false,
  },
  "Operator Setup - Leadership Team Members": {
    purpose: "Named leadership people",
    entity: "People",
    op: true,
    prod: true,
    explorer: true,
    fit: false,
    workflow: false,
  },
  "Operator Setup - Engagement & Reporting": {
    purpose: "Owner engagement reporting rows",
    entity: "Presentation",
    op: true,
    prod: true,
    explorer: true,
    fit: false,
    workflow: false,
  },
  "Operator Setup - Operating Platform": {
    purpose: "Operating platform explorer rows",
    entity: "Presentation",
    op: true,
    prod: true,
    explorer: true,
    fit: false,
    workflow: false,
  },
  "Operator Setup - Brand Relationships": {
    purpose: "Explorer Brand tab presentation rows (NOT normalized brand-approval graph)",
    entity: "Presentation",
    op: true,
    prod: true,
    explorer: true,
    fit: false,
    workflow: false,
  },
  "Operator Setup - Infrastructure Platform": {
    purpose: "Infrastructure explorer rows",
    entity: "Presentation",
    op: true,
    prod: true,
    explorer: true,
    fit: false,
    workflow: false,
  },
  "Operator Setup - Diligence QA": {
    purpose: "Diligence QA checklist child",
    entity: "Workflow",
    op: true,
    prod: "partial",
    explorer: false,
    fit: false,
    workflow: true,
  },
  "Operator Deal Requests": {
    purpose: "Deal↔operator outreach junction + stored alignment",
    entity: "Deal workflow",
    op: true,
    prod: true,
    explorer: false,
    fit: "snapshot",
    workflow: true,
  },
  "Operator Intelligence - Claims": {
    purpose: "Structured claim spine for research auditability",
    entity: "Claim",
    op: true,
    prod: "pilot",
    explorer: "evidence",
    fit: "evidence",
    workflow: false,
  },
  "Operator Intelligence - Market Presence": {
    purpose: "Normalized country presence types",
    entity: "Presence",
    op: true,
    prod: "pilot",
    explorer: true,
    fit: true,
    workflow: false,
  },
  "Operator Fit - Shortlist": {
    purpose: "Internal pilot shortlist + immutable snapshot",
    entity: "Fit workflow",
    op: true,
    prod: "internal-pilot",
    explorer: false,
    fit: true,
    workflow: true,
  },
  "Partner Intelligence - Source Library": {
    purpose: "Shared evidence sources (brand+operator)",
    entity: "Source",
    op: true,
    prod: true,
    explorer: true,
    fit: "evidence",
    workflow: false,
  },
  "Partner Intelligence - Extracted Facts": {
    purpose: "Extracted facts before publish",
    entity: "Fact",
    op: true,
    prod: true,
    explorer: "pipeline",
    fit: false,
    workflow: false,
  },
  "Partner Intelligence - Published Explorer Fields": {
    purpose: "Published explorer field registry",
    entity: "Publish",
    op: true,
    prod: true,
    explorer: true,
    fit: false,
    workflow: false,
  },
  "Partner Intelligence - Helena Outreach Intake": {
    purpose: "Helena outreach intake",
    entity: "Outreach",
    op: true,
    prod: "partial",
    explorer: false,
    fit: false,
    workflow: true,
  },
  "Partner Intelligence - Brand Asset Registry": {
    purpose: "Brand image/asset registry (brand-primary)",
    entity: "Asset",
    op: false,
    prod: true,
    explorer: false,
    fit: false,
    workflow: false,
  },
  "Company Profile": {
    purpose: "Platform commercial company onboarding (multi-role)",
    entity: "Company (platform)",
    op: "partial",
    prod: true,
    explorer: false,
    fit: false,
    workflow: true,
  },
  "Companies": {
    purpose: "Outreach CRM companies",
    entity: "Company (outreach)",
    op: "partial",
    prod: "outreach",
    explorer: false,
    fit: false,
    workflow: true,
  },
  "Contacts": {
    purpose: "People contacts",
    entity: "Contact",
    op: "partial",
    prod: true,
    explorer: false,
    fit: false,
    workflow: true,
  },
};

function esc(s) {
  return String(s ?? "")
    .replace(/\|/g, "\\|")
    .replace(/\n/g, " ");
}

function opts(f) {
  if (!f.options) return "—";
  if (f.options.choices) return `options: ${f.options.choices.join("; ")}`;
  if (f.options.linkedTableId) return `link→${f.options.linkedTableId}`;
  if (f.options.formula) return `formula: ${String(f.options.formula).slice(0, 80)}`;
  if (f.options.recordLinkFieldId) return `lookup/rollup via ${f.options.recordLinkFieldId}`;
  return "—";
}

function purposeGuess(table, field, type) {
  if (type === "multipleRecordLinks") return "Link to related records";
  if (/display_order|row_key|section|title|subtitle|body|extra/i.test(field)) {
    return "Explorer presentation row";
  }
  if (/^bf_/i.test(field)) return "Best-fit / commercial narrative or structure";
  if (/ov_|cap_|dna_/i.test(field)) return "Legacy/card presentation blob";
  return "See schema docs / bindings";
}

let tableMd = `# Operator Explorer — Airtable Table Inventory

**Mode:** Read-only live meta audit  
**Base:** \`appvtnDurnMSjINP6\` (\`AIRTABLE_BASE_ID\`)  
**Generated:** ${dump.generatedAt}  
**Branch/Commit:** ${branch} / ${commit.slice(0, 7)}

| Table | Airtable ID | Purpose | Primary Entity | Operator Relevant? | Production Used? | Explorer Relevant? | Fit Relevant? | Workflow Only? | Fields |
| ----- | ----------- | ------- | -------------- | ------------------ | ---------------- | ------------------ | ------------- | -------------- | -----: |
`;

for (const t of dump.tableInventory) {
  const p = tablePurpose[t.name] || {
    purpose: "Review required",
    entity: "—",
    op: "review",
    prod: "unknown",
    explorer: "unknown",
    fit: "unknown",
    workflow: false,
  };
  tableMd += `| ${t.name} | \`${t.id}\` | ${p.purpose} | ${p.entity} | ${p.op} | ${p.prod} | ${p.explorer} | ${p.fit} | ${p.workflow} | ${t.fieldCount} |\n`;
}

tableMd += `
## Notes

- Legacy \`3rd Party Operator - *\` tables were **not present** in live base meta (96 tables). Treat as retired or moved; some code maps may still reference legacy names.
- Canonical Operator Master for Explorer/Fit is **Operator Setup - Master**, not \`Companies\` or \`Company Profile\`.
- \`Operator Setup - Brand Relationships\` is a **presentation row store**, not a normalized brand-approval graph.
- Machine dump: \`reports/operator-explorer-architecture-live-schema-dump.json\` (${dump.fieldInventoryCount} fields across ${dump.tableInventory.length} tables).
- Volume snapshot: Claims=${dump.universe.claimsCount}, Market Presence=${dump.universe.marketPresenceCount}, Case Studies=${dump.universe.caseStudiesCount}, Brand Relationships rows=${dump.universe.brandRelationshipsCount}, Shortlist=${dump.universe.shortlistCount}, Masters=${dump.universe.masterCount}.
`;
writeFileSync(join(root, "reports/operator-explorer-airtable-table-inventory.md"), tableMd);

const measured = {
  "Operator Setup - Master|Data Confidence Level": completeness.completeness["M:Data Confidence Level"],
  "Operator Setup - Master|Source Type": completeness.completeness["M:Source Type"],
  "Operator Setup - Master|Last Updated Date": completeness.completeness["M:Last Updated Date"],
  "Operator Setup - Master|Validation Status": completeness.completeness["M:Validation Status"],
  "Operator Setup - Master|Usage Permission": completeness.completeness["M:Usage Permission"],
  "Operator Setup - Master|Company Validated": completeness.completeness["M:Company Validated"],
  "Operator Setup - Master|External Display Status": completeness.completeness["M:External Display Status"],
  "Operator Setup - Master|company_name": { pct: 100 },
  "Operator Setup - Master|submission_status": { pct: 100 },
  "Operator Setup - Platform & Markets|Active Countries": completeness.completeness["P:Active Countries"],
  "Operator Setup - Platform & Markets|Active Markets / Cities":
    completeness.completeness["P:Active Markets / Cities"],
  "Operator Setup - Platform & Markets|Market Presence Type":
    completeness.completeness["P:Market Presence Type"],
  "Operator Setup - Profile & Positioning|chainScalesSupported":
    completeness.completeness["PR:chainScalesSupported"],
  "Operator Setup - Profile & Positioning|Service Models Supported":
    completeness.completeness["PR:Service Models Supported"],
  "Operator Setup - Profile & Positioning|brands": completeness.completeness["PR:brands"],
  "Operator Setup - Profile & Positioning|Brand Families Operated":
    completeness.completeness["PR:Brand Families Operated"],
  "Operator Setup - Commercial Fit & Terms|Management Structures Supported":
    completeness.completeness["C:Management Structures Supported"],
  "Operator Setup - Commercial Fit & Terms|New-Build Opening Experience":
    completeness.completeness["C:New-Build Opening Experience"],
  "Operator Setup - Commercial Fit & Terms|Pre-Opening Support Capability":
    completeness.completeness["C:Pre-Opening Support Capability"],
  "Operator Setup - Commercial Fit & Terms|Conversion / Reflag Experience":
    completeness.completeness["C:Conversion / Reflag Experience"],
  "Operator Setup - Commercial Fit & Terms|bf_selected_deal_structures":
    completeness.completeness["C:bf_selected_deal_structures"],
  "Operator Setup - Commercial Fit & Terms|bf_not_ideal_for":
    completeness.completeness["C:bf_not_ideal_for"],
  "Operator Setup - Governance, Delivery & Diligence|Offered Services":
    completeness.completeness["G:Offered Services"],
  "Operator Setup - Governance, Delivery & Diligence|Owner Reporting Level":
    completeness.completeness["G:Owner Reporting Level"],
  "Operator Setup - Governance, Delivery & Diligence|Governance Cadence":
    completeness.completeness["G:Governance Cadence"],
  "Operator Setup - Governance, Delivery & Diligence|Revenue Management Capability":
    completeness.completeness["G:Revenue Management Capability"],
  "Operator Setup - Governance, Delivery & Diligence|Sales Platform":
    completeness.completeness["G:Sales Platform"],
  "Operator Setup - Governance, Delivery & Diligence|F&B Capability Level":
    completeness.completeness["G:F&B Capability Level"],
};

let fieldMd = `# Operator Explorer — Airtable Field Inventory

**Mode:** Complete live schema audit (no sampling of fields)  
**Fields audited:** ${dump.fieldInventoryCount}  
**Generated:** ${dump.generatedAt}  
**Active completeness sample:** n=${completeness.activeCount} where measured; otherwise \`n/a\`

| Table | Field | Field ID | Field Type | Linked To / Options | Current Purpose | Populated % | Code Reads? | Code Writes? | Explorer Candidate? | Fit Input? |
| ----- | ----- | -------- | ---------- | ------------------- | --------------- | ----------: | ----------- | ------------ | ------------------- | ---------- |
`;

for (const f of dump.fieldInventory) {
  const key = `${f.table}|${f.field}`;
  const pop = measured[key]?.pct;
  const popStr = pop == null ? "n/a" : String(pop);
  let reads = "see dependency map";
  let writes = "see dependency map";
  let expl = "maybe";
  let fit = "no";
  if (f.table.includes("Claims") || f.table.includes("Market Presence")) {
    expl = "evidence";
    fit = "evidence";
    reads = "yes";
    writes = "research scripts";
  }
  if (f.table.includes("Shortlist") || f.table.includes("Deal Requests")) {
    expl = "no";
    fit = "workflow";
    reads = "yes";
    writes = "yes";
  }
  if (
    f.table.startsWith("Operator Setup") &&
    !/Leadership|Materials|Engagement|Operating Platform|Infrastructure|Brand Relationships|Diligence/.test(
      f.table
    )
  ) {
    expl = "yes";
    fit = /Active Countries|chainScale|Management Structure|Conversion|brands|submission_status|company_name|Offered Services|Market Presence/i.test(
      f.field
    )
      ? "yes"
      : "maybe";
  }
  if (
    /Brand Relationships|Explorer Materials|Leadership|Engagement|Operating Platform|Infrastructure/.test(
      f.table
    )
  ) {
    expl = "yes";
    fit = "no";
  }
  if (f.table === "Company Profile" || f.table === "Companies" || f.table === "Contacts") {
    expl = "no";
    fit = "no";
  }
  fieldMd += `| ${esc(f.table)} | ${esc(f.field)} | \`${f.fieldId}\` | ${f.type} | ${esc(opts(f)).slice(0, 140)} | ${esc(purposeGuess(f.table, f.field, f.type))} | ${popStr} | ${reads} | ${writes} | ${expl} | ${fit} |\n`;
}
writeFileSync(join(root, "reports/operator-explorer-airtable-field-inventory.md"), fieldMd);

let uni = `# Operator Explorer — Current Universe Audit

**Live Master count:** ${classified.length}  
**Classification counts:** ${JSON.stringify(counts)}  
**Generated:** ${dump.generatedAt}

| Company | Current ID | Status | Classification | Explorer Eligible? | Fit Eligible? | Action Recommendation |
| ------- | ---------- | ------ | -------------- | ------------------ | ------------- | --------------------- |
`;

for (const c of classified.sort((a, b) => String(a.company).localeCompare(String(b.company)))) {
  uni += `| ${esc(c.company)} | \`${c.id}\` | ${c.status} | ${c.class} | ${c.explorer} | ${c.fit} | ${esc(c.action)} |\n`;
}

uni += `
## Dummy / test identification

Explicitly treated as **Beta / Dummy** (In Review factory/demo names; Antillano Norte documented as sample/demo in protected baseline rules):

`;
for (const id of DUMMY_IDS) {
  const c = classified.find((x) => x.id === id);
  if (c) uni += `- **${c.company}** (\`${id}\`)\n`;
}

uni += `
## Research Stage (real Argentina wave)

`;
for (const id of RESEARCH_STAGE_IDS) {
  const c = classified.find((x) => x.id === id);
  if (c) uni += `- **${c.company}** (\`${id}\`)\n`;
}

uni += `
## Notes

- No records deleted in this audit.
- Brand-managed Active rows are real parent companies but are not third-party Operator Explorer gold profiles.
- \`Companies\` (n=${dump.universe.companiesCount}) and \`Company Profile\` (n=${dump.universe.companyProfileCount}) are separate platform/outreach entities — do not conflate with Operator Setup Master.
`;
writeFileSync(join(root, "reports/operator-explorer-current-universe-audit.md"), uni);

writeFileSync(
  join(root, "reports/operator-explorer-architecture-classification-summary.json"),
  JSON.stringify(
    {
      counts,
      classified,
      fieldCount: dump.fieldInventoryCount,
      tableCount: dump.tableInventory.length,
      claims: dump.universe.claimsCount,
      presence: dump.universe.marketPresenceCount,
      caseStudies: dump.universe.caseStudiesCount,
      brandRels: dump.universe.brandRelationshipsCount,
    },
    null,
    2
  )
);

console.log(
  JSON.stringify(
    {
      ok: true,
      counts,
      fields: dump.fieldInventoryCount,
      tables: dump.tableInventory.length,
    },
    null,
    2
  )
);
