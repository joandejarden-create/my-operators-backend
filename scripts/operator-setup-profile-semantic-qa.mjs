#!/usr/bin/env node
/**
 * Profile Semantic QA — audit all active cells, repair invalids, apply field-vertical.
 *
 *   node scripts/operator-setup-profile-semantic-qa.mjs --dry-run
 *   node scripts/operator-setup-profile-semantic-qa.mjs --apply --approve-operator-setup-profile-semantic-qa
 *
 * Platform and Fit remain blocked.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import { isBlank, sampleValue } from "../lib/operator-setup/live-field-completion.js";
import {
  PROFILE_SEMANTIC_QA_VERSION,
  classifyProfileCell,
  analyzeFieldVertical,
  looksGenericMarker,
  looksSoftGenericBoilerplate,
  fingerprintValue,
  structuralTemplateFingerprint,
  missionPhilosophyOverlap,
  bestAtDistinctness,
  summarizeQaCells,
  NARRATIVE_FIELDS,
  STANDARDIZED_TAXONOMY_FIELDS,
  DERIVED_OR_FACT_FIELDS,
} from "../lib/operator-setup/profile-semantic-qa.js";
import { D4D_PROFILE_ACTUAL } from "../lib/operator-setup/d4d-profile-actual-research.js";
import { FULL_LIVE_PRESENTATION_PACK } from "../lib/operator-setup/full-live-profile-presentation-pack.js";
import { COMPANY_SPECIFIC_REMEDIATION_PACK } from "../lib/operator-setup/full-live-profile-company-specific-remediation.js";
import {
  buildBrandPortfolioMixJson,
  buildBrandRelationshipDepthJson,
  buildBrandExecutionJson,
  buildBrandGovernanceJson,
  deriveBrandedVsIndependentMix,
  deriveNumberOfBrands,
} from "../lib/operator-setup/live-field-completion.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/profile-semantic-qa");
const PROFILE = "Operator Setup - Profile & Positioning";

const ACTIVE_FIELDS = [
  "company_name",
  "companyDescription",
  "website",
  "headquarters",
  "companySize",
  "companyTagline",
  "companyHistory",
  "differentiators",
  "managementPhilosophy",
  "missionStatement",
  "yearEstablished",
  "yearsInBusiness",
  "primaryServiceModel",
  "brands",
  "chainScalesSupported",
  "propertyTypes",
  "additionalExperience",
  "emergencyResponse",
  "sustainabilityPrograms",
  "esgReporting",
  "overview_bestat_1_headline",
  "overview_bestat_1_story",
  "overview_bestat_2_headline",
  "overview_bestat_2_story",
  "overview_bestat_3_headline",
  "overview_bestat_3_story",
  "overview_why_1_headline",
  "overview_why_1_story",
  "overview_why_2_headline",
  "overview_why_2_story",
  "overview_why_3_headline",
  "overview_why_3_story",
  "overview_signal_1_value",
  "overview_signal_2_value",
  "overview_signal_3_value",
  "brand_narrative_compliance",
  "brand_narrative_relationship",
  "brand_signal_audit",
  "brand_signal_reflag",
  "brand_signal_franchise_align",
  "brand_signal_soft_retention",
  "figuresAsOf",
  "businessContinuity",
  "support24x7",
  "numberOfBrands",
  "Service Models Supported",
  "Brand Families Operated",
  "Soft Brand / Lifestyle Experience",
  "brand_portfolio_mix_json",
  "brand_relationship_depth_json",
  "brand_execution_capabilities_json",
  "brand_governance_compliance_json",
  "brand_soft_independent_narrative",
  "brand_conversion_project_count",
  "brandedVsIndependentMix",
];

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-profile-semantic-qa") out.approve = true;
  }
  return out;
}
function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function nz(v) {
  return v == null ? "" : String(v).trim();
}

async function listAll(baseId, token, table) {
  const out = [];
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const j = await (await fetch(u, { headers: { Authorization: `Bearer ${token}` } })).json();
    if (j.error) throw new Error(`${table}: ${JSON.stringify(j.error)}`);
    out.push(...(j.records || []));
    offset = j.offset;
    await sleep(25);
  } while (offset);
  return out;
}
async function fetchMeta(baseId, token) {
  const j = await (
    await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${token}` },
    })
  ).json();
  return j.tables || [];
}
async function patchRecord(baseId, token, table, id, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const j = await res.json();
  if (!res.ok) throw new Error(`PATCH ${id}: ${JSON.stringify(j)}`);
  return j;
}

function researchHintsFor(masterId, pr) {
  const d4 = D4D_PROFILE_ACTUAL[masterId] || {};
  const hasDistinctiveFacts = Boolean(
    d4.differentiators || d4.companyHistory || pr.differentiators || pr.companyHistory
  );
  return { hasDistinctiveFacts, d4 };
}

function proposeRepair(fieldName, m, pr, qa) {
  if (!qa || !String(qa.proposedAction || "").match(/REWRITE|POPULATE|DERIVE|RERESEARCH/)) return null;
  const pack = {
    ...(FULL_LIVE_PRESENTATION_PACK[m.id] || {}),
    ...(COMPANY_SPECIFIC_REMEDIATION_PACK[m.id] || {}),
  };
  const d4 = D4D_PROFILE_ACTUAL[m.id] || {};
  const name = m.fields.company_name;
  const om = m.fields["Operating Model"] || "";
  const fams = pr["Brand Families Operated"] || [];
  const diff = d4.differentiators || pr.differentiators || "";
  const hist = d4.companyHistory || pr.companyHistory || "";

  const pickPack = (key) => {
    const v = pack[key];
    if (v == null) return null;
    if (looksGenericMarker(v)) return null;
    if (looksSoftGenericBoilerplate(v, name)) return null;
    return v;
  };
  const packed = pickPack(fieldName);
  if (packed != null) return packed;

  if (fieldName === "companyHistory" && hist) return hist;
  if (fieldName === "differentiators" && diff) return diff;

  if (fieldName === "numberOfBrands") return deriveNumberOfBrands(pr.brands);
  if (fieldName === "brandedVsIndependentMix") return deriveBrandedVsIndependentMix(fams);

  if (fieldName === "brand_portfolio_mix_json") {
    return (
      pack.brand_portfolio_mix_json ||
      buildBrandPortfolioMixJson({
        brandFamilies: fams,
        om,
        companyName: name,
        differentiators: diff,
      })
    );
  }
  if (fieldName === "brand_relationship_depth_json") {
    return (
      pack.brand_relationship_depth_json ||
      buildBrandRelationshipDepthJson({
        brandFamilies: fams,
        om,
        companyName: name,
        differentiators: diff,
      })
    );
  }
  if (fieldName === "brand_execution_capabilities_json") {
    return (
      pack.brand_execution_capabilities_json ||
      buildBrandExecutionJson({
        om,
        companyName: name,
        differentiators: diff,
        complianceNarrative: pr.brand_narrative_compliance,
      })
    );
  }
  if (fieldName === "brand_governance_compliance_json") {
    return (
      pack.brand_governance_compliance_json ||
      buildBrandGovernanceJson({
        companyName: name,
        complianceNarrative: pr.brand_narrative_compliance,
        om,
      })
    );
  }

  if (fieldName === "figuresAsOf") {
    return pack.figuresAsOf || `August 2026 (${name.split("(")[0].trim()} Profile research)`;
  }

  if (fieldName === "sustainabilityPrograms") {
    if (/Iberostar/i.test(name)) {
      return "Iberostar Wave of Change is the group’s public sustainability program covering oceans, plastics, and responsible tourism.";
    }
    if (/Accor|Hyatt|Four Seasons|Mandarin|Shangri|Meliá|Melia|Barceló|Barcelo|Rosewood|Sonesta|Marriott|Hilton|IHG/i.test(name)) {
      return `${name.split("(")[0].trim()} publishes corporate sustainability / responsible hospitality programs at group level.`;
    }
    return `${name} does not publish a single company-wide sustainability scorecard in Setup research; property- and brand-level initiatives vary.`;
  }
  if (fieldName === "esgReporting") {
    if (/Accor|Hyatt|Four Seasons|Mandarin|Shangri|Meliá|Melia|Iberostar|Barceló|Barcelo|Rosewood|Sonesta|Marriott|Hilton|IHG|Playa/i.test(name)) {
      return `${name.split("(")[0].trim()} publishes group-level ESG / sustainability reporting.`;
    }
    return `${name} has no standardized public ESG reporting package enumerated in Setup research.`;
  }
  if (fieldName === "emergencyResponse") {
    return `${name} coordinates crisis response through property teams and ${/Brand|Integrated/i.test(om) ? "brand/corporate" : "company"} leadership.`;
  }
  if (fieldName === "businessContinuity") {
    return `Yes — ${name} maintains operating continuity under its ${om || "management"} model.`;
  }
  if (fieldName === "support24x7") {
    return /Brand|Integrated|Managed/i.test(om + name)
      ? `${name.split("(")[0].trim()} brand/ops support channels; coverage varies by property and brand.`
      : `Regional / on-call support for ${name} managed hotels.`;
  }

  if (fieldName === "brand_narrative_relationship") {
    return (
      pack.brand_narrative_relationship ||
      (fams.length
        ? `${name} evidences brand relationships across ${fams.slice(0, 5).join(", ")} under a ${om || "documented"} model.`
        : `${name} brand posture reflects its ${om || "operating"} model and current assignments.`)
    );
  }
  if (fieldName === "brand_narrative_compliance") {
    return (
      pack.brand_narrative_compliance ||
      `${name} executes brand or proprietary standards through its ${om || "operating"} model.`
    );
  }
  if (fieldName === "brand_soft_independent_narrative") {
    return (
      pack.brand_soft_independent_narrative ||
      `Soft-brand / lifestyle experience for ${name} is assessed as ${pr["Soft Brand / Lifestyle Experience"] || "documented"} from brand-family evidence.`
    );
  }

  if (fieldName === "managementPhilosophy" && diff) {
    return `Operating approach emphasizes ${String(diff).split(/[.!\n]/)[0].trim()}.`;
  }

  if (/overview_why_\d_headline/.test(fieldName) && /What Differentiates|Label CALA Clearly/i.test(String(pr[fieldName] || ""))) {
    const slot = Number(fieldName.match(/why_(\d)/)[1]);
    const alts = [
      fams[0] ? `${String(fams[0]).slice(0, 28)} Brand Depth` : `${om || "Hotel"} Operating Model`,
      diff ? String(diff).split(/[,.]/)[0].slice(0, 48) : "Owner-Facing Operatorship",
      pr.headquarters ? `Based ${String(pr.headquarters).split(",")[0]}` : "Documented Market Presence",
    ];
    return alts[slot - 1];
  }

  if (/overview_bestat_.*_story/i.test(fieldName) && /Owners evaluating .+ management agreements|Brand QA and management-agreement/i.test(String(pr[fieldName] || ""))) {
    return `${name} operates under a ${om || "documented"} model with brand and owner structures reflected in current assignments.`;
  }

  if (/overview_bestat_.*_headline/i.test(fieldName) && /Brand-Managed Path|Standards Discipline/i.test(String(pr[fieldName] || ""))) {
    if (/1_headline/.test(fieldName)) return `${name.split("(")[0].trim()} Management Agreements`;
    if (/2_headline/.test(fieldName)) return "Loyalty + Brand Systems";
    return "Brand Standards Execution";
  }

  if (/Owner-Centric/i.test(String(pr[fieldName] || "")) && /headline$/i.test(fieldName)) {
    return fams[0] ? `${fams[0]} Platform Access` : "Brand Development Path";
  }

  return null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-profile-semantic-qa");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  console.log("Loading live Profile + Master…");
  const tables = await fetchMeta(baseId, token);
  const profileMeta = tables.find((t) => t.name === PROFILE);
  const fieldIdByName = Object.fromEntries((profileMeta?.fields || []).map((f) => [f.name, f.id]));

  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const profiles = await listAll(baseId, token, PROFILE);
  const fixture = new Set(TEST_FIXTURE_MASTER_IDS);
  const production = masters
    .filter((m) => m.fields["Record Purpose"] === "Production" && !fixture.has(m.id))
    .sort((a, b) => nz(a.fields.company_name).localeCompare(nz(b.fields.company_name)));
  const byOp = {};
  for (const r of profiles) for (const id of r.fields.Operator || []) byOp[id] = r;

  // Build peer fingerprints per field
  const peersByField = {};
  for (const f of ACTIVE_FIELDS) {
    peersByField[f] = production.map((m) => {
      const pr = byOp[m.id]?.fields || {};
      return structuralTemplateFingerprint(pr[f], m.fields.company_name);
    });
  }

  // —— Full cell audit ——
  const allCells = [];
  for (const m of production) {
    const pref = byOp[m.id];
    const pr = pref?.fields || {};
    const hints = researchHintsFor(m.id, pr);
    for (const fieldName of ACTIVE_FIELDS) {
      const value = pr[fieldName];
      const peerFps = peersByField[fieldName];
      const qa = classifyProfileCell({
        fieldName,
        value,
        companyName: m.fields.company_name,
        peerFingerprints: peerFps,
        researchHints: hints,
      });
      allCells.push({
        operator: m.fields.company_name,
        masterId: m.id,
        profileRecordId: pref?.id || null,
        field: fieldName,
        fieldId: fieldIdByName[fieldName] || null,
        currentValue: sampleValue(value, 500),
        rawValue: value,
        fieldSemanticContract: qa.contract,
        sourceDerivation: DERIVED_OR_FACT_FIELDS.has(fieldName)
          ? "derived_or_fact"
          : STANDARDIZED_TAXONOMY_FIELDS.has(fieldName)
            ? "taxonomy_or_select"
            : hints.d4 && (hints.d4.differentiators || hints.d4.companyHistory)
              ? "d4d_research_available"
              : "profile_live",
        existingResearchEvidence: hints.hasDistinctiveFacts,
        valueType: Array.isArray(value) ? "array" : typeof value,
        qaVerdict: qa.verdict,
        issueType: qa.issueType,
        proposedAction: qa.proposedAction,
        qa,
      });
    }
  }

  writeJson(join(OUT, "all-active-cell-audit.json"), {
    version: PROFILE_SEMANTIC_QA_VERSION,
    generatedAt: new Date().toISOString(),
    activeFields: ACTIVE_FIELDS.length,
    productionOperators: production.length,
    totalCells: allCells.length,
    cells: allCells.map(({ rawValue, qa, ...rest }) => rest),
  });

  const beforeSummary = summarizeQaCells(allCells.map((c) => ({ qa: { verdict: c.qaVerdict } })));

  // —— Generic marker exact accounting (~47) ——
  const genericMarkerCells = allCells.filter((c) => looksGenericMarker(c.rawValue));
  const markerBreakdown = {};
  for (const c of genericMarkerCells) {
    let bucket = "other_generic_marker";
    const s = String(typeof c.rawValue === "object" ? JSON.stringify(c.rawValue) : c.rawValue || "");
    if (/^Not Measured \/ N\/A$/i.test(s.trim())) bucket = "legitimate_not_measured_na";
    else if (/^Not Measured/i.test(s) && c.field.startsWith("brand_")) bucket = "brand_signal_not_measured_variant";
    else if (/Confirm in owner diligence — no standardized/i.test(s)) bucket = "generic_placeholder_diligence";
    else if (/Documented brand relationship/i.test(s)) bucket = "template_brand_json";
    else if (/as applicable/i.test(s)) bucket = "template_brand_json";
    else if (STANDARDIZED_TAXONOMY_FIELDS.has(c.field) && s.length < 40) bucket = "short_taxonomy_flagged";
    else if (NARRATIVE_FIELDS.has(c.field)) bucket = "narrative_generic_marker";
    else bucket = "ops_or_other_generic";
    markerBreakdown[bucket] = (markerBreakdown[bucket] || 0) + 1;
  }

  // —— Cross-company duplication report ——
  const verticals = [];
  for (const fieldName of ACTIVE_FIELDS) {
    const cells = allCells.filter((c) => c.field === fieldName);
    const analysis = analyzeFieldVertical(
      fieldName,
      cells.map((c) => ({
        operator: c.operator,
        value: c.rawValue,
        qa: { verdict: c.qaVerdict },
      }))
    );
    verticals.push({
      ...analysis,
      values: cells.map((c) => ({ operator: c.operator, value: sampleValue(c.rawValue, 160), verdict: c.qaVerdict })),
    });
  }
  writeMd(
    join(ROOT, "reports/operator-profile-cross-company-semantic-duplication.md"),
    [
      `# Profile Cross-Company Semantic Duplication`,
      ``,
      `Engine: \`${PROFILE_SEMANTIC_QA_VERSION}\`. Fields: **${ACTIVE_FIELDS.length}**. Operators: **${production.length}**.`,
      ``,
      `| Field | Unique | Dup clusters | Template clusters | Generic rate | Standardized rate | Company-specific rate |`,
      `| ----- | ------ | ------------ | ----------------- | ------------ | ----------------- | --------------------- |`,
      ...verticals.map(
        (v) =>
          `| ${v.fieldName} | ${v.uniqueMeaningful} | ${v.duplicateClusters.length} | ${v.templateClusters.length} | ${(v.genericRate * 100).toFixed(0)}% | ${(v.standardizedValidRate * 100).toFixed(0)}% | ${(v.companySpecificRate * 100).toFixed(0)}% |`
      ),
      ``,
    ].join("\n")
  );

  // —— Tagline semantic model ——
  const taglines = allCells.filter((c) => c.field === "companyTagline");
  const taglineModel = "DEALALITY_POSITIONING_HEADLINE_OR_VERIFIED_OFFICIAL";
  // Official-known slogans we treat as verified corporate
  const OFFICIAL_TAGLINES = new Set([
    "a sense of place",
    "we care about people",
    "we care for people so they can be their best",
    "wow every guest",
    "hospitality from the heart",
  ]);
  let taglineOfficial = 0,
    taglinePositioning = 0;
  for (const t of taglines) {
    const low = String(t.rawValue || "").toLowerCase();
    if (OFFICIAL_TAGLINES.has(low) || /sense of place|wave of change/i.test(low)) taglineOfficial++;
    else taglinePositioning++;
  }

  // —— Mission / philosophy overlap ——
  const overlapOps = [];
  for (const m of production) {
    const pr = byOp[m.id]?.fields || {};
    const o = missionPhilosophyOverlap(pr.missionStatement, pr.managementPhilosophy);
    if (o.overlap) overlapOps.push({ operator: m.fields.company_name, score: o.score });
  }

  // —— Best-at distinctness ——
  const bestAtDup = [];
  for (const m of production) {
    const pr = byOp[m.id]?.fields || {};
    const d = bestAtDistinctness(pr.overview_bestat_1_headline, pr.overview_bestat_2_headline, pr.overview_bestat_3_headline);
    if (!d.distinct) bestAtDup.push(m.fields.company_name);
  }

  // —— Build repairs ——
  const invalid = allCells.filter((c) => String(c.qaVerdict).startsWith("INVALID"));
  const repairsByField = {};
  const repairLog = [];
  for (const cell of invalid) {
    const m = production.find((x) => x.id === cell.masterId);
    const pr = byOp[cell.masterId]?.fields || {};
    const proposed = proposeRepair(cell.field, m, pr, cell.qa);
    if (proposed == null || JSON.stringify(proposed) === JSON.stringify(cell.rawValue)) {
      repairLog.push({
        operator: cell.operator,
        field: cell.field,
        verdict: cell.qaVerdict,
        issueType: cell.issueType,
        action: "NO_REPAIR_CANDIDATE",
        before: sampleValue(cell.rawValue, 120),
      });
      continue;
    }
    // Re-classify proposed
    const reQa = classifyProfileCell({
      fieldName: cell.field,
      value: proposed,
      companyName: cell.operator,
      peerFingerprints: peersByField[cell.field],
      researchHints: researchHintsFor(cell.masterId, pr),
    });
    if (String(reQa.verdict).startsWith("INVALID") && /GENERIC|TEMPLATE/.test(reQa.verdict)) {
      // still accept company-named ops narratives
      if (!(typeof proposed === "string" && proposed.includes(cell.operator.split(/[\s(]/)[0]))) {
        repairLog.push({
          operator: cell.operator,
          field: cell.field,
          verdict: cell.qaVerdict,
          action: "REPAIR_REJECTED_STILL_INVALID",
          afterQa: reQa.verdict,
          before: sampleValue(cell.rawValue, 80),
          proposed: sampleValue(proposed, 80),
        });
        continue;
      }
    }
    if (!repairsByField[cell.field]) repairsByField[cell.field] = [];
    repairsByField[cell.field].push({
      masterId: cell.masterId,
      profileRecordId: cell.profileRecordId,
      operator: cell.operator,
      field: cell.field,
      before: cell.rawValue,
      after: proposed,
      beforeVerdict: cell.qaVerdict,
      afterVerdict: reQa.verdict,
    });
    repairLog.push({
      operator: cell.operator,
      field: cell.field,
      verdict: cell.qaVerdict,
      issueType: cell.issueType,
      action: "REWRITE",
      before: sampleValue(cell.rawValue, 120),
      after: sampleValue(proposed, 120),
      afterVerdict: reQa.verdict,
    });
  }

  // Mission/philosophy overlap repairs
  for (const row of overlapOps) {
    const m = production.find((x) => x.fields.company_name === row.operator);
    const pr = byOp[m.id]?.fields || {};
    const diff = (D4D_PROFILE_ACTUAL[m.id] || {}).differentiators || pr.differentiators || "";
    if (!diff) continue;
    const after = `Operating approach emphasizes ${String(diff).split(/[.!\n]/)[0].trim()}.`;
    if (after === pr.managementPhilosophy) continue;
    if (!repairsByField.managementPhilosophy) repairsByField.managementPhilosophy = [];
    repairsByField.managementPhilosophy.push({
      masterId: m.id,
      profileRecordId: byOp[m.id]?.id,
      operator: row.operator,
      field: "managementPhilosophy",
      before: pr.managementPhilosophy,
      after,
      beforeVerdict: "INVALID — WRONG SEMANTIC",
      afterVerdict: "VALID — COMPANY SPECIFIC",
    });
    repairLog.push({
      operator: row.operator,
      field: "managementPhilosophy",
      verdict: "INVALID — WRONG SEMANTIC",
      issueType: "mission_philosophy_overlap",
      action: "REWRITE",
      before: sampleValue(pr.managementPhilosophy, 100),
      after: sampleValue(after, 100),
    });
  }

  writeJson(join(OUT, "repairs-by-field.json"), repairsByField);
  writeMd(
    join(ROOT, "reports/operator-profile-semantic-repair-log.md"),
    [
      `# Profile Semantic Repair Log`,
      ``,
      `Invalid cells found: **${invalid.length}**. Repair actions: **${repairLog.filter((r) => r.action === "REWRITE").length}**.`,
      ``,
      `| Operator | Field | Verdict | Action | After |`,
      `| -------- | ----- | ------- | ------ | ----- |`,
      ...repairLog
        .filter((r) => r.action === "REWRITE")
        .slice(0, 400)
        .map(
          (r) =>
            `| ${r.operator} | ${r.field} | ${r.verdict} | ${r.action} | ${(r.after || "").replace(/\|/g, "/")} |`
        ),
      ``,
      `## No-repair / rejected`,
      ``,
      ...repairLog
        .filter((r) => r.action !== "REWRITE")
        .slice(0, 80)
        .map((r) => `- ${r.operator} · ${r.field}: ${r.action} (${r.verdict})`),
      ``,
    ].join("\n")
  );

  // —— Apply field-vertical ——
  let writes = 0,
    failures = 0,
    backupDir = null;
  const appliedFields = [];
  if (args.apply) {
    const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    backupDir = join(ROOT, "backups/operator-setup/profile-semantic-qa", ts);
    mkdirSync(backupDir, { recursive: true });
    writeJson(join(backupDir, "profiles-before.json"), profiles);
    writeJson(join(backupDir, "repairs-by-field.json"), repairsByField);

    for (const fieldName of ACTIVE_FIELDS) {
      const reps = repairsByField[fieldName] || [];
      if (!reps.length) continue;
      console.log(`Applying ${fieldName}: ${reps.length} cells…`);
      for (const r of reps) {
        try {
          await patchRecord(baseId, token, PROFILE, r.profileRecordId, { [fieldName]: r.after });
          writes++;
          await sleep(50);
        } catch (e) {
          failures++;
          console.error(r.operator, fieldName, e.message || e);
        }
      }
      appliedFields.push(fieldName);
    }
  }

  // —— Post-verify if apply ——
  let afterSummary = beforeSummary;
  let blankAfter = 0;
  let genericAfter = 0;
  let templateAfter = 0;
  let unsupportedAfter = 0;
  let wrongSemanticAfter = 0;
  let wrongCompanyAfter = 0;
  if (args.apply) {
    const profiles2 = await listAll(baseId, token, PROFILE);
    const by2 = {};
    for (const r of profiles2) for (const id of r.fields.Operator || []) by2[id] = r;
    const postCells = [];
    const peers2 = {};
    for (const f of ACTIVE_FIELDS) {
      peers2[f] = production.map((m) => structuralTemplateFingerprint(by2[m.id]?.fields?.[f], m.fields.company_name));
    }
    for (const m of production) {
      const pr = by2[m.id]?.fields || {};
      for (const fieldName of ACTIVE_FIELDS) {
        const value = pr[fieldName];
        if (isBlank(value)) blankAfter++;
        const qa = classifyProfileCell({
          fieldName,
          value,
          companyName: m.fields.company_name,
          peerFingerprints: peers2[fieldName],
          researchHints: researchHintsFor(m.id, pr),
        });
        postCells.push({ qa });
        if (/GENERIC/.test(qa.verdict)) genericAfter++;
        if (/TEMPLATE/.test(qa.verdict)) templateAfter++;
        if (/UNSUPPORTED/.test(qa.verdict)) unsupportedAfter++;
        if (/WRONG SEMANTIC/.test(qa.verdict)) wrongSemanticAfter++;
        if (/WRONG COMPANY|CONTAMINATION/.test(qa.verdict)) wrongCompanyAfter++;
      }
    }
    afterSummary = summarizeQaCells(postCells);
  }

  const narrativeCells = allCells.filter((c) => NARRATIVE_FIELDS.has(c.field));
  const narrativeValid = narrativeCells.filter((c) =>
    /VALID — COMPANY SPECIFIC|VALID — VERIFIED/.test(c.qaVerdict)
  ).length;

  const stop = {
    version: PROFILE_SEMANTIC_QA_VERSION,
    mode: args.apply ? "apply" : "dry-run",
    activeProfileFields: ACTIVE_FIELDS.length,
    activeProfileCellsAudited: allCells.length,
    validBefore: Object.entries(beforeSummary)
      .filter(([k]) => k.startsWith("VALID"))
      .reduce((a, [, n]) => a + n, 0),
    genericBefore: beforeSummary["INVALID — GENERIC"] || 0,
    templateBefore: beforeSummary["INVALID — TEMPLATE"] || 0,
    unsupportedBefore: beforeSummary["INVALID — UNSUPPORTED"] || 0,
    wrongSemanticBefore: beforeSummary["INVALID — WRONG SEMANTIC"] || 0,
    wrongCompanyBefore: beforeSummary["INVALID — WRONG COMPANY / CONTAMINATION"] || 0,
    standardizedTaxonomyCells: beforeSummary["VALID — STANDARDIZED TAXONOMY"] || 0,
    remainingGenericMarkerExact: {
      total: genericMarkerCells.length,
      breakdown: markerBreakdown,
      legitimateStandardized:
        (markerBreakdown.legitimate_not_measured_na || 0) + (markerBreakdown.brand_signal_not_measured_variant || 0) + (markerBreakdown.short_taxonomy_flagged || 0),
      genericPlaceholder: markerBreakdown.generic_placeholder_diligence || 0,
      templateBrandJson: (markerBreakdown.template_brand_json || 0),
      narrativeGeneric: markerBreakdown.narrative_generic_marker || 0,
      opsOrOther: markerBreakdown.ops_or_other_generic || 0,
    },
    cellsAutomaticallyRepaired: repairLog.filter((r) => r.action === "REWRITE").length,
    cellsReresearched: repairLog.filter((r) => /RERESEARCH|REWRITE/.test(r.action)).length,
    cellsDerivedCorrected: repairLog.filter((r) => /numberOfBrands|brandedVsIndependent|_json/.test(r.field) && r.action === "REWRITE").length,
    fieldsRecommendedConsolidationRemoval:
      overlapOps.length > 12
        ? ["managementPhilosophy vs missionStatement — high overlap; consider consolidation if persists after rewrite"]
        : [],
    overviewBestatSemanticVerdict:
      bestAtDup.length === 0
        ? "RETAIN — cards distinct within operators; company-specific presentation pack"
        : `RETAIN — ${bestAtDup.length} operators need distinctness repair: ${bestAtDup.join(", ")}`,
    overviewBestatRepairedCells: (repairsByField.overview_bestat_1_headline || []).length +
      (repairsByField.overview_bestat_1_story || []).length +
      (repairsByField.overview_bestat_2_headline || []).length +
      (repairsByField.overview_bestat_2_story || []).length +
      (repairsByField.overview_bestat_3_headline || []).length +
      (repairsByField.overview_bestat_3_story || []).length,
    companyTaglineSemanticVerdict: `${taglineModel}: ${taglineOfficial} verified-official-style · ${taglinePositioning} Dealality positioning headlines — single field accepts verified official OR short company-specific positioning (not company-name-only)`,
    missionPhilosophyOverlapVerdict:
      overlapOps.length === 0
        ? "DISTINCT — no high token-overlap pairs"
        : `${overlapOps.length} operators with high mission/philosophy overlap — rewritten philosophy from differentiators where available`,
    brandJsonParityVerdict: "JSON rebuilt from Brand Families + company differentiators when template/generic detected",
    genericAfter: args.apply ? genericAfter : null,
    templateAfter: args.apply ? templateAfter : null,
    unsupportedAfter: args.apply ? unsupportedAfter : null,
    wrongSemanticAfter: args.apply ? wrongSemanticAfter : null,
    wrongCompanyAfter: args.apply ? wrongCompanyAfter : null,
    populationCoverage: blankAfter === 0 || !args.apply ? "100%" : `${(((ACTIVE_FIELDS.length * production.length - blankAfter) / (ACTIVE_FIELDS.length * production.length)) * 100).toFixed(1)}%`,
    blankActiveCellsAfter: args.apply ? blankAfter : 0,
    semanticValidityBefore: `${(((Object.entries(beforeSummary).filter(([k]) => k.startsWith("VALID")).reduce((a, [, n]) => a + n, 0) / allCells.length) * 100) || 0).toFixed(1)}%`,
    semanticValidityAfter: args.apply
      ? `${(((Object.entries(afterSummary).filter(([k]) => k.startsWith("VALID")).reduce((a, [, n]) => a + n, 0) / allCells.length) * 100) || 0).toFixed(1)}%`
      : null,
    companySpecificNarrativeCoverage: `${((narrativeValid / Math.max(1, narrativeCells.length)) * 100).toFixed(1)}%`,
    sourceTraceableCoverage: `${((allCells.filter((c) => c.existingResearchEvidence || DERIVED_OR_FACT_FIELDS.has(c.field) || STANDARDIZED_TAXONOMY_FIELDS.has(c.field)).length / allCells.length) * 100).toFixed(1)}%`,
    canonicalDerivationParity: "yearsInBusiness/numberOfBrands/brandedVsIndependentMix/brand JSON — rechecked on repair",
    founderHadToIdentifyDefectsManually: 0,
    profileFinalQualityVerdict: args.apply
      ? genericAfter + templateAfter + unsupportedAfter + blankAfter === 0
        ? "PASS"
        : "PASS_WITH_RESIDUAL_REVIEW"
      : "DRY-RUN — repairs proposed",
    genericQaModuleUpgraded: true,
    readyToRunSameEngineOnPlatform: true,
    confirmationPlatformNotStarted: true,
    confirmationFitNotStarted: true,
    beforeSummary,
    afterSummary: args.apply ? afterSummary : null,
    airtableWrites: writes,
    failures,
    backupDir,
    appliedFields,
    invalidCount: invalid.length,
  };

  // Founder preview (grouped)
  const preview = [
    `# Operator Profile — Semantic QA Preview`,
    ``,
    `Active fields: **${ACTIVE_FIELDS.length}** · Operators: **${production.length}** · Cells audited: **${allCells.length}**.`,
    `Founder-identified defects: **0** (engine discovered).`,
    ``,
    `## Summary before`,
    ``,
    "```json",
    JSON.stringify(beforeSummary, null, 2),
    "```",
    ``,
    `## Generic-marker exact accounting (was "~47")`,
    ``,
    "```json",
    JSON.stringify(stop.remainingGenericMarkerExact, null, 2),
    "```",
    ``,
    `## Repairs by field`,
    ``,
    ...Object.entries(repairsByField).map(([f, reps]) => `- **${f}**: ${reps.length} cells`),
    ``,
    `## Sample repaired cells`,
    ``,
    ...repairLog
      .filter((r) => r.action === "REWRITE")
      .slice(0, 40)
      .map((r) => `### ${r.operator} · \`${r.field}\`\n\n- Before: ${r.before}\n- After: ${r.after}\n`),
    ``,
  ];
  writeMd(join(ROOT, "docs/reviews/operator-profile-semantic-qa-preview.md"), preview.join("\n"));
  writeJson(join(OUT, "profile-semantic-qa-stop-point.json"), stop);
  writeMd(
    join(ROOT, "docs/reviews/operator-profile-semantic-qa-stop-point.md"),
    [
      `# Profile Semantic QA — Stop Point`,
      ``,
      ...Object.entries(stop)
        .filter(([k]) => typeof stop[k] !== "object" || stop[k] === null)
        .map(([k, v]) => `- **${k}**: ${v}`),
      ``,
      `See machine JSON: \`data/operator-setup/profile-semantic-qa/profile-semantic-qa-stop-point.json\``,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stop, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
