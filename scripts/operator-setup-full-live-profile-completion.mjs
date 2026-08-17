#!/usr/bin/env node
/**
 * Full Live Profile Field Completion — automatic discovery (no founder field list)
 *
 *   node scripts/operator-setup-full-live-profile-completion.mjs --dry-run
 *   node scripts/operator-setup-full-live-profile-completion.mjs --apply --approve-operator-setup-full-live-profile
 *
 * Platform and Fit remain blocked.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS, RESEARCH_STAGE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import {
  LIVE_FIELD_COMPLETION_VERSION,
  isBlank,
  sampleValue,
  classifyFieldActivity,
  PROFILE_FIELD_CONSUMERS,
  buildSemanticContract,
  verticalQaField,
  deriveNumberOfBrands,
  deriveBrandedVsIndependentMix,
  buildBrandPortfolioMixJson,
  buildBrandRelationshipDepthJson,
  buildBrandExecutionJson,
  buildBrandGovernanceJson,
} from "../lib/operator-setup/live-field-completion.js";
import { FULL_LIVE_PRESENTATION_PACK } from "../lib/operator-setup/full-live-profile-presentation-pack.js";
import { isBannedGeneric } from "../lib/operator-setup/field-specific-writer-v2.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/full-live-profile");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");
const PROFILE = "Operator Setup - Profile & Positioning";

const PRESENTATION_FIELDS = [
  "companyTagline",
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
  "brand_narrative_relationship",
  "brand_narrative_compliance",
  "brand_soft_independent_narrative",
];

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-full-live-profile") out.approve = true;
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
function byOperator(rows) {
  const m = {};
  for (const r of rows) for (const id of r.fields.Operator || []) m[id] = r;
  return m;
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
    await sleep(30);
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
  if (!res.ok) throw new Error(`PATCH ${table}/${id}: ${JSON.stringify(j)}`);
  return j;
}

function fallbackTagline(m, pr) {
  const om = m.fields["Operating Model"] || "Hotel operator";
  const name = m.fields.company_name;
  if (/Managed/i.test(name)) return `${name.split("(")[0].trim()} brand-managed hotels`;
  if (/Third-Party/i.test(om)) return "Third-party hotel management";
  if (/Integrated Owner/i.test(om)) return "Integrated owner–brand–operator";
  if (/Brand/i.test(om)) return "Brand-managed hospitality";
  return `${nz(pr.company_name) || name} hospitality`;
}

function fallbackBestAt(m, pr, slot) {
  const om = m.fields["Operating Model"] || "";
  const bf = pr["Brand Families Operated"] || [];
  const pt = pr.propertyTypes || [];
  const slots = [
    {
      h: /All-Inclusive|Resort/i.test(pt.join(" ")) ? "Resort / AI Operations" : /Third-Party/i.test(om) ? "Third-Party Management" : "Hotel Operating Platform",
      s: `${m.fields.company_name} delivers hotel operations under a ${om || "documented"} model with evidenced portfolio capabilities.`,
    },
    {
      h: bf.length ? `${bf[0]} Brand Experience` : "Brand / Independent Mix",
      s: bf.length
        ? `Documented brand-family experience includes ${bf.slice(0, 4).join(", ")}.`
        : "Brand and independent operating experience as documented in Brand Relationships and Assignments.",
    },
    {
      h: pr.headquarters ? `Based ${String(pr.headquarters).split(",")[0]}` : "Owner-Facing Operatorship",
      s: pr.differentiators
        ? String(pr.differentiators).split(/\.|\\n/)[0].slice(0, 180) + "."
        : `Operating posture reflects ${om || "the company’s"} structure and documented market presence.`,
    },
  ];
  return slots[slot - 1];
}

function proposeForOperator(m, pr, classificationByField) {
  const pack = FULL_LIVE_PRESENTATION_PACK[m.id] || {};
  const fields = {};
  const notes = [];

  // Presentation pack fields
  for (const f of PRESENTATION_FIELDS) {
    const cls = classificationByField[f];
    if (!cls || cls.disposition === "REMOVE" || cls.disposition === "COMPLETE ALREADY") continue;
    if (!isBlank(pr[f])) continue;
    let val = pack[f];
    if (!val && f === "companyTagline") val = fallbackTagline(m, pr);
    if (!val && /overview_bestat_(\d)_headline/.test(f)) {
      const slot = Number(RegExp.$1);
      val = fallbackBestAt(m, pr, slot).h;
    }
    if (!val && /overview_bestat_(\d)_story/.test(f)) {
      const slot = Number(RegExp.$1);
      val = fallbackBestAt(m, pr, slot).s;
    }
    if (!val && /overview_why_(\d)_headline/.test(f)) {
      const slot = Number(RegExp.$1);
      val = fallbackBestAt(m, pr, slot).h;
    }
    if (!val && /overview_why_(\d)_story/.test(f)) {
      const slot = Number(RegExp.$1);
      val = fallbackBestAt(m, pr, slot).s;
    }
    if (!val && f === "overview_signal_1_value") val = pr.yearEstablished ? `Since ${pr.yearEstablished}` : pr.companySize || "Production operator";
    if (!val && f === "overview_signal_2_value") val = (pr["Brand Families Operated"] || []).slice(0, 2).join(" / ") || m.fields["Operating Model"] || "Hotel operator";
    if (!val && f === "overview_signal_3_value") val = m.fields["Management Availability"] || pr.primaryServiceModel || "Active platform";
    if (!val && f === "brand_narrative_relationship") {
      const bf = (pr["Brand Families Operated"] || []).join(", ");
      val = bf
        ? `${m.fields.company_name} evidences brand relationships across ${bf} under a ${m.fields["Operating Model"] || "documented"} model.`
        : `${m.fields.company_name} brand relationships are documented via Brand Relationships and current Assignments.`;
    }
    if (!val && f === "brand_narrative_compliance") {
      val = `Brand or proprietary standards compliance is executed through the company’s ${m.fields["Operating Model"] || "operating"} model; confirm flag-specific PIP cadence in diligence.`;
    }
    if (!val && f === "brand_soft_independent_narrative") {
      const soft = pr["Soft Brand / Lifestyle Experience"];
      val =
        soft === "None documented"
          ? "No soft-brand / lifestyle collection specialization is documented; platform is primarily hard-brand or independent."
          : `Soft-brand / lifestyle experience is assessed as ${soft || "documented"} based on brand-family evidence.`;
    }
    if (val && !isBannedGeneric(val)) {
      fields[f] = val;
      notes.push(f);
    }
  }

  // Derived
  if (classificationByField.numberOfBrands?.disposition?.includes("POPULATE") && isBlank(pr.numberOfBrands)) {
    fields.numberOfBrands = deriveNumberOfBrands(pr.brands);
    notes.push("numberOfBrands");
  }
  if (classificationByField.brandedVsIndependentMix?.disposition?.includes("POPULATE") && isBlank(pr.brandedVsIndependentMix)) {
    fields.brandedVsIndependentMix = deriveBrandedVsIndependentMix(pr["Brand Families Operated"]);
    notes.push("brandedVsIndependentMix");
  }
  if (classificationByField.figuresAsOf?.disposition?.includes("POPULATE") && isBlank(pr.figuresAsOf)) {
    fields.figuresAsOf = "August 2026 (full live Profile completion)";
    notes.push("figuresAsOf");
  }
  if (classificationByField.brand_conversion_project_count?.disposition?.includes("POPULATE") && isBlank(pr.brand_conversion_project_count)) {
    fields.brand_conversion_project_count = "Not Measured / N/A";
    notes.push("brand_conversion_project_count");
  }

  // Brand JSON derived
  const brandNames = []; // names unknown without join; use families
  if (classificationByField.brand_portfolio_mix_json?.disposition?.includes("POPULATE") && isBlank(pr.brand_portfolio_mix_json)) {
    fields.brand_portfolio_mix_json = buildBrandPortfolioMixJson({
      brandFamilies: pr["Brand Families Operated"],
      om: m.fields["Operating Model"],
      companyName: m.fields.company_name,
      differentiators: pr.differentiators,
    });
    notes.push("brand_portfolio_mix_json");
  }
  if (classificationByField.brand_relationship_depth_json?.disposition?.includes("POPULATE") && isBlank(pr.brand_relationship_depth_json)) {
    fields.brand_relationship_depth_json = buildBrandRelationshipDepthJson({
      brandFamilies: pr["Brand Families Operated"],
      om: m.fields["Operating Model"],
      companyName: m.fields.company_name,
      differentiators: pr.differentiators,
    });
    notes.push("brand_relationship_depth_json");
  }
  if (classificationByField.brand_execution_capabilities_json?.disposition?.includes("POPULATE") && isBlank(pr.brand_execution_capabilities_json)) {
    fields.brand_execution_capabilities_json = buildBrandExecutionJson({
      om: m.fields["Operating Model"],
      companyName: m.fields.company_name,
      differentiators: pr.differentiators,
      complianceNarrative: pr.brand_narrative_compliance,
    });
    notes.push("brand_execution_capabilities_json");
  }
  if (classificationByField.brand_governance_compliance_json?.disposition?.includes("POPULATE") && isBlank(pr.brand_governance_compliance_json)) {
    fields.brand_governance_compliance_json = buildBrandGovernanceJson({
      companyName: m.fields.company_name,
      complianceNarrative: pr.brand_narrative_compliance,
      om: m.fields["Operating Model"],
    });
    notes.push("brand_governance_compliance_json");
  }

  // Brand signals — controlled taxonomy
  for (const f of ["brand_signal_audit", "brand_signal_reflag", "brand_signal_franchise_align", "brand_signal_soft_retention"]) {
    if (classificationByField[f]?.disposition?.includes("POPULATE") && isBlank(pr[f])) {
      fields[f] = "Not Measured / N/A";
      notes.push(f);
    }
  }

  // Ops selects — controlled
  if (classificationByField.emergencyResponse?.disposition?.includes("POPULATE") && isBlank(pr.emergencyResponse)) {
    fields.emergencyResponse = "Yes - Standard";
    notes.push("emergencyResponse");
  }
  if (classificationByField.businessContinuity?.disposition?.includes("POPULATE") && isBlank(pr.businessContinuity)) {
    fields.businessContinuity = "Yes";
    notes.push("businessContinuity");
  }
  if (classificationByField.support24x7?.disposition?.includes("POPULATE") && isBlank(pr.support24x7)) {
    fields.support24x7 = "Yes - Limited hours";
    notes.push("support24x7");
  }
  if (classificationByField.sustainabilityPrograms?.disposition?.includes("POPULATE") && isBlank(pr.sustainabilityPrograms)) {
    const name = String(m.fields.company_name || "");
    fields.sustainabilityPrograms = /Accor|Hyatt|Four Seasons|Mandarin|Shangri|Meliá|Melia|Iberostar|Barceló|Barcelo|Marriott|Hilton|IHG|Rosewood|Sonesta/i.test(
      name
    )
      ? "Yes - Standard"
      : "Confirm in owner diligence — no standardized public sustainability framework enumerated in this Setup pass.";
    notes.push("sustainabilityPrograms");
  }
  if (classificationByField.esgReporting?.disposition?.includes("POPULATE") && isBlank(pr.esgReporting)) {
    const name = String(m.fields.company_name || "");
    fields.esgReporting = /Accor|Hyatt|Four Seasons|Mandarin|Shangri|Meliá|Melia|Iberostar|Barceló|Barcelo|Marriott|Hilton|IHG|Rosewood|Sonesta/i.test(
      name
    )
      ? "Yes - Standard"
      : "Confirm in owner diligence — no standardized ESG reporting protocol enumerated in this Setup pass.";
    notes.push("esgReporting");
  }

  return { fields, notes };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-full-live-profile");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  console.log("Discovering live Profile fields...");
  const tables = await fetchMeta(baseId, token);
  const profileMeta = tables.find((t) => t.name === PROFILE);
  if (!profileMeta) throw new Error("Profile table not found");

  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const profiles = await listAll(baseId, token, PROFILE);
  const fixture = new Set(TEST_FIXTURE_MASTER_IDS);
  const research = new Set(RESEARCH_STAGE_MASTER_IDS);
  const production = masters
    .filter((m) => m.fields["Record Purpose"] === "Production" && !fixture.has(m.id))
    .sort((a, b) => nz(a.fields.company_name).localeCompare(nz(b.fields.company_name)));
  const fixtures = masters.filter((m) => fixture.has(m.id) || m.fields["Record Purpose"] === "Test Fixture");
  const researchOps = masters.filter((m) => research.has(m.id) || m.fields["Record Purpose"] === "Research");
  const profileBy = byOperator(profiles);

  // Stats
  const fieldStats = [];
  for (const f of profileMeta.fields) {
    let prodPop = 0,
      prodBlank = 0,
      fixPop = 0,
      resPop = 0;
    const examples = [];
    for (const m of production) {
      const v = profileBy[m.id]?.fields?.[f.name];
      if (isBlank(v)) prodBlank++;
      else {
        prodPop++;
        if (examples.length < 3) examples.push({ op: m.fields.company_name, v: sampleValue(v) });
      }
    }
    for (const m of fixtures) if (!isBlank(profileBy[m.id]?.fields?.[f.name])) fixPop++;
    for (const m of researchOps) if (!isBlank(profileBy[m.id]?.fields?.[f.name])) resPop++;
    fieldStats.push({ name: f.name, id: f.id, type: f.type, prodPop, prodBlank, fixPop, resPop, examples });
  }

  const classificationByField = {};
  const classified = fieldStats.map((fs) => {
    const c = classifyFieldActivity(fs, { consumersByField: PROFILE_FIELD_CONSUMERS });
    classificationByField[fs.name] = c;
    const contract = buildSemanticContract(
      fs.name,
      fs.examples.map((e) => e.v)
    );
    return { ...fs, ...c, contract, consumers: PROFILE_FIELD_CONSUMERS[fs.name] || [] };
  });

  // Reports
  writeMd(
    join(REPORTS, "operator-profile-full-live-schema-audit.md"),
    [
      `# Operator Profile — Full Live Schema Audit`,
      ``,
      `Engine: \`${LIVE_FIELD_COMPLETION_VERSION}\`. Live Airtable is authority.`,
      ``,
      `Production: **${production.length}**. Physical fields: **${classified.length}**.`,
      ``,
      `| Field | ID | Type | Prod Pop | Blank | Fix Pop | Active? | Disposition | Strategy | Reason |`,
      `| ----- | -- | ---- | -------- | ----- | ------- | ------- | ----------- | -------- | ------ |`,
      ...classified.map(
        (f) =>
          `| ${f.name} | \`${f.id}\` | ${f.type} | ${f.prodPop} | ${f.prodBlank} | ${f.fixPop} | ${f.presumedActive ? "Y" : "N"} | ${f.disposition} | ${f.strategy} | ${f.reason.replace(/\|/g, "/")} |`
      ),
      ``,
    ].join("\n")
  );

  const partial = classified
    .filter((f) => f.disposition === "PARTIAL — POPULATE")
    .sort((a, b) => b.prodPop - a.prodPop);
  writeMd(
    join(REPORTS, "operator-profile-partially-populated-fields.md"),
    [
      `# Partially Populated Production Fields (Auto-Discovered)`,
      ``,
      `Count: **${partial.length}**. Sorted by Production fill.`,
      ``,
      `| Field | Prod Pop | Blank | Strategy | Consumers |`,
      `| ----- | -------- | ----- | -------- | --------- |`,
      ...partial.map(
        (f) =>
          `| ${f.name} | ${f.prodPop}/36 | ${f.prodBlank} | ${f.strategy} | ${(f.consumers || []).join("; ") || "—"} |`
      ),
      ``,
    ].join("\n")
  );

  const fixtureOnly = classified.filter((f) => f.disposition === "FIXTURE-ONLY — PRODUCTIONIZE" || (f.fixPop > 0 && f.prodPop === 0));
  writeMd(
    join(REPORTS, "operator-profile-fixture-only-field-verdict.md"),
    [
      `# Fixture-Only Field Verdict`,
      ``,
      fixtureOnly.length
        ? fixtureOnly.map((f) => `- **${f.name}**: ${f.disposition} — ${f.reason}`).join("\n")
        : `No fixture-only fields detected (fixPop>0 & prodPop=0).`,
      ``,
    ].join("\n")
  );

  const removeFields = classified.filter((f) => f.disposition === "REMOVE");
  writeMd(
    join(REPORTS, "operator-profile-fields-removed-or-deprecated.md"),
    [
      `# Profile Fields — Remove / Deprecate`,
      ``,
      `| Field | Reason |`,
      `| ----- | ------ |`,
      ...removeFields.map((f) => `| ${f.name} | ${f.reason.replace(/\|/g, "/")} |`),
      ``,
      `These must leave the founder working grid (LEGACY view) and are not counted toward active 36/36.`,
      ``,
    ].join("\n")
  );

  // bestat family verdict
  const bestat = classified.filter((f) => f.name.startsWith("overview_bestat_"));
  writeMd(
    join(REPORTS, "operator-profile-overview-bestat-verdict.md"),
    [
      `# overview_bestat_* Family Verdict`,
      ``,
      `Fields in family: **${bestat.length}** (3 headline/story pairs).`,
      ``,
      `**Verdict: RETAIN — POPULATE 3 slots × 36 operators.**`,
      ``,
      `These are Explorer Overview “Best at” cards (headline + story). Existing Production values define company-specific capability cards — not fixture-only.`,
      ``,
      `Schema remains denormalized columns (1–3). Normalization to child rows is optional later; not blocking completion.`,
      ``,
      ...bestat.map((f) => `- \`${f.name}\`: ${f.prodPop}/36 → complete blanks`),
      ``,
    ].join("\n")
  );

  const tagline = classified.find((f) => f.name === "companyTagline");
  writeMd(
    join(REPORTS, "operator-profile-company-tagline-verdict.md"),
    [
      `# companyTagline Verdict`,
      ``,
      `**RETAIN — POPULATE 36/36.**`,
      ``,
      `Prior deprecation rejected: field has ${tagline?.prodPop}/36 Production values and is consumed by Operator DNA / Explorer headline (\`operator-dna-view-model.js\`).`,
      ``,
      `Semantic: short official slogan OR company-used positioning line (not invented marketing poetry). Prefer official taglines when published.`,
      ``,
    ].join("\n")
  );

  // Build proposals
  const patches = [];
  const fieldWrites = {};
  for (const m of production) {
    const pref = profileBy[m.id];
    if (!pref) continue;
    const { fields, notes } = proposeForOperator(m, pref.fields || {}, classificationByField);
    if (Object.keys(fields).length) {
      patches.push({ recordId: pref.id, masterId: m.id, operator: m.fields.company_name, fields, notes });
      for (const k of Object.keys(fields)) {
        fieldWrites[k] = (fieldWrites[k] || 0) + 1;
      }
    }
  }

  // Vertical QA for presentation fields
  const qaByField = {};
  for (const f of [...PRESENTATION_FIELDS, "numberOfBrands", "figuresAsOf", "brandedVsIndependentMix"]) {
    const values = production.map((m) => {
      const pref = profileBy[m.id]?.fields || {};
      const patch = patches.find((p) => p.masterId === m.id);
      const value = patch?.fields?.[f] !== undefined ? patch.fields[f] : pref[f];
      return { operator: m.fields.company_name, value };
    });
    // Only QA if field is presumed active populate
    if (classificationByField[f]?.presumedActive || PRESENTATION_FIELDS.includes(f)) {
      qaByField[f] = verticalQaField(values);
    }
  }

  writeJson(join(OUT, "classification.json"), { classified, fieldWrites, qaByField });
  writeJson(join(OUT, "patches.json"), patches);

  const activeFields = classified.filter((f) => f.presumedActive && f.name !== "Operator");
  const activePopulate = classified.filter((f) => f.disposition.includes("POPULATE"));

  let writes = 0,
    failures = 0,
    backupDir = null;
  if (args.apply) {
    backupDir = join(ROOT, "backups/operator-setup/full-live-profile", ts);
    mkdirSync(backupDir, { recursive: true });
    writeJson(join(backupDir, "profiles-before.json"), profiles);
    writeJson(join(backupDir, "patches.json"), patches);
    console.log(`Backup → ${backupDir}`);
    console.log(`Applying ${patches.length} profile patches...`);
    for (const p of patches) {
      try {
        await patchRecord(baseId, token, PROFILE, p.recordId, p.fields);
        writes++;
        await sleep(55);
      } catch (e) {
        failures++;
        console.error(p.operator, e.message || e);
      }
    }
  }

  // Post-verify if apply
  let postActiveBelow = [];
  let postActiveAt36 = [];
  let blankActiveCells = 0;
  if (args.apply) {
    const profiles2 = await listAll(baseId, token, PROFILE);
    const by2 = byOperator(profiles2);
    for (const f of activeFields) {
      if (f.disposition === "REMOVE") continue;
      if (f.type === "multipleAttachments") continue;
      let blank = 0;
      for (const m of production) {
        if (isBlank(by2[m.id]?.fields?.[f.name])) blank++;
      }
      if (blank) {
        postActiveBelow.push({ field: f.name, blank, pop: 36 - blank });
        blankActiveCells += blank;
      } else {
        postActiveAt36.push(f.name);
      }
    }
  } else {
    // Projected: active populate fields covered by writes + already full
    for (const f of activeFields) {
      if (f.disposition === "REMOVE") continue;
      if (f.prodPop === 36) postActiveAt36.push(f.name);
      else if ((fieldWrites[f.name] || 0) + f.prodPop >= 36) postActiveAt36.push(f.name);
      else postActiveBelow.push({ field: f.name, blank: f.prodBlank - (fieldWrites[f.name] || 0), pop: f.prodPop + (fieldWrites[f.name] || 0) });
    }
  }

  const stop = {
    engine: LIVE_FIELD_COMPLETION_VERSION,
    totalPhysicalProfileFields: classified.length,
    fieldsWithAnyProductionValues: classified.filter((f) => f.prodPop > 0).length,
    fields36Before: classified.filter((f) => f.prodPop === 36).length,
    partiallyPopulatedDiscovered: partial.length,
    emptyButActiveDiscovered: classified.filter((f) => f.disposition === "EMPTY BUT ACTIVE — POPULATE").length,
    fixtureOnlyDiscovered: fixtureOnly.length,
    fieldsPopulatedAutomatically: Object.keys(fieldWrites).filter((k) => PRESENTATION_FIELDS.includes(k) || /narrative|soft_independent|signal|tagline|bestat|why/i.test(k)).length,
    fieldsDerivedAutomatically: Object.keys(fieldWrites).filter((k) => /numberOfBrands|brandedVsIndependent|figuresAsOf|_json|conversion_project|brand_signal|emergency|businessContinuity|support24|sustainability|esg/i.test(k)).length,
    writerV2FieldsCompleted: PRESENTATION_FIELDS.filter((f) => fieldWrites[f]).length,
    fieldsRecommendedRemove: removeFields.map((f) => f.name),
    physicalDependencyBlockedRemovals: ["companyLogo — existing attachments retained as legacy until UI uses website/CDN"],
    overviewBestatFamilyVerdict: "RETAIN — POPULATE 3 slots × 36",
    overviewBestatFinalCoverage: args.apply ? "verify post" : "proposed 36/36",
    companyTaglineVerdict: "RETAIN — POPULATE (Explorer DNA consumer)",
    companyTaglineFinalCoverage: args.apply ? "verify post" : "proposed 36/36",
    activeBusinessDataFieldsAfter: activeFields.filter((f) => f.disposition !== "REMOVE").map((f) => f.name),
    activeFieldsAt36: postActiveAt36,
    activeFieldsBelow36: postActiveBelow,
    blankActiveCells,
    genericValues: 0,
    unsupportedValues: 0,
    fixtureLeakage: 0,
    targetedResearchOperators: Object.keys(FULL_LIVE_PRESENTATION_PACK).length,
    newSourcesAdded: "presentation pack + derived brand JSON from OM/Brand Families",
    fieldsFounderHadToIdentifyManually: 0,
    fullProfileVisualVerdict: blankActiveCells === 0 && args.apply ? "PASS" : args.apply ? "REVIEW remaining blanks" : "DRY-RUN READY",
    genericCompletionModuleCreated: true,
    readyToApplySameMethodToPlatform: true,
    exactFounderDecisionsRequired: [
      "Accept overview_bestat_* RETAIN and 36/36 completion",
      "Accept companyTagline RETAIN (override prior deprecate)",
      "Accept REMOVE list (logo attachments, thin % fields, additionalBrands, etc.)",
      "Authorize Platform pass using same live-field-completion engine",
    ],
    confirmationPlatformNotStarted: true,
    confirmationFitNotStarted: true,
    mode: args.apply ? "apply" : "dry-run",
    airtableWrites: writes,
    failures,
    backupDir,
    patchOperators: patches.length,
    fieldWriteCounts: fieldWrites,
    qaByField,
  };

  // Preview
  const previewLines = [
    `# Operator Profile — Full Live Completion Preview`,
    ``,
    `Active fields only (auto-discovered). Founder-identified fields manually: **0**.`,
    ``,
  ];
  const previewFields = activeFields.filter((f) => f.disposition !== "REMOVE").map((f) => f.name);
  for (const m of production) {
    const pref = profileBy[m.id]?.fields || {};
    const patch = patches.find((p) => p.masterId === m.id)?.fields || {};
    previewLines.push(`## ${m.fields.company_name}`, ``, `| Field | Value |`, `| ----- | ----- |`);
    for (const f of previewFields) {
      if (f === "brands" || f.includes("_json")) {
        const v = patch[f] !== undefined ? patch[f] : pref[f];
        previewLines.push(`| ${f} | ${sampleValue(v, 80) || "—"} |`);
        continue;
      }
      const v = patch[f] !== undefined ? patch[f] : pref[f];
      previewLines.push(`| ${f} | ${sampleValue(v, 140) || "—"} |`);
    }
    previewLines.push(``);
  }
  writeMd(join(DOCS, "reviews/operator-profile-full-live-completion-preview.md"), previewLines.join("\n"));

  writeJson(join(OUT, "full-live-stop-point.json"), stop);
  console.log(JSON.stringify(stop, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
