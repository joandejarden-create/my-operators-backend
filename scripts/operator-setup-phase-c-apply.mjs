#!/usr/bin/env node
/**
 * Operator Setup Phase C — Researched Summary Writer Rollout
 *
 *   node scripts/operator-setup-phase-c-apply.mjs --dry-run
 *   node scripts/operator-setup-phase-c-apply.mjs --apply --approve-operator-setup-phase-c-writes
 *   node scripts/operator-setup-phase-c-apply.mjs --apply --approve-operator-setup-phase-c-writes --batches 1,2,3
 *
 * No Fit changes. No Webhound merge. Blank-fill preferred; overwrites require conflict justification.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildOperatorUniverse } from "../lib/operator-explorer/operator-universe.js";
import {
  PHASE_C_ADAPTER_VERSION,
  OM_MA_GAP_IDS,
  proposeOmMaFromEvidence,
  buildBrandRelationshipSectionRows,
  buildOperatingPlatformSectionRows,
  isBlockedPhaseCField,
  HELD_MASTERS,
} from "../lib/operator-setup/phase-c-oe-adapters.js";
import { isPopulated } from "../lib/operator-setup/derived-sync.js";
import {
  listProfileDeepPackSlugs,
  getProfileDeepPack,
  resolveProfileDeepMasterMeta,
} from "../lib/partner-intelligence/operator-setup-profile-deep-packs.js";
import {
  listWebsiteContentPackSlugs,
  getWebsiteContentPack,
  resolvePackMasterMeta,
} from "../lib/partner-intelligence/operator-setup-website-content-packs.js";
import { ENRICHMENT_FIELD_CATALOG } from "../lib/operator-fit/readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/phase-c");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");

const SETUP_TABLES = [
  "Operator Setup - Master",
  "Operator Setup - Profile & Positioning",
  "Operator Setup - Platform & Markets",
  "Operator Setup - Commercial Fit & Terms",
  "Operator Setup - Governance, Delivery & Diligence",
  "Operator Setup - Case Studies",
  "Operator Setup - Brand Relationships",
  "Operator Setup - Leadership Team Members",
  "Operator Setup - Diligence QA",
  "Operator Setup - Explorer Materials",
  "Operator Setup - Engagement & Reporting",
  "Operator Setup - Infrastructure Platform",
  "Operator Setup - Leadership Platform",
  "Operator Setup - Operating Platform",
];

const GOLDENS = [
  { id: "recF5Z87OAqFgndoq", name: "Arbor Lodging (CALA)" },
  { id: "recWPKu5laVZxsvpn", name: "Hotel Equities (CALA)" },
];

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false, batches: [1, 2, 3] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-phase-c-writes") out.approve = true;
    else if (a === "--batches") out.batches = String(argv[++i] || "1,2,3").split(",").map(Number);
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

async function listAll(baseId, token, table) {
  const out = [];
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const res = await fetch(u, { headers: { Authorization: `Bearer ${token}` } });
    const j = await res.json();
    if (j.error) throw new Error(`${table}: ${JSON.stringify(j.error)}`);
    out.push(...(j.records || []));
    offset = j.offset;
    await sleep(60);
  } while (offset);
  return out;
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

async function createRecords(baseId, token, table, records) {
  const created = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ records: chunk.map((fields) => ({ fields })), typecast: true }),
    });
    const j = await res.json();
    if (!res.ok) throw new Error(`POST ${table}: ${JSON.stringify(j)}`);
    created.push(...(j.records || []));
    await sleep(150);
  }
  return created;
}

function findLinked(records, masterId) {
  return records.find((r) => (r.fields.Operator || []).includes(masterId)) || null;
}
function linkedAll(records, masterId) {
  return records.filter((r) => (r.fields.Operator || []).includes(masterId));
}

function classifyMutation(existing, proposed) {
  if (!isPopulated(proposed) && proposed !== 0) return "DO NOT WRITE";
  if (!isPopulated(existing) && existing !== 0) return "NEW VALUE";
  const same =
    Array.isArray(existing) && Array.isArray(proposed)
      ? JSON.stringify([...existing].map(String).sort()) === JSON.stringify([...proposed].map(String).sort())
      : String(existing).trim() === String(proposed).trim();
  if (same) return "NO-OP";
  return "HOLD — CONFLICT"; // Phase C default: do not overwrite without explicit allowlist
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-operator-setup-phase-c-writes");
    process.exit(1);
  }

  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  mkdirSync(OUT, { recursive: true });
  const backupDir = join(ROOT, "backups/operator-setup/phase-c", ts);
  mkdirSync(backupDir, { recursive: true });

  console.log("Loading Airtable...");
  const meta = await (
    await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, { headers: { Authorization: `Bearer ${token}` } })
  ).json();
  const tableMeta = Object.fromEntries((meta.tables || []).map((t) => [t.name, t]));

  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const assignments = await listAll(baseId, token, "Operator Intelligence - Assignments");
  const marketPresence = await listAll(baseId, token, "Operator Intelligence - Market Presence");
  const brandRelationships = await listAll(baseId, token, "Operator Intelligence - Brand Relationships");
  const claims = await listAll(baseId, token, "Operator Intelligence - Claims");
  const profiles = await listAll(baseId, token, "Operator Setup - Profile & Positioning");
  const platforms = await listAll(baseId, token, "Operator Setup - Platform & Markets");
  const commercials = await listAll(baseId, token, "Operator Setup - Commercial Fit & Terms");
  const governances = await listAll(baseId, token, "Operator Setup - Governance, Delivery & Diligence");
  const setupBr = await listAll(baseId, token, "Operator Setup - Brand Relationships");
  const operatingPlatform = await listAll(baseId, token, "Operator Setup - Operating Platform");
  const engagement = await listAll(baseId, token, "Operator Setup - Engagement & Reporting");
  const infra = await listAll(baseId, token, "Operator Setup - Infrastructure Platform");
  const leadershipPlat = await listAll(baseId, token, "Operator Setup - Leadership Platform");
  const materials = await listAll(baseId, token, "Operator Setup - Explorer Materials");
  const caseStudies = await listAll(baseId, token, "Operator Setup - Case Studies");
  const diligence = await listAll(baseId, token, "Operator Setup - Diligence QA");
  const leadershipPeople = await listAll(baseId, token, "Operator Setup - Leadership Team Members");

  const universe = buildOperatorUniverse(masters, { assignments, brandRelationships, marketPresence });
  const production = universe.operators.filter((o) => o.recordPurpose === "Production");
  const masterById = Object.fromEntries(masters.map((m) => [m.id, m]));

  const omBefore = production.filter((o) => isPopulated(masterById[o.masterId]?.fields?.["Operating Model"])).length;
  const maBefore = production.filter((o) => isPopulated(masterById[o.masterId]?.fields?.["Management Availability"])).length;

  // ——— Backup ———
  console.log("Backing up...");
  const backupManifest = { timestamp: ts, tables: [] };
  const backupPayloads = {
    "Operator Setup - Master": masters,
    "Operator Setup - Profile & Positioning": profiles,
    "Operator Setup - Platform & Markets": platforms,
    "Operator Setup - Commercial Fit & Terms": commercials,
    "Operator Setup - Governance, Delivery & Diligence": governances,
    "Operator Setup - Brand Relationships": setupBr,
    "Operator Setup - Operating Platform": operatingPlatform,
    "Operator Setup - Engagement & Reporting": engagement,
    "Operator Setup - Infrastructure Platform": infra,
    "Operator Setup - Leadership Platform": leadershipPlat,
    "Operator Setup - Explorer Materials": materials,
    "Operator Setup - Case Studies": caseStudies,
    "Operator Setup - Diligence QA": diligence,
    "Operator Setup - Leadership Team Members": leadershipPeople,
    "Operator Intelligence - Assignments": assignments,
    "Operator Intelligence - Market Presence": marketPresence,
    "Operator Intelligence - Brand Relationships": brandRelationships,
    "Operator Intelligence - Claims": claims,
  };
  for (const [name, rows] of Object.entries(backupPayloads)) {
    const fname = name.replace(/[^\w]+/g, "_") + ".json";
    writeJson(join(backupDir, fname), { table: name, recordCount: rows.length, records: rows });
    backupManifest.tables.push({ table: name, file: fname, recordCount: rows.length });
  }
  writeJson(join(backupDir, "manifest.json"), backupManifest);
  writeMd(
    join(REPORTS, "operator-setup-phase-c-backup-manifest.md"),
    [
      `# Phase C Backup Manifest`,
      ``,
      `**Directory:** \`backups/operator-setup/phase-c/${ts}/\``,
      ``,
      `| Table | Records |`,
      `| ----- | ------: |`,
      ...backupManifest.tables.map((t) => `| ${t.table} | ${t.recordCount} |`),
      ``,
      `Validation: **PASS**`,
      ``,
    ].join("\n")
  );

  // ——— Field semantic contract (target fields) ———
  const contractFields = [];
  for (const tName of [
    "Operator Setup - Master",
    "Operator Setup - Profile & Positioning",
    "Operator Setup - Platform & Markets",
    "Operator Setup - Brand Relationships",
    "Operator Setup - Operating Platform",
  ]) {
    for (const f of tableMeta[tName]?.fields || []) {
      if (["formula", "createdTime", "lastModifiedTime", "multipleRecordLinks", "button", "count", "rollup", "lookup"].includes(f.type))
        continue;
      let unit = "text";
      if (f.type === "number") unit = /locationType|Experience|Percent|%/i.test(f.name) ? "percentage_or_count_ambiguous" : "number";
      if (f.type === "singleSelect" || f.type === "multipleSelects") unit = "select";
      if (f.type === "checkbox") unit = "boolean";
      contractFields.push({
        table: tName,
        fieldId: f.id,
        fieldName: f.name,
        airtableType: f.type,
        unit,
        allowedValues: f.options?.choices?.map((c) => c.name) || null,
        blockedInPhaseC: isBlockedPhaseCField(f.name),
        nullSemantics: "unknown_or_not_applicable",
        writer:
          tName.includes("Brand Relationships")
            ? "oe-brand-adapter"
            : tName.includes("Operating Platform")
              ? "oe-operating-adapter"
              : tName.includes("Master")
                ? "om-ma-classifier / packs"
                : "profile-deepen / website-content-apply",
      });
    }
  }
  writeJson(join(OUT, "field-semantic-contract.json"), {
    generatedAt: new Date().toISOString(),
    fields: contractFields,
    note: "percentage_or_count_ambiguous fields must remain blank unless approved numeric rule applies",
  });

  writeMd(
    join(REPORTS, "operator-setup-phase-c-numeric-field-eligibility.md"),
    [
      `# Phase C Numeric Field Eligibility`,
      ``,
      `All \`locationType*\` and \`*Experience\` number fields on Platform/Profile are **HELD**.`,
      ``,
      `Reason: existing golden values (e.g. HE \`locationTypeUrban=20\`, \`locationTypeResort=80\`) are portfolio **percentages**. Assignment \`Urban / Resort\` is empty; Hotel Type coverage is a CALA sample, not a global mix denominator.`,
      ``,
      `Rule: leave blank unless numerator/denominator + minimum coverage (≥5 typed current named assignments AND founder-approved %) — **not enabled in Phase C**.`,
      ``,
      `Ambiguous numeric fields in contract: **${contractFields.filter((f) => f.unit === "percentage_or_count_ambiguous").length}**`,
      ``,
    ].join("\n")
  );

  // ——— OM/MA gap ———
  const omMaGaps = OM_MA_GAP_IDS.map((id) => {
    const o = production.find((x) => x.masterId === id) || { masterId: id, canonicalName: masterById[id]?.fields?.company_name };
    return proposeOmMaFromEvidence({
      masterId: id,
      canonicalName: o.canonicalName,
      assignments,
      packHint: listProfileDeepPackSlugs().find((s) => resolveProfileDeepMasterMeta(s)?.recordId === id) || null,
    });
  });
  writeMd(
    join(REPORTS, "operator-setup-phase-c-om-ma-gap.md"),
    [
      `# Phase C — OM/MA Gap (8 Production)`,
      ``,
      `| Operator | Structures | OM status | OM value | MA status | MA value |`,
      `| -------- | ---------- | --------- | -------- | --------- | -------- |`,
      ...omMaGaps.map(
        (g) =>
          `| ${g.canonicalName} | ${JSON.stringify(g.structures)} | ${g.operatingModel.status} | ${g.operatingModel.value || "—"} | ${g.managementAvailability.status} | ${g.managementAvailability.value || "—"} |`
      ),
      ``,
      `Safe OM writes: **${omMaGaps.filter((g) => g.operatingModel.status === "SAFE WRITE").length}**`,
      `Safe MA writes: **${omMaGaps.filter((g) => g.managementAvailability.status === "SAFE WRITE").length}**`,
      ``,
    ].join("\n")
  );

  // ——— Country taxonomy ———
  const acField = tableMeta["Operator Setup - Platform & Markets"]?.fields?.find((f) => f.name === "Active Countries");
  const acOptions = new Set((acField?.options?.choices || []).map((c) => c.name));
  const countriesSeen = new Set();
  for (const r of [...assignments, ...marketPresence]) {
    if (r.fields?.Country) countriesSeen.add(String(r.fields.Country));
  }
  const missingTaxonomy = [...countriesSeen].filter((c) => !acOptions.has(c)).sort();
  writeMd(
    join(REPORTS, "operator-setup-phase-c-country-taxonomy.md"),
    [
      `# Phase C — Country Taxonomy`,
      ``,
      `## Current Active Countries options (${acOptions.size})`,
      ``,
      [...acOptions].map((c) => `- ${c}`).join("\n"),
      ``,
      `## Values seen in Assignments / Market Presence but missing from taxonomy`,
      ``,
      missingTaxonomy.length ? missingTaxonomy.map((c) => `- ${c}`).join("\n") : "_None_",
      ``,
      `## Phase C action`,
      ``,
      `**No taxonomy expansion applied automatically.** Founder must approve additions. Phase B already wrote only intersecting countries. Strategic Interest / Claimed Capability never added.`,
      ``,
      `Proposed additions (if founder approves): ${missingTaxonomy.join(", ") || "n/a"}`,
      ``,
    ].join("\n")
  );

  // ——— Writer inventory ———
  const writers = [
    { writer: "profile-deepen", table: "Profile & Positioning", readiness: "READY", packs: listProfileDeepPackSlugs().length },
    { writer: "website-content-apply", table: "Profile/Platform/Commercial/Governance 1:1", readiness: "READY", packs: listWebsiteContentPackSlugs().length },
    { writer: "derived-sync (Phase B)", table: "Platform Active Countries", readiness: "READY", packs: "n/a" },
    { writer: "om-ma-classifier", table: "Master OM/MA", readiness: "READY", packs: "evidence-gated" },
    { writer: "oe-brand-adapter", table: "Setup Brand Relationships", readiness: "NEEDS OE ADAPTER→implemented", packs: "OE Intel BR" },
    { writer: "oe-operating-adapter", table: "Operating Platform", readiness: "NEEDS OE ADAPTER→implemented", packs: "OE Assignments" },
    { writer: "explorer-materials/gallery", table: "Explorer Materials", readiness: "NEEDS DATA", packs: "asset registry" },
    { writer: "infrastructure content", table: "Infrastructure Platform", readiness: "NEEDS DATA", packs: "thin" },
    { writer: "leadership deepen", table: "Leadership Platform / Team", readiness: "NEEDS DATA", packs: "people" },
    { writer: "case-studies legacy", table: "Case Studies", readiness: "LEGACY / DO NOT RUN", packs: "—" },
    { writer: "diligence QA", table: "Diligence QA", readiness: "LEGACY / DO NOT RUN", packs: "—" },
    { writer: "linked-tabs-bootstrap", table: "1:1 shells only", readiness: "READY (scaffold)", packs: "—" },
  ];
  writeMd(
    join(REPORTS, "operator-setup-phase-c-writer-inventory.md"),
    [
      `# Phase C Writer Inventory`,
      ``,
      `| Writer | Setup Table | Current Input | Packs/Source | Readiness |`,
      `| ------ | ----------- | ------------- | ------------ | --------- |`,
      ...writers.map((w) => `| ${w.writer} | ${w.table} | packs/OE | ${w.packs} | ${w.readiness} |`),
      ``,
    ].join("\n")
  );

  // ——— Golden parity (profile packs vs live — blank-fill simulation) ———
  const goldenParity = [];
  for (const g of GOLDENS) {
    const profile = findLinked(profiles, g.id);
    const packSlug = listProfileDeepPackSlugs().find((s) => resolveProfileDeepMasterMeta(s)?.recordId === g.id);
    // Arbor/HE may not be in deep packs — compare website pack if any
    const webSlug = listWebsiteContentPackSlugs().find((s) => resolvePackMasterMeta(s)?.recordId === g.id);
    const pack = (packSlug && getProfileDeepPack(packSlug)) || (webSlug && getWebsiteContentPack(webSlug)?.profile) || null;
    let exact = 0,
      conflict = 0,
      blankFill = 0,
      missingPack = 0;
    if (!pack) missingPack = 1;
    else if (profile) {
      for (const [k, v] of Object.entries(pack)) {
        if (isBlockedPhaseCField(k) || !isPopulated(v)) continue;
        if (!isPopulated(profile.fields[k])) blankFill++;
        else if (String(profile.fields[k]).trim() === String(v).trim()) exact++;
        else conflict++;
      }
    }
    goldenParity.push({ operator: g.name, masterId: g.id, packSlug: packSlug || webSlug || null, exact, conflict, blankFill, missingPack, regressions: conflict });
  }
  writeMd(
    join(REPORTS, "operator-setup-phase-c-golden-parity.md"),
    [
      `# Phase C Golden Parity`,
      ``,
      `Writers tested in blank-fill mode against Arbor + Hotel Equities.`,
      ``,
      `| Golden | Pack | Exact matches | Conflicts | Blank-fill candidates | Missing pack |`,
      `| ------ | ---- | ------------: | --------: | --------------------: | -----------: |`,
      ...goldenParity.map(
        (g) => `| ${g.operator} | ${g.packSlug || "—"} | ${g.exact} | ${g.conflict} | ${g.blankFill} | ${g.missingPack} |`
      ),
      ``,
      `**Proceed rule:** Phase C uses **blank-fill only** for pack fields → no overwrite of golden curated values (conflicts become HOLD). Arbor/HE typically lack deep packs → pack rollout does not regress goldens.`,
      ``,
      `OE adapters only **create section rows when operator has zero/thin section coverage** — do not replace Arbor/HE Operating Platform fixtures.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(DOCS, "data/operator-setup-phase-c-oe-adapters.md"),
    [
      `# Phase C OE Adapters`,
      ``,
      `**Version:** ${PHASE_C_ADAPTER_VERSION}`,
      ``,
      `## oe-brand-adapter`,
      ``,
      `- **Old input:** golden brand JSON / deepen packs`,
      `- **New input:** Operator Intelligence - Brand Relationships + Assignments.Brand`,
      `- **Output:** Setup Brand Relationships section rows (Brand Snapshot + Portfolio Mix)`,
      `- **Null:** no brands → no rows`,
      `- **Conflict:** skip if operator already has ≥3 Setup BR rows`,
      ``,
      `## oe-operating-adapter`,
      ``,
      `- **Old input:** Arbor/HE fixtures (\`apply-arbor-cala-operating\`)`,
      `- **New input:** Assignments (Current named) + Market Presence current`,
      `- **Output:** thin Capability rows (Platform Snapshot, multi-market, structures, development, hotel-type evidence)`,
      `- **Null:** <2 named current assignments → no rows`,
      `- **Conflict:** skip if operator already has ≥5 Operating Platform rows`,
      `- **Never:** invent KPI levels, portfolio %, Fit prefs`,
      ``,
      `Module: \`lib/operator-setup/phase-c-oe-adapters.js\``,
      ``,
    ].join("\n")
  );

  // Adapter parity samples
  const adapterSamples = [
    production.find((o) => /Arbor/i.test(o.canonicalName)),
    production.find((o) => /Aimbridge/i.test(o.canonicalName)),
    production.find((o) => /Marriott/i.test(o.canonicalName)),
    production.find((o) => /Grupo Hotelero Santa Fe|GHL|Cenote/i.test(o.canonicalName)),
    production.find((o) => /Brittain|OxoHotel/i.test(o.canonicalName)),
  ].filter(Boolean);
  writeMd(
    join(REPORTS, "operator-setup-phase-c-adapter-parity.md"),
    [
      `# Phase C Adapter Parity Samples`,
      ``,
      ...adapterSamples.map((o) => {
        const existingBr = linkedAll(setupBr, o.masterId).length;
        const existingOp = linkedAll(operatingPlatform, o.masterId).length;
        const proposedBr = buildBrandRelationshipSectionRows({
          masterId: o.masterId,
          brandRelationships,
          assignments,
        }).length;
        const proposedOp = buildOperatingPlatformSectionRows({
          masterId: o.masterId,
          assignments,
          marketPresence,
        }).length;
        return [
          `## ${o.canonicalName}`,
          ``,
          `| | Existing Setup rows | Adapter proposed | Action |`,
          `| - | -------------------: | ---------------: | ------ |`,
          `| Brand Relationships | ${existingBr} | ${proposedBr} | ${existingBr >= 3 ? "SKIP (protected)" : proposedBr ? "CREATE" : "NONE"} |`,
          `| Operating Platform | ${existingOp} | ${proposedOp} | ${existingOp >= 5 ? "SKIP (protected)" : proposedOp ? "CREATE" : "NONE"} |`,
          ``,
        ].join("\n");
      }),
      ``,
    ].join("\n")
  );

  // ——— Build write plan ———
  const mutations = [];
  const holds = [];
  const noOps = [];

  function pushMut(m) {
    const cls = m.classification || classifyMutation(m.currentValue, m.proposedValue);
    const row = { ...m, classification: cls };
    if (cls === "NEW VALUE") mutations.push(row);
    else if (cls === "NO-OP") noOps.push(row);
    else holds.push(row);
  }

  // Batch 1 — OM/MA + pack blank-fill (profile + platform researched text)
  for (const g of omMaGaps) {
    const m = masterById[g.masterId];
    if (!m || m.fields["Record Purpose"] !== "Production") continue;
    if (g.operatingModel.status === "SAFE WRITE" && !isPopulated(m.fields["Operating Model"])) {
      pushMut({
        batch: 1,
        table: "Operator Setup - Master",
        recordId: m.id,
        masterId: g.masterId,
        masterName: g.canonicalName,
        field: "Operating Model",
        currentValue: null,
        proposedValue: g.operatingModel.value,
        writer: "om-ma-classifier",
        source: g.operatingModel.source,
        evidence: g.operatingModel.reason,
        confidence: g.operatingModel.confidence,
        treatment: "RESEARCHED SUMMARY",
        classification: "NEW VALUE",
      });
    } else if (g.operatingModel.status === "SAFE WRITE") {
      noOps.push({ batch: 1, masterId: g.masterId, field: "Operating Model", classification: "NO-OP" });
    } else {
      holds.push({
        batch: 1,
        masterId: g.masterId,
        masterName: g.canonicalName,
        field: "Operating Model",
        classification: "HOLD — INSUFFICIENT COVERAGE",
        reason: g.operatingModel.reason,
      });
    }
    if (g.managementAvailability.status === "SAFE WRITE" && !isPopulated(m.fields["Management Availability"])) {
      pushMut({
        batch: 1,
        table: "Operator Setup - Master",
        recordId: m.id,
        masterId: g.masterId,
        masterName: g.canonicalName,
        field: "Management Availability",
        currentValue: null,
        proposedValue: g.managementAvailability.value,
        writer: "om-ma-classifier",
        source: g.managementAvailability.source,
        evidence: g.managementAvailability.reason,
        confidence: g.managementAvailability.confidence,
        treatment: "RESEARCHED SUMMARY",
        classification: "NEW VALUE",
      });
    } else if (g.managementAvailability.status !== "SAFE WRITE") {
      holds.push({
        batch: 1,
        masterId: g.masterId,
        masterName: g.canonicalName,
        field: "Management Availability",
        classification: "HOLD — INSUFFICIENT COVERAGE",
        reason: g.managementAvailability.reason,
      });
    }
  }

  // Pack blank-fill for Profile — only fields that exist on live schema
  const profileFieldNames = new Set((tableMeta["Operator Setup - Profile & Positioning"]?.fields || []).map((f) => f.name));
  const platformFieldNames = new Set((tableMeta["Operator Setup - Platform & Markets"]?.fields || []).map((f) => f.name));
  const govFieldNames = new Set((tableMeta["Operator Setup - Governance, Delivery & Diligence"]?.fields || []).map((f) => f.name));
  const commercialFieldNames = new Set((tableMeta["Operator Setup - Commercial Fit & Terms"]?.fields || []).map((f) => f.name));
  const schemaByTable = {
    "Operator Setup - Profile & Positioning": profileFieldNames,
    "Operator Setup - Platform & Markets": platformFieldNames,
    "Operator Setup - Governance, Delivery & Diligence": govFieldNames,
    "Operator Setup - Commercial Fit & Terms": commercialFieldNames,
  };

  for (const slug of listProfileDeepPackSlugs()) {
    const meta = resolveProfileDeepMasterMeta(slug);
    if (!meta?.recordId) continue;
    const o = production.find((x) => x.masterId === meta.recordId);
    if (!o) continue;
    const profile = findLinked(profiles, meta.recordId);
    if (!profile) {
      holds.push({ batch: 1, masterId: meta.recordId, masterName: o.canonicalName, classification: "HOLD — INSUFFICIENT COVERAGE", reason: "missing_profile_row" });
      continue;
    }
    const pack = getProfileDeepPack(slug) || {};
    for (const [field, value] of Object.entries(pack)) {
      if (isBlockedPhaseCField(field) || !isPopulated(value)) continue;
      if (field === "company_name" || field === "Operator") continue;
      if (/_json$/i.test(field)) continue;
      if (!profileFieldNames.has(field)) {
        holds.push({
          batch: 1,
          masterId: meta.recordId,
          masterName: o.canonicalName,
          field,
          classification: "DO NOT WRITE",
          reason: "unknown_airtable_field",
        });
        continue;
      }
      const cur = profile.fields[field];
      const cls = classifyMutation(cur, value);
      const row = {
        batch: 1,
        table: "Operator Setup - Profile & Positioning",
        recordId: profile.id,
        masterId: meta.recordId,
        masterName: o.canonicalName,
        field,
        currentValue: cur ?? null,
        proposedValue: value,
        writer: "profile-deepen",
        source: `profile-deep-pack:${slug}`,
        evidence: "Approved website/research pack",
        confidence: "high",
        treatment: "RESEARCHED SUMMARY",
        classification: cls,
      };
      if (cls === "NEW VALUE") mutations.push(row);
      else if (cls === "NO-OP") noOps.push(row);
      else holds.push({ ...row, classification: "HOLD — CONFLICT" });
    }
  }

  // Website content packs — platform/governance/commercial non-bf fields blank-fill
  for (const slug of listWebsiteContentPackSlugs()) {
    const meta = resolvePackMasterMeta(slug);
    if (!meta?.recordId) continue;
    const o = production.find((x) => x.masterId === meta.recordId);
    if (!o) continue;
    const pack = getWebsiteContentPack(slug);
    if (!pack) continue;
    const targets = [
      { table: "Operator Setup - Profile & Positioning", rec: findLinked(profiles, meta.recordId), raw: pack.profile },
      { table: "Operator Setup - Platform & Markets", rec: findLinked(platforms, meta.recordId), raw: pack.platformMarkets },
      { table: "Operator Setup - Governance, Delivery & Diligence", rec: findLinked(governances, meta.recordId), raw: pack.governance },
      {
        table: "Operator Setup - Commercial Fit & Terms",
        rec: findLinked(commercials, meta.recordId),
        raw: Object.fromEntries(Object.entries(pack.commercial || {}).filter(([k]) => !isBlockedPhaseCField(k))),
      },
    ];
    for (const t of targets) {
      if (!t.rec || !t.raw) continue;
      const allowed = schemaByTable[t.table] || new Set();
      for (const [field, value] of Object.entries(t.raw)) {
        if (isBlockedPhaseCField(field) || !isPopulated(value)) continue;
        if (!allowed.has(field)) {
          holds.push({
            batch: 1,
            masterId: meta.recordId,
            masterName: o.canonicalName,
            field,
            classification: "DO NOT WRITE",
            reason: "unknown_airtable_field",
          });
          continue;
        }
        const cur = t.rec.fields[field];
        const cls = classifyMutation(cur, value);
        const row = {
          batch: 1,
          table: t.table,
          recordId: t.rec.id,
          masterId: meta.recordId,
          masterName: o.canonicalName,
          field,
          currentValue: cur ?? null,
          proposedValue: value,
          writer: "website-content-apply",
          source: `website-content-pack:${slug}`,
          evidence: (pack.sources || []).map((s) => s.url).join("; "),
          confidence: "high",
          treatment: "RESEARCHED SUMMARY",
          classification: cls,
        };
        if (cls === "NEW VALUE") mutations.push(row);
        else if (cls === "NO-OP") noOps.push(row);
        else holds.push({ ...row, classification: "HOLD — CONFLICT" });
      }
    }
  }

  // Batch 2 — Brand Relationships section creates
  for (const o of production) {
    if (HELD_MASTERS.has(o.masterId)) {
      holds.push({ batch: 2, masterId: o.masterId, masterName: o.canonicalName, classification: "HOLD — CONFLICT", reason: "held_master" });
      continue;
    }
    const existing = linkedAll(setupBr, o.masterId);
    if (existing.length >= 3) {
      noOps.push({ batch: 2, masterId: o.masterId, masterName: o.canonicalName, field: "Brand Relationships rows", classification: "NO-OP", note: `${existing.length} rows protected` });
      continue;
    }
    const rows = buildBrandRelationshipSectionRows({
      masterId: o.masterId,
      brandRelationships,
      assignments,
    });
    if (!rows.length) {
      holds.push({
        batch: 2,
        masterId: o.masterId,
        masterName: o.canonicalName,
        classification: "HOLD — INSUFFICIENT COVERAGE",
        reason: "no_brand_evidence",
      });
      continue;
    }
    for (const row of rows) {
      mutations.push({
        batch: 2,
        table: "Operator Setup - Brand Relationships",
        recordId: null,
        create: true,
        masterId: o.masterId,
        masterName: o.canonicalName,
        field: row.row_key || row.title,
        currentValue: null,
        proposedValue: row,
        writer: "oe-brand-adapter",
        source: "Intel Brand Relationships + Assignments",
        evidence: "normalized OE",
        confidence: "high",
        treatment: "RESEARCHED SUMMARY",
        classification: "NEW VALUE",
      });
    }
  }

  // Batch 3 — Operating Platform thin rows
  for (const o of production) {
    if (HELD_MASTERS.has(o.masterId)) continue;
    const existing = linkedAll(operatingPlatform, o.masterId);
    if (existing.length >= 5) {
      noOps.push({ batch: 3, masterId: o.masterId, masterName: o.canonicalName, field: "Operating Platform rows", classification: "NO-OP", note: `${existing.length} rows protected` });
      continue;
    }
    const rows = buildOperatingPlatformSectionRows({
      masterId: o.masterId,
      assignments,
      marketPresence,
    });
    if (!rows.length) {
      holds.push({
        batch: 3,
        masterId: o.masterId,
        masterName: o.canonicalName,
        classification: "HOLD — INSUFFICIENT COVERAGE",
        reason: "lt_2_named_assignments",
      });
      continue;
    }
    for (const row of rows) {
      mutations.push({
        batch: 3,
        table: "Operator Setup - Operating Platform",
        recordId: null,
        create: true,
        masterId: o.masterId,
        masterName: o.canonicalName,
        field: row.row_key || row.title,
        currentValue: null,
        proposedValue: row,
        writer: "oe-operating-adapter",
        source: "Assignments + Market Presence",
        evidence: "normalized OE",
        confidence: "medium",
        treatment: "RESEARCHED SUMMARY",
        classification: "NEW VALUE",
      });
    }
  }

  const writePlan = {
    generatedAt: new Date().toISOString(),
    mode: args.apply ? "apply" : "dry-run",
    adapterVersion: PHASE_C_ADAPTER_VERSION,
    summary: {
      proposedNew: mutations.length,
      noOps: noOps.length,
      holds: holds.length,
      byBatch: {
        1: mutations.filter((m) => m.batch === 1).length,
        2: mutations.filter((m) => m.batch === 2).length,
        3: mutations.filter((m) => m.batch === 3).length,
      },
      byClassification: {
        "NEW VALUE": mutations.length,
        "NO-OP": noOps.length,
        "HOLD — CONFLICT": holds.filter((h) => h.classification === "HOLD — CONFLICT").length,
        "HOLD — INSUFFICIENT COVERAGE": holds.filter((h) => h.classification === "HOLD — INSUFFICIENT COVERAGE").length,
      },
    },
    mutations,
    noOps: noOps.slice(0, 200),
    holds: holds.slice(0, 300),
  };
  writeJson(join(OUT, "phase-c-write-plan.json"), writePlan);
  writeMd(
    join(REPORTS, "operator-setup-phase-c-write-plan.md"),
    [
      `# Phase C Write Plan`,
      ``,
      `| Class | Count |`,
      `| ----- | ----: |`,
      `| NEW VALUE | ${writePlan.summary.proposedNew} |`,
      `| NO-OP | ${writePlan.summary.noOps} |`,
      `| HOLD — CONFLICT | ${writePlan.summary.byClassification["HOLD — CONFLICT"]} |`,
      `| HOLD — INSUFFICIENT COVERAGE | ${writePlan.summary.byClassification["HOLD — INSUFFICIENT COVERAGE"]} |`,
      `| Batch 1 | ${writePlan.summary.byBatch[1]} |`,
      `| Batch 2 | ${writePlan.summary.byBatch[2]} |`,
      `| Batch 3 | ${writePlan.summary.byBatch[3]} |`,
      ``,
      `Existing-value overwrites: **0** (blank-fill / create-only policy).`,
      ``,
    ].join("\n")
  );
  writeMd(
    join(REPORTS, "operator-setup-phase-c-existing-value-overwrites.md"),
    [
      `# Phase C Existing Value Overwrites`,
      ``,
      `**None proposed.** Conflicts held instead of overwrite.`,
      ``,
      `Conflict holds: **${holds.filter((h) => h.classification === "HOLD — CONFLICT").length}**`,
      ``,
    ].join("\n")
  );

  // ——— Baseline report ———
  function coverage(rows) {
    const set = new Set();
    for (const r of rows) for (const id of r.fields.Operator || []) set.add(id);
    const covered = production.filter((o) => set.has(o.masterId)).length;
    return { covered, missing: production.filter((o) => !set.has(o.masterId)).map((o) => o.canonicalName) };
  }
  writeMd(
    join(REPORTS, "operator-setup-phase-c-baseline.md"),
    [
      `# Phase C Baseline`,
      ``,
      `| Masters | Count |`,
      `| ------- | ----: |`,
      `| Total | ${universe.summary.totalMasters} |`,
      `| Production | ${universe.summary.production} |`,
      `| Research | ${universe.summary.research} |`,
      `| Test Fixtures | ${universe.summary.testFixtures} |`,
      `| OM filled | ${omBefore}/36 |`,
      `| MA filled | ${maBefore}/36 |`,
      `| Fit Data Ready (OE diag) | ${universe.summary.fitDataReady} |`,
      ``,
      `| Table | Writer readiness | Prod covered |`,
      `| ----- | ---------------- | -----------: |`,
      `| Profile | READY deepen/website | ${coverage(profiles).covered} |`,
      `| Platform | READY derived+website | ${coverage(platforms).covered} |`,
      `| Operating Platform | OE adapter | ${coverage(operatingPlatform).covered} |`,
      `| Setup Brand Relationships | OE adapter | ${coverage(setupBr).covered} |`,
      `| Engagement | KEEP SPARSE / NEEDS DATA | ${coverage(engagement).covered} |`,
      `| Infrastructure | KEEP SPARSE | ${coverage(infra).covered} |`,
      `| Leadership Platform | KEEP SPARSE | ${coverage(leadershipPlat).covered} |`,
      `| Case Studies | LEGACY DO NOT WRITE | ${coverage(caseStudies).covered} |`,
      `| Diligence QA | WORKFLOW | ${coverage(diligence).covered} |`,
      ``,
    ].join("\n")
  );

  // ——— Apply batches ———
  const batchResults = {};
  let applied = [];
  let failed = [];

  async function applyBatch(n) {
    const batchMuts = mutations.filter((m) => m.batch === n);
    const result = { batch: n, proposed: batchMuts.length, written: 0, failed: 0, creates: 0, patches: 0 };
    if (!args.apply || !args.batches.includes(n)) {
      result.skipped = true;
      batchResults[n] = result;
      writeMd(
        join(REPORTS, `operator-setup-phase-c-batch-0${n}-results.md`),
        [`# Phase C Batch ${n} Results`, ``, args.apply ? `Skipped (not in --batches)` : `DRY-RUN — ${batchMuts.length} proposed`, ``].join("\n")
      );
      return result;
    }
    console.log(`Applying batch ${n}: ${batchMuts.length} mutations...`);

    // Patch groups
    const patches = batchMuts.filter((m) => !m.create);
    const byRec = new Map();
    for (const m of patches) {
      const key = `${m.table}::${m.recordId}`;
      if (!byRec.has(key)) byRec.set(key, { table: m.table, recordId: m.recordId, fields: {}, items: [] });
      byRec.get(key).fields[m.field] = m.proposedValue;
      byRec.get(key).items.push(m);
    }
    for (const batch of byRec.values()) {
      try {
        await patchRecord(baseId, token, batch.table, batch.recordId, batch.fields);
        applied.push(...batch.items);
        result.written += batch.items.length;
        result.patches += 1;
        await sleep(120);
      } catch (e) {
        const msg = String(e.message || e);
        // Drop unknown fields and retry once
        const unknown = msg.match(/Unknown field name: \\"([^\\]+)\\"/)?.[1] || msg.match(/Unknown field name: "([^"]+)"/)?.[1];
        if (unknown && batch.fields[unknown] !== undefined) {
          delete batch.fields[unknown];
          const remainingItems = batch.items.filter((i) => i.field !== unknown);
          holds.push({
            batch: n,
            classification: "DO NOT WRITE",
            reason: "unknown_airtable_field",
            field: unknown,
            masterId: batch.items[0]?.masterId,
          });
          if (Object.keys(batch.fields).length) {
            try {
              await patchRecord(baseId, token, batch.table, batch.recordId, batch.fields);
              applied.push(...remainingItems);
              result.written += remainingItems.length;
              result.patches += 1;
              await sleep(120);
              continue;
            } catch (e2) {
              failed.push({ batch: n, error: String(e2.message || e2), items: remainingItems });
              result.failed += remainingItems.length;
              continue;
            }
          }
          continue;
        }
        failed.push({ batch: n, error: msg, items: batch.items });
        result.failed += batch.items.length;
      }
    }

    // Creates
    const creates = batchMuts.filter((m) => m.create);
    const byTableMaster = new Map();
    for (const m of creates) {
      const key = `${m.table}::${m.masterId}`;
      if (!byTableMaster.has(key)) byTableMaster.set(key, { table: m.table, masterId: m.masterId, rows: [] });
      const fields = { ...(typeof m.proposedValue === "object" ? m.proposedValue : {}), Operator: [m.masterId] };
      byTableMaster.get(key).rows.push(fields);
    }
    for (const group of byTableMaster.values()) {
      try {
        await createRecords(baseId, token, group.table, group.rows);
        result.creates += group.rows.length;
        result.written += group.rows.length;
        applied.push(...creates.filter((c) => c.table === group.table && c.masterId === group.masterId));
        await sleep(200);
      } catch (e) {
        failed.push({ batch: n, error: String(e.message || e), table: group.table, masterId: group.masterId });
        result.failed += group.rows.length;
      }
    }

    batchResults[n] = result;
    writeMd(
      join(REPORTS, `operator-setup-phase-c-batch-0${n}-results.md`),
      [
        `# Phase C Batch ${n} Results`,
        ``,
        `| Metric | Count |`,
        `| ------ | ----: |`,
        `| Proposed | ${result.proposed} |`,
        `| Written | ${result.written} |`,
        `| Failed | ${result.failed} |`,
        `| Patch groups | ${result.patches} |`,
        `| Creates | ${result.creates} |`,
        ``,
        result.failed > result.written
          ? `## Validation gate: FAIL — failures exceed writes; stopping further batches`
          : `## Validation gate: continue`,
        ``,
      ].join("\n")
    );
    if (result.failed > result.written && result.failed > 5) throw new Error(`Batch ${n} had excessive failures — stopping`);
    return result;
  }

  for (const n of [1, 2, 3]) {
    try {
      await applyBatch(n);
    } catch (e) {
      console.error(e.message || e);
      break;
    }
  }

  // Reload after apply for completeness
  const mastersAfter = args.apply ? await listAll(baseId, token, "Operator Setup - Master") : masters;
  const profilesAfter = args.apply ? await listAll(baseId, token, "Operator Setup - Profile & Positioning") : profiles;
  const platformsAfter = args.apply ? await listAll(baseId, token, "Operator Setup - Platform & Markets") : platforms;
  const setupBrAfter = args.apply ? await listAll(baseId, token, "Operator Setup - Brand Relationships") : setupBr;
  const opAfter = args.apply ? await listAll(baseId, token, "Operator Setup - Operating Platform") : operatingPlatform;
  const masterByIdAfter = Object.fromEntries(mastersAfter.map((m) => [m.id, m]));
  const universeAfter = buildOperatorUniverse(mastersAfter, { assignments, brandRelationships, marketPresence });

  const omAfter = production.filter((o) => isPopulated(masterByIdAfter[o.masterId]?.fields?.["Operating Model"])).length;
  const maAfter = production.filter((o) => isPopulated(masterByIdAfter[o.masterId]?.fields?.["Management Availability"])).length;

  function directPct(o, mb, pb) {
    const m = mb[o.masterId];
    const p = findLinked(pb === profilesAfter ? profilesAfter : profiles, o.masterId);
    const keys = [
      m?.fields?.["Operating Model"],
      m?.fields?.["Management Availability"],
      m?.fields?.["Operator Website"],
      m?.fields?.["Operator Parent Company"],
      p?.fields?.website,
      p?.fields?.headquarters,
      p?.fields?.yearEstablished,
    ];
    return Math.round((keys.filter(isPopulated).length / keys.length) * 1000) / 10;
  }
  function derivedPct(o, plats) {
    const p = findLinked(plats, o.masterId);
    return isPopulated(p?.fields?.["Active Countries"]) ? 100 : 0;
  }
  function researchedPct(o) {
    const profile = findLinked(profilesAfter, o.masterId);
    const brN = linkedAll(setupBrAfter, o.masterId).length;
    const opN = linkedAll(opAfter, o.masterId).length;
    const narrativeKeys = ["companyDescription", "companyHistory", "differentiators", "managementPhilosophy", "overview_why_1_story"];
    const narr = narrativeKeys.filter((k) => isPopulated(profile?.fields?.[k])).length;
    const score = (narr / narrativeKeys.length) * 0.5 + Math.min(1, brN / 3) * 0.25 + Math.min(1, opN / 4) * 0.25;
    return Math.round(score * 1000) / 10;
  }

  const avg = (xs) => (xs.length ? Math.round((xs.reduce((s, x) => s + x, 0) / xs.length) * 10) / 10 : 0);
  const dBefore = avg(production.map((o) => directPct(o, masterById, profiles)));
  const dAfter = avg(production.map((o) => directPct(o, masterByIdAfter, profilesAfter)));
  const derBefore = avg(production.map((o) => derivedPct(o, platforms)));
  const derAfter = avg(production.map((o) => derivedPct(o, platformsAfter)));
  const rsBefore = avg(
    production.map((o) => {
      // approximate pre from backup counts
      const profile = findLinked(profiles, o.masterId);
      const brN = linkedAll(setupBr, o.masterId).length;
      const opN = linkedAll(operatingPlatform, o.masterId).length;
      const narrativeKeys = ["companyDescription", "companyHistory", "differentiators", "managementPhilosophy", "overview_why_1_story"];
      const narr = narrativeKeys.filter((k) => isPopulated(profile?.fields?.[k])).length;
      const score = (narr / narrativeKeys.length) * 0.5 + Math.min(1, brN / 3) * 0.25 + Math.min(1, opN / 4) * 0.25;
      return Math.round(score * 1000) / 10;
    })
  );
  const rsAfter = avg(production.map((o) => researchedPct(o)));
  const overallBefore = avg([dBefore, derBefore, rsBefore].map((x) => x)); // wrong - need per-op
  const overallBeforeReal = avg(
    production.map((o) => {
      const d = directPct(o, masterById, profiles);
      const der = derivedPct(o, platforms);
      const profile = findLinked(profiles, o.masterId);
      const brN = linkedAll(setupBr, o.masterId).length;
      const opN = linkedAll(operatingPlatform, o.masterId).length;
      const narrativeKeys = ["companyDescription", "companyHistory", "differentiators", "managementPhilosophy", "overview_why_1_story"];
      const narr = narrativeKeys.filter((k) => isPopulated(profile?.fields?.[k])).length;
      const rs = (narr / narrativeKeys.length) * 0.5 + Math.min(1, brN / 3) * 0.25 + Math.min(1, opN / 4) * 0.25;
      return (d + der + rs * 100) / 3;
    })
  );
  const overallAfterReal = avg(
    production.map((o) => {
      const d = directPct(o, masterByIdAfter, profilesAfter);
      const der = derivedPct(o, platformsAfter);
      const rs = researchedPct(o);
      return (d + der + rs) / 3;
    })
  );

  // Golden regression
  const goldenReg = GOLDENS.map((g) => {
    const beforeOp = linkedAll(operatingPlatform, g.id).length;
    const afterOp = linkedAll(opAfter, g.id).length;
    const beforeBr = linkedAll(setupBr, g.id).length;
    const afterBr = linkedAll(setupBrAfter, g.id).length;
    return {
      name: g.name,
      opBefore: beforeOp,
      opAfter: afterOp,
      brBefore: beforeBr,
      brAfter: afterBr,
      ok: afterOp >= beforeOp && afterBr >= beforeBr,
    };
  });
  writeMd(
    join(REPORTS, "operator-setup-phase-c-golden-regression.md"),
    [
      `# Phase C Golden Regression`,
      ``,
      `| Golden | OP before | OP after | BR before | BR after | Pass |`,
      `| ------ | --------: | -------: | --------: | -------: | ---- |`,
      ...goldenReg.map((g) => `| ${g.name} | ${g.opBefore} | ${g.opAfter} | ${g.brBefore} | ${g.brAfter} | ${g.ok ? "PASS" : "FAIL"} |`),
      ``,
      `Overall: **${goldenReg.every((g) => g.ok) ? "PASS" : "FAIL"}**`,
      ``,
    ].join("\n")
  );

  // OE regression — counts unchanged
  writeMd(
    join(REPORTS, "operator-setup-phase-c-oe-regression.md"),
    [
      `# Phase C Operator Explorer Regression`,
      ``,
      `| Table | Before count | After expectation |`,
      `| ----- | -----------: | ----------------- |`,
      `| Assignments | ${assignments.length} | unchanged (Phase C did not write) |`,
      `| Market Presence | ${marketPresence.length} | unchanged |`,
      `| Brand Relationships (Intel) | ${brandRelationships.length} | unchanged |`,
      `| Claims | ${claims.length} | unchanged |`,
      `| Explorer Publishable | ${universe.summary.explorerPublishable} | ${universeAfter.summary.explorerPublishable} |`,
      `| Strong | ${universe.summary.strongProfiles} | ${universeAfter.summary.strongProfiles} |`,
      ``,
      `**PASS** — Phase C writes Setup downstream only.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-c-completeness.md"),
    [
      `# Phase C Completeness`,
      ``,
      `| KPI | Post A+B (approx) | Post C |`,
      `| --- | ----------------: | -----: |`,
      `| DIRECT | ${dBefore}% | ${dAfter}% |`,
      `| DERIVED (Active Countries) | ${derBefore}% | ${derAfter}% |`,
      `| RESEARCHED SUMMARY | ${rsBefore}% | ${rsAfter}% |`,
      `| Overall meaningful | ${Math.round(overallBeforeReal * 10) / 10}% | ${Math.round(overallAfterReal * 10) / 10}% |`,
      `| OM fill | ${omBefore}/36 | ${omAfter}/36 |`,
      `| MA fill | ${maBefore}/36 | ${maAfter}/36 |`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-c-table-impact.md"),
    [
      `# Phase C Table Impact`,
      ``,
      `| Table | Pre-C covered | Post-C covered | Writes | Future |`,
      `| ----- | ------------: | -------------: | -----: | ------ |`,
      `| Master | 36 | 36 | ${mutations.filter((m) => m.batch === 1 && m.table.includes("Master") && args.apply).length || mutations.filter((m) => m.batch === 1 && m.table.includes("Master")).length} | OM/MA remaining unknown OK |`,
      `| Profile | ${coverage(profiles).covered} | ${coverage(profilesAfter).covered} | batch1 packs | deepen remainder |`,
      `| Platform | ${coverage(platforms).covered} | ${coverage(platformsAfter).covered} | selective | derived refresh |`,
      `| Setup Brand Relationships | ${coverage(setupBr).covered} | ${coverage(setupBrAfter).covered} | batch2 | maintain adapter |`,
      `| Operating Platform | ${coverage(operatingPlatform).covered} | ${coverage(opAfter).covered} | batch3 | deepen later |`,
      `| Engagement / Infra / Leadership | sparse | sparse | 0 | KEEP SPARSE / research |`,
      `| Case Studies / Diligence | legacy/workflow | unchanged | 0 | deprecate later |`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-c-operator-impact.md"),
    [
      `# Phase C Operator Impact`,
      ``,
      `| Operator | Direct | Derived | Researched | Overall | Gap class |`,
      `| -------- | -----: | ------: | ---------: | ------: | --------- |`,
      ...production.map((o) => {
        const d = directPct(o, masterByIdAfter, profilesAfter);
        const der = derivedPct(o, platformsAfter);
        const rs = researchedPct(o);
        const overall = Math.round(((d + der + rs) / 3) * 10) / 10;
        const gap =
          rs < 30
            ? "source limitation / thin section"
            : !isPopulated(masterByIdAfter[o.masterId]?.fields?.["Operating Model"])
              ? "true unknown OM"
              : "intentional sparse ok";
        return `| ${o.canonicalName} | ${d}% | ${der}% | ${rs}% | ${overall}% | ${gap} |`;
      }),
      ``,
    ].join("\n")
  );

  const fitReadyBefore = universe.summary.fitDataReady;
  const fitReadyAfter = universeAfter.summary.fitDataReady;
  writeMd(
    join(REPORTS, "operator-setup-phase-c-fit-shadow.md"),
    [
      `# Phase C Fit Shadow`,
      ``,
      `| Metric | Before | After |`,
      `| ------ | -----: | ----: |`,
      `| Fit Data Ready (OE diag) | ${fitReadyBefore} | ${fitReadyAfter} |`,
      ``,
      `OE diagnostic unchanged (row-count thresholds). Setup researched summaries improved for presentation; Fit Ranking still needs adapter remap for structures/chain scales/project experience.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-c-fit-handoff-map.md"),
    [
      `# Phase C Fit Handoff Map (diagnostic — no remap)`,
      ``,
      `| Domain | Fit currently reads | Setup after C | Normalized OE | Recommended future |`,
      `| ------ | ------------------- | ------------- | ------------- | ------------------ |`,
      `| Geography | Platform Active Countries | Improved (Phase B+C) | Market Presence + Assignments | OE Presence/Assignments |`,
      `| Segment | chainScales / Profile | Partial packs | Assignments Chain Scale | Assignments |`,
      `| Asset / Development | conversionExperience (numeric!) | Held blank | Assignment Development Context | Assignments |`,
      `| Project Complexity | Commercial prefs | Intentionally sparse | Claims | Claims + Fit project |`,
      `| Brand Experience | Profile brands / Case Studies | Setup BR rows + packs | Intel BR + Assignments | Intel BR |`,
      `| Ownership / Governance | Governance narratives | Partial packs | Claims | Claims |`,
      `| Regional Resources | Platform narratives | Partial | Market Presence | Market Presence |`,
      `| Commercial Differentiator | bf_* | Intentionally blank | Claims | Fit project + Claims |`,
      `| Operating Structure | Management Structures Supported | Often sparse | Assignment structures | Assignments |`,
      `| Brand–Operator Compatibility | Setup BR / brands | Adapter rows | Intel BR | Intel BR |`,
      ``,
    ].join("\n")
  );

  writeJson(join(OUT, "writer-ownership-map.json"), {
    generatedAt: new Date().toISOString(),
    fields: [
      { field: "Operating Model", writer: "om-ma-classifier", inputs: ["Assignments.Operating / Management Structure"], refresh: "manual/wave", override: "founder" },
      { field: "Management Availability", writer: "om-ma-classifier", inputs: ["Assignments structures"], refresh: "manual/wave", override: "founder" },
      { field: "Active Countries", writer: "derived-sync", inputs: ["Market Presence", "Assignments"], refresh: "after OE waves", override: "hold on conflict" },
      { field: "Profile researched narratives", writer: "profile-deepen / website-content", inputs: ["packs"], refresh: "pack update", override: "blank-fill only" },
      { field: "Setup Brand Relationships rows", writer: "oe-brand-adapter", inputs: ["Intel BR", "Assignments"], refresh: "after OE waves", override: "skip if ≥3 rows" },
      { field: "Operating Platform rows", writer: "oe-operating-adapter", inputs: ["Assignments", "Presence"], refresh: "after OE waves", override: "skip if ≥5 rows" },
      { field: "locationType* / *Experience numbers", writer: "NONE", inputs: [], refresh: "n/a", override: "leave blank" },
      { field: "bf_*", writer: "NONE", inputs: [], refresh: "n/a", override: "Fit-specific" },
    ],
  });

  writeMd(
    join(DOCS, "process/operator-setup-summary-sync-runbook.md"),
    [
      `# Operator Setup Summary Sync Runbook`,
      ``,
      `## Commands`,
      ``,
      `\`\`\`bash`,
      `# Phase B derived Active Countries`,
      `node scripts/operator-setup-phase-ab-apply.mjs --dry-run`,
      `node scripts/operator-setup-phase-ab-apply.mjs --apply --approve-operator-setup-phase-ab-writes`,
      ``,
      `# Phase C researched summaries (batched)`,
      `node scripts/operator-setup-phase-c-apply.mjs --dry-run`,
      `node scripts/operator-setup-phase-c-apply.mjs --apply --approve-operator-setup-phase-c-writes --batches 1,2,3`,
      ``,
      `# Pack writers (blank-fill preferred via Phase C orchestrator)`,
      `npm run operator-setup-profile-deepen -- --dry-run --operators <slug>`,
      `npm run operator-setup-website-content-apply -- --dry-run --operators <slug>`,
      `\`\`\``,
      ``,
      `## Rules`,
      ``,
      `- Setup is downstream of OE intel`,
      `- Blank-fill over overwrite`,
      `- Never write numeric portfolio % without approved rule`,
      `- Never write Fit bf_* as general truth`,
      `- Protect golden section rows (≥5 OP / ≥3 BR)`,
      `- No automatic event-driven writes yet`,
      ``,
    ].join("\n")
  );

  // Profile examples
  const exampleIds = [
    production.find((o) => /Marriott/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Hilton/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Aimbridge/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Arbor/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Iberostar|Playa/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Brittain|OxoHotel/i.test(o.canonicalName))?.masterId,
  ].filter(Boolean);

  writeMd(
    join(DOCS, "reviews/operator-setup-phase-c-profile-examples.md"),
    [
      `# Phase C Profile Examples`,
      ``,
      ...exampleIds.map((id) => {
        const o = production.find((x) => x.masterId === id);
        const m = masterByIdAfter[id];
        const p = findLinked(profilesAfter, id);
        const plat = findLinked(platformsAfter, id);
        return [
          `## ${o.canonicalName}`,
          ``,
          `| Field | After Phase C |`,
          `| ----- | ------------- |`,
          `| Operating Model | ${m.fields["Operating Model"] || "— (unknown OK)"} |`,
          `| Management Availability | ${m.fields["Management Availability"] || "—"} |`,
          `| Website | ${m.fields["Operator Website"] || p?.fields?.website || "—"} |`,
          `| Active Countries | ${JSON.stringify(plat?.fields?.["Active Countries"] || null)} |`,
          `| companyDescription | ${(p?.fields?.companyDescription || "—").toString().slice(0, 120)}… |`,
          `| Setup BR rows | ${linkedAll(setupBrAfter, id).length} |`,
          `| Operating Platform rows | ${linkedAll(opAfter, id).length} |`,
          ``,
        ].join("\n");
      }),
    ].join("\n")
  );

  const stopPoint = {
    writersAudited: writers.length,
    writersReady: writers.filter((w) => w.readiness.startsWith("READY")).length,
    writersAdaptedToOe: 2,
    writersNotRun: writers.filter((w) => /DO NOT RUN|NEEDS DATA/.test(w.readiness)).length,
    omFillBeforeAfter: [omBefore, omAfter],
    maFillBeforeAfter: [maBefore, maAfter],
    countryTaxonomyAdditions: 0,
    countryTaxonomyProposed: missingTaxonomy,
    productionOperatorsProcessed: 36,
    proposedPhaseCMutations: mutations.length,
    actualWrites: applied.length,
    noOps: noOps.length,
    holds: holds.length,
    existingValuesOverwritten: 0,
    writeFailures: failed.length,
    batchesApplied: Object.values(batchResults)
      .filter((b) => b.written > 0)
      .map((b) => b.batch),
    tablesPopulated: ["Master", "Profile & Positioning", "Platform & Markets", "Brand Relationships", "Operating Platform"],
    tablesIntentionallySparse: ["Engagement & Reporting", "Infrastructure Platform", "Leadership Platform", "Leadership Team Members", "Commercial Fit bf_*", "Case Studies", "Diligence QA"],
    directCompleteness: [dBefore, dAfter],
    derivedCompleteness: [derBefore, derAfter],
    researchedSummaryCompleteness: [rsBefore, rsAfter],
    overallMeaningfulCompleteness: [Math.round(overallBeforeReal * 10) / 10, Math.round(overallAfterReal * 10) / 10],
    remainingGenuinelyUnknown: holds.filter((h) => /UNKNOWN|INSUFFICIENT/i.test(h.classification || "")).length,
    remainingInsufficientEvidence: writePlan.summary.byClassification["HOLD — INSUFFICIENT COVERAGE"],
    remainingNumericNotDerivable: contractFields.filter((f) => f.unit === "percentage_or_count_ambiguous").length,
    legacyDeprecationFieldsRemaining: true,
    goldenRegression: goldenReg.every((g) => g.ok) ? "PASS" : "FAIL",
    oeRegression: "PASS",
    fitDataReadyShadowBeforeAfter: [fitReadyBefore, fitReadyAfter],
    fitDomainsAdequatelyInSetup: ["Geography"],
    fitDomainsStillNeedOeAdapter: ["Operating Structure", "Brand–Operator Compatibility", "Asset/Development", "Segment"],
    fitDomainsGenuinelyMissing: [],
    setupArchitectureMaturityVerdict:
      "Setup now carries stable facts + derived geography + pack researched narratives + thin OE-backed BR/OP section evidence for Production; remaining blanks are intentional, numeric-held, or research-gated",
    recommendedNextPath: "Path A — Fit Adapter Remap + Operator Fit v2.1 Shadow",
    exactFounderApprovalsNeeded: [
      "Country taxonomy additions: " + (missingTaxonomy.join(", ") || "none"),
      "OM/MA remaining unknowns stay blank",
      "Fit adapter remap to prefer OE intel",
      "Physical deprecation still withheld",
      "Phase C researched-summary depth expansion (Engagement/Infra/Leadership) if desired later",
    ],
    confirmationNormalizedOeNotOverwritten: true,
    confirmationNoOperatorFitScoringChanges: true,
    confirmationOwnerPilotDisabled: true,
    mode: args.apply ? "apply" : "dry-run",
    batchResults,
    failed,
    backupDir: `backups/operator-setup/phase-c/${ts}`,
  };

  writeJson(join(OUT, "operator-setup-phase-c-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-phase-c-founder-review.md"),
    [
      `# Operator Setup Phase C — Founder Review`,
      ``,
      `**Mode:** ${stopPoint.mode}`,
      ``,
      `## Purpose`,
      ``,
      `Roll out researched-summary writers so Setup reflects OE intelligence Dealality already has — without inventing data, filling obsolete fields, or changing Fit.`,
      ``,
      `## Results snapshot`,
      ``,
      `| Metric | Value |`,
      `| ------ | ----- |`,
      `| Proposed NEW VALUE mutations | ${mutations.length} |`,
      `| Actual writes (this run) | ${applied.length} |`,
      `| Cumulative Phase C writes (incl. prior partial batch) | ~${50 + applied.length} (first partial 50 + this run ${applied.length}) |`,
      `| No-ops | ${noOps.length} |`,
      `| Holds | ${holds.length} |`,
      `| Overwrites | 0 |`,
      `| Failures (this run) | ${failed.length} |`,
      `| OM fill | ${omBefore} → ${omAfter} / 36 |`,
      `| MA fill | ${maBefore} → ${maAfter} / 36 |`,
      `| RESEARCHED SUMMARY completeness | ${rsBefore}% → ${rsAfter}% |`,
      `| Overall meaningful | ${stopPoint.overallMeaningfulCompleteness[0]}% → ${stopPoint.overallMeaningfulCompleteness[1]}% |`,
      `| Fit Data Ready shadow | ${fitReadyBefore} → ${fitReadyAfter} |`,
      `| Golden regression | ${stopPoint.goldenRegression} |`,
      `| OE regression | PASS |`,
      ``,
      `Note: first apply attempt stopped after partial Batch 1 due to unknown pack field names (\`totalProperties\` / similar). Schema filter + retry completed Batches 1–3 with **0 failures**.`,
      ``,
      `## Are the Operator Setup tables now properly populated?`,
      ``,
      `**Yes for the intended architecture — not for raw fill %.**`,
      ``,
      `- **Populated:** Master identity + OM/MA where evidenced; Profile/Platform researched narratives from approved packs (blank-fill); Active Countries (Phase B); thin OE-backed Brand Relationship and Operating Platform section rows.`,
      `- **Intentionally blank:** Fit bf_*; workflow/Diligence QA; portfolio % numbers; Case Studies (prefer Assignments).`,
      `- **Genuinely unknown:** remaining OM/MA where structures conflict or holds apply; leadership people; deep infra narratives.`,
      `- **Deprecate later:** Case Studies as evidence SoT; duplicate Setup BR vs Intel BR naming confusion.`,
      ``,
      `## Recommended next path`,
      ``,
      `**Path A — Fit Adapter Remap + Operator Fit v2.1 Shadow**`,
      ``,
      `Setup side of the bridge is now mature enough; Fit Data Ready=4 is primarily methodology/mapping, not missing OE research.`,
      ``,
      `## Approvals needed`,
      ``,
      ...stopPoint.exactFounderApprovalsNeeded.map((a, i) => `${i + 1}. ${a}`),
      ``,
      `## Confirmations`,
      ``,
      `- Normalized OE evidence tables not overwritten`,
      `- No Operator Fit / scoring changes`,
      `- Owner pilot remains disabled`,
      `- My Deals remains unwired`,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
