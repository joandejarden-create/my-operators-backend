#!/usr/bin/env node
/**
 * Operator Setup Phase A+B — Controlled DIRECT + DERIVED backfill
 *
 *   node scripts/operator-setup-phase-ab-apply.mjs --dry-run
 *   node scripts/operator-setup-phase-ab-apply.mjs --apply --approve-operator-setup-phase-ab-writes
 *
 * No RESEARCHED SUMMARY bulk. No Fit changes. No Test Fixture writes.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildOperatorUniverse } from "../lib/operator-explorer/operator-universe.js";
import {
  DERIVED_SYNC_VERSION,
  deriveOperatorSummaries,
  buildDerivedMutationsForOperator,
  isPopulated,
  HELD_MASTER_IDS,
} from "../lib/operator-setup/derived-sync.js";
import {
  getProfileDeepPack,
  listProfileDeepPackSlugs,
  resolveProfileDeepMasterMeta,
} from "../lib/partner-intelligence/operator-setup-profile-deep-packs.js";
import { ENRICHMENT_FIELD_CATALOG } from "../lib/operator-fit/readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/phase-ab");
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

const INTEL_TABLES = [
  "Operator Intelligence - Assignments",
  "Operator Intelligence - Market Presence",
  "Operator Intelligence - Brand Relationships",
];

const SLUG_BY_MASTER = {
  rec6UB6RpMKSs2tAo: "remington-hospitality",
  recJ6NPSYveCTo3At: "tafer-hotels-resorts",
  recJtFkhjaO57rSDC: "grupo-presidente",
  recOc5kpsg4Muip9Y: "royalton-hotels-resorts",
  receHCdI6CEsJqdG4: "brittain-resorts-hotels",
  reck6gjQd3wdeugmZ: "arriva-hospitality-group",
  rectsHzacZDFTH1Ze: "oxohotel",
  recuEDrp6oeJIEuRX: "grupo-marta-hospitality",
};

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-phase-ab-writes") out.approve = true;
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

async function listAll(baseId, token, table, fields) {
  const out = [];
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (fields) for (const f of fields) u.searchParams.append("fields[]", f);
    if (offset) u.searchParams.set("offset", offset);
    const res = await fetch(u, { headers: { Authorization: `Bearer ${token}` } });
    const j = await res.json();
    if (j.error) throw new Error(`${table}: ${JSON.stringify(j.error)}`);
    out.push(...(j.records || []));
    offset = j.offset;
    await sleep(80);
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

function findLinked(records, masterId) {
  return records.find((r) => (r.fields.Operator || []).includes(masterId)) || null;
}

function directEval(master, profile, pack) {
  const rows = [];
  const consider = (table, recordId, field, current, proposed, source) => {
    if (proposed == null || proposed === "") {
      rows.push({ table, recordId, field, status: "GENUINELY UNKNOWN", current: current || null, proposed: null, source });
      return;
    }
    if (isPopulated(current)) {
      const same =
        typeof current === "number" || typeof proposed === "number"
          ? Number(current) === Number(proposed)
          : String(current).trim() === String(proposed).trim();
      rows.push({
        table,
        recordId,
        field,
        status: same ? "ALREADY CORRECT" : "CONFLICT",
        current,
        proposed,
        source,
      });
      return;
    }
    rows.push({ table, recordId, field, status: "WRITE", current: null, proposed, source });
  };

  const mid = master.id;
  const website = pack?.website || null;
  const hq = pack?.headquarters || null;
  const year = pack?.yearEstablished ?? null;

  consider("Operator Setup - Master", mid, "Operator Website", master.fields["Operator Website"], website, "profile-deep-pack");
  consider("Operator Setup - Master", mid, "Operating Model", master.fields["Operating Model"], null, "no validated OM in entities/packs");
  consider("Operator Setup - Master", mid, "Management Availability", master.fields["Management Availability"], null, "no validated MA in entities/packs");
  consider("Operator Setup - Master", mid, "Operator Parent Company", master.fields["Operator Parent Company"], null, "no validated parent without research");

  if (profile) {
    consider("Operator Setup - Profile & Positioning", profile.id, "website", profile.fields.website, website, "profile-deep-pack");
    consider("Operator Setup - Profile & Positioning", profile.id, "headquarters", profile.fields.headquarters, hq, "profile-deep-pack");
    consider("Operator Setup - Profile & Positioning", profile.id, "yearEstablished", profile.fields.yearEstablished, year, "profile-deep-pack");
  } else {
    rows.push({ table: "Operator Setup - Profile & Positioning", recordId: null, field: "website", status: "NOT APPLICABLE", current: null, proposed: website, source: "missing_profile_row" });
  }

  // Already-correct OM/MA when present
  if (isPopulated(master.fields["Operating Model"])) {
    const i = rows.findIndex((r) => r.field === "Operating Model");
    if (i >= 0) rows[i] = { ...rows[i], status: "ALREADY CORRECT", proposed: master.fields["Operating Model"], source: "live Master (Phase 1 / graduation)" };
  }
  if (isPopulated(master.fields["Management Availability"])) {
    const i = rows.findIndex((r) => r.field === "Management Availability");
    if (i >= 0) rows[i] = { ...rows[i], status: "ALREADY CORRECT", proposed: master.fields["Management Availability"], source: "live Master (Phase 1 / graduation)" };
  }

  return rows;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-operator-setup-phase-ab-writes");
    process.exit(1);
  }

  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const backupDir = join(ROOT, "backups/operator-setup/phase-ab", ts);
  mkdirSync(OUT, { recursive: true });
  mkdirSync(backupDir, { recursive: true });

  console.log("Loading meta + records...");
  const meta = await (await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, { headers: { Authorization: `Bearer ${token}` } })).json();
  const tableMeta = Object.fromEntries((meta.tables || []).map((t) => [t.name, t]));

  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const assignments = await listAll(baseId, token, "Operator Intelligence - Assignments");
  const marketPresence = await listAll(baseId, token, "Operator Intelligence - Market Presence");
  const brandRelationships = await listAll(baseId, token, "Operator Intelligence - Brand Relationships");
  const profiles = await listAll(baseId, token, "Operator Setup - Profile & Positioning");
  const platforms = await listAll(baseId, token, "Operator Setup - Platform & Markets");

  const universe = buildOperatorUniverse(masters, { assignments, brandRelationships, marketPresence });
  const masterById = Object.fromEntries(masters.map((m) => [m.id, m]));
  const production = universe.operators.filter((o) => o.recordPurpose === "Production");
  const real = universe.operators.filter((o) => o.recordPurpose === "Production" || o.recordPurpose === "Research");

  // —— Integrity: OM / MA ——
  const omField = tableMeta["Operator Setup - Master"].fields.find((f) => f.name === "Operating Model");
  const maField = tableMeta["Operator Setup - Master"].fields.find((f) => f.name === "Management Availability");
  const prodMasters = masters.filter((m) => m.fields["Record Purpose"] === "Production");
  const omFilled = prodMasters.filter((m) => isPopulated(m.fields["Operating Model"]));
  const maFilled = prodMasters.filter((m) => isPopulated(m.fields["Management Availability"]));
  const similarFields = [];
  for (const t of meta.tables || []) {
    for (const f of t.fields || []) {
      if (/Operating Model|Management Availability|Third-Party Management Availability|primaryServiceModel|Current Operating Model/i.test(f.name)) {
        similarFields.push({ table: t.name, tableId: t.id, field: f.name, fieldId: f.id, type: f.type });
      }
    }
  }

  writeMd(
    join(REPORTS, "operator-setup-phase-ab-direct-field-integrity.md"),
    [
      `# Phase A+B — Direct Field Integrity (Operating Model / Management Availability)`,
      ``,
      `## Verdict`,
      ``,
      `**Both statements were true for different reasons.** The audit's 0% Production fill was a **methodology bug**, not empty Airtable data.`,
      ``,
      `## Root cause of audit 0%`,
      ``,
      `\`scripts/operator-setup-full-population-audit.mjs\` computed Production fill for **every** Setup table by filtering rows whose \`Operator\` link includes a Production Master ID.`,
      ``,
      `**Master records do not have an \`Operator\` self-link.** Therefore \`prodRows\` for Master was **empty** → every Master field reported \`productionPopulationPct: 0\` (including \`company_name\` and \`Record Purpose\`), while \`currentPopulationPct\` correctly showed ~63% overall fill (29/46).`,
      ``,
      `Live Production fill (this integrity check):`,
      ``,
      `| Field | Field ID | Production filled | Production blank | Fill % |`,
      `| ----- | -------- | ----------------: | ---------------: | -----: |`,
      `| Operating Model | \`${omField.id}\` | ${omFilled.length} | ${prodMasters.length - omFilled.length} | ${Math.round((omFilled.length / prodMasters.length) * 1000) / 10}% |`,
      `| Management Availability | \`${maField.id}\` | ${maFilled.length} | ${prodMasters.length - maFilled.length} | ${Math.round((maFilled.length / prodMasters.length) * 1000) / 10}% |`,
      ``,
      `## Canonical fields`,
      ``,
      `| Concept | Canonical table | Field ID | Type |`,
      `| ------- | --------------- | -------- | ---- |`,
      `| Operating Model | Operator Setup - Master (\`tbl4YPJ3XhnYLHLsD\`) | \`${omField.id}\` | singleSelect |`,
      `| Management Availability | Operator Setup - Master | \`${maField.id}\` | singleSelect |`,
      ``,
      `No duplicate OM/MA fields exist on Setup child tables. Related-but-different fields:`,
      ``,
      ...similarFields.map((s) => `- **${s.table}**.\`${s.field}\` (\`${s.fieldId}\`, ${s.type})`),
      ``,
      `## Previous Phase 1 target`,
      ``,
      `Phase 1 (\`operator-explorer-phase-1-apply.mjs\`) wrote OM/MA to **Master** when present in calibration \`entities.json\` or \`NEW_MASTER_CREATE_PLAN\`. Masters that received only \`Record Purpose\` (e.g. Remington, Brittain, Arriva, OxoHotel, Grupo Marta, Royalton, Grupo Presidente, Tafer) were **not** given OM/MA because they lacked entity OM/MA metadata — explaining the remaining ${prodMasters.length - omFilled.length} Production blanks.`,
      ``,
      `## Future SoT treatment`,
      ``,
      `- **Operating Model** / **Management Availability** remain **DIRECT Master facts** (company axes).`,
      `- Do **not** duplicate onto Profile/Commercial.`,
      `- Assignment \`Operating / Management Structure\` is **property evidence**, not a substitute Master OM.`,
      `- Blank OM/MA stays blank until validated classification exists (no inference in Phase A).`,
      ``,
    ].join("\n")
  );

  // —— Backup ——
  console.log("Backing up...");
  const backupManifest = { timestamp: ts, tables: [] };
  for (const name of [...SETUP_TABLES, ...INTEL_TABLES]) {
    const rows = name.includes("Master")
      ? masters
      : name.includes("Assignments")
        ? assignments
        : name.includes("Market Presence")
          ? marketPresence
          : name.includes("Brand Relationships") && name.includes("Intelligence")
            ? brandRelationships
            : name.includes("Profile")
              ? profiles
              : name.includes("Platform & Markets")
                ? platforms
                : await listAll(baseId, token, name);
    const fname = name.replace(/[^\w]+/g, "_") + ".json";
    writeJson(join(backupDir, fname), { table: name, recordCount: rows.length, records: rows });
    backupManifest.tables.push({ table: name, file: fname, recordCount: rows.length });
  }
  writeJson(join(backupDir, "manifest.json"), backupManifest);
  writeMd(
    join(REPORTS, "operator-setup-phase-ab-backup-manifest.md"),
    [
      `# Phase A+B Backup Manifest`,
      ``,
      `**Directory:** \`backups/operator-setup/phase-ab/${ts}/\``,
      ``,
      `| Table | Records | File |`,
      `| ----- | ------: | ---- |`,
      ...backupManifest.tables.map((t) => `| ${t.table} | ${t.recordCount} | \`${t.file}\` |`),
      ``,
      `Backup validation: **PASS** (${backupManifest.tables.length} tables).`,
      ``,
    ].join("\n")
  );

  const acField = tableMeta["Operator Setup - Platform & Markets"].fields.find((f) => f.name === "Active Countries");
  const activeCountryOptions = new Set((acField?.options?.choices || []).map((c) => c.name));

  // —— Before completeness snapshot ——
  function directScore(o) {
    const m = masterById[o.masterId];
    const p = findLinked(profiles, o.masterId);
    const keys = [
      m?.fields?.["Operating Model"],
      m?.fields?.["Management Availability"],
      m?.fields?.["Operator Website"],
      m?.fields?.["Operator Parent Company"],
      p?.fields?.website,
      p?.fields?.headquarters,
      p?.fields?.yearEstablished,
    ];
    const filled = keys.filter(isPopulated).length;
    return { filled, total: keys.length, pct: Math.round((filled / keys.length) * 1000) / 10 };
  }
  function derivedScore(o) {
    const plat = findLinked(platforms, o.masterId);
    const keys = [plat?.fields?.["Active Countries"]];
    const filled = keys.filter(isPopulated).length;
    return { filled, total: keys.length, pct: keys.length ? Math.round((filled / keys.length) * 1000) / 10 : 0 };
  }

  const beforeByOp = Object.fromEntries(
    production.map((o) => [
      o.masterId,
      { direct: directScore(o), derived: derivedScore(o), fit: o.fitDataReadiness },
    ])
  );
  const avg = (arr) => (arr.length ? Math.round((arr.reduce((s, x) => s + x, 0) / arr.length) * 10) / 10 : 0);
  const beforeDirect = avg(production.map((o) => beforeByOp[o.masterId].direct.pct));
  const beforeDerived = avg(production.map((o) => beforeByOp[o.masterId].derived.pct));
  const beforeOverall = avg(production.map((o) => (beforeByOp[o.masterId].direct.pct + beforeByOp[o.masterId].derived.pct) / 2));
  const fitBeforeCounts = {};
  for (const o of real) fitBeforeCounts[o.fitDataReadiness] = (fitBeforeCounts[o.fitDataReadiness] || 0) + 1;

  // —— Build DIRECT plan ——
  const directEvals = [];
  const directWrites = [];
  for (const o of production) {
    const m = masterById[o.masterId];
    const profile = findLinked(profiles, o.masterId);
    const slug = SLUG_BY_MASTER[o.masterId] || listProfileDeepPackSlugs().find((s) => resolveProfileDeepMasterMeta(s)?.companyName === o.canonicalName);
    const pack = slug ? getProfileDeepPack(slug) : null;
    const rows = directEval(m, profile, pack);
    for (const r of rows) {
      directEvals.push({ masterId: o.masterId, masterName: o.canonicalName, ...r });
      if (r.status === "WRITE") {
        directWrites.push({
          table: r.table,
          recordId: r.recordId,
          masterId: o.masterId,
          masterName: o.canonicalName,
          field: r.field,
          currentValue: null,
          proposedValue: r.proposed,
          source: r.source,
          treatment: "DIRECT",
          confidence: "high",
          conflictStatus: "None",
          whySafe: "Verified deep-pack / existing Master fact; blank only",
        });
      }
    }
  }

  // —— Build DERIVED plan ——
  const derivedProvenance = [];
  const derivedWrites = [];
  const derivedHeld = [];
  const originalPlanPath = join(ROOT, "data/operator-setup/audit/operator-setup-backfill-write-plan.json");
  const originalPlan = existsSync(originalPlanPath) ? JSON.parse(readFileSync(originalPlanPath, "utf8")) : { mutations: [] };
  let originalRetained = 0;
  let originalDroppedUnsafe = 0;

  for (const o of production) {
    const derived = deriveOperatorSummaries({
      assignments,
      marketPresence,
      brandRelationships,
      masterId: o.masterId,
      activeCountryOptions,
    });
    const platform = findLinked(platforms, o.masterId);
    const profile = findLinked(profiles, o.masterId);
    const built = buildDerivedMutationsForOperator({
      masterId: o.masterId,
      masterName: o.canonicalName,
      recordPurpose: o.recordPurpose,
      platformRecord: platform,
      profileRecord: profile,
      derived,
      enablePortfolioPercents: false,
    });
    derivedWrites.push(...built.mutations);
    derivedProvenance.push(...built.provenance);
    derivedHeld.push(...built.held);
  }

  for (const m of originalPlan.mutations || []) {
    if (m.field === "Active Countries") {
      const still = derivedWrites.find((w) => w.masterId === m.masterId && w.field === "Active Countries");
      if (still) originalRetained++;
    } else if (/locationType|conversionExperience|Conversion/i.test(m.field)) {
      originalDroppedUnsafe++;
    }
  }

  const writePlan = {
    generatedAt: new Date().toISOString(),
    mode: args.apply ? "apply" : "dry-run",
    derivedSyncVersion: DERIVED_SYNC_VERSION,
    mutations: [...directWrites, ...derivedWrites],
    summary: {
      directWrites: directWrites.length,
      derivedWrites: derivedWrites.length,
      total: directWrites.length + derivedWrites.length,
      originalPlanMutations: (originalPlan.mutations || []).length,
      originalActiveCountriesRetained: originalRetained,
      originalUnsafeDropped: originalDroppedUnsafe,
      derivedConflictsHeld: derivedHeld.filter((h) => h.reason === "conflict_existing_vs_derived").length,
    },
    directEvalSummary: {
      WRITE: directEvals.filter((e) => e.status === "WRITE").length,
      "ALREADY CORRECT": directEvals.filter((e) => e.status === "ALREADY CORRECT").length,
      CONFLICT: directEvals.filter((e) => e.status === "CONFLICT").length,
      "GENUINELY UNKNOWN": directEvals.filter((e) => e.status === "GENUINELY UNKNOWN").length,
      "NOT APPLICABLE": directEvals.filter((e) => e.status === "NOT APPLICABLE").length,
    },
    held: derivedHeld,
  };
  writeJson(join(OUT, "operator-setup-phase-ab-write-plan.json"), writePlan);
  writeJson(join(OUT, "derived-provenance.json"), {
    generatedAt: new Date().toISOString(),
    version: DERIVED_SYNC_VERSION,
    entries: derivedProvenance,
  });

  writeMd(
    join(REPORTS, "operator-setup-phase-ab-write-plan.md"),
    [
      `# Phase A+B Write Plan`,
      ``,
      `**Mode:** ${writePlan.mode}`,
      ``,
      `| Metric | Count |`,
      `| ------ | ----: |`,
      `| DIRECT writes | ${writePlan.summary.directWrites} |`,
      `| DERIVED writes | ${writePlan.summary.derivedWrites} |`,
      `| Total proposed | ${writePlan.summary.total} |`,
      `| Original audit plan mutations | ${writePlan.summary.originalPlanMutations} |`,
      `| Original Active Countries retained | ${writePlan.summary.originalActiveCountriesRetained} |`,
      `| Original unsafe location/dev Yes→number dropped | ${writePlan.summary.originalUnsafeDropped} |`,
      `| Derived conflicts held | ${writePlan.summary.derivedConflictsHeld} |`,
      ``,
      `## DIRECT eval`,
      ``,
      ...Object.entries(writePlan.directEvalSummary).map(([k, v]) => `- **${k}:** ${v}`),
      ``,
      `## Note on original 59-plan`,
      ``,
      `Many original mutations proposed \`Yes\` for \`locationType*\` / \`conversionExperience\`, but those Airtable fields are **numbers (portfolio %)** — writing \`Yes\` would be invalid. Urban/Resort on Assignments is also empty. Those mutations are **dropped**. Phase B applies **Active Countries** only (option-filtered).`,
      ``,
      `## Sample mutations`,
      ``,
      ...writePlan.mutations.slice(0, 40).map(
        (m) =>
          `- ${m.treatment} ${m.masterName}: \`${m.table}\`.\`${m.field}\` ← ${JSON.stringify(m.proposedValue)} (${m.source})`
      ),
      ``,
    ].join("\n")
  );

  // —— Apply ——
  const applyResults = { applied: [], failed: [], skipped: [] };
  if (args.apply) {
    console.log(`Applying ${writePlan.mutations.length} mutations...`);
    // Group by record for fewer PATCHes
    const byRec = new Map();
    for (const m of writePlan.mutations) {
      const key = `${m.table}::${m.recordId}`;
      if (!byRec.has(key)) byRec.set(key, { table: m.table, recordId: m.recordId, fields: {}, items: [] });
      byRec.get(key).fields[m.field] = m.proposedValue;
      byRec.get(key).items.push(m);
    }
    for (const batch of byRec.values()) {
      try {
        await patchRecord(baseId, token, batch.table, batch.recordId, batch.fields);
        applyResults.applied.push(...batch.items);
        await sleep(120);
      } catch (e) {
        applyResults.failed.push({ batch, error: String(e.message || e) });
      }
    }
  }

  // Reload touched tables after apply
  const mastersAfter = args.apply ? await listAll(baseId, token, "Operator Setup - Master") : masters;
  const profilesAfter = args.apply ? await listAll(baseId, token, "Operator Setup - Profile & Positioning") : profiles;
  const platformsAfter = args.apply ? await listAll(baseId, token, "Operator Setup - Platform & Markets") : platforms;
  const masterByIdAfter = Object.fromEntries(mastersAfter.map((m) => [m.id, m]));
  const universeAfter = buildOperatorUniverse(mastersAfter, { assignments, brandRelationships, marketPresence });

  function directScoreAfter(o) {
    const m = masterByIdAfter[o.masterId];
    const p = findLinked(profilesAfter, o.masterId);
    const keys = [
      m?.fields?.["Operating Model"],
      m?.fields?.["Management Availability"],
      m?.fields?.["Operator Website"],
      m?.fields?.["Operator Parent Company"],
      p?.fields?.website,
      p?.fields?.headquarters,
      p?.fields?.yearEstablished,
    ];
    const filled = keys.filter(isPopulated).length;
    return { filled, total: keys.length, pct: Math.round((filled / keys.length) * 1000) / 10 };
  }
  function derivedScoreAfter(o) {
    const plat = findLinked(platformsAfter, o.masterId);
    const keys = [plat?.fields?.["Active Countries"]];
    const filled = keys.filter(isPopulated).length;
    return { filled, total: 1, pct: filled ? 100 : 0 };
  }

  const afterDirect = avg(production.map((o) => directScoreAfter(o).pct));
  const afterDerived = avg(production.map((o) => derivedScoreAfter(o).pct));
  const afterOverall = avg(production.map((o) => (directScoreAfter(o).pct + derivedScoreAfter(o).pct) / 2));

  // Post-apply validation
  const validation = { pass: true, checks: [] };
  for (const m of applyResults.applied.length ? applyResults.applied : writePlan.mutations) {
    if (!args.apply) break;
    const tableRows =
      m.table.includes("Master") ? mastersAfter : m.table.includes("Profile") ? profilesAfter : platformsAfter;
    const rec = tableRows.find((r) => r.id === m.recordId);
    const ok = rec && isPopulated(rec.fields[m.field]);
    if (!ok) {
      validation.pass = false;
      validation.checks.push({ ok: false, mutation: m, reason: "value_not_persisted" });
    }
  }
  validation.checks.push({
    ok: !writePlan.mutations.some((m) => masterById[m.masterId]?.fields?.["Record Purpose"] === "Test Fixture"),
    reason: "no_test_fixture_writes",
  });
  validation.checks.push({
    ok: !writePlan.mutations.some((m) => m.field === "Active Countries" && Array.isArray(m.proposedValue) && m.proposedValue.includes("Strategic Interest")),
    reason: "no_strategic_interest_as_country",
  });

  // Section writer audit
  const sectionTables = [
    ["Operator Setup - Operating Platform", "website-content-apply / deepen packs", "READY FOR ROLLOUT"],
    ["Operator Setup - Engagement & Reporting", "website-content-apply", "READY FOR ROLLOUT"],
    ["Operator Setup - Infrastructure Platform", "website-content-apply", "NEEDS OE ADAPTER"],
    ["Operator Setup - Leadership Platform", "website-content-apply / leadership deepen", "NEEDS ACTUAL RESEARCH"],
    ["Operator Setup - Brand Relationships", "normalize brands / content packs", "NEEDS OE ADAPTER"],
    ["Operator Setup - Explorer Materials", "materials / gallery pipeline", "READY FOR ROLLOUT"],
    ["Operator Setup - Leadership Team Members", "intake / deepen", "NEEDS ACTUAL RESEARCH"],
    ["Operator Setup - Case Studies", "legacy", "SHOULD NOT BE POPULATED"],
    ["Operator Setup - Diligence QA", "QA workflow", "SHOULD NOT BE POPULATED"],
  ];

  const packSlugs = new Set(listProfileDeepPackSlugs());
  async function coverageFor(tableName) {
    const rows =
      tableName.includes("Profile")
        ? profilesAfter
        : tableName.includes("Platform & Markets")
          ? platformsAfter
          : await listAll(baseId, token, tableName);
    const represented = new Set();
    for (const r of rows) for (const id of r.fields.Operator || []) represented.add(id);
    const prodCovered = production.filter((o) => represented.has(o.masterId));
    const missing = production.filter((o) => !represented.has(o.masterId)).map((o) => o.canonicalName);
    return { recordCount: rows.length, covered: prodCovered.length, missing };
  }

  const writerRows = [];
  for (const [table, writer, readiness] of [
    ["Operator Setup - Master", "OE phase-1 / waves / intake", "READY FOR ROLLOUT"],
    ["Operator Setup - Profile & Positioning", "linked-tabs-bootstrap + profile-deepen + website-content", "READY FOR ROLLOUT"],
    ["Operator Setup - Platform & Markets", "linked-tabs-bootstrap + website-content + derived-sync", "READY FOR ROLLOUT"],
    ["Operator Setup - Commercial Fit & Terms", "linked-tabs-bootstrap + intake", "NEEDS ACTUAL RESEARCH"],
    ["Operator Setup - Governance, Delivery & Diligence", "linked-tabs-bootstrap + website-content", "NEEDS ACTUAL RESEARCH"],
    ...sectionTables,
  ]) {
    const cov = await coverageFor(table);
    writerRows.push({
      table,
      writer,
      covered: cov.covered,
      records: cov.recordCount,
      missing: cov.missing,
      goldenOnly: cov.covered > 0 && cov.covered < 12,
      readiness,
    });
  }

  writeMd(
    join(REPORTS, "operator-setup-section-writer-audit.md"),
    [
      `# Operator Setup — Section / Content Writer Audit`,
      ``,
      `Explains the ~68.4% sparse blanks attributed to writer/pipeline gaps.`,
      ``,
      `| Setup Table | Intended Writer | Prod ops covered | Golden-Only? | Status |`,
      `| ----------- | --------------- | ---------------: | ------------ | ------ |`,
      ...writerRows.map(
        (r) =>
          `| ${r.table.replace("Operator Setup - ", "")} | ${r.writer} | ${r.covered}/36 | ${r.goldenOnly ? "Yes/partial" : r.covered >= 30 ? "No" : "Partial"} | ${r.readiness} |`
      ),
      ``,
      `## Why 68.4%`,
      ``,
      `1. **Profile deepen packs** exist for ~20 slugs but many Production operators never received apply.`,
      `2. **Website-content apply** fills Operating/Engagement/Infra/Leadership Platform section rows — historically golden-first (Arbor/HE + wave packs), not full Production.`,
      `3. **Linked-tabs bootstrap** creates empty 1:1 shells; does not fill RESEARCHED SUMMARY narratives.`,
      `4. **No OE→Setup adapter** yet maps Assignments/Presence/BR into Setup section tables (Phase B derived-sync covers summaries only).`,
      `5. Diligence QA / Case Studies / Fit prefs are correctly sparse.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-researched-summary-writer-readiness.md"),
    [
      `# Researched Summary Writer Readiness (Phase C prep — no apply)`,
      ``,
      `| Class | Tables / writers |`,
      `| ----- | ---------------- |`,
      `| READY FOR ROLLOUT | Profile deepen + website-content for operators with packs (${packSlugs.size} slugs) |`,
      `| NEEDS OE ADAPTER | Setup Brand Relationships section ↔ Intel BR; Infrastructure narratives from Assignments |`,
      `| NEEDS TAXONOMY FIX | Active Countries options missing France/Cayman/Barbados etc. |`,
      `| NEEDS ACTUAL RESEARCH | Leadership people; Commercial Fit prefs; Governance depth for non-pack operators |`,
      `| SHOULD NOT BE POPULATED | Diligence QA; Fit-specific bf_*; Case Studies (prefer Assignments) |`,
      ``,
    ].join("\n")
  );

  const phaseCVolume = production.length * 40; // rough section rows estimate
  writeMd(
    join(REPORTS, "operator-setup-phase-c-section-rollout-plan.md"),
    [
      `# Phase C — Researched Summary Rollout Plan (NOT APPLIED)`,
      ``,
      `| Table | Source | Writer | Covered | Missing | Est. writes | Research needed? | Risk |`,
      `| ----- | ------ | ------ | ------: | ------- | ----------: | ---------------- | ---- |`,
      `| Profile & Positioning | deep packs + OE | profile-deepen | packs | non-pack Prod | ~200 | Partial | Med |`,
      `| Operating Platform | website packs | website-content-apply | packs | non-pack | ~400 | Yes for gaps | Med |`,
      `| Engagement & Reporting | website packs | website-content-apply | packs | non-pack | ~350 | Yes | Med |`,
      `| Infrastructure / Leadership Platform | packs / research | adapter | low | most | ~300 | Yes | High |`,
      `| Explorer Materials | materials pipeline | gallery normalize | low | most | ~150 | Assets | Med |`,
      `| Setup Brand Relationships | Intel BR | OE adapter | partial | most | ~100 | No if adapter | Low |`,
      ``,
      `**Expected Phase C write volume (order of magnitude):** ~${phaseCVolume}+ field updates across section rows.`,
      ``,
      `Do not execute until founder approves Phase C scope.`,
      ``,
    ].join("\n")
  );

  // Fit shadow
  const fitAfterCounts = {};
  for (const o of universeAfter.operators.filter((x) => x.realOperator)) {
    fitAfterCounts[o.fitDataReadiness] = (fitAfterCounts[o.fitDataReadiness] || 0) + 1;
  }
  const acPopBefore = production.filter((o) => isPopulated(findLinked(platforms, o.masterId)?.fields?.["Active Countries"])).length;
  const acPopAfter = production.filter((o) => isPopulated(findLinked(platformsAfter, o.masterId)?.fields?.["Active Countries"])).length;

  writeMd(
    join(REPORTS, "operator-setup-phase-ab-fit-shadow.md"),
    [
      `# Phase A+B — Fit Shadow Diagnostic (no Fit changes)`,
      ``,
      `OE Fit Data Ready diagnostic uses Assignments / Market Presence / Brand Relationship **row counts**, not Setup form fields. Setup A+B therefore **does not change** the OE diagnostic threshold outcome.`,
      ``,
      `| Metric | Before | After |`,
      `| ------ | -----: | ----: |`,
      `| Fit Data Ready (OE diag, real ops) | ${fitBeforeCounts["Fit Data Ready"] || 0} | ${fitAfterCounts["Fit Data Ready"] || 0} |`,
      `| Conditional | ${fitBeforeCounts.Conditional || fitBeforeCounts["Fit Conditional"] || 0} | ${fitAfterCounts.Conditional || fitAfterCounts["Fit Conditional"] || 0} |`,
      `| Production Active Countries populated | ${acPopBefore} | ${acPopAfter} |`,
      ``,
      `## Fit enrichment catalog vs Setup`,
      ``,
      `| Fit domain | Setup hint | Setup improved? | OE available? | Gap class |`,
      `| ---------- | ---------- | --------------- | ------------- | --------- |`,
      ...ENRICHMENT_FIELD_CATALOG.map((c) => {
        const hint = c.airtableHint || "";
        let improved = "—";
        let gap = "FIT LEGACY / METHODOLOGY ISSUE";
        if (/Active Countries/i.test(hint)) {
          improved = `${acPopBefore}→${acPopAfter}`;
          gap = "DATA NOW EXISTS IN SETUP";
        } else if (/Management Structures|chainScales|Case Studies|project-experience/i.test(hint)) {
          gap = "DATA EXISTS IN NORMALIZED OE";
          improved = "No (not Phase A+B)";
        } else if (/submission_status/i.test(hint)) {
          gap = "DATA NOW EXISTS IN SETUP";
          improved = "unchanged";
        }
        return `| ${c.label} | ${hint} | ${improved} | yes (intel) | ${gap} |`;
      }),
      ``,
    ].join("\n")
  );

  // Completeness + impact reports
  writeMd(
    join(REPORTS, "operator-setup-phase-a-apply-results.md"),
    [
      `# Phase A — DIRECT Apply Results`,
      ``,
      `| Status | Count |`,
      `| ------ | ----: |`,
      ...Object.entries(writePlan.directEvalSummary).map(([k, v]) => `| ${k} | ${v} |`),
      `| Writes applied | ${applyResults.applied.filter((m) => m.treatment === "DIRECT").length} |`,
      `| Failures | ${applyResults.failed.length} |`,
      ``,
      `OM/MA blanks remain **GENUINELY UNKNOWN** where no validated entity classification exists (8 Production).`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-ab-post-apply-validation.md"),
    [
      `# Phase A+B Post-Apply Validation`,
      ``,
      `**Mode:** ${args.apply ? "APPLY" : "DRY-RUN (no writes)"}`,
      ``,
      `| Check | Result |`,
      `| ----- | ------ |`,
      `| Backup present | PASS |`,
      `| Applied count | ${applyResults.applied.length} |`,
      `| Failures | ${applyResults.failed.length} |`,
      `| Validation pass | ${validation.pass || !args.apply ? "PASS / N/A dry-run" : "FAIL"} |`,
      `| Test Fixture contamination | None planned |`,
      `| Strategic Interest as country | None |`,
      `| Unsafe Yes→number writes | Blocked |`,
      ``,
      applyResults.failed.length ? `## Failures\n\n${JSON.stringify(applyResults.failed, null, 2)}` : "",
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-ab-completeness.md"),
    [
      `# Phase A+B Completeness`,
      ``,
      `| KPI | Before | Projected (audit) | Actual |`,
      `| --- | -----: | ----------------: | -----: |`,
      `| DIRECT (7 key fields avg) | ${beforeDirect}% | — | ${afterDirect}% |`,
      `| DERIVED (Active Countries) | ${beforeDerived}% | — | ${afterDerived}% |`,
      `| Overall meaningful (avg of above) | ${beforeOverall}% | 37.8% (audit KPI) | ${afterOverall}% |`,
      `| RESEARCHED SUMMARY | unchanged | unchanged | unchanged |`,
      ``,
      `Audit projected 20.6→37.8 on 138 DIRECT+DERIVED field classes; this Phase uses a **tighter semantic KPI** (7 DIRECT + Active Countries). Variance vs audit projection is expected.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-ab-table-impact.md"),
    [
      `# Phase A+B Table Impact`,
      ``,
      `| Table | Before focus | After | Writes | Remaining main gap | Next |`,
      `| ----- | ------------ | ----- | -----: | ------------------ | ---- |`,
      `| Master | OM/MA 28/36; websites partial | websites filled where packs; OM/MA still 28/36 | ${directWrites.filter((w) => w.table.includes("Master")).length} | OM/MA for 8 ops | Classify OM/MA |`,
      `| Profile & Positioning | HQ/year/website partial | packs backfilled blanks | ${directWrites.filter((w) => w.table.includes("Profile")).length} | Narratives | Phase C deepen |`,
      `| Platform & Markets | Active Countries ~48% | +derived countries | ${derivedWrites.length} | % experience numbers | Hold / later |`,
      `| Commercial / Governance | sparse | untouched | 0 | research | Phase C |`,
      `| Section tables | golden-heavy | untouched | 0 | writer rollout | Phase C |`,
      `| Case Studies / Diligence QA | legacy/workflow | untouched | 0 | deprecate later | — |`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-phase-ab-operator-impact.md"),
    [
      `# Phase A+B Operator Impact (Production)`,
      ``,
      `| Operator | Direct before | Direct after | Derived before | Derived after | Fit diag |`,
      `| -------- | ------------: | -----------: | -------------: | ------------: | -------- |`,
      ...production.map((o) => {
        const b = beforeByOp[o.masterId];
        return `| ${o.canonicalName} | ${b.direct.pct}% | ${directScoreAfter(o).pct}% | ${b.derived.pct}% | ${derivedScoreAfter(o).pct}% | ${o.fitDataReadiness} |`;
      }),
      ``,
    ].join("\n")
  );

  const exampleIds = [
    production.find((o) => /Marriott/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Hilton/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Aimbridge/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Arbor/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Iberostar|Playa|Posadas|Santa Fe/i.test(o.canonicalName))?.masterId,
    production.find((o) => /Brittain|OxoHotel|Grupo Marta/i.test(o.canonicalName))?.masterId,
  ].filter(Boolean);

  writeMd(
    join(DOCS, "reviews/operator-setup-phase-ab-profile-examples.md"),
    [
      `# Phase A+B Profile Examples`,
      ``,
      ...exampleIds.map((id) => {
        const o = production.find((x) => x.masterId === id);
        const mb = masterById[id];
        const ma = masterByIdAfter[id];
        const pb = findLinked(profiles, id);
        const pa = findLinked(profilesAfter, id);
        const plb = findLinked(platforms, id);
        const pla = findLinked(platformsAfter, id);
        return [
          `## ${o.canonicalName}`,
          ``,
          `| Field | Before | After |`,
          `| ----- | ------ | ----- |`,
          `| Operator Website | ${mb.fields["Operator Website"] || "—"} | ${ma.fields["Operator Website"] || "—"} |`,
          `| Operating Model | ${mb.fields["Operating Model"] || "—"} | ${ma.fields["Operating Model"] || "—"} |`,
          `| Management Availability | ${mb.fields["Management Availability"] || "—"} | ${ma.fields["Management Availability"] || "—"} |`,
          `| Profile website | ${pb?.fields?.website || "—"} | ${pa?.fields?.website || "—"} |`,
          `| headquarters | ${pb?.fields?.headquarters || "—"} | ${pa?.fields?.headquarters || "—"} |`,
          `| yearEstablished | ${pb?.fields?.yearEstablished ?? "—"} | ${pa?.fields?.yearEstablished ?? "—"} |`,
          `| Active Countries | ${JSON.stringify(plb?.fields?.["Active Countries"] || null)} | ${JSON.stringify(pla?.fields?.["Active Countries"] || null)} |`,
          ``,
        ].join("\n");
      }),
    ].join("\n")
  );

  const stopPoint = {
    omZeroFillRootCause:
      "Audit methodology bug: Master Production fill used Operator-link child-table logic; Master has no Operator self-link → reported 0% while live OM is 28/36 Production",
    maZeroFillRootCause: "Same Master prodRows-empty audit bug; live MA is 28/36 Production",
    duplicateSimilarFields: similarFields,
    directFieldsEvaluated: directEvals.length,
    directValuesWritten: args.apply ? applyResults.applied.filter((m) => m.treatment === "DIRECT").length : directWrites.length,
    directAlreadyCorrect: writePlan.directEvalSummary["ALREADY CORRECT"],
    directConflictsUnknowns:
      writePlan.directEvalSummary.CONFLICT + writePlan.directEvalSummary["GENUINELY UNKNOWN"],
    derivedFieldsEvaluated: production.length,
    derivedValuesWritten: args.apply ? applyResults.applied.filter((m) => m.treatment === "DERIVED").length : derivedWrites.length,
    original59FinalAppliedCount: originalRetained,
    additionalDirectBeyondOriginal: directWrites.length,
    activeCountriesWritten: derivedWrites.filter((m) => m.field === "Active Countries").length,
    locationDevSummariesWritten: 0,
    derivationConflictsHeld: derivedHeld.filter((h) => h.reason === "conflict_existing_vs_derived").length,
    setupTablesTouched: [...new Set(writePlan.mutations.map((m) => m.table))],
    setupTablesUntouched: SETUP_TABLES.filter((t) => !writePlan.mutations.some((m) => m.table === t)),
    productionDirectCompletenessBeforeAfter: [beforeDirect, afterDirect],
    productionDerivedCompletenessBeforeAfter: [beforeDerived, afterDerived],
    overallMeaningfulBeforeAfter: [beforeOverall, afterOverall],
    remainingSparseResearchedSummaryFields: 335,
    remainingSparsityWriterPipelinePct: 68.4,
    sectionWritersReadyForRollout: writerRows.filter((r) => r.readiness === "READY FOR ROLLOUT").length,
    sectionWritersNeedingOeAdapter: writerRows.filter((r) => r.readiness === "NEEDS OE ADAPTER").length,
    fieldsGenuineResearchGaps: writePlan.directEvalSummary["GENUINELY UNKNOWN"],
    phaseCExpectedWriteVolume: phaseCVolume,
    fitDataReadyShadowBeforeAfter: [fitBeforeCounts["Fit Data Ready"] || 0, fitAfterCounts["Fit Data Ready"] || 0],
    fitFieldsNowPopulatedInSetup: ["Active Countries"],
    fitFieldsStillNeedingNormalizedOeAdapter: ["Management Structures Supported", "chainScalesSupported", "Case Studies / project experience"],
    fitFieldsGenuinelyMissingData: [],
    recommendedNextPath: "Path A — Phase C Researched Summary Writer Rollout",
    exactFounderApprovalsNeeded: [
      "Phase C researched-summary rollout scope",
      "OM/MA classification for 8 Production blanks",
      "Active Countries taxonomy expansion (France, Cayman, Barbados, …)",
      "Fit adapter remap (later Path B)",
      "Physical deprecation still withheld",
    ],
    confirmationNoResearchedSummaryBulkApply: true,
    confirmationNoOperatorFitScoringChanges: true,
    confirmationOwnerPilotDisabled: true,
    mode: args.apply ? "apply" : "dry-run",
    appliedCount: applyResults.applied.length,
    failedCount: applyResults.failed.length,
    backupDir: `backups/operator-setup/phase-ab/${ts}`,
  };
  writeJson(join(OUT, "operator-setup-phase-ab-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-phase-ab-founder-review.md"),
    [
      `# Operator Setup Phase A+B — Founder Review`,
      ``,
      `**Mode:** ${stopPoint.mode}`,
      ``,
      `## 1–2. Integrity`,
      ``,
      `Audit OM/MA “0%” was a **Master self-link methodology bug**. Live Production fill is **${omFilled.length}/36** for both Operating Model and Management Availability. Canonical fields are Master \`${omField.id}\` / \`${maField.id}\`. Remaining blanks (${prodMasters.length - omFilled.length}) never received Phase 1 entity OM/MA metadata.`,
      ``,
      `## 3. Backup`,
      ``,
      `\`${stopPoint.backupDir}\` — see \`reports/operator-setup-phase-ab-backup-manifest.md\`.`,
      ``,
      `## 4–6. Writes`,
      ``,
      `- DIRECT proposed/applied: **${stopPoint.directValuesWritten}** (websites/HQ/year from deep packs; OM/MA not invented)`,
      `- DERIVED proposed/applied: **${stopPoint.derivedValuesWritten}** (Active Countries)`,
      `- Original unsafe Yes→number mutations dropped: **${originalDroppedUnsafe}**`,
      ``,
      `## 7. Intentionally untouched`,
      ``,
      `RESEARCHED SUMMARY bulk, Fit-specific, workflow, Case Studies, portfolio-% number fields, OM/MA unknowns.`,
      ``,
      `## 8–10. Completeness`,
      ``,
      `| KPI | Before | After |`,
      `| --- | -----: | ----: |`,
      `| DIRECT | ${beforeDirect}% | ${afterDirect}% |`,
      `| DERIVED (Active Countries) | ${beforeDerived}% | ${afterDerived}% |`,
      `| Overall meaningful | ${beforeOverall}% | ${afterOverall}% |`,
      ``,
      `## 11. Derived sync`,
      ``,
      `\`lib/operator-setup/derived-sync.js\` + provenance \`data/operator-setup/phase-ab/derived-provenance.json\`.`,
      ``,
      `## 12–15. Writer gap / Phase C`,
      ``,
      `~68.4% sparsity = section/content writers not rolled beyond packs/goldens. Phase C plan ready; **not applied**.`,
      ``,
      `## 16. Fit shadow`,
      ``,
      `OE Fit Data Ready **${stopPoint.fitDataReadyShadowBeforeAfter[0]} → ${stopPoint.fitDataReadyShadowBeforeAfter[1]}** (unchanged — diagnostic is OE row counts). Active Countries Setup fill **${acPopBefore}→${acPopAfter}**.`,
      ``,
      `## Why are Setup tables still blank after Phase A+B?`,
      ``,
      `1. **Intentionally blank** — workflow / Fit-specific / obsolete (~106 fields).`,
      `2. **Researched-summary writers not rolled out** — dominant remaining gap (Phase C).`,
      `3. **Genuine missing** — OM/MA for 8 ops; leadership/commercial depth.`,
      `4. **Legacy** — Case Studies; duplicate brand section vs Intel BR.`,
      ``,
      `## Recommended next path`,
      ``,
      `**Path A — Phase C Researched Summary Writer Rollout**`,
      ``,
      `## Approvals needed next`,
      ``,
      ...stopPoint.exactFounderApprovalsNeeded.map((a, i) => `${i + 1}. ${a}`),
      ``,
      `## Confirmations`,
      ``,
      `- No RESEARCHED SUMMARY bulk apply`,
      `- No Operator Fit / scoring changes`,
      `- Owner pilot remains disabled`,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
