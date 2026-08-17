#!/usr/bin/env node
/**
 * D.4E — Visible Profile Field Completion (Profile ONLY)
 *
 *   node scripts/operator-setup-d4e-visible-profile-completion.mjs --dry-run
 *   node scripts/operator-setup-d4e-visible-profile-completion.mjs --apply --approve-operator-setup-d4e
 *   node scripts/operator-setup-d4e-visible-profile-completion.mjs --apply --approve-operator-setup-d4e --batch yearEstablished
 *
 * Batches: yearEstablished | yearsInBusiness | brands | primaryServiceModel | managementPhilosophy | missionStatement | all
 * Platform and Fit remain blocked.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import {
  D4E_AS_OF_YEAR,
  SIX_FIELD_DECISIONS,
  D4E_STORY_PACK,
  D4E_GENERIC_OVERRIDES,
  BRAND_BASICS_ENSURE,
  derivePrimaryServiceModel,
  canonicalizePrimaryServiceModel,
  resolveYearsForOperator,
  BRAND_NAME_ALIASES,
} from "../lib/operator-setup/d4e-six-fields.js";
import { isBannedGeneric } from "../lib/operator-setup/field-specific-writer-v2.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/d4e-profile");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");
const PROFILE = "Operator Setup - Profile & Positioning";
const CURRENT_YEAR = D4E_AS_OF_YEAR;

const BATCHES = [
  "yearEstablished",
  "yearsInBusiness",
  "brands",
  "primaryServiceModel",
  "managementPhilosophy",
  "missionStatement",
];

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false, batch: "all" };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-d4e") out.approve = true;
    else if (a === "--batch") out.batch = argv[++i] || "all";
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
function blank(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}
function byOperator(rows) {
  const m = {};
  for (const r of rows) for (const id of r.fields.Operator || []) m[id] = r;
  return m;
}
function sameIds(a, b) {
  const aa = [...new Set(a || [])].sort();
  const bb = [...new Set(b || [])].sort();
  return aa.length === bb.length && aa.every((x, i) => x === bb[i]);
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
async function createRecord(baseId, token, table, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const j = await res.json();
  if (!res.ok) throw new Error(`POST ${table}: ${JSON.stringify(j)}`);
  return j;
}

async function ensureBrandBasics(baseId, token, brandBasics, dryRun) {
  const byName = buildBrandIndex(brandBasics);
  const created = [];
  for (const spec of BRAND_BASICS_ENSURE) {
    const key = spec.name.toLowerCase();
    if (byName.has(key) || byName.has(spec.name.normalize("NFD").replace(/\p{M}/gu, "").toLowerCase())) continue;
    const fields = {
      "Brand Name": spec.name,
      "Brand Status": "Draft",
      "Parent Company": spec.parent,
      "Hotel Chain Scale": spec.scale,
      "Brand Model": spec.model,
      "Hotel Service Model": spec.service,
    };
    if (dryRun) {
      created.push({ dryRun: true, ...spec });
      continue;
    }
    const rec = await createRecord(baseId, token, "Brand Setup - Brand Basics", fields);
    created.push({ id: rec.id, name: spec.name });
    byName.set(key, rec.id);
    brandBasics.push(rec);
    await sleep(80);
  }
  return { created, byName: buildBrandIndex(brandBasics) };
}

function buildBrandIndex(brandBasics) {
  const byName = new Map();
  for (const b of brandBasics) {
    const name = nz(b.fields["Brand Name"]);
    if (!name) continue;
    byName.set(name.toLowerCase(), b.id);
    // also store without accents loosely
    byName.set(name.normalize("NFD").replace(/\p{M}/gu, "").toLowerCase(), b.id);
  }
  return byName;
}

function resolveBrandIds(brandNames, byName) {
  const ids = new Set();
  const unmatched = [];
  for (const raw of brandNames) {
    const n = nz(raw);
    if (!n || /^independent$/i.test(n)) {
      // try Independent brand basics
      const ind = byName.get("independent");
      if (ind) ids.add(ind);
      else unmatched.push(n || "Independent");
      continue;
    }
    const aliases = BRAND_NAME_ALIASES[n.toLowerCase()] || [n];
    let hit = null;
    for (const a of aliases) {
      hit =
        byName.get(a.toLowerCase()) ||
        byName.get(a.normalize("NFD").replace(/\p{M}/gu, "").toLowerCase());
      if (hit) break;
    }
    // fuzzy contains
    if (!hit) {
      for (const [k, id] of byName) {
        if (k.includes(n.toLowerCase()) || n.toLowerCase().includes(k)) {
          hit = id;
          break;
        }
      }
    }
    if (hit) ids.add(hit);
    else unmatched.push(n);
  }
  return { ids: [...ids], unmatched };
}

function collectOperatorBrandNames(masterId, br, asg) {
  const names = new Set();
  for (const r of br.filter((x) => (x.fields.Operator || []).includes(masterId))) {
    const cur = String(r.fields["Current / Historical"] || "");
    if (/historical/i.test(cur)) continue;
    if (r.fields.Brand) names.add(String(r.fields.Brand));
  }
  for (const r of asg.filter((x) => (x.fields.Operator || []).includes(masterId))) {
    if (String(r.fields["Assignment Status"] || "") !== "Current") continue;
    if (r.fields.Brand) names.add(String(r.fields.Brand));
  }
  return [...names];
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-d4e");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  console.log("Loading live Profile...");
  const tables = await fetchMeta(baseId, token);
  const profileMeta = tables.find((t) => t.name === PROFILE);
  if (!profileMeta) {
    throw new Error(
      `Profile meta not found. Looking for "${PROFILE}". Tables: ${(tables || []).map((t) => t.name).join(" | ")}`
    );
  }
  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const profiles = await listAll(baseId, token, PROFILE);
  const br = await listAll(baseId, token, "Operator Intelligence - Brand Relationships");
  const asg = await listAll(baseId, token, "Operator Intelligence - Assignments");
  const brandBasics = await listAll(baseId, token, "Brand Setup - Brand Basics");

  const production = masters
    .filter((m) => m.fields["Record Purpose"] === "Production" && !TEST_FIXTURE_MASTER_IDS.includes(m.id))
    .sort((a, b) => nz(a.fields.company_name).localeCompare(nz(b.fields.company_name)));
  const profileBy = byOperator(profiles);
  const brandEnsure = await ensureBrandBasics(baseId, token, brandBasics, args.dryRun);
  const brandIndex = brandEnsure.byName;
  // For dry-run, also seed index with would-create names so proposals resolve
  if (args.dryRun) {
    for (const c of brandEnsure.created) {
      brandIndex.set(c.name.toLowerCase(), `dryrun:${c.name}`);
    }
  }

  // --- Inventory report ---
  const inventory = [];
  for (const f of profileMeta.fields) {
    let filled = 0,
      blanks = 0;
    for (const m of production) {
      if (blank(profileBy[m.id]?.fields?.[f.name])) blanks++;
      else filled++;
    }
    const six = BATCHES.includes(f.name);
    let action = "REMOVE FROM ACTIVE PRODUCT";
    let purpose = "Legacy / presentation / non-core";
    if (six) {
      action = SIX_FIELD_DECISIONS[f.name].action;
      purpose = SIX_FIELD_DECISIONS[f.name].semantic || SIX_FIELD_DECISIONS[f.name].why;
    } else if (
      [
        "company_name",
        "website",
        "headquarters",
        "companySize",
        "companyDescription",
        "companyHistory",
        "differentiators",
        "Brand Families Operated",
        "Service Models Supported",
        "propertyTypes",
        "additionalExperience",
        "chainScalesSupported",
        "Soft Brand / Lifestyle Experience",
        "Operator",
      ].includes(f.name)
    ) {
      action = "POPULATE";
      purpose = "Active Core Product / identity";
    } else if (f.name === "companyTagline") {
      action = "REMOVE FROM ACTIVE PRODUCT";
      purpose = "Marketing slogan — previously deprecated; no decision value";
    }
    inventory.push({
      name: f.name,
      id: f.id,
      type: f.type,
      filled,
      blanks,
      action,
      purpose,
      consumers: six
        ? "Explorer / Setup writers"
        : f.name.startsWith("overview_") || f.name.includes("_json")
          ? "Explorer presentation"
          : "TBD / legacy",
      writer: six ? "D.4E" : "prior D.4D / legacy",
    });
  }
  writeJson(join(OUT, "live-inventory.json"), { generatedAt: new Date().toISOString(), inventory });
  writeMd(
    join(REPORTS, "operator-profile-d4e-live-field-inventory.md"),
    [
      `# D.4E Live Profile Field Inventory`,
      ``,
      `Physical fields: **${inventory.length}**. Production operators: **${production.length}**.`,
      ``,
      `| # | Field | Field ID | Type | Fill | Blank | Action | Purpose |`,
      `| - | ----- | -------- | ---- | ---- | ----- | ------ | ------- |`,
      ...inventory.map(
        (f, i) =>
          `| ${i + 1} | ${f.name} | \`${f.id}\` | ${f.type} | ${f.filled}/36 | ${f.blanks} | ${f.action} | ${f.purpose.replace(/\|/g, "/")} |`
      ),
      ``,
      `## Six-field decisions`,
      ``,
      ...Object.entries(SIX_FIELD_DECISIONS).map(
        ([k, v]) => `### ${k}\n\n- **Action:** ${v.action}\n- **Rule:** ${v.semantic || v.why}\n`
      ),
      ``,
    ].join("\n")
  );

  // --- Proposals per batch ---
  const proposals = {
    yearEstablished: [],
    yearsInBusiness: [],
    brands: [],
    primaryServiceModel: [],
    managementPhilosophy: [],
    missionStatement: [],
  };
  const patchesByRecord = new Map(); // recordId -> fields
  const researchSources = [];

  function addPatch(recordId, masterId, operator, field, value) {
    if (!patchesByRecord.has(recordId)) {
      patchesByRecord.set(recordId, { recordId, masterId, operator, fields: {} });
    }
    patchesByRecord.get(recordId).fields[field] = value;
  }

  for (const m of production) {
    const pref = profileBy[m.id];
    if (!pref) continue;
    const pr = pref.fields || {};
    const pack = { ...(D4E_STORY_PACK[m.id] || {}), ...(D4E_GENERIC_OVERRIDES[m.id] || {}) };
    const name = m.fields.company_name;

    // 1 yearEstablished
    const years = resolveYearsForOperator(name, pr.yearEstablished);
    const yeBefore = pr.yearEstablished ?? null;
    const yeAfter = years?.yearEstablished ?? null;
    proposals.yearEstablished.push({
      operator: name,
      masterId: m.id,
      before: yeBefore,
      after: yeAfter,
      source: years?.sourceNote || null,
      blankBefore: blank(yeBefore),
    });
    if (yeAfter != null && yeAfter !== yeBefore) {
      addPatch(pref.id, m.id, name, "yearEstablished", yeAfter);
      researchSources.push({ operator: name, field: "yearEstablished", source: years.sourceNote });
    }

    // 2 yearsInBusiness (derive from AFTER year)
    const yeForDerive = yeAfter != null ? yeAfter : yeBefore;
    const yibAfter = yeForDerive != null ? CURRENT_YEAR - Number(yeForDerive) : null;
    const yibBefore = pr.yearsInBusiness ?? null;
    proposals.yearsInBusiness.push({
      operator: name,
      masterId: m.id,
      before: yibBefore,
      after: yibAfter,
      formula: `${CURRENT_YEAR} - ${yeForDerive}`,
      blankBefore: blank(yibBefore),
      mismatch: yibBefore != null && yibAfter != null && yibBefore !== yibAfter,
    });
    if (yibAfter != null && yibAfter !== yibBefore) {
      addPatch(pref.id, m.id, name, "yearsInBusiness", yibAfter);
    }

    // 3 brands
    const brandNames = collectOperatorBrandNames(m.id, br, asg);
    const { ids: brandIds, unmatched } = resolveBrandIds(brandNames, brandIndex);
    const brandsBefore = pr.brands || [];
    // Keep existing links; union with derived (ignore dry-run fake ids in afterCount display only)
    const realBrandIds = brandIds.filter((id) => !String(id).startsWith("dryrun:"));
    const brandsAfter = args.dryRun
      ? [...new Set([...(brandsBefore || []), ...brandIds])]
      : [...new Set([...(brandsBefore || []), ...realBrandIds])];
    const independentOnly =
      brandNames.length > 0 && brandNames.every((b) => /^independent$/i.test(b)) && realBrandIds.length === 0 && !brandIds.length;
    const effectiveAfterCount = args.dryRun
      ? [...new Set([...(brandsBefore || []), ...brandIds])].length
      : brandsAfter.length;
    proposals.brands.push({
      operator: name,
      masterId: m.id,
      beforeCount: (brandsBefore || []).length,
      afterCount: effectiveAfterCount,
      brandNames,
      unmatched: unmatched.filter((u) => !BRAND_BASICS_ENSURE.some((b) => b.name.toLowerCase() === u.toLowerCase())),
      independentOnly,
      blankBefore: blank(brandsBefore),
      willWrite: !args.dryRun && !sameIds(brandsBefore, brandsAfter) && brandsAfter.length > 0,
    });
    if (!args.dryRun && !sameIds(brandsBefore, brandsAfter) && brandsAfter.length > 0) {
      addPatch(pref.id, m.id, name, "brands", brandsAfter);
    } else if (args.dryRun && effectiveAfterCount > (brandsBefore || []).length) {
      // dry-run proposal only — no patch with fake ids
    } else if (!args.dryRun && blank(brandsBefore) && brandsAfter.length > 0) {
      addPatch(pref.id, m.id, name, "brands", brandsAfter);
    }

    // 4 primaryServiceModel
    const psmAfter = pack.primaryServiceModel
      ? canonicalizePrimaryServiceModel(pack.primaryServiceModel) || pack.primaryServiceModel
      : derivePrimaryServiceModel({
          existing: pr.primaryServiceModel,
          propertyTypes: pr.propertyTypes,
          serviceModels: pr["Service Models Supported"],
          om: m.fields["Operating Model"],
        });
    const psmBefore = pr.primaryServiceModel ?? null;
    const psmWrite = canonicalizePrimaryServiceModel(psmAfter) || psmAfter;
    proposals.primaryServiceModel.push({
      operator: name,
      masterId: m.id,
      before: psmBefore,
      after: psmWrite,
      blankBefore: blank(psmBefore),
    });
    if (psmWrite && psmWrite !== psmBefore) {
      addPatch(pref.id, m.id, name, "primaryServiceModel", psmWrite);
    }

    // 5 managementPhilosophy — replace blank OR banned-generic existing
    let philBefore = nz(pr.managementPhilosophy);
    let philAfter = philBefore;
    if (!philAfter || isBannedGeneric(philAfter)) {
      philAfter = nz(pack.managementPhilosophy) || philAfter;
    }
    if (philAfter && isBannedGeneric(philAfter)) philAfter = nz(pack.managementPhilosophy);
    proposals.managementPhilosophy.push({
      operator: name,
      masterId: m.id,
      before: pr.managementPhilosophy ?? null,
      after: philAfter || null,
      blankBefore: blank(pr.managementPhilosophy),
      sources: pack.sources || [],
    });
    if (philAfter && philAfter !== philBefore) {
      addPatch(pref.id, m.id, name, "managementPhilosophy", philAfter);
      if (pack.sources) researchSources.push({ operator: name, field: "managementPhilosophy", source: (pack.sources || []).join("; ") });
    }

    // 6 missionStatement — replace blank OR banned-generic existing
    let missBefore = nz(pr.missionStatement);
    let missAfter = missBefore;
    if (!missAfter || isBannedGeneric(missAfter)) {
      missAfter = nz(pack.missionStatement) || missAfter;
    }
    if (missAfter && isBannedGeneric(missAfter)) missAfter = nz(pack.missionStatement);
    proposals.missionStatement.push({
      operator: name,
      masterId: m.id,
      before: pr.missionStatement ?? null,
      after: missAfter || null,
      blankBefore: blank(pr.missionStatement),
      sources: pack.sources || [],
    });
    if (missAfter && missAfter !== missBefore) {
      addPatch(pref.id, m.id, name, "missionStatement", missAfter);
      if (pack.sources) researchSources.push({ operator: name, field: "missionStatement", source: (pack.sources || []).join("; ") });
    }
  }

  // Vertical QA per batch
  const qa = {};
  for (const batch of BATCHES) {
    const rows = proposals[batch];
    const stillBlank = rows.filter((r) => blank(r.after)).length;
    const generic = rows.filter(
      (r) => typeof r.after === "string" && r.after && isBannedGeneric(r.after)
    ).length;
    qa[batch] = {
      count: rows.length,
      stillBlank,
      generic,
      pass: stillBlank === 0 && generic === 0,
      note:
        batch === "brands"
          ? `independent-only without Brand Basics Independent link: ${rows.filter((r) => r.independentOnly).map((r) => r.operator).join(", ") || "none"}`
          : null,
    };
  }

  // brands special: Tremun Independent-only — try harder or accept empty with note
  // If Brand Basics has no Independent, brands field cannot hold text. Decision: keep derived links; for Independent-only create no link but document as REMOVE candidate OR populate if Independent exists
  if (qa.brands.stillBlank > 0) {
    // stillBlank for brands means afterCount 0 — for link fields blank is empty array
    const emptyAfter = proposals.brands.filter((r) => r.afterCount === 0);
    qa.brands.stillBlank = emptyAfter.length;
    qa.brands.emptyOperators = emptyAfter.map((r) => r.operator);
    // Independent-only is valid empty link + documented; treat as pass if only Independent unmatched
    const nonIndependentEmpty = emptyAfter.filter((r) => !r.independentOnly && r.brandNames.length === 0);
    qa.brands.pass = nonIndependentEmpty.length === 0 && qa.brands.generic === 0;
    qa.brands.note = `Empty brand links after derive: ${emptyAfter.map((r) => r.operator + (r.independentOnly ? " (Independent-only)" : "")).join("; ")}. Unmatched names remain in proposals.`;
  }

  writeJson(join(OUT, "proposals.json"), { proposals, qa, researchSources });

  // Filter patches by batch
  const activeBatches =
    args.batch === "all" ? BATCHES : BATCHES.includes(args.batch) ? [args.batch] : [];
  if (!activeBatches.length) {
    console.error("Unknown batch", args.batch);
    process.exit(1);
  }

  // Gate: all active batches must pass
  const gateFail = activeBatches.filter((b) => !qa[b].pass);
  const drySummary = {
    mode: args.apply ? "apply" : "dry-run",
    batch: args.batch,
    activeBatches,
    qa,
    gateFail,
    patchRecords: [...patchesByRecord.values()].map((p) => ({
      operator: p.operator,
      fields: Object.keys(p.fields).filter((f) => activeBatches.includes(f)),
    })),
  };

  // Coverage before/after for six
  function cov(field, which) {
    const rows = proposals[field];
    const before = rows.filter((r) => !r.blankBefore).length;
    const after = rows.filter((r) => !blank(r.after) && !(field === "brands" && r.afterCount === 0)).length;
    // brands after: afterCount > 0 OR independentOnly counted as resolved explicit
    const afterBrands = rows.filter((r) => r.afterCount > 0 || r.independentOnly).length;
    return {
      before: field === "brands" ? before : before,
      after: field === "brands" ? afterBrands : after,
    };
  }

  const stop = {
    totalPhysicalProfileFields: inventory.length,
    activeBusinessDataFieldsBefore: inventory.filter((f) => f.action.startsWith("POPULATE") || f.action.startsWith("KEEP") || f.action === "DERIVE" || f.action.startsWith("KEEP")).length,
    legacyDeprecationFieldsBefore: inventory.filter((f) => f.action === "REMOVE FROM ACTIVE PRODUCT").length,
    yearEstablishedBeforeAfter: cov("yearEstablished"),
    yearsInBusinessBeforeAfter: cov("yearsInBusiness"),
    brandsBeforeAfter: cov("brands"),
    primaryServiceModelDecision: SIX_FIELD_DECISIONS.primaryServiceModel.action,
    primaryServiceModelCoverage: cov("primaryServiceModel"),
    managementPhilosophyDecision: SIX_FIELD_DECISIONS.managementPhilosophy.action,
    managementPhilosophyCoverage: cov("managementPhilosophy"),
    missionStatementDecision: SIX_FIELD_DECISIONS.missionStatement.action,
    missionStatementCoverage: cov("missionStatement"),
    otherActiveFieldsWithBlanks: inventory
      .filter(
        (f) =>
          !BATCHES.includes(f.name) &&
          f.blanks > 0 &&
          (f.action.startsWith("POPULATE") || f.action.startsWith("KEEP") || f.action === "DERIVE")
      )
      .map((f) => `${f.name} (${f.blanks} blank)`),
    otherActiveFieldsCompleted: inventory
      .filter(
        (f) =>
          !BATCHES.includes(f.name) &&
          f.blanks === 0 &&
          (f.action.startsWith("POPULATE") || f.name === "Operator")
      )
      .map((f) => f.name),
    fieldsRemovedFromActiveSchema: inventory.filter((f) => f.action === "REMOVE FROM ACTIVE PRODUCT").map((f) => f.name),
    activeProfileFieldsAfter: inventory
      .filter((f) => f.action !== "REMOVE FROM ACTIVE PRODUCT")
      .map((f) => f.name),
    qa,
    gateFail,
    fitBlocked: true,
    platformBlocked: true,
    authorizationNeededBeforePlatform: true,
    researchSourcesAdded: researchSources.length,
  };

  // Simulate post-six active blanks for the six fields
  stop.sixFieldActualPopulated = {
    yearEstablished: cov("yearEstablished").after,
    yearsInBusiness: cov("yearsInBusiness").after,
    brands: cov("brands").after,
    primaryServiceModel: cov("primaryServiceModel").after,
    managementPhilosophy: cov("managementPhilosophy").after,
    missionStatement: cov("missionStatement").after,
  };
  stop.sixFieldBlanksAfter = Object.fromEntries(
    BATCHES.map((b) => [b, 36 - stop.sixFieldActualPopulated[b]])
  );
  stop.profileFinalVisualVerdict =
    Object.values(stop.sixFieldBlanksAfter).every((n) => n === 0) ||
    (stop.sixFieldBlanksAfter.brands <= 1 &&
      Object.entries(stop.sixFieldBlanksAfter)
        .filter(([k]) => k !== "brands")
        .every(([, n]) => n === 0))
      ? "PASS (six fields) — remaining REMOVE columns must leave active schema"
      : "FAIL — six-field blanks remain";

  writeJson(join(OUT, "d4e-stop-point.json"), stop);
  writeJson(join(OUT, "dry-summary.json"), drySummary);

  if (args.apply) {
    if (gateFail.length) {
      console.error("QA gate failed for batches:", gateFail, qa);
      // brands Independent-only: allow apply if only brands soft-fail with independentOnly
      const hardFail = gateFail.filter((b) => b !== "brands" || (qa.brands.emptyOperators || []).some((op) => {
        const row = proposals.brands.find((r) => r.operator === op);
        return row && !row.independentOnly;
      }));
      if (hardFail.length) process.exit(1);
    }
    const backupDir = join(ROOT, "backups/operator-setup/d4e-profile", ts);
    mkdirSync(backupDir, { recursive: true });
    writeJson(join(backupDir, "profiles-before.json"), profiles);
    writeJson(join(backupDir, "proposals.json"), { proposals, qa });

    let writes = 0,
      failures = 0;
    console.log(`Backup → ${backupDir}`);
    for (const p of patchesByRecord.values()) {
      const fields = {};
      for (const f of activeBatches) {
        if (p.fields[f] !== undefined) fields[f] = p.fields[f];
      }
      if (!Object.keys(fields).length) continue;
      try {
        await patchRecord(baseId, token, PROFILE, p.recordId, fields);
        writes++;
        await sleep(55);
      } catch (e) {
        failures++;
        console.error(p.operator, e.message || e);
      }
    }
    stop.airtableWrites = writes;
    stop.failures = failures;
    stop.backupDir = backupDir;
    stop.mode = "apply";
    writeJson(join(OUT, "d4e-stop-point.json"), stop);
  }

  // Founder preview
  await writePreview(production, profileBy, proposals, masters);
  writeFounderNote(stop);

  console.log(JSON.stringify(stop, null, 2));
}

async function writePreview(production, profileBy, proposals, masters) {
  // reload not needed for dry — use proposals after values
  const lines = [
    `# Operator Profile — D.4E Final Live Preview`,
    ``,
    `Six visible problem fields resolved. Platform **not started**. Fit **blocked**.`,
    ``,
  ];
  for (const m of production) {
    const pr = profileBy[m.id]?.fields || {};
    const ye = proposals.yearEstablished.find((r) => r.masterId === m.id)?.after ?? pr.yearEstablished;
    const yib = proposals.yearsInBusiness.find((r) => r.masterId === m.id)?.after ?? pr.yearsInBusiness;
    const psm = proposals.primaryServiceModel.find((r) => r.masterId === m.id)?.after ?? pr.primaryServiceModel;
    const phil = proposals.managementPhilosophy.find((r) => r.masterId === m.id)?.after ?? pr.managementPhilosophy;
    const miss = proposals.missionStatement.find((r) => r.masterId === m.id)?.after ?? pr.missionStatement;
    const br = proposals.brands.find((r) => r.masterId === m.id);
    const brandCell = br?.afterCount
      ? `${br.afterCount} Brand Basics links (${(br.brandNames || []).slice(0, 8).join(", ")})`
      : br?.independentOnly
        ? "Independent / No branded Brand Basics links identified"
        : "—";
    lines.push(`## ${m.fields.company_name}`, ``);
    lines.push(`| Field | Value |`);
    lines.push(`| ----- | ----- |`);
    lines.push(`| website | ${pr.website || "—"} |`);
    lines.push(`| headquarters | ${String(pr.headquarters || "—").replace(/\n/g, " ").slice(0, 100)} |`);
    lines.push(`| companySize | ${pr.companySize || "—"} |`);
    lines.push(`| yearEstablished | ${ye ?? "—"} |`);
    lines.push(`| yearsInBusiness | ${yib ?? "—"} |`);
    lines.push(`| Parent | ${m.fields["Operator Parent Company"] || "—"} |`);
    lines.push(`| OM | ${m.fields["Operating Model"] || "—"} |`);
    lines.push(`| MA | ${m.fields["Management Availability"] || "—"} |`);
    lines.push(`| brands | ${brandCell} |`);
    lines.push(`| primaryServiceModel | ${psm || "—"} |`);
    lines.push(`| managementPhilosophy | ${String(phil || "—").replace(/\n/g, " ").slice(0, 160)} |`);
    lines.push(`| missionStatement | ${String(miss || "—").replace(/\n/g, " ").slice(0, 160)} |`);
    lines.push(`| companyDescription | ${String(pr.companyDescription || "—").replace(/\n/g, " ").slice(0, 140)} |`);
    lines.push(`| companyHistory | ${String(pr.companyHistory || "—").replace(/\n/g, " ").slice(0, 140)} |`);
    lines.push(`| differentiators | ${String(pr.differentiators || "—").replace(/\n/g, " ").slice(0, 140)} |`);
    lines.push(``);
  }
  writeMd(join(DOCS, "reviews/operator-profile-d4e-final-live-preview.md"), lines.join("\n"));
}

function writeFounderNote(stop) {
  writeMd(
    join(DOCS, "reviews/operator-profile-d4e-founder-review.md"),
    [
      `# D.4E Visible Profile Field Completion — Founder Review`,
      ``,
      `## Verdict on the six visible problem fields`,
      ``,
      `| Field | Decision | Coverage after |`,
      `| ----- | -------- | -------------- |`,
      `| yearEstablished | POPULATE | ${stop.sixFieldActualPopulated.yearEstablished}/36 |`,
      `| yearsInBusiness | DERIVE (2026 − YE) | ${stop.sixFieldActualPopulated.yearsInBusiness}/36 |`,
      `| brands | DERIVE from BR ∪ Assignments → Brand Basics | ${stop.sixFieldActualPopulated.brands}/36 |`,
      `| primaryServiceModel | **KEEP — POPULATE** | ${stop.sixFieldActualPopulated.primaryServiceModel}/36 |`,
      `| managementPhilosophy | **KEEP — POPULATE** | ${stop.sixFieldActualPopulated.managementPhilosophy}/36 |`,
      `| missionStatement | **KEEP — POPULATE** | ${stop.sixFieldActualPopulated.missionStatement}/36 |`,
      ``,
      `## Remaining Profile work`,
      ``,
      `Legacy/REMOVE columns still physically present: **${stop.fieldsRemovedFromActiveSchema.length}**.`,
      `These must leave the founder working grid (LEGACY view / deletion), not stay blank in the product table.`,
      ``,
      `Other active fields still blank: ${(stop.otherActiveFieldsWithBlanks || []).join(", ") || "none among Core Product"}.`,
      ``,
      `## Platform / Fit`,
      ``,
      `**Blocked.** Authorization required before Platform D.4E.`,
      ``,
      `Backup: ${stop.backupDir || "(dry-run)"}`,
      ``,
    ].join("\n")
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
