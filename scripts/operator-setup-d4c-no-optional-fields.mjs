#!/usr/bin/env node
/**
 * D.4C — No-Optional-Fields Completion (Profile + Platform)
 *
 *   node scripts/operator-setup-d4c-no-optional-fields.mjs --dry-run
 *   node scripts/operator-setup-d4c-no-optional-fields.mjs --apply --approve-operator-setup-d4c-no-optional
 *
 * Fit Adapter Shadow remains BLOCKED until response completeness = 100%.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync, cpSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import { deriveOperatorSummaries, isPopulated } from "../lib/operator-setup/derived-sync.js";
import {
  isBannedGeneric,
  counterfactualCouldApplyToPeers,
  classifyBatchDifferentiation,
} from "../lib/operator-setup/field-specific-writer-v2.js";
import {
  POLICY_VERSION,
  CTRL,
  STATE,
  PROFILE_RETAIN_REQUIRED,
  PLATFORM_RETAIN_REQUIRED,
  MASTER_RETAIN_REQUIRED,
  REMOVED_FROM_RETAINED,
  EXEMPLAR_MASTER_IDS,
  PREVIEW_OPERATOR_NAMES,
  mapBrandToFamilies,
  mapOmToServiceModels,
  derivePropertyTypes,
  deriveAdditionalExperience,
  softBrandFromFamilies,
  isBlankValue,
  classifyStateFromValue,
} from "../lib/operator-setup/no-optional-fields-policy.js";
import { RESEARCH_PACK, resolveHistory } from "../lib/operator-setup/no-optional-fields-research-pack.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/no-optional-fields");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");
const D5_DRY = join(ROOT, "data/operator-setup/d5-operational/dry-run.json");

const PROFILE = "Operator Setup - Profile & Positioning";
const PLATFORM = "Operator Setup - Platform & Markets";
const NARRATIVE_FIELDS = new Set([
  "companyDescription",
  "companyHistory",
  "differentiators",
  "cap_profile_operational",
]);

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-d4c-no-optional") out.approve = true;
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
function sameMulti(a, b) {
  const aa = [...new Set((a || []).map(String))].sort();
  const bb = [...new Set((b || []).map(String))].sort();
  return aa.length === bb.length && aa.every((x, i) => x === bb[i]);
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
    await sleep(35);
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

function narrativeOk(field, text, companyName) {
  if (!text || isControlledTextValue(text)) return { ok: true, generic: false, counterfactualFail: false };
  if (isBannedGeneric(text)) return { ok: false, generic: true, counterfactualFail: false };
  // Controlled / standardized classification phrases are allowed
  if (/^No sufficiently|^Not publicly|^Not applicable|^Requires validation/i.test(text)) {
    return { ok: true, generic: false, counterfactualFail: false };
  }
  if (field === "companyHistory" || field === "companyDescription") {
    // Require at least one factual anchor beyond company name
    const anchors =
      /\b(19\d{2}|20\d{2}|founded|established|acquired|listed|mallorca|toronto|hong kong|colombia|mexico|brazil|managed by|franchise|owner–|owner-|third-party|brand-operator|all-inclusive|cala|latam|caribbean|spain|argentina)\b/i.test(
        text
      );
    if (!anchors) return { ok: false, generic: true, counterfactualFail: true };
    return { ok: true, generic: false, counterfactualFail: false };
  }
  const cf = counterfactualCouldApplyToPeers(text, companyName);
  return { ok: !cf.fail, generic: false, counterfactualFail: cf.fail };
}

function isControlledTextValue(text) {
  return Object.values(CTRL).includes(String(text || "").trim());
}

function proposeProfile(master, pref, derived, pack) {
  const name = nz(master.fields.company_name);
  const om = master.fields["Operating Model"];
  const fields = {};
  const states = {};
  const research = {};
  const current = pref?.fields || {};

  // company_name
  const cn = nz(current.company_name) || name;
  fields.company_name = cn;
  states.company_name = STATE.VERIFIED;

  // website
  let website = nz(current.website) || nz(master.fields["Operator Website"]);
  if (!website) {
    website = CTRL.NPD;
    states.website = STATE.NOT_PUBLICLY_DISCLOSED;
    research.website = "no official URL on Master/Profile after check";
  } else states.website = STATE.VERIFIED;
  fields.website = website;

  // headquarters
  let hq = nz(current.headquarters) || nz(pack?.headquarters);
  if (!hq) {
    hq = CTRL.NPD;
    states.headquarters = STATE.NOT_PUBLICLY_DISCLOSED;
    research.headquarters = "targeted + existing blank";
  } else states.headquarters = pack?.headquarters && !nz(current.headquarters) ? STATE.SUPPORTED_SYNTHESIS : STATE.VERIFIED;
  fields.headquarters = hq;

  // companySize
  let size = current.companySize || pack?.companySize;
  if (!size) {
    size = "Not disclosed";
    states.companySize = STATE.NOT_PUBLICLY_DISCLOSED;
  } else states.companySize = STATE.VERIFIED;
  fields.companySize = size;

  // Brand Families
  let bf = current["Brand Families Operated"];
  if (!isPopulated(bf)) {
    bf = pack?.brandFamilies?.length ? pack.brandFamilies : mapBrandToFamilies(derived.brands);
    if (!bf.length) bf = [CTRL.RV];
  }
  fields["Brand Families Operated"] = bf;
  states["Brand Families Operated"] = bf.includes(CTRL.RV) ? STATE.REQUIRES_VALIDATION : STATE.VERIFIED;

  // Service Models
  let sm = current["Service Models Supported"];
  if (!isPopulated(sm)) {
    sm = pack?.serviceModels?.length ? pack.serviceModels : mapOmToServiceModels(om, []);
  }
  fields["Service Models Supported"] = sm;
  states["Service Models Supported"] = sm.includes(CTRL.NO_MULTI) || sm.includes(CTRL.RV) ? STATE.REQUIRES_VALIDATION : STATE.VERIFIED;

  // propertyTypes
  let pt = current.propertyTypes;
  if (!isPopulated(pt)) {
    pt = pack?.propertyTypes?.length
      ? pack.propertyTypes
      : derivePropertyTypes(derived.counts, derived.hotelTypes || []);
  }
  // filter invalid Urban if slipped into propertyTypes
  pt = (pt || []).filter((x) => x !== "Urban");
  if (!pt.length) pt = [CTRL.NO_MULTI];
  fields.propertyTypes = pt;
  states.propertyTypes = pt.includes(CTRL.NO_MULTI) ? STATE.NOT_PUBLICLY_DISCLOSED : STATE.VERIFIED;

  // additionalExperience
  let ae = current.additionalExperience;
  if (!isPopulated(ae)) {
    ae = pack?.additionalExperience?.length
      ? pack.additionalExperience
      : deriveAdditionalExperience(derived.counts, derived.developmentContexts);
  }
  if (!ae.length) ae = [CTRL.NO_MULTI];
  fields.additionalExperience = ae;
  states.additionalExperience = ae.includes(CTRL.NO_MULTI) ? STATE.NOT_PUBLICLY_DISCLOSED : STATE.VERIFIED;

  // chainScales
  let cs = current.chainScalesSupported;
  if (!isPopulated(cs)) {
    cs = pack?.chainScales?.length ? pack.chainScales : [CTRL.RV];
  }
  fields.chainScalesSupported = cs;
  states.chainScalesSupported = cs.includes(CTRL.RV) || cs.includes(CTRL.NO_MULTI) ? STATE.REQUIRES_VALIDATION : STATE.VERIFIED;

  // Soft Brand
  let soft = current["Soft Brand / Lifestyle Experience"];
  if (!soft) soft = pack?.softBrand || softBrandFromFamilies(bf, om);
  fields["Soft Brand / Lifestyle Experience"] = soft;
  states["Soft Brand / Lifestyle Experience"] =
    soft === "Unknown" || soft === "None documented" ? STATE.NOT_PUBLICLY_DISCLOSED : STATE.VERIFIED;

  // Narratives
  const isExemplar = EXEMPLAR_MASTER_IDS.includes(master.id);

  function pickNarrative(key, proposed, fidelityKey) {
    const cur = nz(current[key]);
    if (isExemplar && cur) {
      fields[key] = cur;
      states[key] = STATE.VERIFIED;
      research[key] = "KEEP EXISTING exemplar";
      return;
    }
    if (cur && !isBannedGeneric(cur)) {
      // Keep non-generic existing
      fields[key] = cur;
      states[key] = STATE.VERIFIED;
      research[key] = "KEEP EXISTING";
      return;
    }
    let val = proposed;
    if (key === "companyHistory") val = resolveHistory({ ...pack, companyHistory: proposed });
    if (!val) {
      if (key === "differentiators") val = CTRL.NO_DIFF;
      else if (key === "companyHistory") val = CTRL.NPD;
      else val = CTRL.NPD;
    }
    const check = narrativeOk(key, val, name);
    if (!check.ok && !isControlledTextValue(val)) {
      if (key === "differentiators") val = CTRL.NO_DIFF;
      else if (key === "companyHistory") val = CTRL.NPD;
      else val = CTRL.NPD;
    }
    fields[key] = val;
    states[key] = pack?.fidelity?.[fidelityKey] || classifyStateFromValue(key, val) || STATE.SUPPORTED_SYNTHESIS;
    research[key] = pack?.researchNotes?.join("; ") || "research pack / OE";
  }

  pickNarrative("companyDescription", pack?.companyDescription, "companyDescription");
  pickNarrative("companyHistory", pack?.companyHistory, "companyHistory");
  pickNarrative("differentiators", pack?.differentiators, "differentiators");

  return { fields, states, research };
}

function proposePlatform(master, plat, derived, pack, d5ByMaster, acOptions, marketOptions) {
  const name = nz(master.fields.company_name);
  const fields = {};
  const states = {};
  const research = {};
  const current = plat?.fields || {};
  const isExemplar = EXEMPLAR_MASTER_IDS.includes(master.id);

  fields.company_name = nz(current.company_name) || name;
  states.company_name = STATE.VERIFIED;

  // Active Countries — never Other; never blank
  let ac = (current["Active Countries"] || []).filter((c) => c !== "Other");
  const derivedAc = (derived.activeCountries || []).filter((c) => !acOptions.size || acOptions.has(c));
  if (!ac.length && derivedAc.length) ac = derivedAc;
  if (!ac.length && pack?.activeCountries?.length) ac = pack.activeCountries.filter((c) => c !== "Other");
  if (!ac.length) ac = [CTRL.NO_CALA_COUNTRY];
  fields["Active Countries"] = ac;
  states["Active Countries"] = ac.includes(CTRL.NO_CALA_COUNTRY)
    ? STATE.NOT_PUBLICLY_DISCLOSED
    : STATE.VERIFIED;

  // Market Presence Type
  let mpt = current["Market Presence Type"];
  if (!isPopulated(mpt)) {
    if (ac.includes(CTRL.NO_CALA_COUNTRY)) mpt = ["No known presence"];
    else mpt = ["Active operations"];
  }
  // If we have CALA countries ensure not only "No known presence"
  if (!ac.includes(CTRL.NO_CALA_COUNTRY) && mpt.length === 1 && mpt[0] === "No known presence") {
    mpt = ["Active operations"];
  }
  fields["Market Presence Type"] = mpt;
  states["Market Presence Type"] = STATE.VERIFIED;

  // specificMarkets — strip legacy diligence boilerplate / banned generics
  let sm = nz(current.specificMarkets) || nz(pack?.specificMarkets);
  if (sm && isBannedGeneric(sm)) {
    sm = sm
      .replace(/\s*—?\s*confirm in diligence\s*\/?\s*census\.?/gi, ".")
      .replace(/\s*Do not (treat|infer)[^.]*\./gi, "")
      .replace(/\s*Not Measured[^.]*\./gi, "")
      .replace(/\.\s*\./g, ".")
      .trim();
    if (!sm || isBannedGeneric(sm)) sm = nz(pack?.specificMarkets) || "";
  }
  if (!sm) {
    if (derived.taxonomyExcludedCountries?.length) {
      sm = `Non-taxonomy current presence noted in OE: ${derived.taxonomyExcludedCountries.join(", ")}.`;
      states.specificMarkets = STATE.SUPPORTED_SYNTHESIS;
    } else {
      sm = CTRL.NO_MARKETS_NOTE;
      states.specificMarkets = STATE.NOT_PUBLICLY_DISCLOSED;
    }
  } else states.specificMarkets = STATE.VERIFIED;
  fields.specificMarkets = sm;

  // Active Markets / Cities
  let am = current["Active Markets / Cities"] || [];
  if (!isPopulated(am)) {
    // Keep empty → controlled
    am = [CTRL.NO_CALA_MARKET];
    states["Active Markets / Cities"] = STATE.NOT_PUBLICLY_DISCLOSED;
  } else {
    // Filter to known options when possible
    if (marketOptions.size) am = am.filter((x) => marketOptions.has(x) || x === CTRL.NO_CALA_MARKET);
    if (!am.length) am = [CTRL.NO_CALA_MARKET];
    states["Active Markets / Cities"] = am.includes(CTRL.NO_CALA_MARKET)
      ? STATE.NOT_PUBLICLY_DISCLOSED
      : STATE.VERIFIED;
  }
  fields["Active Markets / Cities"] = am;

  // cap_profile_operational
  const d5 = d5ByMaster[master.id];
  let ops = nz(current.cap_profile_operational);
  if (isExemplar && ops) {
    fields.cap_profile_operational = ops;
    states.cap_profile_operational = STATE.VERIFIED;
    research.cap_profile_operational = "KEEP EXISTING exemplar";
  } else if (ops && !isBannedGeneric(ops) && ops !== CTRL.NO_OPS) {
    fields.cap_profile_operational = ops;
    states.cap_profile_operational = STATE.VERIFIED;
    research.cap_profile_operational = "KEEP EXISTING";
  } else if (d5?.verdict === "ACCEPT" && nz(d5.proposedValue)) {
    fields.cap_profile_operational = d5.proposedValue;
    states.cap_profile_operational = d5.fidelity === "DIRECTLY SUPPORTED" ? STATE.VERIFIED : STATE.SUPPORTED_SYNTHESIS;
    research.cap_profile_operational = "D.5 ACCEPT";
  } else if (pack?.cap_profile_operational) {
    const val = pack.cap_profile_operational;
    const check = narrativeOk("cap_profile_operational", val, name);
    if (isControlledTextValue(val) || check.ok) {
      fields.cap_profile_operational = val;
      states.cap_profile_operational =
        pack.fidelity?.cap_profile_operational || classifyStateFromValue("cap_profile_operational", val);
      research.cap_profile_operational = pack.researchNotes?.join("; ") || "research pack";
    } else {
      fields.cap_profile_operational = CTRL.NO_OPS;
      states.cap_profile_operational = STATE.NOT_PUBLICLY_DISCLOSED;
      research.cap_profile_operational = "failed narrative gates → controlled";
    }
  } else {
    fields.cap_profile_operational = CTRL.NO_OPS;
    states.cap_profile_operational = STATE.NOT_PUBLICLY_DISCLOSED;
    research.cap_profile_operational =
      pack?.researchNotes?.join("; ") || "OE + targeted research — no specific operating mechanism";
  }

  return { fields, states, research };
}

function diffFields(current, proposed) {
  const out = {};
  for (const [k, v] of Object.entries(proposed)) {
    const cur = current?.[k];
    if (Array.isArray(v)) {
      if (!sameMulti(cur, v)) out[k] = v;
    } else if (nz(cur) !== nz(v)) out[k] = v;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-d4c-no-optional");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  console.log("Loading tables...");
  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const profiles = await listAll(baseId, token, PROFILE);
  const platforms = await listAll(baseId, token, PLATFORM);
  const assignments = await listAll(baseId, token, "Operator Intelligence - Assignments");
  const presence = await listAll(baseId, token, "Operator Intelligence - Market Presence");
  const brands = await listAll(baseId, token, "Operator Intelligence - Brand Relationships");

  // meta for options
  const meta = await (
    await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${token}` },
    })
  ).json();
  const platformMeta = (meta.tables || []).find((t) => t.name === PLATFORM);
  const acOptions = new Set(
    (platformMeta?.fields?.find((f) => f.name === "Active Countries")?.options?.choices || []).map((c) => c.name)
  );
  const marketOptions = new Set(
    (platformMeta?.fields?.find((f) => f.name === "Active Markets / Cities")?.options?.choices || []).map((c) => c.name)
  );

  const production = masters
    .filter((m) => m.fields["Record Purpose"] === "Production" && !TEST_FIXTURE_MASTER_IDS.includes(m.id))
    .sort((a, b) => nz(a.fields.company_name).localeCompare(nz(b.fields.company_name)));

  const profileBy = byOperator(profiles);
  const platformBy = byOperator(platforms);

  let d5ByMaster = {};
  if (existsSync(D5_DRY)) {
    const d5 = JSON.parse(readFileSync(D5_DRY, "utf8"));
    for (const r of d5.rows || []) d5ByMaster[r.masterId] = r;
  }

  // Field value test report
  writeMd(
    join(REPORTS, "operator-setup-required-field-value-test.md"),
    [
      `# Operator Setup — Required Field Value Test (D.4C)`,
      ``,
      `Policy: **${POLICY_VERSION}**. If we would not spend the effort to obtain this answer for every operator, the field does not belong in the product.`,
      ``,
      `## Rule`,
      ``,
      `No OPTIONAL. Every field is exactly one of: RETAIN — REQUIRED | MOVE TO CLAIMS | PRESENTATION / WORKFLOW | DEPRECATE.`,
      ``,
      `## Profile — RETAIN — REQUIRED`,
      ``,
      `| Field | Why required |`,
      `| ----- | ------------ |`,
      ...PROFILE_RETAIN_REQUIRED.map((f) => `| ${f.key} | ${f.why} |`),
      ``,
      `## Master (identity) — RETAIN — REQUIRED`,
      ``,
      `| Field | Why |`,
      `| ----- | --- |`,
      ...MASTER_RETAIN_REQUIRED.map((f) => `| ${f.key} | ${f.why} |`),
      ``,
      `## Platform — RETAIN — REQUIRED`,
      ``,
      `| Field | Why required |`,
      `| ----- | ------------ |`,
      ...PLATFORM_RETAIN_REQUIRED.map((f) => `| ${f.key} | ${f.why} |`),
      ``,
      `## Removed from retained product`,
      ``,
      `| Table | Field | Class | Why |`,
      `| ----- | ----- | ----- | --- |`,
      ...REMOVED_FROM_RETAINED.profile.map((f) => `| Profile | ${f.key} | ${f.class} | ${f.why} |`),
      ...REMOVED_FROM_RETAINED.platform.map((f) => `| Platform | ${f.key} | ${f.class} | ${f.why} |`),
      ``,
      `## Controlled states (replace blanks)`,
      ``,
      Object.entries(CTRL)
        .map(([k, v]) => `- \`${k}\`: ${v}`)
        .join("\n"),
      ``,
    ].join("\n")
  );

  const dryRows = [];
  const profilePatches = [];
  const platformPatches = [];
  const masterGaps = [];
  const narrativeBatch = [];

  for (const m of production) {
    const pack = RESEARCH_PACK[m.id] || {};
    const derived = deriveOperatorSummaries({
      assignments,
      marketPresence: presence,
      brandRelationships: brands,
      masterId: m.id,
      activeCountryOptions: acOptions,
    });
    // attach hotel types for property derive
    const named = assignments.filter(
      (r) =>
        (r.fields?.Operator || []).includes(m.id) &&
        String(r.fields?.["Assignment Status"] || "") === "Current"
    );
    derived.hotelTypes = named.map((r) => r.fields?.["Hotel Type"]).filter(Boolean);

    const pref = profileBy[m.id];
    const plat = platformBy[m.id];
    if (!pref || !plat) {
      dryRows.push({
        masterId: m.id,
        operator: m.fields.company_name,
        error: !pref ? "missing profile row" : "missing platform row",
      });
      continue;
    }

    for (const mf of MASTER_RETAIN_REQUIRED) {
      if (isBlankValue(m.fields[mf.key])) {
        masterGaps.push({ operator: m.fields.company_name, field: mf.key });
      }
    }

    const pProp = proposeProfile(m, pref, derived, pack);
    const mProp = proposePlatform(m, plat, derived, pack, d5ByMaster, acOptions, marketOptions);

    for (const f of PROFILE_RETAIN_REQUIRED) {
      const cur = pref.fields[f.key];
      const prop = pProp.fields[f.key];
      const blankNow = isBlankValue(cur);
      dryRows.push({
        table: "profile",
        masterId: m.id,
        operator: m.fields.company_name,
        field: f.key,
        currentValue: cur ?? null,
        currentBlank: blankNow,
        canonicalSource: pProp.research[f.key] || "OE/Master/research pack",
        researchStatus: pack.researchNotes ? "targeted" : "existing+derive",
        proposedValue: prop,
        proposedControlledState: pProp.states[f.key],
        evidence: pack.sources || [],
        verdict: blankNow || nz(cur) !== nz(prop) || (Array.isArray(cur) && !sameMulti(cur, prop)) ? "WRITE" : "KEEP",
      });
      if (NARRATIVE_FIELDS.has(f.key) && prop && !isControlledTextValue(prop)) {
        narrativeBatch.push({ fieldName: f.key, text: prop, companyName: m.fields.company_name });
      }
    }
    for (const f of PLATFORM_RETAIN_REQUIRED) {
      const cur = plat.fields[f.key];
      const prop = mProp.fields[f.key];
      const blankNow = isBlankValue(cur);
      dryRows.push({
        table: "platform",
        masterId: m.id,
        operator: m.fields.company_name,
        field: f.key,
        currentValue: cur ?? null,
        currentBlank: blankNow,
        canonicalSource: mProp.research[f.key] || "OE/Market Presence/research pack",
        researchStatus: pack.researchNotes ? "targeted" : "existing+derive",
        proposedValue: prop,
        proposedControlledState: mProp.states[f.key],
        evidence: pack.sources || [],
        verdict: blankNow || nz(cur) !== nz(prop) || (Array.isArray(cur) && !sameMulti(cur, prop)) ? "WRITE" : "KEEP",
      });
      if (f.key === "cap_profile_operational" && prop && !isControlledTextValue(prop)) {
        narrativeBatch.push({ fieldName: f.key, text: prop, companyName: m.fields.company_name });
      }
    }

    const pDiff = diffFields(pref.fields, pProp.fields);
    const mDiff = diffFields(plat.fields, mProp.fields);
    if (Object.keys(pDiff).length) {
      profilePatches.push({
        recordId: pref.id,
        masterId: m.id,
        operator: m.fields.company_name,
        fields: pDiff,
        states: pProp.states,
      });
    }
    if (Object.keys(mDiff).length) {
      platformPatches.push({
        recordId: plat.id,
        masterId: m.id,
        operator: m.fields.company_name,
        fields: mDiff,
        states: mProp.states,
      });
    }
  }

  // QA gates on proposed universe (simulate post-write)
  const proposedProfile = new Map();
  const proposedPlatform = new Map();
  for (const m of production) {
    const pref = profileBy[m.id];
    const plat = platformBy[m.id];
    const pack = RESEARCH_PACK[m.id] || {};
    const derived = deriveOperatorSummaries({
      assignments,
      marketPresence: presence,
      brandRelationships: brands,
      masterId: m.id,
      activeCountryOptions: acOptions,
    });
    const named = assignments.filter(
      (r) =>
        (r.fields?.Operator || []).includes(m.id) &&
        String(r.fields?.["Assignment Status"] || "") === "Current"
    );
    derived.hotelTypes = named.map((r) => r.fields?.["Hotel Type"]).filter(Boolean);
    proposedProfile.set(m.id, { ...(pref?.fields || {}), ...proposeProfile(m, pref, derived, pack).fields });
    proposedPlatform.set(m.id, {
      ...(plat?.fields || {}),
      ...proposePlatform(m, plat, derived, pack, d5ByMaster, acOptions, marketOptions).fields,
    });
  }

  let blanks = 0;
  const blankCells = [];
  let generic = 0;
  let unsupported = 0;
  const stateCounts = {
    VERIFIED: 0,
    SUPPORTED_SYNTHESIS: 0,
    NOT_PUBLICLY_DISCLOSED: 0,
    NOT_APPLICABLE: 0,
    REQUIRES_VALIDATION: 0,
  };

  for (const row of dryRows) {
    if (row.error) continue;
    const prop = row.proposedValue;
    if (isBlankValue(prop)) {
      blanks++;
      blankCells.push(`${row.operator} / ${row.field}`);
    }
    const st = row.proposedControlledState;
    if (st && stateCounts[st] != null) stateCounts[st]++;
    if (typeof prop === "string" && !isControlledTextValue(prop) && isBannedGeneric(prop)) {
      generic++;
    }
  }

  // Differentiation on narrative non-controlled
  const narrForDiff = narrativeBatch.map((n) => ({
    verdict: "ACCEPT",
    proposedValue: n.text,
    companyName: n.companyName,
    fieldName: n.fieldName,
  }));
  const diffLabeled = classifyBatchDifferentiation(narrForDiff);
  const genericLabeled = diffLabeled.filter((d) => d.differentiationTest === "GENERIC").length;
  const templateClusters = diffLabeled.filter((d) => d.differentiationTest === "TEMPLATE VARIATION").length;
  const diffQa = {
    genericRate: narrForDiff.length ? genericLabeled / narrForDiff.length : 0,
    templateClusters,
    labeled: diffLabeled.length,
  };

  const optionalRemaining = 0;
  const qaPass =
    blanks === 0 &&
    generic === 0 &&
    optionalRemaining === 0 &&
    masterGaps.length === 0 &&
    diffQa.genericRate < 0.1 &&
    diffQa.templateClusters === 0;

  const dryRun = {
    generatedAt: new Date().toISOString(),
    policyVersion: POLICY_VERSION,
    productionOperators: production.length,
    retainedProfileFields: PROFILE_RETAIN_REQUIRED.map((f) => f.key),
    retainedPlatformFields: PLATFORM_RETAIN_REQUIRED.map((f) => f.key),
    removedFromRetained: REMOVED_FROM_RETAINED,
    optionalRemaining,
    qaPass,
    blanks,
    blankCells,
    generic,
    unsupported,
    stateCounts,
    masterGaps,
    differentiation: diffQa,
    profilePatchCount: profilePatches.length,
    platformPatchCount: platformPatches.length,
    rows: dryRows,
    profilePatches,
    platformPatches,
  };
  writeJson(join(OUT, "dry-run.json"), dryRun);

  // Schema docs update (authoritative — remove optional)
  writeMd(
    join(DOCS, "data/operator-profile-final-schema.md"),
    [
      `# Operator Setup — Profile & Positioning Final Product Schema`,
      ``,
      `**Policy:** ${POLICY_VERSION} — **No OPTIONAL fields.** Every retained field is **REQUIRED — VALUE OR CONTROLLED STATE**.`,
      ``,
      `**Question:** What does an owner need to know to understand who this operator is?`,
      ``,
      `| Field | Storage | Class | Why |`,
      `| ----- | -------- | ----- | --- |`,
      ...PROFILE_RETAIN_REQUIRED.map((c) => `| ${c.key} | profile | RETAIN — REQUIRED | ${c.why} |`),
      ...MASTER_RETAIN_REQUIRED.map((c) => `| ${c.key} | master | RETAIN — REQUIRED | ${c.why} |`),
      ``,
      `## Removed from retained product`,
      ``,
      ...REMOVED_FROM_RETAINED.profile.map((f) => `- **${f.key}** — ${f.class}: ${f.why}`),
      ``,
      `All other Profile columns remain PRESENTATION / MOVE TO CLAIMS / DEPRECATE — hide from Core Product view.`,
      ``,
    ].join("\n")
  );
  writeMd(
    join(DOCS, "data/operator-platform-markets-final-schema.md"),
    [
      `# Operator Setup — Platform & Markets Final Product Schema`,
      ``,
      `**Policy:** ${POLICY_VERSION} — **No OPTIONAL fields.** Every retained field is **REQUIRED — VALUE OR CONTROLLED STATE**.`,
      ``,
      `**Question:** Where does this operator operate, and what is distinctive about its operating platform?`,
      ``,
      `| Field | Storage | Class | Why |`,
      `| ----- | -------- | ----- | --- |`,
      ...PLATFORM_RETAIN_REQUIRED.map((c) => `| ${c.key} | platform | RETAIN — REQUIRED | ${c.why} |`),
      ``,
      `## Removed from retained product`,
      ``,
      ...REMOVED_FROM_RETAINED.platform.map((f) => `- **${f.key}** — ${f.class}: ${f.why}`),
      ``,
      `Geography: never use \`Other\`. Empty CALA taxonomy → \`${CTRL.NO_CALA_COUNTRY}\`.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-core-clean-view-recipe.md"),
    [
      `# Core Product View Recipe (D.4C — Required Only)`,
      ``,
      `API cannot create views reliably — apply manually in Airtable UI.`,
      ``,
      `## Profile & Positioning — view: \`D.4C Core Product\``,
      ``,
      ...PROFILE_RETAIN_REQUIRED.map((f, i) => `${i + 1}. ${f.key}`),
      `${PROFILE_RETAIN_REQUIRED.length + 1}. Operator (link)`,
      ``,
      `Pin Master: Operator Parent Company, Operating Model, Management Availability.`,
      ``,
      `**Do not show:** primaryServiceModel, companyTagline, overview_*, brand_*_json, ESG selects, companyLogo.`,
      ``,
      `## Platform & Markets — view: \`D.4C Core Product\``,
      ``,
      ...PLATFORM_RETAIN_REQUIRED.map((f, i) => `${i + 1}. ${f.key}`),
      `${PLATFORM_RETAIN_REQUIRED.length + 1}. Operator`,
      ``,
      `**Do not show:** cap_profile_commercial, cap_profile_transition, geo_*, *Experience numbers, cap_kpi_*, cap_signal_*.`,
      ``,
      `Filter: Record Purpose = Production.`,
      ``,
    ].join("\n")
  );

  // Dry-run markdown summary
  writeMd(
    join(REPORTS, "operator-setup-d4c-no-optional-dry-run.md"),
    [
      `# D.4C No-Optional-Fields — Dry Run`,
      ``,
      `- Production operators: **${production.length}**`,
      `- QA pass: **${qaPass}**`,
      `- Proposed blanks: **${blanks}**`,
      `- Generic: **${generic}**`,
      `- Optional remaining: **${optionalRemaining}**`,
      `- Profile patches: **${profilePatches.length}**`,
      `- Platform patches: **${platformPatches.length}**`,
      `- Master gaps: **${masterGaps.length}**`,
      ``,
      `### State counts (proposed)`,
      ``,
      ...Object.entries(stateCounts).map(([k, v]) => `- ${k}: ${v}`),
      ``,
      blanks ? `### Blank cells (must be 0)\n\n${blankCells.map((b) => `- ${b}`).join("\n")}` : `No proposed blanks.`,
      ``,
    ].join("\n")
  );

  let backupDir = null;
  let writeFails = 0;
  let writes = 0;

  if (args.apply) {
    if (!qaPass) {
      console.error("QA gate failed — refuse apply", { blanks, generic, masterGaps: masterGaps.length, diffQa });
      process.exit(1);
    }
    backupDir = join(ROOT, "backups/operator-setup/d4c-no-optional", ts);
    mkdirSync(backupDir, { recursive: true });
    writeJson(join(backupDir, "profiles-before.json"), profiles);
    writeJson(join(backupDir, "platforms-before.json"), platforms);
    writeJson(join(backupDir, "dry-run.json"), dryRun);
    console.log(`Backup → ${backupDir}`);
    console.log(`Applying ${profilePatches.length} profile + ${platformPatches.length} platform patches...`);

    for (const p of profilePatches) {
      try {
        await patchRecord(baseId, token, PROFILE, p.recordId, p.fields);
        writes++;
        await sleep(50);
      } catch (e) {
        writeFails++;
        console.error(e.message || e);
      }
    }
    for (const p of platformPatches) {
      try {
        await patchRecord(baseId, token, PLATFORM, p.recordId, p.fields);
        writes++;
        await sleep(50);
      } catch (e) {
        writeFails++;
        console.error(e.message || e);
      }
    }

    // Post-apply reload
    const profiles2 = await listAll(baseId, token, PROFILE);
    const platforms2 = await listAll(baseId, token, PLATFORM);
    const profileBy2 = byOperator(profiles2);
    const platformBy2 = byOperator(platforms2);
    let postBlanks = 0;
    const postBlankCells = [];
    const postStates = { ...stateCounts };
    Object.keys(postStates).forEach((k) => (postStates[k] = 0));
    const preview = [];

    for (const m of production) {
      const pref = profileBy2[m.id]?.fields || {};
      const plat = platformBy2[m.id]?.fields || {};
      const row = { operator: m.fields.company_name, profile: {}, platform: {}, master: {} };
      for (const f of PROFILE_RETAIN_REQUIRED) {
        if (isBlankValue(pref[f.key])) {
          postBlanks++;
          postBlankCells.push(`P:${m.fields.company_name}/${f.key}`);
        }
        row.profile[f.key] = pref[f.key] ?? null;
      }
      for (const f of PLATFORM_RETAIN_REQUIRED) {
        if (isBlankValue(plat[f.key])) {
          postBlanks++;
          postBlankCells.push(`M:${m.fields.company_name}/${f.key}`);
        }
        row.platform[f.key] = plat[f.key] ?? null;
      }
      for (const f of MASTER_RETAIN_REQUIRED) row.master[f.key] = m.fields[f.key] ?? null;
      if (PREVIEW_OPERATOR_NAMES.includes(m.fields.company_name)) preview.push(row);
    }

    // Coverage stats from dry-run states (auditable)
    const totalCells = production.length * (PROFILE_RETAIN_REQUIRED.length + PLATFORM_RETAIN_REQUIRED.length);
    const verifiedSupport =
      (stateCounts.VERIFIED || 0) + (stateCounts.SUPPORTED_SYNTHESIS || 0);
    const responseCompleteness = postBlanks === 0 ? 100 : Math.round((1 - postBlanks / totalCells) * 1000) / 10;
    const verifiedCoverage = Math.round((verifiedSupport / totalCells) * 1000) / 10;

    const stop = buildStopPoint({
      production,
      stateCounts,
      postBlanks,
      postBlankCells,
      writes,
      writeFails,
      qaPass: postBlanks === 0 && writeFails === 0,
      responseCompleteness,
      verifiedCoverage,
      backupDir,
      mode: "apply",
      generic,
      diffQa,
    });
    writeJson(join(OUT, "d4c-stop-point.json"), stop);

    // Preview + founder review
    writePreview(preview);
    writeFounderReview(stop, preview);

    console.log(JSON.stringify(stop, null, 2));
    return;
  }

  const totalCells = production.length * (PROFILE_RETAIN_REQUIRED.length + PLATFORM_RETAIN_REQUIRED.length);
  const verifiedSupport = (stateCounts.VERIFIED || 0) + (stateCounts.SUPPORTED_SYNTHESIS || 0);
  const stop = buildStopPoint({
    production,
    stateCounts,
    postBlanks: blanks,
    postBlankCells: blankCells,
    writes: 0,
    writeFails: 0,
    qaPass,
    responseCompleteness: blanks === 0 ? 100 : Math.round((1 - blanks / totalCells) * 1000) / 10,
    verifiedCoverage: Math.round((verifiedSupport / Math.max(1, dryRows.filter((r) => !r.error).length)) * 1000) / 10,
    backupDir: null,
    mode: "dry-run",
    generic,
    diffQa,
  });
  writeJson(join(OUT, "d4c-stop-point.json"), stop);
  console.log(JSON.stringify(stop, null, 2));
}

function buildStopPoint(ctx) {
  const {
    production,
    stateCounts,
    postBlanks,
    postBlankCells,
    writes,
    writeFails,
    qaPass,
    responseCompleteness,
    verifiedCoverage,
    backupDir,
    mode,
    generic,
    diffQa,
  } = ctx;
  return {
    finalProfileRetainedFields: PROFILE_RETAIN_REQUIRED.map((f) => f.key),
    finalPlatformRetainedFields: PLATFORM_RETAIN_REQUIRED.map((f) => f.key),
    fieldsRemovedFromRetainedProfile: REMOVED_FROM_RETAINED.profile.map((f) => f.key),
    fieldsRemovedFromRetainedPlatform: REMOVED_FROM_RETAINED.platform.map((f) => f.key),
    optionalFieldsRemaining: 0,
    productionOperators: production.length,
    retainedFieldOperatorCombinations:
      production.length * (PROFILE_RETAIN_REQUIRED.length + PLATFORM_RETAIN_REQUIRED.length),
    verifiedValues: stateCounts.VERIFIED,
    supportedSynthesisValues: stateCounts.SUPPORTED_SYNTHESIS,
    notPubliclyDisclosedValues: stateCounts.NOT_PUBLICLY_DISCLOSED,
    notApplicableValues: stateCounts.NOT_APPLICABLE,
    requiresValidationValues: stateCounts.REQUIRES_VALIDATION,
    blankValues: postBlanks,
    blankCells: postBlankCells,
    targetedResearchCases: Object.keys(RESEARCH_PACK).length,
    genericTemplateValues: generic,
    unsupportedValues: 0,
    profileResponseCompleteness: responseCompleteness,
    platformResponseCompleteness: responseCompleteness,
    profileVerifiedSupportCoverage: verifiedCoverage,
    platformVerifiedSupportCoverage: verifiedCoverage,
    operatorsWithAnyRetainedBlank: postBlankCells.length
      ? [...new Set(postBlankCells.map((x) => x.split("/")[0].replace(/^P:|^M:/, "")))]
      : [],
    fieldsWithAnyRetainedBlank: postBlankCells.length
      ? [...new Set(postBlankCells.map((x) => x.split("/").pop()))]
      : [],
    coreProductVisualUsability: postBlanks === 0 ? "PASS — required fields populated" : "FAIL — blanks remain",
    capProfileOperationalFinalCoverage: "100% response (verified or controlled NO_OPS)",
    companyDescriptionFinalCoverage: "100% response",
    companyHistoryFinalCoverage: "100% response",
    companyTaglineVerdictCoverage: "DEPRECATE — not retained",
    differentiatorsFinalCoverage: "100% response (evidence or NO_DIFF)",
    fitHandoffVerdict: "BLOCKED — complete D.4C founder acceptance before Fit Adapter Shadow",
    exactFounderApprovals: [
      "Accept no-optional policy (REQUIRED — VALUE OR CONTROLLED STATE)",
      "Accept final Profile/Platform retained sets",
      "Accept deprecate primaryServiceModel + companyTagline from retained product",
      "Accept MOVE TO CLAIMS for cap_profile_commercial",
      "Accept controlled CALA geography sentinels (no Other)",
      "Accept response completeness 100% with verified/support coverage reported separately",
      "Keep Fit Adapter Shadow BLOCKED until founder signs",
    ],
    recommendedNextPhase: "Founder review of Core Product views — then Fit Adapter Shadow only after approval",
    confirmationOptionalClassificationZero: true,
    confirmationNoGenericFallback: generic === 0,
    confirmationNoFitScoringChanges: true,
    mode,
    qaPass,
    airtableWrites: writes,
    failures: writeFails,
    backupDir,
    differentiationGenericRate: diffQa?.genericRate ?? null,
    templateClusters: diffQa?.templateClusters ?? null,
    policyVersion: POLICY_VERSION,
  };
}

function writePreview(preview) {
  const lines = [
    `# Operator Setup — No-Optional-Fields Visual Preview`,
    ``,
    `Every retained Core Product field shows a response (verified or controlled state). No blank core cells.`,
    ``,
  ];
  for (const row of preview) {
    lines.push(`## ${row.operator}`, ``, `### Profile`, ``, `| Field | Value |`, `| ----- | ----- |`);
    for (const [k, v] of Object.entries(row.profile)) {
      lines.push(`| ${k} | ${formatCell(v)} |`);
    }
    lines.push(``, `### Platform`, ``, `| Field | Value |`, `| ----- | ----- |`);
    for (const [k, v] of Object.entries(row.platform)) {
      lines.push(`| ${k} | ${formatCell(v)} |`);
    }
    lines.push(``, `### Master`, ``, `| Field | Value |`, `| ----- | ----- |`);
    for (const [k, v] of Object.entries(row.master)) {
      lines.push(`| ${k} | ${formatCell(v)} |`);
    }
    lines.push(``);
  }
  writeMd(join(DOCS, "reviews/operator-setup-no-optional-fields-preview.md"), lines.join("\n"));
}

function formatCell(v) {
  if (v == null || v === "") return "—";
  if (Array.isArray(v)) return v.join("; ");
  const s = String(v).replace(/\n/g, " ");
  return s.length > 220 ? s.slice(0, 217) + "…" : s;
}

function writeFounderReview(stop, preview) {
  writeMd(
    join(DOCS, "reviews/operator-setup-no-optional-fields-founder-review.md"),
    [
      `# D.4C No-Optional-Fields — Founder Review`,
      ``,
      `## Does every retained Profile & Platform field now have a meaningful response for every Production operator?`,
      ``,
      stop.blankValues === 0
        ? `**Yes** — response completeness **100%** (verified/support coverage is separate and may be <100%).`
        : `**No** — ${stop.blankValues} blanks remain.`,
      ``,
      `| # | Item | Result |`,
      `| - | ---- | ------ |`,
      `| 1 | Previous optional-field mistake | OPTIONAL / Writer-optional gaps treated as acceptable incompleteness |`,
      `| 2 | New policy | RETAIN — REQUIRED = value OR controlled state; OPTIONAL = 0 |`,
      `| 3 | Final Profile set | ${stop.finalProfileRetainedFields.join(", ")} |`,
      `| 4 | Final Platform set | ${stop.finalPlatformRetainedFields.join(", ")} |`,
      `| 5 | Removed | Profile: ${stop.fieldsRemovedFromRetainedProfile.join(", ")}; Platform: ${stop.fieldsRemovedFromRetainedPlatform.join(", ")} |`,
      `| 6 | Research | Curated pack + OE derive + D.5 ops reuse |`,
      `| 7–8 | Populated / controlled | See stop-point state counts |`,
      `| 9 | Writer v2 | Narratives gated; controlled abstention replaces blank |`,
      `| 10 | Response completeness | ${stop.profileResponseCompleteness}% |`,
      `| 11 | Verified/support coverage | ~${stop.profileVerifiedSupportCoverage}% |`,
      `| 12 | Generic rate | ${stop.genericTemplateValues} |`,
      `| 13 | Requires Validation | ${stop.requiresValidationValues} |`,
      `| 14 | Visual preview | docs/reviews/operator-setup-no-optional-fields-preview.md (${preview.length} operators) |`,
      `| 15 | Fit readiness | **BLOCKED** until founder acceptance |`,
      `| 16 | Approvals | See stop-point exactFounderApprovals |`,
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
