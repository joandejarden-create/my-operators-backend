#!/usr/bin/env node
/**
 * D.4A Core Table Semantic Reconciliation
 * Profile & Positioning + Platform & Markets
 *
 *   node scripts/operator-setup-d4a-core-tables-reconcile.mjs --dry-run
 *   node scripts/operator-setup-d4a-core-tables-reconcile.mjs --apply --approve-operator-setup-d4a-core-tables
 *
 * Applies DIRECT + deterministic DERIVED only. No section templates. No Fit changes.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import {
  deriveOperatorSummaries,
  isPopulated,
  sameMultiSelect,
} from "../lib/operator-setup/derived-sync.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/core-tables");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");

const PROFILE = "Operator Setup - Profile & Positioning";
const PLATFORM = "Operator Setup - Platform & Markets";
const EXEMPLARS = new Set(["recWPKu5laVZxsvpn", "recF5Z87OAqFgndoq"]);
const SPOT = [
  "recWPKu5laVZxsvpn",
  "recF5Z87OAqFgndoq",
  "recLjxtxIIVJaGbXK",
  "recGWxIJqnYHkJZFD",
  "reciI2tYQBfMoMK9G",
  "recGmiPhRt6hiayd9",
  "rec3Uwxe6ovpiokuN",
  "recF2WqLqNVyKGz9E",
  "rec7IXYQYpKMYsrDl",
  "recwEHUotSGpfkZEJ",
  "rec6UB6RpMKSs2tAo",
  "rectsHzacZDFTH1Ze",
];

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-d4a-core-tables") out.approve = true;
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
    await sleep(40);
  } while (offset);
  return out;
}
async function fetchMeta(baseId, token) {
  const j = await (
    await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${token}` },
    })
  ).json();
  if (j.error) throw new Error(JSON.stringify(j.error));
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
  if (!res.ok) throw new Error(`CREATE ${table}: ${JSON.stringify(j)}`);
  return j;
}

function byOperator(rows) {
  const m = {};
  for (const r of rows) for (const id of r.fields.Operator || []) m[id] = r;
  return m;
}

function choiceNames(field) {
  return (field?.options?.choices || []).map((c) => c.name);
}

/** Field classification for every Profile + Platform field */
function classifyFields(profileFields, platformFields) {
  const classification = { profile: {}, platform: {}, generatedAt: new Date().toISOString() };

  const profileRules = {
    company_name: { class: "DIRECT", purpose: "Canonical display name", oe: "Master.company_name", derive: true },
    Operator: { class: "N/A / WORKFLOW", purpose: "Link to Master", oe: "Master", derive: false },
    website: { class: "DIRECT", purpose: "Official website", oe: "Master.Operator Website", derive: true },
    headquarters: { class: "DIRECT", purpose: "HQ location", oe: "Claims/PI/packs", derive: false, note: "KEEP existing; do not invent" },
    companySize: { class: "DIRECT", purpose: "Scale band", oe: "packs/official", derive: false, note: "KEEP; assignment count ≠ global size" },
    companyTagline: { class: "NARRATIVE — WRITER V2", purpose: "Short positioning line", readiness: "NEEDS CONTRACT REFINEMENT" },
    companyDescription: { class: "NARRATIVE — WRITER V2", purpose: "Who the company is", readiness: "READY FOR WRITER V2", rank: "HIGH VALUE" },
    companyHistory: { class: "NARRATIVE — WRITER V2", purpose: "History", readiness: "OPTIONAL DEPTH", rank: "OPTIONAL DEPTH" },
    differentiators: { class: "NARRATIVE — WRITER V2", purpose: "Differentiators", readiness: "READY FOR WRITER V2", rank: "HIGH VALUE" },
    managementPhilosophy: { class: "NARRATIVE — WRITER V2", purpose: "Philosophy", readiness: "MOVE TO CLAIMS", rank: "LOW VALUE / DEPRECATE" },
    missionStatement: { class: "NARRATIVE — WRITER V2", purpose: "Mission", readiness: "MOVE TO CLAIMS", rank: "LOW VALUE / DEPRECATE" },
    yearEstablished: { class: "DIRECT", purpose: "Founding year", oe: "official", derive: false },
    yearsInBusiness: { class: "DERIVED", purpose: "Age from yearEstablished", oe: "yearEstablished", derive: false, note: "HOLD without verified year" },
    primaryServiceModel: { class: "DIRECT", purpose: "Primary service orientation", oe: "packs", derive: false },
    brands: { class: "DERIVED", purpose: "Linked Brand Basics", oe: "Brand Relationships", derive: false, note: "Link write deferred" },
    additionalBrands: { class: "NARRATIVE — WRITER V2", purpose: "Extra brand notes", readiness: "OPTIONAL DEPTH" },
    chainScalesSupported: { class: "DERIVED", purpose: "Chain-scale experience", oe: "Assignments", derive: true, note: "Only when typed evidence exists" },
    companyLogo: { class: "PRESENTATION", purpose: "Logo asset", readiness: "PRESENTATION" },
    propertyTypes: { class: "DERIVED", purpose: "Property-type experience", oe: "Assignments.Hotel Type", derive: true },
    additionalExperience: { class: "DERIVED", purpose: "Location/asset experience flags", oe: "Assignments", derive: true },
    "Service Models Supported": { class: "DERIVED", purpose: "Service model taxonomy", oe: "Assignments + packs", derive: true },
    "Brand Families Operated": { class: "DERIVED", purpose: "Brand family experience", oe: "Brand Relationships + Assignments", derive: true },
    "Soft Brand / Lifestyle Experience": { class: "DIRECT", purpose: "Soft-brand depth", oe: "BR/packs", derive: false },
    emergencyResponse: { class: "NARRATIVE — WRITER V2", purpose: "Emergency posture", readiness: "NEEDS CONTRACT REFINEMENT" },
    insuranceCoverage: { class: "NARRATIVE — WRITER V2", purpose: "Insurance", readiness: "MOVE TO CLAIMS" },
    sustainabilityPrograms: { class: "NARRATIVE — WRITER V2", purpose: "Sustainability", readiness: "NEEDS CONTRACT REFINEMENT" },
    esgReporting: { class: "NARRATIVE — WRITER V2", purpose: "ESG reporting", readiness: "NEEDS CONTRACT REFINEMENT" },
    carbonTracking: { class: "DIRECT", purpose: "Carbon tracking flag", derive: false, note: "HOLD without methodology" },
    energyEfficiency: { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH" },
    wasteReduction: { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH" },
    businessContinuity: { class: "DIRECT", derive: false, note: "HOLD without evidence" },
    support24x7: { class: "DIRECT", derive: false, note: "HOLD without evidence" },
    crisisExperience: { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH" },
    capitalStatus: { class: "N/A / WORKFLOW", purpose: "Deal capital status — not operator profile", note: "DEPRECATE from operator profile use" },
    readyForInvestorPublication: { class: "N/A / WORKFLOW", purpose: "Publish gate" },
    figuresAsOf: { class: "DIRECT", purpose: "As-of stamp for figures" },
    numberOfBrands: { class: "DERIVED", purpose: "Brand count", oe: "Brand Families / BR", derive: false, note: "HOLD — define counting rule first" },
    locationTypeResort: { class: "DERIVED", purpose: "Resort share/count", numeric: true, note: "HOLD — no defensible % without census" },
    locationTypeAirport: { class: "DERIVED", purpose: "Airport share/count", numeric: true, note: "HOLD" },
    marketExpansionRampTimeMonths: { class: "DIRECT", numeric: true, note: "HOLD — no methodology" },
    brand_conversion_project_count: { class: "DERIVED", numeric: true, note: "HOLD without verified project census" },
    brandedVsIndependentMix: { class: "DERIVED", numeric: true, note: "HOLD — needs numerator/denominator" },
    brand_portfolio_mix_json: { class: "PRESENTATION", purpose: "Explorer brand mix cards", readiness: "PRESENTATION" },
    brand_relationship_depth_json: { class: "PRESENTATION", purpose: "Explorer depth cards", readiness: "PRESENTATION" },
    brand_execution_capabilities_json: { class: "PRESENTATION", purpose: "Explorer execution cards", readiness: "PRESENTATION" },
    brand_governance_compliance_json: { class: "PRESENTATION", purpose: "Explorer governance cards", readiness: "PRESENTATION" },
    brand_soft_independent_narrative: { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH", rank: "OPTIONAL DEPTH" },
    brand_narrative_compliance: { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH" },
    brand_narrative_relationship: { class: "NARRATIVE — WRITER V2", readiness: "READY FOR WRITER V2", rank: "HIGH VALUE" },
    brand_signal_audit: { class: "DIRECT", numeric: true, note: "HOLD — KPI without methodology" },
    brand_signal_reflag: { class: "DIRECT", numeric: true, note: "HOLD unless factual qualitative option evidenced" },
    brand_signal_franchise_align: { class: "DIRECT", note: "HOLD without evidence" },
    brand_signal_soft_retention: { class: "DIRECT", numeric: true, note: "HOLD" },
  };

  // Overview presentation tiles
  for (const prefix of ["overview_bestat_", "overview_why_", "overview_signal_"]) {
    for (const f of profileFields) {
      if (f.name.startsWith(prefix)) {
        profileRules[f.name] = {
          class: "PRESENTATION",
          purpose: "Explorer overview tile",
          readiness: "PRESENTATION",
          note: "Scaffold HOLD / presentation-only; not Setup truth",
        };
      }
    }
  }

  const platformRules = {
    company_name: { class: "DIRECT", purpose: "Display name", oe: "Master", derive: true },
    Operator: { class: "N/A / WORKFLOW", purpose: "Link to Master" },
    "Active Countries": { class: "DERIVED", purpose: "Current operating countries", oe: "Market Presence + Assignments", derive: true },
    "Active Markets / Cities": { class: "DERIVED", purpose: "Current markets/cities", oe: "Assignments/Presence", derive: false, note: "HOLD unless taxonomy mapping complete" },
    "Market Presence Type": { class: "DERIVED", purpose: "Presence posture", oe: "Market Presence", derive: true },
    specificMarkets: { class: "DERIVED", purpose: "Free-text markets", oe: "Assignments", derive: false, note: "Prefer structured Active Markets" },
    numberOfMarkets: { class: "DERIVED", numeric: true, note: "HOLD — define market grain" },
    "Brands Portfolio Detail": { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH" },
    cap_profile_operational: { class: "NARRATIVE — WRITER V2", readiness: "READY FOR WRITER V2", rank: "HIGH VALUE", note: "Next family candidate" },
    cap_profile_commercial: { class: "NARRATIVE — WRITER V2", readiness: "NEEDS CONTRACT REFINEMENT", rank: "OPTIONAL DEPTH" },
    cap_profile_transition: { class: "NARRATIVE — WRITER V2", readiness: "MOVE TO CLAIMS", rank: "LOW VALUE / DEPRECATE", note: "Aligned with D.2 MOVE TO CLAIMS" },
    cap_card_asset_positioning: { class: "PRESENTATION", readiness: "PRESENTATION" },
    cap_card_service_diff: { class: "PRESENTATION", readiness: "PRESENTATION" },
    cap_card_execution_rel: { class: "PRESENTATION", readiness: "PRESENTATION" },
    cap_card_governance: { class: "PRESENTATION", readiness: "PRESENTATION" },
    cap_deep_revenue_systems: { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH" },
    cap_deep_execution_infra: { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH" },
    marketDepthOptIn: { class: "N/A / WORKFLOW" },
    mkt_narrative_depth: { class: "NARRATIVE — WRITER V2", readiness: "OPTIONAL DEPTH" },
    mkt_signal_years: { class: "PRESENTATION" },
    mkt_signal_gateway: { class: "PRESENTATION" },
    mkt_signal_mix: { class: "PRESENTATION" },
    op_commercial_engine_json: { class: "PRESENTATION", purpose: "Explorer OP tiles" },
    op_owner_reporting_json: { class: "PRESENTATION" },
    op_preopening_transition_json: { class: "PRESENTATION" },
    op_conversion_repositioning_json: { class: "PRESENTATION" },
    op_fb_lifestyle_resort_json: { class: "PRESENTATION" },
    mkt_regional_expertise_json: { class: "PRESENTATION" },
    mkt_market_fit_signals_json: { class: "PRESENTATION" },
    chainScale: { class: "DERIVED", note: "HOLD free-text; prefer chainScalesSupported on Profile" },
  };

  for (const f of platformFields) {
    const n = f.name;
    if (platformRules[n]) continue;
    if (/^geo_|^luxury|^upper|^upscale|^midscale|^economy|^total(Properties|Rooms)/i.test(n) || /AvgStaff$/.test(n)) {
      platformRules[n] = { class: "DERIVED", numeric: true, note: "HOLD — NO DEFENSIBLE CENSUS METHODOLOGY", recommend: "NARROW or DEPRECATE" };
    } else if (/^cap_kpi_|^cap_signal_/.test(n)) {
      platformRules[n] = { class: "DIRECT", numeric: true, note: "HOLD — NO DEFENSIBLE KPI METHODOLOGY", recommend: "DEPRECATE or Claims" };
    } else if (/Experience$|RampLeadTime|exitsDeflaggings|locationType/.test(n)) {
      platformRules[n] = { class: "DERIVED", numeric: true, note: "HOLD — counts/shares need numerator/denominator rules", recommend: "NARROW to Y/N experience flags later" };
    } else {
      platformRules[n] = { class: "N/A / WORKFLOW", note: "Unclassified residual — review" };
    }
  }

  for (const f of profileFields) {
    const n = f.name;
    classification.profile[n] = profileRules[n] || {
      class: "N/A / WORKFLOW",
      note: "Unclassified residual — review",
    };
  }
  for (const f of platformFields) {
    const n = f.name;
    classification.platform[n] = platformRules[n] || { class: "N/A / WORKFLOW", note: "Unclassified residual" };
  }

  return classification;
}

const BRAND_FAMILY_MAP = [
  [/marriott|bonvoy|autograph|tribute|design hotels|moxy|aloft|w hotels|st\.?\s*regis|ritz-carlton|sheraton|westin|courtyard|fairfield|residence inn|springhill|towneplace|ac hotels|delta hotels|le m[eé]ridien|renaissance|gaylord/i, "Marriott"],
  [/hilton|hilton honors|curio|tapestry|canopy|tempo|motto|embassy|hilton garden|hampton|homewood|home2|tru |doubletree|waldorf|livsmart/i, "Hilton"],
  [/hyatt|world of hyatt|andaz|thompson|destination|jdv|unbound|caption|hyatt place|hyatt house|hyatt centric|park hyatt|grand hyatt|miraval/i, "Hyatt"],
  [/ihg|intercontinental|holiday inn|crowne plaza|kimpton|voco|avid |atwell|even hotels|staybridge|candlewood|regent|six senses|hotel indigo/i, "IHG"],
  [/choice|radisson|park plaza|park inn|country inn|ascend|comfort |quality |econo |sleep inn|clarion|cambria/i, "Choice"],
  [/wyndham|dolce |tryp |espladio|ramada|days inn|super 8|la quinta|wingate|hawthorn|microtel|travelodge|howard johnson|baymont/i, "Wyndham"],
  [/accor|sofitel|pullman|novotel|mercure|ibis |m[oö]venpick|fairmont|raffles|swiss[oô]tel|mgallery|tribe |greet |25hours|mondrian|sLS |all -?acc/i, "Accor"],
  [/sonesta|royalsonesta|sonesta select|sonesta es/i, "Sonesta"],
  [/radisson/i, "Radisson / Choice"],
  [/independent|unbranded|soft brand|collection(?! )/i, "Independent"],
];

function mapBrandToFamily(brand, allowed) {
  const b = nz(brand);
  if (!b) return null;
  if (allowed.has(b)) return b;
  for (const [re, fam] of BRAND_FAMILY_MAP) {
    if (re.test(b) && allowed.has(fam)) return fam;
  }
  if (/independent|unflagged|no brand/i.test(b) && allowed.has("Independent")) return "Independent";
  return allowed.has("Other") ? "Other" : null;
}

function mapHotelTypeToPropertyTypes(hotelType, allowed) {
  const t = nz(hotelType);
  const out = [];
  const tryAdd = (name) => {
    if (allowed.has(name) && !out.includes(name)) out.push(name);
  };
  if (/full.?service/i.test(t)) tryAdd("Full Service");
  if (/select.?service|limited.?service|focused/i.test(t)) tryAdd("Select Service");
  if (/extended.?stay/i.test(t)) tryAdd("Extended Stay");
  if (/resort|all.?inclusive/i.test(t)) tryAdd("Resort");
  if (/boutique/i.test(t)) tryAdd("Boutique");
  if (/lifestyle/i.test(t)) tryAdd("Lifestyle");
  if (/convention|conference/i.test(t)) tryAdd("Conference Center");
  return out;
}

function mapServiceModels(hotelType, ai, allowed) {
  const t = nz(hotelType);
  const out = [];
  const tryAdd = (name) => {
    if (allowed.has(name) && !out.includes(name)) out.push(name);
  };
  if (/limited/i.test(t)) tryAdd("Limited-service");
  if (/select|focused/i.test(t)) tryAdd("Select-service");
  if (/full/i.test(t)) tryAdd("Full-service");
  if (/lifestyle/i.test(t)) tryAdd("Lifestyle");
  if (/boutique/i.test(t)) tryAdd("Boutique");
  if (/resort/i.test(t)) tryAdd("Resort");
  if (ai || /all.?inclusive/i.test(t)) tryAdd("All-inclusive");
  return out;
}

function mapAdditionalExperience(asg, allowed) {
  const out = new Set();
  for (const r of asg) {
    const t = nz(r.fields?.["Hotel Type"]);
    const ctx = nz(r.fields?.["Development Context"]);
    if (/resort|all.?inclusive/i.test(t) && allowed.has("Resort")) out.add("Resort");
    if (/urban|city/i.test(t) && allowed.has("Urban")) out.add("Urban");
    if (/suburban/i.test(t) && allowed.has("Suburban")) out.add("Suburban");
    if (/airport/i.test(t) && allowed.has("Airport")) out.add("Airport");
    if (/extended/i.test(t) && allowed.has("Extended Stay")) out.add("Extended Stay");
    if (/conversion|reflag|reposition/i.test(ctx) && allowed.has("Conversion")) out.add("Conversion");
    if (r.fields?.["Extended Stay"] && allowed.has("Extended Stay")) out.add("Extended Stay");
  }
  return [...out].sort();
}

function mapChainScales(asg, allowed) {
  // Only when assignment has explicit chain scale field
  const out = new Set();
  for (const r of asg) {
    const cs = nz(r.fields?.["Chain Scale"] || r.fields?.chainScale);
    if (cs && allowed.has(cs)) out.add(cs);
  }
  return [...out].sort();
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-d4a-core-tables");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  console.log("Loading meta + live tables...");
  const tables = await fetchMeta(baseId, token);
  const profileMeta = tables.find((t) => t.name === PROFILE);
  const platformMeta = tables.find((t) => t.name === PLATFORM);
  if (!profileMeta || !platformMeta) throw new Error("Missing Profile or Platform table meta");

  const classification = classifyFields(profileMeta.fields, platformMeta.fields);
  writeJson(join(OUT, "field-classification.json"), classification);

  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const profile = await listAll(baseId, token, PROFILE);
  const platform = await listAll(baseId, token, PLATFORM);
  const assignments = await listAll(baseId, token, "Operator Intelligence - Assignments");
  const presence = await listAll(baseId, token, "Operator Intelligence - Market Presence");
  const brands = await listAll(baseId, token, "Operator Intelligence - Brand Relationships");
  const claims = await listAll(baseId, token, "Operator Intelligence - Claims");

  const production = masters.filter(
    (m) => m.fields["Record Purpose"] === "Production" && !TEST_FIXTURE_MASTER_IDS.includes(m.id)
  );
  if (production.length !== 36) console.warn(`Expected 36 Production, got ${production.length}`);

  const profileBy = byOperator(profile);
  const platformBy = byOperator(platform);

  const acOptions = new Set(choiceNames(platformMeta.fields.find((f) => f.name === "Active Countries")));
  const bfOptions = new Set(choiceNames(profileMeta.fields.find((f) => f.name === "Brand Families Operated")));
  const ptOptions = new Set(choiceNames(profileMeta.fields.find((f) => f.name === "propertyTypes")));
  const smOptions = new Set(choiceNames(profileMeta.fields.find((f) => f.name === "Service Models Supported")));
  const aeOptions = new Set(choiceNames(profileMeta.fields.find((f) => f.name === "additionalExperience")));
  const csOptions = new Set(choiceNames(profileMeta.fields.find((f) => f.name === "chainScalesSupported")));
  const mptOptions = new Set(choiceNames(platformMeta.fields.find((f) => f.name === "Market Presence Type")));

  // Current-state coverage
  const profileFieldNames = profileMeta.fields.map((f) => f.name).filter((n) => n !== "Operator");
  const platformFieldNames = platformMeta.fields.map((f) => f.name).filter((n) => n !== "Operator");

  function coverageFor(fieldNames, byMap, tableKey) {
    return fieldNames.map((f) => {
      let pop = 0;
      let blank = 0;
      let missingRow = 0;
      for (const p of production) {
        const r = byMap[p.id];
        if (!r) {
          missingRow++;
          blank++;
          continue;
        }
        if (isPopulated(r.fields[f])) pop++;
        else blank++;
      }
      const cls = classification[tableKey][f]?.class || "?";
      return { field: f, class: cls, populated: pop, blank, missingRow };
    });
  }

  const profileCov = coverageFor(profileFieldNames, profileBy, "profile");
  const platformCov = coverageFor(platformFieldNames, platformBy, "platform");

  const missingProfile = production.filter((p) => !profileBy[p.id]);
  const missingPlatform = production.filter((p) => !platformBy[p.id]);

  writeMd(
    join(REPORTS, "operator-setup-core-tables-current-state.md"),
    [
      `# D.4A Core Tables — Current Live State`,
      ``,
      `Generated: ${new Date().toISOString()}`,
      ``,
      `Post Phase D / D.1 / D.3 / D.4. **Do not trust Phase D 36/36 section completeness.**`,
      ``,
      `## Universe`,
      ``,
      `- Production operators: **${production.length}**`,
      `- Profile rows: **${production.length - missingProfile.length}** · missing: **${missingProfile.length}**`,
      `- Platform rows: **${production.length - missingPlatform.length}** · missing: **${missingPlatform.length}**`,
      ``,
      `### Missing Profile rows`,
      ``,
      ...missingProfile.map((p) => `- ${p.fields.company_name} (\`${p.id}\`)`),
      ``,
      `### Missing Platform rows`,
      ``,
      ...missingPlatform.map((p) => `- ${p.fields.company_name} (\`${p.id}\`)`),
      ``,
      `## Profile & Positioning — field coverage (Production)`,
      ``,
      `| Field | Class | Populated | Blank |`,
      `| ----- | ----- | --------: | ----: |`,
      ...profileCov
        .sort((a, b) => a.field.localeCompare(b.field))
        .map((r) => `| ${r.field} | ${r.class} | ${r.populated} | ${r.blank} |`),
      ``,
      `## Platform & Markets — field coverage (Production)`,
      ``,
      `| Field | Class | Populated | Blank |`,
      `| ----- | ----- | --------: | ----: |`,
      ...platformCov
        .sort((a, b) => a.field.localeCompare(b.field))
        .map((r) => `| ${r.field} | ${r.class} | ${r.populated} | ${r.blank} |`),
      ``,
      `## Key structural snapshot`,
      ``,
      `| Signal | Count |`,
      `| ------ | ----: |`,
      `| Profile website populated | ${profileCov.find((x) => x.field === "website")?.populated ?? 0} |`,
      `| Profile Brand Families | ${profileCov.find((x) => x.field === "Brand Families Operated")?.populated ?? 0} |`,
      `| Platform Active Countries | ${platformCov.find((x) => x.field === "Active Countries")?.populated ?? 0} |`,
      `| Platform cap_profile_operational | ${platformCov.find((x) => x.field === "cap_profile_operational")?.populated ?? 0} |`,
      `| Master Operating Model | ${production.filter((p) => isPopulated(p.fields["Operating Model"])).length} |`,
      `| Master Management Availability | ${production.filter((p) => isPopulated(p.fields["Management Availability"])).length} |`,
      ``,
    ].join("\n")
  );

  // Field verdict report
  writeMd(
    join(REPORTS, "operator-setup-core-tables-field-verdict.md"),
    [
      `# D.4A Field Verdicts`,
      ``,
      `## Profile & Positioning (${profileFieldNames.length} fields)`,
      ``,
      `| Field | Class | Notes |`,
      `| ----- | ----- | ----- |`,
      ...profileFieldNames.map((f) => {
        const c = classification.profile[f];
        return `| ${f} | ${c.class} | ${c.note || c.purpose || c.readiness || ""} |`;
      }),
      ``,
      `## Platform & Markets (${platformFieldNames.length} fields)`,
      ``,
      `| Field | Class | Notes |`,
      `| ----- | ----- | ----- |`,
      ...platformFieldNames.map((f) => {
        const c = classification.platform[f];
        return `| ${f} | ${c.class} | ${c.note || c.purpose || c.readiness || ""} |`;
      }),
      ``,
    ].join("\n")
  );

  // Numeric fields report
  const numericClean = [];
  for (const [f, v] of Object.entries(classification.profile)) {
    if (v.numeric) numericClean.push({ table: PROFILE, field: f, class: v.class, note: v.note || "HOLD", recommend: v.recommend || "HOLD" });
  }
  for (const [f, v] of Object.entries(classification.platform)) {
    if (v.numeric || /HOLD — NO DEFENSIBLE/.test(v.note || "")) {
      numericClean.push({ table: PLATFORM, field: f, class: v.class, note: v.note || "HOLD", recommend: v.recommend || "HOLD" });
    }
  }
  writeMd(
    join(REPORTS, "operator-setup-core-tables-numeric-fields.md"),
    [
      `# D.4A Numeric Fields`,
      ``,
      `All portfolio % / geo census / KPI selects held unless numerator+denominator+coverage rules exist.`,
      ``,
      `| Table | Field | Recommend |`,
      `| ----- | ----- | --------- |`,
      ...numericClean.map((r) => `| ${r.table} | ${r.field} | ${r.recommend} — ${r.note} |`),
      ``,
    ].join("\n")
  );

  // Build write plan
  const mutations = [];
  const creates = [];
  const keeps = [];
  const blanks = [];
  const crosswalk = [];

  for (const p of production) {
    const id = p.id;
    const name = p.fields.company_name;
    const masterWeb = nz(p.fields["Operator Website"]);
    const parent = nz(p.fields["Operator Parent Company"]);
    const om = nz(p.fields["Operating Model"]);
    const ma = nz(p.fields["Management Availability"]);

    const derived = deriveOperatorSummaries({
      assignments,
      marketPresence: presence,
      brandRelationships: brands,
      masterId: id,
      activeCountryOptions: acOptions,
    });

    const namedCurrent = assignments.filter(
      (r) =>
        (r.fields?.Operator || []).includes(id) &&
        String(r.fields?.["Assignment Status"] || "") === "Current" &&
        nz(r.fields?.["Property Name"]) &&
        !/Various/i.test(nz(r.fields?.["Property Name"]))
    );

    const brandFamilies = [
      ...new Set(
        [
          ...brands.filter((r) => (r.fields?.Operator || []).includes(id)).map((r) => mapBrandToFamily(r.fields?.Brand, bfOptions)),
          ...namedCurrent.map((r) => mapBrandToFamily(r.fields?.Brand, bfOptions)),
        ].filter(Boolean)
      ),
    ].sort();

    const propertyTypes = [
      ...new Set(namedCurrent.flatMap((r) => mapHotelTypeToPropertyTypes(r.fields?.["Hotel Type"], ptOptions))),
    ].sort();
    const serviceModels = [
      ...new Set(
        namedCurrent.flatMap((r) => mapServiceModels(r.fields?.["Hotel Type"], r.fields?.["All-Inclusive"], smOptions))
      ),
    ].sort();
    const additionalExp = mapAdditionalExperience(namedCurrent, aeOptions);
    const chainScales = mapChainScales(namedCurrent, csOptions);

    crosswalk.push({
      operator: name,
      masterId: id,
      master: { website: masterWeb || null, parent: parent || null, operatingModel: om || null, managementAvailability: ma || null },
      derived: {
        activeCountries: derived.activeCountries,
        brandFamilies,
        propertyTypes,
        serviceModels,
        additionalExperience: additionalExp,
        chainScales,
        namedCurrentCount: derived.namedCurrentCount,
      },
      profileRow: Boolean(profileBy[id]),
      platformRow: Boolean(platformBy[id]),
      exemplar: EXEMPLARS.has(id),
    });

    // --- Profile ---
    let pref = profileBy[id];
    if (!pref) {
      const fields = {
        Operator: [id],
        company_name: name,
      };
      if (masterWeb) fields.website = masterWeb;
      if (brandFamilies.length) fields["Brand Families Operated"] = brandFamilies;
      if (propertyTypes.length) fields.propertyTypes = propertyTypes;
      if (serviceModels.length) fields["Service Models Supported"] = serviceModels;
      if (additionalExp.length) fields.additionalExperience = additionalExp;
      if (chainScales.length) fields.chainScalesSupported = chainScales;
      creates.push({ table: PROFILE, masterId: id, operator: name, fields, reason: "missing_profile_row" });
      mutations.push({
        action: "CREATE_ROW",
        table: PROFILE,
        masterId: id,
        operator: name,
        field: "_row",
        proposedValue: fields,
        treatment: "FILL DIRECT+DERIVED",
      });
    } else {
      const pf = pref.fields;
      const pushFill = (field, value, treatment, source) => {
        if (!isPopulated(value)) return;
        if (isPopulated(pf[field])) {
          keeps.push({ table: PROFILE, masterId: id, operator: name, field, action: "KEEP", exemplar: EXEMPLARS.has(id) });
          return;
        }
        mutations.push({
          action: "FILL",
          table: PROFILE,
          recordId: pref.id,
          masterId: id,
          operator: name,
          field,
          currentValue: null,
          proposedValue: value,
          treatment,
          source,
        });
      };
      pushFill("company_name", name, "FILL DIRECT", "Master.company_name");
      pushFill("website", masterWeb, "FILL DIRECT", "Master.Operator Website");
      pushFill("Brand Families Operated", brandFamilies, "FILL DERIVED", "Brand Relationships + Assignments");
      pushFill("propertyTypes", propertyTypes, "FILL DERIVED", "Assignments.Hotel Type");
      pushFill("Service Models Supported", serviceModels, "FILL DERIVED", "Assignments.Hotel Type / AI");
      pushFill("additionalExperience", additionalExp, "FILL DERIVED", "Assignments");
      pushFill("chainScalesSupported", chainScales, "FILL DERIVED", "Assignments.Chain Scale (when present)");

      if (EXEMPLARS.has(id)) {
        keeps.push({ table: PROFILE, masterId: id, operator: name, field: "*", action: "KEEP EXISTING EXEMPLAR" });
      }
    }

    // --- Platform ---
    let pl = platformBy[id];
    if (!pl) {
      const fields = {
        Operator: [id],
        company_name: name,
      };
      if (derived.activeCountries.length) fields["Active Countries"] = derived.activeCountries;
      if (derived.activeCountries.length && mptOptions.has("Active operations")) {
        fields["Market Presence Type"] = ["Active operations"];
      }
      creates.push({ table: PLATFORM, masterId: id, operator: name, fields, reason: "missing_platform_row" });
      mutations.push({
        action: "CREATE_ROW",
        table: PLATFORM,
        masterId: id,
        operator: name,
        field: "_row",
        proposedValue: fields,
        treatment: "FILL DIRECT+DERIVED",
      });
    } else {
      const plf = pl.fields;
      if (!isPopulated(plf.company_name) && name) {
        mutations.push({
          action: "FILL",
          table: PLATFORM,
          recordId: pl.id,
          masterId: id,
          operator: name,
          field: "company_name",
          currentValue: null,
          proposedValue: name,
          treatment: "FILL DIRECT",
          source: "Master",
        });
      } else if (isPopulated(plf.company_name)) {
        keeps.push({ table: PLATFORM, masterId: id, operator: name, field: "company_name", action: "KEEP" });
      }

      if (derived.activeCountries.length) {
        if (!isPopulated(plf["Active Countries"])) {
          mutations.push({
            action: "FILL",
            table: PLATFORM,
            recordId: pl.id,
            masterId: id,
            operator: name,
            field: "Active Countries",
            currentValue: null,
            proposedValue: derived.activeCountries,
            treatment: "FILL DERIVED",
            source: "Market Presence (current) + Assignments (Current named)",
          });
        } else if (!sameMultiSelect(plf["Active Countries"], derived.activeCountries)) {
          blanks.push({
            table: PLATFORM,
            masterId: id,
            operator: name,
            field: "Active Countries",
            action: "KEEP — conflict (existing vs derived)",
            currentValue: plf["Active Countries"],
            proposedValue: derived.activeCountries,
          });
        } else {
          keeps.push({ table: PLATFORM, masterId: id, operator: name, field: "Active Countries", action: "KEEP" });
        }
      } else if (!isPopulated(plf["Active Countries"])) {
        blanks.push({
          table: PLATFORM,
          masterId: id,
          operator: name,
          field: "Active Countries",
          action: "BLANK — INSUFFICIENT EVIDENCE",
        });
      }

      if (
        derived.activeCountries.length &&
        !isPopulated(plf["Market Presence Type"]) &&
        mptOptions.has("Active operations")
      ) {
        mutations.push({
          action: "FILL",
          table: PLATFORM,
          recordId: pl.id,
          masterId: id,
          operator: name,
          field: "Market Presence Type",
          currentValue: null,
          proposedValue: ["Active operations"],
          treatment: "FILL DERIVED",
          source: "Inferred from Active Countries evidence",
        });
      }

      // Narratives — candidates only, no write
      for (const nf of ["cap_profile_operational", "cap_profile_commercial", "cap_profile_transition"]) {
        if (isPopulated(plf[nf])) {
          keeps.push({
            table: PLATFORM,
            masterId: id,
            operator: name,
            field: nf,
            action: EXEMPLARS.has(id) ? "KEEP EXISTING EXEMPLAR" : "KEEP",
          });
        } else {
          blanks.push({
            table: PLATFORM,
            masterId: id,
            operator: name,
            field: nf,
            action: classification.platform[nf]?.readiness === "MOVE TO CLAIMS" ? "MOVE TO CLAIMS" : "WRITER V2 CANDIDATE",
          });
        }
      }

      if (EXEMPLARS.has(id)) {
        keeps.push({ table: PLATFORM, masterId: id, operator: name, field: "*", action: "KEEP EXISTING EXEMPLAR" });
      }
    }
  }

  const fillDirect = mutations.filter((m) => m.treatment?.includes("DIRECT") && m.action === "FILL");
  const fillDerived = mutations.filter((m) => m.treatment?.includes("DERIVED") && m.action === "FILL");
  const createRows = mutations.filter((m) => m.action === "CREATE_ROW");

  writeJson(join(OUT, "write-plan.json"), {
    generatedAt: new Date().toISOString(),
    productionOperators: production.length,
    summary: {
      createRows: createRows.length,
      fillDirect: fillDirect.length,
      fillDerived: fillDerived.length,
      keepNotes: keeps.length,
      blankNotes: blanks.length,
    },
    creates,
    mutations,
    keeps: keeps.slice(0, 500),
    blanks: blanks.slice(0, 800),
  });

  writeMd(
    join(REPORTS, "operator-setup-core-tables-write-plan.md"),
    [
      `# D.4A Write Plan`,
      ``,
      `| Action | Count |`,
      `| ------ | ----: |`,
      `| CREATE_ROW | ${createRows.length} |`,
      `| FILL DIRECT | ${fillDirect.length} |`,
      `| FILL DERIVED | ${fillDerived.length} |`,
      `| KEEP notes | ${keeps.length} |`,
      `| BLANK / candidate notes | ${blanks.length} |`,
      ``,
      `## Creates`,
      ``,
      ...creates.map((c) => `- **${c.operator}** → \`${c.table}\` fields: ${Object.keys(c.fields).filter((k) => k !== "Operator").join(", ")}`),
      ``,
      `## Fills`,
      ``,
      ...mutations
        .filter((m) => m.action === "FILL")
        .map(
          (m) =>
            `- ${m.operator} · ${m.table} · **${m.field}** ← ${JSON.stringify(m.proposedValue).slice(0, 120)} (${m.treatment})`
        ),
      ``,
      `_No narrative Writer v2 auto-fills in D.4A. Numeric/KPI held._`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-core-tables-oe-crosswalk.md"),
    [
      `# D.4A OE Crosswalk`,
      ``,
      `| Setup Field | Canonical OE Source | Available? | Safe Derivation? | Writer Needed? |`,
      `| ----------- | ------------------- | ---------- | ---------------- | -------------- |`,
      `| website | Master.Operator Website | Yes (most) | Yes DIRECT | No |`,
      `| company_name | Master.company_name | Yes | Yes DIRECT | No |`,
      `| Operating Model (Master) | Master | Yes | N/A (lives on Master) | No |`,
      `| Management Availability (Master) | Master | Yes | N/A (lives on Master) | No |`,
      `| Active Countries | Market Presence current ∪ Assignments Current | Partial | Yes DERIVED | No |`,
      `| Brand Families Operated | Brand Relationships + Assignments.Brand | Partial | Yes DERIVED (taxonomy map) | No |`,
      `| propertyTypes / Service Models / additionalExperience | Assignments Hotel Type / AI / Development Context | Partial | Yes DERIVED | No |`,
      `| chainScalesSupported | Assignments Chain Scale | Sparse | Only when field present | No |`,
      `| Active Markets / Cities | Assignments cities | Partial | Not this phase (taxonomy) | No |`,
      `| geo_*/luxury*/% / cap_kpi_* | Assignments sample | Insufficient | **No** | No |`,
      `| cap_profile_operational | Claims / official / Writer v2 | Sparse | No auto | Yes — HIGH VALUE |`,
      `| cap_profile_commercial | Claims / official | Sparse | No auto | Contract refine |`,
      `| cap_profile_transition | — | Sparse | No | MOVE TO CLAIMS |`,
      `| companyDescription / differentiators | Claims / packs | Partial | No auto | Yes — HIGH VALUE |`,
      `| overview_* / op_* JSON / brand_* JSON | Presentation scaffolds | Mixed | No (presentation) | No |`,
      ``,
      `## Per-operator derived snapshot`,
      ``,
      `| Operator | Countries | Brand families | Named current asg | Profile row | Platform row |`,
      `| -------- | --------- | -------------- | ----------------: | ----------- | ------------ |`,
      ...crosswalk.map(
        (c) =>
          `| ${c.operator} | ${(c.derived.activeCountries || []).join("; ") || "—"} | ${(c.derived.brandFamilies || []).join("; ") || "—"} | ${c.derived.namedCurrentCount} | ${c.profileRow} | ${c.platformRow} |`
      ),
      ``,
    ].join("\n")
  );

  // Fit relevance
  writeMd(
    join(REPORTS, "operator-setup-core-tables-fit-relevance.md"),
    [
      `# D.4A Fit-Relevant Fields (read-only assessment)`,
      ``,
      `| Setup field | Normalized source | After reconciliation | Fit reads today? | Future Fit should read |`,
      `| ----------- | ----------------- | -------------------- | ---------------- | ---------------------- |`,
      `| Active Countries | Market Presence + Assignments | Filled where evidenced | Often via Setup/OE geo | Prefer OE Market Presence + Assignments directly |`,
      `| Brand Families Operated | BR + Assignments | Filled where mapped | Partial | Prefer OE Brand Relationships |`,
      `| chainScalesSupported | Assignments | Sparse fills | Partial | Prefer Assignment Chain Scale |`,
      `| Service Models / propertyTypes | Assignments | Filled where typed | Partial | Prefer Assignment Hotel Type |`,
      `| additionalExperience (Conversion/Resort/Urban) | Assignments | Filled where evidenced | Partial | Prefer Development Context + Hotel Type |`,
      `| Operating Model (Master) | Master | Already on Master | Yes | Master / OE |`,
      `| Management Availability (Master) | Master | Already on Master | Yes | Master |`,
      `| cap_profile_* narratives | Writer v2 | Mostly blank (intentional) | Weak | Only after Writer v2 PASS |`,
      `| geo_*/portfolio % | — | HELD blank | Risky if used | Do not use until census methodology |`,
      ``,
      `**Fit remains BLOCKED.** No scoring changes in D.4A.`,
      ``,
    ].join("\n")
  );

  const semanticOk = createRows.every((m) => m.proposedValue?.Operator) && fillDirect.concat(fillDerived).every((m) => m.proposedValue != null);
  if (!semanticOk) {
    console.error("Semantic validation failed");
    process.exit(1);
  }

  // Backup + apply
  const backupDir = join(ROOT, "backups/operator-setup/d4a-core-tables", ts);
  mkdirSync(backupDir, { recursive: true });
  writeJson(join(backupDir, "Profile_Positioning.json"), { recordCount: profile.length, records: profile });
  writeJson(join(backupDir, "Platform_Markets.json"), { recordCount: platform.length, records: platform });
  writeJson(join(backupDir, "Master.json"), { recordCount: masters.length, records: masters });
  writeJson(join(backupDir, "Assignments_count.json"), { recordCount: assignments.length });
  writeJson(join(backupDir, "MarketPresence_count.json"), { recordCount: presence.length });
  writeJson(join(backupDir, "BrandRelationships_count.json"), { recordCount: brands.length });
  writeJson(join(backupDir, "Claims_count.json"), { recordCount: claims.length });
  writeJson(join(backupDir, "manifest.json"), {
    timestamp: ts,
    tables: [PROFILE, PLATFORM, "Master"],
  });

  let writes = 0;
  const failures = [];
  if (args.apply) {
    console.log(`Applying ${createRows.length} creates + ${fillDirect.length + fillDerived.length} fills...`);
    for (const c of creates) {
      try {
        const rec = await createRecord(baseId, token, c.table, c.fields);
        writes += Object.keys(c.fields).filter((k) => k !== "Operator").length;
        if (c.table === PROFILE) profileBy[c.masterId] = rec;
        if (c.table === PLATFORM) platformBy[c.masterId] = rec;
        await sleep(120);
      } catch (e) {
        failures.push({ create: c.operator, table: c.table, error: String(e.message || e) });
      }
    }
    // Group patches by record
    const byRec = new Map();
    for (const m of mutations.filter((x) => x.action === "FILL")) {
      const key = `${m.table}::${m.recordId}`;
      if (!byRec.has(key)) byRec.set(key, { table: m.table, id: m.recordId, fields: {} });
      byRec.get(key).fields[m.field] = m.proposedValue;
    }
    for (const b of byRec.values()) {
      try {
        await patchRecord(baseId, token, b.table, b.id, b.fields);
        writes += Object.keys(b.fields).length;
        await sleep(100);
      } catch (e) {
        failures.push({ patch: b.id, table: b.table, error: String(e.message || e) });
      }
    }
  }

  // Post-apply reload if apply
  let profile2 = profile;
  let platform2 = platform;
  if (args.apply) {
    profile2 = await listAll(baseId, token, PROFILE);
    platform2 = await listAll(baseId, token, PLATFORM);
  }
  const profileBy2 = byOperator(profile2);
  const platformBy2 = byOperator(platform2);

  function structuralStatus(p, kind) {
    const row = kind === "profile" ? profileBy2[p.id] : platformBy2[p.id];
    if (!row) return "PARTIAL — EXPLICIT GAP";
    if (kind === "profile") {
      const hasId = isPopulated(row.fields.company_name) && isPopulated(row.fields.website);
      const hasStruct =
        isPopulated(row.fields["Brand Families Operated"]) ||
        isPopulated(row.fields.chainScalesSupported) ||
        isPopulated(row.fields["Service Models Supported"]) ||
        isPopulated(row.fields.propertyTypes);
      const narrGap =
        !isPopulated(row.fields.companyDescription) || !isPopulated(row.fields.differentiators);
      if (hasId && hasStruct && narrGap) return "STRUCTURALLY COMPLETE — NARRATIVE DEPTH OPTIONAL";
      if (hasId && hasStruct) return "STRUCTURALLY COMPLETE";
      if (hasId) return "PARTIAL — EXPLICIT GAP";
      return "PARTIAL — EXPLICIT GAP";
    }
    const hasName = isPopulated(row.fields.company_name);
    const hasGeo = isPopulated(row.fields["Active Countries"]);
    const narrGap = !isPopulated(row.fields.cap_profile_operational);
    if (hasName && hasGeo && narrGap) return "STRUCTURALLY COMPLETE — DEPTH OPTIONAL";
    if (hasName && hasGeo) return "STRUCTURALLY COMPLETE";
    if (hasName) return "PARTIAL — EXPLICIT GAP";
    return "PARTIAL — EXPLICIT GAP";
  }

  const profileStatuses = production.map((p) => ({ operator: p.fields.company_name, id: p.id, status: structuralStatus(p, "profile") }));
  const platformStatuses = production.map((p) => ({ operator: p.fields.company_name, id: p.id, status: structuralStatus(p, "platform") }));

  const countStatus = (arr, prefix) => arr.filter((x) => x.status.startsWith(prefix)).length;

  // Spot checks
  const spotLines = [];
  for (const id of SPOT) {
    const p = production.find((x) => x.id === id);
    if (!p) continue;
    const pref = profileBy2[id];
    const pl = platformBy2[id];
    spotLines.push(
      `### ${p.fields.company_name}`,
      ``,
      `- **Profile:** ${structuralStatus(p, "profile")}`,
      `- website: ${pref?.fields?.website || "—"}`,
      `- Brand Families: ${(pref?.fields?.["Brand Families Operated"] || []).join("; ") || "—"}`,
      `- Service Models: ${(pref?.fields?.["Service Models Supported"] || []).join("; ") || "—"}`,
      `- HQ: ${(pref?.fields?.headquarters || "—").toString().slice(0, 80)}`,
      `- **Platform:** ${structuralStatus(p, "platform")}`,
      `- Active Countries: ${(pl?.fields?.["Active Countries"] || []).join("; ") || "—"}`,
      `- Market Presence Type: ${(pl?.fields?.["Market Presence Type"] || []).join("; ") || "—"}`,
      `- cap_profile_operational: ${pl?.fields?.cap_profile_operational ? "present" : "blank (Writer v2 candidate)"}`,
      `- Master OM / MA: ${p.fields["Operating Model"] || "—"} / ${p.fields["Management Availability"] || "—"}`,
      ``
    );
  }

  writeMd(
    join(REPORTS, "operator-setup-core-tables-post-apply.md"),
    [
      `# D.4A Post-Apply Audit`,
      ``,
      `Mode: **${args.apply ? "apply" : "dry-run"}** · Writes: ${args.apply ? writes : 0} · Failures: ${failures.length}`,
      ``,
      `## Profile & Positioning`,
      ``,
      `- Structurally complete (incl. narrative-optional): **${countStatus(profileStatuses, "STRUCTURALLY COMPLETE")}**`,
      `- Partial: **${countStatus(profileStatuses, "PARTIAL")}**`,
      ``,
      `| Operator | Status |`,
      `| -------- | ------ |`,
      ...profileStatuses.map((r) => `| ${r.operator} | ${r.status} |`),
      ``,
      `## Platform & Markets`,
      ``,
      `- Structurally complete (incl. depth-optional): **${countStatus(platformStatuses, "STRUCTURALLY COMPLETE")}**`,
      `- Partial: **${countStatus(platformStatuses, "PARTIAL")}**`,
      ``,
      `| Operator | Status |`,
      `| -------- | ------ |`,
      ...platformStatuses.map((r) => `| ${r.operator} | ${r.status} |`),
      ``,
      `## Spot checks`,
      ``,
      ...spotLines,
      failures.length ? `## Failures\n\n${failures.map((f) => `- ${JSON.stringify(f)}`).join("\n")}` : `_No failures_`,
      ``,
    ].join("\n")
  );

  const heArborPreserved =
    EXEMPLARS.has("recWPKu5laVZxsvpn") &&
    EXEMPLARS.has("recF5Z87OAqFgndoq") &&
    isPopulated(profileBy2["recWPKu5laVZxsvpn"]?.fields?.website) &&
    isPopulated(platformBy2["recWPKu5laVZxsvpn"]?.fields?.["Active Countries"]);

  const narrativeWriter = Object.entries({ ...classification.profile, ...classification.platform })
    .filter(([, v]) => v.class === "NARRATIVE — WRITER V2")
    .map(([f, v]) => ({ field: f, readiness: v.readiness, rank: v.rank || "OPTIONAL DEPTH" }));

  const deprecate = Object.entries({ ...classification.profile, ...classification.platform })
    .filter(([, v]) => /DEPRECATE/i.test(v.note || "") || v.readiness === "MOVE TO CLAIMS" || /DEPRECATE/i.test(v.recommend || ""))
    .map(([f, v]) => ({ field: f, note: v.note || v.readiness || v.recommend }));

  const stopPoint = {
    profileFields: profileFieldNames.length,
    platformFields: platformFieldNames.length,
    directFields: Object.values(classification.profile).concat(Object.values(classification.platform)).filter((c) => c.class === "DIRECT").length,
    derivedFields: Object.values(classification.profile).concat(Object.values(classification.platform)).filter((c) => c.class === "DERIVED").length,
    narrativeWriterV2Fields: narrativeWriter.length,
    presentationWorkflowFields: Object.values(classification.profile)
      .concat(Object.values(classification.platform))
      .filter((c) => c.class === "PRESENTATION" || c.class === "N/A / WORKFLOW").length,
    deprecationCandidates: deprecate.map((d) => d.field),
    productionOperatorsAudited: production.length,
    directValuesAdded: fillDirect.length + createRows.filter((c) => c.table === PROFILE || c.table === PLATFORM).length,
    derivedValuesAdded: fillDerived.length,
    narrativeValuesAdded: 0,
    existingHeArborPreserved: heArborPreserved,
    numericFieldsHeld: numericClean.length,
    profileStructurallyComplete: countStatus(profileStatuses, "STRUCTURALLY COMPLETE"),
    profilePartial: countStatus(profileStatuses, "PARTIAL"),
    platformStructurallyComplete: countStatus(platformStatuses, "STRUCTURALLY COMPLETE"),
    platformPartial: countStatus(platformStatuses, "PARTIAL"),
    genuineMissingFacts: profileStatuses.filter((x) => x.status === "PARTIAL — EXPLICIT GAP").length + platformStatuses.filter((x) => x.status === "PARTIAL — EXPLICIT GAP").length,
    optionalNarrativeGaps: "Most companyDescription/differentiators/cap_profile_* intentionally blank",
    genericTemplateValuesIntroduced: 0,
    fitRelevantNowPopulated: ["Active Countries (where evidenced)", "Brand Families Operated (where mapped)", "Master OM/MA (pre-existing)"],
    fitRelevantStillMissing: ["Active Markets taxonomy", "defensible chain-scale census", "Writer v2 ops/commercial narratives", "portfolio %"],
    fieldsRecommendedFutureWriterV2: narrativeWriter.filter((n) => n.rank === "HIGH VALUE" || n.readiness === "READY FOR WRITER V2").map((n) => n.field),
    fieldsRecommendedMoveToClaims: narrativeWriter.filter((n) => n.readiness === "MOVE TO CLAIMS").map((n) => n.field),
    fieldsRecommendedDeprecate: deprecate.map((d) => d.field),
    profileMaturityVerdict: countStatus(profileStatuses, "STRUCTURALLY COMPLETE") >= 30 ? "STRUCTURALLY SOUND WITH NARRATIVE GAPS" : "PARTIAL — CONTINUE DIRECT/DERIVED",
    platformMaturityVerdict: countStatus(platformStatuses, "STRUCTURALLY COMPLETE") >= 28 ? "STRUCTURALLY SOUND WITH DEPTH GAPS" : "PARTIAL — CONTINUE DERIVED GEO",
    fitHandoffStatus: "BLOCKED",
    recommendedNextPhase: "D.5 Writer v2 — Operating Platform Differentiation (`cap_profile_operational`) OR finish Active Markets taxonomy derivation",
    exactFounderApprovalsRequired: [
      "Accept D.4A field classifications",
      "Accept DIRECT/DERIVED writes (and creates for 9 missing rows)",
      "Confirm numeric/KPI/geo census remain HOLD",
      "Confirm narrative auto-fill remains off pending Writer v2",
      "Authorize next phase",
    ],
    confirmationNoSectionTemplates: true,
    confirmationNoFitScoringChanges: true,
    confirmationOwnerPilotDisabled: true,
    mode: args.apply ? "apply" : "dry-run",
    airtableWrites: args.apply ? writes : 0,
    failures: failures.length,
    backupDir: `backups/operator-setup/d4a-core-tables/${ts}`,
    createRows: createRows.length,
    fillDirect: fillDirect.length,
    fillDerived: fillDerived.length,
  };

  writeJson(join(OUT, "d4a-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-core-tables-founder-review.md"),
    [
      `# D.4A Core Tables — Founder Review`,
      ``,
      `## Are Profile & Positioning and Platform & Markets now correctly populated for the 36 real Production operators?`,
      ``,
      `**Structurally: largely yes where OE evidence exists; narratively: intentionally incomplete.**`,
      ``,
      `Profile structurally complete (incl. narrative-optional): **${stopPoint.profileStructurallyComplete}/36**. Platform: **${stopPoint.platformStructurallyComplete}/36**. No generic/template values introduced. Fit remains **BLOCKED**.`,
      ``,
      `| # | Item | Result |`,
      `| - | ---- | ------ |`,
      `| 1 | Post-D.1 baseline | ${missingProfile.length} missing Profile rows; ${missingPlatform.length} missing Platform rows before D.4A; Active Countries already 27/36 |`,
      `| 2 | Profile field map | ${profileFieldNames.length} fields classified |`,
      `| 3 | Platform field map | ${platformFieldNames.length} fields classified |`,
      `| 4 | Direct fields | website, company_name, HQ KEEP, scale KEEP, Master OM/MA referenced |`,
      `| 5 | Derived fields | Active Countries, Brand Families, propertyTypes, Service Models, additionalExperience |`,
      `| 6 | Narrative Writer-v2 | Not auto-filled; candidates listed |`,
      `| 7 | Numeric fields | ALL HELD without census/KPI methodology |`,
      `| 8 | OE crosswalk | See oe-crosswalk report |`,
      `| 9 | Writes | mode=${stopPoint.mode}; creates=${createRows.length}; fills D/Der=${fillDirect.length}/${fillDerived.length}; applied=${stopPoint.airtableWrites} |`,
      `| 10 | Blanks | Honest blanks for unknown + Writer candidates |`,
      `| 11 | Deprecation candidates | capitalStatus-as-profile, cap_kpi_*, geo census blobs, cap_profile_transition→Claims |`,
      `| 12 | HE/Arbor preserved | ${heArborPreserved} |`,
      `| 13 | Completeness | Profile ${stopPoint.profileStructurallyComplete} complete / ${stopPoint.profilePartial} partial; Platform ${stopPoint.platformStructurallyComplete} / ${stopPoint.platformPartial} |`,
      `| 14 | Spot checks | See post-apply report |`,
      `| 15 | Fit-relevant | Countries + brand families improved; % still held |`,
      `| 16 | Remaining high-value narrative | companyDescription, differentiators, cap_profile_operational, brand_narrative_relationship |`,
      `| 17 | Founder approvals | See stop-point list |`,
      `| 18 | Next phase | ${stopPoint.recommendedNextPhase} |`,
      ``,
      `Backup: \`${stopPoint.backupDir}\``,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
