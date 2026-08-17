#!/usr/bin/env node
/**
 * Build Operator Explorer Master Universe reconciliation (Airtable-backed).
 *   node scripts/build-operator-explorer-universe-reconciliation.mjs
 *
 * No Operator Fit changes. No research. No Master graduation/deletion.
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildOperatorUniverse,
  dispositionForOperator,
  RECORD_PURPOSE,
  TEST_FIXTURE_MASTER_IDS,
  normalizeEntityKey,
} from "../lib/operator-explorer/operator-universe.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const CAL = join(ROOT, "data", "operator-explorer", "calibration-01");

function loadJson(p) {
  return JSON.parse(readFileSync(p, "utf8"));
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t, "utf8");
}
function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2), "utf8");
}

async function listAll(baseId, token, table, fields) {
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    if (fields) for (const f of fields) qs.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}?${qs}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`${table}: ${JSON.stringify(json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function metaTables(baseId, token) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.tables || [];
}

const BRAND_BASICS_34 = [
  { parent: "Marriott International, Inc.", om: "Hybrid", ma: "Confirmed Direct Management", deep: true, entity: "Marriott International (Managed)", resolution: "Existing Operator Master" },
  { parent: "Hilton Worldwide", om: "Hybrid", ma: "Confirmed Direct Management", deep: true, entity: "Hilton (Managed)", resolution: "Existing Operator Master" },
  { parent: "AccorHotels", om: "Hybrid", ma: "Confirmed Direct Management", deep: true, entity: "Accor (Managed)", resolution: "Existing Operator Master" },
  { parent: "Hyatt Hotels Corporation", om: "Hybrid", ma: "Confirmed Direct Management", deep: true, entity: "Hyatt (Managed)", resolution: "Newly created Operator Master" },
  { parent: "InterContinental Hotels Group", om: "Hybrid", ma: "Conditional / Scoped", deep: true, entity: "IHG Hotels & Resorts (Managed)", resolution: "Existing Operator Master" },
  { parent: "Minor Hotel Group Limited", om: "Hybrid", ma: "Confirmed Direct Management", deep: true, entity: "Minor Hotels (Managed)", resolution: "Existing Operator Master" },
  { parent: "Sonesta International Hotels Corporation", om: "Brand / Operator", ma: "Confirmed Direct Management", deep: true, entity: "Sonesta International", resolution: "Newly created Operator Master" },
  { parent: "Radisson Hotel Group", om: "Hybrid", ma: "Conditional / Scoped", deep: true, entity: "Radisson Hotel Group", resolution: "Newly created Operator Master" },
  { parent: "Four Seasons Hotels and Resorts", om: "Brand / Operator", ma: "Confirmed Direct Management", deep: true, entity: "Four Seasons Hotels and Resorts", resolution: "Newly created Operator Master" },
  { parent: "Rosewood Hotel Group", om: "Brand / Operator", ma: "Confirmed Direct Management", deep: true, entity: "Rosewood Hotel Group", resolution: "Newly created Operator Master" },
  { parent: "Mandarin Oriental Hotel Group", om: "Integrated Brand / Operator", ma: "Conditional / Scoped", deep: true, entity: "Mandarin Oriental Hotel Group", resolution: "Newly created Operator Master" },
  { parent: "Shangri-La Hotels and Resorts", om: "Integrated Brand / Operator", ma: "Conditional / Scoped", deep: true, entity: "Shangri-La Group", resolution: "Newly created Operator Master" },
  { parent: "Iberostar Hotels & Resorts", om: "Integrated Owner / Brand / Operator", ma: "Conditional / Scoped", deep: true, entity: "Grupo Iberostar", resolution: "Existing Operator Master" },
  { parent: "Banyan Tree Hotels & Resorts", om: "Integrated Brand / Operator", ma: "Conditional / Scoped", deep: false, entity: null, resolution: "No direct-management Operator Master required" },
  { parent: "The Peninsula Hotels", om: "Integrated Brand / Operator", ma: "Conditional / Scoped", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Oetker Hotels", om: "Integrated Brand / Operator", ma: "Conditional / Scoped", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Aman Group", om: "Integrated Brand / Operator", ma: "Conditional / Scoped", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Wyndham Hotels & Resorts", om: "Brand / Operator", ma: "Conditional / Scoped", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Choice Hotels International", om: "Brand / Operator", ma: "Conditional / Scoped", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "BWH Hotels", om: "Brand / Operator", ma: "Conditional / Scoped", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Red Roof Franchise, UK", om: "Brand / Operator", ma: "No Direct Management Identified", deep: false, entity: null, resolution: "No direct-management Operator Master required" },
  { parent: "Preferred Hotels & Resorts", om: "Brand / Operator", ma: "No Direct Management Identified", deep: false, entity: null, resolution: "No direct-management Operator Master required" },
  { parent: "Small Luxury Hotels of the World", om: "Brand / Operator", ma: "No Direct Management Identified", deep: false, entity: null, resolution: "No direct-management Operator Master required" },
  { parent: "Leading Hotels of the World", om: "Brand / Operator", ma: "No Direct Management Identified", deep: false, entity: null, resolution: "No direct-management Operator Master required" },
  { parent: "Hyatt Vacation Ownership", om: "To Be Confirmed", ma: "No Direct Management Identified", deep: false, entity: null, resolution: "No direct-management Operator Master required" },
  { parent: "Dealality Operator Setup placeholder", om: "To Be Confirmed", ma: "No Direct Management Identified", deep: false, entity: null, resolution: "Not applicable" },
  { parent: "Staycity Ltd", om: "Owner-Operator", ma: "Unknown", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Northland Properties", om: "Owner-Operator", ma: "Unknown", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Dovetail + Co", om: "To Be Confirmed", ma: "Unknown", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Prem Group", om: "To Be Confirmed", ma: "Unknown", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "AmeriVu Inn and Suites", om: "To Be Confirmed", ma: "Unknown", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Edyn Limited", om: "To Be Confirmed", ma: "Unknown", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "Coast Hotels Limited", om: "To Be Confirmed", ma: "Unknown", deep: false, entity: null, resolution: "Unknown / research required" },
  { parent: "(no parent)", om: "To Be Confirmed", ma: "Unknown", deep: false, entity: null, resolution: "Not applicable" },
];

const ALIASES = [
  { alias: "MxM", canonical: "Marriott International (Managed)", why: "Managed-by-Marriott program name", search: "alias only", ui: "relationship context", bmc: true, legalLater: "Use Marriott Master as counterparty unless separate MxM legal entity evidenced" },
  { alias: "Managed by Marriott", canonical: "Marriott International (Managed)", why: "Program marketing", search: "alias", ui: "Managed lens", bmc: true, legalLater: "Same as MxM" },
  { alias: "Marriott Managed", canonical: "Marriott International (Managed)", why: "Display lens", search: "alias", ui: "yes via Master", bmc: true, legalLater: "No" },
  { alias: "HMS", canonical: "Hilton (Managed)", why: "Hilton Management Services acronym", search: "alias", ui: "context", bmc: true, legalLater: "Hilton Master unless distinct HMS contracting entity" },
  { alias: "Hilton Management Services", canonical: "Hilton (Managed)", why: "Division/program", search: "alias", ui: "context", bmc: true, legalLater: "Same" },
  { alias: "AccorHotels", canonical: "Accor (Managed)", why: "Legacy parent string in Brand Basics", search: "alias", ui: "Master name Accor (Managed)", bmc: true, legalLater: "No" },
  { alias: "Accor Managed", canonical: "Accor (Managed)", why: "Display lens", search: "alias", ui: "yes", bmc: true, legalLater: "No" },
  { alias: "NH Hotels / NH Collection (as operator)", canonical: "Minor Hotels (Managed)", why: "Brand scope under Minor; not separate Master", search: "brand not Master", ui: "Brand Relationship", bmc: true, legalLater: "Brand-level; counterparty Minor unless separate NH co." },
  { alias: "IHG Managed / IHG management division", canonical: "IHG Hotels & Resorts (Managed)", why: "Division naming", search: "alias", ui: "Master", bmc: true, legalLater: "No" },
  { alias: "Iberostar Managed (twin)", canonical: "Grupo Iberostar", why: "Do not create Managed twin", search: "no", ui: "Grupo Iberostar only", bmc: false, legalLater: "Single Master" },
  { alias: "Auberge / Melia / Barcelo spelling variants", canonical: "Matching Phase-1 Masters", why: "Orthography aliases on Master Aliases field", search: "via aliases", ui: "canonical Master", bmc: "varies", legalLater: "No" },
];

const FIXTURE_NOTES = {
  recTUjuDxL96yWcQA: "Antillano Norte — documented sample/demo in OE baseline rules",
  recBReJUmxdOUvQzp: "Cordillera One Gestión — In Review factory/demo",
  recZPHT2zqc8K6itx: "Viento Sur Gestión Hotelera — demo",
  recZgNR85WZKDItLF: "Mangle Azul Hospitalidad — demo",
  recbT3q8ApRIBu4j5: "Panamerican Lodging Partners — demo",
  reckO98E46sKTn3F3: "Río Plata Hotel Partners — demo",
  recq3NiRxOerg4kZU: "Barrio Hotelero CDMX — demo",
  recwbyY4qfNP1bV3r: "Metro Lodging São Paulo — demo",
  recxAa86Qoc0nFRSt: "Oro Verde Lodge & Hotel Operators — demo",
};

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const entities = loadJson(join(CAL, "entities.json")).entities;
  const cross = existsSync(join(ROOT, "data", "operator-explorer", "phase-1-provisional-crosswalk.json"))
    ? loadJson(join(ROOT, "data", "operator-explorer", "phase-1-provisional-crosswalk.json"))
    : {};

  const masters = await listAll(baseId, token, "Operator Setup - Master", [
    "company_name",
    "Record Purpose",
    "submission_status",
    "Operating Model",
    "Management Availability",
    "Operator Aliases",
    "Operator Website",
    "Operator Parent Company",
  ]);
  const assignments = await listAll(baseId, token, "Operator Intelligence - Assignments", [
    "Assignment ID",
    "Operator",
    "Property Name",
    "Country",
    "Brand",
  ]);
  const brandRelationships = await listAll(baseId, token, "Operator Intelligence - Brand Relationships", [
    "Brand Relationship ID",
    "Operator",
    "Brand",
    "Relationship Type",
  ]);
  const marketPresence = await listAll(baseId, token, "Operator Intelligence - Market Presence", [
    "Presence Key",
    "Operator",
    "Country",
    "Market Presence Type",
  ]);

  const tables = await metaTables(baseId, token);
  const masterTable = tables.find((t) => t.name === "Operator Setup - Master");
  const views = masterTable?.views || [];

  const calibrationByMasterId = {};
  for (const e of entities) {
    const mid = cross[e.entityId] || e.existingMasterId || e.entityId;
    calibrationByMasterId[mid] = {
      track: e.track,
      provisionalId: e.provisionalEntityId,
      canonicalName: e.canonicalName,
      wasProvisional: Boolean(e.provisionalEntityId),
    };
  }

  const universe = buildOperatorUniverse(masters, {
    assignments,
    brandRelationships,
    marketPresence,
    calibrationByMasterId,
    aliases: ALIASES,
  });

  // Enrich dispositions + brand basics flags
  const byName = Object.fromEntries(
    universe.operators.map((o) => [normalizeEntityKey(o.canonicalName), o])
  );
  function findMaster(name) {
    if (!name) return null;
    const k = normalizeEntityKey(name);
    if (byName[k]) return byName[k];
    return (
      universe.operators.find(
        (o) =>
          normalizeEntityKey(o.canonicalName).includes(k) ||
          k.includes(normalizeEntityKey(o.canonicalName)) ||
          normalizeEntityKey(o.aliases || "").includes(k)
      ) || null
    );
  }

  for (const o of universe.operators) {
    o.disposition = dispositionForOperator(o);
    o.brandBasicsParent = false;
    o.notes = [];
    if (o.testFixture) o.notes.push("Excluded from Explorer/Fit/research production universes");
    if (o.contentCompleteButLifecycleGated) o.notes.push("Content complete; Record Purpose Research gates publishable");
    if (o.calibration01) o.notes.push(`Calibration 01 Track ${o.calibrationTrack}`);
  }

  const brandBasicsResolved = BRAND_BASICS_34.map((b) => {
    const hit = findMaster(b.entity) || (b.entity ? findMaster(b.parent) : null);
    let resolution = b.resolution;
    let masterId = hit?.masterId || null;
    if (b.entity && hit) {
      if (hit.calibration01 && hit.calibrationTrack === 2 && !hit.canonicalName.includes("Iberostar")) {
        // already set
      }
      if (/Newly created/.test(b.resolution) && hit) resolution = "Newly created Operator Master";
      if (/Existing/.test(b.resolution) && hit) resolution = "Existing Operator Master";
    }
    // AccorHotels parent string → Accor Master = alias resolution of parent naming
    if (b.parent === "AccorHotels" && hit) {
      resolution = "Existing Operator Master";
    }
    if (b.parent === "Minor Hotel Group Limited") {
      // NH is alias — parent maps to Minor Master
    }
    return {
      ...b,
      masterId,
      masterName: hit?.canonicalName || null,
      resolution,
      explorerCandidate: Boolean(hit && hit.recordPurpose !== "Test Fixture"),
    };
  });

  // Mark brand basics membership on operators
  for (const b of brandBasicsResolved) {
    if (b.masterId) {
      const o = universe.operators.find((x) => x.masterId === b.masterId);
      if (o) {
        o.brandBasicsParent = true;
        o.brandBasicsParentName = b.parent;
      }
    }
  }

  // Canonical dataset
  const canonical = {
    generatedAt: universe.generatedAt,
    baseId,
    summary: universe.summary,
    definitionsRef: "docs/product/operator-explorer-universe-definitions.md",
    operators: universe.operators.map((o) => ({
      masterId: o.masterId,
      canonicalName: o.canonicalName,
      parent: o.parent,
      aliases: o.aliases,
      website: o.website,
      recordPurpose: o.recordPurpose,
      lifecycle: o.lifecycle,
      operatingModel: o.operatingModel,
      managementAvailability: o.managementAvailability,
      calibration01: o.calibration01,
      calibrationTrack: o.calibrationTrack,
      brandBasicsParent: o.brandBasicsParent,
      brandManagedDiscovery: o.brandManagedDiscovery,
      explorerResearchState: o.explorerResearchState,
      explorerContentReadiness: o.explorerContentReadiness,
      explorerContentComplete: o.explorerContentComplete,
      contentCompleteButLifecycleGated: o.contentCompleteButLifecycleGated,
      canonicalExplorerEligibility: o.explorerPublishable ? "Eligible now" : o.contentCompleteButLifecycleGated ? "Eligible after Production graduation" : o.testFixture ? "Never" : "Needs research",
      explorerPublishable: o.explorerPublishable,
      strongExplorerProfile: o.strongExplorerProfile,
      fitDataReadiness: o.fitDataReadiness,
      fitProductionEligibility: o.fitProductionEligible ? "Eligible (reported; Fit not rewired)" : "Not eligible",
      testFixture: o.testFixture,
      ownerVisibleNow: false,
      disposition: o.disposition,
      counts: o.counts,
      reasonNotes: (o.notes || []).join("; "),
    })),
    aliases: ALIASES,
    brandBasicsParents: brandBasicsResolved,
    sets: universe.sets,
  };
  writeJson(join(ROOT, "data", "operator-explorer", "operator-universe-canonical.json"), canonical);
  writeJson(join(ROOT, "data", "operator-explorer", "operator-universe-dashboard.json"), {
    generatedAt: universe.generatedAt,
    summary: universe.summary,
    operators: canonical.operators,
  });

  // Reports
  let m46 = `# Operator Explorer — Master Universe (46)\n\n**Total:** ${masters.length}\n\n| Master ID | Canonical Name | Record Purpose | Lifecycle | Calibration? | Brand Basics? | Explorer Status | Fit Status | Disposition |\n| --------- | -------------- | -------------- | --------- | ------------ | ------------- | --------------- | ---------- | ----------- |\n`;
  for (const o of universe.operators.sort((a, b) => String(a.canonicalName).localeCompare(String(b.canonicalName)))) {
    m46 += `| \`${o.masterId}\` | ${o.canonicalName} | ${o.recordPurpose} | ${o.lifecycle} | ${o.calibration01 ? `T${o.calibrationTrack}` : "no"} | ${o.brandBasicsParent ? "yes" : "no"} | ${o.usefulness}${o.explorerPublishable ? " · Pub" : ""} | ${o.fitDataReadiness} | ${o.disposition} |\n`;
  }
  writeMd(join(ROOT, "reports", "operator-explorer-master-universe-46.md"), m46);

  let c27 = `# Calibration-27 Reconciliation\n\n| Canonical Name | Track | Final Master ID | Was provisional? | Record Purpose | Content | Publishable | Fit diag | Gap |\n| -------------- | ----: | --------------- | ---------------- | -------------- | ------- | ----------- | -------- | --- |\n`;
  let unresolved = [];
  for (const e of entities) {
    const mid = cross[e.entityId] || e.existingMasterId || e.entityId;
    const o = universe.operators.find((x) => x.masterId === mid);
    if (!o) unresolved.push(e.canonicalName);
    const gap = !o
      ? "MISSING MASTER"
      : o.explorerPublishable
        ? "—"
        : o.contentCompleteButLifecycleGated
          ? "Lifecycle gate (Research)"
          : "Needs enrichment / named depth";
    c27 += `| ${e.canonicalName} | ${e.track} | \`${mid}\` | ${e.provisionalEntityId ? "yes" : "no"} | ${o?.recordPurpose || "?"} | ${o?.explorerContentReadiness || "?"} | ${o?.explorerPublishable} | ${o?.fitDataReadiness || "?"} | ${gap} |\n`;
  }
  c27 += `\n## Resolve check\n\nAll 27 resolve to Airtable Masters: **${unresolved.length === 0 ? "YES" : "NO — " + unresolved.join(", ")}**\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-calibration-27-reconciliation.md"), c27);

  let bb = `# Brand Basics-34 Reconciliation\n\n| Brand Parent | Operator Master Exists? | Master ID | Alias/Parent Resolution | OM | MA | Deep Calibration? | Explorer Candidate? |\n| ------------ | ----------------------- | --------- | ----------------------- | -- | -- | ----------------- | ------------------- |\n`;
  for (const b of brandBasicsResolved) {
    bb += `| ${b.parent} | ${b.masterId ? "yes" : "no"} | ${b.masterId ? "`" + b.masterId + "`" : "—"} | ${b.resolution} | ${b.om} | ${b.ma} | ${b.deep} | ${b.explorerCandidate} |\n`;
  }
  const bbCounts = {
    withMaster: brandBasicsResolved.filter((b) => b.masterId).length,
    alias: brandBasicsResolved.filter((b) => /Alias/.test(b.resolution)).length,
    noRequired: brandBasicsResolved.filter((b) => /No direct-management|Not applicable/.test(b.resolution)).length,
    unknown: brandBasicsResolved.filter((b) => /Unknown/.test(b.resolution)).length,
  };
  // AccorHotels etc counted as existing master not alias row — note NH under Minor
  bb += `\n## Counts\n\n- With Operator Master: ${bbCounts.withMaster}\n- No Master required / N/A: ${bbCounts.noRequired}\n- Unknown / research required: ${bbCounts.unknown}\n- Explicit alias resolutions documented separately (MxM/HMS/NH)\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-brand-basics-34-reconciliation.md"), bb);

  writeMd(
    join(ROOT, "reports", "operator-explorer-management-alias-map.md"),
    `# Management Alias Map\n\n| Alias | Canonical Operator Master | Why alias | Search | UI | BMC | Separate legal later? |\n| ----- | ------------------------- | --------- | ------ | -- | --- | --------------------- |\n` +
      ALIASES.map(
        (a) =>
          `| ${a.alias} | ${a.canonical} | ${a.why} | ${a.search} | ${a.ui} | ${a.bmc} | ${a.legalLater} |`
      ).join("\n") +
      `\n\n**No duplicate Masters** for these aliases.\n`
  );

  let tf = `# Test Fixture Universe\n\n| Master ID | Name | Purpose | Explorer excl | Fit excl | Research excl | Owner excl | Migrate to code later? |\n| --------- | ---- | ------- | ------------- | -------- | ------------- | ---------- | ---------------------- |\n`;
  for (const id of TEST_FIXTURE_MASTER_IDS) {
    const o = universe.operators.find((x) => x.masterId === id);
    tf += `| \`${id}\` | ${o?.canonicalName || "?"} | ${FIXTURE_NOTES[id] || "Beta/dummy In Review"} | yes | yes | yes | yes | Yes — when code fixtures replace demos |\n`;
  }
  tf += `\nCentrally excluded via \`Record Purpose = Test Fixture\` + \`lib/operator-explorer/phase-1-universe.js\` / \`operator-universe.js\`.\n**Do not delete.**\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-test-fixture-universe.md"), tf);

  let research = `# Research Universe (Record Purpose = Research)\n\n`;
  const researchRows = universe.operators.filter((o) => o.recordPurpose === RECORD_PURPOSE.RESEARCH);
  research += `**Count:** ${researchRows.length}\n\n`;
  research += `## Content complete but intentionally gated\n\n`;
  for (const o of researchRows.filter((r) => r.contentCompleteButLifecycleGated || r.explorerContentComplete)) {
    research += `- **${o.canonicalName}** (\`${o.masterId}\`) — content ${o.explorerContentReadiness}; gated by Research purpose. Graduation condition: founder Production approval after identity/OM/MA/conflict review.\n`;
  }
  research += `\n## Genuinely incomplete research\n\n`;
  for (const o of researchRows.filter((r) => !r.explorerContentComplete)) {
    research += `- **${o.canonicalName}** (\`${o.masterId}\`) — ${o.explorerContentReadiness}; asg=${o.counts.namedAssignments}, countries=${o.counts.countries}, BMC=${o.counts.hasBmc}. Needs named assignments / presence / BMC evidence.\n`;
  }
  writeMd(join(ROOT, "reports", "operator-explorer-research-universe.md"), research);

  let prod = `# Production Universe\n\n**A Production record is not automatically Explorer Publishable.**\n\n| Operator | Content | Publishable | Strong | Fit diag | Gaps |\n| -------- | ------- | ----------- | ------ | -------- | ---- |\n`;
  for (const o of universe.operators.filter((x) => x.recordPurpose === RECORD_PURPOSE.PRODUCTION)) {
    const gaps = [];
    if (o.counts.namedAssignments < 2) gaps.push("named asg");
    if (o.counts.countries < 1) gaps.push("geo");
    if (!o.explorerPublishable) gaps.push("not publishable");
    prod += `| ${o.canonicalName} | ${o.explorerContentReadiness} | ${o.explorerPublishable} | ${o.strongExplorerProfile} | ${o.fitDataReadiness} | ${gaps.join(", ") || "—"} |\n`;
  }
  writeMd(join(ROOT, "reports", "operator-explorer-production-universe.md"), prod);

  // Crosswalk
  const sections = {
    "Production — Publishable": universe.operators.filter((o) => o.recordPurpose === "Production" && o.explorerPublishable),
    "Production — Needs Enrichment": universe.operators.filter((o) => o.recordPurpose === "Production" && !o.explorerPublishable),
    "Research — Content Complete": universe.operators.filter(
      (o) => o.recordPurpose === "Research" && o.explorerContentComplete
    ),
    "Research — Needs Enrichment": universe.operators.filter(
      (o) => o.recordPurpose === "Research" && !o.explorerContentComplete
    ),
    "Test Fixtures": universe.operators.filter((o) => o.testFixture),
  };
  let xw = `# Operator Explorer — Universe Crosswalk\n\n`;
  for (const [title, list] of Object.entries(sections)) {
    xw += `## ${title}\n\n| Operator | Master ID | Purpose | Calibration | Brand Basics | OM | MA | Explorer Content | Publishable | Fit Ready | Fit Production | Notes |\n| -------- | --------- | ------- | ----------- | ------------ | -- | -- | ---------------- | ----------- | --------- | -------------- | ----- |\n`;
    for (const o of list.sort((a, b) => String(a.canonicalName).localeCompare(String(b.canonicalName)))) {
      xw += `| ${o.canonicalName} | \`${o.masterId}\` | ${o.recordPurpose} | ${o.calibration01 ? "Y" : ""} | ${o.brandBasicsParent ? "Y" : ""} | ${o.operatingModel || ""} | ${o.managementAvailability || ""} | ${o.explorerContentReadiness} | ${o.explorerPublishable} | ${o.fitDataReadiness} | ${o.fitProductionEligible} | ${(o.notes || []).join("; ")} |\n`;
    }
    xw += `\n`;
  }
  xw += `## Brand Parents Without Operator Master\n\n`;
  for (const b of brandBasicsResolved.filter((x) => !x.masterId)) {
    xw += `- ${b.parent} — ${b.resolution}\n`;
  }
  xw += `\n## Aliases\n\nSee \`reports/operator-explorer-management-alias-map.md\`.\n`;
  writeMd(join(ROOT, "reports", "operator-explorer-universe-crosswalk.md"), xw);

  writeMd(
    join(ROOT, "docs", "product", "operator-explorer-universe-definitions.md"),
    `# Operator Explorer — Universe Definitions\n\n| Concept | Definition |\n| ------- | ---------- |\n| **Operator Master Universe** | All canonical rows in \`Operator Setup - Master\` (currently 46). |\n| **Real Operator Universe** | Record Purpose = Production **or** Research (excludes Test Fixtures). |\n| **Production Operator Universe** | Record Purpose = Production. |\n| **Research Operator Universe** | Record Purpose = Research. |\n| **Calibration Universe** | The exact 27 entities used in Calibration 01 architecture validation. |\n| **Brand-Managed Discovery Universe** | The 34 Brand Basics parents evaluated for management capability (not all are Masters). |\n| **Explorer Content-Complete Universe** | Operators whose **content** meets Useful/Strong gates regardless of Record Purpose. |\n| **Explorer Publishable Universe** | Production + content-complete (canonical readiness). Research never publishable until graduated. |\n| **Fit Data Ready Universe** | Diagnostic: enough structured intel for Fit evaluation discussion. |\n| **Fit Production Candidate Universe** | Operators allowed into production Fit candidate selection (report: Production only; Fit engine not rewired here). |\n| **Test Fixture Universe** | Synthetic Masters for testing only. |\n\nThese sets **overlap** and are **not** expected to have equal counts.\n`
  );

  writeMd(
    join(ROOT, "docs", "product", "operator-explorer-why-lists-differ.md"),
    `# Why Operator Lists Differ\n\n## Exact current counts (Airtable-backed reconciliation)\n\n| List | Count | What it is |\n| ---- | ----: | ---------- |\n| Airtable Operator Masters | **${universe.summary.totalMasters}** | Every Master row |\n| Production | **${universe.summary.production}** | Record Purpose Production |\n| Research | **${universe.summary.research}** | Record Purpose Research |\n| Test Fixtures | **${universe.summary.testFixtures}** | Synthetic |\n| Real operators | **${universe.summary.realOperators}** | Production + Research |\n| Calibration 01 | **27** | Architecture test cohort |\n| Brand Basics parents | **34** | Brand-parent discovery set |\n| Explorer content-complete | **${universe.summary.explorerContentComplete}** | Content gates pass |\n| Explorer Publishable (canonical) | **${universe.summary.explorerPublishable}** | Production ∩ content-complete |\n| Fit Data Ready (diagnostic) | **${universe.summary.fitDataReady}** | Diagnostic only |\n\n## Simple examples\n\n- **MxM** is not a Master — it is an alias of Marriott International (Managed).\n- A **Test Fixture** exists in Airtable (46) but is not in the real universe (37).\n- A **Brand Basics parent** like Preferred Hotels may have **No Direct Management** — no Operator Master required.\n- A **Research** operator (e.g. Four Seasons) may be **content-complete** but still **not Explorer Publishable** until Production graduation.\n- **Calibration 27** is a research sample, not the whole Master universe (46).\n`
  );

  writeMd(
    join(ROOT, "reports", "operator-explorer-airtable-view-audit.md"),
    `# Airtable View Audit — Operator Setup - Master\n\n| View name | Table | Filter logic | Record count | Universe represented | Misleading? |\n| --------- | ----- | ------------ | -----------: | -------------------- | ----------- |\n` +
      (views.length
        ? views
            .map(
              (v) =>
                `| ${v.name} | Operator Setup - Master | ${v.type || "grid"} (API does not expose filter formula for all view types) | ${masters.length} (unfiltered Grid) | Full Master table when no filter | ${v.name === "Grid view" ? "Neutral default — not an OE universe" : "Review name"} |`
            )
            .join("\n")
        : "| (none found) | | | | | |") +
      `\n\n## Finding\n\nLive meta API currently exposes only **Grid view** on \`Operator Setup - Master\`. There is **no** dedicated OE Production / Research / Publishable view yet — founder confusion is expected if comparing Calibration-27 docs, Brand Basics-34 sheets, and the unfiltered 46-row Grid.\n\nIf a Webflow/internal page titled “Operating Companies” shows a subset, it is almost certainly **application filters**, not Record Purpose views (those views do not exist yet).\n`
  );

  writeMd(
    join(ROOT, "docs", "data", "operator-explorer-airtable-view-spec.md"),
    `# Recommended Airtable Views — Operator Setup - Master\n\nMinimum set (do not create redundant views):\n\n| View | Filter |\n| ---- | ------ |\n| **OE — All Operator Masters** | (none) |\n| **OE — Production** | \`{Record Purpose} = "Production"\` |\n| **OE — Research** | \`{Record Purpose} = "Research"\` |\n| **OE — Test Fixtures** | \`{Record Purpose} = "Test Fixture"\` |\n| **OE — Explorer Publishable** | Production + manual or synced publishable flag *(until formula/sync exists, use internal dashboard)* |\n| **OE — Needs Enrichment** | Real operators not publishable *(approx: Production OR Research, exclude fixtures)* |\n| **OE — Fit Data Ready** | Internal diagnostic only — optional |\n\n**Do not rename** existing Grid view without founder approval. Add OE-prefixed views alongside.\n`
  );

  writeMd(
    join(ROOT, "reports", "operator-explorer-universe-resolver-audit.md"),
    `# Universe Resolver Audit\n\n## Duplicate logic found\n\n| Location | What it decides | Risk |\n| -------- | --------------- | ---- |\n| \`lib/operator-explorer/phase-1-universe.js\` | Test Fixture IDs + Record Purpose map | Keep as SoT for fixtures |\n| \`scripts/operator-explorer-phase-1-apply.mjs\` | Ad-hoc readiness thresholds (superseded) | **Was** inconsistent with dry-run |\n| \`scripts/build-operator-explorer-calibration-01.mjs\` \`buildProfile\` | Dry-run readiness | Should call shared module |\n| OE protected baseline / factory queues | Quality baselines, not universe Purpose | Separate concern |\n| Operator Fit shortlist / candidate code | Fit candidates | **Do not rewire in this phase** — report only |\n\n## Recommendation\n\n**One shared module:** \`lib/operator-explorer/operator-universe.js\` + \`lib/operator-explorer/readiness.js\`.\n\nStatus: **created**. Fit production path **not** rewired.\n`
  );

  writeMd(
    join(ROOT, "reports", "operator-explorer-canonical-readiness-results.md"),
    `# Canonical Readiness Results (Airtable-backed)\n\nGenerated via \`lib/operator-explorer/readiness.js\` + universe builder.\n\n| Class | Count |\n| ----- | ----: |\n| Strong Profile | ${universe.summary.strongProfiles} |\n| Explorer Publishable | ${universe.summary.explorerPublishable} |\n| Content Complete (any Purpose) | ${universe.summary.explorerContentComplete} |\n| Content complete but lifecycle gated (Research) | ${universe.summary.contentCompleteButLifecycleGated} |\n| Fit Data Ready (diag) | ${universe.summary.fitDataReady} |\n| Fit Conditional | ${universe.summary.fitConditional} |\n| Fit Research Required | ${universe.summary.fitResearchRequired} |\n\n## Usefulness breakdown (canonical, Purpose-aware)\n\n| Usefulness | Count |\n| ---------- | ----: |\n| Strong Profile | ${universe.operators.filter((o) => o.usefulness === "Strong Profile").length} |\n| Useful Profile | ${universe.operators.filter((o) => o.usefulness === "Useful Profile").length} |\n| Thin Profile | ${universe.operators.filter((o) => o.usefulness === "Thin Profile").length} |\n| Not Publishable | ${universe.operators.filter((o) => o.usefulness === "Not Publishable").length} |\n\nPhase 1 \`asg≥5/8\` mismatch eliminated for this reconciliation path.\n`
  );

  // Internal HTML
  const dash = loadJson(join(ROOT, "data", "operator-explorer", "operator-universe-dashboard.json"));
  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Operator Explorer — Universe (Internal)</title>
  <style>
    :root { --ink:#1a1a1a; --muted:#5c5c5c; --line:#d9d4cb; --bg:#f7f4ef; --card:#fff; --accent:#2f5d50; }
    body { margin:0; font-family:"Segoe UI", system-ui, sans-serif; background:var(--bg); color:var(--ink); }
    header { padding:1.5rem 1.75rem; border-bottom:1px solid var(--line); background:linear-gradient(180deg,#fff, #f3efe6); }
    h1 { margin:0 0 .35rem; font-size:1.45rem; letter-spacing:-0.02em; }
    .sub { color:var(--muted); font-size:.92rem; }
    .metrics { display:flex; flex-wrap:wrap; gap:.6rem; padding:1rem 1.75rem; }
    .metric { background:var(--card); border:1px solid var(--line); padding:.65rem .9rem; min-width:7rem; }
    .metric b { display:block; font-size:1.25rem; color:var(--accent); }
    .controls { padding:0 1.75rem 1rem; display:flex; flex-wrap:wrap; gap:.5rem; align-items:center; }
    input, select { padding:.45rem .6rem; border:1px solid var(--line); background:#fff; }
    table { width:calc(100% - 3.5rem); margin:0 1.75rem 2rem; border-collapse:collapse; background:var(--card); font-size:.84rem; }
    th, td { border-bottom:1px solid var(--line); padding:.45rem .5rem; text-align:left; vertical-align:top; }
    th { position:sticky; top:0; background:#efebe3; }
    .tag { display:inline-block; padding:.1rem .35rem; border:1px solid var(--line); font-size:.72rem; }
    .warn { color:#8a4b1f; }
  </style>
</head>
<body>
  <header>
    <h1>Operator Explorer — Master Universe</h1>
    <div class="sub">Internal reconciliation only · Not owner-facing · Generated ${dash.generatedAt}</div>
  </header>
  <div class="metrics" id="metrics"></div>
  <div class="controls">
    <input id="q" placeholder="Search name / id" />
    <select id="purpose"><option value="">All purposes</option><option>Production</option><option>Research</option><option>Test Fixture</option></select>
    <select id="pub"><option value="">Publishable: any</option><option value="yes">Publishable only</option><option value="no">Not publishable</option></select>
    <select id="cal"><option value="">Calibration: any</option><option value="yes">Calibration yes</option><option value="no">Not calibration</option></select>
    <select id="bm"><option value="">Brand-managed: any</option><option value="yes">Yes</option><option value="no">No</option></select>
    <label><input type="checkbox" id="need" /> Needs research</label>
  </div>
  <table>
    <thead>
      <tr>
        <th>Canonical Name</th><th>Master ID</th><th>Purpose</th><th>Lifecycle</th><th>OM</th><th>MA</th>
        <th>Explorer</th><th>Fit</th><th>Calib</th><th>BB</th><th>Gaps</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
  <script>
    const DATA = ${JSON.stringify(dash)};
    const metrics = document.getElementById('metrics');
    const s = DATA.summary;
    const metricPairs = [
      ['Masters', s.totalMasters],['Production', s.production],['Research', s.research],['Fixtures', s.testFixtures],
      ['Publishable', s.explorerPublishable],['Content complete', s.explorerContentComplete],['Strong', s.strongProfiles],
      ['Fit Ready', s.fitDataReady],['Gated Research', s.contentCompleteButLifecycleGated]
    ];
    metrics.innerHTML = metricPairs.map(([k,v]) => '<div class="metric"><b>'+v+'</b>'+k+'</div>').join('');
    function render() {
      const q = document.getElementById('q').value.toLowerCase();
      const purpose = document.getElementById('purpose').value;
      const pub = document.getElementById('pub').value;
      const cal = document.getElementById('cal').value;
      const bm = document.getElementById('bm').value;
      const need = document.getElementById('need').checked;
      const rows = DATA.operators.filter(o => {
        if (q && !(String(o.canonicalName||'').toLowerCase().includes(q) || String(o.masterId).toLowerCase().includes(q))) return false;
        if (purpose && o.recordPurpose !== purpose) return false;
        if (pub === 'yes' && !o.explorerPublishable) return false;
        if (pub === 'no' && o.explorerPublishable) return false;
        if (cal === 'yes' && !o.calibration01) return false;
        if (cal === 'no' && o.calibration01) return false;
        if (bm === 'yes' && !o.brandManagedDiscovery) return false;
        if (bm === 'no' && o.brandManagedDiscovery) return false;
        if (need && (o.explorerPublishable || o.testFixture)) return false;
        return true;
      });
      document.getElementById('tbody').innerHTML = rows.map(o => {
        const gaps = [];
        if (o.contentCompleteButLifecycleGated) gaps.push('lifecycle gate');
        if (!o.explorerContentComplete && !o.testFixture) gaps.push('enrich');
        return '<tr><td>'+o.canonicalName+'</td><td><code>'+o.masterId+'</code></td><td><span class="tag">'+o.recordPurpose+'</span></td><td>'+(o.lifecycle||'')+'</td><td>'+(o.operatingModel||'')+'</td><td>'+(o.managementAvailability||'')+'</td><td>'+o.explorerContentReadiness+(o.explorerPublishable?' · <b>Pub</b>':'')+'</td><td>'+o.fitDataReadiness+'</td><td>'+(o.calibration01?('T'+o.calibrationTrack):'')+'</td><td>'+(o.brandBasicsParent?'Y':'')+'</td><td class="warn">'+gaps.join(', ')+'</td></tr>';
      }).join('');
    }
    ['q','purpose','pub','cal','bm','need'].forEach(id => document.getElementById(id).addEventListener('input', render));
    render();
  </script>
</body>
</html>`;
  writeFileSync(join(ROOT, "public", "internal", "operator-explorer-universe.html"), html, "utf8");

  // Founder review
  const pubProd = universe.operators.filter((o) => o.recordPurpose === "Production" && o.explorerPublishable).length;
  const prodNeed = universe.operators.filter((o) => o.recordPurpose === "Production" && !o.explorerPublishable).length;
  const gated = universe.summary.contentCompleteButLifecycleGated;
  const withMaster = brandBasicsResolved.filter((b) => b.masterId).length;
  const noReq = brandBasicsResolved.filter((b) => /No direct-management|Not applicable/.test(b.resolution)).length;
  const unknown = brandBasicsResolved.filter((b) => /Unknown/.test(b.resolution)).length;

  writeMd(
    join(ROOT, "docs", "reviews", "operator-explorer-master-universe-founder-review.md"),
    `# Operator Explorer — Master Universe Founder Review\n\n**Generated:** ${universe.generatedAt}\n**Fit/scoring unchanged · Owner pilot disabled · No broad research**\n\n## Which list should I look at?\n\n| If you want… | Look here |\n| ------------ | --------- |\n| Every Operator Master | Airtable \`Operator Setup - Master\` (46) / OE — All / \`operator-universe-canonical.json\` |\n| Every **real** operator | Production + Research (37) — exclude Test Fixtures |\n| Operators **approved for Explorer** (publishable) | Canonical Explorer Publishable (**${universe.summary.explorerPublishable}**) |\n| Operators still being researched | Record Purpose = Research (13) + Production needing enrichment |\n| Operators Fit can evaluate (diagnostic) | Fit Data Ready (**${universe.summary.fitDataReady}**) — Fit engine not rewired |\n| Test/dummy records | Record Purpose = Test Fixture (9) |\n| Internal admin reconciliation | \`/internal/operator-explorer-universe.html\` |\n\n## 1. Why lists looked inconsistent\n\nDifferent lists answer different questions (46 Masters ≠ 27 calibration ≠ 34 Brand Basics parents). Phase 1 also used a stricter readiness classifier than dry-run. Only **Grid view** exists on Master — no OE Purpose views yet.\n\n## 2–5. Current 46-Master universe\n\n- Total Masters: **${universe.summary.totalMasters}**\n- Production: **${universe.summary.production}**\n- Research: **${universe.summary.research}**\n- Test Fixtures: **${universe.summary.testFixtures}**\n- Real: **${universe.summary.realOperators}**\n\n## 6–7. Calibration 27 & Brand Basics 34\n\n- All 27 resolve to Masters: **${unresolved.length === 0 ? "YES" : "NO"}**\n- Brand parents with Masters: **${withMaster}**\n- No Master required / N/A: **${noReq}**\n- Unknown / research required: **${unknown}**\n- Aliases (MxM/HMS/NH/…): see alias map — **not** separate Masters\n\n## 8–11. Key distinctions\n\n- Research content-complete but gated: **${gated}**\n- Production Explorer Publishable: **${pubProd}**\n- Production needing enrichment: **${prodNeed}**\n- Canonical Strong: **${universe.summary.strongProfiles}**\n- Fit Data Ready: **${universe.summary.fitDataReady}**\n\n## 12–18. Artifacts\n\n- Definitions: \`docs/product/operator-explorer-universe-definitions.md\`\n- Why lists differ: \`docs/product/operator-explorer-why-lists-differ.md\`\n- Crosswalk: \`reports/operator-explorer-universe-crosswalk.md\`\n- Resolver: \`lib/operator-explorer/operator-universe.js\` (**created**)\n- Readiness: \`lib/operator-explorer/readiness.js\` (**created**; canonical counts recalculated)\n- View audit/spec: reports + \`docs/data/operator-explorer-airtable-view-spec.md\`\n- Dashboard: \`data/operator-explorer/operator-universe-dashboard.json\` + internal HTML\n\n## 19. Founder approvals required\n\n1. Canonical universe definitions\n2. Canonical resolver module\n3. Canonical readiness module\n4. Airtable OE view naming/filter spec (create views)\n5. Keep Research content-complete gated (recommended: **yes**)\n6. Individual Research graduation later only\n7. Internal universe dashboard as authoritative admin view\n8. Next phase after reconciliation\n\n## 20. Recommended next phase\n\nAdopt shared readiness in Phase 1 payload path → create OE Airtable views → targeted enrichment (named CALA / Webhound when complete). **Still no Fit/owner.**\n`
  );

  const stop = {
    totalMasters: universe.summary.totalMasters,
    production: universe.summary.production,
    research: universe.summary.research,
    testFixtures: universe.summary.testFixtures,
    realOperators: universe.summary.realOperators,
    calibration27Resolved: unresolved.length === 0,
    brandBasicsWithMaster: withMaster,
    brandBasicsNoMasterRequired: noReq,
    brandBasicsUnknown: unknown,
    researchContentCompleteGated: gated,
    productionPublishable: pubProd,
    productionNeedsEnrichment: prodNeed,
    strong: universe.summary.strongProfiles,
    explorerPublishable: universe.summary.explorerPublishable,
    fitDataReady: universe.summary.fitDataReady,
    viewsAudited: views.length,
  };
  writeJson(join(ROOT, "data", "operator-explorer", "universe-reconciliation-stop-point.json"), stop);
  console.log(JSON.stringify({ ok: true, stop, unresolved }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
