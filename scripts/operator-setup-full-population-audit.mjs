#!/usr/bin/env node
/**
 * Operator Setup — Full Airtable Population Audit (READ-ONLY)
 *   node scripts/operator-setup-full-population-audit.mjs
 * No Airtable writes. No Fit changes.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildOperatorUniverse } from "../lib/operator-explorer/operator-universe.js";
import { isAggregateAssignmentName } from "../lib/operator-explorer/readiness.js";
import { ENRICHMENT_FIELD_CATALOG } from "../lib/operator-fit/readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/audit");
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

const SECTION_TABLES = new Set([
  "Operator Setup - Operating Platform",
  "Operator Setup - Engagement & Reporting",
  "Operator Setup - Infrastructure Platform",
  "Operator Setup - Leadership Platform",
  "Operator Setup - Brand Relationships",
  "Operator Setup - Explorer Materials",
]);

const ONE_TO_ONE = new Set([
  "Operator Setup - Profile & Positioning",
  "Operator Setup - Platform & Markets",
  "Operator Setup - Commercial Fit & Terms",
  "Operator Setup - Governance, Delivery & Diligence",
]);

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
function isPopulated(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === "boolean") return true;
  if (typeof v === "number") return !Number.isNaN(v);
  if (typeof v === "string") return v.trim().length > 0;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return Boolean(v);
}
function fillBand(pct) {
  if (pct < 25) return "Critically Sparse";
  if (pct < 50) return "Sparse";
  if (pct < 80) return "Partial";
  if (pct < 95) return "Healthy";
  return "Near Complete";
}

function classifyField(tableName, fieldName, fieldType) {
  const n = fieldName;
  const nl = n.toLowerCase();
  const notes = [];

  // System / link / identity
  if (n === "Operator" || fieldType === "multipleRecordLinks" && /Operator/i.test(n)) {
    return { classification: "WORKFLOW ONLY", intendedSource: "Master link", treatment: "Keep as link spine", notes: "Master linkage" };
  }
  if (["created_at", "updated_at", "createdTime", "lastModifiedTime"].includes(n) || fieldType === "createdTime" || fieldType === "lastModifiedTime") {
    return { classification: "WORKFLOW ONLY", intendedSource: "system", treatment: "Leave", notes: "" };
  }

  // Fit-specific
  if (/fit_score|fitScore|shortlist|candidate.?rank|recommendation|deal.?risk|project.?compat|bf_fit|bf_q_|bf_signal|idealProject|marketsToAvoid|priorityMarkets|feeExpectation|knownRedFlags/i.test(n)) {
    return { classification: "FIT-SPECIFIC", intendedSource: "Operator Fit project context", treatment: "Remain blank in general Setup", notes: "Project-specific / Fit preference" };
  }

  // OE status sync fields
  if (/^OE /.test(n) || n === "Record Purpose") {
    return { classification: "WORKFLOW ONLY", intendedSource: "OE universe sync", treatment: "Keep; sync from OE", notes: "Explorer lifecycle / readiness sync" };
  }

  // Section content tables
  if (SECTION_TABLES.has(tableName)) {
    if (["title", "section", "row_key", "row_type", "display_order", "subtitle", "body", "extra"].includes(n)) {
      return {
        classification: "RESEARCHED SUMMARY",
        intendedSource: "Explorer content packs / profile deepen",
        treatment: "Populate via Explorer content pipeline for Production profiles needing Explorer UI",
        notes: "Section-row content model (not form field)",
      };
    }
  }

  // Direct company facts
  if (
    /^(company_name|website|headquarters|Operator Website|Operator Parent Company|Operator Aliases|yearEstablished|yearsInBusiness|companySize|companyTagline|companyHistory|companyDescription|missionStatement|primaryServiceModel|Operating Model|Management Availability|capitalStatus|figuresAsOf)$/i.test(
      n
    )
  ) {
    return { classification: "DIRECT", intendedSource: "Company research / Master facts", treatment: "Direct backfill when verified", notes: "" };
  }

  // Derived geo / brand / experience
  if (
    /Active Countries|Active Markets|specificMarkets|numberOfMarkets|Brand Families|brands|additionalBrands|numberOfBrands|chainScales|chainScale|propertyTypes|locationType|newBuildExperience|conversionExperience|turnaroundExperience|preOpening|renovationExperience|transitionExperience|stabilizedExperience|luxury|upscale|midscale|economy|geo_|totalProperties|totalRooms|All-Inclusive|Extended Stay|Soft Brand|Management Structures Supported|New-Build Opening|Conversion \/ Reflag|Market Presence Type/i.test(
      n
    )
  ) {
    return {
      classification: "DERIVED",
      intendedSource: "Assignments + Market Presence + Brand Relationships",
      treatment: "Derive on sync; do not manually maintain as SoT",
      notes: "Normalized OE should drive these summaries",
    };
  }

  // JSON explorer blobs
  if (/_json$/i.test(n) || /overview_|cap_|ov_|mkt_|brand_narrative|brand_signal|cap_profile|cap_card|cap_deep|cap_kpi|cap_signal/i.test(n)) {
    return {
      classification: "RESEARCHED SUMMARY",
      intendedSource: "Profile deepen / website content packs / Explorer narrative",
      treatment: "Populate for Explorer-quality profiles; not Fit SoT",
      notes: "Narrative / explorer presentation",
    };
  }

  // Governance / commercial narrative
  if (
    tableName.includes("Governance") ||
    tableName.includes("Commercial") ||
    /differentiators|managementPhilosophy|ownerEngagement|reportingFrequency|feeStructure|diligence|insurance|sustainability|esg|crisis|businessContinuity/i.test(n)
  ) {
    if (/OptIn|checkbox/i.test(n) || fieldType === "checkbox") {
      return { classification: "WORKFLOW ONLY", intendedSource: "intake form", treatment: "Leave unless intake completes", notes: "" };
    }
    return {
      classification: "RESEARCHED SUMMARY",
      intendedSource: "Operator intake / researched capability claims",
      treatment: "Backfill only with evidence; else Claims",
      notes: "",
    };
  }

  // Diligence QA / review
  if (tableName.includes("Diligence QA") || /Validation Status|Reviewed By|Last Reviewed|Refresh Due|Missing Data|Internal Notes|Company Validated|External Display|Usage Permission|Source Region|Evidence Notes|submission_status|Data Confidence|Source Type/i.test(n)) {
    return { classification: "WORKFLOW ONLY", intendedSource: "QA workflow", treatment: "Do not backfill blanks for appearance", notes: "" };
  }

  // Case studies
  if (tableName.includes("Case Studies")) {
    return {
      classification: "OBSOLETE / DUPLICATE",
      intendedSource: "Historically case-study stories; Assignments now preferred for evidence",
      treatment: "Retain for compatibility; new evidence → Assignments; migrate stories later",
      notes: "Duplicate concept with Assignments + Explorer stories",
    };
  }

  // Leadership team
  if (tableName.includes("Leadership Team")) {
    return {
      classification: "RESEARCHED SUMMARY",
      intendedSource: "Leadership research",
      treatment: "Optional profile enrichment; not blocking Fit",
      notes: "",
    };
  }

  // Explorer materials
  if (tableName.includes("Explorer Materials")) {
    return {
      classification: "RESEARCHED SUMMARY",
      intendedSource: "Explorer materials pipeline",
      treatment: "Populate for published Explorer profiles",
      notes: "",
    };
  }

  // Master PI / users links
  if (/Partner Intelligence|Users|Operator Deal Requests|Operator Setup -|Operator Intelligence|Operator Fit/i.test(n) && fieldType === "multipleRecordLinks") {
    return { classification: "WORKFLOW ONLY", intendedSource: "Airtable links", treatment: "Keep", notes: "Relationship fields" };
  }

  if (/companyLogo|gallery|hero/i.test(n)) {
    return { classification: "RESEARCHED SUMMARY", intendedSource: "Asset pipeline", treatment: "Populate for Explorer UI", notes: "" };
  }

  return { classification: "UNKNOWN", intendedSource: "TBD", treatment: "Needs founder/field-owner review", notes: "Heuristic unclassified" };
}

function rootCauseForSparse(field, classification, oeCross) {
  // I — correctly blank / not applicable for cosmetics
  if (classification === "WORKFLOW ONLY" || classification === "FIT-SPECIFIC") return "I";
  // D/E — legacy / duplicate architecture
  if (classification === "OBSOLETE / DUPLICATE") return "D";
  // Explorer section-row content: research exists for operators, but deepen packs never wrote rows
  if (field.isSectionTable && classification === "RESEARCHED SUMMARY") return "F";
  // DERIVED summaries: OE holds evidence; Setup sync intentionally deferred
  if (classification === "DERIVED") {
    if (oeCross?.normalizedCoverage >= 18) return "C"; // ≥50% of Production have OE evidence
    if (oeCross?.normalizedCoverage > 0) return "C";
    return "F"; // derivation pipeline not built / no OE evidence yet
  }
  // DIRECT / RESEARCHED company facts
  if (classification === "DIRECT" || classification === "RESEARCHED SUMMARY") {
    if (oeCross?.normalizedCoverage >= 18) return "B";
    if (oeCross?.normalizedCoverage > 0 && field.fillProduction < 50) return "B";
    if (field.fillProduction < 15 && field.tableHasLinkedRows) return "F";
    return "A";
  }
  if (classification === "UNKNOWN") return "J";
  return "J";
}

const TABLE_META = {
  "Operator Setup - Master": {
    purpose: "Canonical operator identity + lifecycle + OE readiness sync",
    consumers: ["OE universe", "Operator Fit eligibility (status)", "Explorer", "PI governance"],
    writers: ["operator-setup writers", "OE wave scripts", "intake forms"],
    status: "Active",
  },
  "Operator Setup - Profile & Positioning": {
    purpose: "Company profile, branding, chain scales, service model, overview narratives",
    consumers: ["Operator Explorer", "Operator Fit (chainScales, brands)", "DNA explorer JSON"],
    writers: ["new-base writer", "profile-deepen", "website-content-apply", "linked-tabs-bootstrap"],
    status: "Partially Active",
  },
  "Operator Setup - Platform & Markets": {
    purpose: "Geography, scale, development experience, market narratives",
    consumers: ["Operator Fit (Active Countries)", "Explorer", "DNA JSON"],
    writers: ["new-base writer", "website-content-apply", "linked-tabs-bootstrap"],
    status: "Partially Active",
  },
  "Operator Setup - Commercial Fit & Terms": {
    purpose: "Commercial terms, owner engagement, fee structures, Fit preferences",
    consumers: ["Operator Fit (structures, commercial prefs)", "Explorer"],
    writers: ["new-base writer", "intake", "website-content-apply"],
    status: "Partially Active",
  },
  "Operator Setup - Governance, Delivery & Diligence": {
    purpose: "Governance, delivery, diligence narratives and controls",
    consumers: ["Explorer", "intake QA"],
    writers: ["new-base writer", "website-content-apply"],
    status: "Partially Active",
  },
  "Operator Setup - Case Studies": {
    purpose: "Historical operator case-study stories",
    consumers: ["Legacy Explorer / Fit project-experience hints"],
    writers: ["legacy intake / deepen packs"],
    status: "Legacy",
  },
  "Operator Setup - Brand Relationships": {
    purpose: "Explorer section rows for brand relationship storytelling (NOT intel BR table)",
    consumers: ["Operator Explorer UI sections"],
    writers: ["Explorer content packs / normalize brands"],
    status: "Partially Active",
  },
  "Operator Setup - Leadership Team Members": {
    purpose: "Named leadership people",
    consumers: ["Explorer leadership tab"],
    writers: ["intake / deepen"],
    status: "Partially Active",
  },
  "Operator Setup - Diligence QA": {
    purpose: "Diligence checklist / QA workflow rows",
    consumers: ["Internal QA"],
    writers: ["intake / QA scripts"],
    status: "Workflow",
  },
  "Operator Setup - Explorer Materials": {
    purpose: "Explorer materials / gallery / presentation artifacts",
    consumers: ["Operator Explorer materials"],
    writers: ["materials pipeline"],
    status: "Partially Active",
  },
  "Operator Setup - Engagement & Reporting": {
    purpose: "Explorer section rows — engagement & reporting narratives",
    consumers: ["Operator Explorer UI"],
    writers: ["content packs"],
    status: "Partially Active",
  },
  "Operator Setup - Infrastructure Platform": {
    purpose: "Explorer section rows — infrastructure platform",
    consumers: ["Operator Explorer UI"],
    writers: ["content packs"],
    status: "Partially Active",
  },
  "Operator Setup - Leadership Platform": {
    purpose: "Explorer section rows — leadership platform",
    consumers: ["Operator Explorer UI"],
    writers: ["content packs"],
    status: "Partially Active",
  },
  "Operator Setup - Operating Platform": {
    purpose: "Explorer section rows — operating platform capabilities",
    consumers: ["Operator Explorer UI"],
    writers: ["content packs / deepen"],
    status: "Partially Active",
  },
};

async function listAll(baseId, token, table) {
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}?${qs}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`LIST ${table}: ${JSON.stringify(json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(80);
  } while (offset);
  return out;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE credentials required");
  mkdirSync(OUT, { recursive: true });

  const metaRes = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const meta = await metaRes.json();
  const tableMetaByName = Object.fromEntries((meta.tables || []).map((t) => [t.name, t]));

  console.log("Loading Masters + OE intel...");
  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const asg = await listAll(baseId, token, "Operator Intelligence - Assignments");
  const brIntel = await listAll(baseId, token, "Operator Intelligence - Brand Relationships");
  const mp = await listAll(baseId, token, "Operator Intelligence - Market Presence");
  const claims = await listAll(baseId, token, "Operator Intelligence - Claims");

  const cross = existsSync(join(ROOT, "data/operator-explorer/phase-1-provisional-crosswalk.json"))
    ? JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/phase-1-provisional-crosswalk.json"), "utf8"))
    : {};
  const entities = existsSync(join(ROOT, "data/operator-explorer/calibration-01/entities.json"))
    ? JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/calibration-01/entities.json"), "utf8")).entities
    : [];
  const calibrationByMasterId = {};
  for (const e of entities) {
    const mid = cross[e.entityId] || e.existingMasterId || e.entityId;
    calibrationByMasterId[mid] = { track: e.track };
  }
  const universe = buildOperatorUniverse(masters, {
    assignments: asg,
    brandRelationships: brIntel,
    marketPresence: mp,
    calibrationByMasterId,
  });

  const masterById = Object.fromEntries(masters.map((m) => [m.id, m]));
  const purposeOf = (id) => masterById[id]?.fields?.["Record Purpose"] || null;

  const baseline = {
    generatedAt: new Date().toISOString(),
    summary: universe.summary,
    operators: universe.operators.map((o) => ({
      masterId: o.masterId,
      canonicalName: o.canonicalName,
      recordPurpose: o.recordPurpose,
      explorerPublishable: o.explorerPublishable,
      strongExplorerProfile: o.strongExplorerProfile,
      fitDataReadiness: o.fitDataReadiness,
      counts: o.counts,
    })),
  };
  writeJson(join(OUT, "operator-master-baseline.json"), baseline);
  if (universe.summary.totalMasters !== 46 || universe.summary.production !== 36 || universe.summary.research !== 1 || universe.summary.testFixtures !== 9) {
    console.warn("WARNING: Master baseline differs from expected 46/36/1/9", universe.summary);
  }

  // OE coverage per master
  const oeByMaster = {};
  for (const o of universe.operators) {
    const a = asg.filter((r) => (r.fields.Operator || []).includes(o.masterId) && !isAggregateAssignmentName(r.fields["Property Name"]));
    const countries = new Set(
      [
        ...a.map((r) => r.fields.Country),
        ...mp
          .filter((r) => (r.fields.Operator || []).includes(o.masterId) && /Current Operating|Current Managed/i.test(r.fields["Market Presence Type"] || ""))
          .map((r) => r.fields.Country),
      ].filter(Boolean)
    );
    const brands = new Set(
      [
        ...a.map((r) => r.fields.Brand),
        ...brIntel.filter((r) => (r.fields.Operator || []).includes(o.masterId)).map((r) => r.fields.Brand),
      ].filter(Boolean)
    );
    const dev = new Set(a.map((r) => r.fields["Development Context"]).filter(Boolean));
    const urbanResort = new Set(a.map((r) => r.fields["Urban / Resort"]).filter(Boolean));
    oeByMaster[o.masterId] = {
      namedAssignments: a.length,
      currentCountries: [...countries],
      brands: [...brands],
      developmentContexts: [...dev],
      urbanResort: [...urbanResort],
      allInclusive: a.some((r) => r.fields["All-Inclusive"] === true),
      extendedStay: a.some((r) => r.fields["Extended Stay"] === true),
      marketPresenceRows: mp.filter((r) => (r.fields.Operator || []).includes(o.masterId)).length,
      brandRelRows: brIntel.filter((r) => (r.fields.Operator || []).includes(o.masterId)).length,
      claims: claims.filter((r) => (r.fields.Operator || []).includes(o.masterId)).length,
    };
  }

  console.log("Loading Setup child tables...");
  const setupData = {};
  for (const name of SETUP_TABLES) {
    console.log(" ", name);
    setupData[name] = await listAll(baseId, token, name);
  }

  // Field classification + population
  const fieldClassifications = [];
  const tableSummaries = [];
  let totalFields = 0;

  for (const tableName of SETUP_TABLES) {
    const tmeta = tableMetaByName[tableName];
    if (!tmeta) throw new Error(`Missing meta for ${tableName}`);
    const records = setupData[tableName];
    const fields = tmeta.fields || [];
    totalFields += fields.length;

    // Resolve operator links
    const byPurpose = { Production: [], Research: [], "Test Fixture": [], Unlinked: [] };
    const mastersRepresented = new Set();
    for (const r of records) {
      const ops = r.fields.Operator || [];
      if (!ops.length) {
        byPurpose.Unlinked.push(r);
        continue;
      }
      for (const oid of ops) {
        mastersRepresented.add(oid);
        const p = purposeOf(oid) || "Unlinked";
        if (!byPurpose[p]) byPurpose[p] = [];
        byPurpose[p].push({ record: r, masterId: oid });
      }
    }

    const productionMasters = universe.operators.filter((o) => o.recordPurpose === "Production").map((o) => o.masterId);
    const researchMasters = universe.operators.filter((o) => o.recordPurpose === "Research").map((o) => o.masterId);
    const fixtureMasters = universe.operators.filter((o) => o.recordPurpose === "Test Fixture").map((o) => o.masterId);

    const prodWithRows = productionMasters.filter((id) => mastersRepresented.has(id)).length;
    const emptyReal = universe.operators
      .filter((o) => o.recordPurpose !== "Test Fixture" && !mastersRepresented.has(o.masterId))
      .map((o) => o.canonicalName);

    // For 1:1 tables, evaluate field fill on linked Production rows
    const prodRows = [];
    for (const id of productionMasters) {
      const rows = records.filter((r) => (r.fields.Operator || []).includes(id));
      if (rows.length) prodRows.push(...rows);
    }
    const researchRows = [];
    for (const id of researchMasters) {
      const rows = records.filter((r) => (r.fields.Operator || []).includes(id));
      if (rows.length) researchRows.push(...rows);
    }
    const fixtureRows = [];
    for (const id of fixtureMasters) {
      const rows = records.filter((r) => (r.fields.Operator || []).includes(id));
      if (rows.length) fixtureRows.push(...rows);
    }

    const fieldStats = [];
    for (const f of fields) {
      const cls = classifyField(tableName, f.name, f.type);
      const allPop = records.filter((r) => isPopulated(r.fields[f.name])).length;
      const prodPop = prodRows.filter((r) => isPopulated(r.fields[f.name])).length;
      const resPop = researchRows.filter((r) => isPopulated(r.fields[f.name])).length;
      const fixPop = fixtureRows.filter((r) => isPopulated(r.fields[f.name])).length;
      const allPct = records.length ? Math.round((allPop / records.length) * 1000) / 10 : 0;
      const prodPct = prodRows.length ? Math.round((prodPop / prodRows.length) * 1000) / 10 : 0;
      const resPct = researchRows.length ? Math.round((resPop / researchRows.length) * 1000) / 10 : 0;
      const fixPct = fixtureRows.length ? Math.round((fixPop / fixtureRows.length) * 1000) / 10 : 0;

      // OE crosswalk hint
      let oeHint = null;
      if (/Active Countries|geo_cala|specificMarkets/i.test(f.name)) oeHint = { source: "Market Presence + Assignments", normalizedCoverage: productionMasters.filter((id) => (oeByMaster[id]?.currentCountries || []).length > 0).length };
      else if (/Brand Families|brands|Brand Experience|additionalBrands/i.test(f.name)) oeHint = { source: "Brand Relationships + Assignments", normalizedCoverage: productionMasters.filter((id) => (oeByMaster[id]?.brands || []).length > 0).length };
      else if (/conversion|newBuild|reflag|renovation|preOpening|Development/i.test(f.name)) oeHint = { source: "Assignment Development Context", normalizedCoverage: productionMasters.filter((id) => (oeByMaster[id]?.developmentContexts || []).length > 0).length };
      else if (/locationType|Urban|Resort|propertyTypes|All-Inclusive|Extended/i.test(f.name)) oeHint = { source: "Assignment Urban/Resort + flags", normalizedCoverage: productionMasters.filter((id) => (oeByMaster[id]?.urbanResort || []).length > 0).length };
      else if (/Operating Model|Management Availability|website|Operator Website|Parent/i.test(f.name)) oeHint = { source: "Master / research", normalizedCoverage: productionMasters.filter((id) => isPopulated(masterById[id]?.fields?.[f.name] || masterById[id]?.fields?.["Operator Website"])).length };

      const band = fillBand(prodPct);
      const root = rootCauseForSparse(
        {
          fillProduction: prodPct,
          tableHasLinkedRows: prodRows.length > 0,
          isSectionTable: SECTION_TABLES.has(tableName),
        },
        cls.classification,
        oeHint
      );

      const row = {
        table: tableName,
        fieldId: f.id,
        fieldName: f.name,
        fieldType: f.type,
        classification: cls.classification,
        intendedSource: cls.intendedSource,
        currentPopulationPct: allPct,
        productionPopulationPct: prodPct,
        researchPopulationPct: resPct,
        testFixturePopulationPct: fixPct,
        populatedAll: allPop,
        blankAll: records.length - allPop,
        populatedProduction: prodPop,
        blankProduction: Math.max(0, (ONE_TO_ONE.has(tableName) ? productionMasters.length : prodRows.length) - prodPop),
        productionRowDenom: ONE_TO_ONE.has(tableName) ? productionMasters.length : prodRows.length,
        productionBand: band,
        currentConsumer: TABLE_META[tableName]?.consumers?.join("; ") || "",
        currentWriter: TABLE_META[tableName]?.writers?.join("; ") || "",
        proposedFutureTreatment: cls.treatment,
        notes: cls.notes,
        rootCauseCode: root,
        oeCrosswalk: oeHint,
      };
      fieldClassifications.push(row);
      fieldStats.push(row);
    }

    const meaningful = fieldStats.filter((f) => !["WORKFLOW ONLY", "FIT-SPECIFIC", "OBSOLETE / DUPLICATE"].includes(f.classification));
    const meaningfulProdFill =
      meaningful.length === 0
        ? 0
        : Math.round((meaningful.reduce((s, f) => s + f.productionPopulationPct, 0) / meaningful.length) * 10) / 10;

    let tableClass = "Needs Architecture Decision";
    if (tableName === "Operator Setup - Master") tableClass = "Healthy";
    else if (SECTION_TABLES.has(tableName)) tableClass = meaningfulProdFill < 30 ? "Needs Backfill" : "Needs Derivation";
    else if (tableName.includes("Diligence QA")) tableClass = "Workflow Only";
    else if (tableName.includes("Case Studies")) tableClass = "Mostly Legacy";
    else if (ONE_TO_ONE.has(tableName)) {
      if (prodWithRows < productionMasters.length * 0.9) tableClass = "Needs Backfill";
      else if (meaningfulProdFill < 40) tableClass = "Needs Derivation";
      else if (meaningfulProdFill < 70) tableClass = "Needs Backfill";
      else tableClass = "Healthy";
    }

    // Refine section tables
    if (SECTION_TABLES.has(tableName)) {
      tableClass = prodWithRows < 10 ? "Needs Backfill" : "Partially Active — Explorer content";
      if (tableName.includes("Brand Relationships") && tableName.startsWith("Operator Setup")) {
        tableClass = "Candidate for Deprecation"; // vs intel BR — but keep for Explorer sections
        // Actually Setup BR is section content - keep populate for explorer
        tableClass = "Needs Backfill";
      }
    }
    if (tableName.includes("Case Studies")) tableClass = "Candidate for Deprecation";
    if (tableName.includes("Diligence QA")) tableClass = "Workflow Only";

    tableSummaries.push({
      table: tableName,
      tableId: tmeta.id,
      recordCount: records.length,
      fieldCount: fields.length,
      linkedToMaster: fields.some((f) => f.name === "Operator"),
      intendedPurpose: TABLE_META[tableName]?.purpose,
      consumers: TABLE_META[tableName]?.consumers,
      writers: TABLE_META[tableName]?.writers,
      apparentStatus: TABLE_META[tableName]?.status,
      productionMastersRepresented: prodWithRows,
      productionCoveragePct: Math.round((prodWithRows / productionMasters.length) * 1000) / 10,
      avgRecordsPerRepresentedOperator:
        mastersRepresented.size === 0 ? 0 : Math.round((records.length / mastersRepresented.size) * 10) / 10,
      emptyRealOperators: emptyReal,
      orphanRecords: byPurpose.Unlinked.length,
      testFixtureLinkedRecords: fixtureRows.length,
      meaningfulProductionFillAvg: meaningfulProdFill,
      tableClass,
      criticallySparseMeaningful: meaningful.filter((f) => f.productionBand === "Critically Sparse").length,
    });
  }

  writeJson(join(OUT, "operator-setup-field-classification.json"), {
    generatedAt: new Date().toISOString(),
    totalTables: SETUP_TABLES.length,
    totalFields,
    fields: fieldClassifications,
  });

  // Root cause quantification — all Production Sparse/Critically Sparse fields (incl. workflow → I)
  const sparseAll = fieldClassifications.filter(
    (f) => f.productionBand === "Critically Sparse" || f.productionBand === "Sparse"
  );
  const sparseMeaningful = sparseAll.filter(
    (f) => !["WORKFLOW ONLY", "FIT-SPECIFIC", "OBSOLETE / DUPLICATE"].includes(f.classification)
  );
  // Re-stamp WORKFLOW/FIT/OBSOLETE root codes for all-sparse denominator
  for (const f of sparseAll) {
    if (f.classification === "WORKFLOW ONLY" || f.classification === "FIT-SPECIFIC") f.rootCauseCode = "I";
    if (f.classification === "OBSOLETE / DUPLICATE") f.rootCauseCode = f.rootCauseCode === "E" ? "E" : "D";
  }
  const causeCounts = {};
  for (const f of sparseAll) {
    causeCounts[f.rootCauseCode] = (causeCounts[f.rootCauseCode] || 0) + 1;
  }
  const causeTotal = sparseAll.length || 1;
  const causePct = Object.fromEntries(Object.entries(causeCounts).map(([k, v]) => [k, Math.round((v / causeTotal) * 1000) / 10]));
  const causeCountsMeaningful = {};
  for (const f of sparseMeaningful) {
    causeCountsMeaningful[f.rootCauseCode] = (causeCountsMeaningful[f.rootCauseCode] || 0) + 1;
  }
  const causeTotalMeaningful = sparseMeaningful.length || 1;
  const causePctMeaningful = Object.fromEntries(
    Object.entries(causeCountsMeaningful).map(([k, v]) => [k, Math.round((v / causeTotalMeaningful) * 1000) / 10])
  );

  function productionMastersWith(fn) {
    return universe.operators.filter((o) => o.recordPurpose === "Production" && fn(o.masterId)).length;
  }
  function avgFieldPct(re, tableHint) {
    const rows = fieldClassifications.filter((f) => re.test(f.fieldName) && (!tableHint || f.table.includes(tableHint)));
    if (!rows.length) return null;
    return Math.round((rows.reduce((s, f) => s + f.productionPopulationPct, 0) / rows.length) * 10) / 10;
  }

  // Crosswalk rows
  const crosswalk = [
    {
      setupField: "Platform.Active Countries",
      setupCoverage: fieldClassifications.find((f) => f.table.includes("Platform") && f.fieldName === "Active Countries")?.productionPopulationPct,
      normalizedSource: "Market Presence (Current Operating/Managed) + Assignments.Country",
      normalizedCoverage: Math.round((productionMastersWith((id) => oeByMaster[id].currentCountries.length > 0) / 36) * 1000) / 10,
      derivable: true,
      treatment: "DERIVE — never manually SoT",
    },
    {
      setupField: "Profile.Brand Families Operated / brands",
      setupCoverage: avgFieldPct(/Brand Families Operated|^brands$/i, "Profile"),
      normalizedSource: "Operator Intelligence - Brand Relationships + Assignments.Brand",
      normalizedCoverage: Math.round((productionMastersWith((id) => oeByMaster[id].brands.length > 0) / 36) * 1000) / 10,
      derivable: true,
      treatment: "DERIVE",
    },
    {
      setupField: "Platform.conversionExperience / Commercial.Conversion / Reflag",
      setupCoverage: avgFieldPct(/conversionExperience|Conversion \/ Reflag/i),
      normalizedSource: "Assignments.Development Context",
      normalizedCoverage: Math.round((productionMastersWith((id) => oeByMaster[id].developmentContexts.length > 0) / 36) * 1000) / 10,
      derivable: true,
      treatment: "DERIVE",
    },
    {
      setupField: "Profile.locationTypeResort / Platform.locationTypeUrban",
      setupCoverage: avgFieldPct(/locationType/i),
      normalizedSource: "Assignments.Urban / Resort",
      normalizedCoverage: Math.round((productionMastersWith((id) => oeByMaster[id].urbanResort.length > 0) / 36) * 1000) / 10,
      derivable: true,
      treatment: "DERIVE",
    },
    {
      setupField: "Master.Operating Model / Management Availability",
      setupCoverage: avgFieldPct(/Operating Model|Management Availability/, "Master"),
      normalizedSource: "Master DIRECT (+ Assignments structure as evidence)",
      normalizedCoverage: Math.round((productionMastersWith((id) => isPopulated(masterById[id]?.fields?.["Operating Model"])) / 36) * 1000) / 10,
      derivable: false,
      treatment: "DIRECT backfill",
    },
    {
      setupField: "Setup Brand Relationships (section rows)",
      setupCoverage: null,
      normalizedSource: "Operator Intelligence - Brand Relationships (authoritative structured)",
      normalizedCoverage: Math.round((productionMastersWith((id) => oeByMaster[id].brandRelRows > 0) / 36) * 1000) / 10,
      derivable: false,
      treatment: "Keep Setup BR as Explorer narrative only; intel BR is SoT",
    },
    {
      setupField: "Case Studies",
      setupCoverage: null,
      normalizedSource: "Assignments (+ optional story Claims)",
      normalizedCoverage: Math.round((productionMastersWith((id) => oeByMaster[id].namedAssignments > 0) / 36) * 1000) / 10,
      derivable: false,
      treatment: "LEGACY — Assignments preferenced for evidence",
    },
  ];

  // Active-field completeness KPI (Master + 1:1 DIRECT/DERIVED only — exclude section row spam)
  const activeFields = fieldClassifications.filter(
    (f) =>
      ["DIRECT", "DERIVED"].includes(f.classification) &&
      (f.table === "Operator Setup - Master" || ONE_TO_ONE.has(f.table))
  );
  const activeCurrent =
    activeFields.length === 0
      ? 0
      : Math.round((activeFields.reduce((s, f) => s + f.productionPopulationPct, 0) / activeFields.length) * 10) / 10;
  // Projected: for each DERIVED field with OE hint coverage ≥18, assume fill rises to min(95, max(current, oeCoverage%))
  const activeProjected =
    activeFields.length === 0
      ? 0
      : Math.round(
          (activeFields.reduce((s, f) => {
            if (f.classification === "DERIVED" && f.oeCrosswalk?.normalizedCoverage) {
              const oePct = Math.round((f.oeCrosswalk.normalizedCoverage / 36) * 1000) / 10;
              return s + Math.min(95, Math.max(f.productionPopulationPct, oePct));
            }
            if (f.classification === "DIRECT" && f.productionPopulationPct < 50) {
              // conservative: modest direct backfill gain (+15pp capped at 70)
              return s + Math.min(70, f.productionPopulationPct + 15);
            }
            return s + f.productionPopulationPct;
          }, 0) /
            activeFields.length) *
            10
        ) / 10;

  // Operator completeness (kept for operator-level report)
  const operatorCompleteness = universe.operators
    .filter((o) => o.recordPurpose !== "Test Fixture")
    .map((o) => {
      const oe = oeByMaster[o.masterId];
      const profile = setupData["Operator Setup - Profile & Positioning"].find((r) => (r.fields.Operator || []).includes(o.masterId));
      const platform = setupData["Operator Setup - Platform & Markets"].find((r) => (r.fields.Operator || []).includes(o.masterId));
      const commercial = setupData["Operator Setup - Commercial Fit & Terms"].find((r) => (r.fields.Operator || []).includes(o.masterId));
      const gov = setupData["Operator Setup - Governance, Delivery & Diligence"].find((r) => (r.fields.Operator || []).includes(o.masterId));
      let directFilled = 0;
      let directTotal = 0;
      for (const k of ["Operating Model", "Management Availability", "Operator Website", "Operator Parent Company"]) {
        directTotal++;
        if (isPopulated(masterById[o.masterId]?.fields?.[k])) directFilled++;
      }
      for (const k of ["website", "headquarters", "yearEstablished", "companyDescription"]) {
        directTotal++;
        if (isPopulated(profile?.fields?.[k])) directFilled++;
      }
      const derivedPotential = [
        oe.currentCountries.length > 0,
        oe.brands.length > 0,
        oe.developmentContexts.length > 0,
        oe.urbanResort.length > 0,
      ].filter(Boolean).length;
      const linkedTabs = [profile, platform, commercial, gov].filter(Boolean).length;
      const intel = oe.namedAssignments + oe.marketPresenceRows + oe.brandRelRows;
      const setupCountries = isPopulated(platform?.fields?.["Active Countries"]);
      const trueMissing =
        Math.max(0, 4 - derivedPotential) +
        (directFilled < 3 ? 1 : 0) +
        (oe.currentCountries.length > 0 && !setupCountries ? 0 : 0); // sync gap counted separately
      const syncGap = oe.currentCountries.length > 0 && !setupCountries ? 1 : 0;
      const setupCompleteness = Math.round(((directFilled / directTotal) * 0.4 + (derivedPotential / 4) * 0.4 + (linkedTabs / 4) * 0.2) * 1000) / 10;
      return {
        operator: o.canonicalName,
        masterId: o.masterId,
        purpose: o.recordPurpose,
        directSetupPct: Math.round((directFilled / directTotal) * 1000) / 10,
        derivedSetupPotentialPct: Math.round((derivedPotential / 4) * 1000) / 10,
        researchedSummaryLinkedTabs: linkedTabs,
        intelligenceCoverage: intel,
        trueMissingScore: trueMissing,
        syncGap,
        setupCompleteness,
        oe,
        hasProfile: Boolean(profile),
        hasPlatform: Boolean(platform),
        hasCommercial: Boolean(commercial),
        hasGovernance: Boolean(gov),
      };
    });

  const prodCompleteness = operatorCompleteness.filter((o) => o.purpose === "Production");
  const currentMeaningful = activeCurrent;
  const projected = activeProjected;

  // Dry-run backfill candidates (no apply)
  const backfillPlan = { generatedAt: new Date().toISOString(), mode: "dry-run-only", mutations: [] };
  function pushMutation(m) {
    backfillPlan.mutations.push(m);
  }
  for (const o of universe.operators.filter((x) => x.recordPurpose === "Production")) {
    const oe = oeByMaster[o.masterId];
    const platform = setupData["Operator Setup - Platform & Markets"].find((r) => (r.fields.Operator || []).includes(o.masterId));
    const profile = setupData["Operator Setup - Profile & Positioning"].find((r) => (r.fields.Operator || []).includes(o.masterId));
    const commercial = setupData["Operator Setup - Commercial Fit & Terms"].find((r) => (r.fields.Operator || []).includes(o.masterId));

    if (oe.currentCountries.length && platform && !isPopulated(platform.fields["Active Countries"])) {
      pushMutation({
        table: "Operator Setup - Platform & Markets",
        recordId: platform.id,
        masterId: o.masterId,
        masterName: o.canonicalName,
        field: "Active Countries",
        currentValue: platform.fields["Active Countries"] || null,
        proposedValue: oe.currentCountries,
        source: "Market Presence Current + Assignments",
        treatment: "DERIVED",
        confidence: "high",
        conflictStatus: "None",
        whySafe: "Derived from verified current presence/assignments; Strategic Interest excluded",
      });
    }

    if (profile && oe.brands.length) {
      for (const brandField of ["Brand Families Operated", "brands"]) {
        if (profile.fields[brandField] !== undefined && !isPopulated(profile.fields[brandField])) {
          pushMutation({
            table: "Operator Setup - Profile & Positioning",
            recordId: profile.id,
            masterId: o.masterId,
            masterName: o.canonicalName,
            field: brandField,
            currentValue: null,
            proposedValue: oe.brands.slice(0, 12),
            source: "Brand Relationships + Assignments",
            treatment: "DERIVED",
            confidence: "high",
            conflictStatus: "Taxonomy may need option ensure",
            whySafe: "Derive from normalized brand names; ensure select options exist before apply",
          });
          break;
        }
      }
    }

    // Urban / Resort / conversion — field may be absent on blank Airtable records
    const profileFieldsAvail = new Set(
      (tableMetaByName["Operator Setup - Profile & Positioning"]?.fields || []).map((f) => f.name)
    );
    const platformFieldsAvail = new Set(
      (tableMetaByName["Operator Setup - Platform & Markets"]?.fields || []).map((f) => f.name)
    );
    const locTargets = [
      { table: "Operator Setup - Profile & Positioning", rec: profile, field: "locationTypeResort", avail: profileFieldsAvail, test: (x) => /resort/i.test(String(x)) },
      { table: "Operator Setup - Platform & Markets", rec: platform, field: "locationTypeUrban", avail: platformFieldsAvail, test: (x) => /urban|city/i.test(String(x)) },
      { table: "Operator Setup - Profile & Positioning", rec: profile, field: "locationTypeUrban", avail: profileFieldsAvail, test: (x) => /urban|city/i.test(String(x)) },
      { table: "Operator Setup - Platform & Markets", rec: platform, field: "locationTypeResort", avail: platformFieldsAvail, test: (x) => /resort/i.test(String(x)) },
    ];
    for (const t of locTargets) {
      if (!t.rec || !t.avail.has(t.field)) continue;
      const yes = (oe.urbanResort || []).some(t.test);
      if (yes && !isPopulated(t.rec.fields[t.field])) {
        pushMutation({
          table: t.table,
          recordId: t.rec.id,
          masterId: o.masterId,
          masterName: o.canonicalName,
          field: t.field,
          currentValue: t.rec.fields[t.field] || null,
          proposedValue: "Yes",
          source: "Assignments.Urban / Resort",
          treatment: "DERIVED",
          confidence: "medium",
          conflictStatus: "Confirm select option vocabulary; Urban/Resort currently sparse on Assignments",
          whySafe: "Only set Yes when named assignments evidence location type",
        });
      }
    }
    if (
      platform &&
      platformFieldsAvail.has("conversionExperience") &&
      (oe.developmentContexts || []).some((x) => /conversion|reflag|flag conversion/i.test(String(x))) &&
      !isPopulated(platform.fields.conversionExperience)
    ) {
      pushMutation({
        table: "Operator Setup - Platform & Markets",
        recordId: platform.id,
        masterId: o.masterId,
        masterName: o.canonicalName,
        field: "conversionExperience",
        currentValue: platform.fields.conversionExperience || null,
        proposedValue: "Yes",
        source: "Assignments.Development Context",
        treatment: "DERIVED",
        confidence: "medium",
        conflictStatus: "Confirm option vocabulary (Yes/Evidence/etc.)",
        whySafe: "Derived from assignment development context evidence",
      });
    }
  }

  backfillPlan.summary = {
    derivedActiveCountries: backfillPlan.mutations.filter((m) => m.field === "Active Countries").length,
    derivedBrandFamilies: backfillPlan.mutations.filter((m) => /Brand Families|^brands$/i.test(m.field)).length,
    derivedLocationDevFlags: backfillPlan.mutations.filter((m) => /locationType|conversion|Conversion/i.test(m.field)).length,
    totalProposed: backfillPlan.mutations.length,
    note: "DRY-RUN ONLY — not applied. Taxonomy ensure required for multi-selects before apply.",
  };
  writeJson(join(OUT, "operator-setup-backfill-write-plan.json"), backfillPlan);

  // Fit consumer map
  const fitConsumers = ENRICHMENT_FIELD_CATALOG.map((c) => {
    const hint = c.airtableHint || "";
    let setupPop = null;
    let oeAvail = null;
    let gap = "UNKNOWN";
    if (/Active Countries/i.test(hint)) {
      setupPop = fieldClassifications.find((f) => f.fieldName === "Active Countries")?.productionPopulationPct;
      oeAvail = Math.round((productionMastersWith((id) => oeByMaster[id].currentCountries.length > 0) / 36) * 100);
      gap = setupPop < 50 && oeAvail > 70 ? "DATA EXISTS — SETUP NOT BACKFILLED" : setupPop < 50 ? "DATA EXISTS — NORMALIZED OE NOT MAPPED TO FIT" : "OK";
    } else if (/Management Structures/i.test(hint)) {
      setupPop = fieldClassifications.find((f) => f.fieldName === "Management Structures Supported")?.productionPopulationPct;
      oeAvail = Math.round((productionMastersWith((id) => oeByMaster[id].namedAssignments > 0) / 36) * 100);
      gap = "DATA EXISTS — NORMALIZED OE NOT MAPPED TO FIT";
    } else if (/chainScales/i.test(hint)) {
      setupPop = fieldClassifications.find((f) => f.fieldName === "chainScalesSupported")?.productionPopulationPct;
      oeAvail = Math.round((productionMastersWith((id) => oeByMaster[id].namedAssignments > 0) / 36) * 100);
      gap = setupPop < 40 ? "DATA EXISTS — SETUP NOT BACKFILLED" : "PARTIAL";
    } else if (/Case Studies|project-experience/i.test(hint)) {
      setupPop = null;
      oeAvail = Math.round((productionMastersWith((id) => oeByMaster[id].namedAssignments >= 2) / 36) * 100);
      gap = "DATA EXISTS — NORMALIZED OE NOT MAPPED TO FIT";
    } else if (/submission_status|Active status/i.test(hint)) {
      setupPop = fieldClassifications.find((f) => f.fieldName === "submission_status")?.productionPopulationPct;
      gap = "OK";
    } else if (/PI Source/i.test(hint)) {
      gap = "PARTIAL — PI Sources exist; Fit not fully wired to OE evidence";
    } else {
      gap = "FIT EXPECTATION USES SETUP FORM FIELDS — often sparse";
    }
    return {
      fitDomain: c.id,
      label: c.label,
      requiredForRanking: c.requiredForRanking,
      airtableHint: hint,
      setupPopulationPct: setupPop,
      normalizedOeAvailablePct: oeAvail,
      gapClass: gap,
    };
  });

  const gapClassCounts = {};
  for (const f of fitConsumers) gapClassCounts[f.gapClass] = (gapClassCounts[f.gapClass] || 0) + 1;

  // ================= REPORTS =================

  writeMd(
    join(REPORTS, "operator-setup-full-table-inventory.md"),
    [
      `# Operator Setup — Full Table Inventory`,
      ``,
      `**Generated:** ${new Date().toISOString()}`,
      `**Operator Setup tables:** ${SETUP_TABLES.length}`,
      `**Total Setup fields:** ${totalFields}`,
      ``,
      `| Table | ID | Records | Fields | Linked to Master? | Purpose | Consumers | Writers | Status |`,
      `| ----- | -- | ------: | -----: | ----------------- | ------- | --------- | ------- | ------ |`,
      ...tableSummaries.map(
        (t) =>
          `| ${t.table} | \`${t.tableId}\` | ${t.recordCount} | ${t.fieldCount} | ${t.linkedToMaster ? "Yes" : "No"} | ${t.intendedPurpose} | ${(t.consumers || []).join("; ")} | ${(t.writers || []).join("; ")} | ${t.apparentStatus} |`
      ),
      ``,
      `## Counts`,
      ``,
      `- Active / Partially Active: ${tableSummaries.filter((t) => /Active/i.test(t.apparentStatus)).length}`,
      `- Legacy: ${tableSummaries.filter((t) => t.apparentStatus === "Legacy").length}`,
      `- Workflow: ${tableSummaries.filter((t) => t.apparentStatus === "Workflow").length}`,
      ``,
      `Also related (not named Operator Setup but Fit/OE): Operator Fit - Shortlist, Operator Intelligence -*, Operator Deal Requests.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-field-population-matrix.md"),
    [
      `# Operator Setup Field Population Matrix`,
      ``,
      `Production denominator: for 1:1 tables = 36 Production Masters (missing row = blank); for multi-row tables = Production-linked records.`,
      ``,
      `| Table | Field | Class | Prod % | Band | All % | Root |`,
      `| ----- | ----- | ----- | -----: | ---- | ----: | ---- |`,
      ...fieldClassifications
        .filter((f) => !["WORKFLOW ONLY"].includes(f.classification) || f.productionBand === "Critically Sparse")
        .slice(0, 400)
        .map(
          (f) =>
            `| ${f.table.replace("Operator Setup - ", "")} | ${f.fieldName} | ${f.classification} | ${f.productionPopulationPct} | ${f.productionBand} | ${f.currentPopulationPct} | ${f.rootCauseCode} |`
        ),
      ``,
      `*(Truncated display of non-workflow-first rows; full machine file has all ${totalFields} fields.)*`,
      ``,
      `## Band counts (meaningful fields only)`,
      ``,
      ...["Critically Sparse", "Sparse", "Partial", "Healthy", "Near Complete"].map((b) => {
        const n = fieldClassifications.filter(
          (f) => !["WORKFLOW ONLY", "FIT-SPECIFIC", "OBSOLETE / DUPLICATE"].includes(f.classification) && f.productionBand === b
        ).length;
        return `- **${b}:** ${n}`;
      }),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-table-population-summary.md"),
    [
      `# Operator Setup Table Population Summary`,
      ``,
      `| Table | Prod coverage % | Avg rows/op | Empty real ops | Meaningful prod fill avg | Class |`,
      `| ----- | --------------: | ----------: | -------------: | -----------------------: | ----- |`,
      ...tableSummaries.map(
        (t) =>
          `| ${t.table.replace("Operator Setup - ", "")} | ${t.productionCoveragePct} | ${t.avgRecordsPerRepresentedOperator} | ${t.emptyRealOperators.length} | ${t.meaningfulProductionFillAvg} | ${t.tableClass} |`
      ),
      ``,
      `## Empty real operators by table (no linked rows)`,
      ``,
      ...tableSummaries
        .filter((t) => t.emptyRealOperators.length)
        .map((t) => `- **${t.table}**: ${t.emptyRealOperators.slice(0, 20).join(", ")}${t.emptyRealOperators.length > 20 ? "…" : ""}`),
      ``,
    ].join("\n")
  );

  const causeLabels = {
    A: "Never researched",
    B: "Research exists elsewhere (OE intel)",
    C: "Derivation intentionally deferred",
    D: "Legacy field",
    E: "Duplicate concept",
    F: "Writer/pipeline missing",
    G: "Taxonomy mismatch",
    H: "Link failure",
    I: "Correctly blank",
    J: "Other",
  };

  writeMd(
    join(REPORTS, "operator-setup-empty-field-root-causes.md"),
    [
      `# Empty Field Root Causes`,
      ``,
      `## All sparse Production fields (incl. workflow / Fit / obsolete)`,
      ``,
      `Among **${sparseAll.length}** Production Sparse/Critically Sparse fields:`,
      ``,
      `| Code | Reason | Count | % |`,
      `| ---- | ------ | ----: | -: |`,
      ...Object.entries(causeCounts)
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => `| ${k} | ${causeLabels[k] || k} | ${v} | ${causePct[k]}% |`),
      ``,
      `## Meaningful sparse fields only`,
      ``,
      `Among **${sparseMeaningful.length}** meaningful sparse fields (excludes workflow / Fit / obsolete):`,
      ``,
      `| Code | Reason | Count | % |`,
      `| ---- | ------ | ----: | -: |`,
      ...Object.entries(causeCountsMeaningful)
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => `| ${k} | ${causeLabels[k] || k} | ${v} | ${causePctMeaningful[k]}% |`),
      ``,
      `## Plain-language`,
      ``,
      `Most blanks are **not** because Dealality lacks operator intelligence:`,
      ``,
      `1. **Explorer section tables** were deep-filled for golden operators only — blanks = missing content pipeline (F).`,
      `2. **Derived Setup summaries** were intentionally not synced after Assignments / Presence / BR launched (C).`,
      `3. **Workflow + Fit-preference fields** are correctly blank (I).`,
      `4. A minority of company-fact / deep narrative fields still need genuine research (A).`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-vs-operator-intelligence-crosswalk.md"),
    [
      `# Setup vs Operator Intelligence Crosswalk`,
      ``,
      `| Setup Field | Setup Coverage (Prod %) | Normalized Source | Normalized Coverage (Prod %) | Derivable? | Recommended Treatment |`,
      `| ----------- | ----------------------: | ----------------- | ---------------------------: | ---------- | --------------------- |`,
      ...crosswalk.map(
        (c) =>
          `| ${c.setupField} | ${c.setupCoverage ?? "n/a"} | ${c.normalizedSource} | ${c.normalizedCoverage} | ${c.derivable ? "Yes" : "No"} | ${c.treatment} |`
      ),
      ``,
      `**Rule:** Do not manually maintain Setup geography/brand/experience lists when Assignments / Presence / Brand Relationships already evidence them.`,
      ``,
    ].join("\n")
  );

  // Table-by-table verdict
  const verdictLines = [`# Operator Setup — Table-by-Table Verdict`, ``];
  for (const t of tableSummaries) {
    const keep = !/Deprecation/i.test(t.tableClass) || t.table.includes("Brand Relationships") || t.table.includes("Case Studies");
    const populate = ONE_TO_ONE.has(t.table) || t.table.includes("Master") || SECTION_TABLES.has(t.table);
    const derive = ONE_TO_ONE.has(t.table) && /Platform|Profile|Commercial/.test(t.table);
    const deprecateLater = t.table.includes("Case Studies") || (t.table === "Operator Setup - Brand Relationships");
    verdictLines.push(`## ${t.table}`);
    verdictLines.push(``);
    verdictLines.push(`- **Originally:** ${t.intendedPurpose}`);
    verdictLines.push(`- **Still belongs?** ${t.table.includes("Case Studies") ? "Concept yes; table legacy vs Assignments" : "Yes"}`);
    verdictLines.push(`- **Used by code?** ${(t.consumers || []).length ? "Yes" : "Limited"}`);
    verdictLines.push(`- **Replaced by OE?** ${t.table.includes("Case Studies") || t.table.includes("Platform & Markets") ? "Partially (geo/brand/dev evidence)" : "No"}`);
    verdictLines.push(`- **Unique info?** ${SECTION_TABLES.has(t.table) ? "Explorer narrative rows" : ONE_TO_ONE.has(t.table) ? "Form summaries + narratives" : "Identity/lifecycle"}`);
    verdictLines.push(`- **Populate?** ${populate ? "Yes (selective)" : "No"}`);
    verdictLines.push(`- **Derive?** ${derive ? "Yes for geo/brand/experience" : "Limited"}`);
    verdictLines.push(`- **Workflow-only?** ${t.apparentStatus === "Workflow" ? "Yes" : "No"}`);
    verdictLines.push(`- **Deprecate later?** ${deprecateLater ? "Candidate after consumer migration" : "No"}`);
    verdictLines.push(`- **Verdict class:** ${t.tableClass}`);
    verdictLines.push(``);
  }
  writeMd(join(REPORTS, "operator-setup-table-by-table-verdict.md"), verdictLines.join("\n"));

  writeMd(
    join(REPORTS, "operator-setup-source-of-truth-conflicts.md"),
    [
      `# Source-of-Truth Conflicts`,
      ``,
      `| Concept | Competing places | Authoritative source |`,
      `| ------- | ---------------- | -------------------- |`,
      `| Current countries | Platform.Active Countries; Master OE; Market Presence; Assignments | **Market Presence (current) + Assignments** → derive Setup |`,
      `| Brand experience | Profile.brands; Setup BR section rows; Intel Brand Relationships; Assignments | **Intel Brand Relationships + Assignments** |`,
      `| Conversion / new build | Platform experience counts; Commercial selects; Assignments.Development Context | **Assignments.Development Context** |`,
      `| Urban / Resort / AI | Profile locationType*; Assignments | **Assignments** |`,
      `| Case evidence | Case Studies table; Assignments; Claims | **Assignments** (stories optional) |`,
      `| Operating model | Master.Operating Model; Commercial.Management Structures; Assignments structure | **Master.Operating Model** + Assignment evidence |`,
      `| Company website/parent | Master; Profile.website | **Master** (Profile mirrors) |`,
      `| Fit preferences | Commercial bf_* / ideal* | **Operator Fit project layer** — not general truth |`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(DOCS, "data/operator-setup-source-of-truth-policy.md"),
    [
      `# Operator Setup — Source of Truth Policy`,
      ``,
      `**Status:** Proposed (founder approval required)  
`,
      `**Date:** ${new Date().toISOString().slice(0, 10)}`,
      ``,
      `## Layers`,
      ``,
      `| Layer | Role | Must not |`,
      `| ----- | ---- | -------- |`,
      `| **Operator Setup (Master + 1:1 form tabs)** | Stable company facts + **derived structured summaries** for profile UX | Compete with Assignments as evidence SoT |`,
      `| **Assignments** | Hotel-property operating evidence | Store company narrative essays |`,
      `| **Market Presence** | Geographic presence SoT | Treat Strategic Interest as current operating |`,
      `| **Brand Relationships (Intel)** | Brand/operator relationship SoT | Duplicate as manual Profile brand lists |`,
      `| **Claims** | Evidence-backed company-level narrative/capability | Inflate for every blank Setup field |`,
      `| **PI Source Library** | Evidence/source SoT | Be omitted from publishable facts |`,
      `| **Setup section tables** (Operating/Engagement/Infra/Leadership Platform, Setup BR rows) | Explorer presentation content | Be treated as Fit scoring inputs |`,
      `| **Operator Fit** | Project-specific interpretation only | Write general operator truth into Fit-only fields |`,
      ``,
      `## Setup = stable + derived summary`,
      ``,
      `Example: Assignments (resort hotels) → Setup \`Resort Experience = Yes\`; Market Presence (MX/DR/CO) → Setup \`Active Countries\`; Intel BR → Setup brand summary.`,
      ``,
      `Summaries must refresh from normalized intelligence and never become a competing manual SoT.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(DOCS, "data/operator-setup-derived-field-spec.md"),
    [
      `# Operator Setup — Derived Field Spec`,
      ``,
      `## Active Countries`,
      ``,
      `- **Source:** Market Presence where type ∈ {Current Operating Portfolio, Current Managed Property} ∪ Assignments with Assignment Status=Current`,
      `- **Exclude:** Strategic Interest, Claimed Capability, Historical-only`,
      `- **Transform:** distinct Country set`,
      `- **Min evidence:** ≥1 current presence or current named assignment`,
      `- **Conflict:** Prefer assignment-backed countries if Presence conflicts`,
      `- **Blank:** if no current evidence`,
      `- **Refresh:** on OE wave apply / nightly sync`,
      ``,
      `## Brand Families Operated`,
      ``,
      `- **Source:** Intel Brand Relationships (Currently Operates / BMC) ∪ Assignments.Brand`,
      `- **Min evidence:** ≥1 brand name`,
      `- **Taxonomy:** ensure select options before write`,
      ``,
      `## Development experience flags/counts`,
      ``,
      `- **Source:** Assignments.Development Context distinct values`,
      `- **Map:** New Build → newBuild; Conversion/Reflag/Repositioning → conversion; etc.`,
      ``,
      `## Urban / Resort / AI / Extended Stay`,
      ``,
      `- **Source:** Assignments Urban/Resort + checkbox flags`,
      `- **Rule:** Yes if ≥1 current assignment matches`,
      ``,
      `## Management Structures Supported`,
      ``,
      `- **Source:** distinct Assignments.Operating / Management Structure (+ Master.Operating Model)`,
      `- **Caution:** taxonomy mapping required`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-operator-completeness.md"),
    [
      `# Operator Setup — Operator Completeness (Real Operators)`,
      ``,
      `| Operator | Purpose | Direct Setup % | Derived Potential % | Linked 1:1 tabs | Intel coverage | True missing score | Setup completeness |`,
      `| -------- | ------- | -------------: | ------------------: | --------------: | -------------: | -----------------: | -----------------: |`,
      ...operatorCompleteness.map(
        (o) =>
          `| ${o.operator} | ${o.purpose} | ${o.directSetupPct} | ${o.derivedSetupPotentialPct} | ${o.researchedSummaryLinkedTabs}/4 | ${o.intelligenceCoverage} | ${o.trueMissingScore} | ${o.setupCompleteness} |`
      ),
      ``,
      `**Current Production avg operator-level completeness heuristic:** ${Math.round((prodCompleteness.reduce((s, o) => s + o.setupCompleteness, 0) / Math.max(1, prodCompleteness.length)) * 10) / 10}%`,
      `**Active DIRECT+DERIVED field fill (KPI):** ${currentMeaningful}% → projected **${projected}%**`,
      `**Production operators with Active Countries sync gap (OE has countries, Setup blank):** ${prodCompleteness.filter((o) => o.syncGap).length}`,
      ``,
      `## Production operator review (36)`,
      ``,
      `For each Production operator: Direct Setup already partially populated vs OE-derived potential vs true research gaps.`,
      ``,
      `| Operator | Setup already | Should direct-populate | Safely derivable now | Needs research | Leave blank / retire |`,
      `| -------- | ------------- | ---------------------- | -------------------- | -------------- | -------------------- |`,
      ...prodCompleteness.map((o) => {
        const already = [
          o.hasProfile ? "Profile row" : null,
          o.hasPlatform ? "Platform row" : null,
          o.directSetupPct >= 50 ? `Direct~${o.directSetupPct}%` : null,
        ]
          .filter(Boolean)
          .join("; ") || "Thin";
        const direct = o.directSetupPct < 60 ? "Website/HQ/year/OM/MA if sourced" : "Mostly done";
        const derive = [
          o.oe.currentCountries.length ? "Countries" : null,
          o.oe.brands.length ? "Brands" : null,
          o.oe.urbanResort.length ? "Urban/Resort" : null,
          o.oe.developmentContexts.length ? "Dev context" : null,
        ]
          .filter(Boolean)
          .join("; ") || "—";
        const research = o.trueMissingScore >= 2 ? "Deep narratives / leadership / commercial prefs" : "Selective narratives only";
        const leave = "Fit prefs; workflow QA; Strategic Interest as Active";
        return `| ${o.operator} | ${already} | ${direct} | ${derive} | ${research} | ${leave} |`;
      }),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-direct-backfill-plan.md"),
    [
      `# Direct Company-Fact Backfill Plan`,
      ``,
      `Safely backfill only when verified:`,
      ``,
      `- Master: Operating Model, Management Availability, Operator Website, Parent, Aliases (many already set in OE waves)`,
      `- Profile: website, headquarters, yearEstablished, companyDescription, companyTagline — from official sites / existing registries (\`operator-setup-*-registry.js\`)`,
      ``,
      `Do **not** invent founded year, ownership type, or public/private without sources.`,
      ``,
      `Priority: Production operators missing website/OM/MA first.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-fields-that-should-remain-unpopulated.md"),
    [
      `# Fields That Should Remain Unpopulated`,
      ``,
      `- All **FIT-SPECIFIC** Commercial \`bf_*\`, ideal project, markets to avoid, fee expectation vs market (unless company states general policy)`,
      `- Workflow QA checkboxes / review assignees`,
      `- Unsupported governance assumptions`,
      `- Vague differentiators without Claims evidence`,
      `- Manual country/brand lists when derivation is safer`,
      `- Strategic Interest as Active Countries`,
      ``,
      `Count classified FIT-SPECIFIC: **${fieldClassifications.filter((f) => f.classification === "FIT-SPECIFIC").length}**`,
      `Count WORKFLOW ONLY: **${fieldClassifications.filter((f) => f.classification === "WORKFLOW ONLY").length}**`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-deprecation-candidates.md"),
    [
      `# Deprecation Candidates`,
      ``,
      `## Safe to deprecate later`,
      ``,
      `- None physically — consumers still reference Case Studies / Setup BR section rows`,
      ``,
      `## Retain for compatibility`,
      ``,
      `- Operator Setup - Brand Relationships (Explorer section rows)`,
      `- Operating / Engagement / Infra / Leadership Platform section tables`,
      ``,
      `## Needs migration first`,
      ``,
      `- **Case Studies** → Assignments (+ optional story Claims)`,
      `- Manual Platform geo/brand experience fields → derived sync`,
      ``,
      `## Unknown / founder decision`,
      ``,
      `- Whether Setup Brand Relationships table should be renamed to avoid confusion with Intel Brand Relationships`,
      ``,
      `**No deletions in this audit.**`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-to-fit-consumer-map.md"),
    [
      `# Operator Setup → Fit Consumer Map (Diagnostic)`,
      ``,
      `| Fit domain | Setup hint | Setup pop % | OE available % | Gap class |`,
      `| ---------- | ---------- | ----------: | -------------: | --------- |`,
      ...fitConsumers.map(
        (f) =>
          `| ${f.label} | ${f.airtableHint} | ${f.setupPopulationPct ?? "—"} | ${f.normalizedOeAvailablePct ?? "—"} | ${f.gapClass} |`
      ),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-fit-readiness-root-cause.md"),
    [
      `# Fit Readiness Root Cause (from Setup audit)`,
      ``,
      `Two different “Fit Ready” concepts exist:`,
      ``,
      `1. **OE diagnostic** \`classifyFitDataReadinessDiagnostic\`: asg≥6 && mpRows≥3 && brRows≥2 → currently **4** operators`,
      `2. **Operator Fit Ranking Ready** (enrichment catalog): requires Setup fields like Active Countries, Management Structures, chainScales, project experience`,
      ``,
      `## Gap class counts (Fit enrichment catalog)`,
      ``,
      ...Object.entries(gapClassCounts).map(([k, v]) => `- **${k}:** ${v}`),
      ``,
      `## Primary explanation`,
      ``,
      `Fit Data Ready (OE diagnostic) stays at 4 because thresholds exceed Explorer Strong and many Strong profiles have asg=5 or thin BR/MP **row** counts.`,
      ``,
      `Separately, Fit Ranking Ready stays blocked because Fit still reads **sparse Setup form fields** even when normalized OE already has geography/brand/assignment evidence — i.e. **DATA EXISTS — SETUP NOT BACKFILLED / NOT MAPPED**.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(DOCS, "data/operator-setup-cleanup-strategy.md"),
    [
      `# Operator Setup Cleanup Strategy`,
      ``,
      `## Phase A — Safe direct backfill`,
      `Verified website/parent/OM/MA/HQ/year where sourced.`,
      ``,
      `## Phase B — Derived summary sync`,
      `Active Countries, brand families, urban/resort/AI/dev experience from Assignments/Presence/BR.`,
      ``,
      `## Phase C — Researched summary backfill`,
      `Explorer narratives / section rows for priority Production profiles (not all 597 fields).`,
      ``,
      `## Phase D — Consumer migration`,
      `Point Fit adapters to prefer Market Presence / Assignments / Intel BR before Setup form fields.`,
      ``,
      `## Phase E — Deprecation`,
      `Case Studies migration; clarify Setup vs Intel Brand Relationships naming — after consumers move.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-backfill-write-plan.md"),
    [
      `# Operator Setup Backfill Write Plan (DRY-RUN ONLY)`,
      ``,
      `**Not applied.** See \`data/operator-setup/audit/operator-setup-backfill-write-plan.json\`.`,
      ``,
      `| Metric | Count |`,
      `| ------ | ----: |`,
      `| Proposed mutations | ${backfillPlan.mutations.length} |`,
      `| Derived Active Countries | ${backfillPlan.summary.derivedActiveCountries} |`,
      `| Derived Brand Families | ${backfillPlan.summary.derivedBrandFamilies} |`,
      `| Derived location/dev flags | ${backfillPlan.summary.derivedLocationDevFlags} |`,
      ``,
      `## Sample`,
      ``,
      ...backfillPlan.mutations.slice(0, 25).map(
        (m) =>
          `- ${m.masterName}: \`${m.table}\`.\`${m.field}\` ← ${JSON.stringify(m.proposedValue)} (${m.treatment}; ${m.source})`
      ),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-cleanup-completeness-projection.md"),
    [
      `# Cleanup Completeness Projection`,
      ``,
      `| Metric | Value |`,
      `| ------ | ----- |`,
      `| Current active DIRECT+DERIVED Prod fill (Master + 1:1 tables) | **${currentMeaningful}%** |`,
      `| Projected after safe direct + derived sync | **${projected}%** |`,
      `| Active fields in KPI | ${activeFields.length} |`,
      `| Excluded from KPI | workflow / Fit-specific / obsolete / Explorer section-row body fields |`,
      ``,
      `Raw fill across all ${totalFields} fields is a poor KPI — many fields are correctly blank.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-founder-table-verdict.md"),
    [
      `# Founder Table Verdict`,
      ``,
      `| Setup Table | Current State | Why Empty | Keep? | Populate? | Derive? | Deprecate Later? |`,
      `| ----------- | ------------- | --------- | ----- | --------- | ------- | ---------------- |`,
      ...tableSummaries.map((t) => {
        const why =
          t.table.includes("Diligence")
            ? "Workflow"
            : t.meaningfulProductionFillAvg < 35
              ? "OE intel not synced + golden-only deepen"
              : "Partial deepen";
        return `| ${t.table.replace("Operator Setup - ", "")} | ${t.apparentStatus} / ${t.tableClass} | ${why} | Yes | ${t.table.includes("Diligence") ? "No" : "Selective"} | ${/Platform|Profile|Commercial/.test(t.table) ? "Yes" : "No"} | ${t.table.includes("Case Studies") ? "Yes" : "No"} |`;
      }),
      ``,
    ].join("\n")
  );

  // Quantify for founder review
  const classCounts = {};
  for (const f of fieldClassifications) classCounts[f.classification] = (classCounts[f.classification] || 0) + 1;
  const meaningfulFields = fieldClassifications.filter((f) => !["WORKFLOW ONLY", "FIT-SPECIFIC", "OBSOLETE / DUPLICATE"].includes(f.classification));
  const criticallySparseProd = meaningfulFields.filter((f) => f.productionBand === "Critically Sparse").length;

  writeMd(
    join(DOCS, "reviews/operator-setup-full-audit-founder-review.md"),
    [
      `# Operator Setup Full Audit — Founder Review`,
      ``,
      `## Why this audit was needed`,
      ``,
      `Operator Explorer intelligence is mature (36 Publishable Production, Assignments/Presence/BR filled), but **Operator Setup form tables still look empty** in Airtable — blocking confidence before Fit v2.1.`,
      ``,
      `## Snapshot`,
      ``,
      `| Item | Count |`,
      `| ---- | ----: |`,
      `| Operator Setup tables | ${SETUP_TABLES.length} |`,
      `| Setup fields | ${totalFields} |`,
      `| Masters | ${universe.summary.totalMasters} (Prod ${universe.summary.production} / Research ${universe.summary.research} / TF ${universe.summary.testFixtures}) |`,
      `| Meaningful fields critically sparse (Prod) | ${criticallySparseProd} |`,
      `| Current active DIRECT+DERIVED Prod completeness | ${currentMeaningful}% |`,
      `| Projected after cleanup | ${projected}% |`,
      `| Active DIRECT+DERIVED fields in KPI | ${activeFields.length} |`,
      `| Dry-run backfill mutations proposed | ${backfillPlan.mutations.length} |`,
      ``,
      `## Why are my Operator Setup tables still empty?`,
      ``,
      `Quantified root causes among **all** sparse Production fields (${sparseAll.length}):`,
      ``,
      ...Object.entries(causePct)
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => `- **${causeLabels[k]} (${k}):** ${v}%`),
      ``,
      `Meaningful-only sparse (${sparseMeaningful.length}): research missing (A) ${causePctMeaningful.A || 0}% · pipeline (F) ${causePctMeaningful.F || 0}% · deferred derivation (C) ${causePctMeaningful.C || 0}% · exists elsewhere (B) ${causePctMeaningful.B || 0}%.`,
      ``,
      `In plain language:`,
      ``,
      `1. **Research moved into Operator Intelligence** and was **not written back** into Setup summary fields (sync/derivation gap).`,
      `2. **Derivation was deferred** by design when OE normalized entities launched.`,
      `3. **Explorer deepen packs** heavily filled Arbor / Hotel Equities / a few goldens — not the full Production universe.`,
      `4. Many blanks are **workflow or Fit-preference fields** that should stay empty.`,
      `5. Genuine “never researched” blanks are a minority once workflow/section pipeline gaps are separated.`,
      ``,
      `## Classifications`,
      ``,
      ...Object.entries(classCounts).map(([k, v]) => `- **${k}:** ${v}`),
      ``,
      `## Table verdict`,
      ``,
      `See \`reports/operator-setup-founder-table-verdict.md\`.`,
      ``,
      `## Source-of-truth`,
      ``,
      `See \`docs/data/operator-setup-source-of-truth-policy.md\` — Setup becomes stable facts + derived summaries; OE intel remains evidence SoT.`,
      ``,
      `## Fit`,
      ``,
      `Fit still keys off sparse Setup fields (Active Countries, structures, chain scales) while OE already has the data → **mapping/backfill problem**, plus OE diagnostic threshold asg≥6 keeps Fit Data Ready diagnostic at 4.`,
      ``,
      `## Proposed cleanup phases`,
      ``,
      `A Direct → B Derived sync → C Researched summaries → D Fit consumer migration → E Deprecation.`,
      ``,
      `## Exact founder approvals required`,
      ``,
      `1. Source-of-truth policy`,
      `2. Field classifications`,
      `3. Table keep/populate/derive/deprecate verdicts`,
      `4. Direct backfill`,
      `5. Derived summary sync`,
      `6. Researched-summary backfill scope`,
      `7. Fields intentionally blank`,
      `8. Deprecation candidates`,
      `9. Fit consumer migration direction`,
      `10. Cleanup apply phase`,
      ``,
      `## Recommended next phase`,
      ``,
      `**Phase A+B apply (safe direct + derived Setup sync)** after approvals — still before broad Fit v2.1 scoring changes. Optionally parallel: Fit adapter preference for OE intel (Path A from graduation).`,
      ``,
      `## Confirmations`,
      ``,
      `- **No Airtable cleanup writes** in this audit`,
      `- **No Operator Fit / scoring changes**`,
      `- Owner pilot remains disabled`,
      ``,
    ].join("\n")
  );

  // Stop-point JSON (full §31 return)
  const tablesKeep = tableSummaries.length;
  const tablesPopulate = tableSummaries.filter((t) => !t.table.includes("Diligence")).length;
  const tablesDerive = tableSummaries.filter((t) => /Platform|Profile|Commercial|Infrastructure|Leadership Platform|Operating Platform/.test(t.table)).length;
  const tablesDeprecateLater = tableSummaries.filter((t) => t.table.includes("Case Studies")).length;
  const remainBlankFields =
    (classCounts["FIT-SPECIFIC"] || 0) + (classCounts["WORKFLOW ONLY"] || 0) + (classCounts["OBSOLETE / DUPLICATE"] || 0);
  const fitExistsSetupNotBackfilled = Object.entries(gapClassCounts)
    .filter(([k]) => /SETUP NOT BACKFILLED|NORMALIZED OE NOT MAPPED/i.test(k))
    .reduce((s, [, v]) => s + v, 0);
  const fitGenuineMissing = Object.entries(gapClassCounts)
    .filter(([k]) => /DATA MISSING|UNKNOWN/i.test(k))
    .reduce((s, [, v]) => s + v, 0);
  const fitSparseSetup = Object.entries(gapClassCounts)
    .filter(([k]) => /SETUP FORM FIELDS|SPARSE/i.test(k))
    .reduce((s, [, v]) => s + v, 0);

  const stopPoint = {
    // §31 items 1–30
    totalSetupTables: SETUP_TABLES.length,
    totalSetupFields: totalFields,
    activeTables: tableSummaries.filter((t) => /Active/i.test(t.apparentStatus)).length,
    legacyWorkflowTables: tableSummaries.filter((t) => ["Legacy", "Workflow"].includes(t.apparentStatus)).length,
    productionFieldsMateriallySparse: criticallySparseProd,
    pctBlanksResearchMissing: causePct.A || 0,
    pctBlanksExistsElsewhere: causePct.B || 0,
    pctBlanksDeferredDerivation: causePct.C || 0,
    pctBlanksLegacyDuplicate: (causePct.D || 0) + (causePct.E || 0),
    pctCorrectlyBlank: causePct.I || 0,
    pctBlanksWriterPipelineMissing: causePct.F || 0,
    directFieldsSafelyBackfillable: classCounts.DIRECT || 0,
    derivedFieldsSafelyBackfillable: classCounts.DERIVED || 0,
    researchedSummariesSafelyBackfillable: classCounts["RESEARCHED SUMMARY"] || 0,
    genuineMissingDataFields: Math.round(((causePctMeaningful.A || 0) / 100) * sparseMeaningful.length),
    fieldsRecommendedRemainBlank: remainBlankFields,
    tablesRecommendedKeep: tablesKeep,
    tablesRecommendedPopulate: tablesPopulate,
    tablesRecommendedDerive: tablesDerive,
    tablesRecommendedDeprecateLater: tablesDeprecateLater,
    currentMeaningfulProductionCompleteness: currentMeaningful,
    projectedCompletenessAfterCleanup: projected,
    fitFieldsRelyingOnSparseSetup: fitSparseSetup,
    fitRequirementsDataExistsInOe: fitExistsSetupNotBackfilled,
    fitRequirementsGenuinelyMissingData: fitGenuineMissing,
    primaryExplanationFitDataReady4:
      "OE diagnostic requires asg≥6 && mp≥3 && br≥2 (stricter than Strong); Fit Ranking Ready also blocked by sparse Setup form fields while OE intel already holds geography/brand/assignment evidence",
    proposedCleanupWriteVolume: backfillPlan.mutations.length,
    exactFounderApprovalsRequired: [
      "Source-of-truth policy",
      "Field classifications",
      "Table keep/populate/derive/deprecate verdicts",
      "Direct backfill",
      "Derived summary sync",
      "Researched-summary backfill scope",
      "Fields intentionally blank",
      "Deprecation candidates",
      "Fit consumer migration direction",
      "Cleanup apply phase",
    ],
    recommendedNextPhase:
      "Phase A+B apply (safe direct + derived Setup sync) after founder approvals — then Fit adapter remapping; no scoring changes yet",
    confirmationNoAirtableCleanupWrites: true,
    confirmationNoOperatorFitScoringChanges: true,
    classCounts,
    causePctAllSparse: causePct,
    causePctMeaningfulSparse: causePctMeaningful,
    activeDirectDerivedFieldCount: activeFields.length,
    fitGapClassCounts: gapClassCounts,
    masterBaseline: universe.summary,
    backfillSummary: backfillPlan.summary,
  };
  writeJson(join(OUT, "operator-setup-audit-stop-point.json"), stopPoint);

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
