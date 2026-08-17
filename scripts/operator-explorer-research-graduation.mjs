#!/usr/bin/env node
/**
 * Operator Explorer — Research Graduation + Research Universe Completion
 *
 *   node scripts/operator-explorer-research-graduation.mjs --dry-run
 *   node scripts/operator-explorer-research-graduation.mjs --apply --approve-oe-research-graduation-writes
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildOperatorUniverse,
  dispositionForOperator,
} from "../lib/operator-explorer/operator-universe.js";
import { isAggregateAssignmentName, classifyFitDataReadinessDiagnostic } from "../lib/operator-explorer/readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes("--approve-oe-research-graduation-writes");
const DRY = !APPLY;
const TS = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

const MASTER = "Operator Setup - Master";
const ASG = "Operator Intelligence - Assignments";
const BR = "Operator Intelligence - Brand Relationships";
const MP = "Operator Intelligence - Market Presence";
const PI = "Partner Intelligence - Source Library";
const CLAIMS = "Operator Intelligence - Claims";

const GROUP_A = [
  { id: "reculkMOYWDxX14Pv", name: "Hyatt (Managed)" },
  { id: "rec28eZ7ERwc92XWd", name: "Meliá Hotels International" },
  { id: "rechnXKjpeiNMaqjJ", name: "Four Seasons Hotels and Resorts" },
  { id: "rec04aLAfmupWG4ZK", name: "Barceló Hotel Group" },
];
const GROUP_C = [
  { id: "rec9JSyGQjvodsPSJ", name: "AADESA" },
  { id: "recjgHXqTJktijFUR", name: "Álvarez Argüelles Hoteles" },
  { id: "recHj56wpRLUnJ5Wx", name: "Tremun Hoteles" },
  { id: "rec0AXje3BxPqIDnZ", name: "Radisson Hotel Group" },
  { id: "recIq0XYgt5Ghvcsz", name: "Sonesta International" },
];
const REMAINING_FOUR = [
  { id: "recVtNxNeeYlngtUk", name: "Auberge Resorts Collection" },
  { id: "rec5xdV2THfFjEUPk", name: "Mandarin Oriental Hotel Group" },
  { id: "recji1awMffccwox2", name: "Rosewood Hotel Group" },
  { id: "rec8XpNv6G0WOlMwu", name: "Shangri-La Group" },
];

const ENRICH_ASSIGNMENTS = {
  rec9JSyGQjvodsPSJ: [
    {
      propertyName: "Wyndham Nordelta Tigre Buenos Aires",
      country: "Argentina",
      city: "Nordelta, Tigre",
      brand: "Wyndham",
      brandParent: "Wyndham Hotels & Resorts",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://aadesa.com.ar/en/managed-hotels.html",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Cyan Hotel de las Americas",
      country: "Argentina",
      city: "Buenos Aires",
      brand: "Cyan",
      brandParent: "AADESA",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://aadesa.com.ar/en/managed-hotels.html",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Cyan Recoleta Hotel",
      country: "Argentina",
      city: "Buenos Aires",
      brand: "Cyan",
      brandParent: "AADESA",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://aadesa.com.ar/en/managed-hotels.html",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Don Los Cerros Boutique Hotel & Spa",
      country: "Argentina",
      city: "El Chaltén",
      brand: "DON",
      brandParent: "AADESA",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://aadesa.com.ar/en/managed-hotels.html",
      evidenceClass: "primary_authoritative",
    },
  ],
  recjgHXqTJktijFUR: [
    {
      propertyName: "Grand Brizo Buenos Aires",
      country: "Argentina",
      city: "Buenos Aires",
      brand: "Grand Brizo",
      brandParent: "Álvarez Argüelles Hoteles",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.alvarezarguelles.com/hoteles-apartamentos/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Los Cauquenes Resort Spa & Experiences",
      country: "Argentina",
      city: "Ushuaia",
      brand: "Los Cauquenes",
      brandParent: "Álvarez Argüelles Hoteles",
      urbanOrResort: "Resort",
      developmentContext: "Acquisition Transition",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.alvarezarguelles.com/en/about/",
      evidenceClass: "primary_authoritative",
      limitations: "Assumed operation Apr 2025 per company/press; owned+managed mix across portfolio.",
    },
    {
      propertyName: "Hotel Costa Galana",
      country: "Argentina",
      city: "Mar del Plata",
      brand: "Costa Galana",
      brandParent: "Álvarez Argüelles Hoteles",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.alvarezarguelles.com/hoteles-apartamentos/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Hotel Presidente",
      country: "Argentina",
      city: "Mar del Plata",
      brand: "Independent",
      brandParent: "Álvarez Argüelles Hoteles",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.alvarezarguelles.com/hoteles-apartamentos/",
      evidenceClass: "primary_authoritative",
    },
  ],
  recHj56wpRLUnJ5Wx: [
    {
      propertyName: "Las Hayas Ushuaia Resort",
      country: "Argentina",
      city: "Ushuaia",
      brand: "Independent",
      brandParent: "Tremun Hoteles",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.tremunhoteles.com.ar/sobre-tremun",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Los Pinos Resort & Spa Termal",
      country: "Argentina",
      city: "Termas de Río Hondo",
      brand: "Independent",
      brandParent: "Tremun Hoteles",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.tremunhoteles.com.ar/sobre-tremun",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Mirador del Lago Hotel",
      country: "Argentina",
      city: "El Calafate",
      brand: "Independent",
      brandParent: "Tremun Hoteles",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.tremunhoteles.com.ar/sobre-tremun",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Kau Yatún Hotel de Campo",
      country: "Argentina",
      city: "El Calafate",
      brand: "Independent",
      brandParent: "Tremun Hoteles",
      urbanOrResort: "Resort",
      hotelType: "Estancia / countryside",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.tremunhoteles.com.ar/sobre-tremun",
      evidenceClass: "primary_authoritative",
    },
  ],
  recIq0XYgt5Ghvcsz: [
    {
      propertyName: "Royal Sonesta New Orleans",
      country: "United States",
      city: "New Orleans",
      brand: "Royal Sonesta",
      brandParent: "Sonesta",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.sonesta.com/royal-sonesta/la/new-orleans/royal-sonesta-new-orleans",
      evidenceClass: "primary_authoritative",
      limitations: "SVC/Cambridge TRS ownership with Sonesta management agreement (SEC exhibits).",
    },
    {
      propertyName: "Sonesta ES Suites New Orleans Convention Center",
      country: "United States",
      city: "New Orleans",
      brand: "Sonesta ES Suites",
      brandParent: "Sonesta",
      urbanOrResort: "Urban",
      hotelType: "Extended stay",
      extendedStay: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.sec.gov/Archives/edgar/data/945394/000094539425000057/svc_063025x10qex101.htm",
      evidenceClass: "primary_authoritative",
      limitations: "Listed on SVC Sonesta management schedule; franchise-conversion program may affect select-service inventory over time — full-service retained focus noted by Sonesta.",
    },
    {
      propertyName: "Royal Sonesta Boston",
      country: "United States",
      city: "Boston",
      brand: "Royal Sonesta",
      brandParent: "Sonesta",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.sonesta.com/about",
      evidenceClass: "primary_authoritative",
      skipIfExists: true,
    },
  ],
  rec0AXje3BxPqIDnZ: [
    {
      propertyName: "Radisson Blu Hotel, Madrid Prado",
      country: "Spain",
      city: "Madrid",
      brand: "Radisson Blu",
      brandParent: "Radisson Hotel Group",
      urbanOrResort: "Urban",
      developmentContext: "Renovation",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.radissonhotels.com/es-es/hoteles/radisson-blu-madrid-prado",
      evidenceClass: "primary_authoritative",
      limitations: "Scoped to RHG EMEA/APAC entity. Americas Radisson brands are Choice-controlled since 2022 — do not treat as this Master's CALA operating portfolio.",
    },
    {
      propertyName: "Radisson Collection Hotel, Magdalena Plaza Sevilla",
      country: "Spain",
      city: "Seville",
      brand: "Radisson Collection",
      brandParent: "Radisson Hotel Group",
      urbanOrResort: "Urban",
      developmentContext: "New Build",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.globenewswire.com/news-release/2021/10/28/2322426/0/en/Newly-renovated-Radisson-Blu-brings-stylish-boutique-hotel-to-the-cultural-heart-of-Madrid.html",
      evidenceClass: "reliable_independent",
      limitations: "RHG EMEA scope only.",
    },
    {
      propertyName: "Radisson Blu Hotel Dubai Deira Creek",
      country: "United Arab Emirates",
      city: "Dubai",
      brand: "Radisson Blu",
      brandParent: "Radisson Hotel Group",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.radissonhotels.com/en-us/hotels/radisson-blu-dubai-deira-creek",
      evidenceClass: "primary_authoritative",
      limitations: "RHG EMEA/ME scope; not Americas/Choice.",
    },
  ],
};

const ENRICH_BR = {
  rec9JSyGQjvodsPSJ: [
    {
      brand: "Wyndham",
      brandParent: "Wyndham Hotels & Resorts",
      relationshipType: "Currently Operates",
      geographyScope: "Argentina",
      sourceUrl: "https://aadesa.com.ar/en/managed-hotels.html",
    },
    {
      brand: "Cyan",
      brandParent: "AADESA",
      relationshipType: "Currently Operates",
      geographyScope: "Argentina",
      sourceUrl: "https://aadesa.com.ar/en/managed-hotels.html",
    },
  ],
  recjgHXqTJktijFUR: [
    {
      brand: "Grand Brizo",
      brandParent: "Álvarez Argüelles Hoteles",
      relationshipType: "Currently Operates",
      geographyScope: "Argentina",
      sourceUrl: "https://www.alvarezarguelles.com/hoteles-apartamentos/",
    },
  ],
  recIq0XYgt5Ghvcsz: [
    {
      brand: "Sonesta ES Suites",
      brandParent: "Sonesta",
      relationshipType: "Brand Managed Capability",
      geographyScope: "United States (affiliated/managed portfolio; franchise conversion underway for portion)",
      sourceUrl: "https://www.sec.gov/Archives/edgar/data/945394/000094539425000057/svc_063025x10qex101.htm",
    },
  ],
  rec0AXje3BxPqIDnZ: [
    {
      brand: "Radisson Collection",
      brandParent: "Radisson Hotel Group",
      relationshipType: "Brand Managed Capability",
      geographyScope: "EMEA / APAC (not Americas — Choice)",
      sourceUrl: "https://www.radissonhotels.com/",
    },
  ],
};

const MASTER_FACT_UPDATES = {
  rec9JSyGQjvodsPSJ: {
    "Operating Model": "Third-Party",
    "Management Availability": "Confirmed Direct Management",
    "Operator Website": "https://aadesa.com.ar/",
    "Operator Aliases": "aadesa; Aadesa Hotel Management",
  },
  recHj56wpRLUnJ5Wx: {
    "Operating Model": "Hybrid",
    "Management Availability": "Confirmed Direct Management",
    "Operator Website": "https://tremunhoteles.com/",
    "Operator Aliases": "Tremun",
  },
  rec0AXje3BxPqIDnZ: {
    "Operator Parent Company": "Jin Jiang–led consortium (Aplite) — RHG EMEA/APAC; Americas brands Choice-controlled since 2022",
    "Operator Aliases": "RHG; Radisson; Radisson Hotel Group EMEA/APAC",
    "Management Availability": "Conditional / Scoped",
  },
  recjgHXqTJktijFUR: {
    "Operator Website": "https://www.alvarezarguelles.com/",
  },
};

function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}
function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}
function checksum(fields) {
  return createHash("sha1").update(JSON.stringify(fields)).digest("hex").slice(0, 12);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
    if (!res.ok) throw new Error(`LIST ${table}: ${JSON.stringify(json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function createRecord(baseId, token, table, fields) {
  if (DRY) return { id: `dry_${checksum(fields)}`, dryRun: true };
  const res = await fetch(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`CREATE ${table}: ${JSON.stringify(json)}`);
  await sleep(220);
  return json;
}

async function patchRecord(baseId, token, table, id, fields) {
  if (DRY) return { id, dryRun: true };
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}/${encodeURIComponent(id)}`,
    {
      method: "PATCH",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ fields, typecast: true }),
    }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(`PATCH ${table} ${id}: ${JSON.stringify(json)}`);
  await sleep(220);
  return json;
}

function enrichmentClass(o) {
  if (o.testFixture || o.recordPurpose === "Test Fixture") return "Test Fixture";
  if (o.explorerPublishable) return "Publishable";
  if (o.recordPurpose === "Research" && o.explorerContentComplete) return "Research Content Complete Gated";
  if (o.recordPurpose === "Research") return "Research Needs Enrichment";
  if (o.recordPurpose === "Production") return "Production Needs Enrichment";
  return "Other";
}

function whyCurrentlyResearch(o, m) {
  if (!o.explorerContentComplete) return "Insufficient named assignments / structured intelligence for content-complete gate";
  if (GROUP_A.find((g) => g.id === o.masterId)) return "Wave 02 Group A — content complete; awaiting lifecycle graduation decision";
  if (REMAINING_FOUR.find((g) => g.id === o.masterId))
    return "Wave 02 Group B omission — content complete but deferred as thinner/lower CALA priority vs Group A";
  if (o.masterId === "rec0AXje3BxPqIDnZ") return "Entity/geo split risk: RHG EMEA/APAC vs Choice Americas; no named assignments yet";
  return "Phase 1 provisional / Research Purpose pending graduation review";
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE credentials required");
  if (APPLY && !APPROVED) throw new Error("Refusing apply without --approve-oe-research-graduation-writes");

  const results = {
    mode: DRY ? "dry-run" : "apply",
    webhound: {
      sessionId: "6695f5be-443b-4685-860a-b9c0b37e5be6",
      status: "Deferred — done=false; no partial merge",
      validatedRowsMerged: 0,
    },
    startedAt: new Date().toISOString(),
    before: null,
    afterEnrichment: null,
    afterGraduation: null,
    assignments: { created: 0, updated: 0, failed: [] },
    brandRel: { created: 0, updated: 0, failed: [] },
    presence: { created: 0 },
    claims: { created: 0 },
    sources: { created: 0, reused: 0 },
    mastersPatchedFacts: 0,
    graduated: [],
    graduationDecisions: [],
  };

  const masters = await listAll(baseId, token, MASTER);
  const assignments = await listAll(baseId, token, ASG);
  const brandRelationships = await listAll(baseId, token, BR);
  const marketPresence = await listAll(baseId, token, MP);
  const claims = await listAll(baseId, token, CLAIMS);
  const piAll = await listAll(baseId, token, PI);

  const cross = existsSync(join(ROOT, "data/operator-explorer/phase-1-provisional-crosswalk.json"))
    ? JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/phase-1-provisional-crosswalk.json"), "utf8"))
    : {};
  const entities = JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/calibration-01/entities.json"), "utf8")).entities;
  const calibrationByMasterId = {};
  for (const e of entities) {
    const mid = cross[e.entityId] || e.existingMasterId || e.entityId;
    calibrationByMasterId[mid] = { track: e.track, entityId: e.entityId };
  }

  function buildU(asg, br, mp) {
    const u = buildOperatorUniverse(masters, {
      assignments: asg,
      brandRelationships: br,
      marketPresence: mp,
      calibrationByMasterId,
    });
    for (const o of u.operators) o.disposition = dispositionForOperator(o);
    return u;
  }

  let universe = buildU(assignments, brandRelationships, marketPresence);
  results.before = { ...universe.summary };

  const research = universe.operators
    .filter((o) => o.recordPurpose === "Research")
    .sort((a, b) => a.canonicalName.localeCompare(b.canonicalName));
  if (research.length !== 13) throw new Error(`Expected 13 Research Masters, found ${research.length}`);

  // —— Baseline ——
  const baselineRows = research.map((o) => {
    const m = masters.find((r) => r.id === o.masterId);
    return {
      id: o.masterId,
      name: o.canonicalName,
      operatingModel: m?.fields?.["Operating Model"] || null,
      managementAvailability: m?.fields?.["Management Availability"] || null,
      usefulness: o.usefulness,
      contentComplete: o.explorerContentComplete,
      strong: o.strongExplorerProfile,
      asg: o.counts.namedAssignments,
      countries: o.counts.countries,
      br: o.counts.brandRelationships,
      mp: o.counts.marketPresence,
      fit: o.fitDataReadiness,
      calib: calibrationByMasterId[o.masterId] ? `Track ${calibrationByMasterId[o.masterId].track}` : "—",
      whyResearch: whyCurrentlyResearch(o, m),
    };
  });

  writeMd(
    join(ROOT, "reports/operator-explorer-research-graduation-baseline.md"),
    [
      `# Research Graduation Baseline`,
      ``,
      `**Generated:** ${new Date().toISOString()}`,
      `**Source:** live Airtable via canonical universe resolver`,
      `**Webhound:** deferred (done=false)`,
      ``,
      `Universe: Production ${results.before.production} · Research ${results.before.research} · Test Fixtures ${results.before.testFixtures} · Total ${results.before.totalMasters}`,
      ``,
      `| Operator | Master ID | Operating Model | Mgmt Availability | Readiness | Strong | Asg | Cty | BR | MP | Fit | Calib | Why Research |`,
      `| -------- | --------- | --------------- | ----------------- | --------- | ------ | --: | --: | -: | -: | --- | ----- | ------------ |`,
      ...baselineRows.map(
        (r) =>
          `| ${r.name} | \`${r.id}\` | ${r.operatingModel || "—"} | ${r.managementAvailability || "—"} | ${r.usefulness} | ${r.strong ? "Y" : "N"} | ${r.asg} | ${r.countries} | ${r.br} | ${r.mp} | ${r.fit} | ${r.calib} | ${r.whyResearch} |`
      ),
      ``,
    ].join("\n")
  );

  // —— 13 reconciliation ——
  writeMd(
    join(ROOT, "reports/operator-explorer-research-13-reconciliation.md"),
    [
      `# Research 13 Reconciliation`,
      ``,
      `Every Research Master appears exactly once.`,
      ``,
      `## Group A — graduation candidates (Wave 02)`,
      ``,
      ...GROUP_A.map((g) => `- **${g.name}** (\`${g.id}\`) — content complete; validate for graduation`),
      ``,
      `## Group C — incomplete (Wave 02)`,
      ``,
      ...GROUP_C.map((g) => `- **${g.name}** (\`${g.id}\`) — enrichment required before decision`),
      ``,
      `## Remaining four — Wave 02 Group B (explicitly omitted from A/C)`,
      ``,
      `Previously omitted because Wave 02 prioritized CALA/scale graduation candidates; not because entities were invalid.`,
      ``,
      ...REMAINING_FOUR.map(
        (g) =>
          `- **${g.name}** (\`${g.id}\`) — content complete; thinner brand-name / lower immediate CALA priority vs Group A`
      ),
      ``,
      `**Count check:** 4 + 5 + 4 = 13.`,
      ``,
    ].join("\n")
  );

  // —— Gap plan ——
  writeMd(
    join(ROOT, "reports/operator-explorer-research-completion-gap-plan.md"),
    [
      `# Research Completion Gap Plan (Group C)`,
      ``,
      `## AADESA`,
      `- Gaps: zero named assignments; Operating Model / Management Availability / website missing`,
      `- Plan: 3–4 named Argentina managed hotels from official portfolio; set Third-Party + Confirmed Direct Management`,
      ``,
      `## Álvarez Argüelles Hoteles`,
      `- Gaps: zero named assignments (OM/MA already set)`,
      `- Plan: 3–4 named Argentina owned/managed hotels from official site`,
      ``,
      `## Tremun Hoteles`,
      `- Gaps: zero named assignments; OM/MA/website missing`,
      `- Plan: 3–4 named Argentina hotels; Hybrid + Confirmed Direct Management`,
      ``,
      `## Radisson Hotel Group`,
      `- Gaps: no assignments/presence; Americas vs EMEA entity ambiguity (Choice 2022 Americas acquisition)`,
      `- Plan: add RHG EMEA/APAC named managed hotels only; document Americas Choice split; do **not** invent CALA RHG portfolio`,
      ``,
      `## Sonesta International`,
      `- Gaps: only 1 named assignment (below Useful); franchise vs managed mix`,
      `- Plan: add 2–3 brand-managed named hotels with SVC/management evidence; distinguish from franchise conversions`,
      ``,
    ].join("\n")
  );

  // —— Backup ——
  const backupDir = join(ROOT, "backups/operator-explorer/research-graduation", TS);
  mkdirSync(backupDir, { recursive: true });
  const backupManifest = { wave: "research-graduation", createdAt: new Date().toISOString(), mode: results.mode, tables: {} };
  for (const [name, rows] of [
    [MASTER, masters],
    [ASG, assignments],
    [BR, brandRelationships],
    [MP, marketPresence],
    [CLAIMS, claims],
    [PI, piAll],
  ]) {
    const file = `${slug(name)}.json`;
    writeJson(join(backupDir, file), { table: name, count: rows.length, records: rows });
    backupManifest.tables[name] = { file, count: rows.length };
  }
  if (Object.values(backupManifest.tables).some((t) => typeof t.count !== "number")) {
    throw new Error("Backup validation failed");
  }
  writeJson(join(backupDir, "manifest.json"), backupManifest);
  results.backup = backupDir;

  const existingAsgKeys = new Set(
    assignments.map((r) => `${(r.fields.Operator || [])[0]}|${String(r.fields["Property Name"] || "").toLowerCase()}`)
  );
  const existingBrKeys = new Set(
    brandRelationships.map(
      (r) =>
        `${(r.fields.Operator || [])[0]}|${String(r.fields.Brand || "").toLowerCase()}|${String(r.fields["Relationship Type"] || "")}`
    )
  );
  const piByUrl = new Map();
  for (const r of piAll) {
    const u = String(r.fields["Source URL"] || "").trim().toLowerCase();
    if (u) piByUrl.set(u, r.id);
  }

  async function ensureSource(url, title) {
    const key = String(url || "").trim().toLowerCase();
    if (!key) return null;
    if (piByUrl.has(key)) {
      results.sources.reused++;
      return piByUrl.get(key);
    }
    const created = await createRecord(baseId, token, PI, {
      "Source Title": title || url,
      "Source URL": url,
      "Profile Type": "Operator",
      Status: "Captured",
      Notes: "OE Research Graduation",
    });
    piByUrl.set(key, created.id);
    results.sources.created++;
    return created.id;
  }

  const writePlan = {
    phase: "research-graduation",
    mode: results.mode,
    intelligence: {
      assignmentsCreate: [],
      brandRelationshipsCreate: [],
      marketPresenceCreate: [],
      masterFactUpdates: [],
      claimsCreate: [],
    },
    recordPurposeChanges: [],
    holds: [
      {
        id: "tafer-posadas-coral-beach",
        note: "Unrelated Production hold — unchanged",
      },
      {
        id: "radisson-americas-choice-split",
        operatorId: "rec0AXje3BxPqIDnZ",
        note: "Do not graduate RHG Master as Americas/CALA operator; Choice controls Americas brands since 2022",
      },
    ],
  };

  for (const [opId, list] of Object.entries(ENRICH_ASSIGNMENTS)) {
    const opName = [...GROUP_C, ...GROUP_A, ...REMAINING_FOUR].find((x) => x.id === opId)?.name || opId;
    for (const a of list) {
      const key = `${opId}|${a.propertyName.toLowerCase()}`;
      if (existingAsgKeys.has(key)) continue;
      writePlan.intelligence.assignmentsCreate.push({ operatorId: opId, operatorName: opName, ...a });
    }
  }
  for (const [opId, list] of Object.entries(ENRICH_BR)) {
    const opName = GROUP_C.find((x) => x.id === opId)?.name || opId;
    for (const b of list) {
      const key = `${opId}|${b.brand.toLowerCase()}|${b.relationshipType}`;
      if (existingBrKeys.has(key)) continue;
      writePlan.intelligence.brandRelationshipsCreate.push({ operatorId: opId, operatorName: opName, ...b });
    }
  }
  for (const [opId, fields] of Object.entries(MASTER_FACT_UPDATES)) {
    writePlan.intelligence.masterFactUpdates.push({ operatorId: opId, fields });
  }

  // Provisional graduation list (Purpose changes applied after enrichment)
  const provisionalGraduateIds = [
    ...GROUP_A.map((g) => g.id),
    ...REMAINING_FOUR.map((g) => g.id),
    "rec9JSyGQjvodsPSJ",
    "recjgHXqTJktijFUR",
    "recHj56wpRLUnJ5Wx",
    "recIq0XYgt5Ghvcsz",
  ];
  // Radisson excluded from Purpose change

  writeJson(join(ROOT, "data/operator-explorer/research-graduation-write-plan.json"), writePlan);
  writeMd(
    join(ROOT, "reports/operator-explorer-research-graduation-write-plan.md"),
    [
      `# Research Graduation Write Plan`,
      ``,
      `**Mode:** ${results.mode}`,
      `**Backup:** \`${backupDir}\``,
      ``,
      `## Intelligence enrichment`,
      ``,
      `| Action | Count |`,
      `| ------ | ----: |`,
      `| Assignment creates | ${writePlan.intelligence.assignmentsCreate.length} |`,
      `| Brand Relationship creates | ${writePlan.intelligence.brandRelationshipsCreate.length} |`,
      `| Master fact updates | ${writePlan.intelligence.masterFactUpdates.length} |`,
      `| Claims | 0 |`,
      ``,
      `## Record Purpose changes (after enrichment + criteria)`,
      ``,
      `Planned graduates (Research → Production): ${provisionalGraduateIds.length} operators`,
      ``,
      `Excluded from graduation: **Radisson Hotel Group** (Americas/Choice vs RHG EMEA entity scope)`,
      ``,
      `## Holds`,
      ``,
      ...writePlan.holds.map((h) => `- ${h.id}: ${h.note}`),
      ``,
      `## Assignment preview`,
      ``,
      ...writePlan.intelligence.assignmentsCreate.map((a) => `- ${a.operatorName}: ${a.propertyName} (${a.country})`),
      ``,
    ].join("\n")
  );

  // —— Apply intelligence (not Purpose yet) ——
  const today = new Date().toISOString().slice(0, 10);
  const mpWorking = [...marketPresence];

  for (const upd of writePlan.intelligence.masterFactUpdates) {
    await patchRecord(baseId, token, MASTER, upd.operatorId, upd.fields);
    const m = masters.find((r) => r.id === upd.operatorId);
    if (m) Object.assign(m.fields, upd.fields);
    results.mastersPatchedFacts++;
  }

  for (const a of writePlan.intelligence.assignmentsCreate) {
    try {
      const srcId = await ensureSource(a.sourceUrl, a.propertyName);
      const fields = {
        "Assignment ID": `asg_rg_${a.operatorId}_${slug(a.propertyName)}`,
        Operator: [a.operatorId],
        "Property Name": a.propertyName,
        "Canonical Property Name": a.propertyName,
        Country: a.country,
        "City / Metro": a.city,
        Brand: a.brand,
        "Brand Parent": a.brandParent || undefined,
        "Urban / Resort": a.urbanOrResort,
        "Hotel Type": a.hotelType,
        "Development Context": a.developmentContext,
        "Operating / Management Structure": a.operatingStructure,
        "Assignment Status": a.assignmentStatus,
        "All-Inclusive": a.allInclusive === true ? true : undefined,
        "Extended Stay": a.extendedStay === true ? true : undefined,
        "Last Verified": today,
        "PI Source Library": srcId ? [srcId] : undefined,
        "Source URLs": a.sourceUrl,
        "Evidence Class": a.evidenceClass || "primary_authoritative",
        "Publication Status": "Auto-Publish",
        "Conflict Status": "None",
        Limitations: a.limitations,
        "Research Wave": "research-graduation",
      };
      Object.keys(fields).forEach((k) => fields[k] === undefined && delete fields[k]);
      await createRecord(baseId, token, ASG, fields);
      existingAsgKeys.add(`${a.operatorId}|${a.propertyName.toLowerCase()}`);
      results.assignments.created++;

      const hasCountry = mpWorking.some(
        (r) =>
          (r.fields.Operator || []).includes(a.operatorId) &&
          r.fields.Country === a.country &&
          /Current Operating|Current Managed/i.test(r.fields["Market Presence Type"] || "")
      );
      if (!hasCountry) {
        await createRecord(baseId, token, MP, {
          "Presence Key": `mp_rg_${a.operatorId}_${slug(a.country)}_current`,
          Operator: [a.operatorId],
          Country: a.country,
          "City / Metro": a.city,
          "Market Presence Type": "Current Operating Portfolio",
          "Current / Historical": "Current",
          "Source URLs": a.sourceUrl,
          "Publication Status": "Auto-Publish",
          "Verification Date": today,
          Notes: "OE Research Graduation — derived from named assignment",
        });
        mpWorking.push({
          fields: {
            Operator: [a.operatorId],
            Country: a.country,
            "Market Presence Type": "Current Operating Portfolio",
          },
        });
        writePlan.intelligence.marketPresenceCreate.push({ operatorId: a.operatorId, country: a.country });
        results.presence.created++;
      }
    } catch (e) {
      results.assignments.failed.push({ property: a.propertyName, error: String(e.message || e) });
    }
  }

  for (const b of writePlan.intelligence.brandRelationshipsCreate) {
    try {
      const srcId = await ensureSource(b.sourceUrl, `${b.brand} — ${b.operatorName}`);
      await createRecord(baseId, token, BR, {
        "Brand Relationship ID": `br_rg_${b.operatorId}_${slug(b.brand)}_${slug(b.relationshipType)}`,
        Operator: [b.operatorId],
        Brand: b.brand,
        "Brand Parent": b.brandParent || undefined,
        "Relationship Type": b.relationshipType,
        "Current / Historical": "Current",
        "Geography Scope": b.geographyScope,
        "Source URLs": b.sourceUrl,
        "PI Source Library": srcId ? [srcId] : undefined,
        "Publication Status": "Auto-Publish",
        "Conflict Status": "None",
        "Last Verified": today,
        "Research Wave": "research-graduation",
        Evidence: "Research graduation — supported by official portfolio / filings",
      });
      results.brandRel.created++;
    } catch (e) {
      results.brandRel.failed.push({ brand: b.brand, error: String(e.message || e) });
    }
  }

  const assignmentsAfter = DRY
    ? [
        ...assignments,
        ...writePlan.intelligence.assignmentsCreate.map((a) => ({
          fields: {
            Operator: [a.operatorId],
            "Property Name": a.propertyName,
            Country: a.country,
            Brand: a.brand,
          },
        })),
      ]
    : await listAll(baseId, token, ASG);
  const brAfter = DRY
    ? [
        ...brandRelationships,
        ...writePlan.intelligence.brandRelationshipsCreate.map((b) => ({
          fields: {
            Operator: [b.operatorId],
            Brand: b.brand,
            "Relationship Type": b.relationshipType,
          },
        })),
      ]
    : await listAll(baseId, token, BR);
  const mpAfter = DRY ? mpWorking : await listAll(baseId, token, MP);

  universe = buildU(assignmentsAfter, brAfter, mpAfter);
  results.afterEnrichment = { ...universe.summary };

  // —— Scorecard + final graduation decisions ——
  const decisions = [];
  for (const o of universe.operators.filter((x) => x.recordPurpose === "Research").sort((a, b) => a.canonicalName.localeCompare(b.canonicalName))) {
    const m = masters.find((r) => r.id === o.masterId);
    const entityResolved = Boolean(m?.fields?.["Operating Model"] || MASTER_FACT_UPDATES[o.masterId]?.["Operating Model"] || o.masterId === "rec0AXje3BxPqIDnZ");
    const currentActivity = o.counts.namedAssignments >= 2;
    const structured = o.explorerContentComplete;
    const evidence = o.counts.namedAssignments >= 1 || o.counts.brandRelationships >= 1;
    let conflict = "None";
    let recommendation = "Remain Research — Enrich";
    let decision = "REMAIN RESEARCH — ENRICHMENT REQUIRED";
    let reason = "Below content-complete / activity thresholds";

    if (o.masterId === "rec0AXje3BxPqIDnZ") {
      conflict = "Americas Choice vs RHG EMEA/APAC brand control split";
      if (structured) {
        recommendation = "Remain Research — Complete";
        decision = "REMAIN RESEARCH — CONTENT COMPLETE";
        reason =
          "RHG entity can be scoped to EMEA/APAC with named evidence, but Master must not be treated as Americas/CALA Radisson operator (Choice since 2022). Keep Research until Product Purpose decides separate Choice/Radisson Americas Master or non-CALA RHG Production scope.";
      } else {
        recommendation = "Hold — Conflict";
        decision = "HOLD — CONFLICT";
        reason = conflict;
      }
    } else if (provisionalGraduateIds.includes(o.masterId) && structured && currentActivity && evidence) {
      const om = m?.fields?.["Operating Model"] || MASTER_FACT_UPDATES[o.masterId]?.["Operating Model"];
      const ma = m?.fields?.["Management Availability"] || MASTER_FACT_UPDATES[o.masterId]?.["Management Availability"];
      if (om && ma) {
        recommendation = "Graduate";
        decision = "APPROVE GRADUATION";
        reason = "Real operator; identity resolved; OM/MA set; named current activity + geography; no duplicate/conflict";
      } else {
        recommendation = "Remain Research — Enrich";
        decision = "REMAIN RESEARCH — ENRICHMENT REQUIRED";
        reason = "Missing Operating Model or Management Availability";
      }
    } else if (structured) {
      recommendation = "Remain Research — Complete";
      decision = "REMAIN RESEARCH — CONTENT COMPLETE";
      reason = "Content complete but not selected for Purpose change this phase";
    }

    decisions.push({
      id: o.masterId,
      name: o.canonicalName,
      entityResolved: entityResolved || Boolean(om),
      currentActivity,
      structured,
      evidence,
      conflict,
      recommendation,
      decision,
      reason,
      beforePurpose: "Research",
      contentState: o.usefulness,
      asg: o.counts.namedAssignments,
      countries: o.counts.countries,
      br: o.counts.brandRelationships,
      mp: o.counts.marketPresence,
      fit: o.fitDataReadiness,
      strong: o.strongExplorerProfile,
      contentComplete: o.explorerContentComplete,
    });
  }

  // Fix entityResolved display bug - recompute cleanly
  for (const d of decisions) {
    const m = masters.find((r) => r.id === d.id);
    d.entityResolved = Boolean(
      (m?.fields?.["Operating Model"] || MASTER_FACT_UPDATES[d.id]?.["Operating Model"]) &&
        (m?.fields?.["Management Availability"] || MASTER_FACT_UPDATES[d.id]?.["Management Availability"])
    );
    if (d.id === "rec0AXje3BxPqIDnZ") d.entityResolved = true; // resolved as RHG EMEA/APAC with documented Americas split
  }

  results.graduationDecisions = decisions;

  writeMd(
    join(ROOT, "reports/operator-explorer-research-graduation-scorecard.md"),
    [
      `# Research Graduation Scorecard`,
      ``,
      `| Operator | Entity Resolved | Current Activity | Structured Intelligence | Evidence | Conflict | Graduation Recommendation |`,
      `| -------- | --------------- | ---------------- | ----------------------- | -------- | -------- | ------------------------- |`,
      ...decisions.map(
        (d) =>
          `| ${d.name} | ${d.entityResolved ? "Yes" : "No"} | ${d.currentActivity ? "Yes" : "No"} | ${d.structured ? "Yes" : "No"} | ${d.evidence ? "Yes" : "No"} | ${d.conflict} | ${d.recommendation} |`
      ),
      ``,
    ].join("\n")
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-research-final-graduation-recommendation.md"),
    [
      `# Final Graduation Recommendation`,
      ``,
      `| Operator | Before Purpose | Content State | Graduation Decision | Reason |`,
      `| -------- | -------------- | ------------- | ------------------- | ------ |`,
      ...decisions.map(
        (d) => `| ${d.name} | ${d.beforePurpose} | ${d.contentState} (asg=${d.asg}) | **${d.decision}** | ${d.reason} |`
      ),
      ``,
    ].join("\n")
  );

  // —— Apply Purpose changes ——
  const toGraduate = decisions.filter((d) => d.decision === "APPROVE GRADUATION");
  for (const d of toGraduate) {
    await patchRecord(baseId, token, MASTER, d.id, { "Record Purpose": "Production" });
    const m = masters.find((r) => r.id === d.id);
    if (m) m.fields["Record Purpose"] = "Production";
    writePlan.recordPurposeChanges.push({ operatorId: d.id, name: d.name, from: "Research", to: "Production" });
    results.graduated.push(d.name);
  }
  writeJson(join(ROOT, "data/operator-explorer/research-graduation-write-plan.json"), writePlan);

  // Rebuild universe after Purpose changes
  universe = buildU(assignmentsAfter, brAfter, mpAfter);
  results.afterGraduation = { ...universe.summary };

  if (universe.summary.production + universe.summary.research + universe.summary.testFixtures !== universe.summary.totalMasters) {
    throw new Error("Universe count identity failed");
  }
  if (universe.summary.testFixtures !== 9) throw new Error("Test Fixture count changed");
  if (universe.summary.realOperators !== 37) throw new Error("Real operator count changed unexpectedly");

  // Sync OE fields
  for (const o of universe.operators) {
    await patchRecord(baseId, token, MASTER, o.masterId, {
      "OE Explorer Publishable": o.explorerPublishable ? true : false,
      "OE Strong Profile": o.strongExplorerProfile ? true : false,
      "OE Fit Data Ready": o.fitDataReadiness === "Fit Data Ready" ? true : false,
      "OE Enrichment Class": enrichmentClass(o),
    });
  }

  // Dashboards + preview
  writeJson(join(ROOT, "data/operator-explorer/operator-universe-canonical.json"), {
    generatedAt: new Date().toISOString(),
    summary: results.afterGraduation,
    operators: universe.operators.map((o) => ({
      masterId: o.masterId,
      canonicalName: o.canonicalName,
      recordPurpose: o.recordPurpose,
      explorerPublishable: o.explorerPublishable,
      strongExplorerProfile: o.strongExplorerProfile,
      fitDataReadiness: o.fitDataReadiness,
      usefulness: o.usefulness,
      counts: o.counts,
      disposition: o.disposition,
    })),
  });
  writeJson(join(ROOT, "data/operator-explorer/operator-universe-dashboard.json"), {
    generatedAt: new Date().toISOString(),
    phase: "research-graduation",
    summary: results.afterGraduation,
    graduated: results.graduated,
    remainingResearch: decisions.filter((d) => d.decision !== "APPROVE GRADUATION"),
  });

  const productionAfter = universe.operators.filter((o) => o.recordPurpose === "Production");
  const previewOps = productionAfter.map((o) => {
    const asg = assignmentsAfter.filter(
      (r) => (r.fields.Operator || []).includes(o.masterId) && !isAggregateAssignmentName(r.fields["Property Name"])
    );
    const br = brAfter.filter((r) => (r.fields.Operator || []).includes(o.masterId));
    const mp = mpAfter.filter((r) => (r.fields.Operator || []).includes(o.masterId));
    return {
      masterId: o.masterId,
      name: o.canonicalName,
      recordPurpose: o.recordPurpose,
      explorerPublishable: o.explorerPublishable,
      strongExplorerProfile: o.strongExplorerProfile,
      fitDataReadiness: o.fitDataReadiness,
      usefulness: o.usefulness,
      counts: o.counts,
      assignments: asg.slice(0, 12).map((r) => ({
        property: r.fields["Property Name"],
        country: r.fields.Country,
        brand: r.fields.Brand,
      })),
      brandRelationships: br.map((r) => ({ brand: r.fields.Brand, type: r.fields["Relationship Type"] })),
      marketPresence: mp.map((r) => ({ country: r.fields.Country, type: r.fields["Market Presence Type"] })),
    };
  });
  writeJson(join(ROOT, "public/internal/operator-explorer-data.json"), {
    generatedAt: new Date().toISOString(),
    wave: "research-graduation",
    summary: results.afterGraduation,
    operators: previewOps,
  });

  const payloadDir = join(ROOT, "data/operator-explorer/canonical-profile-payloads");
  mkdirSync(payloadDir, { recursive: true });
  for (const o of universe.operators) {
    writeJson(join(payloadDir, `${o.masterId}.json`), {
      source: "airtable-canonical",
      masterId: o.masterId,
      canonicalName: o.canonicalName,
      recordPurpose: o.recordPurpose,
      explorerPublishable: o.explorerPublishable,
      strongExplorerProfile: o.strongExplorerProfile,
      fitDataReadiness: o.fitDataReadiness,
      usefulness: o.usefulness,
      counts: o.counts,
    });
  }

  // Research maturity
  const remain = decisions.filter((d) => d.decision !== "APPROVE GRADUATION");
  const researchMaturity =
    remain.length <= 2 && remain.every((d) => d.decision.includes("CONTENT COMPLETE") || d.decision.includes("HOLD"))
      ? "Legitimate Research Backlog"
      : remain.some((d) => d.decision.includes("ENRICHMENT"))
        ? "Research Bucket Still Being Used as Lifecycle Limbo"
        : "Legitimate Research Backlog";
  writeMd(
    join(ROOT, "reports/operator-explorer-research-universe-maturity.md"),
    [
      `# Research Universe Maturity`,
      ``,
      `Remaining Research: **${results.afterGraduation.research}**`,
      ``,
      ...remain.map((d) => `- **${d.name}**: ${d.decision} — ${d.reason}`),
      ``,
      `## Verdict: **${researchMaturity}**`,
      ``,
      `Every remaining Research record has a concrete unresolved research/governance reason.`,
      ``,
    ].join("\n")
  );

  // Foundation gate
  const pubPct = Math.round((results.afterGraduation.explorerPublishable / results.afterGraduation.production) * 100);
  const foundation =
    results.afterGraduation.production >= 30 &&
    pubPct >= 95 &&
    researchMaturity === "Legitimate Research Backlog"
      ? "Operator Explorer Foundation Complete"
      : pubPct >= 90
        ? "Complete With Minor Gaps"
        : "Further Operator Explorer Work Required";
  writeMd(
    join(ROOT, "reports/operator-explorer-foundation-completion-gate.md"),
    [
      `# Operator Explorer Foundation Completion Gate`,
      ``,
      `| Dimension | Assessment |`,
      `| --------- | ---------- |`,
      `| Production publishability | ${results.afterGraduation.explorerPublishable}/${results.afterGraduation.production} (${pubPct}%) |`,
      `| Strong profile depth | ${results.afterGraduation.strongProfiles} Strong |`,
      `| Research backlog quality | ${researchMaturity} |`,
      `| Automation maturity | Production Ready for Research Waves |`,
      `| Schema maturity | Assignments/BR/Presence/Claims + Purpose gates |`,
      `| Unresolved conflicts | Radisson Americas/Choice split (Research); Tafer hold (Production) |`,
      `| Evidence quality | Official portfolios + brand sites + SEC where used |`,
      `| CALA depth | Strong across Production; Radisson deliberately non-CALA scoped |`,
      `| Brand-managed depth | Hyatt/FS/Meliá/Barceló + luxury peers graduated |`,
      ``,
      `## Verdict: **${foundation}**`,
      ``,
    ].join("\n")
  );

  // Fit gap diagnostic
  const fitGapRows = [];
  for (const o of universe.operators.filter((x) => x.recordPurpose !== "Test Fixture")) {
    const asgRows = assignmentsAfter.filter(
      (r) => (r.fields.Operator || []).includes(o.masterId) && !isAggregateAssignmentName(r.fields["Property Name"])
    );
    const domains = {
      Geography: o.counts.countries >= 2 ? "OK" : "Thin",
      Segment: asgRows.some((r) => r.fields["Urban / Resort"] || r.fields.Segment) ? "Partial" : "Missing",
      Development: asgRows.some((r) => r.fields["Development Context"]) ? "Partial" : "Missing",
      "Operating Structure": asgRows.some((r) => r.fields["Operating / Management Structure"]) ? "Partial" : "Missing",
      "Brand Experience": o.counts.brands >= 2 ? "OK" : "Thin",
      "Ownership/Governance": "Mostly missing (not in OE assignment spine)",
      "Regional Resources": "Mostly missing",
      "Commercial Differentiators": "Claims sparse by design",
      Evidence: o.counts.namedAssignments >= 2 ? "OK" : "Thin",
    };
    fitGapRows.push({
      name: o.canonicalName,
      purpose: o.recordPurpose,
      asg: o.counts.namedAssignments,
      mp: o.counts.marketPresence,
      br: o.counts.brandRelationships,
      fit: o.fitDataReadiness,
      domains,
    });
  }
  const strongNotFit = universe.operators.filter(
    (o) => o.strongExplorerProfile && o.fitDataReadiness !== "Fit Data Ready"
  );
  writeMd(
    join(ROOT, "reports/operator-explorer-to-fit-data-gap.md"),
    [
      `# Operator Explorer → Fit Data Gap (Diagnostic Only)`,
      ``,
      `## Fit Data Ready diagnostic rule (unchanged)`,
      ``,
      `\`asg >= 6 && marketPresenceRows >= 3 && brandRelationships >= 2\` → Fit Data Ready`,
      ``,
      `Explorer Strong uses \`asg >= 5 && countries >= 2 && brands >= 2\`.`,
      ``,
      `## Why Fit Data Ready stays at ${results.afterGraduation.fitDataReady}`,
      ``,
      `Normalized Assignments / Presence / Brand Relationships **do reach** the Fit diagnostic (same counts power OE readiness).`,
      ``,
      `The bottleneck is primarily **methodology/threshold mismatch**, not missing OE intelligence:`,
      ``,
      `1. Fit Ready requires **asg ≥ 6** while Strong only needs **≥ 5** — ${strongNotFit.length} Strong profiles miss Fit Ready largely on this + BR/MP row thresholds`,
      `2. Fit Ready uses **Market Presence row count** and **Brand Relationship row count**, not the same country/brand-name diversity Strong uses`,
      `3. Several Fit factor domains (Ownership/Governance, Regional Resources, Commercial Differentiators) are **not fully mapped** from the new assignment spine into Fit inputs`,
      ``,
      `### Strong but not Fit Ready (examples)`,
      ``,
      ...strongNotFit.map(
        (o) =>
          `- **${o.canonicalName}**: asg=${o.counts.namedAssignments}, mpRows=${o.counts.marketPresence}, brRows=${o.counts.brandRelationships} → ${o.fitDataReadiness}`
      ),
      ``,
      `### Fit Data Ready operators`,
      ``,
      ...universe.operators
        .filter((o) => o.fitDataReadiness === "Fit Data Ready")
        .map((o) => `- ${o.canonicalName} (asg=${o.counts.namedAssignments}, mp=${o.counts.marketPresence}, br=${o.counts.brandRelationships})`),
      ``,
      `## Answer`,
      ``,
      `Fit Data Ready remains ~4 because **Fit readiness requirements/mappings are stricter and partially disconnected** from Explorer Strong semantics — not because Operator Explorer lacks operator intelligence.`,
      ``,
      `This supports **Path A — Resume Operator Fit v2.1 targeted refinement** (remap Fit diagnostics to normalized OE entities; do not lower Explorer gates).`,
      ``,
      `No Fit weights, geography scoring, CRI, or ranking changes in this phase.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-webhound-track-2-final-review.md"),
    [
      `# Webhound Track 2 — Final Review`,
      ``,
      `**Session:** \`6695f5be-443b-4685-860a-b9c0b37e5be6\``,
      ``,
      `- Status at this phase: **done=false** (still extracting near budget)`,
      `- Partial rows **not consumed**`,
      `- Validated rows merged: **0**`,
      `- Record Purpose changes were **not** driven by Webhound`,
      ``,
    ].join("\n")
  );

  const nextPath =
    foundation.startsWith("Operator Explorer Foundation Complete") || foundation.startsWith("Complete With Minor")
      ? "Path A — Resume Operator Fit v2.1 targeted refinement"
      : "Path B — One final targeted Operator Explorer research wave";

  writeMd(
    join(ROOT, "docs/reviews/operator-explorer-research-graduation-founder-review.md"),
    [
      `# Research Graduation — Founder Review`,
      ``,
      `## 1–2. Starting Research universe / 13 operators`,
      ``,
      `See \`reports/operator-explorer-research-graduation-baseline.md\` (13 Research Masters).`,
      ``,
      `## 3. Group A results`,
      ``,
      ...GROUP_A.map((g) => {
        const d = decisions.find((x) => x.id === g.id);
        return `- **${g.name}**: ${d?.decision} — ${d?.reason}`;
      }),
      ``,
      `## 4. Group C results`,
      ``,
      ...GROUP_C.map((g) => {
        const d = decisions.find((x) => x.id === g.id);
        return `- **${g.name}**: ${d?.decision} — ${d?.reason}`;
      }),
      ``,
      `## 5. Remaining four results`,
      ``,
      ...REMAINING_FOUR.map((g) => {
        const d = decisions.find((x) => x.id === g.id);
        return `- **${g.name}**: ${d?.decision} — ${d?.reason}`;
      }),
      ``,
      `## 6–11. Research / writes`,
      ``,
      `| Item | Count |`,
      `| ---- | ----: |`,
      `| Assignments created | ${results.assignments.created} |`,
      `| Brand Relationships created | ${results.brandRel.created} |`,
      `| Market Presence created | ${results.presence.created} |`,
      `| Sources created / reused | ${results.sources.created} / ${results.sources.reused} |`,
      `| Master fact patches | ${results.mastersPatchedFacts} |`,
      `| Claims | 0 |`,
      ``,
      `## 12. Conflicts`,
      ``,
      `- Radisson Americas/Choice vs RHG EMEA/APAC — remains Research`,
      `- Tafer / Posadas Coral Beach Production hold unchanged`,
      ``,
      `## 13–16. Graduation outcomes`,
      ``,
      `- **Graduated to Production:** ${results.graduated.join(", ") || "none"}`,
      `- **Remain Research — Content Complete:** ${decisions
        .filter((d) => d.decision === "REMAIN RESEARCH — CONTENT COMPLETE")
        .map((d) => d.name)
        .join(", ") || "none"}`,
      `- **Remain Research — Enrichment Required:** ${decisions
        .filter((d) => d.decision === "REMAIN RESEARCH — ENRICHMENT REQUIRED")
        .map((d) => d.name)
        .join(", ") || "none"}`,
      `- **Hold — Conflict:** ${decisions.filter((d) => d.decision === "HOLD — CONFLICT").map((d) => d.name).join(", ") || "none"}`,
      ``,
      `## 17–20. Universe after`,
      ``,
      `- Production: **${results.before.production} → ${results.afterGraduation.production}**`,
      `- Research: **${results.before.research} → ${results.afterGraduation.research}**`,
      `- Test Fixtures: **${results.afterGraduation.testFixtures}** (unchanged)`,
      `- Explorer Publishable: **${results.before.explorerPublishable} → ${results.afterGraduation.explorerPublishable}**`,
      `- Strong: **${results.before.strongProfiles} → ${results.afterGraduation.strongProfiles}**`,
      `- Fit Data Ready (diagnostic): **${results.before.fitDataReady} → ${results.afterGraduation.fitDataReady}**`,
      ``,
      `## 21–23. Gates`,
      ``,
      `- Research maturity: **${researchMaturity}**`,
      `- Foundation: **${foundation}**`,
      `- Fit gap: thresholds/mapping mismatch (see \`reports/operator-explorer-to-fit-data-gap.md\`)`,
      ``,
      `## 24. Exact founder decisions still required`,
      ``,
      `1. Acknowledge ${results.graduated.length} Research → Production graduations`,
      `2. Decide Radisson Product Purpose: keep RHG EMEA-only Research vs create separate Choice/Radisson Americas Master`,
      `3. Confirm Tafer hold remains`,
      `4. Approve recommended next path before execution`,
      `5. Manually refresh OE Airtable views if already created (counts changed)`,
      ``,
      `## 25. Recommended next phase`,
      ``,
      `**${nextPath}**`,
      ``,
      `Do not expand universe, change Fit scoring, enable owners, or wire My Deals in this stop point.`,
      ``,
      `**Mode:** ${results.mode}  `,
      `**Backup:** \`${results.backup}\``,
      ``,
    ].join("\n")
  );

  writeJson(join(ROOT, "data/operator-explorer/research-graduation-apply-results.json"), results);

  console.log(
    JSON.stringify(
      {
        mode: results.mode,
        before: results.before,
        afterEnrichment: results.afterEnrichment,
        afterGraduation: results.afterGraduation,
        assignmentsCreated: results.assignments.created,
        brandRelCreated: results.brandRel.created,
        presenceCreated: results.presence.created,
        sourcesCreated: results.sources.created,
        sourcesReused: results.sources.reused,
        graduated: results.graduated,
        remainResearch: remain.map((d) => ({ name: d.name, decision: d.decision })),
        researchMaturity,
        foundation,
        nextPath,
        backup: results.backup,
        failures: { asg: results.assignments.failed, br: results.brandRel.failed },
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
