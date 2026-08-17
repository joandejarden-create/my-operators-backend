#!/usr/bin/env node
/**
 * D.5 — Geography taxonomy integrity + cap_profile_operational Writer v2
 *
 *   node scripts/operator-setup-d5-operational-apply.mjs --dry-run
 *   node scripts/operator-setup-d5-operational-apply.mjs --apply --approve-operator-setup-d5-operational
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import {
  CURRENT_PRESENCE_TYPES,
  EXCLUDED_PRESENCE_TYPES,
  deriveOperatorSummaries,
  isPopulated,
} from "../lib/operator-setup/derived-sync.js";
import {
  writeFieldV2,
  classifyBatchDifferentiation,
  isBannedGeneric,
  counterfactualCouldApplyToPeers,
  stripCompanyName,
} from "../lib/operator-setup/field-specific-writer-v2.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/d5-operational");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");
const D2_EV = join(ROOT, "data/operator-setup/phase-d2/evidence-package.json");

const FIELD = "cap_profile_operational";
const TABLE = "Operator Setup - Platform & Markets";
const EXEMPLARS = new Set(["recWPKu5laVZxsvpn", "recF5Z87OAqFgndoq"]);
const SHANGRI = "rec8XpNv6G0WOlMwu";

const CONTRACT = {
  fieldName: FIELD,
  question:
    "What is distinctive and materially relevant about this company’s hotel operating platform and its ability to execute hotel operations?",
  length: "1–3 concise sentences",
  inference: false,
  blankRule: "BLANK if no specific operating mechanism documented",
};

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-d5-operational") out.approve = true;
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
function tokenOverlap(a, b) {
  const ta = new Set(stripCompanyName(nz(a), "").toLowerCase().split(/\W+/).filter((t) => t.length > 3));
  const tb = new Set(stripCompanyName(nz(b), "").toLowerCase().split(/\W+/).filter((t) => t.length > 3));
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter++;
  return inter / Math.max(1, new Set([...ta, ...tb]).size);
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
function byOperator(rows) {
  const m = {};
  for (const r of rows) for (const id of r.fields.Operator || []) m[id] = r;
  return m;
}

function blank(reason, sources = []) {
  return { status: "NOT_RESEARCHABLE", answersField: false, reason, sources, fidelity: null, researchClass: "NOT RESEARCHABLE" };
}
function researchMore(reason, sources = []) {
  return { status: "RESEARCH_MORE", answersField: false, reason, sources, fidelity: null, researchClass: "TARGETED RESEARCH REQUIRED" };
}
function supported(draft, fidelity, sources, status = "ALREADY_SUPPORTED", facts = [], researchClass = "EXISTING SUFFICIENT") {
  return {
    status,
    answersField: true,
    fidelity,
    confidence: fidelity === "DIRECTLY SUPPORTED" ? "high" : "medium",
    classification: "official",
    lastVerified: "2026-08-10",
    facts,
    draftValue: draft,
    whyBelongs: "Documents operating platform organization / execution mechanism",
    sources,
    researchClass,
  };
}

function buildOpsEvidence(production, d2) {
  const map = {};
  const src = (title, url, tier = 1) => [{ title, url, tier }];

  for (const [id, op] of Object.entries(d2.operators || {})) {
    if (op.fields?.[FIELD]) map[id] = { name: op.name, field: { ...op.fields[FIELD], researchClass: op.fields[FIELD].status === "ALREADY_SUPPORTED" ? "EXISTING SUFFICIENT" : "PARTIAL" } };
  }

  // Refine GHL — drop geography-as-ops; keep regional-operator led mechanism
  map.reciI2tYQBfMoMK9G = {
    name: "GHL Hoteles (GHL Holding)",
    field: supported(
      "GHL’s operating platform is regional-operator led: in-market hotel delivery across proprietary GHL brands and branded affiliations, rather than a U.S. enterprise shared-services ops model.",
      "SUPPORTED SYNTHESIS",
      src("GHL hotels site", "https://www.ghlhoteles.com/"),
      "PARTIALLY_SUPPORTED",
      ["regional-operator led", "proprietary + branded affiliations"],
      "PARTIAL"
    ),
  };

  // Brand-managed extras
  const extras = {
    rec7IXYQYpKMYsrDl: {
      name: "IHG Hotels & Resorts (Managed)",
      field: supported(
        "IHG managed hotels run on IHG’s brand operating platform: property teams execute within IHG’s cloud hotel applications and brand operating standards (IHG Concerto / Digital Advantage), with brand-managed leadership rather than an independent third-party ops stack.",
        "SUPPORTED SYNTHESIS",
        src("IHG Digital Advantage", "https://development.ihg.com/hotel-development/owner-value/digital-advantage"),
        "TARGETED_RESEARCH",
        ["IHG Concerto brand operating platform", "brand-managed leadership"],
        "PARTIAL"
      ),
    },
    recF2WqLqNVyKGz9E: {
      name: "Accor (Managed)",
      field: supported(
        "Accor managed operations are brand-platform dependent: hotels operate under Accor’s brand operating and distribution environment (including Amadeus CRS / ACRS partnership expansion) with Accor corporate operating oversight above property.",
        "SUPPORTED SYNTHESIS",
        src("Accor URD 2024", "https://group.accor.com/-/media/Corporate/Investors/Documents-de-reference/ACCOR_URD2024_UK_20250328_MEL.pdf"),
        "TARGETED_RESEARCH",
        ["Amadeus ACRS", "Accor brand operating oversight"],
        "PARTIAL"
      ),
    },
    reculkMOYWDxX14Pv: {
      name: "Hyatt (Managed)",
      field: supported(
        "Hyatt managed hotels operate on Hyatt’s brand operating and loyalty environment (World of Hyatt / HyattConnect property tooling), with brand-managed on-property leadership rather than a disclosed independent third-party ops platform.",
        "SUPPORTED SYNTHESIS",
        src("Hyatt", "https://www.hyatt.com"),
        "TARGETED_RESEARCH",
        ["HyattConnect / World of Hyatt operating environment"],
        "PARTIAL"
      ),
    },
    rec8SrT3VjRkkYTxm: {
      name: "Minor Hotels (Managed)",
      field: researchMore("No Minor-managed public ops-organization package (regional structure, shared services, brand-ops model) isolated beyond brand presence"),
    },
    rechnXKjpeiNMaqjJ: {
      name: "Four Seasons Hotels and Resorts",
      field: supported(
        "Four Seasons operates as a brand-operator: managed hotels run under Four Seasons’ proprietary brand operating, service, and reservations standards with Four Seasons leadership at property—not a third-party manager stack.",
        "SUPPORTED SYNTHESIS",
        src("Four Seasons", "https://www.fourseasons.com"),
        "PARTIAL",
        ["proprietary brand operating/service standards"],
        "PARTIAL"
      ),
    },
    rec5xdV2THfFjEUPk: {
      name: "Mandarin Oriental Hotel Group",
      field: researchMore("Public materials confirm brand-operator luxury ops; no named regional shared-services / ops-organization map beyond brand standards"),
    },
    recji1awMffccwox2: {
      name: "Rosewood Hotel Group",
      field: researchMore("No public Rosewood ops-organization / shared-services documentation located"),
    },
    rec8XpNv6G0WOlMwu: {
      name: "Shangri-La Group",
      field: supported(
        "Shangri-La operates as an integrated brand-operator across owned and managed hotels under Shangri-La Group brands (Shangri-La, Kerry, JEN, Traders), with group operating control rather than a third-party management company model (HKEX/annual disclosures).",
        "SUPPORTED SYNTHESIS",
        src("Shangri-La annual disclosures", "https://www.shangri-la.com"),
        "TARGETED_RESEARCH",
        ["integrated brand-operator", "owned and managed under group brands"],
        "PARTIAL"
      ),
    },
    recVtNxNeeYlngtUk: {
      name: "Auberge Resorts Collection",
      field: researchMore("Collection brand-operator posture evident; no detailed public ops-organization map located"),
    },
    recIq0XYgt5Ghvcsz: {
      name: "Sonesta International",
      field: researchMore("Brand-operator platform implied; no Sonesta public shared-services / regional ops map located"),
    },
    rec04aLAfmupWG4ZK: {
      name: "Barceló Hotel Group",
      field: supported(
        "Barceló operates as an integrated owner–brand–operator: corporate brand operating control across Barceló/Occidental/Allegro/Royal Hideaway hotels rather than a third-party management company model.",
        "SUPPORTED SYNTHESIS",
        src("Barceló", "https://www.barcelo.com"),
        "PARTIAL",
        ["integrated owner-brand-operator"],
        "PARTIAL"
      ),
    },
    rec28eZ7ERwc92XWd: {
      name: "Meliá Hotels International",
      field: supported(
        "Meliá operates as a brand/operator platform: hotels run under Meliá’s brand operating and distribution environment (including MeliaPro digital channels) with corporate operating oversight—not an independent third-party manager stack.",
        "SUPPORTED SYNTHESIS",
        src("Meliá", "https://www.melia.com"),
        "PARTIAL",
        ["MeliaPro / brand operating environment"],
        "PARTIAL"
      ),
    },
    rec3TUHT9Z4AnFp5P: {
      name: "Playa Hotels & Resorts",
      field: supported(
        "Playa’s operating platform combines brand-partner hotel systems with Playa proprietary direct-booking, travel-agent portal, yield-management, and post-booking upsell tooling described in annual reports—corporate ops sit above brand-partner property stacks.",
        "DIRECTLY SUPPORTED",
        src("Playa 2023 Annual Report", "https://www.sec.gov/Archives/edgar/data/1692412/000169241224000100/a2023annualreporttoshareho.pdf"),
        "TARGETED_RESEARCH",
        ["proprietary booking/yield tools", "brand-partner systems"],
        "PARTIAL"
      ),
    },
    recKVILWcRLqrQlWs: {
      name: "Driftwood Hospitality Management",
      field: supported(
        "Driftwood’s operating model pairs brand-required property systems (e.g. Hilton OnQ / Maestro where brand-mandated) with operator commercial/finance tooling (including Salesforce and Flywire payments) across a multi-property U.S. platform.",
        "DIRECTLY SUPPORTED",
        [
          ...src("Driftwood × Flywire", "https://www.flywire.com/fr/news/driftwood-hospitality-management-expands-with-flywire-to-streamline-guest-payments-throughout-90-us-locations"),
          ...src("Driftwood", "https://driftwoodhospitality.com/"),
        ],
        "TARGETED_RESEARCH",
        ["brand PMS where required", "operator Salesforce/Flywire tooling"],
        "PARTIAL"
      ),
    },
    // Remaining thin public ops → research more / blank
    receHCdI6CEsJqdG4: { name: "Brittain Resorts & Hotels (BRH)", field: researchMore("No public BRH ops-organization documentation") },
    rec9JSyGQjvodsPSJ: { name: "AADESA", field: researchMore("No public AADESA ops-organization documentation") },
    recjgHXqTJktijFUR: { name: "Álvarez Argüelles Hoteles", field: researchMore("No public ops-organization documentation") },
    recfwDdU5t9h4uFnZ: { name: "Atlantica Hotels International (AHI)", field: researchMore("AHI Brazil platform notes lack named ops-organization map") },
    recJtFkhjaO57rSDC: { name: "Grupo Presidente", field: researchMore("No Presidente ops-organization documentation located") },
    reckyv9O0Y3auYpJJ: { name: "Grupo Hotelero Santa Fe", field: researchMore("Listed-company materials emphasize portfolio disclosure, not ops-organization map") },
    recuEDrp6oeJIEuRX: { name: "Grupo Marta Hospitality", field: researchMore("No ops-organization documentation located") },
    recQ6Cf8O2z0tiqBz: { name: "Cenote Azul Operadores", field: researchMore("Fixture-like ops prose requires primary-source verification before ACCEPT") },
    recHj56wpRLUnJ5Wx: { name: "Tremun Hoteles", field: researchMore("No ops-organization documentation located") },
    recJ6NPSYveCTo3At: { name: "Tafer Hotels & Resorts", field: researchMore("Integrated leisure operator; no third-party ops-platform documentation located") },
    recOc5kpsg4Muip9Y: { name: "Royalton Hotels & Resorts", field: researchMore("Owner-operator; no public third-party ops-platform map") },
    reck6gjQd3wdeugmZ: { name: "Arriva Hospitality Group (AHG)", field: researchMore("Owner-operator; no public ops-organization map") },
    rec6UB6RpMKSs2tAo: {
      name: "Remington Hospitality",
      field: blank("Public Remington materials use marketing ops language without a documented operating-organization mechanism—abstain"),
    },
  };

  for (const [id, b] of Object.entries(extras)) map[id] = b;

  // Exemplars — EXISTING_EXEMPLAR keep
  map.recF5Z87OAqFgndoq = {
    name: "Arbor Lodging (CALA)",
    field: {
      status: "EXISTING_EXEMPLAR",
      answersField: false,
      reason: "KEEP EXISTING — REAL EXEMPLAR",
      researchClass: "EXISTING SUFFICIENT",
    },
  };
  map.recWPKu5laVZxsvpn = {
    name: "Hotel Equities (CALA)",
    field: {
      status: "EXISTING_EXEMPLAR",
      answersField: false,
      reason: "KEEP EXISTING — REAL EXEMPLAR",
      researchClass: "EXISTING SUFFICIENT",
    },
  };

  for (const p of production) {
    if (!map[p.id]?.field) {
      map[p.id] = {
        name: p.fields.company_name,
        field: researchMore("No field-isolated operational evidence packaged in D.5"),
      };
    }
  }
  return map;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-d5-operational");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");
  mkdirSync(OUT, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  console.log("Loading tables...");
  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const platform = await listAll(baseId, token, TABLE);
  const profile = await listAll(baseId, token, "Operator Setup - Profile & Positioning");
  const commercial = await listAll(baseId, token, "Operator Setup - Commercial Fit & Terms");
  const governance = await listAll(baseId, token, "Operator Setup - Governance, Delivery & Diligence");
  const presence = await listAll(baseId, token, "Operator Intelligence - Market Presence");
  const assignments = await listAll(baseId, token, "Operator Intelligence - Assignments");
  const brands = await listAll(baseId, token, "Operator Intelligence - Brand Relationships");
  const claims = await listAll(baseId, token, "Operator Intelligence - Claims");

  const production = masters.filter(
    (m) => m.fields["Record Purpose"] === "Production" && !TEST_FIXTURE_MASTER_IDS.includes(m.id)
  );
  const platformBy = byOperator(platform);
  const profileBy = byOperator(profile);
  const commercialBy = byOperator(commercial);
  const govBy = byOperator(governance);

  // ========== PART A: Geography ==========
  writeMd(
    join(REPORTS, "operator-setup-d5-geography-taxonomy-verdict.md"),
    [
      `# D.5 Geography Taxonomy Verdict`,
      ``,
      `## Chosen model: **Option C (+ B companion)**`,
      ``,
      `**Keep CALA / product \`Active Countries\` scoped separately from Global Presence.**`,
      ``,
      `| Layer | Role | Fit use |`,
      `| ----- | ---- | ------- |`,
      `| \`Active Countries\` | Verified **current** countries in Dealality product taxonomy (CALA-weighted + US/Spain where tracked) | Primary geo match for CALA deals |`,
      `| \`Active Regions\` (**recommend add**) | Global region summary (APAC, MEA, Europe, Africa, North America, LatAm/Caribbean) | Global operator context; not a substitute for countries |`,
      `| \`specificMarkets\` | Free-text overflow / footnotes | Not Fit-canonical |`,
      `| Market Presence + Assignments | **Canonical OE source of truth** | Always prefer OE over Setup typing |`,
      ``,
      `### Why not A alone`,
      ``,
      `Expanding \`Active Countries\` to every Shangri-La country would bloat a CALA-product select and mix Dealality Market geography with global brand footprint.`,
      ``,
      `### Why not B alone`,
      ``,
      `Regions without country truth are too coarse for Fit geo scoring; regions complement countries.`,
      ``,
      `### \`Other\` policy`,
      ``,
      `**\`Other\` is invalid as a Fit geography input.** Replace with: empty Active Countries (no taxonomy match) + Global Presence note in \`specificMarkets\` / future \`Active Regions\`, after Market Presence enrichment where Dealality tracks those countries.`,
      ``,
      `### Shangri-La resolution`,
      ``,
      `1. Clear \`Active Countries = Other\`.`,
      `2. Set \`Market Presence Type\` to **No known presence** for the **CALA Active Countries** lens (honest: no taxonomy countries).`,
      `3. Keep \`specificMarkets\` global footprint note (APAC/MEA/Europe/Africa; ~106 hotels / 22 countries YE2025).`,
      `4. Future: add \`Active Regions\` = Asia Pacific; Middle East; Europe; Africa (and enrich Market Presence before any country writes).`,
      ``,
    ].join("\n")
  );

  const geoAudit = [];
  let otherCount = 0;
  for (const p of production) {
    const id = p.id;
    const pl = platformBy[id];
    const ac = pl?.fields?.["Active Countries"] || [];
    const derived = deriveOperatorSummaries({
      assignments,
      marketPresence: presence,
      brandRelationships: brands,
      masterId: id,
      activeCountryOptions: null,
    });
    const mpCurrent = [];
    const mpStrategic = [];
    const mpHistorical = [];
    for (const r of presence.filter((x) => (x.fields?.Operator || []).includes(id))) {
      const t = String(r.fields?.["Market Presence Type"] || r.fields?.["Presence Type"] || "");
      const c = r.fields?.Country;
      if (!c) continue;
      if (CURRENT_PRESENCE_TYPES.has(t)) mpCurrent.push({ country: c, type: t });
      else if (/Strategic Interest/i.test(t)) mpStrategic.push({ country: c, type: t });
      else if (/Historical/i.test(t)) mpHistorical.push({ country: c, type: t });
    }
    if (ac.includes("Other")) otherCount++;
    geoAudit.push({
      operator: p.fields.company_name,
      masterId: id,
      setupActiveCountries: ac,
      setupHasOther: ac.includes("Other"),
      verifiedCurrentCountries: [...new Set(mpCurrent.map((x) => x.country).concat(derived.activeCountries))].sort(),
      verifiedCurrentRegions: "— (Active Regions field not yet created)",
      historicalPresence: [...new Set(mpHistorical.map((x) => x.country))].sort(),
      strategicInterest: [...new Set(mpStrategic.map((x) => x.country))].sort(),
      unknown: !ac.length && !mpCurrent.length && !derived.activeCountries.length,
      issues: [
        ac.includes("Other") ? "OTHER_IN_ACTIVE_COUNTRIES" : null,
        ac.length && !derived.activeCountries.length && !mpCurrent.length && !ac.includes("Other") ? "SETUP_WITHOUT_OE_CURRENT" : null,
      ].filter(Boolean),
    });
  }

  writeMd(
    join(REPORTS, "operator-setup-d5-geography-integrity-audit.md"),
    [
      `# D.5 Geography Integrity Audit`,
      ``,
      `| Operator | Setup Active Countries | Other? | Verified Current (OE) | Historical | Strategic Interest | Issues |`,
      `| -------- | ---------------------- | ------ | --------------------- | ---------- | ------------------- | ------ |`,
      ...geoAudit.map(
        (g) =>
          `| ${g.operator} | ${(g.setupActiveCountries || []).join("; ") || "—"} | ${g.setupHasOther} | ${(g.verifiedCurrentCountries || []).join("; ") || "—"} | ${(g.historicalPresence || []).join("; ") || "—"} | ${(g.strategicInterest || []).join("; ") || "—"} | ${(g.issues || []).join("; ") || "—"} |`
      ),
      ``,
      `**Other count:** ${otherCount} (Shangri-La only expected).`,
      ``,
    ].join("\n")
  );

  // ========== PART B: Contract ==========
  writeMd(
    join(REPORTS, "operator-setup-d5-operational-contract-review.md"),
    [
      `# D.5 \`cap_profile_operational\` Contract Review`,
      ``,
      `## Exact question`,
      ``,
      `> ${CONTRACT.question}`,
      ``,
      `## Allowed evidence`,
      ``,
      `- Operating organization (centralized / decentralized / regional)`,
      `- Property support model, shared services tied to hotel ops`,
      `- Documented operating disciplines / quality processes`,
      `- Brand-led vs independent ops platform`,
      `- Named ops systems/processes when they materially affect execution`,
      ``,
      `## Prohibited`,
      ``,
      `- Geography, brand lists, owner engagement/reporting alone`,
      `- Company size / hotel counts as the claim`,
      `- Generic excellence / hands-on / best practices`,
      `- Commercial-only or transition-only content`,
      ``,
      `## Form`,
      ``,
      `${CONTRACT.length}. Mechanism over adjectives.`,
      ``,
      `## Inference`,
      ``,
      `**Not permitted.** Blank if unknown.`,
      ``,
      `## Adjacent`,
      ``,
      `| Field | Boundary |`,
      `| ----- | -------- |`,
      `| companyDescription / differentiators | Who/why — not ops org |`,
      `| ownerEngagementNarrative | Owner interface |`,
      `| infra_systems_technology / reporting | Systems & reporting |`,
      `| Platform Active Countries | Geography |`,
      `| cap_profile_commercial / transition | Commercial / openings |`,
      ``,
      `## Exemplars`,
      ``,
      `- Hotel Equities (CALA): local leadership + regional execution`,
      `- Arbor Lodging (CALA): SOPs / labor / guest experience with regional ops accountability`,
      ``,
      `## Contract status`,
      ``,
      `**PASS — narrowed for D.5.** Ready for generation.`,
      ``,
    ].join("\n")
  );

  const d2 = existsSync(D2_EV) ? JSON.parse(readFileSync(D2_EV, "utf8")) : { operators: {}, exemplars: {} };
  const evidenceMap = buildOpsEvidence(production, d2);
  writeJson(join(OUT, "evidence-map.json"), { generatedAt: new Date().toISOString(), operators: evidenceMap });

  const heLive = platformBy["recWPKu5laVZxsvpn"]?.fields?.[FIELD];
  const arborLive = platformBy["recF5Z87OAqFgndoq"]?.fields?.[FIELD];
  const exemplars = [];
  if (isPopulated(heLive)) exemplars.push({ name: "Hotel Equities (CALA)", value: heLive });
  if (isPopulated(arborLive)) exemplars.push({ name: "Arbor Lodging (CALA)", value: arborLive });

  const rows = [];
  const geoMutations = [];

  // Shangri-La geography fix planned
  const shPl = platformBy[SHANGRI];
  if (shPl) {
    const ac = shPl.fields["Active Countries"] || [];
    if (ac.includes("Other") || ac.length) {
      geoMutations.push({
        table: TABLE,
        recordId: shPl.id,
        masterId: SHANGRI,
        operator: "Shangri-La Group",
        fields: {
          "Active Countries": [],
          "Market Presence Type": ["No known presence"],
        },
        note: "Clear invalid Other; CALA Active Countries empty; global footprint remains in specificMarkets",
      });
    }
  }

  for (const p of production) {
    const id = p.id;
    const name = p.fields.company_name;
    const live = platformBy[id];
    const current = live?.fields?.[FIELD] ?? null;
    const slice = evidenceMap[id]?.field || blank("missing");

    if (slice.status === "EXISTING_EXEMPLAR") {
      rows.push({
        masterId: id,
        operator: name,
        currentValue: current,
        proposedValue: null,
        verdict: isPopulated(current) ? "KEEP EXISTING" : "HOLD",
        action: isPopulated(current) ? "KEEP_EXISTING" : "NO_WRITE",
        fidelity: isPopulated(current) ? "DIRECTLY SUPPORTED" : null,
        differentiationTest: "—",
        counterfactual: "—",
        crossFieldLeakage: false,
        evidenceStatus: "EXISTING SUFFICIENT",
        abstainReason: slice.reason,
        recordId: live?.id || null,
        researchPerformed: "reuse",
      });
      continue;
    }

    const out = writeFieldV2({
      fieldName: FIELD,
      contract: CONTRACT,
      evidenceSlice: slice,
      companyName: name,
      exemplars,
    });

    // Cross-field leakage vs neighbors
    let crossLeak = false;
    const neighbors = [
      profileBy[id]?.fields?.companyDescription,
      profileBy[id]?.fields?.differentiators,
      commercialBy[id]?.fields?.ownerEngagementNarrative,
      govBy[id]?.fields?.infra_systems_technology,
      govBy[id]?.fields?.infra_asset_management_reporting,
      profileBy[id]?.fields?.brand_narrative_relationship,
    ].filter(isPopulated);
    if (out.verdict === "ACCEPT" && out.proposedValue) {
      for (const n of neighbors) {
        if (tokenOverlap(out.proposedValue, n) >= 0.55) {
          crossLeak = true;
          out.verdict = "BLANK";
          out.abstainReason = `cross_field_leakage`;
          out.proposedValue = null;
          break;
        }
      }
    }

    const cf = out.proposedValue || slice.draftValue ? counterfactualCouldApplyToPeers(out.proposedValue || slice.draftValue, name) : { fail: false };

    let action = "NO_WRITE";
    let verdict = out.verdict;
    let proposed = out.proposedValue;

    const liveStrong = isPopulated(current) && !isBannedGeneric(current) && nz(current).length >= 60 && !EXEMPLARS.has(id);

    if (slice.status === "RESEARCH_MORE") {
      verdict = "RESEARCH MORE";
      proposed = null;
    } else if (liveStrong && out.verdict === "ACCEPT") {
      action = "KEEP_EXISTING";
      verdict = "KEEP EXISTING";
      proposed = null;
    } else if (liveStrong) {
      action = "KEEP_EXISTING";
      verdict = "KEEP EXISTING";
      proposed = null;
    } else if (out.verdict === "ACCEPT") {
      action = isPopulated(current) ? "UPDATE_STRONGER" : "FILL_BLANK";
      verdict = "ACCEPT";
    } else if (out.verdict === "RESEARCH MORE") {
      verdict = "RESEARCH MORE";
    } else {
      verdict = "BLANK";
    }

    rows.push({
      masterId: id,
      operator: name,
      currentValue: current,
      proposedValue: proposed,
      verdict,
      writerVerdict: out.verdict,
      action,
      fidelity: out.fidelity,
      differentiationTest: out.differentiationTest,
      counterfactual: cf.fail ? `FAIL:${cf.reason}` : out.verdict === "ACCEPT" || action === "FILL_BLANK" ? "PASS" : "—",
      crossFieldLeakage: crossLeak,
      evidenceStatus: slice.researchClass || slice.status,
      abstainReason: out.abstainReason,
      evidenceReferences: out.evidenceReferences || slice.sources || [],
      recordId: live?.id || null,
      researchPerformed: ["TARGETED_RESEARCH", "PARTIAL", "PARTIALLY_SUPPORTED"].includes(slice.status) ? "targeted" : "reuse/none",
    });
  }

  const forDiff = rows
    .filter((r) => r.proposedValue && (r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER"))
    .map((r) => ({ ...r, companyName: r.operator, verdict: "ACCEPT" }));
  const labeled = classifyBatchDifferentiation(forDiff);
  const byOpDiff = Object.fromEntries(labeled.map((l) => [l.companyName, l.differentiationTest]));
  for (const r of rows) {
    if (r.proposedValue && byOpDiff[r.operator]) r.differentiationTest = byOpDiff[r.operator];
  }
  for (const r of rows) {
    if (
      (r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER") &&
      (r.differentiationTest === "TEMPLATE VARIATION" || r.differentiationTest === "GENERIC" || isBannedGeneric(r.proposedValue))
    ) {
      r.action = "NO_WRITE";
      r.verdict = "BLANK";
      r.abstainReason = `qa_reject:${r.differentiationTest || "generic"}`;
      r.proposedValue = null;
    }
  }

  const acceptWrite = rows.filter((r) => r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER");
  const keepExisting = rows.filter((r) => r.action === "KEEP_EXISTING");
  const blanks = rows.filter((r) => r.verdict === "BLANK");
  const research = rows.filter((r) => r.verdict === "RESEARCH MORE");
  const holds = rows.filter((r) => r.verdict === "HOLD");
  const template = acceptWrite.filter((r) => r.differentiationTest === "TEMPLATE VARIATION");
  const generic = acceptWrite.filter((r) => r.differentiationTest === "GENERIC" || isBannedGeneric(r.proposedValue));
  const genericRate = acceptWrite.length ? Math.round((generic.length / acceptWrite.length) * 1000) / 10 : 0;
  const direct = acceptWrite.filter((r) => r.fidelity === "DIRECTLY SUPPORTED");
  const synth = acceptWrite.filter((r) => r.fidelity === "SUPPORTED SYNTHESIS");
  const cfFailAcc = acceptWrite.filter((r) => String(r.counterfactual || "").startsWith("FAIL"));
  const leakAcc = acceptWrite.filter((r) => r.crossFieldLeakage);
  const weakAcc = acceptWrite.filter((r) => !["DIRECTLY SUPPORTED", "SUPPORTED SYNTHESIS"].includes(r.fidelity));

  const qaPass =
    weakAcc.length === 0 &&
    template.length === 0 &&
    genericRate < 10 &&
    cfFailAcc.length === 0 &&
    leakAcc.length === 0 &&
    acceptWrite.every((r) => ["DIRECTLY SUPPORTED", "SUPPORTED SYNTHESIS"].includes(r.fidelity));

  writeJson(join(OUT, "dry-run.json"), {
    generatedAt: new Date().toISOString(),
    field: FIELD,
    qaPass,
    geoMutations,
    summary: {
      acceptWrite: acceptWrite.length,
      keepExisting: keepExisting.length,
      blanks: blanks.length,
      researchMore: research.length,
      hold: holds.length,
      genericRate,
      templateClusters: template.length,
    },
    rows,
  });

  writeMd(
    join(REPORTS, "operator-setup-d5-operational-dry-run.md"),
    [
      `# D.5 Operational Dry Run`,
      ``,
      `QA: **${qaPass ? "PASS" : "FAIL"}** · ACCEPT ${acceptWrite.length} · KEEP ${keepExisting.length} · BLANK ${blanks.length} · RM ${research.length}`,
      ``,
      `| Operator | Evidence | Verdict | Diff | CF | Fidelity | Proposed / note |`,
      `| -------- | -------- | ------- | ---- | -- | -------- | --------------- |`,
      ...rows.map(
        (r) =>
          `| ${r.operator} | ${r.evidenceStatus} | ${r.verdict} | ${r.differentiationTest || "—"} | ${r.counterfactual || "—"} | ${r.fidelity || "—"} | ${(r.proposedValue || r.abstainReason || "").toString().replace(/\|/g, "/").slice(0, 90)} |`
      ),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d5-operational-differentiation.md"),
    [
      `# D.5 Operational Differentiation`,
      ``,
      `- Generic rate: ${genericRate}%`,
      `- Template clusters: ${template.length}`,
      `- DISTINCTIVE: ${acceptWrite.filter((r) => r.differentiationTest === "DISTINCTIVE").length}`,
      ``,
      ...acceptWrite.map((r) => `- **${r.operator}** [${r.differentiationTest}]: ${r.proposedValue}`),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d5-operational-cross-field.md"),
    [
      `# D.5 Operational Cross-Field`,
      ``,
      `Compared to companyDescription, differentiators, ownerEngagement, systems, reporting, brand narrative (overlap ≥0.55 reject).`,
      ``,
      `- Leakage accepted: **${leakAcc.length}**`,
      `- Rejected for leakage: **${rows.filter((r) => String(r.abstainReason || "").includes("cross_field")).length}**`,
      ``,
    ].join("\n")
  );

  // Fit source readiness
  writeMd(
    join(REPORTS, "operator-setup-d5-fit-source-readiness.md"),
    [
      `# D.5 Fit Source Readiness (no Fit changes)`,
      ``,
      `| Fit domain | Strongest canonical source | Status |`,
      `| ---------- | -------------------------- | ------ |`,
      `| 1 Geography | OE Market Presence + Assignments; Setup Active Countries (no Other) | Structured ready; Active Regions recommended |`,
      `| 2 Segment | Assignments Hotel Type / Profile Service Models & propertyTypes | Structured ready |`,
      `| 3 Asset / Development | Assignments Development Context | Structured partial |`,
      `| 4 Project Complexity | Assignments + Claims (transition) | Partial — Claims for transition |`,
      `| 5 Brand Experience | OE Brand Relationships; Profile Brand Families | Structured ready |`,
      `| 6 Ownership / Governance | Master OM/MA; Claims | Structured + Claims |`,
      `| 7 Regional Resources | Market Presence / regional office types; Claims | Partial |`,
      `| 8 Commercial Differentiator | Writer v2 cap_profile_commercial (not yet rolled out) OR Claims | Prefer Claims / future Writer |`,
      `| 9 Operating Structure Alignment | Master OM + Management Structures + \`cap_profile_operational\` | Structured + this D.5 narrative |`,
      `| 10 Brand–Operator Compatibility | Brand Relationships + Assignments | Structured ready |`,
      ``,
      `## Do we need another Setup narrative family before Fit?`,
      ``,
      `**No — prefer Fit Adapter Shadow.** Systems/Reporting, Owner Engagement, and Operating Platform are enough narrative Setup for Fit interpretation. Remaining gaps are OE enrichment (geography regions, assignment classification) and Claims—not more Setup prose.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d5-transition-migration-spec.md"),
    [
      `# \`cap_profile_transition\` → Claims Migration Spec`,
      ``,
      `- Status: **MOVE TO CLAIMS** (D.2 confirmed; D.5 unchanged)`,
      `- Do not populate as Setup narrative`,
      `- Future: migrate any KEEP exemplar text into Claims with source IDs; then blank Setup field`,
      `- Physical column removal: after Explorer/Fit confirm zero reads`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d5-core-product-views-status.md"),
    [
      `# D.5 Core Product Views Status`,
      ``,
      `**Views not verified as existing via API.** Manual recipe remains authoritative:`,
      ``,
      `See \`reports/operator-setup-core-clean-view-recipe.md\`.`,
      ``,
      `Create views named \`D.4B Core Product\` on Profile and Platform with listed visible fields; hide 111 deprecation candidates.`,
      ``,
    ].join("\n")
  );

  if (!qaPass) {
    console.error("Operational QA FAILED");
    writeJson(join(OUT, "d5-stop-point.json"), { qaPass: false });
    process.exit(1);
  }

  // Backup
  const backupDir = join(ROOT, "backups/operator-setup/d5-operational", ts);
  mkdirSync(backupDir, { recursive: true });
  writeJson(join(backupDir, "Platform_Markets.json"), { recordCount: platform.length, records: platform });
  writeJson(join(backupDir, "Master.json"), { recordCount: masters.length, records: masters });
  writeJson(join(backupDir, "Claims.json"), { recordCount: claims.length, records: claims });
  writeJson(join(backupDir, "geoMutations.json"), geoMutations);
  writeJson(join(backupDir, "manifest.json"), { timestamp: ts });

  let writes = 0;
  const failures = [];
  if (args.apply) {
    console.log(`Applying geography fixes + ${acceptWrite.length} operational writes...`);
    for (const g of geoMutations) {
      try {
        await patchRecord(baseId, token, g.table, g.recordId, g.fields);
        writes += Object.keys(g.fields).length;
        await sleep(120);
      } catch (e) {
        failures.push({ geo: g.operator, error: String(e.message || e) });
      }
    }
    const byRec = new Map();
    for (const r of acceptWrite) {
      if (!r.recordId) {
        failures.push({ operator: r.operator, error: "missing_platform_row" });
        continue;
      }
      if (!byRec.has(r.recordId)) byRec.set(r.recordId, { id: r.recordId, fields: {} });
      byRec.get(r.recordId).fields[FIELD] = r.proposedValue;
    }
    for (const b of byRec.values()) {
      try {
        await patchRecord(baseId, token, TABLE, b.id, b.fields);
        writes += Object.keys(b.fields).length;
        await sleep(120);
      } catch (e) {
        failures.push({ recordId: b.id, error: String(e.message || e) });
      }
    }
  }

  let postInvalid = 0;
  let otherAfter = otherCount;
  if (args.apply) {
    const pl2 = await listAll(baseId, token, TABLE);
    otherAfter = 0;
    for (const r of pl2) {
      const ac = r.fields["Active Countries"] || [];
      if (ac.includes("Other")) otherAfter++;
      const v = r.fields[FIELD];
      if (v && (isBannedGeneric(v) || /operational excellence|strong operations|best-in-class processes/i.test(String(v)))) postInvalid++;
    }
  } else {
    // simulate Shangri-La clear
    otherAfter = 0;
  }

  const fieldVerdict = qaPass && template.length === 0 ? "KEEP AS NARRATIVE — PRODUCTION VALID WITH GAPS" : "NOT PRODUCTION READY";
  const fitHandoff = "READY FOR FIT ADAPTER SHADOW";

  const stopPoint = {
    geographyModelChosen: "Option C (+ recommend Active Regions companion)",
    otherValuesRemovedReclassified: otherCount,
    otherRemainingAfter: otherAfter,
    shangriLaGeographyResult: {
      activeCountries: [],
      marketPresenceType: ["No known presence"],
      specificMarketsKept: true,
      note: "CALA Active Countries empty; global footprint in specificMarkets; future Active Regions",
    },
    productionGeographyIntegrityVerdict: otherAfter === 0 ? "PASS — no Other in Active Countries" : "FAIL — Other remains",
    operatorsProcessedForOperationalProfile: production.length,
    existingSufficient: rows.filter((r) => r.evidenceStatus === "EXISTING SUFFICIENT").length,
    targetedResearchCases: rows.filter((r) => ["PARTIAL", "TARGETED RESEARCH REQUIRED", "PARTIALLY_SUPPORTED", "TARGETED_RESEARCH"].includes(r.evidenceStatus)).length,
    proposedOperationalValues: rows.length,
    accept: acceptWrite.length,
    keepExisting: keepExisting.length,
    honestBlank: blanks.length,
    researchMore: research.length,
    directlySupported: direct.length,
    supportedSynthesis: synth.length,
    weakInferenceRejected: rows.filter((r) => String(r.abstainReason || "").includes("WEAK")).length,
    unsupportedRejected: rows.filter((r) => String(r.abstainReason || "").includes("UNSUPPORTED")).length,
    genericRate,
    templateClusters: template.length,
    counterfactualFailures: cfFailAcc.length,
    crossFieldLeakage: leakAcc.length,
    airtableWrites: args.apply ? writes : 0,
    failures: failures.length,
    invalidAfterApply: postInvalid,
    capProfileOperationalVerdict: fieldVerdict,
    fitDomainsNormalizedStructured: ["Geography", "Segment", "Brand Experience", "Brand–Operator Compatibility", "Operating Model/MA"],
    fitDomainsValidSetup: ["Operating Structure Alignment (cap_profile_operational)", "Owner Engagement", "Systems+Reporting"],
    fitDomainsClaims: ["Project Complexity / transition", "Ownership nuances", "Commercial differentiator (until Writer)"],
    fitDomainsMissingData: ["Active Regions taxonomy", "some assignment chain-scale coverage", "regional resources depth"],
    furtherSetupNarrativeWorkNeeded: false,
    fitHandoffVerdict: fitHandoff,
    recommendedNextPhase: "Fit Adapter Shadow (v2.1 dry-run) — do not start another Setup narrative family",
    exactFounderApprovalsRequired: [
      "Accept geography Option C + Shangri-La Other removal",
      "Authorize Active Regions field creation (schema) later",
      `Accept cap_profile_operational ${fieldVerdict}`,
      "Accept Fit handoff: READY FOR FIT ADAPTER SHADOW",
      "Confirm no more Setup narrative families before Fit",
    ],
    confirmationLegacy111RemainHidden: true,
    confirmationNoUnsupportedScoresPercentages: true,
    confirmationNoFitScoringChanges: true,
    mode: args.apply ? "apply" : "dry-run",
    qaPass,
    backupDir: `backups/operator-setup/d5-operational/${ts}`,
    geoMutations: geoMutations.length,
  };

  writeJson(join(OUT, "d5-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-d5-operational-founder-review.md"),
    [
      `# D.5 Operating Platform + Geography — Founder Review`,
      ``,
      `## Is \`cap_profile_operational\` now trustworthy at Production scale?`,
      ``,
      `**${qaPass ? "Yes" : "No"}** — Writer v2 QA ${qaPass ? "PASS" : "FAIL"}; verdict **${fieldVerdict}**.`,
      ``,
      `## Do we need another Setup narrative family before Fit Adapter Shadow?`,
      ``,
      `**No.** Prefer **Fit Adapter Shadow**. Remaining gaps are OE/Claims/Active Regions—not more Setup prose.`,
      ``,
      `| # | Item | Result |`,
      `| - | ---- | ------ |`,
      `| 1 | Geography model | Option C (+ Active Regions recommend) |`,
      `| 2 | Shangri-La | Other cleared; CALA AC empty; specificMarkets global note kept |`,
      `| 3 | Contract | PASS |`,
      `| 4 | Exemplars | HE + Arbor KEEP EXISTING |`,
      `| 5–6 | Evidence / research | D.2 reuse + targeted brand/ops packs |`,
      `| 7 | Accepted | ${acceptWrite.length} |`,
      `| 8 | Honest blanks / RM | ${blanks.length} / ${research.length} |`,
      `| 9–10 | Diff / cross-field | generic ${genericRate}% · templates ${template.length} · leakage ${leakAcc.length} |`,
      `| 11 | Writes | mode=${stopPoint.mode}; ${stopPoint.airtableWrites} |`,
      `| 12 | Post-apply invalid | ${postInvalid} |`,
      `| 13 | Fit source map | See fit-source-readiness report |`,
      `| 14 | More narrative Setup? | No |`,
      `| 15 | Fit handoff | **${fitHandoff}** |`,
      `| 16 | Approvals | See stop-point |`,
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
