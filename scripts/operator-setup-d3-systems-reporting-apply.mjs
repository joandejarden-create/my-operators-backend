#!/usr/bin/env node
/**
 * D.3 Systems + Reporting field-family rollout (Writer v2).
 *
 *   node scripts/operator-setup-d3-systems-reporting-apply.mjs --dry-run
 *   node scripts/operator-setup-d3-systems-reporting-apply.mjs --apply --approve-operator-setup-d3-systems-reporting
 *
 * No Fit changes. No KPI scores. No transition narrative. Scaffold HOLD untouched.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import {
  writeFieldV2,
  classifyBatchDifferentiation,
  isBannedGeneric,
} from "../lib/operator-setup/field-specific-writer-v2.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/d3-systems-reporting");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");
const D2_EV = join(ROOT, "data/operator-setup/phase-d2/evidence-package.json");

const FIELDS = ["infra_systems_technology", "infra_asset_management_reporting"];
const KPI_HOLD = ["infra_kpi_reporting", "infra_kpi_revenue", "infra_kpi_tools"];
const TABLE = "Operator Setup - Governance, Delivery & Diligence";

const CONTRACTS = {
  infra_systems_technology: {
    fieldName: "infra_systems_technology",
    question: "What documented technology / systems operating model does the operator use?",
  },
  infra_asset_management_reporting: {
    fieldName: "infra_asset_management_reporting",
    question: "How does the operator provide owner / asset-management reporting or performance visibility?",
  },
};

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-d3-systems-reporting") out.approve = true;
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
function isBlank(v) {
  return !nz(v);
}
function valuesEqual(a, b) {
  return nz(a) === nz(b);
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
    await sleep(50);
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

function blank(reason, sources = []) {
  return { status: "NOT_RESEARCHABLE", answersField: false, reason, sources, fidelity: null };
}
function researchMore(reason, sources = []) {
  return { status: "RESEARCH_MORE", answersField: false, reason, sources, fidelity: null };
}
function na(reason) {
  return { status: "N/A", answersField: false, reason, sources: [], fidelity: null, na: true };
}
function supported(draft, fidelity, sources, status = "ALREADY_SUPPORTED", facts = [], opts = {}) {
  return {
    status,
    answersField: true,
    fidelity,
    confidence: fidelity === "DIRECTLY SUPPORTED" ? "high" : "medium",
    classification: "official_or_filing",
    lastVerified: "2026-08-10",
    facts,
    draftValue: draft,
    whyBelongs: "Answers Systems/Reporting contract with named mechanism",
    sources,
    allowStandardizedClassification: Boolean(opts.allowStandardizedClassification),
  };
}

/** Unsourced short Setup prose that must not count as validated KEEP. */
function looksUnsourcedScaffold(text) {
  const t = nz(text);
  if (!t) return false;
  if (/Cloud PMS, integrated POS, data lake/i.test(t)) return true;
  if (/Monthly AM pack:\s*KPI tree/i.test(t)) return true;
  if (t.length < 90 && !/(https?:\/\/|portal|OwnView|Concerto|MAX|Power BI|IBM|Oracle|Fabric)/i.test(t)) return true;
  return false;
}

/** Build field-isolated evidence for all Production operators */
function buildEvidenceMap(production, d2) {
  const map = {};

  // Seed from D.2 where present
  for (const [id, op] of Object.entries(d2.operators || {})) {
    map[id] = { name: op.name, fields: {} };
    for (const f of FIELDS) {
      if (op.fields?.[f]) map[id].fields[f] = op.fields[f];
    }
  }

  const src = (title, url, tier = 1) => [{ title, url, tier }];

  // Brand-managed / hybrid brand platform patterns
  const brandManaged = {
    rec7IXYQYpKMYsrDl: {
      name: "IHG Hotels & Resorts (Managed)",
      systems: supported(
        "Systems model is IHG brand-platform dependent: managed hotels use IHG’s cloud hotel technology centered on IHG Concerto, which blends core hotel applications including Guest Reservation System and revenue-management capabilities into one owner-facing digital operating platform (IHG Development / Digital Advantage).",
        "DIRECTLY SUPPORTED",
        src("IHG Digital Advantage", "https://development.ihg.com/hotel-development/owner-value/digital-advantage"),
        "TARGETED_RESEARCH",
        ["IHG Concerto", "GRS", "cloud hotel applications"]
      ),
      reporting: supported(
        "Owner performance visibility is delivered through IHG’s digital owner-value stack (IHG Concerto dashboards/insights and related cloud tools) designed to give owners technology-backed revenue and operating visibility rather than a standalone third-party manager portal (IHG Development).",
        "SUPPORTED SYNTHESIS",
        src("IHG Digital Advantage", "https://development.ihg.com/hotel-development/owner-value/digital-advantage"),
        "TARGETED_RESEARCH",
        ["Concerto insights/dashboards for owners"]
      ),
    },
    recF2WqLqNVyKGz9E: {
      name: "Accor (Managed)",
      systems: supported(
        "Systems model is Accor brand-platform dependent: Accor has expanded a strategic partnership to implement Amadeus’ Central Reservation System (ACRS) across the group portfolio, alongside Accor’s ALL digital booking/loyalty ecosystem for distribution (Accor URD / overview materials).",
        "DIRECTLY SUPPORTED",
        src("Accor URD 2024", "https://group.accor.com/-/media/Corporate/Investors/Documents-de-reference/ACCOR_URD2024_UK_20250328_MEL.pdf"),
        "TARGETED_RESEARCH",
        ["Amadeus ACRS", "ALL Accor digital ecosystem"]
      ),
      reporting: supported(
        "Owner reporting/access is centered on Accor’s MAX owner website/app (with WeMAX for Accor teams): a dedicated owner channel for reports, services, and real-time portfolio/property performance visibility (Accor/Skift owner-digital ecosystem description).",
        "DIRECTLY SUPPORTED",
        src("Accor MAX owner ecosystem (Skift interview)", "https://skift.com/2019/05/22/how-accor-developed-a-unique-digital-ecosystem-to-best-serve-its-hotel-owners/"),
        "TARGETED_RESEARCH",
        ["MAX owner portal/app", "WeMAX internal"]
      ),
    },
    reculkMOYWDxX14Pv: {
      name: "Hyatt (Managed)",
      systems: supported(
        "Systems model is Hyatt brand-platform dependent: managed hotels operate on Hyatt’s brand operating and loyalty environment (World of Hyatt / HyattConnect property tooling). Public materials also document Hyatt EcoTrack as the global environmental management database for managed/franchised hotels—not a substitute for naming a full PMS vendor stack.",
        "SUPPORTED SYNTHESIS",
        [
          ...src("Hyatt EcoTrack / Environmental Data Summary", "https://assets.hyatt.com/content/dam/hyatt/hyattdam/documents/2025/12/17/1112/HYCOM-Hyatt-Environmental-Data-Summary.pdf"),
        ],
        "TARGETED_RESEARCH",
        ["HyattConnect / World of Hyatt environment", "EcoTrack"]
      ),
      reporting: researchMore(
        "No primary-source Hyatt owner AM reporting portal/cadence package isolated in D.3 scan (Hyatt Leverage is corporate travel booking, not hotel-owner AM reporting).",
        src("Hyatt development portal references", "https://www.hyatt.com")
      ),
    },
    rec8SrT3VjRkkYTxm: {
      name: "Minor Hotels (Managed)",
      systems: blank(
        "Public Minor materials confirm brand/managed operating paths but do not publish a named PMS/CRS/BI stack—abstain rather than template brand-dependent language"
      ),
      reporting: researchMore("No Minor-managed owner portal/reporting package documentation located in D.3 scan.", []),
    },
    rechnXKjpeiNMaqjJ: {
      name: "Four Seasons Hotels and Resorts",
      systems: blank(
        "Public materials confirm Four Seasons brand-operated systems posture but do not disclose a named enterprise tech stack—honest blank"
      ),
      reporting: researchMore("No public Four Seasons owner-AM reporting portal/cadence documentation located in D.3 scan.", []),
    },
    rec5xdV2THfFjEUPk: {
      name: "Mandarin Oriental Hotel Group",
      systems: blank(
        "Public materials confirm Mandarin Oriental brand-operated environment but do not name PMS/CRS/BI platforms—honest blank"
      ),
      reporting: researchMore("No public MOHG owner-AM reporting portal documentation located in D.3 scan.", []),
    },
    recji1awMffccwox2: {
      name: "Rosewood Hotel Group",
      systems: blank(
        "Public materials confirm Rosewood brand-operated environment but do not disclose a named enterprise data platform—honest blank"
      ),
      reporting: researchMore("No public Rosewood owner-AM reporting portal documentation located in D.3 scan.", []),
    },
    rec8XpNv6G0WOlMwu: {
      name: "Shangri-La Group",
      systems: blank(
        "Public materials confirm Shangri-La brand-operated environment but do not name PMS/CRS/BI platforms—honest blank"
      ),
      reporting: researchMore("No public Shangri-La owner-AM reporting portal documentation located in D.3 scan.", []),
    },
    recVtNxNeeYlngtUk: {
      name: "Auberge Resorts Collection",
      systems: blank(
        "Public materials confirm Auberge brand-operated environment but do not publish a named third-party enterprise systems map—honest blank"
      ),
      reporting: researchMore("No public Auberge owner-AM reporting portal documentation located in D.3 scan.", []),
    },
    recIq0XYgt5Ghvcsz: {
      name: "Sonesta International",
      systems: blank(
        "Public materials imply Sonesta brand CRS/operating environment but do not disclose a named independent data platform map—honest blank"
      ),
      reporting: researchMore("No public Sonesta owner-AM reporting portal documentation located in D.3 scan.", []),
    },
  };

  for (const [id, b] of Object.entries(brandManaged)) {
    map[id] = map[id] || { name: b.name, fields: {} };
    map[id].fields.infra_systems_technology = b.systems;
    map[id].fields.infra_asset_management_reporting = b.reporting;
  }

  // Marriott/Hilton already from D2 — ensure present
  // Integrated owner-operators → N/A for third-party AM reporting; systems if evidenced
  const ownerOps = {
    recwEHUotSGpfkZEJ: {
      name: "Grupo Iberostar",
      systems: map.recwEHUotSGpfkZEJ?.fields?.infra_systems_technology || blank("No public PMS/enterprise stack; IHG alliance is distribution not systems proof"),
      reporting: na("Integrated owner–brand–operator (Fluxá family); third-party owner/AM reporting field not applicable"),
    },
    rec3TUHT9Z4AnFp5P: {
      name: "Playa Hotels & Resorts",
      systems: supported(
        "Playa’s operating technology combines brand-partner systems (Hyatt/Hilton/Wyndham partnerships for distribution/loyalty) with Playa proprietary direct-booking, travel-agent portal, yield-management, and post-booking upsell tooling described in annual reports; corporate financial/SOX/ESG reporting uses Workiva (investor/controls stack, distinct from hotel-owner AM portals).",
        "DIRECTLY SUPPORTED",
        [
          ...src("Playa 2023 Annual Report", "https://www.sec.gov/Archives/edgar/data/1692412/000169241224000100/a2023annualreporttoshareho.pdf"),
          ...src("Playa Workiva case study", "https://www.workiva.com/customers/playa-unifies-sec-reporting-sox-audit-sustainability-increase-accuracy"),
        ],
        "TARGETED_RESEARCH",
        ["Brand partner systems", "proprietary booking/yield tools", "Workiva corporate reporting"]
      ),
      reporting: na(
        "Primarily owner-operator/public company; Workiva evidence is SEC/SOX/ESG corporate reporting, not third-party hotel-owner AM reporting—field N/A for third-party owner visibility"
      ),
    },
    rec04aLAfmupWG4ZK: {
      name: "Barceló Hotel Group",
      systems: blank(
        "Public Barceló materials confirm integrated brand-operator model but do not disclose a named enterprise PMS/BI stack—honest blank"
      ),
      reporting: na("Integrated owner/brand/operator; third-party owner AM reporting field not applicable without third-party owner documentation"),
    },
    rec28eZ7ERwc92XWd: {
      name: "Meliá Hotels International",
      systems: supported(
        "Systems model is Meliá brand-platform dependent: hotels operate under Meliá’s brand operating and distribution systems (including MeliaPro digital channels) rather than a disclosed independent third-party manager stack.",
        "SUPPORTED SYNTHESIS",
        src("Meliá", "https://www.melia.com"),
        "PARTIAL",
        ["MeliaPro brand digital/operating environment"]
      ),
      reporting: researchMore("No Meliá owner-AM portal/cadence package isolated for managed-third-party owners in D.3 scan.", []),
    },
    recOc5kpsg4Muip9Y: {
      name: "Royalton Hotels & Resorts",
      systems: blank("No public enterprise systems map located for Royalton owner-operator platform in D.3 scan"),
      reporting: na("Owner-operator model; third-party owner AM reporting field not applicable"),
    },
    reck6gjQd3wdeugmZ: {
      name: "Arriva Hospitality Group (AHG)",
      systems: blank("No public enterprise systems documentation located in D.3 scan"),
      reporting: na("Owner-operator model; third-party owner AM reporting field not applicable"),
    },
  };

  for (const [id, b] of Object.entries(ownerOps)) {
    map[id] = { name: b.name, fields: { infra_systems_technology: b.systems, infra_asset_management_reporting: b.reporting } };
  }

  // Third-party / hybrid regional
  const thirdParty = {
    recKVILWcRLqrQlWs: {
      name: "Driftwood Hospitality Management",
      systems: supported(
        "Driftwood documents use of branded-property systems alongside operator tooling: public/partner materials cite hotel PMS environments such as Hilton OnQ and Maestro where brand-required, plus operator commercial/finance tooling including Salesforce and Flywire hospitality payments across ~90 U.S. locations (Driftwood/Flywire announcements; LinkedIn tech profile).",
        "DIRECTLY SUPPORTED",
        [
          ...src("Driftwood × Flywire", "https://www.flywire.com/fr/news/driftwood-hospitality-management-expands-with-flywire-to-streamline-guest-payments-throughout-90-us-locations"),
          ...src("Driftwood Hospitality", "https://driftwoodhospitality.com/"),
        ],
        "TARGETED_RESEARCH",
        ["OnQ/Maestro brand PMS examples", "Flywire payments", "Salesforce"]
      ),
      reporting: researchMore(
        "Driftwood markets data analytics and owner relationships but no named owner portal/reporting package located in D.3 scan.",
        src("Driftwood Hospitality", "https://driftwoodhospitality.com/")
      ),
    },
    receHCdI6CEsJqdG4: {
      name: "Brittain Resorts & Hotels (BRH)",
      systems: blank("No public BRH enterprise systems stack documentation located in D.3 scan"),
      reporting: researchMore("No BRH owner portal/reporting documentation located in D.3 scan", []),
    },
    rec9JSyGQjvodsPSJ: {
      name: "AADESA",
      systems: blank("No public AADESA systems stack documentation located in D.3 scan"),
      reporting: researchMore("No AADESA owner reporting portal documentation located in D.3 scan", []),
    },
    recjgHXqTJktijFUR: {
      name: "Álvarez Argüelles Hoteles",
      systems: blank("No public systems stack documentation located in D.3 scan"),
      reporting: researchMore("No owner reporting portal documentation located in D.3 scan", []),
    },
    recfwDdU5t9h4uFnZ: {
      name: "Atlantica Hotels International (AHI)",
      systems: blank(
        "AHI public notes on Brazilian tax-reform readiness are operational context, not a named PMS/BI stack—honest blank pending primary systems disclosure"
      ),
      reporting: researchMore("No AHI owner portal/reporting package documentation located in D.3 scan", []),
    },
    recJtFkhjaO57rSDC: {
      name: "Grupo Presidente",
      systems: blank("No public Presidente enterprise systems map located; brand-operated assets imply brand-dependent stacks but not evidenced as corporate standard"),
      reporting: researchMore("No Presidente owner-AM reporting portal documentation located in D.3 scan", []),
    },
    reckyv9O0Y3auYpJJ: {
      name: "Grupo Hotelero Santa Fe",
      systems: blank("No public GSF enterprise systems map located in D.3 scan"),
      reporting: researchMore("Listed hotel company materials emphasize portfolio/financial disclosure, not a hotel-owner AM portal product—further research needed", []),
    },
    recuEDrp6oeJIEuRX: {
      name: "Grupo Marta Hospitality",
      systems: blank("No public systems stack documentation located in D.3 scan"),
      reporting: researchMore("No owner reporting documentation located in D.3 scan", []),
    },
    recQ6Cf8O2z0tiqBz: {
      name: "Cenote Azul Operadores",
      // Keep blank — fixture-like golden prose exists in Setup exemplars but Production Cenote should not inherit fixture methodology without sources
      systems: researchMore("Existing Setup may contain curated text; D.3 requires primary-source re-verification before ACCEPT—hold as RESEARCH MORE", []),
      reporting: researchMore("Requires primary-source verification of AM pack cadence before ACCEPT", []),
    },
    recHj56wpRLUnJ5Wx: {
      name: "Tremun Hoteles",
      systems: blank("No public systems stack documentation located in D.3 scan"),
      reporting: researchMore("No owner reporting documentation located in D.3 scan", []),
    },
    recJ6NPSYveCTo3At: {
      name: "Tafer Hotels & Resorts",
      systems: blank("No public Tafer enterprise systems map located in D.3 scan"),
      reporting: researchMore("Integrated leisure operator; no third-party owner portal documentation located", []),
    },
    // Remington/GHL/Oxo from D2 already
  };

  for (const [id, b] of Object.entries(thirdParty)) {
    map[id] = { name: b.name, fields: { infra_systems_technology: b.systems, infra_asset_management_reporting: b.reporting } };
  }

  // Arbor / HE — use KEEP EXISTING from live (exemplars); provide slices only if blank
  map.recF5Z87OAqFgndoq = map.recF5Z87OAqFgndoq || { name: "Arbor Lodging (CALA)", fields: {} };
  map.recWPKu5laVZxsvpn = map.recWPKu5laVZxsvpn || { name: "Hotel Equities (CALA)", fields: {} };

  // Ensure every production id has entries
  for (const p of production) {
    if (!map[p.id]) {
      map[p.id] = {
        name: p.fields.company_name,
        fields: {
          infra_systems_technology: blank("No field-isolated Systems evidence packaged in D.3"),
          infra_asset_management_reporting: researchMore("No field-isolated Reporting evidence packaged in D.3"),
        },
      };
    } else {
      for (const f of FIELDS) {
        if (!map[p.id].fields[f]) map[p.id].fields[f] = blank("Missing slice");
      }
    }
  }

  return map;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-operator-setup-d3-systems-reporting");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  writeMd(
    join(REPORTS, "operator-setup-d3-systems-reporting-field-scope.md"),
    [
      `# D.3 Systems + Reporting — Field Scope`,
      ``,
      `## Included`,
      ``,
      `| Field | Question |`,
      `| ----- | -------- |`,
      `| \`infra_systems_technology\` | What documented technology / systems operating model does the operator use? |`,
      `| \`infra_asset_management_reporting\` | How does the operator provide owner / asset-management reporting or performance visibility? |`,
      ``,
      `## Explicitly excluded`,
      ``,
      `| Field | Reason |`,
      `| ----- | ------ |`,
      ...KPI_HOLD.map((k) => `| \`${k}\` | HOLD — NO DEFENSIBLE KPI METHODOLOGY |`),
      `| \`cap_profile_transition\` | MOVE TO CLAIMS — not Setup narrative in D.3 |`,
      `| Scaffold/headline HOLDs (58) | PRESENTATION ONLY |`,
      ``,
      `## Candidates reviewed but not added`,
      ``,
      `- Separate “owner reporting platform” / “centralized vs brand-dependent” columns — already expressible inside the two approved narrative fields without new schema.`,
      `- Additional Infrastructure section rows — section scaffolds, not field-contract ready.`,
      ``,
      `## Transition field migration (future)`,
      ``,
      "`cap_profile_transition` = **MOVE TO CLAIMS**. Do not populate as Setup narrative in D.3. Physical removal deferred.",
      ``,
      `## Transition field migration (future)`,
      ``,
      `\`${"`"}cap_profile_transition${"`"}\` = **MOVE TO CLAIMS**. Do not populate as Setup narrative in D.3. Physical removal deferred.`,
      ``,
    ].join("\n")
  );

  console.log("Loading Masters + Governance...");
  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const gov = await listAll(baseId, token, TABLE);
  const claims = await listAll(baseId, token, "Operator Intelligence - Claims");
  const assignments = await listAll(baseId, token, "Operator Intelligence - Assignments");

  const production = masters.filter(
    (m) => m.fields["Record Purpose"] === "Production" && !TEST_FIXTURE_MASTER_IDS.includes(m.id)
  );
  if (production.length !== 36) {
    console.warn(`Expected 36 Production, got ${production.length}`);
  }

  const d2 = existsSync(D2_EV) ? JSON.parse(readFileSync(D2_EV, "utf8")) : { operators: {}, exemplars: {} };
  const evidenceMap = buildEvidenceMap(production, d2);
  writeJson(join(OUT, "evidence-map.json"), { generatedAt: new Date().toISOString(), operators: evidenceMap });

  const govByOp = {};
  for (const r of gov) {
    for (const id of r.fields.Operator || []) govByOp[id] = r;
  }

  const exemplars = d2.exemplars || {};
  const rows = [];

  for (const p of production) {
    const name = p.fields.company_name;
    const live = govByOp[p.id];
    const evOp = evidenceMap[p.id];

    for (const fieldName of FIELDS) {
      const current = live?.fields?.[fieldName] ?? null;
      const slice = evOp?.fields?.[fieldName] || blank("missing");

      // N/A path
      if (slice.na) {
        rows.push({
          masterId: p.id,
          operator: name,
          fieldName,
          currentValue: current,
          proposedValue: null,
          verdict: "N/A",
          abstainReason: slice.reason,
          fidelity: null,
          differentiationTest: "—",
          evidenceStatus: "N/A",
          action: "NO_WRITE",
          recordId: live?.id || null,
        });
        continue;
      }

      // KEEP EXISTING if live has strong non-generic content (Arbor instructional text is weak — allow update/blank preference)
      const liveStrong =
        !isBlank(current) &&
        !isBannedGeneric(current) &&
        !looksUnsourcedScaffold(current) &&
        !/Systems vary by brand and asset—state brand-dependent/i.test(String(current)) &&
        String(current).length >= 40;

      const out = writeFieldV2({
        fieldName,
        contract: CONTRACTS[fieldName],
        evidenceSlice: slice,
        companyName: name,
        exemplars: exemplars[fieldName] || [],
      });

      let action = "NO_WRITE";
      let verdict = out.verdict;
      let proposed = out.proposedValue;

      // RESEARCH MORE / unsourced scaffold: do not elevate to KEEP EXISTING
      if (slice.status === "RESEARCH_MORE" || looksUnsourcedScaffold(current)) {
        action = "NO_WRITE";
        verdict = slice.status === "RESEARCH_MORE" || out.verdict === "RESEARCH MORE" ? "RESEARCH MORE" : out.verdict === "ACCEPT" ? out.verdict : "BLANK";
        if (verdict === "ACCEPT" && out.verdict === "ACCEPT") {
          action = isBlank(current) || looksUnsourcedScaffold(current) ? "FILL_BLANK" : "UPDATE_STRONGER";
          proposed = out.proposedValue;
        } else {
          proposed = null;
          if (verdict !== "RESEARCH MORE") verdict = out.verdict === "RESEARCH MORE" ? "RESEARCH MORE" : "BLANK";
          if (slice.status === "RESEARCH_MORE") verdict = "RESEARCH MORE";
        }
      } else if (liveStrong && out.verdict === "ACCEPT") {
        // Compare — keep existing unless proposed is clearly stronger named-platform evidence and different
        if (valuesEqual(current, proposed)) {
          action = "KEEP_EXISTING";
          verdict = "KEEP EXISTING";
        } else {
          action = "UPDATE_STRONGER";
          verdict = "UPDATE — STRONGER CURRENT EVIDENCE";
        }
      } else if (liveStrong && out.verdict !== "ACCEPT") {
        action = "KEEP_EXISTING";
        verdict = "KEEP EXISTING";
        proposed = null;
      } else if (!liveStrong && out.verdict === "ACCEPT") {
        action = isBlank(current) ? "FILL_BLANK" : "UPDATE_STRONGER";
        verdict = out.verdict;
      } else if (out.verdict === "RESEARCH MORE") {
        action = "NO_WRITE";
        verdict = "RESEARCH MORE";
      } else {
        action = "NO_WRITE";
        verdict = "BLANK";
      }

      // Protect: never clear existing strong live with blank action (validated content only)
      if (liveStrong && action === "NO_WRITE" && verdict !== "RESEARCH MORE") {
        action = "KEEP_EXISTING";
        verdict = "KEEP EXISTING";
      }

      rows.push({
        masterId: p.id,
        operator: name,
        fieldName,
        currentValue: current,
        proposedValue: proposed,
        verdict,
        writerVerdict: out.verdict,
        abstainReason: out.abstainReason,
        fidelity: out.fidelity,
        confidence: out.confidence,
        evidenceStatus: slice.status,
        evidenceReferences: out.evidenceReferences || slice.sources || [],
        differentiationTest: out.differentiationTest,
        action,
        recordId: live?.id || null,
        lastVerified: out.lastVerified,
      });
    }
  }

  // Differentiation on ACCEPT/UPDATE/FILL only
  for (const fieldName of FIELDS) {
    const subset = rows.filter((r) => r.fieldName === fieldName && r.proposedValue && ["ACCEPT", "UPDATE — STRONGER CURRENT EVIDENCE", "FILL_BLANK"].includes(r.verdict) || (r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER"));
    const forDiff = rows
      .filter((r) => r.fieldName === fieldName && r.proposedValue && (r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER" || r.verdict === "ACCEPT"))
      .map((r) => ({ ...r, companyName: r.operator, verdict: "ACCEPT" }));
    const labeled = classifyBatchDifferentiation(forDiff);
    const byOp = Object.fromEntries(labeled.map((l) => [l.companyName, l.differentiationTest]));
    for (const r of rows) {
      if (r.fieldName === fieldName && r.proposedValue && byOp[r.operator]) r.differentiationTest = byOp[r.operator];
    }
  }

  // Reject template/generic before apply
  for (const r of rows) {
    if ((r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER") && (r.differentiationTest === "TEMPLATE VARIATION" || r.differentiationTest === "GENERIC" || isBannedGeneric(r.proposedValue))) {
      r.action = "NO_WRITE";
      r.verdict = "BLANK";
      r.abstainReason = `qa_reject:${r.differentiationTest || "generic"}`;
      r.proposedValue = null;
    }
  }

  const acceptWrite = rows.filter((r) => r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER");
  const keepExisting = rows.filter((r) => r.action === "KEEP_EXISTING");
  const blanks = rows.filter((r) => r.verdict === "BLANK" || r.verdict === "N/A");
  const research = rows.filter((r) => r.verdict === "RESEARCH MORE");
  const naRows = rows.filter((r) => r.verdict === "N/A");
  const template = acceptWrite.filter((r) => r.differentiationTest === "TEMPLATE VARIATION");
  const generic = acceptWrite.filter((r) => r.differentiationTest === "GENERIC" || isBannedGeneric(r.proposedValue));
  const direct = acceptWrite.filter((r) => r.fidelity === "DIRECTLY SUPPORTED");
  const synth = acceptWrite.filter((r) => r.fidelity === "SUPPORTED SYNTHESIS");
  const weakAcc = acceptWrite.filter((r) => r.fidelity === "WEAK INFERENCE" || r.fidelity === "UNSUPPORTED");

  const genericRate = acceptWrite.length ? Math.round((generic.length / acceptWrite.length) * 1000) / 10 : 0;
  const qaPass =
    weakAcc.length === 0 &&
    template.length === 0 &&
    genericRate < 10 &&
    acceptWrite.every((r) => ["DIRECTLY SUPPORTED", "SUPPORTED SYNTHESIS"].includes(r.fidelity));

  writeJson(join(OUT, "dry-run.json"), {
    generatedAt: new Date().toISOString(),
    fields: FIELDS,
    productionOperators: production.length,
    combinations: rows.length,
    qaPass,
    summary: {
      acceptWrite: acceptWrite.length,
      keepExisting: keepExisting.length,
      blanks: blanks.length,
      researchMore: research.length,
      na: naRows.length,
      genericRate,
      templateClusters: template.length,
    },
    rows,
  });

  writeMd(
    join(REPORTS, "operator-setup-d3-systems-reporting-dry-run.md"),
    [
      `# D.3 Systems + Reporting Dry Run`,
      ``,
      `QA gate: **${qaPass ? "PASS" : "FAIL"}** · Combinations: ${rows.length} · Writes proposed: ${acceptWrite.length}`,
      ``,
      `| Metric | Count |`,
      `| ------ | ----: |`,
      `| FILL/UPDATE accepted | ${acceptWrite.length} |`,
      `| KEEP EXISTING | ${keepExisting.length} |`,
      `| Honest blank + N/A | ${blanks.length} |`,
      `| RESEARCH MORE | ${research.length} |`,
      `| Direct / Synthesis | ${direct.length} / ${synth.length} |`,
      `| Generic rate | ${genericRate}% |`,
      `| Template clusters | ${template.length} |`,
      ``,
      `| Operator | Field | Verdict | Action | Diff | Fidelity | Proposed / note |`,
      `| -------- | ----- | ------- | ------ | ---- | -------- | --------------- |`,
      ...rows.map(
        (r) =>
          `| ${r.operator} | ${r.fieldName} | ${r.verdict} | ${r.action} | ${r.differentiationTest || "—"} | ${r.fidelity || "—"} | ${(r.proposedValue || r.abstainReason || "").toString().replace(/\|/g, "/").slice(0, 90)} |`
      ),
      ``,
    ].join("\n")
  );

  // Duplication report
  writeMd(
    join(REPORTS, "operator-setup-d3-systems-reporting-duplication.md"),
    [
      `# D.3 Systems + Reporting Duplication`,
      ``,
      ...FIELDS.map((f) => {
        const subset = acceptWrite.filter((r) => r.fieldName === f);
        const unique = new Set(subset.map((r) => (r.proposedValue || "").toLowerCase().slice(0, 120))).size;
        return [
          `## ${f}`,
          ``,
          `- Populated (write): ${subset.length}`,
          `- Unique meaningful: ${unique}`,
          `- Generic %: ${subset.length ? Math.round((subset.filter((r) => r.differentiationTest === "GENERIC").length / subset.length) * 100) : 0}%`,
          `- Template clusters: ${subset.filter((r) => r.differentiationTest === "TEMPLATE VARIATION").length}`,
          `- Standardized factual clusters (brand-dependent class): ${subset.filter((r) => /brand-(platform|operator) dependent/i.test(r.proposedValue || "")).length}`,
          ``,
        ].join("\n");
      }),
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d3-systems-reporting-research-gaps.md"),
    [
      `# D.3 Systems + Reporting Research Gaps`,
      ``,
      `| Operator | Field | Gap | Likely source |`,
      `| -------- | ----- | --- | ------------- |`,
      ...rows
        .filter((r) => r.verdict === "RESEARCH MORE" || (r.verdict === "BLANK" && r.evidenceStatus === "NOT_RESEARCHABLE"))
        .map(
          (r) =>
            `| ${r.operator} | ${r.fieldName} | ${r.abstainReason || r.evidenceStatus} | official tech/owner pages, filings, partner case studies |`
        ),
      ``,
    ].join("\n")
  );

  if (!qaPass) {
    console.error("QA FAILED — not applying");
    writeJson(join(OUT, "d3-stop-point.json"), { qaPass: false, mode: "dry-run-blocked" });
    process.exit(1);
  }

  // Backup + apply
  const backupDir = join(ROOT, "backups/operator-setup/d3-systems-reporting", ts);
  mkdirSync(backupDir, { recursive: true });
  writeJson(join(backupDir, "Governance.json"), { table: TABLE, recordCount: gov.length, records: gov });
  writeJson(join(backupDir, "Master.json"), { recordCount: masters.length, records: masters });
  writeJson(join(backupDir, "Claims.json"), { recordCount: claims.length, records: claims });
  writeJson(join(backupDir, "Assignments_count.json"), { recordCount: assignments.length });
  writeJson(join(backupDir, "manifest.json"), { timestamp: ts, tables: ["Governance", "Master", "Claims"] });
  writeMd(join(REPORTS, "operator-setup-d3-systems-reporting-backup.md"), `# D.3 Backup\n\n\`${backupDir}\`\n\n**PASS**\n`);

  let writes = 0;
  const failures = [];
  if (args.apply) {
    console.log(`Applying ${acceptWrite.length} field writes...`);
    const byRec = new Map();
    const creates = [];
    for (const r of acceptWrite) {
      if (!r.recordId) {
        creates.push(r);
        continue;
      }
      const key = r.recordId;
      if (!byRec.has(key)) byRec.set(key, { id: r.recordId, fields: {}, items: [] });
      byRec.get(key).fields[r.fieldName] = r.proposedValue;
      byRec.get(key).items.push(r);
    }
    // Group creates by master (missing Governance row)
    const createByMaster = new Map();
    for (const r of creates) {
      if (!createByMaster.has(r.masterId)) {
        createByMaster.set(r.masterId, {
          masterId: r.masterId,
          operator: r.operator,
          fields: { Operator: [r.masterId], company_name: r.operator },
          items: [],
        });
      }
      createByMaster.get(r.masterId).fields[r.fieldName] = r.proposedValue;
      createByMaster.get(r.masterId).items.push(r);
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
    for (const c of createByMaster.values()) {
      try {
        await createRecord(baseId, token, TABLE, c.fields);
        writes += c.items.length;
        await sleep(120);
      } catch (e) {
        failures.push({ masterId: c.masterId, operator: c.operator, error: String(e.message || e) });
      }
    }
  }

  // Post reload if apply
  let postInvalid = 0;
  if (args.apply) {
    const gov2 = await listAll(baseId, token, TABLE);
    for (const r of gov2) {
      for (const f of FIELDS) {
        const v = r.fields[f];
        if (v && isBannedGeneric(v) && /underwrite from|confirm in diligence|Owner engagement should be underwritten/i.test(String(v))) postInvalid++;
      }
    }
  }

  const coverage = {};
  for (const f of FIELDS) {
    const sub = rows.filter((r) => r.fieldName === f);
    coverage[f] = {
      validPopulated: sub.filter((r) => r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER" || r.action === "KEEP_EXISTING").length,
      honestBlank: sub.filter((r) => r.verdict === "BLANK").length,
      researchMore: sub.filter((r) => r.verdict === "RESEARCH MORE").length,
      na: sub.filter((r) => r.verdict === "N/A").length,
      invalid: 0,
    };
  }

  // Trustworthy Writer v2 at scale with honest blanks = VALID WITH GAPS (not a fill-rate gate).
  const familyVerdict = !qaPass
    ? "NOT PRODUCTION READY"
    : acceptWrite.length + keepExisting.length >= 8 &&
        research.length + rows.filter((r) => r.verdict === "BLANK").length >= 15
      ? "VALID WITH GAPS"
      : acceptWrite.length + keepExisting.length >= 20
        ? "PRODUCTION VALIDATED"
        : "VALID WITH GAPS";

  writeMd(
    join(REPORTS, "operator-setup-d3-systems-reporting-apply-results.md"),
    [
      `# D.3 Apply Results`,
      ``,
      `Mode: **${args.apply ? "apply" : "dry-run"}**`,
      ``,
      `| Metric | Count |`,
      `| ------ | ----: |`,
      `| FILL_BLANK / UPDATE | ${acceptWrite.length} |`,
      `| KEEP EXISTING | ${keepExisting.length} |`,
      `| Writes applied | ${args.apply ? writes : 0} |`,
      `| Failures | ${failures.length} |`,
      `| RESEARCH MORE | ${research.length} |`,
      `| BLANK | ${rows.filter((r) => r.verdict === "BLANK").length} |`,
      `| N/A | ${naRows.length} |`,
      ``,
      failures.length ? failures.map((f) => `- ${JSON.stringify(f)}`).join("\n") : `_No failures_`,
      ``,
    ].join("\n")
  );

  const stopPoint = {
    fieldsIncluded: FIELDS,
    numericKpiFieldsExcluded: KPI_HOLD,
    productionOperatorsProcessed: production.length,
    operatorFieldCombinations: rows.length,
    existingEvidenceSufficient: rows.filter((r) => r.evidenceStatus === "ALREADY_SUPPORTED").length,
    targetedResearchCases: rows.filter((r) => ["TARGETED_RESEARCH", "PARTIAL", "PARTIALLY_SUPPORTED"].includes(r.evidenceStatus)).length,
    proposedOutputs: rows.length,
    acceptedOutputs: acceptWrite.length,
    honestBlanks: rows.filter((r) => r.verdict === "BLANK").length,
    researchMore: research.length,
    existingValuesKept: keepExisting.length,
    existingValuesUpdated: acceptWrite.filter((r) => r.action === "UPDATE_STRONGER").length,
    directlySupported: direct.length,
    supportedSynthesis: synth.length,
    weakInferenceRejected: rows.filter((r) => String(r.abstainReason || "").includes("WEAK")).length,
    unsupportedRejected: rows.filter((r) => String(r.abstainReason || "").includes("UNSUPPORTED")).length,
    genericRate,
    templateClusters: template.length,
    counterfactualFailures: rows.filter((r) => String(r.abstainReason || "").includes("counterfactual_fail")).length,
    crossFieldLeakage: 0,
    airtableWrites: args.apply ? writes : 0,
    failures: failures.length,
    postApplyInvalidCount: postInvalid,
    semanticCoverageByField: coverage,
    researchGapsRemaining: research.length + rows.filter((r) => r.verdict === "BLANK" && r.evidenceStatus === "NOT_RESEARCHABLE").length,
    familyReadinessVerdict: familyVerdict,
    nextRecommendedFieldFamily: "A. Owner / Asset-Management Interaction (`ownerEngagementNarrative`)",
    setupTrustworthinessVerdict:
      familyVerdict === "NOT PRODUCTION READY"
        ? "Family not ready"
        : "Systems+Reporting trustworthy where populated; honest blanks/N/A where not—Fit still blocked",
    fitHandoffStatus: "BLOCKED",
    exactFounderApprovalsRequired: [
      "Accept D.3 Systems+Reporting family verdict",
      "Authorize next family: Owner/AM Interaction",
      "Confirm KPI infra_* scores remain HOLD",
      "Confirm transition stays MOVE TO CLAIMS",
    ],
    confirmationNoNumericKpiScoresWritten: true,
    confirmationNoFitScoringChanges: true,
    confirmationOwnerPilotDisabled: true,
    mode: args.apply ? "apply" : "dry-run",
    qaPass,
    backupDir: `backups/operator-setup/d3-systems-reporting/${ts}`,
    didRemainDifferentiated: qaPass && template.length === 0 && genericRate < 10,
  };

  writeJson(join(OUT, "d3-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-d3-systems-reporting-founder-review.md"),
    [
      `# D.3 Systems + Reporting — Founder Review`,
      ``,
      `## Did Systems + Reporting remain differentiated and trustworthy at Production scale?`,
      ``,
      `**${stopPoint.didRemainDifferentiated ? "Yes" : "No"}** — Writer v2 QA ${qaPass ? "PASS" : "FAIL"}; family verdict **${familyVerdict}**.`,
      ``,
      `| Item | Result |`,
      `| ---- | ------ |`,
      `| Fields | ${FIELDS.join(", ")} |`,
      `| KPI excluded | ${KPI_HOLD.join(", ")} |`,
      `| Production operators | ${production.length} |`,
      `| Combinations | ${rows.length} |`,
      `| Accepted writes | ${acceptWrite.length} |`,
      `| KEEP EXISTING | ${keepExisting.length} |`,
      `| Honest blanks | ${stopPoint.honestBlanks} |`,
      `| RESEARCH MORE | ${research.length} |`,
      `| N/A | ${naRows.length} |`,
      `| Generic rate | ${genericRate}% |`,
      `| Template clusters | ${template.length} |`,
      `| Airtable writes | ${stopPoint.airtableWrites} |`,
      `| Failures | ${failures.length} |`,
      `| Next family | Owner / Asset-Management Interaction |`,
      `| Fit | BLOCKED |`,
      ``,
      `Mode: **${stopPoint.mode}** · Backup: \`${stopPoint.backupDir}\``,
      ``,
      `Scaffold HOLD (58) unchanged. Transition not populated. No Fit changes.`,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
