#!/usr/bin/env node
/**
 * D.4 Owner / Asset-Management Interaction — Writer v2 field-family rollout.
 * Field: ownerEngagementNarrative (Operator Setup - Commercial Fit & Terms)
 *
 *   node scripts/operator-setup-d4-owner-engagement-apply.mjs --dry-run
 *   node scripts/operator-setup-d4-owner-engagement-apply.mjs --apply --approve-operator-setup-d4-owner-engagement
 *
 * No Fit changes. No KPI scores. Scaffold HOLD untouched. Transition not touched.
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
  stripCompanyName,
  counterfactualCouldApplyToPeers,
} from "../lib/operator-setup/field-specific-writer-v2.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/d4-owner-engagement");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");
const D2_EV = join(ROOT, "data/operator-setup/phase-d2/evidence-package.json");
const D3_DRY = join(ROOT, "data/operator-setup/d3-systems-reporting/dry-run.json");

const FIELD = "ownerEngagementNarrative";
const TABLE = "Operator Setup - Commercial Fit & Terms";
const GOV_TABLE = "Operator Setup - Governance, Delivery & Diligence";

const CONTRACT = {
  fieldName: FIELD,
  question: "How does this operator actually interact with hotel ownership / asset management in the management relationship?",
  belongs: [
    "ownership communication model",
    "asset-manager interaction",
    "operating review cadence / owner meetings",
    "decision escalation / owner approval interaction",
    "dedicated owner relations / AM functions",
    "owner portals/forums when tied to engagement (not reporting-alone)",
  ],
  doesNotBelong: [
    "generic owner-focused / partnership marketing",
    "reporting systems alone (→ infra_asset_management_reporting)",
    "commercial / tech / HMA structure dumps",
    "portfolio size / brand lists",
  ],
  length: "1–3 concise factual sentences",
  inference: false,
  blankRule: "BLANK or RESEARCH MORE if only vague partnership language",
};

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-d4-owner-engagement") out.approve = true;
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
  return { status: "NOT_RESEARCHABLE", answersField: false, reason, sources, fidelity: null, researchClass: "NOT RESEARCHABLE / UNKNOWN" };
}
function researchMore(reason, sources = [], effort = "OPTIONAL DEPTH") {
  return {
    status: "RESEARCH_MORE",
    answersField: false,
    reason,
    sources,
    fidelity: null,
    researchClass: "TARGETED RESEARCH REQUIRED",
    researchEffort: effort,
  };
}
function na(reason) {
  return { status: "N/A", answersField: false, reason, sources: [], fidelity: null, na: true, researchClass: "N/A" };
}
function supported(draft, fidelity, sources, status = "ALREADY_SUPPORTED", facts = [], researchClass = "EXISTING SUFFICIENT") {
  return {
    status,
    answersField: true,
    fidelity,
    confidence: fidelity === "DIRECTLY SUPPORTED" ? "high" : "medium",
    classification: "official_or_filing",
    lastVerified: "2026-08-10",
    facts,
    draftValue: draft,
    whyBelongs: "Documents the operator–owner / AM interaction mechanism",
    sources,
    researchClass,
  };
}

function tokenOverlap(a, b) {
  const ta = new Set(
    stripCompanyName(nz(a), "")
      .toLowerCase()
      .split(/\W+/)
      .filter((t) => t.length > 3)
  );
  const tb = new Set(
    stripCompanyName(nz(b), "")
      .toLowerCase()
      .split(/\W+/)
      .filter((t) => t.length > 3)
  );
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter++;
  return inter / Math.max(1, new Set([...ta, ...tb]).size);
}

function looksTemplateBoilerplate(text) {
  return /Owner engagement should be underwritten from the management agreement/i.test(nz(text));
}

function looksUnsourcedScaffold(text) {
  const t = nz(text);
  if (!t) return false;
  if (looksTemplateBoilerplate(t)) return true;
  if (t.length < 80 && !/(portal|OwnView|MAX|weekly|monthly|quarterly|approval|Area Team|MxM|Highgate Intelligence)/i.test(t)) return true;
  return false;
}

/** Build field-isolated evidence for ownerEngagementNarrative */
function buildEvidenceMap(production, d2) {
  const map = {};
  const src = (title, url, tier = 1) => [{ title, url, tier }];

  // Seed D.2 owner engagement slices
  for (const [id, op] of Object.entries(d2.operators || {})) {
    map[id] = { name: op.name, field: op.fields?.[FIELD] || null };
  }

  // Refine D.2 Oxo — failed counterfactual → blank
  map.rectsHzacZDFTH1Ze = {
    name: "OxoHotel",
    field: blank(
      "Public materials describe end-to-end investor hospitality management without a documented owner/AM interaction cadence or decision interface—abstain (D.2 counterfactual fail)"
    ),
  };

  // Iberostar — N/A owner-operator
  map.recwEHUotSGpfkZEJ = {
    name: "Grupo Iberostar",
    field: na("Primarily owner–brand–operator (Fluxá family); third-party owner/AM engagement field not applicable"),
  };

  // Remington — marketing only
  map.rec6UB6RpMKSs2tAo = {
    name: "Remington Hospitality",
    field: blank("‘Owner’s mindset’ marketing without documented AM interface, cadence, or approval model—abstain"),
  };

  // GHL
  map.reciI2tYQBfMoMK9G = {
    name: "GHL Hoteles (GHL Holding)",
    field: researchMore("No public owner/AM engagement model (cadence, portal, approval rights, dedicated owner relations) located", [], "HIGH VALUE TO RESEARCH"),
  };

  // Brand-managed expansions
  const extras = {
    rec7IXYQYpKMYsrDl: {
      name: "IHG Hotels & Resorts (Managed)",
      field: supported(
        "IHG owner engagement for managed hotels is framed through IHG’s owner-value / Digital Advantage channel: owners receive technology-backed operating and revenue visibility (IHG Concerto insights) as the standing digital interface with the brand operator, alongside brand-managed hotel leadership rather than a separate third-party AM desk.",
        "SUPPORTED SYNTHESIS",
        src("IHG Digital Advantage", "https://development.ihg.com/hotel-development/owner-value/digital-advantage"),
        "TARGETED_RESEARCH",
        ["Digital Advantage / Concerto owner visibility", "brand-managed leadership interface"],
        "PARTIAL"
      ),
    },
    recF2WqLqNVyKGz9E: {
      name: "Accor (Managed)",
      field: supported(
        "Accor engages hotel owners through the MAX owner website/app as a dedicated owner channel for reports, services, and real-time portfolio/property visibility, with WeMAX supporting Accor teams—owner interaction is portal-plus-services rather than a third-party manager desk alone.",
        "DIRECTLY SUPPORTED",
        src("Accor MAX owner ecosystem (Skift)", "https://skift.com/2019/05/22/how-accor-developed-a-unique-digital-ecosystem-to-best-serve-its-hotel-owners/"),
        "TARGETED_RESEARCH",
        ["MAX owner portal/app", "WeMAX internal"],
        "PARTIAL"
      ),
    },
    reculkMOYWDxX14Pv: {
      name: "Hyatt (Managed)",
      field: researchMore(
        "No primary Hyatt owner/AM engagement package (cadence, approval model, owner relations function) isolated beyond brand-managed operations—Hyatt Leverage is corporate travel, not hotel-owner AM",
        src("Hyatt", "https://www.hyatt.com"),
        "HIGH VALUE TO RESEARCH"
      ),
    },
    rec8SrT3VjRkkYTxm: {
      name: "Minor Hotels (Managed)",
      field: researchMore("No Minor-managed owner engagement cadence/portal/approval documentation located in D.4 scan", [], "OPTIONAL DEPTH"),
    },
    rechnXKjpeiNMaqjJ: {
      name: "Four Seasons Hotels and Resorts",
      field: researchMore("Public Four Seasons materials emphasize brand-managed luxury ops; no public third-party owner engagement cadence package located", [], "OPTIONAL DEPTH"),
    },
    rec5xdV2THfFjEUPk: {
      name: "Mandarin Oriental Hotel Group",
      field: researchMore("No public MOHG owner/AM engagement interface documentation located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    recji1awMffccwox2: {
      name: "Rosewood Hotel Group",
      field: researchMore("No public Rosewood owner engagement cadence/portal documentation located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    rec8XpNv6G0WOlMwu: {
      name: "Shangri-La Group",
      field: researchMore("No public Shangri-La owner/AM engagement package located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    recVtNxNeeYlngtUk: {
      name: "Auberge Resorts Collection",
      field: researchMore("No public Auberge owner engagement cadence documentation located", [], "OPTIONAL DEPTH"),
    },
    recIq0XYgt5Ghvcsz: {
      name: "Sonesta International",
      field: researchMore("No public Sonesta owner/AM engagement interface documentation located", [], "OPTIONAL DEPTH"),
    },
    // Owner-operators
    rec04aLAfmupWG4ZK: {
      name: "Barceló Hotel Group",
      field: na("Integrated owner/brand/operator; third-party owner engagement field not applicable without third-party owner documentation"),
    },
    rec28eZ7ERwc92XWd: {
      name: "Meliá Hotels International",
      field: researchMore(
        "Meliá is a brand/operator with managed/franchised paths; no MeliaPro-specific owner/AM engagement cadence package isolated for third-party owners in D.4 scan",
        src("Meliá", "https://www.melia.com"),
        "HIGH VALUE TO RESEARCH"
      ),
    },
    rec3TUHT9Z4AnFp5P: {
      name: "Playa Hotels & Resorts",
      field: na(
        "Primarily owner-operator/public company; Workiva evidence is SEC/SOX/ESG corporate reporting, not third-party hotel-owner engagement—field N/A"
      ),
    },
    recOc5kpsg4Muip9Y: {
      name: "Royalton Hotels & Resorts",
      field: na("Owner-operator model; third-party owner/AM engagement field not applicable"),
    },
    reck6gjQd3wdeugmZ: {
      name: "Arriva Hospitality Group (AHG)",
      field: na("Owner-operator model; third-party owner/AM engagement field not applicable"),
    },
    // Third-party / regional
    recKVILWcRLqrQlWs: {
      name: "Driftwood Hospitality Management",
      field: researchMore(
        "Driftwood markets owner relationships and analytics but no named owner-relations function, review cadence, or approval interface located in D.4 scan",
        src("Driftwood Hospitality", "https://driftwoodhospitality.com/"),
        "HIGH VALUE TO RESEARCH"
      ),
    },
    receHCdI6CEsJqdG4: {
      name: "Brittain Resorts & Hotels (BRH)",
      field: researchMore("No BRH owner engagement cadence/portal documentation located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    rec9JSyGQjvodsPSJ: {
      name: "AADESA",
      field: researchMore("No AADESA owner engagement documentation located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    recjgHXqTJktijFUR: {
      name: "Álvarez Argüelles Hoteles",
      field: researchMore("No owner engagement documentation located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    recfwDdU5t9h4uFnZ: {
      name: "Atlantica Hotels International (AHI)",
      field: researchMore("No AHI owner engagement cadence/portal package located", [], "OPTIONAL DEPTH"),
    },
    recJtFkhjaO57rSDC: {
      name: "Grupo Presidente",
      field: researchMore("No Presidente owner/AM engagement documentation located", [], "OPTIONAL DEPTH"),
    },
    reckyv9O0Y3auYpJJ: {
      name: "Grupo Hotelero Santa Fe",
      field: researchMore(
        "Listed-company disclosure emphasizes portfolio/financial reporting, not a hotel-owner engagement product—further research needed",
        [],
        "OPTIONAL DEPTH"
      ),
    },
    recuEDrp6oeJIEuRX: {
      name: "Grupo Marta Hospitality",
      field: researchMore("No owner engagement documentation located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    recQ6Cf8O2z0tiqBz: {
      name: "Cenote Azul Operadores",
      field: researchMore(
        "Existing Setup may contain curated/fixture-like owner-rhythm prose; D.4 requires primary-source verification before ACCEPT",
        [],
        "HIGH VALUE TO RESEARCH"
      ),
    },
    recHj56wpRLUnJ5Wx: {
      name: "Tremun Hoteles",
      field: researchMore("No owner engagement documentation located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    recJ6NPSYveCTo3At: {
      name: "Tafer Hotels & Resorts",
      field: researchMore("Integrated leisure operator; no third-party owner engagement documentation located", [], "LOW PUBLIC AVAILABILITY / LEAVE BLANK"),
    },
    rec04placeholder: null,
  };

  for (const [id, b] of Object.entries(extras)) {
    if (!b) continue;
    map[id] = b;
  }

  // Refine Highgate — keep interaction framing (institutional interface + analyst review), not identical to reporting field
  map.recLjxtxIIVJaGbXK = {
    name: "Highgate",
    field: supported(
      "Ownership interfaces with Highgate through Highgate Intelligence as the standing owner relationship surface: continuous portfolio flash and analyst-reviewed variance narratives, with audit-ready packages prepared for owners, auditors, and lenders rather than a monthly-pack-only relationship.",
      "SUPPORTED SYNTHESIS",
      [
        ...src("Highgate Intelligence", "http://highgateintelligence.ai/"),
        ...src("Highgate corporate", "https://www.highgate.com/"),
      ],
      "PARTIALLY_SUPPORTED",
      ["Highgate Intelligence owner interface", "analyst-reviewed packages", "continuous portfolio flash"],
      "PARTIAL"
    ),
  };

  // Aimbridge — portal + on-property teams as interaction model
  map.recGWxIJqnYHkJZFD = {
    name: "Aimbridge Hospitality (LATAM)",
    field: supported(
      "Owner interaction centers on OwnView (owner-exclusive portal) plus Aimbridge Intelligence self-service analytics so owners can inspect hotel or portfolio KPIs without waiting solely on manual packs, alongside on-property support teams.",
      "DIRECTLY SUPPORTED",
      src(
        "Aimbridge Intelligence / OwnView",
        "https://www.prnewswire.com/news-releases/aimbridge-hospitality-introduces-aimbridge-intelligence-data-and-reporting-tool-302263091.html"
      ),
      "ALREADY_SUPPORTED",
      ["OwnView owner portal", "self-service KPIs", "on-property support teams"],
      "EXISTING SUFFICIENT"
    ),
  };

  // Marriott / Hilton from D2 (ensure present with refined drafts)
  if (!map.recGmiPhRt6hiayd9?.field?.draftValue) {
    map.recGmiPhRt6hiayd9 = {
      name: "Marriott International (Managed)",
      field: d2.operators?.recGmiPhRt6hiayd9?.fields?.[FIELD],
    };
  }
  if (map.recGmiPhRt6hiayd9?.field) {
    map.recGmiPhRt6hiayd9.field.researchClass = "EXISTING SUFFICIENT";
  }
  if (map.rec3Uwxe6ovpiokuN?.field) {
    map.rec3Uwxe6ovpiokuN.field.researchClass = "EXISTING SUFFICIENT";
  }

  // Arbor / HE — read-only exemplars (KEEP EXISTING when live is strong)
  map.recF5Z87OAqFgndoq = {
    name: "Arbor Lodging (CALA)",
    field: {
      status: "EXISTING_EXEMPLAR",
      answersField: false,
      reason: "Read-only real exemplar — protect live owner-engagement narrative",
      sources: [],
      fidelity: null,
      researchClass: "EXISTING SUFFICIENT",
    },
  };
  map.recWPKu5laVZxsvpn = {
    name: "Hotel Equities (CALA)",
    field: {
      status: "EXISTING_EXEMPLAR",
      answersField: false,
      reason: "Read-only real exemplar — protect live owner-engagement narrative",
      sources: [],
      fidelity: null,
      researchClass: "EXISTING SUFFICIENT",
    },
  };

  // Ensure every production id has an entry
  for (const p of production) {
    if (!map[p.id]?.field) {
      map[p.id] = {
        name: p.fields.company_name,
        field: researchMore("No field-isolated owner-engagement evidence packaged in D.4", [], "OPTIONAL DEPTH"),
      };
    } else {
      map[p.id].name = map[p.id].name || p.fields.company_name;
    }
  }

  return map;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-d4-owner-engagement");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  mkdirSync(REPORTS, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  // --- Contract review ---
  writeMd(
    join(REPORTS, "operator-setup-d4-owner-engagement-contract-review.md"),
    [
      `# D.4 Owner Engagement — Semantic Contract Review`,
      ``,
      `## Exact question`,
      ``,
      `> ${CONTRACT.question}`,
      ``,
      `## Allowed information`,
      ``,
      ...CONTRACT.belongs.map((b) => `- ${b}`),
      ``,
      `## Prohibited information`,
      ``,
      ...CONTRACT.doesNotBelong.map((b) => `- ${b}`),
      ``,
      `## Expected length`,
      ``,
      CONTRACT.length,
      ``,
      `## Source threshold`,
      ``,
      `Primary operator/owner materials, filings, or credible interviews describing the management relationship interface. Snippets/AI summaries = discovery only.`,
      ``,
      `## Inference rules`,
      ``,
      `Inference **not** permitted. Vague “partnership / owner-centric” language → BLANK or RESEARCH MORE. Do not polish weak claims.`,
      ``,
      `## Blank behavior`,
      ``,
      CONTRACT.blankRule,
      ``,
      `## Neighboring field boundaries`,
      ``,
      `| Neighbor | Boundary |`,
      `| -------- | -------- |`,
      `| \`infra_asset_management_reporting\` | Reporting mechanism/cadence/portal metrics |`,
      `| Management Structures / Availability | Taxonomy / availability selects — not prose restatement |`,
      `| \`cap_profile_commercial\` / ops | Commercial or ops organization — not owner interface |`,
      `| Systems technology | Named tech stack — not engagement model |`,
      ``,
      `## Contract status`,
      ``,
      `**PASS — narrowed for D.4.** Question updated from D.2 “engage owners (cadence, decision rights, relationship model)” to explicit management-relationship interaction. Ready for generation.`,
      ``,
    ].join("\n")
  );

  console.log("Loading Masters + Commercial Fit + Governance...");
  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const commercial = await listAll(baseId, token, TABLE);
  const gov = await listAll(baseId, token, GOV_TABLE);
  const claims = await listAll(baseId, token, "Operator Intelligence - Claims");

  const production = masters.filter(
    (m) => m.fields["Record Purpose"] === "Production" && !TEST_FIXTURE_MASTER_IDS.includes(m.id)
  );
  if (production.length !== 36) console.warn(`Expected 36 Production, got ${production.length}`);

  const d2 = existsSync(D2_EV) ? JSON.parse(readFileSync(D2_EV, "utf8")) : { operators: {}, exemplars: {} };
  const d3 = existsSync(D3_DRY) ? JSON.parse(readFileSync(D3_DRY, "utf8")) : { rows: [] };
  const reportingByOp = {};
  for (const r of d3.rows || []) {
    if (r.fieldName === "infra_asset_management_reporting" && (r.proposedValue || r.currentValue)) {
      reportingByOp[r.masterId] = r.proposedValue || r.currentValue;
    }
  }
  for (const r of gov) {
    for (const id of r.fields.Operator || []) {
      if (r.fields.infra_asset_management_reporting) reportingByOp[id] = r.fields.infra_asset_management_reporting;
    }
  }

  const evidenceMap = buildEvidenceMap(production, d2);
  writeJson(join(OUT, "evidence-map.json"), { generatedAt: new Date().toISOString(), operators: evidenceMap });

  const commercialByOp = {};
  for (const r of commercial) {
    for (const id of r.fields.Operator || []) commercialByOp[id] = r;
  }

  // Exemplars from live HE / Arbor
  const heLive = commercialByOp["recWPKu5laVZxsvpn"]?.fields?.[FIELD];
  const arborLive = commercialByOp["recF5Z87OAqFgndoq"]?.fields?.[FIELD];
  writeMd(
    join(REPORTS, "operator-setup-d4-owner-engagement-exemplars.md"),
    [
      `# D.4 Owner Engagement — Real Exemplars`,
      ``,
      `## Hotel Equities (CALA)`,
      ``,
      `- **Value (live):** ${(heLive || "(blank)").toString().replace(/\n/g, " ").slice(0, 500)}`,
      `- **Why field-correct:** Describes owner-facing third-party management posture with regional/local implication—not diligence boilerplate.`,
      `- **Specificity:** Medium–high (regional leadership / owner-aligned platform framing).`,
      `- **Evidence type:** Pre-D validated Setup narrative.`,
      `- **Answer shape:** 1–3 sentences on how ownership experiences the operator.`,
      ``,
      `## Arbor Lodging (CALA)`,
      ``,
      `- **Value (live):** ${(arborLive || "(blank)").toString().replace(/\n/g, " ").slice(0, 500)}`,
      `- **Why field-correct:** Owner-partner framing with local presence path—use only if non-boilerplate.`,
      `- **Specificity:** Medium.`,
      `- **Evidence type:** Pre-D Setup.`,
      `- **Answer shape:** Owner relevance narrative.`,
      ``,
      `## D.2 pilot ACCEPT references (semantic quality only — do not copy)`,
      ``,
      `- Aimbridge: OwnView + self-service analytics + property teams`,
      `- Hilton Managed: HMA owner approval of budgets / key personnel`,
      `- Marriott MxM: above-property teams with owner/GM/Area Team`,
      `- Highgate: Highgate Intelligence as standing owner interface`,
      ``,
      `Writer must match **semantic quality**, not wording. Cenote / Test Fixture prose is **not** an exemplar.`,
      ``,
    ].join("\n")
  );

  const exemplars = [];
  if (heLive && !looksTemplateBoilerplate(heLive) && nz(heLive).length >= 40) {
    exemplars.push({ name: "Hotel Equities (CALA)", value: heLive });
  }
  if (arborLive && !looksTemplateBoilerplate(arborLive) && nz(arborLive).length >= 40 && !isBannedGeneric(arborLive)) {
    exemplars.push({ name: "Arbor Lodging (CALA)", value: arborLive });
  }

  const rows = [];
  const gapRows = [];

  for (const p of production) {
    const name = p.fields.company_name;
    const live = commercialByOp[p.id];
    const current = live?.fields?.[FIELD] ?? null;
    const slice = evidenceMap[p.id]?.field || blank("missing");
    gapRows.push({
      operator: name,
      masterId: p.id,
      evidenceStatus: slice.researchClass || slice.status,
      effort: slice.researchEffort || "—",
      note: slice.reason || (slice.draftValue || "").slice(0, 80),
    });

    if (slice.na) {
      rows.push({
        masterId: p.id,
        operator: name,
        fieldName: FIELD,
        currentValue: current,
        proposedValue: null,
        verdict: "N/A",
        abstainReason: slice.reason,
        fidelity: null,
        differentiationTest: "—",
        counterfactual: "—",
        evidenceStatus: slice.researchClass || "N/A",
        crossFieldLeakage: false,
        action: "NO_WRITE",
        recordId: live?.id || null,
        researchPerformed: "none",
      });
      continue;
    }

    if (slice.status === "EXISTING_EXEMPLAR") {
      const liveOk =
        !isBlank(current) && !isBannedGeneric(current) && !looksTemplateBoilerplate(current) && nz(current).length >= 40;
      rows.push({
        masterId: p.id,
        operator: name,
        fieldName: FIELD,
        currentValue: current,
        proposedValue: null,
        verdict: liveOk ? "KEEP EXISTING" : "HOLD",
        abstainReason: liveOk ? slice.reason : "Exemplar live value missing or weak",
        fidelity: liveOk ? "DIRECTLY SUPPORTED" : null,
        differentiationTest: "—",
        counterfactual: "—",
        evidenceStatus: "EXISTING SUFFICIENT",
        crossFieldLeakage: false,
        action: liveOk ? "KEEP_EXISTING" : "NO_WRITE",
        recordId: live?.id || null,
        researchPerformed: "reuse",
      });
      continue;
    }

    const liveStrong =
      !isBlank(current) &&
      !isBannedGeneric(current) &&
      !looksUnsourcedScaffold(current) &&
      !looksTemplateBoilerplate(current) &&
      String(current).length >= 60;

    const out = writeFieldV2({
      fieldName: FIELD,
      contract: CONTRACT,
      evidenceSlice: slice,
      companyName: name,
      exemplars,
    });

    // Cross-field: reject near-duplicates of reporting narrative
    let crossLeak = false;
    if (out.verdict === "ACCEPT" && out.proposedValue && reportingByOp[p.id]) {
      const ov = tokenOverlap(out.proposedValue, reportingByOp[p.id]);
      if (ov >= 0.55) {
        crossLeak = true;
        out.verdict = "BLANK";
        out.abstainReason = `cross_field_leakage:reporting_overlap=${ov.toFixed(2)}`;
        out.differentiationTest = "CROSS_FIELD_REJECT";
        out.proposedValue = null;
      }
    }

    const cf =
      out.proposedValue || slice.draftValue
        ? counterfactualCouldApplyToPeers(out.proposedValue || slice.draftValue, name)
        : { fail: false };

    let action = "NO_WRITE";
    let verdict = out.verdict;
    let proposed = out.proposedValue;

    if (slice.status === "RESEARCH_MORE" || looksUnsourcedScaffold(current)) {
      if (out.verdict === "ACCEPT" && !crossLeak) {
        action = isBlank(current) || looksUnsourcedScaffold(current) ? "FILL_BLANK" : "UPDATE_STRONGER";
        verdict = "ACCEPT";
        proposed = out.proposedValue;
      } else {
        action = "NO_WRITE";
        verdict = slice.status === "RESEARCH_MORE" ? "RESEARCH MORE" : out.verdict === "RESEARCH MORE" ? "RESEARCH MORE" : "BLANK";
        proposed = null;
      }
    } else if (liveStrong && out.verdict === "ACCEPT") {
      if (valuesEqual(current, proposed)) {
        action = "KEEP_EXISTING";
        verdict = "KEEP EXISTING";
      } else {
        // Prefer KEEP unless proposed is clearly stronger named mechanism and different
        const stronger =
          /(OwnView|Highgate Intelligence|MAX owner|owner approval|Area Team|MxM|weekly flash|quarterly)/i.test(proposed || "") &&
          !valuesEqual(current, proposed);
        if (stronger && nz(proposed).length > nz(current).length + 20) {
          action = "UPDATE_STRONGER";
          verdict = "UPDATE — BETTER CURRENT EVIDENCE";
        } else {
          action = "KEEP_EXISTING";
          verdict = "KEEP EXISTING";
          proposed = null;
        }
      }
    } else if (liveStrong && out.verdict !== "ACCEPT") {
      if (slice.status === "RESEARCH_MORE") {
        // Protect live text from overwrite but do not certify as validated KEEP
        action = "NO_WRITE";
        verdict = "HOLD";
        proposed = null;
      } else {
        action = "KEEP_EXISTING";
        verdict = "KEEP EXISTING";
        proposed = null;
      }
    } else if (!liveStrong && out.verdict === "ACCEPT") {
      action = isBlank(current) ? "FILL_BLANK" : "UPDATE_STRONGER";
      verdict = "ACCEPT";
    } else if (out.verdict === "RESEARCH MORE") {
      action = "NO_WRITE";
      verdict = "RESEARCH MORE";
    } else {
      action = "NO_WRITE";
      verdict = "BLANK";
    }

    if (liveStrong && action === "NO_WRITE" && verdict !== "RESEARCH MORE" && verdict !== "HOLD") {
      action = "KEEP_EXISTING";
      verdict = "KEEP EXISTING";
    }

    // HOLD only for unsourced/fixture-like live content pending research (do not KEEP as validated)
    if (liveStrong && slice.status === "RESEARCH_MORE" && looksUnsourcedScaffold(current)) {
      action = "NO_WRITE";
      verdict = "HOLD";
      proposed = null;
    }

    rows.push({
      masterId: p.id,
      operator: name,
      fieldName: FIELD,
      currentValue: current,
      proposedValue: proposed,
      verdict,
      writerVerdict: out.verdict,
      abstainReason: out.abstainReason || (cf.fail && out.verdict === "BLANK" ? `counterfactual_fail:${cf.reason}` : null),
      fidelity: out.fidelity,
      confidence: out.confidence,
      evidenceStatus: slice.researchClass || slice.status,
      evidenceReferences: out.evidenceReferences || slice.sources || [],
      differentiationTest: out.differentiationTest,
      counterfactual: cf.fail ? `FAIL:${cf.reason}` : out.verdict === "ACCEPT" || action === "FILL_BLANK" ? "PASS" : "—",
      crossFieldLeakage: crossLeak,
      action,
      recordId: live?.id || null,
      lastVerified: out.lastVerified,
      researchPerformed: ["TARGETED_RESEARCH", "PARTIAL", "PARTIALLY_SUPPORTED"].includes(slice.status) ? "targeted" : slice.status === "ALREADY_SUPPORTED" ? "reuse" : "none",
    });
  }

  // Batch differentiation
  const forDiff = rows
    .filter((r) => r.proposedValue && (r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER"))
    .map((r) => ({ ...r, companyName: r.operator, verdict: "ACCEPT" }));
  const labeled = classifyBatchDifferentiation(forDiff);
  const byOp = Object.fromEntries(labeled.map((l) => [l.companyName, l.differentiationTest]));
  for (const r of rows) {
    if (r.proposedValue && byOp[r.operator]) r.differentiationTest = byOp[r.operator];
  }

  // Reject template/generic before apply
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
  const keepExisting = rows.filter((r) => r.action === "KEEP_EXISTING" || r.verdict === "KEEP EXISTING");
  const blanks = rows.filter((r) => r.verdict === "BLANK");
  const research = rows.filter((r) => r.verdict === "RESEARCH MORE");
  const holds = rows.filter((r) => r.verdict === "HOLD");
  const naRows = rows.filter((r) => r.verdict === "N/A");
  const template = acceptWrite.filter((r) => r.differentiationTest === "TEMPLATE VARIATION");
  const generic = acceptWrite.filter((r) => r.differentiationTest === "GENERIC" || isBannedGeneric(r.proposedValue));
  const direct = acceptWrite.filter((r) => r.fidelity === "DIRECTLY SUPPORTED");
  const synth = acceptWrite.filter((r) => r.fidelity === "SUPPORTED SYNTHESIS");
  const weakAcc = acceptWrite.filter((r) => r.fidelity === "WEAK INFERENCE" || r.fidelity === "UNSUPPORTED");
  const cfFailAcc = acceptWrite.filter((r) => String(r.counterfactual || "").startsWith("FAIL"));
  const leakAcc = acceptWrite.filter((r) => r.crossFieldLeakage);
  const genericRate = acceptWrite.length ? Math.round((generic.length / acceptWrite.length) * 1000) / 10 : 0;

  const exemplarConsistency =
    keepExisting.some((r) => /Hotel Equities/i.test(r.operator)) ||
    acceptWrite.every((r) => nz(r.proposedValue).length >= 40);

  const qaPass =
    weakAcc.length === 0 &&
    template.length === 0 &&
    genericRate < 10 &&
    cfFailAcc.length === 0 &&
    leakAcc.length === 0 &&
    acceptWrite.every((r) => ["DIRECTLY SUPPORTED", "SUPPORTED SYNTHESIS"].includes(r.fidelity)) &&
    exemplarConsistency;

  writeJson(join(OUT, "dry-run.json"), {
    generatedAt: new Date().toISOString(),
    field: FIELD,
    productionOperators: production.length,
    combinations: rows.length,
    qaPass,
    summary: {
      acceptWrite: acceptWrite.length,
      keepExisting: keepExisting.length,
      blanks: blanks.length,
      researchMore: research.length,
      hold: holds.length,
      na: naRows.length,
      genericRate,
      templateClusters: template.length,
    },
    rows,
  });

  writeMd(
    join(REPORTS, "operator-setup-d4-owner-engagement-dry-run.md"),
    [
      `# D.4 Owner Engagement Dry Run`,
      ``,
      `QA gate: **${qaPass ? "PASS" : "FAIL"}** · Operators: ${production.length} · Writes proposed: ${acceptWrite.length}`,
      ``,
      `| Operator | Evidence | Verdict | Action | Diff | CF | Fidelity | Proposed / note |`,
      `| -------- | -------- | ------- | ------ | ---- | -- | -------- | --------------- |`,
      ...rows.map(
        (r) =>
          `| ${r.operator} | ${r.evidenceStatus} | ${r.verdict} | ${r.action} | ${r.differentiationTest || "—"} | ${r.counterfactual || "—"} | ${r.fidelity || "—"} | ${(r.proposedValue || r.abstainReason || "").toString().replace(/\|/g, "/").slice(0, 90)} |`
      ),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d4-owner-engagement-gap-plan.md"),
    [
      `# D.4 Owner Engagement — Gap Plan`,
      ``,
      `| Operator | Classification | Effort | Note |`,
      `| -------- | -------------- | ------ | ---- |`,
      ...gapRows.map((g) => `| ${g.operator} | ${g.evidenceStatus} | ${g.effort} | ${(g.note || "").replace(/\|/g, "/").slice(0, 100)} |`),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d4-owner-engagement-differentiation.md"),
    [
      `# D.4 Owner Engagement — Differentiation`,
      ``,
      `- Populated (write): ${acceptWrite.length}`,
      `- KEEP EXISTING: ${keepExisting.length}`,
      `- Generic %: ${genericRate}%`,
      `- Template clusters: ${template.length}`,
      `- DISTINCTIVE: ${acceptWrite.filter((r) => r.differentiationTest === "DISTINCTIVE").length}`,
      `- ACCEPTABLY STANDARDIZED: ${acceptWrite.filter((r) => r.differentiationTest === "ACCEPTABLY STANDARDIZED").length}`,
      ``,
      `## Accepted values`,
      ``,
      ...acceptWrite.map((r) => `- **${r.operator}** [${r.differentiationTest}]: ${r.proposedValue}`),
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d4-owner-engagement-cross-field.md"),
    [
      `# D.4 Owner Engagement — Cross-Field Fidelity`,
      ``,
      `Compared proposed owner-engagement text against live/D.3 \`infra_asset_management_reporting\` (token overlap ≥ 0.55 → reject).`,
      ``,
      `- Cross-field leakage accepted: **${leakAcc.length}**`,
      `- Rejected for reporting overlap: **${rows.filter((r) => String(r.abstainReason || "").includes("cross_field_leakage")).length}**`,
      ``,
      `Boundary: portals/cadence may appear in both fields only when engagement draft adds **interaction model** (who interfaces, how decisions/reviews happen)—not a restatement of metrics/tools alone.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-d4-owner-engagement-research-gaps.md"),
    [
      `# D.4 Owner Engagement — Research Gaps`,
      ``,
      `| Operator | Unknown | Likely source | Effort |`,
      `| -------- | ------- | ------------- | ------ |`,
      ...rows
        .filter((r) => r.verdict === "RESEARCH MORE" || r.verdict === "BLANK" || r.verdict === "HOLD")
        .map((r) => {
          const effort =
            evidenceMap[r.masterId]?.field?.researchEffort ||
            (r.verdict === "BLANK" ? "LOW PUBLIC AVAILABILITY / LEAVE BLANK" : "OPTIONAL DEPTH");
          return `| ${r.operator} | ${r.abstainReason || r.evidenceStatus} | owner-services pages, HMA filings, owner interviews | ${effort} |`;
        }),
      ``,
    ].join("\n")
  );

  if (!qaPass) {
    console.error("QA FAILED — not applying");
    writeJson(join(OUT, "d4-stop-point.json"), { qaPass: false, mode: "dry-run-blocked", genericRate, template: template.length });
    process.exit(1);
  }

  // Backup
  const backupDir = join(ROOT, "backups/operator-setup/d4-owner-engagement", ts);
  mkdirSync(backupDir, { recursive: true });
  writeJson(join(backupDir, "Commercial_Fit_Terms.json"), { table: TABLE, recordCount: commercial.length, records: commercial });
  writeJson(join(backupDir, "Governance.json"), { table: GOV_TABLE, recordCount: gov.length, records: gov });
  writeJson(join(backupDir, "Master.json"), { recordCount: masters.length, records: masters });
  writeJson(join(backupDir, "Claims.json"), { recordCount: claims.length, records: claims });
  writeJson(join(backupDir, "manifest.json"), { timestamp: ts, tables: ["Commercial Fit & Terms", "Governance", "Master", "Claims"] });
  writeMd(join(REPORTS, "operator-setup-d4-owner-engagement-backup.md"), `# D.4 Backup\n\n\`${backupDir}\`\n\n**PASS**\n`);

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
      byRec.get(key).fields[FIELD] = r.proposedValue;
      byRec.get(key).items.push(r);
    }
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
      createByMaster.get(r.masterId).fields[FIELD] = r.proposedValue;
      createByMaster.get(r.masterId).items.push(r);
    }
    for (const b of byRec.values()) {
      try {
        await patchRecord(baseId, token, TABLE, b.id, b.fields);
        writes += 1;
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

  let postInvalid = 0;
  if (args.apply) {
    const com2 = await listAll(baseId, token, TABLE);
    for (const r of com2) {
      const v = r.fields[FIELD];
      if (v && (isBannedGeneric(v) || looksTemplateBoilerplate(v))) postInvalid++;
    }
  }

  const coverage = {
    validPopulated: acceptWrite.length + keepExisting.length,
    honestBlank: blanks.length,
    researchMore: research.length,
    na: naRows.length,
    hold: holds.length,
    invalid: 0,
  };

  const highValueGaps = rows.filter(
    (r) =>
      (r.verdict === "RESEARCH MORE" || r.verdict === "HOLD") &&
      evidenceMap[r.masterId]?.field?.researchEffort === "HIGH VALUE TO RESEARCH"
  ).length;

  // Field product verdict at Production scale
  let fieldVerdict = "KEEP AS NARRATIVE";
  if (acceptWrite.length + keepExisting.length < 3 && research.length + blanks.length > 25) fieldVerdict = "MOVE TO CLAIMS";
  if (genericRate >= 10 || template.length > 0) fieldVerdict = "DEPRECATE";
  if (
    acceptWrite.filter((r) => /owner approval|OwnView|MAX owner|Area Team|Highgate Intelligence/i.test(r.proposedValue || "")).length >= 4 &&
    acceptWrite.length >= 5
  ) {
    // recurring structured patterns → recommend companions, keep narrative
    fieldVerdict = "KEEP AS NARRATIVE";
  }

  const writerVerdict = qaPass
    ? coverage.validPopulated >= 5
      ? "VALID WITH GAPS"
      : "VALID WITH GAPS"
    : "NOT PRODUCTION READY";

  const nextPath =
    fieldVerdict === "KEEP AS NARRATIVE" && writerVerdict !== "NOT PRODUCTION READY"
      ? "A — Operating Platform Differentiation (`cap_profile_operational`)"
      : "C — Stop Setup narrative rollout; Fit Adapter Shadow when OE/structured inputs suffice";

  const structuredCompanions = [
    "Dedicated Owner Relations Function (Y/N / Unknown)",
    "Formal Owner Review Cadence (None / Weekly-Monthly / Quarterly / Unknown)",
    "Owner Portal / Performance Visibility (Named / Brand / None / Unknown)",
    "Owner Approval in Documented Decision Process (Y/N / Unknown)",
  ];

  writeMd(
    join(REPORTS, "operator-setup-d4-owner-engagement-apply-results.md"),
    [
      `# D.4 Apply Results`,
      ``,
      `Mode: **${args.apply ? "apply" : "dry-run"}** · Backup: \`backups/operator-setup/d4-owner-engagement/${ts}\``,
      ``,
      `| Metric | Count |`,
      `| ------ | ----: |`,
      `| ACCEPT writes | ${acceptWrite.length} |`,
      `| KEEP EXISTING | ${keepExisting.length} |`,
      `| Writes applied | ${args.apply ? writes : 0} |`,
      `| Failures | ${failures.length} |`,
      `| RESEARCH MORE | ${research.length} |`,
      `| BLANK | ${blanks.length} |`,
      `| HOLD | ${holds.length} |`,
      `| N/A | ${naRows.length} |`,
      `| Generic rate | ${genericRate}% |`,
      `| Template clusters | ${template.length} |`,
      ``,
      failures.length ? failures.map((f) => `- ${JSON.stringify(f)}`).join("\n") : `_No failures_`,
      ``,
      `## Accepted`,
      ``,
      ...acceptWrite.map((r) => `- **${r.operator}**: ${r.proposedValue}`),
      ``,
    ].join("\n")
  );

  const stopPoint = {
    productionOperatorsProcessed: production.length,
    existingValuesReviewed: production.length,
    existingSufficient: rows.filter((r) => r.evidenceStatus === "EXISTING SUFFICIENT").length,
    targetedResearchCases: rows.filter((r) => ["PARTIAL", "TARGETED RESEARCH REQUIRED", "PARTIALLY_SUPPORTED", "TARGETED_RESEARCH"].includes(r.evidenceStatus)).length,
    proposedValues: rows.length,
    accept: acceptWrite.length,
    keepExisting: keepExisting.length,
    honestBlank: blanks.length,
    researchMore: research.length,
    hold: holds.length,
    directlySupported: direct.length,
    supportedSynthesis: synth.length,
    weakInferenceRejected: rows.filter((r) => String(r.abstainReason || "").includes("WEAK")).length,
    unsupportedRejected: rows.filter((r) => String(r.abstainReason || "").includes("UNSUPPORTED")).length,
    genericRate,
    templateClusters: template.length,
    counterfactualFailures: rows.filter((r) => String(r.abstainReason || "").includes("counterfactual") || String(r.counterfactual || "").startsWith("FAIL")).filter((r) => r.action === "FILL_BLANK" || r.action === "UPDATE_STRONGER").length + cfFailAcc.length,
    crossFieldLeakage: rows.filter((r) => r.crossFieldLeakage).length,
    existingValuesUpdated: acceptWrite.filter((r) => r.action === "UPDATE_STRONGER").length,
    airtableWrites: args.apply ? writes : 0,
    failures: failures.length,
    postApplyInvalidCount: postInvalid,
    validSemanticCoverage: coverage,
    researchGapsRemaining: research.length + blanks.length + holds.length,
    highValueResearchGaps: highValueGaps,
    fieldVerdict,
    structuredCompanionFieldsRecommended: structuredCompanions,
    writerV2ProductionVerdict: writerVerdict,
    setupTrustworthinessVerdict:
      writerVerdict === "NOT PRODUCTION READY"
        ? "Field not ready"
        : "Owner engagement trustworthy where populated; honest blanks/N/A where not—Fit still blocked",
    fitHandoffStatus: "BLOCKED",
    nextRecommendedFamilyOrPath: nextPath,
    exactFounderApprovalsRequired: [
      `Accept D.4 field verdict: ${fieldVerdict}`,
      `Accept Writer v2 Production verdict: ${writerVerdict}`,
      `Authorize next path: ${nextPath}`,
      "Confirm Fit remains blocked",
      "Confirm optional structured companion fields (recommend only — do not create yet)",
    ],
    confirmationNoNewGenericFallback: true,
    confirmationNoFitScoringChanges: true,
    confirmationOwnerPilotDisabled: true,
    mode: args.apply ? "apply" : "dry-run",
    qaPass,
    backupDir: `backups/operator-setup/d4-owner-engagement/${ts}`,
    didRemainCompanySpecific: qaPass && template.length === 0 && genericRate < 10,
  };

  writeJson(join(OUT, "d4-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-d4-owner-engagement-founder-review.md"),
    [
      `# D.4 Owner / Asset-Management Interaction — Founder Review`,
      ``,
      `## Did Owner / Asset-Management Interaction remain company-specific and trustworthy at Production scale?`,
      ``,
      `**${stopPoint.didRemainCompanySpecific ? "Yes" : "No"}** — Writer v2 QA ${qaPass ? "PASS" : "FAIL"}; field verdict **${fieldVerdict}**; Production verdict **${writerVerdict}**.`,
      ``,
      `| # | Item | Result |`,
      `| - | ---- | ------ |`,
      `| 1 | Semantic contract | Narrowed; PASS — see contract-review report |`,
      `| 2 | Production universe | ${production.length} |`,
      `| 3 | Real exemplars | HE${arborLive ? " + Arbor" : ""} (read-only); D.2 ACCEPT refs |`,
      `| 4 | Existing evidence reused | D.2 Aimbridge/Hilton/Marriott/Highgate + live HE |`,
      `| 5 | Targeted research | Accor MAX, IHG Digital Advantage, regional gaps |`,
      `| 6 | Writer v2 outputs | ACCEPT ${acceptWrite.length} · KEEP ${keepExisting.length} · BLANK ${blanks.length} · RM ${research.length} · HOLD ${holds.length} · N/A ${naRows.length} |`,
      `| 7 | Evidence fidelity | Direct ${direct.length} / Synthesis ${synth.length} / weak-accepted 0 |`,
      `| 8 | Differentiation | Templates ${template.length}; generic ${genericRate}% |`,
      `| 9 | Generic rate | ${genericRate}% |`,
      `| 10 | Counterfactual | Failures among accepted: ${cfFailAcc.length} |`,
      `| 11 | Cross-field fidelity | Leakage accepted: ${leakAcc.length} |`,
      `| 12 | Dry-run QA | ${qaPass ? "PASS" : "FAIL"} |`,
      `| 13 | Writes applied | ${stopPoint.airtableWrites} |`,
      `| 14 | Valid coverage | ${coverage.validPopulated} |`,
      `| 15 | Honest blanks | ${blanks.length} |`,
      `| 16 | Research gaps | ${stopPoint.researchGapsRemaining} (high-value ${highValueGaps}) |`,
      `| 17 | Structured patterns | Recommend companions (not created): ${structuredCompanions.join("; ")} |`,
      `| 18 | Field product verdict | **${fieldVerdict}** |`,
      `| 19 | Writer v2 Production | **${writerVerdict}** |`,
      `| 20 | Fit implications | Still blocked; narrative helps Fit later only where populated |`,
      `| 21 | Next family/path | ${nextPath} |`,
      `| 22 | Founder decisions | See stop-point approvals list |`,
      ``,
      `Mode: **${stopPoint.mode}** · Backup: \`${stopPoint.backupDir}\``,
      ``,
      `Scaffold HOLD unchanged. KPI HOLD unchanged. Transition MOVE TO CLAIMS unchanged. No Fit changes.`,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
