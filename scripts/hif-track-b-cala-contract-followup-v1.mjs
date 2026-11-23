#!/usr/bin/env node
/**
 * Track B CALA Contract Intelligence Follow-up V1.
 * Consumes Track A dealsignal-global-source-cala-discovery-v1 handoff.
 * CUSTOMER_BLOCKED. $0.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  buildCalaContractFollowupDossiers,
  CALA_FOLLOWUP_STAGED,
  FINN_NOTAS_4T2025,
  FINN_GOV_2025,
  FINN_3T2026,
  FINN_ASG_2025,
  FIHO_Q2_BMV,
  FIHO_HR_2026,
  SANTANDER_SECOP,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-cala-contract-followup-dossiers-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-cala-contract-followup-v1");
const AS_OF = "2026-11-23";
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const TRACK_A_HANDOFF = "C:/Dev/dealality/ws-hif-v6-track-a/reports/dealsignal-global-source-cala-discovery-v1/TRACK_B_HANDOFF.md";

async function fetchText(url) {
  let lastErr = null;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const res = await fetch(url, { headers: { "User-Agent": UA, Accept: "text/html,application/pdf,*/*" } });
      const buf = Buffer.from(await res.arrayBuffer());
      return { ok: res.ok, status: res.status, text: buf.toString("utf8"), bytes: buf.length };
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 600 * attempt));
    }
  }
  throw lastErr;
}

function localTxt(pdfPath) {
  const t = pdfPath.replace(/\.pdf$/i, ".txt");
  return fs.existsSync(t) ? fs.readFileSync(t, "utf8") : "";
}

async function hpcMeta() {
  if (!token) return null;
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${GRAPH_BASE_ID_CANONICAL}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) return null;
  const table = (json.tables || []).find((t) => t.id === HPC_TABLE_ID);
  const fields = table?.fields || [];
  const pick = (preds) => fields.find((f) => preds.some((p) => p.test(f.name)))?.name || null;
  return {
    nameField: pick([/^Hotel Name$/i, /^Name$/i, /Property Name/i]),
    cityField: pick([/^City$/i]),
    countryField: pick([/^Country$/i, /Physical Country/i]),
  };
}

function countryMatches(value, expected) {
  const c = String(value || "").toLowerCase();
  const map = { MX: ["mexico", "méxico", "mx"], CO: ["colombia", "co"], DO: ["dominican", "dominicana", "do"] };
  return (map[expected] || [String(expected || "").toLowerCase()]).some((x) => c.includes(x));
}

async function searchHpc(fields, query, country) {
  const q = String(query || "").replace(/"/g, "").slice(0, 40);
  if (!q || !fields?.nameField) return { hpcMatchStatus: "NOT_SEARCHED", hpcRecordId: null };
  const formula = `FIND("${q.toLowerCase()}", LOWER({${fields.nameField}}&""))`;
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "8", pageSize: "8" });
  for (const f of [fields.countryField, fields.cityField, fields.nameField]) if (f) qs.append("fields[]", f);
  try {
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) return { hpcMatchStatus: "SEARCH_ERROR", hpcRecordId: null };
    const hits = (json.records || []).filter((r) =>
      fields.countryField ? countryMatches(r.fields?.[fields.countryField], country) : true,
    );
    if (hits.length === 1) {
      return {
        hpcMatchStatus: "MATCHED_UNIQUE_NAME_AND_COUNTRY",
        hpcRecordId: hits[0].id,
        hpcName: hits[0].fields?.[fields.nameField] || null,
      };
    }
    if (hits.length > 1) return { hpcMatchStatus: "AMBIGUOUS_MULTIPLE", hpcRecordId: null, hpcHitCount: hits.length };
    return { hpcMatchStatus: "DISCOVERED_NOT_IN_HPC", hpcRecordId: null };
  } catch {
    return { hpcMatchStatus: "SEARCH_ERROR", hpcRecordId: null };
  }
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!fs.existsSync(TRACK_A_HANDOFF)) throw new Error("TRACK_A_HANDOFF_MISSING");

  const checks = {};
  const notas = await fetchText(FINN_NOTAS_4T2025);
  const notasText = localTxt(path.join(ROOT, "tmp/finn-notas-4t2025-en.pdf")) || notas.text;
  checks.finnNotas = {
    ok: /10 and 20 years/i.test(notasText) && /Puebla/i.test(notasText) && /July 24,\s*2025/i.test(notasText),
    status: notas.status,
  };
  if (!checks.finnNotas.ok) throw new Error("FINN_NOTAS_VERIFY_FAIL");

  const gov = await fetchText(FINN_GOV_2025);
  const govText = localTxt(path.join(ROOT, "tmp/fibra-gov.pdf")) || gov.text;
  checks.finnGov = {
    ok:
      (/Holiday Inn[\s\S]{0,40}Monterrey Valle/i.test(govText) || /Monterrey Valle hotel \(MTYZV\)/i.test(govText)) &&
      /Courtyard by Marriott/i.test(govText),
    status: gov.status,
  };
  if (!checks.finnGov.ok) throw new Error("FINN_GOV_VERIFY_FAIL");

  const q3 = await fetchText(FINN_3T2026);
  const q3Text = localTxt(path.join(ROOT, "tmp/finn-3t2026.pdf")) || q3.text;
  checks.finn3t = {
    ok: /Aimbridge Latam/i.test(q3Text) && /424/i.test(q3Text),
    status: q3.status,
  };
  if (!checks.finn3t.ok) throw new Error("FINN_3T_VERIFY_FAIL");

  const asgLocal = path.join(ROOT, "tmp/finn-asg-2025.txt");
  let asgBody = fs.existsSync(asgLocal) ? fs.readFileSync(asgLocal, "utf8") : "";
  if (!asgBody) {
    const { PDFParse } = await import("pdf-parse");
    const buf = Buffer.from(await (await fetch(FINN_ASG_2025, { headers: { "User-Agent": UA } })).arrayBuffer());
    const p2 = await new PDFParse({ data: buf }).getText();
    asgBody = p2.text;
    fs.writeFileSync(asgLocal, asgBody);
  }
  checks.finnAsg = {
    ok: /Bajo acuerdo de gestión/i.test(asgBody) && /Westin Monterrey Valle/i.test(asgBody) && /JW Marriott Monterrey/i.test(asgBody),
  };
  if (!checks.finnAsg.ok) throw new Error("FINN_ASG_VERIFY_FAIL");

  const fihoQ2 = await fetchText(FIHO_Q2_BMV);
  const fihoQ2Text = localTxt(path.join(ROOT, "tmp/fiho-eventfid-1574632.pdf")) || fihoQ2.text;
  checks.fihoQ2 = {
    ok: /Villahermosa/i.test(fihoQ2Text) && /Yucat/i.test(fihoQ2Text) && /arrendamiento/i.test(fihoQ2Text),
    status: fihoQ2.status,
  };
  if (!checks.fihoQ2.ok) throw new Error("FIHO_Q2_VERIFY_FAIL");

  const hrText = localTxt(path.join(ROOT, "tmp/fiho-hr-2026.pdf"));
  checks.fihoHr = { ok: /Ciudad del Carmen/i.test(hrText) && /InHouse/i.test(hrText) };
  if (!checks.fihoHr.ok) throw new Error("FIHO_HR_VERIFY_FAIL");

  const sant = await fetchText(SANTANDER_SECOP);
  checks.santander = {
    ok: sant.ok && /bien inmueble/i.test(sant.text) && !/hotel hotelero/i.test(sant.text.slice(0, 2000)),
    hotelAffirmed: /hotel/i.test(sant.text) && /hospedaje|hotelero|alojamiento/i.test(sant.text),
    status: sant.status,
  };

  const dossiers = buildCalaContractFollowupDossiers();
  const fields = await hpcMeta();
  const cache = new Map();
  for (const d of dossiers) {
    if (!d.persistEligible || !d.hpcQuery) {
      d.hpcMatchStatus = d.hpcQuery ? "PENDING_HPC_SEARCH" : "NOT_SEARCHED";
      continue;
    }
    if (!fields) {
      d.hpcMatchStatus = "NOT_SEARCHED";
      continue;
    }
    const key = `${d.hpcQuery}|${d.physicalCountry}`;
    if (cache.has(key)) Object.assign(d, cache.get(key));
    else {
      const hit = await searchHpc(fields, d.hpcQuery, d.physicalCountry);
      cache.set(key, hit);
      Object.assign(d, hit);
      await new Promise((r) => setTimeout(r, 110));
    }
  }

  const persistable = dossiers.filter((d) => d.persistEligible);
  const staged = [
    ...CALA_FOLLOWUP_STAGED,
    ...dossiers
      .filter((d) => !d.persistEligible)
      .map((d) => ({
        id: d.findingId,
        hotelName: d.hotelName,
        reason: "STAGED_NON_HOTEL_OR_RESEARCH_GAP",
        trackA: d.trackA,
      })),
  ];

  const trackA = dossiers.filter((d) => d.trackAPayload).map((d) => d.trackAPayload);
  const trackC = dossiers
    .filter((d) => d.trackC)
    .map((d) => ({
      discoveryPropertyId: d.discoveryPropertyId,
      hotelName: d.hotelName,
      physicalCountry: d.physicalCountry,
      issue: d.trackC,
      hpcMatchStatus: d.hpcMatchStatus,
    }));

  const summary = {
    asOf: AS_OF,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    trackAHandoffConsumed: TRACK_A_HANDOFF,
    sourceVerification: checks,
    hotelSpecificFranchiseDatesFound: [
      {
        hotel: "Holiday Inn Monterrey Valle",
        fact: "Prior franchise expired; new Holiday Inn franchise executed (FY2025 approval)",
        newExpiration: "UNKNOWN",
        verified: true,
        inferred: false,
        source: FINN_GOV_2025,
      },
      {
        hotel: "Wyndham Garden Guadalajara Acueducto",
        fact: "Conversion to Courtyard by Marriott approved",
        newExpiration: "UNKNOWN",
        verified: true,
        inferred: false,
        source: FINN_GOV_2025,
      },
      {
        hotel: "Portfolio IHG/Hilton/Wyndham/Marriott",
        fact: "Only 10–20 year term range disclosed",
        newExpiration: "NOT_HOTEL_SPECIFIC",
        verified: true,
        inferred: false,
        source: FINN_NOTAS_4T2025,
        limitation: "Do not assign range to individual hotels",
      },
    ],
    managementAgreementsStillPotentiallyOpen: [
      {
        hotel: "The Westin Monterrey Valle",
        operator: "Aimbridge Latam",
        rooms: 174,
        asOf: "2026-09-30",
        expiration: "UNKNOWN",
        note: "Co-owned; Bajo acuerdo de gestión; room-count match to Aimbridge residual 424",
      },
      {
        hotel: "JW Marriott Monterrey Valle",
        operator: "Aimbridge Latam",
        rooms: 250,
        asOf: "2026-09-30",
        expiration: "UNKNOWN",
        note: "Co-owned; internalization-at-expiry policy constrains competing MCT",
      },
    ],
    managementClosedNotOpenMct: [
      "Holiday Inn Puebla La Noria HMA non-renewal → terminated 2025-07-24 → internalized 2025-08-01",
      "Holiday Inn Monterrey Valle ops internalized 2025-09-01",
      "28-hotel Aimbridge buyout completed 2026-01-15 (prior ISE wave)",
    ],
    leaseVsHma: [
      {
        hotel: "Villahermosa",
        prior: "MANAGEMENT + FRANCHISE terminated",
        successor: "LEASE (arrendamiento) InHouse from 2026-07-01",
      },
      {
        hotel: "Ciudad del Carmen",
        prior: "Fiesta Inn affiliation",
        successor: "LEASE fixed-rent InHouse from 2026-04-01",
      },
      {
        hotel: "The Yucatán Resort Playa del Carmen",
        prior: "Ops suspended 2026-04-30 for lease portfolio",
        successor: "LEASE target; counterparty/brand unnamed",
      },
    ],
    ownersAndParties: {
      fibraInn: "Trust DB/1616 / FINN13; Servicios Hoteleros FINN; Aimbridge Latam (residual)",
      fibraHotel: "FIHO12; InHouse Hotels (lessee/brand)",
      caboRojo: "Pro-Pedernales; Consorcio Cabo Rojo / Grupo Puntacana",
      santander: "Departamento de Santander (asset type unconfirmed)",
    },
    dossierCount: dossiers.length,
    persistEligible: persistable.length,
    stagedCount: staged.length,
    trackAResearchCandidates: trackA.length,
    trackCIssues: trackC.length,
  };

  fs.writeFileSync(path.join(OUT, "WAVE_DOSSIERS.json"), JSON.stringify({ dossiers: persistable, allDossiers: dossiers, staged, summary }, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_A_HANDOFF.json"), JSON.stringify({ researchCandidates: trackA, closedOrGaps: dossiers.filter((d) => !d.trackAPayload).map((d) => ({ id: d.findingId, classification: d.trackA })) }, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_C_HANDOFF.json"), JSON.stringify({ handoffs: trackC }, null, 2));
  fs.writeFileSync(path.join(OUT, "SOURCE_VERIFICATION.json"), JSON.stringify(checks, null, 2));
  fs.writeFileSync(path.join(OUT, "RESEARCH_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# Track B — CALA Contract Intelligence Follow-up V1

**As of:** ${AS_OF}  
**Publication:** CUSTOMER_BLOCKED · **Cost:** $0  
**Starts from:** Track A \`dealsignal-global-source-cala-discovery-v1\`

## 1. Hotel-specific franchise dates found
${summary.hotelSpecificFranchiseDatesFound.map((x) => `- **${x.hotel}:** ${x.fact}. New expiration: ${x.newExpiration}. Source: ${x.source}${x.limitation ? ` — ${x.limitation}` : ""}`).join("\n")}

## 2. Management agreements still potentially open
${summary.managementAgreementsStillPotentiallyOpen.map((x) => `- **${x.hotel}** — ${x.operator}, ${x.rooms} rooms, as of ${x.asOf}; expiration ${x.expiration}. ${x.note}`).join("\n")}

Closed (not open MCT): ${summary.managementClosedNotOpenMct.join("; ")}

## 3. Lease versus HMA reconciliation
${summary.leaseVsHma.map((x) => `- **${x.hotel}:** prior ${x.prior} → successor ${x.successor}`).join("\n")}

## 4. Owners / parties
- Fibra Inn: ${summary.ownersAndParties.fibraInn}
- FibraHotel: ${summary.ownersAndParties.fibraHotel}
- Cabo Rojo: ${summary.ownersAndParties.caboRojo}
- Santander: ${summary.ownersAndParties.santander}

## 5–6. Sources, dates, limitations
See \`RESEARCH_SUMMARY.json\` and dossier \`evidenceSnippet\` fields. Spanish/English titles preserved. No generic 10–20 year franchise terms assigned to hotels.

## Persistence
Use \`hif-track-b-global-waves-persist.mjs --batch-dir=reports/sec-edgar-hif-track-b-cala-contract-followup-v1\`

## Track A / C / IC
- Track A: remaining Aimbridge HMAs = Emerging date-unknown; franchise schedule = research gap; lease transitions committed/emerging — **no auto-Verified conversions**
- Track C: Courtyard GDL alias; JW Marriott Arboleda vs Valle; no HPC for Santander/Cabo Rojo parent
- IC: update open PR #556 on same branch after local prechecks
`,
  );

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
