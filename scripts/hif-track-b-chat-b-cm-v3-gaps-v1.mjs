#!/usr/bin/env node
/**
 * Track B Chat B — Critical-Mass V3 evidence-gap investigation runner.
 * Does NOT modify Track A PR #568. CUSTOMER_BLOCKED.
 * Builds dossiers + founder report; optional HPC match. Persist via global-waves-persist.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";
import {
  ASSESSMENT,
  GAP_FINDINGS,
  WAVE,
  buildChatBGapDossiers,
  summarizeGapAssessments,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-chat-b-cm-v3-gaps-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-chat-b-cm-v3-gaps-v1");
const AS_OF = "2026-11-23";
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

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
  const map = {
    ES: ["spain", "españa", "es"],
    PT: ["portugal", "pt"],
    DO: ["dominican", "república dominicana", "do"],
    MX: ["mexico", "méxico", "mx"],
  };
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

function rowMd(g) {
  const trigger = g.verifiedContractTrigger ? "YES" : "NO";
  const unresolved = g.decisionUnresolved ? "YES" : "NO";
  const pathway = g.competingPathway ? "YES" : "NO";
  const deadline = g.bidDeadline || g.validityEnd || "n/a";
  return `| ${g.hotelName.slice(0, 42)} | ${g.priorTrackAClass} → **${g.chatBAssessment}** | ${trigger} | ${unresolved} | ${pathway} | ${deadline} | ${g.decisionEntity.slice(0, 48)} |`;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const dossiers = buildChatBGapDossiers();
  const fields = await hpcMeta();
  const cache = new Map();
  for (const d of dossiers) {
    if (!d.persistEligible || !d.hpcQuery || !fields) {
      if (!d.hpcMatchStatus || d.hpcMatchStatus === "PENDING_HPC_SEARCH") {
        d.hpcMatchStatus = fields ? "NOT_SEARCHED" : "NOT_SEARCHED";
      }
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

  const byAssessment = summarizeGapAssessments(GAP_FINDINGS);
  const persistable = dossiers.filter((d) => d.persistEligible);
  const contractable = dossiers.filter((d) => contractPersistAllowed(d) && d.contractId);
  const upgraded = []; // none — retain only
  const retainedQualified = GAP_FINDINGS.filter((g) => g.chatBAssessment === ASSESSMENT.QUALIFIED_RETAINED_STRENGTHENED);
  const emerging = GAP_FINDINGS.filter((g) => g.chatBAssessment === ASSESSMENT.EMERGING_RETAINED);
  const ambiguous = GAP_FINDINGS.filter((g) => g.chatBAssessment === ASSESSMENT.AMBIGUOUS_REVIEW);
  const underResearch = GAP_FINDINGS.filter((g) => g.chatBAssessment === ASSESSMENT.UNDER_RESEARCH);
  const committed = GAP_FINDINGS.filter(
    (g) => g.chatBAssessment === ASSESSMENT.COMMITTED_CLOSED || g.chatBAssessment === ASSESSMENT.HISTORICAL_EXECUTED,
  );

  const permanentRuleQualifiedAfter = {
    baselinePermanentRule: 5,
    note: "Founder baseline ≈5 permanent-rule Qualified (Voir + Baha Mar + Oromana + Águeda + Areia when #568 lands). Chat B adds 0 net Qualified promotions.",
    chatBNetNewQualified: 0,
    retainedStrengthened: retainedQualified.map((g) => g.trackAId),
    stillTowardTarget20: "Need ~15 more Europe/CALA Qualified with BCT/MCT + unresolved + pathway — Cap Cana / Bergantín / Uriz / FINN do not clear the bar yet.",
  };

  const summary = {
    asOf: AS_OF,
    wave: WAVE,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    paidToolsUsd: 0,
    pr568: {
      state: "OPEN",
      url: "https://github.com/joandejarden-create/my-operators-backend/pull/568",
      chatBAction: "DO_NOT_MODIFY — parallel Track B evidence pack only",
    },
    assessmentCounts: byAssessment,
    permanentRuleQualifiedAfter,
    upgradesToQualified: upgraded,
    retainedQualifiedStrengthened: retainedQualified.map((g) => ({
      id: g.trackAId,
      hotel: g.hotelName,
      deadline: g.bidDeadline || g.validityEnd,
      pathway: g.competingPathway,
    })),
    emergingRetained: emerging.map((g) => ({ id: g.trackAId, hotel: g.hotelName, why: g.additionalEvidenceStillNeeded[0] })),
    ambiguousReview: ambiguous.map((g) => ({ id: g.trackAId, hotel: g.hotelName, why: g.additionalEvidenceStillNeeded[0] })),
    underResearch: underResearch.map((g) => ({ id: g.trackAId, hotel: g.hotelName, why: g.additionalEvidenceStillNeeded[0] })),
    committedOrHistorical: committed.map((g) => ({ id: g.trackAId, hotel: g.hotelName })),
    dossierCount: dossiers.length,
    persistEligible: persistable.length,
    contractPersistAllowed: contractable.length,
    finnNoDuplicateWrite: true,
  };

  const trackA = {
    note: "Track A handoff — Chat B does not rewrite #568 classifications. Retain / Ambiguous / Emerging guidance only.",
    doNotDowngrade: retainedQualified.map((g) => g.trackAId),
    doNotPromote: [...emerging, ...ambiguous, ...underResearch].map((g) => g.trackAId),
    researchCandidates: dossiers.map((d) => d.trackAPayload),
  };

  const trackC = dossiers
    .filter((d) => d.chatBAssessment === ASSESSMENT.QUALIFIED_RETAINED_STRENGTHENED || d.physicalCountry === "MX")
    .map((d) => ({
      discoveryPropertyId: d.discoveryPropertyId,
      hotelName: d.hotelName,
      physicalCountry: d.physicalCountry,
      hpcMatchStatus: d.hpcMatchStatus,
      issue:
        d.trackAId === "cm3-agueda-passivhaus"
          ? "Legal entity name / NIF unpublished on EEN — Track C identity pending partner intro"
          : d.trackAId === "finn-residual-aimbridge"
            ? "JW Marriott Monterrey Arboleda vs Valle alias unresolved"
            : "HPC match optional; no unauthorized HPC create",
    }));

  fs.writeFileSync(
    path.join(OUT, "WAVE_DOSSIERS.json"),
    JSON.stringify({ dossiers: persistable, allDossiers: dossiers, summary }, null, 2),
  );
  fs.writeFileSync(path.join(OUT, "GAP_FINDINGS.json"), JSON.stringify({ findings: GAP_FINDINGS, byAssessment }, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_A_HANDOFF.json"), JSON.stringify(trackA, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_C_HANDOFF.json"), JSON.stringify({ handoffs: trackC }, null, 2));
  fs.writeFileSync(path.join(OUT, "RESEARCH_SUMMARY.json"), JSON.stringify(summary, null, 2));

  const md = `# Track B Chat B — Critical-Mass V3 Evidence Gaps

**As of:** ${AS_OF}  
**Publication:** CUSTOMER_BLOCKED · **Research cost:** $0  
**PR #568:** OPEN — **not modified** (parallel Track B pack)  
**Permanent-rule Qualified:** baseline ~5 retained; **Chat B net new Qualified = 0**

## Verdict matrix

| Candidate | Class | Contract trigger | Unresolved | Competing pathway | Deadline | Decision entity |
|---|---|---|---|---|---|---|
${GAP_FINDINGS.map(rowMd).join("\n")}

## Upgraded to Qualified
**None.** Chat B did not invent new Qualified. The three Track A V3 Qualified (Oromana / Águeda / Areia Branca) are **retained and strengthened** with primary-source corroboration.

## Retained Qualified (strengthened)
1. **Hotel Oromana** — Restricted concession C-2026/032 open to **27 Nov 2026 23:59**; PCAP/PPT + Anexo III staff subrogation; TARCJA 610/2026 re-tender after partial annulment; 40-year MCT; no mandated franchise brand (4★ category target).
2. **Águeda Passivhaus** — EEN BOPT20261105031 hybrid lease/management open to **6 Nov 2027**; sole proprietorship Jul 2026 Águeda; **legal name/NIF still unpublished**; partner not selected.
3. **Areia Branca REVIVE** — DR **27619/2026** + VORTAL; proposals to **17 Mar 2027**; 50-year hotelaria concession; CIM Beira Baixa owner / Turismo de Portugal tramitación; brand not locked in notice.

## Remain Emerging
4. **Cap Cana (Spain Inmobiliaria)** — gated email teaser only; no public RFP / parcels. Do **not** conflate with committed AC Residences by Marriott (Gesproin) or Lamborghini/Huxley Cap Cana.
5. **Punta Bergantín residual / 4th hotel** — Meliá/Westin/Dreams named; fourth still undefined; Hilton remains conversations; no open competitive pathway.

## Ambiguous / Under research (not Qualified)
6. **Torre de Uriz** — bid window **closed 19 Nov 2026**; award pending as of 20–23 Nov. New competitors cannot enter → **AMBIGUOUS_REVIEW** (monitor award/desertion). Not Historical until award.
7. **FINN residual Aimbridge HMAs** — still active Westin Valle + JW Arboleda/Valle; **no hotel-level HMA expiry dates**; internalization-at-expiry constrains competing MCT → **UNDER_RESEARCH** (reuse prior CALA rows; no duplicate write).

## Committed / Historical from this gap set
**None newly classified as committed/historical.** (Separate Cap Cana brand-committed projects noted only as disambiguation.)

## Evidence still required
| Gap | Next evidence |
|---|---|
| Oromana | Full PPT brand-affiliation clause text; post-deadline award |
| Águeda | Legal entity / NIF; draft hybrid HMA term sheet |
| Areia Branca | Full VORTAL peças (CapEx floor / brand constraints) |
| Cap Cana | Named parcel teaser + selection rules |
| FINN | HMA exhibits with expiry / notice |
| Bergantín | Named 4th-hotel SPE + open RFP |
| Torre de Uriz | Award or desertion notice |

## Persistence
- Persistable dossiers: **${persistable.length}** (FINN excluded — reuse prior)
- Contract-eligible: **${contractable.length}**
- Dry-run then apply:
  \`\`\`bash
  npm run hif-track-b-chat-b-cm-v3-gaps-v1
  npm run hif-track-b-global-waves-persist -- --batch-dir=reports/sec-edgar-hif-track-b-chat-b-cm-v3-gaps-v1
  npm run hif-track-b-global-waves-persist -- --batch-dir=reports/sec-edgar-hif-track-b-chat-b-cm-v3-gaps-v1 --apply
  \`\`\`
- All writes **CUSTOMER_BLOCKED**. Dedup by discoveryPropertyId / findingId / contractId / milestoneId.
- Do **not** re-apply CALA / coverage-audit 54 / V4 DRH-SVC ledgers.

## IC coordination
- Open a **new Track B PR** on \`feat/hif-v6-track-b-chat-b-cm-v3-gaps\` — do not push commits onto #568.
- Track A: retain three V3 Qualified; keep Cap Cana/Bergantín Emerging; Uriz Ambiguous; FINN under research.
- Toward ~20 Qualified: still need additional Europe/CALA BCT/MCT with unresolved + pathway — this pack protects quality, does not inflate count.
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
