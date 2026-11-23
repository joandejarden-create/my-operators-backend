#!/usr/bin/env node
/**
 * Track B V4 forward-looking qualification runner — research artifacts only.
 * NO Airtable apply. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildFwdQualifyFindings,
  buildTrackCIdentityHandoff,
  summarizeQualClasses,
  QUAL_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-fwd-qualify-v4.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-fwd-qualify-v4");
const AS_OF = new Date().toISOString();

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const findings = buildFwdQualifyFindings();
  const byClass = summarizeQualClasses(findings);
  const trackC = buildTrackCIdentityHandoff();
  const verifiedOpen = findings.filter((f) => f.qualClass === QUAL_CLASS.VERIFIED_OPEN_OPPORTUNITY);

  const calaLedger = {
    note: "CALA follow-up batch from merged #556 — do not re-apply",
    dryRunPlanned: { findings: 9, contracts: 8, inventory: 3, milestones: 4, organizations: 1 },
    applyLedgerOpsObserved: { findings: 9, contracts: 8, inventoryOps: 12, milestones: 4, organizations: 2 },
    reApply: { creates: 0, noops: 34, failures: 0 },
    founderBaselineClaim: { findings: 9, contracts: 8, inventory: 8, milestones: 4, organizations: 2 },
    reconciliation:
      "Founder baseline (9/8/8/4/2) matches findings/contracts/milestones/orgs from CALA apply ops; inventory '8' vs dry-run inventory creates '3' reflects shared discoveryPropertyIds / prior inventory noops — do not re-write. Coverage-audit 54 and V4 DRH/SVC batches are separate and also must not be re-applied.",
  };

  const summary = {
    asOf: AS_OF,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    pr556: { state: "MERGED", mergeCommit: "05b9d23162e3f5e9e94547494010db9e6236ae78" },
    pr560: { state: "OPEN", note: "DRH/SVC decision-window extraction; this qualify pack appends research-only" },
    calaLedger,
    qualClassCounts: byClass,
    verifiedOpenCount: verifiedOpen.length,
    persistEligibleCount: findings.filter((f) => f.persistEligible).length,
    airtableWriteAuthorization: "STOPPED — founder review required before expanded live writes",
    secExpansionYield: {
      thisQualifyPack: "No new SEC bulk extraction; references V4 DRH/SVC + prior #556 PEB/CLDT",
      documentsExaminedPriorV4: 7,
      hotelLevelSchedules: ["DRH", "SVC Crowne Plaza"],
      diminishingReturns: ["Host", "Sunstone", "Ashford YE2025 10-K ranges only"],
    },
  };

  fs.writeFileSync(path.join(OUT, "QUALIFICATION_FINDINGS.json"), JSON.stringify({ findings, byClass }, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_A_QUALIFICATION.json"), JSON.stringify({
    researchCandidates: findings.map((f) => ({
      findingId: f.findingId,
      hotelName: f.hotelName,
      qualClass: f.qualClass,
      trackAStatus: f.trackAStatus,
      agreementType: f.agreementType,
      answers: f.answers,
      doNotAutoPromote: true,
      persistEligible: false,
    })),
    verifiedOpenOpportunities: verifiedOpen,
  }, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_C_IDENTITY_HANDOFF.json"), JSON.stringify({ handoffs: trackC }, null, 2));
  fs.writeFileSync(path.join(OUT, "RESEARCH_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(path.join(OUT, "PERSISTENCE_READINESS.json"), JSON.stringify({
    readyToApply: false,
    reason: "Founder STOP before expanded live writes",
    wouldReuse: "hif-track-b-global-waves-persist.mjs + existing destinations",
    duplicatePrevention: [
      "tb_cala_fu_* already applied — noop only",
      "tb_gsca_* coverage audit 54 — do not re-apply",
      "tb_v4_* DRH/SVC — already applied on #560",
      "This pack uses qual_tb_fwd_* IDs and persistEligible=false",
    ],
    schemaChangesRequired: false,
  }, null, 2));

  const md = `# Track B V4 — Forward-Looking Decision Windows & Track A Qualification

**As of:** ${AS_OF}  
**Publication:** CUSTOMER_BLOCKED · **Cost:** $0  
**Airtable:** NO NEW APPLY (founder STOP)

## 1. Verified management / franchise / lease dates
| Asset | Type | Verified date fact | Precision |
|---|---|---|---|
| Westin Monterrey Valle | MANAGEMENT (Aimbridge) | Active as of 2026-09-30; expiry unknown | UNKNOWN |
| JW Marriott Monterrey Arboleda/Valle | MANAGEMENT (Aimbridge) | Active as of 2026-09-30; expiry unknown | UNKNOWN |
| Holiday Inn Monterrey Valle | FRANCHISE | Renewed FY2025; new end unknown; ops internalized 2025-09-01 | DAY (ops) |
| Courtyard GDL (ex-Wyndham Acueducto) | FRANCHISE | Conversion approved FY2025 | COMMITTED |
| Fairfield Villahermosa → InHouse | LEASE | Commenced 2026-07-01 | DAY |
| Cd. Carmen → InHouse | LEASE | Commenced 2026-04-01 | DAY |
| Yucatán Resort | LEASE target | Ops suspended 2026-04-30 | DAY |
| Westin Boston Seaport (DRH) | FRANCHISE | Expires 12/2026 | MONTH |
| Desert Ridge (RHP) | MANAGEMENT | Expires 2027 + 5× auto 10yr if thresholds | YEAR_ONLY |

## 2. Renewal / termination mechanics
- **FINN gov (xix):** at third-party HMA expiration → incorporate into internal ops (Servicios Hoteleros FINN).
- **FINN gov (xx):** early Aimbridge termination authorized only for **100%-owned** hotels (28-hotel buyout).
- Co-owned Westin/JW **excluded** from early buyout; residual Aimbridge HMAs continue.
- HMA notice / performance / change-of-control clauses for residuals: **not published**.
- Desert Ridge: automatic renewals if performance thresholds met.

## 3. Owner / operator decision authority
- FINN residuals: Technical Committee + co-owners; Aimbridge operates; brands separate.
- FIHO leases: FibraHotel controls lease conversions (executed for Villahermosa/Cd Carmen).
- DRH/RHP: US REIT owners control franchise/HMA decisions.

## 4. New potential decision windows
- Westin Monterrey Valle Aimbridge HMA (EMERGING / D) — expiry unknown; internalization-at-expiry constrains competing MCT.
- JW Marriott Monterrey Aimbridge HMA (EMERGING / D) — same + identity alias.
- Yucatán Resort lease lessee/brand (EMERGING / D) — may still be open if unsigned.
- DRH Westin Boston franchise 12/2026 (D).
- Desert Ridge 2027 (D, auto-renewal caveat).

## 5. Historical / committed excluded
HI Monterrey franchise renewal; GDL Courtyard conversion; Villahermosa & Cd Carmen InHouse leases; 28-hotel Aimbridge buyout; HI Puebla/Monterrey HMA internalizations; DRH Highgate Feb 2026 transitions.

## 6. HPC / Track C
See \`TRACK_C_IDENTITY_HANDOFF.json\` — Arboleda vs Valle; Acueducto vs Andares; Fairfield vs Fiesta Inn Villahermosa; Santander no-create.

## 7. Track A qualification
| Class | Count |
|---|---|
| HISTORICAL_COMPLETED | ${byClass[QUAL_CLASS.HISTORICAL_COMPLETED] || 0} |
| COMMITTED_NOT_OPEN | ${byClass[QUAL_CLASS.COMMITTED_NOT_OPEN] || 0} |
| CONTRACT_WATCH | ${byClass[QUAL_CLASS.CONTRACT_WATCH] || 0} |
| POTENTIAL_DECISION_WINDOW | ${byClass[QUAL_CLASS.POTENTIAL_DECISION_WINDOW] || 0} |
| **VERIFIED_OPEN_OPPORTUNITY** | **${byClass[QUAL_CLASS.VERIFIED_OPEN_OPPORTUNITY] || 0}** |

**No customer-facing Qualified Opportunities proposed.**

## 8. SEC extraction yield
No new bulk SEC this pack. Prior V4 on #560: DRH hotel schedules + SVC Crowne Plaza. Host/Sunstone/Ashford YE2025 diminishing returns → next EX-10 only.

## 9. Airtable dedup / readiness
CALA 9 findings / 8 contracts / 4 milestones already applied (re-apply = noops). Coverage-audit 54 + V4 DRH/SVC untouched. This pack \`persistEligible=false\`. **STOP before expanded writes.**

## 10. Research cost
**$0** (free IR PDFs + prior EDGAR caches).

## 11. PR / CI
- #556 MERGED @ \`05b9d231\`
- #560 OPEN (decision-window extraction) — append this research commit for IC
- No production deploy / no automation / no customer publication

## 12. Blockers / approvals required
1. Founder authorization for any new Airtable apply of \`qual_tb_fwd_*\` (or updates to CALA residual rows).
2. Track C resolve Arboleda/Valle and Acueducto/Andares before any customer signal.
3. Primary-source HMA expiry for residual Aimbridge hotels (still missing).
4. Yucatán lease counterparty disclosure.
5. IC merge of #560 after CI green.
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(JSON.stringify({ ok: true, out: OUT, findings: findings.length, byClass, verifiedOpen: verifiedOpen.length }, null, 2));
}

main();
