#!/usr/bin/env node
/**
 * $0 saved-evidence audit of Design Hotels unresolved opportunities.
 * Does NOT change STAGED / CURRENT. No paid calls.
 *
 * Revisits LIVE_RECOVERY_REPORT rows: harvest junk-tolerant candidates,
 * run commercial ranking + alternative-person reconnect, report recoverable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const {
  runBuyerAlternativePersonResearch,
  harvestAlternativePersonCandidates,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js"
);
const {
  evaluateNamedContactCoverageStandard,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);

const REPORT = path.join(
  ROOT,
  "reports/gdi-buyer-who-live-recovery-v1/LIVE_RECOVERY_REPORT.json",
);
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-buyer-alternative-person-research-v1",
);

async function main() {
  if (!fs.existsSync(REPORT)) {
    console.error("Missing LIVE_RECOVERY_REPORT.json — cannot audit");
    process.exit(1);
  }
  const live = JSON.parse(fs.readFileSync(REPORT, "utf8"));
  const audit = {
    at: new Date().toISOString(),
    contract: "gdi-design-hotels-saved-evidence-alternative-person-audit-v1",
    stagedChanged: false,
    paidCalls: 0,
    totals: {
      rows: 0,
      unresolved: 0,
      hadPlausibleCandidatesAmongJunk: 0,
      recoverableNamedViaRanking: 0,
      stillUnresolved: 0,
      ladderWouldExecute: 0,
    },
    highlighted: [],
    recoverable: [],
    stillUnresolvedSample: [],
  };

  for (const h of live.hotelDetails || []) {
    for (const row of h.rows || []) {
      audit.totals.rows += 1;
      const opp = {
        ...(row.opportunity || {}),
        id: row.opportunityId || row.opportunity?.id,
        organizationName:
          row.organizationName || row.opportunity?.organizationName,
        hotelId: h.hotelId,
      };
      const status = row.status || "";
      if (status === "VERIFIED_NAMED_BUYER") continue;
      audit.totals.unresolved += 1;

      const harvest = harvestAlternativePersonCandidates(opp);
      if (harvest.accepted.length) {
        audit.totals.hadPlausibleCandidatesAmongJunk += 1;
      }

      const result = await runBuyerAlternativePersonResearch(opp, {
        authorizePaidResearch: false,
      });
      const cov = evaluateNamedContactCoverageStandard(result.opportunity);
      if ((result.trace.strategiesAttempted || []).length) {
        audit.totals.ladderWouldExecute += 1;
      }

      const entry = {
        hotelId: h.hotelId,
        opportunityId: opp.id,
        organizationName: opp.organizationName,
        priorStatus: status,
        junkRejected: harvest.rejected.length,
        plausibleAccepted: harvest.accepted.length,
        acceptedNames: harvest.accepted.map((p) => p.name),
        selectedPrimary: result.opportunity.primaryContact?.name || null,
        selectedSecondary: result.opportunity.secondaryContact?.name || null,
        primaryKind: result.trace.primaryKind,
        strategiesAttempted: (result.trace.strategiesAttempted || []).map(
          (a) => a.strategyId,
        ),
        namedCount: cov.namedCount,
        finalizeEligible: cov.finalizeEligible,
      };

      // Highlight forensic examples
      const names = harvest.accepted.map((p) => p.name);
      if (
        names.includes("Craig Davis") ||
        names.includes("Amy Yoder") ||
        /craig davis|amy yoder/i.test(JSON.stringify(opp.namedStakeholders || []))
      ) {
        audit.highlighted.push({
          ...entry,
          note: "Forensic example — verify employer/relevance before accepting",
        });
      }

      if (cov.namedCount >= 1 && cov.finalizeEligible) {
        audit.totals.recoverableNamedViaRanking += 1;
        audit.recoverable.push(entry);
      } else {
        audit.totals.stillUnresolved += 1;
        if (audit.stillUnresolvedSample.length < 15) {
          audit.stillUnresolvedSample.push(entry);
        }
      }
    }
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, "SAVED_EVIDENCE_110_AUDIT.json");
  fs.writeFileSync(outPath, JSON.stringify(audit, null, 2));
  console.log(JSON.stringify(audit.totals, null, 2));
  console.log(`highlighted=${audit.highlighted.length}`);
  console.log(`WROTE ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
