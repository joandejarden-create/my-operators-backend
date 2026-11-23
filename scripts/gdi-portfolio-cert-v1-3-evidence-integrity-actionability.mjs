#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_PORTFOLIO_CERT_V1_3,
  runV13OfflineCertification,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-cert-v1-3-evidence-integrity-actionability-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const outDir = path.join(
  root,
  "reports",
  "gdi-portfolio-cert-v1-3-evidence-integrity-actionability",
);

const result = runV13OfflineCertification(root);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "offline-certification.json"),
  JSON.stringify(result, null, 2),
);

const lines = [];
lines.push("# GDI Portfolio Certification V1.3 — Founder Report");
lines.push("");
lines.push(`Version: \`${GDI_PORTFOLIO_CERT_V1_3}\``);
lines.push(`Generated: ${result.at}`);
lines.push("Mode: **$0 validation + controlled remediation** (no paid Exa, no Airtable, no publication).");
lines.push("");
lines.push("## 1. PR #564 / #566 integration status");
lines.push("");
lines.push(`- ${JSON.stringify(result.phase1Integration)}`);
lines.push("- Both PRs CI-green / MERGEABLE at last check; **not yet merged into main**.");
lines.push("- GDI product chat does not merge. IC must merge **#564 then #566** (dependency order) before claiming main-integrated V1.2.");
lines.push("- V1.3 branch is cut from the #566 tip to avoid duplicate V1.1 commits.");
lines.push("");
lines.push("## 2. Evidence propagation test results");
lines.push("");
lines.push(`- Fixture pass: **${result.evidencePropagation.pass}**`);
lines.push(`- Mapped investigatedUrls include housing URL: ${(result.evidencePropagation.mappedInvestigatedUrls || []).join(", ")}`);
lines.push(`- Empty-provider completion complete=false: **${!result.evidencePropagation.emptyProviderCompletion.complete}** (${result.evidencePropagation.emptyProviderCompletion.status})`);
lines.push(`- Genuine EXCLUDE negatives pass: **${result.genuineExcludeNegatives.pass}**`);
lines.push("");
lines.push("## 3. Actual investigated URL traceability (frozen V1.1 eight)");
lines.push("");
lines.push("| Opportunity | Seed | Investigated | Completion | Improvement |");
lines.push("|---|---:|---:|---|---|");
for (const t of result.urlTraces) {
  lines.push(
    `| ${t.opportunityId} (${t.organizationName}) | ${t.seedUrlCount} | ${t.investigatedUrlCount} | ${t.deepCompletion.status} | ${t.improvementVerdict} |`,
  );
}
lines.push("");
lines.push(
  `Frozen investigatedUrls still empty on **${result.qualificationBenchmark.investigatedUrlsStillEmptyOnFrozen}/8** (V1.1 defect). V1.2/V1.3 propagation fixes apply to future runs; frozen ledger is not rewritten as if URLs existed.`,
);
lines.push("");
lines.push("## 4–7. Three-candidate commercial review (AAEI / AHA / ESCP)");
lines.push("");
for (const c of result.threeCandidateReview.candidates) {
  lines.push(`### ${c.organizationName} (${c.opportunityId})`);
  lines.push(`- Event: ${c.eventTitle} | ${c.eventDates}`);
  lines.push(`- Venue: ${c.venue}`);
  lines.push(`- Organizer: ${c.organizer}`);
  lines.push(`- Named lodging buyer: ${c.namedLodgingBuyer || "none verified"}`);
  lines.push(`- Hotel sourcing evidence: ${c.hotelSourcingDecisionEvidence}`);
  lines.push(
    `- Public contact: ${c.publicContactChannel.type} ${c.publicContactChannel.email || c.publicContactChannel.organizationAddress || "n/a"}`,
  );
  lines.push(`- Pilot hotel relevance: ${c.hotelRelevanceToPilot}`);
  lines.push(`- Lodging demand validated (any hotel): ${c.lodgingDemandValidated}; for pilot hotel: ${c.lodgingDemandForPilotHotel}`);
  lines.push(`- Disposition: **${c.dispositionRecommendation}** | Sales Ready: ${c.salesReady}`);
  lines.push(`- Continue: ${c.continueResearch} — ${c.continueReason}`);
  lines.push("");
}
lines.push("## 8. Research-continuation quality");
lines.push("");
for (const p of result.researchContinuation.plans) {
  lines.push(
    `- **#${p.priorityRank} ${p.organizationName}**: ${p.decision} — ${p.estimatedValueOfAnotherPass}`,
  );
}
lines.push("");
lines.push("## 9. Before/after commercial qualification");
lines.push("");
lines.push(`- V1.2 verification pass: **${result.v12Verification.pass}** (WATCHLIST=${result.v12Verification.watchlistResearch}, EXCLUDE=${result.v12Verification.incorrectExclude}, Sales Ready=${result.v12Verification.salesReady}, publication=${result.v12Verification.customerPublication})`);
lines.push("- WATCHLIST does **not** imply verified pilot-hotel demand (eligibility gaps retained).");
lines.push(
  `- Public re-review: ${result.qualificationBenchmark.competitorCommittedPrimary} of priority-three are STOP_PRIMARY_COMPETITOR_COMMITTED; 0 pilot-hotel lodging demand validated.`,
);
lines.push("");
lines.push("## 10. Recommended next paid pilot (design only)");
lines.push("");
lines.push(`- Aggregate max: **$${result.paidPilotDesign.aggregateMaxSpendUsd}** across ≤3 opps (Liatris, ASGSR, IRF).`);
lines.push("- Excluded from paid: AAEI, AHA (competitor housing committed), ESCP (weak YOTEL↔Palexpo fit — free monitor only).");
lines.push("- **Requires separate founder GO** — not executed in V1.3.");
lines.push("");
lines.push("## 11. CI results");
lines.push("");
lines.push("- See PR checks after open; local suites in test plan.");
lines.push("");
lines.push("## 12. Readiness verdict");
lines.push("");
lines.push(`**${result.readinessVerdict}**`);
lines.push("");
lines.push("System can preserve incomplete research without false EXCLUDE and can distinguish competitor-committed lodging from open pilot-hotel demand. Not certified for broader hotel scaling.");
lines.push("");
lines.push("## STOP");
lines.push("");
lines.push("Return for founder review before additional paid research, live persistence, customer publication, or broader rollout.");
lines.push("");
lines.push("`AWAITING_INTEGRATION_CONTROLLER`");

fs.writeFileSync(path.join(outDir, "FOUNDER_REPORT.md"), lines.join("\n"));
fs.writeFileSync(
  path.join(outDir, "paid-pilot-design.json"),
  JSON.stringify(result.paidPilotDesign, null, 2),
);
fs.writeFileSync(
  path.join(outDir, "three-candidate-commercial-review.json"),
  JSON.stringify(result.threeCandidateReview, null, 2),
);

console.log(
  JSON.stringify(
    {
      version: result.version,
      outDir,
      v12Pass: result.v12Verification.pass,
      propagationPass: result.evidencePropagation.pass,
      negativesPass: result.genuineExcludeNegatives.pass,
      readinessVerdict: result.readinessVerdict,
      paidMaxUsd: result.paidPilotDesign.aggregateMaxSpendUsd,
    },
    null,
    2,
  ),
);
