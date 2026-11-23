#!/usr/bin/env node
/**
 * Offline V1.2 commercial conversion recovery — frozen V1.1 evidence only.
 * Usage: node scripts/gdi-portfolio-cert-v1-2-commercial-conversion-recovery.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_PORTFOLIO_CERT_V1_2,
  runOfflineConversionRecovery,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-cert-v1-2-commercial-conversion-recovery-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const outDir = path.join(
  root,
  "reports",
  "gdi-portfolio-cert-v1-2-commercial-conversion-recovery",
);

function loadSelectionAudit() {
  const hotels = [
    {
      hotelId: "recLuxvwwxID7U2B8",
      name: "Bethesda Marriott",
      run: "gdi_portfolio_cert_v1_1_paid_pilot__recLuxvwwxID7U2B8",
      eligibleDeep: 61,
    },
    {
      hotelId: "recrPQcZg7SFARRb2",
      name: "YOTEL Geneva Lake",
      run: "gdi_portfolio_cert_v1_1_paid_pilot__recrPQcZg7SFARRb2",
      eligibleDeep: 53,
    },
  ];
  const audits = [];
  for (const h of hotels) {
    const regPath = path.join(
      root,
      "reports",
      "gdi-portfolio-cert-v1-1-paid-pilot",
      "hotels",
      h.hotelId,
      h.run,
      "deep-research-selection-register.json",
    );
    if (!fs.existsSync(regPath)) {
      audits.push({ ...h, error: "register_missing" });
      continue;
    }
    const reg = JSON.parse(fs.readFileSync(regPath, "utf8"));
    const rows = reg.register || [];
    const selected = rows.filter((r) =>
      String(r.selectionDecision || "").startsWith("SELECTED"),
    );
    const deferred = rows.filter(
      (r) => !String(r.selectionDecision || "").startsWith("SELECTED"),
    );
    const sampleDeferred = deferred
      .slice()
      .sort(
        (a, b) =>
          Number(b.preliminaryScore || b.ranking?.score || 0) -
          Number(a.preliminaryScore || a.ranking?.score || 0),
      )
      .slice(0, 5)
      .map((r) => ({
        id: r.opportunity?.opportunityId || r.candidateId,
        org: r.opportunity?.organizationName || null,
        title: String(r.opportunity?.title || "").slice(0, 100),
        segment: r.segmentId,
        score: r.preliminaryScore,
        reason: r.decisionReason,
      }));
    audits.push({
      hotelId: h.hotelId,
      hotelName: h.name,
      eligibleDeepClaimed: h.eligibleDeep,
      registerTotal: rows.length,
      selectedCount: selected.length,
      deferredCount: deferred.length,
      selected: selected.map((r) => ({
        id: r.opportunity?.opportunityId || r.candidateId,
        org: r.opportunity?.organizationName || null,
        title: String(r.opportunity?.title || "").slice(0, 100),
        segment: r.segmentId,
        score: r.preliminaryScore,
        decision: r.selectionDecision,
        reason: r.decisionReason,
        dimensions: r.dimensions || null,
      })),
      sampleUnselectedHigherOrEqual: sampleDeferred,
      weaknesses: [
        "fair_segment_floor forces a slot per live segment even when lodging potential is thin (associations/gov at score≈55.99).",
        "below_cap_or_outranked deferred rows can equal selected scores — segment floor, not hotel-specific demand, drove inclusion.",
        "Priority favors segment coverage over reachable buyer + defensible room-night evidence.",
      ],
      controlledRankingCorrection:
        "Prefer candidates with (a) future need dates, (b) lodging/meeting mechanism in discovery evidence, (c) named org contact route, over segment-floor fillers when remaining deep slots are scarce. Do not re-run discovery.",
    });
  }
  return audits;
}

function buildFounderMarkdown(result, selectionAudit) {
  const lines = [];
  lines.push("# GDI Portfolio Certification V1.2 — Founder Report");
  lines.push("");
  lines.push(`Version: \`${GDI_PORTFOLIO_CERT_V1_2}\``);
  lines.push(`Generated: ${result.at}`);
  lines.push("Mode: **$0 offline audit + controlled remediation** (no paid research, no Airtable writes, no customer publication).");
  lines.push("");
  lines.push("## Phase 1 — Integration (#564)");
  lines.push("");
  lines.push("- PR #564 (V1.1 paid pilot): **OPEN** at last check — CI SUCCESS, MERGEABLE, awaiting Integration Controller merge.");
  lines.push("- V1.2 ships as a follow-on PR; GDI product chat does not merge.");
  lines.push("- Dirty product worktrees preserved; work performed in isolated temp worktree.");
  lines.push("");
  lines.push("## 1. Exact root cause for every EXCLUDE");
  lines.push("");
  for (const r of result.rows) {
    lines.push(`### ${r.opportunityId} — ${r.organizationName}`);
    lines.push(`- Hotel: ${r.hotelName}`);
    lines.push(`- Before: \`${r.beforeDisposition}\` → After: \`${r.afterDisposition}\`${r.changed ? " **(remediated)**" : ""}`);
    lines.push(`- Eligibility: \`${r.eligibilityStatus}\` (preserve=${r.preserveOpportunity})`);
    lines.push(`- Forensic: ${r.forensicClasses.join(", ")}`);
    lines.push(`- Root cause: ${r.excludeRootCause.summary}`);
    if (r.excludeRootCause.eligibilityGaps?.length) {
      lines.push(`- Gaps: ${r.excludeRootCause.eligibilityGaps.join(", ")}`);
    }
    lines.push("");
  }
  lines.push("## 2. Deep research evidentiary improvement");
  lines.push("");
  lines.push("| Opportunity | Provider run | Investigated URLs | Verdict |");
  lines.push("|---|---|---|---|");
  for (const r of result.rows) {
    const d = r.deepResearchImprovedEvidence;
    lines.push(
      `| ${r.opportunityId} | ${d.providerRunRecorded} | ${r.investigatedUrlCount} (seed ${r.seedUrlCount}) | ${d.verdict} |`,
    );
  }
  lines.push("");
  lines.push(
    `Counts: ${JSON.stringify(result.deepResearchImprovementCounts)}`,
  );
  lines.push("");
  lines.push(
    "Finding: Paid Exa Agent runs were recorded (`exaRunId`), but `investigatedUrls` stayed empty and evidence rows remained discovery seed URLs. Paid call ≠ successful deep research without evidentiary improvement. Remediation: propagate supportingSources/claim URLs into `investigatedUrls` on mapping/restore.",
  );
  lines.push("");
  lines.push("## 3. Named buyer / contact research (public / frozen only)");
  lines.push("");
  for (const r of result.rows) {
    const c = r.contactAudit;
    lines.push(
      `- **${r.organizationName}**: named=${c.namedBuyerFound}; role=${c.roleEstablished}; personal email=${c.personalEmailSupported}; org route=${c.organizationalContactRoute || "none"}; buyers=${c.namedBuyers.map((b) => b.name).join(", ") || "—"}`,
    );
  }
  lines.push("");
  lines.push(
    "No Surfe/Findymail/PDL. No invented emails. No live Airtable PII persistence. Liatris alone has named stakeholders (Frank Yang / Riley Engels) + org contact page; remaining seven lack named buyers on frozen ORS.",
  );
  lines.push("");
  lines.push("## 4. Qualification defects");
  lines.push("");
  lines.push("1. **Disposition reconcile**: `RESEARCH_CONTINUATION` + `preserveOpportunity` incorrectly treated V41 `EXCLUDE` as aligned → eight false EXCLUDEs.");
  lines.push("2. **V3 contactOk**: requires email `@`; named buyer without email + thin lodging evidence fell through to EXCLUDE.");
  lines.push("3. **assessOrganizationLevel**: association/corporate watchlist potential gated on contactOk — circular with missing email.");
  lines.push("4. **investigatedUrls / namedStakeholdersFound**: not propagated from Exa dossier → empty URL audit + empty register stakeholder counts.");
  lines.push("5. Sales Ready bar was not the defect — none of the eight should be Sales Ready.");
  lines.push("");
  lines.push("## 5. Corrected disposition distribution");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(result.dispositionDistribution, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 6. Candidate-selection weaknesses");
  lines.push("");
  for (const a of selectionAudit) {
    lines.push(`### ${a.hotelName}`);
    lines.push(
      `- Eligible deep pool: ${a.eligibleDeepClaimed}; register ${a.registerTotal}; selected ${a.selectedCount}`,
    );
    lines.push(
      `- Selected: ${a.selected?.map((s) => `${s.org} (${s.segment}, score=${s.score})`).join("; ") || a.error}`,
    );
    for (const w of a.weaknesses || []) lines.push(`- Weakness: ${w}`);
    lines.push(`- Controlled correction: ${a.controlledRankingCorrection}`);
    lines.push("");
  }
  lines.push("## 7. Before/after benchmark");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(result.benchmark, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 8. False-positive / false-negative analysis");
  lines.push("");
  lines.push(
    `- False-positive EXCLUDE remediated: **${result.benchmark.falsePositiveExcludeRemediated}** (credible demand → WATCHLIST_RESEARCH / RESEARCH_CONTINUATION).`,
  );
  lines.push(
    `- False-negative Sales Ready forced: **${result.benchmark.falseNegativeSalesReadyForced}** (strict bar preserved).`,
  );
  lines.push(
    "- Residual risk: some WATCHLIST rows may still be low hotel-fit (Bethesda metro geography / YOTEL event lodging not yet evidenced) — continuation, not outreach-ready.",
  );
  lines.push("");
  lines.push("## 9. Bethesda scorecard contradiction");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(result.scorecardContradiction.resolution, null, 2));
  lines.push("```");
  lines.push("");
  lines.push(
    `Economics after fix: contactsGained=${result.scorecardContradiction.economicsAfter.verifiedCommercialContactsGained}, actionable=${result.scorecardContradiction.economicsAfter.opportunitiesBecomingActionable}, canonicalSalesReady=${result.scorecardContradiction.economicsAfter.canonicalSalesReadyCount}, researchContinuation=${result.scorecardContradiction.economicsAfter.researchContinuationCount}`,
  );
  lines.push("");
  lines.push("## 10. Drawer completeness");
  lines.push("");
  lines.push(
    `- customerPublicationReady on all eight: **false** (correct — no publication).`,
  );
  lines.push(
    "- Liatris has richer CIB/salesBrief fields; events rows remain discovery-thin after empty investigatedUrls.",
  );
  lines.push("");
  lines.push("## 11. Source / evidence freshness");
  lines.push("");
  lines.push(
    "- Frozen V1.1 pilot artifacts dated 2026-11-23; seed URLs are live public pages from discovery.",
  );
  lines.push(
    "- No new paid retrieval in V1.2; freshness = frozen pilot package.",
  );
  lines.push("");
  lines.push("## 12. PR / CI");
  lines.push("");
  lines.push("- See PR description after open; required local suites listed in test plan.");
  lines.push("");
  lines.push("## 13. Recommendation for second paid pilot");
  lines.push("");
  lines.push("**HOLD paid discovery expansion.** Next paid dollar should be a **targeted deep-research continuation** only on WATCHLIST rows with:");
  lines.push("1. Verified lodging/meeting mechanism questions (not summary-from-seed).");
  lines.push("2. Explicit investigatedUrls / citation capture gate before spend settlement.");
  lines.push("3. Named buyer + org route hunt with public-first contact intelligence (no Surfe until public exhausted).");
  lines.push("4. Selection correction: replace segment-floor fillers lacking lodging signals.");
  lines.push("5. Cap: reuse $12.50/hotel dual ledger; do not expand hotel count until ≥1 Sales Ready **or** documented research-complete ineligibility.");
  lines.push("");
  lines.push("## STOP");
  lines.push("");
  lines.push(
    "Return for founder review before further paid discovery, live Airtable persistence, customer publication, broader hotel rollout, or production deployment.",
  );
  lines.push("");
  lines.push("`AWAITING_INTEGRATION_CONTROLLER`");
  return lines.join("\n");
}

const result = runOfflineConversionRecovery(root, { replayV3: true });
const selectionAudit = loadSelectionAudit();
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "offline-conversion-recovery.json"),
  JSON.stringify(result, null, 2),
);
fs.writeFileSync(
  path.join(outDir, "selection-quality-audit.json"),
  JSON.stringify(selectionAudit, null, 2),
);
const md = buildFounderMarkdown(result, selectionAudit);
fs.writeFileSync(path.join(outDir, "FOUNDER_REPORT.md"), md);
console.log(
  JSON.stringify(
    {
      version: result.version,
      outDir,
      rows: result.rows.length,
      dispositionDistribution: result.dispositionDistribution,
      benchmark: result.benchmark,
      deepResearchImprovementCounts: result.deepResearchImprovementCounts,
    },
    null,
    2,
  ),
);
