/**
 * YOTEL Geneva Lake — Account Expansion Recovery Pass V1 (DRY RUN).
 *
 *   node scripts/gdi-yotel-account-expansion-recovery-v1.mjs
 *
 * Does NOT apply to the live opportunity bag.
 * Founder review required before any apply.
 */

import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isCustomerFacingOpportunity } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  YOTEL_HOTEL_ID,
  YOTEL_DEMAND_SIGNALS,
  YOTEL_ACCOUNT_RESEARCH,
  buildEvaluatedExpansionCandidates,
  GDI_MATURITY_STATE,
} from "../lib/group-demand-intelligence/expansion-pilot/yotel-account-expansion-recovery-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/yotel-account-expansion-v1");

if (process.argv.includes("--apply")) {
  console.error("DRY RUN ONLY. Refusing --apply. Founder review required.");
  process.exit(2);
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(p, body.endsWith("\n") ? body : body + "\n", "utf8");
  return p;
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const { accepted, rejected, evaluated, nowDate } = buildEvaluatedExpansionCandidates({
    nowDate: "2026-10-05",
  });

  const live = loadOpportunities(YOTEL_HOTEL_ID);
  const liveOpps = Array.isArray(live?.opportunities) ? live.opportunities : [];
  const liveReady = liveOpps.filter((o) =>
    isGdiCustomerOpportunityReady(o, { nowDate }).ok
  );
  const liveVisible = liveOpps.filter((o) =>
    isCustomerFacingOpportunity(o, { nowDate })
  );

  const byMaturity = { SIGNAL: 0, CANDIDATE: 0, QUALIFIED: 0, ACTIONABLE: 0 };
  for (const row of evaluated) {
    byMaturity[row.opportunity.gdiMaturityState] =
      (byMaturity[row.opportunity.gdiMaturityState] || 0) + 1;
  }

  const ranked = [...evaluated].sort((a, b) => {
    const ua = Number(a.account.usefulnessScore || 0);
    const ub = Number(b.account.usefulnessScore || 0);
    if (ub !== ua) return ub - ua;
    return (
      Number(b.opportunity.hotelFitScore || 0) - Number(a.opportunity.hotelFitScore || 0)
    );
  });
  const top20 = ranked.slice(0, 20);

  // Quality metrics (precision-minded)
  const salesWorthyTop20 = top20.filter((r) => Number(r.account.usefulnessScore || 0) >= 70);
  const novelTop20 = top20.filter((r) => Number(r.account.noveltyScore || 0) >= 70);
  const venueLeak = evaluated.filter((r) =>
    /VENUE|Palexpo SA/i.test(r.opportunity.organizationName + r.opportunity.participationRole)
  ).length;
  const organizerLeak = evaluated.filter((r) =>
    /^(ORGANIZER|EVENT_ORGANIZER|SECRETARIAT|PARENT_SOCIETY|UNIVERSITY_HOST)$/i.test(
      String(r.opportunity.participationRole || "")
    )
  ).length;
  const genericActionable = evaluated.filter(
    (r) =>
      r.opportunity.gdiMaturityState === GDI_MATURITY_STATE.ACTIONABLE &&
      (r.buyerPath?.class === "SOURCE_PAGE" ||
        r.buyerPath?.class === "GENERAL_ORG_CONTACT")
  ).length;
  const speculativeRoomClaims = evaluated.filter((r) => {
    const o = r.opportunity;
    return (
      o.lodgingVerified === true ||
      /\bneeds\s+\d+\s+rooms\b/i.test(o.summaryWhat || "") ||
      (o.peakRoomsClaimKind === "FACT" && o.modeledRoomsMin != null)
    );
  }).length;
  const modeledHonest =
    evaluated.length === 0
      ? 100
      : Math.round(
          (100 *
            evaluated.filter((r) =>
              /modeled|not verified/i.test(
                `${r.opportunity.modeledDemandDisclaimer || ""} ${r.opportunity.modeledDemandBasis || ""}`
              )
            ).length) /
            evaluated.length
        );

  const top20Precision =
    top20.length === 0
      ? 0
      : Math.round((100 * salesWorthyTop20.length) / top20.length);
  const usefulnessAt20 =
    top20.length === 0
      ? 0
      : Math.round(
          top20.reduce((s, r) => s + Number(r.account.usefulnessScore || 0), 0) /
            top20.length
        );
  const noveltyAt20 =
    top20.length === 0
      ? 0
      : Math.round(
          top20.reduce((s, r) => s + Number(r.account.noveltyScore || 0), 0) /
            top20.length
        );

  // If applied (QUALIFIED+ACTIONABLE only, no Ready bypass) — hypothetical customer-visible
  const wouldBeVisibleIfQualifiedFlag = evaluated.filter(
    (r) =>
      r.opportunity.gdiMaturityState === GDI_MATURITY_STATE.QUALIFIED ||
      r.opportunity.gdiMaturityState === GDI_MATURITY_STATE.ACTIONABLE
  );
  const customerVisibleIfApplied =
    liveVisible.length +
    wouldBeVisibleIfQualifiedFlag.filter(
      (r) => r.opportunity.gdiMaturityState === GDI_MATURITY_STATE.ACTIONABLE
    ).length;
  // QUALIFIED visibility would need flag; report both
  const customerVisibleIfQualifiedEnabled =
    liveVisible.length + wouldBeVisibleIfQualifiedFlag.length;

  // 01 summary
  write(
    "01-signal-expansion-summary.md",
    `# YOTEL Account Expansion Recovery Pass V1 — Signal Expansion Summary

**Mode:** DRY RUN (no apply)  
**Hotel:** \`${YOTEL_HOTEL_ID}\` (YOTEL Geneva Lake)  
**Generated:** ${new Date().toISOString()}  
**Now date:** ${nowDate}

## Live bag (unchanged)

| Metric | Count |
| --- | ---: |
| Live opportunities | ${liveOpps.length} |
| Live Ready / ACTIONABLE | ${liveReady.length} |
| Live customer-visible | ${liveVisible.length} |

Ready survivors (unchanged): ${liveReady.map((o) => o.organizationName || o.title).join("; ") || "—"}

## Signals expanded

${YOTEL_DEMAND_SIGNALS.map((s) => `- **${s.label}** (\`${s.id}\`) — ${s.eventStartDate} → ${s.eventEndDate} @ ${s.venue}`).join("\n")}

## Research counts

| Metric | Count |
| --- | ---: |
| Accounts researched | ${YOTEL_ACCOUNT_RESEARCH.length} |
| Accepted (named + evidenced) | ${accepted.length} |
| Rejected | ${rejected.length} |
| CANDIDATE | ${byMaturity.CANDIDATE} |
| QUALIFIED | ${byMaturity.QUALIFIED} |
| ACTIONABLE (new) | ${byMaturity.ACTIONABLE} |

## Key product findings

1. **AidEx** yields the strongest named international exhibitor set (logistics / TMC / aviation) with defensible traveling cohorts and YOTEL corridor fit.
2. **Watches & Wonders** yields support-team-shaped brands (NOMOS, Sinn, Bremont, Porsche Design debut) — ultra-luxury maisons (Rolex, Patek) rejected for YOTEL fit.
3. **SETAC Geneva 2027** exhibitor list is **not yet published**; Maastricht 2026 exhibitors kept as **CANDIDATE** recurring-pattern only.
4. **ECOSOC/OCHA 2026 HAS is in New York** — Geneva lodging expansion rejected (honest geography).
5. **Art Genève** keeps international galleries only; local Geneva galleries rejected.
6. **No new ACTIONABLE** — Ready gate unchanged; buyer paths remain source-page insufficient.

## Recommendation

**DO NOT APPLY yet** without founder review. Preferred apply scope if approved: AidEx + W&W QUALIFIED rows only, after optional buyer-research pass. SETAC stays CANDIDATE until Geneva list publishes.
`
  );

  // 02 candidates csv
  const candHeaders = [
    "rank",
    "accountName",
    "parentDemandSignalLabel",
    "accountRole",
    "gdiMaturityState",
    "hotelFitScore",
    "usefulnessScore",
    "noveltyScore",
    "modeledRoomsMin",
    "modeledRoomsMax",
    "travelingCohortType",
    "lodgingControlHypothesis",
    "buyerContactPathClass",
    "isOpportunityMultiplier",
    "evidenceUrl",
  ];
  const candRows = ranked.map((r, i) => ({
    rank: i + 1,
    accountName: r.account.accountName,
    parentDemandSignalLabel: r.opportunity.parentDemandSignalLabel,
    accountRole: r.account.accountRole,
    gdiMaturityState: r.opportunity.gdiMaturityState,
    hotelFitScore: r.opportunity.hotelFitScore,
    usefulnessScore: r.account.usefulnessScore,
    noveltyScore: r.account.noveltyScore,
    modeledRoomsMin: r.opportunity.modeledRoomsMin,
    modeledRoomsMax: r.opportunity.modeledRoomsMax,
    travelingCohortType: r.opportunity.travelingCohortType,
    lodgingControlHypothesis: r.opportunity.lodgingControlHypothesis,
    buyerContactPathClass: r.buyerPath?.class || "",
    isOpportunityMultiplier: r.opportunity.isOpportunityMultiplier === true,
    evidenceUrl: r.account.evidenceItems?.[0]?.sourceUrl || "",
  }));
  write(
    "02-account-candidates.csv",
    [
      candHeaders.join(","),
      ...candRows.map((row) => candHeaders.map((h) => csvEscape(row[h])).join(",")),
    ].join("\n")
  );

  // 03 rejections
  const rejHeaders = ["accountName", "parentDemandSignalId", "accountRole", "rejectReason"];
  write(
    "03-rejections.csv",
    [
      rejHeaders.join(","),
      ...rejected.map((a) =>
        [
          a.accountName,
          a.parentDemandSignalId,
          a.accountRole,
          a.rejectReason || "",
        ]
          .map(csvEscape)
          .join(",")
      ),
    ].join("\n")
  );

  // 04 qualified
  const qualified = evaluated.filter(
    (r) => r.opportunity.gdiMaturityState === GDI_MATURITY_STATE.QUALIFIED
  );
  write(
    "04-qualified-candidates.md",
    `# QUALIFIED Candidates

Count: **${qualified.length}**

${
  qualified.length === 0
    ? "_None reached QUALIFIED under canonical evaluator (check cohort evidence / summary / account class)._\n"
    : qualified
        .map(
          (r) => `## ${r.account.accountName}
- Signal: ${r.opportunity.parentDemandSignalLabel}
- Cohort: ${r.opportunity.travelingCohortType} — ${r.opportunity.travelingCohortSummary}
- Rooms: ${r.opportunity.modeledDemandDisclaimer}
- Lodging control: ${r.opportunity.lodgingControlHypothesis} (${r.opportunity.lodgingControlConfidence})
- Fit: ${r.opportunity.hotelFitScore} — ${(r.opportunity.hotelFitRationale || []).join("; ")}
- Buyer path: ${r.buyerPath?.class} (not Ready-eligible)
- Missing: ${(r.opportunity.missingValidation || []).join(", ")}
- Why sales time: usefulness ${r.account.usefulnessScore}/100
`
        )
        .join("\n")
}
`
  );

  // 05 actionable
  const actionable = evaluated.filter(
    (r) => r.opportunity.gdiMaturityState === GDI_MATURITY_STATE.ACTIONABLE
  );
  write(
    "05-actionable-candidates.md",
    `# ACTIONABLE Candidates (new expansion)

Count: **${actionable.length}**

Existing live Ready (unchanged): ${liveReady.map((o) => o.organizationName).join("; ")}

${
  actionable.length === 0
    ? `_No new ACTIONABLE from expansion — Ready gate unchanged. Buyer paths on expansion rows are source-page / insufficient._\n`
    : actionable.map((r) => `- ${r.account.accountName}`).join("\n")
}
`
  );

  // 06 evidence audit
  write(
    "06-evidence-audit.md",
    `# Evidence Audit

## Preferred source order used
1. Official event exhibitor/sponsor pages
2. Official press releases / programme books
3. Official speaker pages
4. Reputable specialist publications

## Accepted evidence highlights
${accepted
  .map(
    (a) =>
      `- **${a.accountName}** → ${(a.evidenceItems || [])
        .map((e) => `${e.evidenceType} (${e.confidence}) ${e.sourceUrl || ""}`)
        .join("; ")}`
  )
  .join("\n")}

## Rejected evidence notes
${rejected.map((a) => `- **${a.accountName}**: ${a.rejectReason}`).join("\n")}

## Speculative room claim rate
**${speculativeRoomClaims}** / ${evaluated.length} (target 0)

## Modeled demand labeled honestly
**${modeledHonest}%** (target 100%)
`
  );

  // 07 yotel fit
  write(
    "07-yotel-fit-audit.md",
    `# YOTEL Fit Audit

Favor: 10–100 rooms, international, multi-night, airport/Palexpo, corporate/exhibitor/vendor crews, price-sensitive overflow.

| Account | Fit | Band | Rationale |
| --- | ---: | --- | --- |
${ranked
  .map(
    (r) =>
      `| ${r.account.accountName} | ${r.opportunity.hotelFitScore} | ${r.opportunity.modeledRoomsMin}–${r.opportunity.modeledRoomsMax} | ${(r.opportunity.hotelFitRationale || []).join("; ")} |`
  )
  .join("\n")}

## Down-ranked / rejected for fit
- Rolex, Patek Philippe (ultra-luxury)
- Local Geneva galleries / UBS / Protectas (local-heavy)
- ECOSOC NY participants (wrong city)
`
  );

  // 08 buyer path
  write(
    "08-buyer-path-audit.md",
    `# Buyer Path Audit

Buyer research status for expansion QUALIFIED/CANDIDATE rows — **no ACTIONABLE via generic homepage**.

| Account | Maturity | Path class | Status |
| --- | --- | --- | --- |
${ranked
  .map(
    (r) =>
      `| ${r.account.accountName} | ${r.opportunity.gdiMaturityState} | ${r.buyerPath?.class || "—"} | ${r.opportunity.buyerResearchStatus} |`
  )
  .join("\n")}

Generic-contact ACTIONABLE count: **${genericActionable}** (target 0)
`
  );

  // Top-20 pack + RETURN json
  const returnPayload = {
    SIGNALS_EXPANDED: YOTEL_DEMAND_SIGNALS.map((s) => s.label),
    NAMED_ACCOUNTS_FOUND: accepted.length,
    TOP_20: top20.map((r, i) => ({
      rank: i + 1,
      accountName: r.account.accountName,
      signal: r.opportunity.parentDemandSignalLabel,
      maturity: r.opportunity.gdiMaturityState,
      usefulness: r.account.usefulnessScore,
      novelty: r.account.noveltyScore,
      cohort: r.opportunity.travelingCohortSummary,
      rooms: `${r.opportunity.modeledRoomsMin}–${r.opportunity.modeledRoomsMax}`,
      lodgingControl: r.opportunity.lodgingControlHypothesis,
      buyerPath: r.buyerPath?.class,
      whySalesTime: r.account.recommendedNextAction,
      multiplier: r.opportunity.isOpportunityMultiplier === true,
    })),
    SPLIT: byMaturity,
    REJECTED_COUNT: rejected.length,
    EXISTING_READY: liveReady.map((o) => ({
      id: o.id,
      org: o.organizationName,
    })),
    NEW_ACTIONABLE: actionable.length,
    CUSTOMER_VISIBLE_IF_APPLIED_ACTIONABLE_ONLY: customerVisibleIfApplied,
    CUSTOMER_VISIBLE_IF_QUALIFIED_FLAG_ON: customerVisibleIfQualifiedEnabled,
    TOP_20_PRECISION: top20Precision,
    USEFULNESS_AT_20: usefulnessAt20,
    NOVELTY_AT_20: noveltyAt20,
    VENUE_OPERATOR_LEAKAGE: venueLeak,
    ORGANIZER_SHELL_LEAKAGE: organizerLeak,
    SPECULATIVE_ROOM_CLAIM_RATE: speculativeRoomClaims,
    MODELED_HONEST_PCT: modeledHonest,
    GENERIC_CONTACT_ACTIONABLE: genericActionable,
    RECOMMENDATION: "DO_NOT_APPLY_PENDING_FOUNDER_REVIEW",
    APPLY: false,
  };

  write("RETURN_SUMMARY.json", JSON.stringify(returnPayload, null, 2));

  write(
    "00-FOUNDER_RETURN.md",
    `# YOTEL Account Expansion Recovery Pass V1 — Founder Return

**APPLY: NO** (dry-run only)

1. **SIGNALS EXPANDED:** ${YOTEL_DEMAND_SIGNALS.map((s) => s.label).join("; ")}
2. **NAMED ACCOUNTS FOUND:** ${accepted.length} accepted / ${rejected.length} rejected / ${YOTEL_ACCOUNT_RESEARCH.length} researched
3. **TOP 20:** see RETURN_SUMMARY.json / 02-account-candidates.csv
4. **SPLIT:** CANDIDATE ${byMaturity.CANDIDATE} · QUALIFIED ${byMaturity.QUALIFIED} · ACTIONABLE ${byMaturity.ACTIONABLE}
5–10. See ranked CSV (cohort, rooms, lodging control, buyer path, evidence)
11. **REJECTED EXAMPLES:** ECOSOC NY geography; Rolex/Patek YOTEL-fit; local galleries; venue/organizer shells; generic compression shells
12. **EXISTING AIDEX / CHI READY:** preserved (${liveReady.length} Ready) — ${liveReady.map((o) => o.organizationName).join("; ")}
13. **NEW ACTIONABLE:** ${actionable.length}
14. **CUSTOMER-VISIBLE IF APPLIED:** Actionable-only → ${customerVisibleIfApplied}; if QUALIFIED flag on → ${customerVisibleIfQualifiedEnabled}
15. **TOP-20 PRECISION:** ${top20Precision}%
16. **USEFULNESS@20:** ${usefulnessAt20}
17. **NOVELTY@20:** ${noveltyAt20}
18. **VENUE / ORGANIZER LEAKAGE:** venue ${venueLeak} · organizer ${organizerLeak} (target 0)
19. **SPECULATIVE CLAIM RATE:** ${speculativeRoomClaims} (target 0); modeled honest ${modeledHonest}%
20. **RECOMMENDATION:** **DO NOT APPLY** until founder review. Prefer AidEx + mid-tier W&W QUALIFIED after buyer research; keep SETAC as CANDIDATE until Geneva exhibitor list publishes; do not revive ECOSOC as Geneva lodging.
`
  );

  console.log(JSON.stringify(returnPayload, null, 2));
  console.log(`\nWrote reports to ${OUT}`);
}

main();
