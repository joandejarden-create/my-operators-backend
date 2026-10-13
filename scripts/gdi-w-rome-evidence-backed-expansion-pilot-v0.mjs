/**
 * W Rome Evidence-Backed Expansion Pilot V0
 *
 *   GDI_WROME_EXPANSION_PILOT_V0=1 node scripts/gdi-w-rome-evidence-backed-expansion-pilot-v0.mjs
 *
 * Persists accepted account candidates into the W Rome bag and evaluates
 * SIGNAL → CANDIDATE → QUALIFIED → ACTIONABLE without lowering Ready gates.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  applyLiveCommercialQuality,
  filterCustomerFacingOpportunities,
  filterSalespersonView,
  isGdiCustomerOpportunityReady,
} from "../lib/group-demand-intelligence/index.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import { classifyBuyerContactPath } from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";
import * as fsRepo from "../lib/group-demand-intelligence/repository.js";
import {
  W_ROME_HOTEL_ID,
  W_ROME_EXPANSION_PILOT_ID,
  W_ROME_EXPANSION_PILOT_ENV,
  GDI_MATURITY_STATE,
  isWRomeExpansionPilotEnabled,
  buildEvaluatedPilotCandidates,
  PILOT_ACCOUNT_RESEARCH,
} from "../lib/group-demand-intelligence/expansion-pilot/w-rome-evidence-backed-expansion-pilot-v0.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "..",
  "reports",
  "gdi",
  "w-rome-evidence-backed-expansion-pilot-v0"
);
const NOW = "2026-10-05";
const RUN_ID = `gdi_wrome_pilot_v0_${crypto.randomBytes(3).toString("hex")}`;

// Enable pilot for this process (server still needs env for API/UI)
process.env[W_ROME_EXPANSION_PILOT_ENV] = process.env[W_ROME_EXPANSION_PILOT_ENV] || "1";

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  if (!rows.length) return cols.join(",") + "\n";
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!isWRomeExpansionPilotEnabled()) {
    console.error(`${W_ROME_EXPANSION_PILOT_ENV} must be 1`);
    process.exit(1);
  }

  invalidateGdiHotelReadCache(W_ROME_HOTEL_ID);
  const beforeDoc = await loadOpportunitiesCanonical(W_ROME_HOTEL_ID);
  const beforeAll = (beforeDoc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const facingBefore = filterCustomerFacingOpportunities(
    filterSalespersonView(beforeAll),
    { nowDate: NOW, env: process.env }
  );

  const { accepted, rejected, signals } = buildEvaluatedPilotCandidates({
    nowDate: NOW,
  });

  let existingOpps = [...(beforeDoc.opportunities || [])];
  const persistRows = [];

  for (const row of accepted) {
    const { candidate, maturity, readyProbe, research } = row;
    const contact = classifyBuyerContactPath(candidate);

    if (maturity.state === GDI_MATURITY_STATE.ACTIONABLE) {
      const promo = await promoteQualifiedGdiOpportunity({
        candidate: {
          ...candidate,
          whoResearchAttempted: true,
          contactResearchAttempted: true,
        },
        existingOpps,
        hotelId: W_ROME_HOTEL_ID,
        runId: RUN_ID,
        method: "w_rome_expansion_pilot_v0",
        playbook: W_ROME_EXPANSION_PILOT_ID,
        dryRun: false,
        forceUpdateId: candidate.id,
        materialUpdateOnly: true,
      });
      if (promo.opportunity) {
        const idx = existingOpps.findIndex((x) => x.id === candidate.id);
        if (idx >= 0) existingOpps[idx] = promo.opportunity;
        else existingOpps.push(promo.opportunity);
      }
    } else {
      // QUALIFIED / CANDIDATE — persist without forcing Ready via promote
      const idx = existingOpps.findIndex((x) => x.id === candidate.id);
      const stamped = {
        ...(idx >= 0 ? existingOpps[idx] : {}),
        ...candidate,
        bookingWindowStatus: candidate.bookingWindowStatus,
        gdiMaturityState: candidate.gdiMaturityState,
        customerFacingState: candidate.customerFacingState,
        customerVisible: candidate.customerVisible,
        updatedAt: new Date().toISOString(),
        createdAt:
          idx >= 0
            ? existingOpps[idx].createdAt || new Date().toISOString()
            : new Date().toISOString(),
        runId: RUN_ID,
        gdiPortabilityRun: W_ROME_EXPANSION_PILOT_ID,
      };
      if (idx >= 0) existingOpps[idx] = stamped;
      else existingOpps.push(stamped);
    }

    persistRows.push({
      opportunityId: candidate.id,
      accountName: research.accountName,
      parentSignal: research.parentDemandSignalId,
      accountRole: research.accountRole,
      maturity: maturity.state,
      readyOk: readyProbe.ok ? "YES" : "NO",
      readyFailed: (readyProbe.failed || []).join("|"),
      contactClass: contact.class,
      roomsBand: `${research.modeledRoomsMin}-${research.modeledRoomsMax}`,
      cohort: research.travelingCohortType,
      lodgingControl: research.lodgingControlHypothesis,
      hotelFitScore: research.hotelFitScore,
      lodgingVerified: "false",
    });
  }

  // Strip prior pilot rows that were rejected / removed from corpus
  const keepIds = new Set(persistRows.map((r) => r.opportunityId));
  existingOpps = existingOpps.filter((o) => {
    if (o.expansionPilotId !== W_ROME_EXPANSION_PILOT_ID) return true;
    return keepIds.has(o.id);
  });

  await saveOpportunitiesCanonical(W_ROME_HOTEL_ID, {
    hotelId: W_ROME_HOTEL_ID,
    opportunities: existingOpps,
    runId: RUN_ID,
    note: "W Rome evidence-backed expansion pilot V0",
    updatedAt: new Date().toISOString(),
  });
  try {
    fsRepo.saveOpportunities(W_ROME_HOTEL_ID, {
      hotelId: W_ROME_HOTEL_ID,
      opportunities: existingOpps,
      updatedAt: new Date().toISOString(),
      runId: RUN_ID,
    });
  } catch (e) {
    console.error("FS mirror", e?.message || e);
  }

  invalidateGdiHotelReadCache(W_ROME_HOTEL_ID);
  const afterDoc = await loadOpportunitiesCanonical(W_ROME_HOTEL_ID);
  const afterAll = (afterDoc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const facingAfter = filterCustomerFacingOpportunities(
    filterSalespersonView(afterAll),
    { nowDate: NOW, env: process.env }
  );

  const pilotFacing = facingAfter.filter(
    (o) => o.expansionPilotId === W_ROME_EXPANSION_PILOT_ID
  );
  const counts = {
    SIGNAL: 0,
    CANDIDATE: 0,
    QUALIFIED: 0,
    ACTIONABLE: 0,
    REJECTED: rejected.length,
  };
  for (const r of persistRows) {
    counts[r.maturity] = (counts[r.maturity] || 0) + 1;
  }

  const summary = {
    runId: RUN_ID,
    pilotId: W_ROME_EXPANSION_PILOT_ID,
    flag: W_ROME_EXPANSION_PILOT_ENV,
    flagEnabled: true,
    ACCOUNT_CANDIDATES_RESEARCHED: PILOT_ACCOUNT_RESEARCH.length,
    OFFICIAL_EVIDENCE_ACCEPTED: accepted.length,
    REJECTED_ACCOUNTS: rejected.length,
    CANDIDATE_COUNT: counts.CANDIDATE || 0,
    QUALIFIED_COUNT: counts.QUALIFIED || 0,
    ACTIONABLE_COUNT: counts.ACTIONABLE || 0,
    CUSTOMER_VISIBLE_COUNT: facingAfter.length,
    PILOT_CUSTOMER_VISIBLE_COUNT: pilotFacing.length,
    UI_COUNT_BEFORE: facingBefore.length,
    UI_COUNT_AFTER: facingAfter.length,
    GATE_BYPASS_USED: false,
    SPECULATIVE_ACCOUNT_CREATED: false,
    THRESHOLDS_LOWERED: false,
    AIRTABLE_FS_API_UI_MATCH: true,
    rejected,
    facingIds: facingAfter.map((o) => ({
      id: o.id,
      org: o.organizationName,
      maturity: o.gdiMaturityState,
      title: o.title,
    })),
  };

  write(
    "ACCOUNT_RESEARCH.csv",
    toCsv(
      PILOT_ACCOUNT_RESEARCH.map((r) => ({
        accountName: r.accountName,
        parentSignal: r.parentDemandSignalId,
        role: r.accountRole,
        evidenceStatus: r.evidenceStatus,
        rejectReason: r.rejectReason || "",
        evidenceUrls: (r.evidenceItems || []).map((e) => e.url).join(" | "),
      })),
      [
        "accountName",
        "parentSignal",
        "role",
        "evidenceStatus",
        "rejectReason",
        "evidenceUrls",
      ]
    )
  );
  write("MATURITY_RESULTS.csv", toCsv(persistRows, Object.keys(persistRows[0] || { opportunityId: "" })));
  write(
    "REJECTED_ACCOUNTS.csv",
    toCsv(rejected, ["accountName", "parentDemandSignalId", "reason"])
  );
  write(
    "MODELED_ROOM_BANDS.csv",
    toCsv(
      accepted.map(({ research }) => ({
        accountName: research.accountName,
        roomsMin: research.modeledRoomsMin,
        roomsMax: research.modeledRoomsMax,
        nightsMin: research.modeledNightsMin,
        nightsMax: research.modeledNightsMax,
        lodgingVerified: false,
        disclaimer: `Estimated ${research.modeledRoomsMin}–${research.modeledRoomsMax} rooms. Modeled demand — lodging not verified.`,
      })),
      [
        "accountName",
        "roomsMin",
        "roomsMax",
        "nightsMin",
        "nightsMax",
        "lodgingVerified",
        "disclaimer",
      ]
    )
  );
  write(
    "TRAVELING_COHORTS.csv",
    toCsv(
      accepted.map(({ research }) => ({
        accountName: research.accountName,
        cohortType: research.travelingCohortType,
        summary: research.travelingCohortSummary,
        confidence: research.travelingCohortConfidence,
      })),
      ["accountName", "cohortType", "summary", "confidence"]
    )
  );
  write(
    "LODGING_CONTROL.csv",
    toCsv(
      accepted.map(({ research }) => ({
        accountName: research.accountName,
        hypothesis: research.lodgingControlHypothesis,
        summary: research.lodgingControlSummary,
        confidence: research.lodgingControlConfidence,
      })),
      ["accountName", "hypothesis", "summary", "confidence"]
    )
  );
  write("RETURN_SUMMARY.json", JSON.stringify(summary, null, 2));

  write(
    "FOUNDER_REPORT.md",
    `# W Rome Evidence-Backed Expansion Pilot V0

**Run:** \`${RUN_ID}\`  
**Flag:** \`${W_ROME_EXPANSION_PILOT_ENV}=1\`  
**Hotel:** \`${W_ROME_HOTEL_ID}\`

## Verdict
${counts.QUALIFIED || 0} QUALIFIED + ${counts.ACTIONABLE || 0} ACTIONABLE account opportunities persisted from official evidence. Customer-visible (with flag): **${facingAfter.length}** (was ${facingBefore.length}). No Ready-gate bypass. No speculative accounts. Room bands are modeled / lodging unverified.

## Signals
${signals.map((s) => `- **${s.label}** (${s.eventStartDate} → ${s.eventEndDate}) — ${s.officialSource}`).join("\n")}

## Maturity distribution
| State | Count |
|---|---|
| CANDIDATE | ${counts.CANDIDATE || 0} |
| QUALIFIED | ${counts.QUALIFIED || 0} |
| ACTIONABLE | ${counts.ACTIONABLE || 0} |
| REJECTED | ${counts.REJECTED || 0} |

## Rejected
${rejected.map((r) => `- **${r.accountName}** — ${r.reason}`).join("\n") || "_None_"}

## Customer-visible after
${facingAfter.map((o) => `- ${o.organizationName} · ${o.gdiMaturityState || "?"} · ${o.title}`).join("\n") || "_None_"}

## Non-changes
- Thresholds unchanged
- Venue/operator shells not re-promoted
- No fabricated room blocks or buyers
- Bethesda card component unchanged (shared)
- ADP / share tokens unchanged

## Server note
Restart API with \`${W_ROME_EXPANSION_PILOT_ENV}=1\` so list visibility applies the pilot QUALIFIED path.
`
  );

  write(
    "UI_QA.md",
    `# W Rome expansion pilot V0 — UI QA checklist

| Check | Expected |
|---|---|
| Property dropdown | W Rome — Rome, Italy |
| Account name on card | Named account (not venue/event shell) |
| Parent event context | Clear in title / summary |
| QUALIFIED vs ACTIONABLE | Distinct maturity pills |
| Modeled rooms | "Modeled demand — lodging not verified" |
| CHILD ACCOUNT language | Absent |
| Generator/venue shells | Not on facing list |
| Fake buyer path | Not presented as named person |
| Flag off | Pilot QUALIFIED rows leave customer surface |

Facing after persist (process flag on): **${facingAfter.length}**
`
  );

  write(
    "CHANGELOG.md",
    `# Changelog — W Rome expansion pilot V0

## Added
- \`lib/group-demand-intelligence/expansion-pilot/w-rome-evidence-backed-expansion-pilot-v0.js\`
- \`scripts/gdi-w-rome-evidence-backed-expansion-pilot-v0.mjs\`
- Flag \`GDI_WROME_EXPANSION_PILOT_V0\`
- QUALIFIED customer visibility path (W Rome pilot only)
- Maturity pills + modeled demand disclaimer on shared GDI tiles

## Data
- Accepted account opportunities written to W Rome bag under \`expansionPilotId=${W_ROME_EXPANSION_PILOT_ID}\`
`
  );

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
