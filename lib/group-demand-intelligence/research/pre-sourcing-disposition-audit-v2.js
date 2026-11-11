/**
 * Offline audit: replay 22-candidate three-hotel cohort under separated
 * demand-evidence dimensions + proposed pre-sourcing disposition.
 * No paid research. No publication / Airtable / deploy changes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runCommercialYieldRecoveryOffline,
  BENCHMARK_CERT,
} from "./commercial-yield-recovery-v2.js";
import {
  assessDemandEvidenceDimensions,
  proposePreSourcingDisposition,
  DOWNGRADE_CAUSE,
  RECOMMENDED_RULE_CHANGES_FOR_APPROVAL,
  DEMAND_EVIDENCE_DIMENSIONS_VERSION,
} from "./demand-evidence-dimensions-v2.js";
import { qualifyHotelSpecificProspectV2 } from "./hotel-specific-qualification-v2.js";

export const PRE_SOURCING_AUDIT_VERSION = "gdi-pre-sourcing-disposition-audit-v2";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
const OUT_DIR = path.join(REPO_ROOT, "reports", "gdi-pre-sourcing-disposition-audit-v2");

const HOTEL_META = Object.freeze({
  recC2kbCaS48jFjFF: {
    hotelName: "Nobu Hotel London Portman Square",
    oppPath: "data/group-demand-intelligence/hotels/recC2kbCaS48jFjFF/opportunities.json",
  },
  recLuxvwwxID7U2B8: {
    hotelName: "Bethesda Marriott",
    oppPath: "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json",
  },
  recnPB3Q02NO8Ma1o: {
    hotelName: "Wyndham San Diego Bayside",
    oppPath: "data/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities.json",
  },
});

function readJson(rel) {
  const p = path.isAbsolute(rel) ? rel : path.join(REPO_ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function loadCorpusOpp(hotelId, organizationName) {
  const meta = HOTEL_META[hotelId];
  const ops = readJson(meta?.oppPath)?.opportunities || [];
  return ops.find((o) => String(o.organizationName || "").trim() === organizationName)
    || ops.find((o) => String(o.organizationName || "").toLowerCase().includes(
      organizationName.toLowerCase().split(/\s+/).slice(0, 2).join(" ").toLowerCase(),
    ))
    || null;
}

function claimsFromYieldRow(row, opp) {
  const claims = [];
  // Prefer yield-recovery supporting statements via afterRecovery next action context
  if (opp?.summaryWhyHotel) {
    claims.push({
      statement: opp.summaryWhyHotel,
      url: opp.officialSource || null,
      provenance: "frozen_corpus",
      accepted: true,
    });
  }
  if (opp?.evidenceQuote) {
    claims.push({
      statement: opp.evidenceQuote,
      url: opp.officialSource || null,
      provenance: "frozen_corpus_quote",
      accepted: true,
    });
  }
  for (const bit of [opp?.eventLocationSummary, opp?.namedHostHotel, opp?.hostHotel].filter(Boolean)) {
    claims.push({ statement: String(bit), url: opp?.officialSource || null, provenance: "frozen_corpus_location", accepted: true });
  }
  // Dossier-backed mechanism from yield row
  if (row.verifiedDemandMechanism === "GUESTROOM_OR_HOTEL_SOURCING_CONTROL") {
    claims.push({
      statement: "hotel sourcing and accommodation services for incentive programmes",
      url: row.leadFollowOutcomes?.[0]?.url || opp?.officialSource || null,
      provenance: "yield_recovery_retained_hotel_sourcing",
      accepted: true,
    });
  }
  return claims;
}

function mapPrimaryCause(row, proposal) {
  if (proposal.primaryDowngradeCause) return proposal.primaryDowngradeCause;
  if (row.failureMode === "GENUINE_VENUE_LISTING_EXCLUSION") {
    return DOWNGRADE_CAUSE.VENUE_LISTING_OR_LOCAL_ONLY;
  }
  if (row.failureMode === "GEOGRAPHY_MISMATCH_OR_WEAK_FIT") {
    return DOWNGRADE_CAUSE.MISSING_HOTEL_SPECIFIC_FIT;
  }
  if (row.failureMode === "GENUINE_OR_EXTRACTED_CONTACT_GAP") {
    return DOWNGRADE_CAUSE.MISSING_CONTACT_ROUTE;
  }
  if (row.afterRecovery?.commerciallyUsefulInternal) {
    return DOWNGRADE_CAUSE.ALREADY_USEFUL_OR_RECOVERED;
  }
  if (row.competitorHost) return DOWNGRADE_CAUSE.COMPETITOR_HOTEL_COMMITMENT;
  return DOWNGRADE_CAUSE.MISSING_EXPLICIT_GUESTROOM_DESPITE_CREDIBLE_DEMAND;
}

export function runPreSourcingDispositionAuditOffline() {
  const recovery = runCommercialYieldRecoveryOffline();
  const rows = recovery.candidateRootCauseTable || [];
  const auditRows = [];

  for (const row of rows) {
    const opp = loadCorpusOpp(row.hotelId, row.organizationName) || {
      organizationName: row.organizationName,
      contactEmail: row.contactRoute,
      namedHostHotel: row.competitorHost,
    };
    const claims = claimsFromYieldRow(row, opp);
    const pageTexts = claims.map((c) => c.statement);
    const contact = row.contactRoute || opp.contactEmail || null;

    const production = qualifyHotelSpecificProspectV2({
      opportunity: {
        ...opp,
        organizationName: row.organizationName,
        contactEmail: contact,
        contact: contact ? { businessEmail: contact } : {},
        namedHostHotel: row.competitorHost || opp.namedHostHotel,
        targetHotel: row.hotelName,
      },
      acceptedClaims: claims,
      pageTexts,
      targetHotel: row.hotelName,
      usableContactRoute: contact,
    });

    const proposal = proposePreSourcingDisposition({
      opportunity: {
        ...opp,
        organizationName: row.organizationName,
        contactEmail: contact,
        namedHostHotel: row.competitorHost || opp.namedHostHotel,
        futureCycleSupported: row.afterRecovery?.pathway === "COMPETITIVE_FUTURE_CYCLE",
      },
      acceptedClaims: claims,
      pageTexts,
      targetHotel: row.hotelName,
      usableContactRoute: contact,
      productionQualification: production,
    });

    // Align competitor future-cycle already recovered
    if (row.afterRecovery?.commerciallyUsefulInternal && !proposal.proposed.commerciallyUsefulInternal) {
      proposal.proposed.commerciallyUsefulInternal = true;
      proposal.proposed.usefulKind = row.afterRecovery.usefulKind;
      proposal.proposed.pathway = row.afterRecovery.pathway;
      proposal.changedToUsefulUnderProposedRules = false;
      proposal.primaryDowngradeCause = DOWNGRADE_CAUSE.ALREADY_USEFUL_OR_RECOVERED;
    }

    const dims = proposal.dimensions;
    auditRows.push({
      organizationName: row.organizationName,
      hotelId: row.hotelId,
      hotelName: row.hotelName,
      liveFailureMode: row.failureMode,
      yieldRecoveryUseful: row.afterRecovery?.commerciallyUsefulInternal === true,
      productionUseful: production.commerciallyUsefulInternal === true,
      proposedUseful: proposal.proposed.commerciallyUsefulInternal === true,
      changedToUsefulUnderProposedRules: proposal.changedToUsefulUnderProposedRules === true,
      primaryDowngradeCause: mapPrimaryCause(row, proposal),
      changeReason: proposal.changeReason,
      dimensions: {
        verifiedGroupActivity: dims.layers.VERIFIED_GROUP_ACTIVITY.present,
        inferredLodgingPotential: dims.layers.INFERRED_LODGING_POTENTIAL.present,
        inferredLodgingReason: dims.layers.INFERRED_LODGING_POTENTIAL.reason,
        verifiedAccommodationDemand: dims.layers.VERIFIED_ACCOMMODATION_DEMAND.present,
        venueFinderOrListing: dims.venueFinderOrListing,
        localOrDayOnly: dims.localOrDayOnly,
      },
      sourcingStatusProposed: proposal.sourcingStatus,
      contactRoute: contact,
      competitorHost: proposal.competitorHost || row.competitorHost,
      supportingSourceEvidence: dims.supportingExcerpts,
      productionPathway: production.pathway,
      proposedPathway: proposal.proposed.pathway,
      proposedKind: proposal.proposed.usefulKind,
      proposedNextAction: proposal.proposed.nextSalesAction,
      falsePositiveRiskFlags: proposal.falsePositiveRiskFlags,
      publicationEligibilityChanged: false,
      customerFacingClassificationChanged: false,
    });
  }

  const beforeUseful = auditRows.filter((r) => r.yieldRecoveryUseful).length;
  const afterProposed = auditRows.filter((r) => r.proposedUseful).length;
  const newlyUseful = auditRows.filter((r) => r.changedToUsefulUnderProposedRules);

  const causeCounts = {};
  for (const r of auditRows) {
    causeCounts[r.primaryDowngradeCause] = (causeCounts[r.primaryDowngradeCause] || 0) + 1;
  }

  const overReliance = {
    thesis: "Current V2 internal useful largely requires VERIFIED_ACCOMMODATION_DEMAND (guestroom/overnight/sourcing language), which under-discovers pre-hotel-selection demand.",
    candidatesBlockedPrimarilyByMissingExplicitGuestroom: auditRows.filter((r) =>
      r.primaryDowngradeCause === DOWNGRADE_CAUSE.MISSING_EXPLICIT_GUESTROOM_DESPITE_CREDIBLE_DEMAND
      || (r.dimensions.verifiedGroupActivity && !r.dimensions.verifiedAccommodationDemand && !r.proposedUseful && r.contactRoute),
    ).length,
    retainedExclusions: {
      venueOrLocal: auditRows.filter((r) => r.primaryDowngradeCause === DOWNGRADE_CAUSE.VENUE_LISTING_OR_LOCAL_ONLY).length,
      geography: auditRows.filter((r) => r.primaryDowngradeCause === DOWNGRADE_CAUSE.MISSING_HOTEL_SPECIFIC_FIT).length,
      contact: auditRows.filter((r) => r.primaryDowngradeCause === DOWNGRADE_CAUSE.MISSING_CONTACT_ROUTE).length,
      competitor: auditRows.filter((r) => r.primaryDowngradeCause === DOWNGRADE_CAUSE.COMPETITOR_HOTEL_COMMITMENT).length,
    },
  };

  const falsePositiveSummary = newlyUseful.flatMap((r) =>
    (r.falsePositiveRiskFlags || []).map((f) => ({
      organizationName: r.organizationName,
      hotelName: r.hotelName,
      ...f,
    })),
  );

  const packageOut = {
    version: PRE_SOURCING_AUDIT_VERSION,
    dimensionsVersion: DEMAND_EVIDENCE_DIMENSIONS_VERSION,
    offline: true,
    spent: false,
    liveCalls: 0,
    candidateCount: auditRows.length,
    beforeAfter: {
      yieldRecoveryUsefulInternal: beforeUseful,
      proposedPreSourcingUsefulInternal: afterProposed,
      newlyUsefulUnderProposedRules: newlyUseful.length,
      newlyUsefulNames: newlyUseful.map((r) => `${r.organizationName} @ ${r.hotelName}`),
      unchangedUseful: auditRows.filter((r) => r.yieldRecoveryUseful && r.proposedUseful).map((r) => r.organizationName),
    },
    overRelianceOnExplicitGuestroomEvidence: overReliance,
    downgradeCauseCounts: causeCounts,
    candidateAuditTable: auditRows,
    opportunitiesThatChange: newlyUseful.map((r) => ({
      organizationName: r.organizationName,
      hotelName: r.hotelName,
      why: r.changeReason,
      dimensions: r.dimensions,
      sourcingStatus: r.sourcingStatusProposed,
      supportingSourceEvidence: r.supportingSourceEvidence,
      falsePositiveRiskFlags: r.falsePositiveRiskFlags,
      proposedNextAction: r.proposedNextAction,
    })),
    falsePositiveRisksIfAdopted: falsePositiveSummary,
    recommendedRuleChangesForApproval: RECOMMENDED_RULE_CHANGES_FOR_APPROVAL,
    controls: {
      noPaidResearch: true,
      noPublicationGateChange: true,
      noCustomerFacingClassificationChange: true,
      noAirtable: true,
      noDeploy: true,
      sourcingStatusUnknownUnlessVerifiedAccommodation: true,
      rulesNotAutoAppliedToProduction: true,
    },
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(path.join(OUT_DIR, "AUDIT.json"), `${JSON.stringify(packageOut, null, 2)}\n`);
  fs.writeFileSync(
    path.join(OUT_DIR, "CANDIDATE_DEMAND_DIMENSIONS.json"),
    `${JSON.stringify(auditRows, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "BEFORE_AFTER.json"),
    `${JSON.stringify(packageOut.beforeAfter, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "RECOMMENDED_RULE_CHANGES.json"),
    `${JSON.stringify(RECOMMENDED_RULE_CHANGES_FOR_APPROVAL, null, 2)}\n`,
  );

  const mdLines = [
    "# Pre-sourcing disposition audit V2",
    "",
    `**Spent:** $0 · **Candidates:** ${auditRows.length}`,
    `**Yield-recovery useful:** ${beforeUseful} → **Proposed pre-sourcing useful:** ${afterProposed} (Δ +${newlyUseful.length})`,
    "",
    "## Over-reliance finding",
    overReliance.thesis,
    "",
    "## Downgrade cause counts",
    ...Object.entries(causeCounts).map(([k, v]) => `- ${k}: ${v}`),
    "",
    "## Newly useful under proposed rules",
    ...(newlyUseful.length
      ? newlyUseful.map((r) => `- **${r.organizationName}** @ ${r.hotelName} — ${r.changeReason}`)
      : ["- (none)"]),
    "",
    "## Candidate table",
    "",
    "| Organization | Hotel | Group | Inferred lodging | Verified rooms | Cause | Yield useful | Proposed useful |",
    "|---|---|---|---|---|---|---|---|",
    ...auditRows.map((r) => `| ${r.organizationName.slice(0, 42)} | ${r.hotelName.replace(" Hotel", "").slice(0, 18)} | ${r.dimensions.verifiedGroupActivity ? "Y" : "n"} | ${r.dimensions.inferredLodgingPotential ? "Y" : "n"} | ${r.dimensions.verifiedAccommodationDemand ? "Y" : "n"} | ${String(r.primaryDowngradeCause).slice(0, 36)} | ${r.yieldRecoveryUseful ? "Y" : "n"} | ${r.proposedUseful ? "Y" : "n"} |`),
    "",
    "## Recommended rule changes",
    `Status: **${RECOMMENDED_RULE_CHANGES_FOR_APPROVAL.status}**`,
    ...RECOMMENDED_RULE_CHANGES_FOR_APPROVAL.rules.map((r) => `- \`${r.id}\`: ${r.change}`),
    "",
    "Publication gate, customer classifications, Airtable, and deploy: **unchanged**.",
    "",
  ];
  fs.writeFileSync(path.join(OUT_DIR, "AUDIT.md"), mdLines.join("\n"));

  return packageOut;
}
