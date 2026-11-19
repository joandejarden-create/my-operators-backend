/**
 * HE 5.0 idempotent Airtable dry-run mutation package.
 * applyAuthorized: false — STOP before production writes.
 */

import { APPLY_GATE, HE50_CANONICAL_BASE_ID, HE50_HPC_TABLE_ID, HE50_PILOT_VERSION } from "./contract.js";
import { BETHESDA_FREE_FINDINGS, SEVEN_HOTEL_FREE_IDENTITY_PACKETS } from "./free-research-findings-v1.js";
import { summarizeReconciliation } from "./mutation-reconciliation-v1.js";
import { MULLEN_PILOT_HOTELS } from "../hpc-population/mullen-pilot-cohort-v1.js";

function mutation(partial) {
  return {
    applyAuthorized: false,
    baseId: HE50_CANONICAL_BASE_ID,
    conflictHandling: "PROTECT_NEWER_VERIFIED",
    duplicateCheck: "REQUIRED",
    researchDate: "2026-11-19",
    ...partial,
  };
}

export function buildHe50DryRunMutationPackage(opts = {}) {
  const mutations = [];

  // Bethesda HPC null-fills
  mutations.push(
    mutation({
      id: "M-BETH-001",
      destinationTable: HE50_HPC_TABLE_ID,
      destinationLabel: "Hotel Property Census",
      action: "UPDATE",
      canonicalEntityId: BETHESDA_FREE_FINDINGS.hpcId,
      field: "Year Built",
      existingValue: "UNKNOWN_OR_EMPTY",
      proposedValue: 1979,
      supportingSource: "Travel Weekly / CMBS / Cvent consensus",
      verificationStatus: "VERIFIED_FREE",
      reason: "Null-fill year built from convergent free authoritative sources",
    })
  );
  mutations.push(
    mutation({
      id: "M-BETH-002",
      destinationTable: "HI Graph Evidence",
      destinationLabel: "HI Graph Evidence",
      action: "CREATE",
      canonicalEntityId: BETHESDA_FREE_FINDINGS.hpcId,
      field: "renovation_evidence",
      existingValue: null,
      proposedValue: BETHESDA_FREE_FINDINGS.findings.find((f) => f.fieldKey === "lastRenovation")?.value,
      supportingSource: "Bethesda Magazine 2015 + EIN 2017 + Travel Weekly",
      verificationStatus: "SOURCE_CONFLICT_DOCUMENTED",
      reason: "Preserve dual renovation dates; do not overwrite with single fabricated year",
    })
  );
  mutations.push(
    mutation({
      id: "M-BETH-003",
      destinationTable: "HI Graph Evidence",
      destinationLabel: "HI Graph Evidence",
      action: "CREATE",
      canonicalEntityId: BETHESDA_FREE_FINDINGS.hpcId,
      field: "cmbs_financing_note",
      existingValue: null,
      proposedValue: BETHESDA_FREE_FINDINGS.findings.find((f) => f.fieldKey === "cmbsFinancingNote")?.value,
      supportingSource: "https://cmbs.news/property/bethesda-marriott-bethesda-md/",
      verificationStatus: "EVIDENCE_ONLY",
      reason: "Financing evidence without asserting current lender org edge",
    })
  );

  // Reuse prior Bethesda dry-runs (stakeholder / contacts) as referenced packets — not re-executed
  mutations.push(
    mutation({
      id: "M-BETH-REF-STAKEHOLDER",
      destinationTable: "HI Organizations + HI Hotel–Org Relationships",
      destinationLabel: "HI graph (prior dry-run)",
      action: "REFERENCE_PRIOR_DRY_RUN",
      canonicalEntityId: BETHESDA_FREE_FINDINGS.hpcId,
      field: "multiple",
      existingValue: "see HE-BETH-STAKEHOLDER-UPSERTS-DRY-RUN-V1.json",
      proposedValue: "unchanged package — Marbeth Legal Owner, MREC Economic Owner, MHM Asset Manager, Marriott Operator",
      supportingSource: "reports/hotel-explorer/phase4-quality/HE-BETH-STAKEHOLDER-UPSERTS-DRY-RUN-V1.json",
      verificationStatus: "PRIOR_STAGED",
      reason: "Reuse existing dry-run; do not duplicate CREATE plans",
    })
  );

  for (const p of SEVEN_HOTEL_FREE_IDENTITY_PACKETS) {
    if (p.proposedHpcAction === "CREATE_AFTER_FOUNDER_APPROVAL") {
      mutations.push(
        mutation({
          id: `M-NEW-${p.pilotKey}`,
          destinationTable: HE50_HPC_TABLE_ID,
          destinationLabel: "Hotel Property Census",
          action: "CREATE",
          canonicalEntityId: null,
          field: "minimum_identity",
          existingValue: null,
          proposedValue: {
            name: p.officialName,
            brand: p.brand,
            address: p.address,
            city: p.city,
            country: p.country,
            website: p.website,
            phone: p.phone || null,
            rooms: p.rooms?.value ?? null,
            roomsConfidence: p.rooms?.confidence,
            status: p.status,
            operator: p.operator || null,
            mullenAssociation: p.mullenAssociation,
          },
          supportingSource: (p.sources || []).join(" | "),
          verificationStatus: "MIN_IDENTITY_FREE_VERIFIED",
          reason: "NEW_HPC after collision search confirmed absent; create only after founder approval",
          duplicateCheck: "HPC_LIVE_COLLISION_ALREADY_RUN",
          founderRecommendation: "READY",
        })
      );
    } else if (p.proposedHpcAction === "STEWARD_PROMOTE_CENSUS_TO_HPC_NO_DUPLICATE") {
      mutations.push(
        mutation({
          id: `M-PROMOTE-${p.pilotKey}`,
          destinationTable: HE50_HPC_TABLE_ID,
          destinationLabel: "Hotel Property Census",
          action: "STEWARD_PROMOTE_FROM_CENSUS",
          canonicalEntityId: p.hotelCensusRecordId,
          field: "census_to_hpc_bridge",
          existingValue: { hotelCensusId: p.hotelCensusRecordId },
          proposedValue: {
            promoteToHpc: true,
            rooms: p.rooms?.value,
            website: p.website,
            doNotCreateDuplicate: true,
            preserveCensusId: true,
            mullenAssociation: "CENSUS_OWNER_WEBSITE_SIGNAL_ONLY",
          },
          supportingSource: (p.sources || []).join(" | "),
          verificationStatus: "CENSUS_MATCHED",
          reason: "Secrets Papagayo exists on Hotel Census — promote/link, never duplicate HPC",
          founderRecommendation: "READY",
        })
      );
    } else if (p.proposedHpcAction === "NO_OP_IDENTITY_VERIFIED_DISTINCT_FROM_DREAMS") {
      mutations.push(
        mutation({
          id: `M-UPD-${p.pilotKey}`,
          destinationTable: HE50_HPC_TABLE_ID,
          destinationLabel: "Hotel Property Census",
          action: "NO_OP",
          canonicalEntityId: p.hpcRecordId,
          field: "identity_confirmation_only",
          existingValue: {
            name: p.officialName,
            brand: p.brand,
            lat: p.latitude,
            lng: p.longitude,
            dreamsNeighborHpcId: p.dreamsNeighborHpcId,
          },
          proposedValue: null,
          supportingSource: (p.sources || []).join(" | "),
          verificationStatus: "IDENTITY_VERIFIED_NO_WRITE",
          reason: "Secrets Cap Cana HPC already correct and distinct from Dreams — REJECT field UPDATE",
          founderRecommendation: "REJECT",
        })
      );
    } else if (p.proposedHpcAction?.startsWith("UPDATE")) {
      mutations.push(
        mutation({
          id: `M-UPD-${p.pilotKey}`,
          destinationTable: HE50_HPC_TABLE_ID,
          destinationLabel: "Hotel Property Census",
          action: "UPDATE",
          canonicalEntityId: p.hpcRecordId,
          field: "alias_rooms_identity",
          existingValue: "MATCHED_HPC",
          proposedValue: {
            officialName: p.officialName,
            rooms: p.rooms?.value,
            aliases: p.aliases || [],
            website: p.website,
            // Explicitly omit mullenAssociation from graph-affecting payload
          },
          supportingSource: (p.sources || []).join(" | "),
          verificationStatus: "MATCHED_PLUS_FREE_CORROBORATION",
          reason: p.proposedHpcAction,
          founderRecommendation: "READY",
        })
      );
    }
  }

  const reconciliation = summarizeReconciliation();

  // Explicit non-mutations
  const blocked = [
    "No Surfe-derived contact persistence",
    "No automatic OWNED_BY edges for unverified Mullen holdings",
    "No Market Alerts article CREATE without URL dedupe pass (Bethesda currently absent)",
    "No Guest Reputation review-body scrape",
    "No production schema changes in this packet",
  ];

  return {
    contract: "HE_5_0_AIRTABLE_DRY_RUN_PACKAGE_V1",
    version: HE50_PILOT_VERSION,
    packageRevision: "founder-pre-apply-reconciliation-v1",
    generatedAt: opts.generatedAt || new Date().toISOString(),
    applyGate: {
      ...APPLY_GATE,
      stopMessage:
        "STOP — AWAITING SEPARATE FOUNDER AUTHORIZATION FOR PRODUCTION AIRTABLE APPLY.",
    },
    cohortSize: MULLEN_PILOT_HOTELS.length,
    mutationCount: mutations.length,
    mutations,
    reconciliationSummary: {
      READY: reconciliation.counts.READY,
      HOLD: reconciliation.counts.HOLD,
      REJECT: reconciliation.counts.REJECT,
      readyIds: reconciliation.readyIds,
      holdIds: reconciliation.holdIds,
      rejectIds: reconciliation.rejectIds,
    },
    blockedOperations: blocked,
    priorDryRunReferences: [
      "reports/hotel-explorer/hpc-population/HE-MULLEN-PILOT-AIRTABLE-DRY-RUN-PACKET-V1.md",
      "reports/hotel-explorer/phase4-quality/HE-BETH-STAKEHOLDER-UPSERTS-DRY-RUN-V1.json",
      "reports/hotel-explorer/phase4-quality/HE-BETH-PUBLIC-CONTACT-UPSERTS-DRY-RUN-V1.json",
      "reports/hotel-explorer/phase3-research/HE-BETH-PHASE3-HI-GRAPH-DRY-RUN-V1.json",
    ],
    persistenceClassification: "PERSISTENCE_DESIGNED",
    stopMessage:
      "STOP — AWAITING SEPARATE FOUNDER AUTHORIZATION FOR PRODUCTION AIRTABLE APPLY.",
  };
}
