/**
 * Shared ORS / commercial-qualification handoff.
 * Specialist hunters deposit evidence here — never auto Sales Ready / auto-publish.
 */

import { HUNTER_HYPOTHESIS_STATUS } from "../specialist-hunter-contract-v1.js";
import {
  DISCOVERY_EVIDENCE_CLASS,
  isSyntheticOrFixtureEvidenceClass,
  resolveDiscoveryEvidenceClass,
} from "./discovery-evidence-class-v1.js";

export const QUALIFICATION_HANDOFF_SCHEMA = "gdi-nextgen-qualification-handoff-v1";

/**
 * Build a handoff packet for the existing ORS / qualification process.
 * Synthetic fixtures are retained for test reporting but marked ineligible.
 */
export function buildQualificationHandoffPacket({
  hunterId,
  hotelId,
  hypotheses = [],
  graph = null,
  discoveryEvidenceClass = null,
  dryRun = true,
} = {}) {
  const packets = [];
  for (const hyp of hypotheses) {
    const cls = resolveDiscoveryEvidenceClass(
      hyp,
      discoveryEvidenceClass || DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE
    );
    const synthetic = isSyntheticOrFixtureEvidenceClass(cls);
    const readyForOrs =
      !synthetic &&
      (hyp.hypothesisStatus === HUNTER_HYPOTHESIS_STATUS.READY_FOR_ORS ||
        hyp.hypothesisStatus === HUNTER_HYPOTHESIS_STATUS.DEMAND_TRIGGER_INFERRED ||
        hyp.hypothesisStatus === HUNTER_HYPOTHESIS_STATUS.HOTEL_THESIS_FORMED ||
        hyp.hypothesisStatus === HUNTER_HYPOTHESIS_STATUS.RESEARCH_CONTINUATION);

    packets.push({
      schema: QUALIFICATION_HANDOFF_SCHEMA,
      hunterId,
      hotelId,
      hypothesisId: hyp.id || null,
      organizationName: hyp.organizationName || null,
      activityTitle: hyp.activityTitle || null,
      discoveryEvidenceClass: cls,
      customerInventoryEligible: !synthetic && hyp.customerInventoryEligible !== false,
      publicationEligible: false,
      autoSalesReady: false,
      salesReady: false,
      routeToOrs: readyForOrs,
      orsStatus: readyForOrs
        ? "QUEUED_FOR_QUALIFICATION"
        : synthetic
          ? "FIXTURE_EXCLUDED_FROM_ORS"
          : "RESEARCH_STAGE",
      dryRun,
      uncertaintyPreserved: true,
      graphNodeCount: graph?.stats
        ? Object.values(graph.stats).reduce((a, b) => a + Number(b || 0), 0)
        : 0,
      notSalesReadyReason:
        hyp.notSalesReadyReason ||
        "Hunter output is evidence for ORS/qualification — never auto Sales Ready",
    });
  }

  return {
    schema: QUALIFICATION_HANDOFF_SCHEMA,
    hunterId,
    hotelId,
    dryRun,
    createdAt: new Date().toISOString(),
    packets,
    summary: {
      total: packets.length,
      routedToOrs: packets.filter((p) => p.routeToOrs).length,
      fixturesExcluded: packets.filter(
        (p) => p.orsStatus === "FIXTURE_EXCLUDED_FROM_ORS"
      ).length,
      researchStage: packets.filter((p) => p.orsStatus === "RESEARCH_STAGE").length,
      salesReady: 0,
      autoPublished: 0,
    },
  };
}
