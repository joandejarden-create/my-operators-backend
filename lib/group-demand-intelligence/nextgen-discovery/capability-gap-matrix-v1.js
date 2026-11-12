/**
 * Phase 0 capability gap matrix — exists / extend / genuinely new.
 */

import {
  SPECIALIST_HUNTER_LIST,
  SPECIALIST_HUNTER_META,
  HUNTER_CAPABILITY,
} from "./specialist-hunter-contract-v1.js";
import { APIFY_ACTOR_REGISTRY, listEnabledApifyActors } from "../research/apify/apify-actor-registry-v1.js";
import { buildFourteenWorkflowApifyCoverageMatrix } from "../research/apify/apify-fourteen-workflow-coverage-v1.js";
import { APIFY_V7_LESSONS_RETAINED } from "./hunter-provider-routing-v1.js";
import { NEED_DATE_RESEARCH_STRATEGY } from "../need-date-demand/need-date-demand-contract-v1.js";

export function buildCapabilityGapMatrixV1() {
  const coverage = buildFourteenWorkflowApifyCoverageMatrix();
  const fourteen = Object.values(NEED_DATE_RESEARCH_STRATEGY);

  const hunterRows = SPECIALIST_HUNTER_LIST.map((id) => {
    const m = SPECIALIST_HUNTER_META[id];
    return {
      hunterId: id,
      label: m.label,
      wave: m.wave,
      capability: m.capability,
      action:
        m.capability === HUNTER_CAPABILITY.EXISTS
          ? "REUSE_AND_WRAP"
          : m.capability === HUNTER_CAPABILITY.PARTIAL
            ? "EXTEND_EXISTING_WORKFLOW"
            : "NEW_SPECIALIST_PATH",
      mapsToWorkflows: m.workflowIds,
      mapsToResearchSegments: m.researchSegments,
      tenBases: m.tenBases,
      apifyRole: m.apifyRole,
      notes: m.notes,
      createsFifteenthSegment: false,
    };
  });

  return {
    schema: "gdi-nextgen-capability-gap-matrix-v1",
    generatedAt: new Date().toISOString(),
    fourteenDemandSegments: fourteen,
    fourteenCount: fourteen.length,
    apify: {
      registryVersion: "gdi-apify-actor-registry-v1",
      actorCount: APIFY_ACTOR_REGISTRY.length,
      enabledCount: listEnabledApifyActors().length,
      fourteenWorkflowCoverage: coverage.counts,
      unfinishedProductionWiring:
        "Apify Full E2E orchestration exists but production rail remains Exa-primary until hotel-adaptive yield proven",
      lessonsRetained: APIFY_V7_LESSONS_RETAINED,
    },
    existingCoverageMap: {
      corporate: ["scaling_companies", "corporate_meetings_incentives", "CORPORATE_TRIGGER_DEMAND"],
      association: ["association_events", "HISTORIC_ROTATION_PREDICTION"],
      procurement: ["government_public_sector"],
      project: ["projects_infrastructure", "PROJECT_WORKFORCE_DEMAND", "relocation_extended_stay"],
      sports: ["sports_entertainment", "SPORTS_ENTERTAINMENT_PRODUCTION"],
      university: ["education_scientific", "tours_educational_specialist"],
      medical: ["education_scientific", "medical_academic", "PHARMA_MEDICAL_ECOSYSTEM"],
    },
    hunterRows,
    summary: {
      exists: hunterRows.filter((r) => r.capability === HUNTER_CAPABILITY.EXISTS).length,
      partial: hunterRows.filter((r) => r.capability === HUNTER_CAPABILITY.PARTIAL)
        .length,
      new: hunterRows.filter((r) => r.capability === HUNTER_CAPABILITY.NEW).length,
    },
    nonGoals: [
      "No fifteenth demand segment",
      "No parallel GDI supervisor",
      "No Sales Ready from organization announcements alone",
      "No auto-publish",
      "No unauthorized paid Apify/Exa",
      "No duplicate Apify adapters",
    ],
  };
}
