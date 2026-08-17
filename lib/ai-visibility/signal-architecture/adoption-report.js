/**
 * Adoption report builder — formal lock of signal/flag architecture.
 */

import {
  SIGNAL_ARCHITECTURE_VERSION,
  SIGNAL_ARCHITECTURE_ADOPTION,
  PRODUCTION_SIGNALS,
  SIGNAL_DEFINITIONS,
  SIGNAL_PAYLOAD_FIELDS,
} from "./production-signals.js";
import {
  DEV_SIGNAL_VALIDATION_SNAPSHOT,
  assertReadinessIndependence,
} from "./readiness.js";
import { confirmMetricContractsUnchanged } from "./metric-contracts.js";
import { evaluateSignalPublicationPlan } from "./publication-gate.js";
import { summarizeProductSurfaceAudit } from "./product-surface-audit.js";
import { buildSignalValidationScorecard } from "./scorecard.js";
import { classifyOld10ClassGateRole } from "./internal-taxonomy.js";
import { listRecallWorkstreams } from "./recall-workstreams.js";
import { evaluateHoldoutReadiness } from "./holdout-strategy.js";

export function buildSignalArchitectureAdoptionReport() {
  const metrics = confirmMetricContractsUnchanged();
  const readinessIndep = assertReadinessIndependence();
  const publication = evaluateSignalPublicationPlan();
  const surfaces = summarizeProductSurfaceAudit();
  const scorecard = buildSignalValidationScorecard();
  const taxonomy = classifyOld10ClassGateRole();
  const workstreams = listRecallWorkstreams();
  const holdout = evaluateHoldoutReadiness();

  const presence = DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.PRESENCE;
  const recommended = DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.RECOMMENDED;
  const first = DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.FIRST_RECOMMENDATION;
  const negative = DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.NEGATIVE_OR_QUALIFIED;
  const comparator = DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.COMPARATOR;

  const architectureOk =
    SIGNAL_ARCHITECTURE_ADOPTION === "ADOPTED" &&
    metrics.confirmedUnchanged &&
    readinessIndep.ok &&
    taxonomy.PRODUCTION_CONTRACT === false &&
    taxonomy.INTERNAL_RESEARCH_ONLY === true;

  const nextStep = architectureOk
    ? "READY_FOR_PRESENCE_HOLDOUT_AND_PRODUCT_INTEGRATION"
    : "SIGNAL_ARCHITECTURE_REVIEW_REQUIRED";

  const finalStatus = architectureOk
    ? "AI_INTELLIGENCE_SIGNAL_ARCHITECTURE_ADOPTION_PASS"
    : "AI_INTELLIGENCE_SIGNAL_ARCHITECTURE_ADOPTION_REVIEW_REQUIRED";

  return {
    phase: "AI_INTELLIGENCE_SIGNAL_ARCHITECTURE_ADOPTION_COMPLETE",
    status: finalStatus,
    nextStep,
    architecture: {
      PRODUCTION_MODEL: "SIGNAL_AND_FLAG",
      SIGNALS: PRODUCTION_SIGNALS,
      SIGNAL_DEFINITIONS,
      SIGNAL_PAYLOAD_FIELDS,
      INTERNAL_10_CLASS_PRESERVED: "YES",
      COMPOSITE_SCORE: "NO",
      MUTUALLY_EXCLUSIVE_ROLE: "NO",
      version: SIGNAL_ARCHITECTURE_VERSION,
    },
    definitions: {
      PRESENCE: SIGNAL_DEFINITIONS.PRESENCE,
      RECOMMENDED: SIGNAL_DEFINITIONS.RECOMMENDED,
      FIRST_RECOMMENDATION: SIGNAL_DEFINITIONS.FIRST_RECOMMENDATION,
      NEGATIVE_OR_QUALIFIED: SIGNAL_DEFINITIONS.NEGATIVE_OR_QUALIFIED,
      COMPARATOR: SIGNAL_DEFINITIONS.COMPARATOR,
    },
    DEV_readiness: {
      PRESENCE: {
        P: presence.precision,
        R: presence.recall,
        F1: presence.f1,
        STATUS: presence.readiness,
        GATE: presence.gateStatus,
      },
      RECOMMENDED: {
        P: recommended.precision,
        R: recommended.recall,
        F1: recommended.f1,
        STATUS: recommended.readiness,
      },
      FIRST_RECOMMENDATION: {
        P: first.precision,
        R: first.recall,
        F1: first.f1,
        STATUS: first.readiness,
      },
      NEGATIVE: {
        P: negative.precision,
        R: negative.recall,
        F1: negative.f1,
        STATUS: negative.readiness,
        sparse: true,
      },
      COMPARATOR: {
        P: comparator.precision,
        R: comparator.recall,
        F1: comparator.f1,
        STATUS: comparator.readiness,
        sparse: true,
      },
    },
    productSurfaceAudit: surfaces,
    metricContracts: {
      ...metrics.contracts,
      confirmedUnchanged: metrics.confirmedUnchanged,
    },
    holdout,
    validationScorecard: scorecard,
    oldTaxonomy: taxonomy,
    publication,
    readinessIndependence: readinessIndep,
    recallWorkstreams: workstreams,
    productionGates: {
      PRESENCE_GATE: presence.gateStatus,
      RECOMMENDED_GATE: recommended.gateStatus,
      FIRST_REC_GATE: first.gateStatus,
      NEGATIVE_GATE: negative.gateStatus,
      COMPARATOR_GATE: comparator.gateStatus,
      COMPOSITE: "NONE",
      OLD_10_CLASS: "INTERNAL_RESEARCH_VALIDATION",
    },
    hardGuards: {
      LIVE_PROVIDER_CALLS: 0,
      NEW_MONITORING: 0,
      HOLDOUT_ACCESS: 0,
      AUTO_GT_CHANGES: 0,
      AIRTABLE_WRITES: 0,
      SCHEMA_CHANGES: 0,
      DEPLOYS: 0,
    },
  };
}
