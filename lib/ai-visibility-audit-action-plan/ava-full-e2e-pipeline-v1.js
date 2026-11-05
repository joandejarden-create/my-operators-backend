/**
 * AVA Full E2E — canonical stage contract
 *
 * Evidence stages and Customer Quality Assurance are mandatory.
 * Cannot be skipped by callers of runMandatoryAvaEvidenceStages.
 */
import { runAvaFullE2eEvidenceStage } from "./autonomous-evidence-resolution-v1.js";
import { runCustomerQualityAssuranceLoop } from "./customer-quality-remediation-v1.js";
import {
  applyCanonicalActionRegisterToPayload,
  detectCanonicalActionIntegrityDefects,
} from "./canonical-action-register-v1.js";
import { detectIncompleteCustomerNarratives } from "./narrative-completeness-v1.js";
import { AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1 } from "./customer-report-quality-standard-v1.js";

export const AVA_FULL_E2E_PIPELINE_CONTRACT = "AVA_FULL_E2E_PIPELINE_V1";

export const AVA_FULL_E2E_STAGES = Object.freeze([
  { id: 1, name: "LOAD_CERTIFIED_ADP_BASELINE", mandatory: true },
  { id: 2, name: "RESEARCH_HOTEL_AND_COMPETITIVE_POSITIONING", mandatory: true },
  { id: 3, name: "DISCOVER_DIGITAL_SOURCES_AND_FACTS_REGISTER", mandatory: true },
  { id: 4, name: "GENERATE_EVIDENCE_BACKED_FINDINGS", mandatory: true },
  { id: 5, name: "TRAVELER_QUESTION_AND_PLATFORM_CONSISTENCY", mandatory: true },
  { id: 6, name: "IDENTIFY_RECOMMENDED_ACTIONS", mandatory: true },
  { id: 7, name: "DIGITAL_AUTHORITY_ANALYSIS", mandatory: false },
  { id: 8, name: "DIGITAL_CONTENT_READINESS_ANALYSIS", mandatory: true },
  { id: 9, name: "VERIFY_ALL_EVIDENCE_URLS", mandatory: true },
  { id: 10, name: "AUTONOMOUS_EVIDENCE_REMEDIATION", mandatory: true },
  { id: 11, name: "REVALIDATE_CORRECTED_CLAIMS", mandatory: true },
  { id: 11.5, name: "CANONICAL_ACTION_REGISTER_AND_NARRATIVE_GATE", mandatory: true },
  { id: 12, name: "GENERATE_STAGED_PDF", mandatory: true },
  { id: 13, name: "VALIDATE_PDF_HYPERLINKS", mandatory: true },
  { id: 14, name: "REPAIR_PDF_LINKS_AND_REGENERATE", mandatory: true },
  { id: 15, name: "CUSTOMER_QUALITY_ASSURANCE", mandatory: true },
  { id: 16, name: "FINAL_AUDIT_QUALITY_CERTIFICATION", mandatory: true },
  { id: 17, name: "STAGE_FOR_FOUNDER_REVIEW", mandatory: true },
  { id: 18, name: "EXPLICIT_FOUNDER_APPROVAL_BEFORE_PUBLICATION", mandatory: true },
]);

/**
 * Run mandatory evidence stages + customer-quality assurance.
 * Callers must not skip this before AUDIT_VALIDATED.
 */
export async function runMandatoryAvaEvidenceStages(payload, options = {}) {
  if (options.skipEvidenceStages === true) {
    const err = new Error("AVA_EVIDENCE_STAGES_CANNOT_BE_SKIPPED");
    err.code = "AVA_EVIDENCE_STAGES_CANNOT_BE_SKIPPED";
    throw err;
  }
  if (options.skipCustomerQualityAssurance === true) {
    const err = new Error("AVA_CUSTOMER_QUALITY_ASSURANCE_CANNOT_BE_SKIPPED");
    err.code = "AVA_CUSTOMER_QUALITY_ASSURANCE_CANNOT_BE_SKIPPED";
    throw err;
  }
  const result = await runAvaFullE2eEvidenceStage(payload, options);
  let working = result.payload;

  // Permanent Full E2E gate: canonicalize actions + narrative/integrity gates
  const narrativeDefects = detectIncompleteCustomerNarratives(working);
  const { payload: normalized, register } =
    applyCanonicalActionRegisterToPayload(working);
  const integrityDefects = detectCanonicalActionIntegrityDefects({
    ...normalized,
    customerActionRegister: register,
  });
  working = {
    ...normalized,
    customerReportQualityStandard: AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1,
    customerActionRegister: register,
    stage115IntegrityDefects: [...narrativeDefects, ...integrityDefects],
  };

  const cqa = await runCustomerQualityAssuranceLoop(working, {
    ...options,
    preCqaNarrativeDefects: narrativeDefects,
    regeneratePdfFn: options.generatePdfFn
      ? async (p) => {
          await options.generatePdfFn({ payload: p });
        }
      : options.regeneratePdfFn || null,
  });
  working = cqa.payload;
  const evidencePass = result.certification?.gate?.pass === true;
  const cqaPass = cqa.customerQualityCertified === true;
  return {
    contract: AVA_FULL_E2E_PIPELINE_CONTRACT,
    stages: AVA_FULL_E2E_STAGES.filter((s) => s.id >= 9 && s.id <= 15),
    result: {
      ...result,
      payload: working,
      customerQualityAssurance: cqa,
      eligibleForAuditValidated: evidencePass && cqaPass,
      eligibleForFounderReview: evidencePass && cqaPass,
    },
    customerQualityAssurance: cqa,
    nextAllowedStatus:
      evidencePass && cqaPass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    founderApprovalGranted: false,
    customerPublicationAuthorized: false,
  };
}
