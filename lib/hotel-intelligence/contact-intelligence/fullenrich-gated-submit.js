/**
 * Contact Intelligence → FullEnrich submit helper.
 * Always runs shared provider-submission gate before any paid call.
 */

import {
  validateProviderSubmissionInput,
  classifyInvalidInputEvaluation,
} from "./provider-submission-gate.js";
import {
  OWNER_PERSON_ENRICHMENT_WORKFLOW,
  validateOwnerPersonEnrichmentSubmission,
} from "./owner-person-enrichment-gate.js";

/**
 * @param {object[]} candidates — each with person, organization, identifiers, rejected_domains, etc.
 * @param {{ provider?: string, workflow?: string }} [opts]
 *   workflow=owner_person_enrichment → requires hotel→owner + corroborated affiliation
 *   (people *search* must not pass this workflow).
 * @returns {{ allowed: object[], rejected: object[], invalid_input_evaluations: object[] }}
 */
export function gateProviderCandidates(
  candidates = [],
  { provider = "fullenrich", workflow = null } = {}
) {
  const allowed = [];
  const rejected = [];
  for (const c of candidates) {
    // Omission defaults to the owner gate. Non-owner callers must declare their scope.
    // A row cannot downgrade an owner workflow selected by its caller.
    const ownerContext = c.hotel_to_owner || c.hotelToOwner || c.owner_entity_id || c.affiliation_corroboration;
    const wf = workflow === OWNER_PERSON_ENRICHMENT_WORKFLOW || c.workflow === OWNER_PERSON_ENRICHMENT_WORKFLOW || ownerContext
      ? OWNER_PERSON_ENRICHMENT_WORKFLOW : (workflow || c.workflow || OWNER_PERSON_ENRICHMENT_WORKFLOW);
    const gate =
      !["hotel_contact", "gdi_contact", "owner_person_search"].includes(wf)
        ? validateOwnerPersonEnrichmentSubmission({
            ...c,
            provider,
            allow_linkedin_only: provider === "fullenrich",
          })
        : validateProviderSubmissionInput({
            ...c,
            provider,
            allow_linkedin_only: provider === "fullenrich",
          });
    if (!gate.ok) {
      rejected.push({
        ...c,
        gate,
        classification: "REJECTED_PRE_SUBMISSION",
        exclude_from_provider_coverage_metrics: true,
      });
      continue;
    }
    allowed.push({
      ...c,
      gate,
      submit_row: {
        first_name: gate.sanitized_identifiers.first_name,
        last_name: gate.sanitized_identifiers.last_name,
        domain: gate.sanitized_identifiers.domain || undefined,
        company_name: gate.sanitized_identifiers.company_name || undefined,
        linkedin_url: gate.sanitized_identifiers.linkedin_url || undefined,
        custom: c.custom || { subject_id: c.subject_id || c.id || "" },
      },
    });
  }
  return { allowed, rejected };
}

/**
 * Submit work-email-only FullEnrich bulk after gating.
 * Never submits rejected-domain / wrong-person rows.
 * Revalidates original evidence and rebuilds identifiers on every submission.
 */
export async function submitFullEnrichWorkEmailsGated({
  name,
  candidates,
  silentFail = true,
  webhook_url,
} = {}) {
  // Never trust cached gate.ok or submit_row: identifiers/evidence may have changed.
  const { allowed: allAllowed, rejected } = gateProviderCandidates(candidates, { provider: "fullenrich", workflow: OWNER_PERSON_ENRICHMENT_WORKFLOW });
  if (!allAllowed.length) {
    return {
      submitted: false,
      reason: "NO_GATED_CANDIDATES",
      allowed: allAllowed,
      rejected,
      enrichment_id: null,
      http_status: null,
      payload: null,
    };
  }
  const { startFullEnrichBulkWorkEmailsOnly } = await import("../../fullenrich/client.js");
  const submit = await startFullEnrichBulkWorkEmailsOnly({
    name,
    data: allAllowed.map((a) => a.submit_row),
    silentFail,
    webhook_url,
  });
  return {
    submitted: Boolean(submit.payload?.enrichment_id),
    reason: submit.payload?.enrichment_id ? "SUBMITTED" : "REQUEST_ERROR",
    allowed: allAllowed,
    rejected,
    enrichment_id: submit.payload?.enrichment_id || null,
    http_status: submit.http_status,
    payload: submit.payload,
  };
}

export {
  validateProviderSubmissionInput,
  classifyInvalidInputEvaluation,
};

// Lazy imports keep offline validation independent of provider credentials/dependencies.
export async function pollFullEnrichBulkUntilDone(...args) {
  return (await import("../../fullenrich/client.js")).pollFullEnrichBulkUntilDone(...args);
}
export async function getFullEnrichBulkResult(...args) {
  return (await import("../../fullenrich/client.js")).getFullEnrichBulkResult(...args);
}
