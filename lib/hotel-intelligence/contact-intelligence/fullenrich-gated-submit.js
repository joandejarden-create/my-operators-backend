/**
 * Contact Intelligence → FullEnrich submit helper.
 * Always runs shared provider-submission gate before any paid call.
 */

import {
  startFullEnrichBulkWorkEmailsOnly,
  pollFullEnrichBulkUntilDone,
  getFullEnrichBulkResult,
} from "../../fullenrich/client.js";
import {
  validateProviderSubmissionInput,
  classifyInvalidInputEvaluation,
} from "./provider-submission-gate.js";

/**
 * @param {object[]} candidates — each with person, organization, identifiers, rejected_domains, etc.
 * @returns {{ allowed: object[], rejected: object[], invalid_input_evaluations: object[] }}
 */
export function gateProviderCandidates(candidates = [], { provider = "fullenrich" } = {}) {
  const allowed = [];
  const rejected = [];
  for (const c of candidates) {
    const gate = validateProviderSubmissionInput({
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
 * Reuses precomputed submit_row when gate already passed.
 */
export async function submitFullEnrichWorkEmailsGated({
  name,
  candidates,
  silentFail = true,
  webhook_url,
} = {}) {
  const preAllowed = [];
  const needGate = [];
  for (const c of candidates || []) {
    if (c?.submit_row && c?.gate?.ok) preAllowed.push(c);
    else needGate.push(c);
  }
  const { allowed, rejected } = gateProviderCandidates(needGate, { provider: "fullenrich" });
  const allAllowed = [...preAllowed, ...allowed];
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
  pollFullEnrichBulkUntilDone,
  getFullEnrichBulkResult,
};
