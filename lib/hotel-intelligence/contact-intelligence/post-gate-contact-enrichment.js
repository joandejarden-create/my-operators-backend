/**
 * Post owner-person-gate contact enrichment provider selection.
 *
 * Production default remains Surfe until comparison review.
 * PDL is an approved evaluation / optional contact provider — never owner discovery.
 *
 * contact_provider: "surfe" | "pdl" | "comparison"
 * Env: CONTACT_INTELLIGENCE_CONTACT_PROVIDER
 *
 * Does not write canonical Contact Intelligence, Census, or Airtable.
 * Does not publish to customers from this module.
 */

import {
  enrichPdlPerson,
  buildPdlPersonEnrichParams,
  isPdlConfigured,
} from "../../pdl/client.js";
import {
  CONTACT_PROVIDER,
  DEFAULT_CONTACT_PROVIDER,
  resolveContactProvider,
  scorePdlEnrichmentResult,
} from "./pdl-contact-enrichment-eval.js";
import {
  OWNER_PERSON_ENRICHMENT_WORKFLOW,
  validateOwnerPersonEnrichmentSubmission,
} from "./owner-person-enrichment-gate.js";

export const POST_GATE_CONTACT_ENRICHMENT_VERSION = "post-gate-contact-enrichment-v1";

export {
  CONTACT_PROVIDER,
  DEFAULT_CONTACT_PROVIDER,
  resolveContactProvider,
};

/**
 * Build PDL input from gate-sanitized identifiers. Never includes email/phone.
 */
export function buildPostGatePdlInput(gate, person = {}) {
  const ids = gate?.sanitized_identifiers || {};
  const input = {
    id: person.id || person.subject_id || null,
    first_name: ids.first_name || person.first_name,
    last_name: ids.last_name || person.last_name,
    company_name: ids.company_name || person.organization || person.company_name,
    company_domain: ids.domain || person.domain || person.company_domain,
  };
  if (ids.linkedin_url) input.linkedin_url = ids.linkedin_url;
  else if (person.linkedin_url && person.linkedin_evidenced !== false) {
    input.linkedin_url = person.linkedin_url;
  }
  delete input.email;
  delete input.work_email;
  delete input.phone;
  return input;
}

/**
 * Run post-gate contact enrichment with provider selection.
 * Callers must supply a Surfe enrich fn for surfe/comparison arms (keeps Surfe scripts authoritative).
 *
 * @param {object} opts
 * @param {object} opts.candidate — owner-person gate candidate
 * @param {"surfe"|"pdl"|"comparison"} [opts.contact_provider]
 * @param {Function} [opts.surfeEnrichFn] — async (sanitized) => surfe result (required for surfe/comparison)
 * @param {Function} [opts.pdlEnrichFn] — async override for tests
 * @param {object} [opts.subject] — scoring subject (domain, known_email_for_scoring, …)
 * @param {boolean} [opts.require_gate=true]
 */
export async function enrichOwnerPersonContactAfterGate(opts = {}) {
  const provider = resolveContactProvider(opts);
  const candidate = opts.candidate || {};
  const requireGate = opts.require_gate !== false;

  let gate = opts.gate || null;
  if (!gate && requireGate) {
    gate = validateOwnerPersonEnrichmentSubmission({
      ...candidate,
      provider: provider === CONTACT_PROVIDER.PDL ? "pdl" : "surfe",
      workflow: OWNER_PERSON_ENRICHMENT_WORKFLOW,
    });
  }

  if (requireGate && gate && !gate.ok) {
    return {
      version: POST_GATE_CONTACT_ENRICHMENT_VERSION,
      contact_provider: provider,
      status: "REJECTED_PRE_SUBMISSION",
      gate,
      surfe: null,
      pdl: null,
      canonical_writes: false,
      customer_publication: "BLOCKED",
    };
  }

  const out = {
    version: POST_GATE_CONTACT_ENRICHMENT_VERSION,
    contact_provider: provider,
    status: "OK",
    gate,
    surfe: null,
    pdl: null,
    canonical_writes: false,
    customer_publication: "BLOCKED",
  };

  const runSurfe = provider === CONTACT_PROVIDER.SURFE || provider === CONTACT_PROVIDER.COMPARISON;
  const runPdl = provider === CONTACT_PROVIDER.PDL || provider === CONTACT_PROVIDER.COMPARISON;

  if (runSurfe) {
    if (typeof opts.surfeEnrichFn !== "function") {
      out.surfe = {
        status: "SURFE_FN_REQUIRED",
        note: "Pass surfeEnrichFn — production Surfe path unchanged; default provider is surfe.",
      };
      if (provider === CONTACT_PROVIDER.SURFE) {
        out.status = "SURFE_FN_REQUIRED";
        return out;
      }
    } else {
      const sanitized = gate?.sanitized_identifiers || {};
      out.surfe = await opts.surfeEnrichFn({
        first_name: sanitized.first_name || candidate.first_name,
        last_name: sanitized.last_name || candidate.last_name,
        domain: sanitized.domain || candidate.domain,
        company_name: sanitized.company_name || candidate.organization,
        linkedin_url: sanitized.linkedin_url || candidate.linkedin_url,
      });
    }
  }

  if (runPdl) {
    const pdlInput = buildPostGatePdlInput(gate, {
      ...candidate,
      ...(opts.subject || {}),
    });
    if (pdlInput.email || pdlInput.phone) {
      throw new Error("PDL_INPUT_MUST_NOT_INCLUDE_EMAIL_OR_PHONE");
    }
    // Params throw if insufficient / forbidden fields
    buildPdlPersonEnrichParams(pdlInput);

    const enrichFn =
      typeof opts.pdlEnrichFn === "function"
        ? opts.pdlEnrichFn
        : async (person) => {
            if (!isPdlConfigured()) {
              return {
                http_status: 0,
                ok: false,
                latency_ms: 0,
                request_params_sanitized: buildPdlPersonEnrichParams(person),
                normalized: {
                  matched: false,
                  status: 0,
                  error: "PDL_NOT_CONFIGURED",
                  emails: [],
                  phones: [],
                },
              };
            }
            return enrichPdlPerson(person, { retries: 0, min_likelihood: 6 });
          };

    const res = await enrichFn(pdlInput);
    const score = scorePdlEnrichmentResult({
      subject: opts.subject || {
        id: candidate.subject_id || candidate.id,
        domain: pdlInput.company_domain,
        full_name: candidate.person?.full_name || candidate.full_name,
        first_name: pdlInput.first_name,
        last_name: pdlInput.last_name,
        known_email_for_scoring: opts.known_email_for_scoring || null,
      },
      normalized: res.normalized,
      http_status: res.http_status,
      latency_ms: res.latency_ms,
      cost_credits: res.normalized?.matched ? 1 : 0,
    });
    out.pdl = {
      request_params_sanitized: res.request_params_sanitized || buildPdlPersonEnrichParams(pdlInput),
      http_status: res.http_status,
      latency_ms: res.latency_ms,
      score,
      // Minimum fields only — never attach full raw payload
      retention_fields_used: [
        "work_email",
        "email_provider_status",
        "phone",
        "phone_type",
        "likelihood",
        "identity.full_name",
        "identity.job_company_website",
        "identity.job_last_verified",
        "identity.pdl_id",
      ],
    };
  }

  return out;
}
