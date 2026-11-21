/**
 * GDI FullEnrich on-demand contact provider V1 (work-email only).
 * Thin adapter over lib/fullenrich/client.js — never import from browser.
 * Results are ephemeral; never persist returned PII.
 */

import {
  getFullEnrichApiKeyFromEnv,
  startFullEnrichBulkWorkEmailsOnly,
  pollFullEnrichBulkUntilDone,
  FULLENRICH_CREDIT_COSTS,
  FULLENRICH_CLIENT_VERSION,
  FULLENRICH_JOB_STATUS,
} from "../fullenrich/client.js";
import {
  putFullEnrichEphemeral,
  getFullEnrichEphemeral,
  beginFullEnrichInFlight,
  endFullEnrichInFlight,
  fullEnrichEphemeralKey,
} from "./gdi-fullenrich-ephemeral-store-v1.js";
import { FE_JOB_STATUS } from "./gdi-fullenrich-spend-ledger-v1.js";

export const GDI_FULLENRICH_CONTACT_PROVIDER_V1 =
  "gdi-fullenrich-contact-provider-v1";

function splitName(fullName = "") {
  const parts = String(fullName || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (!parts.length) return { firstName: null, lastName: null };
  if (parts.length === 1) return { firstName: parts[0], lastName: null };
  return { firstName: parts[0], lastName: parts.slice(1).join(" ") };
}

function extractWorkEmail(payload) {
  const datas = Array.isArray(payload?.datas)
    ? payload.datas
    : Array.isArray(payload?.data)
      ? payload.data
      : Array.isArray(payload?.results)
        ? payload.results
        : [];
  const first = datas[0] || null;
  if (!first) return null;
  const contact = first.contact || first.result?.contact || first;
  const most =
    contact?.most_probable_work_email ||
    contact?.mostProbableWorkEmail ||
    null;
  if (most && typeof most === "string") return most.trim() || null;
  if (most && typeof most === "object" && most.email) {
    return String(most.email).trim() || null;
  }
  const emails = contact?.work_emails || contact?.workEmails || [];
  if (Array.isArray(emails) && emails.length) {
    const hit = emails.find((e) => {
      const status = String(e?.status || e?.email_status || "").toUpperCase();
      return (
        status === "DELIVERABLE" ||
        status === "HIGH_PROBABILITY" ||
        status === "CATCH_ALL" ||
        status === "CATCHALL" ||
        Boolean(e?.email)
      );
    });
    const email = hit?.email || emails[0]?.email || null;
    return email ? String(email).trim() : null;
  }
  return null;
}

/**
 * @param {{
 *   maxPollWaitMs?: number,
 *   pollIntervalMs?: number,
 *   hotelId?: string,
 *   opportunityId?: string,
 *   abortSignal?: AbortSignal,
 * }} [opts]
 */
export function createGdiFullEnrichContactRevealProvider(opts = {}) {
  const maxPollWaitMs = Number(opts.maxPollWaitMs || 120000);
  const pollIntervalMs = Number(opts.pollIntervalMs || 5000);

  return {
    id: GDI_FULLENRICH_CONTACT_PROVIDER_V1,
    isMock: false,
    contactProvider: "FULLENRICH",
    fullEnrichClientVersion: FULLENRICH_CLIENT_VERSION,

    /**
     * @param {{
     *   channel?: string,
     *   contact?: object,
     *   organization?: object,
     *   recoverEnrichmentId?: string|null,
     *   onSubmitted?: (info:{enrichmentId:string}) => void|Promise<void>,
     * }} [args]
     */
    async reveal({
      channel,
      contact = {},
      organization = {},
      recoverEnrichmentId = null,
      onSubmitted = null,
    } = {}) {
      const started = Date.now();
      // V1: work-email only
      if (String(channel || "").toLowerCase() === "phone") {
        return {
          ok: false,
          error: "PHONE_FALLBACK_DISABLED_V1",
          message: "FullEnrich phone fallback is disabled in V1.",
          contactDetails: null,
          providerCalls: 0,
          contactProvider: "FULLENRICH",
          providerStatus: FE_JOB_STATUS.ERROR,
        };
      }

      if (!getFullEnrichApiKeyFromEnv()) {
        return {
          ok: false,
          error: "MISSING_FULL_ENRICH_API_KEY",
          message: "FullEnrich API key is not configured.",
          contactDetails: null,
          providerCalls: 0,
          contactProvider: "FULLENRICH",
          providerStatus: FE_JOB_STATUS.ERROR,
        };
      }

      const name = String(contact.name || "").trim();
      const { firstName, lastName } = splitName(name);
      const linkedInUrl = String(contact.linkedinUrl || contact.profileUrl || "")
        .trim()
        .replace(/\/$/, "");
      const companyName =
        String(
          contact.companyName ||
            contact.employerName ||
            organization.name ||
            "",
        ).trim() || null;
      const companyDomain = String(
        contact.companyDomain ||
          contact.employerDomain ||
          organization.domain ||
          organization.organizationDomain ||
          "",
      )
        .trim()
        .toLowerCase()
        .replace(/^https?:\/\//, "")
        .replace(/^www\./, "")
        .split("/")[0] || null;

      const recovering = Boolean(String(recoverEnrichmentId || "").trim());
      if (
        !recovering &&
        !linkedInUrl &&
        (!firstName || (!companyDomain && !companyName))
      ) {
        return {
          ok: false,
          error: "INSUFFICIENT_PERSON_IDENTIFIERS",
          message:
            "Need a named person with company/domain or LinkedIn before FullEnrich.",
          contactDetails: null,
          providerCalls: 0,
          contactProvider: "FULLENRICH",
          providerStatus: FE_JOB_STATUS.ERROR,
        };
      }

      const ephKey = fullEnrichEphemeralKey({
        hotelId: opts.hotelId || contact.hotelId || "",
        opportunityId: opts.opportunityId || contact.opportunityId || "",
        contactName: name || String(recoverEnrichmentId || ""),
        channel: "email",
      });

      const cached = getFullEnrichEphemeral(ephKey, { consume: false });
      if (cached?.contactDetails?.email) {
        return {
          ok: true,
          miss: false,
          contactDetails: {
            email: cached.contactDetails.email,
            emailType: "work",
          },
          providerCalls: 0,
          enrichmentId: cached.enrichmentId || null,
          contactProvider: "FULLENRICH",
          fromEphemeralCache: true,
          providerLatencyMs: Date.now() - started,
          providerStatus: FE_JOB_STATUS.FINISHED,
          message: "FullEnrich result from short-lived session cache (not saved).",
          distinguishedFromPublicSource: true,
        };
      }

      const flight = beginFullEnrichInFlight(ephKey);
      if (!flight.ok) {
        return {
          ok: false,
          pending: true,
          error: flight.error,
          message: flight.message,
          contactDetails: null,
          providerCalls: 0,
          enrichmentId: flight.enrichmentId || null,
          contactProvider: "FULLENRICH",
          providerStatus: FE_JOB_STATUS.PENDING,
        };
      }

      let providerCalls = 0;
      let enrichmentId = String(recoverEnrichmentId || "").trim() || null;
      let recoveredExistingJob = recovering;
      try {
        if (opts.abortSignal?.aborted) {
          return {
            ok: false,
            error: "ABORTED",
            message: "FullEnrich lookup canceled.",
            contactDetails: null,
            providerCalls: 0,
            contactProvider: "FULLENRICH",
            providerStatus: FE_JOB_STATUS.ABORTED,
          };
        }

        if (!enrichmentId) {
          const jobName = `GDI ${name}`.slice(0, 120);
          const start = await startFullEnrichBulkWorkEmailsOnly({
            name: jobName,
            silentFail: true,
            data: [
              {
                first_name: firstName,
                last_name: lastName || firstName,
                domain: companyDomain || undefined,
                company_name: companyName || undefined,
                linkedin_url:
                  linkedInUrl && /linkedin\.com\/in\//i.test(linkedInUrl)
                    ? linkedInUrl
                    : undefined,
                custom: {
                  gdi_surface: "gdi_drawer",
                  channel: "email",
                },
              },
            ],
          });
          providerCalls += 1;

          if (start.http_status === 401 || start.http_status === 403) {
            return {
              ok: false,
              error: "FULLENRICH_UNAUTHORIZED",
              message: "FullEnrich rejected the API key.",
              contactDetails: null,
              providerCalls,
              contactProvider: "FULLENRICH",
              providerHttpStatus: start.http_status,
              providerStatus: FE_JOB_STATUS.ERROR,
            };
          }
          if (
            start.http_status === 402 ||
            start.payload?.status === FULLENRICH_JOB_STATUS.CREDITS_INSUFFICIENT
          ) {
            return {
              ok: false,
              error: "INSUFFICIENT_CREDITS",
              message: "FullEnrich credits are insufficient for this lookup.",
              contactDetails: null,
              providerCalls,
              contactProvider: "FULLENRICH",
              providerHttpStatus: start.http_status,
              providerStatus: FE_JOB_STATUS.ERROR,
            };
          }
          if (start.http_status < 200 || start.http_status >= 300) {
            return {
              ok: false,
              error: "FULLENRICH_START_FAILED",
              message: String(
                start.payload?.message ||
                  start.payload?.code ||
                  "FullEnrich start failed",
              ).slice(0, 200),
              contactDetails: null,
              providerCalls,
              contactProvider: "FULLENRICH",
              providerHttpStatus: start.http_status,
              providerStatus: FE_JOB_STATUS.ERROR,
            };
          }

          enrichmentId =
            start.payload?.enrichment_id ||
            start.payload?.enrichmentId ||
            start.payload?.id ||
            null;
        }

        if (enrichmentId && typeof onSubmitted === "function") {
          await onSubmitted({ enrichmentId, recoveredExistingJob });
        }

        const polled = await pollFullEnrichBulkUntilDone(enrichmentId, {
          intervalMs: pollIntervalMs,
          maxWaitMs: maxPollWaitMs,
          onTick: async () => {
            if (opts.abortSignal?.aborted) {
              const err = new Error("ABORTED");
              err.code = "ABORTED";
              throw err;
            }
          },
        });
        providerCalls += 1;

        if (polled.timed_out) {
          return {
            ok: false,
            pending: true,
            error: "FULLENRICH_TIMEOUT",
            message:
              "FullEnrich enrichment did not complete in time. Do not resubmit — recover the existing job id.",
            contactDetails: null,
            providerCalls,
            enrichmentId,
            contactProvider: "FULLENRICH",
            providerLatencyMs: Date.now() - started,
            providerStatus: FE_JOB_STATUS.TIMEOUT,
            // Never assume timeout was free
            providerCreditsCharged: undefined,
            creditCertainty: "unknown",
            recoveredExistingJob,
          };
        }

        const status = polled.payload?.status || null;
        if (status === FULLENRICH_JOB_STATUS.CREDITS_INSUFFICIENT) {
          return {
            ok: false,
            error: "INSUFFICIENT_CREDITS",
            message: "FullEnrich reported insufficient credits.",
            contactDetails: null,
            providerCalls,
            enrichmentId,
            contactProvider: "FULLENRICH",
            providerStatus: FE_JOB_STATUS.ERROR,
            creditCertainty: "unknown",
          };
        }
        if (
          status === FULLENRICH_JOB_STATUS.CANCELED ||
          status === FULLENRICH_JOB_STATUS.RATE_LIMIT
        ) {
          return {
            ok: false,
            error:
              status === FULLENRICH_JOB_STATUS.RATE_LIMIT
                ? "RATE_LIMIT"
                : "CANCELED",
            message: `FullEnrich job ended with status ${status}.`,
            contactDetails: null,
            providerCalls,
            enrichmentId,
            contactProvider: "FULLENRICH",
            providerStatus: FE_JOB_STATUS.ERROR,
            creditCertainty: "unknown",
          };
        }

        const email = extractWorkEmail(polled.payload);
        const latencyMs = Date.now() - started;
        if (!email) {
          return {
            ok: true,
            miss: true,
            message:
              "FullEnrich did not recover a work email. Billing remains UNKNOWN until reconciled — do not assume free.",
            contactDetails: null,
            providerCalls,
            enrichmentId,
            contactProvider: "FULLENRICH",
            providerLatencyMs: latencyMs,
            providerStatus: recoveredExistingJob
              ? FE_JOB_STATUS.RECOVERED
              : FE_JOB_STATUS.NO_MATCH,
            providerCreditsCharged: undefined,
            creditCertainty: "unknown",
            distinguishedFromPublicSource: true,
            recoveredExistingJob,
          };
        }

        const details = { email, emailType: "work" };
        putFullEnrichEphemeral(ephKey, {
          contactDetails: details,
          enrichmentId,
          at: new Date().toISOString(),
        });

        return {
          ok: true,
          miss: false,
          contactDetails: details,
          providerCalls,
          enrichmentId,
          contactProvider: "FULLENRICH",
          providerLatencyMs: latencyMs,
          providerStatus: recoveredExistingJob
            ? FE_JOB_STATUS.RECOVERED
            : FE_JOB_STATUS.FINISHED,
          providerCreditsCharged:
            FULLENRICH_CREDIT_COSTS.work_email_found_deliverable_high_probability_or_catchall,
          creditCertainty: "estimated",
          message:
            "FullEnrich returned a work email (ephemeral — not saved to Dealality records). Not a permanent public-source GDI contact.",
          incrementalEmailFound: true,
          distinguishedFromPublicSource: true,
          recoveredExistingJob,
        };
      } catch (err) {
        if (err?.code === "ABORTED" || /ABORTED/i.test(String(err?.message || ""))) {
          return {
            ok: false,
            error: "ABORTED",
            message: "FullEnrich lookup canceled (drawer closed or aborted).",
            contactDetails: null,
            providerCalls,
            enrichmentId,
            contactProvider: "FULLENRICH",
            providerStatus: FE_JOB_STATUS.ABORTED,
            creditCertainty: "unknown",
          };
        }
        return {
          ok: false,
          error: "FULLENRICH_PROVIDER_ERROR",
          message: String(err?.message || err).slice(0, 200),
          contactDetails: null,
          providerCalls,
          enrichmentId,
          contactProvider: "FULLENRICH",
          providerStatus: FE_JOB_STATUS.ERROR,
          creditCertainty: "unknown",
        };
      } finally {
        endFullEnrichInFlight(ephKey);
      }
    },
  };
}

/** Deterministic mock FullEnrich provider for fixture tests. */
export function createMockGdiFullEnrichContactRevealProvider(scenario = "hit") {
  return {
    id: "gdi-fullenrich-mock",
    isMock: true,
    contactProvider: "FULLENRICH",
    async reveal({ recoverEnrichmentId = null, onSubmitted = null } = {}) {
      const enrichmentId =
        String(recoverEnrichmentId || "").trim() || `mock_fe_${scenario}`;
      if (typeof onSubmitted === "function") {
        await onSubmitted({
          enrichmentId,
          recoveredExistingJob: Boolean(recoverEnrichmentId),
        });
      }
      if (scenario === "error") throw new Error("mock_fullenrich_down");
      if (scenario === "timeout") {
        return {
          ok: false,
          pending: true,
          error: "FULLENRICH_TIMEOUT",
          message: "timed out",
          contactDetails: null,
          providerCalls: 1,
          enrichmentId,
          contactProvider: "FULLENRICH",
          providerStatus: FE_JOB_STATUS.TIMEOUT,
          creditCertainty: "unknown",
        };
      }
      if (scenario === "insufficient") {
        return {
          ok: false,
          error: "INSUFFICIENT_CREDITS",
          message: "no credits",
          contactDetails: null,
          providerCalls: 1,
          enrichmentId,
          contactProvider: "FULLENRICH",
          providerStatus: FE_JOB_STATUS.ERROR,
          creditCertainty: "unknown",
        };
      }
      if (scenario === "miss") {
        return {
          ok: true,
          miss: true,
          contactDetails: null,
          providerCalls: 1,
          enrichmentId,
          contactProvider: "FULLENRICH",
          providerStatus: FE_JOB_STATUS.NO_MATCH,
          creditCertainty: "unknown",
          message: "no match",
          distinguishedFromPublicSource: true,
        };
      }
      return {
        ok: true,
        miss: false,
        contactDetails: {
          email: "recovered.buyer@fixture.example",
          emailType: "work",
        },
        providerCalls: 1,
        enrichmentId,
        contactProvider: "FULLENRICH",
        providerStatus: recoverEnrichmentId
          ? FE_JOB_STATUS.RECOVERED
          : FE_JOB_STATUS.FINISHED,
        providerCreditsCharged: 1,
        creditCertainty: "estimated",
        incrementalEmailFound: true,
        distinguishedFromPublicSource: true,
        recoveredExistingJob: Boolean(recoverEnrichmentId),
        message: "mock hit",
      };
    },
  };
}
