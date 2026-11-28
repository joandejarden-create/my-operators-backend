/**
 * GDI Surfe on-demand contact reveal provider.
 * Live calls only when explicitly constructed (founder-authorized path).
 * Results are ephemeral — never persist returned PII.
 */
import {
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  pickSurfePhoneNumber,
  buildSurfeEnrichPersonInput,
  getSurfeApiKeyFromEnv,
  SURFE_CLIENT_VERSION,
} from "../surfe/client.js";
import {
  surfeIncludeForChannel as sharedSurfeIncludeForChannel,
  resolveEnrichmentChannelRequest,
} from "../shared-contact-intelligence/missing-channel-enrichment-v3.js";

export const GDI_SURFE_CONTACT_PROVIDER_V1 = "gdi-surfe-contact-provider-v1";

/**
 * Build include flags from channel so we do not request mobile when only email is needed.
 * Shared with Hotel Explorer (missing-channel-enrichment-v3).
 */
export function surfeIncludeForChannel(channel = "both") {
  return sharedSurfeIncludeForChannel(channel);
}

/**
 * Create a Surfe-backed reveal provider for GDI contact CTA.
 * @param {{ pollOnEnrich?: boolean, maxPollWaitMs?: number }} [opts]
 */
export function createGdiSurfeContactRevealProvider(opts = {}) {
  const pollOnEnrich = opts.pollOnEnrich !== false;
  const maxPollWaitMs = Number(opts.maxPollWaitMs || 45000);

  return {
    id: GDI_SURFE_CONTACT_PROVIDER_V1,
    isMock: false,
    surfeClientVersion: SURFE_CLIENT_VERSION,

    async reveal({ channel, contact = {}, organization = {} } = {}) {
      if (!getSurfeApiKeyFromEnv()) {
        return {
          ok: false,
          error: "MISSING_SURFE_API_KEY",
          message: "Surfe API key is not configured on the server.",
          contactDetails: null,
          providerCalls: 0,
        };
      }

      const name = String(contact.name || "").trim();
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
      // Identity preflight sets linkedinTrustedForSurfe=false to block unverified URLs.
      // Legacy callers that omit the flag keep prior /in/ behavior.
      const linkedInBlocked = contact.linkedinTrustedForSurfe === false;
      // Prefer durable missing-channel state when contact already carries channels;
      // otherwise honor explicit channel (email|phone|both) from the shared mapper.
      const resolved = resolveEnrichmentChannelRequest({
        person: {
          email: contact.email || contact.workEmail || null,
          phone: contact.phone || contact.mobile || null,
          linkedinUrl: contact.linkedinUrl || contact.profileUrl || null,
          channels: contact.channels || [],
          emails: contact.emails || contact.emailContacts || [],
          phones: contact.phones || contact.phoneContacts || [],
        },
        channelHint: channel,
      });
      const include =
        resolved.source === "durable_channels" ||
        resolved.source === "durable_channels_complete"
          ? resolved.surfeInclude
          : surfeIncludeForChannel(channel);

      if (resolved.channel === "none" || (!include.email && !include.mobile)) {
        return {
          ok: true,
          miss: false,
          skipped: true,
          message: "Verified email and phone already on file — no Surfe credit used.",
          contactDetails: null,
          providerCalls: 0,
          requestedInclude: { email: false, mobile: false },
          missingChannelState: resolved.missingChannelState,
        };
      }

      // Prefer verified LinkedIn (`linkedinUrl` per Surfe docs) over name-only matching.
      const person = buildSurfeEnrichPersonInput({
        personName: name,
        companyName,
        companyDomain,
        linkedinUrl: linkedInBlocked
          ? null
          : contact.linkedinUrl || contact.profileUrl || contact.linkedInUrl,
        jobTitle: contact.title || contact.jobTitle || null,
      });

      if (!person.linkedinUrl && !person.firstName) {
        return {
          ok: false,
          error: "INSUFFICIENT_PERSON_IDENTIFIERS",
          message: "Need a named person (and preferably LinkedIn) before Surfe enrichment.",
          contactDetails: null,
          providerCalls: 0,
        };
      }

      const start = await startSurfePeopleEnrichment({
        people: [person],
        include: {
          email: include.email === true,
          mobile: include.mobile === true,
          linkedInUrl: include.linkedInUrl !== false,
        },
      });

      if (!start.ok) {
        return {
          ok: false,
          error: start.error || "SURFE_ENRICH_START_FAILED",
          message: String(start.payload?.message || start.error || "Surfe enrich start failed").slice(
            0,
            200,
          ),
          contactDetails: null,
          providerCalls: 1,
          enrichmentId: start.payload?.enrichmentID || start.payload?.id || null,
        };
      }

      const enrichmentId =
        start.payload?.enrichmentID ||
        start.payload?.enrichmentId ||
        start.payload?.id ||
        null;

      // Initial POST is often async — do not treat as final email/phone.
      let finalPayload = start.payload;
      let providerCalls = 1;
      if (pollOnEnrich && enrichmentId) {
        const polled = await pollSurfePeopleEnrichment(enrichmentId, {
          maxWaitMs: maxPollWaitMs,
        });
        providerCalls += 1;
        if (polled.timed_out) {
          return {
            ok: false,
            pending: true,
            error: "SURFE_ENRICH_TIMEOUT",
            message:
              "Surfe enrichment did not complete in time. Try again later; nothing was saved.",
            contactDetails: null,
            providerCalls,
            enrichmentId,
          };
        }
        if (!polled.ok) {
          return {
            ok: false,
            error: polled.error || "SURFE_ENRICH_POLL_FAILED",
            message: "Surfe enrichment polling failed.",
            contactDetails: null,
            providerCalls,
            enrichmentId,
          };
        }
        finalPayload = polled.payload;
      } else if (enrichmentId && !pollOnEnrich) {
        return {
          ok: true,
          pending: true,
          message: "Surfe enrichment job started; awaiting completion.",
          contactDetails: null,
          providerCalls,
          enrichmentId,
        };
      }

      const people = Array.isArray(finalPayload?.people)
        ? finalPayload.people
        : Array.isArray(finalPayload?.results)
          ? finalPayload.results
          : [];
      const first = people[0] || null;
      if (!first) {
        return {
          ok: true,
          miss: true,
          message:
            "Paid enrichment did not recover email or phone. This does not prove publicly available channels are absent.",
          contactDetails: null,
          providerCalls,
          enrichmentId,
        };
      }

      const norm = normalizeSurfePerson(first);
      const details = {};
      const emailValue =
        norm.emails?.[0]?.email ||
        first.emails?.[0]?.email ||
        first.email ||
        null;
      const picked = pickSurfePhoneNumber(norm);
      if (include.email && emailValue) {
        details.email = emailValue;
        details.emailType = "work";
      }
      // Landlines are free and returned even when include.mobile is false — surface them.
      // Paid mobile cascade still only runs when include.mobile is true.
      if (picked.phone && typeof picked.phone === "string") {
        if (picked.phoneKind === "mobile" && include.mobile !== true) {
          // Mobile was not requested; do not surface paid-cascade mobile accidentally.
        } else {
          details.phone = picked.phone;
          details.phoneType = picked.phoneKind || "phone";
        }
      }

      if (!details.email && !details.phone) {
        return {
          ok: true,
          miss: true,
          message:
            "Paid enrichment did not recover email or phone. This does not prove publicly available channels are absent.",
          contactDetails: null,
          providerCalls,
          enrichmentId,
        };
      }

      return {
        ok: true,
        miss: false,
        contactDetails: details,
        providerCalls,
        enrichmentId,
        message: "Provider returned contact details (ephemeral — not saved).",
      };
    },
  };
}
