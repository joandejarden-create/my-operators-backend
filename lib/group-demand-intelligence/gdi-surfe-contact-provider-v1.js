/**
 * GDI Surfe on-demand contact reveal provider.
 * Live calls only when explicitly constructed (founder-authorized path).
 * Results are ephemeral — never persist returned PII.
 */
import {
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  getSurfeApiKeyFromEnv,
  SURFE_CLIENT_VERSION,
} from "../surfe/client.js";

export const GDI_SURFE_CONTACT_PROVIDER_V1 = "gdi-surfe-contact-provider-v1";

function splitName(fullName = "") {
  const parts = String(fullName || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (!parts.length) return { firstName: null, lastName: null };
  if (parts.length === 1) return { firstName: parts[0], lastName: null };
  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(" "),
  };
}

/**
 * Build include flags from channel so we do not request mobile when only email is needed.
 */
export function surfeIncludeForChannel(channel = "both") {
  const c = String(channel || "both").toLowerCase();
  if (c === "email") return { email: true, mobile: false };
  if (c === "phone") return { email: false, mobile: true };
  return { email: true, mobile: true };
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
      const { firstName, lastName } = splitName(name);
      const linkedInUrl = String(contact.linkedinUrl || contact.profileUrl || "")
        .trim()
        .replace(/\/$/, "");
      const companyName = String(organization.name || "").trim() || null;
      const include = surfeIncludeForChannel(channel);

      // Prefer LinkedIn as primary identifier when verified (founder / Surfe best practice).
      const person = {};
      if (linkedInUrl && /linkedin\.com\/in\//i.test(linkedInUrl)) {
        person.linkedInUrl = linkedInUrl;
        if (firstName) person.firstName = firstName;
        if (lastName) person.lastName = lastName;
      } else {
        if (firstName) person.firstName = firstName;
        if (lastName) person.lastName = lastName;
        if (companyName) person.companyName = companyName;
      }

      if (!person.linkedInUrl && !firstName) {
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
          message: "No email or phone found for this person.",
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
        null;
      const phoneValue =
        norm.mobile_phones?.[0]?.number ||
        first.mobilePhones?.[0]?.mobilePhone ||
        first.mobilePhones?.[0]?.phoneNumber ||
        first.mobilePhones?.[0]?.phone ||
        null;
      if (include.email && emailValue) {
        details.email = emailValue;
        details.emailType = "work";
      }
      if (include.mobile && phoneValue && typeof phoneValue === "string") {
        details.phone = phoneValue;
        details.phoneType = "mobile";
      }

      if (!details.email && !details.phone) {
        return {
          ok: true,
          miss: true,
          message: "No email or phone found for this person.",
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
