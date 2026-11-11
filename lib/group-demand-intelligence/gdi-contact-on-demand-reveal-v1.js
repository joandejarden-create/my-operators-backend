/**
 * GDI user-initiated contact reveal V1
 *
 * Ephemeral only — never persist Surfe/provider PII to Airtable or canonical stores.
 * Live provider calls require explicit authorizeLiveProvider=true (founder-gated).
 */

export const GDI_CONTACT_ON_DEMAND_REVEAL_V1 =
  "gdi-contact-on-demand-reveal-v1";

/**
 * @param {object} args
 * @param {'email'|'phone'|'both'} args.channel
 * @param {object} args.contact — selected person { name, role, linkedinUrl? }
 * @param {object} args.organization — { name }
 * @param {object} [args.provider] — injectable { reveal({channel, contact, organization}) }
 * @param {boolean} [args.authorizeLiveProvider]
 */
export async function revealGdiContactOnDemand(args = {}) {
  const channel = String(args.channel || "both").toLowerCase();
  const contact = args.contact || {};
  const organization = args.organization || {};
  const name = String(contact.name || "").trim();

  if (!name || /^(relevant contact being researched|—|-)$/i.test(name)) {
    return {
      ok: false,
      ephemeral: true,
      persisted: false,
      error: "NO_NAMED_CONTACT_SELECTED",
      message: "Select a named contact before retrieving email or phone.",
      contactDetails: null,
      version: GDI_CONTACT_ON_DEMAND_REVEAL_V1,
    };
  }

  if (!["email", "phone", "both"].includes(channel)) {
    return {
      ok: false,
      ephemeral: true,
      persisted: false,
      error: "INVALID_CHANNEL",
      message: "channel must be email, phone, or both",
      contactDetails: null,
      version: GDI_CONTACT_ON_DEMAND_REVEAL_V1,
    };
  }

  const provider = args.provider || null;
  if (!provider) {
    if (!args.authorizeLiveProvider) {
      return {
        ok: false,
        ephemeral: true,
        persisted: false,
        error: "LIVE_PROVIDER_NOT_AUTHORIZED",
        message:
          "Live contact enrichment is not authorized. Use mock provider in tests or obtain founder authorization.",
        contactDetails: null,
        version: GDI_CONTACT_ON_DEMAND_REVEAL_V1,
        liveVerified: false,
      };
    }
    return {
      ok: false,
      ephemeral: true,
      persisted: false,
      error: "PROVIDER_ADAPTER_NOT_WIRED",
      message:
        "Live Surfe adapter for GDI is not wired in this build. LIVE_CONTACT_RETRIEVAL_NOT_VERIFIED.",
      contactDetails: null,
      version: GDI_CONTACT_ON_DEMAND_REVEAL_V1,
      liveVerified: false,
    };
  }

  try {
    const result = await provider.reveal({
      channel,
      contact: { name, role: contact.role || null, linkedinUrl: contact.linkedinUrl || null },
      organization: { name: organization.name || null },
    });
    return {
      ok: result?.ok !== false,
      ephemeral: true,
      persisted: false,
      miss: result?.miss === true,
      pending: result?.pending === true,
      error: result?.error || null,
      message: result?.message || null,
      contactDetails: result?.contactDetails
        ? {
            email: result.contactDetails.email || null,
            emailType: result.contactDetails.emailType || null,
            phone: result.contactDetails.phone || null,
            phoneType: result.contactDetails.phoneType || null,
            source: "provider_on_demand",
            distinguishedFromSourceVerified: true,
          }
        : null,
      providerCalls: result?.providerCalls || 1,
      enrichmentId: result?.enrichmentId || null,
      version: GDI_CONTACT_ON_DEMAND_REVEAL_V1,
      liveVerified: Boolean(args.authorizeLiveProvider && !args.provider?.isMock),
    };
  } catch (err) {
    return {
      ok: false,
      ephemeral: true,
      persisted: false,
      error: "PROVIDER_ERROR",
      message: String(err?.message || err).slice(0, 200),
      contactDetails: null,
      version: GDI_CONTACT_ON_DEMAND_REVEAL_V1,
    };
  }
}

/** Deterministic mock provider for tests. */
export function createMockGdiContactRevealProvider(scenario = "hit") {
  return {
    isMock: true,
    async reveal({ channel }) {
      if (scenario === "error") throw new Error("mock_provider_down");
      if (scenario === "unauthorized") {
        return { ok: false, error: "CONTACT_ENRICHMENT_DISABLED", message: "disabled" };
      }
      if (scenario === "miss") {
        return {
          ok: true,
          miss: true,
          message: "No email or phone found for this person.",
          contactDetails: null,
          providerCalls: 1,
        };
      }
      // hit
      const details = {};
      if (channel === "email" || channel === "both") {
        details.email = "alex.buyer@fixture.example";
        details.emailType = "work";
      }
      if (channel === "phone" || channel === "both") {
        details.phone = "+1-202-555-0199";
        details.phoneType = "direct";
      }
      return {
        ok: true,
        miss: false,
        contactDetails: details,
        providerCalls: 1,
        message: "Provider returned contact details (ephemeral).",
      };
    },
  };
}
