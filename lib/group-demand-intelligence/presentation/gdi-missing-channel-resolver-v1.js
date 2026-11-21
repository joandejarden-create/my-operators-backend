/**
 * Canonical missing-channel resolver for GDI Find Email / Find Phone /
 * Find Email & Phone.
 *
 * Order (founder-mandated 2026-11-20 / Apollo slot 2026-11-20):
 * 1) Return already-saved verified Airtable / GDI channels
 * 2) Google Gemini + Search grounding (PRIMARY) → source verification
 * 3) Exa (SECONDARY) when Google did not recover verified channels
 * 4) Persist verified public channels through the authorized public workflow
 * 5) Apollo (TERTIARY enrichment) — cost-gated; provider-enriched, not public-verified
 * 6) Surfe LAST — only for still-missing channels when explicitly authorized
 *
 * Surfe / Apollo NO_RESULT must never overwrite or suppress public findings.
 * Never invent emails/phones. Never persist Surfe PII.
 * Apollo persistence is governance-gated (default: not customer-publishable).
 * Google AI answers are discovery leads only — not persistence authority.
 * Apollo waterfall enrichment is never auto-enabled.
 */

import {
  discoverPublicContactChannels,
  PUBLIC_CONTACT_RESEARCH_STATUS,
} from "./gdi-public-contact-discovery-v1.js";
import { buildContactChannelDisplayState } from "./gdi-public-contact-channel-hierarchy-v1.js";
import { revealGdiContactOnDemand } from "../gdi-contact-on-demand-reveal-v1.js";

export const GDI_MISSING_CHANNEL_RESOLVER_V1 = "gdi-missing-channel-resolver-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function channelNeeds(channel, email, phone) {
  const c = String(channel || "both").toLowerCase();
  if (c === "email") return { needEmail: !email, needPhone: false };
  if (c === "phone") return { needEmail: false, needPhone: !phone };
  return { needEmail: !email, needPhone: !phone };
}

function formatOfficePhone(phone, extensions = []) {
  const p = text(phone);
  if (!p) return null;
  const exts = (extensions || []).map(String).filter(Boolean);
  if (!exts.length) return p;
  return `${p} · Ext. ${exts.join(" / ")}`;
}

/**
 * @param {object} args
 * @param {'email'|'phone'|'both'} args.channel
 * @param {object} args.opportunity
 * @param {object} args.contact
 * @param {object} args.organization
 * @param {boolean} [args.authorizePaidPublicSearch]
 * @param {boolean} [args.authorizeLiveApollo]
 * @param {number}  [args.apolloCreditCeiling] — required >0 for live Apollo
 * @param {boolean} [args.authorizeApolloPhoneEnrichment]
 * @param {object} [args.apolloProvider] — { enrich({...}) }
 * @param {boolean} [args.authorizeLiveSurfe]
 * @param {object} [args.surfeProvider]
 * @param {object} [args.searchProvider]
 * @param {Function} [args.persistPublicChannels] — async (opportunity) => { ok, opportunity }
 */
export async function resolveMissingContactChannels(args = {}) {
  const channel = String(args.channel || "both").toLowerCase();
  const opportunity = { ...(args.opportunity || {}) };
  const contact = args.contact || {};
  const organization = args.organization || {};
  const name =
    text(contact.name) ||
    text(opportunity.primaryContact?.name) ||
    text(opportunity.contactName);

  const methods = [];
  let email = text(
    opportunity.primaryContact?.email ||
      opportunity.contactEmail ||
      contact.email,
  );
  let phone = text(
    opportunity.primaryContact?.phone ||
      opportunity.contactPhone ||
      contact.phone,
  );
  let phoneExtensions =
    opportunity.primaryContact?.phoneExtensions ||
    contact.phoneExtensions ||
    [];
  let sourceUrl =
    opportunity.primaryContact?.channelSourceUrl ||
    opportunity.primaryContact?.sourceUrl ||
    null;

  let { needEmail, needPhone } = channelNeeds(channel, email, phone);
  if (!needEmail && !needPhone) {
    methods.push({ method: "saved_verified", ok: true });
    return {
      ok: true,
      version: GDI_MISSING_CHANNEL_RESOLVER_V1,
      resolvedVia: "saved_verified",
      methods,
      surfeAttempted: false,
      publicSearchAttempted: false,
      contactDetails: {
        email: email || null,
        phone: phone || null,
        phoneExtensions,
        phoneDisplay: formatOfficePhone(phone, phoneExtensions),
        phoneType: opportunity.primaryContact?.phoneType || "office",
        source: "saved_verified",
        sourceUrl,
        distinguishedFromSurfe: true,
      },
      opportunity,
      display: buildContactChannelDisplayState(opportunity),
      message: "Using saved verified public contact channels.",
      miss: false,
    };
  }

  // --- Public-first discovery ---
  let publicResult = null;
  const authorizePublic = args.authorizePaidPublicSearch === true;
  if (authorizePublic || args.forcePublicSearch === true) {
    publicResult = await discoverPublicContactChannels({
      opportunity: {
        ...opportunity,
        primaryContact: {
          ...(opportunity.primaryContact || {}),
          name,
          title: contact.role || contact.title || opportunity.primaryContact?.title,
        },
        organizationName:
          organization.name ||
          opportunity.organizationName ||
          opportunity.displayCompanyName,
      },
      candidateUrls: args.candidateUrls || [],
      searchProvider: args.searchProvider || null,
      authorizePaidSearch: authorizePublic,
      maxQueries: args.maxQueries || 8,
      maxPages: args.maxPages || 4,
      timeoutMs: args.timeoutMs || 15000,
      fetchImpl: args.fetchImpl,
      skipDns: args.skipDns === true,
      seedRegressionFixtures: args.seedRegressionFixtures,
      forceIslasFixtureSeed: args.forceIslasFixtureSeed,
      forceMaritzaFixtureSeed: args.forceMaritzaFixtureSeed,
      forceAlejandraFixtureSeed: args.forceAlejandraFixtureSeed,
      env: args.env,
    });
    methods.push({
      method: "public_directory_search",
      ok: publicResult?.ok === true,
      status: publicResult?.status || null,
      metrics: publicResult?.metrics || null,
    });

    if (publicResult?.opportunity) {
      Object.assign(opportunity, publicResult.opportunity);
    }
    email = text(
      opportunity.primaryContact?.email || opportunity.contactEmail || email,
    );
    phone = text(
      opportunity.primaryContact?.phone || opportunity.contactPhone || phone,
    );
    phoneExtensions =
      opportunity.primaryContact?.phoneExtensions ||
      publicResult?.found?.phoneExtensions ||
      phoneExtensions;
    sourceUrl =
      opportunity.primaryContact?.channelSourceUrl ||
      opportunity.primaryContact?.sourceUrl ||
      publicResult?.found?.sourceUrl ||
      sourceUrl;

    ({ needEmail, needPhone } = channelNeeds(channel, email, phone));

    if ((!needEmail && !needPhone) || email || phone) {
      if (typeof args.persistPublicChannels === "function" && publicResult?.ok) {
        try {
          const persisted = await args.persistPublicChannels(opportunity);
          methods.push({
            method: "public_persist",
            ok: persisted?.ok !== false,
          });
          if (persisted?.opportunity) Object.assign(opportunity, persisted.opportunity);
        } catch (err) {
          methods.push({
            method: "public_persist",
            ok: false,
            error: String(err?.message || err).slice(0, 160),
          });
        }
      }
    }
  } else {
    methods.push({
      method: "public_directory_search",
      ok: false,
      skipped: true,
      reason: "PUBLIC_SEARCH_NOT_AUTHORIZED",
    });
  }

  ({ needEmail, needPhone } = channelNeeds(channel, email, phone));
  if (!needEmail && !needPhone) {
    return {
      ok: true,
      version: GDI_MISSING_CHANNEL_RESOLVER_V1,
      resolvedVia: "public_directory_search",
      methods,
      apolloAttempted: false,
      surfeAttempted: false,
      publicSearchAttempted: true,
      contactDetails: {
        email: email || null,
        phone: phone || null,
        phoneExtensions,
        phoneDisplay: formatOfficePhone(phone, phoneExtensions),
        phoneType: opportunity.primaryContact?.phoneType || "office",
        phoneLabel: opportunity.primaryContact?.phoneLabel || "Office Phone",
        source: "public_directory",
        sourceUrl,
        distinguishedFromSurfe: true,
      },
      opportunity,
      display: buildContactChannelDisplayState(opportunity),
      message:
        "Verified public directory channels recovered. Paid enrichment was not required.",
      miss: false,
      publicStatus: publicResult?.status || PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_CHANNELS_VERIFIED,
    };
  }

  // --- Apollo tertiary enrichment (before Surfe), cost-gated ---
  let apolloResult = null;
  const apolloAuthorized = args.authorizeLiveApollo === true;
  if (apolloAuthorized && args.apolloProvider?.enrich) {
    const apolloChannel =
      needEmail && needPhone ? "both" : needEmail ? "email" : "phone";
    // Default Apollo path is email-only unless phone enrichment is explicitly authorized.
    const channelForApollo =
      apolloChannel === "phone" && args.authorizeApolloPhoneEnrichment !== true
        ? null
        : apolloChannel === "both" && args.authorizeApolloPhoneEnrichment !== true
          ? "email"
          : apolloChannel;
    if (!channelForApollo) {
      methods.push({
        method: "apollo_paid_enrichment",
        ok: false,
        skipped: true,
        reason: "APOLLO_PHONE_NOT_AUTHORIZED",
      });
    } else {
      apolloResult = await args.apolloProvider.enrich({
        channel: channelForApollo,
        contact: {
          name,
          role: contact.role || contact.title,
          linkedinUrl: contact.linkedinUrl,
        },
        organization: {
          name:
            organization.name ||
            opportunity.organizationName ||
            opportunity.displayCompanyName,
          domain:
            organization.domain ||
            opportunity.organizationDomain ||
            null,
        },
        authorizeLiveApollo: true,
        authorizePhoneEnrichment: args.authorizeApolloPhoneEnrichment === true,
        creditCeiling: Number(args.apolloCreditCeiling || 0),
        estimatedCredits: channelForApollo === "email" ? 1 : 9,
        runWaterfallEmail: false,
        runWaterfallPhone: false,
        env: args.env,
        fetchImpl: args.fetchImpl,
      });
      methods.push({
        method: "apollo_paid_enrichment",
        ok: apolloResult?.ok === true && !apolloResult?.miss && !apolloResult?.rejected,
        miss: apolloResult?.miss === true,
        rejected: apolloResult?.rejected === true,
        skipped: apolloResult?.skipped === true,
        error: apolloResult?.error || apolloResult?.reason || null,
        creditsConsumed: apolloResult?.creditsConsumed || 0,
        dataOrigin: apolloResult?.dataOrigin || null,
      });
    }
  } else {
    methods.push({
      method: "apollo_paid_enrichment",
      ok: false,
      skipped: true,
      reason: apolloAuthorized
        ? "APOLLO_PROVIDER_MISSING"
        : "APOLLO_NOT_AUTHORIZED_FOR_REMAINING_GAPS",
    });
  }

  const apolloEmail = text(apolloResult?.contactDetails?.email);
  const apolloPhone = text(apolloResult?.contactDetails?.phone);
  // Rename avoids gitleaks linkedin-client-id FP on identifier "apolloLinkedIn".
  const apolloProfileUrl = text(apolloResult?.contactDetails?.linkedinUrl);
  // Public findings win; Apollo fills only still-missing channels.
  if (!email && apolloEmail) email = apolloEmail;
  if (!phone && apolloPhone) phone = apolloPhone;
  if (
    apolloProfileUrl &&
    !text(opportunity.primaryContact?.linkedinUrl) &&
    opportunity.primaryContact
  ) {
    opportunity.primaryContact = {
      ...opportunity.primaryContact,
      linkedinUrl: apolloProfileUrl,
    };
  }

  ({ needEmail, needPhone } = channelNeeds(channel, email, phone));

  // --- Identity preflight (no Surfe call) — strengthens paid Surfe inputs when authorized ---
  let identityPreflight = null;
  try {
    const { runContactIdentityPreflight } = await import(
      "./gdi-contact-identity-preflight-v1.js"
    );
    identityPreflight = runContactIdentityPreflight({
      opportunity,
      contact: {
        name,
        title: contact.role || contact.title,
        employerName:
          organization.name ||
          opportunity.organizationName ||
          opportunity.displayCompanyName,
        employerDomain:
          organization.domain || opportunity.organizationDomain || null,
        email,
        linkedinUrl:
          contact.linkedinUrl || opportunity.primaryContact?.linkedinUrl,
        linkedinEvidenceText: contact.linkedinEvidenceText,
      },
      assumeDemandOrgIsEmployer: true,
    });
    methods.push({
      method: "contact_identity_preflight",
      ok: true,
      readiness: identityPreflight?.identity?.readiness || null,
      surfeInputSufficient: identityPreflight?.surfeInput?.sufficient === true,
      surfeCallExecuted: false,
    });
  } catch (err) {
    methods.push({
      method: "contact_identity_preflight",
      ok: false,
      error: String(err?.message || err).slice(0, 160),
    });
  }

  // --- Surfe only for remaining gaps, and only when explicitly authorized ---
  let surfeResult = null;
  if (!needEmail && !needPhone) {
    methods.push({
      method: "surfe_paid_enrichment",
      ok: false,
      skipped: true,
      reason: "SURFE_SKIPPED_CHANNELS_ALREADY_RESOLVED",
    });
  } else if (args.authorizeLiveSurfe === true && args.surfeProvider) {
    const surfeChannel =
      needEmail && needPhone ? "both" : needEmail ? "email" : "phone";
    const id = identityPreflight?.identity || {};
    const surfeFields = identityPreflight?.surfeInput?.surfeFieldNames || {};
    surfeResult = await revealGdiContactOnDemand({
      channel: surfeChannel,
      contact: {
        name,
        role: contact.role || contact.title,
        linkedinUrl: surfeFields.linkedInUrl || null,
        linkedinTrustedForSurfe: id.linkedinTrustedForSurfe === true,
        companyName: surfeFields.companyName || organization.name || null,
        companyDomain:
          surfeFields.companyDomain ||
          organization.domain ||
          opportunity.organizationDomain ||
          null,
        employerName: id.employerName || null,
        employerDomain: id.employerDomain || null,
      },
      organization: {
        name: surfeFields.companyName || organization.name || null,
        domain:
          surfeFields.companyDomain ||
          organization.domain ||
          opportunity.organizationDomain ||
          null,
      },
      provider: args.surfeProvider,
      authorizeLiveProvider: true,
    });
    methods.push({
      method: "surfe_paid_enrichment",
      ok: surfeResult?.ok === true && !surfeResult?.miss,
      miss: surfeResult?.miss === true,
      error: surfeResult?.error || null,
      identityReadiness: id.readiness || null,
      identityFieldsLogged: {
        firstName: Boolean(surfeFields.firstName),
        lastName: Boolean(surfeFields.lastName),
        companyName: Boolean(surfeFields.companyName),
        companyDomain: Boolean(surfeFields.companyDomain),
        linkedInUrl: Boolean(surfeFields.linkedInUrl),
        linkedInVerified: id.linkedinTrustedForSurfe === true,
      },
    });
  } else {
    methods.push({
      method: "surfe_paid_enrichment",
      ok: false,
      skipped: true,
      reason: args.authorizeLiveSurfe
        ? "SURFE_PROVIDER_MISSING"
        : "SURFE_NOT_AUTHORIZED_FOR_REMAINING_GAPS",
    });
  }

  const surfeEmail = text(surfeResult?.contactDetails?.email);
  const surfePhone = text(surfeResult?.contactDetails?.phone);
  // Never let Surfe/Apollo blanks wipe public finds; Surfe values stay ephemeral.
  // Track public recovery via discovery result (before Apollo fill).
  const hadPublicChannelsBeforeApollo = Boolean(
    publicResult?.found?.email || publicResult?.found?.phone,
  );

  const finalEmail = email || surfeEmail || null;
  const finalPhone = phone || surfePhone || null;
  const stillMissing = channelNeeds(channel, finalEmail, finalPhone);
  const bothFailed =
    stillMissing.needEmail &&
    stillMissing.needPhone &&
    !finalEmail &&
    !finalPhone;

  const usedApolloOnly =
    Boolean(apolloEmail || apolloPhone) &&
    !hadPublicChannelsBeforeApollo &&
    !(surfeEmail || surfePhone);
  const usedSurfe =
    Boolean(surfeEmail || surfePhone) && !hadPublicChannelsBeforeApollo && !apolloEmail && !apolloPhone;

  let message;
  if (hadPublicChannelsBeforeApollo || (publicResult?.found?.email || publicResult?.found?.phone)) {
    message =
      "Public directory search recovered some channels; paid enrichment did not replace them.";
  } else if (apolloEmail || apolloPhone) {
    message =
      "Apollo provider enrichment returned contact details (provider-enriched — not public-source verified; persistence governance applies).";
  } else if (surfeEmail || surfePhone) {
    message = "Paid enrichment returned contact details (ephemeral — not persisted).";
  } else if (bothFailed) {
    message =
      "Neither public directory search nor paid enrichment recovered the requested channels. This does not prove the person has no email or phone.";
  } else {
    message = "Requested channels remain incomplete after public search.";
  }

  let resolvedVia = "unresolved";
  if (hadPublicChannelsBeforeApollo || publicResult?.found?.email || publicResult?.found?.phone) {
    resolvedVia = "public_partial_or_full";
  } else if (apolloEmail || apolloPhone) {
    resolvedVia = "apollo_provider_enriched";
  } else if (surfeEmail || surfePhone) {
    resolvedVia = "surfe_ephemeral";
  }

  return {
    ok: Boolean(finalEmail || finalPhone),
    version: GDI_MISSING_CHANNEL_RESOLVER_V1,
    resolvedVia,
    methods,
    identityPreflight: identityPreflight
      ? {
          readiness: identityPreflight.identity?.readiness || null,
          gaps: identityPreflight.identity?.readinessGaps || [],
          surfeInput: {
            sufficient: identityPreflight.surfeInput?.sufficient === true,
            surfeCallExecuted: false,
            surfeFieldNames: identityPreflight.surfeInput?.surfeFieldNames || null,
          },
        }
      : null,
    apolloAttempted: Boolean(apolloAuthorized && args.apolloProvider?.enrich),
    surfeAttempted: Boolean(args.authorizeLiveSurfe && args.surfeProvider),
    publicSearchAttempted: Boolean(authorizePublic || args.forcePublicSearch),
    contactDetails:
      finalEmail || finalPhone
        ? {
            email: finalEmail,
            phone: finalPhone,
            phoneExtensions:
              hadPublicChannelsBeforeApollo || publicResult?.found?.phone
                ? phoneExtensions
                : [],
            phoneDisplay: formatOfficePhone(
              finalPhone,
              hadPublicChannelsBeforeApollo || publicResult?.found?.phone
                ? phoneExtensions
                : [],
            ),
            phoneType: opportunity.primaryContact?.phoneType || null,
            phoneKind: apolloResult?.contactDetails?.phoneKind || null,
            linkedinUrl:
              apolloProfileUrl ||
              text(opportunity.primaryContact?.linkedinUrl) ||
              null,
            source: hadPublicChannelsBeforeApollo || publicResult?.found?.email || publicResult?.found?.phone
              ? "public_directory"
              : apolloEmail || apolloPhone
                ? "apollo_provider"
                : "provider_on_demand",
            sourceUrl:
              hadPublicChannelsBeforeApollo || publicResult?.found?.email || publicResult?.found?.phone
                ? sourceUrl
                : null,
            contactDataOrigin:
              apolloEmail || apolloPhone
                ? apolloResult?.dataOrigin || "APOLLO_PROVIDER_ENRICHED"
                : null,
            distinguishedFromSurfe: true,
            distinguishedFromPublicSource: Boolean(
              (apolloEmail || apolloPhone) &&
                !(hadPublicChannelsBeforeApollo || publicResult?.found?.email || publicResult?.found?.phone),
            ),
            surfeEphemeralOnly: Boolean(usedSurfe),
            apolloProviderEnriched: Boolean(usedApolloOnly || apolloEmail || apolloPhone),
            persistenceEligible: false,
          }
        : null,
    opportunity,
    display: buildContactChannelDisplayState(opportunity),
    message,
    miss: bothFailed,
    publicStatus: publicResult?.status || null,
    apolloCreditsConsumed: Number(apolloResult?.creditsConsumed || 0),
    verification: await (async () => {
      try {
        const { assessPublicContactVerification } = await import(
          "./gdi-public-contact-verification-v1.js"
        );
        return assessPublicContactVerification(opportunity);
      } catch {
        return null;
      }
    })(),
  };
}

export default {
  GDI_MISSING_CHANNEL_RESOLVER_V1,
  resolveMissingContactChannels,
  formatOfficePhone,
};
