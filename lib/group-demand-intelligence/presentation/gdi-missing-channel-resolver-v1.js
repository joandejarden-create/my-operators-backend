/**
 * Canonical missing-channel resolver for GDI Find Email / Find Phone /
 * Find Email & Phone.
 *
 * Order (founder-mandated 2026-11-20):
 * 1) Return already-saved verified Airtable / GDI channels
 * 2) Google Gemini + Search grounding (PRIMARY) → source verification
 * 3) Exa (SECONDARY) when Google did not recover verified channels
 * 4) Persist verified public channels through the authorized public workflow
 * 5) Surfe LAST — only for still-missing channels when explicitly authorized
 *
 * Surfe NO_RESULT must never overwrite or suppress public findings.
 * Never invent emails/phones. Never persist Surfe PII.
 * Google AI answers are discovery leads only — not persistence authority.
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

  // --- Surfe only for remaining gaps, and only when explicitly authorized ---
  let surfeResult = null;
  if (args.authorizeLiveSurfe === true && args.surfeProvider) {
    const surfeChannel =
      needEmail && needPhone ? "both" : needEmail ? "email" : "phone";
    surfeResult = await revealGdiContactOnDemand({
      channel: surfeChannel,
      contact: { name, role: contact.role || contact.title, linkedinUrl: contact.linkedinUrl },
      organization: { name: organization.name },
      provider: args.surfeProvider,
      authorizeLiveProvider: true,
    });
    methods.push({
      method: "surfe_paid_enrichment",
      ok: surfeResult?.ok === true && !surfeResult?.miss,
      miss: surfeResult?.miss === true,
      error: surfeResult?.error || null,
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
  // Never let Surfe blanks wipe public finds; Surfe values stay ephemeral.
  const finalEmail = email || surfeEmail || null;
  const finalPhone = phone || surfePhone || null;
  const stillMissing = channelNeeds(channel, finalEmail, finalPhone);
  const bothFailed =
    stillMissing.needEmail &&
    stillMissing.needPhone &&
    !finalEmail &&
    !finalPhone;

  let message;
  if (email || phone) {
    message =
      "Public directory search recovered some channels; paid enrichment did not replace them.";
  } else if (surfeEmail || surfePhone) {
    message = "Paid enrichment returned contact details (ephemeral — not persisted).";
  } else if (bothFailed) {
    message =
      "Neither public directory search nor paid enrichment recovered the requested channels. This does not prove the person has no email or phone.";
  } else {
    message = "Requested channels remain incomplete after public search.";
  }

  return {
    ok: Boolean(finalEmail || finalPhone),
    version: GDI_MISSING_CHANNEL_RESOLVER_V1,
    resolvedVia: email || phone ? "public_partial_or_full" : surfeEmail || surfePhone ? "surfe_ephemeral" : "unresolved",
    methods,
    surfeAttempted: Boolean(args.authorizeLiveSurfe && args.surfeProvider),
    publicSearchAttempted: Boolean(authorizePublic || args.forcePublicSearch),
    contactDetails:
      finalEmail || finalPhone
        ? {
            email: finalEmail,
            phone: finalPhone,
            phoneExtensions: email || phone ? phoneExtensions : [],
            phoneDisplay: formatOfficePhone(finalPhone, email || phone ? phoneExtensions : []),
            phoneType: opportunity.primaryContact?.phoneType || null,
            source: email || phone ? "public_directory" : "provider_on_demand",
            sourceUrl: email || phone ? sourceUrl : null,
            distinguishedFromSurfe: true,
            surfeEphemeralOnly: Boolean(!(email || phone) && (surfeEmail || surfePhone)),
          }
        : null,
    opportunity,
    display: buildContactChannelDisplayState(opportunity),
    message,
    miss: bothFailed,
    publicStatus: publicResult?.status || null,
  };
}

export default {
  GDI_MISSING_CHANNEL_RESOLVER_V1,
  resolveMissingContactChannels,
  formatOfficePhone,
};
