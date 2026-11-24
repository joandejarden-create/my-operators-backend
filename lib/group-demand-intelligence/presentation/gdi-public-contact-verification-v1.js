/**
 * Permanent public-source contact verification standards (founder V1).
 *
 * Maps evidence onto:
 *   VERIFIED_DIRECT | VERIFIED_ORGANIZATION_ROUTE | CANDIDATE_UNVERIFIED
 *   | NOT_FOUND | STALE_OR_CONFLICTED
 *
 * Never invents emails/phones. Never treats guessed formats as verified.
 * Never promotes provider-ephemeral Surfe/FullEnrich values to verified public.
 */

import { CONTACT_DATA_ORIGIN } from "../extract-contact-intelligence-from-source.js";
import { isGenericOrganizationEmail } from "../scaling-companies-sales-presentation-v1.js";
import { personIdentityKey as provenancePersonIdentityKey } from "../workflow/contact-field-provenance-v1.js";

export const GDI_PUBLIC_CONTACT_VERIFICATION_V1 =
  "gdi-public-contact-verification-v1";

export const PUBLIC_CONTACT_VERIFICATION_STATUS = Object.freeze({
  VERIFIED_DIRECT: "VERIFIED_DIRECT",
  VERIFIED_ORGANIZATION_ROUTE: "VERIFIED_ORGANIZATION_ROUTE",
  CANDIDATE_UNVERIFIED: "CANDIDATE_UNVERIFIED",
  NOT_FOUND: "NOT_FOUND",
  STALE_OR_CONFLICTED: "STALE_OR_CONFLICTED",
});

export const PUBLIC_CONTACT_CHANNEL_KIND = Object.freeze({
  DIRECT_WORK_EMAIL: "DIRECT_WORK_EMAIL",
  ORGANIZATION_EMAIL_ROUTE: "ORGANIZATION_EMAIL_ROUTE",
  DIRECT_BUSINESS_PHONE: "DIRECT_BUSINESS_PHONE",
  ORGANIZATION_PHONE_ROUTE: "ORGANIZATION_PHONE_ROUTE",
  UNKNOWN: "UNKNOWN",
});

/** Shared / functional mailbox locals (EN + common Romance/PT/DE). */
const ROLE_MAILBOX_RE =
  /^(info|contact|contacto|contato|contatti|kontakt|hello|hola|office|oficina|admin|administracion|administracao|support|soporte|sales|ventas|vendas|inquiries|enquiry|enquiries|reception|recepcion|mail|mailto|team|equipo|press|prensa|media|comunicaciones|customerservice|servicio|atencion|atendimento|acogida|hq|front.?desk|summit|conference|congress|events?|eventos|housing|registration|registrations|bookings?|reservations?|reservas|rsvp|general|helpdesk|help|webmaster|noreply|no-reply|donotreply)$/i;

const PROVIDER_ORIGIN_RE =
  /APOLLO|PROSPEO|PDL|PEOPLE.?DATA.?LABS|SURFE|FULLENRICH|PROVIDER_ENRICHED|PROVIDER_EPHEMERAL|ON_DEMAND_ENRICHMENT/i;

const STALE_DAYS_DEFAULT = 365;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function publicContactPersonKey({ name, organization, domain } = {}) {
  const n = text(name);
  const o = text(organization || domain);
  if (!n || n.split(/\s+/).filter(Boolean).length < 2) return null;
  if (!o || o.length < 2) return null;
  return provenancePersonIdentityKey({ name: n, organization: o, domain });
}

export function isRoleOrGenericMailbox(email) {
  const e = text(email).toLowerCase();
  if (!e || !e.includes("@")) return false;
  if (isGenericOrganizationEmail(e)) return true;
  const local = (e.split("@")[0] || "").replace(/\+.*$/, "");
  // Strip common separators then re-test whole local (contacto, contact-us, info.hotel)
  if (ROLE_MAILBOX_RE.test(local)) return true;
  const head = local.split(/[._+-]/)[0] || "";
  return ROLE_MAILBOX_RE.test(head);
}

/**
 * True when the email local-part plausibly belongs to the named person
 * (first/last token overlap). Shared inboxes must fail this check.
 */
export function emailLocalPartMatchesPerson(email, personName) {
  const e = text(email).toLowerCase();
  const name = text(personName).toLowerCase();
  if (!e.includes("@") || !name) return false;
  const local = (e.split("@")[0] || "")
    .replace(/\+.*$/, "")
    .replace(/[^a-z0-9._-]/g, "");
  if (!local || isRoleOrGenericMailbox(e)) return false;
  const nameTokens = name
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 2);
  if (!nameTokens.length) return false;
  const localNorm = local
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[._-]/g, "");
  // Require at least one name token (≥3 chars preferred; allow 2-char initials pairs)
  return nameTokens.some((tok) => {
    if (tok.length >= 3 && localNorm.includes(tok)) return true;
    if (tok.length >= 3 && local.split(/[._-]/).includes(tok)) return true;
    return false;
  });
}

/** Person-profile LinkedIn only — company /posts/ and /company/ are not person channels. */
export function isPersonLinkedInProfileUrl(url) {
  const u = text(url);
  if (!u) return false;
  return /^https?:\/\/([a-z0-9-]+\.)?linkedin\.com\/in\/[A-Za-z0-9_%-]+\/?/i.test(
    u,
  );
}

function extractEmail(contact = {}, opportunity = {}) {
  const raw =
    contact.email ||
    contact.workEmail ||
    contact.businessEmail ||
    opportunity.contactEmail ||
    opportunity.primaryContactEmail ||
    null;
  if (raw && typeof raw === "object") {
    return text(raw.email || raw.value || raw.address).toLowerCase() || null;
  }
  const e = text(raw).toLowerCase();
  if (!e || !e.includes("@") || /\[object Object\]/i.test(e)) return null;
  return e;
}

function extractPhone(contact = {}, opportunity = {}) {
  const candidates = [
    contact.phone,
    contact.directPhone,
    contact.workPhone,
    contact.officePhone,
    opportunity.contactPhone,
    opportunity.companyPhone,
  ]
    .map((v) => text(v))
    .filter((v) => v && /\d/.test(v));
  return candidates[0] || null;
}

function originOf(contact = {}, opportunity = {}) {
  return text(
    contact.contactDataOrigin ||
      opportunity.contactDataOrigin ||
      contact.dataOrigin ||
      "",
  );
}

function isProviderOrigin(origin) {
  return (
    origin === CONTACT_DATA_ORIGIN.PROVIDER_EPHEMERAL ||
    PROVIDER_ORIGIN_RE.test(origin)
  );
}

function parseCheckedAt(contact = {}, opportunity = {}) {
  const raw =
    contact.channelLastVerifiedAt ||
    contact.lastVerifiedAt ||
    opportunity.contactVerifiedAt ||
    null;
  if (!raw) return null;
  const t = Date.parse(String(raw));
  return Number.isFinite(t) ? t : null;
}

function isStale(checkedAtMs, staleDays = STALE_DAYS_DEFAULT) {
  if (!checkedAtMs) return false;
  const ageDays = (Date.now() - checkedAtMs) / (24 * 3600 * 1000);
  return ageDays > staleDays;
}

/**
 * Classify a single channel (email or phone) for a named person.
 */
export function classifyPublicContactChannel({
  channel,
  value,
  contact = {},
  opportunity = {},
  personName = null,
  organizationName = null,
  sourceUrl = null,
  staleDays = STALE_DAYS_DEFAULT,
} = {}) {
  const ch = String(channel || "").toLowerCase();
  const v = text(value);
  const name =
    text(personName) ||
    text(contact.name) ||
    text(opportunity.primaryContact?.name) ||
    text(opportunity.contactName);
  const org =
    text(organizationName) ||
    text(contact.organization) ||
    text(opportunity.organizationName) ||
    text(opportunity.displayCompanyName);
  const origin = originOf(contact, opportunity);
  const src =
    text(sourceUrl) ||
    text(contact.channelSourceUrl) ||
    text(contact.sourceUrl) ||
    text(opportunity.contactSourceUrl) ||
    null;
  const checkedAt = parseCheckedAt(contact, opportunity);
  const verificationHint = text(
    contact.channelVerificationStatus || contact.verificationStatus || "",
  ).toUpperCase();

  if (!v) {
    return {
      channel: ch,
      value: null,
      status: PUBLIC_CONTACT_VERIFICATION_STATUS.NOT_FOUND,
      kind: PUBLIC_CONTACT_CHANNEL_KIND.UNKNOWN,
      personIdentified: Boolean(name && name.split(/\s+/).length >= 2),
      organizationEstablished: Boolean(org),
      sourceUrl: src,
      checkedAt: checkedAt ? new Date(checkedAt).toISOString() : null,
      permittedUseUnknown: false,
      reasons: ["NO_VALUE"],
    };
  }

  // Provider-returned values are never verified public retention candidates.
  if (isProviderOrigin(origin)) {
    return {
      channel: ch,
      value: null, // do not echo provider PII into verification records
      status: PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED,
      kind: PUBLIC_CONTACT_CHANNEL_KIND.UNKNOWN,
      personIdentified: Boolean(name && name.split(/\s+/).length >= 2),
      organizationEstablished: Boolean(org),
      sourceUrl: null,
      checkedAt: null,
      permittedUseUnknown: true,
      reasons: ["PROVIDER_ORIGIN_NOT_PUBLIC_VERIFIED"],
      providerOrigin: true,
    };
  }

  if (isStale(checkedAt, staleDays)) {
    return {
      channel: ch,
      value: v,
      status: PUBLIC_CONTACT_VERIFICATION_STATUS.STALE_OR_CONFLICTED,
      kind: PUBLIC_CONTACT_CHANNEL_KIND.UNKNOWN,
      personIdentified: Boolean(name && name.split(/\s+/).length >= 2),
      organizationEstablished: Boolean(org),
      sourceUrl: src,
      checkedAt: checkedAt ? new Date(checkedAt).toISOString() : null,
      permittedUseUnknown: false,
      reasons: ["STALE_VERIFICATION"],
    };
  }

  const personOk = Boolean(name && name.split(/\s+/).length >= 2);
  const orgOk = Boolean(org);
  const reasons = [];

  if (ch === "email") {
    const roleMailbox = isRoleOrGenericMailbox(v);
    const personLocalMatch =
      !roleMailbox && personOk && emailLocalPartMatchesPerson(v, name);
    // Shared inbox / non-person local → organization route, never DIRECT_WORK_EMAIL.
    const kind =
      roleMailbox || (personOk && !personLocalMatch)
        ? PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_EMAIL_ROUTE
        : PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL;

    // Guessed-format guard: no source + not already marked publicly verified.
    if (!src && !/PUBLICLY_VERIFIED|VERIFIED_DIRECT|VERIFIED_ORGANIZATION/i.test(verificationHint)) {
      reasons.push("MISSING_SOURCE_URL");
      return {
        channel: "email",
        value: v,
        status: PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED,
        kind,
        personIdentified: personOk,
        organizationEstablished: orgOk,
        sourceUrl: src,
        checkedAt: checkedAt ? new Date(checkedAt).toISOString() : null,
        permittedUseUnknown: true,
        reasons,
      };
    }

    if (!personOk) reasons.push("PERSON_NOT_IDENTIFIED");
    if (!orgOk) reasons.push("ORGANIZATION_NOT_ESTABLISHED");

    if (roleMailbox || (personOk && !personLocalMatch)) {
      if (roleMailbox) reasons.push("ROLE_OR_GENERIC_MAILBOX");
      else reasons.push("LOCAL_PART_NOT_PERSON_MATCH");
      return {
        channel: "email",
        value: v,
        status: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE,
        kind: PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_EMAIL_ROUTE,
        personIdentified: personOk,
        organizationEstablished: orgOk,
        sourceUrl: src,
        checkedAt: checkedAt
          ? new Date(checkedAt).toISOString()
          : new Date().toISOString(),
        permittedUseUnknown: false,
        reasons: reasons.length ? reasons : ["ORGANIZATION_EMAIL_ROUTE"],
      };
    }

    if (!personOk || !orgOk) {
      return {
        channel: "email",
        value: v,
        status: PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED,
        kind,
        personIdentified: personOk,
        organizationEstablished: orgOk,
        sourceUrl: src,
        checkedAt: checkedAt ? new Date(checkedAt).toISOString() : null,
        permittedUseUnknown: true,
        reasons,
      };
    }

    return {
      channel: "email",
      value: v,
      status: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT,
      kind: PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL,
      personIdentified: true,
      organizationEstablished: true,
      sourceUrl: src,
      checkedAt: checkedAt
        ? new Date(checkedAt).toISOString()
        : new Date().toISOString(),
      permittedUseUnknown: false,
      reasons: [],
    };
  }

  // phone — org switchboard/main line is never a verified personal direct phone.
  const hasExtension =
    Array.isArray(contact.phoneExtensions) && contact.phoneExtensions.length > 0;
  const phoneLabel = text(
    contact.phoneType || contact.phoneKind || contact.phoneLabel || "",
  );
  const labeledOrg =
    /switchboard|main.?line|reception|front.?desk|organization.?route|company.?number|centralita|conmutador/i.test(
      phoneLabel,
    );
  const labeledDirect =
    /direct|mobile|cell|personal|extension|desk.?line/i.test(phoneLabel);
  const kind =
    labeledOrg && !hasExtension
      ? PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_PHONE_ROUTE
      : hasExtension || labeledDirect
        ? PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_BUSINESS_PHONE
        : personOk && src
          ? PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_BUSINESS_PHONE
          : PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_PHONE_ROUTE;

  if (!src && !/PUBLICLY_VERIFIED|VERIFIED/i.test(verificationHint) && !hasExtension) {
    return {
      channel: "phone",
      value: v,
      status: PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED,
      kind,
      personIdentified: personOk,
      organizationEstablished: orgOk,
      sourceUrl: src,
      checkedAt: checkedAt ? new Date(checkedAt).toISOString() : null,
      permittedUseUnknown: true,
      reasons: ["MISSING_SOURCE_URL"],
    };
  }

  if (!personOk) {
    return {
      channel: "phone",
      value: v,
      status: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE,
      kind: PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_PHONE_ROUTE,
      personIdentified: false,
      organizationEstablished: orgOk,
      sourceUrl: src,
      checkedAt: checkedAt ? new Date(checkedAt).toISOString() : null,
      permittedUseUnknown: false,
      reasons: ["COMPANY_NUMBER_WITHOUT_NAMED_PERSON"],
    };
  }

  if (kind === PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_PHONE_ROUTE) {
    return {
      channel: "phone",
      value: v,
      status: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE,
      kind,
      personIdentified: personOk,
      organizationEstablished: orgOk,
      sourceUrl: src,
      checkedAt: checkedAt
        ? new Date(checkedAt).toISOString()
        : new Date().toISOString(),
      permittedUseUnknown: false,
      reasons: ["ORGANIZATION_ROUTE_LABEL"],
    };
  }

  return {
    channel: "phone",
    value: v,
    status: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT,
    kind,
    personIdentified: personOk,
    organizationEstablished: orgOk,
    sourceUrl: src,
    checkedAt: checkedAt
      ? new Date(checkedAt).toISOString()
      : new Date().toISOString(),
    permittedUseUnknown: false,
    reasons: hasExtension ? ["OFFICE_WITH_EXTENSION"] : [],
  };
}

/**
 * Full person+opportunity verification snapshot (non-mutating).
 */
export function assessPublicContactVerification(opportunity = {}, contact = null) {
  const pc = contact || opportunity.primaryContact || {};
  const name =
    text(pc.name) ||
    text(opportunity.contactName) ||
    text(opportunity.primaryContactName);
  const org =
    text(pc.organization) ||
    text(opportunity.organizationName) ||
    text(opportunity.displayCompanyName);
  const email = extractEmail(pc, opportunity);
  const phone = extractPhone(pc, opportunity);
  const emailClass = classifyPublicContactChannel({
    channel: "email",
    value: email,
    contact: pc,
    opportunity,
    personName: name,
    organizationName: org,
  });
  const phoneClass = classifyPublicContactChannel({
    channel: "phone",
    value: phone,
    contact: pc,
    opportunity,
    personName: name,
    organizationName: org,
  });

  const personKey = publicContactPersonKey({ name, organization: org });

  return {
    version: GDI_PUBLIC_CONTACT_VERIFICATION_V1,
    personIdentityKey: personKey,
    personName: name || null,
    organizationName: org || null,
    email: emailClass,
    phone: phoneClass,
    hasVerifiedDirectEmail:
      emailClass.status === PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT,
    hasVerifiedDirectPhone:
      phoneClass.status === PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT,
    hasVerifiedOrgEmailRoute:
      emailClass.status ===
      PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE,
    hasVerifiedOrgPhoneRoute:
      phoneClass.status ===
      PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE,
    surfeEmailEligible:
      emailClass.status === PUBLIC_CONTACT_VERIFICATION_STATUS.NOT_FOUND ||
      emailClass.status === PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED ||
      emailClass.status === PUBLIC_CONTACT_VERIFICATION_STATUS.STALE_OR_CONFLICTED,
    surfePhoneEligible:
      phoneClass.status === PUBLIC_CONTACT_VERIFICATION_STATUS.NOT_FOUND ||
      phoneClass.status === PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED ||
      phoneClass.status === PUBLIC_CONTACT_VERIFICATION_STATUS.STALE_OR_CONFLICTED,
    // FullEnrich only after genuine Surfe email miss — never auto here.
    fullEnrichEmailEligible: false,
  };
}

/**
 * Channel-specific enrichment decision (no paid calls).
 */
export function decideEnrichmentChannels(assessment) {
  const a = assessment || {};
  return {
    workEmail: a.hasVerifiedDirectEmail
      ? {
          action: "USE_PUBLIC_VERIFIED",
          surfeAvailable: false,
          fullEnrichAvailable: false,
        }
      : a.hasVerifiedOrgEmailRoute
        ? {
            action: "USE_ORG_ROUTE_OR_SURFE_FOR_DIRECT",
            surfeAvailable: true,
            fullEnrichAvailable: false,
            note: "Organization mailbox is not a verified personal work email.",
          }
        : {
            action: "OFFER_SURFE_ON_DEMAND",
            surfeAvailable: true,
            fullEnrichAvailable: false,
            note: "FullEnrich only after genuine Surfe email miss + separate flags.",
          },
    telephone: a.hasVerifiedDirectPhone
      ? {
          action: "USE_PUBLIC_VERIFIED",
          surfeAvailable: false,
          fullEnrichPhoneAvailable: false,
        }
      : a.hasVerifiedOrgPhoneRoute
        ? {
            action: "LABEL_ORG_ROUTE",
            surfeAvailable: true,
            fullEnrichPhoneAvailable: false,
            note: "Company number is not a verified personal direct line.",
          }
        : {
            action: "OFFER_SURFE_ON_DEMAND",
            surfeAvailable: true,
            fullEnrichPhoneAvailable: false,
          },
  };
}
