/**
 * Registry-sourced owner contact verification (Bystreet-style identity graph layer).
 * Property → entity (CoStar) → legal rep (public registry) → email/phone (website/LinkedIn).
 */
import {
  MAP_GTM_CONTACT,
  VAL_GTM_CONTACT_VERIFICATION_SOURCE,
  VAL_GTM_CONTACT_VERIFICATION_TIER,
  VAL_GTM_PHONE_VERIFICATION_TIER,
} from "./contact-field-map.js";
import { resolveContactPhoneFields } from "./registry-phone-verification.js";

export { VAL_GTM_CONTACT_VERIFICATION_TIER, VAL_GTM_CONTACT_VERIFICATION_SOURCE };

const BROKER_RE = /\b(broker|brokerage|compass|cushman|cbre|jll|colliers)\b/i;

/** Shared inboxes — not a verified email for a named executive. */
const GENERIC_MAILBOX_LOCAL_RE =
  /^(info|contact|hello|admin|office|reception|reservations|booking|hotel|mail|enquiries|inquiries|general|support|customerservice|sales|marketing|hr|careers|jobs|media|press|webmaster|postmaster|noreply|no-reply|donotreply|soporte|ventas|reservas|contacto|informes)$/i;

/** Role inboxes — valid corp channel for that function, not the person's named mailbox. */
const ROLE_MAILBOX_LOCAL_RE =
  /^(ir|investorrelations|investors|investor\.relations|relaciones\.inversionistas|asg|esg|investor\.relations)$/i;

const IR_TITLE_RE =
  /\b(investor relations|relaciones con inversionistas|relaciones inversionistas|ir\b|asg|esg)\b/i;

/**
 * @param {string} email
 */
export function emailLocalPart(email) {
  return String(email || "")
    .trim()
    .toLowerCase()
    .split("@")[0] || "";
}

/**
 * Generic company switchboard (info@, contact@, etc.) — not a named person.
 * @param {string} email
 */
export function isGenericMailboxEmail(email) {
  const local = emailLocalPart(email).replace(/[._-]/g, "");
  if (!local) return false;
  return GENERIC_MAILBOX_LOCAL_RE.test(local);
}

/**
 * Role-based mailbox (ir@, etc.) — corporate channel, not personal email.
 * @param {string} email
 */
export function isRoleMailboxEmail(email) {
  const local = emailLocalPart(email);
  return ROLE_MAILBOX_LOCAL_RE.test(local);
}

/**
 * Whether local part plausibly belongs to the named contact (e.g. jose.carlos@, granados@).
 * @param {string} email
 * @param {string} contactName
 */
export function isNamedPersonEmail(email, contactName) {
  const em = String(email || "").trim().toLowerCase();
  const name = String(contactName || "").trim();
  if (!em || !name || !em.includes("@")) return false;
  if (isGenericMailboxEmail(em) || isRoleMailboxEmail(em)) return false;

  const local = emailLocalPart(em).replace(/[^a-z0-9]/g, "");
  if (/^[a-z]+[._-][a-z]+/.test(emailLocalPart(em))) return true;

  const tokens = name
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 3 && !/^(de|del|la|los|las|von|van|jr|sr|ii|iii)$/i.test(t));

  return tokens.some((token) => {
    const slice = token.slice(0, Math.min(token.length, 8));
    return slice.length >= 3 && local.includes(slice);
  });
}

/**
 * Role or switchboard mailbox — not a verified personal email (often filtered).
 * @param {string} email
 */
export function isNonPersonMailboxEmail(email) {
  return isGenericMailboxEmail(email) || isRoleMailboxEmail(email);
}

/**
 * Email qualifies for V1R — named person on entity domain only.
 * @param {string} email
 * @param {string} contactName
 */
export function isVerifiedPersonEmail(email, contactName) {
  return isNamedPersonEmail(email, contactName);
}

/**
 * Role mailbox with IR-style title — still not verified for outreach (filtered).
 * @param {string} email
 * @param {string} [title]
 */
export function isRoleChannelEmail(email, title) {
  if (!isRoleMailboxEmail(email)) return false;
  return IR_TITLE_RE.test(String(title || ""));
}

/**
 * Reachable corp email for outreach shortlists (includes role/generic on entity domain).
 * @param {string} email
 * @param {string} [website]
 * @param {string} [entityName]
 */
export function isReachableCorporateEmail(email, website, entityName) {
  const em = String(email || "").trim();
  if (!em) return false;
  return emailMatchesEntityDomain(em, website, entityName);
}

/**
 * @param {string} email
 * @param {string} [website]
 * @param {string} [entityName]
 */
export function emailMatchesEntityDomain(email, website, entityName) {
  const em = String(email || "").trim().toLowerCase();
  if (!em || !em.includes("@")) return false;
  const domain = em.split("@")[1];
  if (!domain || BROKER_RE.test(domain)) return false;

  const web = String(website || "").trim().toLowerCase();
  if (web) {
    try {
      const host = new URL(web.startsWith("http") ? web : `https://${web}`).hostname.replace(/^www\./, "");
      if (host && (domain === host || domain.endsWith(`.${host}`) || host.endsWith(domain))) return true;
    } catch {
      /* ignore invalid URL */
    }
  }

  const entityTokens = String(entityName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 4 && !/^(grupo|hotel|hoteles|sa|cv|de|the|and)$/.test(t));
  if (entityTokens.some((t) => domain.includes(t.slice(0, Math.min(t.length, 8))))) return true;
  return false;
}

/**
 * CoStar contact export path (existing V1).
 * @param {object} contact
 * @param {object} [calaClass]
 */
export function isCostarVerifiedOwnerContact(contact, calaClass = {}) {
  const relevance = String(contact.contactRelevance || "");
  const matchType = String(calaClass.matchType || contact.calaMatchType || "");
  const calaYes =
    calaClass.calaHotelContact === "yes" ||
    contact.calaHotelContact === "yes" ||
    contact[MAP_GTM_CONTACT.calaHotelContact] === "yes";
  const hasEmail = Boolean(String(contact.email || contact[MAP_GTM_CONTACT.email] || "").trim());

  if (!hasEmail) return false;
  if (relevance === "broker") return false;
  if (!calaYes) return false;
  if (!["owner_exact", "alias_exact", "loose_exact"].includes(matchType)) return false;
  return relevance === "hospitality" || relevance === "" || relevance === "unknown";
}

/**
 * Public registry legal rep + provenance (V1R / V2).
 * @param {object} contact
 * @param {object} [calaClass]
 */
export function isRegistryVerifiedOwnerContact(contact, calaClass = {}) {
  const relevance = String(contact.contactRelevance || contact[MAP_GTM_CONTACT.contactRelevance] || "");
  const calaYes =
    calaClass.calaHotelContact === "yes" ||
    contact.calaHotelContact === "yes" ||
    contact[MAP_GTM_CONTACT.calaHotelContact] === "yes";
  const tier = String(contact.verificationTier || contact[MAP_GTM_CONTACT.verificationTier] || "");
  const source = String(contact.verificationSource || contact[MAP_GTM_CONTACT.verificationSource] || "");
  const verificationUrl = String(contact.verificationUrl || contact[MAP_GTM_CONTACT.verificationUrl] || "").trim();
  const legalRep = String(
    contact.legalRepresentativeName || contact[MAP_GTM_CONTACT.legalRepresentativeName] || ""
  ).trim();
  const entityName = String(contact.registryEntityName || contact[MAP_GTM_CONTACT.registryEntityName] || "").trim();
  const email = String(contact.email || contact[MAP_GTM_CONTACT.email] || "").trim();
  const linkedIn = String(contact.linkedIn || contact[MAP_GTM_CONTACT.linkedIn] || "").trim();
  const contactName = String(contact.name || contact[MAP_GTM_CONTACT.name] || legalRep || "").trim();
  const title = String(contact.title || contact[MAP_GTM_CONTACT.title] || "").trim();

  if (relevance === "broker") return false;
  if (!calaYes) return false;
  if (!verificationUrl) return false;
  if (!legalRep && !entityName) return false;
  if (source !== "public_registry" && source !== "company_website" && tier !== "V1R" && tier !== "V2") {
    if (source !== "public_registry") return false;
  }

  if (tier === "V1R" || (source === "public_registry" && email)) {
    if (!email) return false;
    const website = contact.website || contact[MAP_GTM_CONTACT.website];
    if (!emailMatchesEntityDomain(email, website, entityName || contact.company)) return false;
    if (isNonPersonMailboxEmail(email)) return false;
    if (isNamedPersonEmail(email, contactName)) {
      return relevance === "hospitality" || relevance === "" || relevance === "unknown";
    }
    return false;
  }

  if (tier === "V2" && linkedIn && legalRep) {
    return true;
  }

  return false;
}

/**
 * @param {object} contact
 * @param {object} [calaClass]
 */
export function isVerifiedOwnerContact(contact, calaClass = {}) {
  return isCostarVerifiedOwnerContact(contact, calaClass) || isRegistryVerifiedOwnerContact(contact, calaClass);
}

/**
 * @param {object} contact
 * @returns {string | null}
 */
export function resolveVerificationTier(contact) {
  if (isCostarVerifiedOwnerContact(contact)) return "V1";
  if (isRegistryVerifiedOwnerContact(contact)) {
    const tier = String(contact.verificationTier || contact[MAP_GTM_CONTACT.verificationTier] || "");
    if (tier === "V2") return "V2";
    return "V1R";
  }
  const email = String(contact.email || contact[MAP_GTM_CONTACT.email] || "").trim();
  const legalRep = String(
    contact.legalRepresentativeName || contact[MAP_GTM_CONTACT.legalRepresentativeName] || ""
  ).trim();
  if (legalRep && !email) return "V2";
  if (legalRep || contact.registryEntityName) return "V3";
  return null;
}

/**
 * @param {object} enrichment JSON record from data/internal/gtm-registry-enrichments/
 */
export function validateRegistryEnrichmentRecord(enrichment) {
  /** @type {string[]} */
  const failures = [];
  if (!enrichment?.ownerName && !enrichment?.ownerTargetId) {
    failures.push("ownerName or ownerTargetId required");
  }
  if (!enrichment?.registry?.system) failures.push("registry.system required");
  if (!enrichment?.registry?.entityName) failures.push("registry.entityName required");
  if (!enrichment?.registry?.verificationUrl) failures.push("registry.verificationUrl required");
  if (!enrichment?.contact?.name && !enrichment?.registry?.legalRepresentative) {
    failures.push("contact.name or registry.legalRepresentative required");
  }

  const tier = enrichment.contact?.verificationTier || enrichment.verificationTier;
  if (tier === "V1R") {
    const email = enrichment.contact?.email || "";
    const name = enrichment.contact?.name || enrichment.registry?.legalRepresentative || "";
    if (email && isNonPersonMailboxEmail(email)) {
      failures.push("V1R requires named person email on entity domain — not info@, ir@, or other role/switchboard mailboxes");
    }
    if (email && !isNamedPersonEmail(email, name)) {
      failures.push("email is not a verified named-person address");
    }
  }
  if (tier && !VAL_GTM_CONTACT_VERIFICATION_TIER.includes(tier)) {
    failures.push(`invalid verificationTier: ${tier}`);
  }
  const source = enrichment.contact?.verificationSource || "public_registry";
  if (!VAL_GTM_CONTACT_VERIFICATION_SOURCE.includes(source)) {
    failures.push(`invalid verificationSource: ${source}`);
  }

  const phoneTier = enrichment.contact?.phoneVerificationTier;
  if (phoneTier && !VAL_GTM_PHONE_VERIFICATION_TIER.includes(phoneTier)) {
    failures.push(`invalid phoneVerificationTier: ${phoneTier}`);
  }
  if (phoneTier === "VP1" || phoneTier === "VP2") {
    const hasPhone =
      enrichment.contact?.mobilePhone ||
      enrichment.contact?.businessPhone ||
      enrichment.contact?.phone;
    if (!hasPhone) failures.push(`${phoneTier} requires businessPhone or mobilePhone`);
    if (!enrichment?.registry?.verificationUrl && !enrichment.contact?.verificationUrl) {
      failures.push(`${phoneTier} requires verificationUrl proof`);
    }
  }

  return { ok: failures.length === 0, failures };
}

/**
 * @param {object} enrichment validated registry enrichment record
 */
export function buildContactFieldsFromRegistryEnrichment(enrichment) {
  const reg = enrichment.registry || {};
  const contact = enrichment.contact || {};
  const ownerName = enrichment.ownerName || reg.entityName || "";
  const name = contact.name || reg.legalRepresentative || "";
  const email = contact.email || null;
  const entityName = reg.entityName || ownerName;
  const website = contact.website || reg.website || null;

  let tier = contact.verificationTier || enrichment.verificationTier || null;
  if (!tier) {
    if (email && emailMatchesEntityDomain(email, website, entityName)) {
      if (isNamedPersonEmail(email, name)) tier = "V1R";
      else if (isNonPersonMailboxEmail(email)) tier = "V3";
      else tier = "V3";
    } else if (contact.linkedIn && reg.legalRepresentative) tier = "V2";
    else if (reg.legalRepresentative) tier = "V3";
  } else if (tier === "V1R" && email && isNonPersonMailboxEmail(email)) {
    tier = "V3";
  }

  const notes = [
    `Registry enrichment: ${reg.system || "unknown"} (${reg.country || ""})`,
    reg.entityId ? `${reg.entityIdLabel || "ID"}: ${reg.entityId}` : null,
    reg.verificationUrl ? `Proof: ${reg.verificationUrl}` : null,
    enrichment.enrichedAt ? `Enriched: ${enrichment.enrichedAt}` : null,
    enrichment.enrichedBy ? `By: ${enrichment.enrichedBy}` : null,
  ]
    .filter(Boolean)
    .join("\n");

  const phoneResolution = resolveContactPhoneFields({
    phone: contact.phone,
    businessPhone: contact.businessPhone,
    mobilePhone: contact.mobilePhone,
    phoneType: contact.phoneType,
    businessPhoneTier: contact.businessPhoneTier,
    mobilePhoneTier: contact.mobilePhoneTier,
    phoneVerificationTier: contact.phoneVerificationTier,
    verificationUrl: reg.verificationUrl || contact.verificationUrl,
    name,
    country: reg.country || contact.country,
    entitySwitchboardPhone: contact.entitySwitchboardPhone || reg.entitySwitchboardPhone || enrichment.entitySwitchboardPhone,
  });

  /** @type {Record<string, unknown>} */
  const fields = {
    [MAP_GTM_CONTACT.name]: name,
    [MAP_GTM_CONTACT.company]: entityName,
    [MAP_GTM_CONTACT.title]: contact.title || (reg.legalRepresentative === name ? "Representante Legal" : contact.title) || "Representante Legal",
    [MAP_GTM_CONTACT.email]: email,
    [MAP_GTM_CONTACT.linkedIn]: contact.linkedIn || null,
    [MAP_GTM_CONTACT.website]: website,
    [MAP_GTM_CONTACT.country]: reg.country || contact.country || null,
    [MAP_GTM_CONTACT.specialty]: "Hotel Operation, Owner",
    [MAP_GTM_CONTACT.matchedOwnerName]: ownerName,
    [MAP_GTM_CONTACT.calaHotelContact]: "yes",
    [MAP_GTM_CONTACT.contactRelevance]: "hospitality",
    [MAP_GTM_CONTACT.calaMatchType]: "owner_exact",
    [MAP_GTM_CONTACT.verificationTier]: tier,
    [MAP_GTM_CONTACT.verificationSource]: contact.verificationSource || "public_registry",
    [MAP_GTM_CONTACT.registrySystem]: reg.system || null,
    [MAP_GTM_CONTACT.registryCountry]: reg.country || null,
    [MAP_GTM_CONTACT.registryEntityName]: entityName,
    [MAP_GTM_CONTACT.registryEntityId]: reg.entityId || null,
    [MAP_GTM_CONTACT.legalRepresentativeName]: reg.legalRepresentative || name,
    [MAP_GTM_CONTACT.verificationUrl]: reg.verificationUrl || null,
    [MAP_GTM_CONTACT.verifiedAt]: enrichment.enrichedAt || new Date().toISOString().slice(0, 10),
    [MAP_GTM_CONTACT.sourceFile]: "registry_enrichment",
    [MAP_GTM_CONTACT.outreachStatus]: "researching",
    [MAP_GTM_CONTACT.internalNotes]: notes,
    ...phoneResolution.fields,
  };

  for (const key of Object.keys(fields)) {
    if (fields[key] == null || fields[key] === "") delete fields[key];
  }

  return fields;
}

export function scoreRegistryContactForOwnerPrimary(contact, calaClass = {}) {
  let score = 0;
  const tier = String(contact.verificationTier || contact[MAP_GTM_CONTACT.verificationTier] || "");
  if (tier === "V1R") score += 55;
  else if (tier === "V1") score += 50;
  else if (tier === "V2") score += 35;
  if (contact.verificationUrl || contact[MAP_GTM_CONTACT.verificationUrl]) score += 25;
  if (contact.legalRepresentativeName || contact[MAP_GTM_CONTACT.legalRepresentativeName]) score += 20;
  if (calaClass.calaHotelContact === "yes" || contact.calaHotelContact === "yes") score += 20;
  if (contact.email || contact[MAP_GTM_CONTACT.email]) score += 15;
  if (contact.phone || contact[MAP_GTM_CONTACT.phone]) score += 10;
  if (contact.mobilePhone || contact[MAP_GTM_CONTACT.mobilePhone]) score += 12;
  if (contact.businessPhone || contact[MAP_GTM_CONTACT.businessPhone]) score += 8;
  const phoneTier = String(contact.phoneVerificationTier || contact[MAP_GTM_CONTACT.phoneVerificationTier] || "");
  if (phoneTier === "VP2") score += 20;
  else if (phoneTier === "VP1") score += 15;
  else if (phoneTier === "VP3") score += 3;
  if (String(contact.verificationSource || contact[MAP_GTM_CONTACT.verificationSource]) === "public_registry") {
    score += 15;
  }
  return score;
}

/**
 * @param {Record<string, unknown>} fields Airtable contact fields keyed by MAP_GTM_CONTACT
 */
export function registryContactDedupeKey(fields) {
  const email = String(fields[MAP_GTM_CONTACT.email] || "")
    .trim()
    .toLowerCase();
  if (email) return `registry:email:${email}`;

  const entityId = String(fields[MAP_GTM_CONTACT.registryEntityId] || "").trim().toLowerCase();
  const entityName = String(fields[MAP_GTM_CONTACT.registryEntityName] || fields[MAP_GTM_CONTACT.company] || "")
    .trim()
    .toLowerCase();
  const name = String(fields[MAP_GTM_CONTACT.name] || "").trim().toLowerCase();
  const entity = entityId || entityName;
  if (entity && name) return `registry:${entity}:${name}`;
  if (entity) return `registry:${entity}`;
  return "";
}
