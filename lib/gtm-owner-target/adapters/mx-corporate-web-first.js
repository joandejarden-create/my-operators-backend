/**
 * Mexico Wave 1 contact path — corporate website / IR / LinkedIn first.
 * SIGER (requires CURP account) and RNT are optional fallbacks only.
 */
import { isMxRntLookupEnabled } from "./mx-rnt-portal-config.js";
import {
  MX_SIGER_URL,
  MX_SAT_RFC_VALIDATOR_URL,
  MX_SIEM_OPEN_DATA_URL,
  mxSigerSearchTerms,
  buildMxSigerSearchPlan,
} from "./mx-siger-registry.js";
import {
  resolveMxCorporateSeed,
  pickRecommendedOutreachContact,
} from "./mx-corporate-web-seeds.js";
import { findCompanyProfileEnrichment } from "../company-profile-enrichments.js";
import { isNamedPersonEmail } from "../registry-contact-verification.js";

/**
 * @typedef {object} MxCorporateWebPlan
 * @property {"corporate_web_first" | "hotel_website_then_corporate"} registryPath
 * @property {string} bridgeStrategy
 * @property {string} ownerName
 * @property {import("./mx-corporate-web-seeds.js").MxCorporateWebSeed | null} seed
 * @property {string} website
 * @property {string} [investorRelationsUrl]
 * @property {string} [managementUrl]
 * @property {string} [entityName]
 * @property {string} [entityType]
 * @property {string[]} targetTitles
 * @property {string[]} manualSteps
 * @property {string[]} fallbackSteps
 * @property {object | null} recommendedContact
 * @property {object | null} property
 * @property {string[]} sigerSearchTerms
 * @property {string} sigerUrl
 * @property {string} siemOpenDataUrl
 */

/**
 * @param {"direct_entity" | "rnt_bridge" | "opaque_spv"} bridgeStrategy
 * @returns {"corporate_web_first" | "hotel_website_then_corporate"}
 */
export function resolveMexicoRegistryPath(bridgeStrategy) {
  if (bridgeStrategy === "direct_entity") return "corporate_web_first";
  return "hotel_website_then_corporate";
}

/**
 * Merge seed website with CoStar company profile enrichment when available.
 * @param {string} ownerName
 * @param {import("./mx-corporate-web-seeds.js").MxCorporateWebSeed | null} seed
 */
function resolveCorporateWebsite(ownerName, seed) {
  if (seed?.website) return seed.website;
  const profile = findCompanyProfileEnrichment(ownerName);
  return profile?.company?.website || "";
}

/**
 * @param {object} queueItem
 * @param {object} [property]
 * @returns {MxCorporateWebPlan}
 */
export function buildMxCorporateWebPlan(queueItem, property) {
  const bridgeStrategy = queueItem.bridgeStrategy || "direct_entity";
  const registryPath = resolveMexicoRegistryPath(bridgeStrategy);
  const ownerName = queueItem.ownerName || queueItem.entitySearchName || "";
  const seed = resolveMxCorporateSeed(ownerName);
  const website = resolveCorporateWebsite(ownerName, seed);
  const searchTerms = mxSigerSearchTerms(queueItem.entitySearchName || ownerName);
  const recommended = seed ? pickRecommendedOutreachContact(seed) : null;

  /** @type {string[]} */
  const manualSteps = [];

  if (registryPath === "hotel_website_then_corporate") {
    manualSteps.push(
      "Bridge SPV/opaque CoStar owner via hotel operating entity.",
      property?.buildingName
        ? `Open hotel site for "${property.buildingName}" — footer / legal / privacy for razón social.`
        : "Open sample property hotel website — footer for operating entity.",
      "Search corporate site + LinkedIn for that entity's leadership.",
    );
  } else if (seed?.entityType === "public_reit") {
    manualSteps.push(
      website ? `Open corporate site: ${website}` : "Find corporate / IR website.",
      seed.investorRelationsUrl
        ? `Investor relations: ${seed.investorRelationsUrl}`
        : "Locate investor relations or management page.",
      seed.managementUrl ? `Management team: ${seed.managementUrl}` : "Find management / leadership page.",
      `Target titles: ${seed.targetTitles.join(", ")}.`,
      recommended?.email
        ? `Recommended first contact: ${recommended.name} (${recommended.title}) — ${recommended.email}`
        : recommended?.linkedIn
          ? `Recommended first contact: ${recommended.name} — LinkedIn ${recommended.linkedIn}`
          : "Pick CEO, IR, or Director de Desarrollo from management page.",
      "Save proof URL (management/IR page) + corporate email or LinkedIn for import.",
    );
  } else {
    manualSteps.push(
      website ? `Open corporate site: ${website}` : `Google "${ownerName}" + sitio oficial / LinkedIn company.`,
      seed?.managementUrl ? `Team/contact page: ${seed.managementUrl}` : "Find Quiénes somos / Contacto / team page.",
      `Target titles: ${(seed?.targetTitles || ["Director General", "Director Comercial"]).join(", ")}.`,
      "LinkedIn: filter company employees by title; prefer corp email on company domain.",
      "Save proof URL + contact for import (V1R = corp email; V2 = LinkedIn + named exec).",
    );
  }

  /** @type {string[]} */
  const fallbackSteps = [
    "Optional — SIEM bulk CSV name search (no signup): " + MX_SIEM_OPEN_DATA_URL,
    "Optional — SAT RFC validator (public): " + MX_SAT_RFC_VALIDATOR_URL,
    "Skip SIGER registration unless you have a Mexican CURP and need legal-rep proof for V1R.",
    "SIGER (account required): " + MX_SIGER_URL,
  ];

  if (isMxRntLookupEnabled()) {
    fallbackSteps.unshift("Optional RNT consulta if portal loads in your browser.");
  } else {
    fallbackSteps.unshift("RNT skipped (portals often down; MX_RNT_LOOKUP_ENABLED=1 to include).");
  }

  if (seed?.researchNotes?.length) {
    manualSteps.push(...seed.researchNotes.map((n) => `Note: ${n}`));
  }

  return {
    registryPath,
    bridgeStrategy,
    ownerName,
    seed,
    website,
    investorRelationsUrl: seed?.investorRelationsUrl || "",
    managementUrl: seed?.managementUrl || "",
    entityName: seed?.entityName || queueItem.entitySearchName || ownerName,
    entityType: seed?.entityType || "private_operator",
    targetTitles: seed?.targetTitles || ["Director General", "Director Comercial"],
    manualSteps,
    fallbackSteps,
    recommendedContact: recommended,
    property: property || null,
    sigerSearchTerms: searchTerms,
    sigerUrl: MX_SIGER_URL,
    siemOpenDataUrl: MX_SIEM_OPEN_DATA_URL,
  };
}

/**
 * @param {object} queueItem
 * @param {object} [property]
 */
export function buildDraftEnrichmentFromCorporateWebPlan(queueItem, property) {
  const plan = buildMxCorporateWebPlan(queueItem, property);
  const recommended = plan.recommendedContact;

  return {
    ownerName: queueItem.ownerName,
    ownerTargetId: queueItem.id || null,
    enrichedAt: null,
    enrichedBy: "mx_corporate_web_plan",
    status: "draft",
    bridgeProperty: property
      ? {
          buildingName: property.buildingName,
          city: property.city,
          country: property.country,
        }
      : null,
    registry: {
      system: "MX_CORPORATE_WEB",
      country: "Mexico",
      entityName: plan.entityName,
      entityId: null,
      entityIdLabel: "RFC",
      legalRepresentative: recommended?.name || null,
      verificationUrl: recommended?.verificationUrl || plan.managementUrl || plan.website || null,
      lookupNotes: [...plan.manualSteps, ...plan.fallbackSteps],
      registryPath: plan.registryPath,
      entityType: plan.entityType,
    },
    contact: {
      name: recommended?.name || null,
      title: recommended?.title || null,
      email: recommended?.email || null,
      phone: recommended?.phone || plan.seed?.phone || null,
      linkedIn: recommended?.linkedIn || null,
      website: plan.website || null,
      verificationTier: recommended?.verificationTier || "V3",
      verificationSource: recommended?.verificationSource || "company_website",
    },
    corporateWebPlan: plan,
    optionalSigerSearchPlan: buildMxSigerSearchPlan(queueItem, property),
  };
}

/**
 * Build import-ready enrichment from a seed's recommended contact.
 * @param {import("./mx-corporate-web-seeds.js").MxCorporateWebSeed} seed
 * @param {object} [options]
 * @param {string} [options.ownerTargetId]
 * @param {string} [options.contactKey] outreachRole or contact name match
 */
export function buildEnrichmentFromSeedContact(seed, options = {}) {
  const contacts = seed.knownContacts || [];
  let contact = contacts.find((c) => c.outreachRole === options.contactKey);
  if (!contact && options.contactKey) {
    contact = contacts.find((c) =>
      c.name.toLowerCase().includes(String(options.contactKey).toLowerCase())
    );
  }
  if (!contact && options.contactKey?.endsWith("_downgrade")) {
    contact = contacts.find((c) => c.outreachRole === options.contactKey);
  }
  if (!contact) contact = pickRecommendedOutreachContact(seed);
  if (!contact) {
    throw new Error(`No known contact for seed ${seed.slug}`);
  }

  const tier =
    contact.verificationTier ||
    (contact.email && isNamedPersonEmail(contact.email, contact.name)
      ? "V1R"
      : contact.linkedIn
        ? "V2"
        : "V3");
  const source = contact.verificationSource || (contact.email ? "company_website" : "linkedin");

  return {
    ownerName: seed.ownerNameMatch[0],
    ownerTargetId: options.ownerTargetId || null,
    enrichedAt: new Date().toISOString().slice(0, 10),
    enrichedBy: options.enrichedBy || "wave1_corporate_web",
    status: "ready",
    registry: {
      system: seed.registrySystem || options.registrySystem || "MX_CORPORATE_WEB",
      country: seed.country || options.country || "Mexico",
      entityName: seed.entityName,
      entityId: null,
      entityIdLabel: seed.entityIdLabel || (seed.country && seed.country !== "Mexico" ? "Registry ID" : "RFC"),
      legalRepresentative: contact.name,
      verificationUrl: contact.verificationUrl,
      lookupNotes: [
        `Wave 1 corporate web path — no SIGER/RNT signup.`,
        `Proof: ${contact.verificationUrl}`,
        ...(seed.researchNotes || []),
      ].join("\n"),
      registryPath: "corporate_web_first",
      entityType: seed.entityType,
    },
    contact: {
      name: contact.name,
      title: contact.title,
      email: contact.email || null,
      phone: contact.phone || null,
      businessPhone: contact.businessPhone || null,
      mobilePhone: contact.mobilePhone || null,
      phoneType: contact.phoneType || null,
      businessPhoneTier: contact.businessPhoneTier || null,
      mobilePhoneTier: contact.mobilePhoneTier || null,
      phoneVerificationTier: contact.phoneVerificationTier || null,
      entitySwitchboardPhone: seed.phone || null,
      linkedIn: contact.linkedIn || null,
      website: seed.website,
      verificationTier: tier,
      verificationSource: source,
    },
    entitySwitchboardPhone: seed.phone || null,
  };
}
