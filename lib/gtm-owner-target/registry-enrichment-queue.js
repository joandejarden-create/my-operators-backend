/**
 * Build human/agent registry enrichment work queue from GTM owner targets + properties.
 */
import { summarizePropertyFootprint } from "./cala-footprint.js";
import {
  resolveRegistryForCountry,
  inferEntityBridgeStrategy,
  entitySearchHints,
} from "./registry-contact-config.js";
import { isVerifiedOwnerContact } from "./registry-contact-verification.js";
import {
  resolveMexicoRegistryPath,
} from "./adapters/mx-corporate-web-first.js";
import { mxSigerSearchTerms } from "./adapters/mx-siger-registry.js";
import {
  resolveCorporateWebSeed,
} from "./adapters/corporate-web-seeds-resolver.js";
import { pickRecommendedOutreachContact } from "./adapters/mx-corporate-web-seeds.js";

/**
 * @param {object} owner
 * @param {object[]} properties
 * @param {object} [options]
 * @param {boolean} [options.includeVerified=false]
 */
export function buildRegistryEnrichmentQueueItem(owner, properties, options = {}) {
  const footprint = summarizePropertyFootprint(properties || []);
  const primaryCountry =
    footprint.calaCountries?.[0]?.country ||
    parsePrimaryCountry(owner.countriesSummary);

  const registry = resolveRegistryForCountry(primaryCountry);
  const bridgeStrategy = inferEntityBridgeStrategy(owner.ownerName);
  const hasEmail = Boolean(String(owner.primaryContactEmail || "").trim());
  const hasVerified = Boolean(owner.hasVerifiedContact);

  let verificationStatus = "needs_registry";
  if (hasVerified) verificationStatus = "verified";
  else if (hasEmail) verificationStatus = "needs_verification";
  else if (!registry) verificationStatus = "needs_manual_country";
  else if (bridgeStrategy === "opaque_spv" || bridgeStrategy === "rnt_bridge") {
    verificationStatus = "needs_entity_bridge";
  }

  const sampleProperties = (properties || [])
    .slice(0, 5)
    .map((p) => [p.buildingName, p.city, p.country].filter(Boolean).join(" — "))
    .filter(Boolean);

  const corpSeed = resolveCorporateWebSeed(owner.ownerName);
  const mxRecommendedContact = corpSeed ? pickRecommendedOutreachContact(corpSeed) : null;
  const mxRecommended = mxRecommendedContact
    ? {
        name: mxRecommendedContact.name,
        title: mxRecommendedContact.title,
        email: mxRecommendedContact.email || "",
        outreachRole: mxRecommendedContact.outreachRole || "",
      }
    : null;

  let enrichmentPriority = 3;
  if (owner.priorityTier === "A") enrichmentPriority = 1;
  else if (owner.priorityTier === "B") enrichmentPriority = 2;
  if (bridgeStrategy !== "direct_entity") enrichmentPriority = Math.min(3, enrichmentPriority + 1);

  let nextAction = "registry_lookup_legal_rep";
  if (verificationStatus === "verified") nextAction = "ready_for_outreach";
  else if (verificationStatus === "needs_entity_bridge") {
    nextAction =
      primaryCountry === "Mexico" ? "resolve_entity_via_hotel_website_then_siger" : "resolve_entity_via_tourism_registry";
  } else if (primaryCountry === "Mexico") {
    nextAction =
      resolveMexicoRegistryPath(bridgeStrategy) === "corporate_web_first"
        ? "corporate_web_research"
        : "hotel_website_then_corporate";
  }

  const item = {
    id: owner.id || null,
    ownerName: owner.ownerName,
    priorityTier: owner.priorityTier || "C",
    icpSegment: owner.icpSegment || "",
    strikeList: Boolean(owner.strikeList),
    calaPropertyCount: owner.calaPropertyCount ?? footprint.calaPropertyCount,
    propertyCount: owner.propertyCount ?? footprint.totalPropertyCount,
    countriesSummary: owner.countriesSummary || footprint.calaCountriesSummary,
    primaryCountry: primaryCountry || "",
    registrySystem: registry?.id || "",
    registryLabel: registry?.label || "",
    commercialRegistryUrl: registry?.commercialRegistryUrl || "",
    tourismRegistryUrl: registry?.tourismRegistryUrl || "",
    taxRegistryUrl: registry?.taxRegistryUrl || "",
    entityIdLabel: registry?.entityIdLabel || "",
    entitySearchName: owner.ownerName,
    bridgeStrategy,
    entitySearchHints: entitySearchHints(owner.ownerName, bridgeStrategy),
    registryLookupNotes: registry?.lookupNotes || [],
    registryPrimaryPath: primaryCountry === "Mexico" ? resolveMexicoRegistryPath(bridgeStrategy) : null,
    sigerSearchTerms: primaryCountry === "Mexico" ? mxSigerSearchTerms(owner.ownerName) : [],
    corporateSeedSlug: corpSeed?.slug || "",
    corporateWebsite: corpSeed?.website || "",
    corporateEntityType: corpSeed?.entityType || "",
    hasKnownCorporateContact: Boolean(corpSeed?.knownContacts?.length),
    recommendedOutreach: mxRecommended,
    sampleProperties,
    primaryContactName: owner.primaryContactEmail ? owner.primaryContactName : "",
    primaryContactEmail: owner.primaryContactEmail || "",
    hasVerifiedContact: hasVerified,
    verificationStatus,
    enrichmentPriority,
    nextAction,
  };

  if (options.includeVerified === false && item.verificationStatus === "verified") {
    return null;
  }
  return item;
}

/**
 * @param {string} summary
 */
function parsePrimaryCountry(summary) {
  const raw = String(summary || "").split(/[,;|]/)[0]?.trim();
  return raw || "";
}

/**
 * @param {object[]} owners
 * @param {Map<string, object[]>} propertiesByOwnerId
 * @param {object} [options]
 */
export function buildRegistryEnrichmentQueue(owners, propertiesByOwnerId, options = {}) {
  const limit = options.limit ?? null;
  const tierFilter = options.tierFilter || null;
  const strikeOnly = options.strikeOnly !== false;

  /** @type {ReturnType<buildRegistryEnrichmentQueueItem>[]} */
  const items = [];

  for (const owner of owners) {
    if (strikeOnly && !owner.strikeList && !owner.forceQueue) continue;
    if (tierFilter && owner.priorityTier !== tierFilter) continue;

    const properties = propertiesByOwnerId.get(owner.id) || [];
    const item = buildRegistryEnrichmentQueueItem(owner, properties, options);
    if (item) items.push(item);
  }

  items.sort(
    (a, b) =>
      a.enrichmentPriority - b.enrichmentPriority ||
      (b.calaPropertyCount || 0) - (a.calaPropertyCount || 0) ||
      String(a.ownerName).localeCompare(String(b.ownerName))
  );

  if (limit && limit > 0) return items.slice(0, limit);
  return items;
}

/**
 * Summarize queue for report header.
 * @param {ReturnType<buildRegistryEnrichmentQueueItem>[]} items
 */
export function summarizeRegistryQueue(items) {
  const byStatus = {};
  const byCountry = {};
  for (const item of items) {
    byStatus[item.verificationStatus] = (byStatus[item.verificationStatus] || 0) + 1;
    const c = item.primaryCountry || "Unknown";
    byCountry[c] = (byCountry[c] || 0) + 1;
  }
  return {
    total: items.length,
    byVerificationStatus: byStatus,
    byPrimaryCountry: byCountry,
    priority1: items.filter((i) => i.enrichmentPriority === 1).length,
  };
}

/**
 * @param {object} contact
 * @param {object} calaClass
 */
export function contactPassesVerification(contact, calaClass) {
  return isVerifiedOwnerContact(contact, calaClass);
}
