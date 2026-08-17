/**
 * Build branding-decision target rows from GTM Airtable (shared by report + enrichment queue).
 */
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
} from "./field-map.js";
import {
  GTM_CONTACT_TABLE,
  MAP_GTM_CONTACT,
} from "./contact-field-map.js";
import { getGtmAirtableBase } from "./platform-base.js";
import {
  fetchAllGtmProperties,
  groupAirtablePropertiesByOwner,
} from "./properties-read.js";
import { normalizeOwnerKey } from "./normalize.js";
import { pickLeadProperty } from "./owner-lead-asset.js";
import {
  scoreContactForOwnerPrimary,
  classifyContactRelevance,
  buildEnrichmentOwnerIndex,
  buildOwnerTargetIndexes,
} from "./owner-contact-sync.js";
import {
  buildContactCalaMatchContext,
  classifyContactCalaFootprint,
} from "./contact-cala-match.js";
import {
  findCompanyProfileEnrichment,
  COMPANY_PROFILE_ENRICHMENTS,
} from "./company-profile-enrichments.js";
import { isVerifiedOwnerContact } from "./icp-classify.js";
import { resolveContactPhoneFields } from "./registry-phone-verification.js";
import { resolveCorporateWebSeed } from "./adapters/corporate-web-seeds-resolver.js";
import {
  scoreOwnerBrandingIntent,
  buildBrandingOutreachTarget,
  parseDevelopmentCountFromProfileNotes,
} from "./branding-decision-signals.js";
import { isCalaCountry } from "./cala-footprint.js";

const CONTACT_FIELD_SETS = [
  [
    MAP_GTM_CONTACT.name,
    MAP_GTM_CONTACT.email,
    MAP_GTM_CONTACT.phone,
    MAP_GTM_CONTACT.businessPhone,
    MAP_GTM_CONTACT.mobilePhone,
    MAP_GTM_CONTACT.phoneVerificationTier,
    MAP_GTM_CONTACT.company,
    MAP_GTM_CONTACT.title,
    MAP_GTM_CONTACT.linkedIn,
    MAP_GTM_CONTACT.ownerTargets,
    MAP_GTM_CONTACT.contactRelevance,
    MAP_GTM_CONTACT.calaHotelContact,
    MAP_GTM_CONTACT.calaMatchType,
    MAP_GTM_CONTACT.verificationTier,
    MAP_GTM_CONTACT.verificationSource,
    MAP_GTM_CONTACT.verificationUrl,
    MAP_GTM_CONTACT.legalRepresentativeName,
    MAP_GTM_CONTACT.registryEntityName,
    MAP_GTM_CONTACT.website,
  ],
  [
    MAP_GTM_CONTACT.name,
    MAP_GTM_CONTACT.email,
    MAP_GTM_CONTACT.phone,
    MAP_GTM_CONTACT.company,
    MAP_GTM_CONTACT.title,
    MAP_GTM_CONTACT.linkedIn,
    MAP_GTM_CONTACT.ownerTargets,
    MAP_GTM_CONTACT.contactRelevance,
    MAP_GTM_CONTACT.calaHotelContact,
    MAP_GTM_CONTACT.calaMatchType,
    MAP_GTM_CONTACT.verificationTier,
    MAP_GTM_CONTACT.verificationSource,
    MAP_GTM_CONTACT.verificationUrl,
    MAP_GTM_CONTACT.legalRepresentativeName,
    MAP_GTM_CONTACT.registryEntityName,
    MAP_GTM_CONTACT.website,
  ],
];

async function loadOwnerTargets(base) {
  const records = await base(GTM_OWNER_TARGET_TABLES.ownerTargets)
    .select({
      fields: [
        MAP_GTM_OWNER_TARGET.ownerName,
        MAP_GTM_OWNER_TARGET.priorityTier,
        MAP_GTM_OWNER_TARGET.icpSegment,
        MAP_GTM_OWNER_TARGET.calaPropertyCount,
        MAP_GTM_OWNER_TARGET.countriesSummary,
        MAP_GTM_OWNER_TARGET.primaryContactName,
        MAP_GTM_OWNER_TARGET.primaryContactEmail,
        MAP_GTM_OWNER_TARGET.primaryContactPhone,
        MAP_GTM_OWNER_TARGET.dealTrigger,
        MAP_GTM_OWNER_TARGET.strikeList,
      ],
    })
    .all();
  return records.map((rec) => ({
    id: rec.id,
    ownerName: rec.fields[MAP_GTM_OWNER_TARGET.ownerName],
    priorityTier: rec.fields[MAP_GTM_OWNER_TARGET.priorityTier],
    icpSegment: rec.fields[MAP_GTM_OWNER_TARGET.icpSegment],
    calaPropertyCount: rec.fields[MAP_GTM_OWNER_TARGET.calaPropertyCount],
    countriesSummary: rec.fields[MAP_GTM_OWNER_TARGET.countriesSummary],
    primaryContactName: rec.fields[MAP_GTM_OWNER_TARGET.primaryContactName],
    primaryContactEmail: rec.fields[MAP_GTM_OWNER_TARGET.primaryContactEmail],
    primaryContactPhone: rec.fields[MAP_GTM_OWNER_TARGET.primaryContactPhone],
    dealTrigger: rec.fields[MAP_GTM_OWNER_TARGET.dealTrigger],
    strikeList: Boolean(rec.fields[MAP_GTM_OWNER_TARGET.strikeList]),
  }));
}

async function loadContacts(base) {
  let lastError = null;
  for (const fields of CONTACT_FIELD_SETS) {
    try {
      return await base(GTM_CONTACT_TABLE).select({ fields }).all();
    } catch (err) {
      lastError = err;
      if (!String(err.message || err).includes("Unknown field")) throw err;
    }
  }
  throw lastError;
}

function getEntitySwitchboardFromProfile(profile, ownerName = "") {
  if (profile) {
    const fromPrimary = profile.ownerTarget?.primaryContact?.phone;
    if (fromPrimary) return String(fromPrimary).trim();
    const notes = String(profile.company?.internalNotes || "");
    const match = notes.match(/Company phone:\s*([^\n.]+)/i);
    if (match) return match[1].trim();
  }
  const seed = resolveCorporateWebSeed(ownerName);
  return seed?.phone ? String(seed.phone).trim() : "";
}

function resolveOwnerContact(ownerRec, contactRecords, matchContext, profile) {
  const ownerId = ownerRec.id;
  /** @type {object[]} */
  const candidates = [];

  for (const rec of contactRecords) {
    const ownerLinks = rec.fields[MAP_GTM_CONTACT.ownerTargets] || [];
    if (!ownerLinks.includes(ownerId)) continue;
    const calaClass = classifyContactCalaFootprint(rec.fields, matchContext);
    candidates.push({
      name: rec.fields[MAP_GTM_CONTACT.name],
      email: rec.fields[MAP_GTM_CONTACT.email],
      phone: rec.fields[MAP_GTM_CONTACT.phone],
      businessPhone: rec.fields[MAP_GTM_CONTACT.businessPhone],
      mobilePhone: rec.fields[MAP_GTM_CONTACT.mobilePhone],
      phoneVerificationTier: rec.fields[MAP_GTM_CONTACT.phoneVerificationTier],
      linkedIn: rec.fields[MAP_GTM_CONTACT.linkedIn],
      title: rec.fields[MAP_GTM_CONTACT.title],
      verificationTier: rec.fields[MAP_GTM_CONTACT.verificationTier],
      verificationSource: rec.fields[MAP_GTM_CONTACT.verificationSource],
      verificationUrl: rec.fields[MAP_GTM_CONTACT.verificationUrl],
      legalRepresentativeName: rec.fields[MAP_GTM_CONTACT.legalRepresentativeName],
      registryEntityName: rec.fields[MAP_GTM_CONTACT.registryEntityName],
      website: rec.fields[MAP_GTM_CONTACT.website],
      contactRelevance: classifyContactRelevance(rec.fields, calaClass),
      calaHotelContact: rec.fields[MAP_GTM_CONTACT.calaHotelContact],
      calaMatchType: calaClass.matchType,
      hasVerifiedContact: isVerifiedOwnerContact(rec.fields, calaClass),
      score: scoreContactForOwnerPrimary(rec.fields, calaClass),
    });
  }

  candidates.sort((a, b) => (b.score || 0) - (a.score || 0));
  const best = candidates[0] || null;
  const verified = candidates.find((c) => c.hasVerifiedContact) || null;
  const primary = verified || best;
  const entitySwitchboard = getEntitySwitchboardFromProfile(profile, ownerRec.ownerName);
  const phones = resolveContactPhoneFields({
    phone: primary?.phone,
    businessPhone: primary?.businessPhone,
    mobilePhone: primary?.mobilePhone,
    phoneVerificationTier: primary?.phoneVerificationTier,
    verificationUrl: primary?.verificationUrl,
    entitySwitchboardPhone: entitySwitchboard,
    name: primary?.name,
    country: ownerRec.countriesSummary || "",
  });

  return {
    primaryContactName: primary?.name || ownerRec.primaryContactName || "",
    primaryContactEmail: primary?.email || (primary ? "" : ownerRec.primaryContactEmail) || "",
    primaryContactPhone:
      phones.primaryOutreachPhone ||
      primary?.phone ||
      (primary ? "" : ownerRec.primaryContactPhone) ||
      "",
    businessPhone: phones.businessPhone || primary?.businessPhone || "",
    mobilePhone: phones.mobilePhone || primary?.mobilePhone || "",
    businessPhoneTier: phones.businessPhoneTier || "",
    mobilePhoneTier: phones.mobilePhoneTier || "",
    phoneVerificationTier: phones.phoneVerificationTier || primary?.phoneVerificationTier || "",
    linkedIn: primary?.linkedIn || "",
    verificationTier: primary?.verificationTier || "",
    verificationUrl: primary?.verificationUrl || "",
    website: primary?.website || "",
    hasVerifiedContact: Boolean(verified || (primary && primary.hasVerifiedContact)),
    name: primary?.name || "",
    email: primary?.email || "",
    phone: phones.primaryOutreachPhone || primary?.phone || "",
    linkedContactCount: candidates.length,
  };
}

function filterPropertiesByCountry(properties, countryFilter) {
  if (!countryFilter) return properties;
  const needle = countryFilter.toLowerCase();
  return properties.filter((p) => String(p.country || "").toLowerCase().includes(needle));
}

/**
 * @param {object} [options]
 * @param {string} [options.country]
 * @param {number} [options.minScore]
 * @param {boolean} [options.brandDecisionOnly]
 * @param {boolean} [options.outreachReadyOnly]
 * @param {boolean} [options.preDecisionOnly]
 * @param {boolean} [options.needsEnrichmentOnly]
 */
export async function fetchBrandingDecisionTargetRows(options = {}) {
  const {
    country = null,
    minScore = 25,
    brandDecisionOnly = false,
    outreachReadyOnly = false,
    preDecisionOnly = false,
    needsEnrichmentOnly = false,
  } = options;

  const base = getGtmAirtableBase();
  const [ownerTargets, propertyPayload, contactRecords] = await Promise.all([
    loadOwnerTargets(base),
    fetchAllGtmProperties(),
    loadContacts(base),
  ]);

  const propertyGroups = groupAirtablePropertiesByOwner(propertyPayload.records);
  const propertiesByOwnerKey = new Map(propertyGroups.map((g) => [g.ownerKey, g.properties]));

  const matchContext = buildContactCalaMatchContext({
    ownerGroups: propertyGroups,
    profileEnrichments: COMPANY_PROFILE_ENRICHMENTS,
  });

  const ownerIndexes = buildOwnerTargetIndexes(
    ownerTargets.map((o) => ({ id: o.id, ownerName: o.ownerName }))
  );
  const enrichmentByOwnerId = buildEnrichmentOwnerIndex(COMPANY_PROFILE_ENRICHMENTS, ownerIndexes);

  /** @type {object[]} */
  const rows = [];

  for (const owner of ownerTargets) {
    if ((owner.calaPropertyCount || 0) < 1) continue;
    if (owner.icpSegment === "franchisor_brand" || owner.icpSegment === "broker_advisor" || owner.icpSegment === "skip") {
      continue;
    }

    const ownerKey = normalizeOwnerKey(owner.ownerName);
    let properties = propertiesByOwnerKey.get(ownerKey) || [];
    properties = filterPropertiesByCountry(properties.filter((p) => isCalaCountry(p.country)), country);
    if (country && properties.length === 0) continue;

    const profile = findCompanyProfileEnrichment(owner.ownerName);
    const profileEnrichment = enrichmentByOwnerId.get(owner.id);
    const devCount = parseDevelopmentCountFromProfileNotes(profile?.company?.internalNotes || "");

    const brandingIntent = scoreOwnerBrandingIntent(properties, {
      ownerName: owner.ownerName,
      icpSegment: owner.icpSegment || "",
      developmentPipelineCount: devCount,
    });

    if (brandingIntent.intentScore < minScore) continue;
    if (brandingIntent.primaryDealTrigger === "none_known") continue;

    const contact = resolveOwnerContact(
      owner,
      contactRecords,
      matchContext,
      profileEnrichment || profile
    );
    const target = buildBrandingOutreachTarget(owner, brandingIntent, contact);
    const mxSeed = resolveCorporateWebSeed(owner.ownerName);

    if (outreachReadyOnly && !target.outreachReady) continue;
    if (preDecisionOnly && target.brandDecisionTiming === "post_decision") continue;
    if (preDecisionOnly && target.dealalityFit === "late_for_brand_rfp") continue;
    if (brandDecisionOnly && target.outreachTrack === "integrated_operator_house_brand_only") continue;
    if (needsEnrichmentOnly && target.outreachReady) continue;

    rows.push({
      ...target,
      strikeList: owner.strikeList,
      linkedContactCount: contact.linkedContactCount || 0,
      mxCorporateSeedSlug: mxSeed?.slug || "",
      companyWebsite: profile?.company?.website || mxSeed?.website || "",
    });
  }

  rows.sort(
    (a, b) =>
      Number(b.outreachReady) - Number(a.outreachReady) ||
      b.intentScore - a.intentScore ||
      b.outreachScore - a.outreachScore ||
      String(a.priorityTier).localeCompare(String(b.priorityTier)) ||
      (b.calaPropertyCount || 0) - (a.calaPropertyCount || 0)
  );

  return rows;
}

/**
 * @param {object} row branding target row
 */
export function deriveContactEnrichmentGaps(row) {
  const c = row.contact || {};
  /** @type {string[]} */
  const gaps = [];

  if (row.outreachReady) {
    gaps.push("none_outreach_ready");
    return gaps;
  }

  if (!c.name) gaps.push("missing_contact_name");
  if (!c.email && !c.linkedIn) gaps.push("missing_contact_channel");
  else {
    if (!c.hasVerifiedPersonEmail) {
      if (!c.email) gaps.push("missing_email");
      else if (c.isNonPersonMailbox) gaps.push("non_person_email");
      else if (!c.hasVerifiedPersonEmail) gaps.push("needs_named_email_verification");
    }
    if (!c.linkedIn) gaps.push("missing_linkedin");
    if (!c.hasVerifiedPersonPhone) {
      if (!c.phone && !c.mobilePhone && !c.businessPhone) gaps.push("missing_phone");
      else gaps.push("needs_verified_phone");
    }
  }
  if (!c.hasVerifiedContact && c.linkedIn && c.verificationTier === "V2") {
    gaps.push("linkedin_outreach_v2");
  } else if (!c.hasVerifiedContact) {
    gaps.push("needs_verification_tier");
  }

  return gaps;
}

/**
 * @param {object} row
 */
export function deriveEnrichmentPriority(row) {
  if (row.outreachReady) return "P0_outreach_ready";
  if (row.contact?.verificationTier === "V2" && row.contact?.linkedIn && row.intentScore >= 70) {
    return "P1b_linkedin_ready_tier_a";
  }
  if (row.priorityTier === "A" && row.intentScore >= 70) return "P1_high_intent_tier_a";
  if (row.priorityTier === "A" && row.intentScore >= 45) return "P2_tier_a";
  if (row.intentScore >= 60) return "P3_high_intent";
  if (row.intentScore >= 45) return "P4_medium_intent";
  return "P5_backlog";
}

/**
 * @param {object} row
 */
export function suggestEnrichmentAction(row) {
  if (row.outreachReady) return "Send intro — outreach ready";

  const gaps = deriveContactEnrichmentGaps(row);
  if (gaps.includes("non_person_email")) {
    return "Find named exec email on corp site / IR; downgrade info@/ir@ to V3";
  }
  if (gaps.includes("linkedin_outreach_v2")) {
    return "LinkedIn outreach (V2) — find named email for V1R upgrade";
  }
  if (gaps.includes("missing_linkedin") && gaps.includes("missing_email")) {
    return "Corporate web + LinkedIn company page — CEO/development contact";
  }
  if (gaps.includes("missing_linkedin")) {
    return "LinkedIn search for primary exec; keep named email if present";
  }
  if (gaps.includes("missing_email")) {
    return "Corp website / IR page for named person email";
  }
  if (gaps.includes("needs_verified_phone")) {
    return "IR/management page for direct line or mobile (VP1/VP2)";
  }
  if (row.mxCorporateSeedSlug) {
    return `Review mx seed ${row.mxCorporateSeedSlug}; complete missing fields`;
  }
  return "Manual corp web research + registry optional";
}

/**
 * @param {object} row
 */
export function toEnrichmentQueueItem(row) {
  const top =
    pickLeadProperty(row.topProperties || [], row.ownerName) ||
    row.topProperties?.find((p) => p.brandDecisionEligible !== false) ||
    row.topProperties?.[0];
  const gaps = deriveContactEnrichmentGaps(row);

  return {
    ownerTargetId: row.ownerTargetId,
    ownerName: row.ownerName,
    priorityTier: row.priorityTier,
    icpSegment: row.icpSegment,
    enrichmentPriority: deriveEnrichmentPriority(row),
    outreachReady: row.outreachReady,
    outreachTrack: row.outreachTrack,
    intentScore: row.intentScore,
    outreachScore: row.outreachScore,
    primaryDealTrigger: row.primaryDealTrigger,
    brandDecisionTiming: row.brandDecisionTiming,
    brandDecisionEligiblePropertyCount: row.brandDecisionEligiblePropertyCount || 0,
    houseBrandPropertyCount: row.houseBrandPropertyCount || 0,
    calaPropertyCount: row.calaPropertyCount,
    countriesSummary: row.countriesSummary,
    contactName: row.contact?.name || "",
    contactEmail: row.contact?.email || "",
    contactPhone: row.contact?.phone || "",
    contactLinkedIn: row.contact?.linkedIn || "",
    hasVerifiedPersonEmail: row.contact?.hasVerifiedPersonEmail || false,
    hasVerifiedPersonPhone: row.contact?.hasVerifiedPersonPhone || false,
    hasVerifiedContact: row.contact?.hasVerifiedContact || false,
    contactGaps: gaps.join("|"),
    needsEnrichment: !row.outreachReady,
    linkedContactCount: row.linkedContactCount || 0,
    mxCorporateSeedSlug: row.mxCorporateSeedSlug || "",
    companyWebsite: row.companyWebsite || "",
    topLeadAsset: top?.buildingName || "",
    topLeadBrand: top?.brandAffiliation || "",
    topLeadCity: top?.city || "",
    pitchAngle: row.pitchAngle,
    suggestedAction: suggestEnrichmentAction(row),
    strikeList: row.strikeList || false,
  };
}
