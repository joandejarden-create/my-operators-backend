/**
 * Link hospitality Contacts → Owner Targets and populate primary outreach fields.
 *
 * Usage:
 *   node scripts/ensure-gtm-owner-contact-links.mjs --apply
 *   node scripts/sync-gtm-owner-target-contacts.mjs
 *   node scripts/sync-gtm-owner-target-contacts.mjs --apply
 *   node scripts/sync-gtm-owner-target-contacts.mjs --apply --tier=A
 *   node scripts/sync-gtm-owner-target-contacts.mjs --apply --overwrite
 *
 * Report: reports/gtm-owner-target-contact-sync.json
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
} from "../lib/gtm-owner-target/field-map.js";
import {
  GTM_CONTACT_TABLE,
  MAP_GTM_CONTACT,
  VAL_GTM_CONTACT_RELEVANCE,
} from "../lib/gtm-owner-target/contact-field-map.js";
import { GTM_COMPANY_TABLE, MAP_GTM_COMPANY } from "../lib/gtm-owner-target/company-field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import {
  fetchAllGtmProperties,
  groupAirtablePropertiesByOwner,
} from "../lib/gtm-owner-target/properties-read.js";
import { COMPANY_PROFILE_ENRICHMENTS } from "../lib/gtm-owner-target/company-profile-enrichments.js";
import {
  buildContactCalaMatchContext,
  classifyContactCalaFootprint,
} from "../lib/gtm-owner-target/contact-cala-match.js";
import {
  buildOwnerTargetIndexes,
  resolveLabelToOwnerTargets,
  classifyContactRelevance,
  pickPrimaryContact,
  scoreContactForOwnerPrimary,
  buildEnrichmentOwnerIndex,
} from "../lib/gtm-owner-target/owner-contact-sync.js";
import { pickPrimaryOutreachPhone } from "../lib/gtm-owner-target/registry-phone-verification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT = join(__dirname, "..", "reports", "gtm-owner-target-contact-sync.json");
const APPLY = process.argv.includes("--apply");
const OVERWRITE = process.argv.includes("--overwrite");
const tierArg = process.argv.find((a) => a.startsWith("--tier="));
const TIER_FILTER = tierArg ? tierArg.split("=")[1].toUpperCase() : null;

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

function appendNotes(existing, addition) {
  const prev = String(existing || "").trim();
  const next = String(addition || "").trim();
  if (!next) return prev || null;
  if (!prev) return next;
  if (prev.includes(next)) return prev;
  return `${prev}\n\n${next}`;
}

function isEmpty(value) {
  return value == null || String(value).trim() === "";
}

function namesRoughlyMatch(a, b) {
  const na = String(a || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  const nb = String(b || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  if (!na || !nb) return false;
  if (na === nb) return true;
  return na.includes(nb) || nb.includes(na);
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const contactFields = [
    MAP_GTM_CONTACT.name,
    MAP_GTM_CONTACT.email,
    MAP_GTM_CONTACT.phone,
    MAP_GTM_CONTACT.businessPhone,
    MAP_GTM_CONTACT.mobilePhone,
    MAP_GTM_CONTACT.phoneVerificationTier,
    MAP_GTM_CONTACT.verificationUrl,
    MAP_GTM_CONTACT.company,
    MAP_GTM_CONTACT.title,
    MAP_GTM_CONTACT.specialty,
    MAP_GTM_CONTACT.country,
    MAP_GTM_CONTACT.sourceFile,
    MAP_GTM_CONTACT.matchedOwnerName,
    MAP_GTM_CONTACT.verificationTier,
    MAP_GTM_CONTACT.legalRepresentativeName,
    MAP_GTM_CONTACT.calaHotelContact,
    MAP_GTM_CONTACT.contactRelevance,
  ];

  const ownerFields = [
    MAP_GTM_OWNER_TARGET.ownerName,
    MAP_GTM_OWNER_TARGET.priorityTier,
    MAP_GTM_OWNER_TARGET.primaryContactName,
    MAP_GTM_OWNER_TARGET.primaryContactEmail,
    MAP_GTM_OWNER_TARGET.primaryContactPhone,
    MAP_GTM_OWNER_TARGET.pitchAngle,
    MAP_GTM_OWNER_TARGET.contactPath,
    MAP_GTM_OWNER_TARGET.outreachStatus,
    MAP_GTM_OWNER_TARGET.pitchStatus,
    MAP_GTM_OWNER_TARGET.nextAction,
    MAP_GTM_OWNER_TARGET.internalNotes,
    MAP_GTM_OWNER_TARGET.ownerType,
  ];

  const [{ records: propertyRecords }, companyRecords, ownerRecords, contactRecords] = await Promise.all([
    fetchAllGtmProperties(),
    base(GTM_COMPANY_TABLE).select({ fields: [MAP_GTM_COMPANY.company] }).all(),
    base(GTM_OWNER_TARGET_TABLES.ownerTargets).select({ fields: ownerFields }).all(),
    base(GTM_CONTACT_TABLE).select({ fields: contactFields }).all(),
  ]);

  const ownerGroups = groupAirtablePropertiesByOwner(propertyRecords);
  const matchContext = buildContactCalaMatchContext({
    ownerGroups,
    companyNames: companyRecords.map((r) => String(r.fields[MAP_GTM_COMPANY.company] || "")),
    profileEnrichments: COMPANY_PROFILE_ENRICHMENTS,
  });

  const ownerIndexes = buildOwnerTargetIndexes(
    ownerRecords.map((r) => ({
      id: r.id,
      ownerName: String(r.fields[MAP_GTM_OWNER_TARGET.ownerName] || ""),
    }))
  );
  const enrichmentByOwnerId = buildEnrichmentOwnerIndex(COMPANY_PROFILE_ENRICHMENTS, ownerIndexes);

  /** @type {Map<string, { contact: object, calaClass: object, relevance: string, id: string }[]>} */
  const contactsByOwnerId = new Map();

  /** @type {{ id: string, fields: Record<string, unknown> }[]} */
  const contactUpdates = [];

  for (const rec of contactRecords) {
    const contact = {
      id: rec.id,
      name: rec.fields[MAP_GTM_CONTACT.name],
      email: rec.fields[MAP_GTM_CONTACT.email],
      phone: rec.fields[MAP_GTM_CONTACT.phone],
      businessPhone: rec.fields[MAP_GTM_CONTACT.businessPhone],
      mobilePhone: rec.fields[MAP_GTM_CONTACT.mobilePhone],
      phoneVerificationTier: rec.fields[MAP_GTM_CONTACT.phoneVerificationTier],
      verificationUrl: rec.fields[MAP_GTM_CONTACT.verificationUrl],
      company: rec.fields[MAP_GTM_CONTACT.company],
      title: rec.fields[MAP_GTM_CONTACT.title],
      specialty: rec.fields[MAP_GTM_CONTACT.specialty],
      country: rec.fields[MAP_GTM_CONTACT.country],
      sourceFile: rec.fields[MAP_GTM_CONTACT.sourceFile],
      verificationTier: rec.fields[MAP_GTM_CONTACT.verificationTier],
      legalRepresentativeName: rec.fields[MAP_GTM_CONTACT.legalRepresentativeName],
      matchedOwnerName: rec.fields[MAP_GTM_CONTACT.matchedOwnerName],
    };
    const calaClass = classifyContactCalaFootprint(contact, matchContext);
    const relevance = classifyContactRelevance(contact, calaClass);
    const ownerResolveLabel =
      contact.sourceFile === "registry_enrichment" && contact.matchedOwnerName
        ? contact.matchedOwnerName
        : contact.company;
    const ownerHits = resolveLabelToOwnerTargets(
      ownerResolveLabel,
      matchContext.nameAliasIndex,
      ownerIndexes
    );

    const ownerIds = ownerHits.map((h) => h.id);
    /** @type {Record<string, unknown>} */
    const contactFieldUpdates = {};
    if (VAL_GTM_CONTACT_RELEVANCE.includes(relevance)) {
      contactFieldUpdates[MAP_GTM_CONTACT.contactRelevance] = relevance;
    }
    if (ownerIds.length) {
      contactFieldUpdates[MAP_GTM_CONTACT.ownerTargets] = ownerIds;
    }
    if (Object.keys(contactFieldUpdates).length) {
      contactUpdates.push({ id: rec.id, fields: contactFieldUpdates });
    }

    for (const hit of ownerHits) {
      if (!contactsByOwnerId.has(hit.id)) contactsByOwnerId.set(hit.id, []);
      contactsByOwnerId.get(hit.id).push({ contact, calaClass, relevance, id: rec.id });
    }
  }

  /** @type {{ id: string, fields: Record<string, unknown>, ownerName: string }[]} */
  const ownerUpdates = [];
  /** @type {object[]} */
  const plan = [];

  let ownersConsidered = 0;
  for (const rec of ownerRecords) {
    const ownerName = String(rec.fields[MAP_GTM_OWNER_TARGET.ownerName] || "");
    const tier = String(rec.fields[MAP_GTM_OWNER_TARGET.priorityTier] || "");
    if (TIER_FILTER && tier !== TIER_FILTER) continue;
    ownersConsidered++;

    const profile = enrichmentByOwnerId.get(rec.id);
    const matchedContacts = contactsByOwnerId.get(rec.id) || [];
    const hospitalityContacts = matchedContacts.filter((c) => c.relevance === "hospitality");
    const picked = pickPrimaryContact(hospitalityContacts.length ? hospitalityContacts : matchedContacts);

    const enrichmentPc = profile?.ownerTarget?.primaryContact;
    const enrichmentFields = profile?.ownerTarget || null;

    /** @type {Record<string, unknown>} */
    const fields = {};
    let primarySource = "none";

    const setPrimary = (name, email, phone, source, force = false) => {
      if (!name && !email && !phone) return;
      const canWrite =
        force ||
        OVERWRITE ||
        (isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.primaryContactName]) &&
          isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.primaryContactEmail]));
      if (!canWrite) return;
      if (name) fields[MAP_GTM_OWNER_TARGET.primaryContactName] = name;
      if (email) {
        fields[MAP_GTM_OWNER_TARGET.primaryContactEmail] = email;
      } else if (force && !isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.primaryContactEmail])) {
        fields[MAP_GTM_OWNER_TARGET.primaryContactEmail] = "";
      }
      if (phone) {
        fields[MAP_GTM_OWNER_TARGET.primaryContactPhone] = phone;
      } else if (force && !isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.primaryContactPhone])) {
        fields[MAP_GTM_OWNER_TARGET.primaryContactPhone] = "";
      }
      primarySource = source;
    };

    if (picked?.contact) {
      const pickedInner = picked.contact;
      const bestPhone = pickPrimaryOutreachPhone({
        mobilePhone: pickedInner.mobilePhone,
        mobileTier: pickedInner.phoneVerificationTier === "VP2" ? "VP2" : pickedInner.mobilePhoneTier,
        businessPhone: pickedInner.businessPhone,
        businessTier:
          pickedInner.phoneVerificationTier === "VP1"
            ? "VP1"
            : pickedInner.phoneVerificationTier === "VP3"
              ? "VP3"
              : pickedInner.businessPhoneTier,
        phone: pickedInner.phone,
      });
      const registryVerified = ["V1R", "V2"].includes(String(pickedInner.verificationTier || ""));
      const pickedScore = scoreContactForOwnerPrimary(pickedInner, picked.calaClass);
      const currentPrimaryName = String(rec.fields[MAP_GTM_OWNER_TARGET.primaryContactName] || "").trim();
      const currentPrimaryEmail = String(rec.fields[MAP_GTM_OWNER_TARGET.primaryContactEmail] || "").trim();
      const currentMatch = matchedContacts.find(
        (m) =>
          namesRoughlyMatch(m.contact.name, currentPrimaryName) ||
          (currentPrimaryEmail && String(m.contact.email || "").toLowerCase() === currentPrimaryEmail.toLowerCase())
      );
      const currentScore = currentMatch
        ? scoreContactForOwnerPrimary(currentMatch.contact, currentMatch.calaClass)
        : 0;
      const enrichmentPrimaryMatch =
        enrichmentPc?.name && namesRoughlyMatch(pickedInner.name, enrichmentPc.name);
      const canUpgradePrimary =
        OVERWRITE ||
        (isEmpty(currentPrimaryName) && isEmpty(currentPrimaryEmail)) ||
        (registryVerified && pickedInner.sourceFile === "registry_enrichment") ||
        (registryVerified && pickedScore > currentScore) ||
        (enrichmentPrimaryMatch && registryVerified);
      if (canUpgradePrimary) {
        const outreachPhone =
          bestPhone ||
          (enrichmentPrimaryMatch && enrichmentPc?.phone ? enrichmentPc.phone : null);
        setPrimary(pickedInner.name, pickedInner.email, outreachPhone, "contact_record", true);
      }
    } else if (enrichmentPc) {
      setPrimary(enrichmentPc.name, enrichmentPc.email, enrichmentPc.phone, "profile_enrichment");
    }

    if (enrichmentFields) {
      if (enrichmentFields.pitchAngle && (OVERWRITE || isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.pitchAngle]))) {
        fields[MAP_GTM_OWNER_TARGET.pitchAngle] = enrichmentFields.pitchAngle;
      }
      if (enrichmentFields.contactPath && (OVERWRITE || isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.contactPath]))) {
        fields[MAP_GTM_OWNER_TARGET.contactPath] = enrichmentFields.contactPath;
      }
      if (enrichmentFields.ownerType && (OVERWRITE || isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.ownerType]))) {
        fields[MAP_GTM_OWNER_TARGET.ownerType] = enrichmentFields.ownerType;
      }
      if (enrichmentFields.internalNotes) {
        fields[MAP_GTM_OWNER_TARGET.internalNotes] = appendNotes(
          rec.fields[MAP_GTM_OWNER_TARGET.internalNotes],
          enrichmentFields.internalNotes
        );
      }
    }

    const hasPrimary = Boolean(
      fields[MAP_GTM_OWNER_TARGET.primaryContactName] ||
        fields[MAP_GTM_OWNER_TARGET.primaryContactEmail] ||
        (!isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.primaryContactName]) &&
          !fields[MAP_GTM_OWNER_TARGET.primaryContactName])
    );

    if (
      hasPrimary &&
      String(rec.fields[MAP_GTM_OWNER_TARGET.outreachStatus] || "not_contacted") === "not_contacted"
    ) {
      fields[MAP_GTM_OWNER_TARGET.outreachStatus] = "researching";
    }

    if (
      hasPrimary &&
      tier === "A" &&
      isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.nextAction]) &&
      (OVERWRITE || isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.nextAction]))
    ) {
      fields[MAP_GTM_OWNER_TARGET.nextAction] = "Draft intro email / LinkedIn outreach";
    }

    if (!Object.keys(fields).length) continue;

    ownerUpdates.push({ id: rec.id, fields, ownerName });
    plan.push({
      ownerName,
      tier,
      primarySource,
      primaryName: fields[MAP_GTM_OWNER_TARGET.primaryContactName] || rec.fields[MAP_GTM_OWNER_TARGET.primaryContactName],
      primaryEmail:
        MAP_GTM_OWNER_TARGET.primaryContactEmail in fields
          ? fields[MAP_GTM_OWNER_TARGET.primaryContactEmail]
          : rec.fields[MAP_GTM_OWNER_TARGET.primaryContactEmail],
      linkedContacts: matchedContacts.length,
      hospitalityContacts: hospitalityContacts.length,
      hasPitchAngle: Boolean(fields[MAP_GTM_OWNER_TARGET.pitchAngle] || !isEmpty(rec.fields[MAP_GTM_OWNER_TARGET.pitchAngle])),
    });
  }

  const relevanceCounts = { hospitality: 0, broker: 0, other: 0, unknown: 0 };
  for (const upd of contactUpdates) {
    const rel = upd.fields[MAP_GTM_CONTACT.contactRelevance];
    if (rel) relevanceCounts[rel] = (relevanceCounts[rel] || 0) + 1;
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    filters: { tier: TIER_FILTER, overwrite: OVERWRITE },
    summary: {
      ownersConsidered,
      ownerUpdatesPlanned: ownerUpdates.length,
      contactUpdatesPlanned: contactUpdates.length,
      contactRelevanceTagged: relevanceCounts,
      ownersWithLinkedContacts: contactsByOwnerId.size,
    },
    plan: plan.sort((a, b) => String(a.tier).localeCompare(String(b.tier)) || b.linkedContacts - a.linkedContacts),
  };

  mkdirSync(dirname(REPORT), { recursive: true });
  writeFileSync(REPORT, JSON.stringify(report, null, 2));

  console.log("GTM Owner Target ↔ Contact sync");
  console.log(`  Owners considered: ${ownersConsidered}`);
  console.log(`  Owner updates planned: ${ownerUpdates.length}`);
  console.log(`  Contact link/relevance updates: ${contactUpdates.length}`);
  console.log(`  Owners with ≥1 linked contact: ${contactsByOwnerId.size}`);
  console.log("  Contact relevance:", relevanceCounts);

  console.log("\nSample owner primary contacts:");
  for (const row of plan.filter((p) => p.primaryName).slice(0, 15)) {
    console.log(
      `  [${row.tier}] ${row.ownerName} → ${row.primaryName} <${row.primaryEmail || "no email"}> (${row.primarySource})`
    );
  }

  if (!APPLY) {
    console.log(`\nDry-run. Wrote ${REPORT}`);
    console.log("Run: node scripts/ensure-gtm-owner-contact-links.mjs --apply");
    console.log("Then: node scripts/sync-gtm-owner-target-contacts.mjs --apply");
    return;
  }

  for (const batch of chunk(contactUpdates, 10)) {
    await base(GTM_CONTACT_TABLE).update(batch, { typecast: true });
  }
  for (const batch of chunk(ownerUpdates, 10)) {
    await base(GTM_OWNER_TARGET_TABLES.ownerTargets).update(
      batch.map(({ id, fields }) => ({ id, fields })),
      { typecast: true }
    );
  }

  console.log(`\nApplied ${contactUpdates.length} contact updates and ${ownerUpdates.length} owner target updates.`);
  console.log(`Wrote ${REPORT}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
