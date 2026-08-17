/**
 * Batch-enrich GTM base from CoStar company profile data (internal only).
 *
 * Usage:
 *   node scripts/enrich-gtm-company-profiles.mjs              # dry-run
 *   node scripts/enrich-gtm-company-profiles.mjs --apply      # write
 *   node scripts/enrich-gtm-company-profiles.mjs --slug=ghl-hoteles --apply
 */
import "../load-env.js";
import {
  COMPANY_PROFILE_ENRICHMENTS,
  PROFILE_SOURCE_FILE,
} from "../lib/gtm-owner-target/company-profile-enrichments.js";
import { GTM_COMPANY_TABLE, MAP_GTM_COMPANY } from "../lib/gtm-owner-target/company-field-map.js";
import { GTM_CONTACT_TABLE, MAP_GTM_CONTACT } from "../lib/gtm-owner-target/contact-field-map.js";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
} from "../lib/gtm-owner-target/field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import { companyDedupeKey } from "../lib/gtm-owner-target/company-to-airtable.js";
import { contactDedupeKey } from "../lib/gtm-owner-target/contact-to-airtable.js";

const APPLY = process.argv.includes("--apply");
const slugArg = process.argv.find((a) => a.startsWith("--slug="));
const SLUG_FILTER = slugArg ? slugArg.split("=")[1] : null;

function appendNotes(existing, addition) {
  const prev = String(existing || "").trim();
  const next = String(addition || "").trim();
  if (!next) return prev || null;
  if (!prev) return next;
  if (prev.includes(next)) return prev;
  return `${prev}\n\n${next}`;
}

function buildCompanyFields(profile) {
  const c = profile.company;
  const dedupeInput = {
    Company: c.name,
    "HQ City": c.hqCity || "",
    "HQ Country": c.hqCountry || "",
  };
  /** @type {Record<string, unknown>} */
  const fields = {
    [MAP_GTM_COMPANY.company]: c.name,
    [MAP_GTM_COMPANY.sourceFile]: PROFILE_SOURCE_FILE,
    [MAP_GTM_COMPANY.companyDedupeKey]: companyDedupeKey(dedupeInput),
    [MAP_GTM_COMPANY.internalNotes]: c.internalNotes || null,
  };
  if (c.companyOverview) fields[MAP_GTM_COMPANY.companyOverview] = c.companyOverview;
  if (c.specialty) fields[MAP_GTM_COMPANY.specialty] = c.specialty;
  if (c.hqMarket) fields[MAP_GTM_COMPANY.hqMarket] = c.hqMarket;
  if (c.hqCity) fields[MAP_GTM_COMPANY.hqCity] = c.hqCity;
  if (c.hqState) fields[MAP_GTM_COMPANY.hqState] = c.hqState;
  if (c.hqCountry) fields[MAP_GTM_COMPANY.hqCountry] = c.hqCountry;
  if (c.website) fields[MAP_GTM_COMPANY.website] = c.website;
  if (c.employees != null) fields[MAP_GTM_COMPANY.employees] = c.employees;
  if (c.locations != null) fields[MAP_GTM_COMPANY.locations] = c.locations;
  if (c.ownedProperties != null) fields[MAP_GTM_COMPANY.ownedProperties] = c.ownedProperties;
  if (c.operatedProperties != null) fields[MAP_GTM_COMPANY.operatedProperties] = c.operatedProperties;
  if (c.saleTransactions3Y != null) fields[MAP_GTM_COMPANY.saleTransactions3Y] = c.saleTransactions3Y;
  if (c.saleTransactionsSf3Y != null) fields[MAP_GTM_COMPANY.saleTransactionsSf3Y] = c.saleTransactionsSf3Y;
  return fields;
}

function buildContactFields(contact, companyName) {
  const dedupeInput = {
    Email: contact.email || "",
    Name: contact.name || "",
    Company: contact.company || companyName,
    Phone: contact.phone || "",
  };
  /** @type {Record<string, unknown>} */
  const fields = {
    [MAP_GTM_CONTACT.name]: contact.name,
    [MAP_GTM_CONTACT.company]: contact.company || companyName,
    [MAP_GTM_CONTACT.sourceFile]: PROFILE_SOURCE_FILE,
    [MAP_GTM_CONTACT.contactDedupeKey]: contactDedupeKey(dedupeInput),
  };
  if (contact.title) fields[MAP_GTM_CONTACT.title] = contact.title;
  if (contact.email) fields[MAP_GTM_CONTACT.email] = contact.email;
  if (contact.phone) fields[MAP_GTM_CONTACT.phone] = contact.phone;
  if (contact.linkedIn) fields[MAP_GTM_CONTACT.linkedIn] = contact.linkedIn;
  if (contact.internalNotes) fields[MAP_GTM_CONTACT.internalNotes] = contact.internalNotes;
  return fields;
}

function buildOwnerTargetFields(profile) {
  const ot = profile.ownerTarget;
  if (!ot) return null;
  /** @type {Record<string, unknown>} */
  const fields = {};
  if (ot.ownerType) fields[MAP_GTM_OWNER_TARGET.ownerType] = ot.ownerType;
  if (ot.priorityTier) fields[MAP_GTM_OWNER_TARGET.priorityTier] = ot.priorityTier;
  if (ot.pitchAngle) fields[MAP_GTM_OWNER_TARGET.pitchAngle] = ot.pitchAngle;
  if (ot.contactPath) fields[MAP_GTM_OWNER_TARGET.contactPath] = ot.contactPath;
  if (ot.internalNotes) fields[MAP_GTM_OWNER_TARGET.internalNotes] = ot.internalNotes;
  const pc = ot.primaryContact;
  if (pc?.name) fields[MAP_GTM_OWNER_TARGET.primaryContactName] = pc.name;
  if (pc?.email) fields[MAP_GTM_OWNER_TARGET.primaryContactEmail] = pc.email;
  if (pc?.phone) fields[MAP_GTM_OWNER_TARGET.primaryContactPhone] = pc.phone;
  return Object.keys(fields).length ? fields : null;
}

function resolveOwnerTarget(profile, ownerByName) {
  const ot = profile.ownerTarget;
  if (!ot) return null;
  if (ot.ownerTargetId) {
    return { id: ot.ownerTargetId, name: ot.preferredNames?.[0] || profile.company.name };
  }
  for (const name of ot.preferredNames || []) {
    const hit = ownerByName.get(name);
    if (hit) return hit;
  }
  return null;
}

async function loadIndexes(base) {
  const [companies, contacts, owners] = await Promise.all([
    base(GTM_COMPANY_TABLE).select({ fields: [MAP_GTM_COMPANY.companyDedupeKey, MAP_GTM_COMPANY.internalNotes] }).all(),
    base(GTM_CONTACT_TABLE).select({ fields: [MAP_GTM_CONTACT.contactDedupeKey, MAP_GTM_CONTACT.internalNotes] }).all(),
    base(GTM_OWNER_TARGET_TABLES.ownerTargets).select({ fields: [MAP_GTM_OWNER_TARGET.ownerName, MAP_GTM_OWNER_TARGET.internalNotes] }).all(),
  ]);

  const companyByKey = new Map();
  for (const rec of companies) {
    const key = String(rec.fields[MAP_GTM_COMPANY.companyDedupeKey] || "");
    if (key) companyByKey.set(key, rec);
  }

  const contactByKey = new Map();
  for (const rec of contacts) {
    const key = String(rec.fields[MAP_GTM_CONTACT.contactDedupeKey] || "");
    if (key) contactByKey.set(key, rec);
  }

  const ownerByName = new Map();
  for (const rec of owners) {
    const name = String(rec.fields[MAP_GTM_OWNER_TARGET.ownerName] || "");
    if (name) ownerByName.set(name, { id: rec.id, name, record: rec });
  }

  return { companyByKey, contactByKey, ownerByName };
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  let profiles = COMPANY_PROFILE_ENRICHMENTS;
  if (SLUG_FILTER) {
    profiles = profiles.filter((p) => p.slug === SLUG_FILTER);
    if (!profiles.length) {
      throw new Error(`No profile found for slug: ${SLUG_FILTER}`);
    }
  }

  const { companyByKey, contactByKey, ownerByName } = await loadIndexes(base);

  /** @type {{ action: string, table: string, id?: string, label: string }[]} */
  const plan = [];

  const companyCreates = [];
  const companyUpdates = [];
  const contactCreates = [];
  const contactUpdates = [];
  const ownerUpdates = [];

  for (const profile of profiles) {
    const companyFields = buildCompanyFields(profile);
    const dedupeKey = String(companyFields[MAP_GTM_COMPANY.companyDedupeKey]);
    const existingCompany = companyByKey.get(dedupeKey);

    if (existingCompany) {
      const merged = { ...companyFields };
      merged[MAP_GTM_COMPANY.internalNotes] = appendNotes(
        existingCompany.fields[MAP_GTM_COMPANY.internalNotes],
        companyFields[MAP_GTM_COMPANY.internalNotes]
      );
      companyUpdates.push({ id: existingCompany.id, fields: merged });
      plan.push({ action: "update", table: GTM_COMPANY_TABLE, id: existingCompany.id, label: profile.slug });
    } else {
      companyCreates.push({ fields: companyFields });
      plan.push({ action: "create", table: GTM_COMPANY_TABLE, label: profile.slug });
    }

    for (const contact of profile.contacts || []) {
      const contactFields = buildContactFields(contact, profile.company.name);
      const cKey = String(contactFields[MAP_GTM_CONTACT.contactDedupeKey]);
      if (!cKey) {
        plan.push({ action: "skip", table: GTM_CONTACT_TABLE, label: `${profile.slug}:${contact.name} (no dedupe key)` });
        continue;
      }
      const existingContact = contactByKey.get(cKey);
      if (existingContact) {
        const merged = { ...contactFields };
        merged[MAP_GTM_CONTACT.internalNotes] = appendNotes(
          existingContact.fields[MAP_GTM_CONTACT.internalNotes],
          contactFields[MAP_GTM_CONTACT.internalNotes]
        );
        contactUpdates.push({ id: existingContact.id, fields: merged });
        plan.push({ action: "update", table: GTM_CONTACT_TABLE, id: existingContact.id, label: `${profile.slug}:${contact.name}` });
      } else {
        contactCreates.push({ fields: contactFields });
        plan.push({ action: "create", table: GTM_CONTACT_TABLE, label: `${profile.slug}:${contact.name}` });
      }
    }

    const ownerHit = resolveOwnerTarget(profile, ownerByName);
    const ownerFields = buildOwnerTargetFields(profile);
    if (ownerFields && ownerHit) {
      const merged = { ...ownerFields };
      merged[MAP_GTM_OWNER_TARGET.internalNotes] = appendNotes(
        ownerHit.record.fields[MAP_GTM_OWNER_TARGET.internalNotes],
        ownerFields[MAP_GTM_OWNER_TARGET.internalNotes]
      );
      ownerUpdates.push({ id: ownerHit.id, fields: merged });
      plan.push({ action: "update", table: GTM_OWNER_TARGET_TABLES.ownerTargets, id: ownerHit.id, label: `${profile.slug} → ${ownerHit.name}` });
    } else if (profile.ownerTarget && !ownerHit) {
      plan.push({ action: "skip", table: GTM_OWNER_TARGET_TABLES.ownerTargets, label: `${profile.slug} (no owner target match)` });
    }
  }

  console.log(`Mode: ${APPLY ? "APPLY" : "DRY-RUN"}`);
  console.log(`Profiles: ${profiles.length}`);
  console.log(
    `Plan: companies +${companyCreates.length} ~${companyUpdates.length}, contacts +${contactCreates.length} ~${contactUpdates.length}, owner targets ~${ownerUpdates.length}`
  );
  for (const step of plan) {
    console.log(`  [${step.action}] ${step.table} ${step.label}${step.id ? ` (${step.id})` : ""}`);
  }

  if (!APPLY) {
    console.log("\nDry-run complete. Re-run with --apply to write.");
    return;
  }

  const chunk = (arr, size) => {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
  };

  for (const batch of chunk(companyCreates, 10)) {
    await base(GTM_COMPANY_TABLE).create(batch, { typecast: true });
  }
  for (const batch of chunk(companyUpdates, 10)) {
    await base(GTM_COMPANY_TABLE).update(batch, { typecast: true });
  }
  for (const batch of chunk(contactCreates, 10)) {
    await base(GTM_CONTACT_TABLE).create(batch, { typecast: true });
  }
  for (const batch of chunk(contactUpdates, 10)) {
    await base(GTM_CONTACT_TABLE).update(batch, { typecast: true });
  }
  for (const batch of chunk(ownerUpdates, 10)) {
    await base(GTM_OWNER_TARGET_TABLES.ownerTargets).update(batch, { typecast: true });
  }

  console.log("\nCompany profile enrichment complete.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
