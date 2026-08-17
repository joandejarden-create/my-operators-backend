/**
 * Classify GTM Owner Targets by ICP segment and strike-list eligibility.
 *
 * Usage:
 *   node scripts/classify-gtm-owner-target-icp.mjs
 *   node scripts/classify-gtm-owner-target-icp.mjs --apply
 *   node scripts/classify-gtm-owner-target-icp.mjs --min-cala-properties=3
 *
 * Reports:
 *   reports/gtm-owner-target-icp-classification.json
 *   reports/gtm-owner-target-icp-classification.csv
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
} from "../lib/gtm-owner-target/contact-field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import {
  fetchAllGtmProperties,
  groupAirtablePropertiesByOwner,
} from "../lib/gtm-owner-target/properties-read.js";
import { normalizeOwnerKey } from "../lib/gtm-owner-target/normalize.js";
import {
  buildOwnerTargetIndexes,
  resolveLabelToOwnerTargets,
  classifyContactRelevance,
  pickPrimaryContact,
  scoreContactForOwnerPrimary,
} from "../lib/gtm-owner-target/owner-contact-sync.js";
import {
  buildContactCalaMatchContext,
  classifyContactCalaFootprint,
} from "../lib/gtm-owner-target/contact-cala-match.js";
import { COMPANY_PROFILE_ENRICHMENTS, findCompanyProfileEnrichment } from "../lib/gtm-owner-target/company-profile-enrichments.js";
import {
  classifyOwnerIcp,
  isVerifiedOwnerContact,
} from "../lib/gtm-owner-target/icp-classify.js";
import { parseDevelopmentCountFromProfileNotes } from "../lib/gtm-owner-target/branding-decision-signals.js";
import { validateOwnerTargetWrite } from "../lib/gtm-owner-target/validate.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-owner-target-icp-classification.json");
const REPORT_CSV = join(__dirname, "..", "reports", "gtm-owner-target-icp-classification.csv");
const APPLY = process.argv.includes("--apply");

const minCalaArg = process.argv.find((a) => a.startsWith("--min-cala-properties="));
const MIN_CALA_PROPERTIES = minCalaArg ? Number(minCalaArg.split("=")[1]) : 3;

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/**
 * @param {object[]} contactRecords
 * @param {ReturnType<buildOwnerTargetIndexes>} ownerIndexes
 * @param {object} matchContext
 */
function buildOwnerContactIndex(contactRecords, ownerIndexes, matchContext) {
  /** @type {Map<string, { verified: object | null, best: object | null, linkedCount: number, candidates: object[] }>} */
  const byOwnerId = new Map();

  for (const rec of contactRecords) {
    const contact = {
      name: rec.fields[MAP_GTM_CONTACT.name],
      email: rec.fields[MAP_GTM_CONTACT.email],
      phone: rec.fields[MAP_GTM_CONTACT.phone],
      company: rec.fields[MAP_GTM_CONTACT.company],
      title: rec.fields[MAP_GTM_CONTACT.title],
      specialty: rec.fields[MAP_GTM_CONTACT.specialty],
      country: rec.fields[MAP_GTM_CONTACT.country],
      contactRelevance: rec.fields[MAP_GTM_CONTACT.contactRelevance],
      calaHotelContact: rec.fields[MAP_GTM_CONTACT.calaHotelContact],
      calaMatchType: rec.fields[MAP_GTM_CONTACT.calaMatchType],
      verificationTier: rec.fields[MAP_GTM_CONTACT.verificationTier],
      verificationSource: rec.fields[MAP_GTM_CONTACT.verificationSource],
      verificationUrl: rec.fields[MAP_GTM_CONTACT.verificationUrl],
      legalRepresentativeName: rec.fields[MAP_GTM_CONTACT.legalRepresentativeName],
      registryEntityName: rec.fields[MAP_GTM_CONTACT.registryEntityName],
      website: rec.fields[MAP_GTM_CONTACT.website],
      linkedIn: rec.fields[MAP_GTM_CONTACT.linkedIn],
      sourceFile: rec.fields[MAP_GTM_CONTACT.sourceFile],
    };

    const labels = [contact.company, contact.name].filter(Boolean);
    /** @type {Map<string, { id: string, ownerName: string, matchType: string }>} */
    const ownerHits = new Map();
    for (const label of labels) {
      for (const hit of resolveLabelToOwnerTargets(label, matchContext.nameAliasIndex, ownerIndexes)) {
        ownerHits.set(hit.id, hit);
      }
    }
    if (!ownerHits.size && rec.fields[MAP_GTM_CONTACT.matchedOwnerName]) {
      for (const hit of resolveLabelToOwnerTargets(
        rec.fields[MAP_GTM_CONTACT.matchedOwnerName],
        matchContext.nameAliasIndex,
        ownerIndexes
      )) {
        ownerHits.set(hit.id, hit);
      }
    }

    for (const hit of ownerHits.values()) {
      const calaClass = classifyContactCalaFootprint(hit.ownerName, matchContext);
      const relevance =
        contact.contactRelevance ||
        classifyContactRelevance(contact, calaClass);
      const enriched = { ...contact, contactRelevance: relevance };
      const entry = byOwnerId.get(hit.id) || { verified: null, best: null, linkedCount: 0, candidates: [] };
      entry.linkedCount += 1;

      const calaClassWithMatch = { ...calaClass, matchType: hit.matchType };
      entry.candidates.push({ contact: enriched, calaClass: calaClassWithMatch });

      if (isVerifiedOwnerContact(enriched, calaClassWithMatch)) {
        const score = scoreContactForOwnerPrimary(enriched, calaClassWithMatch);
        const prevScore = entry.verified
          ? scoreContactForOwnerPrimary(entry.verified, calaClassWithMatch)
          : -1;
        if (score > prevScore) entry.verified = enriched;
      }

      byOwnerId.set(hit.id, entry);
    }
  }

  for (const [ownerId, entry] of byOwnerId) {
    entry.best = pickPrimaryContact(entry.candidates)?.contact || null;
    byOwnerId.set(ownerId, entry);
  }

  return byOwnerId;
}

function buildIcpAirtableFields(classification, { preserveDealTrigger }) {
  const fields = {
    [MAP_GTM_OWNER_TARGET.icpSegment]: classification.icpSegment,
    [MAP_GTM_OWNER_TARGET.strikeList]: classification.strikeList,
    [MAP_GTM_OWNER_TARGET.icpClassificationNotes]: classification.icpClassificationNotes,
    [MAP_GTM_OWNER_TARGET.calaPropertyCount]: classification.calaPropertyCount,
  };
  if (!preserveDealTrigger || !classification.existingDealTrigger) {
    fields[MAP_GTM_OWNER_TARGET.dealTrigger] = classification.dealTrigger;
  }
  return fields;
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const ownerFields = [
    MAP_GTM_OWNER_TARGET.ownerName,
    MAP_GTM_OWNER_TARGET.ownerType,
    MAP_GTM_OWNER_TARGET.priorityTier,
    MAP_GTM_OWNER_TARGET.propertyCount,
    MAP_GTM_OWNER_TARGET.primaryContactName,
    MAP_GTM_OWNER_TARGET.primaryContactEmail,
    MAP_GTM_OWNER_TARGET.countriesSummary,
  ];

  const [{ records: propertyRecords }, ownerRecords, contactRecords] = await Promise.all([
    fetchAllGtmProperties(),
    base(GTM_OWNER_TARGET_TABLES.ownerTargets).select({ fields: ownerFields }).all(),
    base(GTM_CONTACT_TABLE)
      .select({
        fields: [
          MAP_GTM_CONTACT.name,
          MAP_GTM_CONTACT.email,
          MAP_GTM_CONTACT.phone,
          MAP_GTM_CONTACT.company,
          MAP_GTM_CONTACT.title,
          MAP_GTM_CONTACT.specialty,
          MAP_GTM_CONTACT.country,
          MAP_GTM_CONTACT.contactRelevance,
          MAP_GTM_CONTACT.calaHotelContact,
          MAP_GTM_CONTACT.calaMatchType,
          MAP_GTM_CONTACT.matchedOwnerName,
          MAP_GTM_CONTACT.verificationTier,
          MAP_GTM_CONTACT.verificationSource,
          MAP_GTM_CONTACT.verificationUrl,
          MAP_GTM_CONTACT.legalRepresentativeName,
          MAP_GTM_CONTACT.registryEntityName,
          MAP_GTM_CONTACT.website,
          MAP_GTM_CONTACT.linkedIn,
          MAP_GTM_CONTACT.sourceFile,
        ],
      })
      .all(),
  ]);

  const ownerGroups = groupAirtablePropertiesByOwner(propertyRecords);
  const propertiesByOwnerKey = new Map(
    ownerGroups.map((g) => [normalizeOwnerKey(g.ownerName), g.properties])
  );

  const ownerIndexes = buildOwnerTargetIndexes(
    ownerRecords.map((r) => ({
      id: r.id,
      ownerName: String(r.fields[MAP_GTM_OWNER_TARGET.ownerName] || ""),
    }))
  );

  const matchContext = buildContactCalaMatchContext({
    ownerGroups,
    companyNames: [],
    profileEnrichments: COMPANY_PROFILE_ENRICHMENTS,
  });

  const contactByOwnerId = buildOwnerContactIndex(contactRecords, ownerIndexes, matchContext);

  /** @type {object[]} */
  const rows = [];
  /** @type {{ id: string, fields: object }[]} */
  const updates = [];

  for (const rec of ownerRecords) {
    const ownerName = String(rec.fields[MAP_GTM_OWNER_TARGET.ownerName] || "");
    const ownerKey = normalizeOwnerKey(ownerName);
    const properties = propertiesByOwnerKey.get(ownerKey) || [];
    const contactEntry = contactByOwnerId.get(rec.id);
    const verifiedContact = contactEntry?.verified || null;
    const bestContact = contactEntry?.best || null;
    const primaryEmail = String(rec.fields[MAP_GTM_OWNER_TARGET.primaryContactEmail] || "").trim();
    const existingDealTrigger = String(rec.fields[MAP_GTM_OWNER_TARGET.dealTrigger] || "").trim();

    const profile = findCompanyProfileEnrichment(ownerName);
    const developmentPipelineCount = parseDevelopmentCountFromProfileNotes(
      profile?.company?.internalNotes || ""
    );

    let classification = classifyOwnerIcp({
      ownerName,
      ownerType: rec.fields[MAP_GTM_OWNER_TARGET.ownerType],
      priorityTier: rec.fields[MAP_GTM_OWNER_TARGET.priorityTier],
      propertyCount: rec.fields[MAP_GTM_OWNER_TARGET.propertyCount],
      properties,
      existingDealTrigger: existingDealTrigger || undefined,
      developmentPipelineCount,
      contact: {
        hasVerifiedContact: Boolean(verifiedContact),
        primaryContactEmail: primaryEmail || verifiedContact?.email || bestContact?.email,
        primaryContactName:
          rec.fields[MAP_GTM_OWNER_TARGET.primaryContactName] ||
          verifiedContact?.name ||
          bestContact?.name,
      },
    });

    if (classification.calaPropertyCount < MIN_CALA_PROPERTIES) {
      classification = {
        ...classification,
        strikeList: false,
        icpClassificationNotes: `${classification.icpClassificationNotes}; strike_blocked_min_cala_${MIN_CALA_PROPERTIES}`,
      };
    }

    const row = {
      id: rec.id,
      ownerName,
      priorityTier: classification.priorityTier,
      propertyCount: classification.propertyCount,
      calaPropertyCount: classification.calaPropertyCount,
      icpSegment: classification.icpSegment,
      strikeList: classification.strikeList,
      dealTrigger: existingDealTrigger && existingDealTrigger !== "none_known"
        ? existingDealTrigger
        : classification.dealTrigger,
      hasVerifiedContact: classification.hasVerifiedContact,
      primaryContactEmail:
        primaryEmail || verifiedContact?.email || bestContact?.email || "",
      primaryContactName:
        rec.fields[MAP_GTM_OWNER_TARGET.primaryContactName] ||
        verifiedContact?.name ||
        bestContact?.name ||
        "",
      linkedContacts: contactEntry?.linkedCount || 0,
      icpClassificationNotes: classification.icpClassificationNotes,
      countriesSummary: rec.fields[MAP_GTM_OWNER_TARGET.countriesSummary] || "",
    };
    rows.push(row);

    const fields = buildIcpAirtableFields(
      { ...classification, dealTrigger: row.dealTrigger },
      { preserveDealTrigger: true }
    );
    const validation = validateOwnerTargetWrite({
      [MAP_GTM_OWNER_TARGET.ownerName]: ownerName,
      ...fields,
    });
    if (!validation.ok) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[classify-gtm-owner-target-icp] validation skip", ownerName, validation.failures);
      }
      continue;
    }
    updates.push({ id: rec.id, fields });
  }

  const segmentCounts = {};
  for (const row of rows) {
    segmentCounts[row.icpSegment] = (segmentCounts[row.icpSegment] || 0) + 1;
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    filters: { minCalaProperties: MIN_CALA_PROPERTIES },
    summary: {
      ownerCount: rows.length,
      strikeListCount: rows.filter((r) => r.strikeList).length,
      verifiedContactCount: rows.filter((r) => r.hasVerifiedContact).length,
      segmentCounts,
    },
    strikeList: rows.filter((r) => r.strikeList),
    rows,
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  const csvHeaders = [
    "ownerName",
    "priorityTier",
    "icpSegment",
    "strikeList",
    "calaPropertyCount",
    "propertyCount",
    "hasVerifiedContact",
    "primaryContactName",
    "primaryContactEmail",
    "linkedContacts",
    "dealTrigger",
    "countriesSummary",
    "icpClassificationNotes",
  ];
  const csvLines = [
    csvHeaders.join(","),
    ...rows.map((r) => csvHeaders.map((h) => csvEscape(r[h])).join(",")),
  ];
  writeFileSync(REPORT_CSV, csvLines.join("\n"));

  console.log(`Classified ${rows.length} owners → ${report.summary.strikeListCount} on strike list`);
  console.log("Segment counts:", segmentCounts);
  console.log("Wrote", REPORT_JSON);
  console.log("Wrote", REPORT_CSV);

  if (APPLY && updates.length) {
    let updated = 0;
    for (const batch of chunk(updates, 10)) {
      await base(GTM_OWNER_TARGET_TABLES.ownerTargets).update(batch, { typecast: true });
      updated += batch.length;
    }
    console.log(`Applied ${updated} owner target ICP updates`);
  } else if (APPLY) {
    console.log("No updates to apply");
  } else {
    console.log("Dry-run only. Re-run with --apply to write ICP fields to Airtable.");
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
