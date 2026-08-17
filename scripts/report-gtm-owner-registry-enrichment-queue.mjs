/**
 * Export registry enrichment work queue for strike-list owners missing verified contacts.
 *
 * Usage:
 *   node scripts/report-gtm-owner-registry-enrichment-queue.mjs
 *   node scripts/report-gtm-owner-registry-enrichment-queue.mjs --from-classification
 *   node scripts/report-gtm-owner-registry-enrichment-queue.mjs --from-classification --tier-a-eligible --limit=30
 *   node scripts/report-gtm-owner-registry-enrichment-queue.mjs --from-classification --merge-airtable-properties
 *   node scripts/report-gtm-owner-registry-enrichment-queue.mjs --include-verified
 *
 * Reports:
 *   reports/gtm-owner-registry-enrichment-queue.json
 *   reports/gtm-owner-registry-enrichment-queue.csv
 *   reports/gtm-owner-registry-enrichment-queue.md
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
  VAL_GTM_ICP_STRIKE_ELIGIBLE,
} from "../lib/gtm-owner-target/field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import {
  fetchAllGtmProperties,
  groupAirtablePropertiesByOwner,
} from "../lib/gtm-owner-target/properties-read.js";
import {
  buildRegistryEnrichmentQueue,
  summarizeRegistryQueue,
} from "../lib/gtm-owner-target/registry-enrichment-queue.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLASSIFICATION_JSON = join(__dirname, "..", "reports", "gtm-owner-target-icp-classification.json");
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-owner-registry-enrichment-queue.json");
const REPORT_CSV = join(__dirname, "..", "reports", "gtm-owner-registry-enrichment-queue.csv");
const REPORT_MD = join(__dirname, "..", "reports", "gtm-owner-registry-enrichment-queue.md");

const FROM_CLASSIFICATION = process.argv.includes("--from-classification");
const TIER_A_ELIGIBLE = process.argv.includes("--tier-a-eligible");
const MERGE_AIRTABLE_PROPERTIES = process.argv.includes("--merge-airtable-properties");
const INCLUDE_VERIFIED = process.argv.includes("--include-verified");

const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : null;

const tierArg = process.argv.find((a) => a.startsWith("--tier="));
const TIER_FILTER = tierArg ? tierArg.split("=")[1].toUpperCase() : null;

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function mapOwnerFromAirtable(rec) {
  const f = rec.fields || {};
  return {
    id: rec.id,
    ownerName: f[MAP_GTM_OWNER_TARGET.ownerName],
    priorityTier: f[MAP_GTM_OWNER_TARGET.priorityTier],
    icpSegment: f[MAP_GTM_OWNER_TARGET.icpSegment],
    strikeList: Boolean(f[MAP_GTM_OWNER_TARGET.strikeList]),
    calaPropertyCount: f[MAP_GTM_OWNER_TARGET.calaPropertyCount],
    propertyCount: f[MAP_GTM_OWNER_TARGET.propertyCount],
    countriesSummary: f[MAP_GTM_OWNER_TARGET.countriesSummary],
    primaryContactName: f[MAP_GTM_OWNER_TARGET.primaryContactName],
    primaryContactEmail: f[MAP_GTM_OWNER_TARGET.primaryContactEmail],
    hasVerifiedContact: Boolean(f[MAP_GTM_OWNER_TARGET.primaryContactEmail]),
  };
}

function mapOwnerFromClassification(row) {
  return {
    id: row.id,
    ownerName: row.ownerName,
    priorityTier: row.priorityTier,
    icpSegment: row.icpSegment,
    strikeList: row.strikeList,
    calaPropertyCount: row.calaPropertyCount,
    propertyCount: row.propertyCount,
    countriesSummary: row.countriesSummary,
    primaryContactName: row.primaryContactName,
    primaryContactEmail: row.primaryContactEmail,
    hasVerifiedContact: row.hasVerifiedContact,
    forceQueue: !row.hasVerifiedContact,
  };
}

async function loadAirtablePropertiesByOwnerId() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const [ownerRecords, { records: propertyRecords }] = await Promise.all([
    base(GTM_OWNER_TARGET_TABLES.ownerTargets)
      .select({ fields: [MAP_GTM_OWNER_TARGET.ownerName] })
      .all(),
    fetchAllGtmProperties(),
  ]);

  const ownerGroups = groupAirtablePropertiesByOwner(propertyRecords);
  /** @type {Map<string, object[]>} */
  const propertiesByOwnerId = new Map();
  for (const group of ownerGroups) {
    const ownerRec = ownerRecords.find(
      (r) => String(r.fields[MAP_GTM_OWNER_TARGET.ownerName]) === group.ownerName
    );
    if (ownerRec) {
      propertiesByOwnerId.set(
        ownerRec.id,
        group.properties.map((p) => ({
          buildingName: p.buildingName,
          city: p.city,
          country: p.country,
          submarket: p.submarket,
          market: p.market,
        }))
      );
    }
  }
  return propertiesByOwnerId;
}

async function loadOwnersFromAirtable() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const fields = [
    MAP_GTM_OWNER_TARGET.ownerName,
    MAP_GTM_OWNER_TARGET.priorityTier,
    MAP_GTM_OWNER_TARGET.icpSegment,
    MAP_GTM_OWNER_TARGET.strikeList,
    MAP_GTM_OWNER_TARGET.calaPropertyCount,
    MAP_GTM_OWNER_TARGET.propertyCount,
    MAP_GTM_OWNER_TARGET.countriesSummary,
    MAP_GTM_OWNER_TARGET.primaryContactName,
    MAP_GTM_OWNER_TARGET.primaryContactEmail,
  ];

  const [ownerRecords, { records: propertyRecords }] = await Promise.all([
    base(GTM_OWNER_TARGET_TABLES.ownerTargets).select({ fields }).all(),
    fetchAllGtmProperties(),
  ]);

  const ownerGroups = groupAirtablePropertiesByOwner(propertyRecords);
  /** @type {Map<string, object[]>} */
  const propertiesByOwnerId = new Map();
  for (const group of ownerGroups) {
    const ownerRec = ownerRecords.find(
      (r) => String(r.fields[MAP_GTM_OWNER_TARGET.ownerName]) === group.ownerName
    );
    if (ownerRec) {
      propertiesByOwnerId.set(
        ownerRec.id,
        group.properties.map((p) => ({
          buildingName: p.buildingName,
          city: p.city,
          country: p.country,
          submarket: p.submarket,
        }))
      );
    }
  }

  return {
    source: "airtable",
    owners: ownerRecords.map(mapOwnerFromAirtable),
    propertiesByOwnerId,
  };
}

function loadOwnersFromClassification() {
  if (!existsSync(CLASSIFICATION_JSON)) {
    throw new Error(`Missing ${CLASSIFICATION_JSON}. Run classify-gtm-owner-target-icp.mjs first.`);
  }
  const data = JSON.parse(readFileSync(CLASSIFICATION_JSON, "utf8"));
  const rows = (data.rows || []).filter((r) => {
    if (INCLUDE_VERIFIED && r.hasVerifiedContact) return true;
    if (r.hasVerifiedContact) return false;

    if (TIER_A_ELIGIBLE) {
      return (
        r.priorityTier === "A" &&
        VAL_GTM_ICP_STRIKE_ELIGIBLE.includes(r.icpSegment) &&
        (r.calaPropertyCount || 0) >= 3
      );
    }

    return (
      r.strikeList ||
      (VAL_GTM_ICP_STRIKE_ELIGIBLE.includes(r.icpSegment) &&
        (r.priorityTier === "A" || r.priorityTier === "B") &&
        (r.calaPropertyCount || 0) >= 3)
    );
  });
  return {
    source: TIER_A_ELIGIBLE ? "classification_tier_a_eligible" : "classification_report",
    owners: rows.map(mapOwnerFromClassification),
    propertiesByOwnerId: new Map(),
  };
}

function buildMarkdownReport(items, summary) {
  const lines = [
    "# GTM Owner Registry Enrichment Queue",
    "",
    `Generated: ${new Date().toISOString()}`,
    "",
    "## Summary",
    "",
    `- Total queue items: **${summary.total}**`,
    `- Priority 1 (Tier A): **${summary.priority1}**`,
    "",
    "### By verification status",
    "",
  ];
  for (const [status, count] of Object.entries(summary.byVerificationStatus || {})) {
    lines.push(`- ${status}: ${count}`);
  }
  lines.push("", "### By primary country", "");
  for (const [country, count] of Object.entries(summary.byPrimaryCountry || {})) {
    lines.push(`- ${country}: ${count}`);
  }
  lines.push("", "## Work items (top 25)", "");
  for (const item of items.slice(0, 25)) {
    lines.push(`### ${item.ownerName} (${item.priorityTier}, P${item.enrichmentPriority})`);
    lines.push("");
    lines.push(`- **Status:** ${item.verificationStatus}`);
    lines.push(`- **Country:** ${item.primaryCountry || "—"}`);
    lines.push(`- **Registry:** ${item.registryLabel || "—"}`);
    lines.push(`- **Bridge:** ${item.bridgeStrategy}`);
    lines.push(`- **CALA properties:** ${item.calaPropertyCount}`);
    if (item.primaryCountry === "Mexico") {
      lines.push("- **Primary path:** " + (item.registryPrimaryPath || "corporate_web_first"));
      lines.push("- **Corporate site:** " + (item.corporateWebsite || "—"));
      if (item.corporateEntityType) lines.push("- **Entity type:** " + item.corporateEntityType);
      if (item.recommendedOutreach?.name) {
        lines.push(
          "- **Recommended contact:** " +
            [item.recommendedOutreach.name, item.recommendedOutreach.title, item.recommendedOutreach.email]
              .filter(Boolean)
              .join(" — ")
        );
      }
      lines.push("- **SIGER fallback (optional):** " + (item.commercialRegistryUrl || "https://www.siger.gob.mx/"));
    } else {
      if (item.commercialRegistryUrl) lines.push(`- **Commercial registry:** ${item.commercialRegistryUrl}`);
      if (item.tourismRegistryUrl) lines.push(`- **Tourism registry:** ${item.tourismRegistryUrl}`);
    }
    lines.push("- **Entity search:** " + item.entitySearchName);
    if (item.sampleProperties?.length) {
      lines.push("- **Sample properties:**");
      for (const p of item.sampleProperties) lines.push(`  - ${p}`);
    }
    lines.push("- **Hints:**");
    for (const h of item.entitySearchHints || []) lines.push(`  - ${h}`);
    if (item.registryLookupNotes?.length) {
      lines.push("- **Registry notes:**");
      for (const n of item.registryLookupNotes) lines.push(`  - ${n}`);
    }
    lines.push("");
  }
  lines.push("## Enrichment output", "");
  lines.push("Save completed lookups to `data/internal/gtm-registry-enrichments/*.json` and run:");
  lines.push("");
  lines.push("```bash");
  lines.push("node scripts/import-gtm-registry-contact-enrichments.mjs --dry-run");
  lines.push("node scripts/import-gtm-registry-contact-enrichments.mjs --apply");
  lines.push("node scripts/sync-gtm-owner-target-contacts.mjs --apply");
  lines.push("```");
  return lines.join("\n");
}

async function main() {
  const useClassification = FROM_CLASSIFICATION || TIER_A_ELIGIBLE;
  let loaded = useClassification ? loadOwnersFromClassification() : await loadOwnersFromAirtable();

  if (MERGE_AIRTABLE_PROPERTIES || useClassification) {
    try {
      const propertiesByOwnerId = await loadAirtablePropertiesByOwnerId();
      loaded = {
        ...loaded,
        propertiesByOwnerId,
        source: `${loaded.source}+airtable_properties`,
      };
    } catch (err) {
      if (MERGE_AIRTABLE_PROPERTIES) throw err;
      console.warn("Could not merge Airtable properties:", err.message || err);
    }
  }

  const items = buildRegistryEnrichmentQueue(loaded.owners, loaded.propertiesByOwnerId, {
    limit: LIMIT,
    tierFilter: TIER_FILTER,
    strikeOnly: !useClassification,
    includeVerified: INCLUDE_VERIFIED,
  });

  const summary = summarizeRegistryQueue(items);
  const report = {
    generatedAt: new Date().toISOString(),
    source: loaded.source,
    options: {
      limit: LIMIT,
      tierFilter: TIER_FILTER,
      includeVerified: INCLUDE_VERIFIED,
      tierAEligible: TIER_A_ELIGIBLE,
      mergeAirtableProperties: MERGE_AIRTABLE_PROPERTIES || useClassification,
    },
    summary,
    items,
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  const csvHeaders = [
    "ownerName",
    "priorityTier",
    "enrichmentPriority",
    "verificationStatus",
    "primaryCountry",
    "registrySystem",
    "bridgeStrategy",
    "calaPropertyCount",
    "entitySearchName",
    "commercialRegistryUrl",
    "tourismRegistryUrl",
    "nextAction",
    "primaryContactEmail",
    "airtableOwnerId",
  ];
  const csvLines = [
    csvHeaders.join(","),
    ...items.map((item) =>
      csvHeaders
        .map((h) => {
          if (h === "airtableOwnerId") return csvEscape(item.id);
          return csvEscape(item[h]);
        })
        .join(",")
    ),
  ];
  writeFileSync(REPORT_CSV, csvLines.join("\n"));
  writeFileSync(REPORT_MD, buildMarkdownReport(items, summary));

  console.log(`Queue: ${items.length} items (source=${loaded.source})`);
  console.log("Wrote", REPORT_JSON);
  console.log("Wrote", REPORT_CSV);
  console.log("Wrote", REPORT_MD);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
