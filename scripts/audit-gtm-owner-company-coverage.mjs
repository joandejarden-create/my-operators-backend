/**
 * Compare GTM Owner Targets (True Owners) against Companies table coverage.
 *
 * Usage:
 *   node scripts/audit-gtm-owner-company-coverage.mjs
 *   node scripts/audit-gtm-owner-company-coverage.mjs --min-properties=2
 *   node scripts/audit-gtm-owner-company-coverage.mjs --tier=A
 *
 * Reports:
 *   reports/gtm-owner-company-coverage.json
 *   reports/gtm-owner-company-coverage-missing.csv
 *   reports/gtm-owner-company-coverage-needs-profile.csv
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { GTM_OWNER_TARGET_TABLES, MAP_GTM_OWNER_TARGET } from "../lib/gtm-owner-target/field-map.js";
import { GTM_COMPANY_TABLE, MAP_GTM_COMPANY } from "../lib/gtm-owner-target/company-field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import { normalizeOwnerKey } from "../lib/gtm-owner-target/normalize.js";
import { COMPANY_PROFILE_ENRICHMENTS } from "../lib/gtm-owner-target/company-profile-enrichments.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-owner-company-coverage.json");
const REPORT_MISSING_CSV = join(__dirname, "..", "reports", "gtm-owner-company-coverage-missing.csv");
const REPORT_NEEDS_PROFILE_CSV = join(
  __dirname,
  "..",
  "reports",
  "gtm-owner-company-coverage-needs-profile.csv"
);

const minPropertiesArg = process.argv.find((a) => a.startsWith("--min-properties="));
const MIN_PROPERTIES = minPropertiesArg ? Number(minPropertiesArg.split("=")[1]) : 0;
const tierArg = process.argv.find((a) => a.startsWith("--tier="));
const TIER_FILTER = tierArg ? tierArg.split("=")[1].toUpperCase() : null;

const LEGAL_SUFFIX_RE =
  /\b(sa de cv|s a de c v|sas|s a s|sa|srl|ltda|llc|inc|corp|gmbh|limited|holdings?|group|grupo|plc|ag|bv|nv|lp|llp)\b/g;

function normalizeForMatch(name) {
  return normalizeOwnerKey(name).replace(LEGAL_SUFFIX_RE, " ").replace(/\s+/g, " ").trim();
}

function splitCompoundOwnerName(ownerName) {
  const raw = String(ownerName || "").trim();
  if (!raw) return [];
  const parts = raw
    .split(/\s*\|\s*/)
    .map((p) => p.replace(/^owner\s*\d+\s*:\s*/i, "").trim())
    .filter(Boolean);
  return parts.length ? parts : [raw];
}

/**
 * @param {string} ownerName
 * @param {Map<string, object[]>} companiesByNorm
 * @param {Map<string, object[]>} companiesByLooseNorm
 */
function findCompanyMatches(ownerName, companiesByNorm, companiesByLooseNorm) {
  const candidates = splitCompoundOwnerName(ownerName);
  /** @type {{ matchType: string, company: object, matchedOn: string }[]} */
  const hits = [];
  const seen = new Set();

  for (const candidate of candidates) {
    const norm = normalizeOwnerKey(candidate);
    const loose = normalizeForMatch(candidate);
    if (!norm) continue;

    const exact = companiesByNorm.get(norm) || [];
    for (const company of exact) {
      const id = company.id;
      if (seen.has(id)) continue;
      seen.add(id);
      hits.push({ matchType: "exact", company, matchedOn: candidate });
    }

    if (!exact.length && loose.length >= 4) {
      const looseHit = companiesByLooseNorm.get(loose) || [];
      for (const company of looseHit) {
        const id = company.id;
        if (seen.has(id)) continue;
        seen.add(id);
        hits.push({ matchType: "loose_exact", company, matchedOn: candidate });
      }
    }
  }

  if (!hits.length) {
    for (const candidate of candidates) {
      const norm = normalizeOwnerKey(candidate);
      const loose = normalizeForMatch(candidate);
      if (norm.length < 4 && loose.length < 4) continue;

      for (const company of companiesByNorm.values()) {
        for (const rec of company) {
          const companyNorm = normalizeOwnerKey(rec.name);
          const companyLoose = normalizeForMatch(rec.name);
          const contains =
            (norm.length >= 4 && (companyNorm.includes(norm) || norm.includes(companyNorm))) ||
            (loose.length >= 4 && (companyLoose.includes(loose) || loose.includes(companyLoose)));
          if (!contains || seen.has(rec.id)) continue;
          seen.add(rec.id);
          hits.push({ matchType: "partial", company: rec, matchedOn: candidate });
        }
      }
    }
  }

  return hits;
}

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows, columns) {
  const header = columns.join(",");
  const lines = rows.map((row) => columns.map((col) => csvEscape(row[col])).join(","));
  return [header, ...lines].join("\n");
}

function companyResearchStatus(company) {
  const overview = String(company.companyOverview || "").trim();
  const website = String(company.website || "").trim();
  const sourceFile = String(company.sourceFile || "").trim();
  const hasManualProfile = /costar_profile_manual/i.test(sourceFile) || overview.length > 0;
  if (hasManualProfile && overview.length > 0) return "profile_complete";
  if (hasManualProfile) return "profile_partial";
  if (website || company.ownedProperties != null || company.operatedProperties != null) {
    return "import_only";
  }
  return "minimal";
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const enrichedNameKeys = new Set();
  for (const profile of COMPANY_PROFILE_ENRICHMENTS) {
    enrichedNameKeys.add(normalizeOwnerKey(profile.company.name));
    for (const name of profile.ownerTarget?.preferredNames || []) {
      enrichedNameKeys.add(normalizeOwnerKey(name));
    }
  }

  const [ownerRecords, companyRecords] = await Promise.all([
    base(GTM_OWNER_TARGET_TABLES.ownerTargets)
      .select({
        fields: [
          MAP_GTM_OWNER_TARGET.ownerName,
          MAP_GTM_OWNER_TARGET.priorityTier,
          MAP_GTM_OWNER_TARGET.propertyCount,
          MAP_GTM_OWNER_TARGET.totalRbaSf,
          MAP_GTM_OWNER_TARGET.countriesSummary,
          MAP_GTM_OWNER_TARGET.marketsSummary,
        ],
      })
      .all(),
    base(GTM_COMPANY_TABLE)
      .select({
        fields: [
          MAP_GTM_COMPANY.company,
          MAP_GTM_COMPANY.companyOverview,
          MAP_GTM_COMPANY.website,
          MAP_GTM_COMPANY.hqCity,
          MAP_GTM_COMPANY.hqCountry,
          MAP_GTM_COMPANY.ownedProperties,
          MAP_GTM_COMPANY.operatedProperties,
          MAP_GTM_COMPANY.sourceFile,
        ],
      })
      .all(),
  ]);

  /** @type {object[]} */
  const companies = companyRecords.map((rec) => ({
    id: rec.id,
    name: String(rec.fields[MAP_GTM_COMPANY.company] || "").trim(),
    companyOverview: rec.fields[MAP_GTM_COMPANY.companyOverview],
    website: rec.fields[MAP_GTM_COMPANY.website],
    hqCity: rec.fields[MAP_GTM_COMPANY.hqCity],
    hqCountry: rec.fields[MAP_GTM_COMPANY.hqCountry],
    ownedProperties: rec.fields[MAP_GTM_COMPANY.ownedProperties],
    operatedProperties: rec.fields[MAP_GTM_COMPANY.operatedProperties],
    sourceFile: rec.fields[MAP_GTM_COMPANY.sourceFile],
  }));

  const companiesByNorm = new Map();
  const companiesByLooseNorm = new Map();
  for (const company of companies) {
    const norm = normalizeOwnerKey(company.name);
    const loose = normalizeForMatch(company.name);
    if (norm) {
      if (!companiesByNorm.has(norm)) companiesByNorm.set(norm, []);
      companiesByNorm.get(norm).push(company);
    }
    if (loose) {
      if (!companiesByLooseNorm.has(loose)) companiesByLooseNorm.set(loose, []);
      companiesByLooseNorm.get(loose).push(company);
    }
  }

  /** @type {object[]} */
  let owners = ownerRecords.map((rec) => ({
    id: rec.id,
    ownerName: String(rec.fields[MAP_GTM_OWNER_TARGET.ownerName] || "").trim(),
    priorityTier: rec.fields[MAP_GTM_OWNER_TARGET.priorityTier] || "",
    propertyCount: Number(rec.fields[MAP_GTM_OWNER_TARGET.propertyCount] || 0),
    totalRbaSf: Number(rec.fields[MAP_GTM_OWNER_TARGET.totalRbaSf] || 0),
    countriesSummary: rec.fields[MAP_GTM_OWNER_TARGET.countriesSummary] || "",
    marketsSummary: rec.fields[MAP_GTM_OWNER_TARGET.marketsSummary] || "",
  }));

  if (MIN_PROPERTIES > 0) {
    owners = owners.filter((o) => o.propertyCount >= MIN_PROPERTIES);
  }
  if (TIER_FILTER) {
    owners = owners.filter((o) => String(o.priorityTier).toUpperCase() === TIER_FILTER);
  }

  const missing = [];
  const partial = [];
  const matched = [];
  const needsProfile = [];
  const matchedCompanyIds = new Set();

  for (const owner of owners.sort((a, b) => b.propertyCount - a.propertyCount || b.totalRbaSf - a.totalRbaSf)) {
    const hits = findCompanyMatches(owner.ownerName, companiesByNorm, companiesByLooseNorm);
    const best = hits.find((h) => h.matchType === "exact" || h.matchType === "loose_exact") || hits[0];
    const ownerNorm = normalizeOwnerKey(owner.ownerName);
    const hasEnrichmentProfile = enrichedNameKeys.has(ownerNorm);

    if (!best) {
      missing.push({
        ...owner,
        researchAction: "create_company_and_profile",
        hasEnrichmentProfile,
      });
      continue;
    }

    const company = best.company;
    matchedCompanyIds.add(company.id);
    const researchStatus = companyResearchStatus(company);
    const row = {
      ...owner,
      matchType: best.matchType,
      matchedOn: best.matchedOn,
      companyId: company.id,
      companyName: company.name,
      researchStatus,
      hasCompanyOverview: Boolean(String(company.companyOverview || "").trim()),
      hasEnrichmentProfile: hasEnrichmentProfile || enrichedNameKeys.has(normalizeOwnerKey(company.name)),
    };

    if (best.matchType === "partial") {
      partial.push(row);
    } else {
      matched.push(row);
    }

    if (researchStatus !== "profile_complete") {
      needsProfile.push({
        ownerName: owner.ownerName,
        priorityTier: owner.priorityTier,
        propertyCount: owner.propertyCount,
        countriesSummary: owner.countriesSummary,
        companyName: company.name,
        matchType: best.matchType,
        researchStatus,
        hasCompanyOverview: row.hasCompanyOverview,
        website: company.website || "",
        researchAction:
          researchStatus === "import_only"
            ? "add_costar_profile_screenshot"
            : researchStatus === "minimal"
              ? "verify_match_and_add_profile"
              : "complete_company_overview",
      });
    }
  }

  const unmatchedCompanies = companies
    .filter((company) => !matchedCompanyIds.has(company.id))
    .map((company) => ({
      companyId: company.id,
      companyName: company.name,
      hqCity: company.hqCity || "",
      hqCountry: company.hqCountry || "",
      researchStatus: companyResearchStatus(company),
    }))
    .sort((a, b) => a.companyName.localeCompare(b.companyName));

  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    filters: {
      minProperties: MIN_PROPERTIES,
      tier: TIER_FILTER,
    },
    summary: {
      ownerTargetCount: owners.length,
      companyCount: companies.length,
      matchedCount: matched.length,
      partialMatchCount: partial.length,
      missingCompanyCount: missing.length,
      needsProfileCount: needsProfile.length,
      orphanCompanyCount: unmatchedCompanies.length,
      missingByTier: {
        A: missing.filter((o) => o.priorityTier === "A").length,
        B: missing.filter((o) => o.priorityTier === "B").length,
        C: missing.filter((o) => o.priorityTier === "C").length,
      },
      needsProfileByTier: {
        A: needsProfile.filter((o) => o.priorityTier === "A").length,
        B: needsProfile.filter((o) => o.priorityTier === "B").length,
        C: needsProfile.filter((o) => o.priorityTier === "C").length,
      },
    },
    missingCompanies: missing,
    partialMatches: partial,
    needsProfile,
    orphanCompanies: unmatchedCompanies,
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  writeFileSync(
    REPORT_MISSING_CSV,
    toCsv(missing, [
      "ownerName",
      "priorityTier",
      "propertyCount",
      "totalRbaSf",
      "countriesSummary",
      "marketsSummary",
      "researchAction",
      "hasEnrichmentProfile",
    ])
  );

  writeFileSync(
    REPORT_NEEDS_PROFILE_CSV,
    toCsv(needsProfile, [
      "ownerName",
      "priorityTier",
      "propertyCount",
      "countriesSummary",
      "companyName",
      "matchType",
      "researchStatus",
      "hasCompanyOverview",
      "website",
      "researchAction",
    ])
  );

  console.log("GTM Owner → Company coverage audit");
  console.log(`  Owner Targets analyzed: ${owners.length}`);
  console.log(`  Companies in base: ${companies.length}`);
  console.log(`  Matched: ${matched.length}`);
  console.log(`  Partial match (review): ${partial.length}`);
  console.log(`  Missing company record: ${missing.length} (A=${report.summary.missingByTier.A}, B=${report.summary.missingByTier.B}, C=${report.summary.missingByTier.C})`);
  console.log(`  Has company, needs profile: ${needsProfile.length} (A=${report.summary.needsProfileByTier.A})`);
  console.log(`  Orphan companies (no owner target): ${unmatchedCompanies.length}`);
  console.log("\nTop missing (by property count):");
  for (const row of missing.slice(0, 15)) {
    console.log(
      `  [${row.priorityTier}] ${row.ownerName} — ${row.propertyCount} props, ${row.countriesSummary}`
    );
  }
  console.log("\nTop needs-profile (Tier A):");
  for (const row of needsProfile.filter((r) => r.priorityTier === "A").slice(0, 15)) {
    console.log(`  ${row.ownerName} → ${row.companyName} (${row.researchStatus})`);
  }
  console.log(`\nWrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MISSING_CSV}`);
  console.log(`Wrote ${REPORT_NEEDS_PROFILE_CSV}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
