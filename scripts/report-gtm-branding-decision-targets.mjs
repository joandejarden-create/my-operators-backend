/**
 * Report CALA hotel owners likely approaching branding / operator / development decisions,
 * with verified contact details for outreach.
 *
 * Usage:
 *   node scripts/report-gtm-branding-decision-targets.mjs
 *   node scripts/report-gtm-branding-decision-targets.mjs --outreach-ready-only
 *   node scripts/report-gtm-branding-decision-targets.mjs --pre-decision-only --outreach-ready-only --country=Mexico
 *
 * Writes:
 *   reports/gtm-branding-decision-targets.json
 *   reports/gtm-branding-decision-targets.csv
 *   reports/gtm-branding-decision-targets.md
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, readFileSync, existsSync } from "fs";
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
  pickPrimaryContact,
  scoreContactForOwnerPrimary,
  classifyContactRelevance,
  buildEnrichmentOwnerIndex,
  buildOwnerTargetIndexes,
} from "../lib/gtm-owner-target/owner-contact-sync.js";
import {
  buildContactCalaMatchContext,
  classifyContactCalaFootprint,
} from "../lib/gtm-owner-target/contact-cala-match.js";
import {
  findCompanyProfileEnrichment,
} from "../lib/gtm-owner-target/company-profile-enrichments.js";
import { isVerifiedOwnerContact } from "../lib/gtm-owner-target/icp-classify.js";
import { resolveContactPhoneFields } from "../lib/gtm-owner-target/registry-phone-verification.js";
import {
  COMPANY_PROFILE_ENRICHMENTS,
} from "../lib/gtm-owner-target/company-profile-enrichments.js";
import { resolveMxCorporateSeed } from "../lib/gtm-owner-target/adapters/mx-corporate-web-seeds.js";
import {
  scoreOwnerBrandingIntent,
  buildBrandingOutreachTarget,
  parseDevelopmentCountFromProfileNotes,
  MAP_BRANDING_DECISION_CONFIG,
} from "../lib/gtm-owner-target/branding-decision-signals.js";
import { isCalaCountry } from "../lib/gtm-owner-target/cala-footprint.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_JSON = join(ROOT, "reports", "gtm-branding-decision-targets.json");
const OUT_CSV = join(ROOT, "reports", "gtm-branding-decision-targets.csv");
const OUT_MD = join(ROOT, "reports", "gtm-branding-decision-targets.md");

const OUTREACH_READY_ONLY = process.argv.includes("--outreach-ready-only");
const PRE_DECISION_ONLY = process.argv.includes("--pre-decision-only");
const BRAND_DECISION_ONLY = process.argv.includes("--brand-decision-only");
const minScoreArg = process.argv.find((a) => a.startsWith("--min-score="));
const MIN_SCORE = minScoreArg ? Number(minScoreArg.split("=")[1]) : 25;
const countryArg = process.argv.find((a) => a.startsWith("--country="));
const COUNTRY_FILTER = countryArg ? countryArg.split("=")[1].replace(/^"|"$/g, "") : null;

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function parseArgsFromClassificationFallback() {
  const path = join(ROOT, "reports", "gtm-owner-target-icp-classification.json");
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

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
  const seed = resolveMxCorporateSeed(ownerName);
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
    primaryContactEmail: primary?.email || ownerRec.primaryContactEmail || "",
    primaryContactPhone: phones.primaryOutreachPhone || primary?.phone || ownerRec.primaryContactPhone || "",
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
  };
}

function filterPropertiesByCountry(properties) {
  if (!COUNTRY_FILTER) return properties;
  const needle = COUNTRY_FILTER.toLowerCase();
  return properties.filter((p) => String(p.country || "").toLowerCase().includes(needle));
}

async function main() {
  assertGtmBaseConfigured();
  assertNotProductBase();
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
    properties = filterPropertiesByCountry(properties.filter((p) => isCalaCountry(p.country)));
    if (COUNTRY_FILTER && properties.length === 0) continue;

    const profile = findCompanyProfileEnrichment(owner.ownerName);
    const devCount = parseDevelopmentCountFromProfileNotes(profile?.company?.internalNotes || "");

    const brandingIntent = scoreOwnerBrandingIntent(properties, {
      ownerName: owner.ownerName,
      icpSegment: owner.icpSegment || "",
      developmentPipelineCount: devCount,
    });

    if (brandingIntent.intentScore < MIN_SCORE) continue;
    if (brandingIntent.primaryDealTrigger === "none_known") continue;

    const contact = resolveOwnerContact(owner, contactRecords, matchContext, enrichmentByOwnerId.get(owner.id));
    const target = buildBrandingOutreachTarget(owner, brandingIntent, contact);

    if (OUTREACH_READY_ONLY && !target.outreachReady) continue;
    if (PRE_DECISION_ONLY && target.brandDecisionTiming === "post_decision") continue;
    if (PRE_DECISION_ONLY && target.dealalityFit === "late_for_brand_rfp") continue;
    if (BRAND_DECISION_ONLY && target.outreachTrack === "integrated_operator_house_brand_only") continue;
    rows.push(target);
  }

  rows.sort(
    (a, b) =>
      Number(b.outreachReady) - Number(a.outreachReady) ||
      b.outreachScore - a.outreachScore ||
      (b.calaPropertyCount || 0) - (a.calaPropertyCount || 0)
  );

  const summary = {
    total: rows.length,
    outreachReady: rows.filter((r) => r.outreachReady).length,
    withVerifiedContact: rows.filter((r) => r.contact.hasVerifiedContact).length,
    byDealTrigger: {},
    byConfidence: {},
    byOutreachTrack: {},
  };
  for (const row of rows) {
    summary.byDealTrigger[row.primaryDealTrigger] = (summary.byDealTrigger[row.primaryDealTrigger] || 0) + 1;
    summary.byConfidence[row.confidence] = (summary.byConfidence[row.confidence] || 0) + 1;
    summary.byOutreachTrack[row.outreachTrack] = (summary.byOutreachTrack[row.outreachTrack] || 0) + 1;
  }

  const outSuffix = COUNTRY_FILTER
    ? `-mx-${COUNTRY_FILTER.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`
    : PRE_DECISION_ONLY
      ? "-pre-decision"
      : "";
  const outJson = OUT_JSON.replace(".json", `${outSuffix}.json`);
  const outCsv = OUT_CSV.replace(".csv", `${outSuffix}.csv`);
  const outMd = OUT_MD.replace(".md", `${outSuffix}.md`);

  mkdirSync(dirname(outJson), { recursive: true });
  writeFileSync(
    outJson,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        filters: {
          minScore: MIN_SCORE,
          country: COUNTRY_FILTER,
          outreachReadyOnly: OUTREACH_READY_ONLY,
          preDecisionOnly: PRE_DECISION_ONLY,
          brandDecisionOnly: BRAND_DECISION_ONLY,
        },
        config: MAP_BRANDING_DECISION_CONFIG,
        dataLimits: [
          "Franchise contract expiry dates are NOT in CoStar — brand_renewal_window is heuristic.",
          "Land purchase / pre-permit hotel sites need news, permits, or advisor intel (Phase 2).",
        ],
        summary,
        items: rows,
      },
      null,
      2
    )
  );

  const csvHeaders = [
    "ownerName",
    "priorityTier",
    "primaryDealTrigger",
    "intentScore",
    "outreachScore",
    "confidence",
    "brandDecisionTiming",
    "dealalityFit",
    "outreachReady",
    "outreachTrack",
    "brandDecisionEligiblePropertyCount",
    "contactName",
    "contactEmail",
    "verifiedContact",
    "calaPropertyCount",
    "countriesSummary",
    "topProperty",
    "topPropertyBrand",
    "signalSummary",
    "pitchAngle",
  ];
  const csvLines = [csvHeaders.join(",")];
  for (const row of rows) {
    const top = row.topProperties?.[0];
    csvLines.push(
      [
        row.ownerName,
        row.priorityTier,
        row.primaryDealTrigger,
        row.intentScore,
        row.outreachScore,
        row.confidence,
        row.brandDecisionTiming,
        row.dealalityFit,
        row.outreachReady,
        row.outreachTrack,
        row.brandDecisionEligiblePropertyCount,
        row.contact.name,
        row.contact.email,
        row.contact.hasVerifiedContact,
        row.calaPropertyCount,
        row.countriesSummary,
        top?.buildingName || "",
        top?.brandAffiliation || "",
        row.signals.map((s) => s.id).join("|"),
        row.pitchAngle,
      ]
        .map(csvEscape)
        .join(",")
    );
  }
  writeFileSync(outCsv, csvLines.join("\n"));

  const md = buildMarkdownReport(rows, summary);
  writeFileSync(outMd, md);

  console.log(`Branding decision targets: ${rows.length}`);
  console.log(`Outreach-ready (score + contact): ${summary.outreachReady}`);
  console.log(`With verified contact: ${summary.withVerifiedContact}`);
  console.log("Wrote", outJson);
  console.log("Wrote", outCsv);
  console.log("Wrote", outMd);
}

function buildMarkdownReport(rows, summary) {
  const lines = [
    "# Branding Decision Targets (CALA)",
    "",
    `Generated: ${new Date().toISOString()}`,
    "",
    "Hotels and owners **likely** approaching brand, operator, or development decisions — ranked by intent score + contact readiness.",
    "",
    "## Important data limits",
    "",
    "- **Franchise renewal dates** are not in CoStar. `brand_renewal_window` uses build/renov year + typical 20-year term — treat as a lead, not a confirmed expiry.",
    "- **Land purchases / greenfield** before CoStar listing need press, permits, or advisor signals (Phase 2).",
    "- **Outreach-ready** = intent score ≥ " +
      MAP_BRANDING_DECISION_CONFIG.outreachReadyMinScore +
      " plus verified contact channel, brand-decision-eligible asset(s), and track ≠ house-brand-only.",
    "",
    "Integrated operators on their **own house brands** (Iberostar, Barceló, etc.) are excluded from brand-decision outreach unless they have third-party-flag or unbranded assets.",
    "",
    `- Total scored owners: **${summary.total}**`,
    `- Outreach-ready: **${summary.outreachReady}**`,
    `- With verified contact: **${summary.withVerifiedContact}**`,
    "",
    "## Outreach-ready now",
    "",
  ];

  const ready = rows.filter((r) => r.outreachReady);
  if (!ready.length) {
    lines.push("_None at current thresholds — lower `--min-score` or enrich contacts._");
  } else {
    for (const row of ready.slice(0, 25)) {
      lines.push(`### ${row.ownerName} (${row.priorityTier}) — ${row.primaryDealTrigger}`);
      lines.push("");
      lines.push(`- **Outreach track:** ${row.outreachTrack} (${row.brandDecisionEligiblePropertyCount || 0} brand-decision assets)`);
      lines.push(`- **Intent / outreach score:** ${row.intentScore} / ${row.outreachScore}`);
      lines.push(`- **Timing:** ${row.brandDecisionTiming} (${row.dealalityFit})`);
      lines.push(`- **Confidence:** ${row.confidence}`);
      lines.push(`- **Contact:** ${row.contact.name || "—"} ${row.contact.email ? `<${row.contact.email}>` : row.contact.linkedIn || ""}`);
      if (row.contact.phonesDisplay) {
        lines.push(`- **Phone:** ${row.contact.phonesDisplay}`);
      } else if (row.contact.phone) {
        lines.push(`- **Phone:** ${row.contact.phone}`);
      }
      lines.push(`- **Verified:** ${row.contact.hasVerifiedContact ? "yes" : "no"}`);
      if (row.topProperties?.[0]) {
        const p = row.topProperties[0];
        lines.push(`- **Lead asset:** ${p.buildingName} — ${p.city}, ${p.country} (${p.brandAffiliation || "Independent"})`);
      }
      lines.push(`- **Signals:** ${row.signals.map((s) => s.label).join("; ")}`);
      lines.push(`- **Pitch:** ${row.pitchAngle}`);
      if (row.dataGaps?.length) lines.push(`- **Data gaps:** ${row.dataGaps.join("; ")}`);
      lines.push("");
    }
  }

  lines.push("## High intent — needs contact research", "");
  for (const row of rows.filter((r) => !r.outreachReady).slice(0, 20)) {
    lines.push(
      `- **${row.ownerName}** (${row.primaryDealTrigger}, score ${row.intentScore}) — ${row.topProperties?.[0]?.buildingName || "portfolio"}`
    );
  }

  lines.push("", "## Commands", "", "```bash", "node scripts/report-gtm-branding-decision-targets.mjs --country=Mexico --brand-decision-only --outreach-ready-only", "node scripts/report-gtm-branding-decision-targets.mjs --country=Mexico --min-score=35", "node scripts/import-gtm-wave1-mx-enrichments.mjs --apply", "```");

  return lines.join("\n");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
