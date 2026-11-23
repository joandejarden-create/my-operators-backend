#!/usr/bin/env node
/**
 * GDI Contact Intelligence V2 — Stage-2 public discovery evaluation (IC).
 *
 * Distinct from:
 *   - 15-contact Findymail evaluation
 *   - 30-contact public evaluation (foundation PR #536)
 *
 * - Cohort size 60–100 (default 80)
 * - Person-level only (org placeholders excluded)
 * - Canonical person-key dedupe
 * - Free / already-authorized research only (no Surfe/Findymail/PDL/Prospeo/FullEnrich)
 * - Live writes hard-off
 *
 * Usage:
 *   node scripts/gdi-contact-intelligence-v2-stage2-public-eval-v1.mjs
 *   node scripts/gdi-contact-intelligence-v2-stage2-public-eval-v1.mjs --limit 80 --data-root <path>
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const args = process.argv.slice(2);
function argVal(flag) {
  const i = args.indexOf(flag);
  return i >= 0 ? args[i + 1] : null;
}
const LIMIT = Math.max(60, Math.min(100, Number(argVal("--limit") || 80)));
const DATA_ROOT =
  argVal("--data-root") ||
  process.env.GDI_DATA_ROOT ||
  (fs.existsSync(path.join("C:/Dev/dealality/ws-gdi/data/group-demand-intelligence/hotels"))
    ? "C:/Dev/dealality/ws-gdi/data/group-demand-intelligence"
    : path.join(ROOT, "data/group-demand-intelligence"));

process.env.GDI_DATA_ROOT = DATA_ROOT;
process.env.GDI_PUBLIC_CONTACT_METRICS_DIR = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-stage2-eval/metrics-run",
);

const OUT_DIR = path.join(ROOT, "reports/gdi-contact-intelligence-v2-stage2-eval");
fs.mkdirSync(OUT_DIR, { recursive: true });
fs.mkdirSync(process.env.GDI_PUBLIC_CONTACT_METRICS_DIR, { recursive: true });

const { looksLikePersonName } = await import(
  "../lib/group-demand-intelligence/secondary-named-contact-v1.js"
);
const {
  runPublicContactDiscoveryWorkflow,
  clearPublicContactResearchCacheForTests,
} = await import(
  "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-workflow-v1.js"
);
const {
  assessPublicContactVerification,
  publicContactPersonKey,
  PUBLIC_CONTACT_VERIFICATION_STATUS,
} = await import(
  "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js"
);
const { assessPublicContactPersistenceReadiness } = await import(
  "../lib/group-demand-intelligence/presentation/gdi-public-contact-persistence-readiness-v1.js"
);
const {
  recordPublicContactResearchEvent,
  summarizePublicContactResearchMetrics,
} = await import(
  "../lib/group-demand-intelligence/presentation/gdi-public-contact-research-metrics-v1.js"
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function looksNamed(name) {
  const n = text(name);
  if (!looksLikePersonName(n)) return false;
  if (
    /\b(team|desk|office|housing|headquarters|committee|staff|department|contact us|inc|llc|ltd|corp|company|association|society|university|club|juniors|front desk|reservations|sales office)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  return n.split(/\s+/).filter(Boolean).length >= 2;
}

function listHotels() {
  const dir = path.join(DATA_ROOT, "hotels");
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => ({ hotelId: d.name }));
}

function loadHotelOpportunities(hotelId) {
  const file = path.join(DATA_ROOT, "hotels", hotelId, "opportunities.json");
  if (!fs.existsSync(file)) return { opportunities: [] };
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return { opportunities: [] };
  }
}

function publicCoverageHint(o) {
  const urls = [
    o.primaryContact?.linkedinUrl,
    o.primaryContact?.sourceUrl,
    o.sourceUrl,
    o.evidenceUrl,
    ...(Array.isArray(o.evidenceUrls) ? o.evidenceUrls : []),
  ]
    .map(text)
    .filter(Boolean);
  if (urls.length >= 2) return "strong";
  if (urls.length === 1) return "moderate";
  return "weak";
}

function buildCohort(limit) {
  const hotels = listHotels();
  const people = new Map();
  for (const h of hotels) {
    const hotelId = h.hotelId;
    const doc = loadHotelOpportunities(hotelId);
    for (const o of doc.opportunities || []) {
      const name = text(o.primaryContact?.name || o.contactName || o.primaryContactName);
      if (!looksNamed(name)) continue;
      const org = text(o.organizationName || o.displayCompanyName || o.company);
      if (!org) continue;
      const key = publicContactPersonKey({ name, organization: org });
      if (!key) continue;
      const email = text(o.primaryContact?.email || o.contactEmail || o.primaryContactEmail);
      const phone = text(o.primaryContact?.phone || o.contactPhone || o.primaryContactPhone);
      const market = text(h.market || o.market || o.city || "");
      if (!people.has(key)) {
        people.set(key, {
          personIdentityKey: key,
          name,
          org,
          hotelIds: new Set([hotelId]),
          hasEmail: Boolean(email),
          hasPhone: Boolean(phone),
          market,
          opportunity: o,
          hotelId,
          coverage: publicCoverageHint(o),
        });
      } else {
        const p = people.get(key);
        p.hotelIds.add(hotelId);
        if (email) p.hasEmail = true;
        if (phone) p.hasPhone = true;
        if (publicCoverageHint(o) === "strong") p.coverage = "strong";
      }
    }
  }

  const all = [...people.values()];
  const buckets = {
    multiHotel: all.filter((p) => p.hotelIds.size > 1),
    missingBoth: all.filter((p) => !p.hasEmail && !p.hasPhone),
    emailOnly: all.filter((p) => p.hasEmail && !p.hasPhone),
    phoneOnly: all.filter((p) => !p.hasEmail && p.hasPhone),
    both: all.filter((p) => p.hasEmail && p.hasPhone),
    orgRouteLikely: all.filter((p) => !p.hasEmail && !p.hasPhone && p.coverage !== "strong"),
    strongCov: all.filter((p) => p.coverage === "strong"),
    weakCov: all.filter((p) => p.coverage === "weak"),
  };

  const picked = [];
  const pushFrom = (arr, targetCount) => {
    for (const p of arr) {
      if (picked.length >= targetCount) break;
      if (picked.some((x) => x.personIdentityKey === p.personIdentityKey)) continue;
      picked.push(p);
    }
  };

  // Balanced fill toward `limit`
  pushFrom(buckets.multiHotel, Math.min(12, limit));
  pushFrom(buckets.missingBoth, Math.min(28, limit));
  pushFrom(buckets.emailOnly, Math.min(44, limit));
  pushFrom(buckets.phoneOnly, Math.min(52, limit));
  pushFrom(buckets.weakCov, Math.min(64, limit));
  pushFrom(buckets.strongCov, Math.min(72, limit));
  pushFrom(buckets.both, limit);
  pushFrom(all, limit);

  return picked.slice(0, limit).map((p) => ({
    ...p,
    hotelIds: [...p.hotelIds],
  }));
}

clearPublicContactResearchCacheForTests();
const cohort = buildCohort(LIMIT);

const freeze = {
  schema: "gdi-contact-intelligence-v2-stage2-cohort-v1",
  frozenAt: new Date().toISOString(),
  originMainSha: process.env.DEALALITY_GIT_HEAD || null,
  dataRoot: DATA_ROOT,
  limit: LIMIT,
  distinctPeople: cohort.length,
  composition: {
    withEmail: cohort.filter((p) => p.hasEmail).length,
    missingEmail: cohort.filter((p) => !p.hasEmail).length,
    withPhone: cohort.filter((p) => p.hasPhone).length,
    missingPhone: cohort.filter((p) => !p.hasPhone).length,
    missingBoth: cohort.filter((p) => !p.hasEmail && !p.hasPhone).length,
    multiHotel: cohort.filter((p) => p.hotelIds.length > 1).length,
    coverageStrong: cohort.filter((p) => p.coverage === "strong").length,
    coverageModerate: cohort.filter((p) => p.coverage === "moderate").length,
    coverageWeak: cohort.filter((p) => p.coverage === "weak").length,
    distinctHotels: new Set(cohort.flatMap((p) => p.hotelIds)).size,
  },
  people: cohort.map((p) => ({
    personIdentityKey: p.personIdentityKey,
    name: p.name,
    organization: p.org,
    hotelIds: p.hotelIds,
    hasEmail: p.hasEmail,
    hasPhone: p.hasPhone,
    coverage: p.coverage,
    market: p.market,
  })),
};
fs.writeFileSync(path.join(OUT_DIR, "STAGE2_COHORT_FREEZE.json"), JSON.stringify(freeze, null, 2));

const rows = [];
let wrongPersonOrEmployer = 0;
let unresolved = 0;
let reused = 0;
let searchAttempts = 0;

for (const person of cohort) {
  const before = assessPublicContactVerification(person.opportunity);
  const result = await runPublicContactDiscoveryWorkflow({
    opportunity: person.opportunity,
    hotelId: person.hotelId,
    authorizePaidPublicSearch: false,
    persist: false,
    reuseCrossHotelCache: true,
    effort: { maxQueries: 0, maxPages: 3, stopWhenDirectEmailAndPhone: true },
    skipDns: false,
  });
  const after = result.after || before;
  const readiness =
    result.persistenceReadiness ||
    assessPublicContactPersistenceReadiness({
      opportunity: result.opportunity || person.opportunity,
      verification: after,
      hotelId: person.hotelId,
    });
  if (result.ok) recordPublicContactResearchEvent(result);

  const rejection =
    /WRONG_PERSON|WRONG_EMPLOYER|EMPLOYER_MISMATCH|IDENTITY_REJECT/i.test(
      String(result.researchStatus || result.error || ""),
    ) ||
    (Array.isArray(result.methods) &&
      result.methods.some((m) =>
        /WRONG_PERSON|WRONG_EMPLOYER/i.test(String(m?.reason || m?.method || "")),
      ));
  if (rejection) wrongPersonOrEmployer += 1;
  if (result.metrics?.crossHotelReuse === true) reused += 1;
  if (
    after.hasVerifiedDirectEmail !== true &&
    after.hasVerifiedDirectPhone !== true &&
    after.hasVerifiedOrgEmailRoute !== true &&
    after.hasVerifiedOrgPhoneRoute !== true
  ) {
    unresolved += 1;
  }
  searchAttempts += Number(result.metrics?.pagesFetched || 0);

  rows.push({
    personIdentityKey: person.personIdentityKey,
    name: person.name,
    organization: person.org,
    hotelId: person.hotelId,
    hotelCount: person.hotelIds.length,
    coverage: person.coverage,
    before: {
      emailStatus: before.email?.status,
      phoneStatus: before.phone?.status,
      directEmail: before.hasVerifiedDirectEmail === true,
      directPhone: before.hasVerifiedDirectPhone === true,
      orgEmail: before.hasVerifiedOrgEmailRoute === true,
      orgPhone: before.hasVerifiedOrgPhoneRoute === true,
    },
    after: {
      emailStatus: after.email?.status || after.emailStatus || null,
      phoneStatus: after.phone?.status || after.phoneStatus || null,
      directEmail: after.hasVerifiedDirectEmail === true,
      directPhone: after.hasVerifiedDirectPhone === true,
      orgEmail: after.hasVerifiedOrgEmailRoute === true,
      orgPhone: after.hasVerifiedOrgPhoneRoute === true,
    },
    incremental: {
      directEmail:
        before.hasVerifiedDirectEmail !== true && after.hasVerifiedDirectEmail === true,
      directPhone:
        before.hasVerifiedDirectPhone !== true && after.hasVerifiedDirectPhone === true,
      orgEmail:
        before.hasVerifiedOrgEmailRoute !== true && after.hasVerifiedOrgEmailRoute === true,
      orgPhone:
        before.hasVerifiedOrgPhoneRoute !== true && after.hasVerifiedOrgPhoneRoute === true,
    },
    researchStatus: result.researchStatus || result.error || null,
    stoppingReason: result.stoppingReason || result.metrics?.stoppingReason || null,
    crossHotelReuse: result.metrics?.crossHotelReuse === true,
    pagesFetched: result.metrics?.pagesFetched || 0,
    paidPublicSearchUsed: result.metrics?.paidPublicSearchUsed === true,
    surfeAttempted: result.metrics?.surfeAttempted === true,
    persistAttempted: result.metrics?.persistAttempted === true,
    persistenceBlocks: readiness.blocks || [],
    persistenceGatePassed: readiness.gatePassed === true,
    identityRejection: rejection === true,
  });
}

const beforeEmail = rows.filter((r) => r.before.directEmail).length;
const afterEmail = rows.filter((r) => r.after.directEmail).length;
const beforePhone = rows.filter((r) => r.before.directPhone).length;
const afterPhone = rows.filter((r) => r.after.directPhone).length;
const incEmail = rows.filter((r) => r.incremental.directEmail).length;
const incPhone = rows.filter((r) => r.incremental.directPhone).length;
const incOrgEmail = rows.filter((r) => r.incremental.orgEmail).length;
const incOrgPhone = rows.filter((r) => r.incremental.orgPhone).length;
const totalIncChannels = incEmail + incPhone + incOrgEmail + incOrgPhone;

const summary = {
  schema: "gdi-contact-intelligence-v2-stage2-eval-v1",
  evaluatedAt: new Date().toISOString(),
  publicationState: "CUSTOMER_BLOCKED",
  distinctFromPriorEvals: [
    "findymail-15-contact-eval",
    "public-30-contact-eval-pr536",
  ],
  cohortSize: rows.length,
  freezePath: "STAGE2_COHORT_FREEZE.json",
  costUsd: 0,
  paidProvidersAttempted: {
    surfe: rows.some((r) => r.surfeAttempted),
    findymail: false,
    pdl: false,
    prospeo: false,
    fullenrich: false,
    paidPublicSearch: rows.some((r) => r.paidPublicSearchUsed),
  },
  searchAttemptsPages: searchAttempts,
  before: {
    verifiedDirectEmail: beforeEmail,
    verifiedDirectPhone: beforePhone,
    verifiedOrgEmail: rows.filter((r) => r.before.orgEmail).length,
    verifiedOrgPhone: rows.filter((r) => r.before.orgPhone).length,
  },
  after: {
    verifiedDirectEmail: afterEmail,
    verifiedDirectPhone: afterPhone,
    verifiedOrgEmail: rows.filter((r) => r.after.orgEmail).length,
    verifiedOrgPhone: rows.filter((r) => r.after.orgPhone).length,
  },
  incrementalVerified: {
    directEmail: incEmail,
    directPhone: incPhone,
    orgEmailRoute: incOrgEmail,
    orgPhoneRoute: incOrgPhone,
    totalChannels: totalIncChannels,
  },
  wrongPersonOrEmployerRejections: wrongPersonOrEmployer,
  unresolvedContacts: unresolved,
  reusedContacts: reused,
  crossHotelReuseOpportunities: rows.filter((r) => r.hotelCount > 1).length,
  persistence: {
    anyPersistAttempted: rows.some((r) => r.persistAttempted),
    gatePassedCount: rows.filter((r) => r.persistenceGatePassed).length,
    classification: "PERSISTENCE_DESIGNED",
    liveWrites: "DISABLED",
  },
  costPerIncrementalVerifiedChannelUsd:
    totalIncChannels === 0 ? null : 0 / totalIncChannels,
  metricsLedger: summarizePublicContactResearchMetrics(),
};

fs.writeFileSync(path.join(OUT_DIR, "STAGE2_EVAL_RESULTS.json"), JSON.stringify({ summary, rows }, null, 2));
fs.writeFileSync(path.join(OUT_DIR, "STAGE2_EVAL_SUMMARY.json"), JSON.stringify(summary, null, 2));

console.log(JSON.stringify(summary, null, 2));
