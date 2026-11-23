#!/usr/bin/env node
/**
 * GDI Contact Intelligence V2.1 — re-evaluate frozen Stage-2 cohort.
 *
 * Uses STAGE2_COHORT_FREEZE.json (does not rebuild / substitute contacts).
 * Free research only. Live writes hard-off. Exercises cross-hotel reuse.
 *
 *   node scripts/gdi-contact-intelligence-v2-1-stage2-reeval.mjs
 *   node scripts/gdi-contact-intelligence-v2-1-stage2-reeval.mjs --data-root <path>
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

const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-1-stage2-reeval",
);
fs.mkdirSync(OUT_DIR, { recursive: true });

const FREEZE_PATH =
  argVal("--freeze") ||
  path.join(
    ROOT,
    "reports/gdi-contact-intelligence-v2-stage2-eval/STAGE2_COHORT_FREEZE.json",
  );
const BASELINE_SUMMARY_PATH = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-stage2-eval/STAGE2_EVAL_SUMMARY.json",
);
const DATA_ROOT =
  argVal("--data-root") ||
  process.env.GDI_DATA_ROOT ||
  (fs.existsSync(
    path.join("C:/Dev/dealality/ws-gdi/data/group-demand-intelligence/hotels"),
  )
    ? "C:/Dev/dealality/ws-gdi/data/group-demand-intelligence"
    : path.join(ROOT, "data/group-demand-intelligence"));

process.env.GDI_DATA_ROOT = DATA_ROOT;
process.env.GDI_PUBLIC_CONTACT_METRICS_DIR = path.join(OUT_DIR, "metrics-run");
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
const { classifyUnresolvedCohort } = await import(
  "../lib/group-demand-intelligence/presentation/gdi-contact-intelligence-v2-1-unresolved-classification-v1.js"
);
const {
  resolvePreferredEmployerDomain,
  isWeakEmployerOrganizationName,
} = await import(
  "../lib/group-demand-intelligence/presentation/gdi-public-contact-employer-domain-v1.js"
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
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

function findOpportunityForPerson(person) {
  for (const hotelId of person.hotelIds || []) {
    const doc = loadHotelOpportunities(hotelId);
    for (const o of doc.opportunities || []) {
      const name = text(o.primaryContact?.name || o.contactName);
      const org = text(o.organizationName || o.displayCompanyName || o.company);
      const key = publicContactPersonKey({ name, organization: org });
      if (key && key === person.personIdentityKey) {
        return { opportunity: o, hotelId };
      }
      if (
        text(name).toLowerCase() === text(person.name).toLowerCase() &&
        text(org).toLowerCase() === text(person.organization).toLowerCase()
      ) {
        return { opportunity: o, hotelId };
      }
    }
  }
  return { opportunity: null, hotelId: person.hotelIds?.[0] || null };
}

function stripChannels(opportunity) {
  const next = structuredClone(opportunity);
  if (next.primaryContact) {
    delete next.primaryContact.email;
    delete next.primaryContact.workEmail;
    delete next.primaryContact.phone;
    delete next.primaryContact.directPhone;
  }
  delete next.contactEmail;
  delete next.contactPhone;
  delete next.primaryContactEmail;
  delete next.primaryContactPhone;
  return next;
}

const freeze = JSON.parse(fs.readFileSync(FREEZE_PATH, "utf8"));
const baseline = fs.existsSync(BASELINE_SUMMARY_PATH)
  ? JSON.parse(fs.readFileSync(BASELINE_SUMMARY_PATH, "utf8"))
  : null;

clearPublicContactResearchCacheForTests();

const rows = [];
let wrongPersonOrEmployer = 0;
let unresolved = 0;
let reused = 0;
let searchAttempts = 0;
let avoidedFetches = 0;

for (const person of freeze.people || []) {
  const { opportunity: foundOpp, hotelId } = findOpportunityForPerson(person);
  if (!foundOpp) {
    rows.push({
      personIdentityKey: person.personIdentityKey,
      name: person.name,
      organization: person.organization,
      hotelId: null,
      hotelCount: (person.hotelIds || []).length,
      coverage: person.coverage,
      error: "OPPORTUNITY_NOT_FOUND_IN_DATA_ROOT",
      researchStatus: "PUBLIC_RESEARCH_INCOMPLETE",
      before: {},
      after: {},
      pagesFetched: 0,
      crossHotelReuse: false,
      persistenceBlocks: ["NO_VERIFIED_PUBLIC_CHANNEL", "LIVE_WRITES_DISABLED"],
      persistenceGatePassed: false,
      hypotheticalPersistOk: false,
    });
    unresolved += 1;
    continue;
  }

  const domainResolve = resolvePreferredEmployerDomain({
    organizationDomain: foundOpp.organizationDomain,
    organizationName: person.organization,
    sourceUrls: [
      foundOpp.primaryContact?.sourceUrl,
      foundOpp.sourceUrl,
      foundOpp.evidenceUrl,
    ].filter(Boolean),
  });
  const opportunity = {
    ...foundOpp,
    organizationDomain:
      domainResolve.preferredDomain || foundOpp.organizationDomain || null,
  };

  const before = assessPublicContactVerification(opportunity);
  const result = await runPublicContactDiscoveryWorkflow({
    opportunity,
    hotelId,
    authorizePaidPublicSearch: false,
    persist: false,
    reuseCrossHotelCache: true,
    effort: { maxQueries: 0, maxPages: 4, stopWhenDirectEmailAndPhone: true },
    skipDns: false,
  });
  const after = result.after || before;

  // Multi-hotel reuse exercise: second hotel with channels stripped
  let reuseResult = null;
  if ((person.hotelIds || []).length > 1) {
    const secondHotel =
      (person.hotelIds || []).find((h) => h !== hotelId) || person.hotelIds[1];
    const stripped = stripChannels(result.opportunity || opportunity);
    reuseResult = await runPublicContactDiscoveryWorkflow({
      opportunity: stripped,
      hotelId: secondHotel,
      authorizePaidPublicSearch: false,
      persist: false,
      reuseCrossHotelCache: true,
      effort: { maxQueries: 0, maxPages: 1 },
      skipDns: true,
    });
    if (reuseResult.metrics?.crossHotelReuse === true) {
      reused += 1;
      avoidedFetches += Math.max(
        0,
        1 - Number(reuseResult.metrics?.pagesFetched || 0),
      );
    }
  }

  const readinessActual = assessPublicContactPersistenceReadiness({
    opportunity: result.opportunity || opportunity,
    verification: after,
    hotelId,
  });
  const readinessHypothetical = assessPublicContactPersistenceReadiness({
    opportunity: result.opportunity || opportunity,
    verification: after,
    hotelId,
    excludeLiveWritesDisabledBlock: true,
  });

  if (result.ok) recordPublicContactResearchEvent(result);

  const personGateFail =
    !looksLikePersonName(person.name) ||
    isWeakEmployerOrganizationName(person.organization);
  if (personGateFail) wrongPersonOrEmployer += 1;

  const resolved =
    after.hasVerifiedDirectEmail === true ||
    after.hasVerifiedDirectPhone === true ||
    after.hasVerifiedOrgEmailRoute === true ||
    after.hasVerifiedOrgPhoneRoute === true;
  if (!resolved) unresolved += 1;
  searchAttempts += Number(result.metrics?.pagesFetched || 0);

  rows.push({
    personIdentityKey: person.personIdentityKey,
    name: person.name,
    organization: person.organization,
    hotelId,
    hotelCount: (person.hotelIds || []).length,
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
      emailStatus: after.email?.status || null,
      phoneStatus: after.phone?.status || null,
      directEmail: after.hasVerifiedDirectEmail === true,
      directPhone: after.hasVerifiedDirectPhone === true,
      orgEmail: after.hasVerifiedOrgEmailRoute === true,
      orgPhone: after.hasVerifiedOrgPhoneRoute === true,
    },
    incremental: {
      directEmail:
        before.hasVerifiedDirectEmail !== true &&
        after.hasVerifiedDirectEmail === true,
      directPhone:
        before.hasVerifiedDirectPhone !== true &&
        after.hasVerifiedDirectPhone === true,
      orgEmail:
        before.hasVerifiedOrgEmailRoute !== true &&
        after.hasVerifiedOrgEmailRoute === true,
      orgPhone:
        before.hasVerifiedOrgPhoneRoute !== true &&
        after.hasVerifiedOrgPhoneRoute === true,
    },
    researchStatus: result.researchStatus || result.error || null,
    stoppingReason: result.stoppingReason || null,
    crossHotelReuse: reuseResult?.metrics?.crossHotelReuse === true,
    pagesFetched: result.metrics?.pagesFetched || 0,
    paidPublicSearchUsed: false,
    surfeAttempted: false,
    persistAttempted: false,
    persistenceBlocks: readinessActual.blocks || [],
    persistenceGatePassed: readinessActual.gatePassed === true,
    hypotheticalPersistOk:
      readinessHypothetical.hypotheticalGatePassedExcludingLiveWrite === true ||
      readinessHypothetical.gatePassed === true,
    hypotheticalBlocks: readinessHypothetical.blocksExcludingLiveWrite || [],
    identityRejection: personGateFail,
    personNameGate: looksLikePersonName(person.name),
    weakEmployer: isWeakEmployerOrganizationName(person.organization),
  });
}

const unresolvedRows = rows.filter(
  (r) =>
    r.after?.directEmail !== true &&
    r.after?.directPhone !== true &&
    r.after?.orgEmail !== true &&
    r.after?.orgPhone !== true,
);
const classification = classifyUnresolvedCohort(unresolvedRows);

const beforeEmail = rows.filter((r) => r.before.directEmail).length;
const afterEmail = rows.filter((r) => r.after.directEmail).length;
const beforePhone = rows.filter((r) => r.before.directPhone).length;
const afterPhone = rows.filter((r) => r.after.directPhone).length;

const persistenceActualPass = rows.filter((r) => r.persistenceGatePassed).length;
const persistenceHypoPass = rows.filter((r) => r.hypotheticalPersistOk).length;

const hypoBlockCounts = {};
for (const r of rows) {
  for (const b of r.hypotheticalBlocks || []) {
    hypoBlockCounts[b] = (hypoBlockCounts[b] || 0) + 1;
  }
  if ((r.hypotheticalBlocks || []).length === 0 && !r.hypotheticalPersistOk) {
    /* noop */
  }
}

const summary = {
  schema: "gdi-contact-intelligence-v2-1-stage2-reeval-v1",
  evaluatedAt: new Date().toISOString(),
  publicationState: "CUSTOMER_BLOCKED",
  freezePath: FREEZE_PATH,
  freezeDistinctPeople: freeze.distinctPeople,
  cohortSize: rows.length,
  costUsd: 0,
  paidProvidersAttempted: {
    surfe: false,
    findymail: false,
    pdl: false,
    prospeo: false,
    fullenrich: false,
    paidPublicSearch: false,
  },
  baselineV2: baseline
    ? {
        verifiedDirectEmail: baseline.after?.verifiedDirectEmail,
        verifiedDirectPhone: baseline.after?.verifiedDirectPhone,
        verifiedOrgEmail: baseline.after?.verifiedOrgEmail,
        verifiedOrgPhone: baseline.after?.verifiedOrgPhone,
        unresolvedContacts: baseline.unresolvedContacts,
        reusedContacts: baseline.reusedContacts,
        searchAttemptsPages: baseline.searchAttemptsPages,
      }
    : {
        verifiedDirectEmail: 33,
        verifiedDirectPhone: 31,
        verifiedOrgEmail: 4,
        verifiedOrgPhone: 0,
        unresolvedContacts: 35,
        reusedContacts: 0,
        searchAttemptsPages: 112,
      },
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
    directEmail: rows.filter((r) => r.incremental?.directEmail).length,
    directPhone: rows.filter((r) => r.incremental?.directPhone).length,
    orgEmailRoute: rows.filter((r) => r.incremental?.orgEmail).length,
    orgPhoneRoute: rows.filter((r) => r.incremental?.orgPhone).length,
  },
  wrongPersonOrEmployerRejections: wrongPersonOrEmployer,
  unresolvedContacts: unresolved,
  reusedContacts: reused,
  avoidedDuplicatePageFetches: avoidedFetches,
  crossHotelReuseOpportunities: rows.filter((r) => r.hotelCount > 1).length,
  searchAttemptsPages: searchAttempts,
  persistence: {
    actualGatePassed: persistenceActualPass,
    hypotheticalExcludingLiveWrite: persistenceHypoPass,
    classification: "PERSISTENCE_DESIGNED",
    liveWrites: "DISABLED",
    hypotheticalBlockCounts: hypoBlockCounts,
  },
  unresolvedClassification: {
    byPrimary: classification.byPrimary,
    byRecovery: classification.byRecovery,
    prioritizedCount: classification.prioritizedForFreeResearch.length,
  },
  metricsLedger: summarizePublicContactResearchMetrics(),
};

fs.writeFileSync(
  path.join(OUT_DIR, "V21_REEVAL_RESULTS.json"),
  JSON.stringify({ summary, rows, classification }, null, 2),
);
fs.writeFileSync(
  path.join(OUT_DIR, "V21_REEVAL_SUMMARY.json"),
  JSON.stringify(summary, null, 2),
);
fs.writeFileSync(
  path.join(OUT_DIR, "V21_UNRESOLVED_CLASSIFICATION.json"),
  JSON.stringify(classification, null, 2),
);

console.log(JSON.stringify(summary, null, 2));
