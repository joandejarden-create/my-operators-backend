/**
 * Controlled 30-contact public-source discovery evaluation (founder V1).
 *
 * - No Surfe / FullEnrich
 * - No paid public search (authorizePaidPublicSearch=false)
 * - No persistence of newly found details
 * - Never invents emails/phones
 *
 * Usage:
 *   node scripts/gdi-public-source-contact-discovery-20-eval-v1.mjs
 *   node scripts/gdi-public-source-contact-discovery-20-eval-v1.mjs --data-root <path>
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

// Allow reading hotel opportunities from the dirty registered GDI home without mutating it.
const args = process.argv.slice(2);
function argVal(flag) {
  const i = args.indexOf(flag);
  return i >= 0 ? args[i + 1] : null;
}
const DATA_ROOT =
  argVal("--data-root") ||
  process.env.GDI_DATA_ROOT ||
  (fs.existsSync(path.join("C:/Dev/dealality/ws-gdi/data/group-demand-intelligence/hotels"))
    ? "C:/Dev/dealality/ws-gdi/data/group-demand-intelligence"
    : path.join(ROOT, "data/group-demand-intelligence"));

process.env.GDI_DATA_ROOT = DATA_ROOT;
process.env.GDI_PUBLIC_CONTACT_METRICS_DIR = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-foundation-integration-v1/metrics-run",
);

const {
  listGdiSelectableHotels,
  loadOpportunities,
} = await import("../lib/group-demand-intelligence/repository.js");

// Monkeypatch hotelDir via env if repository supports it — otherwise load JSON directly.
function loadHotelOpportunities(hotelId) {
  const file = path.join(DATA_ROOT, "hotels", hotelId, "opportunities.json");
  if (!fs.existsSync(file)) {
    try {
      return loadOpportunities(hotelId);
    } catch {
      return { opportunities: [] };
    }
  }
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return { opportunities: [] };
  }
}

function listHotels() {
  const dir = path.join(DATA_ROOT, "hotels");
  if (fs.existsSync(dir)) {
    return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .map((d) => ({ hotelId: d.name, id: d.name }));
  }
  try {
    return listGdiSelectableHotels() || [];
  } catch {
    return [];
  }
}

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

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function looksNamed(name) {
  const n = text(name);
  if (!looksLikePersonName(n)) return false;
  if (
    /\b(team|desk|office|housing|headquarters|committee|staff|department|contact us|inc|llc|ltd|corp|company|association|society|university|club|juniors)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  return n.split(/\s+/).filter(Boolean).length >= 2;
}

/** Diversified 30-contact cohort: multi-hotel, multi-geo, missing vs present channels. */
function buildCohort(limit = 30) {
  const hotels = listHotels();
  const people = new Map();
  for (const h of hotels) {
    const hotelId = h.hotelId || h.id;
    const doc = loadHotelOpportunities(hotelId);
    const opps = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
    for (const o of opps) {
      const name = text(o.primaryContact?.name || o.contactName || o.primaryContactName);
      if (!looksNamed(name)) continue;
      const org = text(o.organizationName || o.displayCompanyName || o.company);
      if (!org) continue;
      const key = publicContactPersonKey({ name, organization: org });
      if (!key) continue;
      const email = text(o.primaryContact?.email || o.contactEmail || o.primaryContactEmail);
      const phone = text(o.primaryContact?.phone || o.contactPhone);
      const market = text(h.market || o.market || "");
      if (!people.has(key)) {
        people.set(key, {
          personIdentityKey: key,
          name,
          org,
          hotelIds: new Set([hotelId]),
          opportunities: [],
          hasEmail: Boolean(email),
          hasPhone: Boolean(phone),
          market,
          opportunity: o,
          hotelId,
        });
      } else {
        const p = people.get(key);
        p.hotelIds.add(hotelId);
        p.opportunities.push(o.id || o.opportunityId);
        if (email) p.hasEmail = true;
        if (phone) p.hasPhone = true;
      }
      people.get(key).opportunities.push({
        hotelId,
        opportunityId: o.id || o.opportunityId,
      });
    }
  }

  const all = [...people.values()];
  const multiHotel = all.filter((p) => p.hotelIds.size > 1);
  const missingBoth = all.filter((p) => !p.hasEmail && !p.hasPhone);
  const emailOnly = all.filter((p) => p.hasEmail && !p.hasPhone);
  const phoneOnly = all.filter((p) => !p.hasEmail && p.hasPhone);
  const both = all.filter((p) => p.hasEmail && p.hasPhone);

  const picked = [];
  const take = (arr, n) => {
    for (const p of arr) {
      if (picked.length >= limit) break;
      if (picked.some((x) => x.personIdentityKey === p.personIdentityKey)) continue;
      picked.push(p);
      if (picked.filter((x) => arr.includes(x) || true).length && picked.length >= n && arr === multiHotel) {
        /* continue filling from bucket */
      }
    }
  };

  // Prefer diversity across 30-contact cohort
  for (const p of multiHotel) {
    if (picked.length >= 6) break;
    picked.push(p);
  }
  for (const p of missingBoth) {
    if (picked.length >= 14) break;
    if (picked.some((x) => x.personIdentityKey === p.personIdentityKey)) continue;
    picked.push(p);
  }
  for (const p of emailOnly) {
    if (picked.length >= 20) break;
    if (picked.some((x) => x.personIdentityKey === p.personIdentityKey)) continue;
    picked.push(p);
  }
  for (const p of phoneOnly) {
    if (picked.length >= 24) break;
    if (picked.some((x) => x.personIdentityKey === p.personIdentityKey)) continue;
    picked.push(p);
  }
  for (const p of both) {
    if (picked.length >= limit) break;
    if (picked.some((x) => x.personIdentityKey === p.personIdentityKey)) continue;
    picked.push(p);
  }
  for (const p of all) {
    if (picked.length >= limit) break;
    if (picked.some((x) => x.personIdentityKey === p.personIdentityKey)) continue;
    picked.push(p);
  }

  return picked.slice(0, limit).map((p) => ({
    ...p,
    hotelIds: [...p.hotelIds],
  }));
}

clearPublicContactResearchCacheForTests();
const cohort = buildCohort(30);
const rows = [];

for (const person of cohort) {
  const before = assessPublicContactVerification(person.opportunity);
  const result = await runPublicContactDiscoveryWorkflow({
    opportunity: person.opportunity,
    hotelId: person.hotelId,
    authorizePaidPublicSearch: false,
    persist: false,
    reuseCrossHotelCache: true,
    effort: { maxQueries: 0, maxPages: 3, stopWhenDirectEmailAndPhone: true },
    // Network fetch of already-known URLs only (free). Fail soft if offline.
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
  rows.push({
    personIdentityKey: person.personIdentityKey,
    name: person.name,
    organization: person.org,
    hotelId: person.hotelId,
    hotelCount: person.hotelIds.length,
    opportunityId: person.opportunity.id || person.opportunity.opportunityId || null,
    before: {
      emailStatus: before.email.status,
      phoneStatus: before.phone.status,
      directEmail: before.hasVerifiedDirectEmail,
      directPhone: before.hasVerifiedDirectPhone,
      orgEmail: before.hasVerifiedOrgEmailRoute,
      orgPhone: before.hasVerifiedOrgPhoneRoute,
    },
    after: {
      emailStatus: after.email?.status || after.emailStatus || null,
      phoneStatus: after.phone?.status || after.phoneStatus || null,
      directEmail: after.hasVerifiedDirectEmail === true,
      directPhone: after.hasVerifiedDirectPhone === true,
      orgEmail: after.hasVerifiedOrgEmailRoute === true,
      orgPhone: after.hasVerifiedOrgPhoneRoute === true,
    },
    enrichmentDecision: result.enrichmentDecision || null,
    researchStatus: result.researchStatus || result.error || null,
    crossHotelReuse: result.metrics?.crossHotelReuse === true,
    researchMs: result.metrics?.researchMs || 0,
    pagesFetched: result.metrics?.pagesFetched || 0,
    paidPublicSearchUsed: result.metrics?.paidPublicSearchUsed === true,
    surfeAttempted: result.metrics?.surfeAttempted === true,
    persistAttempted: result.metrics?.persistAttempted === true,
    persistenceBlocks: readiness.blocks || [],
    persistenceGatePassed: readiness.gatePassed === true,
    methods: (result.methods || []).map((m) => m.method),
  });
}

const summary = {
  cohortSize: rows.length,
  distinctPersonKeys: new Set(rows.map((r) => r.personIdentityKey)).size,
  multiHotelPeople: rows.filter((r) => r.hotelCount > 1).length,
  before: {
    verifiedDirectEmail: rows.filter((r) => r.before.directEmail).length,
    verifiedDirectPhone: rows.filter((r) => r.before.directPhone).length,
    verifiedOrgEmail: rows.filter((r) => r.before.orgEmail).length,
    verifiedOrgPhone: rows.filter((r) => r.before.orgPhone).length,
    emailNotFound: rows.filter((r) => r.before.emailStatus === "NOT_FOUND").length,
    phoneNotFound: rows.filter((r) => r.before.phoneStatus === "NOT_FOUND").length,
    candidateUnverifiedEmail: rows.filter(
      (r) => r.before.emailStatus === "CANDIDATE_UNVERIFIED",
    ).length,
  },
  after: {
    verifiedDirectEmail: rows.filter((r) => r.after.directEmail).length,
    verifiedDirectPhone: rows.filter((r) => r.after.directPhone).length,
    verifiedOrgEmail: rows.filter((r) => r.after.orgEmail).length,
    verifiedOrgPhone: rows.filter((r) => r.after.orgPhone).length,
    emailNotFound: rows.filter((r) => r.after.emailStatus === "NOT_FOUND").length,
    phoneNotFound: rows.filter((r) => r.after.phoneStatus === "NOT_FOUND").length,
  },
  deltas: {
    newVerifiedDirectEmail: rows.filter(
      (r) => !r.before.directEmail && r.after.directEmail,
    ).length,
    newVerifiedDirectPhone: rows.filter(
      (r) => !r.before.directPhone && r.after.directPhone,
    ).length,
  },
  surfeEmailStillNeeded: rows.filter(
    (r) => r.enrichmentDecision?.workEmail?.surfeAvailable === true,
  ).length,
  surfePhoneStillNeeded: rows.filter(
    (r) => r.enrichmentDecision?.telephone?.surfeAvailable === true,
  ).length,
  surfeEmailAvoided: rows.filter(
    (r) => r.after.directEmail === true,
  ).length,
  crossHotelReuses: rows.filter((r) => r.crossHotelReuse).length,
  paidPublicSearchUsed: rows.filter((r) => r.paidPublicSearchUsed).length,
  surfeAttempted: rows.filter((r) => r.surfeAttempted).length,
  persistAttempted: rows.filter((r) => r.persistAttempted).length,
  persistenceGatesAllBlocked: rows.every((r) => r.persistenceGatePassed !== true),
  wrongPersonEmployerRejections: rows.filter((r) =>
    (r.methods || []).includes("NO_VERIFIED_NAMED_PERSON"),
  ).length,
  estimatedSurfeDependencyReductionPct: null,
  researchSpendUsd: 0,
  costPerIncrementalVerifiedEmailUsd: 0,
  costPerIncrementalVerifiedPhoneUsd: 0,
  avgPagesFetched:
    rows.length > 0
      ? Math.round(
          (rows.reduce((s, r) => s + (r.pagesFetched || 0), 0) / rows.length) * 10,
        ) / 10
      : 0,
  avgResearchMs:
    rows.length > 0
      ? Math.round(rows.reduce((s, r) => s + r.researchMs, 0) / rows.length)
      : 0,
};

const emailMissingBefore = rows.filter((r) => !r.before.directEmail).length;
if (emailMissingBefore > 0) {
  summary.estimatedSurfeDependencyReductionPct =
    Math.round((summary.deltas.newVerifiedDirectEmail / emailMissingBefore) * 1000) / 10;
} else {
  summary.estimatedSurfeDependencyReductionPct = 0;
}

const metricsSummary = summarizePublicContactResearchMetrics(process.env);

const outDir = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-foundation-integration-v1",
);
fs.mkdirSync(outDir, { recursive: true });
const payload = {
  generatedAt: new Date().toISOString(),
  mode: "FREE_AUTHORITATIVE_URL_ONLY",
  authorizePaidPublicSearch: false,
  persist: false,
  surfe: false,
  fullEnrich: false,
  dataRoot: DATA_ROOT,
  summary,
  metricsSummary,
  rows,
};
fs.writeFileSync(
  path.join(outDir, "EVAL_30_RESULTS.json"),
  JSON.stringify(payload, null, 2),
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: path.join(outDir, "EVAL_30_RESULTS.json"),
      summary,
    },
    null,
    2,
  ),
);
