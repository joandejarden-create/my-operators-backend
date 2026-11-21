/**
 * Non-PII measurement for public-source contact discovery.
 * Distinguishes research findings from paid Surfe/FullEnrich returns.
 * Never stores emails, phones, or raw provider payloads.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const GDI_PUBLIC_CONTACT_RESEARCH_METRICS_V1 =
  "gdi-public-contact-research-metrics-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v == null ? "" : v).trim();
}

function metricsDir(env = process.env) {
  if (env.GDI_PUBLIC_CONTACT_METRICS_DIR) {
    return path.resolve(env.GDI_PUBLIC_CONTACT_METRICS_DIR);
  }
  return path.join(ROOT, "data/group-demand-intelligence/public-contact-research");
}

export function publicContactResearchMetricsPath(env = process.env) {
  if (env.GDI_PUBLIC_CONTACT_METRICS_PATH) {
    return path.resolve(env.GDI_PUBLIC_CONTACT_METRICS_PATH);
  }
  return path.join(metricsDir(env), "gdi-public-contact-research-metrics-v1.json");
}

function emptyDoc() {
  return {
    version: GDI_PUBLIC_CONTACT_RESEARCH_METRICS_V1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    totals: {
      namedContactsResearched: 0,
      distinctPersonKeys: 0,
      verifiedDirectEmailFinds: 0,
      verifiedDirectPhoneFinds: 0,
      verifiedOrgEmailRoutes: 0,
      verifiedOrgPhoneRoutes: 0,
      notFoundEmail: 0,
      notFoundPhone: 0,
      candidateUnverified: 0,
      staleOrConflicted: 0,
      surfeEmailWouldBeRequired: 0,
      surfePhoneWouldBeRequired: 0,
      surfeLookupsAvoidedEmail: 0,
      surfeLookupsAvoidedPhone: 0,
      fullEnrichFallbackEligible: 0,
      crossHotelReuses: 0,
      paidPublicSearchRuns: 0,
      estimatedPaidSearchCostUsd: 0,
      researchMsTotal: 0,
      persistBlocked: 0,
    },
    events: [],
  };
}

function readDoc(env = process.env) {
  const file = publicContactResearchMetricsPath(env);
  try {
    if (fs.existsSync(file)) {
      const doc = JSON.parse(fs.readFileSync(file, "utf8"));
      if (doc && typeof doc === "object") return { file, doc };
    }
  } catch {
    /* fall through */
  }
  return { file, doc: emptyDoc() };
}

function atomicWrite(file, doc) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(doc, null, 2));
  fs.renameSync(tmp, file);
}

/**
 * Append a non-PII research event from a workflow result.
 */
export function recordPublicContactResearchEvent(result = {}, env = process.env) {
  const { file, doc } = readDoc(env);
  const v = result.verification || result.after || {};
  const m = result.metrics || {};
  const event = {
    at: new Date().toISOString(),
    hotelId: text(result.hotelId) || null,
    opportunityId: text(result.opportunity?.id || result.opportunity?.opportunityId) || null,
    personIdentityKey: text(result.personIdentityKey) || null,
    researchStatus: text(result.researchStatus) || null,
    emailStatus: text(v.email?.status) || null,
    phoneStatus: text(v.phone?.status) || null,
    hasVerifiedDirectEmail: v.hasVerifiedDirectEmail === true,
    hasVerifiedDirectPhone: v.hasVerifiedDirectPhone === true,
    hasVerifiedOrgEmailRoute: v.hasVerifiedOrgEmailRoute === true,
    hasVerifiedOrgPhoneRoute: v.hasVerifiedOrgPhoneRoute === true,
    surfeEmailEligible: v.surfeEmailEligible === true,
    surfePhoneEligible: v.surfePhoneEligible === true,
    crossHotelReuse: m.crossHotelReuse === true,
    paidPublicSearchUsed: m.paidPublicSearchUsed === true,
    estimatedPaidSearchCostUsd: Number(m.estimatedPaidSearchCostUsd) || 0,
    researchMs: Number(m.researchMs) || 0,
    persistAttempted: m.persistAttempted === true,
    surfeAttempted: m.surfeAttempted === true,
    fullEnrichAttempted: m.fullEnrichAttempted === true,
  };

  doc.events = Array.isArray(doc.events) ? doc.events : [];
  doc.events.push(event);
  if (doc.events.length > 2000) doc.events = doc.events.slice(-2000);

  const t = doc.totals;
  t.namedContactsResearched += 1;
  if (event.hasVerifiedDirectEmail) {
    t.verifiedDirectEmailFinds += 1;
    t.surfeLookupsAvoidedEmail += 1;
  }
  if (event.hasVerifiedDirectPhone) {
    t.verifiedDirectPhoneFinds += 1;
    t.surfeLookupsAvoidedPhone += 1;
  }
  if (event.hasVerifiedOrgEmailRoute) t.verifiedOrgEmailRoutes += 1;
  if (event.hasVerifiedOrgPhoneRoute) t.verifiedOrgPhoneRoutes += 1;
  if (event.emailStatus === "NOT_FOUND") t.notFoundEmail += 1;
  if (event.phoneStatus === "NOT_FOUND") t.notFoundPhone += 1;
  if (
    event.emailStatus === "CANDIDATE_UNVERIFIED" ||
    event.phoneStatus === "CANDIDATE_UNVERIFIED"
  ) {
    t.candidateUnverified += 1;
  }
  if (
    event.emailStatus === "STALE_OR_CONFLICTED" ||
    event.phoneStatus === "STALE_OR_CONFLICTED"
  ) {
    t.staleOrConflicted += 1;
  }
  if (event.surfeEmailEligible) t.surfeEmailWouldBeRequired += 1;
  if (event.surfePhoneEligible) t.surfePhoneWouldBeRequired += 1;
  if (event.crossHotelReuse) t.crossHotelReuses += 1;
  if (event.paidPublicSearchUsed) t.paidPublicSearchRuns += 1;
  t.estimatedPaidSearchCostUsd += event.estimatedPaidSearchCostUsd;
  t.researchMsTotal += event.researchMs;
  if (!event.persistAttempted) t.persistBlocked += 1;

  const keys = new Set(
    doc.events.map((e) => e.personIdentityKey).filter(Boolean),
  );
  t.distinctPersonKeys = keys.size;
  doc.updatedAt = event.at;
  atomicWrite(file, doc);
  return { path: file, event, totals: t };
}

export function summarizePublicContactResearchMetrics(env = process.env) {
  const { doc } = readDoc(env);
  const t = doc.totals || {};
  const researched = Number(t.namedContactsResearched) || 0;
  return {
    version: GDI_PUBLIC_CONTACT_RESEARCH_METRICS_V1,
    generatedAt: new Date().toISOString(),
    totals: t,
    rates: {
      publicWorkEmailDiscoveryRate:
        researched > 0
          ? Math.round((t.verifiedDirectEmailFinds / researched) * 1000) / 10
          : null,
      publicDirectPhoneDiscoveryRate:
        researched > 0
          ? Math.round((t.verifiedDirectPhoneFinds / researched) * 1000) / 10
          : null,
      pctRequiringSurfeEmail:
        researched > 0
          ? Math.round((t.surfeEmailWouldBeRequired / researched) * 1000) / 10
          : null,
      avgResearchMs:
        researched > 0
          ? Math.round(t.researchMsTotal / researched)
          : null,
    },
    privacy: {
      includesEmails: false,
      includesPhones: false,
      includesProviderPayloads: false,
    },
  };
}
