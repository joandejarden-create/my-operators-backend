/**
 * Market Alerts contact enrichment orchestrator (Phase A).
 *
 * Plan path: eligibility → roles → orgs → named person → would-search / would-enrich
 * Execute path: cache → Surfe find → Surfe enrich (gated, credit-capped)
 *
 * Provider failures never throw to callers of Market Alerts sync.
 */

import {
  assessContactEnrichmentEligibility,
  isContactEnrichmentEnabled,
  getContactEnrichmentLimits,
  CONTACT_BLOCK_REASONS,
} from "./eligibility.js";
import { stakeholderRoleLabels, isBlockedGenericRole } from "./stakeholder-roles.js";
import { rankContactCandidates } from "./rank-candidates.js";
import { classifyContactConfidence } from "./confidence.js";
import {
  readPersonContactCache,
  writePersonContactCache,
  readAlertContactCache,
  writeAlertContactCache,
  writePendingEnrichment,
} from "./cache.js";
import { createSurfeContactProvider } from "./surfe-adapter.js";
import { createNoopContactProvider } from "./provider-interface.js";
import { createContactRecord, toContactCards } from "./contact-card.js";
import { logContactOp } from "./safe-log.js";

/**
 * Build a dry-run / planning decision for one alert. Zero Surfe calls.
 */
export function planContactEnrichment(alert = {}) {
  const assessment = assessContactEnrichmentEligibility(alert);
  const limits = getContactEnrichmentLimits();
  const enrichmentEnabled = isContactEnrichmentEnabled();

  if (!assessment.eligible) {
    return {
      eligible: false,
      skipReason: assessment.reason,
      organizations: assessment.organizations || [],
      targetRoles: [],
      namedCandidate: assessment.namedStakeholder || null,
      wouldSearch: false,
      wouldEnrich: false,
      estimatedSurfeCalls: { search: 0, enrich: 0 },
      cachedReuse: false,
      enrichmentEnabled,
    };
  }

  const targetCompany = assessment.organizations[0] || null;
  const named = assessment.namedStakeholder;
  const roleLabels = stakeholderRoleLabels(assessment.roles);
  const surfeTitles = assessment.roles.flatMap((r) => r.surfeTitles || []).slice(0, 8);

  let cachedReuse = false;
  if (named?.personName && targetCompany) {
    const hit = readPersonContactCache({ personName: named.personName, companyName: targetCompany });
    cachedReuse = Boolean(hit);
  }

  const wouldSearch = !cachedReuse && Boolean(targetCompany);
  const wouldEnrich = wouldSearch && !cachedReuse;
  // dry-run with CONTACT_ENRICHMENT_ENABLED=false → would* describe intent only
  const estimatedSearch = wouldSearch ? 1 : 0;
  const estimatedEnrich = wouldEnrich ? 1 : 0;

  return {
    eligible: true,
    skipReason: null,
    providerBlockedReason: enrichmentEnabled ? null : CONTACT_BLOCK_REASONS.DISABLED,
    providerWouldRun: enrichmentEnabled && limits.dryRunProvider !== true ? true : enrichmentEnabled,
    organizations: assessment.organizations,
    targetCompany,
    targetRoles: roleLabels,
    surfeTitles,
    namedCandidate: named,
    wouldSearch,
    wouldEnrich,
    estimatedSurfeCalls: { search: estimatedSearch, enrich: estimatedEnrich },
    cachedReuse,
    enrichmentEnabled,
    actionable: assessment.actionable,
    maxCandidates: limits.maxCandidatesPerAlert,
  };
}

/**
 * Plan many alerts; apply per-run enrichment cap to "would" estimates.
 */
export function planContactEnrichmentBatch(alerts = [], { maxEnrichments } = {}) {
  const limits = getContactEnrichmentLimits();
  const cap = maxEnrichments ?? limits.maxEnrichmentsPerRun;
  const plans = [];
  let enrichBudget = cap;
  let searchTotal = 0;
  let enrichTotal = 0;
  let cached = 0;
  let eligible = 0;
  let skipped = 0;

  for (const alert of alerts) {
    const plan = planContactEnrichment(alert);
    if (!plan.eligible) {
      skipped += 1;
      plans.push({ ...plan, alertId: alert.id || alert.alertId || null, title: alert.title || null });
      continue;
    }
    eligible += 1;
    if (plan.cachedReuse) cached += 1;

    let wouldSearch = plan.wouldSearch;
    let wouldEnrich = plan.wouldEnrich;
    if (wouldEnrich && enrichBudget <= 0) {
      wouldEnrich = false;
      wouldSearch = false;
      plan.skipReason = CONTACT_BLOCK_REASONS.QUOTA;
      plan.eligible = false;
    } else if (wouldEnrich) {
      enrichBudget -= 1;
    }

    searchTotal += wouldSearch ? 1 : 0;
    enrichTotal += wouldEnrich ? 1 : 0;
    plans.push({
      ...plan,
      wouldSearch,
      wouldEnrich,
      estimatedSurfeCalls: { search: wouldSearch ? 1 : 0, enrich: wouldEnrich ? 1 : 0 },
      alertId: alert.id || alert.alertId || null,
      title: alert.title || null,
    });
  }

  return {
    enrichmentEnabled: isContactEnrichmentEnabled(),
    limits,
    summary: {
      considered: alerts.length,
      eligible,
      skipped,
      cachedContactsReused: cached,
      estimatedSurfeCalls: { search: searchTotal, enrich: enrichTotal },
      schemaImpact: "NONE - local JSON cache only (data/market-alerts/contact-enrichment/). No Airtable mutation.",
    },
    plans,
  };
}

/**
 * Execute enrichment for one alert. Safe when disabled (returns plan only).
 * Never throws — returns { ok, error?, contacts? }.
 */
export async function enrichAlertContacts(alert = {}, opts = {}) {
  try {
    const plan = planContactEnrichment(alert);
    const limits = getContactEnrichmentLimits();
    const alertId = alert.id || alert.alertId || null;

    if (!plan.eligible) {
      return { ok: true, skipped: true, reason: plan.skipReason, plan, contacts: [] };
    }

    // Cache-first named person
    if (plan.namedCandidate?.personName && plan.targetCompany) {
      const hit = readPersonContactCache({
        personName: plan.namedCandidate.personName,
        companyName: plan.targetCompany,
      });
      if (hit) {
        const record = createContactRecord({
          ...hit,
          alertId,
          entityKey: alert.entityKey || null,
          stakeholderRole: plan.targetRoles[0] || null,
          reasonSelected: hit.reasonSelected || "Cached verified contact",
          sourceArticle: alert.sourceUrl || alert.title || null,
          enrichmentStatus: "cached",
        });
        if (alertId) {
          writeAlertContactCache(alertId, { contacts: [record], status: "cached" });
        }
        return { ok: true, cached: true, plan, contacts: [record], cards: toContactCards([record]) };
      }
    }

    if (!isContactEnrichmentEnabled()) {
      return {
        ok: true,
        skipped: true,
        reason: CONTACT_BLOCK_REASONS.DISABLED,
        plan,
        contacts: [],
        cards: [],
      };
    }

    // Dry-run provider flag still blocks paid enrich unless explicitly set.
    if (!limits.dryRunProvider && opts.forceProvider !== true && opts.dryRun === true) {
      return { ok: true, skipped: true, reason: "DRY_RUN_NO_PROVIDER", plan, contacts: [], cards: [] };
    }

    const provider =
      opts.provider ||
      (getSurfeApiKeyFromEnvSafe() ? createSurfeContactProvider({ pollOnEnrich: opts.pollOnEnrich === true }) : createNoopContactProvider());

    if (provider.id === "noop") {
      return { ok: true, skipped: true, reason: "MISSING_SURFE_API_KEY", plan, contacts: [], cards: [] };
    }

    const titles = (plan.surfeTitles || []).filter((t) => !isBlockedGenericRole(t));
    const searchRes = await provider.findPeople({
      companyName: plan.targetCompany,
      roles: titles,
      limit: limits.maxCandidatesPerAlert,
      namedPerson: plan.namedCandidate
        ? {
            fullName: plan.namedCandidate.personName,
          }
        : undefined,
    });

    if (!searchRes.ok) {
      logContactOp("enrich_search_failed", { error: searchRes.error, alertId });
      return { ok: true, providerError: searchRes.error, plan, contacts: [], cards: [] };
    }

    const candidates = (searchRes.people || []).slice(0, limits.maxCandidatesPerAlert);
    if (!candidates.length) {
      return { ok: true, empty: true, plan, contacts: [], cards: [] };
    }

    const enrichRes = await provider.enrichPeople({
      people: candidates,
      includeEmail: true,
      includeMobile: limits.includeMobile,
      includeLinkedIn: true,
    });

    if (!enrichRes.ok) {
      logContactOp("enrich_failed", { error: enrichRes.error, alertId });
      return { ok: true, providerError: enrichRes.error, plan, contacts: [], cards: [] };
    }

    if (enrichRes.pending && enrichRes.enrichmentId) {
      writePendingEnrichment(enrichRes.enrichmentId, {
        alertId,
        status: "PENDING",
        candidateCount: candidates.length,
        company: plan.targetCompany,
      });
      if (alertId) {
        writeAlertContactCache(alertId, {
          status: "pending",
          enrichmentId: enrichRes.enrichmentId,
          contacts: [],
        });
      }
      return {
        ok: true,
        pending: true,
        enrichmentId: enrichRes.enrichmentId,
        plan,
        contacts: [],
        cards: [],
      };
    }

    const enriched = enrichRes.people?.length ? enrichRes.people : candidates;
    const contacts = enriched.map((p, idx) => {
      const namedInArticle =
        plan.namedCandidate &&
        p.personName &&
        String(p.personName).toLowerCase() === String(plan.namedCandidate.personName).toLowerCase();
      const hasVerifiedWorkEmail =
        Boolean(p.email) && /VALID|work|business|provider_returned/i.test(String(p.emailStatus || "VALID"));
      const confidence = classifyContactConfidence({
        namedInArticle: Boolean(namedInArticle),
        titleCompanyConfirmed: Boolean(p.jobTitle && p.companyName),
        hasVerifiedWorkEmail,
        hasBusinessContact: Boolean(p.email || p.phone || p.linkedinUrl),
        strongCompanyRoleMatch: Boolean(p.jobTitle),
        currentEmploymentConfirmed: Boolean(p.companyName),
        weakRoleMatch: false,
        ambiguousMatch: false,
      });
      const record = createContactRecord({
        ...p,
        alertId,
        entityKey: alert.entityKey || null,
        stakeholderRole: plan.targetRoles[Math.min(idx, plan.targetRoles.length - 1)] || plan.targetRoles[0] || null,
        matchConfidence: confidence,
        reasonSelected: buildReasonSelected(plan, p, namedInArticle),
        sourceArticle: alert.sourceUrl || alert.title || null,
        enrichmentStatus: "enriched",
        lastVerifiedAt: p.lastVerifiedAt || new Date().toISOString(),
      });
      writePersonContactCache(record);
      return record;
    });

    if (alertId) {
      writeAlertContactCache(alertId, { status: "ready", contacts });
    }

    return {
      ok: true,
      plan,
      contacts,
      cards: toContactCards(contacts),
    };
  } catch (err) {
    logContactOp("enrich_throw_swallowed", { message: String(err?.message || err).slice(0, 120) });
    return { ok: true, error: "INTERNAL_SWALLOWED", contacts: [], cards: [] };
  }
}

function buildReasonSelected(plan, person, namedInArticle) {
  if (namedInArticle) {
    return `Named stakeholder in article — ${person.jobTitle || plan.targetRoles[0] || "decision maker"}`;
  }
  const role = plan.targetRoles[0] || "Commercial decision maker";
  return `${role} for ${plan.targetCompany || "this project"}`;
}

function getSurfeApiKeyFromEnvSafe() {
  try {
    return Boolean(process.env.SURFE_API_KEY && String(process.env.SURFE_API_KEY).trim());
  } catch {
    return false;
  }
}

/**
 * Safe hook for RSS sync — never fails ingestion.
 */
export async function maybeEnrichAfterIngest(alert, opts = {}) {
  // On-demand Surfe only — ingest never calls Surfe. Identify + persist stakeholders (zero provider).
  try {
    const { identifyAlertStakeholders } = await import("./identify-stakeholders.js");
    const { persistIdentificationResult } = await import("./stakeholder-airtable.js");
    const identification = identifyAlertStakeholders(alert);
    if (!identification.moduleVisible || !identification.stakeholders.length) {
      return { skipped: true, reason: identification.skipReason || "NOT_APPLICABLE", surfeCalls: 0 };
    }
    if (opts.dryRun === true) {
      return { ok: true, dryRun: true, identification, surfeCalls: 0 };
    }
    const persisted = await persistIdentificationResult(identification, { dryRun: false });
    return { ok: persisted.ok, identification, surfeCalls: 0, persisted };
  } catch (err) {
    logContactOp("ingest_hook_swallowed", { message: String(err?.message || err).slice(0, 80) });
    return { skipped: true, reason: "ERROR_SWALLOWED", surfeCalls: 0 };
  }
}

export function getCachedAlertContacts(alertId, { includeLow = false } = {}) {
  const cached = readAlertContactCache(alertId);
  if (!cached?.contacts?.length) {
    return { status: cached?.status || "empty", contacts: [], cards: [], enrichmentId: cached?.enrichmentId || null };
  }
  return {
    status: cached.status || "ready",
    contacts: cached.contacts,
    cards: toContactCards(cached.contacts, { includeLow }),
    enrichmentId: cached.enrichmentId || null,
  };
}
