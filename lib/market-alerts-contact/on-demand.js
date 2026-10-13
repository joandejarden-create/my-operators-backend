/**
 * On-demand Surfe actions for Market Alerts stakeholders.
 *
 * FIND_PERSON  — people search; persist normalized name/title/company only
 * GET_CONTACT_DETAILS — enrich; return ephemeral details; persist NOTHING from Surfe
 */

import {
  assessContactEnrichmentEligibility,
  isContactEnrichmentEnabled,
  getContactEnrichmentLimits,
} from "./eligibility.js";
import { stakeholderRoleLabels, isBlockedGenericRole } from "./stakeholder-roles.js";
import { rankContactCandidates } from "./rank-candidates.js";
import { createSurfeContactProvider } from "./surfe-adapter.js";
import { createNoopContactProvider } from "./provider-interface.js";
import { withInflightGuard } from "./inflight-guard.js";
import {
  findStakeholderByStableId,
  upsertStakeholder,
  updateStakeholderLookupStatus,
  listStakeholdersForAlert,
} from "./stakeholder-airtable.js";
import {
  IDENTIFICATION_STATUS,
  CONTACT_LOOKUP_STATUS,
  STAKEHOLDER_SOURCE_TYPE,
  buildStakeholderStableKey,
  stripSurfeContactDetails,
} from "./stakeholder-schema.js";
import { logContactOp } from "./safe-log.js";

function hasSurfeKey() {
  return Boolean(process.env.SURFE_API_KEY && String(process.env.SURFE_API_KEY).trim());
}

function getProvider(opts = {}) {
  if (!hasSurfeKey()) return createNoopContactProvider();
  return createSurfeContactProvider({
    pollOnEnrich: opts.pollOnEnrich === true,
    maxPollWaitMs: Number(opts.maxPollWaitMs || process.env.SURFE_ENRICH_POLL_MAX_MS || 90000),
  });
}

/**
 * User-triggered: Find decision maker (Surfe people search).
 * Persists only normalized Dealality identity fields.
 */
export async function findDecisionMakerOnDemand(alert, { stakeholderId } = {}) {
  const alertId = alert?.id || alert?.alertId;
  return withInflightGuard(alertId, stakeholderId || "primary", "FIND_PERSON", async () => {
    if (!isContactEnrichmentEnabled()) {
      return { ok: false, error: "CONTACT_ENRICHMENT_DISABLED", surfeCalls: 0 };
    }

    const assessment = assessContactEnrichmentEligibility(alert);
    if (!assessment.eligible) {
      return { ok: false, error: assessment.reason, surfeCalls: 0 };
    }

    const company =
      assessment.organizations[0] ||
      assessment.namedStakeholder?.companyName ||
      null;
    if (!company) {
      return { ok: false, error: "NO_TARGET_COMPANY", surfeCalls: 0 };
    }

    if (stakeholderId) {
      await updateStakeholderLookupStatus(stakeholderId, CONTACT_LOOKUP_STATUS.PENDING);
    }

    const provider = getProvider({ pollOnEnrich: false });
    if (provider.id === "noop") {
      if (stakeholderId) {
        await updateStakeholderLookupStatus(stakeholderId, CONTACT_LOOKUP_STATUS.FAILED_RETRYABLE);
      }
      return { ok: false, error: "MISSING_SURFE_API_KEY", surfeCalls: 0 };
    }

    const roleLabels = stakeholderRoleLabels(assessment.roles || []);
    const titles = (assessment.roles || [])
      .flatMap((r) => r.surfeTitles || [])
      .filter((t) => !isBlockedGenericRole(t))
      .slice(0, 8);

    let searchRes;
    try {
      searchRes = await provider.findPeople({
        companyName: company,
        roles: titles,
        limit: getContactEnrichmentLimits().maxCandidatesPerAlert,
      });
    } catch (err) {
      logContactOp("find_person_throw", { message: String(err?.message || err).slice(0, 80) });
      if (stakeholderId) {
        await updateStakeholderLookupStatus(stakeholderId, CONTACT_LOOKUP_STATUS.FAILED_RETRYABLE);
      }
      return { ok: false, error: "PROVIDER_ERROR", surfeCalls: 1 };
    }

    if (!searchRes.ok) {
      if (stakeholderId) {
        await updateStakeholderLookupStatus(stakeholderId, CONTACT_LOOKUP_STATUS.FAILED_RETRYABLE);
      }
      return {
        ok: false,
        error: searchRes.error || "SEARCH_FAILED",
        surfeCalls: 1,
      };
    }

    const raw = (searchRes.people || []).slice(0, 5);
    // Strip any accidental contact fields from search results before ranking/persist
    const cleaned = raw.map((p) =>
      stripSurfeContactDetails({
        personName: p.personName || p.fullName || null,
        jobTitle: p.jobTitle || null,
        companyName: p.companyName || company,
      })
    );

    const ranked = rankContactCandidates(cleaned, {
      targetCompany: company,
      targetRoles: roleLabels,
      surfeTitles: titles,
    });

    const selected = ranked.selected;
    if (!selected?.personName) {
      if (stakeholderId) {
        await updateStakeholderLookupStatus(stakeholderId, CONTACT_LOOKUP_STATUS.MISS);
      }
      return {
        ok: true,
        miss: true,
        identificationStatus: IDENTIFICATION_STATUS.COMPANY_ONLY,
        stakeholder: null,
        surfeCalls: 1,
        message: "No verified decision maker found",
      };
    }

    const now = new Date().toISOString();
    const newId = buildStakeholderStableKey({
      alertId,
      personName: selected.personName,
      company,
      stakeholderClass: "DEVELOPMENT_EXECUTIVE",
      jobTitle: selected.jobTitle,
    });

    const row = {
      stakeholderId: newId,
      alertId,
      personName: selected.personName,
      jobTitle: selected.jobTitle || roleLabels[0] || null,
      company,
      stakeholderClass: "DEVELOPMENT_EXECUTIVE",
      relationshipToProject: "Identified via on-demand people search",
      whyRelevant: `Decision-maker candidate for ${company}`,
      articleDerived: false,
      quoted: false,
      confidence: "MEDIUM",
      identificationStatus: IDENTIFICATION_STATUS.IDENTIFIED,
      selectedPrimary: true,
      selectedSecondary: false,
      decisionOpen: true,
      sourceType: STAKEHOLDER_SOURCE_TYPE.USER_FIND_PERSON,
      identifiedAt: now,
      updatedAt: now,
      contactLookupStatus: CONTACT_LOOKUP_STATUS.NOT_REQUESTED,
      sourceArticleUrl: alert.sourceUrl || null,
      sourceArticleTitle: alert.title || null,
    };

    // HARD: ensure no Surfe PII on write
    const persist = await upsertStakeholder(stripSurfeContactDetails(row));
    if (stakeholderId && stakeholderId !== newId) {
      await updateStakeholderLookupStatus(stakeholderId, CONTACT_LOOKUP_STATUS.NOT_REQUESTED);
    }

    logContactOp("find_person_complete", {
      alertId,
      persistedIdentity: true,
      surfePiiPersisted: false,
    });

    return {
      ok: true,
      surfeCalls: 1,
      identificationStatus: IDENTIFICATION_STATUS.IDENTIFIED,
      stakeholder: persist.row || row,
      persistOk: persist.ok === true,
      // Never return email/phone/linkedin from search
      contactDetails: null,
    };
  });
}

/**
 * User-triggered: Get contact details (Surfe enrich).
 * Returns ephemeral contact details — persists NOTHING from provider result.
 */
export async function revealContactDetailsOnDemand(alert, { stakeholderId } = {}) {
  const alertId = alert?.id || alert?.alertId;
  return withInflightGuard(alertId, stakeholderId || "reveal", "GET_CONTACT_DETAILS", async () => {
    if (!isContactEnrichmentEnabled()) {
      return { ok: false, error: "CONTACT_ENRICHMENT_DISABLED", surfeCalls: 0 };
    }

    let stakeholder = stakeholderId ? await findStakeholderByStableId(stakeholderId) : null;
    if (!stakeholder && alertId) {
      const list = await listStakeholdersForAlert(alertId);
      stakeholder = list.find((s) => s.selectedPrimary && s.personName) || list.find((s) => s.personName);
    }

    if (!stakeholder?.personName || !stakeholder?.company) {
      return { ok: false, error: "STAKEHOLDER_PERSON_REQUIRED", surfeCalls: 0 };
    }

    await updateStakeholderLookupStatus(
      stakeholder.stakeholderId,
      CONTACT_LOOKUP_STATUS.PENDING
    );

    const limits = getContactEnrichmentLimits();
    const provider = getProvider({ pollOnEnrich: true });
    if (provider.id === "noop") {
      await updateStakeholderLookupStatus(
        stakeholder.stakeholderId,
        CONTACT_LOOKUP_STATUS.FAILED_RETRYABLE
      );
      return { ok: false, error: "MISSING_SURFE_API_KEY", surfeCalls: 0 };
    }

    let enrichRes;
    try {
      enrichRes = await provider.enrichPeople({
        people: [
          {
            personName: stakeholder.personName,
            jobTitle: stakeholder.jobTitle || null,
            companyName: stakeholder.company,
            // Prefer Dealality-resolved LinkedIn as Surfe input when available
            linkedinUrl:
              stakeholder.linkedinResolutionSource === "DEALALITY_PUBLIC_RESEARCH"
                ? stakeholder.linkedinUrl || null
                : null,
          },
        ],
        includeEmail: true,
        includeMobile: limits.includeMobile === true,
        // Surfe LinkedIn is ephemeral UI-only; Dealality LinkedIn is the identity source
        includeLinkedIn: true,
      });
    } catch (err) {
      logContactOp("reveal_throw", { message: String(err?.message || err).slice(0, 80) });
      await updateStakeholderLookupStatus(
        stakeholder.stakeholderId,
        CONTACT_LOOKUP_STATUS.FAILED_RETRYABLE
      );
      return { ok: false, error: "PROVIDER_ERROR", surfeCalls: 1 };
    }

    if (!enrichRes.ok) {
      await updateStakeholderLookupStatus(
        stakeholder.stakeholderId,
        CONTACT_LOOKUP_STATUS.FAILED_RETRYABLE
      );
      return { ok: false, error: enrichRes.error || "ENRICH_FAILED", surfeCalls: 1 };
    }

    if (enrichRes.pending && !enrichRes.people?.length) {
      // Keep PENDING workflow status only — no PII
      return {
        ok: true,
        pending: true,
        surfeCalls: 1,
        contactDetails: null,
        message: "Finding…",
      };
    }

    const person = (enrichRes.people || [])[0] || null;
    const email = person?.email || null;
    const phone = person?.phone || null;
    const dealalityLinkedIn =
      stakeholder.linkedinResolutionSource === "DEALALITY_PUBLIC_RESEARCH" &&
      stakeholder.linkedinUrl
        ? stakeholder.linkedinUrl
        : null;
    // Prefer Dealality LinkedIn; Surfe LinkedIn remains request-scoped only (never persisted)
    const linkedinUrl = dealalityLinkedIn || person?.linkedinUrl || null;
    const hasAny = Boolean(email || phone || linkedinUrl);

    await updateStakeholderLookupStatus(
      stakeholder.stakeholderId,
      hasAny ? CONTACT_LOOKUP_STATUS.NOT_REQUESTED : CONTACT_LOOKUP_STATUS.MISS
    );

    // HARD RULE: do not write email/phone/linkedin/provider IDs anywhere.
    // Only workflow status was updated above.

    logContactOp("reveal_complete", {
      alertId,
      hasEmail: Boolean(email),
      hasPhone: Boolean(phone),
      hasLinkedin: Boolean(linkedinUrl),
      surfePiiPersisted: false,
    });

    return {
      ok: true,
      surfeCalls: 1,
      ephemeral: true,
      persisted: false,
      contactDetails: hasAny
        ? {
            personName: stakeholder.personName,
            jobTitle: stakeholder.jobTitle || null,
            companyName: stakeholder.company,
            email: email || null,
            emailStatus: person?.emailStatus || null,
            phone: phone || null,
            phoneStatus: person?.phoneStatus || null,
            linkedinUrl: linkedinUrl || null,
            verificationState: hasAny ? "provider_returned" : "miss",
          }
        : null,
      miss: !hasAny,
      message: hasAny ? null : "No verified contact details found",
    };
  });
}
