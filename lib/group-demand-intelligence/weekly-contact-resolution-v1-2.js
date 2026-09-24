/**
 * Weekly Contact Resolution V1.2 — WHO-first, bounded official-page research.
 * No Surfe/PDL persistence. Does not block opportunity promotion.
 */

import { extractContactsFromHtmlV5 } from "./contact-candidate/native-who-v3/html-contact-extract.js";
import {
  discoverContactCandidates,
  applyWhoDiscoveryToOpportunity,
} from "./contact-candidate/index.js";
import { isLikelyPersonName, hasNamedPerson, enrichContactRecord, gradeContact } from "./contact-resolution.js";
import {
  CONTACT_TIER,
  classifyContactTier,
  stripSurfeProviderPii,
  CONTACT_BLANK_REASON,
  isUsableContactPath,
} from "./contact-tiers-v1-2.js";
import {
  extractContactIntelligenceFromSource,
  applySourceContactExtractionToOpportunity,
} from "./extract-contact-intelligence-from-source.js";
import { recoverOfficialContactSources } from "./contact-source-recovery-v1-1.js";
import { shouldSkipDeepContactResearch } from "./contact-gap-classify-v1-2.js";

async function fetchHtml(url) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, error: "no_url", fetchesUsed: 0 };
  }
  try {
    const r = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(20000),
      headers: { "User-Agent": "DealalityGDI-ContactResolution/1.2" },
    });
    const html = await r.text();
    return {
      ok: r.status >= 200 && r.status < 400,
      status: r.status,
      finalUrl: r.url,
      html,
      fetchesUsed: 1,
    };
  } catch (err) {
    return { ok: false, error: err.message || String(err), fetchesUsed: 1 };
  }
}

function pickOfficialUrls(opportunity = {}) {
  const urls = [];
  for (const u of [
    opportunity.officialSource,
    opportunity.discoverySource,
    opportunity.lodgingEvidenceUrl,
    opportunity.contactOfficialUrl,
    opportunity.primaryContact?.sourceUrl,
    ...(Array.isArray(opportunity.sources)
      ? opportunity.sources.map((s) => s.url || s)
      : []),
  ]) {
    const s = String(u || "").trim();
    if (/^https?:\/\//i.test(s) && !urls.includes(s)) urls.push(s);
  }
  return urls.slice(0, 4);
}

function toDiscoveryCandidate(person, url) {
  const name = String(person.name || "").trim();
  const email = person.email || null;
  const role = person.role || person.title || null;
  if (/^(FUNCTIONAL_BACKUP|PRIMARY_CONTACT|UNRESOLVED|UNKNOWN|N\/A)$/i.test(name)) {
    return null;
  }
  const named = isLikelyPersonName(name) && hasNamedPerson({ name });
  return {
    name: named ? name : name || (email ? String(email).split("@")[0] : "Organization contact"),
    role,
    title: role,
    organization: person.organization || null,
    email,
    phone: person.phone || null,
    sourceUrl: url,
    source: url,
    functionalEntity: !named,
    whyThisPerson: named
      ? "Extracted from official event/organization page"
      : "Functional / organization contact on official page",
    gdiContactRole: named ? "MEETINGS_OWNER" : "ORGANIZATION_CONTACT",
    relevance: named ? "PRIMARY_DECISION_MAKER" : "OPERATIONAL_CONTACT",
  };
}

function functionalFromPage(functional = [], url) {
  return (functional || [])
    .filter((f) => f && (f.email || f.phone))
    .slice(0, 3)
    .map((f) => ({
      name: f.role || f.email || "Events / meetings desk",
      role: f.role || "Functional contact",
      email: f.email || null,
      phone: f.phone || null,
      sourceUrl: url,
      source: url,
      functionalEntity: true,
      whyThisPerson: "Functional contact published on official page",
      gdiContactRole: "ORGANIZATION_CONTACT",
      relevance: "OPERATIONAL_CONTACT",
    }));
}

/**
 * Resolve WHO for one opportunity using official pages + offline candidate ranker.
 * Never calls Surfe. Strips any accidental provider PII before return.
 */
export async function resolveOpportunityContact(opportunity = {}, opts = {}) {
  const beforeTier = classifyContactTier(opportunity);
  const skip = shouldSkipDeepContactResearch(opportunity);
  if (skip.skip) {
    return {
      opportunity: {
        ...opportunity,
        contactTier: beforeTier,
        lastContactResearchAt: new Date().toISOString(),
        contactResearchStatus: "SKIPPED",
        unresolvedContactReason: skip.reason,
        nextBestContactPath: "NONE_SKIPPED",
        contactResearchAudit: {
          pass: "gdi_weekly_contact_resolution_v1_2",
          beforeTier,
          afterTier: beforeTier,
          blankReason: skip.reason,
          skipped: true,
          surfeUsed: false,
        },
      },
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      namedAdded: false,
      functionalAdded: false,
      blankReason: skip.reason,
      fetchesUsed: 0,
      queriesUsed: 0,
      surfeUsed: false,
      skipped: true,
    };
  }

  let urls = pickOfficialUrls(opportunity);
  let fetchesUsed = 0;
  let queriesUsed = 0;
  const pageCandidates = [];
  const sourceUrls = [];
  let blankReason = null;
  let working = opportunity;

  // When no official URLs on record, run bounded source-path recovery first
  if (!urls.length && opts.allowSourceRecovery !== false) {
    const recovery = await recoverOfficialContactSources(opportunity, {
      allowNetworkDomainResolution: opts.allowNetworkDomainResolution !== false,
      budget: opts.recoveryBudget,
      fetchPage: opts.fetchPage,
    });
    fetchesUsed += recovery.metrics?.additionalFetches || 0;
    queriesUsed += recovery.metrics?.domainQueries || 0;
    working = recovery.opportunity || opportunity;
    urls = pickOfficialUrls(working);
    const recoveredTier = classifyContactTier(working);
    if (
      recoveredTier !== CONTACT_TIER.NO_CONTACT &&
      recoveredTier !== CONTACT_TIER.GENERIC_ONLY
    ) {
      // Recovery already produced a usable path — return without re-fetching
      return {
        opportunity: {
          ...working,
          contactTier: recoveredTier,
          lastContactResearchAt: new Date().toISOString(),
          contactResearchStatus:
            recoveredTier === beforeTier ? "UNCHANGED" : "IMPROVED",
          contactResearchAudit: {
            pass: "gdi_weekly_contact_resolution_v1_2",
            beforeTier,
            afterTier: recoveredTier,
            fetchesUsed,
            queriesUsed,
            blankReason: null,
            surfeUsed: false,
            sourceRecovery: true,
            recoveryResult: recovery.result,
            runId: opts.runId || null,
          },
        },
        beforeTier,
        afterTier: recoveredTier,
        improved: beforeTier !== recoveredTier && isUsableContactPath(recoveredTier),
        namedAdded:
          recoveredTier === CONTACT_TIER.NAMED_DIRECT ||
          recoveredTier === CONTACT_TIER.NAMED_PARTIAL,
        functionalAdded: recoveredTier === CONTACT_TIER.FUNCTIONAL_CONTACT,
        blankReason: null,
        fetchesUsed,
        queriesUsed,
        surfeUsed: false,
        sourceRecovery: true,
      };
    }
  }

  for (const url of urls) {
    const page = await fetchHtml(url);
    fetchesUsed += page.fetchesUsed || 0;
    if (!page.ok) {
      blankReason = blankReason || CONTACT_BLANK_REASON.EVENT_PAGE_BLOCKED;
      continue;
    }
    sourceUrls.push(page.finalUrl || url);
    const extraction = extractContactIntelligenceFromSource({
      source: { url: page.finalUrl || url, sourceType: "official_web" },
      pageContent: page.html || "",
      opportunityContext: working,
    });
    for (const p of extraction.people || []) {
      pageCandidates.push(
        toDiscoveryCandidate(
          { name: p.personName, role: p.role, email: p.email, phone: p.phone, organization: p.organization },
          page.finalUrl || url
        )
      );
    }
    for (const f of extraction.functionalContacts || []) {
      pageCandidates.push({
        name: f.label,
        role: f.label,
        email: f.functionType === "GENERIC_INBOX" ? null : f.email,
        phone: f.phone,
        sourceUrl: page.finalUrl || url,
        source: page.finalUrl || url,
        functionalEntity: true,
        whyThisPerson: `Official ${f.label} on source page`,
        gdiContactRole: "ORGANIZATION_CONTACT",
        relevance: "OPERATIONAL_CONTACT",
      });
    }
    // Bounded follow-up: up to 2 staff/contact links when still thin
    const followups = (extraction.followupLinks || []).slice(0, 2);
    for (const link of followups) {
      if (pageCandidates.filter(Boolean).some((c) => hasNamedPerson(c))) break;
      const sub = await fetchHtml(link.url);
      fetchesUsed += sub.fetchesUsed || 0;
      if (!sub.ok) continue;
      const subEx = extractContactIntelligenceFromSource({
        source: { url: link.url, sourceType: "official_web" },
        pageContent: sub.html || "",
        opportunityContext: working,
      });
      for (const p of subEx.people || []) {
        const cand = toDiscoveryCandidate(
          { name: p.personName, role: p.role, email: p.email, phone: p.phone },
          link.url
        );
        if (cand) pageCandidates.push(cand);
      }
    }
  }

  // Filter nulls from toDiscoveryCandidate
  const cleanedPageCandidates = pageCandidates.filter(Boolean);

  // Offline ranker over page + any seeded evidence already on the opportunity
  const discovery = discoverContactCandidates(working, {
    extraCandidates: cleanedPageCandidates,
  });
  queriesUsed += Array.isArray(discovery.searchQueries)
    ? discovery.searchQueries.length
    : 0;

  // Prefer page-extracted named people when ranker unresolved
  if (
    (!discovery.primaryCandidate || discovery.primaryKind === "UNRESOLVED") &&
    cleanedPageCandidates.length
  ) {
    const named = cleanedPageCandidates.find((c) => hasNamedPerson(c));
    const functional = cleanedPageCandidates.find((c) => c.functionalEntity);
    discovery.primaryKind = named ? "NAMED_PERSON" : functional ? "FUNCTIONAL_ENTITY" : "UNRESOLVED";
    discovery.primaryCandidate = named || functional || cleanedPageCandidates[0];
    discovery.backupCandidates = cleanedPageCandidates
      .filter((c) => c !== discovery.primaryCandidate)
      .slice(0, 3);
  }

  const applied = applyWhoDiscoveryToOpportunity(working, discovery);
  let next = applied.opportunity || working;

  // Ensure public contact fields + strip Surfe PII always
  if (next.primaryContact) {
    next.primaryContact = stripSurfeProviderPii(
      enrichContactRecord(next.primaryContact, next),
      { surfeUsed: false }
    );
    const graded = gradeContact(next.primaryContact, next);
    next.contactGrade = graded.contactGrade;
    next.contactGradeLabel = graded.contactGradeLabel;
    next.contactQuality = next.primaryContact.contactQuality || next.contactQuality;
    next.primaryContactName = next.primaryContact.name || null;
    next.primaryContactRole = next.primaryContact.role || null;
    // Never persist provider HOW — clear email/phone if tagged surfe
    if (next.primaryContact.surfeEnriched) {
      next.primaryContactEmail = null;
      next.primaryContactPhone = null;
    } else {
      // Public-source reachability is allowed to persist
      next.primaryContactEmail = next.primaryContact.email || null;
      next.primaryContactPhone = next.primaryContact.phone || null;
    }
  }

  const afterTier = classifyContactTier(next);
  if (afterTier === CONTACT_TIER.NO_CONTACT) {
    blankReason =
      blankReason ||
      (urls.length === 0
        ? CONTACT_BLANK_REASON.SOURCE_TOO_THIN
        : cleanedPageCandidates.length === 0
          ? CONTACT_BLANK_REASON.NO_PUBLIC_STAFF
          : CONTACT_BLANK_REASON.ROLE_AMBIGUOUS);
  } else if (afterTier === CONTACT_TIER.GENERIC_ONLY) {
    blankReason = CONTACT_BLANK_REASON.ONLY_GENERIC_CONTACT;
  }

  next = {
    ...next,
    contactTier: afterTier,
    contactOfficialUrl: sourceUrls[0] || next.contactOfficialUrl || urls[0] || null,
    contactSourceUrl: sourceUrls[0] || next.contactSourceUrl || null,
    contactSourceType: "official_web",
    contactVerifiedAt: new Date().toISOString(),
    contactResearchAudit: {
      pass: "gdi_weekly_contact_resolution_v1_2",
      beforeTier,
      afterTier,
      fetchesUsed,
      queriesUsed,
      pageCandidateCount: cleanedPageCandidates.length,
      blankReason: isUsableContactPath(afterTier) ? null : blankReason,
      surfeUsed: false,
      runId: opts.runId || null,
      targetId: opts.targetId || null,
      targetRunId: opts.targetRunId || null,
      opportunityId: next.id,
    },
  };

  // Contact improvement is UPDATED, never NEW
  if (
    beforeTier !== afterTier &&
    next.weeklyDeltaState === "NEW" &&
    opts.preserveNewWeeklyState !== true
  ) {
    // leave NEW if this is same promotion cycle; caller controls
  } else if (beforeTier !== afterTier && opts.markUpdated === true) {
    next.weeklyDeltaState = "UPDATED";
    next.isNewThisWeek = false;
  }

  return {
    opportunity: next,
    beforeTier,
    afterTier,
    improved: beforeTier !== afterTier && isUsableContactPath(afterTier),
    namedAdded: afterTier === CONTACT_TIER.NAMED_DIRECT || afterTier === CONTACT_TIER.NAMED_PARTIAL,
    functionalAdded: afterTier === CONTACT_TIER.FUNCTIONAL_CONTACT,
    blankReason: next.contactResearchAudit.blankReason,
    fetchesUsed,
    queriesUsed,
    surfeUsed: false,
  };
}

/**
 * Batch contact resolution for actionable opportunities with weak/blank contacts.
 */
export async function resolveContactsForOpportunities(opportunities = [], opts = {}) {
  const limit = Math.max(0, Number(opts.limit) || 20);
  const prioritize = (o) => {
    const t = classifyContactTier(o);
    if (t === CONTACT_TIER.NO_CONTACT) return 0;
    if (t === CONTACT_TIER.GENERIC_ONLY) return 1;
    if (t === CONTACT_TIER.ORGANIZATION_PATH && !o.primaryContactName) return 2;
    if (t === CONTACT_TIER.NAMED_PARTIAL) return 3;
    return 9;
  };
  const sorted = [...opportunities].sort((a, b) => prioritize(a) - prioritize(b));
  const selected = sorted.filter((o) => prioritize(o) <= 3).slice(0, limit);

  const results = [];
  for (const o of selected) {
    results.push(await resolveOpportunityContact(o, opts));
  }
  return {
    researched: results.length,
    results,
    namedAdded: results.filter((r) => r.namedAdded && r.beforeTier !== r.afterTier).length,
    functionalAdded: results.filter((r) => r.functionalAdded && r.beforeTier !== r.afterTier).length,
    improved: results.filter((r) => r.improved).length,
    stillBlank: results.filter((r) => r.afterTier === CONTACT_TIER.NO_CONTACT).length,
    fetchesUsed: results.reduce((s, r) => s + (r.fetchesUsed || 0), 0),
    queriesUsed: results.reduce((s, r) => s + (r.queriesUsed || 0), 0),
    surfeCalls: 0,
  };
}
