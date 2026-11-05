/**
 * AVA Digital Authority, Backlink & AI Citation Analysis V1
 *
 * Public-research mode by default. Paid backlink providers (Ahrefs/Semrush/etc.)
 * are NOT used unless founder-authorized separately.
 *
 * Classification discipline:
 * - OBSERVED_AI_CITATION — evidenced in ADP/AI response data
 * - VERIFIED_BACKLINK — confirmed hyperlink to the hotel website
 * - VERIFIED_EXTERNAL_MENTION — confirmed third-party reference (may not be a hyperlink)
 * - VERIFIED_COMPETITOR_BACKLINK — confirmed hyperlink to a competitor hotel website
 * - CONFIRMED_SUBJECT_ABSENCE — subject hotel missing after sufficient inventory review
 * - SUBJECT_COVERAGE_NOT_ESTABLISHED — absence cannot be confirmed
 * - POTENTIAL_BACKLINK_OPPORTUNITY — credible source where a link/listing could help
 * - POTENTIAL_CITATION_OPPORTUNITY — credible source where inclusion could help AI/discovery
 *
 * Opportunity scores are prioritization aids, NOT measured SEO performance
 * or predicted AI ranking improvements.
 */
import {
  resolveAvaResearchAuthorization,
  applyResearchAuthorizationToDigitalAuthority,
} from "./research-authorization-v1.js";

export const DIGITAL_AUTHORITY_CONTRACT = "AVA_DIGITAL_AUTHORITY_V1";

export const DIGITAL_AUTHORITY_FINDING_CLASSES = Object.freeze({
  OBSERVED_AI_CITATION: "OBSERVED_AI_CITATION",
  VERIFIED_BACKLINK: "VERIFIED_BACKLINK",
  VERIFIED_EXTERNAL_MENTION: "VERIFIED_EXTERNAL_MENTION",
  VERIFIED_COMPETITOR_BACKLINK: "VERIFIED_COMPETITOR_BACKLINK",
  CONFIRMED_SUBJECT_ABSENCE: "CONFIRMED_SUBJECT_ABSENCE",
  SUBJECT_COVERAGE_NOT_ESTABLISHED: "SUBJECT_COVERAGE_NOT_ESTABLISHED",
  POTENTIAL_BACKLINK_OPPORTUNITY: "POTENTIAL_BACKLINK_OPPORTUNITY",
  POTENTIAL_CITATION_OPPORTUNITY: "POTENTIAL_CITATION_OPPORTUNITY",
});

export const DIGITAL_AUTHORITY_SCORE_WEIGHTS = Object.freeze({
  demandRelevance: 0.3,
  sourceCredibility: 0.25,
  competitiveDifferentiation: 0.2,
  implementationLikelihood: 0.15,
  discoveryValue: 0.1,
});

/**
 * @param {{ demandRelevance:number, sourceCredibility:number, competitiveDifferentiation:number, implementationLikelihood:number, discoveryValue:number }} factors 1–10
 */
export function scoreDigitalAuthorityOpportunity(factors = {}) {
  const w = DIGITAL_AUTHORITY_SCORE_WEIGHTS;
  const n = (v) => Math.max(0, Math.min(10, Number(v) || 0));
  const raw =
    n(factors.demandRelevance) * w.demandRelevance +
    n(factors.sourceCredibility) * w.sourceCredibility +
    n(factors.competitiveDifferentiation) * w.competitiveDifferentiation +
    n(factors.implementationLikelihood) * w.implementationLikelihood +
    n(factors.discoveryValue) * w.discoveryValue;
  return Math.round(raw * 10) / 10;
}

export function emptyDigitalAuthorityBlock({ propertyId, hotelName, website } = {}) {
  return {
    contract: DIGITAL_AUTHORITY_CONTRACT,
    moduleId: "M06_BACKLINK_AUTHORITY",
    propertyId: propertyId || null,
    hotelName: hotelName || null,
    officialWebsite: website || null,
    researchMode: "PUBLIC_PARTIAL",
    paidProviderUsed: false,
    paidResearchUsd: 0,
    inventoryCompleteness:
      "PARTIAL_PUBLIC_SEARCH — not a comprehensive backlink inventory. Ahrefs/Semrush/Majestic-style coverage was not authorized.",
    executiveFinding: "",
    competitiveComparison: "",
    sources: [],
    competitorBacklinks: [],
    competitorGaps: [],
    opportunities: [],
    actions: [],
    methodologyNotes: [
      "Public web search and page verification only unless a paid provider is separately authorized.",
      "A search result mentioning the hotel is not an observed AI citation.",
      "A mention without a hyperlink is not a verified backlink.",
      "A verified backlink does not prove future AI recommendation change.",
      "Opportunity scores prioritize commercial action; they are not SEO performance metrics.",
    ],
    limitations: [
      "No proprietary domain-authority scores invented.",
      "No complete backlink crawl without an authorized provider.",
      "Unverified absence is labeled SUBJECT_COVERAGE_NOT_ESTABLISHED, not a confirmed gap.",
    ],
    generatedAt: new Date().toISOString(),
  };
}

export function collectDigitalAuthorityEvidenceUrls(digitalAuthority) {
  const urls = [];
  const push = (u) => {
    if (u && /^https?:\/\//i.test(String(u))) urls.push(String(u));
  };
  for (const s of digitalAuthority?.sources || []) {
    push(s.linkingPageUrl);
    push(s.hotelDestinationUrl);
    push(s.evidenceUrl);
  }
  for (const b of digitalAuthority?.competitorBacklinks || []) {
    push(b.linkingPageUrl);
    push(b.competitorDestinationUrl);
    push(b.evidenceUrl);
  }
  for (const g of digitalAuthority?.competitorGaps || []) {
    push(g.competitorLinkingPageUrl);
    push(g.evidenceUrl);
  }
  for (const o of digitalAuthority?.opportunities || []) {
    push(o.targetOrganizationUrl);
    push(o.evidenceUrl);
    for (const e of o.evidenceLinks || []) push(e.url || e);
  }
  for (const a of digitalAuthority?.actions || []) {
    for (const e of a.evidenceLinks || []) push(e.url || e);
    for (const e of a.supportingEvidence || []) push(e.url || e);
  }
  return [...new Set(urls)];
}

function opportunityKey(o) {
  return String(o.opportunityId || o.title || o.organization || "")
    .trim()
    .toLowerCase();
}

function sourceKey(s) {
  return `${String(s.findingClass || "").toLowerCase()}|${String(
    s.linkingPageUrl || s.evidenceUrl || ""
  )
    .trim()
    .toLowerCase()}|${String(s.referringOrganization || s.label || "")
    .trim()
    .toLowerCase()}`;
}

function dedupeByKey(items, keyFn) {
  const seen = new Set();
  const out = [];
  for (const item of items || []) {
    const k = keyFn(item);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(item);
  }
  return out;
}

/**
 * Ensure DA actions satisfy AVA PDF action contract fields.
 */
export function normalizeDigitalAuthorityActionForPdf(action = {}) {
  const evidenceLinks = Array.isArray(action.evidenceLinks)
    ? action.evidenceLinks
    : [];
  const supportingEvidence =
    Array.isArray(action.supportingEvidence) && action.supportingEvidence.length
      ? action.supportingEvidence
      : evidenceLinks.map((e) => ({
          label: e.label || "Evidence",
          url: e.url || e,
          kind: e.kind || "VERIFIED_PUBLIC",
        }));
  const urls = supportingEvidence
    .filter((e) => e?.url)
    .map((e) => ({
      label: e.label || "Evidence",
      url: e.url,
      kind: e.kind || "VERIFIED_PUBLIC",
    }));
  const steps = Array.isArray(action.recommendedSteps)
    ? action.recommendedSteps
    : [];
  return {
    ...action,
    supportingParties:
      action.supportingParties != null && String(action.supportingParties).trim() !== ""
        ? action.supportingParties
        : "Brand content / agency",
    estimatedEffort: action.estimatedEffort || "MEDIUM",
    completionCriteria: action.completionCriteria || {
      investigationCompleted:
        "Opportunity owner and required facts confirmed with hotel leadership.",
      correctionSubmitted:
        "Outreach or listing update request filed with supporting evidence.",
      correctionVerifiedLive:
        "Live destination/partner page checked and archived after update (or documented refusal).",
      noChangeRequired:
        "Valid if leadership documents that the opportunity is out of scope with evidence retained.",
    },
    requiresHotelApproval:
      action.requiresHotelApproval === undefined
        ? true
        : action.requiresHotelApproval,
    hotelApprovalNotes:
      action.hotelApprovalNotes ||
      "Approve outreach copy, official URL, and factual claims before partner contact.",
    supportingEvidence,
    technicalDetails: action.technicalDetails || {
      urls: urls.length
        ? urls
        : [{ label: "Official site", url: action.targetUrl || null, kind: "VERIFIED_PUBLIC" }].filter(
            (u) => u.url
          ),
      changesOrInvestigation: steps.length
        ? steps
        : [
            "Confirm official URL and demand-relevant facts.",
            "Identify inventory owner.",
            "Submit approved profile if policy allows.",
            "Re-verify live listing and classification.",
          ],
      evidence: supportingEvidence,
      verificationMethod:
        action.verificationMethod ||
        "Live-page QA with archived screenshots and classification note.",
      dependencies: [
        "Hotel leadership approves facts before outreach.",
      ],
      notes: "Public partial Digital Authority research only; no paid SEO metrics invented.",
    },
    implementationStatus:
      action.implementationStatus || "AWAITING_HOTEL_DECISION",
    claimType: action.claimType || "OPTIONAL_COMMERCIAL_OPPORTUNITY",
  };
}

/**
 * Normalize a digitalAuthority payload block and attach priority scores.
 */
export function finalizeDigitalAuthorityBlock(block, authOptions = {}) {
  const auth = resolveAvaResearchAuthorization(authOptions);
  let out = {
    ...emptyDigitalAuthorityBlock(block),
    ...block,
    contract: DIGITAL_AUTHORITY_CONTRACT,
    moduleId: "M06_BACKLINK_AUTHORITY",
  };
  out = applyResearchAuthorizationToDigitalAuthority(out, auth);
  if (!auth.paidResearchAuthorized) {
    out.paidProviderUsed = false;
    out.paidResearchUsd = 0;
    out.researchMode = "PUBLIC_PARTIAL";
  }

  out.sources = dedupeByKey(out.sources || [], sourceKey);
  out.competitorBacklinks = dedupeByKey(
    out.competitorBacklinks || [],
    (b) =>
      `${String(b.linkingPageUrl || "").toLowerCase()}|${String(
        b.competitorName || ""
      ).toLowerCase()}`
  );
  out.competitorGaps = dedupeByKey(
    out.competitorGaps || [],
    (g) =>
      `${String(g.competitorName || "").toLowerCase()}|${String(
        g.gapClass || g.findingClass || g.gapSummary || ""
      ).toLowerCase()}`
  );
  out.opportunities = dedupeByKey(out.opportunities || [], opportunityKey).map(
    (o) => {
      const score =
        typeof o.priorityScore === "number"
          ? o.priorityScore
          : scoreDigitalAuthorityOpportunity(o.scoreFactors || {});
      return {
        ...o,
        priorityScore: score,
        findingClass:
          o.findingClass ||
          DIGITAL_AUTHORITY_FINDING_CLASSES.POTENTIAL_CITATION_OPPORTUNITY,
      };
    }
  );
  out.opportunities.sort(
    (a, b) => (b.priorityScore || 0) - (a.priorityScore || 0)
  );
  out.actions = dedupeByKey(out.actions || [], (a) =>
    String(a.actionId || "").toLowerCase()
  ).map(normalizeDigitalAuthorityActionForPdf);
  out.generatedAt = out.generatedAt || new Date().toISOString();
  return out;
}

/**
 * Idempotent merge of digital authority into AVA payload.
 * - Replaces digitalAuthority block
 * - Upserts DA actions by actionId (no duplicates)
 * - Dedupes actionOverview by actionId
 * - Does not silently overwrite founder-decided recommendations' decision fields
 */
export function appendDigitalAuthorityActions(payload, digitalAuthority, authOptions = {}) {
  const da = finalizeDigitalAuthorityBlock(digitalAuthority, authOptions);
  const actionsById = new Map();
  for (const a of payload.actions || []) {
    if (a?.actionId) actionsById.set(a.actionId, a);
  }

  const daActionIds = new Set();
  for (const a of da.actions || []) {
    if (!a?.actionId) continue;
    daActionIds.add(a.actionId);
    const existing = actionsById.get(a.actionId);
    if (existing) {
      const decision =
        existing.hotelDecision ||
        existing.founderDecision ||
        existing.decision ||
        null;
      if (decision && /APPROVE|APPROVED|REJECT|DEFER/i.test(String(decision))) {
        // Preserve decision; refresh evidence/copy but flag revision
        actionsById.set(a.actionId, {
          ...a,
          hotelDecision: existing.hotelDecision,
          founderDecision: existing.founderDecision,
          decision: existing.decision,
          decisionAt: existing.decisionAt,
          revisionNote:
            "Digital Authority content refreshed; prior founder/hotel decision preserved for review.",
        });
      } else {
        actionsById.set(a.actionId, { ...existing, ...a });
      }
    } else {
      actionsById.set(a.actionId, a);
    }
  }

  // Drop stale DA-prefixed actions no longer in block? Keep them to avoid surprising removals.
  const mergedActions = [...actionsById.values()];

  const overviewById = new Map();
  for (const row of payload.actionOverview || []) {
    if (row?.actionId) overviewById.set(row.actionId, row);
  }
  for (const a of da.actions || []) {
    if (!a?.actionId) continue;
    overviewById.set(a.actionId, {
      actionId: a.actionId,
      priority: a.priority,
      headline: a.headline,
      businessRelevance: a.whyItMatters || a.priorityWhy,
      accountableOwner: a.accountableOwner,
      suggestedTimeframe: a.suggestedTimeframe,
    });
  }

  return {
    ...payload,
    digitalAuthority: da,
    actions: mergedActions,
    actionOverview: [...overviewById.values()],
  };
}

/**
 * Summarize classification counts for certification reporting.
 */
export function summarizeDigitalAuthorityClassifications(digitalAuthority) {
  const counts = {};
  const bump = (cls) => {
    const k = cls || "UNKNOWN";
    counts[k] = (counts[k] || 0) + 1;
  };
  for (const s of digitalAuthority?.sources || []) bump(s.findingClass);
  for (const b of digitalAuthority?.competitorBacklinks || [])
    bump(b.findingClass || "VERIFIED_COMPETITOR_BACKLINK");
  for (const g of digitalAuthority?.competitorGaps || [])
    bump(g.gapClass || g.findingClass || "SUBJECT_COVERAGE_NOT_ESTABLISHED");
  for (const o of digitalAuthority?.opportunities || []) bump(o.findingClass);
  return counts;
}
