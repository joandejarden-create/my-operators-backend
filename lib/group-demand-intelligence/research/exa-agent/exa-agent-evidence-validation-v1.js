/**
 * Validate Exa Agent structured output before Dealality CIB / ORS ingest.
 * Reuses Stage 6 publisher/entity and Association isolation rules.
 */
import {
  isPressPublisherHost,
  isPressPublisherNonCompanyRoute,
  filterPressPublisherOrgRoute,
} from "../../press-publisher-entity-v1.js";
import { hasAssociationProcurementBoilerplate } from "../../scaling-drawer-presentation-contract-v1.js";

export const EXA_AGENT_EVIDENCE_VALIDATION_V1 =
  "gdi-scaling-exa-agent-evidence-validation-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function pushIssue(issues, severity, code, message, path = null) {
  issues.push({ severity, code, message, path });
}

export function validateDiscoveryStructured(structured = {}) {
  const issues = [];
  if (!structured || typeof structured !== "object") {
    pushIssue(issues, "P0", "missing_structured", "Discovery structured output missing");
    return { ok: false, issues, acceptedCandidates: [] };
  }
  const candidates = Array.isArray(structured.candidates) ? structured.candidates : [];
  if (!candidates.length) {
    pushIssue(issues, "P1", "no_candidates", "No discovery candidates returned");
  }
  const acceptedCandidates = [];
  for (let i = 0; i < candidates.length; i += 1) {
    const c = candidates[i];
    const path = `candidates[${i}]`;
    const name = text(c?.companyIdentity?.legalOrTradingName);
    let rejectCandidate = false;
    if (!name) {
      pushIssue(issues, "P0", "identity_missing", "Candidate missing company name", path);
      rejectCandidate = true;
    }
    if (/mynewsdesk|pr newswire|business wire/i.test(name)) {
      pushIssue(
        issues,
        "P0",
        "publisher_as_company",
        `Publisher misclassified as company: ${name}`,
        path,
      );
      rejectCandidate = true;
    }
    const site = text(c?.companyIdentity?.officialWebsite);
    if (site && isPressPublisherNonCompanyRoute(site)) {
      pushIssue(
        issues,
        "P0",
        "publisher_website",
        "Official website is a press-publisher shell",
        path,
      );
      rejectCandidate = true;
    }
    for (const claim of c?.evidenceClaims || []) {
      if (isPressPublisherNonCompanyRoute(claim.url)) {
        pushIssue(
          issues,
          "P1",
          "publisher_shell_as_claim_evidence",
          `Publisher shell used as claim evidence: ${claim.url}`,
          path,
        );
      }
    }
    if (hasAssociationProcurementBoilerplate(c?.hotelDemandHypothesis?.mechanism)) {
      pushIssue(
        issues,
        "P0",
        "association_boilerplate",
        "Association procurement language in Scaling discovery demand hypothesis",
        path,
      );
      rejectCandidate = true;
    }
    if (
      c.discoveryDisposition === "ACCEPT_FOR_DEEP_RESEARCH" &&
      c.hotelDemandHypothesis?.evidenceStatus === "unsupported"
    ) {
      pushIssue(
        issues,
        "P1",
        "accept_without_demand",
        "Accepted for deep research without credible demand mechanism",
        path,
      );
    }
    if (!rejectCandidate) acceptedCandidates.push(c);
  }
  const p0 = issues.filter((x) => x.severity === "P0");
  return {
    ok: p0.length === 0,
    issues,
    acceptedCandidates,
    version: EXA_AGENT_EVIDENCE_VALIDATION_V1,
  };
}

export function validateDeepResearchStructured(structured = {}) {
  const issues = [];
  if (!structured || typeof structured !== "object") {
    pushIssue(issues, "P0", "missing_structured", "Deep research structured output missing");
    return { ok: false, issues, sanitized: null };
  }

  const name = text(structured.companyIdentity?.legalOrTradingName);
  if (!name) {
    pushIssue(issues, "P0", "identity_missing", "companyIdentity.legalOrTradingName required");
  }
  if (/mynewsdesk/i.test(name)) {
    pushIssue(issues, "P0", "publisher_as_company", "Target company looks like a publisher");
  }

  const action = text(structured.recommendedCommercialAction?.nextAction);
  if (hasAssociationProcurementBoilerplate(action)) {
    pushIssue(
      issues,
      "P0",
      "association_boilerplate_action",
      "Association procurement language in recommendedCommercialAction",
    );
  }
  for (const q of structured.recommendedCommercialAction?.qualificationQuestions || []) {
    if (hasAssociationProcurementBoilerplate(q)) {
      pushIssue(issues, "P0", "association_boilerplate_question", String(q));
    }
  }

  const routes = [];
  for (const r of structured.organizationalContactRoutes || []) {
    const url = text(r.url);
    if (!url) continue;
    if (isPressPublisherNonCompanyRoute(url) || !filterPressPublisherOrgRoute(url)) {
      pushIssue(
        issues,
        "P0",
        "publisher_org_route",
        `Press publisher / non-company route rejected: ${url}`,
      );
      continue;
    }
    routes.push({
      ...r,
      url,
      isPressPublisher: isPressPublisherHost(url),
      isTargetCompanyRoute: r.isTargetCompanyRoute !== false,
    });
  }

  for (const b of structured.buyerCandidates || []) {
    if (b.email != null || b.phone != null) {
      pushIssue(
        issues,
        "P0",
        "invented_or_enriched_contact_channel",
        "Deep research must not supply email/phone (Surfe/on-demand only)",
      );
    }
    if (b.relevanceClass === "direct_buyer" && !text(b.fullName)) {
      pushIssue(issues, "P0", "nameless_direct_buyer", "direct_buyer requires fullName");
    }
    if (
      b.relevanceClass === "direct_buyer" &&
      b.employerVerified === false
    ) {
      pushIssue(
        issues,
        "P1",
        "unverified_employer_direct_buyer",
        "direct_buyer with employerVerified=false",
      );
    }
  }

  for (const s of structured.supportingSources || []) {
    if (isPressPublisherNonCompanyRoute(s.url)) {
      pushIssue(
        issues,
        "P1",
        "publisher_shell_in_sources",
        `Drop publisher shell from supportingSources: ${s.url}`,
      );
    }
  }

  const supportingSources = (structured.supportingSources || []).filter(
    (s) => s?.url && !isPressPublisherNonCompanyRoute(s.url),
  );

  const sanitized = {
    ...structured,
    organizationalContactRoutes: routes,
    supportingSources,
    buyerCandidates: (structured.buyerCandidates || []).map((b) => ({
      ...b,
      email: null,
      phone: null,
    })),
  };

  const p0 = issues.filter((x) => x.severity === "P0");
  return {
    ok: p0.length === 0,
    issues,
    sanitized,
    version: EXA_AGENT_EVIDENCE_VALIDATION_V1,
  };
}
