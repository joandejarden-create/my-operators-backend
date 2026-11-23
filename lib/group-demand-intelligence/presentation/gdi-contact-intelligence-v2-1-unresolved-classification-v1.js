/**
 * Stage-2 unresolved contact classification (V2.1 Phase 1).
 *
 * Categories A–H may overlap. Each person gets one primary + optional
 * secondary blockers. Unique people are never double-counted in totals.
 */

import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import { isWeakEmployerOrganizationName } from "./gdi-public-contact-employer-domain-v1.js";

export const GDI_CONTACT_INTEL_V21_UNRESOLVED_CLASSIFICATION_V1 =
  "gdi-contact-intelligence-v2-1-unresolved-classification-v1";

export const UNRESOLVED_ROOT_CAUSE = Object.freeze({
  A_NAMED_PERSON_UNVERIFIED: "A_NAMED_PERSON_UNVERIFIED",
  B_EMPLOYER_UNVERIFIED: "B_EMPLOYER_UNVERIFIED",
  C_COMPANY_DOMAIN_UNCERTAIN: "C_COMPANY_DOMAIN_UNCERTAIN",
  D_DIRECT_WORK_EMAIL_MISSING: "D_DIRECT_WORK_EMAIL_MISSING",
  E_DIRECT_BUSINESS_PHONE_MISSING: "E_DIRECT_BUSINESS_PHONE_MISSING",
  F_EVIDENCE_FOUND_VERIFICATION_FAILED: "F_EVIDENCE_FOUND_VERIFICATION_FAILED",
  G_RESEARCH_EXHAUSTED: "G_RESEARCH_EXHAUSTED",
  H_RETRIEVAL_PARSING_MATCHING_DEFECT: "H_RETRIEVAL_PARSING_MATCHING_DEFECT",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * @param {object} row — Stage-2 eval row or freeze person + after/before
 * @returns {{ primary: string, secondary: string[], recoveryPotential: string, notes: string }}
 */
export function classifyUnresolvedContact(row = {}) {
  const name = text(row.name);
  const org = text(row.organization || row.org);
  const pages = Number(row.pagesFetched || 0);
  const coverage = text(row.coverage || "").toLowerCase();
  const after = row.after || {};
  const secondary = [];
  let primary = null;
  let recoveryPotential = "LOW";
  const notes = [];

  const personOk = looksLikePersonName(name);
  const employerWeak = isWeakEmployerOrganizationName(org);
  const missingEmail = after.directEmail !== true;
  const missingPhone = after.directPhone !== true;

  if (!personOk) {
    primary = UNRESOLVED_ROOT_CAUSE.A_NAMED_PERSON_UNVERIFIED;
    secondary.push(UNRESOLVED_ROOT_CAUSE.H_RETRIEVAL_PARSING_MATCHING_DEFECT);
    notes.push("Name fails person-identity gate (org/UI placeholder likely).");
    recoveryPotential = "NONE";
  } else if (employerWeak) {
    primary = UNRESOLVED_ROOT_CAUSE.B_EMPLOYER_UNVERIFIED;
    secondary.push(UNRESOLVED_ROOT_CAUSE.C_COMPANY_DOMAIN_UNCERTAIN);
    notes.push("Employer string is event/UI chrome or non-employer placeholder.");
    recoveryPotential = "LOW";
  } else if (pages >= 3 && missingEmail && missingPhone) {
    primary = UNRESOLVED_ROOT_CAUSE.G_RESEARCH_EXHAUSTED;
    if (missingEmail) secondary.push(UNRESOLVED_ROOT_CAUSE.D_DIRECT_WORK_EMAIL_MISSING);
    if (missingPhone) secondary.push(UNRESOLVED_ROOT_CAUSE.E_DIRECT_BUSINESS_PHONE_MISSING);
    notes.push("Budgeted free page fetches exhausted without verified channels.");
    recoveryPotential = "LOW";
  } else if (coverage === "weak" && pages <= 1) {
    primary = UNRESOLVED_ROOT_CAUSE.H_RETRIEVAL_PARSING_MATCHING_DEFECT;
    secondary.push(UNRESOLVED_ROOT_CAUSE.C_COMPANY_DOMAIN_UNCERTAIN);
    if (missingEmail) secondary.push(UNRESOLVED_ROOT_CAUSE.D_DIRECT_WORK_EMAIL_MISSING);
    if (missingPhone) secondary.push(UNRESOLVED_ROOT_CAUSE.E_DIRECT_BUSINESS_PHONE_MISSING);
    notes.push("Thin source coverage; likely seed-URL / matching gap.");
    recoveryPotential = "MEDIUM";
  } else if (personOk && !employerWeak && missingEmail && missingPhone) {
    primary = UNRESOLVED_ROOT_CAUSE.D_DIRECT_WORK_EMAIL_MISSING;
    secondary.push(UNRESOLVED_ROOT_CAUSE.E_DIRECT_BUSINESS_PHONE_MISSING);
    notes.push(
      "Person + employer appear identity-viable; public channels not recovered.",
    );
    recoveryPotential = pages < 3 ? "HIGH" : "MEDIUM";
  } else if (personOk && missingEmail) {
    primary = UNRESOLVED_ROOT_CAUSE.D_DIRECT_WORK_EMAIL_MISSING;
    if (missingPhone) {
      secondary.push(UNRESOLVED_ROOT_CAUSE.E_DIRECT_BUSINESS_PHONE_MISSING);
    }
    recoveryPotential = "MEDIUM";
  } else if (personOk && missingPhone) {
    primary = UNRESOLVED_ROOT_CAUSE.E_DIRECT_BUSINESS_PHONE_MISSING;
    recoveryPotential = "MEDIUM";
  } else {
    primary = UNRESOLVED_ROOT_CAUSE.F_EVIDENCE_FOUND_VERIFICATION_FAILED;
    notes.push("Residual unresolved after identity/employer/channel checks.");
    recoveryPotential = "LOW";
  }

  // Domain uncertainty as secondary when org looks multi-entity / URL-contaminated
  if (
    /;|http|\bpowered by\b|\bin relation to\b/i.test(org) &&
    !secondary.includes(UNRESOLVED_ROOT_CAUSE.C_COMPANY_DOMAIN_UNCERTAIN)
  ) {
    secondary.push(UNRESOLVED_ROOT_CAUSE.C_COMPANY_DOMAIN_UNCERTAIN);
  }

  return {
    personIdentityKey: row.personIdentityKey || null,
    name,
    organization: org,
    primary,
    secondary: [...new Set(secondary)],
    recoveryPotential,
    notes,
    pagesFetched: pages,
    hotelCount: Number(row.hotelCount || row.hotelIds?.length || 1),
  };
}

/**
 * Classify a list of unresolved Stage-2 rows without double-counting people.
 */
export function classifyUnresolvedCohort(rows = []) {
  const classifications = rows.map(classifyUnresolvedContact);
  const byPrimary = {};
  const byRecovery = { HIGH: 0, MEDIUM: 0, LOW: 0, NONE: 0 };
  for (const c of Object.values(UNRESOLVED_ROOT_CAUSE)) byPrimary[c] = 0;
  for (const c of classifications) {
    byPrimary[c.primary] = (byPrimary[c.primary] || 0) + 1;
    byRecovery[c.recoveryPotential] = (byRecovery[c.recoveryPotential] || 0) + 1;
  }
  const prioritized = classifications
    .filter((c) => c.recoveryPotential === "HIGH" || c.recoveryPotential === "MEDIUM")
    .sort((a, b) => {
      const rank = { HIGH: 0, MEDIUM: 1, LOW: 2, NONE: 3 };
      return rank[a.recoveryPotential] - rank[b.recoveryPotential];
    });

  return {
    version: GDI_CONTACT_INTEL_V21_UNRESOLVED_CLASSIFICATION_V1,
    uniquePeople: classifications.length,
    byPrimary,
    byRecovery,
    prioritizedForFreeResearch: prioritized,
    classifications,
  };
}

export default {
  GDI_CONTACT_INTEL_V21_UNRESOLVED_CLASSIFICATION_V1,
  UNRESOLVED_ROOT_CAUSE,
  classifyUnresolvedContact,
  classifyUnresolvedCohort,
};
