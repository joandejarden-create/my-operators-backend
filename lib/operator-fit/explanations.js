/**
 * Explanation / validation layer.
 */

import { CANDIDATE_TYPE } from "./config.js";
import { isKnownPositive } from "./adapters/field-state.js";

export function buildExplanations({
  operator,
  eligibility,
  factors,
  brandCompat,
  structureAlign,
  confidence,
  coverage,
  risk,
  economics,
}) {
  const whyItMatches = [];
  const potentialConcerns = [];
  const unknowns = [];
  const validationQuestions = [];
  const sourceSummaries = [];

  for (const f of factors || []) {
    if (!f.applicable) continue;
    if (f.state === "known" && f.score >= 70 && f.positiveEvidence?.length) {
      whyItMatches.push(...f.positiveEvidence.slice(0, 2));
    } else if (f.state === "known" && f.score < 40 && f.negativeEvidence?.length) {
      potentialConcerns.push(...f.negativeEvidence.slice(0, 2));
    }
    if (f.unknownNotes?.length) {
      unknowns.push(...f.unknownNotes);
      for (const u of f.unknownNotes) {
        validationQuestions.push(`Validate: ${u}`);
      }
    }
  }

  for (const r of eligibility.reasons || []) {
    if (whyItMatches.length < 3) whyItMatches.push(r);
  }
  for (const c of eligibility.conditions || []) {
    validationQuestions.push(c);
  }
  for (const u of eligibility.unknowns || []) {
    unknowns.push(u);
  }
  for (const h of eligibility.hardConflicts || []) {
    potentialConcerns.push(h);
  }

  if (brandCompat.rationale && brandCompat.state === "known" && brandCompat.numericForComposition >= 55) {
    whyItMatches.push(brandCompat.rationale);
  }
  validationQuestions.push(...(brandCompat.validationItems || []));
  if (brandCompat.state === "unknown") unknowns.push(brandCompat.rationale);

  if (structureAlign.state === "known" && structureAlign.score >= 70) {
    whyItMatches.push(structureAlign.rationale);
  } else if (structureAlign.state === "unknown") {
    unknowns.push(structureAlign.rationale);
  }
  validationQuestions.push(...(structureAlign.validationItems || []));

  for (const item of risk.items || []) {
    if (item.kind === "confirmed_risk") potentialConcerns.push(item.message);
    else if (item.kind === "potential_concern") potentialConcerns.push(item.message);
    else {
      unknowns.push(item.message);
      validationQuestions.push(item.message);
    }
  }

  unknowns.push(...(coverage.materialMissingFields || []).map((f) => `Missing: ${f}`));
  validationQuestions.push(...(economics.validationItems || []));
  if (economics.state !== "present") unknowns.push(economics.note);

  if (isKnownPositive(operator.sources)) {
    for (const s of (operator.sources.value || []).slice(0, 5)) {
      sourceSummaries.push({
        label: s.label || s.url || "Source",
        class: s.verified
          ? "verified_project_level"
          : s.independent
            ? "independently_referenced"
            : "detailed_operator_provided",
      });
    }
  } else if (isKnownPositive(operator.comparables)) {
    for (const c of (operator.comparables.value || []).slice(0, 3)) {
      sourceSummaries.push({
        label: c.propertyName || c.name || "Comparable",
        class: c.verified
          ? "verified_project_level"
          : c.referenced
            ? "independently_referenced"
            : "detailed_operator_provided",
      });
    }
  } else {
    sourceSummaries.push({
      label: "No independent sources attached",
      class: "unknown",
    });
  }

  if (operator.candidateType === CANDIDATE_TYPE.BRAND_MANAGED) {
    whyItMatches.unshift("Candidate type: Brand Managed (not a third-party operator profile).");
  }

  const dedupe = (arr) => {
    const out = [];
    const seen = new Set();
    for (const x of arr) {
      const t = String(x || "").trim();
      if (!t || seen.has(t)) continue;
      seen.add(t);
      out.push(t);
    }
    return out;
  };

  return {
    whyItMatches: dedupe(whyItMatches).slice(0, 3),
    potentialConcerns: dedupe(potentialConcerns).slice(0, 2),
    unknowns: dedupe(unknowns).slice(0, 8),
    validationQuestions: dedupe(validationQuestions).slice(0, 8),
    sourceSummaries,
    confidenceNote: confidence.rationale,
  };
}
