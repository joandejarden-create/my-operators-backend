/**
 * Evidence-fidelity evaluation for Commercial Intelligence Briefs.
 * Complements the 30-point narrative score — a longer brief is not automatically better.
 */

export const GDI_BRIEF_EVIDENCE_FIDELITY_V1 = "gdi-brief-evidence-fidelity-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function briefBlob(opp = {}) {
  const c = opp.primaryContact || {};
  return [
    opp.whatIsHappening,
    opp.whyThisHotel,
    opp.potentialHotelBusiness,
    opp.whyActNow,
    opp.recommendedNextStep,
    Array.isArray(opp.uncertainties) ? opp.uncertainties.join(" ") : opp.uncertainties,
    opp.commercialPosture || opp.briefV2?.commercialPosture,
    // Named buyer lines count toward Level-1 fidelity when present on the opportunity
    c.name,
    c.email,
    c.organizationContactRoute,
  ]
    .map(text)
    .join(" \n ");
}

function factPresent(blob, fact) {
  const needles = (fact.matchNeedles || [fact.claim]).map((n) => String(n).toLowerCase());
  const hay = blob.toLowerCase();
  return needles.some((n) => n && hay.includes(n.toLowerCase()));
}

/**
 * @param {object} opp — composed opportunity / brief fields
 * @param {object} catalog — { materialFacts: [...], unsupportedRiskPatterns?: [...] }
 */
export function evaluateBriefEvidenceFidelity(opp = {}, catalog = {}) {
  const blob = briefBlob(opp);
  const material = Array.isArray(catalog.materialFacts) ? catalog.materialFacts : [];
  const retained = [];
  const omitted = [];
  const researchAbsent = [];

  for (const fact of material) {
    const row = {
      id: fact.id,
      claim: fact.claim,
      commercialImportance: fact.commercialImportance || "MEDIUM",
      changesPursuit: Boolean(fact.changesPursuit),
      destination: fact.destination || "BRIEF",
      sourceUrl: fact.sourceUrl || null,
      verificationStatus: fact.verificationStatus || "CONFIRMED",
      origin: fact.origin || "ARCHIVED_RESEARCH",
    };
    if (fact.origin === "ABSENT_FROM_RESEARCH") {
      researchAbsent.push(row);
      continue;
    }
    if (factPresent(blob, fact)) retained.push(row);
    else omitted.push(row);
  }

  const unsupportedIntroduced = [];
  for (const pattern of catalog.unsupportedRiskPatterns || []) {
    const raw = pattern.test;
    if (!raw) continue;
    const re =
      raw instanceof RegExp
        ? raw
        : pattern.regex === true
          ? new RegExp(String(raw), "i")
          : new RegExp(String(raw).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
    if (!re.test(blob)) continue;
    if (pattern.allowIfAlsoMatches) {
      const allowRe =
        pattern.allowIfAlsoMatches instanceof RegExp
          ? pattern.allowIfAlsoMatches
          : new RegExp(String(pattern.allowIfAlsoMatches), "i");
      if (allowRe.test(blob)) continue;
    }
    unsupportedIntroduced.push({
      id: pattern.id,
      description: pattern.description,
      matched: String(raw),
    });
  }

  const materialForBrief = material.filter(
    (f) => f.origin !== "ABSENT_FROM_RESEARCH" && (f.destination || "BRIEF") === "BRIEF",
  );
  const retainedBrief = retained.filter((f) => (f.destination || "BRIEF") === "BRIEF");
  const omittedMaterialBrief = omitted.filter(
    (f) =>
      (f.destination || "BRIEF") === "BRIEF" &&
      (f.commercialImportance === "HIGH" || f.changesPursuit),
  );

  const wordCount = blob.split(/\s+/).filter(Boolean).length;
  const verbose = wordCount > 420;

  const defects = [];
  if (omittedMaterialBrief.length) defects.push("MATERIAL_BRIEF_FACTS_OMITTED");
  if (unsupportedIntroduced.length) defects.push("UNSUPPORTED_STATEMENTS_INTRODUCED");
  if (verbose) defects.push("UNNECESSARY_VERBOSITY");

  // Fidelity score 0–100: retention of brief-bound material facts minus unsupported hits
  const denom = Math.max(1, materialForBrief.length);
  const retentionPct = Math.round((retainedBrief.length / denom) * 100);
  const penalty = unsupportedIntroduced.length * 15 + (verbose ? 5 : 0);
  const fidelityScore = Math.max(0, Math.min(100, retentionPct - penalty));

  let judgment = "NEEDS_WORK";
  if (fidelityScore >= 85 && unsupportedIntroduced.length === 0) judgment = "STRONG";
  else if (fidelityScore >= 70 && unsupportedIntroduced.length <= 1) judgment = "ACCEPTABLE";
  else if (unsupportedIntroduced.length >= 2) judgment = "UNSUPPORTED_RISK";

  return {
    version: GDI_BRIEF_EVIDENCE_FIDELITY_V1,
    fidelityScore,
    judgment,
    wordCount,
    retained,
    omitted,
    researchAbsent,
    unsupportedIntroduced,
    omittedMaterialBrief,
    defects,
    summary: {
      materialFactsInCatalog: material.length,
      retainedCount: retained.length,
      omittedCount: omitted.length,
      unsupportedCount: unsupportedIntroduced.length,
      researchAbsentCount: researchAbsent.length,
    },
  };
}

export default {
  GDI_BRIEF_EVIDENCE_FIDELITY_V1,
  evaluateBriefEvidenceFidelity,
};
