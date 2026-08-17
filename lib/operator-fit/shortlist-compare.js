/**
 * Side-by-side shortlist comparison (up to 4) + difference drivers.
 */

import { explainRankingDifference } from "./ranking-difference.js";
import { listRankingChangeValidations } from "./ranking-change-validations.js";

const COMPARE_FIELDS = [
  { key: "operatorName", label: "Operator" },
  { key: "candidateType", label: "Candidate type" },
  { key: "operatingStructure", label: "Operating structure" },
  { key: "brand", label: "Brand relevance" },
  { key: "alignment", label: "Alignment" },
  { key: "confidence", label: "Evidence Confidence" },
  { key: "coverage", label: "Data Coverage" },
  { key: "eligibility", label: "Eligibility" },
  { key: "geography", label: "Geography" },
  { key: "comparables", label: "Comparable experience" },
  { key: "segment", label: "Segment / project relevance" },
  { key: "brandRelationships", label: "Brand relationships" },
  { key: "ownership", label: "Ownership / governance" },
  { key: "regionalResources", label: "Regional resources" },
  { key: "differentiators", label: "Main differentiators" },
  { key: "concerns", label: "Main concerns" },
  { key: "unknowns", label: "Important unknowns" },
];

function asList(v) {
  if (Array.isArray(v)) return v.filter(Boolean);
  if (v == null || v === "") return [];
  return [String(v)];
}

function normalizeCandidate(c = {}) {
  return {
    operatorId: c.operatorId || c.candidateId || null,
    operatorName: c.operatorName || c.name || "—",
    candidateType: c.candidateType || c.lifecycle || "Third-party operator",
    operatingStructure: c.operatingStructure || (asList(c.structures)[0] || "—"),
    brand: c.brand || asList(c.brands)[0] || "—",
    alignment: c.displayedOperatorAlignment ?? c.alignment ?? null,
    confidence: c.evidenceConfidence || c.confidence || "Unknown",
    coverage: c.dataCoveragePct ?? c.coverage ?? null,
    eligibility: c.eligibilityStatus || c.eligibility || "—",
    geography: c.geography || asList(c.countries).join(", ") || "—",
    comparables: asList(c.comparables).slice(0, 3).join("; ") || "Unknown",
    segment: c.segment || c.whyItMatches?.[0] || "—",
    brandRelationships: c.brandRelationships || "Unknown",
    ownership: c.ownership || "Unknown",
    regionalResources: c.regionalResources || "Unknown",
    differentiators: asList(c.whyItMatches || c.reasons).slice(0, 3),
    concerns: asList(c.potentialConcerns || c.concerns).slice(0, 3),
    unknowns: asList(c.unknowns).slice(0, 3),
    validationQuestions: c.validationQuestions || [],
    factorBreakdown: c.factorBreakdown || c.operatorProjectFactors || [],
    lifecycle: c.lifecycle || null,
    readiness: c.readiness || null,
  };
}

/**
 * @param {object[]} candidates — max 4
 * @param {object} [project]
 */
export function buildShortlistComparison(candidates = [], project = null) {
  const list = (candidates || []).slice(0, 4).map(normalizeCandidate);
  const rows = COMPARE_FIELDS.map((field) => {
    const values = list.map((c) => {
      const v = c[field.key];
      if (Array.isArray(v)) return v.length ? v.join("; ") : "Unknown";
      if (v == null || v === "") return "Unknown";
      return v;
    });
    const unique = new Set(values.map((x) => String(x).toLowerCase()));
    const isDifferentiator = unique.size > 1;
    const allUnknown = values.every((v) => /unknown|^—$/i.test(String(v)));
    return {
      field: field.key,
      label: field.label,
      values,
      highlight: isDifferentiator && !allUnknown,
      unknownOnly: allUnknown,
    };
  });

  const pairDrivers = [];
  for (let i = 0; i < list.length; i++) {
    for (let j = i + 1; j < list.length; j++) {
      pairDrivers.push({
        a: list[i].operatorName,
        b: list[j].operatorName,
        ...explainRankingDifference(list[i], list[j], { maxDrivers: 5 }),
      });
    }
  }

  const validations = list.map((c) => ({
    operatorId: c.operatorId,
    operatorName: c.operatorName,
    items: listRankingChangeValidations(project, {
      geography: { marketPresence: [] },
      operatingStructures: c.operatingStructure !== "—" ? [c.operatingStructure] : [],
      brandsOperated: c.brand !== "—" ? [c.brand] : [],
      comparables: c.comparables !== "Unknown" ? [c.comparables] : [],
      specialistExperience: {},
    }),
  }));

  return {
    operators: list,
    rows,
    pairDrivers,
    validations,
    maxCompared: 4,
    note: "Unknown is visually distinct from negative evidence; only genuine differences are highlighted.",
  };
}
