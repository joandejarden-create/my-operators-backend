/**
 * Field-Specific Writer v2 — evidence-isolated Setup narrative writer.
 * Returns value OR blank. Never emits generic hospitality boilerplate.
 */
export const WRITER_V2_VERSION = "operator-setup-field-writer-v2-1";

const BANNED = [
  /strong operating platform/i,
  /owner-focused(?! )/i,
  /owner-centric/i,
  /owner-friendly/i,
  /partnership approach/i,
  /aligned with owners/i,
  /partners closely with owners/i,
  /strong owner relationships/i,
  /collaborative asset management/i,
  /hands-on (management|approach)/i,
  /highly responsive/i,
  /institutional quality/i,
  /robust reporting/i,
  /sophisticated systems/i,
  /comprehensive commercial support/i,
  /flexible approach/i,
  /experienced team/i,
  /tailored (owner )?solutions/i,
  /best-in-class/i,
  /centralized capabilities/i,
  /underwrite from the management agreement/i,
  /confirm in diligence/i,
  /systems vary by (brand and )?asset/i,
  /no unverified scorecards/i,
  /owner’s mindset|owners'? mindset/i,
];

export function isBannedGeneric(text) {
  if (!text) return false;
  return BANNED.some((re) => re.test(String(text)));
}

export function stripCompanyName(text, companyName) {
  if (!text) return "";
  let t = String(text);
  if (companyName) {
    t = t.split(companyName).join("[OPERATOR]");
    const short = companyName.split(/\s|\(/)[0];
    if (short && short.length > 3) t = t.split(new RegExp(short, "gi")).join("[OPERATOR]");
  }
  return t.replace(/\s+/g, " ").trim();
}

export function counterfactualCouldApplyToPeers(text, companyName, peerCountThreshold = 3) {
  const stripped = stripCompanyName(text, companyName).toLowerCase();
  // If after stripping names, no specific named platform/process/portal remains, fail
  const specificityMarkers = [
    /ownview/,
    /aimbridge intelligence/,
    /s\.?p\.?a\.?r\.?k/,
    /highgate intelligence/,
    /microsoft fabric/,
    /oracle fusion/,
    /ibm planning/,
    /managed by marriott|mxm/,
    /hilton honors|hits agreement|hilton systems|program fees|hilton onq|\bonq\b/,
    /ihg concerto|\bconcerto\b|concerto dashboards|digital advantage|guest reservation system/,
    /amadeus|acrs|\bwemax\b|\bmax owner\b/,
    /hyattconnect|ecotrack|world of hyatt/,
    /meli[aá]pro|\[operator\]pro/,
    /workiva/,
    /flywire/,
    /maestro/,
    /management agreements?/,
    /operating and capital budgets|key personnel|incentive fees/,
    /wave of change/,
    /priority owners/,
    /flux[aá]/,
    /ihg (strategic )?alliance|iberostar beachfront/,
    /salesforce/,
    /power bi/,
    /weekly flash/,
    /monthly owner/,
    /quarterly business review/,
    /pre-opening milestone/,
    /takeover|reposition/,
    /all-inclusive division/,
    /concept-to-operation|conceptualization/,
    /air-passenger|visitation intelligence/,
    /mexico city|colombia|latam|caribbean|peru|chile|guatemala/,
    /owner–brand–operator|owner-brand-operator|integrated owner/,
    /yield-management|post-booking upsell|direct-booking/,
    /tax-reform operational/,
    /regional-operator|shared-services|business units|segment-focused/,
    /brand operating platform|brand-managed leadership|brand-operator/,
    /above-property|on-property leadership/,
    /concerto|digital advantage|acrs|melia?pro/,
    /flywire|onq|maestro/,
    /concept-to-operation|multi-brand, multi-product/,
    /owner approval|owner approves|key personnel/,
    /above-property|area team|owner–gm|owner-gm/,
    /owner relations|asset-management interface|asset management interface/,
    /owner advisory|franchise advisory|owner council/,
    /self-service analytics|owner-exclusive portal/,
    /weekly flash|monthly (owner )?packs?|quarterly (business )?reviews?/,
    /regional leadership|local presence|mexico city team/,
  ];
  const hasMarker = specificityMarkers.some((re) => re.test(stripped));
  if (!hasMarker) return { fail: true, reason: "no_specificity_markers_after_name_strip" };
  return { fail: false, reason: null };
}

/**
 * @param {{ fieldName: string, contract: object, evidenceSlice: object, companyName: string, exemplars?: object[] }} args
 */
export function writeFieldV2({ fieldName, contract, evidenceSlice, companyName, exemplars = [] }) {
  const base = {
    fieldName,
    companyName,
    writerVersion: WRITER_V2_VERSION,
    proposedValue: null,
    evidenceReferences: [],
    evidenceClassification: null,
    confidence: "none",
    lastVerified: null,
    abstainReason: null,
    exemplarComparison: null,
    differentiationTest: null,
    fidelity: null,
    verdict: "BLANK",
  };

  if (!contract) {
    return { ...base, abstainReason: "missing_semantic_contract", verdict: "FIELD DESIGN PROBLEM" };
  }
  if (!evidenceSlice) {
    return { ...base, abstainReason: "no_evidence_slice", verdict: "BLANK" };
  }
  if (evidenceSlice.status === "NOT_RESEARCHABLE") {
    return { ...base, abstainReason: evidenceSlice.reason || "not_researchable", verdict: "BLANK" };
  }
  if (evidenceSlice.status === "RESEARCH_MORE") {
    return {
      ...base,
      abstainReason: evidenceSlice.reason || "needs_more_research",
      evidenceReferences: evidenceSlice.sources || [],
      verdict: "RESEARCH MORE",
    };
  }
  if (!evidenceSlice.answersField || !evidenceSlice.facts?.length) {
    return {
      ...base,
      abstainReason: "evidence_does_not_answer_field",
      evidenceReferences: evidenceSlice.sources || [],
      verdict: "BLANK",
    };
  }

  const fidelity = evidenceSlice.fidelity || "DIRECTLY SUPPORTED";
  if (fidelity === "WEAK INFERENCE" || fidelity === "UNSUPPORTED") {
    return {
      ...base,
      abstainReason: `rejected_fidelity:${fidelity}`,
      evidenceReferences: evidenceSlice.sources || [],
      fidelity,
      verdict: "BLANK",
    };
  }

  const value = evidenceSlice.draftValue;
  if (!value || String(value).trim().length < 20) {
    return { ...base, abstainReason: "empty_draft", fidelity, verdict: "BLANK" };
  }
  if (isBannedGeneric(value)) {
    return {
      ...base,
      abstainReason: "banned_generic_language",
      fidelity,
      evidenceReferences: evidenceSlice.sources || [],
      verdict: "BLANK",
    };
  }

  const cf = counterfactualCouldApplyToPeers(value, companyName);
  if (cf.fail && !evidenceSlice.allowStandardizedClassification) {
    return {
      ...base,
      abstainReason: `counterfactual_fail:${cf.reason}`,
      fidelity,
      evidenceReferences: evidenceSlice.sources || [],
      proposedValue: value,
      verdict: "BLANK",
      differentiationTest: "FAIL_COUNTERFACTUAL",
    };
  }

  const exemplarNote =
    exemplars?.length > 0
      ? `Matches exemplar specificity bar (${exemplars.map((e) => e.name).join(", ")}): ${evidenceSlice.exemplarAlignment || "comparable specificity"}`
      : "No exemplar compare";

  return {
    ...base,
    proposedValue: String(value).trim(),
    evidenceReferences: evidenceSlice.sources || [],
    evidenceClassification: evidenceSlice.classification || "official",
    confidence: evidenceSlice.confidence || "medium",
    lastVerified: evidenceSlice.lastVerified || new Date().toISOString().slice(0, 10),
    abstainReason: null,
    exemplarComparison: exemplarNote,
    differentiationTest: "PENDING_BATCH",
    fidelity,
    verdict: "ACCEPT",
    whyBelongsInField: evidenceSlice.whyBelongs || contract.question,
  };
}

export function classifyBatchDifferentiation(outputsForField) {
  const accepted = outputsForField.filter((o) => o.verdict === "ACCEPT" && o.proposedValue);
  const fps = accepted.map((o) =>
    stripCompanyName(o.proposedValue, o.companyName)
      .toLowerCase()
      .replace(/\d+/g, "N")
      .replace(/\s+/g, " ")
      .slice(0, 200)
  );
  const clusters = new Map();
  for (let i = 0; i < fps.length; i++) {
    const fp = fps[i];
    let matched = false;
    for (const [k, idxs] of clusters) {
      const a = new Set(k.split(" "));
      const b = new Set(fp.split(" "));
      let inter = 0;
      for (const t of a) if (b.has(t)) inter++;
      const j = inter / Math.max(1, new Set([...a, ...b]).size);
      if (j >= 0.72) {
        idxs.push(i);
        matched = true;
        break;
      }
    }
    if (!matched) clusters.set(fp, [i]);
  }
  return accepted.map((o, i) => {
    let label = "DISTINCTIVE";
    for (const idxs of clusters.values()) {
      if (idxs.includes(i) && idxs.length >= 3) label = "TEMPLATE VARIATION";
      else if (idxs.includes(i) && idxs.length === 2) label = "ACCEPTABLY STANDARDIZED";
    }
    if (isBannedGeneric(o.proposedValue)) label = "GENERIC";
    return { ...o, differentiationTest: label };
  });
}
