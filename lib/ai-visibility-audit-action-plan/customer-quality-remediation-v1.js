/**
 * AVA Customer Quality Autonomous Remediation V1
 *
 * Bounded detect → classify → repair → regenerate → recertify loop.
 * Never invents facts. Never grants founder approval or publication.
 */
import {
  assessCustomerQuality,
  buildDefaultActionOverviewIntro,
  uniqueActionIds,
  CQA_DEFECT_TYPES,
  CUSTOMER_QUALITY_ASSURANCE_CONTRACT,
} from "./customer-quality-assurance-v1.js";
import {
  replaceInternalLanguage,
  claimTypeCustomerLabel,
} from "./customer-facing-language-v1.js";
import { normalizeDigitalAuthorityActionForPdf } from "./digital-authority-v1.js";

export const CUSTOMER_QUALITY_REMEDIATION_CONTRACT =
  "AVA_CUSTOMER_QUALITY_REMEDIATION_V1";

export const DEFAULT_CQA_LIMITS = Object.freeze({
  maxIterations: 3,
  maxPdfRegenerations: 2,
});

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function hostOf(u) {
  try {
    return new URL(u).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function walkStrings(obj, fn, skipKeys = new Set()) {
  if (obj == null) return;
  if (typeof obj === "string") return;
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      if (typeof obj[i] === "string") obj[i] = fn(obj[i]);
      else walkStrings(obj[i], fn, skipKeys);
    }
    return;
  }
  if (typeof obj === "object") {
    for (const [k, v] of Object.entries(obj)) {
      if (skipKeys.has(k)) continue;
      if (typeof v === "string") obj[k] = fn(v);
      else walkStrings(v, fn, skipKeys);
    }
  }
}

function scrubEvidenceKinds(items, log, section) {
  let changed = 0;
  for (const e of items || []) {
    const label = String(e?.label || "");
    if (/hilton|curio/i.test(label) && /^VERIFIED/i.test(String(e.kind || ""))) {
      const before = e.kind;
      e.kind = "NOT_CURRENTLY_VERIFIED";
      if (
        e.url &&
        /waterstoneboca\.com/i.test(String(e.url)) &&
        !/hilton\.com/i.test(String(e.url))
      ) {
        e.url = null;
        e.note =
          (e.note ? `${e.note} ` : "") +
          "Brand/Hilton property URL could not be re-verified; hotel-operated URL removed as brand evidence.";
      } else if (!e.note) {
        e.note =
          "Could not be re-verified as current customer-facing evidence (brand access failure).";
      }
      log.push({
        defect: CQA_DEFECT_TYPES.CRITICAL_FACTUAL_CONTRADICTION,
        section,
        severity: "CRITICAL",
        originalText: `${label} [${before}]`,
        correctedText: `${label} [NOT_CURRENTLY_VERIFIED]`,
        reason:
          "Executive/findings state brand figure could not be re-verified; VERIFIED label removed",
      });
      changed++;
    }
  }
  return changed;
}

function repairEvidenceContradictions(payload, log) {
  let n = 0;
  for (const f of payload.keyFindings || []) {
    n += scrubEvidenceKinds(
      f.evidence,
      log,
      `keyFindings:${f.title || f.relatedActionId}`
    );
  }
  for (const a of payload.actions || []) {
    n += scrubEvidenceKinds(
      a.supportingEvidence,
      log,
      `actions:${a.actionId}:supportingEvidence`
    );
    n += scrubEvidenceKinds(
      a.technicalDetails?.urls,
      log,
      `actions:${a.actionId}:technicalDetails.urls`
    );
    n += scrubEvidenceKinds(
      a.technicalDetails?.evidence,
      log,
      `actions:${a.actionId}:technicalDetails.evidence`
    );

    const why = String(a.whyItMatters || "");
    if (/both claims are verified|both figures are verified/i.test(why)) {
      const next = why
        .replace(
          /Both claims are verified, but their scopes are not\./i,
          "Only the hotel-operated meetings figure is currently verified as public evidence; the Hilton/brand figure could not be re-verified."
        )
        .replace(
          /both (?:claims|figures) are verified/gi,
          "the hotel-operated figure is verified while the brand figure remains unverified"
        );
      if (next !== why) {
        log.push({
          defect: CQA_DEFECT_TYPES.CRITICAL_FACTUAL_CONTRADICTION,
          section: `actions:${a.actionId}.whyItMatters`,
          severity: "CRITICAL",
          originalText: why,
          correctedText: next,
          reason: "Align action language with evidence status",
        });
        a.whyItMatters = next;
        n++;
      }
    }

    const found = String(a.whatWeFound || "");
    if (
      /^Hilton states/i.test(found) &&
      /could not be re-verified|brand error/i.test(
        JSON.stringify(payload.executiveSummary || {})
      )
    ) {
      const next = found.replace(
        /^Hilton states/i,
        "A previously cited Hilton listing stated"
      );
      if (next !== found && !/could not be re-verified/i.test(found)) {
        a.whatWeFound =
          next +
          " That Hilton/brand page could not be re-verified in current checks (brand error / access failure).";
        log.push({
          defect: CQA_DEFECT_TYPES.CRITICAL_FACTUAL_CONTRADICTION,
          section: `actions:${a.actionId}.whatWeFound`,
          severity: "CRITICAL",
          originalText: found,
          correctedText: a.whatWeFound,
          reason: "Qualify Hilton claim as previously cited, not currently verified",
        });
        n++;
      }
    }

    // Soften priorityWhy "Two trusted guest-facing sources" when brand unverified
    if (
      /two trusted/i.test(String(a.priorityWhy || "")) &&
      /hilton/i.test(String(a.priorityWhy || ""))
    ) {
      const before = a.priorityWhy;
      a.priorityWhy =
        "The hotel-operated site publishes a meetings total that differs from a previously cited Hilton figure that could not be re-verified.";
      log.push({
        defect: CQA_DEFECT_TYPES.COMMERCIAL_CLARITY_ISSUE,
        section: `actions:${a.actionId}.priorityWhy`,
        severity: "MEDIUM",
        originalText: before,
        correctedText: a.priorityWhy,
        reason: "Avoid implying unverified brand figure is currently trusted evidence",
      });
      n++;
    }
  }
  return n;
}

function repairActionReconciliation(payload, log) {
  let n = 0;
  const ids = uniqueActionIds(payload);

  // Deduplicate overview
  const seen = new Set();
  const overview = [];
  for (const row of payload.actionOverview || []) {
    if (!row?.actionId) continue;
    if (seen.has(row.actionId)) {
      log.push({
        defect: CQA_DEFECT_TYPES.DUPLICATE_ACTION,
        section: "actionOverview",
        severity: "HIGH",
        originalText: row.actionId,
        correctedText: "(removed duplicate)",
        reason: "Duplicate overview entry removed",
      });
      n++;
      continue;
    }
    seen.add(row.actionId);
    overview.push(row);
  }
  // Ensure every action appears once in overview
  for (const a of payload.actions || []) {
    if (!a?.actionId || seen.has(a.actionId)) continue;
    overview.push({
      actionId: a.actionId,
      priority: a.priority,
      headline: a.headline,
      businessRelevance: a.whyItMatters || a.priorityWhy,
      accountableOwner: a.accountableOwner,
      suggestedTimeframe: a.suggestedTimeframe,
    });
    seen.add(a.actionId);
    n++;
  }
  payload.actionOverview = overview;

  const intro = buildDefaultActionOverviewIntro(ids.length);
  if (payload.actionOverviewIntro !== intro) {
    log.push({
      defect: CQA_DEFECT_TYPES.ACTION_COUNT_MISMATCH,
      section: "actionOverviewIntro",
      severity: "HIGH",
      originalText: payload.actionOverviewIntro,
      correctedText: intro,
      reason: `Align intro count with ${ids.length} unique actions`,
    });
    payload.actionOverviewIntro = intro;
    n++;
  }

  // Sync hotel decisions for every action
  if (!payload.hotelDecisions) payload.hotelDecisions = { intro: "", items: [] };
  if (/validation-stage payload/i.test(payload.hotelDecisions.intro || "")) {
    const before = payload.hotelDecisions.intro;
    payload.hotelDecisions.intro =
      "No action is pre-approved. Hotel leadership must confirm facts and authorize public or platform changes.";
    log.push({
      defect: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
      section: "hotelDecisions.intro",
      severity: "HIGH",
      originalText: before,
      correctedText: payload.hotelDecisions.intro,
      reason: "Remove internal validation-stage wording",
    });
    n++;
  }

  const decisionById = new Map(
    (payload.hotelDecisions.items || []).map((d) => [d.relatedActionId, d])
  );
  const items = [];
  for (const a of payload.actions || []) {
    if (!a?.actionId) continue;
    const existing = decisionById.get(a.actionId);
    if (existing) {
      items.push(existing);
    } else {
      items.push({
        title: a.headline,
        detail: a.requiresHotelApproval
          ? "Hotel leadership must confirm facts and authorize changes before implementation."
          : "Confirm owner, timing, and completion criteria before starting.",
        relatedActionId: a.actionId,
      });
      log.push({
        defect: CQA_DEFECT_TYPES.ACTION_COUNT_MISMATCH,
        section: "hotelDecisions",
        severity: "MEDIUM",
        originalText: `(missing ${a.actionId})`,
        correctedText: a.headline,
        reason: "Add decision item for action present in detailed plan",
      });
      n++;
    }
  }
  payload.hotelDecisions.items = items;
  return n;
}

function repairDigitalAuthorityClassifications(payload, log) {
  let n = 0;
  const da = payload.digitalAuthority;
  if (!da) return 0;
  const officialHost = hostOf(
    payload.property?.website || da.officialWebsite || ""
  );
  for (const s of da.sources || []) {
    const srcHost = hostOf(s.linkingPageUrl || s.evidenceUrl || "");
    if (
      s.findingClass === "VERIFIED_EXTERNAL_MENTION" &&
      officialHost &&
      srcHost === officialHost
    ) {
      const before = s.findingClass;
      s.findingClass = "FIRST_PARTY_OFFICIAL_SITE";
      s.note =
        (s.note ? `${s.note} ` : "") +
        "Reclassified: hotel-operated / first-party site is not a third-party mention.";
      log.push({
        defect: CQA_DEFECT_TYPES.INVALID_EVIDENCE_CLASSIFICATION,
        section: "digitalAuthority.sources",
        severity: "HIGH",
        originalText: before,
        correctedText: s.findingClass,
        reason: "First-party site cannot be an external mention",
      });
      n++;
    }
  }

  // Customer-facing DA prose: strip codes
  for (const field of [
    "executiveFinding",
    "competitiveComparison",
    "inventoryCompleteness",
  ]) {
    if (!da[field]) continue;
    const before = da[field];
    const after = replaceInternalLanguage(before);
    if (after !== before) {
      da[field] = after;
      log.push({
        defect: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
        section: `digitalAuthority.${field}`,
        severity: "HIGH",
        originalText: before.slice(0, 200),
        correctedText: after.slice(0, 200),
        reason: "Translate internal research codes to customer language",
      });
      n++;
    }
  }
  return n;
}

function repairCommunicationLanguage(payload, log) {
  let n = 0;
  const skip = new Set([
    "actionId",
    "relatedActionId",
    "propertyId",
    "adpPeriodId",
    "contract",
    "moduleId",
    "findingClass",
    "claimType",
    "kind",
    "gapClass",
    "status",
    "classification",
    "authorityStatus",
    "sourceCategory",
    "researchMode",
    "url",
    "evidenceUrl",
    "linkingPageUrl",
    "targetOrganizationUrl",
    "website",
  ]);

  const beforeSnap = JSON.stringify(payload);
  walkStrings(
    payload,
    (s) => {
      const next = replaceInternalLanguage(s);
      if (next !== s) n++;
      return next;
    },
    skip
  );
  // Don't count every string mutation in log as separate — summarize
  if (n > 0) {
    log.push({
      defect: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
      section: "payload",
      severity: "HIGH",
      originalText: "(multiple internal tokens)",
      correctedText: "(customer language)",
      reason: `Translated ${n} internal-language occurrences across customer fields`,
      verificationResult: beforeSnap === JSON.stringify(payload) ? "NOOP" : "APPLIED",
    });
  }

  // Methodology claim guide → customer labels
  if (Array.isArray(payload.methodology?.claimTypeGuide)) {
    payload.methodology.claimTypeGuide = payload.methodology.claimTypeGuide.map(
      (line) => {
        const before = line;
        let after = replaceInternalLanguage(line);
        after = after
          .replace(/\bVERIFIED_ISSUE\b/g, claimTypeCustomerLabel("VERIFIED_ISSUE"))
          .replace(
            /\bRECOMMENDED_INVESTIGATION\b/g,
            claimTypeCustomerLabel("RECOMMENDED_INVESTIGATION")
          )
          .replace(
            /\bREQUIRES_HOTEL_CONFIRMATION\b/g,
            claimTypeCustomerLabel("REQUIRES_HOTEL_CONFIRMATION")
          )
          .replace(
            /\bOPTIONAL_COMMERCIAL_OPPORTUNITY\b/g,
            claimTypeCustomerLabel("OPTIONAL_COMMERCIAL_OPPORTUNITY")
          );
        if (after !== before) {
          log.push({
            defect: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
            section: "methodology.claimTypeGuide",
            severity: "MEDIUM",
            originalText: before,
            correctedText: after,
            reason: "Replace raw claim-type codes with customer labels",
          });
          n++;
        }
        return after;
      }
    );
  }
  return n;
}

function repairHotelSpecificNextStep(payload, log) {
  const next = String(payload.methodology?.nextStep || "");
  if (
    !next ||
    !/confirm the decision items|authorize the highest-priority validated work/i.test(
      next
    )
  ) {
    return 0;
  }
  const hotel = payload.property?.name || "the hotel";
  const top = (payload.actions || []).find((a) => /HIGH/i.test(a.priority));
  const theme =
    top?.headline ||
    payload.executiveSummary?.headline ||
    "the highest-priority confirmation items";
  const corrected = `For ${hotel}: confirm the decision items — starting with “${theme}” — then authorize the validated work your leadership prioritizes.`;
  log.push({
    defect: CQA_DEFECT_TYPES.COMMERCIAL_CLARITY_ISSUE,
    section: "methodology.nextStep",
    severity: "MEDIUM",
    originalText: next,
    correctedText: corrected,
    reason: "Replace generic next step with hotel-specific guidance",
  });
  payload.methodology.nextStep = corrected;
  return 1;
}

/**
 * Apply one remediation pass. Returns { payload, repairs, log }.
 */
export function remediateCustomerQualityOnce(payload, assessment) {
  const next = clone(payload);
  const log = [];
  let repairs = 0;

  const types = new Set((assessment?.defects || []).map((d) => d.type));

  if (
    types.has(CQA_DEFECT_TYPES.CRITICAL_FACTUAL_CONTRADICTION) ||
    types.has(CQA_DEFECT_TYPES.UNSUPPORTED_CLAIM)
  ) {
    repairs += repairEvidenceContradictions(next, log);
  }
  if (
    types.has(CQA_DEFECT_TYPES.ACTION_COUNT_MISMATCH) ||
    types.has(CQA_DEFECT_TYPES.DUPLICATE_ACTION)
  ) {
    repairs += repairActionReconciliation(next, log);
  }
  if (types.has(CQA_DEFECT_TYPES.INVALID_EVIDENCE_CLASSIFICATION)) {
    repairs += repairDigitalAuthorityClassifications(next, log);
  }
  if (types.has(CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK)) {
    repairs += repairDigitalAuthorityClassifications(next, log);
    repairs += repairCommunicationLanguage(next, log);
  }
  if (types.has(CQA_DEFECT_TYPES.COMMERCIAL_CLARITY_ISSUE)) {
    repairs += repairHotelSpecificNextStep(next, log);
    // Also fix priorityWhy contradictions if present
    repairs += repairEvidenceContradictions(next, log);
  }

  // Always run light language scrub if any HIGH leaks remain after targeted fixes
  if ((assessment?.gates?.communicationQuality?.defects || []).length) {
    repairs += repairCommunicationLanguage(next, log);
  }

  // Ensure every action meets PDF contract fields before regeneration
  if (Array.isArray(next.actions)) {
    next.actions = next.actions.map((a) => {
      const normalized = normalizeDigitalAuthorityActionForPdf(a);
      if (!normalized.suggestedTimeframe) {
        normalized.suggestedTimeframe =
          a.suggestedTimeframe || "Confirm timing with hotel leadership";
      }
      if (!normalized.accountableOwner) {
        normalized.accountableOwner =
          a.accountableOwner || "RESPONSIBLE_PARTY_TO_CONFIRM";
      }
      return normalized;
    });
  }

  return { payload: next, repairs, log };
}

/**
 * Full bounded CQA loop.
 */
export async function runCustomerQualityAssuranceLoop(payload, options = {}) {
  const limits = { ...DEFAULT_CQA_LIMITS, ...(options.limits || {}) };
  let current = clone(payload);
  const history = [];
  let totalRepairs = 0;
  let pdfRegens = 0;
  let assessment = assessCustomerQuality(current, options);

  for (let i = 0; i < limits.maxIterations; i++) {
    if (assessment.overallPass) break;
    const { payload: repaired, repairs, log } = remediateCustomerQualityOnce(
      current,
      assessment
    );
    if (repairs === 0 && log.length === 0) {
      history.push({
        iteration: i + 1,
        repairs: 0,
        note: "No deterministic repairs available; stopping",
        defectsRemaining: assessment.defectCount,
      });
      break;
    }
    current = repaired;
    totalRepairs += repairs;
    history.push({
      iteration: i + 1,
      repairs,
      log,
      defectsBefore: assessment.defectCount,
    });

    if (options.regeneratePdfFn && repairs > 0 && pdfRegens < limits.maxPdfRegenerations) {
      await options.regeneratePdfFn(current);
      pdfRegens++;
    }

    assessment = assessCustomerQuality(current, {
      ...options,
      pdfBuffer: options.pdfBuffer || null,
    });
    history[history.length - 1].defectsAfter = assessment.defectCount;
    history[history.length - 1].passAfter = assessment.overallPass;
  }

  const unresolved = (assessment.defects || []).filter(
    (d) => d.severity === "CRITICAL" || d.severity === "HIGH"
  );

  return {
    contract: CUSTOMER_QUALITY_REMEDIATION_CONTRACT,
    assuranceContract: CUSTOMER_QUALITY_ASSURANCE_CONTRACT,
    payload: current,
    assessment,
    history,
    totalRepairs,
    pdfRegenerations: pdfRegens,
    customerQualityCertified: assessment.overallPass === true,
    unresolvedMaterialIssues: unresolved,
    founderApprovedForCustomer: false,
    publicationAuthorized: false,
    blocked:
      assessment.overallPass !== true
        ? unresolved.map((d) => d.message)
        : [],
  };
}
