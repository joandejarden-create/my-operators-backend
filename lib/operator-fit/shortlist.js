/**
 * Operator Shortlist — field map + snapshot helpers (internal pilot).
 */

export const OPERATOR_SHORTLIST_TABLE = "Operator Fit - Shortlist";

export const SHORTLIST_STATUS = Object.freeze({
  SHORTLISTED: "Shortlisted",
  UNDER_REVIEW: "Under Review",
  REMOVED: "Removed",
  ADVANCED_TO_OUTREACH: "Advanced to Outreach",
});

export const map_operatorShortlistFields = Object.freeze({
  shortlistId: "Shortlist ID",
  deal: "Deal ID",
  dealLabel: "Deal Label",
  operator: "Operator",
  operatorName: "Operator Name",
  brand: "Brand",
  candidateType: "Candidate Type",
  operatingStructure: "Operating Structure",
  status: "Shortlist Status",
  shortlistedDate: "Shortlisted Date",
  shortlistedBy: "Shortlisted By",
  alignmentAtShortlist: "Alignment at Shortlist",
  confidenceAtShortlist: "Evidence Confidence at Shortlist",
  coverageAtShortlist: "Data Coverage at Shortlist",
  eligibilityAtShortlist: "Eligibility at Shortlist",
  readinessAtShortlist: "Readiness at Shortlist",
  lifecycleAtShortlist: "Lifecycle at Shortlist",
  engineVersion: "Engine Version",
  reasonsAtShortlist: "Reasons at Shortlist",
  concernsAtShortlist: "Concerns at Shortlist",
  unknownsAtShortlist: "Unknowns at Shortlist",
  advisorNote: "Advisor Note",
  removedDate: "Removed Date",
  removedBy: "Removed By",
  removalReason: "Removal Reason",
  outreachStatus: "Outreach Status",
  snapshotJson: "Decision Snapshot JSON",
});

/**
 * Build immutable decision snapshot for shortlist persistence.
 */
export function buildShortlistDecisionSnapshot(row = {}) {
  return {
    capturedAt: new Date().toISOString(),
    operatorId: row.operatorId || null,
    operatorName: row.operatorName || null,
    lifecycle: row.lifecycle || null,
    eligibility: row.eligibility || null,
    readiness: row.readiness || null,
    alignment: row.alignment ?? null,
    confidence: row.confidence || null,
    coverage: row.coverage ?? null,
    reasons: row.reasons || [],
    concerns: row.concerns || [],
    unknowns: row.unknowns || [],
    engineVersion: row.engineVersion || null,
    marketPresence: row.marketPresence || null,
    brand: row.brand || null,
    candidateType: row.candidateType || null,
    operatingStructure: row.operatingStructure || null,
  };
}

export function fieldsFromShortlistCreate(input, snapshot) {
  const F = map_operatorShortlistFields;
  const snap = snapshot || buildShortlistDecisionSnapshot(input);
  return {
    [F.shortlistId]: input.shortlistId || `osl_${Date.now()}`,
    [F.deal]: input.dealId || "",
    [F.dealLabel]: input.dealLabel || "",
    ...(input.operatorRecordId ? { [F.operator]: [input.operatorRecordId] } : {}),
    [F.operatorName]: input.operatorName || snap.operatorName || "",
    [F.brand]: input.brand || "",
    [F.candidateType]: input.candidateType || "Third-party operator",
    [F.operatingStructure]: input.operatingStructure || "",
    [F.status]: input.status || SHORTLIST_STATUS.SHORTLISTED,
    [F.shortlistedDate]: (input.shortlistedDate || new Date().toISOString()).slice(0, 10),
    [F.shortlistedBy]: input.shortlistedBy || "",
    [F.alignmentAtShortlist]: snap.alignment,
    [F.confidenceAtShortlist]: snap.confidence || "",
    [F.coverageAtShortlist]: snap.coverage,
    [F.eligibilityAtShortlist]: snap.eligibility || "",
    [F.readinessAtShortlist]: snap.readiness || "",
    [F.lifecycleAtShortlist]: snap.lifecycle || "",
    [F.engineVersion]: snap.engineVersion || "",
    [F.reasonsAtShortlist]: (snap.reasons || []).join("\n"),
    [F.concernsAtShortlist]: (snap.concerns || []).join("\n"),
    [F.unknownsAtShortlist]: (snap.unknowns || []).join("\n"),
    [F.advisorNote]: input.advisorNote || "",
    [F.outreachStatus]: input.outreachStatus || "Not started",
    [F.snapshotJson]: JSON.stringify(snap),
  };
}
