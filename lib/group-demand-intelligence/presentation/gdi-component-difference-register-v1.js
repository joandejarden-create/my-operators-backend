/**
 * Permanent GDI Component Difference Register V1.
 * Stable IDs; historical findings are never deleted — mark RESOLVED.
 */
import fs from "node:fs";
import path from "node:path";

export const GDI_COMPONENT_DIFFERENCE_REGISTER_V1 =
  "GDI_COMPONENT_DIFFERENCE_REGISTER_V1";

export const DIFFERENCE_CATEGORIES = Object.freeze([
  "CONTACT_FIELD_DIFFERENCE",
  "SVG_STATE_DIFFERENCE",
  "COMMERCIAL_ACTION_DIFFERENCE",
  "CARD_STRUCTURE_DIFFERENCE",
  "CSS_STYLE_DIFFERENCE",
  "DRAWER_CONTENT_DIFFERENCE",
  "TOOLBAR_DIFFERENCE",
  "FILTER_BEHAVIOR_DIFFERENCE",
  "ASSET_VERSION_DIFFERENCE",
  "PUBLICATION_DATA_DIFFERENCE",
  "CONFIGURATION_DIFFERENCE",
  "PERMISSION_DIFFERENCE",
  "RESPONSIVE_LAYOUT_DIFFERENCE",
  "GUIDED_TOUR_DIFFERENCE",
  "AUTH_LIVE_RECOMPUTE_DIFFERENCE",
]);

export const RESOLUTION_STATUS = Object.freeze({
  OPEN: "OPEN",
  RESOLVED: "RESOLVED",
  INTENTIONAL: "INTENTIONAL",
  NOT_VERIFIED: "NOT_VERIFIED",
  AWAITING_PUBLICATION_APPROVAL: "AWAITING_PUBLICATION_APPROVAL",
});

/**
 * @param {object} partial
 */
export function createDifferenceRecord(partial = {}) {
  const now = new Date().toISOString();
  const id =
    partial.differenceId ||
    [
      partial.category || "UNKNOWN",
      partial.hotelId || "hotel",
      partial.opportunityId || "opp",
      partial.componentName || "component",
    ]
      .join("__")
      .replace(/[^A-Za-z0-9._-]+/g, "_")
      .slice(0, 180);
  return {
    differenceId: id,
    componentName: partial.componentName || null,
    hotelId: partial.hotelId || null,
    opportunityId: partial.opportunityId || null,
    authenticatedRoute: partial.authenticatedRoute || null,
    externalShareRoute: partial.externalShareRoute || null,
    publicationVersion: partial.publicationVersion || null,
    expectedValue: partial.expectedValue ?? null,
    actualLocalAuth: partial.actualLocalAuth ?? null,
    actualLocalShare: partial.actualLocalShare ?? null,
    actualProductionShare: partial.actualProductionShare ?? null,
    category: partial.category || "CONFIGURATION_DIFFERENCE",
    severity: partial.severity || "MATERIAL",
    rootCause: partial.rootCause || null,
    owningSourceModule: partial.owningSourceModule || null,
    cssOrSvgFile: partial.cssOrSvgFile || null,
    proposedCorrection: partial.proposedCorrection || null,
    resolutionStatus: partial.resolutionStatus || RESOLUTION_STATUS.OPEN,
    fixCommit: partial.fixCommit || null,
    testCoverage: partial.testCoverage || null,
    verificationEvidence: partial.verificationEvidence || null,
    dateFirstDiscovered: partial.dateFirstDiscovered || now,
    dateResolved: partial.dateResolved || null,
  };
}

export function upsertDifference(register, record) {
  const list = Array.isArray(register?.differences)
    ? register.differences.slice()
    : [];
  const idx = list.findIndex((d) => d.differenceId === record.differenceId);
  if (idx >= 0) {
    const prev = list[idx];
    list[idx] = {
      ...prev,
      ...record,
      dateFirstDiscovered: prev.dateFirstDiscovered || record.dateFirstDiscovered,
    };
  } else {
    list.push(record);
  }
  return {
    version: GDI_COMPONENT_DIFFERENCE_REGISTER_V1,
    updatedAt: new Date().toISOString(),
    differences: list,
  };
}

export function markResolved(register, differenceId, evidence = {}) {
  const list = (register?.differences || []).map((d) => {
    if (d.differenceId !== differenceId) return d;
    return {
      ...d,
      resolutionStatus: RESOLUTION_STATUS.RESOLVED,
      dateResolved: new Date().toISOString(),
      fixCommit: evidence.fixCommit || d.fixCommit,
      verificationEvidence: evidence.verificationEvidence || d.verificationEvidence,
      testCoverage: evidence.testCoverage || d.testCoverage,
    };
  });
  return {
    version: GDI_COMPONENT_DIFFERENCE_REGISTER_V1,
    updatedAt: new Date().toISOString(),
    differences: list,
  };
}

export function loadRegister(filePath) {
  if (!fs.existsSync(filePath)) {
    return {
      version: GDI_COMPONENT_DIFFERENCE_REGISTER_V1,
      updatedAt: null,
      differences: [],
    };
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export function saveRegister(filePath, register) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(register, null, 2));
}

export function openMaterialCount(register) {
  return (register?.differences || []).filter(
    (d) =>
      d.severity === "MATERIAL" &&
      d.resolutionStatus !== RESOLUTION_STATUS.RESOLVED &&
      d.resolutionStatus !== RESOLUTION_STATUS.INTENTIONAL,
  ).length;
}
