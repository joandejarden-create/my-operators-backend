/**
 * Canonical Operator Universe resolver (deterministic sets).
 * Does not change Operator Fit production behavior — Fit eligibility is reported separately.
 */

import {
  RECORD_PURPOSE,
  TEST_FIXTURE_MASTER_IDS,
  filterProductionUniverse,
  isTestFixtureMaster,
  normalizeEntityKey,
} from "./phase-1-universe.js";
import {
  classifyExplorerReadiness,
  classifyFitDataReadinessDiagnostic,
  isAggregateAssignmentName,
} from "./readiness.js";

export {
  RECORD_PURPOSE,
  TEST_FIXTURE_MASTER_IDS,
  filterProductionUniverse,
  isTestFixtureMaster,
  normalizeEntityKey,
  classifyExplorerReadiness,
  classifyFitDataReadinessDiagnostic,
  isAggregateAssignmentName,
};

/**
 * @param {Array<{id:string, fields:object}>} masters
 * @param {object} [intel] optional { assignments, brandRelationships, marketPresence, calibrationByMasterId, brandBasicsParents }
 */
export function buildOperatorUniverse(masters, intel = {}) {
  const assignments = intel.assignments || [];
  const brandRelationships = intel.brandRelationships || [];
  const marketPresence = intel.marketPresence || [];
  const calibrationByMasterId = intel.calibrationByMasterId || {};
  const aliases = intel.aliases || [];

  const rows = (masters || []).map((m) => {
    const purpose = m.fields?.["Record Purpose"] || null;
    const asg = assignments.filter((r) => (r.fields?.Operator || []).includes(m.id));
    const namedAsg = asg.filter((r) => !isAggregateAssignmentName(r.fields?.["Property Name"]));
    const br = brandRelationships.filter((r) => (r.fields?.Operator || []).includes(m.id));
    const mp = marketPresence.filter((r) => (r.fields?.Operator || []).includes(m.id));
    const countries = [
      ...new Set(
        [
          ...mp.map((r) => r.fields?.Country),
          ...namedAsg.map((r) => r.fields?.Country),
        ].filter(Boolean)
      ),
    ];
    const brandNames = [
      ...new Set(
        [
          ...br.map((r) => r.fields?.Brand),
          ...namedAsg.map((r) => r.fields?.Brand),
        ].filter(Boolean)
      ),
    ];
    const hasBmc = br.some((r) => r.fields?.["Relationship Type"] === "Brand Managed Capability");
    const calib = calibrationByMasterId[m.id] || null;
    const track = calib?.track || (hasBmc || /Managed\)/i.test(m.fields?.company_name || "") ? 2 : 0);

    const readiness = classifyExplorerReadiness({
      namedAssignmentCount: namedAsg.length,
      distinctCountryCount: countries.length,
      distinctBrandNameCount: brandNames.length,
      track: track === 1 || track === 2 ? track : hasBmc ? 2 : 1,
      hasBrandManagedCapability: hasBmc,
      recordPurpose: purpose,
    });

    const fitDiag = classifyFitDataReadinessDiagnostic({
      namedAssignmentCount: namedAsg.length,
      marketPresenceRowCount: mp.length,
      brandRelationshipCount: br.length,
      distinctCountryCount: countries.length,
      distinctBrandNameCount: brandNames.length,
    });

    const testFixture = purpose === RECORD_PURPOSE.TEST_FIXTURE || TEST_FIXTURE_MASTER_IDS.includes(m.id);
    const real = purpose === RECORD_PURPOSE.PRODUCTION || purpose === RECORD_PURPOSE.RESEARCH;

    // Fit production eligibility: report-only — do not rewire Fit. Exclude fixtures; require Production.
    const fitProductionEligible = purpose === RECORD_PURPOSE.PRODUCTION && !testFixture;

    return {
      masterId: m.id,
      canonicalName: m.fields?.company_name || null,
      parent: m.fields?.["Operator Parent Company"] || null,
      aliases: m.fields?.["Operator Aliases"] || null,
      website: m.fields?.["Operator Website"] || null,
      recordPurpose: purpose,
      lifecycle: m.fields?.submission_status || null,
      operatingModel: m.fields?.["Operating Model"] || null,
      managementAvailability: m.fields?.["Management Availability"] || null,
      testFixture,
      realOperator: real,
      calibration01: Boolean(calib),
      calibrationTrack: calib?.track || null,
      brandManagedDiscovery: Boolean(calib?.track === 2 || hasBmc || /Managed\)/i.test(m.fields?.company_name || "")),
      explorerResearchState: readiness.researchCompleteEnough
        ? readiness.contentComplete
          ? "Research Complete Enough"
          : "Partial"
        : "Research Required",
      explorerContentReadiness: readiness.contentClass,
      explorerContentComplete: readiness.contentComplete,
      contentCompleteButLifecycleGated: readiness.contentCompleteButLifecycleGated,
      explorerPublishable: readiness.explorerPublishable,
      strongExplorerProfile: readiness.strongExplorerProfile,
      usefulness: readiness.usefulness,
      fitDataReadiness: fitDiag,
      fitProductionEligible,
      ownerVisibleNow: false,
      counts: {
        namedAssignments: namedAsg.length,
        brandRelationships: br.length,
        marketPresence: mp.length,
        countries: countries.length,
        brands: brandNames.length,
        hasBmc,
      },
      reasons: [],
    };
  });

  const byPurpose = (p) => rows.filter((r) => r.recordPurpose === p);

  return {
    generatedAt: new Date().toISOString(),
    summary: {
      totalMasters: rows.length,
      production: byPurpose(RECORD_PURPOSE.PRODUCTION).length,
      research: byPurpose(RECORD_PURPOSE.RESEARCH).length,
      testFixtures: byPurpose(RECORD_PURPOSE.TEST_FIXTURE).length,
      realOperators: rows.filter((r) => r.realOperator).length,
      explorerContentComplete: rows.filter((r) => r.explorerContentComplete).length,
      explorerPublishable: rows.filter((r) => r.explorerPublishable).length,
      strongProfiles: rows.filter((r) => r.strongExplorerProfile).length,
      contentCompleteButLifecycleGated: rows.filter((r) => r.contentCompleteButLifecycleGated).length,
      fitDataReady: rows.filter((r) => r.fitDataReadiness === "Fit Data Ready").length,
      fitConditional: rows.filter((r) => r.fitDataReadiness === "Conditional").length,
      fitResearchRequired: rows.filter((r) => r.fitDataReadiness === "Research Required").length,
      calibrationMembership: rows.filter((r) => r.calibration01).length,
      brandManagedMembership: rows.filter((r) => r.brandManagedDiscovery).length,
    },
    sets: {
      allMasters: rows.map((r) => r.masterId),
      realOperators: rows.filter((r) => r.realOperator).map((r) => r.masterId),
      productionOperators: byPurpose(RECORD_PURPOSE.PRODUCTION).map((r) => r.masterId),
      researchOperators: byPurpose(RECORD_PURPOSE.RESEARCH).map((r) => r.masterId),
      testFixtures: byPurpose(RECORD_PURPOSE.TEST_FIXTURE).map((r) => r.masterId),
      explorerContentComplete: rows.filter((r) => r.explorerContentComplete).map((r) => r.masterId),
      explorerPublishable: rows.filter((r) => r.explorerPublishable).map((r) => r.masterId),
      fitDataReady: rows.filter((r) => r.fitDataReadiness === "Fit Data Ready").map((r) => r.masterId),
      fitProductionEligible: rows.filter((r) => r.fitProductionEligible).map((r) => r.masterId),
    },
    operators: rows,
    aliases,
  };
}

export function dispositionForOperator(row) {
  if (row.testFixture || row.recordPurpose === RECORD_PURPOSE.TEST_FIXTURE) {
    return "Test Fixture / Excluded";
  }
  if (row.recordPurpose === RECORD_PURPOSE.PRODUCTION) {
    if (row.explorerPublishable) return "Production / Explorer Candidate";
    return "Production / Needs More Research";
  }
  if (row.recordPurpose === RECORD_PURPOSE.RESEARCH) {
    if (row.contentCompleteButLifecycleGated || row.explorerContentComplete) {
      return "Research / Content Complete but Gated";
    }
    return "Research / Needs More Research";
  }
  return "Other — missing Record Purpose";
}
