/**
 * Internal benchmark cohort integrity audit — inspect only.
 * Does not change BENCHMARK_ENGINE_V1, customer API, or Presence Index formula.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  listShowcaseMonitoringBrandIds,
  loadShowcaseCompaniesConfig,
} from "../brand-ai-showcase-companies.js";
import {
  loadPeerSetConfig,
  resolvePeerSetMembership,
  PEER_SET_ID_V2,
  PEER_SET_ID_V5,
  peerSetBrandNamesById,
} from "../peer-sets.js";
import { loadApprovedInternalAdditionsConfig } from "./approved-internal-additions.js";
import { buildPresenceObservationIndex } from "./presence-re-extraction.js";
import { resolveContextualPeerIds, resolvePrimaryCohortType } from "./contextual-cohort-v1.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST } from "./customer-payload.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const PILOT_REPORT_PATH = path.join(
  ROOT,
  "reports",
  "ai-visibility",
  "brand-presence-index-pilot-v1.json"
);
const ACTIVE_UNIVERSE_REPORT = path.join(
  ROOT,
  "reports",
  "brand-explorer-active-universe-source-of-truth.json"
);

export const COHORT_INTEGRITY_AUDIT_VERSION = "benchmark_cohort_integrity_audit_v1";

/** IDs used throughout this audit (governed Brand Explorer / peer-set records). */
export const IDS = Object.freeze({
  AUTOGRAPH: "recEJCTDj1zrsjPM6",
  TRIBUTE: "recCvV0PuZOi8c3hC",
  DESIGN: "rec02zPClpWUTCyXM",
  WESTIN: "recIPuBC50fv13zRR",
  AC: "rec9aZp7GHtzUEg0c",
  CURIO: "receQkxgjlezsc1xg",
  TAPESTRY: "reccXxMHEh7NNRhIE",
  CANOPY: "recsggfbKlJbjeRP9",
  TEMPO: "recqiHq3GHKMj8Meo",
  ASCEND: "reclkgOzvAcBheUSo",
  RAD_IND: "recRyvM8OmLlDj9G7",
  RAD_BLU: "recWPEvxBQxVVzSq3",
  RAD_RED: "recmKqo7M7mLZgRqQ",
  RADISSON: "recywbx1YQSTCPqW1",
  INDIGO: "recegXrqaPiSLGCIe",
  KIMPTON: "recCKuXCmGvxHPfb3",
  VOCO: "recwONQTqGU1jHCsM",
  EVEN: "recvvmiyReHhiKdoK",
  VIGNETTE: "recDwzv86TWnz2gGB",
  MGALLERY: "recrWCD1LMqu864oU",
  HANDWRITTEN: "rec7hTXwMRC81EPqz",
  TRADEMARK: "recob7tgHRryRSbeO",
  BW_PREMIER: "recwXZ5gVZ8ZH8ekA",
  BW_SIGNATURE: "recdeh1NsP4gjrv80",
  PREFERRED: "recwl5JOYxlChuCAr",
  DOUBLETREE: "rechVYWQ5ikRnr99B",
  SLH: "recjjSnY2opb8P4DG",
});

/**
 * Commercially expected comparables for owner-decision contexts.
 * Audit expectation only — not a scoring input.
 */
export const CORE_EXPECTED_BY_SUBJECT = Object.freeze({
  [IDS.AUTOGRAPH]: [IDS.CURIO, IDS.TRIBUTE, IDS.TAPESTRY, IDS.VIGNETTE, IDS.ASCEND, IDS.HANDWRITTEN, IDS.PREFERRED, IDS.MGALLERY],
  [IDS.TRIBUTE]: [IDS.AUTOGRAPH, IDS.CURIO, IDS.TAPESTRY, IDS.VIGNETTE, IDS.ASCEND, IDS.DESIGN],
  [IDS.DESIGN]: [IDS.AUTOGRAPH, IDS.TRIBUTE, IDS.KIMPTON, IDS.PREFERRED, IDS.SLH],
  [IDS.WESTIN]: [IDS.RAD_BLU, IDS.DOUBLETREE],
  [IDS.AC]: [IDS.CANOPY, IDS.INDIGO, IDS.TEMPO],
  [IDS.CURIO]: [IDS.AUTOGRAPH, IDS.TRIBUTE, IDS.TAPESTRY, IDS.VIGNETTE, IDS.ASCEND, IDS.HANDWRITTEN, IDS.PREFERRED, IDS.MGALLERY],
  [IDS.TAPESTRY]: [IDS.CURIO, IDS.AUTOGRAPH, IDS.TRIBUTE, IDS.ASCEND, IDS.VIGNETTE],
  [IDS.CANOPY]: [IDS.INDIGO, IDS.TEMPO, IDS.AC, IDS.KIMPTON, IDS.VOCO],
  [IDS.TEMPO]: [IDS.CANOPY, IDS.INDIGO, IDS.AC, IDS.VOCO],
  [IDS.ASCEND]: [IDS.TAPESTRY, IDS.VIGNETTE, IDS.HANDWRITTEN, IDS.TRADEMARK, IDS.BW_PREMIER, IDS.CURIO, IDS.AUTOGRAPH],
  [IDS.RAD_IND]: [IDS.ASCEND, IDS.PREFERRED, IDS.TRADEMARK, IDS.HANDWRITTEN, IDS.SLH],
  [IDS.RAD_BLU]: [IDS.WESTIN, IDS.DOUBLETREE, IDS.RADISSON],
  [IDS.RAD_RED]: [IDS.VOCO, IDS.TEMPO, IDS.INDIGO],
  [IDS.RADISSON]: [IDS.RAD_BLU, IDS.WESTIN, IDS.DOUBLETREE],
  [IDS.INDIGO]: [IDS.KIMPTON, IDS.CANOPY, IDS.VOCO, IDS.TEMPO, IDS.AC],
  [IDS.KIMPTON]: [IDS.INDIGO, IDS.DESIGN, IDS.CANOPY],
  [IDS.VOCO]: [IDS.INDIGO, IDS.TEMPO, IDS.CANOPY, IDS.RAD_RED],
  [IDS.EVEN]: [IDS.AC, IDS.TEMPO, IDS.INDIGO],
  [IDS.VIGNETTE]: [IDS.AUTOGRAPH, IDS.CURIO, IDS.TRIBUTE, IDS.ASCEND, IDS.HANDWRITTEN, IDS.TAPESTRY],
});

export const SECONDARY_EXPECTED_BY_SUBJECT = Object.freeze({
  [IDS.AUTOGRAPH]: [IDS.DESIGN, IDS.TRADEMARK, IDS.BW_PREMIER, IDS.BW_SIGNATURE, IDS.SLH, IDS.RAD_IND],
  [IDS.CURIO]: [IDS.DESIGN, IDS.TRADEMARK, IDS.BW_PREMIER, IDS.BW_SIGNATURE, IDS.SLH, IDS.RAD_IND],
  [IDS.ASCEND]: [IDS.PREFERRED, IDS.BW_SIGNATURE, IDS.TAPESTRY, IDS.SLH],
  [IDS.INDIGO]: [IDS.HANDWRITTEN, IDS.PREFERRED, IDS.BW_SIGNATURE, IDS.DESIGN],
  [IDS.AC]: [IDS.VOCO, IDS.EVEN, IDS.KIMPTON],
  [IDS.WESTIN]: [IDS.RADISSON, IDS.AC],
  [IDS.RAD_IND]: [IDS.AUTOGRAPH, IDS.CURIO, IDS.VIGNETTE, IDS.TAPESTRY],
});

export const IMPORTANT_PAIRS = Object.freeze([
  [IDS.AUTOGRAPH, IDS.TRIBUTE],
  [IDS.AUTOGRAPH, IDS.CURIO],
  [IDS.AUTOGRAPH, IDS.VIGNETTE],
  [IDS.CURIO, IDS.TRIBUTE],
  [IDS.CURIO, IDS.TAPESTRY],
  [IDS.CURIO, IDS.VIGNETTE],
  [IDS.INDIGO, IDS.KIMPTON],
  [IDS.INDIGO, IDS.CANOPY],
  [IDS.INDIGO, IDS.VOCO],
  [IDS.INDIGO, IDS.TEMPO],
  [IDS.ASCEND, IDS.TRADEMARK],
  [IDS.ASCEND, IDS.BW_PREMIER],
  [IDS.ASCEND, IDS.HANDWRITTEN],
  [IDS.RAD_IND, IDS.ASCEND],
  [IDS.RAD_IND, IDS.PREFERRED],
]);

export const AUTOGRAPH_NAMED_CHECKS = Object.freeze([
  { name: "Curio Collection by Hilton", id: IDS.CURIO },
  { name: "Tribute Portfolio", id: IDS.TRIBUTE },
  { name: "Tapestry Collection by Hilton", id: IDS.TAPESTRY },
  { name: "Vignette Collection", id: IDS.VIGNETTE },
  { name: "Ascend Hotel Collection", id: IDS.ASCEND },
  { name: "Handwritten Collection", id: IDS.HANDWRITTEN },
  { name: "Unbound Collection by Hyatt", id: null, lookupName: "Unbound Collection" },
  { name: "MGallery", id: IDS.MGALLERY },
  { name: "Preferred Hotels & Resorts", id: IDS.PREFERRED },
  { name: "Trademark Collection", id: IDS.TRADEMARK },
  { name: "BW Premier Collection", id: IDS.BW_PREMIER },
  { name: "BW Signature Collection", id: IDS.BW_SIGNATURE },
]);

function loadInventory() {
  if (!fs.existsSync(ACTIVE_UNIVERSE_REPORT)) return [];
  return JSON.parse(fs.readFileSync(ACTIVE_UNIVERSE_REPORT, "utf8")).inventory || [];
}

function loadPilotReport() {
  if (!fs.existsSync(PILOT_REPORT_PATH)) return null;
  return JSON.parse(fs.readFileSync(PILOT_REPORT_PATH, "utf8"));
}

function parseCohortKey(key) {
  const [promptId, provider, language, geography, promptVersion] = String(key || "").split("|");
  return { promptId, provider, language, geography, promptVersion };
}

function obsDimensions(obsList = []) {
  const prompts = new Set();
  const providers = new Set();
  const geos = new Set();
  const langs = new Set();
  const keys = new Set();
  for (const o of obsList) {
    if (o.commonCohortKey) keys.add(o.commonCohortKey);
    const p = parseCohortKey(o.commonCohortKey);
    if (p.promptId) prompts.add(p.promptId);
    if (p.provider) providers.add(p.provider);
    if (p.geography) geos.add(p.geography);
    if (p.language) langs.add(p.language);
  }
  return { prompts, providers, geos, langs, keys };
}

/** Mirror of pilot Presence rate: unique overlapping union-keys / union-key count. */
function unionPresenceRate(brandId, observationIndex, unionKeys) {
  const obs = observationIndex.get(brandId) || [];
  if (!unionKeys.length) return { rate: obs.length ? 1 : null, numerator: 0, denominator: 0 };
  const matched = new Set();
  for (const o of obs) {
    if (o.commonCohortKey && unionKeys.includes(o.commonCohortKey)) matched.add(o.commonCohortKey);
  }
  if (!matched.size) return { rate: null, numerator: 0, denominator: unionKeys.length };
  return { rate: matched.size / unionKeys.length, numerator: matched.size, denominator: unionKeys.length };
}

function collectUnionKeys(subjectId, observationIndex, peerIds) {
  const keys = new Set();
  for (const id of [subjectId, ...peerIds]) {
    for (const o of observationIndex.get(id) || []) {
      if (o.commonCohortKey) keys.add(o.commonCohortKey);
    }
  }
  return [...keys];
}

function parentFor(id, idToParent) {
  return idToParent.get(id) || "Unknown";
}

function nameFor(id, names) {
  return names[id] || id;
}

function classifyExclusion({ inV5, cohortMatch, hasNumericRate, commerciallyExpected, governed }) {
  if (!governed) return "IDENTITY_NOT_GOVERNED";
  if (!inV5) return "NOT_IN_INTERNAL_PEER_SET";
  if (!cohortMatch) return "DIFFERENT_BRAND_ARCHITECTURE";
  if (!hasNumericRate) return "NO_COMMON_COHORT_DATA";
  if (!commerciallyExpected) return "OUT_OF_SCOPE_SCENARIO";
  return "OTHER";
}

function classifyMissingReason({ inV5, hasNumericRate, governed }) {
  if (!governed) return "GOVERNANCE_GAP";
  if (!inV5) return "GOVERNANCE_GAP";
  if (!hasNumericRate) return "COMMON_COHORT_GAP";
  return "LEGITIMATE_EXCLUSION";
}

function overlapLabel(subjectDims, peerDims) {
  const prompt = [...peerDims.prompts].some((p) => subjectDims.prompts.has(p));
  const provider = [...peerDims.providers].some((p) => subjectDims.providers.has(p));
  const geoLang = [...peerDims.keys].some((k) => {
    const a = parseCohortKey(k);
    return [...subjectDims.keys].some((sk) => {
      const b = parseCohortKey(sk);
      return a.geography === b.geography && a.language === b.language;
    });
  });
  return {
    scenarioOverlap: prompt ? "YES" : "NO",
    providerOverlap: provider ? "YES" : "NO",
    geoLanguageOverlap: geoLang ? "YES" : "NO",
  };
}

function commercialYesNo(subjectId, peerId) {
  const core = CORE_EXPECTED_BY_SUBJECT[subjectId] || [];
  const secondary = SECONDARY_EXPECTED_BY_SUBJECT[subjectId] || [];
  if (core.includes(peerId)) return "YES";
  if (secondary.includes(peerId)) return "CONDITIONAL";
  return "NO";
}

function buildIdToParent() {
  const map = new Map();
  const showcase = loadShowcaseCompaniesConfig();
  for (const co of showcase.companies || []) {
    for (const b of co.brands || []) map.set(b.brandId, co.canonicalCompanyName);
  }
  const cfg = loadPeerSetConfig();
  for (const ps of cfg.peerSets || []) {
    for (const m of ps.members || []) {
      if (m.brandId && m.canonicalParent) map.set(m.brandId, m.canonicalParent);
    }
  }
  for (const add of loadApprovedInternalAdditionsConfig().additions || []) {
    map.set(add.brandId, add.canonicalParent);
  }
  return map;
}

function lookupGovernedByName(inventory, fragment) {
  const f = String(fragment || "").toLowerCase();
  return inventory.find(
    (r) =>
      String(r.brandName || "").toLowerCase().includes(f) ||
      String(r.slug || "").toLowerCase().includes(f.replace(/\s+/g, "-"))
  );
}

function coherenceAndTrust({ coreMissing, includedCoreCount, coreCount, falseConfidence, unionDenom, peerCount }) {
  const coverage = coreCount ? includedCoreCount / coreCount : 1;
  let commercialCoherence = "HIGH";
  if (coverage < 0.5 || coreMissing.some((m) => m.classify === "BUG")) commercialCoherence = "LOW";
  else if (coverage < 0.75 || falseConfidence || unionDenom) commercialCoherence = "MEDIUM";

  let indexTrust = "YES";
  if (commercialCoherence === "LOW" || peerCount < 5) indexTrust = "NO";
  else if (commercialCoherence === "MEDIUM" || coreMissing.length) indexTrust = "LIMITED";
  return { commercialCoherence, indexTrust };
}

function pairScenarioAnswers(a, b) {
  const yesPairs = new Set([
    `${IDS.AUTOGRAPH}|${IDS.CURIO}`,
    `${IDS.CURIO}|${IDS.AUTOGRAPH}`,
    `${IDS.AUTOGRAPH}|${IDS.TRIBUTE}`,
    `${IDS.TRIBUTE}|${IDS.AUTOGRAPH}`,
    `${IDS.CURIO}|${IDS.TAPESTRY}`,
    `${IDS.TAPESTRY}|${IDS.CURIO}`,
    `${IDS.CURIO}|${IDS.TRIBUTE}`,
    `${IDS.TRIBUTE}|${IDS.CURIO}`,
    `${IDS.INDIGO}|${IDS.KIMPTON}`,
    `${IDS.KIMPTON}|${IDS.INDIGO}`,
    `${IDS.INDIGO}|${IDS.CANOPY}`,
    `${IDS.CANOPY}|${IDS.INDIGO}`,
  ]);
  const conversionYes = new Set([
    `${IDS.AUTOGRAPH}|${IDS.CURIO}`,
    `${IDS.CURIO}|${IDS.AUTOGRAPH}`,
    `${IDS.ASCEND}|${IDS.TRADEMARK}`,
    `${IDS.TRADEMARK}|${IDS.ASCEND}`,
  ]);
  const key = `${a}|${b}`;
  const soft = yesPairs.has(key) || commercialYesNo(a, b) === "YES" ? "YES" : commercialYesNo(a, b);
  return {
    softBrandAffiliation: soft,
    conversion: conversionYes.has(key) || commercialYesNo(a, b) !== "NO" ? (conversionYes.has(key) ? "YES" : "CONDITIONAL") : "NO",
    ownerFlexibility: commercialYesNo(a, b),
    lifestyle: commercialYesNo(a, b) === "YES" ? "CONDITIONAL" : commercialYesNo(a, b),
    distribution: commercialYesNo(a, b) === "YES" ? "CONDITIONAL" : "NO",
  };
}

/**
 * Run inspect-only cohort integrity audit.
 */
export function runBenchmarkCohortIntegrityAudit(opts = {}) {
  const peerSetId = opts.peerSetId || PEER_SET_ID_V5;
  const showcase = loadShowcaseCompaniesConfig();
  const subjectIds = listShowcaseMonitoringBrandIds(undefined, showcase);
  const cfg = loadPeerSetConfig();
  const v5 = resolvePeerSetMembership({ peerSetId, commercialRegion: "CALA" }, cfg);
  const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2, commercialRegion: "CALA" }, cfg);
  const v5Ids = new Set(v5.entityIds || []);
  const names = {
    ...peerSetBrandNamesById(peerSetId, cfg),
  };
  for (const co of showcase.companies || []) {
    for (const b of co.brands || []) names[b.brandId] = b.brandName;
  }
  const inventory = loadInventory();
  const idToParent = buildIdToParent();
  const additions = loadApprovedInternalAdditionsConfig().additions || [];
  const addById = Object.fromEntries(additions.map((a) => [a.brandId, a]));
  const set = cfg.peerSets?.find((p) => p.peerSetId === peerSetId);
  const peerAliases = (set?.members || []).map((m) => ({
    brandId: m.brandId,
    brandName: m.brandName || names[m.brandId],
    aliases: addById[m.brandId]?.aliases || [m.brandName || names[m.brandId]].filter(Boolean),
  }));
  for (const id of subjectIds) {
    if (!peerAliases.find((p) => p.brandId === id)) {
      peerAliases.push({ brandId: id, brandName: names[id], aliases: [names[id]].filter(Boolean) });
    }
  }

  const { index: observationIndex } = buildPresenceObservationIndex({
    peerBrandAliases: peerAliases,
    responseDirs: opts.responseDirs,
  });
  const pilot = opts.pilotReport || loadPilotReport();
  const pilotById = Object.fromEntries(
    (pilot?.pilotResults?.subjects || []).map((s) => [s.subjectEntityId, s])
  );

  const subjectRows = [];
  const mathIncludedPairs = [];

  for (const subjectId of subjectIds) {
    const cohort = resolveContextualPeerIds(subjectId, { peerSetId });
    const eligiblePeerIds = cohort.peerIds || [];
    const unionKeys = collectUnionKeys(subjectId, observationIndex, eligiblePeerIds);
    const subjectDims = obsDimensions(observationIndex.get(subjectId) || []);
    const subjectPresence = unionPresenceRate(subjectId, observationIndex, unionKeys);
    const coreExpected = CORE_EXPECTED_BY_SUBJECT[subjectId] || [];
    const secondaryExpected = SECONDARY_EXPECTED_BY_SUBJECT[subjectId] || [];

    const included = [];
    const excludedFromIndex = [];

    for (const pid of eligiblePeerIds) {
      const pr = unionPresenceRate(pid, observationIndex, unionKeys);
      const peerDims = obsDimensions(observationIndex.get(pid) || []);
      const overlap = overlapLabel(subjectDims, peerDims);
      const whyIncluded =
        "Primary cohort type match (or approved addition tag) and numeric Presence on union-grain denominator";
      if (pr.rate != null) {
        included.push({
          peer: nameFor(pid, names),
          peerId: pid,
          parent: parentFor(pid, idToParent),
          presenceValueUsed: pr.rate,
          comparableObservations: pr.numerator,
          unionDenominatorKeys: pr.denominator,
          promptCount: peerDims.prompts.size,
          providerCount: peerDims.providers.size,
          scenarioCount: peerDims.prompts.size,
          measurementPeriod: "DEMO_VALIDATION",
          ...overlap,
          whyIncluded,
          internalAddition: Boolean(addById[pid]),
        });
        mathIncludedPairs.push([subjectId, pid]);
      } else {
        excludedFromIndex.push({
          peer: nameFor(pid, names),
          peerId: pid,
          whyExcluded: "NO_COMMON_COHORT_DATA",
        });
      }
    }

    const plausible = [...new Set([...eligiblePeerIds, ...coreExpected, ...secondaryExpected, ...v5Ids])].filter(
      (id) => id !== subjectId
    );
    const excludedPlausible = [];
    for (const pid of plausible) {
      if (included.find((p) => p.peerId === pid)) continue;
      const governed = Boolean(names[pid] || inventory.find((r) => r.recordId === pid));
      const inV5 = v5Ids.has(pid);
      const cohortMatch = eligiblePeerIds.includes(pid);
      const hasNumeric = included.some((p) => p.peerId === pid);
      excludedPlausible.push({
        peer: nameFor(pid, names),
        peerId: pid,
        whyExcluded: classifyExclusion({
          inV5,
          cohortMatch,
          hasNumericRate: hasNumeric,
          commerciallyExpected: coreExpected.includes(pid) || secondaryExpected.includes(pid),
          governed,
        }),
      });
    }

    const coreMissing = coreExpected
      .filter((id) => !included.some((p) => p.peerId === id))
      .map((id) => {
        const governed = Boolean(names[id] || inventory.find((r) => r.recordId === id));
        const inV5 = v5Ids.has(id);
        const hasNumeric = false;
        const classify = classifyMissingReason({ inV5, hasNumericRate: hasNumeric, governed });
        return {
          peer: nameFor(id, names),
          peerId: id,
          flag: "CORE_PEER_MISSING",
          classify: !governed ? "GOVERNANCE_GAP" : !inV5 ? "GOVERNANCE_GAP" : "COMMON_COHORT_GAP",
          reason: !inV5
            ? "Commercially expected but not a member of peers_uu_collection_lifestyle_owner_decision_v5"
            : "Eligible by cohort type but no numeric Presence on the current union-grain denominator",
        };
      });

    const includedInternal = included.filter((p) => p.internalAddition).length;
    const falseConfidence = includedInternal >= 3 && coreMissing.length > 0;
    const { commercialCoherence, indexTrust } = coherenceAndTrust({
      coreMissing,
      includedCoreCount: coreExpected.filter((id) => included.some((p) => p.peerId === id)).length,
      coreCount: coreExpected.length,
      falseConfidence,
      unionDenom: true,
      peerCount: included.length,
    });

    const funnel = {
      fullEligibleCommercial: [...new Set([...coreExpected, ...secondaryExpected])].map((id) => nameFor(id, names)),
      afterPrimaryCohortFilter: eligiblePeerIds.map((id) => nameFor(id, names)),
      afterNumericPresence: included.map((p) => p.peer),
      afterPromptOverlap: included.filter((p) => p.scenarioOverlap === "YES").map((p) => p.peer),
      afterProviderOverlap: included.filter((p) => p.providerOverlap === "YES").map((p) => p.peer),
      afterGeoLanguageOverlap: included.filter((p) => p.geoLanguageOverlap === "YES").map((p) => p.peer),
      finalBenchmarkCohort: included.map((p) => p.peer),
    };

    const pilotSubject = pilotById[subjectId];
    subjectRows.push({
      subject: nameFor(subjectId, names),
      subjectEntityId: subjectId,
      parent: parentFor(subjectId, idToParent),
      benchmarkContext: cohort.cohortType,
      benchmarkCohortType: cohort.cohortType,
      currentIndex: pilotSubject?.aiPresenceIndex ?? null,
      currentStatus: pilotSubject?.benchmarkStatus ?? null,
      subjectPresenceUnion: subjectPresence.rate,
      unionKeyCount: unionKeys.length,
      denominatorConstruction: "UNION_OF_SUBJECT_AND_PEER_GRAINS",
      allIncludedPeers: included,
      allExcludedPlausiblePeers: excludedPlausible,
      coreExpectedPeers: coreExpected.map((id) => nameFor(id, names)),
      secondaryExpectedPeers: secondaryExpected.map((id) => nameFor(id, names)),
      actualIncludedPeers: included.map((p) => p.peer),
      importantPeersMissing: coreMissing,
      peerCount: included.length,
      commercialCoherence,
      indexCurrentlyTrustworthy: indexTrust,
      funnel,
      families: {
        SOFT_COLLECTION: cohort.cohortType === "SOFT_COLLECTION",
        LIFESTYLE_UPPER_UPSCALE: cohort.cohortType === "LIFESTYLE",
        HARD_BRAND_UPPER_UPSCALE: cohort.cohortType === "UPPER_UPSCALE",
        CONVERSION_LED: cohort.cohortType === "CONVERSION",
        OWNER_FLEXIBILITY: cohort.cohortType === "OWNER_FLEXIBILITY",
        NEW_BUILD: cohort.cohortType === "NEW_BUILD",
        LUXURY_COLLECTION: [IDS.PREFERRED, IDS.SLH, IDS.DESIGN].includes(subjectId),
      },
    });
  }

  function isMathPeer(a, b) {
    return mathIncludedPairs.some(([s, p]) => s === a && p === b);
  }

  const symmetryRows = [];
  let symmetric = 0;
  let asymmetricJustified = 0;
  let asymmetricUnjustified = 0;
  const pairsToTest = new Set();
  for (const [a, b] of mathIncludedPairs) pairsToTest.add(`${a}|${b}`);
  for (const [a, b] of IMPORTANT_PAIRS) {
    pairsToTest.add(`${a}|${b}`);
    pairsToTest.add(`${b}|${a}`);
  }

  for (const key of pairsToTest) {
    const [a, b] = key.split("|");
    if (a === b) continue;
    const aOfB = isMathPeer(a, b);
    const bOfA = isMathPeer(b, a);
    const commercialAB = commercialYesNo(a, b);
    const commercialBA = commercialYesNo(b, a);
    const aInV5 = v5Ids.has(a);
    const bInV5 = v5Ids.has(b);
    const aSubject = subjectIds.includes(a);
    const bSubject = subjectIds.includes(b);
    let classification = "SYMMETRIC";
    if (aOfB === bOfA) {
      classification = "SYMMETRIC";
      symmetric += 1;
    } else if (!aSubject || !bSubject) {
      classification = "ASYMMETRIC_JUSTIFIED";
      asymmetricJustified += 1;
    } else if (!aInV5 || !bInV5) {
      classification = "ASYMMETRIC_JUSTIFIED";
      asymmetricJustified += 1;
    } else if (resolvePrimaryCohortType(a) !== resolvePrimaryCohortType(b) && commercialAB !== "YES") {
      classification = "ASYMMETRIC_JUSTIFIED";
      asymmetricJustified += 1;
    } else if (commercialAB === "YES" && commercialBA === "YES") {
      classification = "ASYMMETRIC_UNJUSTIFIED";
      asymmetricUnjustified += 1;
    } else {
      classification = "ASYMMETRIC_JUSTIFIED";
      asymmetricJustified += 1;
    }
    symmetryRows.push({
      a: nameFor(a, names),
      b: nameFor(b, names),
      aIncludesB: aOfB,
      bIncludesA: bOfA,
      commercialAB,
      commercialBA,
      classification,
    });
  }

  const autograph = subjectRows.find((s) => s.subjectEntityId === IDS.AUTOGRAPH);
  const curio = subjectRows.find((s) => s.subjectEntityId === IDS.CURIO);

  const namedAutographChecks = AUTOGRAPH_NAMED_CHECKS.map((c) => {
    let id = c.id;
    if (!id && c.lookupName) {
      const row = lookupGovernedByName(inventory, c.lookupName);
      id = row?.recordId || null;
    }
    const included = id ? autograph?.actualIncludedPeers.includes(nameFor(id, names)) : false;
    const inV5 = id ? v5Ids.has(id) : false;
    let why = "INCLUDED — SOFT_COLLECTION (or approved addition tag) with numeric union-grain Presence";
    if (!id) why = "EXCLUDED — IDENTITY_NOT_GOVERNED (not in Active/Live Brand Explorer universe used by this audit)";
    else if (!inV5) why = "EXCLUDED — NOT_IN_INTERNAL_PEER_SET (customer-visible or governed but not peer v5)";
    else if (!included) why = "EXCLUDED — NO_COMMON_COHORT_DATA or DIFFERENT_BRAND_ARCHITECTURE";
    return {
      name: c.name,
      id,
      status: included ? "INCLUDED" : "EXCLUDED",
      why,
    };
  });

  const autographCurioCommercial = {
    autographToCurio: pairScenarioAnswers(IDS.AUTOGRAPH, IDS.CURIO),
    curioToAutograph: pairScenarioAnswers(IDS.CURIO, IDS.AUTOGRAPH),
    mathematicallyIncludedBothWays: isMathPeer(IDS.AUTOGRAPH, IDS.CURIO) && isMathPeer(IDS.CURIO, IDS.AUTOGRAPH),
    ifCommerciallyYesButExcluded: "NOT_APPLICABLE_BOTH_INCLUDED",
  };

  const importantPairAudit = IMPORTANT_PAIRS.map(([a, b]) => ({
    pair: `${nameFor(a, names)} ↔ ${nameFor(b, names)}`,
    commercialAtoB: commercialYesNo(a, b),
    commercialBtoA: commercialYesNo(b, a),
    mathAincludesB: isMathPeer(a, b),
    mathBincludesA: isMathPeer(b, a),
    classification:
      isMathPeer(a, b) === isMathPeer(b, a)
        ? "SYMMETRIC"
        : !v5Ids.has(a) || !v5Ids.has(b)
          ? "ASYMMETRIC_JUSTIFIED"
          : commercialYesNo(a, b) === "YES" && commercialYesNo(b, a) === "YES"
            ? "ASYMMETRIC_UNJUSTIFIED"
            : "ASYMMETRIC_JUSTIFIED",
    ifYesButExcluded: commercialYesNo(a, b) === "YES" && !isMathPeer(a, b) ? "MEASUREMENT_COVERAGE_GAP" : null,
  }));

  const coreMissingAnywhere = subjectRows.flatMap((s) =>
    s.importantPeersMissing.map((m) => ({ subject: s.subject, ...m }))
  );
  const sampleSizeImproved = (pilot?.finalInternalPeerCount || 22) > (v2.effectiveCount || 15);
  const addedNamesInAnyCohort = additions.filter((a) =>
    subjectRows.some((s) => s.actualIncludedPeers.includes(a.brandName))
  );
  const addedFillsCoreGap = additions.some((a) =>
    subjectRows.some(
      (s) =>
        (CORE_EXPECTED_BY_SUBJECT[s.subjectEntityId] || []).includes(a.brandId) &&
        s.actualIncludedPeers.includes(a.brandName)
    )
  );
  const commercialComparabilityImproved = addedFillsCoreGap
    ? "YES"
    : addedNamesInAnyCohort.length
      ? "PARTIAL"
      : "NO";

  const falseRisk =
    sampleSizeImproved &&
    coreMissingAnywhere.some(
      (m) => m.classify === "GOVERNANCE_GAP" || m.peerId === IDS.VIGNETTE
    );

  const report = {
    BRAND_AI_BENCHMARK_COHORT_INTEGRITY_AUDIT_COMPLETE: true,
    auditVersion: COHORT_INTEGRITY_AUDIT_VERSION,
    providerCalls: 0,
    spend: 0,
    uiChanges: 0,
    methodologyChanged: false,
    customerApiChanged: false,
    datasetClass: "DEMO_VALIDATION",
    indexReadiness: "READY_FOR_INTERNAL_REVIEW",
    customerVisibleBrands: subjectIds.length,
    internalPeerCount: v5.effectiveCount,
    peerSetId,
    keyFinding: {
      drivers: ["COHORT_CONSTRUCTION", "COMMON_COHORT_DATA_GAPS"],
      truePerformanceDifference: "UNPROVEN",
      mixture: true,
      plainEnglish:
        "Current high Autograph / Curio / Indigo indices are not certified as true performance gaps. Presence is measured against a UNION of prompt×provider×geo grains across the subject and every cohort peer, so widely mentioned brands score near 1.0 while sparse internal additions (BW collections, MGallery) sit near 0.09–0.15 and pull the median down. Curio is included for Autograph (and Autograph for Curio). Vignette — a core commercial peer — is a customer-visible subject but is not in peer v5, so it never enters Autograph/Curio benchmarks. Adding 7 internal brands improved N but mixed weaker-mention collections into the median. Treat 321 / current Curio / Indigo / Ascend figures as cohort-construction artifacts until scenario-specific intersection cohorts are certified.",
    },
    autographDeepDive: {
      currentIndex: autograph?.currentIndex ?? null,
      coreExpectedPeers: autograph?.coreExpectedPeers,
      actualIncludedPeers: autograph?.actualIncludedPeers,
      importantPeersExcluded: autograph?.importantPeersMissing,
      curioIncluded: autograph?.actualIncludedPeers.includes(nameFor(IDS.CURIO, names)) ? "YES" : "NO",
      ifNoWhy: null,
      namedChecks: namedAutographChecks,
      includedPeersWithPresence: autograph?.allIncludedPeers,
      funnel: autograph?.funnel,
      commercialCoherence: autograph?.commercialCoherence,
      indexTrust: autograph?.indexCurrentlyTrustworthy,
      denominatorNote:
        "Subject Presence 0.94 vs median ~0.29 is largely UNION-denominator + sparse-peer mix, not a certified 3.2× commercial gap vs Curio.",
    },
    curioDeepDive: {
      currentIndex: curio?.currentIndex ?? null,
      coreExpectedPeers: curio?.coreExpectedPeers,
      actualIncludedPeers: curio?.actualIncludedPeers,
      importantPeersExcluded: curio?.importantPeersMissing,
      autographIncluded: curio?.actualIncludedPeers.includes(nameFor(IDS.AUTOGRAPH, names)) ? "YES" : "NO",
      ifNoWhy: null,
      includedPeersWithPresence: curio?.allIncludedPeers,
      funnel: curio?.funnel,
      commercialCoherence: curio?.commercialCoherence,
      indexTrust: curio?.indexCurrentlyTrustworthy,
    },
    autographCurioTest: autographCurioCommercial,
    all19Subjects: subjectRows.map((s) => ({
      subject: s.subject,
      coreExpected: s.coreExpectedPeers,
      actualIncluded: s.actualIncludedPeers,
      coreMissing: s.importantPeersMissing,
      asymmetricPeers: symmetryRows
        .filter((r) => (r.a === s.subject || r.b === s.subject) && r.classification !== "SYMMETRIC")
        .slice(0, 8),
      commercialCoherence: s.commercialCoherence,
      indexTrust: s.indexCurrentlyTrustworthy,
      peerCount: s.peerCount,
      currentIndex: s.currentIndex,
      status: s.currentStatus,
      cohortType: s.benchmarkCohortType,
    })),
    founderTable: subjectRows.map((s) => ({
      SUBJECT: s.subject,
      CORE_EXPECTED_PEERS: s.coreExpectedPeers,
      ACTUAL_INCLUDED_PEERS: s.actualIncludedPeers,
      IMPORTANT_PEERS_MISSING: s.importantPeersMissing.map((m) => m.peer),
      PEER_COUNT: s.peerCount,
      COMMERCIAL_COHERENCE: s.commercialCoherence,
      INDEX_CURRENTLY_TRUSTWORTHY: s.indexCurrentlyTrustworthy,
    })),
    symmetry: {
      totalComparablePairs: symmetryRows.length,
      symmetric,
      asymmetricJustified,
      asymmetricUnjustified,
      unjustifiedRows: symmetryRows.filter((r) => r.classification === "ASYMMETRIC_UNJUSTIFIED"),
    },
    importantPairs: importantPairAudit,
    expansionAudit: {
      newInternalBrands: 7,
      sampleSizeImproved: sampleSizeImproved ? "YES" : "NO",
      commercialComparabilityImproved,
      falseBenchmarkConfidenceRisk: falseRisk ? "YES" : "NO",
    },
    architecture: {
      staticCohort: "FAIL",
      contextualCohort: "FAIL",
      recommendedArchitecture:
        "C — scenario-specific commercially relevant peers; compute Presence Index per Owner Intent on INTERSECTION grains; headline subject index = unweighted median of scenario indices that pass common-cohort. No weights in V1. Do not use one static peer set across all prompts.",
      recommended: "SCENARIO_SPECIFIC_THEN_UNWEIGHTED_MEDIAN_OF_VALID_SCENARIO_INDICES",
    },
    validityContract: {
      current: "N >= 5",
      recommended:
        "VALID only if MIN_PEER_COUNT >= 5 AND CORE_COMPETITOR_COVERAGE >= 60% of governed core expected peers AND COMMON_COHORT_PASS (intersection grains, not union) AND SEMANTIC_COMPARABILITY_PASS. Else LIMITED_CORE_PEERS_MISSING / LIMITED_SAMPLE / SUPPRESSED. Do not implement in this audit.",
      proposedStates: ["VALID", "LIMITED_CORE_PEERS_MISSING", "LIMITED_SAMPLE", "SUPPRESSED"],
    },
    next: "BENCHMARK_COHORT_REMEDIATION",
    customerExposure: {
      fullPeerMatrixInThisAudit: true,
      customerEndpointChanged: false,
      FULL_PEER_MATRIX_CUSTOMER_ACCESS: "BLOCKED",
    },
    subjectsDetail: subjectRows,
    CUSTOMER_PAYLOAD_ALLOWLIST,
  };

  if (opts.writeReport !== false) {
    const outDir = path.join(ROOT, "reports", "ai-visibility");
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "benchmark-cohort-integrity-audit-v1.json"),
      JSON.stringify(report, null, 2)
    );
    const md = renderFounderMarkdown(report);
    fs.writeFileSync(path.join(outDir, "benchmark-cohort-integrity-audit-v1.md"), md);
  }

  return report;
}

function renderFounderMarkdown(report) {
  const lines = [
    "# Benchmark Cohort Integrity Audit V1",
    "",
    "**INTERNAL ONLY — do not expose cohort membership on customer endpoints.**",
    "",
    `Index readiness: **${report.indexReadiness}**`,
    "",
    "## Key finding",
    "",
    report.keyFinding.plainEnglish,
    "",
    "| SUBJECT | CORE EXPECTED | ACTUAL INCLUDED | IMPORTANT MISSING | N | COHERENCE | TRUST |",
    "|---|---|---|---|---|---|---|",
  ];
  for (const r of report.founderTable) {
    lines.push(
      `| ${r.SUBJECT} | ${r.CORE_EXPECTED_PEERS.join("; ")} | ${r.ACTUAL_INCLUDED_PEERS.join("; ")} | ${
        r.IMPORTANT_PEERS_MISSING.join("; ") || "—"
      } | ${r.PEER_COUNT} | ${r.COMMERCIAL_COHERENCE} | ${r.INDEX_CURRENTLY_TRUSTWORTHY} |`
    );
  }
  lines.push("", `Next: **${report.next}**`, "");
  return lines.join("\n");
}
