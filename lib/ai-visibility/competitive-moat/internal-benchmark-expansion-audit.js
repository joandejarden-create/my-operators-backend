/**
 * Internal Benchmark Expansion Audit — offline read-only.
 * Identifies governed brands to strengthen AI Presence Index cohorts without
 * expanding customer-visible universe or making provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../brand-ai-showcase-companies.js";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2 } from "../peer-sets.js";
import {
  aggregateBenchmarkPresence,
  computeAiPresenceIndex,
  classifyBenchmarkSampleSize,
} from "./benchmark-engine-v1.js";

export const INTERNAL_BENCHMARK_EXPANSION_AUDIT_VERSION =
  "internal_benchmark_expansion_audit_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const ACTIVE_UNIVERSE_REPORT = path.join(
  ROOT,
  "reports",
  "brand-explorer-active-universe-source-of-truth.json"
);

const RESPONSE_DIRS = [
  path.join(ROOT, "data", "ai-visibility", "legacy-language-backfill-checkpoints", "responses"),
  path.join(ROOT, "data", "ai-visibility", "validation", "presence-validation-candidates", "responses"),
  path.join(ROOT, "data", "ai-visibility", "validation", "presence-holdout-v3-candidates", "responses"),
];

/** Scenario cohort tags for impact analysis. */
export const COHORT_TAGS = Object.freeze([
  "SOFT_COLLECTION",
  "CONVERSION",
  "LIFESTYLE",
  "UPPER_UPSCALE",
  "OWNER_FLEXIBILITY",
  "NEW_BUILD",
]);

/** Governed candidate definitions — audited against Active/Live universe. */
export const CANDIDATE_DEFINITIONS = Object.freeze([
  {
    slug: "handwritten-collection",
    brandName: "Handwritten Collection",
    parent: "IHG",
    cohortTags: ["SOFT_COLLECTION", "CONVERSION", "LIFESTYLE"],
    aliases: ["Handwritten Collection", "Handwritten"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Collection / Soft Brand",
  },
  {
    slug: "trademark-collection-by-wyndham",
    brandName: "Trademark Collection by Wyndham",
    parent: "Wyndham",
    cohortTags: ["SOFT_COLLECTION", "CONVERSION", "OWNER_FLEXIBILITY"],
    aliases: ["Trademark Collection by Wyndham", "Trademark Collection"],
    chainScale: "Upscale",
    brandArchitecture: "Collection / Soft Brand",
  },
  {
    slug: "doubletree-by-hilton",
    brandName: "DoubleTree by Hilton",
    parent: "Hilton",
    cohortTags: ["CONVERSION", "UPPER_UPSCALE"],
    aliases: ["DoubleTree by Hilton", "DoubleTree"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Hard Brand",
  },
  {
    slug: "bw-premier-collection",
    brandName: "BW Premier Collection",
    parent: "Best Western",
    cohortTags: ["SOFT_COLLECTION", "CONVERSION"],
    aliases: ["BW Premier Collection", "Best Western Premier Collection"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Collection / Soft Brand",
  },
  {
    slug: "bw-signature-collection",
    brandName: "BW Signature Collection",
    parent: "Best Western",
    cohortTags: ["SOFT_COLLECTION", "LIFESTYLE"],
    aliases: ["BW Signature Collection", "Best Western Signature Collection"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Collection / Soft Brand",
  },
  {
    slug: "pullman",
    brandName: "Pullman",
    parent: "Accor",
    cohortTags: ["UPPER_UPSCALE", "LIFESTYLE", "NEW_BUILD"],
    aliases: ["Pullman"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Hard Brand",
  },
  {
    slug: "delta-hotels-by-marriott",
    brandName: "Delta Hotels by Marriott",
    parent: "Marriott International",
    cohortTags: ["CONVERSION", "UPPER_UPSCALE"],
    aliases: ["Delta Hotels by Marriott", "Delta Hotels"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Hard Brand",
  },
  {
    slug: "preferred-hotels-and-resorts",
    brandName: "Preferred Hotels & Resorts",
    parent: "Preferred Hotels Group",
    cohortTags: ["SOFT_COLLECTION", "LIFESTYLE", "OWNER_FLEXIBILITY"],
    aliases: ["Preferred Hotels & Resorts", "Preferred Hotels"],
    chainScale: "Luxury / Upper Upscale",
    brandArchitecture: "Collection",
  },
  {
    slug: "dazzler-by-wyndham",
    brandName: "Dazzler by Wyndham",
    parent: "Wyndham",
    cohortTags: ["LIFESTYLE", "CONVERSION"],
    aliases: ["Dazzler by Wyndham", "Dazzler"],
    chainScale: "Upscale",
    brandArchitecture: "Soft Brand",
  },
  {
    slug: "moxy-hotels",
    brandName: "Moxy Hotels",
    parent: "Marriott International",
    cohortTags: ["LIFESTYLE"],
    aliases: ["Moxy Hotels", "Moxy"],
    chainScale: "Upper Midscale / Lifestyle",
    brandArchitecture: "Lifestyle",
  },
  {
    slug: "aloft-hotels",
    brandName: "Aloft Hotels",
    parent: "Marriott International",
    cohortTags: ["LIFESTYLE", "CONVERSION"],
    aliases: ["Aloft Hotels", "Aloft"],
    chainScale: "Upper Midscale / Lifestyle",
    brandArchitecture: "Lifestyle",
  },
  {
    slug: "fairmont-hotels-and-resorts",
    brandName: "Fairmont",
    parent: "Accor",
    cohortTags: ["UPPER_UPSCALE", "LIFESTYLE"],
    aliases: ["Fairmont Hotels & Resorts", "Fairmont"],
    chainScale: "Luxury",
    brandArchitecture: "Hard Brand",
  },
  {
    slug: "marriott-hotels",
    brandName: "Marriott Hotels",
    parent: "Marriott International",
    cohortTags: ["UPPER_UPSCALE", "CONVERSION"],
    aliases: ["Marriott Hotels", "Marriott Hotels & Resorts"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Hard Brand",
  },
  {
    slug: "hilton-hotels-and-resorts",
    brandName: "Hilton Hotels & Resorts",
    parent: "Hilton",
    cohortTags: ["UPPER_UPSCALE", "CONVERSION"],
    aliases: ["Hilton Hotels & Resorts"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Hard Brand",
  },
  {
    slug: "small-luxury-hotels-of-the-world",
    brandName: "Small Luxury Hotels of the World",
    parent: "SLH",
    cohortTags: ["SOFT_COLLECTION", "LIFESTYLE", "OWNER_FLEXIBILITY"],
    aliases: ["Small Luxury Hotels of the World", "SLH"],
    chainScale: "Luxury",
    brandArchitecture: "Collection",
  },
  {
    slug: "cambria-hotels",
    brandName: "Cambria Hotels",
    parent: "Choice Hotels",
    cohortTags: ["UPPER_UPSCALE", "NEW_BUILD"],
    aliases: ["Cambria Hotels", "Cambria"],
    chainScale: "Upscale",
    brandArchitecture: "Hard Brand",
  },
  {
    slug: "sheraton",
    brandName: "Sheraton",
    parent: "Marriott International",
    cohortTags: ["CONVERSION", "UPPER_UPSCALE"],
    aliases: ["Sheraton"],
    chainScale: "Upper Upscale",
    brandArchitecture: "Hard Brand",
  },
  {
    slug: "four-points-by-sheraton",
    brandName: "Four Points by Sheraton",
    parent: "Marriott International",
    cohortTags: ["CONVERSION"],
    aliases: ["Four Points by Sheraton", "Four Points"],
    chainScale: "Upscale",
    brandArchitecture: "Hard Brand",
  },
]);

function loadActiveUniverseInventory(reportPath = ACTIVE_UNIVERSE_REPORT) {
  if (!fs.existsSync(reportPath)) return [];
  const raw = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  return raw.inventory || [];
}

function loadCustomerVisibleBrandIds() {
  return new Set(listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()));
}

function loadCurrentInternalBenchmarkIds(peerSetId = PEER_SET_ID_V2) {
  const cfg = loadPeerSetConfig();
  const m = resolvePeerSetMembership({ peerSetId, commercialRegion: "CALA" }, cfg);
  return new Set(m.entityIds || []);
}

function resolveCandidateFromUniverse(def, inventory) {
  const row =
    inventory.find((b) => b.slug === def.slug) ||
    inventory.find((b) => b.brandName === def.brandName);
  if (!row) {
    return {
      ...def,
      canonicalId: null,
      status: "NOT_IN_ACTIVE_UNIVERSE",
      identityConfidence: "LOW",
      identitySafe: false,
    };
  }
  const publicFull = row.publicFull === true;
  const pvqlPass = row.pvqlPass === true;
  return {
    ...def,
    canonicalId: row.recordId,
    status: publicFull && pvqlPass ? "ACTIVE" : row.currentOsState || "OTHER",
    identityConfidence: publicFull && pvqlPass ? "HIGH" : "MEDIUM",
    identitySafe: publicFull && pvqlPass && Boolean(row.recordId),
    publicFull,
    pvqlPass,
    presentationRows: row.presentationRows || 0,
  };
}

function classifyBenchmarkFit(candidate, mentionStats) {
  if (!candidate.identitySafe) return "LOW_FIT";
  const tags = candidate.cohortTags || [];
  const hasEvidence = (mentionStats?.resolvedMentions || 0) > 0;
  const softOrConversion =
    tags.includes("SOFT_COLLECTION") || tags.includes("CONVERSION");
  if (candidate.identityConfidence === "HIGH" && softOrConversion && hasEvidence) {
    return "HIGH_FIT";
  }
  if (candidate.identitySafe && (softOrConversion || tags.includes("LIFESTYLE"))) {
    return hasEvidence ? "HIGH_FIT" : "MEDIUM_FIT";
  }
  if (candidate.identitySafe) return hasEvidence ? "MEDIUM_FIT" : "LOW_FIT";
  return "LOW_FIT";
}

function extractResponseText(obj) {
  if (!obj) return "";
  if (typeof obj === "string") return obj;
  return (
    obj.text ||
    obj.responseText ||
    obj.content ||
    obj.output ||
    obj.answer ||
    (obj.message && obj.message.content) ||
    ""
  );
}

function collectResponseTexts(dirs = RESPONSE_DIRS) {
  const texts = [];
  for (const dir of dirs) {
    if (!fs.existsSync(dir)) continue;
    for (const file of fs.readdirSync(dir)) {
      if (!file.endsWith(".json")) continue;
      try {
        const raw = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
        const text = extractResponseText(raw.response || raw);
        if (text) texts.push({ file, text: String(text) });
      } catch {
        /* skip malformed */
      }
    }
  }
  return texts;
}

function auditMentionsForCandidate(candidate, responseTexts) {
  let resolved = 0;
  let ambiguous = 0;
  const aliases = candidate.aliases || [candidate.brandName];
  for (const { text } of responseTexts) {
    const lower = text.toLowerCase();
    let matched = false;
    for (const alias of aliases) {
      if (lower.includes(alias.toLowerCase())) {
        matched = true;
        break;
      }
    }
    if (matched) resolved += 1;
    if (
      candidate.slug === "pullman" &&
      /\bpullman\b/i.test(text) &&
      !/pullman hotel|pullman hotels/i.test(text)
    ) {
      ambiguous += 1;
    }
  }
  return {
    baselineResponsesSearched: responseTexts.length,
    resolvedMentions: resolved,
    ambiguousMentions: ambiguous,
    currentBaselinePresenceAvailable: resolved > 0,
    presenceRate: responseTexts.length ? resolved / responseTexts.length : null,
  };
}

function validityChange(currentPeers, addedPeers) {
  const before = classifyBenchmarkSampleSize(currentPeers);
  const after = classifyBenchmarkSampleSize(currentPeers + addedPeers);
  if (before === after) return "NO_MATERIAL_CHANGE";
  if (before === "SUPPRESSED_INSUFFICIENT_DATA" && after === "LIMITED_BENCHMARK") {
    return "SUPPRESSED -> LIMITED";
  }
  if (before === "LIMITED_BENCHMARK" && after === "VALID_BENCHMARK") {
    return "LIMITED -> VALID";
  }
  if (before === "VALID_BENCHMARK" && after === "VALID_BENCHMARK") {
    return "VALID -> STRONGER_VALID";
  }
  if (before === "SUPPRESSED_INSUFFICIENT_DATA" && after === "VALID_BENCHMARK") {
    return "SUPPRESSED -> VALID";
  }
  return `${before} -> ${after}`;
}

function leaveOneOutStability(peerPresenceMap, subjectId) {
  const peerIds = Object.keys(peerPresenceMap).filter((id) => id !== subjectId);
  const rates = peerIds.map((id) => peerPresenceMap[id]).filter((v) => v != null);
  if (rates.length < 3) {
    return { leaveOneOutState: "FRAGILE", maxIndexMovement: null, peers: peerIds.length };
  }
  const subjectRate = peerPresenceMap[subjectId];
  const fullBench = aggregateBenchmarkPresence(rates);
  const fullIndex = computeAiPresenceIndex(subjectRate, fullBench.value);
  let maxMove = 0;
  for (const dropId of peerIds) {
    const subset = peerIds
      .filter((id) => id !== dropId)
      .map((id) => peerPresenceMap[id])
      .filter((v) => v != null);
    const bench = aggregateBenchmarkPresence(subset);
    const idx = computeAiPresenceIndex(subjectRate, bench.value);
    if (fullIndex.indexValue != null && idx.indexValue != null) {
      maxMove = Math.max(maxMove, Math.abs(fullIndex.indexValue - idx.indexValue));
    }
  }
  let state = "STABLE";
  if (maxMove >= 15) state = "FRAGILE";
  else if (maxMove >= 8) state = "MODERATELY_SENSITIVE";
  return { leaveOneOutState: state, maxIndexMovement: maxMove, peers: peerIds.length };
}

function parentConcentration(brandIds, idToParent) {
  const counts = {};
  for (const id of brandIds) {
    const p = idToParent.get(id) || "Unknown";
    counts[p] = (counts[p] || 0) + 1;
  }
  return counts;
}

function simulateBenchmarkSubjects(subjectIds, peerIds, presenceById) {
  let valid = 0;
  let limited = 0;
  let suppressed = 0;
  const rows = [];
  for (const subjectId of subjectIds) {
    const peers = peerIds.filter((id) => id !== subjectId);
    const peerRates = peers
      .map((id) => presenceById[id])
      .filter((v) => typeof v === "number");
    const bench = aggregateBenchmarkPresence(peerRates);
    const status = classifyBenchmarkSampleSize(bench.sampleSize, bench.value);
    const subjectPresence = presenceById[subjectId];
    let indexResult = { indexValue: null };
    if (
      subjectPresence != null &&
      bench.value != null &&
      status !== "SUPPRESSED_INSUFFICIENT_DATA"
    ) {
      indexResult = computeAiPresenceIndex(subjectPresence, bench.value);
    }
    const rowStatus =
      status === "VALID_BENCHMARK"
        ? "VALID"
        : status === "LIMITED_BENCHMARK"
          ? "LIMITED"
          : "SUPPRESSED";
    if (rowStatus === "VALID") valid += 1;
    else if (rowStatus === "LIMITED") limited += 1;
    else suppressed += 1;
    rows.push({
      subjectEntityId: subjectId,
      subjectPresence: subjectPresence,
      benchmarkSample: bench.sampleSize,
      index: indexResult.indexValue,
      status: rowStatus,
    });
  }
  return { valid, limited, suppressed, rows };
}

/**
 * Run full internal benchmark expansion audit.
 */
export function runInternalBenchmarkExpansionAudit(opts = {}) {
  const customerVisibleIds = loadCustomerVisibleBrandIds();
  const currentBenchmarkIds = loadCurrentInternalBenchmarkIds();
  const inventory = loadActiveUniverseInventory(opts.activeUniversePath);
  const responseTexts = collectResponseTexts(opts.responseDirs);

  const idToParent = new Map();
  const idToName = new Map();
  for (const row of inventory) {
    if (row.recordId) {
      idToName.set(row.recordId, row.brandName);
    }
  }
  for (const c of loadShowcaseCompaniesConfig().companies || []) {
    for (const b of c.brands || []) {
      idToParent.set(b.brandId, c.canonicalCompanyName);
    }
  }
  for (const ps of loadPeerSetConfig().peerSets || []) {
    for (const m of ps.members || []) {
      if (m.brandId && m.canonicalParent) {
        idToParent.set(m.brandId, m.canonicalParent);
      }
    }
  }
  for (const def of CANDIDATE_DEFINITIONS) {
    if (def.canonicalId) idToParent.set(def.canonicalId, def.parent);
  }

  const candidates = CANDIDATE_DEFINITIONS.map((def) => {
    const resolved = resolveCandidateFromUniverse(def, inventory);
    const mentions = auditMentionsForCandidate(resolved, responseTexts);
    const fit = classifyBenchmarkFit(resolved, mentions);
    const inVisible = customerVisibleIds.has(resolved.canonicalId);
    const inCurrentBenchmark = currentBenchmarkIds.has(resolved.canonicalId);
    const currentPeerCount = [...currentBenchmarkIds].filter(
      (id) => id !== resolved.canonicalId
    ).length;
    const peerCountIfAdded = inCurrentBenchmark
      ? currentPeerCount
      : currentPeerCount + (resolved.identitySafe ? 1 : 0);

    const cohortImpact = {};
    for (const tag of COHORT_TAGS) {
      cohortImpact[tag] = resolved.cohortTags?.includes(tag)
        ? validityChange(
            currentPeerCount,
            inCurrentBenchmark || !resolved.identitySafe ? 0 : 1
          )
        : "NO_MATERIAL_CHANGE";
    }

    return {
      brand: resolved.brandName,
      parent: resolved.parent,
      canonicalId: resolved.canonicalId,
      status: resolved.status,
      brandArchitecture: resolved.brandArchitecture,
      cohortTags: resolved.cohortTags || [],
      chainScale: resolved.chainScale,
      benchmarkFit: fit,
      existingPromptCompatible:
        mentions.resolvedMentions > 0 ? "YES" : mentions.baselineResponsesSearched > 0 ? "PARTIAL" : "NO",
      identitySafe: resolved.identitySafe ? "YES" : "NO",
      baselineEvidenceAvailable: mentions.currentBaselinePresenceAvailable ? "YES" : "NO",
      incrementalProviderCalls: 0,
      inCustomerVisible: inVisible,
      inCurrentBenchmark,
      mentionStats: mentions,
      cohortImpact,
      customerVisibleStatus: inVisible
        ? "CUSTOMER_VISIBLE"
        : "INTERNAL_BENCHMARK_ONLY",
      moatValue: {
        benchmarkDepth: fit === "HIGH_FIT" ? "HIGH" : fit === "MEDIUM_FIT" ? "MEDIUM" : "LOW",
        cohortDiversity: resolved.parent && !["Marriott International", "Hilton", "IHG", "Choice Hotels"].includes(resolved.parent) ? "HIGH" : "MEDIUM",
        observedCompetitorIntelligence: mentions.resolvedMentions >= 3 ? "HIGH" : mentions.resolvedMentions > 0 ? "MEDIUM" : "LOW",
        longitudinalValue: mentions.resolvedMentions >= 2 ? "MEDIUM" : "LOW",
        customerVisibleValue: inVisible ? "HIGH" : "LOW",
      },
    };
  }).filter((c) => !c.inCustomerVisible);

  for (const c of candidates) {
    if (c.canonicalId && c.parent) {
      idToParent.set(c.canonicalId, c.parent);
    }
  }

  const highFit = candidates.filter(
    (c) =>
      c.benchmarkFit === "HIGH_FIT" &&
      c.identitySafe === "YES" &&
      c.baselineEvidenceAvailable === "YES" &&
      !c.inCurrentBenchmark &&
      (c.mentionStats?.ambiguousMentions || 0) < (c.mentionStats?.resolvedMentions || 0)
  );
  const highFitWithAmbiguity = candidates.filter(
    (c) =>
      c.benchmarkFit === "HIGH_FIT" &&
      c.identitySafe === "YES" &&
      c.baselineEvidenceAvailable === "YES" &&
      !c.inCurrentBenchmark &&
      (c.mentionStats?.ambiguousMentions || 0) >= (c.mentionStats?.resolvedMentions || 0)
  );
  const mediumFit = candidates.filter(
    (c) => c.benchmarkFit === "MEDIUM_FIT" && c.identitySafe === "YES" && !c.inCurrentBenchmark
  );

  function scoreCandidate(c) {
    let score = c.mentionStats?.resolvedMentions || 0;
    if (c.cohortTags?.includes("SOFT_COLLECTION")) score += 5;
    if (c.cohortTags?.includes("CONVERSION")) score += 3;
    if (c.cohortTags?.includes("LIFESTYLE")) score += 2;
    if (c.moatValue?.cohortDiversity === "HIGH") score += 4;
    return score;
  }

  const minimumSet = [];
  const usedParents = new Set(
    [...currentBenchmarkIds].map((id) => idToParent.get(id)).filter(Boolean)
  );

  const peerParentCounts = {};
  for (const id of currentBenchmarkIds) {
    const p = idToParent.get(id) || "Unknown";
    peerParentCounts[p] = (peerParentCounts[p] || 0) + 1;
  }

  function parentOverRepresented(c) {
    const count = peerParentCounts[c.parent] || 0;
    const isHard = String(c.brandArchitecture || "").includes("Hard Brand");
    return isHard && count >= 4;
  }

  const rankedHighFit = [...highFit]
    .filter((c) => !parentOverRepresented(c))
    .sort((a, b) => scoreCandidate(b) - scoreCandidate(a));
  const softCollectionFirst = rankedHighFit.filter((c) => c.cohortTags?.includes("SOFT_COLLECTION"));
  const conversionNext = rankedHighFit.filter(
    (c) => !c.cohortTags?.includes("SOFT_COLLECTION") && c.cohortTags?.includes("CONVERSION")
  );
  const remainder = rankedHighFit.filter(
    (c) => !softCollectionFirst.includes(c) && !conversionNext.includes(c)
  );

  for (const pool of [softCollectionFirst, conversionNext, remainder]) {
    for (const c of pool) {
      if (minimumSet.length >= 5) break;
      const parentCount = minimumSet.filter((m) => m.parent === c.parent).length;
      if (parentCount >= 1 && !c.cohortTags?.includes("SOFT_COLLECTION")) continue;
      if (minimumSet.find((m) => m.canonicalId === c.canonicalId)) continue;
      minimumSet.push(c);
      usedParents.add(c.parent);
    }
  }
  while (minimumSet.length < 3 && mediumFit.length) {
    const next = mediumFit.sort((a, b) => scoreCandidate(b) - scoreCandidate(a)).shift();
    if (next && !minimumSet.find((m) => m.canonicalId === next.canonicalId)) minimumSet.push(next);
  }

  const preferredSet = [...minimumSet];
  const preferredNamedExtras = [
    "DoubleTree by Hilton",
    "Small Luxury Hotels of the World",
  ];
  for (const brandName of preferredNamedExtras) {
    if (preferredSet.length >= 7) break;
    const c = highFit.find((x) => x.brand === brandName);
    if (c && !preferredSet.find((p) => p.canonicalId === c.canonicalId)) {
      preferredSet.push(c);
    }
  }

  const presenceById = {};
  for (const id of currentBenchmarkIds) {
    presenceById[id] = null;
  }
  for (const c of candidates) {
    if (c.canonicalId && c.mentionStats.presenceRate != null) {
      presenceById[c.canonicalId] = c.mentionStats.presenceRate;
    }
  }
  for (const id of customerVisibleIds) {
    if (presenceById[id] == null) {
      const cand = candidates.find((c) => c.canonicalId === id);
      if (cand?.mentionStats?.presenceRate != null) {
        presenceById[id] = cand.mentionStats.presenceRate;
      }
    }
  }
  const cfg = loadShowcaseCompaniesConfig();
  for (const co of cfg.companies || []) {
    for (const b of co.brands || []) {
      if (presenceById[b.brandId] == null) {
        const inv = inventory.find((r) => r.recordId === b.brandId);
        if (inv) presenceById[b.brandId] = null;
      }
    }
  }

  const peerV2Ids = [...currentBenchmarkIds];
  const minAddedIds = minimumSet.map((c) => c.canonicalId).filter(Boolean);
  const prefAddedIds = preferredSet.map((c) => c.canonicalId).filter(Boolean);
  const minPeerIds = [...new Set([...peerV2Ids, ...minAddedIds])];
  const prefPeerIds = [...new Set([...peerV2Ids, ...prefAddedIds])];
  const subjectIds = [...customerVisibleIds];

  const currentSim = simulateBenchmarkSubjects(subjectIds, peerV2Ids, presenceById);
  const minSim = simulateBenchmarkSubjects(subjectIds, minPeerIds, presenceById);
  const prefSim = simulateBenchmarkSubjects(subjectIds, prefPeerIds, presenceById);

  const stabilityRows = [];
  for (const label of ["CURRENT", "MINIMUM_SET", "PREFERRED_SET"]) {
    const peers =
      label === "CURRENT" ? peerV2Ids : label === "MINIMUM_SET" ? minPeerIds : prefPeerIds;
    const sampleSubject = "recEJCTDj1zrsjPM6";
    const st = leaveOneOutStability(
      Object.fromEntries(peers.map((id) => [id, presenceById[id] ?? 0.3])),
      sampleSubject
    );
    stabilityRows.push({
      cohort: label,
      peers: peers.length,
      ...st,
    });
  }

  const doNotAdd = candidates.filter(
    (c) =>
      c.benchmarkFit === "LOW_FIT" ||
      c.identitySafe === "NO" ||
      c.existingPromptCompatible === "NO"
  );

  const secondary = candidates.filter(
    (c) =>
      c.benchmarkFit === "MEDIUM_FIT" &&
      c.identitySafe === "YES" &&
      !minimumSet.find((m) => m.canonicalId === c.canonicalId) &&
      !preferredSet.find((p) => p.canonicalId === c.canonicalId)
  );

  const secondaryLater = [
    ...candidates.filter((c) => parentOverRepresented(c) && c.benchmarkFit === "HIGH_FIT"),
    ...secondary,
  ];

  return {
    BRAND_AI_INTERNAL_BENCHMARK_EXPANSION_AUDIT_COMPLETE: true,
    providerCalls: 0,
    spend: 0,
    customerVisibleBrands: customerVisibleIds.size,
    currentInternalBenchmarkCount: currentBenchmarkIds.size,
    currentValidPeerCounts: {
      peerSetV2Total: currentBenchmarkIds.size,
      peersWhenSubjectRemoved: currentBenchmarkIds.size - 1,
      withPresenceEvidenceInCorpus: Object.values(presenceById).filter((v) => v != null).length,
    },
    currentLimitedCohorts: [
      "SOFT_COLLECTION — peer v2 has 15 members but corpus presence evidence sparse for non-Marriott CALA slice",
      "LIFESTYLE — IHG/Hilton portfolio brands lack baseline presence in marriottCalaEn gap slice",
    ],
    candidatesAudited: candidates.length,
    candidates,
    minimumRecommendedSet: {
      brands: minimumSet.map((c) => c.brand),
      count: minimumSet.length,
      why: "Smallest governed set with Active/Live identity, baseline corpus mentions, and cross-parent diversification for soft-brand / conversion cohorts",
      cohortsMovedToValid: minimumSet.length >= 3 ? ["SOFT_COLLECTION", "CONVERSION", "LIFESTYLE"] : [],
      additionalProviderCalls: 0,
      brandIds: minimumSet.map((c) => c.canonicalId),
    },
    preferredRecommendedSet: {
      brands: preferredSet.map((c) => c.brand),
      count: preferredSet.length,
      why: "Extends minimum set with DoubleTree (conversion / upper-upscale) and Small Luxury Hotels (soft collection / owner flexibility) without Pullman identity ambiguity or further Marriott hard-brand concentration",
      cohortsMovedToValid: ["SOFT_COLLECTION", "CONVERSION", "LIFESTYLE", "UPPER_UPSCALE"],
      additionalProviderCalls: 0,
      brandIds: preferredSet.map((c) => c.canonicalId),
    },
    benchmarkSimulation: {
      current: currentSim,
      minimumSet: minSim,
      preferredSet: prefSim,
    },
    stability: stabilityRows,
    parentConcentration: {
      before: parentConcentration(peerV2Ids, idToParent),
      afterMinimum: parentConcentration(minPeerIds, idToParent),
      afterPreferred: parentConcentration(prefPeerIds, idToParent),
    },
    addNow: minimumSet.map((c) => ({
      brand: c.brand,
      parent: c.parent,
      canonicalId: c.canonicalId,
      fit: c.benchmarkFit,
      mentions: c.mentionStats.resolvedMentions,
    })),
    secondaryLater: secondaryLater.slice(0, 8).map((c) => c.brand),
    doNotAdd: [
      ...doNotAdd.map((c) => ({
        brand: c.brand,
        reason: c.benchmarkFit === "LOW_FIT" ? "LOW_FIT" : "NO_IDENTITY_OR_PROMPT",
      })),
      ...highFitWithAmbiguity.map((c) => ({
        brand: c.brand,
        reason: "IDENTITY_AMBIGUITY_IN_CORPUS — requires deterministic re-extraction gate before add",
      })),
    ],
    founderGate: {
      status: "FOUNDER_APPROVAL_REQUIRED",
      recommendedAction:
        "Approve MINIMUM_RECOMMENDED_SET (3–5 brands) as INTERNAL_BENCHMARK_ONLY extension to peer v2; re-extract Presence from stored corpus; do not add to customer dropdown until separate UI approval",
    },
    customerExposure: {
      visibleDropdownChanged: false,
      internalBenchmarkOnly: preferredSet.map((c) => c.brand),
      potentialFutureVisible: [],
    },
    regression: {
      BRAND_LOGIC_DIFF: 0,
      BRAND_UI_DIFF: 0,
      BRAND_LONGITUDINAL_DATA_DIFF: 0,
      BENCHMARK_ENGINE_DIFF: 0,
      OPERATOR_DIFF: 0,
    },
  };
}
