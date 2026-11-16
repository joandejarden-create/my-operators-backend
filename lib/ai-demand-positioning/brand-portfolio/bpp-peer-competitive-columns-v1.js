/**
 * Displacement vs You / Scenarios Shared — UNIQUE_SCENARIO grain from the
 * subject hotel's Loyalty corpus (peer mentions in subject responses).
 *
 * Shared with Bethesda first-cycle semantics:
 *   shared = subject present AND peer present (any provider in scenario)
 *   displacement = subject absent AND peer present
 */

import {
  detectPropertyMention,
  normalizeSubjectHaystack,
  findTokenBoundaryIndex,
} from "../execution/response-parser.js";
import { loadPeriod, isLoyaltyMeasurementPeriod } from "../data-model.js";

export const BPP_PEER_COMPETITIVE_COLUMNS_V1 = "BPP_PEER_COMPETITIVE_COLUMNS_V1";

function peerMentioned(response, peerHotel) {
  if (!response || !peerHotel) return false;
  const text = normalizeSubjectHaystack(response);
  const variants = [
    peerHotel,
    peerHotel.replace(/,.*$/, "").trim(),
    peerHotel.replace(/\s+(Curio Collection|A Tapestry Collection).*$/i, "").trim(),
    peerHotel.replace(/\s+by Hilton.*$/i, "").trim(),
    peerHotel.replace(/\s+Hotel$/i, "").trim(),
  ].filter((v) => v && v.length >= 5);
  for (const v of variants) {
    const needle = normalizeSubjectHaystack(v);
    if (needle.length < 5) continue;
    if (findTokenBoundaryIndex(text, needle) !== -1) return true;
  }
  return false;
}

function comparableObs(obs) {
  return obs && !obs.error && obs.rawResponse && obs.metricInclusion !== false;
}

/**
 * @param {object} args
 * @param {object} args.profile subject profile
 * @param {object} args.period Loyalty period (subject)
 * @param {Array<{canonicalEntityId?:string,name?:string,peerHotel?:string}>} args.peers
 */
export function computePeerCompetitiveColumnsFromSubjectLoyalty({
  profile,
  period,
  peers,
}) {
  if (!profile || !period || !isLoyaltyMeasurementPeriod(period)) {
    return { ok: false, error: "invalid_loyalty_period" };
  }
  const scenarioIds = Array.isArray(period.scenarioIds)
    ? period.scenarioIds
    : [...new Set((period.observations || []).map((o) => o.scenarioId).filter(Boolean))];
  if (!scenarioIds.length) return { ok: false, error: "no_scenarios" };

  const peerList = (peers || [])
    .map((p) => ({
      canonicalEntityId: p.canonicalEntityId || null,
      peerHotel: p.peerHotel || p.name || p.displayName || null,
    }))
    .filter((p) => p.peerHotel);

  const byScenario = new Map();
  for (const sid of scenarioIds) byScenario.set(sid, []);
  for (const obs of period.observations || []) {
    if (!byScenario.has(obs.scenarioId)) continue;
    byScenario.get(obs.scenarioId).push(obs);
  }

  const scenarioRows = [];
  for (const sid of scenarioIds) {
    const obsList = (byScenario.get(sid) || []).filter(comparableObs);
    let subjectPresent = false;
    const peersPresent = new Set();
    for (const obs of obsList) {
      if (detectPropertyMention(obs.rawResponse, profile).mentioned) {
        subjectPresent = true;
      }
      for (const peer of peerList) {
        if (peerMentioned(obs.rawResponse, peer.peerHotel)) {
          peersPresent.add(peer.canonicalEntityId || peer.peerHotel);
        }
      }
    }
    scenarioRows.push({
      scenarioId: sid,
      subjectPresent,
      peersPresent: [...peersPresent],
    });
  }

  const byPeer = {};
  for (const peer of peerList) {
    const key = peer.canonicalEntityId || peer.peerHotel;
    const displacement = scenarioRows.filter(
      (r) => !r.subjectPresent && r.peersPresent.includes(key)
    ).length;
    const shared = scenarioRows.filter(
      (r) => r.subjectPresent && r.peersPresent.includes(key)
    ).length;
    byPeer[key] = {
      canonicalEntityId: peer.canonicalEntityId,
      peerHotel: peer.peerHotel,
      displacementScenarios: displacement,
      scenariosShared: shared,
    };
  }

  return {
    ok: true,
    contract: BPP_PEER_COMPETITIVE_COLUMNS_V1,
    periodId: period.periodId,
    scenarioCount: scenarioIds.length,
    byPeer,
  };
}

/**
 * Fill ranking.rows displacementDisplay / sharedDisplay when stubbed as "—".
 */
export function hydrateRankingCompetitiveColumns(bpp, profile) {
  if (!bpp?.ranking?.rows?.length || !profile) return bpp;
  const rows = bpp.ranking.rows;
  const needsHydrate = rows.some(
    (r) =>
      !r.isSubject &&
      (r.displacementDisplay == null ||
        r.displacementDisplay === "—" ||
        r.sharedDisplay == null ||
        r.sharedDisplay === "—")
  );
  if (!needsHydrate) return bpp;

  const periodId =
    bpp.periodId ||
    bpp.portfolioPeriodId ||
    bpp.measurement?.periodId ||
    rows.find((r) => r.isSubject)?.periodId ||
    null;
  if (!periodId) return bpp;
  const period = loadPeriod(periodId);
  if (!period || !isLoyaltyMeasurementPeriod(period)) return bpp;

  const peers = rows
    .filter((r) => !r.isSubject)
    .map((r) => ({
      canonicalEntityId: r.canonicalEntityId,
      peerHotel: r.name,
      name: r.name,
    }));
  const computed = computePeerCompetitiveColumnsFromSubjectLoyalty({
    profile,
    period,
    peers,
  });
  if (!computed.ok) return bpp;

  const nextRows = rows.map((r) => {
    if (r.isSubject) {
      return {
        ...r,
        displacementDisplay: "—",
        sharedDisplay: "—",
      };
    }
    const key = r.canonicalEntityId || r.name;
    const stats = computed.byPeer[key];
    if (!stats) return r;
    return {
      ...r,
      displacementDisplay: String(stats.displacementScenarios ?? 0),
      sharedDisplay: String(stats.scenariosShared ?? 0),
    };
  });

  return {
    ...bpp,
    ranking: {
      ...bpp.ranking,
      rows: nextRows,
    },
    _bppPeerCompetitiveColumns: {
      contract: BPP_PEER_COMPETITIVE_COLUMNS_V1,
      periodId: computed.periodId,
      hydrated: true,
    },
  };
}
