/**
 * Competitive gap intelligence — Presence rank + peer gaps (no arbitrary score).
 */

export const COMPETITIVE_GAP_INTEL_VERSION =
  "ai_visibility_competitive_gap_intelligence_v1";

function pct(v) {
  if (v == null || !Number.isFinite(v)) return null;
  const n = v <= 1 ? v * 100 : v;
  return `${(Math.round(n * 10) / 10).toFixed(1)}%`;
}

/**
 * Build peer gap rows vs subject for comparable Presence rates.
 */
export function buildCompetitiveGapView(opts = {}) {
  const subject = opts.subject || {};
  const peers = Array.isArray(opts.peers) ? opts.peers : [];
  const peerPresentSubjectMissingN =
    typeof opts.PEER_PRESENT_SUBJECT_MISSING_N === "number"
      ? opts.PEER_PRESENT_SUBJECT_MISSING_N
      : null;

  const subjectRate =
    typeof subject.presenceRate === "number" ? subject.presenceRate : null;
  const rows = peers
    .filter((p) => !p.isSubject)
    .map((p) => {
      const peerRate =
        typeof p.aiPresenceRate === "number"
          ? p.aiPresenceRate
          : typeof p.presenceRate === "number"
            ? p.presenceRate
            : null;
      const delta =
        subjectRate != null && peerRate != null ? peerRate - subjectRate : null;
      return {
        peerId: p.entityId || p.peerId || null,
        peerName: p.entityName || p.name || null,
        SUBJECT_PRESENCE_RATE: subjectRate,
        PEER_PRESENCE_RATE: peerRate,
        PRESENCE_DELTA: delta,
        presenceDeltaDisplay:
          delta == null
            ? "—"
            : `${delta >= 0 ? "+" : ""}${Math.round(delta * 1000) / 10} pp`,
        competitivePosition: p.competitivePosition ?? null,
      };
    })
    .sort((a, b) => (b.PEER_PRESENCE_RATE ?? -1) - (a.PEER_PRESENCE_RATE ?? -1));

  const rank =
    typeof subject.competitivePosition === "number"
      ? subject.competitivePosition
      : typeof subject.rank === "number"
        ? subject.rank
        : null;
  const peerCount =
    typeof subject.peerCount === "number" ? subject.peerCount : peers.length || null;

  const statements = [];
  if (peerPresentSubjectMissingN != null && peerPresentSubjectMissingN > 0) {
    const peerNames = [
      ...new Set(
        (opts.peerPresentRows || [])
          .flatMap((r) => (r.PEERS_PRESENT || []).map((p) => p.entityName || p.entityId))
          .filter(Boolean)
      ),
    ].slice(0, 5);
    const peerPhrase =
      peerNames.length >= 2
        ? `${peerNames.length} peer brands`
        : peerNames[0] || "Peer brands";
    statements.push({
      text: `${peerPhrase} appeared in ${peerPresentSubjectMissingN} monitored owner question${
        peerPresentSubjectMissingN === 1 ? "" : "s"
      } where ${subject.name || "the brand"} was not observed.`,
      evidenceRef: "peerPresentSubjectMissing",
    });
  }
  if (rank != null && peerCount != null && subject.name) {
    const scope = opts.scopeLabel || "this geography";
    statements.push({
      text: `${subject.name} ranks #${rank} of ${peerCount} peers for observed Presence in ${scope}.`,
      evidenceRef: "competitivePosition",
    });
  }
  const largest = rows.find(
    (r) => typeof r.PRESENCE_DELTA === "number" && r.PRESENCE_DELTA >= 0.1
  );
  if (largest) {
    statements.push({
      text: `${largest.peerName || "A peer"} shows higher Observed Presence (${pct(
        largest.PEER_PRESENCE_RATE
      )}) than ${subject.name || "the brand"} (${pct(subjectRate)}).`,
      evidenceRef: "presenceDelta",
    });
  }

  return {
    version: COMPETITIVE_GAP_INTEL_VERSION,
    SUBJECT_PRESENCE_RATE: subjectRate,
    PRESENCE_RANK: rank,
    PEER_COUNT: peerCount,
    PEER_PRESENT_SUBJECT_MISSING_N: peerPresentSubjectMissingN,
    peerGaps: rows,
    statements,
    ARBITRARY_SCORE: false,
  };
}
