/**
 * GDI Cross-Segment Opportunity Deduplication V2
 *
 * Collapses the same underlying organization / event / program appearing
 * through multiple segment paths BEFORE deep research selection.
 *
 * Observed pilot failures this corrects:
 *   - CARPHA: disc_a6def3588729 + disc_023923963f08 (LaLuna)
 *   - Advantage Travel: disc_9cd2c082c05d + disc_51073448c775 (Memmo)
 *
 * Preserves: original evidence, all segment associations, stable IDs of the
 * canonical pick, demand dates, research history, contact intelligence.
 * Does not collapse distinct event editions / projects / sourcing decisions.
 * Does not spend. Does not publish.
 */

import { normalizeOrgIdentityKey } from "../workflow/reuse-prior-intelligence-v1.js";

export const GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2 =
  "gdi-cross-segment-opportunity-dedupe-v2";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function orgDeepKeyLocal(opp = {}) {
  return normalizeOrgIdentityKey(
    opp.organizationName || opp.organizingEntity || opp.venueName || "",
  );
}

function editionIdentityKeyLocal(opp = {}) {
  const org = orgDeepKeyLocal(opp);
  const start = String(opp.eventStartDate || opp.programmeStartDate || "").slice(
    0,
    10,
  );
  const title = text(opp.title || opp.programmeName || opp.activityTitle)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 80);
  return `${org}|${start || "undated"}|${title || "untitled"}`;
}

/** Extract parenthetical / leading acronyms (e.g. CARPHA). */
export function extractOrgAcronyms(name = "") {
  const raw = text(name);
  const out = new Set();
  for (const m of raw.matchAll(/\(([A-Z][A-Z0-9]{1,11})\)/g)) {
    out.add(m[1].toLowerCase());
  }
  const lead = raw.match(/^([A-Z][A-Z0-9]{2,11})\b/);
  if (lead) out.add(lead[1].toLowerCase());
  return [...out];
}

/**
 * Soft org key: strip legal suffixes, collapse whitespace, keep acronym tokens.
 */
export function softOrgIdentityKey(opp = {}) {
  const base =
    orgDeepKeyLocal(opp) || normalizeOrgIdentityKey(opp.organizationName || "");
  if (!base) return "";
  return base
    .replace(
      /\b(chapter|inc|incorporated|llc|l\.?l\.?c\.?|corp|corporation|ltd|co|agency|partnership|association|society|board|ministry of \w+)\b/g,
      " ",
    )
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Program / event cluster key — same org (soft) + undated edition family.
 * Distinct dated editions with different start dates stay separate when years differ.
 */
export function canonicalProgramClusterKey(opp = {}) {
  const soft = softOrgIdentityKey(opp);
  const acronyms = extractOrgAcronyms(opp.organizationName || opp.organizingEntity || "");
  const orgToken = soft || acronyms[0] || "";
  const start = String(opp.eventStartDate || opp.programmeStartDate || "").slice(0, 10);
  const year =
    (start && start.slice(0, 4)) ||
    (text(opp.title || opp.programmeName || "").match(/\b(20[2-9]\d)\b/) || [])[1] ||
    "undated";
  const titleNorm = text(opp.title || opp.programmeName || opp.activityTitle)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\b(annual|st|nd|rd|th|conference|congress|meeting|week|edition)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 60);
  // Prefer acronym when present so "Caribbean Public Health Agency (CARPHA)"
  // clusters with "CARPHA; Ministry of Health…"
  const orgPart = acronyms[0] || orgToken;
  return `${orgPart}|${year}|${titleNorm || "untitled"}`;
}

export function orgsLikelySameEntity(a = {}, b = {}) {
  const ka = softOrgIdentityKey(a);
  const kb = softOrgIdentityKey(b);
  if (!ka || !kb) {
    const aa = extractOrgAcronyms(a.organizationName || "");
    const ba = extractOrgAcronyms(b.organizationName || "");
    if (aa.length && ba.length && aa.some((x) => ba.includes(x))) return true;
    return false;
  }
  if (ka === kb) return true;
  if (ka.includes(kb) || kb.includes(ka)) return true;
  const aa = extractOrgAcronyms(a.organizationName || "");
  const ba = extractOrgAcronyms(b.organizationName || "");
  if (aa.length && ba.length && aa.some((x) => ba.includes(x))) return true;
  // Token overlap ≥ 2 meaningful tokens
  const ta = new Set(ka.split(" ").filter((t) => t.length > 2));
  const tb = new Set(kb.split(" ").filter((t) => t.length > 2));
  let overlap = 0;
  for (const t of ta) if (tb.has(t)) overlap += 1;
  return overlap >= 2 && overlap / Math.min(ta.size, tb.size) >= 0.5;
}

export function areDistinctEventEditions(a = {}, b = {}) {
  const sa = String(a.eventStartDate || "").slice(0, 10);
  const sb = String(b.eventStartDate || "").slice(0, 10);
  if (sa && sb && sa !== sb) {
    // Same year CARPHA editions with identical titles are NOT distinct
    if (sa.slice(0, 4) === sb.slice(0, 4)) {
      const ea = editionIdentityKeyLocal(a);
      const eb = editionIdentityKeyLocal(b);
      // If titles normalize similarly, treat as same edition noise
      const ta = ea.split("|")[2] || "";
      const tb = eb.split("|")[2] || "";
      if (ta && tb && (ta.includes(tb.slice(0, 20)) || tb.includes(ta.slice(0, 20)))) {
        return false;
      }
    }
    return true;
  }
  return false;
}

function candidateScoreHint(opp = {}) {
  const screen = opp.preDeepScreen || {};
  let score = Number(opp._selectionScoreHint) || 0;
  if (screen.eligibleForDeep) score += 10;
  if (screen.outcome === "ELIGIBLE_FOR_DEEP_RESEARCH") score += 5;
  const evidence =
    (opp.seedUrls || []).length +
    (opp.evidence || []).length +
    Number(opp.investigatedUrlCount || 0);
  score += Math.min(20, evidence);
  if (opp.primaryContact?.name || opp.primaryContactName || opp.contactName) {
    score += 15;
  }
  if ((opp.namedStakeholders || []).length) score += 10;
  // Prefer richer org naming (full agency name over abbreviated list)
  score += Math.min(8, text(opp.organizationName).length / 20);
  return score;
}

/**
 * Cluster candidates that represent the same underlying program/event.
 * Returns canonical list + suppressed map with preserved segment associations.
 */
export function dedupeCandidatesAcrossSegments(candidates = [], opts = {}) {
  const rows = (Array.isArray(candidates) ? candidates : []).map((c, idx) => ({
    opp: c,
    idx,
    id: String(c?.id || c?.opportunityId || `idx_${idx}`),
    clusterKey: canonicalProgramClusterKey(c || {}),
    score: candidateScoreHint(c || {}),
  }));

  const clusters = new Map();
  for (const row of rows) {
    // Also merge when soft-org + same year even if title noise differs slightly
    let key = row.clusterKey;
    for (const [existingKey, members] of clusters) {
      const probe = members[0]?.opp;
      if (!probe) continue;
      if (
        orgsLikelySameEntity(row.opp, probe) &&
        !areDistinctEventEditions(row.opp, probe) &&
        canonicalProgramClusterKey(row.opp).split("|")[1] ===
          existingKey.split("|")[1]
      ) {
        // Same org + same year → one cluster unless editions truly distinct
        const titleA = key.split("|")[2] || "";
        const titleB = existingKey.split("|")[2] || "";
        if (
          !titleA ||
          !titleB ||
          titleA.includes(titleB.slice(0, 12)) ||
          titleB.includes(titleA.slice(0, 12)) ||
          extractOrgAcronyms(row.opp.organizationName).length
        ) {
          key = existingKey;
          break;
        }
      }
    }
    if (!clusters.has(key)) clusters.set(key, []);
    clusters.get(key).push({ ...row, clusterKey: key });
  }

  const canonical = [];
  const suppressed = [];
  const decisions = [];

  for (const [clusterKey, members] of clusters) {
    if (members.length === 1) {
      const only = members[0];
      const seg = only.opp.researchSegment || only.opp.segmentId || null;
      const enriched = {
        ...only.opp,
        crossSegmentDedupeV2: {
          version: GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2,
          role: "CANONICAL_SINGLE",
          clusterKey,
          segmentAssociations: seg ? [seg] : [],
          suppressedPeerIds: [],
        },
      };
      canonical.push(enriched);
      decisions.push({
        clusterKey,
        canonicalId: only.id,
        suppressedIds: [],
        segmentAssociations: seg ? [seg] : [],
        reason: "SINGLE_CANDIDATE",
      });
      continue;
    }

    members.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return a.idx - b.idx;
    });
    const winner = members[0];
    const peers = members.slice(1);
    const segments = [
      ...new Set(
        members
          .map((m) => m.opp.researchSegment || m.opp.segmentId)
          .filter(Boolean),
      ),
    ];
    const peerIds = peers.map((p) => p.id);
    const evidenceUrls = [
      ...new Set(
        members.flatMap((m) => [
          ...(m.opp.seedUrls || []),
          ...((m.opp.evidence || []).map((e) => e.url || e).filter(Boolean) || []),
        ]),
      ),
    ];
    const enriched = {
      ...winner.opp,
      seedUrls: evidenceUrls.length
        ? evidenceUrls
        : winner.opp.seedUrls || [],
      crossSegmentDedupeV2: {
        version: GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2,
        role: "CANONICAL",
        clusterKey,
        segmentAssociations: segments,
        suppressedPeerIds: peerIds,
        preservedPeerEvidence: true,
        note: "Peers retained as segment-path associations; only canonical receives deep research",
      },
    };
    canonical.push(enriched);
    for (const peer of peers) {
      suppressed.push({
        ...peer.opp,
        crossSegmentDedupeV2: {
          version: GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2,
          role: "SUPPRESSED_CROSS_SEGMENT_DUPLICATE",
          clusterKey,
          canonicalId: winner.id,
          segmentAssociations: segments,
          eligibleForDeep: false,
        },
        preDeepScreen: {
          ...(peer.opp.preDeepScreen || {}),
          outcome: "DUPLICATE_NO_NEW_EVIDENCE",
          eligibleForDeep: false,
          decisionReason: `cross_segment_dedupe_v2:${clusterKey}:canonical=${winner.id}`,
        },
      });
    }
    decisions.push({
      clusterKey,
      canonicalId: winner.id,
      suppressedIds: peerIds,
      segmentAssociations: segments,
      reason: "CROSS_SEGMENT_SAME_PROGRAM",
    });
  }

  // Restore original relative order of canonicals by first-seen index
  canonical.sort((a, b) => {
    const ia = rows.find((r) => r.id === String(a.id || a.opportunityId))?.idx ?? 0;
    const ib = rows.find((r) => r.id === String(b.id || b.opportunityId))?.idx ?? 0;
    return ia - ib;
  });

  return {
    version: GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2,
    inputCount: rows.length,
    canonicalCount: canonical.length,
    suppressedCount: suppressed.length,
    duplicateClusters: decisions.filter((d) => d.suppressedIds.length > 0).length,
    decisions,
    canonicalCandidates: canonical,
    suppressedCandidates: suppressed,
    beforeAfter: {
      before: rows.length,
      after: canonical.length,
      duplicatesRemoved: suppressed.length,
    },
  };
}

/**
 * Soft-match helper for paidDeepOrgKeys — add acronym + soft keys.
 */
export function expandOrgKeysForPaidDeepTracking(opp = {}) {
  const keys = new Set();
  const deep = orgDeepKeyLocal(opp);
  if (deep) keys.add(deep);
  const soft = softOrgIdentityKey(opp);
  if (soft) keys.add(soft);
  for (const a of extractOrgAcronyms(opp.organizationName || "")) {
    keys.add(a);
  }
  return [...keys].filter(Boolean);
}

export default {
  GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2,
  extractOrgAcronyms,
  softOrgIdentityKey,
  canonicalProgramClusterKey,
  orgsLikelySameEntity,
  areDistinctEventEditions,
  dedupeCandidatesAcrossSegments,
  expandOrgKeysForPaidDeepTracking,
};
