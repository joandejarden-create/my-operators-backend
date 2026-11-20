/**
 * Attribute extracted hotel name hints to existing HPC records.
 * Never auto-creates hotels on uncertain match.
 */

export const HOTEL_ATTRIBUTION_VERSION = "sec-edgar-hotel-attribution-v1";

function normName(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\b(the|hotel|inn|resort|suites|a|an)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenSet(s) {
  return new Set(normName(s).split(" ").filter((t) => t.length > 1));
}

function jaccard(a, b) {
  const A = tokenSet(a);
  const B = tokenSet(b);
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter += 1;
  return inter / (A.size + B.size - inter);
}

/**
 * @param {string} hint
 * @param {Array<{id:string,name:string,city?:string,brand?:string}>} hpcCandidates
 */
export function matchHotelHintToHpc(hint, hpcCandidates = [], opts = {}) {
  const minScore = opts.minScore ?? 0.55;
  const scored = (hpcCandidates || [])
    .map((h) => {
      const score = Math.max(
        jaccard(hint, h.name),
        jaccard(hint, `${h.brand || ""} ${h.name}`),
        jaccard(hint, `${h.name} ${h.city || ""}`),
      );
      return { hotel: h, score };
    })
    .filter((r) => r.score >= minScore)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) {
    return {
      status: "NO_MATCH",
      hint,
      hpcId: null,
      quarantine: true,
      reason: "no_hpc_candidate_above_threshold",
    };
  }
  if (scored.length > 1 && scored[0].score - scored[1].score < 0.08) {
    return {
      status: "AMBIGUOUS",
      hint,
      hpcId: null,
      candidates: scored.slice(0, 3),
      quarantine: true,
      reason: "ambiguous_hpc_match",
    };
  }
  return {
    status: "MATCHED",
    hint,
    hpcId: scored[0].hotel.id,
    hotelName: scored[0].hotel.name,
    score: scored[0].score,
    quarantine: false,
  };
}

export function attributeFindings(verifiedResults = [], hpcCandidates = []) {
  const out = [];
  let matched = 0;
  let quarantined = 0;
  for (const vr of verifiedResults) {
    const f = vr.finding || {};
    const hints = f.hotelNameHints || [];
    if (!hints.length) {
      quarantined += 1;
      out.push({
        ...vr,
        attribution: { status: "UNATTRIBUTED", quarantine: true },
        persistable: false,
      });
      continue;
    }
    let best = null;
    for (const hint of hints) {
      const m = matchHotelHintToHpc(hint, hpcCandidates);
      if (!best || (m.score || 0) > (best.score || 0)) best = m;
    }
    if (best?.status === "MATCHED") {
      matched += 1;
      out.push({
        ...vr,
        attribution: best,
        persistable: vr.persistable === true,
        finding: { ...f, hpcHotelId: best.hpcId, attributedHotelName: best.hotelName },
      });
    } else {
      quarantined += 1;
      out.push({
        ...vr,
        attribution: best || { status: "NO_MATCH", quarantine: true },
        persistable: false,
      });
    }
  }
  return {
    version: HOTEL_ATTRIBUTION_VERSION,
    matched,
    quarantined,
    results: out,
  };
}
