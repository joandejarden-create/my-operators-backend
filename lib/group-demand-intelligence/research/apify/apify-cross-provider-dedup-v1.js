/**
 * Cross-provider dedup: Apify + Exa + existing hotel GDI inventory → one candidate.
 */

import { buildDedupeKey } from "./apify-source-normalize-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim().toLowerCase();
}

function orgTitleBlob(c) {
  return `${c.organizationName || ""} ${c.title || ""} ${c.venueName || ""}`.toLowerCase();
}

function urlsMatch(a, b) {
  const ua = text(a?.sourceUrl || a?.url).replace(/\/$/, "");
  const ub = text(b?.sourceUrl || b?.url).replace(/\/$/, "");
  if (!ua || !ub) return false;
  return ua === ub || ua.includes(ub) || ub.includes(ua);
}

function orgsSimilar(a, b) {
  const xa = text(a.organizationName || a.title);
  const xb = text(b.organizationName || b.title);
  if (!xa || !xb || xa.length < 4 || xb.length < 4) return false;
  return xa === xb || xa.includes(xb) || xb.includes(xa);
}

function datesOverlapHint(a, b) {
  const da = text(a.startDate).slice(0, 7);
  const db = text(b.startDate).slice(0, 7);
  if (!da || !db) return null;
  return da === db;
}

/**
 * Merge Apify candidates into existing pool without double-counting.
 * @param {object[]} apifyCandidates
 * @param {object} inventory - { exa=[], gdi=[], priorApify=[], continuation=[] }
 */
export function dedupeApifyAgainstInventory(apifyCandidates = [], inventory = {}) {
  const pools = [
    ...(inventory.exa || []).map((x) => ({ ...x, _pool: "exa" })),
    ...(inventory.gdi || []).map((x) => ({ ...x, _pool: "gdi" })),
    ...(inventory.priorApify || []).map((x) => ({ ...x, _pool: "apify_prior" })),
    ...(inventory.continuation || []).map((x) => ({ ...x, _pool: "continuation" })),
  ];

  const unique = [];
  const merged = [];
  const duplicates = [];

  for (const cand of apifyCandidates) {
    const key = cand.dedupeKey || buildDedupeKey(cand);
    let match = null;

    for (const existing of pools) {
      if (urlsMatch(cand, existing) || orgsSimilar(cand, existing)) {
        const dateHint = datesOverlapHint(cand, existing);
        if (dateHint === false) continue;
        match = existing;
        break;
      }
    }

    // also against already-accepted unique in this batch
    if (!match) {
      for (const u of unique) {
        if (u.dedupeKey === key || urlsMatch(cand, u) || orgsSimilar(cand, u)) {
          match = u;
          break;
        }
      }
    }

    if (match) {
      const supportingSources = [
        ...(match.supportingSources || []),
        {
          provider: "apify",
          actorId: cand.provenance?.actorId,
          sourceUrl: cand.sourceUrl,
          retrievedAt: cand.provenance?.retrievedAt,
        },
      ];
      const mergedRec = {
        ...match,
        supportingSources,
        discoveryProviders: Array.from(
          new Set([...(match.discoveryProviders || [match._pool || match.discoveryProvider || "unknown"]), "apify"]),
        ),
        apifyCorroboration: true,
      };
      merged.push({ candidateKey: key, matchedPool: match._pool || "batch", merged: mergedRec });
      duplicates.push({ reason: "CROSS_PROVIDER_MATCH", key, pool: match._pool || "batch" });
      // update in-place if in unique
      const ui = unique.findIndex((u) => u.dedupeKey === key || orgsSimilar(u, cand));
      if (ui >= 0) unique[ui] = { ...unique[ui], supportingSources, apifyCorroboration: true };
      continue;
    }

    unique.push({
      ...cand,
      dedupeKey: key,
      discoveryProviders: ["apify"],
      supportingSources: [
        {
          provider: "apify",
          actorId: cand.provenance?.actorId,
          sourceUrl: cand.sourceUrl,
          retrievedAt: cand.provenance?.retrievedAt,
        },
      ],
    });
  }

  return {
    uniqueCandidates: unique,
    mergedIntoExisting: merged,
    duplicates,
    netNewCount: unique.length,
    corroborationCount: merged.length,
  };
}

/**
 * Screen: Apify listings are discovery only — never auto-qualified.
 */
export function commercialRelevanceScreen(candidate) {
  const reasons = [];
  let status = "PASS_TO_ORS";

  if (candidate.relationshipTrackOnly) {
    status = "RELATIONSHIP_CONTINUATION_CANDIDATE";
    reasons.push("Directory/venue listing is not a dated room-block opportunity");
  }
  if (candidate.recordKind === "PUBLISHED_EVENT" && !candidate.startDate) {
    status = "RELATIONSHIP_CONTINUATION_CANDIDATE";
    reasons.push("Published event missing start date");
  }
  if (looksCompetitorHotelHost(candidate)) {
    status = "DEMOTE_COMPETITOR_HOST_RISK";
    reasons.push("Possible competing-hotel hosted event — do not treat as overflow");
  }

  return {
    ...candidate,
    commercialScreen: status,
    commercialScreenReasons: reasons,
    accessibilityClass: null,
    requiresOrs: true,
  };
}

function looksCompetitorHotelHost(c) {
  const blob = orgTitleBlob(c);
  return /\b(hilton|marriott|hyatt|ihg|westin|sheraton|embassy suites|courtyard|residence inn)\b/.test(blob);
}
