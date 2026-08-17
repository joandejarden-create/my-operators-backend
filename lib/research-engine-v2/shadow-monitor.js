/**
 * Read-only shadow monitoring runner helpers.
 */

import { checkHotelFreshness } from "./check-hotel-freshness.js";
import { computeDirectoryGaps } from "./directory-gaps.js";
import { claimFingerprint, applyAlertDedup } from "./shadow-state.js";
import { batchIdentityEnrichmentProposals } from "./identity-enrichment.js";
import { assessOpeningCorroborationFromOfficialPage } from "./opening-corroboration.js";
import { RESEARCH_MODES } from "./research-modes.js";

/**
 * @param {object[]} hotels
 * @param {object} dirs
 * @param {object} shadowState
 * @param {{ fetchDelayMs?: number, brandFamily?: string }} [opts]
 */
export async function runShadowCohort(hotels, dirs, shadowState, opts = {}) {
  const t0 = Date.now();
  /** @type {object[]} */
  const results = [];
  let i = 0;
  for (const hotel of hotels) {
    i++;
    if (opts.onProgress) opts.onProgress(`[shadow ${i}/${hotels.length}] ${hotel.name}`);
    const result = await checkHotelFreshness(hotel, {
      ihgDirectoryRows: dirs.ihgDirectoryRows,
      marriottDirectoryRows: dirs.marriottDirectoryRows,
      choiceDirectoryRows: dirs.choiceDirectoryRows,
      fetchDelayMs: opts.fetchDelayMs ?? 300,
    });
    result.researchMode = RESEARCH_MODES.SHADOW;

    // Opening corroboration for Medium Pipeline→Open
    const statusMat = (result.proposedCorrections || []).find(
      (c) => c.recommended_action === "Proposed Status Change" && c.confidenceBand === "Medium"
    );
    if (statusMat && hotel.currentStatus === "Pipeline") {
      const corr = await assessOpeningCorroborationFromOfficialPage(result.observation, {
        currentStatus: hotel.currentStatus,
      });
      result.openingCorroboration = corr;
      if (corr.upgraded && corr.band === "High") {
        statusMat.confidenceBand = "High";
        statusMat.queue = "proposed_high";
        statusMat.reason = `${statusMat.reason}; upgraded via opening corroboration: ${corr.reason}`;
        statusMat.corroboration = "opening_corroboration_upgrade";
      }
    }

    results.push(result);
  }

  const identityProposals = batchIdentityEnrichmentProposals(results);

  /** @type {object[]} */
  const digestItems = [];
  for (const r of results) {
    for (const c of [...(r.proposedCorrections || []), ...(r.reviewQueue || [])]) {
      if (["No Change", "Insufficient Evidence"].includes(c.recommended_action) && !r.reviewQueue?.includes(c)) {
        if (c.recommended_action === "No Change") continue;
      }
      if (c.recommended_action === "No Change") continue;
      if (c.recommended_action === "Insufficient Evidence" && !c.entityMatchLevel) continue;

      const item = {
        hotel_id: c.hotel_id,
        hotel_name: c.hotel_name,
        field: c.field,
        current_value: c.current_value,
        observed_value: c.observed_value,
        classification: c.classification,
        match_confidence: c.entityMatchLevel || r.entityMatch?.level,
        evidence: c.evidence || [],
        evidence_date: c.evidence?.[0]?.sourceDate || null,
        dealality_last_verified: r.hotel?.dealalityLastVerified || null,
        recommended_action: c.recommended_action,
        reason: c.reason,
        confidenceBand: c.confidenceBand || null,
        queue: c.queue || null,
      };
      item.fingerprint = claimFingerprint(item);
      if (
        ["Proposed Status Change", "Proposed Reflag", "Proposed Update", "Proposed Parent Correction"].includes(
          c.recommended_action
        ) &&
        (c.confidenceBand === "High" || c.queue === "proposed_high")
      ) {
        item.digestBucket = "high_confidence";
      } else if (c.recommended_action === "Review" || c.confidenceBand === "Medium" || c.queue === "review") {
        item.digestBucket = "review";
      } else if (c.recommended_action === "Insufficient Evidence") {
        item.digestBucket = "insufficient";
      } else {
        item.digestBucket = "other";
      }
      digestItems.push(item);
    }
  }

  const gaps = computeDirectoryGaps(hotels, dirs.ihgDirectoryRows || [], {
    brandFamily: opts.brandFamily || "ihg",
    countryFilter: opts.countryFilter || /Mexico/i,
    brandFilter: opts.brandFilter,
  });

  for (const g of gaps.missingCensusCandidates || []) {
    const item = {
      hotel_id: null,
      hotel_name: g.directoryName,
      field: "census_presence",
      current_value: null,
      observed_value: "Present in official directory",
      classification: g.classification,
      match_confidence: g.bestCensusMatch?.level || null,
      evidence: g.officialUrl ? [{ url: g.officialUrl }] : [],
      evidence_date: null,
      dealality_last_verified: null,
      recommended_action: "Missing Census Candidate",
      reason: "Official directory property not matched to Dealality census at Medium+",
      digestBucket: "directory_gap",
    };
    item.fingerprint = claimFingerprint(item);
    digestItems.push(item);
  }

  for (const g of gaps.censusNotInDirectory || []) {
    const item = {
      hotel_id: g.hotelId,
      hotel_name: g.hotelName,
      field: "directory_presence",
      current_value: g.currentStatus,
      observed_value: "Not found in expected official inventory",
      classification: g.classification,
      match_confidence: g.bestDirectoryMatch?.level || null,
      evidence: g.bestDirectoryMatch?.url ? [{ url: g.bestDirectoryMatch.url }] : [],
      evidence_date: null,
      dealality_last_verified: null,
      recommended_action: "Review",
      reason: g.note,
      digestBucket: "stale_candidate",
      possibleExplanations: g.possibleExplanations,
    };
    item.fingerprint = claimFingerprint(item);
    digestItems.push(item);
  }

  const actionable = digestItems.filter((d) => d.digestBucket !== "insufficient");
  const { surface, suppressed } = applyAlertDedup(shadowState, actionable, { suppressDays: 30 });

  const noChangeCount = results.filter(
    (r) =>
      !(r.proposedCorrections || []).some((c) =>
        ["Proposed Status Change", "Proposed Reflag", "Proposed Update"].includes(c.recommended_action)
      )
  ).length;

  return {
    researchMode: RESEARCH_MODES.SHADOW,
    generatedAt: new Date().toISOString(),
    elapsedMs: Date.now() - t0,
    hotelsChecked: hotels.length,
    noChangeCount,
    highConfidence: surface.filter((d) => d.digestBucket === "high_confidence"),
    reviewCandidates: surface.filter((d) => d.digestBucket === "review" || d.digestBucket === "other"),
    directoryGaps: surface.filter((d) => d.digestBucket === "directory_gap"),
    staleCandidates: surface.filter((d) => d.digestBucket === "stale_candidate"),
    suppressed,
    identityProposals,
    openingCorroboration: results
      .filter((r) => r.openingCorroboration)
      .map((r) => ({
        hotelId: r.hotel?.hotelId,
        hotelName: r.hotel?.name,
        ...r.openingCorroboration,
      })),
    results,
    rawGaps: gaps,
  };
}

/**
 * @param {object} digest
 */
export function formatShadowDigestMarkdown(digest) {
  const lines = [
    `# Shadow Monitoring Digest`,
    "",
    `Generated: ${digest.generatedAt}`,
    `Hotels checked: ${digest.hotelsChecked} · No-change: ${digest.noChangeCount} · Suppressed dupes: ${(digest.suppressed || []).length}`,
    `Elapsed: ${digest.elapsedMs} ms · Cost: $0`,
    "",
    `## High-confidence changes (${(digest.highConfidence || []).length})`,
    "",
  ];
  for (const i of digest.highConfidence || []) {
    lines.push(formatItem(i));
  }
  if (!(digest.highConfidence || []).length) lines.push("_None_");

  lines.push("", `## Review candidates (${(digest.reviewCandidates || []).length})`, "");
  for (const i of digest.reviewCandidates || []) lines.push(formatItem(i));
  if (!(digest.reviewCandidates || []).length) lines.push("_None_");

  lines.push("", `## Directory gaps (${(digest.directoryGaps || []).length})`, "");
  for (const i of digest.directoryGaps || []) lines.push(formatItem(i));
  if (!(digest.directoryGaps || []).length) lines.push("_None_");

  lines.push("", `## Potential stale Dealality records (${(digest.staleCandidates || []).length})`, "");
  lines.push("_Not auto-classified as closed._", "");
  for (const i of digest.staleCandidates || []) lines.push(formatItem(i));
  if (!(digest.staleCandidates || []).length) lines.push("_None_");

  lines.push("", "## No-change summary", "");
  lines.push(`${digest.noChangeCount} hotels produced no material proposed change.`);
  return lines.join("\n");
}

function formatItem(i) {
  return [
    `### ${i.hotel_name || "(directory)"}`,
    `- Field: \`${i.field}\``,
    `- Dealality: \`${i.current_value}\` → Observed: \`${i.observed_value}\``,
    `- Classification: ${i.classification}`,
    `- Match confidence: ${i.match_confidence}`,
    `- Evidence: ${i.evidence?.[0]?.url || "_none_"}`,
    `- Evidence date: ${i.evidence_date || "_unknown_"}`,
    `- Last Dealality verification: ${i.dealality_last_verified || "_unknown_"}`,
    `- Recommended action: **${i.recommended_action}**`,
    `- Reason: ${i.reason}`,
    "",
  ].join("\n");
}
