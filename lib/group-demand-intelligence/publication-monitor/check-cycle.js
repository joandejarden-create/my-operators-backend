/**
 * One publication-monitor check cycle.
 * Primary: fetch known official URLs only.
 * Bounded search rediscovery ONLY if source unreachable/moved (opt-in flag).
 * Never Apify. Never broad market discovery.
 */

import {
  MONITOR_STATUS,
  REDECOMPOSITION_STATUS,
  CURRENT_SOURCE_STATE,
  CHANGE_CLASS,
} from "./constants.js";
import { classifySemanticChange, normalizeMonitorContent } from "./change-detection.js";
import { computeCheckSchedule, isDueForCheck } from "./schedule.js";
import {
  upsertPublicationMonitors,
  appendMonitorAudit,
} from "./store.js";
// Dynamic import of trigger-redecomp inside meaningful branch — avoids ESM cycle via GDI index.

async function fetchKnownSource(url) {
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept: "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(25000),
    });
    const ct = res.headers.get("content-type") || "";
    const buf = Buffer.from(await res.arrayBuffer());
    const isPdf = /pdf/i.test(ct) || /\.pdf(\?|$)/i.test(url);
    const text = isPdf
      ? buf.toString("latin1").replace(/[^\x09\x0A\x0D\x20-\x7E\u00A0-\u00FF]/g, " ").slice(0, 80000)
      : buf.toString("utf8").slice(0, 400000);
    return {
      ok: res.ok,
      status: res.status,
      url: res.url || url,
      contentType: ct,
      text,
      isPdf,
      finalUrl: res.url || url,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      error: err?.message || String(err),
    };
  }
}

/**
 * Run due monitors for a hotel (or a single monitorId).
 * @param {string} hotelId
 * @param {object} opts
 * @param {boolean} [opts.force=false] — check even if not due
 * @param {boolean} [opts.dryRun=false] — classify only; no redecomp persist
 * @param {boolean} [opts.allowBoundedRediscovery=false] — if source 404, do NOT SERP unless true
 * @param {string} [opts.monitorId]
 * @param {Date|string} [opts.now]
 */
export async function runPublicationMonitorCycle(hotelId, opts = {}) {
  const { loadPublicationMonitors } = await import("./store.js");
  const doc = loadPublicationMonitors(hotelId);
  const now = opts.now ? new Date(opts.now) : new Date();
  const results = [];

  let monitors = doc.monitors || [];
  if (opts.monitorId) {
    monitors = monitors.filter((m) => m.monitorId === opts.monitorId);
  }

  for (const monitor of monitors) {
    if (
      [MONITOR_STATUS.PAUSED, MONITOR_STATUS.COMPLETED, MONITOR_STATUS.EXPIRED].includes(
        monitor.monitoringStatus
      )
    ) {
      results.push({
        monitorId: monitor.monitorId,
        skipped: true,
        reason: `status_${monitor.monitoringStatus}`,
      });
      continue;
    }

    if (!opts.force && !isDueForCheck(monitor, now)) {
      results.push({
        monitorId: monitor.monitorId,
        skipped: true,
        reason: "not_due",
        nextCheckAt: monitor.nextCheckAt,
      });
      continue;
    }

    const cycleResult = await checkOneMonitor(monitor, {
      dryRun: opts.dryRun === true,
      allowBoundedRediscovery: opts.allowBoundedRediscovery === true,
      now,
    });
    results.push(cycleResult);
  }

  return {
    hotelId,
    checkedAt: now.toISOString(),
    results,
    triggered: results.filter((r) => r.triggerFired).length,
    redecompositions: results.filter((r) => r.decompositionInvoked).length,
  };
}

async function checkOneMonitor(monitor, opts = {}) {
  const now = opts.now || new Date();
  const urls = [
    monitor.triggerSourceUrl,
    ...(Array.isArray(monitor.alternateSourceUrls) ? monitor.alternateSourceUrls : []),
  ].filter(Boolean);

  let page = null;
  let fetchErrors = [];
  for (const url of urls) {
    page = await fetchKnownSource(url);
    if (page.ok && page.text) break;
    fetchErrors.push({ url, status: page.status, error: page.error || null });
  }

  if (!page?.ok || !page.text) {
    const schedule = computeCheckSchedule(monitor, now);
    const updated = {
      ...monitor,
      lastCheckedAt: now.toISOString(),
      currentSourceState: CURRENT_SOURCE_STATE.SOURCE_UNREACHABLE,
      nextCheckAt: schedule.nextCheckAt,
      checkFrequency: schedule.frequency,
      scheduleRationale: schedule.scheduleRationale,
      notes: `${monitor.notes || ""} | Source unreachable at check ${now.toISOString().slice(0, 10)}`.trim(),
      // Bounded rediscovery explicitly NOT run unless allowBoundedRediscovery — logged only
      boundedRediscoveryNeeded: true,
      boundedRediscoveryAllowed: opts.allowBoundedRediscovery === true,
    };
    if (!opts.dryRun) {
      upsertPublicationMonitors(monitor.hotelId, [updated], {
        note: "Monitor check — source unreachable",
      });
      appendMonitorAudit(monitor.hotelId, {
        monitorId: monitor.monitorId,
        campaignId: monitor.campaignId,
        source: monitor.triggerSourceUrl,
        result: "SOURCE_UNREACHABLE",
        contentChanged: false,
        meaningfulChange: false,
        triggerFired: false,
        newEvidenceCount: 0,
        decompositionInvoked: false,
        fetchErrors,
      });
    }
    return {
      monitorId: monitor.monitorId,
      campaignId: monitor.campaignId,
      triggerFired: false,
      meaningfulChange: false,
      decompositionInvoked: false,
      result: "SOURCE_UNREACHABLE",
      fetchErrors,
    };
  }

  const classification = classifySemanticChange({
    prior: {
      lastContentHash: monitor.lastContentHash,
      lastNormalizedSnippet: monitor.lastNormalizedSnippet,
      fingerprint: monitor.lastFingerprint || null,
      url: page.url,
    },
    nextText: page.text,
    nextHtml: page.isPdf ? "" : page.text,
    watchForTypes: monitor.watchForTypes || [monitor.triggerType],
    priorArtifactLinks: monitor.lastArtifactLinks || [],
  });

  const schedule = computeCheckSchedule(
    {
      ...monitor,
      monitoringStatus:
        classification.meaningful && classification.changeClass !== CHANGE_CLASS.NONE
          ? MONITOR_STATUS.TRIGGERED
          : monitor.monitoringStatus,
    },
    now
  );

  let redecomp = {
    invoked: false,
    status: REDECOMPOSITION_STATUS.IDLE,
    newEntityIds: [],
    newAccounts: 0,
    newReady: 0,
    newWatch: 0,
  };

  if (classification.meaningful && !opts.dryRun) {
    const { invokeRedecompositionOnTrigger } = await import("./trigger-redecomp.js");
    redecomp = await invokeRedecompositionOnTrigger(monitor, classification, {
      pageText: page.text,
      pageUrl: page.finalUrl || page.url,
      now,
    });
  } else if (classification.meaningful && opts.dryRun) {
    redecomp = {
      invoked: false,
      status: REDECOMPOSITION_STATUS.QUEUED,
      dryRun: true,
      newEntityIds: [],
      newAccounts: 0,
      newReady: 0,
      newWatch: 0,
    };
  }

  const snippet = normalizeMonitorContent(page.text).slice(0, 1200);
  const historyEntry = {
    at: now.toISOString(),
    result: classification.reason,
    changeClass: classification.changeClass,
    meaningful: classification.meaningful,
    triggerFired: classification.meaningful,
    primaryTriggerType: classification.primaryTriggerType || null,
    newArtifacts: classification.newArtifacts || [],
    decompositionInvoked: redecomp.invoked === true,
  };

  const updated = {
    ...monitor,
    lastCheckedAt: now.toISOString(),
    lastContentHash: classification.contentHash,
    lastFingerprint: classification.fingerprint,
    lastNormalizedSnippet: snippet,
    lastArtifactLinks: classification.artifactLinks || [],
    lastMeaningfulChangeAt: classification.meaningful
      ? now.toISOString()
      : monitor.lastMeaningfulChangeAt || null,
    detectedArtifactType: classification.primaryTriggerType || monitor.detectedArtifactType || null,
    currentSourceState: classification.meaningful
      ? CURRENT_SOURCE_STATE.LIST_PARTIAL
      : monitor.currentSourceState || CURRENT_SOURCE_STATE.LIST_NOT_YET_PUBLISHED,
    monitoringStatus: classification.meaningful
      ? MONITOR_STATUS.TRIGGERED
      : schedule.monitoringStatus || MONITOR_STATUS.ACTIVE,
    nextCheckAt: schedule.nextCheckAt,
    checkFrequency: schedule.frequency,
    scheduleRationale: schedule.scheduleRationale,
    reDecompositionStatus: redecomp.status,
    previouslySeenEntityIds: [
      ...new Set([
        ...(monitor.previouslySeenEntityIds || []),
        ...(redecomp.newEntityIds || []),
      ]),
    ],
    checkHistory: [...(monitor.checkHistory || []).slice(-40), historyEntry],
  };

  // After successful redecomp with new entities, mark COMPLETED only if list-class trigger
  if (
    redecomp.invoked &&
    redecomp.newAccounts > 0 &&
    /LIST|PAPERS|PROGRAMME|MANUAL/i.test(String(classification.primaryTriggerType || ""))
  ) {
    updated.monitoringStatus = MONITOR_STATUS.TRIGGERED; // stay TRIGGERED until human/ops close; don't auto COMPLETE
    updated.currentSourceState = CURRENT_SOURCE_STATE.LIST_AVAILABLE;
  }

  if (!opts.dryRun) {
    upsertPublicationMonitors(monitor.hotelId, [updated], {
      note: "Publication monitor check cycle",
    });
    appendMonitorAudit(monitor.hotelId, {
      monitorId: monitor.monitorId,
      campaignId: monitor.campaignId,
      source: page.finalUrl || page.url,
      result: classification.reason,
      contentChanged: classification.changeClass !== CHANGE_CLASS.NONE,
      meaningfulChange: classification.meaningful,
      triggerFired: classification.meaningful,
      changeClass: classification.changeClass,
      primaryTriggerType: classification.primaryTriggerType || null,
      newEvidenceCount: (classification.newArtifacts || []).length,
      decompositionInvoked: redecomp.invoked === true,
      newAccounts: redecomp.newAccounts || 0,
      newReady: redecomp.newReady || 0,
      newWatch: redecomp.newWatch || 0,
    });
  }

  return {
    monitorId: monitor.monitorId,
    campaignId: monitor.campaignId,
    campaignKey: monitor.campaignKey,
    triggerFired: classification.meaningful,
    meaningfulChange: classification.meaningful,
    changeClass: classification.changeClass,
    reason: classification.reason,
    primaryTriggerType: classification.primaryTriggerType || null,
    decompositionInvoked: redecomp.invoked === true,
    redecomp,
    nextCheckAt: updated.nextCheckAt,
    dryRun: opts.dryRun === true,
  };
}

export { fetchKnownSource, checkOneMonitor };
