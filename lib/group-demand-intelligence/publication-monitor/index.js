/**
 * GDI Publication Trigger Monitor V1 — public API.
 */

export * from "./constants.js";
export * from "./multilingual-terms.js";
export * from "./change-detection.js";
export * from "./schedule.js";
export * from "./store.js";
export * from "./seed-five-campaigns.js";
export * from "./customer-status.js";
export {
  runPublicationMonitorCycle,
  fetchKnownSource,
  checkOneMonitor,
} from "./check-cycle.js";
export {
  invokeRedecompositionOnTrigger,
  extractNamedEntitiesFromPublishedText,
} from "./trigger-redecomp.js";

import { upsertPublicationMonitors, loadPublicationMonitors } from "./store.js";
import { buildFiveCampaignMonitorSeeds, seedMonitorsForHotel } from "./seed-five-campaigns.js";
import { computeCheckSchedule } from "./schedule.js";
import { MONITOR_STATUS } from "./constants.js";

/**
 * Seed / refresh the five frozen campaign monitors without wiping check history.
 */
export function ensureFiveCampaignPublicationMonitors(opts = {}) {
  const now = opts.now ? new Date(opts.now) : new Date();
  const seeds = buildFiveCampaignMonitorSeeds(now);
  const byHotel = new Map();
  for (const s of seeds) {
    if (!byHotel.has(s.hotelId)) byHotel.set(s.hotelId, []);
    byHotel.get(s.hotelId).push(s);
  }
  const out = [];
  for (const [hotelId, list] of byHotel) {
    const existing = loadPublicationMonitors(hotelId);
    const byId = new Map(existing.monitors.map((m) => [m.monitorId, m]));
    const rows = list.map((seed) => {
      const prev = byId.get(seed.monitorId) || {};
      const merged = {
        ...seed,
        ...prev,
        // Seed fields that should refresh from canonical definition
        triggerType: seed.triggerType,
        watchForTypes: seed.watchForTypes,
        triggerSourceUrl: seed.triggerSourceUrl,
        alternateSourceUrls: seed.alternateSourceUrls,
        expectedPublicationWindowStart: seed.expectedPublicationWindowStart,
        expectedPublicationWindowEnd: seed.expectedPublicationWindowEnd,
        eventEndDate: seed.eventEndDate,
        milestoneDates: seed.milestoneDates,
        priorityRank: seed.priorityRank,
        customerMonitoringFor: seed.customerMonitoringFor,
        autoAmericasSpecial: seed.autoAmericasSpecial,
        notes: seed.notes,
        campaignId: seed.campaignId,
        campaignKey: seed.campaignKey,
        hotelId: seed.hotelId,
        monitorId: seed.monitorId,
        monitoringStatus: prev.monitoringStatus || seed.monitoringStatus || MONITOR_STATUS.ACTIVE,
        previouslySeenEntityIds: prev.previouslySeenEntityIds || [],
        checkHistory: prev.checkHistory || [],
        lastContentHash: prev.lastContentHash || null,
        lastCheckedAt: prev.lastCheckedAt || null,
      };
      const schedule = computeCheckSchedule(merged, now);
      return {
        ...merged,
        nextCheckAt: prev.nextCheckAt || schedule.nextCheckAt,
        checkFrequency: schedule.frequency,
        scheduleRationale: schedule.scheduleRationale,
        monitoringStatus:
          schedule.monitoringStatus === MONITOR_STATUS.EXPIRED
            ? MONITOR_STATUS.EXPIRED
            : merged.monitoringStatus,
      };
    });
    const doc = upsertPublicationMonitors(hotelId, rows, {
      note: opts.note || "Ensure five-campaign publication monitors",
    });
    out.push(doc);
  }
  return {
    hotels: out,
    monitors: seeds.map((s) => s.monitorId),
    count: seeds.length,
  };
}

export { seedMonitorsForHotel };
