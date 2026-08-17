/**
 * Provider registry — orchestration picks which providers to call.
 */

import { createHotelbedsProvider } from "./hotelbeds.js";
import { createCensusReadProvider } from "./census-read.js";
import { createStayingApiProvider } from "./stayingapi.js";
import { createSerpApiProvider } from "./serpapi.js";
import { createGiataDriveProvider } from "./giata-drive.js";
import { createTripadvisorApifyProvider } from "./tripadvisor-apify.js";
import { MAP_PROVIDER_IDS } from "../map_hotel_intelligence_fields.js";

export const PROVIDER_REGISTRY_VERSION = "hotel-intelligence-provider-registry-v1";

/**
 * @param {object} [opts]
 */
export function createProviderRegistry(opts = {}) {
  const census = opts.census || createCensusReadProvider(opts);
  const hotelbeds =
    opts.hotelbeds ||
    createHotelbedsProvider({
      env: opts.env,
      forceEnabled: opts.forceHotelbeds,
      ...opts.hotelbedsOpts,
    });
  const stayingapi =
    opts.stayingapi ||
    createStayingApiProvider({
      env: opts.env,
      forceEnabled: opts.forceStayingapi,
      creditCeiling: opts.stayingCreditCeiling,
      tracker: opts.stayingTracker,
      ...opts.stayingapiOpts,
    });
  const serpapi =
    opts.serpapi ||
    createSerpApiProvider({
      env: opts.env,
      forceEnabled: opts.forceSerpapi,
      creditCeiling: opts.serpCreditCeiling,
      tracker: opts.serpTracker,
      ...opts.serpapiOpts,
    });
  const giata_drive =
    opts.giata_drive ||
    createGiataDriveProvider({
      env: opts.env,
      forceEnabled: opts.forceGiataDrive,
      ...opts.giataDriveOpts,
    });
  const tripadvisor_apify =
    opts.tripadvisor_apify ||
    createTripadvisorApifyProvider({
      env: opts.env,
      forceEnabled: opts.forceTripadvisorApify,
      pool: opts.tripadvisorPool,
      ...opts.tripadvisorApifyOpts,
    });

  const byId = {
    [MAP_PROVIDER_IDS.census]: census,
    [MAP_PROVIDER_IDS.hotelbeds]: hotelbeds,
    [MAP_PROVIDER_IDS.stayingapi]: stayingapi,
    [MAP_PROVIDER_IDS.serpapi]: serpapi,
    [MAP_PROVIDER_IDS.giata_drive]: giata_drive,
    [MAP_PROVIDER_IDS.tripadvisor_apify]: tripadvisor_apify,
  };

  function get(providerId) {
    return byId[String(providerId || "").trim()] || null;
  }

  function list() {
    return Object.keys(byId);
  }

  async function availability() {
    const out = {};
    for (const [id, p] of Object.entries(byId)) {
      try {
        out[id] = await p.getAvailabilityStatus();
      } catch (err) {
        out[id] = {
          provider: id,
          status: "unavailable",
          retryable: true,
          message: String(err?.message || err).slice(0, 120),
        };
      }
    }
    return out;
  }

  return {
    version: PROVIDER_REGISTRY_VERSION,
    get,
    list,
    availability,
    census,
    hotelbeds,
    stayingapi,
    serpapi,
    giata_drive,
    tripadvisor_apify,
  };
}
