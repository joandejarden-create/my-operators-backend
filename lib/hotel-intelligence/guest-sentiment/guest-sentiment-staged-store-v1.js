/**
 * Staged Guest Sentiment packets (read-only file store).
 * Live production HE must not treat these as licensed feeds.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildGuestSentimentView } from "./guest-sentiment-model-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

/** @type {ReadonlyArray<{ hpcId: string, relPath: string }>} */
export const STAGED_GUEST_SENTIMENT_PACKETS = Object.freeze([
  {
    hpcId: "recLuxvwwxID7U2B8",
    relPath: "fixtures/hotel-explorer/guest-sentiment/beth-marriott-staged-v1.json",
  },
]);

export function loadStagedGuestSentimentPacket(hpcId) {
  const id = String(hpcId || "").trim();
  const entry = STAGED_GUEST_SENTIMENT_PACKETS.find((p) => p.hpcId === id);
  if (!entry) return null;
  const abs = path.join(ROOT, entry.relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    const raw = JSON.parse(fs.readFileSync(abs, "utf8"));
    return { packet: raw, path: entry.relPath };
  } catch (err) {
    console.warn("[guest-sentiment] staged packet read failed", err?.message || err);
    return null;
  }
}

/**
 * @param {{ id?: string, name?: string }} hotel
 * @param {{ founderPreview?: boolean, forceLiveAuthorize?: boolean }} [opts]
 */
export function resolveGuestSentimentForHotel(hotel = {}, opts = {}) {
  const hotelId = String(hotel.id || hotel.recordId || "").trim();
  const loaded = loadStagedGuestSentimentPacket(hotelId);
  if (!loaded) {
    return buildGuestSentimentView(hotel, { live_ui_authorized: false, observations: [] }, opts);
  }
  const packet = { ...loaded.packet };
  // Hard rule: never flip live authorization from staged files alone.
  if (opts.forceLiveAuthorize !== true) {
    packet.live_ui_authorized = false;
  }
  const view = buildGuestSentimentView(hotel, packet, {
    founderPreview: opts.founderPreview === true,
  });
  return {
    ...view,
    stagedPacketPath: loaded.path,
    applyAuthorized: packet.applyAuthorized === true,
  };
}

/**
 * Airtable dry-run rows (no writes).
 */
export function buildGuestSentimentAirtableDryRun(hpcId) {
  const loaded = loadStagedGuestSentimentPacket(hpcId);
  if (!loaded) {
    return {
      applyAuthorized: false,
      classification: "PROTOTYPE",
      hpcId,
      observations: [],
      themeScores: [],
      note: "No staged packet for hotel",
    };
  }
  const packet = loaded.packet;
  const observations = (packet.observations || []).map((o, idx) => ({
    action: "CREATE_CANDIDATE",
    table: "HI Hotel Reputation Observations",
    fields: {
      "HPC Hotel ID": [hpcId],
      Platform: o.platform,
      "Platform Property ID": o.platformPropertyId || null,
      "Listing URL": o.listingUrl || null,
      "Native Rating": o.rating,
      "Rating Scale Max": o.scaleMax,
      "Review Count": o.reviewCount,
      "Observed At": o.observedAt,
      "Sample Period": o.samplePeriod || null,
      "Collection Method": o.collectionMethod || null,
      "Rights Status": o.rightsStatus || "RIGHTS_REVIEW_REQUIRED",
      "Provenance Notes": o.provenance || null,
      "Dedupe Key": `${hpcId}|${o.platform}|${o.observedAt}|${o.platformPropertyId || idx}`,
    },
  }));
  const themeScores = [];
  for (const o of packet.observations || []) {
    for (const c of o.categoryScores || []) {
      themeScores.push({
        action: "CREATE_CANDIDATE",
        table: "HI Hotel Reputation Theme Scores",
        fields: {
          Observation: `(link → ${o.platform} @ ${o.observedAt})`,
          "Theme Label": c.label,
          Score: c.score,
          "Scale Max": c.scaleMax || o.scaleMax,
          "Evidence Note": "Platform category aggregate — not a review excerpt",
        },
      });
    }
  }
  return {
    applyAuthorized: false,
    classification: "PERSISTENCE_DESIGNED",
    hpcId,
    hotelName: packet.hotelName,
    sourcePacket: loaded.path,
    observations,
    themeScores,
    notes: [
      "No production Airtable writes in this dry-run.",
      "Do not store full copyrighted review text.",
      "Retain historical observation rows; never overwrite prior Observed At snapshots.",
    ],
  };
}
