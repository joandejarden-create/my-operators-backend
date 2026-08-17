/**
 * Local GIATA Drive incremental sync state (read-only staging).
 * Does NOT mutate production census.
 *
 * deletedUrls → GIATA_OPEN_CONTENT_REMOVED (not "hotel closed").
 */

import path from "node:path";
import { readJsonFile, writeJsonFile, ensureDir } from "../local-store.js";
import { giataIdFromUrl } from "../../research-engine-v2/providers/giata-drive/index.js";

export const GIATA_SYNC_VERSION = "giata-drive-sync-state-v1";
export const GIATA_OPEN_CONTENT_REMOVED = "GIATA_OPEN_CONTENT_REMOVED";

const EMPTY_STATE = () => ({
  version: 1,
  latest_giata_revision: null,
  last_giata_sync_at: null,
  known_urls: {},
  events: [],
});

/**
 * @param {{ root?: string, store?: { root: string } }} [opts]
 */
export function createGiataDriveSyncStore(opts = {}) {
  const root = opts.root || opts.store?.root;
  if (!root) throw new Error("giata_sync_root_required");
  const filePath = path.join(root, "giata-drive-sync-state.json");
  ensureDir(root);

  function load() {
    return readJsonFile(filePath, EMPTY_STATE());
  }

  function save(state) {
    writeJsonFile(filePath, state);
  }

  /**
   * Apply an index response (urls / deletedUrls / latestRevision).
   * Stages local events only — no Airtable / census writes.
   */
  function applyIndexSnapshot(index = {}, optsApply = {}) {
    const state = load();
    const now = new Date().toISOString();
    const urls = Array.isArray(index.urls) ? index.urls : [];
    const deletedUrls = Array.isArray(index.deletedUrls)
      ? index.deletedUrls
      : Array.isArray(index.deleted_urls)
        ? index.deleted_urls
        : [];
    const latestRevision =
      index.latestRevision != null
        ? String(index.latestRevision)
        : index.latest_revision != null
          ? String(index.latest_revision)
          : state.latest_giata_revision;

    const prev = state.known_urls || {};
    const next = { ...prev };
    const events = [];

    for (const u of urls) {
      const url = String(u);
      const giataId = giataIdFromUrl(url);
      if (!prev[url]) {
        events.push({
          type: "GIATA_PROPERTY_NEW",
          url,
          giata_id: giataId,
          observed_at: now,
        });
      } else if (
        latestRevision &&
        prev[url].revision &&
        prev[url].revision !== latestRevision
      ) {
        events.push({
          type: "GIATA_PROPERTY_CHANGED",
          url,
          giata_id: giataId,
          observed_at: now,
          previous_revision: prev[url].revision,
          latest_revision: latestRevision,
        });
      }
      next[url] = {
        giata_id: giataId,
        revision: latestRevision,
        last_seen_at: now,
      };
    }

    for (const u of deletedUrls) {
      const url = String(u);
      const giataId = giataIdFromUrl(url);
      events.push({
        type: GIATA_OPEN_CONTENT_REMOVED,
        url,
        giata_id: giataId,
        observed_at: now,
        note: "Property left Open Content — does NOT imply hotel closed",
        hotel_status_changed: false,
      });
      if (next[url]) delete next[url];
    }

    state.known_urls = next;
    state.latest_giata_revision = latestRevision;
    state.last_giata_sync_at = now;
    state.events = [...(state.events || []), ...events].slice(-500);
    if (!optsApply.dry_run) save(state);

    return {
      latest_giata_revision: latestRevision,
      last_giata_sync_at: now,
      new: events.filter((e) => e.type === "GIATA_PROPERTY_NEW").length,
      changed: events.filter((e) => e.type === "GIATA_PROPERTY_CHANGED").length,
      deleted_open_content_urls: events.filter(
        (e) => e.type === GIATA_OPEN_CONTENT_REMOVED
      ).length,
      events,
      hotel_status_auto_changed: false,
    };
  }

  return {
    version: GIATA_SYNC_VERSION,
    filePath,
    load,
    save,
    applyIndexSnapshot,
    GIATA_OPEN_CONTENT_REMOVED,
  };
}
