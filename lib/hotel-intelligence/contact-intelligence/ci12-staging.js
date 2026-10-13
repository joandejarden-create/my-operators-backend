/**
 * Contact Intelligence V1.1 — canonical CI12 staging under the CI store root.
 * Path: data/hotel-intelligence/contact-intelligence/ci12/
 * Does not write Census / OCG / Explorer production.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { writeJsonFile, readJsonFile, ensureDir } from "../local-store.js";
import { createContactStore } from "./store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const CI12_STAGING_VERSION = "ci12-staging-v1.1";

export const STAGING_STORE_CONSOLIDATION = Object.freeze({
  canonical_root: "data/hotel-intelligence/contact-intelligence",
  ci12_subdir: "ci12",
  hotels: "hotels/",
  owners: "owners/",
  legacy_not_deleted: [
    "data/group-demand-intelligence/hotels/*/canonical-contacts.json",
    "owner-contact-resolution-v2 cache nested under owners/",
  ],
  production_writes: false,
  census_writes: false,
  ocg_writes: false,
  explorer_production_writes: false,
});

export function resolveCi12StagingRoot(env = process.env) {
  const store = createContactStore({ env });
  const ci12 = path.join(store.root, "ci12");
  ensureDir(ci12);
  ensureDir(path.join(ci12, "cases"));
  ensureDir(path.join(ci12, "owners"));
  return { storeRoot: store.root, ci12Root: ci12, store };
}

export function createCi12Staging(opts = {}) {
  const { storeRoot, ci12Root, store } = resolveCi12StagingRoot(opts.env);
  const docsMirror = path.resolve(
    ROOT,
    "docs/data-intelligence/contact-intelligence/CI12"
  );

  function casePath(caseId) {
    return path.join(ci12Root, "cases", `${caseId}.json`);
  }

  function putCaseResult(caseId, payload) {
    const next = {
      ...payload,
      case_id: caseId,
      staging_version: CI12_STAGING_VERSION,
      production_write: false,
      updated_at: new Date().toISOString(),
    };
    writeJsonFile(casePath(caseId), next);
    // Also mirror into hotel package store when hotel_id present (staging flag)
    if (next.hotel_id) {
      store.putHotelPackage({
        hotel_id: next.hotel_id,
        hotel_name: next.hotel_name || null,
        owner_entity_id: next.owner_entity_id || null,
        refresh_status: "CI12_STAGING",
        provenance: {
          summary: "ci12_staging_only",
          ci12_case_id: caseId,
          production_write: false,
        },
        hotel_contact: next.hotel_contacts || { channels: [] },
        organization_contact_route: next.organization_contacts || { channels: [] },
        people: next.people || [],
        unresolved_reasons: next.unresolved_reasons || [],
        write_guarantees: {
          production_auto_writes: 0,
          census_owner_mutations: 0,
          outreach_sent: 0,
        },
        ci12: next,
      });
    }
    if (next.owner_entity_id) {
      store.putOwnerRoute({
        owner_entity_id: next.owner_entity_id,
        owner_display_name: next.owner_organization || null,
        ci12_reuse: true,
        channels: next.organization_contacts?.channels || [],
        provenance: { ci12_case_id: caseId, production_write: false },
      });
    }
    return next;
  }

  function getCaseResult(caseId) {
    return readJsonFile(casePath(caseId), null);
  }

  function listCaseResults() {
    const dir = path.join(ci12Root, "cases");
    if (!fs.existsSync(dir)) return [];
    return fs
      .readdirSync(dir)
      .filter((f) => f.endsWith(".json"))
      .map((f) => readJsonFile(path.join(dir, f), null))
      .filter(Boolean);
  }

  function writeArtifact(name, data) {
    ensureDir(docsMirror);
    const p = path.join(docsMirror, name);
    writeJsonFile(p, data);
    // dual-write under data staging
    writeJsonFile(path.join(ci12Root, name), data);
    return p;
  }

  return {
    version: CI12_STAGING_VERSION,
    storeRoot,
    ci12Root,
    docsMirror,
    store,
    consolidation: STAGING_STORE_CONSOLIDATION,
    putCaseResult,
    getCaseResult,
    listCaseResults,
    writeArtifact,
  };
}
