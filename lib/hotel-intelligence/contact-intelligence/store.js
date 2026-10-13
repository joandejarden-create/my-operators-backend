/**
 * Contact Intelligence store — file-backed with atomic writes (HI local-store pattern).
 * No Airtable ownership writes. Not a new database.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import { writeJsonFile, readJsonFile, ensureDir } from "../local-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const STORE_VERSION = "contact-intelligence-store-v1.3";

function nowIso() {
  return new Date().toISOString();
}

function defaultRoot(env = process.env) {
  if (env.CONTACT_INTELLIGENCE_DATA_ROOT) {
    return path.resolve(String(env.CONTACT_INTELLIGENCE_DATA_ROOT));
  }
  if (env.HOTEL_INTELLIGENCE_DATA_DIR) {
    return path.join(path.resolve(String(env.HOTEL_INTELLIGENCE_DATA_DIR)), "contact-intelligence");
  }
  return path.join(ROOT, "data", "hotel-intelligence", "contact-intelligence");
}

export function createContactStore(opts = {}) {
  const root = opts.root || defaultRoot(opts.env || process.env);
  ensureDir(root);
  ensureDir(path.join(root, "hotels"));
  ensureDir(path.join(root, "owners"));
  ensureDir(path.join(root, "jobs"));

  function hotelPath(hotelId) {
    const safe = String(hotelId || "").replace(/[^a-zA-Z0-9_-]/g, "_");
    return path.join(root, "hotels", `${safe}.json`);
  }

  function ownerPath(ownerId) {
    const safe = String(ownerId || "").replace(/[^a-zA-Z0-9_-]/g, "_");
    return path.join(root, "owners", `${safe}.json`);
  }

  function jobsIndexPath() {
    return path.join(root, "jobs", "index.json");
  }

  function getHotelPackage(hotelId) {
    return readJsonFile(hotelPath(hotelId), null);
  }

  function putHotelPackage(pkg, writeOpts = {}) {
    if (!pkg?.hotel_id) throw new Error("hotel_id_required");
    const existing = getHotelPackage(pkg.hotel_id);
    if (
      writeOpts.if_updated_before &&
      existing?.updated_at &&
      String(existing.updated_at) > String(writeOpts.if_updated_before)
    ) {
      return {
        ok: false,
        conflict: true,
        reason: "newer_record_exists",
        existing_updated_at: existing.updated_at,
        package: existing,
      };
    }
    const history = Array.isArray(existing?.verification_history)
      ? existing.verification_history.slice()
      : [];
    history.push({
      event_id: `vh_${crypto.randomBytes(4).toString("hex")}`,
      at: nowIso(),
      kind: pkg.refresh_status || "UPSERT",
      summary: pkg.provenance?.summary || "contact_package_upsert",
      unresolved_reasons: pkg.unresolved_reasons || [],
    });
    const next = {
      ...(existing || {}),
      ...pkg,
      version: STORE_VERSION,
      updated_at: nowIso(),
      verification_history: history.slice(-50),
      restrictions: pkg.restrictions || existing?.restrictions || null,
    };
    writeJsonFile(hotelPath(pkg.hotel_id), next);
    // Backward compatible: default callers receive the package object.
    if (writeOpts.if_updated_before) return { ok: true, conflict: false, package: next };
    return next;
  }

  function getOwnerRoute(ownerId) {
    return readJsonFile(ownerPath(ownerId), null);
  }

  function putOwnerRoute(route, writeOpts = {}) {
    if (!route?.owner_entity_id) throw new Error("owner_entity_id_required");
    const existing = getOwnerRoute(route.owner_entity_id);
    if (
      writeOpts.if_updated_before &&
      existing?.updated_at &&
      String(existing.updated_at) > String(writeOpts.if_updated_before)
    ) {
      return {
        ok: false,
        conflict: true,
        reason: "newer_record_exists",
        existing_updated_at: existing.updated_at,
        route: existing,
      };
    }
    const next = {
      ...(existing || {}),
      ...route,
      version: STORE_VERSION,
      updated_at: nowIso(),
    };
    writeJsonFile(ownerPath(route.owner_entity_id), next);
    if (writeOpts.if_updated_before) return { ok: true, conflict: false, route: next };
    return next;
  }

  function readJobsIndex() {
    return readJsonFile(jobsIndexPath(), {
      version: STORE_VERSION,
      jobs: [],
      updated_at: null,
    });
  }

  function writeJobsIndex(idx) {
    idx.updated_at = nowIso();
    writeJsonFile(jobsIndexPath(), idx);
  }

  function enqueueJob(partial = {}) {
    const hotelId = String(partial.hotel_id || "").trim();
    if (!hotelId) throw new Error("hotel_id_required");
    const templateId = String(partial.template_id || "CONTACT_REFRESH").trim().toUpperCase();
    const idx = readJobsIndex();

    if (partial.idempotency_token) {
      const token = String(partial.idempotency_token);
      const existing = idx.jobs.find((j) => j.idempotency_token === token);
      if (existing) return { job: structuredClone(existing), deduped: true, reason: "idempotency_token" };
    }

    const active = idx.jobs.find(
      (j) =>
        j.hotel_id === hotelId &&
        j.template_id === templateId &&
        (j.status === "QUEUED" || j.status === "RUNNING")
    );
    if (active && !partial.allow_duplicate_active) {
      return { job: structuredClone(active), deduped: true, reason: "active_job_exists" };
    }

    const job = {
      job_id: partial.job_id || `cjob_${crypto.randomBytes(6).toString("hex")}`,
      hotel_id: hotelId,
      owner_entity_id: partial.owner_entity_id || null,
      template_id: templateId,
      status: "QUEUED",
      provider_strategy: partial.provider_strategy || "NATIVE",
      paid: partial.paid === true,
      idempotency_token: partial.idempotency_token || null,
      created_at: nowIso(),
      started_at: null,
      completed_at: null,
      result_summary: null,
      error: null,
    };
    idx.jobs.push(job);
    writeJobsIndex(idx);
    return { job: structuredClone(job), deduped: false, reason: null };
  }

  function updateJob(jobId, patch = {}) {
    const idx = readJobsIndex();
    const i = idx.jobs.findIndex((j) => j.job_id === jobId);
    if (i < 0) return null;
    idx.jobs[i] = { ...idx.jobs[i], ...patch };
    writeJobsIndex(idx);
    return structuredClone(idx.jobs[i]);
  }

  function listJobs({ hotel_id = null, status = null } = {}) {
    let jobs = readJobsIndex().jobs || [];
    if (hotel_id) jobs = jobs.filter((j) => j.hotel_id === hotel_id);
    if (status) jobs = jobs.filter((j) => j.status === status);
    return jobs.map((j) => structuredClone(j));
  }

  return {
    root,
    version: STORE_VERSION,
    durability: {
      class: "HI_LOCAL_ATOMIC_FILE_STORE",
      note:
        "Atomic tmp→copy writes via hotel-intelligence/local-store. Survives process restart on persistent volumes. Ephemeral deploy filesystems still lose data. Optimistic if_updated_before guard only — not a distributed lock.",
    },
    getHotelPackage,
    putHotelPackage,
    getOwnerRoute,
    putOwnerRoute,
    enqueueJob,
    updateJob,
    listJobs,
  };
}

export function migrateContactStorePackages({ sourceRoot, targetStore, dryRun = true } = {}) {
  const src = path.resolve(sourceRoot);
  const report = { dry_run: dryRun, hotels: [], owners: [], skipped_newer: [], errors: [] };
  for (const kind of ["hotels", "owners"]) {
    const dir = path.join(src, kind);
    if (!fs.existsSync(dir)) continue;
    for (const file of fs.readdirSync(dir).filter((f) => f.endsWith(".json"))) {
      try {
        const data = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
        if (kind === "hotels") {
          if (!dryRun) {
            const res = targetStore.putHotelPackage(data, {
              if_updated_before: data.updated_at || "1970-01-01T00:00:00.000Z",
            });
            if (res.conflict) report.skipped_newer.push({ kind, file, reason: res.reason });
            else report.hotels.push(data.hotel_id);
          } else report.hotels.push(data.hotel_id);
        } else if (!dryRun) {
          const res = targetStore.putOwnerRoute(data, {
            if_updated_before: data.updated_at || "1970-01-01T00:00:00.000Z",
          });
          if (res.conflict) report.skipped_newer.push({ kind, file, reason: res.reason });
          else report.owners.push(data.owner_entity_id);
        } else report.owners.push(data.owner_entity_id);
      } catch (err) {
        report.errors.push({ kind, file, error: String(err?.message || err).slice(0, 160) });
      }
    }
  }
  return report;
}
