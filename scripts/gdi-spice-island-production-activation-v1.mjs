#!/usr/bin/env node
/**
 * Spice Island GDI durable publication registry — production activation helpers.
 *
 * Stages (founder-authorized):
 *   --stage publish-local   Build idempotent registry tree from certified manifest
 *   --stage upload-volume   Upload tree to Railway volume (adp-durable-published)
 *   --stage issue-share     Issue gdishare.v1 using production secret (no secret echo)
 *   --stage verify-counts   Local strict-merge count check against published tree
 *
 * Does not email Sheldon. Does not enable feature flags (set via railway variables).
 */
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  createGdiPublishedInventoryRegistry,
  mergePublishedInventoryIntoOpportunities,
  COMMERCIAL_PUBLICATION_CLASS,
  PUBLICATION_STATE,
} from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { issueGdiShareCapability } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-spice-island-production-activation-v1");
const MANIFEST = path.join(
  ROOT,
  "reports/gdi-durable-publication-registry-spice-island-v1/02-spice-island-seven-id-publication-manifest.json"
);
const HOTEL = "recKRJjcPnb4tVDDS";
const EXCLUDED = "gdi_sc_f255eb5e9202";
const VOLUME = "adp-durable-published";
const REMOTE_ROOT = "/gdi-published-inventory";
const AUTH_PHRASE =
  "FOUNDER GO — GDI Durable Publication Registry and Spice Island Activation";

function arg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`) || process.argv.includes(`--stage=${name}`);
}

function stage() {
  return arg("stage") || (hasFlag("publish-local") ? "publish-local" : null);
}

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

function loadManifest() {
  const doc = JSON.parse(fs.readFileSync(MANIFEST, "utf8"));
  if (doc.hotelId !== HOTEL) throw new Error("manifest_hotel_mismatch");
  if (!Array.isArray(doc.rows) || doc.rows.length !== 7) {
    throw new Error(`manifest_row_count_${doc.rows?.length}`);
  }
  if (doc.rows.some((r) => r.opportunityId === EXCLUDED)) {
    throw new Error("excluded_aventa_in_manifest_rows");
  }
  return doc;
}

function approvalPayload() {
  return {
    authorized: true,
    authorizedBy: "founder",
    authorizedAt: new Date().toISOString(),
    authorizationPhrase: AUTH_PHRASE,
    note: "Exact seven Spice Island IDs; Aventa excluded; Airtable remains opportunity SoT",
  };
}

function publishLocal(stagingRoot) {
  const manifest = loadManifest();
  fs.mkdirSync(stagingRoot, { recursive: true });
  const reg = createGdiPublishedInventoryRegistry({ root: stagingRoot });
  const approval = approvalPayload();
  const results = [];

  for (const row of manifest.rows) {
    const snapEntry = (manifest.snapshots || []).find(
      (s) => s.opportunityId === row.opportunityId
    );
    const snapshot = snapEntry?.snapshot || {
      id: row.opportunityId,
      hotelId: HOTEL,
      title: row.canonicalTitle,
      commercialInventoryClass: row.commercialClassification,
      customerFacing: true,
    };
    try {
      const published = reg.publishOpportunity({
        hotelId: HOTEL,
        opportunityId: row.opportunityId,
        classification: row.commercialClassification,
        approval,
        sourceResearchReference:
          "reports/gdi-durable-publication-registry-spice-island-v1/02-spice-island-seven-id-publication-manifest.json",
        snapshot,
      });
      results.push({
        opportunityId: row.opportunityId,
        classification: row.commercialClassification,
        publishedVersion: published.row.publishedVersion,
        state: published.row.publicationState,
        mode: "publish",
      });
    } catch (err) {
      if (err?.code === "ALREADY_PUBLISHED") {
        const updated = reg.updatePublishedOpportunity({
          hotelId: HOTEL,
          opportunityId: row.opportunityId,
          classification: row.commercialClassification,
          approval,
          sourceResearchReference:
            "reports/gdi-durable-publication-registry-spice-island-v1/02-spice-island-seven-id-publication-manifest.json",
          snapshot,
        });
        results.push({
          opportunityId: row.opportunityId,
          classification: row.commercialClassification,
          publishedVersion: updated.row.publishedVersion,
          state: updated.row.publicationState,
          mode: "idempotent_update",
        });
      } else {
        throw err;
      }
    }
  }

  const published = reg.listPublished(HOTEL);
  if (published.length !== 7) {
    throw new Error(`published_count_${published.length}`);
  }
  if (published.some((r) => r.opportunityId === EXCLUDED)) {
    throw new Error("aventa_leaked_into_published");
  }
  const sr = published.filter(
    (r) => r.commercialClassification === COMMERCIAL_PUBLICATION_CLASS.SALES_READY
  ).length;
  const ad = published.filter(
    (r) =>
      r.commercialClassification === COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT
  ).length;
  if (sr !== 1 || ad !== 6) {
    throw new Error(`class_mix_sr${sr}_ad${ad}`);
  }

  const leak = mergePublishedInventoryIntoOpportunities(
    HOTEL,
    [
      ...published.map((r) => ({ id: r.opportunityId, title: "x" })),
      { id: EXCLUDED, title: "Aventa must not leak", customerVisible: true },
    ],
    { registry: reg, strictPublishedOnly: true }
  );
  if (leak.some((o) => o.id === EXCLUDED) || leak.length !== 7) {
    throw new Error("strict_merge_leak_or_count");
  }

  return {
    stagingRoot,
    results,
    counts: { total: 7, salesReady: sr, accountDevelopment: ad, rejected: 0 },
    excluded: EXCLUDED,
    registryFile: reg.paths.registryFile,
  };
}

function uploadVolume(stagingRoot) {
  const r = spawnSync(
    "railway",
    [
      "volume",
      "files",
      "--volume",
      VOLUME,
      "upload",
      stagingRoot,
      REMOTE_ROOT,
      "--json",
    ],
    { cwd: ROOT, encoding: "utf8", shell: true }
  );
  if (r.status !== 0) {
    throw new Error(`volume_upload_failed:${r.stderr || r.stdout}`);
  }
  return { stdout: (r.stdout || "").slice(0, 4000), status: r.status };
}

function issueShare() {
  // Production secret must already be in process.env (caller injects; never log).
  delete process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET;
  if (!process.env.GDI_SHARE_CAPABILITY_SECRET) {
    throw new Error("GDI_SHARE_CAPABILITY_SECRET_missing");
  }
  // Persist into committed registry path for deploy durability / self-heal seed.
  process.env.GDI_SHARE_REGISTRY_DIR = path.join(
    ROOT,
    "config/client-share/gdi-share-registry"
  );
  const issued = issueGdiShareCapability({
    hotelId: HOTEL,
    label: "Spice Island Beach Resort — GDI client share",
    tokenId: `gdisht_spice_island_${Date.now().toString(36)}`,
  });
  const publicDomain =
    process.env.RAILWAY_PUBLIC_DOMAIN ||
    "my-operators-backend-production.up.railway.app";
  const url = `https://${publicDomain}${issued.sharePath}`;
  // Redacted report artifact — never write raw token to disk in reports.
  return {
    tokenId: issued.tokenId,
    hotelId: issued.hotelId,
    sharePathTemplate: "/group-demand-intelligence-share.html?share=gdishare.v1.<redacted>",
    sharePath: issued.sharePath,
    productionUrl: url,
    issuedAt: issued.meta?.issuedAt || null,
  };
}

function main() {
  ensureOut();
  const s = stage();
  const stagingDefault = path.join(OUT, "staging-published-inventory");
  const stagingRoot = arg("root") || stagingDefault;

  if (s === "publish-local") {
    if (fs.existsSync(stagingRoot)) {
      fs.rmSync(stagingRoot, { recursive: true, force: true });
    }
    const result = publishLocal(stagingRoot);
    fs.writeFileSync(
      path.join(OUT, "03-publish-local-result.json"),
      `${JSON.stringify({ ok: true, ...result, at: new Date().toISOString() }, null, 2)}\n`
    );
    console.log(JSON.stringify({ ok: true, stage: s, ...result }, null, 2));
    return;
  }

  if (s === "upload-volume") {
    if (!fs.existsSync(path.join(stagingRoot, "registry", "current-registry-v1.json"))) {
      throw new Error("run_publish-local_first");
    }
    const up = uploadVolume(stagingRoot);
    fs.writeFileSync(
      path.join(OUT, "03b-volume-upload-result.json"),
      `${JSON.stringify({ ok: true, stagingRoot, remoteRoot: REMOTE_ROOT, volume: VOLUME, up, at: new Date().toISOString() }, null, 2)}\n`
    );
    console.log(JSON.stringify({ ok: true, stage: s, remoteRoot: REMOTE_ROOT }, null, 2));
    return;
  }

  if (s === "issue-share") {
    const issued = issueShare();
    // Write redacted summary only
    fs.writeFileSync(
      path.join(OUT, "05-share-issue-redacted.json"),
      `${JSON.stringify(
        {
          ok: true,
          tokenId: issued.tokenId,
          hotelId: issued.hotelId,
          productionUrlPresent: Boolean(issued.productionUrl),
          sharePathPrefix: String(issued.sharePath || "").slice(0, 48),
          issuedAt: issued.issuedAt,
          note: "Full URL retained only in secure local file share-url.local.json (gitignored pattern)",
        },
        null,
        2
      )}\n`
    );
    // Local-only full URL for verification (not for reports commit if gitignored)
    fs.writeFileSync(
      path.join(OUT, "share-url.local.json"),
      `${JSON.stringify(issued, null, 2)}\n`
    );
    console.log(
      JSON.stringify(
        {
          ok: true,
          stage: s,
          tokenId: issued.tokenId,
          hotelId: issued.hotelId,
          // Print URL once for operator verification — secret never printed
          productionUrl: issued.productionUrl,
        },
        null,
        2
      )
    );
    return;
  }

  if (s === "verify-counts") {
    const reg = createGdiPublishedInventoryRegistry({ root: stagingRoot });
    const published = reg.listPublished(HOTEL);
    const out = {
      ok: published.length === 7,
      count: published.length,
      ids: published.map((r) => ({
        opportunityId: r.opportunityId,
        classification: r.commercialClassification,
        version: r.publishedVersion,
        state: r.publicationState,
      })),
      states: [...new Set(published.map((r) => r.publicationState))],
      expectedState: PUBLICATION_STATE.PUBLISHED,
    };
    console.log(JSON.stringify(out, null, 2));
    if (!out.ok) process.exit(1);
    return;
  }

  console.error(
    "Usage: --stage=publish-local|upload-volume|issue-share|verify-counts"
  );
  process.exit(2);
}

main();
