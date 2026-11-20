#!/usr/bin/env node
/**
 * Pilot remediation: AC Hotels (3 scenarios + 6 gallery) and Courtyard (verify/preserve).
 *
 * Default: dry-run. Apply with --apply (founder-authorized controlled remediation).
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import Airtable from "airtable";
import {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
  IMAGE_LIFECYCLE,
  classifyLiveAttachment,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v2.js";
import {
  ingestPresentationImageFromSource,
  readPresentationImage,
  probeAttachmentUrl,
} from "../lib/brand-explorer/factory-2/be-factory-image-attachment-ingest-v2.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v2");
mkdirSync(OUT, { recursive: true });
const apply = process.argv.includes("--apply");

const PILOT = {
  ac: {
    brandId: "rec9aZp7GHtzUEg0c",
    brandName: "AC Hotels by Marriott",
    scenarios: [
      {
        recordId: "recSFmj7OjAMaNpRR",
        slotKey: "overview.scenario.1",
        registryId: "rec9bFXWx4k2jg2I1",
        fallbackPoolRole: "exterior_arrival",
      },
      {
        recordId: "recbwiXL5JJkhtjDp",
        slotKey: "overview.scenario.2",
        registryId: "recVnYYCgtuDJA5BG",
        fallbackPoolRole: "guest_room_suite",
      },
      {
        recordId: "reco4XNiRS8ijAJ20",
        slotKey: "overview.scenario.3",
        registryId: "recu6NgXhV9pGdwuI",
        fallbackPoolRole: "public_space_lobby",
      },
    ],
    galleries: [
      { recordId: "recIZV9ELm296oVXx", slotKey: "materials.gallery.1", role: "exterior_arrival" },
      { recordId: "recfYtIP9fpRfaWH8", slotKey: "materials.gallery.2", role: "guest_room_suite" },
      { recordId: "rectW4s2pi3aedQWN", slotKey: "materials.gallery.3", role: "guest_room_suite" },
      { recordId: "recr0YgqrYonMNsq0", slotKey: "materials.gallery.4", role: "public_space_lobby" },
      { recordId: "reclDNILlYxupQoXw", slotKey: "materials.gallery.5", role: "public_space_lobby" },
      { recordId: "rec5G8LLmVvzwZtf2", slotKey: "materials.gallery.6", role: "property_setting" },
    ],
  },
  courtyard: {
    brandId: "rec6hye5H8zJmAGv3",
    brandName: "Courtyard by Marriott",
    scenarios: [
      { recordId: "recJcNQUm5mltOCHN", slotKey: "overview.scenario.1" },
      { recordId: "reciP5wtZYcgiFZY2", slotKey: "overview.scenario.2" },
      { recordId: "recX6N7od73A3JC1j", slotKey: "overview.scenario.3" },
    ],
  },
};

function loadAcPool() {
  const p = join(ROOT, "fixtures/wave12-ac-hotels-by-marriott-gallery-pool.json");
  if (!existsSync(p)) return [];
  return JSON.parse(readFileSync(p, "utf8"));
}

function pickPoolUrl(pool, role, used) {
  const candidates = pool.filter((x) => x.role === role || !role);
  for (const c of candidates) {
    if (c.imageUrl && !used.has(c.imageUrl)) {
      used.add(c.imageUrl);
      return { url: c.imageUrl, sourcePageUrl: c.sourcePageUrl, propertyName: c.propertyName, role: c.role };
    }
  }
  for (const c of pool) {
    if (c.imageUrl && !used.has(c.imageUrl)) {
      used.add(c.imageUrl);
      return { url: c.imageUrl, sourcePageUrl: c.sourcePageUrl, propertyName: c.propertyName, role: c.role };
    }
  }
  return null;
}

async function loadRegistrySource(registryId) {
  if (!registryId) return null;
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  try {
    const rec = await base("Partner Intelligence - Brand Asset Registry").find(registryId);
    return {
      registryId,
      sourceUrl: rec.get("Source URL") || null,
      pageUrl: rec.get("Source Page URL") || null,
      name: rec.get("Asset Name") || null,
      status: rec.get("Asset Status") || null,
      explorerPerm: rec.get("Explorer Use Permission") || null,
    };
  } catch {
    return null;
  }
}

async function remediateSlot({ recordId, slotKey, sourceUrl, sourceMeta }) {
  const before = await readPresentationImage(recordId);
  const beforeProbe = before.attachment?.url
    ? await probeAttachmentUrl(before.attachment.url)
    : null;
  const beforeClass = classifyLiveAttachment({
    attachment: before.attachment,
    probe: beforeProbe,
  });

  // Preserve working CURRENT attachments (Courtyard path).
  if (before.attachment && beforeProbe?.ok) {
    return {
      recordId,
      slotKey,
      action: "PRESERVE_VALID_CURRENT",
      lifecycle: IMAGE_LIFECYCLE.IMAGE_READBACK_VERIFIED,
      beforeClass,
      afterClass: beforeClass,
      attachment: before.attachment,
      sourceMeta,
      ok: true,
    };
  }

  if (!sourceUrl) {
    return {
      recordId,
      slotKey,
      action: "BLOCKED_NO_SOURCE",
      beforeClass,
      ok: false,
      reason: "no_verified_source_url",
    };
  }

  const result = await ingestPresentationImageFromSource({
    recordId,
    sourceUrl,
    slotKey,
    dryRun: !apply,
  });

  return {
    recordId,
    slotKey,
    action: apply ? "INGEST_CONTENT_API" : "DRY_RUN_VALIDATE_SOURCE",
    sourceUrl,
    sourceMeta,
    beforeClass,
    ...result,
  };
}

const pool = loadAcPool();
const used = new Set();
const results = { generatedAt: new Date().toISOString(), contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2, apply, rows: [] };

// Courtyard first — verify/preserve
for (const s of PILOT.courtyard.scenarios) {
  const row = await remediateSlot({
    recordId: s.recordId,
    slotKey: s.slotKey,
    sourceUrl: null,
    sourceMeta: { brand: PILOT.courtyard.brandName, mode: "verify_preserve" },
  });
  results.rows.push({ brand: PILOT.courtyard.brandName, brandId: PILOT.courtyard.brandId, ...row });
}

// AC scenarios — registry source preferred, pool fallback
for (const s of PILOT.ac.scenarios) {
  const reg = await loadRegistrySource(s.registryId);
  let sourceUrl = reg?.sourceUrl || null;
  let sourceMeta = { registry: reg, brand: PILOT.ac.brandName };
  if (sourceUrl) {
    // Pre-validate; fall back to pool on permanent failure.
    const trial = await ingestPresentationImageFromSource({
      recordId: s.recordId,
      sourceUrl,
      slotKey: s.slotKey,
      dryRun: true,
    });
    if (!trial.ok) {
      const fb = pickPoolUrl(pool, s.fallbackPoolRole, used);
      sourceUrl = fb?.url || null;
      sourceMeta = { ...sourceMeta, registryFailed: trial, fallbackPool: fb };
    }
  } else {
    const fb = pickPoolUrl(pool, s.fallbackPoolRole, used);
    sourceUrl = fb?.url || null;
    sourceMeta = { ...sourceMeta, fallbackPool: fb };
  }
  const row = await remediateSlot({
    recordId: s.recordId,
    slotKey: s.slotKey,
    sourceUrl,
    sourceMeta,
  });
  results.rows.push({ brand: PILOT.ac.brandName, brandId: PILOT.ac.brandId, ...row });
}

// AC galleries
for (const g of PILOT.ac.galleries) {
  const fb = pickPoolUrl(pool, g.role, used);
  const row = await remediateSlot({
    recordId: g.recordId,
    slotKey: g.slotKey,
    sourceUrl: fb?.url || null,
    sourceMeta: { brand: PILOT.ac.brandName, pool: fb },
  });
  results.rows.push({ brand: PILOT.ac.brandName, brandId: PILOT.ac.brandId, ...row });
}

const stamp = results.generatedAt.replace(/[:.]/g, "-");
const path = join(OUT, `10-pilot-remediation-${apply ? "APPLY" : "DRY"}-${stamp}.json`);
writeFileSync(path, JSON.stringify(results, null, 2) + "\n");
const ok = results.rows.filter((r) => r.ok).length;
const fail = results.rows.filter((r) => !r.ok).length;
console.log(
  JSON.stringify(
    {
      ok: fail === 0,
      apply,
      report: path,
      rows: results.rows.length,
      okCount: ok,
      failCount: fail,
      actions: results.rows.map((r) => ({
        brand: r.brand,
        slot: r.slotKey,
        action: r.action,
        ok: r.ok,
        lifecycle: r.lifecycle || null,
        classification: r.classification || r.afterClass || r.beforeClass,
      })),
    },
    null,
    2
  )
);
