/**
 * Apply HE CALA Operating Platform content to:
 *   - Operator Setup - Platform & Markets (legacy cap_* mirror)
 *   - Operator Setup - Operating Platform (child table rows)
 *
 *   node scripts/restore-he-cala-operating-platform.mjs [masterRecordId] [path-to-fixture.json]
 *
 * Default master: recWPKu5laVZxsvpn
 * Default fixture: fixtures/operator-operating-explorer-he-cala.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
  NEW_BASE_PLATFORM_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorOperatingPlatformRows } from "../api/lib/operator-setup-new-base-writer.js";
import { applyOperatingPlatformToLegacyPrefill } from "../api/lib/operator-operating-platform-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recWPKu5laVZxsvpn";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-operating-explorer-he-cala.json");

function parseArgs(argv) {
  const pos = argv.slice(2).filter((a) => !a.startsWith("--"));
  return {
    masterId: (pos[0] || "").trim() || DEFAULT_MASTER,
    fixturePath: (pos[1] || "").trim() || DEFAULT_FIXTURE,
  };
}

function enc(s) {
  return encodeURIComponent(s);
}

const PLATFORM_MIRROR_KEY = /^(cap_|op_)/;

function buildPlatformPatch(platformRaw, operatingPlatform) {
  const patch = {};
  const prefill = {};
  applyOperatingPlatformToLegacyPrefill(prefill, operatingPlatform);
  Object.entries(prefill).forEach(([key, value]) => {
    if (value == null || value === "") return;
    if (!PLATFORM_MIRROR_KEY.test(key)) return;
    patch[key] = value;
  });
  Object.entries(platformRaw || {}).forEach(([key, value]) => {
    if (value == null || value === "") return;
    if (!PLATFORM_MIRROR_KEY.test(key)) return;
    patch[key] = value;
  });
  return patch;
}

async function patchPlatformRecord(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_PLATFORM_TABLE)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) {
    const msg = json?.error?.message || JSON.stringify(json);
    throw new Error(`Platform PATCH failed (${status}): ${msg}`);
  }
  return json;
}

function countOperating(op) {
  const pillars = op.pillars || {};
  const pillarCounts = {};
  Object.keys(pillars).forEach((k) => {
    pillarCounts[k] = (pillars[k].items || []).length;
  });
  return {
    snapshotKpis: (op.snapshotKpis || []).length,
    performanceSignals: (op.performanceSignals || []).length,
    positioningCards: (op.positioningCards || []).length,
    pillarCounts,
  };
}

async function main() {
  const { masterId, fixturePath } = parseArgs(process.argv);
  if (!fs.existsSync(fixturePath)) {
    throw new Error(`Missing fixture: ${fixturePath}`);
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const operatingPlatform = fixture.operatingPlatform;
  if (!operatingPlatform || typeof operatingPlatform !== "object") {
    throw new Error("Fixture missing operatingPlatform object");
  }

  const platRows = await fetchRecordsLinkedToMaster(NEW_BASE_PLATFORM_TABLE, masterId);
  if (!platRows.length) {
    throw new Error(`No Platform row linked to Master ${masterId}`);
  }
  const platId = platRows[0].id;
  const platPatch = buildPlatformPatch(
    { ...(fixture.platformFields || {}), ...(platRows[0].fields || {}) },
    operatingPlatform
  );
  if (!Object.keys(platPatch).length) {
    throw new Error("Fixture has no platform fields to patch");
  }

  await patchPlatformRecord(platId, platPatch);
  const opRes = await replaceOperatorOperatingPlatformRows(
    masterId,
    { operatingPlatform },
    randomUUID()
  );

  const publicFixture = path.join(ROOT, "public", "fixtures", "operator-operating-explorer-he-cala.json");
  fs.mkdirSync(path.dirname(publicFixture), { recursive: true });
  fs.copyFileSync(fixturePath, publicFixture);

  console.log(
    JSON.stringify(
      {
        masterId,
        fixturePath,
        publicFixture,
        operatorName: fixture._meta?.operatorName || null,
        platformRecordId: platId,
        platformFieldCount: Object.keys(platPatch).length,
        operatingCounts: countOperating(operatingPlatform),
        airtable: opRes,
        sampleCommercialTile: operatingPlatform.pillars?.commercialEngine?.items?.[0]?.title || null,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
