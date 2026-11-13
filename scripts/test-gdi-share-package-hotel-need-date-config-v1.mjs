#!/usr/bin/env node
/**
 * PACKAGE share list must include hotelNeedDateConfigV1 so the property bar
 * does not fall back to Not Established while auth shows Hotel Confirmed.
 *
 *   GDI_PARITY_SHARE_TOKEN=... node scripts/test-gdi-share-package-hotel-need-date-config-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const share =
  process.env.GDI_PARITY_SHARE_TOKEN ||
  (fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
    /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
  ) || [])[1]?.trim();
assert.ok(share, "share token required");

const base = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

const url = `${base}/api/group-demand-intelligence/share/hotels/${HOTEL}/opportunities?view=full&share=${encodeURIComponent(share)}`;
const res = await fetch(url);
assert.equal(res.status, 200);
const body = await res.json();
assert.equal(body.listScope, "PUBLICATION_PACKAGE");
assert.ok(
  body.hotelNeedDateConfigV1,
  "PACKAGE response missing hotelNeedDateConfigV1",
);
assert.equal(
  body.hotelNeedDateConfigV1.status,
  "HOTEL_CONFIRMED",
  `expected HOTEL_CONFIRMED, got ${body.hotelNeedDateConfigV1.status}`,
);
console.log(
  JSON.stringify(
    {
      ok: true,
      listScope: body.listScope,
      status: body.hotelNeedDateConfigV1.status,
      confirmedPeriodCount: body.hotelNeedDateConfigV1.confirmedPeriodCount,
    },
    null,
    2,
  ),
);
