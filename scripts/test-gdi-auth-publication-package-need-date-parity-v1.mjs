#!/usr/bin/env node
/**
 * Auth ?publicationPackage=1 list must attach hotelNeedDateConfigV1 the same way
 * share PACKAGE does — otherwise fingerprint-matched founder preview shows
 * "Need dates not established" while customer share shows "Hotel Confirmed".
 *
 * Source contract (no live auth required):
 *   npm run test:gdi-auth-publication-package-need-date-parity-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const api = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);

const authPkgIdx = api.indexOf(
  "Optional immutable publication preview (same contract as external share)",
);
assert.ok(authPkgIdx > 0, "auth publication preview block missing");

const authReturnSlice = api.slice(authPkgIdx, authPkgIdx + 4500);
assert.match(
  authReturnSlice,
  /hotelNeedDateConfigV1/,
  "auth publicationPackage preview must return hotelNeedDateConfigV1",
);
assert.match(
  authReturnSlice,
  /includeUnpublishedEstimates:\s*false/,
  "auth package preview must use customer-safe need-date status (no unpublished estimates)",
);
assert.match(
  authReturnSlice,
  /listScope:\s*"PUBLICATION_PACKAGE"/,
);

const sharePkgIdx = api.indexOf(
  "Hotel-level Need Dates status is live hotel-config metadata",
);
assert.ok(sharePkgIdx > 0, "share PACKAGE need-date comment missing");
const shareSlice = api.slice(sharePkgIdx, sharePkgIdx + 2500);
assert.match(shareSlice, /hotelNeedDateConfigV1/);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "GDI_AUTH_PUBLICATION_PACKAGE_NEED_DATE_PARITY_V1",
      authPackagePreviewIncludesHotelNeedDateConfig: true,
      sharePackageIncludesHotelNeedDateConfig: true,
    },
    null,
    2,
  ),
);
