#!/usr/bin/env node
/**
 * npm run test:gdi-publication-parity-e2e
 *
 * Exact approved-publication parity gate:
 *  - Fingerprint identity
 *  - Inventory IDs / Market Scope
 *  - Controlled DTO fields
 *  - Commercial actions
 *  - Need-date sections present when frozen in package
 *  - Negative cases (mismatched recommendation, wrong fingerprint, etc.)
 *
 * Env:
 *   GDI_PARITY_APPROVED_VERSION / GDI_PARITY_APPROVED_FINGERPRINT
 *   GDI_PARITY_PRODUCTION_URL (optional — live production check)
 *   GDI_PARITY_SHARE_TOKEN (required with production URL)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
  comparePublicationParity,
  comparePublicationInventory,
  runPublicationQualityGate,
  PUBLICATION_CONTENT_FIELDS,
} from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL = "recLuxvwwxID7U2B8";

const failures = [];
function fail(code, detail) {
  failures.push({ code, detail });
}
function assert(cond, code, detail) {
  if (!cond) fail(code, detail);
}

const store = createGdiPublicationPackageStore();
const approvedVersion =
  process.env.GDI_PARITY_APPROVED_VERSION ||
  store.readPointer(HOTEL, "CURRENT")?.publicationVersion ||
  store.readPointer(HOTEL, "STAGED")?.publicationVersion;
const approvedFingerprint =
  process.env.GDI_PARITY_APPROVED_FINGERPRINT ||
  store.readPointer(HOTEL, "CURRENT")?.contentFingerprintSha256 ||
  store.readPointer(HOTEL, "STAGED")?.contentFingerprintSha256;

assert(Boolean(approvedVersion), "APPROVED_VERSION_MISSING");
assert(
  /^[a-f0-9]{64}$/i.test(String(approvedFingerprint || "")),
  "APPROVED_FINGERPRINT_INVALID",
  approvedFingerprint,
);

const loaded = store.loadPackage(HOTEL, approvedVersion);
assert(loaded.ok, "PACKAGE_LOAD_FAILED", loaded.error);
if (!loaded.ok || !loaded.package) {
  console.error(JSON.stringify({ ok: false, failures }, null, 2));
  process.exit(2);
}
const pkg = loaded.package;
const integrity = verifyPublicationPackageIntegrity(pkg);
assert(integrity.ok, "PACKAGE_INTEGRITY_FAILED", integrity.fails);
assert(
  String(pkg.contentFingerprintSha256 || "").toLowerCase() ===
    String(approvedFingerprint).toLowerCase(),
  "FINGERPRINT_MISMATCH_VS_ENV",
  {
    package: pkg.contentFingerprintSha256,
    expected: approvedFingerprint,
  },
);

const inv = comparePublicationInventory(pkg, {
  opportunities: pkg.opportunities,
  sortOrder: pkg.sortOrder,
});
assert(inv.ok, "SELF_INVENTORY_FAIL", inv);
const counts = inv.approvedCounts || pkg.inventoryCounts || {};
assert(counts.all === 67, "INVENTORY_ALL_NOT_67", counts);
assert(counts.hotel === 61, "INVENTORY_HOTEL_NOT_61", counts);
assert(counts.competitive === 10, "INVENTORY_COMPETITIVE_NOT_10", counts);

const uniqueIds = new Set(pkg.opportunities.map((o) => o.id));
assert(uniqueIds.size === 67, "DUPLICATE_OR_MISSING_IDS", uniqueIds.size);

const quality = runPublicationQualityGate({ opportunities: pkg.opportunities });
assert(quality.ok, "QUALITY_BLOCKING", {
  blockingCount: quality.blockingCount,
  sample: quality.blocking?.slice(0, 10),
});

// Commercial action coherence: card field present and non-empty for all.
for (const o of pkg.opportunities) {
  assert(
    Boolean(o.salesActionPillLabel),
    "MISSING_COMMERCIAL_ACTION",
    o.id,
  );
}

// Need-date contract: if package freezes needDateMatchV1 / tileNeedDatePreview,
// flag malformed empty wrappers; presence is inventory-dependent.
let needDatePresent = 0;
for (const o of pkg.opportunities) {
  if (o.needDateMatchV1 || o.tileNeedDatePreview?.visible) needDatePresent += 1;
  const blob = JSON.stringify({
    what: o.whatIsHappening,
    summary: o.summaryWhat || o.summary,
  });
  assert(!/\(\s*\)/.test(blob), "EMPTY_PARENS", o.id);
}

const selfParity = comparePublicationParity(pkg, {
  opportunities: pkg.opportunities,
  sortOrder: pkg.sortOrder,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
});
assert(selfParity.ok, "SELF_PARITY_FAIL", {
  contentOk: selfParity.content?.ok,
  renderedOk: selfParity.rendered?.ok,
});

// --- Negative tests (synthetic) ---
{
  const wrongIds = {
    opportunities: pkg.opportunities.map((o, i) =>
      i === 0 ? { ...o, id: "gdi_opp_WRONG_ID" } : o,
    ),
    sortOrder: pkg.sortOrder.map((id, i) => (i === 0 ? "gdi_opp_WRONG_ID" : id)),
  };
  const invNeg = comparePublicationInventory(pkg, wrongIds);
  assert(!invNeg.ok, "NEG_SAME_COUNT_DIFFERENT_IDS_SHOULD_FAIL");
}
{
  const wrongAction = {
    opportunities: pkg.opportunities.map((o, i) =>
      i === 0
        ? { ...o, salesActionPillLabel: "__WRONG_ACTION__" }
        : o,
    ),
    sortOrder: pkg.sortOrder,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
  };
  const parityNeg = comparePublicationParity(pkg, wrongAction);
  assert(!parityNeg.content.ok, "NEG_DIFFERENT_RECOMMENDATION_SHOULD_FAIL");
}
{
  const wrongFp = {
    opportunities: pkg.opportunities,
    sortOrder: pkg.sortOrder,
    contentFingerprintSha256: "0".repeat(64),
  };
  const parityNeg = comparePublicationParity(pkg, wrongFp);
  assert(!parityNeg.fingerprintMatch, "NEG_WRONG_FINGERPRINT_SHOULD_FAIL");
}
{
  assert(
    PUBLICATION_CONTENT_FIELDS.includes("needDateMatchV1"),
    "NEG_NEED_DATE_FIELD_NOT_IN_CONTRACT",
  );
  assert(
    PUBLICATION_CONTENT_FIELDS.includes("salesActionPillLabel"),
    "NEG_ACTION_FIELD_NOT_IN_CONTRACT",
  );
}

// --- Optional live production check ---
let production = null;
const prodUrl = process.env.GDI_PARITY_PRODUCTION_URL || "";
const shareToken = process.env.GDI_PARITY_SHARE_TOKEN || "";
if (prodUrl && shareToken) {
  const url = `${prodUrl.replace(/\/$/, "")}/api/group-demand-intelligence/share/hotels/${HOTEL}/opportunities?view=full`;
  const res = await fetch(url, {
    headers: { "X-GDI-Share-Capability": shareToken },
  });
  const body = await res.json();
  production = {
    httpOk: res.ok,
    listScope: body.listScope,
    publication: body.publication || null,
    count: body.opportunities?.length,
  };
  assert(res.ok, "PRODUCTION_HTTP_FAIL", res.status);
  assert(
    body.listScope === "PUBLICATION_PACKAGE",
    "PRODUCTION_NOT_PACKAGE_MODE",
    body.listScope,
  );
  assert(
    String(body.publication?.contentFingerprintSha256 || "").toLowerCase() ===
      String(approvedFingerprint).toLowerCase(),
    "PRODUCTION_FINGERPRINT_MISMATCH",
    {
      live: body.publication?.contentFingerprintSha256,
      expected: approvedFingerprint,
    },
  );
  const liveParity = comparePublicationParity(pkg, {
    opportunities: body.opportunities || [],
    sortOrder: (body.opportunities || []).map((o) => o.id),
    contentFingerprintSha256: body.publication?.contentFingerprintSha256,
  });
  assert(liveParity.ok, "PRODUCTION_CONTENT_PARITY_FAIL", {
    fieldDiffs: liveParity.content?.fieldDifferenceCount,
    renderedDiffs: liveParity.rendered?.renderedTextDifferenceCount,
  });

  // DETAIL route must serve the same package (not live reconstruct).
  // Prior false certification only compared LIST payloads.
  const detailNeedDateGaps = [];
  const withNeedDate = (pkg.opportunities || []).filter(
    (o) => o.needDateMatchV1 || o.tileNeedDatePreview?.visible,
  );
  for (const o of withNeedDate) {
    const dUrl = `${prodUrl.replace(/\/$/, "")}/api/group-demand-intelligence/share/hotels/${HOTEL}/opportunities/${encodeURIComponent(o.id)}`;
    const dRes = await fetch(dUrl, {
      headers: { "X-GDI-Share-Capability": shareToken },
    });
    const dBody = await dRes.json();
    const detailOpp = dBody.opportunity || null;
    if (!dRes.ok || !detailOpp) {
      detailNeedDateGaps.push({ id: o.id, error: "DETAIL_HTTP_FAIL" });
      continue;
    }
    if (dBody.listScope && dBody.listScope !== "PUBLICATION_PACKAGE") {
      detailNeedDateGaps.push({
        id: o.id,
        error: "DETAIL_NOT_PACKAGE",
        listScope: dBody.listScope,
      });
    }
    const pkgNd = JSON.stringify(o.needDateMatchV1 || null);
    const liveNd = JSON.stringify(detailOpp.needDateMatchV1 || null);
    if (pkgNd !== liveNd) {
      detailNeedDateGaps.push({
        id: o.id,
        error: "DETAIL_NEED_DATE_MISMATCH",
        packageHas: Boolean(o.needDateMatchV1),
        detailHas: Boolean(detailOpp.needDateMatchV1),
      });
    }
  }
  production.detailNeedDateChecked = withNeedDate.length;
  production.detailNeedDateGaps = detailNeedDateGaps.length;
  assert(
    detailNeedDateGaps.length === 0,
    "PRODUCTION_DETAIL_NEED_DATE_PARITY_FAIL",
    detailNeedDateGaps.slice(0, 15),
  );
}

const outDir = path.join(ROOT, "reports/gdi-local-production-full-reconciliation-v1");
fs.mkdirSync(outDir, { recursive: true });
const report = {
  ok: failures.length === 0,
  approvedVersion,
  approvedFingerprint,
  needDatePresent,
  contentFieldCount: PUBLICATION_CONTENT_FIELDS.length,
  inventory: counts,
  qualityBlocking: quality.blockingCount,
  production,
  failures,
};
fs.writeFileSync(
  path.join(outDir, "09-REGRESSION-RESULTS.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(report, null, 2));
process.exit(failures.length === 0 ? 0 : 2);
