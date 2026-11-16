#!/usr/bin/env node
/**
 * Offline gates for Radisson local founder staged preview wiring.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";
import { classifyOpportunityTypeFilter } from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recH9h3McgMNFQEla";
const PRIOR_FP =
  "ac55190dd783fe6eb5f3ec60d65fd2ffdd9c7f3e3aba7eb35645197e96979c86";
const CURRENT_FP =
  "f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d";
const STAGED_FP =
  "f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d";

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const shellJs = fs.readFileSync(path.join(ROOT, "public/app.js"), "utf8");
const apiJs = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);

assert.match(appJs, /readGdiQueryParams/, "must merge parent hash/search query");
assert.doesNotMatch(
  appJs,
  /gdiFounderStagedPreviewChip|gdi-founder-preview-chip|Fingerprint:|gdiFounderStagedPreviewBanner/,
  "must not render Preview chip or technical package/fingerprint banner",
);
assert.match(appJs, /wantsExplicitStagedPreview/);
assert.match(appJs, /isLocalFounderHost/, "localhost must default to STAGED");
assert.match(
  appJs,
  /path \+= "&preview=staged"/,
  "detail URL must propagate founder staged preview with list",
);
assert.match(appJs, /raceTimeout/, "drawer must not hang on decision/pursuit");
assert.match(shellJs, /preview/);
assert.match(shellJs, /hotelId/);
assert.match(apiJs, /preferStaged:\s*requireStagedPreview/);

// Prior certified package bytes remain on disk.
const priorPkg = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL,
      "gdi_pub_recH9h3McgMNFQEla_20261116_radisson_toronto_six_missing_segments_exa_30usd_v1",
      "package.json",
    ),
    "utf8",
  ),
);
assert.equal(priorPkg.contentFingerprintSha256, PRIOR_FP);

const staged = resolvePreviewOpportunitiesFromPublicationPackage({
  hotelId: HOTEL,
  preferStaged: true,
});
assert.equal(staged.ok, true);
assert.equal(staged.mode, "PACKAGE");
assert.equal(staged.meta?.previewSource, "STAGED");
assert.equal(staged.meta?.contentFingerprintSha256, STAGED_FP);
assert.equal(staged.opportunities?.length, 66);
const hm = staged.opportunities.filter((o) => o.hotelMarketScopeEligible === true);
const cmi = staged.opportunities.filter(
  (o) => o.competitiveMarketScopeEligible === true,
);
assert.equal(hm.length, 50);
assert.equal(cmi.length, 16);

const localCatering = staged.opportunities.filter(
  (o) =>
    classifyOpportunityTypeFilter(o) === "LOCAL_CATERING_DAY_MEETINGS",
);
const scaling = staged.opportunities.filter(
  (o) => classifyOpportunityTypeFilter(o) === "SCALING_COMPANY",
);
assert.equal(localCatering.length, 6);
assert.equal(scaling.length, 0, "conferences must not remain Scaling Companies");

const ovma = staged.opportunities.find((o) => o.id === "rx_d_918bc65e2d44");
assert.ok(ovma);
assert.equal(
  classifyOpportunityTypeFilter(ovma),
  "ASSOCIATION_CONFERENCE_GROUP",
);

// Without preferStaged, resolver serves CURRENT published package.
const currentDefault = resolvePreviewOpportunitiesFromPublicationPackage({
  hotelId: HOTEL,
});
assert.equal(currentDefault.meta?.previewSource, "CURRENT");
assert.equal(currentDefault.meta?.contentFingerprintSha256, CURRENT_FP);

console.log(
  JSON.stringify(
    {
      ok: true,
      stagedFingerprint: STAGED_FP,
      currentFingerprint: CURRENT_FP,
      inventory: { all: 66, hotel: hm.length, cmi: cmi.length },
      localhostDefaultsToStaged: true,
    },
    null,
    2,
  ),
);
