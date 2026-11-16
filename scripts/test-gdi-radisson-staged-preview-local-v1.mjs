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
const FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";

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
assert.equal(staged.meta?.contentFingerprintSha256, FP);
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

const fallback = resolvePreviewOpportunitiesFromPublicationPackage({
  hotelId: HOTEL,
});
assert.equal(fallback.meta?.previewSource, "STAGED");
assert.equal(fallback.meta?.contentFingerprintSha256, FP);

console.log(
  JSON.stringify(
    {
      ok: true,
      priorFingerprintPreserved: PRIOR_FP,
      fingerprint: FP,
      all: staged.opportunities.length,
      hotelMarket: hm.length,
      cmi: cmi.length,
      localCatering: localCatering.length,
      scalingCompany: scaling.length,
    },
    null,
    2,
  ),
);
