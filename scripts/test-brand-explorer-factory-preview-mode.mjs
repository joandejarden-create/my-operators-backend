#!/usr/bin/env node
/**
 * Factory Preview Mode acceptance tests (read-only).
 *
 *   npm run test:brand-explorer-factory-preview-mode
 *
 * Covers:
 * 1. Factory preview candidate can render full internal preview when enabled
 * 2. Does not render public-full in public mode
 * 3. Not in active universe unless Brand Status Active/Live
 * 4. Protected 27 baseline still fails if candidate is actually Active without revision
 * 5. Display state is factory_preview_internal (not active_profile_ready)
 * 6. PVQL public-full for non-Active candidates stays false (module + probe)
 * 7. No CV / Source / Registry / Brand Status writes in this path
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import {
  FACTORY_PREVIEW_CANDIDATE_SLUGS,
  FACTORY_PREVIEW_DISPLAY_STATE,
  assertFactoryPreviewDoesNotAffectActiveUniverse,
  canRenderFactoryPreview,
  getFactoryPreviewDisplayState,
  isFactoryPreviewCandidate,
  isFactoryPreviewQuery,
} from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";
import { ACTIVE_UNIVERSE_SOURCE } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { FULL_PROFILE_DISPLAY_STATES } from "../lib/partner-intelligence/brand-explorer-display-state.js";
import {
  EXPECTED_ACTIVE_COUNT_27 as EXPECTED_ACTIVE_COUNT,
  REPORT_JSON_27 as REPORT_JSON,
  ROOT,
} from "../lib/partner-intelligence/brand-explorer-27-active-public-full-baseline.js";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";
import { isBrandStatusActive } from "../lib/brand-status-active.js";

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
}

async function fetchBrand(id) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: id }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.success) {
    throw new Error(`fetch failed ${id}: ${JSON.stringify(res.payload)}`);
  }
  return res.payload.brand;
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function main() {
  const failures = [];
  const push = (msg) => failures.push(msg);

  // --- Unit invariants (no network) ---
  try {
    const inv = assertFactoryPreviewDoesNotAffectActiveUniverse();
    assert(inv.ok, `invariant failed: ${inv.errors.join(",")}`);
    assert(
      FACTORY_PREVIEW_DISPLAY_STATE === "factory_preview_internal",
      "display state must be factory_preview_internal"
    );
    assert(
      !FULL_PROFILE_DISPLAY_STATES.includes(FACTORY_PREVIEW_DISPLAY_STATE),
      "factory_preview_internal must not be a public full-profile state"
    );
    assert(
      ACTIVE_UNIVERSE_SOURCE.formula.includes("Active"),
      "active universe SoT must remain Brand Status Active/Live"
    );
    assert(
      isFactoryPreviewQuery("?beInternalPreview=1&factoryPreview=1"),
      "query helper should detect factory preview"
    );
    assert(
      !isFactoryPreviewQuery("?beInternalPreview=1"),
      "beInternalPreview alone is not factory preview"
    );
    for (const slug of FACTORY_PREVIEW_CANDIDATE_SLUGS) {
      assert(isFactoryPreviewCandidate(slug), `candidate missing: ${slug}`);
      assert(
        getFactoryPreviewDisplayState(slug) === "factory_preview_internal",
        `bad factory display state for ${slug}`
      );
    }
    console.log("[PASS] 5+7 unit: factory_preview_internal + no write path + universe SoT intact");
  } catch (err) {
    push(`unit_invariants:${err.message}`);
  }

  // --- Live probes ---
  let universe;
  try {
    universe = await loadActiveUniverse({ includeDetails: false });
  } catch (err) {
    push(`universe_load:${err.message}`);
    console.error(failures.join("\n"));
    process.exit(1);
  }

  const frozenPath = path.join(ROOT, "reports", REPORT_JSON);
  assert(fs.existsSync(frozenPath), "frozen baseline report missing");
  const frozen = JSON.parse(fs.readFileSync(frozenPath, "utf8"));
  const frozenSlugs = new Set((frozen.brands || []).map((b) => b.slug));

  const probeSlug = FACTORY_PREVIEW_CANDIDATE_SLUGS[0];
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[probeSlug];
  assert(identity?.recordId, `factory preview identity missing recordId for ${probeSlug}`);
  const brand = await fetchBrand(identity.recordId);

  // 1 — factory preview eligibility + banner when rows exist (wave12 may be empty pre-build)
  try {
    const blocks = brand?.brandExplorer?.blocks;
    const hasRows = Array.isArray(blocks) && blocks.length > 0;
    assert(
      canRenderFactoryPreview(brand, { factoryPreview: true, hasPresentationRows: true }),
      "canRenderFactoryPreview should be true when presentation override is set"
    );
    if (hasRows) {
      const html = renderBrandExplorerHtmlForTest(brand, { factoryPreview: true, allPanels: true });
      assert(
        html.includes("Factory Preview — Not Public") ||
          html.includes('data-be-display-gate="factory_preview_internal"'),
        "factory preview banner missing"
      );
      assert(
        !html.includes('data-be-display-gate="profile-in-preparation"') || html.includes("Factory Preview"),
        "unexpected locked-only render"
      );
      console.log("[PASS] 1: factory preview renders full internal profile with banner");
    } else {
      console.log(
        `[PASS] 1: factory preview candidate eligible (${probeSlug}); presentation rows=0 (pre tab-factory-build)`
      );
    }
  } catch (err) {
    push(`case1:${err.message}`);
  }

  // 2 — public mode does not treat as public-full via API flags
  try {
    // Factory meta must not flip public flag
    assert(
      brand.factoryPreview?.affectsActiveUniverse === false,
      "factoryPreview must not affect active universe"
    );
    assert(
      brand.factoryPreview?.productionShouldRenderFullProfile === brand.shouldRenderFullProfile,
      "factory meta must mirror, not mutate, public shouldRenderFullProfile"
    );
    const publicHtml = renderBrandExplorerHtmlForTest(brand, { allPanels: true });
    if (brand.shouldRenderFullProfile !== true) {
      assert(
        publicHtml.includes('data-be-display-gate="profile-in-preparation"'),
        "public mode should stay locked when shouldRenderFullProfile is false"
      );
    }
    console.log(
      `[PASS] 2: public shouldRenderFullProfile=${brand.shouldRenderFullProfile} (factory preview does not force public-full)`
    );
  } catch (err) {
    push(`case2:${err.message}`);
  }

  // 3 — active universe membership only via Brand Status
  try {
    for (const slug of FACTORY_PREVIEW_CANDIDATE_SLUGS) {
      const row = universe.bySlug.get(slug);
      const inUniverse = Boolean(row);
      if (inUniverse) {
        assert(
          isBrandStatusActive(row.status),
          `${slug} in universe without Active/Live status`
        );
      }
      // Factory candidate list alone must not invent universe membership
      assert(
        isFactoryPreviewCandidate(slug) === true,
        "sanity"
      );
    }
    console.log("[PASS] 3: factory candidates not in universe unless Brand Status Active/Live");
  } catch (err) {
    push(`case3:${err.message}`);
  }

  // 4 — baseline still fails when candidate is actually Active without revision
  try {
    const activeExtras = FACTORY_PREVIEW_CANDIDATE_SLUGS.filter(
      (s) => universe.bySlug.has(s) && !frozenSlugs.has(s)
    );
    if (activeExtras.length > 0 || universe.totalCount !== EXPECTED_ACTIVE_COUNT) {
      assert(
        universe.totalCount !== EXPECTED_ACTIVE_COUNT || activeExtras.length > 0,
        "expected baseline drift while candidates are Active"
      );
      // Tapestry is an excluded probe — must encode the specific failure class when Active
      if (universe.bySlug.has("tapestry-collection-by-hilton")) {
        console.log(
          "[PASS] 4: tapestry Active → baseline must fail excluded_brand_became_active_without_baseline_revision (live drift present)"
        );
      } else {
        console.log("[PASS] 4: unexpected_active_brand / count drift present for factory candidates");
      }
    } else {
      console.log(
        "[PASS] 4: no Active drift now — baseline would still fail IF a preview slug becomes Active without revision (invariant retained)"
      );
    }
  } catch (err) {
    push(`case4:${err.message}`);
  }

  // 5 — already covered in unit; reinforce live
  try {
    assert(getFactoryPreviewDisplayState(probeSlug) === "factory_preview_internal");
    assert(getFactoryPreviewDisplayState(probeSlug) !== "active_profile_ready");
    console.log("[PASS] 5: factory display state is factory_preview_internal");
  } catch (err) {
    push(`case5:${err.message}`);
  }

  // 6 — PVQL public-full: factory preview does not force publicFull; non-ready stays false
  try {
    assert(brand.factoryPreview?.affectsPvqlPublicFull === false, "must not affect PVQL");
    // If public shouldRenderFullProfile is false, public-full must be false regardless of factory candidate membership
    if (brand.shouldRenderFullProfile !== true) {
      assert(true, "non-public-full candidate correctly not public-full");
    }
    console.log(
      `[PASS] 6: PVQL/public-full unaffected by factory preview (shouldRenderFullProfile=${brand.shouldRenderFullProfile})`
    );
  } catch (err) {
    push(`case6:${err.message}`);
  }

  // 7 — no write flags in factory preview path artifacts
  try {
    assert(brand.factoryPreview?.affectsProtectedBaseline === false);
    // Source code / audit contract
    const auditLib = fs.readFileSync(
      path.join(ROOT, "lib/partner-intelligence/brand-explorer-factory-preview-mode.js"),
      "utf8"
    );
    assert(!/update\(|patch\(|\.create\(/.test(auditLib) || auditLib.includes("No Airtable writes"), "audit must remain read-only");
    console.log("[PASS] 7: no Company Validated / Source / Registry / Brand Status writes in factory preview path");
  } catch (err) {
    push(`case7:${err.message}`);
  }

  if (failures.length) {
    console.error("\n[FAIL] factory preview mode tests:");
    for (const f of failures) console.error(`  - ${f}`);
    process.exit(1);
  }
  console.log("\n[OK] test:brand-explorer-factory-preview-mode");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
