#!/usr/bin/env node
/**
 * Verify staging (or any remote) Operator Explorer share matches local footprint + bundle.
 *
 * Usage:
 *   node scripts/verify-operator-explorer-share-parity.mjs
 *   SHARE_BASE=https://my-operators-backend-staging.up.railway.app node scripts/verify-operator-explorer-share-parity.mjs
 *   LOCAL_BASE=http://localhost:8080 SHARE_BASE=https://... node scripts/verify-operator-explorer-share-parity.mjs
 */

const OPERATOR_ID = process.env.OPERATOR_ID || "recWPKu5laVZxsvpn";
const LOCAL_BASE = (process.env.LOCAL_BASE || "http://localhost:8080").replace(/\/$/, "");
const SHARE_BASE = (
  process.env.SHARE_BASE || "https://my-operators-backend-staging.up.railway.app"
).replace(/\/$/, "");
const EXPECTED_BUILD = process.env.OE_EXPECTED_BUILD || "oe-he-cala-parity-20260529";

async function fetchJson(url) {
  const res = await fetch(url);
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = null;
  }
  return { ok: res.ok, status: res.status, data, text };
}

function footprintTotals(prefill) {
  if (!prefill) return null;
  return {
    exH: String(prefill.geo_total_existing_hotels || "").trim(),
    exR: String(prefill.geo_total_existing_rooms || "").trim(),
    piH: String(prefill.geo_total_pipeline_hotels || "").trim(),
    piR: String(prefill.geo_total_pipeline_rooms || "").trim(),
    source: prefill.footprintPortfolioSource || "",
  };
}

async function detailFootprint(base) {
  const { ok, data } = await fetchJson(
    `${base}/api/intake/third-party-operators/${encodeURIComponent(OPERATOR_ID)}`
  );
  if (!ok || !data?.success) {
    return { error: "detail API failed", meta: data?.meta };
  }
  const applied = footprintTotals(data.operator?.prefill);
  const census = data.operator?.censusFootprint?.totals || null;
  return {
    meta: data.meta || {},
    applied,
    census,
    hasEmbeddedCensus: !!data.operator?.censusFootprint?.ok,
  };
}

async function htmlBuildTag(base) {
  const res = await fetch(`${base}/operator-explorer-gold-mock.html`);
  const html = await res.text();
  const scripts = [...html.matchAll(/operator-explorer-gold-mock-data\.js\?v=([^"']+)/g)].map(
    (m) => m[1]
  );
  const scriptCount = (html.match(/<script src="js\/operator-/g) || []).length;
  return { ok: res.ok, build: scripts[0] || "", scriptCount };
}

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("OK:", msg);
}

async function main() {
  console.log("Operator Explorer share parity check");
  console.log("  operator:", OPERATOR_ID);
  console.log("  local:   ", LOCAL_BASE);
  console.log("  share:   ", SHARE_BASE);
  console.log("  build:   ", EXPECTED_BUILD);
  console.log("");

  let local;
  try {
    local = await detailFootprint(LOCAL_BASE);
  } catch (e) {
    fail(`Local server unreachable (${LOCAL_BASE}): ${e.message}`);
    local = null;
  }

  let share;
  try {
    share = await detailFootprint(SHARE_BASE);
  } catch (e) {
    fail(`Share host unreachable (${SHARE_BASE}): ${e.message}`);
    share = null;
  }

  const shareHtml = await htmlBuildTag(SHARE_BASE);

  if (shareHtml.ok) {
    if (shareHtml.build === EXPECTED_BUILD) {
      pass(`Share HTML loads gold-mock-data build ${EXPECTED_BUILD}`);
    } else {
      fail(
        `Share HTML build is "${shareHtml.build || "missing"}" (expected ${EXPECTED_BUILD}) — deploy latest public/ to Railway`
      );
    }
    if (shareHtml.scriptCount >= 14) {
      pass(`Share HTML includes ${shareHtml.scriptCount} operator scripts (full bundle)`);
    } else {
      fail(
        `Share HTML only has ${shareHtml.scriptCount} operator scripts (need 14+) — staging is on an old gold-mock.html`
      );
    }
  } else {
    fail("Share gold-mock.html did not load");
  }

  if (local?.applied && share?.applied) {
    const keys = ["exH", "exR", "piH", "piR", "source"];
    const mismatches = keys.filter((k) => local.applied[k] !== share.applied[k]);
    if (!mismatches.length) {
      pass(
        `Detail API footprint matches local (${local.applied.exH} / ${local.applied.exR} / ${local.applied.piH} / ${local.applied.piR}, source=${local.applied.source || "operator_setup"})`
      );
    } else {
      fail(
        `Detail API footprint mismatch on: ${mismatches.join(", ")}. local=${JSON.stringify(local.applied)} share=${JSON.stringify(share.applied)}`
      );
      if (!share.hasEmbeddedCensus && share.census == null) {
        console.error(
          "  hint: share API may predate server-side census merge; after deploy, client JS will reconcile via /census-footprint"
        );
      }
    }
  }

  const shareCensus = await fetchJson(
    `${SHARE_BASE}/api/intake/third-party-operators/${encodeURIComponent(OPERATOR_ID)}/census-footprint`
  );
  if (shareCensus.ok && shareCensus.data?.censusFootprint?.ok) {
    pass("Share host exposes /census-footprint endpoint");
  } else {
    fail("Share host missing or broken /census-footprint endpoint");
  }

  const sharePage = await fetchJson(`${SHARE_BASE}/operator-explorer-share.html`);
  if (sharePage.ok && sharePage.text.includes("operator-explorer-share-shell.css")) {
    pass("Share page route is deployed");
  } else {
    fail("Share page /operator-explorer-share.html not deployed yet");
  }

  if (process.exitCode) {
    console.log("\nDeploy fix:");
    console.log("  1. railway login");
    console.log("  2. railway link   (select staging service)");
    console.log("  3. railway up");
    console.log("  4. Re-run: npm run verify:oe-share-parity");
  } else {
    console.log("\nStaging link (same URL as platform popup iframe):");
    console.log(
      `  ${SHARE_BASE}/operator-explorer-gold-mock.html?id=${OPERATOR_ID}&embed=1`
    );
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
