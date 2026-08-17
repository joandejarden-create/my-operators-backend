#!/usr/bin/env node
/**
 * Frontend smoke tests for Market Demand page renderer.
 *   node scripts/validate-market-demand-frontend.mjs
 */
import { readFileSync } from "fs";
import vm from "vm";
import { fileURLToPath } from "url";
import path from "path";
import {
  getDealDemandCenters,
  getDealNearbyHotelSupply,
  getDealMarketDemandSnapshot,
} from "../api/market-demand.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const jsPath = path.join(__dirname, "../public/js/market-demand.js");

function loadMarketDemandGlobal() {
  const code = readFileSync(jsPath, "utf8");
  const sandbox = { window: {}, globalThis: {} };
  sandbox.window = sandbox.globalThis;
  vm.runInNewContext(code, sandbox, { filename: "market-demand.js" });
  return sandbox.window.MarketDemand;
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    status(c) {
      out.statusCode = c;
      return this;
    },
    json(b) {
      out.body = b;
      return this;
    },
    out,
  };
}

async function fetchState(dealId) {
  const handlers = [getDealDemandCenters, getDealNearbyHotelSupply, getDealMarketDemandSnapshot];
  const bodies = [];
  for (const h of handlers) {
    const res = mockRes();
    await h({ params: { dealId } }, res);
    bodies.push(res.out.body);
  }
  return {
    demandCenters: bodies[0]?.demandCenters || [],
    demandSummary: bodies[0]?.summary || {},
    nearbyHotelSupply: bodies[1]?.nearbyHotelSupply || [],
    snapshot: bodies[2]?.hasSnapshot ? bodies[2].snapshot : null,
  };
}

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

async function main() {
  const MD = loadMarketDemandGlobal();
  assert(MD && typeof MD.render === "function", "MarketDemand global loaded");

  const root = { innerHTML: "", querySelectorAll: () => [] };
  MD.render(root, { demandCenters: [], nearbyHotelSupply: [], snapshot: null }, { dealId: "recTEST" });
  assert(root.innerHTML.includes("No demand centers have been added"), "empty state copy present");
  assert(root.innerHTML.includes("Map view coming soon"), "map placeholder present");

  const dealId = process.argv[2] || "rec6JMTqtSUn1ygtd";
  const state = await fetchState(dealId);
  const root2 = { innerHTML: "", querySelectorAll: () => [] };
  MD.render(root2, state, { dealId, fullPage: true });
  const html = root2.innerHTML;

  assert(html.includes("Market Demand"), "page title");
  assert(html.includes("Sample International Airport") || state.demandCenters.length === 0, "demand center row or empty");
  assert(html.includes("Sample Comp Hotel") || state.nearbyHotelSupply.length === 0, "hotel row or empty");
  assert(html.includes("Demand Mix"), "demand mix section");
  if (state.snapshot) {
    assert(html.includes(state.snapshot.demandSummary.slice(0, 24)), "snapshot narrative");
  }

  if (state.demandSummary.categories) {
    const mixHtml = html;
    assert(mixHtml.includes(">1<") || state.demandCenters.length === 0, "mix shows counts");
  }

  if (failed) {
    console.error("\n" + failed + " frontend test(s) failed");
    process.exit(1);
  }
  console.log("\nFrontend smoke tests passed for deal", dealId);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
