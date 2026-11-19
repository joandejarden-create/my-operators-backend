#!/usr/bin/env node
/**
 * Ownership & Control Network v1 — model + presentation gates (HE-QL-020).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  OWNERSHIP_CONTROL_NETWORK_V1,
  OCN_EDGE_TYPES,
  OCN_MAX_EXPANSION_HOTELS,
  buildOwnershipControlNetwork,
  layoutOwnershipControlNetwork,
} from "../lib/hotel-intelligence/ownership/ownership-control-network-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

const hotel = {
  id: "recLuxvwwxID7U2B8",
  name: "Bethesda Marriott",
  brand: "Marriott Hotels",
  managementCompany: "Marriott International",
};

const ownership = {
  preview_mode: "PHASE3_PROPOSED",
  report: {
    ownership_and_control: {
      legal_property_owner_propco: {
        name: "Marbeth Acquisition LLC",
        preview_state: "PROPOSED",
        entity_id: "org_marbeth",
      },
      economic_owner_or_group: {
        name: "Mullen Real Estate Capital",
        preview_state: "PROPOSED",
        entity_id: "org_mullen",
      },
      operator: {
        name: "Marriott International",
        status: "HIGH",
        entity_id: "org_marriott",
      },
      brand: { name: "Marriott Hotels", status: "HIGH" },
      ubo_group: { status: "UNRESOLVED", known: false },
      control_status: "PARTIAL",
    },
    decision_authority: {
      people: [
        {
          display_name: "Example Sponsor Contact",
          title: "Principal",
          personId: "p1",
        },
      ],
    },
  },
  organization: {
    owned_controlled_hotels: [
      {
        hotel_name: "Bethesda Marriott",
        airtable_record_id: "recLuxvwwxID7U2B8",
        verification_bucket: "PROPOSED",
      },
      {
        hotel_name: "Proposed Portfolio Hotel",
        verification_bucket: "PROPOSED",
        include_as_owned: true,
      },
      {
        hotel_name: "Verified Sister Hotel",
        airtable_record_id: "recABCDEFGHIJKLMN",
        verification_bucket: "VERIFIED",
      },
    ],
  },
};

const graph = buildOwnershipControlNetwork(hotel, ownership, {
  includePeople: true,
  includePortfolioPreview: false,
});

assert.equal(graph.version, OWNERSHIP_CONTROL_NETWORK_V1);
assert.ok(graph.nodes.some((n) => n.focus && n.kind === "hotel"));

const edgeTypes = new Set(graph.edges.map((e) => e.type));
assert.ok(edgeTypes.has(OCN_EDGE_TYPES.LEGALLY_OWNED_BY));
assert.ok(edgeTypes.has(OCN_EDGE_TYPES.ECONOMICALLY_CONTROLLED_BY));
assert.ok(edgeTypes.has(OCN_EDGE_TYPES.MANAGED_BY));
assert.ok(edgeTypes.has(OCN_EDGE_TYPES.BRANDED_AS));

// Operator must not appear as ownership edge type
for (const e of graph.edges) {
  if (e.type === OCN_EDGE_TYPES.MANAGED_BY || e.type === OCN_EDGE_TYPES.BRANDED_AS) {
    assert.notEqual(e.type, OCN_EDGE_TYPES.LEGALLY_OWNED_BY);
    assert.notEqual(e.type, OCN_EDGE_TYPES.ECONOMICALLY_CONTROLLED_BY);
  }
}

const propco = graph.nodes.find((n) => /Marbeth/i.test(n.label));
const econ = graph.nodes.find((n) => /Mullen/i.test(n.label));
const operator = graph.nodes.find((n) => n.role === "Hotel operator");
assert.ok(propco);
assert.ok(econ);
assert.ok(operator);
assert.equal(propco.verification, "proposed");
assert.equal(econ.verification, "proposed");
assert.notEqual(operator.verification, "proposed");

// No duplicate edges
const ids = graph.edges.map((e) => e.id);
assert.equal(ids.length, new Set(ids).size);

// Default load does not dump full portfolio
assert.ok(!graph.nodes.some((n) => n.role === "Portfolio hotel"));

const expanded = buildOwnershipControlNetwork(hotel, ownership, {
  includePeople: true,
  includePortfolioPreview: true,
  maxPortfolio: OCN_MAX_EXPANSION_HOTELS,
});
const portfolio = expanded.nodes.filter((n) => n.role === "Portfolio hotel");
assert.ok(portfolio.length >= 1);
assert.ok(portfolio.some((n) => n.verification === "proposed"));
assert.ok(portfolio.some((n) => n.pendingHpc === true));
assert.ok(portfolio.every((n) => n.verification !== "verified" || n.hpcId));

const laid = layoutOwnershipControlNetwork(expanded, { width: 900, height: 480 });
assert.ok(laid.positions[graph.focusHotelId]);
assert.ok(Object.keys(laid.positions).length === expanded.nodes.length);

// Presentation wiring
const ws = read("public/js/hotel-intelligence-workspace.js");
assert.ok(ws.includes("data-ocn-host"));
assert.ok(ws.includes("mountOwnershipControlNetwork"));
assert.ok(ws.includes("Interactive Ownership &amp; Control Network"));
assert.ok(ws.includes("Executive Ownership Summary"));
assert.ok(ws.includes("Relevant Decision-Makers"));
assert.ok(!/Property contact fallback \(HPC\)/.test(ws));
assert.ok(!ws.includes("Ownership &amp; Control Hierarchy"));

const browser = read("public/js/he-ownership-control-network-v1.js");
assert.ok(browser.includes("HeOwnershipControlNetwork"));
assert.ok(browser.includes("LEGALLY_OWNED_BY"));
assert.ok(browser.includes("Expand sponsor hotels"));

const preview = read("public/hotel-explorer-preview.html");
assert.ok(preview.includes("he-ownership-control-network-v1.js"));

const css = read("public/css/hotel-explorer.css");
assert.ok(css.includes(".ocn-node"));
assert.ok(css.includes("ocn-legend__item--proposed"));

console.log("PASS test:he-ownership-control-network-v1");
