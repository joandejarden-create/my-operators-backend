#!/usr/bin/env node
/**
 * Ownership / Stakeholder Network v1 — model + presentation gates (HE-QL-020 / HE-QL-026).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  OWNERSHIP_CONTROL_NETWORK_V1,
  OCN_EDGE_TYPES,
  OCN_MAX_EXPANSION_HOTELS,
  OCN_MIN_NODE_LABEL_PX,
  buildOwnershipControlNetwork,
  layoutOwnershipControlNetwork,
  wrapNodeLabel,
  measureNodeBox,
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
        {
          display_name: "Second Decision Maker With A Very Long Display Name",
          title: "VP Asset Management",
          personId: "p2",
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

// Default: no people, no portfolio
const graph = buildOwnershipControlNetwork(hotel, ownership, {});
assert.equal(graph.version, OWNERSHIP_CONTROL_NETWORK_V1);
assert.ok(graph.nodes.some((n) => n.focus && n.kind === "hotel"));
assert.ok(!graph.nodes.some((n) => n.kind === "person"), "default must omit people");
assert.ok(!graph.nodes.some((n) => /Related hotel|Portfolio hotel/.test(n.role || "")));

const edgeTypes = new Set(graph.edges.map((e) => e.type));
assert.ok(edgeTypes.has(OCN_EDGE_TYPES.LEGALLY_OWNED_BY));
assert.ok(edgeTypes.has(OCN_EDGE_TYPES.ECONOMICALLY_CONTROLLED_BY));
assert.ok(edgeTypes.has(OCN_EDGE_TYPES.MANAGED_BY));
assert.ok(edgeTypes.has(OCN_EDGE_TYPES.BRANDED_AS));

for (const e of graph.edges) {
  if (e.type === OCN_EDGE_TYPES.MANAGED_BY || e.type === OCN_EDGE_TYPES.BRANDED_AS) {
    assert.notEqual(e.type, OCN_EDGE_TYPES.LEGALLY_OWNED_BY);
    assert.notEqual(e.type, OCN_EDGE_TYPES.ECONOMICALLY_CONTROLLED_BY);
  }
  assert.ok(e.shortLabel, "edges need short labels");
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

const ids = graph.edges.map((e) => e.id);
assert.equal(ids.length, new Set(ids).size);

const peopleGraph = buildOwnershipControlNetwork(hotel, ownership, {
  includePeople: true,
});
assert.ok(peopleGraph.nodes.some((n) => n.kind === "person"));
assert.ok(peopleGraph.nodes.every((n) => n.kind !== "person" || n.role === "Key person"));

const expanded = buildOwnershipControlNetwork(hotel, ownership, {
  includePeople: true,
  includePortfolioPreview: true,
  maxPortfolio: OCN_MAX_EXPANSION_HOTELS,
});
const portfolio = expanded.nodes.filter((n) => /Related hotel|Portfolio hotel/.test(n.role || ""));
assert.ok(portfolio.length >= 1);
assert.ok(portfolio.some((n) => n.verification === "proposed"));
assert.ok(portfolio.some((n) => n.pendingHpc === true));
assert.ok(portfolio.every((n) => n.verification !== "verified" || n.hpcId));

// Readable wrapping + measured boxes
const lines = wrapNodeLabel("Mullen Real Estate Capital", 22, 3);
assert.ok(lines.length >= 1);
assert.ok(lines.every((l) => l.length <= 23));
const box = measureNodeBox({ label: "Mullen Real Estate Capital", focus: false });
assert.ok(box.width >= 200);
assert.ok(box.height >= 70);
assert.equal(OCN_MIN_NODE_LABEL_PX, 13);

const laid = layoutOwnershipControlNetwork(expanded, { width: 960, height: 520 });
assert.ok(laid.positions[graph.focusHotelId]);
assert.ok(Object.keys(laid.positions).length === expanded.nodes.length);
assert.ok(laid.boxes[graph.focusHotelId]);

// No overlapping node centers within min separation (approx)
const nodeList = expanded.nodes;
for (let i = 0; i < nodeList.length; i++) {
  for (let j = i + 1; j < nodeList.length; j++) {
    const a = nodeList[i];
    const b = nodeList[j];
    const pa = laid.positions[a.id];
    const pb = laid.positions[b.id];
    const ba = laid.boxes[a.id];
    const bb = laid.boxes[b.id];
    const dx = Math.abs(pa.x - pb.x);
    const dy = Math.abs(pa.y - pb.y);
    if (dy < 40) {
      assert.ok(dx + 1 >= (ba.w + bb.w) / 2, `overlap risk ${a.label} vs ${b.label}`);
    }
  }
}

// Presentation wiring
const ws = read("public/js/hotel-intelligence-workspace.js");
assert.ok(ws.includes("data-ocn-host"));
assert.ok(ws.includes("mountOwnershipControlNetwork"));
assert.ok(ws.includes("Hotel Stakeholder"));
assert.ok(ws.includes("Executive Ownership Summary"));
assert.ok(ws.includes("Relevant Decision-Makers"));
assert.ok(!/Property contact fallback \(HPC\)/.test(ws));
assert.ok(!ws.includes("Ownership &amp; Control Hierarchy"));

const browser = read("public/js/he-ownership-control-network-v1.js");
assert.ok(browser.includes("HeOwnershipControlNetwork"));
assert.ok(browser.includes("LEGALLY_OWNED_BY"));
assert.ok(browser.includes("Explore Key People"));
assert.ok(browser.includes("Explore Related Hotels"));
assert.ok(browser.includes("Brand Development view"));
assert.ok(browser.includes("includePeople === true"));
assert.ok(!browser.includes("Expand sponsor hotels"));
assert.ok(browser.includes("wrapNodeLabel"));
assert.ok(browser.includes("elbowPath"));

const preview = read("public/hotel-explorer-preview.html");
assert.ok(preview.includes("he-ownership-control-network-v1.js"));

const css = read("public/css/hotel-explorer.css");
assert.ok(css.includes(".ocn-node"));
assert.ok(css.includes("ocn-legend__item--proposed"));
assert.ok(css.includes("ocn-node__label"));
assert.match(css, /ocn-node__label[\s\S]*font-size:\s*13px/);

const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
assert.ok(std.includes("Ownership & Control Network") || std.includes("Stakeholder"));

const reg = JSON.parse(
  read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
);
assert.ok(reg.lessons.some((l) => l.id === "HE-QL-020"));

console.log("PASS test:he-ownership-control-network-v1");
