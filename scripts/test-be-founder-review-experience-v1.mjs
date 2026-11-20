#!/usr/bin/env node
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildCumulativeProposedBrand } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const preview = path.join(root, "public/brand-explorer-phase2e-preview");
const gate = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-gate1-renderer-parity-payload.json"), "utf8"),
);
const cand = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-collection-phase2h-candidate.json"), "utf8"),
);
const { brand, assembly } = buildCumulativeProposedBrand(gate, cand);
const blocks = brand.brandExplorer.blocks;

const negotiable = blocks.find((b) => b.slotKey === "economics.negotiable_items");
assert.ok(negotiable, "negotiable items retained");
const bullets = String(negotiable.body || "")
  .split(/\n+/)
  .map((s) => s.trim())
  .filter(Boolean);
assert.ok(bullets.length >= 4, "Often Negotiated must remain multi-bullet");
assert.ok(
  !/diligence:|do not reuse|request current/i.test(negotiable.body || ""),
  "no diligence paragraph in Often Negotiated",
);

const members = blocks.find((b) => b.slotKey === "loyalty.kpi.members");
assert.ok(members);
assert.ok(
  !/request current.+development/i.test(members.body || ""),
  "KPI members must not carry diligence prose",
);

assert.ok(
  assembly.provenanceBySlot["economics.negotiable_items"].provenance === "RETAINED_FROM_CURRENT",
);
assert.ok(assembly.provenanceBySlot["loyalty.kpi.members"].provenance === "RETAINED_FROM_CURRENT");

// Structured commercial values remain available for renderer
assert.strictEqual(gate.loyaltyCommercial.formValues.totalGlobalMembersMillions, 165);
assert.strictEqual(gate.loyaltyCommercial.formValues.typicalLoyaltyRoomsPercent, 54);

const html = fs.readFileSync(path.join(root, "public/brand-explorer-compare.html"), "utf8");
assert.ok(html.includes("becsIssuesDrawer"), "issues drawer present");
assert.ok(html.includes("becsViewIssues"), "View Issues control present");
assert.ok(html.includes("becs-advanced-menu"), "Advanced diagnostics menu present");
assert.ok(html.includes('data-becs-mode="full"'), "Full Comparison mode present");

console.log(
  JSON.stringify(
    {
      ok: true,
      negotiableBullets: bullets.length,
      membersBody: members.body,
      heldOut: assembly.incompleteDrafts.length,
    },
    null,
    2,
  ),
);
