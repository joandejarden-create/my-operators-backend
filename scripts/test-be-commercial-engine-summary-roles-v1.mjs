#!/usr/bin/env node
/**
 * Commercial Engine summary tiles must retain narrative content.
 * Numeric KPI unavailable gates must not erase Channels/Campaigns/B2B/Owner Lens.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import {
  BE_FIELD_CONTENT_ROLES_V1,
  commercialSummarySlotKeys,
  isCommercialSummarySlot,
  isNumericContentRole,
  resolveContentRoleForSlot,
  shouldApplyNumericKpiUnavailableGate,
  isInternalResearchInstructionText,
  loadFieldContentRolesContract,
} from "../lib/brand-explorer/be-field-content-roles-v1.js";
import { decideSlotProvenance } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);

{
  const c = loadFieldContentRolesContract();
  assert.equal(c.version, "BE_FIELD_CONTENT_ROLES_CONTRACT_V1");
  assert.equal(BE_FIELD_CONTENT_ROLES_V1, "BE_FIELD_CONTENT_ROLES_V1");
  for (const k of commercialSummarySlotKeys()) {
    assert.equal(isCommercialSummarySlot(k), true);
    assert.equal(isNumericContentRole(resolveContentRoleForSlot(k)), false);
  }
  assert.equal(isNumericContentRole("number"), true);
  assert.equal(isNumericContentRole("percentage"), true);
  console.log("PASS field content roles contract");
}

{
  const curioBodies = {
    "commercial.kpi.channels":
      "Brand.com / Honors app · major OTAs · GDS · metasearch (where active)",
    "commercial.kpi.campaigns":
      "Hilton Honors and parent portfolio campaigns where the hotel is eligible",
    "commercial.kpi.b2b": "Corporate, SME, and RFP programs where active for this flag",
    "commercial.kpi.lens":
      "Underwrite Curio channel mix with local competitive set reality—not corporate averages alone.",
  };
  for (const [slot, body] of Object.entries(curioBodies)) {
    const role = resolveContentRoleForSlot(slot);
    assert.equal(
      shouldApplyNumericKpiUnavailableGate(role, body),
      false,
      `${slot} must not be numeric-N/A gated`,
    );
  }
  // Genuine numeric KPI still gates long prose
  assert.equal(
    shouldApplyNumericKpiUnavailableGate(
      "number",
      "Underwrite Curio channel mix with local competitive set reality—not corporate averages alone.",
    ),
    true,
  );
  console.log("PASS narrative cards retain content; numeric KPIs still gated");
}

{
  const decision = decideSlotProvenance(
    "commercial.kpi.channels",
    [
      {
        slotKey: "commercial.kpi.channels",
        title: "Channels",
        body: "Brand.com / Honors app · major OTAs · GDS · metasearch (where active)",
      },
    ],
    [
      {
        slotKey: "commercial.kpi.channels",
        title: "channels",
        body: "Ascend Hotel Collection channel KPI lens: obtain brand.com / OTA / GDS / direct mix baselines before modeling fee drag.",
      },
    ],
  );
  assert.equal(decision.applyCandidate, false, "must not replace complete summary with research instruction");
  console.log("PASS cumulative assembler retains commercial summaries");
}

{
  assert.equal(
    isInternalResearchInstructionText(
      "KPI figures are not disclosed in free reconstruction evidence. Owner diligence: request current brand/owner scorecards",
    ),
    true,
  );
  assert.equal(
    isInternalResearchInstructionText("Brand.com · major OTAs · GDS · metasearch"),
    false,
  );
  console.log("PASS research-instruction detector");
}

{
  const atelier = fs.readFileSync(
    path.join(ROOT, "public/js/brand-explorer-atelier-from-api.js"),
    "utf8",
  );
  assert.match(atelier, /isNumericKpiRole/);
  assert.match(atelier, /short_commercial_summary/);
  assert.match(atelier, /owner_insight/);
  assert.match(atelier, /COMMERCIAL_SUMMARY_SLOT_META/);
  assert.match(atelier, /Numeric validation applies ONLY to numeric roles/);
  // commercialKpiFromSlot must pass non-numeric role
  assert.match(atelier, /kpiCard\(lbl, val, \{ role: meta\.role/);
  console.log("PASS atelier wires narrative roles for Commercial Engine tiles");
}

{
  // Frozen Curio baseline still has the four populated bodies
  const baseline = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "public/brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json"),
      "utf8",
    ),
  );
  const blocks = baseline.blocks || baseline.presentationBlocks || [];
  const bySlot = Object.fromEntries(
    (Array.isArray(blocks) ? blocks : []).map((b) => [b.slotKey, b]),
  );
  // baseline may nest under presentation
  const list = Array.isArray(baseline.blocks)
    ? baseline.blocks
    : Array.isArray(baseline?.snapshot?.blocks)
      ? baseline.snapshot.blocks
      : baseline?.brandExplorer?.blocks || [];
  const found = {};
  for (const b of list.length ? list : Object.values(bySlot)) {
    if (b && b.slotKey && String(b.slotKey).startsWith("commercial.kpi.")) {
      found[b.slotKey] = b.body;
    }
  }
  // Prefer scanning full JSON text if structure differs
  const raw = fs.readFileSync(
    path.join(ROOT, "public/brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json"),
    "utf8",
  );
  for (const k of commercialSummarySlotKeys()) {
    assert.match(raw, new RegExp(`"slotKey":\\s*"${k.replace(/\./g, "\\.")}"`));
  }
  assert.match(raw, /Brand\.com \/ Honors app/);
  assert.match(raw, /Hilton Honors and parent portfolio campaigns/);
  assert.match(raw, /Corporate, SME, and RFP programs/);
  assert.match(raw, /Underwrite Curio channel mix/);
  console.log("PASS Curio baseline retains four commercial summaries");
}

console.log(
  JSON.stringify({
    ok: true,
    contract: "BE_FIELD_CONTENT_ROLES_CONTRACT_V1",
    airtableWrites: 0,
  }),
);
