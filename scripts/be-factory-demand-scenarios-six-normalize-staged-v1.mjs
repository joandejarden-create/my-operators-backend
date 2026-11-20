#!/usr/bin/env node
/**
 * Normalize staged Factory candidates' commercial.demand rows to exact six.
 * Dry-run by default. Pass --apply to rewrite frozen approval payloads + stage.json.
 * Never writes Airtable.
 *
 * Usage:
 *   npm run be-factory-demand-scenarios-six-normalize
 *   npm run be-factory-demand-scenarios-six-normalize -- --apply
 */
import { existsSync, readdirSync, readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  DEMAND_FIT_LABELS,
  DEMAND_SCENARIO_SLOT,
  enforceDemandScenariosExactSixOnBlocks,
  validateDemandScenariosExactSix,
} from "../lib/brand-explorer/be-factory-permanent-demand-scenarios-six-v1.js";

function rebuildDefaultSix(brandName) {
  const short = brandName || "This brand";
  const rows = [
    ["Gateway Urban", DEMAND_FIT_LABELS.STRONG_FIT, `${short} fits gateway urban demand where parent distribution and loyalty support transient and corporate mixes—underwrite city competitive set.`],
    ["Corporate-Led Urban", DEMAND_FIT_LABELS.MODERATE_FIT, `Corporate-led urban accounts can pull ${short} when the product matches account brand ladders; confirm negotiated rates before assuming contracted volume.`],
    ["Regional & Secondary Markets", DEMAND_FIT_LABELS.MODERATE_FIT, `Secondary markets work when ${short}'s product and fee load clear local ADR reality; diligence source-market seasonality.`],
    ["Resort & Coastal Leisure", DEMAND_FIT_LABELS.LIMITED_FIT, `Leisure resort fit depends on whether ${short}'s service model matches resort operating complexity.`],
    ["Conversion & Repositioning", DEMAND_FIT_LABELS.STRONG_FIT, `Conversion / repositioning is a core owner path when independents seek ${short} affiliation and loyalty access—underwrite PIP scope first.`],
    ["Highway & Interstate Transient", DEMAND_FIT_LABELS.NOT_A_FIT, `Highway / interstate limited-service economics generally sit outside ${short}'s product and fee structure.`],
  ];
  return rows.map(([title, body, explanation], i) => ({
    slotKey: DEMAND_SCENARIO_SLOT,
    title,
    body,
    sort: 2100 + i,
    active: true,
    demandScenarioDescription: explanation,
    source: "factory-2-0-demand-scenarios-six-normalize",
  }));
}

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APPLY = process.argv.includes("--apply");
const previewDir = join(ROOT, "public/brand-explorer-phase2e-preview");
const stageRoot = join(ROOT, "data/brand-explorer/factory-2-0/brands");

function loadJson(p) {
  return JSON.parse(readFileSync(p, "utf8"));
}

function extractBlocks(doc) {
  if (Array.isArray(doc)) return doc;
  if (Array.isArray(doc?.brandExplorer?.blocks)) return doc.brandExplorer.blocks;
  if (Array.isArray(doc.presentationBlocks)) return doc.presentationBlocks;
  if (Array.isArray(doc.blocks)) return doc.blocks;
  if (Array.isArray(doc.candidate?.presentationBlocks)) return doc.candidate.presentationBlocks;
  if (Array.isArray(doc.candidate?.blocks)) return doc.candidate.blocks;
  if (Array.isArray(doc.candidate?.brandExplorer?.blocks)) return doc.candidate.brandExplorer.blocks;
  return null;
}

function writeBlocks(doc, blocks) {
  if (Array.isArray(doc)) return blocks;
  if (doc?.brandExplorer && Array.isArray(doc.brandExplorer.blocks)) {
    return {
      ...doc,
      brandExplorer: { ...doc.brandExplorer, blocks },
    };
  }
  if (Array.isArray(doc.presentationBlocks)) {
    return { ...doc, presentationBlocks: blocks };
  }
  if (Array.isArray(doc.blocks)) {
    return { ...doc, blocks };
  }
  if (doc.candidate && Array.isArray(doc.candidate.presentationBlocks)) {
    return {
      ...doc,
      candidate: { ...doc.candidate, presentationBlocks: blocks },
    };
  }
  if (doc.candidate && Array.isArray(doc.candidate.blocks)) {
    return { ...doc, candidate: { ...doc.candidate, blocks } };
  }
  if (doc.candidate?.brandExplorer && Array.isArray(doc.candidate.brandExplorer.blocks)) {
    return {
      ...doc,
      candidate: {
        ...doc.candidate,
        brandExplorer: { ...doc.candidate.brandExplorer, blocks },
      },
    };
  }
  return doc;
}

function processFile(absPath, rows) {
  const doc = loadJson(absPath);
  const blocks = extractBlocks(doc);
  if (!blocks) return;
  const demand = blocks.filter((b) => String(b.slotKey || "") === DEMAND_SCENARIO_SLOT);
  if (!demand.length) return;
  const before = validateDemandScenariosExactSix(blocks, { allowEmpty: true });
  let enforced = enforceDemandScenariosExactSixOnBlocks(blocks);
  let action = enforced.audit.action;
  let outBlocks = enforced.blocks;

  // Replace thin "Demand Pattern" narrative stub OR incomplete sets with brand-aware exact six
  const onlyStub =
    demand.length === 1 &&
    /^demand\s*pattern$/i.test(String(demand[0].title || ""));
  if (onlyStub || (enforced.needsRemediation && !enforced.validation.ok)) {
    const brandName =
      doc.brandName ||
      doc.candidate?.brandName ||
      doc.brandExplorer?.brandName ||
      "This brand";
    const other = blocks.filter((b) => String(b.slotKey || "") !== DEMAND_SCENARIO_SLOT);
    outBlocks = [...other, ...rebuildDefaultSix(brandName)];
    enforced = {
      blocks: outBlocks,
      audit: { action: "rebuilt_default_exact_six", inputCount: demand.length },
      validation: validateDemandScenariosExactSix(outBlocks),
      needsRemediation: false,
    };
    action = "rebuilt_default_exact_six";
  }

  rows.push({
    file: absPath.replace(ROOT + "\\", "").replace(ROOT + "/", ""),
    beforeCount: before.count,
    beforeOk: before.ok,
    afterCount: enforced.validation.count,
    afterOk: enforced.validation.ok,
    action,
    excluded: (enforced.audit.excluded || []).length,
    needsRemediation: !!enforced.needsRemediation,
    applied: false,
  });
  if (
    APPLY &&
    enforced.validation.ok &&
    (action === "enforced_exact_six" || action === "rebuilt_default_exact_six")
  ) {
    try {
      const next = writeBlocks(doc, outBlocks);
      writeFileSync(absPath, JSON.stringify(next, null, 2) + "\n");
      rows[rows.length - 1].applied = true;
    } catch (err) {
      rows[rows.length - 1].writeError = String(err?.message || err);
    }
  }
}

function main() {
  const rows = [];
  if (existsSync(previewDir)) {
    for (const f of readdirSync(previewDir)) {
      if (!/-factory2-frozen-approval-payload-v1\.json$/i.test(f) && !/frozen-approval-payload/i.test(f)) {
        continue;
      }
      processFile(join(previewDir, f), rows);
    }
  }
  if (existsSync(stageRoot)) {
    for (const id of readdirSync(stageRoot)) {
      const stagePath = join(stageRoot, id, "stage.json");
      const candPath = join(stageRoot, id, "candidate.json");
      if (existsSync(stagePath)) processFile(stagePath, rows);
      if (existsSync(candPath)) processFile(candPath, rows);
    }
  }

  const summary = {
    contract: "BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1",
    apply: APPLY,
    filesTouched: rows.length,
    enforcedOk: rows.filter((r) => r.afterOk).length,
    needsRemediation: rows.filter((r) => r.needsRemediation).length,
    applied: rows.filter((r) => r.applied).length,
    rows,
  };
  const out = join(ROOT, "reports/brand-explorer/factory-2-0/72-demand-scenarios-six-normalize.json");
  writeFileSync(out, JSON.stringify(summary, null, 2) + "\n");
  console.log(JSON.stringify({ ok: true, report: out, ...summary, rows: undefined, sample: rows.slice(0, 8) }, null, 2));
}

main();
