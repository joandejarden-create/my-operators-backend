#!/usr/bin/env node
/**
 * Factory 2.0 — multi-brand research queue (no writeback, no paid research loop).
 * Inherits permanent loyalty-six + regional acronym standards for every queued brand.
 */
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import {
  BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1,
  LOYALTY_PROOF_REQUIRED_COUNT,
  REGIONAL_ACRONYMS,
} from "../lib/brand-explorer/be-factory-permanent-loyalty-region-standards-v1.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/brand-explorer/factory-2-0");
const SELECTION = join(ROOT, "reports/brand-explorer/factory-1-0/01-next-brand-selection.json");

const EXCLUDED = new Set([
  "receQkxgjlezsc1xg", // Curio — verified Presentation; do not mutate in batch
  "reclkgOzvAcBheUSo", // Ascend — verified Presentation; do not mutate in batch
]);

function main() {
  mkdirSync(OUT, { recursive: true });
  if (!existsSync(SELECTION)) {
    console.error("Missing selection report:", SELECTION);
    process.exit(2);
  }
  const sel = JSON.parse(readFileSync(SELECTION, "utf8"));
  const recommended = sel.recommended || null;
  const alts = Array.isArray(sel.top5Alternatives) ? sel.top5Alternatives : [];

  const queue = [];
  const pushBrand = (b, priority, note) => {
    if (!b?.id || EXCLUDED.has(b.id)) return;
    if (queue.some((q) => q.brandId === b.id)) return;
    queue.push({
      priority,
      brandId: b.id,
      brandName: b.name,
      parentCompany: b.parentCompany || null,
      presentationCount: b.presentationCount ?? null,
      factoryPilotScore: b.factoryPilotScore ?? null,
      status: "QUEUED_FOR_RESEARCH",
      writebackAuthorized: false,
      inheritsPermanentStandards: true,
      requiredLoyaltyProofCount: LOYALTY_PROOF_REQUIRED_COUNT,
      regionalAcronyms: REGIONAL_ACRONYMS,
      note,
    });
  };

  // Autograph is the in-flight pilot — keep first if still awaiting approval.
  pushBrand(
    recommended,
    1,
    "Factory 1.0 pilot — exact candidate validation; writeback only after founder approval"
  );
  let p = 2;
  for (const a of alts) {
    pushBrand(a, p++, "Factory 2.0 batch research candidate — prepare without founder interruption");
  }

  const report = {
    contract: "BE_FACTORY_2_0_MULTI_BRAND_RESEARCH_QUEUE_V1",
    generatedAt: new Date().toISOString(),
    permanentStandardsContract: BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1,
    lessonsInherited: ["BE-QL-LOYALTY-SIX-001", "BE-QL-REGION-ACRONYM-001"],
    rules: [
      "Exact six Key Benefits & Program Strengths for participating loyalty programs",
      "Incomplete research never replaces a valid six-card collection",
      "APAC/EMEA/CALA/LATAM always ALL CAPS in headings/components/rendering",
      "No mass Airtable writeback — per-brand exact-candidate approval required",
      "Curio and Ascend excluded from mutation queue",
    ],
    excludedProtected: [...EXCLUDED],
    queue,
    nextActions: queue.map((q) => ({
      brandId: q.brandId,
      steps: [
        "audit existing Airtable Presentation",
        "research gaps under permanent standards",
        "build cumulative candidate",
        "run permanent standards gate + 8080 Proposed QA",
        "dry-run write plan",
        "stop at AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL",
      ],
    })),
  };

  writeFileSync(join(OUT, "01-multi-brand-research-queue.json"), JSON.stringify(report, null, 2));
  writeFileSync(
    join(OUT, "01-multi-brand-research-queue.md"),
    `# Factory 2.0 Multi-Brand Research Queue\n\n` +
      `Permanent standards: \`${BE_FACTORY_PERMANENT_LOYALTY_REGION_STANDARDS_V1}\`\n\n` +
      queue.map((q) => `${q.priority}. **${q.brandName}** (\`${q.brandId}\`) — ${q.status}`).join("\n") +
      `\n`
  );
  console.log(JSON.stringify({ ok: true, queued: queue.length, path: join(OUT, "01-multi-brand-research-queue.json") }, null, 2));
}

main();
