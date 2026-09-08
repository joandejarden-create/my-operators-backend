#!/usr/bin/env node
/**
 * Compile Mexico Explorer Full HI dossiers from deep-research fixtures
 * and seed Research Center archive backfills.
 *
 * Usage:
 *   node scripts/compile-mexico-explorer-full-hi-dossiers.mjs --dry-run
 *   node scripts/compile-mexico-explorer-full-hi-dossiers.mjs --apply
 */

import fs from "node:fs";
import { validateDossier, clearDossierRegistryCache } from "../lib/hotel-intelligence/dossier/index.js";
import {
  writeMexicoExplorerDossierFixtures,
  MEXICO_EXPLORER_DOSSIER_META,
} from "../lib/hotel-intelligence/dossier/adapters/from-mexico-explorer-deep-research.js";
import { createResearchRepository } from "../lib/hotel-intelligence/research/repository.js";
import { ensureAllMexicoExplorerRun1Backfills } from "../lib/hotel-intelligence/research/backfill-mexico-explorer.js";
import { buildResearchCenterPayload } from "../lib/hotel-intelligence/research/center-payload.js";

const apply = process.argv.includes("--apply");

function main() {
  console.log(apply ? "MODE=apply" : "MODE=dry-run (fixtures still written; pass --apply for research-store backfill)");

  const written = writeMexicoExplorerDossierFixtures();
  clearDossierRegistryCache();

  for (const row of written) {
    const meta = MEXICO_EXPLORER_DOSSIER_META[row.hotelId];
    const raw = JSON.parse(fs.readFileSync(row.path, "utf8"));
    const v = validateDossier(raw);
    console.log(
      JSON.stringify({
        hotelId: row.hotelId,
        dossier_id: row.dossier_id,
        fixture: meta.fixture_file,
        validate_ok: v.ok,
        errors: v.errors,
        sections: (raw.sections || []).length,
        open_questions: raw.open_question_count,
        sources: raw.source_count,
        findings: raw.finding_count,
      })
    );
    if (!v.ok) process.exitCode = 1;
  }

  if (apply) {
    const repo = createResearchRepository();
    const requests = ensureAllMexicoExplorerRun1Backfills(repo);
    console.log(
      "backfill",
      requests.map((r) => ({ hotel_id: r.hotel_id, request_id: r.request_id, report_id: r.report_id }))
    );
  } else {
    console.log("skip research-store backfill (use --apply)");
  }

  clearDossierRegistryCache();
  for (const hotelId of Object.keys(MEXICO_EXPLORER_DOSSIER_META)) {
    const p = buildResearchCenterPayload({ hotel_id: hotelId });
    console.log("research_center", hotelId, {
      mode: p.mode,
      archive: (p.archive || []).length,
      latest: Boolean(p.latest_investigation),
      follow_ups: (p.recommended_follow_up || []).length,
      research_more: (p.research_more || []).length,
    });
  }
}

main();
