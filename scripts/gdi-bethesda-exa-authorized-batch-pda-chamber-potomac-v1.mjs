#!/usr/bin/env node
/**
 * Founder-authorized Exa batch — PDA / Greater Bethesda Chamber / Potomac Events.
 * Hard ceiling: 3 paid Agent calls · $12 USD.
 *
 * Usage:
 *   node scripts/gdi-bethesda-exa-authorized-batch-pda-chamber-potomac-v1.mjs --dry-run
 *   node scripts/gdi-bethesda-exa-authorized-batch-pda-chamber-potomac-v1.mjs --apply
 */
import {
  executeBethesdaExaAuthorizedBatch,
  AUTHORIZATION_ID,
  AUTHORIZED_TARGETS,
  SPEND_CEILING,
} from "../lib/group-demand-intelligence/research/bethesda-exa-authorized-batch-pda-chamber-potomac-v1.js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run") || !args.includes("--apply");

console.log(
  JSON.stringify(
    {
      authorizationId: AUTHORIZATION_ID,
      mode: dryRun ? "dry-run" : "apply",
      allowlistIds: AUTHORIZED_TARGETS.map((t) => t.id),
      ceilings: SPEND_CEILING,
    },
    null,
    2
  )
);

const result = await executeBethesdaExaAuthorizedBatch({
  dryRun,
  authorizeExaAgentLive: !dryRun,
});

console.log(
  JSON.stringify(
    {
      ok: result.ok,
      stopped: result.stopped || null,
      paidCallsIssued: result.ledger?.paidCallsIssued,
      spentUsd: result.ledger?.spentUsd,
      remainingUsd:
        result.ledger != null
          ? Number((SPEND_CEILING.maxUsd - result.ledger.spentUsd).toFixed(6))
          : null,
      scorecards: result.scorecards || result.ledger?.scorecards || null,
      persistOk: result.persist?.ok === true,
      outDir: result.outDir || null,
      status: result.ledger?.status,
    },
    null,
    2
  )
);

if (!result.ok && result.stopped) process.exitCode = 2;
