#!/usr/bin/env node
/**
 * Bethesda research consolidation reports (non-mutating).
 * Does NOT apply Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  reconcileBethesdaResearchInventory,
  buildBethesda27ProductionManifest,
  buildRockCreekCorrectionManifest,
  BETHESDA_SOURCE_OF_TRUTH,
  BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-research-consolidation-v1.js";
import {
  evaluateResearchPersistenceAuthorization,
  RESEARCH_PERSISTENCE_ACTIVATION,
  GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { execFileSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-research-consolidation-and-persistence-activation-v1",
);

function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
  );
  return p;
}

fs.mkdirSync(OUT, { recursive: true });

const recon = reconcileBethesdaResearchInventory(ROOT);
const manifest = buildBethesda27ProductionManifest(ROOT);
const rockCreek = buildRockCreekCorrectionManifest();

write("BETHESDA_59_TO_86_RECONCILIATION.json", recon);
write("EXACT_27_RECORD_PRODUCTION_MANIFEST.json", manifest);
write("ROCK_CREEK_SINGLE_FIELD_CORRECTION_MANIFEST.json", rockCreek);

write(
  "BETHESDA_59_TO_86_RECONCILIATION.md",
  `# Bethesda 59 → 86 reconciliation

| Set | Count |
|-----|------:|
| Canonical HEAD opportunities | ${recon.counts.canonical} |
| Preserved snapshot | ${recon.counts.preserved} |
| Shared (must keep) | ${recon.counts.shared} |
| Only in preserved (CREATE candidates) | ${recon.counts.onlyPreserved} |
| Only in canonical | ${recon.counts.onlyCanonical} |
| Shared title/org drift | ${recon.counts.sharedTitleOrgDrift} |
| Prior #372 manifest overlap | ${recon.creates.filter((c) => c.inPrior372Manifest).length}/${recon.counts.onlyPreserved} |

## Source-of-truth decision

**${BETHESDA_SOURCE_OF_TRUTH.decision}**

- Canonical on main: \`${BETHESDA_SOURCE_OF_TRUTH.canonicalFilesystemOnMain}\` (${recon.counts.canonical} records) — **do not overwrite/delete**.
- Preserved additive superset: \`${BETHESDA_SOURCE_OF_TRUTH.preservedSupersetSnapshot}\` (${recon.counts.preserved}).
- Airtable expected baseline for Bethesda: **59**.
- Authorize only the **${recon.counts.onlyPreserved}** composite keys absent from canonical/Airtable.

## Create IDs

${recon.onlyPreservedIds.map((id) => `- \`${id}\``).join("\n")}
`,
);

write(
  "PROVENANCE_AND_EVIDENCE_VALIDATION.md",
  `# Provenance and evidence validation

WATCHLIST / research-only rows are allowed. Missing \`demandThesis\` / \`firstDiscoveredAt\` are **preserved as absent** — not invented.

| opportunityId | org | priority | demandThesis | firstDiscoveredAt | sources | ledger hints | buildOk |
|---------------|-----|----------|--------------|-------------------|---------|--------------|---------|
${manifest.entries
  .map((e) => {
    const s = e.evidenceSummary;
    return `| \`${e.opportunityId}\` | ${s.organizationName || ""} | ${s.priority || ""} | ${s.demandThesis == null ? "null" : s.demandThesis} | ${s.firstDiscoveredAt || "absent"} | ${s.sourceUrlCount} | ${s.ledgerPathHints.length} | ${e.validation.buildOk} |`;
  })
  .join("\n")}

## Notes

- Validation blockers: **${manifest.blockers.length}**
- Ready for founder authorization: **${manifest.readyForFounderAuthorization}**
- Authorization id: \`${BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION}\`
`,
);

write(
  "CANONICAL_RESEARCH_SOURCE_OF_TRUTH.md",
  `# Canonical research source-of-truth decision

${BETHESDA_SOURCE_OF_TRUTH.rules.map((r) => `- ${r}`).join("\n")}

Decision code: \`${BETHESDA_SOURCE_OF_TRUTH.decision}\`

Preserved snapshot confirmed present: **${fs.existsSync(recon.paths.preserved)}**
`,
);

write(
  "ROCK_CREEK_CORRECTION_MANIFEST.md",
  `# Rock Creek single-field correction (NOT APPLIED)

| Field | Value |
|-------|-------|
| Record | \`${rockCreek.airtableRecordId}\` |
| Field | ${rockCreek.field} |
| After | \`${rockCreek.after}\` |
| Preserve NO_LODGING | ${rockCreek.preserveNoLodging} |
| Separate from Bethesda migration | ${rockCreek.separateFromBethesdaResearchMigration} |
| Applied | **false** |
`,
);

const authLocked = evaluateResearchPersistenceAuthorization({
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
});

write("FULL_E2E_AUTOMATED_PERSISTENCE_CERTIFICATION.md", `# Full E2E automated persistence certification

Bridge: \`${GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1}\`

| Capability | Status |
|------------|--------|
| Discover → resolve IDs → dedupe → persist research | Bridge wired in canonical Full E2E entry |
| Material update vs UNCHANGED | \`diffMaterialResearchFields\` / store upsert |
| Preserve firstDiscovered* | Store preserves on update |
| Partial failure isolation | Per-row try/catch in bridge |
| Read-back option | \`readBackResearchPersistence\` |
| GDO / publication separate | Rejected on research bridge |
| Default production | **DISABLED** (\`${RESEARCH_PERSISTENCE_ACTIVATION.envGate}\`) |
| Apply with token but env off | dry-run + blockedApply (certified) |

Production unlocked now: **${authLocked.productionUnlocked}**
`);

write(
  "PRODUCTION_AUTHORIZATION_BOUNDARIES.md",
  `# Production authorization boundaries

1. Bethesda 27 research CREATEs require explicit founder approval of \`${BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION}\` **and** a separate apply instruction.
2. Rock Creek Venue ID correction requires its own allowlist authorization.
3. Automatic Full E2E research persistence requires env \`${RESEARCH_PERSISTENCE_ACTIVATION.envGate}=1\` plus per-run exact write-set tokens.
4. Paid research authorization never unlocks Airtable writes.
5. GDO and customer publication remain independently gated.
6. This package does **not** apply any production writes.
`,
);

let isolationStaged = 0;
let isolationHead = null;
try {
  isolationHead = execFileSync(
    "git",
    ["-C", path.resolve(ROOT, "../wt-gdi-isolation-post-365"), "rev-parse", "--short", "HEAD"],
    { encoding: "utf8" },
  ).trim();
  isolationStaged = execFileSync(
    "git",
    ["-C", path.resolve(ROOT, "../wt-gdi-isolation-post-365"), "diff", "--cached", "--name-only"],
    { encoding: "utf8" },
  )
    .trim()
    .split(/\r?\n/)
    .filter(Boolean).length;
} catch {
  isolationStaged = -1;
}

write(
  "REMAINING_WIP_PRESERVATION_STATUS.md",
  `# Remaining WIP preservation status

| Asset | Status |
|-------|--------|
| Isolation WT | detached \`${isolationHead || "unknown"}\`, staged **${isolationStaged}** |
| Unique commit \`72df16986\` | Keep until evidence archive decision |
| Bethesda 86 preserve file | \`${recon.paths.preserved}\` — **protected** |
| Primary untracked leftovers | Moved under isolation \`_wip-preserve-20261118/from-primary-untracked*\` |
| Pursuits FOLLOW_UP_DUE delta | Still quarantined on \`72df16986\` — not reviewed/merged |
| ADP / nested AVA reconcile | Untouched |

Do not retire isolation or delete \`72df16986\` until durable archive of run reports + segment/research-state is decided.
`,
);

write(
  "FOUNDER_DECISIONS.md",
  `# Founder decisions required

1. **Authorize or reject** the exact 27 Bethesda research CREATEs in \`EXACT_27_RECORD_PRODUCTION_MANIFEST.json\` (ready=${manifest.readyForFounderAuthorization}).
2. **Authorize or reject** Rock Creek Venue ID → \`${rockCreek.after}\` on \`${rockCreek.airtableRecordId}\`.
3. **Activate or defer** automatic production research persistence (\`${RESEARCH_PERSISTENCE_ACTIVATION.envGate}\`).
4. Decide durable archive vs discard for isolation staged evidence / \`72df16986\`.
5. Review quarantined pursuits FOLLOW_UP_DUE change.

**STOP** — no production Airtable writes under this delivery.
`,
);

write(
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  `# Founder executive summary — Bethesda consolidation & persistence activation prep

## Verdict

Bethesda **59 canonical + 27 preserved creates** reconciled. Exact research-only production manifest prepared. Full E2E persistence bridge remains fail-closed. **No Airtable writes applied.**

| Metric | Value |
|--------|------:|
| Canonical Bethesda | ${recon.counts.canonical} |
| Preserved snapshot | ${recon.counts.preserved} |
| CREATE candidates | ${recon.counts.onlyPreserved} |
| Manifest ready | ${manifest.readyForFounderAuthorization} |
| Blockers | ${manifest.blockers.length} |
| Rock Creek applied | false |
| Automatic production persistence | disabled |

Reports directory: \`reports/gdi-bethesda-research-consolidation-and-persistence-activation-v1/\`
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT,
      counts: recon.counts,
      readyForFounderAuthorization: manifest.readyForFounderAuthorization,
      blockers: manifest.blockers,
      rockCreekApplied: false,
      productionWritesAuthorized: false,
    },
    null,
    2,
  ),
);
