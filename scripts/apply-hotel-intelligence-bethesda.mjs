/**
 * Apply Bethesda Marriott Hotel Intelligence (approved canary).
 *
 *   node scripts/apply-hotel-intelligence-bethesda.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { researchHotelIntelligence } from "../lib/hotel-intelligence/research/research-hotel-intelligence.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";
import { buildAdpHotelAttributes } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { loadHotelIntelligenceFromAirtable } from "../lib/hotel-intelligence/schema/hi-airtable-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HPC_ID = "recLuxvwwxID7U2B8";

const applyResult = await researchHotelIntelligence(HPC_ID, {
  mode: "apply",
  knownCventUrl:
    "https://www.cvent.com/venues/bethesda/hotel/bethesda-marriott/venue-759e1222-3f0f-4739-9375-e9cb7962c6a4",
});

// Rebuild profile from Airtable HI + HPC
const profile = await buildHotelIntelligenceProfile(HPC_ID);
const attrs = await buildAdpHotelAttributes(HPC_ID, { profile });
const hiLoaded = await loadHotelIntelligenceFromAirtable(HPC_ID);

const validation = {
  generatedAt: new Date().toISOString(),
  hpcHotelId: HPC_ID,
  apply: {
    ok: applyResult.ok,
    applied: applyResult.applied,
    applyResult: applyResult.applyResult,
    adpSyncResult: applyResult.adpSyncResult
      ? {
          createCount: applyResult.adpSyncResult.createCount,
          updateCount: applyResult.adpSyncResult.updateCount,
          deactivateCount: applyResult.adpSyncResult.deactivateCount,
        }
      : null,
    conflicts: applyResult.conflicts,
    blockers: applyResult.blockers,
    cost: applyResult.cost,
  },
  tables: {
    commercial: hiLoaded.commercial
      ? { id: hiLoaded.commercial.airtableRecordId, confidence: hiLoaded.commercial.confidence }
      : null,
    eventSpaces: hiLoaded.eventSpaces.map((s) => ({ id: s.airtableRecordId, name: s.name })),
    demandNodes: hiLoaded.demandNodes.map((n) => ({ id: n.airtableRecordId, name: n.name })),
    seasonality: hiLoaded.seasonality.map((s) => ({ id: s.airtableRecordId })),
    needPeriods: hiLoaded.needPeriods.map((s) => ({ id: s.airtableRecordId })),
    evidence: hiLoaded.evidence.map((e) => ({
      id: e.airtableRecordId,
      field: e.fieldName,
      confidence: e.confidence,
    })),
  },
  profile: {
    ok: profile.ok,
    completeness: profile.completeness,
    evidenceSummary: profile.evidenceSummary,
    rooms: profile.commercialProfile?.rooms,
    meeting: profile.commercialProfile?.meetingSpace,
    eventSpaceCount: profile.eventSpaces?.length,
    demandNodeCount: profile.demandNodes?.length,
    seasonalityCount: profile.seasonality?.length,
    needPeriodCount: profile.needPeriods?.length,
    fromHiAirtable: {
      commercial: profile.commercialProfile?.fromHiAirtable,
      eventSpaces: profile.eventSpaces?.every((e) => e.fromHiAirtable),
      demandNodes: profile.demandNodes?.every((d) => d.fromHiAirtable),
    },
  },
  adpAttributes: {
    total: attrs.counts?.total,
    usedInAdp: attrs.counts?.usedInAdp,
    unused: (attrs.attributes || []).filter((a) => !a.usedInAdp).map((a) => a.attributeName),
    byCategory: attrs.counts?.byCategory,
    missingCritical: attrs.missingCritical,
    sourceTypes: [...new Set((attrs.attributes || []).map((a) => a.sourceType))],
    sourcedFromHiTables: (attrs.attributes || []).filter((a) =>
      [
        "Hotel Commercial Profile",
        "Hotel Event Space",
        "Hotel Demand Node",
        "Hotel Seasonality",
      ].includes(a.sourceType)
    ).length,
  },
  legacyJsonDependencies: profile.evidenceSummary?.legacyJsonFallback || null,
  reconciliationPass:
    Boolean(hiLoaded.commercial) &&
    hiLoaded.eventSpaces.length >= 1 &&
    hiLoaded.demandNodes.length >= 1 &&
    hiLoaded.seasonality.length === 0 &&
    hiLoaded.needPeriods.length === 0 &&
    profile.commercialProfile?.fromHiAirtable === true &&
    profile.eventSpaces?.every((e) => e.fromHiAirtable) &&
    profile.demandNodes?.every((d) => d.fromHiAirtable) &&
    (attrs.missingCritical || []).length === 0,
};

const outDir = path.join(ROOT, "reports", "hotel-intelligence");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "bethesda-marriott-intelligence-apply-2026-09-29.json"),
  JSON.stringify({ applyResult, profile, attrs: { counts: attrs.counts, attributes: attrs.attributes }, hiLoaded, validation }, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(outDir, "bethesda-marriott-intelligence-validation-2026-09-29.json"),
  JSON.stringify(validation, null, 2) + "\n"
);

const md = [];
md.push("# Bethesda Marriott — Hotel Intelligence Apply + Validation");
md.push("");
md.push(`Generated: ${validation.generatedAt}`);
md.push(`Reconciliation pass: **${validation.reconciliationPass}**`);
md.push("");
md.push("## Record IDs");
md.push("");
md.push(`- Commercial: \`${validation.tables.commercial?.id}\``);
for (const s of validation.tables.eventSpaces) md.push(`- Event space ${s.name}: \`${s.id}\``);
for (const n of validation.tables.demandNodes) md.push(`- Demand node ${n.name}: \`${n.id}\``);
md.push(`- Evidence rows: ${validation.tables.evidence.length}`);
for (const e of validation.tables.evidence) md.push(`  - ${e.field}: \`${e.id}\` (${e.confidence})`);
md.push("");
md.push("## Profile");
md.push("");
md.push("```json");
md.push(JSON.stringify(validation.profile, null, 2));
md.push("```");
md.push("");
md.push("## ADP attributes");
md.push("");
md.push("```json");
md.push(JSON.stringify(validation.adpAttributes, null, 2));
md.push("```");
md.push("");
md.push("## Legacy JSON dependencies");
md.push("");
md.push("```json");
md.push(JSON.stringify(validation.legacyJsonDependencies, null, 2));
md.push("```");
md.push("");

fs.writeFileSync(
  path.join(outDir, "bethesda-marriott-intelligence-validation-2026-09-29.md"),
  md.join("\n")
);

console.log(JSON.stringify(validation, null, 2));
if (!validation.reconciliationPass) process.exitCode = 2;
