#!/usr/bin/env node
/**
 * Run Brand AI Presence Index Pilot V1 — offline, no provider calls.
 */
import { runBrandPresenceIndexPilot } from "../lib/ai-visibility/competitive-moat/brand-presence-index-pilot.js";

const report = runBrandPresenceIndexPilot({ writeReport: true });

console.log("\nBrand AI Presence Index Pilot V1\n");
console.log(`  Customer visible brands: ${report.customerVisibleBrands}`);
console.log(`  Base peer count (v2): ${report.basePeerCount}`);
console.log(`  Internal additions: ${report.newInternalAdditions}`);
console.log(`  Final internal peer count: ${report.finalInternalPeerCount}`);
console.log(`  Peer set: ${report.newPeerSetVersion}`);
console.log(`  Pilot VALID: ${report.pilotResults.valid}/${report.pilotResults.totalSubjects} (${report.pilotResults.validPercent}%)`);
console.log(`  Readiness: ${report.readiness.aiPresenceIndex}`);
console.log(`  Provider calls: ${report.providerCalls}`);
console.log(`\nWrote reports/ai-visibility/brand-presence-index-pilot-v1.json\n`);
