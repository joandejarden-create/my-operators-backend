#!/usr/bin/env node
/**
 * CLI: npm run adp-leak-audit-copy-from-adp-v1 -- --dry-run|--apply
 */
import { copyFromAdpToLeakAudit } from "../lib/ai-demand-positioning/leak-audit/copy-from-adp-v1.js";

const argv = process.argv.slice(2);
const apply = argv.includes("--apply") && !argv.includes("--dry-run");
const result = await copyFromAdpToLeakAudit({ dryRun: !apply });
console.log(JSON.stringify(result, null, 2));
if (!result.ok) process.exitCode = 1;
