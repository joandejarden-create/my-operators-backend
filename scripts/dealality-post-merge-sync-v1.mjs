#!/usr/bin/env node
/**
 * Dealality post-merge product workspace sync.
 *
 *   npm run dealality:post-merge-sync -- --stream he|adp|gdi --pr <n>
 *   npm run dealality:post-merge-sync -- --stream he --pr 390 --dry-run
 */
import {
  formatPostMergeSyncReport,
  postMergeSyncFromArgv,
} from "../lib/dealality-dev-os/post-merge-sync.js";

const argv = process.argv.slice(2);
const json = argv.includes("--json");
const result = postMergeSyncFromArgv(argv);

if (json) {
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} else {
  process.stdout.write(`${formatPostMergeSyncReport(result)}\n`);
}

process.exit(result.ok ? 0 : 2);
