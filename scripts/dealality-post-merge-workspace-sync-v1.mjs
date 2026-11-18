#!/usr/bin/env node
/**
 * Compatibility shim — use the canonical entrypoint only:
 *   npm run dealality:post-merge-sync
 *
 * This file remains so older docs/PRs that referenced the long script name do not break,
 * but it always delegates to scripts/dealality-post-merge-sync-v1.mjs.
 */
import "./dealality-post-merge-sync-v1.mjs";
