#!/usr/bin/env node
/** Thin alias — HI share assets are enforced via assert:production-assets. */
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const r = spawnSync(
  process.execPath,
  ["scripts/assert-production-assets.mjs", "--surface=HOTEL_INTELLIGENCE"],
  { cwd: root, stdio: "inherit" }
);
process.exit(r.status || 0);
