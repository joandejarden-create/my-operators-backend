/**
 * Thin wrapper — Active/Live Deal Terms populate.
 * Prefer: node scripts/apply-brand-deal-terms-batch.mjs --active-only …
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const target = path.join(__dirname, "apply-brand-deal-terms-batch.mjs");
const extra = process.argv.slice(2);
if (!extra.includes("--all") && !extra.includes("--under-review") && !extra.includes("--active-only")) {
  extra.push("--active-only");
}
const r = spawnSync(process.execPath, [target, ...extra], { stdio: "inherit" });
process.exit(r.status ?? 1);
