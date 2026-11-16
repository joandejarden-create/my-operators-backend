#!/usr/bin/env node
/**
 * Sync bundled GDI publication packages (STAGED/CURRENT pointers + package dirs)
 * from a git ref into the current working tree WITHOUT switching branches.
 *
 * Use when a dirty primary checkout serves localhost but lacks the latest
 * staged package that already exists on main.
 *
 *   node scripts/gdi-sync-bundled-staged-packages-from-ref.mjs --ref origin/main --hotel recH9h3McgMNFQEla
 *   node scripts/gdi-sync-bundled-staged-packages-from-ref.mjs --ref origin/main --all-hotels
 *
 * Does not publish, write Airtable, or activate CUSTOMER_PUBLISHED.
 */
import { execSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const STORE = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
);

const args = process.argv.slice(2);
function argVal(name, fallback = null) {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : fallback;
}
const REF = argVal("--ref", "origin/main");
const HOTEL = argVal("--hotel", null);
const ALL = args.includes("--all-hotels");
const DRY = args.includes("--dry-run");

if (!HOTEL && !ALL) {
  console.error("Pass --hotel <recId> or --all-hotels");
  process.exit(2);
}

function sh(cmd) {
  return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
}

const hotels = ALL
  ? fs
      .readdirSync(STORE, { withFileTypes: true })
      .filter((d) => d.isDirectory() && d.name.startsWith("rec"))
      .map((d) => d.name)
  : [HOTEL];

const paths = [];
for (const hotelId of hotels) {
  paths.push(
    `lib/group-demand-intelligence/bundled-publication-packages/${hotelId}`,
  );
}

console.log(
  JSON.stringify(
    {
      ref: REF,
      dryRun: DRY,
      hotels,
      paths,
    },
    null,
    2,
  ),
);

if (DRY) process.exit(0);

sh(`git fetch ${REF.includes("/") ? REF.split("/")[0] : "origin"} ${REF.includes("/") ? REF.split("/").slice(1).join("/") : REF}`.replace(/\s+/g, " "));
// Prefer exact tree checkout from the resolved ref tip.
const tip = sh(`git rev-parse ${REF}`);
for (const p of paths) {
  sh(`git checkout ${tip} -- "${p.replace(/\\/g, "/")}"`);
  console.log(`synced ${p} from ${tip.slice(0, 12)}`);
}

for (const hotelId of hotels) {
  const staged = path.join(STORE, hotelId, "STAGED.json");
  if (fs.existsSync(staged)) {
    const j = JSON.parse(fs.readFileSync(staged, "utf8"));
    console.log(
      JSON.stringify(
        {
          hotelId,
          stagedFingerprint: j.contentFingerprintSha256 || null,
          stagedVersion: j.publicationVersion || null,
        },
        null,
        2,
      ),
    );
  }
}
