#!/usr/bin/env node
/**
 * Brand-agnostic Factory 1.0 candidate entrypoint.
 *
 * Usage:
 *   node scripts/be-factory-1-brand-candidate-v1.mjs --brand-id recEJCTDj1zrsjPM6
 *
 * Resolves a registered factory pack and delegates to the pack builder.
 * Packs must not invent Airtable IDs; brandId is always the Basics record id.
 */
import { spawnSync } from "child_process";
import { pathToFileURL } from "url";
import { join } from "path";

const ROOT = process.cwd();

const PACKS = {
  recEJCTDj1zrsjPM6: {
    id: "autograph-collection",
    module: "./lib/brand-explorer/factory-packs/autograph-collection-v1.js",
  },
  // Future brands register here by Basics record id — no new CLI required.
};

function parseArgs(argv) {
  let brandId = "";
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--brand-id" && argv[i + 1]) brandId = String(argv[++i]).trim();
  }
  return { brandId };
}

async function main() {
  const { brandId } = parseArgs(process.argv);
  if (!brandId) {
    console.error("Require --brand-id <Brand Basics rec…>");
    process.exit(2);
  }
  const entry = PACKS[brandId];
  if (!entry) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          status: "NO_FACTORY_PACK",
          brandId,
          registered: Object.keys(PACKS),
          note: "Register a factory pack under lib/brand-explorer/factory-packs/ then add the Basics id here.",
        },
        null,
        2
      )
    );
    process.exit(3);
  }
  const mod = await import(pathToFileURL(join(ROOT, entry.module.replace(/^\.\//, ""))).href);
  const pack = mod.FACTORY_PACK_AUTOGRAPH_V1 || mod.default || mod.FACTORY_PACK;
  if (!pack || pack.brandId !== brandId) {
    console.error("Pack brandId mismatch or missing export");
    process.exit(4);
  }
  const script = pack.candidateBuilderScript;
  const r = spawnSync(process.execPath, [script], {
    cwd: ROOT,
    encoding: "utf8",
    env: process.env,
    stdio: "inherit",
  });
  process.exit(r.status || 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
