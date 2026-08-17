#!/usr/bin/env node
/**
 * Remediate footprint.openings for all Brand Explorer brands (active + true-incomplete):
 * Ascend card template + CALA-first property selection (fallback when no CALA).
 *
 * Dry-run default:
 *   npm run brand-explorer-openings-ascend-cala-remediation -- --dry-run
 *
 * Apply:
 *   npm run brand-explorer-openings-ascend-cala-remediation -- --apply \
 *     --approve-openings-ascend-cala-remediation \
 *     --confirm-no-company-validation-claim \
 *     --confirm-cala-first-then-fallback
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import {
  OPENINGS_ALL_BRANDS,
  OPENINGS_ASCEND_CALA_REMEDIATION_VERSION,
  PRESENTATION_TABLE,
  buildOpeningsAscendCalaPack,
  enrichChoicePackIfNeeded,
  mergeLiveOpeningsIntoPack,
  planOpeningsAscendCalaPatches,
  visibleOpenings,
} from "../lib/partner-intelligence/brand-explorer-openings-ascend-cala-remediation.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...OPENINGS_ALL_BRANDS];
  return {
    brands,
    apply: argv.includes("--apply"),
    approve: argv.includes("--approve-openings-ascend-cala-remediation"),
    confirmNoCv: argv.includes("--confirm-no-company-validation-claim"),
    confirmCala: argv.includes("--confirm-cala-first-then-fallback"),
    dryRun: !argv.includes("--apply"),
  };
}

async function fetchBrand(recordId) {
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${recordId}`);
  return res.payload.brand;
}

async function airtablePatch({ apiKey, baseId, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json.error?.message || `PATCH failed ${recordId}: ${res.status}`);
  }
  return json;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }
  if (opts.apply && (!opts.approve || !opts.confirmNoCv || !opts.confirmCala)) {
    console.error(
      "Apply requires --approve-openings-ascend-cala-remediation --confirm-no-company-validation-claim --confirm-cala-first-then-fallback"
    );
    process.exit(1);
  }

  const report = {
    version: OPENINGS_ASCEND_CALA_REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    mode: opts.apply ? "apply" : "dry-run",
    brands: [],
    patches: [],
    summary: {},
  };

  for (const slug of opts.brands) {
    let pack = buildOpeningsAscendCalaPack(slug);
    pack = await enrichChoicePackIfNeeded(pack);
    const brand = await fetchBrand(pack.recordId);
    const live = visibleOpenings(brand.brandExplorer?.blocks || []);
    pack = mergeLiveOpeningsIntoPack(pack, live);
    const plan = planOpeningsAscendCalaPatches(live, pack);

    if (opts.apply) {
      for (const patch of plan.patches) {
        await airtablePatch({
          apiKey,
          baseId,
          recordId: patch.recordId,
          fields: patch.fields,
        });
        patch.applied = true;
      }
    }

    report.brands.push({
      brandSlug: slug,
      brandName: pack.brandName,
      tierUsed: pack.tierUsed,
      calaCount: pack.calaCount,
      packCount: pack.cards.length,
      liveCount: live.length,
      patched: plan.patched,
      unmatchedLive: plan.unmatchedLive,
      unmatchedPack: plan.unmatchedPack,
      cardTitles: pack.cards.map((c) => ({ title: c.title, isCala: c.isCala })),
      validation: plan.validation,
    });
    report.patches.push(...plan.patches);
    console.log(
      `${slug}: tier=${pack.tierUsed} cala=${pack.calaCount} pack=${pack.cards.length} live=${live.length} patches=${plan.patched}`
    );
  }

  report.summary = {
    brands: report.brands.length,
    patches: report.patches.length,
    calaFirst: report.brands.filter((b) => b.tierUsed === "cala").length,
    calaPartial: report.brands.filter((b) => b.tierUsed === "cala_partial_fallback").length,
    nonCalaFallback: report.brands.filter((b) => b.tierUsed === "non_cala_fallback").length,
    thinPacks: report.brands.filter((b) => b.packCount < 3).map((b) => b.brandSlug),
  };

  const outJson = path.join(ROOT, "reports", "brand-explorer-openings-ascend-cala-remediation.json");
  const outMd = path.join(ROOT, "reports", "brand-explorer-openings-ascend-cala-remediation.md");
  fs.mkdirSync(path.dirname(outJson), { recursive: true });
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2));
  const md = [
    `# Openings Ascend + CALA-first remediation`,
    ``,
    `Mode: **${report.mode}** · Patches: **${report.summary.patches}**`,
    ``,
    `- CALA-only packs: ${report.summary.calaFirst}`,
    `- CALA + fallback: ${report.summary.calaPartial}`,
    `- Non-CALA fallback: ${report.summary.nonCalaFallback}`,
    `- Thin packs (<3): ${report.summary.thinPacks.join(", ") || "(none)"}`,
    ``,
    `| Brand | Tier | CALA | Pack | Live | Patches |`,
    `| --- | --- | ---: | ---: | ---: | ---: |`,
    ...report.brands.map(
      (b) =>
        `| ${b.brandSlug} | ${b.tierUsed} | ${b.calaCount} | ${b.packCount} | ${b.liveCount} | ${b.patched} |`
    ),
    ``,
  ];
  fs.writeFileSync(outMd, md.join("\n"));
  console.log(`Wrote ${outJson}`);
  console.log(`Wrote ${outMd}`);
  console.log(`Mode: ${report.mode} · patches: ${report.summary.patches}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
