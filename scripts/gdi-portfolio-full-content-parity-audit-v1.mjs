#!/usr/bin/env node
/**
 * Read-only universal GDI full-content parity portfolio audit.
 *
 *   node scripts/gdi-portfolio-full-content-parity-audit-v1.mjs
 *   node scripts/gdi-portfolio-full-content-parity-audit-v1.mjs --root <published-inventory>
 *   node scripts/gdi-portfolio-full-content-parity-audit-v1.mjs --live-share --prod-base <url>
 *
 * Never writes production inventory. Never rotates share tokens.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runGdiPortfolioFullContentParityAudit } from "../lib/group-demand-intelligence/presentation/gdi-portfolio-full-content-parity-v1.js";
import { recordHotelParityStatus } from "../lib/group-demand-intelligence/presentation/gdi-full-content-parity-status-store-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-universal-portfolio-full-content-parity-v1",
);

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

async function main() {
  const root =
    argValue("--root") ||
    process.env.GDI_PUBLISHED_INVENTORY_ROOT ||
    path.join(
      ROOT,
      "reports/gdi-universal-full-presentation-parity-v1/work-gdi-published-inventory",
    );
  const liveShare = hasFlag("--live-share");
  const prodBase =
    argValue("--prod-base") ||
    process.env.GDI_PROD_BASE ||
    "https://my-operators-backend-production.up.railway.app";
  const persistStatus = !hasFlag("--no-persist-status");

  // Optional map: hotelId -> full gdishare.v1 capability (env JSON only; never log).
  let shareTokensByHotelId = {};
  const tokenMapPath = argValue("--share-token-map");
  if (tokenMapPath && fs.existsSync(tokenMapPath)) {
    shareTokensByHotelId = JSON.parse(fs.readFileSync(tokenMapPath, "utf8"));
  } else if (process.env.GDI_SHARE_TOKEN_MAP_JSON) {
    shareTokensByHotelId = JSON.parse(process.env.GDI_SHARE_TOKEN_MAP_JSON);
  }

  const report = await runGdiPortfolioFullContentParityAudit({
    root,
    liveShare,
    prodBase,
    shareTokensByHotelId,
    requireLiveShare: hasFlag("--require-live-share"),
  });

  if (persistStatus) {
    for (const h of report.hotels || []) {
      if (h.verify && !h.verify.skipped) {
        recordHotelParityStatus(h.hotelId, h.verify, { root });
      }
    }
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outFile = path.join(OUT_DIR, "01-portfolio-audit.json");
  fs.writeFileSync(outFile, JSON.stringify(report, null, 2) + "\n", "utf8");

  const md = [
    `# GDI Universal Portfolio Full-Content Parity Audit`,
    ``,
    `- Status: **${report.status}**`,
    `- Generated: ${report.generatedAt}`,
    `- Inventory root: \`${root}\``,
    ``,
    `## Summary`,
    ``,
    ...Object.entries(report.summary || {}).map(
      ([k, v]) => `- ${k}: ${v}`,
    ),
    ``,
    `## Hotels`,
    ``,
    `| Hotel | Published | Parity | Schema-ready | Legacy | Share | Migration |`,
    `| --- | ---: | --- | ---: | ---: | --- | --- |`,
    ...(report.hotels || []).map((h) => {
      const name = (h.hotelName || h.hotelId || "").replace(/\|/g, "/");
      return `| ${name} | ${h.publishedOpportunityCount} | ${h.fullContentParityStatus} | ${h.schemaReadySnapshotCount ?? "—"} | ${h.legacySnapshotCount ?? "—"} | ${h.shareStatus} | ${h.migrationCandidate || "—"} |`;
    }),
    ``,
    `## Critical differences`,
    ``,
  ];
  if (!(report.mismatches || []).length) {
    md.push(`_None._`, ``);
  } else {
    for (const d of report.mismatches.slice(0, 100)) {
      md.push(
        `- **${d.hotelId}** / ${d.opportunityId} · \`${d.fieldPath}\` · ${d.rootCause}`,
      );
      md.push(
        `  - approved: \`${JSON.stringify(d.approvedValue)?.slice(0, 120)}\``,
      );
      md.push(
        `  - published: \`${JSON.stringify(d.publishedValue)?.slice(0, 120)}\``,
      );
      md.push(`  - correction: ${d.requiredCorrection}`);
    }
    md.push(``);
  }
  md.push(
    `## Safety notes`,
    ``,
    `- Read-only audit. No production writes. No share token rotation.`,
    `- Unpublished and NOT_VERIFIABLE hotels are reported separately and never counted as PASS.`,
    `- Legacy share hotels without canonical registry snapshots are migration candidates.`,
    ``,
  );
  const mdFile = path.join(OUT_DIR, "01-portfolio-audit.md");
  fs.writeFileSync(mdFile, md.join("\n"), "utf8");

  console.log(
    JSON.stringify(
      {
        status: report.status,
        summary: report.summary,
        outFile,
        mdFile,
      },
      null,
      2,
    ),
  );

  // Exit non-zero only when explicitly required (CI gate mode).
  if (hasFlag("--fail-on-incomplete") && !String(report.status).endsWith("_PASS")) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
