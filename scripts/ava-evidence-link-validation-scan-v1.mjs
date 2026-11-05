/**
 * Scan one or all AVA payload JSON files for evidence-link resolution.
 *
 *   node scripts/ava-evidence-link-validation-scan-v1.mjs
 *   node scripts/ava-evidence-link-validation-scan-v1.mjs --property adp_bethesda_marriott
 *
 * Does not claim claim-support for foreign hotels automatically; resolution
 * failures are always flagged. For Bethesda, claimSupport map is applied.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  validateAuditEvidenceLinks,
  summarizeEvidenceValidation,
} from "../lib/ai-visibility-audit-action-plan/evidence-link-validation-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_DIR = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads"
);
const OUT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_EVIDENCE_LINK_VALIDATION_PORTFOLIO_SCAN.json"
);

const BETHESDA_SUPPORT = {
  "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/overview/": true,
  "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/": true,
  "https://ors.od.nih.gov/pes/dats/PatientServices/PatientShuttles/Pages/hotel_shuttles.aspx": true,
  "https://assets.hyatt.com/content/dam/hyatt/hyattdam/documents/2021/04/15/1020/Hyatt-Regency-Bethesda-Capacity-Chart-English.pdf": true,
  "https://assets.hyatt.com/content/dam/hyatt/hyattdam/documents/2018/06/06/1840/Hyatt-Regency-Bethesda-Fact-Sheet-English-060618.pdf": true,
  // Known retired / unsupported
  "https://www.cc.nih.gov/about/visitor/hotels":
    "Broken/unusable; does not support Bethesda Marriott NIH-route claims",
  "https://www.hyatt.com/hyatt-regency/en-US/bethe-hyatt-regency-bethesda":
    "Consumer page blocked automated retrieval; replaced with Hyatt PDF assets for capacity claims",
};

function parseArgs(argv) {
  const out = { property: null };
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === "--property") out.property = argv[++i];
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv);
  const files = fs
    .readdirSync(PAYLOAD_DIR)
    .filter((f) => f.endsWith(".json"))
    .sort();

  const hotels = [];
  const probeCache = new Map();
  for (const file of files) {
    const abs = path.join(PAYLOAD_DIR, file);
    const payload = JSON.parse(fs.readFileSync(abs, "utf8"));
    const propertyId = payload?.property?.propertyId || file;
    if (args.property && args.property !== propertyId) continue;

    const claimSupportByUrl =
      propertyId === "adp_bethesda_marriott" ? { ...BETHESDA_SUPPORT } : {};
    // Auto-flag known broken NIH visitor hotels URL wherever it appears.
    if (!claimSupportByUrl["https://www.cc.nih.gov/about/visitor/hotels"]) {
      claimSupportByUrl["https://www.cc.nih.gov/about/visitor/hotels"] =
        "Broken/unusable Clinical Center visitor hotels URL";
    }

    console.log("Validating", propertyId, "...");
    const report = await validateAuditEvidenceLinks(payload, {
      claimSupportByUrl,
      probeCache,
    });
    hotels.push({
      propertyId,
      name: payload?.property?.name || null,
      payloadFile: path.relative(ROOT, abs).replace(/\\/g, "/"),
      summary: summarizeEvidenceValidation(report),
      failing: report.failing.map((f) => ({
        url: f.url,
        label: f.label,
        context: f.context,
        status: f.status,
        flags: f.flags,
        error: f.error,
        finalUrl: f.finalUrl,
      })),
    });
  }

  const withFailures = hotels.filter((h) => h.summary.failCount > 0);
  const summary = {
    contract: "AVA_EVIDENCE_LINK_VALIDATION_V1",
    scannedAt: new Date().toISOString(),
    hotelsScanned: hotels.length,
    hotelsWithResolutionOrSupportFlags: withFailures.length,
    totalFailingCitations: withFailures.reduce(
      (n, h) => n + h.summary.failCount,
      0
    ),
    lessonApplied:
      "Every customer-facing citation must resolve and directly support its claim.",
    hotels,
  };

  fs.writeFileSync(OUT, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        hotelsScanned: summary.hotelsScanned,
        hotelsWithFlags: summary.hotelsWithResolutionOrSupportFlags,
        totalFailingCitations: summary.totalFailingCitations,
        out: path.relative(ROOT, OUT).replace(/\\/g, "/"),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
