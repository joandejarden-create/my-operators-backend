import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveCanonicalHotelId, loadAdpGdiAliasMap } from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { getCensusLinkEntry } from "../lib/ai-demand-positioning/census-link-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const hotels = [
  { adp: "adp_ac_hotel_a_coruna", expectedHpc: "rec2PVBDavppGpenm" },
  { adp: "adp_spice_island_beach_resort", expectedHpc: "recKRJjcPnb4tVDDS" },
];

const out = [];
for (const h of hotels) {
  const pubDir = path.join(ROOT, "data/ai-demand-positioning/published", h.adp);
  const manifest = JSON.parse(fs.readFileSync(path.join(pubDir, "manifest.json"), "utf8"));
  const reportPath = path.join(pubDir, manifest.reportFile);
  const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  const link = getCensusLinkEntry(h.adp);
  const map = loadAdpGdiAliasMap();
  const alias = map.aliases?.[h.adp] || null;
  const canonical = resolveCanonicalHotelId(h.adp);
  const metrics = report.metrics || report.summary || {};
  const findings = report.executiveSummary?.findings || report.findings || [];
  out.push({
    adp: h.adp,
    expectedHpc: h.expectedHpc,
    canonicalFromAdp: canonical,
    censusLink: link,
    alias,
    bindingStatus:
      canonical === h.expectedHpc || link?.censusRecordId === h.expectedHpc || alias?.canonicalHotelId === h.expectedHpc
        ? "PASS"
        : "FAIL",
    period: manifest.latestPeriodId,
    publishStatus: manifest.publishStatus,
    demandCaptureRate: manifest.demandCaptureRate ?? metrics.demandCaptureRate,
    consideration: metrics.considerationRate ?? report.considerationRate ?? report.consideration ?? null,
    presence: metrics.presenceRate ?? report.presenceRate ?? report.presence ?? null,
    propertyReality: metrics.propertyRealityScore ?? report.propertyReality ?? null,
    certifiedHint: report.certification || report.baseline || metrics.certification || null,
    topFindingTitles: (Array.isArray(findings) ? findings : [])
      .slice(0, 5)
      .map((f) => f.title || f.findingType || f.headline || String(f).slice(0, 80)),
    reportKeys: Object.keys(report).slice(0, 40),
  });
}

const outPath = path.join(
  ROOT,
  "reports/hotel-census/ac-spice-persistence-reconciliation-v1/ADP_BINDING.json"
);
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify({ generatedAt: new Date().toISOString(), out }, null, 2) + "\n");
console.log(JSON.stringify({ outPath, out }, null, 2));
