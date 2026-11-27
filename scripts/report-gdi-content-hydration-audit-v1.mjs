#!/usr/bin/env node
/**
 * Product-wide GDI content hydration audit (read-only).
 * Does not remediate non-Wyndham inventories.
 *
 * Usage:
 *   node scripts/report-gdi-content-hydration-audit-v1.mjs
 *   node scripts/report-gdi-content-hydration-audit-v1.mjs --hotelId recnPB3Q02NO8Ma1o
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { auditHotelContentHydration } from "../lib/group-demand-intelligence/presentation/gdi-content-hydration-presentation-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-staged-content-quality-hydration-v1");
fs.mkdirSync(OUT, { recursive: true });

const HOTELS = [
  { id: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside" },
  { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
  { id: "recYotelGenevaLakePlaceholder", name: "YOTEL Geneva Lake", resolve: true },
  { id: "recRenaissanceNYtsPlaceholder", name: "Renaissance New York Times Square", resolve: true },
  { id: "recSheratonGdlPlaceholder", name: "Sheraton Guadalajara Expo", resolve: true },
];

function text(v) {
  return String(v == null ? "" : v).trim();
}

const args = process.argv.slice(2);
const hotelIdx = args.indexOf("--hotelId");
const onlyHotel = hotelIdx >= 0 ? text(args[hotelIdx + 1]) : "";

const store = createGdiPublicationPackageStore({});
const pointerDir = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
);

function discoverHotelIds() {
  if (!fs.existsSync(pointerDir)) return [];
  return fs
    .readdirSync(pointerDir, { withFileTypes: true })
    .filter((d) => d.isDirectory() && /^rec/.test(d.name))
    .map((d) => d.name);
}

const knownNames = new Map(HOTELS.map((h) => [h.id, h.name]));
const allIds = discoverHotelIds();
const targetIds = onlyHotel
  ? [onlyHotel]
  : [...new Set([...HOTELS.map((h) => h.id).filter((id) => allIds.includes(id)), ...allIds])];

const matrix = [];
for (const hotelId of targetIds) {
  if (!allIds.includes(hotelId) && !onlyHotel) continue;
  const staged = store.readPointer(hotelId, "STAGED");
  const current = store.readPointer(hotelId, "CURRENT");
  let opportunities = [];
  let version = null;
  let kind = null;
  if (staged?.publicationVersion) {
    const loaded = store.loadPackage(hotelId, staged.publicationVersion);
    if (loaded?.ok) {
      opportunities = loaded.package.opportunities || [];
      version = staged.publicationVersion;
      kind = "STAGED";
    }
  } else if (current?.publicationVersion) {
    const loaded = store.loadPackage(hotelId, current.publicationVersion);
    if (loaded?.ok) {
      opportunities = loaded.package.opportunities || [];
      version = current.publicationVersion;
      kind = "CURRENT";
    }
  }
  const hotelName =
    opportunities[0]?.hotelName ||
    knownNames.get(hotelId) ||
    hotelId;
  const audit = auditHotelContentHydration(opportunities, { hotelId, hotelName });
  const researchParagraphTitles = audit.rows.filter((r) =>
    r.findings.some((f) => f.code === "RESEARCH_PARAGRAPH_TITLE"),
  ).length;
  const incorrectDemand = audit.rows.filter((r) =>
    r.findings.some((f) => f.code === "INCORRECT_ASSOCIATION_DEFAULT"),
  ).length;
  const missingMandatory = audit.rows.filter((r) =>
    r.findings.some(
      (f) =>
        f.level === "blocking" &&
        f.fieldClass === "MANDATORY" &&
        f.code !== "RESEARCH_PARAGRAPH_TITLE" &&
        f.code !== "INCORRECT_ASSOCIATION_DEFAULT",
    ),
  ).length;
  const contactGaps = audit.rows.filter((r) =>
    r.findings.some((f) =>
      /CONTACT|ROLE|INCOMPLETE_CONTACT|TITLE_UNVERIFIED/.test(f.code),
    ),
  ).length;

  matrix.push({
    hotelId,
    hotelName,
    publicationKind: kind,
    publicationVersion: version,
    inventoryAudited: audit.audited,
    incorrectTitles: researchParagraphTitles,
    incorrectDemandClassifications: incorrectDemand,
    missingMandatoryFields: missingMandatory,
    contactVerificationGaps: contactGaps,
    blockedCount: audit.blockedCount,
    warningOnlyCount: audit.warningOnlyCount,
    certification: audit.certification,
    remediation:
      hotelId === "recnPB3Q02NO8Ma1o"
        ? "REMATERIALIZED_THIS_CYCLE"
        : audit.pass
          ? "NO_ACTION"
          : "AUDIT_ONLY_NO_UNAUTHORIZED_REMEDIATION",
    requiresAdditionalResearch: audit.rows.some((r) =>
      r.findings.some((f) =>
        /HOTEL_DEMAND_UNVERIFIED|GENERIC_POTENTIAL|INCOMPLETE_CONTACT_NAME/.test(
          f.code,
        ),
      ),
    ),
  });
}

const report = {
  contract: "gdi-content-hydration-presentation-gate-v1",
  generatedAt: new Date().toISOString(),
  hotelCount: matrix.length,
  matrix,
};

fs.writeFileSync(
  path.join(OUT, "05-PRODUCT_WIDE_QUALITY_MATRIX.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);

const md = `# Product-wide GDI Content Quality Matrix

| Hotel | Kind | N | Bad titles | Bad demand | Missing mandatory | Contact gaps | Certification | Remediation |
|---|---|---:|---:|---:|---:|---:|---|---|
${matrix
  .map(
    (r) =>
      `| ${r.hotelName} | ${r.publicationKind || "—"} | ${r.inventoryAudited} | ${r.incorrectTitles} | ${r.incorrectDemandClassifications} | ${r.missingMandatoryFields} | ${r.contactVerificationGaps} | ${r.certification} | ${r.remediation} |`,
  )
  .join("\n")}
`;
fs.writeFileSync(path.join(OUT, "05-PRODUCT_WIDE_QUALITY_MATRIX.md"), md);

console.log(JSON.stringify({ ok: true, hotelCount: matrix.length, outDir: OUT }, null, 2));
