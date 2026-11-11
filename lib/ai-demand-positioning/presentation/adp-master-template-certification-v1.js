/**
 * Full E2E certification gate — master ADP report template enforcement.
 * Structural checks only (Bethesda reference). Does not invent measurement.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ADP_MASTER_REPORT_PRESENTATION_V1,
  ADP_CANONICAL_SECTION_IDS_V1,
  assertCanonicalSectionOrder,
  certifyMasterReportPresentation,
} from "./adp-master-report-presentation-v1.js";
import {
  attachCanonicalReportViewModelV1,
  auditCanonicalViewModelStructure,
} from "./adp-canonical-report-view-model-v1.js";
import { assertTrendsPresentationV1 } from "./adp-trends-presentation-v1.js";
import { assertBppPresentationV2 } from "../brand-portfolio/bpp-customer-presentation-v2.js";

export const ADP_MASTER_TEMPLATE_CERTIFICATION_V1 =
  "ADP_MASTER_TEMPLATE_CERTIFICATION_V1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const HTML_SHELLS = [
  "public/owner-ai-demand.html",
  "public/owner-ai-demand-share.html",
  "public/adp-current-report-pdf-render.html",
];

function extractSectionIdsFromHtml(html) {
  const ids = [];
  const re = /data-adp-section="([^"]+)"/g;
  let m;
  while ((m = re.exec(html))) ids.push(m[1]);
  return ids;
}

/**
 * Certify HTML shells share identical canonical section order.
 */
export function certifyHtmlShellSectionParity(cwd = ROOT) {
  const results = [];
  const failures = [];
  for (const rel of HTML_SHELLS) {
    const abs = path.join(cwd, rel);
    if (!fs.existsSync(abs)) {
      failures.push(`MISSING_SHELL:${rel}`);
      results.push({ rel, pass: false, error: "missing" });
      continue;
    }
    const html = fs.readFileSync(abs, "utf8");
    const ids = extractSectionIdsFromHtml(html);
    const ord = assertCanonicalSectionOrder(ids);
    if (!ord.pass) failures.push(...ord.failures.map((f) => `${rel}:${f}`));
    if (!html.includes("adp-v105-master-template") && !html.includes("ai-demand-positioning.js")) {
      failures.push(`${rel}:ASSET_TOKEN_OR_SCRIPT_MISSING`);
    }
    results.push({ rel, pass: ord.pass, sectionIds: ids });
  }
  // Cross-shell equality
  if (results.length >= 2 && results.every((r) => r.sectionIds)) {
    const base = results[0].sectionIds.join(",");
    for (let i = 1; i < results.length; i++) {
      if (results[i].sectionIds.join(",") !== base) {
        failures.push(`SHELL_SECTION_MISMATCH:${results[i].rel}`);
      }
    }
  }
  return {
    gate: "html_shell_section_parity",
    pass: failures.length === 0,
    failures,
    results,
  };
}

/**
 * Certify one owner payload against master template contracts.
 */
export function certifyPayloadMasterTemplate(payload, { propertyId = null } = {}) {
  const enriched = attachCanonicalReportViewModelV1(payload, { propertyId });
  const failures = [];
  const master = certifyMasterReportPresentation(enriched);
  if (!master.pass) failures.push(...master.failures);

  const trendsAssert = assertTrendsPresentationV1(enriched.trendsPresentation);
  if (!trendsAssert.pass) failures.push(...trendsAssert.failures.map((f) => `TRENDS:${f}`));

  if (enriched.brandPortfolioPosition?.status === "READY") {
    const bppAssert = assertBppPresentationV2(enriched.brandPortfolioPosition);
    if (!bppAssert.pass) failures.push(...bppAssert.failures.map((f) => `BPP:${f}`));
  }

  const struct = auditCanonicalViewModelStructure(enriched);
  if (!struct.pass) failures.push(...struct.failures.map((f) => `VM:${f}`));

  return {
    gate: "payload_master_template",
    propertyId: propertyId || enriched.canonicalViewModel?.propertyId,
    pass: failures.length === 0,
    failures,
    trendsState: enriched.trendsPresentation?.state || null,
    bppStatus: enriched.brandPortfolioPosition?.status || null,
    contract: ADP_MASTER_REPORT_PRESENTATION_V1,
  };
}

/**
 * Portfolio gate entry used by Full E2E / audit scripts.
 */
export function runMasterTemplateCertificationGate({
  cwd = ROOT,
  payloadsByPropertyId = {},
} = {}) {
  const shell = certifyHtmlShellSectionParity(cwd);
  const hotelResults = [];
  for (const [propertyId, payload] of Object.entries(payloadsByPropertyId)) {
    hotelResults.push(certifyPayloadMasterTemplate(payload, { propertyId }));
  }
  const hotelFail = hotelResults.filter((h) => !h.pass);
  return {
    version: ADP_MASTER_TEMPLATE_CERTIFICATION_V1,
    sectionOrder: ADP_CANONICAL_SECTION_IDS_V1,
    shell,
    hotels: hotelResults,
    pass: shell.pass && hotelFail.length === 0,
    failures: [
      ...(shell.failures || []),
      ...hotelFail.flatMap((h) =>
        (h.failures || []).map((f) => `${h.propertyId}:${f}`)
      ),
    ],
  };
}
