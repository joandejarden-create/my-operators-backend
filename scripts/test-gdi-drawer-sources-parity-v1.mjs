import fs from "node:fs";
import { execSync } from "node:child_process";

function associationSectionIds(src) {
  const a = src.indexOf("function associationConferenceEventsDetailHtml");
  const b = src.indexOf("function intelligenceDetailHtml", a);
  const block = src.slice(a, b);
  return [...block.matchAll(/data-section-id="([^"]+)"/g)].map((m) => m[1]);
}

function associationHelpers(src) {
  const a = src.indexOf("function associationConferenceEventsDetailHtml");
  const before = src.slice(Math.max(0, a - 2500), a);
  return {
    hasSectionSourcesButton: /function sectionSourcesButtonHtml/.test(before) || /function sectionSourcesButtonHtml/.test(src),
    hasCitationLists: /gdi-section-citations/.test(src.slice(a, a + 8000)),
    hasAllSourcesBlock: /All Sources/.test(src.slice(a, a + 9000)),
    sourcesOpenByDefault: /gdi-section-sources"[^>]*>[\s\S]{0,40}<summary>Sources/.test(src)
      ? /gdi-section-sources" open/.test(src.slice(a, a + 9000))
      : false,
    hasSupportingResearchNestedSources:
      /supportingResearchInEvidenceHtml[\s\S]{0,800}sectionSourcesButtonHtml/.test(src),
  };
}

const main = execSync(
  "git show origin/main:public/js/group-demand-intelligence/dealality-gdi-ui.js",
  { encoding: "utf8", maxBuffer: 30e6 },
);
const cur = fs.readFileSync(
  "public/js/group-demand-intelligence/dealality-gdi-ui.js",
  "utf8",
);
const sMain = associationSectionIds(main);
const sCur = associationSectionIds(cur);
const helpers = associationHelpers(cur);

const report = {
  sectionIdsMain: sMain,
  sectionIdsCurrent: sCur,
  sectionHierarchyEqual: JSON.stringify(sMain) === JSON.stringify(sCur),
  extras: sCur.filter((x) => !sMain.includes(x)),
  missing: sMain.filter((x) => !sCur.includes(x)),
  helpers,
};
fs.mkdirSync("reports/gdi-commercial-fact-selection-v1", { recursive: true });
fs.writeFileSync(
  "reports/gdi-commercial-fact-selection-v1/DRAWER_PARITY.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(report, null, 2));
if (!report.sectionHierarchyEqual) process.exit(2);
if (helpers.hasCitationLists || helpers.hasAllSourcesBlock) process.exit(3);
if (helpers.sourcesOpenByDefault) process.exit(4);
if (helpers.hasSupportingResearchNestedSources) process.exit(5);
