import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const htmlBuilder = fs.readFileSync(
  path.join(ROOT, "lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v1.js"),
  "utf8"
);
if (/not fully field-assessed/i.test(htmlBuilder)) {
  console.error("FAIL builder still has unfinished wording");
  process.exit(1);
}
if (!/partial assessment due to access restrictions/i.test(htmlBuilder)) {
  console.error("FAIL missing partial assessment heading");
  process.exit(1);
}
const payload = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
    ),
    "utf8"
  )
);
const registry = payload.digitalContentReadiness?.platformCoverageRegistry || {};
const identified = Object.values(registry).filter((v) => /IDENTIFIED_NOT/i.test(String(v)));
if (identified.length) {
  console.error("FAIL identified_not", identified);
  process.exit(1);
}
const title = payload.digitalPlatformReadinessV3?.additionalGroupTitle || "";
if (!/partial assessment due to access restrictions/i.test(title)) {
  console.error("FAIL payload missing additionalGroupTitle", title);
  process.exit(1);
}
console.log(
  JSON.stringify(
    { ok: true, gate: "test:ava-partial-platform-terminal-language-v1" },
    null,
    2
  )
);
