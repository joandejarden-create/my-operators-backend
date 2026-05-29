#!/usr/bin/env node
/**
 * Phase 5E — scoring input wiring validation (no weight / BAS / OCS changes).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  normalizeOperatorAlignmentDealInputs,
} from "../lib/operator-alignment-deal-normalize.js";
import {
  scoreDealStructureFactor,
  scoreServiceOfferingsFactor,
} from "../lib/operator-alignment-scoring-factors.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let failed = 0;
function ok(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

const myDealsSrc = fs.readFileSync(path.join(ROOT, "api/my-deals.js"), "utf8");
const ocs = fs.readFileSync(path.join(ROOT, "lib/operator-capability-snapshot-build.js"), "utf8");
const bas = fs.readFileSync(path.join(ROOT, "api/match-score-server.js"), "utf8");

ok(myDealsSrc.includes("normalizeOperatorAlignmentDealInputs"), "my-deals imports deal normalizer");
ok(myDealsSrc.includes("scoreDealStructureFactor"), "my-deals uses structure factor helper");
ok(!ocs.includes("scoreDealStructureFactor"), "OCS unchanged");
ok(!bas.includes("normalizeOperatorAlignmentDealInputs"), "BAS unchanged");

const wBefore = myDealsSrc.match(/const OPERATOR_MATCH_WEIGHTS = \{([^}]+)\}/)?.[0] || "";
ok(wBefore.includes("geographyMarkets: 18"), "geography weight still 18");
ok(wBefore.includes("dealStructureAssignment: 12"), "structure weight still 12");

const deal = normalizeOperatorAlignmentDealInputs(
  { "Project Type": "New Build", "F&B Complexity": "Limited F&B", "Opening Timeline": "Pre-development" },
  { Country: "Mexico", City: "Cancún", "Hotel Chain Scale": "Upper Midscale" },
  { "Preferred Deal Structure": "Franchise Only" },
  {
    "Brand Agreement Structure": "Franchise",
    "Operating Model": "Third-party managed",
    "Preferred Management Structure": [
      "Franchise with third-party operator",
      "Full third-party management",
    ],
    "Must-Have Operator Services": [
      "Full hotel management",
      "Pre-opening planning",
      "Revenue management",
    ],
    "Must-Haves From Brand/Operator": ["Strong Distribution and Marketing Support"],
  }
);

ok(deal.brandAgreementStructure === "Franchise", "reads Brand Agreement Structure");
ok(deal.operatingModel === "Third-party managed", "reads Operating Model");
ok(deal.hasStructuredStructure, "structured structure flag");
ok(deal.hasStructuredServices, "structured services flag");
ok(deal.fieldSources.brandAgreementStructure === "structured_si", "SI source tracked");

const struct = scoreDealStructureFactor(deal, [
  "Full third-party management",
  "Franchise support",
]);
ok(struct.score >= 72, "franchise brand + third-party mgmt not scored as 20 conflict");
ok(struct.fieldSource === "structured", "structure uses structured source");

const structLegacy = scoreDealStructureFactor(
  normalizeOperatorAlignmentDealInputs({}, {}, { "Preferred Deal Structure": "Franchise Only" }, {}),
  ["Management Agreement"]
);
ok(structLegacy.fieldSource === "legacy_mp", "legacy fallback when no structured fields");

const svc = scoreServiceOfferingsFactor(
  deal,
  ["Full hotel management", "Pre-opening planning", "Revenue management", "Sales"],
  {}
);
ok(svc.score >= 75, "structured services overlap");
ok(svc.fieldSource === "structured", "services use structured must-haves first");

const svcGap = scoreServiceOfferingsFactor(deal, [], {});
ok(svcGap.score == null && svcGap.missingDataClass === "needs_validation", "missing op services → needs validation");

console.log(failed ? "\n" + failed + " failure(s)" : "\nAll Phase 5E checks passed.");
process.exit(failed ? 1 : 0);
