import fs from "fs";
import path from "path";
import { applyOperatorAlignmentPrefillAliases } from "../lib/operator-alignment-prefill-map.js";
import { mapNewBaseLeadershipForDetail } from "../api/lib/operator-setup-new-base-read.js";

function check(name, pass, details = {}) {
  return { name, pass: !!pass, details };
}

function containsOrdered(source, tokens) {
  let idx = -1;
  for (const t of tokens) {
    const next = source.indexOf(t, idx + 1);
    if (next === -1) return false;
    idx = next;
  }
  return true;
}

const checks = [];

const explorerPath = path.resolve("public/js/operator-explorer-new-base-profile.js");
const explorerSrc = fs.readFileSync(explorerPath, "utf8");

checks.push(
  check(
    "explorer_companyName_canonical_first_order",
    containsOrdered(explorerSrc, ['["companyName", "company_name", "Company Name"]']),
    { expected: "companyName first, then aliases" }
  )
);
checks.push(
  check(
    "explorer_serviceModelsSupported_canonical_first_order",
    containsOrdered(explorerSrc, ['"serviceModelsSupported"', '"Service Models Supported"', '"primaryServiceModel"']),
    { expected: "serviceModelsSupported first in fallback stack" }
  )
);
checks.push(
  check(
    "explorer_chainScalesSupported_canonical_first_order",
    containsOrdered(explorerSrc, ['"chainScalesSupported"', '"Chain Scales Supported"', '"chainScale"', '"chainScales"']),
    { expected: "chainScalesSupported first in fallback stack" }
  )
);
checks.push(
  check(
    "explorer_geography_key_family_present",
    containsOrdered(explorerSrc, ['"activeCountries", "Active Countries"']) &&
      containsOrdered(explorerSrc, ['"activeMarkets", "Active Markets / Cities", "active_markets"']) &&
      containsOrdered(explorerSrc, ['"regions", "regionsSupported", "Regions Supported"']) &&
      containsOrdered(explorerSrc, ['"specificMarkets", "Specific Markets"']),
    { expected: "canonical geography keys and aliases are explicitly stacked" }
  )
);
checks.push(
  check(
    "explorer_resolution_diagnostics_present",
    explorerSrc.includes("explorer_read_key_resolution") &&
      explorerSrc.includes("fallbackUsed") &&
      explorerSrc.includes("canonicalKey"),
    { expected: "non-user-facing fallback diagnostics added in pickField/pickList" }
  )
);

const alignmentPrefillCanonical = { serviceModelsSupported: "Full hotel management" };
applyOperatorAlignmentPrefillAliases(
  { "Service Models Supported": "Legacy alias value" },
  alignmentPrefillCanonical
);
checks.push(
  check(
    "alignment_prefill_preserves_existing_canonical_value",
    alignmentPrefillCanonical.serviceModelsSupported === "Full hotel management",
    { value: alignmentPrefillCanonical.serviceModelsSupported }
  )
);

const alignmentPrefillAlias = {};
applyOperatorAlignmentPrefillAliases(
  { "Service Models Supported": "Full hotel management" },
  alignmentPrefillAlias
);
checks.push(
  check(
    "alignment_prefill_resolves_from_alias_when_canonical_missing",
    alignmentPrefillAlias.serviceModelsSupported === "Full hotel management",
    { value: alignmentPrefillAlias.serviceModelsSupported }
  )
);

const leadershipMapped = mapNewBaseLeadershipForDetail([
  {
    id: "recLeader1",
    fields: {
      role: "Executive Leadership",
      summary: "Summary text",
      bio: "Bio text",
      headshot: "https://example.com/headshot.png",
      name: "Leader One",
      title: "CEO",
      display_order: 1,
    },
  },
]);
const lead = leadershipMapped[0] || {};
checks.push(
  check(
    "leadership_child_mapping_contract_shape",
    lead.function === "Executive Leadership" &&
      lead.summary === "Summary text" &&
      lead.shortBio === "Summary text" &&
      lead.experienceSummary === "Summary text" &&
      lead.bio === "Bio text" &&
      typeof lead.headshotUrl === "string" &&
      lead.headshotUrl.includes("example.com/headshot.png"),
    {
      function: lead.function,
      summary: lead.summary,
      shortBio: lead.shortBio,
      experienceSummary: lead.experienceSummary,
      bio: lead.bio,
      headshotUrl: lead.headshotUrl,
    }
  )
);

const mirrorPath = path.resolve("public/js/operator-setup-explorer-behavior.js");
const mirrorSrc = fs.readFileSync(mirrorPath, "utf8");
checks.push(
  check(
    "mirror_masking_diagnostics_present",
    mirrorSrc.includes("mirror_prefill_applied") &&
      mirrorSrc.includes("mirror_write_contract") &&
      mirrorSrc.includes("mirrorRetainedForCompatibility"),
    { expected: "explorerProfileJson masking diagnostics are present and compatibility retained" }
  )
);

const passed = checks.every((c) => c.pass);
const output = {
  ok: passed,
  checks,
};
const outputPath = path.resolve("reports", "operator-setup-batch-3b-contract-validation-output.json");
fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
console.log(JSON.stringify({ ok: passed, checks: checks.length, outputPath }, null, 2));
