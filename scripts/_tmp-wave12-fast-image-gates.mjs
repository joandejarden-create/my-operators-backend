#!/usr/bin/env node
/** Fast Wave12 image uniqueness + role-match via Presentation rows only. */
import "dotenv/config";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";

for (const slug of WAVE12_SLUGS) {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  const { rows } = await listPresentationRowsLight(id.recordId, id.name);
  const uniq = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
  const role = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });
  console.log(
    slug,
    "uniq",
    uniq.pass,
    `g${uniq.galleryDistinctCount}/6 s${uniq.scenarioDistinctCount}/3 p${uniq.propertyExampleDistinctCount}/3`,
    "role",
    role.pass,
    "mismatches",
    role.unresolvedRoleMismatchCount
  );
}
