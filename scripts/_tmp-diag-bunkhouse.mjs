import "dotenv/config";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { resolveBrandExplorerDisplayState } from "../lib/partner-intelligence/brand-explorer-display-state.js";

const recordId = "recGv268Wda31PlSZ";
const brandName = "Bunkhouse Hotels";
const slug = "bunkhouse-hotels";
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;

const basicsRes = await fetch(
  `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}/${recordId}`,
  { headers: { Authorization: `Bearer ${apiKey}` } }
);
const basicsJson = await basicsRes.json();
const basics = basicsJson.fields || {};

const { rows } = await listPresentationRowsLight(recordId, brandName);
const uniq = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
const role = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });

console.log(
  JSON.stringify(
    {
      brandName,
      BrandStatus: basics["Brand Status"],
      ActiveProfileApproved: basics["Active Profile Approved"],
      FounderVisualReviewPass: basics["Founder Visual Review Pass"],
      CompanyValidated: basics["Company Validated"],
      rows: rows.length,
      gallery: rows
        .filter((r) => String(r.slotKey).startsWith("materials.gallery"))
        .map((r) => ({ slot: r.slotKey, has: !!r.imageUrl, file: r.imageFilename || r.filename })),
      scenario: rows
        .filter((r) => String(r.slotKey).startsWith("overview.scenario"))
        .map((r) => ({ slot: r.slotKey, has: !!r.imageUrl, file: r.imageFilename || r.filename })),
      openings: rows
        .filter((r) => r.slotKey === "footprint.openings")
        .map((r) => ({
          title: r.title,
          has: !!r.imageUrl,
          file: r.imageFilename || r.filename,
          eds: r.externalDisplayStatus,
          active: r.active,
        })),
      uniqPass: uniq.pass,
      galleryDistinct: uniq.galleryDistinctCount,
      gallerySlots: uniq.gallerySlotCount,
      scenarioDistinct: uniq.scenarioDistinctCount,
      propertyDistinct: uniq.propertyExampleDistinctCount,
      findings: uniq.findings,
      rolePass: role.pass,
      unresolved: role.unresolvedRoleMismatchCount,
      hardFails: (role.evaluations || [])
        .filter((e) => e.hardFail || e.matchStatus === "needs_replacement")
        .map((e) => ({ slot: e.slotKey, issue: e.issue, status: e.matchStatus, cap: e.currentCaption })),
    },
    null,
    2
  )
);

const display = resolveBrandExplorerDisplayState(
  { slug, name: brandName, recordId },
  {
    presentationRows: rows,
    brandBasics: basics,
    imageUniqueness: uniq,
    brandSlug: slug,
  }
);
console.log(
  "display",
  JSON.stringify(
    {
      state: display.brandExplorerDisplayState,
      full: display.shouldRenderFullProfile,
      completeness: display.completeness,
      gates: display.gates,
    },
    null,
    2
  )
);
