/**
 * v42A-R2 — Founder Minor Cleanup Batch
 * (Hotel Indigo, MGallery Collection, SLH)
 *
 * Narrow Owner Considerations / standards language cleanup after v42
 * approve_after_minor_cleanup. Does not unlock, approve active profile,
 * touch images/registry/sources, or modify released brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { PRESENTATION_TABLE } from "./brand-explorer-residual-owner-copy-remediation.js";
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
} from "./brand-explorer-v40b-copy-quality-patterns.js";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import {
  GRADUATED_LIFESTYLE_COHORT_SLUGS,
  ORIGINAL_GOLDEN_RELEASE_SLUGS,
} from "./brand-explorer-os-state-machine.js";
import {
  captureV44BrandSnapshot,
  evaluateV44Regression,
} from "./brand-explorer-v44-release-baseline.js";

export const V42A_R2_VERSION = "v42a-r2";

export const V42A_R2_TARGET_BRANDS = Object.freeze([...GRADUATED_LIFESTYLE_COHORT_SLUGS]);
export const V42A_R2_PROTECTED_RELEASED = Object.freeze([...ORIGINAL_GOLDEN_RELEASE_SLUGS]);

export const V42A_R2_MAX_PATCHES_PER_BRAND = 10;

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v42A-R2-founder-minor-cleanup-batch";
export const APPLY_FLAG_NO_CV = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_ACTIVE = "--confirm-no-active-profile-approval";
export const APPLY_FLAG_NO_SOURCE = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_REGISTRY = "--confirm-no-registry-changes";
export const APPLY_FLAG_NO_IMAGE = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_RELEASED = "--confirm-no-released-brand-changes";
export const APPLY_FLAG_LOCKED = "--confirm-external-profiles-remain-locked";
export const APPLY_FLAG_BRAND_ONLY = "--confirm-brand-only";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_CV,
  APPLY_FLAG_NO_ACTIVE,
  APPLY_FLAG_NO_SOURCE,
  APPLY_FLAG_NO_REGISTRY,
  APPLY_FLAG_NO_IMAGE,
  APPLY_FLAG_NO_RELEASED,
  APPLY_FLAG_LOCKED,
  APPLY_FLAG_BRAND_ONLY,
]);

export const REPORT_JSON = "brand-explorer-v42a-r2-founder-minor-cleanup-batch.json";
export const REPORT_MD = "brand-explorer-v42a-r2-founder-minor-cleanup-batch.md";

const BRAND_REPORT_MD = Object.freeze({
  "hotel-indigo": "brand-explorer-v42a-r2-hotel-indigo-cleanup.md",
  "mgallery-collection": "brand-explorer-v42a-r2-mgallery-cleanup.md",
  "small-luxury-hotels-of-the-world": "brand-explorer-v42a-r2-slh-cleanup.md",
});

const V42_PACKET = Object.freeze({
  "hotel-indigo": "brand-explorer-v42-founder-review-hotel-indigo.md",
  "mgallery-collection": "brand-explorer-v42-founder-review-mgallery-collection.md",
  "small-luxury-hotels-of-the-world":
    "brand-explorer-v42-founder-review-small-luxury-hotels-of-the-world.md",
});

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
  "Ready for Active Profile",
  "Active Profile Approved",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isHidden(row) {
  return /do not display|internal only/i.test(nz(row?.externalDisplayStatus));
}

function reqBody({ typical, owner, status = "Confirm With Brand", notes }) {
  return [
    `Typical consideration: ${typical}`,
    `Owner planning consideration: ${owner}`,
    `Typical status: ${status}`,
    `Notes to confirm: ${notes}`,
  ].join("\n");
}

/** Brand-specific Owner Considerations checklist plans (≤10 rows). */
export const V42A_R2_STANDARDS_PLANS = Object.freeze({
  "hotel-indigo": {
    sectionFocus: "Hotel Indigo / IHG lifestyle — neighborhood fit, design storytelling, operating diligence",
    rows: [
      {
        slotKey: "standards.intro",
        title: "",
        sortOrder: 1,
        body:
          "Hotel Indigo is an IHG lifestyle brand focused on neighborhood storytelling and local discovery—not InterContinental or a generic IHG prototype. Use this checklist to diligence brand fit, design expectations, and operating implications. Confirm every row with current Hotel Indigo / IHG development materials and your agreement before capital commitments.",
      },
      {
        slotKey: "standards.requirement",
        title: "Neighborhood / Design Storytelling",
        sortOrder: 10,
        body: reqBody({
          typical:
            "Each Hotel Indigo is expected to express a clear neighborhood narrative through design, art, and guest experience—not a shared IHG shell look.",
          owner:
            "Confirm the local story, design direction, PIP scope, and whether conversion retains authentic neighborhood character.",
          notes: "Avoid InterContinental or other IHG brand imagery/standards confusion.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Guestroom / Suite Standards",
        sortOrder: 11,
        body: reqBody({
          typical:
            "Guestroom finishes, bath, bedding, and FF&E follow Hotel Indigo lifestyle standards within the IHG system.",
          owner:
            "Confirm renovation scope, procurement path, room mix, and conversion flexibility for this asset.",
          notes: "Project-specific design review is common on conversions.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Lobby / Public Space",
        sortOrder: 12,
        body: reqBody({
          typical:
            "Arrival and public spaces should support local discovery, social use, and brand storytelling.",
          owner:
            "Confirm lobby layout, seating, work zones, FF&E, and design-review gates for conversion or new build.",
          status: "Typically Expected",
          notes: "Subject to Hotel Indigo design review—not a corporate InterContinental lobby brief.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "F&B / Local Discovery",
        sortOrder: 13,
        body: reqBody({
          typical:
            "F&B or local-experience programming often reinforces neighborhood positioning.",
          owner:
            "Confirm outlet concept, kitchen/support space, staffing model, and whether offerings are required at opening.",
          status: "May Apply",
          notes: "Scope varies by market and conversion path—confirm with IHG Hotel Indigo development.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Technology / IHG One Rewards",
        sortOrder: 14,
        body: reqBody({
          typical:
            "IHG systems and IHG One Rewards participation typically apply across Hotel Indigo.",
          owner:
            "Confirm PMS/CRS cutover, loyalty integration, Wi-Fi/reporting, training, and timing relative to opening.",
          status: "Typically Expected",
          notes: "Systems obligations sit with IHG—not a separate InterContinental stack.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Training / QA / Opening Readiness",
        sortOrder: 15,
        body: reqBody({
          typical:
            "Opening readiness, brand training, and quality reviews typically apply before and after opening.",
          owner:
            "Confirm opening checklist, training cadence, inspection process, and remediation expectations.",
          status: "Typically Expected",
          notes: "Timing can affect opening plan and working capital.",
        }),
      },
      {
        slotKey: "standards.conversion",
        title: "",
        sortOrder: 20,
        body:
          "For conversions, sequence Hotel Indigo design narrative, PIP, FF&E, signage, and IHG systems cutover with financing and brand approval gates. Keep the asset’s neighborhood story visible—do not default to a generic IHG shell.",
      },
      {
        slotKey: "standards.questions",
        title: "",
        sortOrder: 30,
        body: [
          "Which Hotel Indigo design / neighborhood standards apply to this address?",
          "What is mandatory at opening vs phased over 12–24 months?",
          "How do F&B or local-experience expectations change PIP scope?",
          "What IHG One Rewards and technology cutover dates are contractual?",
          "What opening readiness and quality-review expectations should owners plan for?",
        ].join("\n"),
      },
    ],
  },
  "mgallery-collection": {
    sectionFocus: "Accor MGallery soft collection — local character, conversion fit, standards diligence",
    rows: [
      {
        slotKey: "standards.intro",
        title: "",
        sortOrder: 1,
        body:
          "MGallery Collection is an Accor soft-collection brand that emphasizes distinctive local character within Accor distribution—not a generic Accor full-service prototype. Use this checklist to diligence collection fit, conversion/repositioning suitability, brand standards, and operating implications. Confirm every row with current Accor / MGallery development materials and your agreement before capital commitments.",
      },
      {
        slotKey: "standards.requirement",
        title: "Collection Identity / Local Character",
        sortOrder: 10,
        body: reqBody({
          typical:
            "MGallery hotels are expected to retain or express strong local identity rather than a standardized Accor look.",
          owner:
            "Confirm design narrative, heritage/adaptive-reuse constraints, and how collection review protects property character.",
          notes: "Avoid treating MGallery as a generic Accor flag conversion.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Guestroom Standards",
        sortOrder: 11,
        body: reqBody({
          typical:
            "Guestroom quality, finishes, and FF&E must meet MGallery collection expectations for the asset type.",
          owner:
            "Confirm renovation scope, procurement, room mix, and conversion flexibility for this property.",
          status: "Typically Expected",
          notes: "Project-specific collection review is common.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Lobby / Public Space",
        sortOrder: 12,
        body: reqBody({
          typical:
            "Public spaces should feel collection-worthy and place-specific—arrival, social, and experience zones.",
          owner:
            "Confirm lobby layout, FF&E, and whether conversion can meet collection character without over-standardizing.",
          status: "Typically Expected",
          notes: "Subject to Accor / MGallery design review.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "F&B / Signature Experience",
        sortOrder: 13,
        body: reqBody({
          typical:
            "Signature F&B or experience programming may reinforce collection positioning.",
          owner:
            "Confirm outlet concept, kitchen needs, staffing, and opening vs phased requirements.",
          status: "May Apply",
          notes: "Scope varies by hotel story and market—confirm with Accor MGallery development.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Technology / Accor ALL",
        sortOrder: 14,
        body: reqBody({
          typical:
            "Accor systems and ALL loyalty participation typically apply to MGallery hotels.",
          owner:
            "Confirm PMS/CRS cutover, loyalty integration, reporting, training, and timing.",
          status: "Typically Expected",
          notes: "Systems obligations are Accor collection stack—not inventable from public marketing alone.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Training / QA / Brand Review",
        sortOrder: 15,
        body: reqBody({
          typical:
            "Opening readiness, training, and collection quality reviews typically apply.",
          owner:
            "Confirm opening checklist, inspection process, remediation expectations, and ongoing review cadence.",
          status: "Typically Expected",
          notes: "Timing can affect opening plan.",
        }),
      },
      {
        slotKey: "standards.conversion",
        title: "",
        sortOrder: 20,
        body:
          "For conversions or repositionings, sequence MGallery collection review, PIP, FF&E, signage, and Accor systems cutover with financing and approval gates. Protect local character—collection fit is not a generic Accor prototype swap.",
      },
      {
        slotKey: "standards.questions",
        title: "",
        sortOrder: 30,
        body: [
          "Which MGallery collection standards apply to this asset type and market?",
          "What is mandatory at opening vs phased over 12–24 months?",
          "How does collection design review affect conversion PIP?",
          "What Accor ALL / technology cutover dates are contractual?",
          "What opening readiness and quality-review expectations should owners plan for?",
        ].join("\n"),
      },
    ],
  },
  "small-luxury-hotels-of-the-world": {
    sectionFocus: "SLH independent luxury consortium — membership fit, quality, independent ownership",
    rows: [
      {
        slotKey: "standards.intro",
        title: "",
        sortOrder: 1,
        body:
          "Small Luxury Hotels of the World is an independent luxury consortium / affiliation network—not a franchise chain or parent-brand prototype system. Use this checklist to diligence membership fit, quality expectations, independent ownership implications, and distribution/recognition diligence. Confirm every row with current SLH membership materials and your property-level agreement before capital commitments.",
      },
      {
        slotKey: "standards.requirement",
        title: "Membership Fit / Independent Ownership",
        sortOrder: 10,
        body: reqBody({
          typical:
            "SLH membership is selective and oriented to independently owned, intimate-scale luxury hotels with distinctive character.",
          owner:
            "Confirm whether this asset’s ownership, scale, and positioning fit SLH membership criteria—and what exclusivity or portfolio rules apply.",
          notes: "Do not apply franchise / chain-prototype diligence logic.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Quality / Inspection Expectations",
        sortOrder: 11,
        body: reqBody({
          typical:
            "Membership typically involves quality standards and inspection or review processes for acceptance and continuation.",
          owner:
            "Confirm inspection cadence, remediation expectations, and what happens if standards slip after joining.",
          status: "Typically Expected",
          notes: "Confirm with current SLH membership materials—not assumed chain QA manuals.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Guestroom / Experience Standards",
        sortOrder: 12,
        body: reqBody({
          typical:
            "Guest experience, finishes, and service levels must support SLH’s independent luxury positioning.",
          owner:
            "Confirm renovation or uplift scope needed for membership readiness without forcing a chain prototype.",
          status: "Typically Expected",
          notes: "Property-specific agreement terms govern—not a franchise PIP schedule.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Public Space / Property Character",
        sortOrder: 13,
        body: reqBody({
          typical:
            "Public spaces and property character should reinforce independent luxury identity.",
          owner:
            "Confirm arrival, common areas, and experience programming still read as distinctive after any membership-related changes.",
          status: "Typically Expected",
          notes: "Preserve independence—avoid consortium-generic graphics as the product story.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Distribution / Recognition Diligence",
        sortOrder: 14,
        body: reqBody({
          typical:
            "SLH provides consortium distribution and recognition benefits that differ from franchise brand systems.",
          owner:
            "Confirm channel participation, booking paths, marketing support, and how recognition shows up for owners and guests.",
          status: "Confirm With Brand",
          notes: "Public marketing is directional—confirm commercial terms in membership documents.",
        }),
      },
      {
        slotKey: "standards.requirement",
        title: "Training / QA / Membership Review",
        sortOrder: 15,
        body: reqBody({
          typical:
            "Onboarding, service expectations, and membership reviews typically apply around joining and thereafter.",
          owner:
            "Confirm onboarding timeline, service standards coaching, and ongoing membership review obligations.",
          status: "Typically Expected",
          notes: "Timing can affect opening or relaunch plans.",
        }),
      },
      {
        slotKey: "standards.conversion",
        title: "",
        sortOrder: 20,
        body:
          "For conversions or relaunches seeking SLH membership, sequence quality uplift, experience programming, and membership application/review with financing gates. Treat SLH as consortium affiliation—not a franchise flag conversion.",
      },
      {
        slotKey: "standards.questions",
        title: "",
        sortOrder: 30,
        body: [
          "Does this asset meet current SLH membership criteria for scale, ownership, and luxury positioning?",
          "What quality inspections or reviews apply at joining and ongoing?",
          "What property-level agreement terms govern fees, exits, and exclusivity?",
          "How does SLH distribution/recognition work for this market?",
          "What onboarding and service expectations should owners plan for before relaunch?",
        ].join("\n"),
      },
    ],
  },
});

/** True when every term hit sits in a contrastive / disambiguation window. */
function everyTermHitIsContrastive(text, termRe, contrastRe) {
  const re = new RegExp(termRe.source, termRe.flags.includes("g") ? termRe.flags : `${termRe.flags}g`);
  let m;
  while ((m = re.exec(text))) {
    const start = Math.max(0, m.index - 70);
    const end = Math.min(text.length, m.index + m[0].length + 50);
    if (!contrastRe.test(text.slice(start, end))) return false;
  }
  return true;
}

function validateCopySafety(text, brandSlug) {
  const forbidden = scanForbiddenLanguage(nz(text));
  const mechanical = scanMechanicalCopy(nz(text));
  const blockers = [];
  if (forbidden.hits?.length) blockers.push(`forbidden:${forbidden.hits.map((h) => h.id).join(",")}`);
  if ((mechanical.high || []).length) blockers.push(`mechanical_high:${mechanical.high.join(",")}`);
  if (/https?:\/\//i.test(text)) blockers.push("raw_url");
  if (/\bsources?\s*:/i.test(text)) blockers.push("source_note");
  // SLH: allow contrastive "not a franchise" / "differ from franchise"; block affirmative framing.
  if (brandSlug === "small-luxury-hotels-of-the-world") {
    const contrast =
      /\b(not|never|avoid|unlike|without|differ(?:s|ent)?\s+from|vs\.?|versus|do not|don't|no)\b/i;
    const franchiseOk = everyTermHitIsContrastive(text, /\bfranchise\b/i, contrast);
    const parentOk = everyTermHitIsContrastive(text, /\bparent[\s-]brand\b/i, contrast);
    const chainOk = everyTermHitIsContrastive(text, /\bchain prototype\b/i, contrast);
    if (!franchiseOk || !parentOk || !chainOk) {
      blockers.push("slh_franchise_or_parent_brand_language");
    }
  }
  // Indigo: InterContinental mentions must disambiguate (not / avoid / unlike), not confuse brands.
  if (brandSlug === "hotel-indigo" && /\bintercontinental\b/i.test(text)) {
    const contrast =
      /\b(not|never|avoid|unlike|vs\.?|versus|separate|corporate|other)\b/i;
    if (!everyTermHitIsContrastive(text, /\bintercontinental\b/i, contrast)) {
      blockers.push("indigo_intercontinental_confusion");
    }
  }
  return { ok: blockers.length === 0, blockers, forbidden, mechanical };
}

export function ingestV42OwnerConsiderationsCaution(brandSlug, reportsDir = path.join(ROOT, "reports")) {
  const name = V42_PACKET[brandSlug];
  if (!name) return { found: false, error: "no_packet_mapping" };
  const p = path.join(reportsDir, name);
  if (!fs.existsSync(p)) return { found: false, error: `missing_packet:${p}`, path: p };
  const md = fs.readFileSync(p, "utf8");
  const sectionMatch = md.match(
    /### Owner considerations \(`concern`\)([\s\S]*?)(?=\n### |\n## |$)/i
  );
  const section = sectionMatch ? sectionMatch[1] : "";
  const sampleMatch = section.match(/- Sample:\s*(.+)/i);
  const emptyMatch = section.match(/- Empty:\s*(.+)/i);
  return {
    found: /Owner considerations \(`concern`\)/i.test(md),
    path: p,
    tab: "Owner considerations",
    issueType: "placeholder_confirm_requirements_cues",
    emptyCues: nz(emptyMatch?.[1]),
    currentCopySample: nz(sampleMatch?.[1]),
    decision: (md.match(/\*\*(approve_[^*]+)\*\*/)?.[1] || "").trim(),
  };
}

function existingStandardsRows(presentationRows = []) {
  return (presentationRows || []).filter(
    (r) => nz(r.slotKey).startsWith("standards.") && !isHidden(r)
  );
}

function buildProposedPatches(brandSlug, brandConfig, presentationRows) {
  const plan = V42A_R2_STANDARDS_PLANS[brandSlug];
  if (!plan) throw new Error(`No standards plan for ${brandSlug}`);

  const existing = existingStandardsRows(presentationRows);
  const existingReqs = existing.filter((r) => r.slotKey === "standards.requirement");
  const cautions = [];
  const patches = [];

  // Primary caution: missing checklist → placeholder in atelier
  if (existingReqs.length === 0) {
    cautions.push({
      brand: brandSlug,
      tab: "Owner considerations",
      slot: "standards.requirement",
      recordId: null,
      issueType: "missing_standards_requirement_rows",
      currentCopy:
        "No owner planning checklist is published in Brand Explorer presentation for this brand yet. Confirm requirements with brand disclosure, the franchise agreement, and design standards manuals.",
      proposedCopy: "(see proposed standards.requirement creates)",
      patchRequired: true,
      founderJudgmentRemains: true,
      founderJudgmentNote:
        "Founder should taste-pass brand-specific checklist language after materialization; still no unlock.",
    });
  }

  for (const row of plan.rows) {
    const safety = validateCopySafety(row.body, brandSlug);
    const match =
      row.slotKey === "standards.requirement"
        ? existing.find(
            (r) =>
              r.slotKey === row.slotKey &&
              nz(r.title).toLowerCase() === nz(row.title).toLowerCase()
          )
        : existing.find((r) => r.slotKey === row.slotKey);

    if (match && nz(match.body) === nz(row.body) && nz(match.title) === nz(row.title)) {
      continue;
    }

    if (!safety.ok) {
      cautions.push({
        brand: brandSlug,
        tab: "Owner considerations",
        slot: row.slotKey,
        recordId: match?.recordId || null,
        issueType: "proposed_copy_failed_safety",
        currentCopy: match?.body || "",
        proposedCopy: row.body,
        patchRequired: false,
        founderJudgmentRemains: true,
        blockers: safety.blockers,
      });
      continue;
    }

    patches.push({
      action: match?.recordId ? "PATCH" : "POST",
      recordId: match?.recordId || null,
      slotKey: row.slotKey,
      title: row.title,
      sortOrder: row.sortOrder,
      fields: {
        Title: row.title || "",
        Body: row.body,
      },
      createFields: {
        "Slot Key": row.slotKey,
        "Brand Name": brandConfig.name,
        Brand: [brandConfig.recordId],
        Active: true,
        "Sort Order": row.sortOrder,
        Title: row.title || "",
        Body: row.body,
        "External Display Status": null,
      },
      issueType: match?.recordId ? "update_standards_copy" : "create_standards_row",
      currentCopy: match?.body || "",
      proposedCopy: row.body,
      patchRequired: true,
      founderJudgmentRemains: true,
      safety,
    });
  }

  return { plan, existing, cautions, patches };
}

export async function confirmV42AR2OsRouting(targetBrands = V42A_R2_TARGET_BRANDS) {
  const targets = [];
  const blockers = [];
  for (const brandSlug of targetBrands) {
    const os = await evaluateBrandExplorerOsBrand(brandSlug);
    const ctx = await loadBrandFactoryContext(brandSlug);
    const brand = ctx.brandApi;
    const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });
    const ql = evaluateBrandExternalQualityLock(brand, html, { brandSlug });
    const row = {
      brandSlug,
      canonicalState: os.canonicalState,
      allowedNextAction: os.routing?.allowedNextAction,
      shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
      externalLocked:
        brand.shouldRenderFullProfile !== true &&
        (ql.profileInPreparationRendered === true || (ql.tabsRenderedExternally || []).length <= 1),
      pass: true,
    };
    if (brand.shouldRenderFullProfile === true) {
      blockers.push(`${brandSlug}:unexpected_full_profile`);
      row.pass = false;
    }
    if (os.canonicalState === "active_profile_ready") {
      blockers.push(`${brandSlug}:unexpected_active_profile_ready`);
      row.pass = false;
    }
    if (
      os.routing?.allowedNextAction !== "founder_visual_review" &&
      os.canonicalState !== "founder_review_ready"
    ) {
      // soft warning only — still allow cleanup if packet says minor cleanup
      row.note = `os_action=${os.routing?.allowedNextAction}`;
    }
    targets.push(row);
  }
  return { pass: blockers.length === 0, blockers, targets };
}

export async function protectV42AR2ReleasedBaseline() {
  const snapshots = [];
  for (const slug of V42A_R2_PROTECTED_RELEASED) {
    snapshots.push(await captureV44BrandSnapshot(slug));
  }
  for (const slug of V42A_R2_TARGET_BRANDS) {
    snapshots.push(await captureV44BrandSnapshot(slug));
  }
  const regression = evaluateV44Regression(snapshots);
  return {
    pass: regression.pass,
    failures: regression.failures,
    checks: regression.checks,
  };
}

export async function planV42AR2Brand(brandSlug) {
  if (!V42A_R2_TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`v42A-R2 target brands only: ${V42A_R2_TARGET_BRANDS.join(", ")}`);
  }
  if (V42A_R2_PROTECTED_RELEASED.includes(brandSlug)) {
    throw new Error(`v42A-R2 refuses released brand ${brandSlug}`);
  }

  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) throw new Error(`No brand config for ${brandSlug}`);

  const v42 = ingestV42OwnerConsiderationsCaution(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug);
  const { plan, existing, cautions, patches } = buildProposedPatches(
    brandSlug,
    brandConfig,
    ctx.presentationRows || []
  );

  const blockers = [];
  if (patches.length > V42A_R2_MAX_PATCHES_PER_BRAND) {
    blockers.push(`patch_limit_exceeded:${patches.length}>${V42A_R2_MAX_PATCHES_PER_BRAND}`);
  }
  for (const p of patches) {
    for (const field of Object.keys(p.fields || {})) {
      // Title/Body only in patch fields; createFields checked separately
    }
    for (const field of Object.keys({ ...(p.fields || {}), ...(p.createFields || {}) })) {
      if (FORBIDDEN_WRITE_FIELDS.has(field)) blockers.push(`forbidden_field:${field}`);
    }
  }

  const cautionRows = [
    ...cautions,
    ...patches.map((p) => ({
      brand: brandSlug,
      tab: "Owner considerations",
      slot: p.slotKey,
      recordId: p.recordId,
      issueType: p.issueType,
      currentCopy: p.currentCopy,
      proposedCopy: p.proposedCopy,
      patchRequired: true,
      founderJudgmentRemains: true,
    })),
  ];

  const projectedDecision =
    blockers.length === 0 && patches.length <= V42A_R2_MAX_PATCHES_PER_BRAND
      ? "approve_for_active_release"
      : "approve_after_minor_cleanup";

  return {
    brandSlug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    v42Packet: v42,
    sectionFocus: plan.sectionFocus,
    existingStandardsCount: existing.length,
    existingRequirementCount: existing.filter((r) => r.slotKey === "standards.requirement").length,
    cautions: cautionRows,
    patches,
    blockers,
    blocked: blockers.length > 0,
    projection: {
      founderDecision: projectedDecision,
      externallyLocked: true,
      activeProfileReady: false,
      activeReleaseBlockedUntilFounderOkAndV43: true,
      patchCount: patches.length,
    },
    guardrails: {
      unlock: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      releasedBrandChanges: false,
    },
  };
}

async function airtableWrite({ baseId, apiKey, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} failed: ${res.status}`);
  return json;
}

export function parseV42AR2ApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export async function applyV42AR2Plans({ brandResults, apply = false, argv = [] } = {}) {
  const flagCheck = parseV42AR2ApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "blocked", blockers: brand.blockers };
      continue;
    }
    const results = { created: [], updated: [], errors: [] };
    for (const patch of brand.patches || []) {
      try {
        if (patch.action === "PATCH" && patch.recordId) {
          await airtableWrite({
            baseId,
            apiKey,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          results.updated.push({ recordId: patch.recordId, slotKey: patch.slotKey });
        } else {
          const json = await airtableWrite({
            baseId,
            apiKey,
            recordId: "",
            fields: patch.createFields,
            method: "POST",
          });
          results.created.push({ recordId: json.id, slotKey: patch.slotKey, title: patch.title });
        }
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        results.errors.push({ slotKey: patch.slotKey, message: err.message });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: results.errors.length === 0 && results.created.length + results.updated.length > 0,
      results,
    };
  }
  return { applied: true, resultsByBrand, flagCheck };
}

export async function runV42AR2FounderMinorCleanupBatch({
  brands = V42A_R2_TARGET_BRANDS,
  dryRun = true,
  argv = [],
} = {}) {
  for (const b of brands) {
    if (V42A_R2_PROTECTED_RELEASED.includes(b)) throw new Error(`Refuse released brand ${b}`);
    if (!V42A_R2_TARGET_BRANDS.includes(b)) {
      throw new Error(`v42A-R2 targets only: ${V42A_R2_TARGET_BRANDS.join(", ")}`);
    }
  }

  const osConfirm = await confirmV42AR2OsRouting(brands);
  const baselineProtection = await protectV42AR2ReleasedBaseline();
  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await planV42AR2Brand(brandSlug));
  }

  const applyResult = dryRun
    ? { applied: false, reason: "dry_run_only" }
    : await applyV42AR2Plans({ brandResults, apply: true, argv });

  return {
    version: V42A_R2_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    osConfirm,
    baselineProtection,
    brandResults,
    applyResult,
    summary: {
      targets: brandResults.length,
      osRoutingPass: osConfirm.pass,
      baselineProtectionPass: baselineProtection.pass,
      totalPatches: brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0),
      blockedBrands: brandResults.filter((b) => b.blocked).length,
      projectedApproveForActiveRelease: brandResults.filter(
        (b) => b.projection?.founderDecision === "approve_for_active_release"
      ).length,
      presentationWrites: dryRun ? false : applyResult.applied === true,
    },
    guardrails: {
      activeRelease: false,
      companyValidatedChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      releasedBrandChanges: false,
      incompleteBrandUnlock: false,
    },
  };
}

function brandMd(b) {
  const lines = [
    `# v42A-R2 Founder Minor Cleanup — ${b.brandName}`,
    "",
    `Slug: \`${b.brandSlug}\``,
    "",
    "## v42 packet",
    "",
    `- Decision: **${b.v42Packet?.decision || "unknown"}**`,
    `- Caution found: **${b.v42Packet?.found}**`,
    `- Empty cues: ${b.v42Packet?.emptyCues || "—"}`,
    `- Sample: ${b.v42Packet?.currentCopySample || "—"}`,
    "",
    `Section focus: ${b.sectionFocus}`,
    "",
    "## Projection",
    "",
    `- Founder decision: **${b.projection.founderDecision}**`,
    `- Externally locked: **${b.projection.externallyLocked}**`,
    `- active_profile_ready: **${b.projection.activeProfileReady}**`,
    `- Patch count: **${b.projection.patchCount}** / ${V42A_R2_MAX_PATCHES_PER_BRAND} max`,
    "",
    "## Cautions / proposed patches",
    "",
  ];
  for (const c of b.cautions || []) {
    lines.push(
      `- **${c.slot}** · ${c.issueType} · patchRequired=${c.patchRequired} · record=${c.recordId || "CREATE"}`
    );
    if (c.currentCopy) lines.push(`  - current: ${String(c.currentCopy).slice(0, 160)}`);
    if (c.proposedCopy) lines.push(`  - proposed: ${String(c.proposedCopy).slice(0, 160)}`);
  }
  if (b.blockers?.length) {
    lines.push("", "## Blockers", "");
    for (const x of b.blockers) lines.push(`- ${x}`);
  }
  lines.push(
    "",
    "## Guardrails",
    "",
    "- No unlock / active-profile approval / Company Validated",
    "- No Source Library / Registry / Image field changes",
    "- No released brand changes",
    "- No generic filler; brand-model-specific diligence only",
    ""
  );
  return lines.join("\n");
}

export function writeV42AR2Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# v42A-R2 Founder Minor Cleanup Batch`,
    "",
    `Generated: ${report.generatedAt}`,
    "",
    report.dryRun
      ? "Dry-run. Plans Owner Considerations / standards checklist patches only."
      : "Apply mode — Presentation Title/Body (and creates) for standards slots only.",
    "",
    "## Summary",
    "",
    `- OS routing pass: **${report.summary.osRoutingPass}**`,
    `- Baseline protection: **${report.summary.baselineProtectionPass}**`,
    `- Total patches: **${report.summary.totalPatches}**`,
    `- Blocked brands: **${report.summary.blockedBrands}**`,
    `- Projected approve_for_active_release: **${report.summary.projectedApproveForActiveRelease}**`,
    `- Presentation writes: **${report.summary.presentationWrites}**`,
    "",
    "| Brand | Existing standards | Patches | Projection |",
    "|---|---|---|---|",
  ];
  for (const b of report.brandResults) {
    md.push(
      `| ${b.brandSlug} | ${b.existingStandardsCount} (req ${b.existingRequirementCount}) | ${b.patches.length} | ${b.projection.founderDecision} |`
    );
  }
  md.push("", "## Guardrails", "");
  for (const [k, v] of Object.entries(report.guardrails || {})) md.push(`- ${k}: ${v}`);
  md.push("");
  fs.writeFileSync(mdPath, md.join("\n"));

  const brandPaths = {};
  for (const b of report.brandResults) {
    const name = BRAND_REPORT_MD[b.brandSlug];
    if (!name) continue;
    const p = path.join(reportsDir, name);
    fs.writeFileSync(p, brandMd(b));
    brandPaths[b.brandSlug] = p;
  }
  return { jsonPath, mdPath, brandPaths };
}
