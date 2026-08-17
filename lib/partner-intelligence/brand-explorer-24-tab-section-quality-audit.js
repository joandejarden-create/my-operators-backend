/**
 * Brand Explorer — 24-brand tab/section public-full quality audit (read-only).
 *
 * Universe: Brand Basics Brand Status Active/Live
 * (lib/partner-intelligence/brand-explorer-active-universe.js)
 *
 * No Airtable writes. Dry-run only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACTIVE_UNIVERSE_SOURCE,
  ACTIVE_UNIVERSE_VERSION,
  NON_ACTIVE_STATUS_CONFLICT_PROBES,
  listActiveUniverseSlugs,
  loadActiveUniverse,
  resolveActiveUniverseRecordId,
} from "./brand-explorer-active-universe.js";
import {
  evaluateBrandPublicVisibility,
  isOwnerFacingPresentationRow,
  scanOwnerFacingForbiddenLanguage,
} from "./brand-explorer-public-visibility-quality-lock.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const AUDIT_VERSION = "24-tab-section-quality-audit-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

/** Slot → tab/section families for founder-facing audit tables. */
export const TAB_SECTION_FAMILIES = Object.freeze([
  {
    tab: "Brand Snapshot",
    sections: [
      { section: "Positioning summary", slots: ["Brand Positioning", "overview.relative_positioning"] },
      { section: "Owner / property fit", slots: ["overview.typical_use_case", "overview.featured_application"] },
      { section: "Audience / target guest", slots: ["Guest Psychographics Description"] },
      { section: "Why value / chips", slots: ["overview.why_value", "overview.differentiators.identity", "overview.differentiators.commercial"] },
    ],
  },
  {
    tab: "Brand Positioning",
    sections: [
      { section: "Relative positioning", slots: ["overview.relative_positioning", "overview.portfolio_context"] },
      { section: "Development model", slots: ["overview.development_model"] },
      { section: "Best-at points", slots: ["overview.bestAt.1", "overview.bestAt.2", "overview.bestAt.3"] },
    ],
  },
  {
    tab: "Where This Brand Creates the Most Value",
    sections: [
      { section: "Value scenario 1", slots: ["overview.scenario.1"] },
      { section: "Value scenario 2", slots: ["overview.scenario.2"] },
      { section: "Value scenario 3", slots: ["overview.scenario.3"] },
      { section: "Owner overview", slots: ["valueOwners.overview"] },
      { section: "Watchouts", slots: ["valueOwners.watchouts"] },
    ],
  },
  {
    tab: "Support Across Lifecycle",
    sections: [
      { section: "Lifecycle phases", slots: ["valueOwners.lifecycle.1", "valueOwners.lifecycle.2", "valueOwners.lifecycle.3", "valueOwners.lifecycle.4", "valueOwners.lifecycle.5", "valueOwners.lifecycle.6"] },
      { section: "Opening path", slots: ["economics.opening.step.1", "economics.opening.step.2", "economics.opening.step.3", "economics.opening.step.4", "economics.opening.step.5"] },
    ],
  },
  {
    tab: "Proof Points / Property Examples",
    sections: [
      { section: "Proof points", slots: ["overview.proof.1", "overview.proof.2", "overview.proof.3", "overview.proof.4"] },
      { section: "Property openings", slots: ["footprint.openings"] },
    ],
  },
  {
    tab: "Portfolio Context",
    sections: [
      { section: "Portfolio context", slots: ["overview.portfolio_context", "footprint.portfolio_context"] },
      { section: "Portfolio mix", slots: ["footprint.portfolio_mix"] },
    ],
  },
  {
    tab: "Geographic Footprint",
    sections: [
      { section: "Geo intro", slots: ["footprint.geo_intro"] },
      { section: "Regions", slots: ["footprint.region.am", "footprint.region.cala", "footprint.region.eu", "footprint.region.mea", "footprint.region.apac"] },
    ],
  },
  {
    tab: "Growth Priorities / Recent Momentum",
    sections: [{ section: "Recent Momentum", slots: ["footprint.momentum"] }],
  },
  {
    tab: "Operating Model",
    sections: [
      { section: "Model structure", slots: ["operations.model.primary_model", "operations.model.management_option", "operations.model.typical_ownership", "operations.model.brand_involvement"] },
      { section: "Systems / staffing", slots: ["operations.model.systems_integration", "operations.model.staffing_intensity", "operations.model.fb_complexity", "operations.model.training"] },
      { section: "Operator compatibility", slots: ["operations.operator_compat.summary", "operations.operator_compat.fit", "operations.operator_compat.tags"] },
    ],
  },
  {
    tab: "Standards / Flexibility / Compliance",
    sections: [
      { section: "Standards philosophy", slots: ["operations.standards_philosophy"] },
      { section: "Flexibility indicators", slots: ["operations.flexibility.design", "operations.flexibility.conversion", "operations.flexibility.localization", "operations.flexibility.operational_rigidity", "operations.flexibility.pip", "operations.flexibility.prototype"] },
      { section: "Compliance", slots: ["operations.compliance.qa_cadence", "operations.compliance.training_rigor", "operations.compliance.reporting", "operations.compliance.brand_interaction"] },
    ],
  },
  {
    tab: "Similar Brands",
    sections: [{ section: "Similar brands", slots: ["standards.similar", "overview.similar", "similar.brands"] }],
  },
  {
    tab: "Owner Considerations",
    sections: [{ section: "Owner considerations", slots: ["standards.owner_considerations", "owner.considerations", "overview.owner_considerations"] }],
  },
  {
    tab: "Questions Owners Should Ask",
    sections: [{ section: "Questions", slots: ["standards.questions", "owner.questions"] }],
  },
  {
    tab: "Modals / CTAs / chips / tags",
    sections: [
      { section: "Gallery", slots: ["materials.gallery.1", "materials.gallery.2", "materials.gallery.3", "materials.gallery.4", "materials.gallery.5", "materials.gallery.6"] },
      { section: "Case study / materials", slots: ["materials.caseStudy"] },
    ],
  },
]);

const PLACEHOLDER_RE =
  /\b(TODO|TBD|lorem ipsum|placeholder|coming soon|profile in preparation|\[insert|n\/a|xxx+|FIXME)\b/i;
const INTERNAL_LEAK_RE =
  /\b(confirm in FDD|Item\s*19|fee stack|source note|methodology|staging|QA gate|Do Not Display|Company Validated|brand-verified)\b/i;
const WRONG_BRAND_MARKERS = Object.freeze([
  {
    re: /\bMarriott Bonvoy\b/i,
    unlessSlugIncludes: ["marriott", "autograph", "tribute", "design-hotels", "moxy"],
    parentCompanyIncludes: ["marriott"],
  },
  {
    re: /\bHilton Honors\b/i,
    unlessSlugIncludes: ["hilton", "curio", "tapestry", "canopy", "tempo", "spark", "motto"],
    parentCompanyIncludes: ["hilton"],
  },
  {
    re: /\bWorld of Hyatt\b/i,
    unlessSlugIncludes: ["hyatt", "unbound", "andaz", "jdv"],
    // Intentionally no broad parentCompanyIncludes: Bunkhouse (Hyatt-affiliated)
    // still needs owner-facing copy that does not treat World of Hyatt as brand identity.
    parentCompanyIncludes: [],
  },
  {
    re: /\bIHG One Rewards\b/i,
    unlessSlugIncludes: [
      "indigo",
      "kimpton",
      "holiday",
      "crowne",
      "intercontinental",
      "voco",
      "avid",
      "vignette",
      "handwritten",
      "even-hotels",
      "staybridge",
      "candlewood",
      "regent",
      "six-senses",
    ],
    parentCompanyIncludes: ["ihg", "intercontinental hotels"],
  },
  {
    re: /\bBest Western Rewards\b/i,
    unlessSlugIncludes: ["best-western", "bw-", "woodspring", "surestay", "everhome", "ascend", "quality", "comfort", "sleep", "clarion", "econo", "rodeway", "suburban", "radisson", "country-inn"],
    parentCompanyIncludes: ["best western", "bwh"],
  },
]);

/**
 * Targeted parent-platform exemptions when slug text alone misses affiliation
 * (e.g. moxy-hotels → Marriott Bonvoy). Does not disable wrong-brand detection.
 */
export const PARENT_PLATFORM_LOYALTY_SLUG_EXEMPTIONS = Object.freeze({
  "moxy-hotels": Object.freeze(["marriott"]),
});

function isWrongBrandMarkerExempt(slug, marker, brand = {}) {
  const s = nz(slug).toLowerCase();
  if ((marker.unlessSlugIncludes || []).some((p) => s.includes(String(p).toLowerCase()))) {
    return true;
  }
  const parent = nz(brand.parentCompany || brand.parent || brand.parentBrand || "").toLowerCase();
  if (parent && (marker.parentCompanyIncludes || []).some((p) => parent.includes(String(p).toLowerCase()))) {
    return true;
  }
  const extras = PARENT_PLATFORM_LOYALTY_SLUG_EXEMPTIONS[s] || [];
  if (extras.some((p) => (marker.unlessSlugIncludes || []).includes(p) || (marker.parentCompanyIncludes || []).includes(p))) {
    return true;
  }
  return false;
}

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(text) {
  return nz(text)
    .split(/\s+/)
    .filter(Boolean).length;
}

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

async function fetchBrandById(brandId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`Brand fetch failed for ${brandId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

function rowsBySlot(rows = []) {
  const map = new Map();
  for (const r of rows) {
    const k = nz(r.slotKey);
    if (!k) continue;
    if (!map.has(k)) map.set(k, []);
    map.get(k).push(r);
  }
  return map;
}

function finding({
  tab,
  section,
  status = "pass",
  finding = "",
  severity = "minor",
  proposedFix = "",
  recordId = null,
  slotKey = null,
  field = null,
}) {
  return { tab, section, status, finding, severity, proposedFix, recordId, slotKey, field };
}

function scoreBand(score, hasBlocker) {
  if (hasBlocker) return "remediation_required";
  if (score >= 95) return "approve_for_baseline_freeze";
  if (score >= 85) return "approve_after_minor_cleanup";
  return "remediation_required";
}

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function auditTabSections({ brandName, slug, ownerRows, brand = {} }) {
  const bySlot = rowsBySlot(ownerRows);
  const findings = [];

  for (const family of TAB_SECTION_FAMILIES) {
    for (const sec of family.sections) {
      const matched = [];
      for (const slot of sec.slots) {
        const hits = bySlot.get(slot) || [];
        matched.push(...hits.map((r) => ({ ...r, _slot: slot })));
      }

      if (!matched.length) {
        // Optional families (similar/questions) → minor; core scenarios → major
        const core =
          /Creates the Most Value|Lifecycle|Snapshot|Positioning|Operating|Standards|Momentum|Proof|Portfolio|Geographic/i.test(
            family.tab
          );
        findings.push(
          finding({
            tab: family.tab,
            section: sec.section,
            status: "missing",
            finding: `No owner-facing Presentation rows for expected slots (${sec.slots.join(", ")})`,
            severity: core ? "major" : "minor",
            proposedFix: core
              ? "Add targeted Presentation Body for missing slot(s)"
              : "Confirm whether section is intentionally suppressed; otherwise add brand-specific copy",
            slotKey: sec.slots[0],
          })
        );
        continue;
      }

      for (const row of matched) {
        const body = nz(row.body);
        const title = nz(row.title);
        const rid = row.recordId || row.id || null;
        const slotKey = row._slot || row.slotKey;

        // Gallery captions live in Title (+ Image); Body is optional.
        const isGallery = /^materials\.gallery\.\d+$/.test(slotKey);
        const isFlexLevel = /^operations\.flexibility\./.test(slotKey);

        if (!body && !isFlexLevel && !isGallery) {
          findings.push(
            finding({
              tab: family.tab,
              section: sec.section,
              status: "empty",
              finding: title ? `Title-only / blank Body on ${slotKey}` : `Empty Body on ${slotKey}`,
              severity: "blocker",
              proposedFix: "Fill Presentation Body or suppress the component",
              recordId: rid,
              slotKey,
              field: "Body",
            })
          );
          continue;
        }

        if (isGallery) {
          if (!nz(row.imageUrl)) {
            findings.push(
              finding({
                tab: family.tab,
                section: sec.section,
                status: "missing_image",
                finding: `Gallery slot ${slotKey} missing image`,
                severity: "blocker",
                proposedFix: "Attach a distinct gallery image",
                recordId: rid,
                slotKey,
                field: "Image",
              })
            );
          } else if (!title) {
            findings.push(
              finding({
                tab: family.tab,
                section: sec.section,
                status: "thin",
                finding: `Gallery slot ${slotKey} missing caption Title`,
                severity: "minor",
                proposedFix: "Add owner-facing image caption in Title",
                recordId: rid,
                slotKey,
                field: "Title",
              })
            );
          }
          continue;
        }

        if (PLACEHOLDER_RE.test(body) || PLACEHOLDER_RE.test(title)) {
          findings.push(
            finding({
              tab: family.tab,
              section: sec.section,
              status: "placeholder",
              finding: `Placeholder / stub language on ${slotKey}`,
              severity: "blocker",
              proposedFix: "Replace placeholder with brand-specific owner-facing copy",
              recordId: rid,
              slotKey,
              field: "Body",
            })
          );
        }

        if (INTERNAL_LEAK_RE.test(body) || INTERNAL_LEAK_RE.test(title)) {
          findings.push(
            finding({
              tab: family.tab,
              section: sec.section,
              status: "internal_leak",
              finding: `Internal / diligence language on ${slotKey}`,
              severity: "major",
              proposedFix: "Rewrite in owner-facing language (no FDD/Item 19/source-process wording)",
              recordId: rid,
              slotKey,
              field: "Body",
            })
          );
        }

        for (const marker of WRONG_BRAND_MARKERS) {
          if (isWrongBrandMarkerExempt(slug, marker, brand)) continue;
          if (marker.re.test(body) || marker.re.test(title)) {
            findings.push(
              finding({
                tab: family.tab,
                section: sec.section,
                status: "wrong_brand",
                finding: `Possible wrong-brand/parent carryover on ${slotKey}: ${String(marker.re)}`,
                severity: "major",
                proposedFix: "Replace with this brand’s loyalty/platform language",
                recordId: rid,
                slotKey,
                field: "Body",
              })
            );
          }
        }

        // Thin prose (skip flexibility level labels)
        if (!/^operations\.flexibility\./.test(slotKey) && body && words(body) < 12 && !/^High|Medium|Low|Moderate$/i.test(body)) {
          findings.push(
            finding({
              tab: family.tab,
              section: sec.section,
              status: "thin",
              finding: `Thin Body (${words(body)} words) on ${slotKey}`,
              severity: "minor",
              proposedFix: "Thicken with brand-specific owner guidance",
              recordId: rid,
              slotKey,
              field: "Body",
            })
          );
        }

        // Scenario cards need images for value section
        if (/^overview\.scenario\.[123]$/.test(slotKey) && !nz(row.imageUrl)) {
          findings.push(
            finding({
              tab: family.tab,
              section: sec.section,
              status: "missing_image",
              finding: `Missing scenario image on ${slotKey}`,
              severity: "blocker",
              proposedFix: "Attach a distinct scenario image matching the card thesis",
              recordId: rid,
              slotKey,
              field: "Image",
            })
          );
        }
      }

      if (!findings.some((f) => f.tab === family.tab && f.section === sec.section && f.status !== "pass")) {
        findings.push(
          finding({
            tab: family.tab,
            section: sec.section,
            status: "pass",
            finding: `Populated (${matched.length} row(s))`,
            severity: "taste",
            proposedFix: "",
          })
        );
      }
    }
  }

  // Brand name presence soft check on positioning
  const positioning = (bySlot.get("Brand Positioning") || [])[0];
  if (positioning && brandName && !nz(positioning.body).toLowerCase().includes(brandName.toLowerCase().split(" ")[0])) {
    findings.push(
      finding({
        tab: "Brand Positioning",
        section: "Brand role",
        status: "specificity",
        finding: "Brand Positioning Body may not name the brand explicitly",
        severity: "taste",
        proposedFix: "Ensure opening sentence names the brand",
        recordId: positioning.recordId || positioning.id,
        slotKey: "Brand Positioning",
        field: "Body",
      })
    );
  }

  return findings;
}

function auditScenarioImageRoles(ownerRows) {
  const scenarios = [1, 2, 3]
    .map((i) => ownerRows.find((r) => nz(r.slotKey) === `overview.scenario.${i}`))
    .filter(Boolean);
  const findings = [];
  const roles = scenarios.map((r) => {
    const title = nz(r.title).toLowerCase();
    const caption = nz(r.imageCaption || r.title).toLowerCase();
    let role = "unknown";
    if (/exterior|arrival|conversion|reposition|facade|street/i.test(`${title} ${caption}`)) role = "exterior_arrival";
    else if (/lobby|distribution|commercial|guest journey|public/i.test(`${title} ${caption}`)) role = "public_space_lobby";
    else if (/room|suite|lifestyle|f&b|wellness|pool|destination|experience/i.test(`${title} ${caption}`))
      role = "experience";
    return { recordId: r.recordId || r.id, slotKey: r.slotKey, title: r.title, imageUrl: r.imageUrl, role };
  });

  const roleCounts = roles.reduce((acc, r) => {
    acc[r.role] = (acc[r.role] || 0) + 1;
    return acc;
  }, {});
  for (const [role, count] of Object.entries(roleCounts)) {
    if (role !== "unknown" && count >= 3) {
      findings.push({
        section: "Where This Brand Creates the Most Value",
        card: "all three scenarios",
        image: roles.map((r) => r.imageUrl).filter(Boolean)[0] || null,
        finding: `repeated_visual_role:${role}`,
        severity: "major",
        proposedFix: "Diversify scenario imagery across exterior / commercial / experience roles",
        issueType: "repeated_visual_role",
        currentCaption: null,
      });
    }
  }
  return { roles, findings };
}

function buildImageFindings({ brandSlug, uniqueness, roleMatch, scenarioRoleFindings }) {
  const out = [];
  for (const g of uniqueness.duplicateGroups || []) {
    out.push({
      brandSlug,
      section: g.section,
      card: (g.slots || []).join(", "),
      image: g.imageUrl || null,
      finding: g.status === "duplicate_or_near_duplicate" ? "near_duplicate" : "duplicate_url",
      issueType: g.status === "duplicate_or_near_duplicate" ? "near_duplicate" : "duplicate_url",
      severity: g.section === "scenario" ? "blocker" : "major",
      proposedFix: g.requiredFix || "Replace with a distinct image",
      currentCaption: (g.titles || [])[0] || null,
      recordIds: null,
      slots: g.slots || [],
    });
  }

  if ((uniqueness.scenarioDistinctCount || 0) < 3) {
    out.push({
      brandSlug,
      section: "Where This Brand Creates the Most Value",
      card: "overview.scenario.1–3",
      image: null,
      finding: `Only ${uniqueness.scenarioDistinctCount || 0}/3 distinct scenario images`,
      issueType: "duplicate_url",
      severity: "blocker",
      proposedFix: "Ensure three distinct scenario image assets",
      currentCaption: null,
    });
  }
  if ((uniqueness.galleryDistinctCount || 0) < 6) {
    out.push({
      brandSlug,
      section: "Gallery",
      card: "materials.gallery.*",
      image: null,
      finding: `Only ${uniqueness.galleryDistinctCount || 0}/6 distinct gallery images`,
      issueType: "duplicate_url",
      severity: "major",
      proposedFix: "Rematerialize gallery to 6 distinct images",
      currentCaption: null,
    });
  }
  if ((uniqueness.propertyExampleDistinctCount || 0) < 3) {
    out.push({
      brandSlug,
      section: "Property Examples",
      card: "footprint.openings",
      image: null,
      finding: `Only ${uniqueness.propertyExampleDistinctCount || 0}/3 distinct property images`,
      issueType: "duplicate_url",
      severity: "major",
      proposedFix: "Use distinct property imagery for openings cards",
      currentCaption: null,
    });
  }

  for (const e of roleMatch.evaluations || roleMatch.entries || []) {
    if (e.matchStatus === "pass" || e.matchStatus === "ok") continue;
    if (e.matchStatus === "ambiguous") {
      out.push({
        brandSlug,
        section: e.slotKey || "image",
        card: e.title || e.slotKey,
        image: e.imageUrl || null,
        finding: e.issue || "ambiguous_role",
        issueType: "caption_mismatch",
        severity: "minor",
        proposedFix: "Clarify caption/role to match visual category",
        currentCaption: e.title || null,
      });
    } else if (/wrong_role|needs_replacement|conflict|overclaim|needs_caption/i.test(e.matchStatus || "")) {
      out.push({
        brandSlug,
        section: e.slotKey || "image",
        card: e.title || e.slotKey,
        image: e.imageUrl || null,
        finding: e.issue || e.matchStatus,
        issueType: e.matchStatus === "wrong_role" ? "wrong_property" : "caption_mismatch",
        severity: /wrong_role|needs_replacement/i.test(e.matchStatus) ? "major" : "minor",
        proposedFix: "Align caption with detected visual role or swap image",
        currentCaption: e.title || null,
      });
    }
  }

  for (const f of scenarioRoleFindings || []) {
    out.push({ brandSlug, ...f });
  }

  return out;
}

function scoreBrand({ tabFindings, imageFindings, tabFactory, pvql, emptyPass, html }) {
  let population = 100;
  let consistency = 100;
  let specificity = 100;
  let usefulness = 100;
  let visual = 100;
  let scenarioDistinct = 100;

  const blockers = [...tabFindings, ...imageFindings].filter((f) => f.severity === "blocker");
  const majors = [...tabFindings, ...imageFindings].filter((f) => f.severity === "major");
  const minors = [...tabFindings, ...imageFindings].filter((f) => f.severity === "minor");

  population -= blockers.filter((f) => /empty|placeholder|missing_image|missing/.test(f.status || f.finding || "")).length * 12;
  population -= majors.filter((f) => /thin|empty|missing/.test(f.status || f.finding || "")).length * 4;
  population -= minors.filter((f) => f.status === "thin").length * 1;
  if (!emptyPass) population -= 15;
  if (/Profile in Preparation/i.test(html || "")) {
    population -= 20;
    blockers.push({ severity: "blocker", finding: "Profile in Preparation visible in public render" });
  }

  if (pvql?.lockPass !== true) {
    specificity -= 20;
    blockers.push({ severity: "blocker", finding: "PVQL lockPass=false" });
  }
  if (tabFactory?.gates?.golden_content_quality === false) specificity -= 10;
  if (tabFactory?.gates?.section_pattern_parity === false) consistency -= 10;
  if (tabFactory?.gates?.rendered_field_completeness === false) population -= 10;

  specificity -= tabFindings.filter((f) => f.status === "wrong_brand" || f.status === "internal_leak").length * 8;
  usefulness -= tabFindings.filter((f) => /Owner|Questions|Lifecycle|Watchouts/i.test(f.tab || "") && f.status !== "pass").length * 3;

  const scenarioImageIssues = imageFindings.filter((f) =>
    /scenario|Creates the Most Value|repeated_visual_role/i.test(`${f.section} ${f.finding}`)
  );
  scenarioDistinct -= scenarioImageIssues.filter((f) => f.severity === "blocker").length * 20;
  scenarioDistinct -= scenarioImageIssues.filter((f) => f.severity === "major").length * 10;
  visual -= imageFindings.filter((f) => f.severity === "blocker").length * 12;
  visual -= imageFindings.filter((f) => f.severity === "major").length * 6;
  visual -= imageFindings.filter((f) => f.severity === "minor").length * 2;

  population = clamp(Math.round(population), 0, 100);
  consistency = clamp(Math.round(consistency), 0, 100);
  specificity = clamp(Math.round(specificity), 0, 100);
  usefulness = clamp(Math.round(usefulness), 0, 100);
  visual = clamp(Math.round(visual), 0, 100);
  scenarioDistinct = clamp(Math.round(scenarioDistinct), 0, 100);

  const composite = Math.round(
    population * 0.22 +
      consistency * 0.12 +
      specificity * 0.18 +
      usefulness * 0.18 +
      visual * 0.15 +
      scenarioDistinct * 0.15
  );

  const hasBlocker = blockers.length > 0;
  return {
    populationCompleteness: population,
    consistency,
    brandSpecificity: specificity,
    ownerUsefulness: usefulness,
    visualQuality: visual,
    scenarioImageDistinctiveness: scenarioDistinct,
    composite,
    recommendation: scoreBand(composite, hasBlocker),
    blockerCount: blockers.length,
    majorCount: majors.length,
    minorCount: minors.length,
  };
}

/**
 * Audit one brand (read-only).
 */
export async function auditBrandTabSectionQuality(slug, { universeRow = null } = {}) {
  const recordId = universeRow?.recordId || resolveActiveUniverseRecordId(slug) || slug;
  const brand = await fetchBrandById(recordId);
  const liveBlocks = brand.brandExplorer?.blocks || [];
  const ownerRows = liveBlocks.filter(isOwnerFacingPresentationRow);
  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });

  const pvql = await evaluateBrandPublicVisibility(slug);
  const tabFactory = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerRows,
    html,
    brandSlug: slug,
  });
  const uniqueness = evaluateImageUniqueness({ brand, presentationRows: ownerRows, brandSlug: slug });
  const roleMatch = evaluateBrandImageRoleMatch({ presentationRows: ownerRows, brandSlug: slug });
  const forbidden = scanOwnerFacingForbiddenLanguage(ownerRows);
  const scenarioRoles = auditScenarioImageRoles(ownerRows);
  const tabFindings = auditTabSections({
    brandName: brand.name,
    slug,
    ownerRows,
    brand: {
      parentCompany: brand.parentCompany || brand.parent || brand.parentBrand || "",
      name: brand.name,
    },
  });

  for (const h of forbidden) {
    tabFindings.push(
      finding({
        tab: "Owner-facing copy",
        section: h.slotKey || "unknown",
        status: "forbidden",
        finding: `Forbidden owner-facing language: ${h.id || h.label}`,
        severity: "blocker",
        proposedFix: "Scrub flagged language from Presentation Body/Title",
        recordId: h.recordId,
        slotKey: h.slotKey,
        field: "Body",
      })
    );
  }

  const imageFindings = buildImageFindings({
    brandSlug: slug,
    uniqueness,
    roleMatch,
    scenarioRoleFindings: scenarioRoles.findings,
  });

  const scores = scoreBrand({
    tabFindings,
    imageFindings,
    tabFactory,
    pvql,
    emptyPass: tabFactory.emptyScan?.pass === true,
    html,
  });

  // Collect image inventory for cross-brand reuse
  const imageInventory = [];
  for (const section of ["gallery", "scenario", "property_example"]) {
    const list =
      section === "gallery"
        ? uniqueness.gallery || []
        : section === "scenario"
          ? uniqueness.scenarios || []
          : uniqueness.properties || [];
    for (const img of list) {
      if (!img.imageUrl) continue;
      imageInventory.push({
        brandSlug: slug,
        brandName: brand.name,
        section,
        slotKey: img.slotKey,
        title: img.title,
        imageUrl: img.imageUrl,
        duplicateGroupId: img.duplicateGroupId,
        recordId: img.recordId,
      });
    }
  }

  return {
    brand: brand.name,
    slug,
    recordId: brand.id || recordId,
    brandStatus: universeRow?.status || brand.status || null,
    publicDisplayState: brand.brandExplorerDisplayState || pvql.publicDisplayState || null,
    shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
    pvqlStatus: pvql.lockPass ? "pass" : "fail",
    pvqlFailures: pvql.failures || [],
    overallRecommendation: scores.recommendation,
    scores,
    tabFindings,
    imageFindings,
    gates: {
      tabFactoryAuditPass: tabFactory.auditPass === true,
      emptyPass: tabFactory.emptyScan?.pass === true,
      sectionPatternParity: tabFactory.gates?.section_pattern_parity === true,
      golden: tabFactory.gates?.golden_content_quality === true,
      imageUniqueness: uniqueness.pass === true,
      imageRoleMatch: roleMatch.pass === true,
      scenarioDistinct: uniqueness.scenarioDistinctCount,
      galleryDistinct: uniqueness.galleryDistinctCount,
      propertyDistinct: uniqueness.propertyExampleDistinctCount,
    },
    scenarioRoles: scenarioRoles.roles,
    imageInventory,
    ownerFacingRowCount: ownerRows.length,
    writePerformed: false,
  };
}

function buildCrossBrandImageIssues(brandResults) {
  const byGroup = new Map();
  for (const b of brandResults) {
    for (const img of b.imageInventory || []) {
      const key = img.duplicateGroupId || img.imageUrl;
      if (!key) continue;
      if (!byGroup.has(key)) byGroup.set(key, []);
      byGroup.get(key).push(img);
    }
  }
  const issues = [];
  for (const [groupId, rows] of byGroup) {
    const brands = [...new Set(rows.map((r) => r.brandSlug))];
    if (brands.length < 2) continue;
    // Same image across brands — flag unless clearly sibling soft-brand share (still report)
    issues.push({
      duplicateGroupId: groupId,
      brands,
      imageUrl: rows[0].imageUrl,
      occurrences: rows.map((r) => ({
        brandSlug: r.brandSlug,
        section: r.section,
        slotKey: r.slotKey,
        title: r.title,
        recordId: r.recordId,
      })),
      issueType: "cross_brand_reuse",
      severity: "minor",
      finding: `Same image group used across brands: ${brands.join(", ")}`,
      proposedFix:
        "Confirm shared hotel/parent context is intentional; otherwise rematerialize brand-specific imagery",
    });
  }
  return issues;
}

function brandMarkdown(b) {
  const lines = [];
  lines.push(`# Brand Explorer quality audit — ${b.brand}`);
  lines.push("");
  lines.push(`Brand: **${b.brand}**`);
  lines.push(`Slug: \`${b.slug}\``);
  lines.push(`Record ID: \`${b.recordId}\``);
  lines.push(`Brand Status: ${b.brandStatus || "—"}`);
  lines.push(`Public Display State: ${b.publicDisplayState || "—"}`);
  lines.push(`shouldRenderFullProfile: ${b.shouldRenderFullProfile}`);
  lines.push(`PVQL Status: **${b.pvqlStatus}**`);
  lines.push(`Overall Recommendation: **${b.overallRecommendation}**`);
  lines.push("");
  lines.push("## Scores");
  lines.push("");
  lines.push(`| Dimension | Score |`);
  lines.push(`|-----------|------:|`);
  lines.push(`| Population completeness | ${b.scores.populationCompleteness} |`);
  lines.push(`| Consistency | ${b.scores.consistency} |`);
  lines.push(`| Brand specificity | ${b.scores.brandSpecificity} |`);
  lines.push(`| Owner usefulness | ${b.scores.ownerUsefulness} |`);
  lines.push(`| Visual quality | ${b.scores.visualQuality} |`);
  lines.push(`| Scenario image distinctiveness | ${b.scores.scenarioImageDistinctiveness} |`);
  lines.push(`| **Composite** | **${b.scores.composite}** |`);
  lines.push("");
  lines.push("## Tab-by-tab");
  lines.push("");
  lines.push("| Tab | Section | Status | Finding | Severity | Proposed Fix |");
  lines.push("|-----|---------|--------|---------|----------|--------------|");
  for (const f of b.tabFindings) {
    if (f.status === "pass") continue;
    lines.push(
      `| ${f.tab} | ${f.section} | ${f.status} | ${String(f.finding).replace(/\|/g, "/")} | ${f.severity} | ${String(f.proposedFix || "").replace(/\|/g, "/")} |`
    );
  }
  if (!b.tabFindings.some((f) => f.status !== "pass")) {
    lines.push("| — | — | pass | No non-pass findings | — | — |");
  }
  lines.push("");
  lines.push("## Image repetition");
  lines.push("");
  lines.push("| Section | Card | Image | Finding | Severity | Proposed Fix |");
  lines.push("|---------|------|-------|---------|----------|--------------|");
  if (!b.imageFindings.length) {
    lines.push("| — | — | — | No image issues | — | — |");
  } else {
    for (const f of b.imageFindings) {
      lines.push(
        `| ${f.section} | ${f.card} | ${(f.image || "—").slice(0, 80)} | ${String(f.finding).replace(/\|/g, "/")} | ${f.severity} | ${String(f.proposedFix || "").replace(/\|/g, "/")} |`
      );
    }
  }
  lines.push("");
  lines.push(`_Audit ${AUDIT_VERSION} · writePerformed=${b.writePerformed}_`);
  lines.push("");
  return lines.join("\n");
}

function masterMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer 24-brand tab/section quality audit`);
  lines.push("");
  lines.push(`Version: \`${report.version}\` · Generated: ${report.generatedAt}`);
  lines.push(`Mode: **dry-run / audit-only** (no Airtable writes)`);
  lines.push("");
  lines.push("## Universe");
  lines.push("");
  lines.push(`- Source: ${report.activeUniverseSource.name}`);
  lines.push(`- Active count: **${report.activeCount}**`);
  lines.push(`- Audited: **${report.auditedCount}**`);
  lines.push("");
  lines.push("## Baseline freeze readiness");
  lines.push("");
  lines.push(`**${report.baselineFreezeDecision}**`);
  lines.push("");
  lines.push(report.baselineFreezeRationale);
  lines.push("");
  lines.push("## Recommendation counts");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(report.recommendationCounts, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Per-brand summary");
  lines.push("");
  lines.push("| Brand | Slug | Composite | Recommendation | Blockers | Image issues |");
  lines.push("|-------|------|----------:|----------------|---------:|-------------:|");
  for (const b of report.brandResults) {
    lines.push(
      `| ${b.brand} | ${b.slug} | ${b.scores.composite} | ${b.overallRecommendation} | ${b.scores.blockerCount} | ${b.imageFindings.length} |`
    );
  }
  lines.push("");
  lines.push("## Excluded status conflicts");
  lines.push("");
  for (const x of report.excludedFromUniverse) {
    lines.push(`- \`${x.slug}\` — ${x.reason}`);
  }
  lines.push("");
  lines.push("## Cross-brand image reuse");
  lines.push("");
  lines.push(`Flagged groups: **${report.crossBrandImageIssues.length}** (see image repetition report)`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(report.guardrails, null, 2));
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}

function imageMasterMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer 24-brand image repetition audit`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push("");
  lines.push("## Within-brand issues");
  lines.push("");
  lines.push("| Brand | Tab/Section | Card | Issue Type | Severity | Proposed Fix |");
  lines.push("|-------|-------------|------|------------|----------|--------------|");
  let any = false;
  for (const b of report.brandResults) {
    for (const f of b.imageFindings) {
      any = true;
      lines.push(
        `| ${b.slug} | ${f.section} | ${String(f.card || "").replace(/\|/g, "/")} | ${f.issueType || f.finding} | ${f.severity} | ${String(f.proposedFix || "").replace(/\|/g, "/")} |`
      );
    }
  }
  if (!any) lines.push("| — | — | — | none | — | — |");
  lines.push("");
  lines.push("## Cross-brand reuse");
  lines.push("");
  lines.push("| Brands | Issue | Image | Severity | Proposed Fix |");
  lines.push("|--------|-------|-------|----------|--------------|");
  if (!report.crossBrandImageIssues.length) {
    lines.push("| — | none | — | — | — |");
  } else {
    for (const i of report.crossBrandImageIssues) {
      lines.push(
        `| ${i.brands.join(", ")} | ${i.issueType} | ${(i.imageUrl || "").slice(0, 70)} | ${i.severity} | ${i.proposedFix} |`
      );
    }
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * Run full 24-brand audit (read-only).
 */
export async function run24TabSectionQualityAudit({ dryRun = true, brands = null } = {}) {
  if (!dryRun) {
    throw new Error("This audit refuses --apply. Use --dry-run only (no writes).");
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  let slugs = await listActiveUniverseSlugs();
  if (Array.isArray(brands) && brands.length) {
    const allow = new Set(brands);
    slugs = slugs.filter((s) => allow.has(s));
  }
  const bySlug = new Map((universe.brands || []).map((b) => [b.slug, b]));

  const brandResults = [];
  for (const slug of slugs) {
    process.stdout.write(`[audit] ${slug}...\n`);
    const row = await auditBrandTabSectionQuality(slug, { universeRow: bySlug.get(slug) });
    brandResults.push(row);
  }

  const crossBrandImageIssues = buildCrossBrandImageIssues(brandResults);
  const recommendationCounts = brandResults.reduce((acc, b) => {
    acc[b.overallRecommendation] = (acc[b.overallRecommendation] || 0) + 1;
    return acc;
  }, {});

  const needsRemediation = (recommendationCounts.remediation_required || 0) > 0;
  const needsMinor = (recommendationCounts.approve_after_minor_cleanup || 0) > 0;
  const allApprove =
    brandResults.length > 0 &&
    brandResults.every((b) => b.overallRecommendation === "approve_for_baseline_freeze");
  const activeCount = universe.totalCount ?? slugs.length;
  let baselineFreezeDecision = "ready_to_freeze_24_brand_baseline";
  let baselineFreezeRationale =
    "All audited brands recommend approve_for_baseline_freeze with no blockers.";
  if (needsRemediation) {
    baselineFreezeDecision = "do_not_freeze_remediation_required";
    baselineFreezeRationale = `${recommendationCounts.remediation_required} brand(s) require remediation before baseline freeze.`;
  } else if (needsMinor) {
    baselineFreezeDecision = "freeze_after_minor_cleanup_pass";
    baselineFreezeRationale = `${recommendationCounts.approve_after_minor_cleanup} brand(s) need minor cleanup before freeze; none require full remediation.`;
  } else if (allApprove && activeCount >= 45) {
    baselineFreezeDecision = "ready_to_freeze_45_active_public_full_baseline";
    baselineFreezeRationale = `All ${activeCount} Active/Live brands recommend approve_for_baseline_freeze.`;
  } else if (allApprove && activeCount >= 39) {
    baselineFreezeDecision = "ready_to_freeze_39_active_public_full_baseline";
    baselineFreezeRationale = `All ${activeCount} Active/Live brands recommend approve_for_baseline_freeze (legacy 39 threshold).`;
  }

  const report = {
    version: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    activeUniverseVersion: ACTIVE_UNIVERSE_VERSION,
    activeCount: universe.totalCount ?? slugs.length,
    auditedCount: brandResults.length,
    recommendationCounts,
    baselineFreezeDecision,
    baselineFreezeRationale,
    brandResults,
    crossBrandImageIssues,
    excludedFromUniverse: NON_ACTIVE_STATUS_CONFLICT_PROBES.map((p) => ({
      slug: p.slug,
      reason: "Not Active/Live — excluded from this audit",
    })),
    guardrails: {
      noAirtableWrites: true,
      noCompanyValidatedWrites: true,
      noSourceLibraryWrites: true,
      noRegistryWrites: true,
      noBrandStatusWrites: true,
      noReleaseFieldWrites: true,
      stale23NotUsedAsUniverse: true,
    },
  };

  return report;
}

export function write24TabSectionQualityReports(report, reportsDir = REPORTS_DIR) {
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(reportsDir, "brand-explorer-24-tab-section-quality-audit.json");
  const mdPath = path.join(reportsDir, "brand-explorer-24-tab-section-quality-audit.md");
  const imgJsonPath = path.join(reportsDir, "brand-explorer-24-image-repetition-audit.json");
  const imgMdPath = path.join(reportsDir, "brand-explorer-24-image-repetition-audit.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-24-tab-section-quality-audit.md");

  // Slim brand results for master JSON (drop bulky inventory in master; keep in per-brand)
  const master = {
    ...report,
    brandResults: report.brandResults.map((b) => ({
      brand: b.brand,
      slug: b.slug,
      recordId: b.recordId,
      brandStatus: b.brandStatus,
      publicDisplayState: b.publicDisplayState,
      shouldRenderFullProfile: b.shouldRenderFullProfile,
      pvqlStatus: b.pvqlStatus,
      overallRecommendation: b.overallRecommendation,
      scores: b.scores,
      gates: b.gates,
      tabFindingCount: b.tabFindings.filter((f) => f.status !== "pass").length,
      imageFindingCount: b.imageFindings.length,
      tabFindings: b.tabFindings.filter((f) => f.status !== "pass"),
      imageFindings: b.imageFindings,
    })),
  };

  fs.writeFileSync(jsonPath, JSON.stringify(master, null, 2), "utf8");
  fs.writeFileSync(mdPath, masterMarkdown(report), "utf8");

  const imageReport = {
    version: AUDIT_VERSION,
    generatedAt: report.generatedAt,
    dryRun: true,
    writePerformed: false,
    brandResults: report.brandResults.map((b) => ({
      slug: b.slug,
      brand: b.brand,
      imageFindings: b.imageFindings,
      scenarioRoles: b.scenarioRoles,
      gates: {
        galleryDistinct: b.gates.galleryDistinct,
        scenarioDistinct: b.gates.scenarioDistinct,
        propertyDistinct: b.gates.propertyDistinct,
        imageUniqueness: b.gates.imageUniqueness,
        imageRoleMatch: b.gates.imageRoleMatch,
      },
    })),
    crossBrandImageIssues: report.crossBrandImageIssues,
  };
  fs.writeFileSync(imgJsonPath, JSON.stringify(imageReport, null, 2), "utf8");
  fs.writeFileSync(imgMdPath, imageMasterMarkdown(report), "utf8");

  const perBrandPaths = [];
  for (const b of report.brandResults) {
    const p = path.join(reportsDir, `brand-explorer-quality-audit-${b.slug}.md`);
    fs.writeFileSync(p, brandMarkdown(b), "utf8");
    perBrandPaths.push(p);
  }

  const docs = `${masterMarkdown(report)}\n\n## How to re-run\n\n\`\`\`bash\nnpm run brand-explorer-24-tab-section-quality-audit -- --dry-run\n\`\`\`\n\nAudit-only. No Airtable writes.\n`;
  fs.writeFileSync(docsPath, docs, "utf8");

  return { jsonPath, mdPath, imgJsonPath, imgMdPath, docsPath, perBrandPaths };
}
