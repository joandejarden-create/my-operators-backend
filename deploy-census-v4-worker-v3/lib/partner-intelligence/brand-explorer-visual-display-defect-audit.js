/**
 * Brand Explorer Visual Display Defect Audit v24.
 *
 * Read-only audit of rendered/displayed Brand Explorer content quality.
 * Compares Tribute Portfolio against completed reference brands (Curio primary).
 * No Airtable writes, no image changes, no Company Validated changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import {
  evaluatePortfolioContextGate,
  isChoiceParent,
  isHiltonParent,
  isIhgParent,
  isMarriottParent,
  readAtelierFrontendSource,
} from "./brand-explorer-portfolio-ladder-mapping.js";
import {
  resolveBrandTarget as resolveBrandTargetV28C,
  getBrandTargetResolverContext,
} from "./brand-explorer-brand-target-resolver.js";
import {
  assessPresentationRowImageGovernance,
  detectBrandAssetImageGovernanceDefects,
  findRegistryAssetForPresentationRow,
  getDiscoveryBrandConfig,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  assessOpeningsRowQuarantine,
  detectOpeningsUiQuarantineDefects,
  isOpeningsEvidenceSlot,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import { listRegistryAssetsForBrand, MAP_BRAND_ASSET } from "./brand-asset-registry-workflow.js";
import { isRegistryAssetApprovedForExplorer } from "./brand-explorer-brand-asset-image-governance.js";
import {
  CARRYOVER_LOGIC_VERSION,
  detectBlockingCarryoverFindings,
  resolveParentFamily,
  scanParentAwareCarryover,
} from "./brand-explorer-parent-aware-carryover.js";

export const AUDIT_VERSION = "24";
export const CARRYOVER_AUDIT_VERSION = CARRYOVER_LOGIC_VERSION;
export const REPORT_JSON_NAME = "brand-explorer-visual-display-defect-audit.json";
export const REPORT_MD_NAME = "brand-explorer-visual-display-defect-audit.md";
export const DOC_MD_NAME = "brand-explorer-visual-display-defect-audit-v24.md";

const CURIO_BRAND_ID = "receQkxgjlezsc1xg";
const DEFAULT_BRAND_ID = TRIBUTE_RECORD_ID;

const PRIMARY_REFERENCE = "Curio Collection by Hilton";
const REFERENCE_BRANDS = [
  "Kimpton Hotels",
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Ascend Hotel Collection",
];

const GENERIC_LADDER_FALLBACKS = new Set([
  "lower-scale brands",
  "mid-scale brands",
  "upscale brands",
  "upper-scale brands",
]);

const PLACEHOLDER_PATTERNS = [
  /\b(tbd|coming soon|lorem ipsum|placeholder|not available yet|insert copy)\b/i,
  /profile analysis/i,
  /no owner planning checklist is published/i,
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function toText(v) {
  if (Array.isArray(v)) return v.filter(hasVal).map(String).join(", ");
  return hasVal(v) ? nz(v) : "";
}

function wordCount(text) {
  return toText(text).split(/\s+/).filter(Boolean).length;
}

function short(text, max = 180) {
  const s = toText(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

function splitBullets(val) {
  if (!hasVal(val)) return [];
  if (Array.isArray(val)) return val.map(String).filter(Boolean);
  return String(val)
    .split(/\n|;|•/g)
    .map((s) => s.replace(/^\s*[-*]\s*/, "").trim())
    .filter(Boolean);
}

function resolveActiveRegistryBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  const bySlug = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === normalized);
  if (bySlug) return bySlug.recordId;
  const byId = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.recordId === raw);
  if (byId) return byId.recordId;
  return null;
}

async function resolveVisualAuditBrandRecordId(raw) {
  const input = nz(raw);
  if (!input || input.toLowerCase() === "tribute-portfolio" || input.toLowerCase() === "tribute portfolio") {
    return DEFAULT_BRAND_ID;
  }
  const activeId = resolveActiveRegistryBrandInput(input);
  if (activeId) return activeId;
  const ctx = await getBrandTargetResolverContext();
  const resolved = await resolveBrandTargetV28C(input, ctx);
  return resolved.recordId || resolved.resolution?.resolvedRecordId || input;
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

function firstBlock(brand, slotKey) {
  return blocksForSlot(brand, slotKey)[0] || null;
}

function mergedBody(brand, slotKey) {
  return blocksForSlot(brand, slotKey)
    .map((b) => [nz(b.title), nz(b.body)].filter(hasVal).join(": "))
    .filter(hasVal)
    .join("\n\n");
}

function parseRequirementBody(raw) {
  const out = { typical: "", owner: "", status: "", notesToConfirm: "" };
  if (!hasVal(raw)) return out;
  for (const line of String(raw).split(/\n/)) {
    const t = line.trim();
    if (!t) continue;
    if (/^Typical consideration:/i.test(t)) out.typical = t.replace(/^Typical consideration:\s*/i, "").trim();
    else if (/^Owner planning consideration:/i.test(t) || /^Owner planning:/i.test(t)) {
      out.owner = t.replace(/^Owner planning( consideration)?:\s*/i, "").trim();
    } else if (/^Typical status:/i.test(t)) out.status = t.replace(/^Typical status:\s*/i, "").trim();
    else if (/^Notes to confirm:/i.test(t)) out.notesToConfirm = t.replace(/^Notes to confirm:\s*/i, "").trim();
  }
  return out;
}

function isGenericPlaceholder(text) {
  const s = toText(text);
  if (!s) return true;
  return PLACEHOLDER_PATTERNS.some((p) => p.test(s));
}

function ladderTierIndexFromRaw(raw) {
  if (!hasVal(raw)) return null;
  const text = nz(raw);
  const firstToken = text.split(/\s+/)[0];
  const n = parseInt(firstToken, 10);
  if (!Number.isNaN(n) && n >= 0 && n <= 3) return n;
  const key = text.toLowerCase();
  if (key === "upper_upscale" || key === "luxury" || key === "tier3") return 3;
  if (key === "upscale" || key === "premium" || key === "tier2") return 2;
  if (key === "upper_mid" || key === "midscale" || key === "tier1") return 1;
  if (key === "economy" || key === "tier0") return 0;
  return null;
}

function portfolioLadderTierIndex(brand) {
  if (typeof brand?.portfolioLadderTier === "number" && brand.portfolioLadderTier >= 0 && brand.portfolioLadderTier <= 3) {
    return brand.portfolioLadderTier;
  }
  const ctxRow = firstBlock(brand, "overview.portfolio_context");
  if (ctxRow) {
    let tierRaw = hasVal(ctxRow.title) ? nz(ctxRow.title) : "";
    if (!tierRaw && hasVal(ctxRow.body)) {
      const firstLine = nz(ctxRow.body).split(/\n+/)[0];
      if (/^\d$/.test(firstLine)) tierRaw = firstLine;
    }
    const fromCtx = ladderTierIndexFromRaw(tierRaw);
    if (fromCtx != null) return fromCtx;
  }
  return null;
}

function reconstructPortfolioContext(brand) {
  const ctxRow = firstBlock(brand, "overview.portfolio_context");
  const tierIdx = portfolioLadderTierIndex(brand);
  const parent = nz(brand?.parentCompany);
  const isHilton = isHiltonParent(parent);
  const isMarriott = isMarriottParent(parent);
  const isChoice = isChoiceParent(parent);
  const isIhg = isIhgParent(parent);
  const ctxBody = nz(ctxRow?.body);
  const ctxTitle = nz(ctxRow?.title);
  let frontendSource = "";
  try {
    frontendSource = readAtelierFrontendSource();
  } catch {
    frontendSource = "";
  }
  const gate = evaluatePortfolioContextGate(brand, frontendSource);
  const usesGenericFallback =
    !isHilton &&
    !isChoice &&
    !isIhg &&
    !isMarriott &&
    (!hasVal(ctxBody) || GENERIC_LADDER_FALLBACKS.has(ctxBody.toLowerCase().split("\n")[0]));
  return {
    tierIndex: tierIdx,
    slotKey: "overview.portfolio_context",
    title: ctxTitle,
    body: ctxBody,
    parentCompany: parent,
    usesParentStaticLadder: gate.usesParentStaticLadder,
    usesGenericScaleLabels: gate.usesGenericScaleLabels || usesGenericFallback,
    marriottLadderMappingReady: gate.marriottPortfolioReady,
    ihgLadderMappingReady: gate.ihgPortfolioReady,
    tributeHighlightedInLadder: gate.tributeHighlightedInLadder,
    narrativeRenders: gate.narrativeRenders,
    parentPortfolioReady: gate.parentPortfolioReady,
    portfolioContextReady: gate.portfolioContextReady,
    referencePattern: isHilton
      ? "Hilton static portfolio ladder with sibling brand names per tier"
      : isIhg
        ? "IHG owner-planning ladder with luxury/lifestyle sibling labels (InterContinental, Regent, Kimpton, etc.)"
        : isMarriott
          ? "Marriott owner-planning ladder with soft-collection sibling labels (Autograph Collection, Design Hotels, etc.)"
          : "Parent-company ladder with actual sibling flags, not generic scale labels",
  };
}

function reconstructScenarioCards(brand, prefix, count, defaultTitles = []) {
  const cards = [];
  for (let i = 0; i < count; i++) {
    const slotKey = `${prefix}.${i + 1}`;
    const row = firstBlock(brand, slotKey);
    const title = hasVal(row?.title) ? nz(row.title) : defaultTitles[i] || `Scenario ${i + 1}`;
    const body = hasVal(row?.body) ? nz(row.body) : "";
    const imageUrl = hasVal(row?.imageUrl) ? nz(row.imageUrl) : "";
    cards.push({ slotKey, title, body, imageUrl, wordCount: wordCount(body), hasImage: hasVal(imageUrl) });
  }
  return cards;
}

function reconstructBulletList(brand, slotKey, fallbackField, padTo = 5) {
  const merged = mergedBody(brand, slotKey);
  let bullets = hasVal(merged) ? splitBullets(merged) : splitBullets(brand?.[fallbackField]);
  const padded = [...bullets];
  while (padded.length < padTo) padded.push("");
  return {
    slotKey,
    bullets: padded.slice(0, padTo),
    filledCount: bullets.filter(hasVal).length,
    emptyIndices: padded
      .slice(0, padTo)
      .map((b, i) => (!hasVal(b) ? i + 1 : null))
      .filter((x) => x != null),
    source: hasVal(merged) ? "presentation_slot" : hasVal(brand?.[fallbackField]) ? "brand_basics_fallback" : "none",
  };
}

function reconstructBestAtCards(brand) {
  const defaultTitles = ["Conversion & Repositioning", "Blended-Demand Markets", "Owner Speed-to-Flag"];
  return defaultTitles.map((fallbackTitle, i) => {
    const slotKey = `overview.bestAt.${i + 1}`;
    const row = firstBlock(brand, slotKey);
    const title = hasVal(row?.title) ? nz(row.title) : fallbackTitle;
    const body = mergedBody(brand, slotKey) || (splitBullets(brand?.brandPillars)[i] || "");
    return { slotKey, title, body, wordCount: wordCount(body) };
  });
}

function reconstructStandardsTable(brand) {
  const rows = blocksForSlot(brand, "standards.requirement");
  const parsed = rows.map((r) => {
    const area = hasVal(r.title) ? nz(r.title) : "Requirement";
    const p = parseRequirementBody(r.body);
    const hasStructured = hasVal(p.typical) || hasVal(p.owner) || hasVal(p.status);
    return {
      slotKey: "standards.requirement",
      area,
      body: nz(r.body),
      hasStructured,
      sort: r.sort ?? null,
    };
  });
  return {
    rowCount: rows.length,
    structuredRowCount: parsed.filter((r) => r.hasStructured).length,
    rows: parsed,
    hasCompletedBrandTable: rows.length >= 4 && parsed.filter((r) => r.hasStructured).length >= 3,
  };
}

function reconstructFeaturedApplication(brand) {
  const slotRow = firstBlock(brand, "overview.featured_application");
  const slotTitle = nz(slotRow?.title);
  const slotBody = nz(slotRow?.body);
  const lead = slotTitle || nz(brand?.brandTaglineMotto);
  const body = slotBody || nz(brand?.brandPositioning) || nz(brand?.brandCustomerPromise);
  const displayed = hasVal(lead) || hasVal(body);
  const truncatedBody = !slotBody && hasVal(body) && body.length > 220;
  return {
    displayPath: "overview.featured_application",
    sourceFields: slotBody
      ? ["overview.featured_application"]
      : ["brandTaglineMotto", "brandPositioning", "brandCustomerPromise"],
    lead,
    body,
    displayed,
    truncatedInUi: truncatedBody,
    wordCount: wordCount(body),
    tagChips: splitBullets(brand?.brandPillars || brand?.keyBrandDifferentiators).slice(0, 6),
  };
}

function reconstructMaterials(brand) {
  const gallery = Array.from({ length: 6 }, (_, i) => {
    const slotKey = `materials.gallery.${i + 1}`;
    const row = firstBlock(brand, slotKey);
    const visibleInApi = Boolean(row);
    return {
      slotKey,
      title: nz(row?.title),
      hasImage: hasVal(row?.imageUrl),
      imageUrl: nz(row?.imageUrl),
      visibleInApi,
      deferredHidden: !visibleInApi,
    };
  });
  const files = blocksForSlot(brand, "materials.file").map((r) => ({
    slotKey: "materials.file",
    title: nz(r.title),
    body: nz(r.body),
    hasUrl: /https?:\/\//i.test(nz(r.body) + nz(r.summaryUrl)),
  }));
  const caseStudy = firstBlock(brand, "materials.caseStudy");
  return { gallery, files, caseStudy: caseStudy ? { title: nz(caseStudy.title), body: nz(caseStudy.body) } : null };
}

function reconstructInsight(brand) {
  return {
    slotKey: "insight.summary",
    body: mergedBody(brand, "insight.summary"),
    loadWarnings: Array.isArray(brand?.loadWarnings) ? brand.loadWarnings : [],
    wordCount: wordCount(mergedBody(brand, "insight.summary")),
  };
}

function reconstructVisibleModel(brand) {
  return {
    brandId: brand?.id || brand?.brandId,
    brandName: nz(brand?.name),
    blockCount: brand?.brandExplorer?.blocks?.length || 0,
    sections: {
      valueScenarios: {
        label: "Where This Brand Creates the Most Value",
        tab: "Overview",
        cards: reconstructScenarioCards(brand, "overview.scenario", 3, [
          "Urban Repositioning",
          "Leisure-Forward Conversions",
          "Boutique Resort Adjacency",
        ]),
      },
      whyValueStrongest: {
        label: "Why Value Is Strongest in These Scenarios",
        tab: "Overview",
        ...reconstructBulletList(brand, "overview.why_value", "brandValueProposition", 5),
      },
      keyDifferentiators: {
        label: "Key Differentiators",
        tab: "Overview",
        identity: reconstructBulletList(brand, "overview.differentiators.identity", "keyBrandDifferentiators", 4),
        commercial: reconstructBulletList(brand, "overview.differentiators.commercial", "keyBrandDifferentiators", 4),
      },
      bestAt: {
        label: "What This Brand Is Best At",
        tab: "Overview",
        cards: reconstructBestAtCards(brand),
      },
      ownerValueSnapshot: {
        label: "Owner Value Snapshot",
        tab: "Overview",
        outcomes: splitBullets(brand?.brandValueProposition).slice(0, 4),
        experience: splitBullets(mergedBody(brand, "overview.owner_experience") || brand?.companyHistory).slice(0, 4),
      },
      featuredApplication: {
        label: "Featured Application / Conversion Example",
        tab: "Overview",
        ...reconstructFeaturedApplication(brand),
      },
      portfolioContext: {
        label: "Portfolio Context",
        tab: "Overview",
        ...reconstructPortfolioContext(brand),
      },
      valueCreationScenarios: {
        label: "Value Creation Scenarios",
        tab: "Value to Owners",
        cards: reconstructScenarioCards(brand, "valueOwners.scenario", 4, [
          "Independent Reflag",
          "Tired Upscale Asset",
          "Markets With Strong Brand Presence",
          "Third-Party Operator–Led",
        ]),
      },
      ownershipProfiles: {
        label: "Ownership Profiles That Benefit Most",
        tab: "Value to Owners",
        staticPills: true,
        note: "Static UI pills — not presentation-driven",
      },
      lifecycleSupport: {
        label: "Support Across the Lifecycle",
        tab: "Value to Owners",
        phases: reconstructScenarioCards(brand, "valueOwners.lifecycle", 6),
      },
      keyWatchouts: {
        label: "Key Watchouts",
        tab: "Value to Owners",
        ...reconstructBulletList(brand, "valueOwners.watchouts", "keyBrandDifferentiators", 5),
      },
      standardsDetail: {
        label: "Standard Detail, Where Available",
        tab: "Owner Considerations",
        table: reconstructStandardsTable(brand),
        intro: mergedBody(brand, "standards.intro"),
        lastReviewed: mergedBody(brand, "standards.last_reviewed"),
      },
      conversionTransitions: {
        label: "Conversion & Sister-Flag Transitions",
        tab: "Owner Considerations",
        body: mergedBody(brand, "standards.conversion"),
        wordCount: wordCount(mergedBody(brand, "standards.conversion")),
      },
      brandMaterials: {
        label: "Brand Materials",
        tab: "Brand Materials",
        ...reconstructMaterials(brand),
      },
      dealalityInsight: {
        label: "Dealality Insight",
        tab: "Dealality Insight",
        ...reconstructInsight(brand),
      },
    },
  };
}

function referenceDepthStats(referenceModels, sectionKey, path) {
  const depths = [];
  for (const m of referenceModels) {
    const section = m.sections[sectionKey];
    if (!section) continue;
    if (path === "cards" && Array.isArray(section.cards)) {
      for (const c of section.cards) depths.push(c.wordCount || wordCount(c.body));
    } else if (path === "bullets" && section.filledCount != null) {
      depths.push(section.filledCount);
    } else if (path === "table" && section.table) {
      depths.push(section.table.rowCount);
    } else if (path === "body" && section.wordCount != null) {
      depths.push(section.wordCount);
    }
  }
  const avg = depths.length ? depths.reduce((a, b) => a + b, 0) / depths.length : 0;
  return { avg, samples: depths.length };
}

function addDefect(defects, defect) {
  defects.push({
    ...defect,
    safeForFutureWriter: defect.safeForFutureWriter ?? false,
  });
}

function resolveAuditBrandTarget(brandId, brandApi) {
  const slugFromName = slugifyAuditName(brandApi?.name);
  const discovery = getDiscoveryBrandConfig(slugFromName);
  if (discovery) {
    return {
      slug: discovery.slug,
      recordId: brandId,
      name: discovery.name || nz(brandApi?.name),
      resolution: { resolutionSource: "expansion_backlog" },
    };
  }
  return (
    ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.recordId === brandId) ||
    ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === nz(brandId).toLowerCase()) || {
      slug: slugFromName,
      recordId: brandId,
      name: nz(brandApi?.name) || BRAND_NAME,
      resolution: { resolutionSource: "live_lookup" },
    }
  );
}

function slugifyAuditName(name) {
  return nz(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function detectWrongBrandCopyDefects(brandApi, brandTarget, defects) {
  const targetFamily = resolveParentFamily(brandApi, brandTarget);
  const blocking = detectBlockingCarryoverFindings(brandApi, brandTarget);
  const allFindings = scanParentAwareCarryover(brandApi, brandTarget);
  const allowedCount = allFindings.filter((f) =>
    ["allowed_parent_reference", "sibling_context_allowed"].includes(f.classification)
  ).length;

  for (const finding of blocking) {
    addDefect(defects, {
      section: "Cross-section",
      slotKey: finding.slotKey || "(multiple)",
      displayPath: finding.surface || "copy_scan",
      currentTributeValue: finding.excerpt,
      referencePattern: `${finding.message} (target family: ${targetFamily})`,
      defectType: "wrong_brand_copy",
      severity: finding.severity || "critical",
      carryoverClassification: finding.classification,
      markerId: finding.markerId,
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Remove or replace cross-brand contaminated copy with brand-appropriate language.",
      safeForFutureWriter: true,
    });
  }

  return { targetFamily, blockingCount: blocking.length, allowedCount };
}

function detectDefects(tributeModel, curioModel, referenceModels, sortOrderContext, brandApi = null, brandTarget = null) {
  const defects = [];
  const tribute = tributeModel.sections;
  const curio = curioModel?.sections || {};

  // 1. overview.scenario.3 missing image
  const scen3 = tribute.valueScenarios.cards[2];
  const curioScen3 = curio.valueScenarios?.cards?.[2];
  if (scen3 && !scen3.hasImage) {
    addDefect(defects, {
      section: tribute.valueScenarios.label,
      slotKey: "overview.scenario.3",
      displayPath: "overview.scenario.3.imageUrl",
      currentTributeValue: short(scen3.body) || "(title only)",
      referencePattern: curioScen3?.hasImage
        ? "Curio scenario.3 has image attachment promoted to imageUrl"
        : "Completed brands attach scenario image on overview.scenario.N rows",
      defectType: "missing_card_image",
      severity: "high",
      fixRequiresCopy: false,
      fixRequiresImage: true,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Add approved scenario image to overview.scenario.3 presentation row when asset exists in Brand Asset Registry.",
      safeForFutureWriter: false,
      verifiedKnownIssue: true,
    });
  }
  if (scen3 && !scen3.hasImage) {
    addDefect(defects, {
      section: tribute.valueScenarios.label,
      slotKey: "overview.scenario.3",
      displayPath: "overview.scenario.3.visual",
      currentTributeValue: "blank image placeholder (Image)",
      referencePattern: "Curio shows scenario-card__visual with image",
      defectType: "blank_image_placeholder",
      severity: "high",
      fixRequiresCopy: false,
      fixRequiresImage: true,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Promote registry-approved asset or leave blank until asset approved — do not invent image.",
      safeForFutureWriter: false,
      verifiedKnownIssue: true,
    });
  }

  // Scenario cards thin/empty bodies
  for (const card of tribute.valueScenarios.cards) {
    if (!hasVal(card.body)) {
      addDefect(defects, {
        section: tribute.valueScenarios.label,
        slotKey: card.slotKey,
        displayPath: `${card.slotKey}.body`,
        currentTributeValue: card.title,
        referencePattern: "Curio scenario cards have title + substantive body copy",
        defectType: "title_only_card",
        severity: card.slotKey === "overview.scenario.3" ? "high" : "medium",
        fixRequiresCopy: true,
        fixRequiresImage: false,
        fixRequiresSourceEvidence: true,
        fixRequiresDisplayMapping: false,
        proposedCorrection: "Add source-backed scenario body copy via presentation row Body field.",
        safeForFutureWriter: true,
      });
    } else if (card.wordCount < 12) {
      addDefect(defects, {
        section: tribute.valueScenarios.label,
        slotKey: card.slotKey,
        displayPath: `${card.slotKey}.body`,
        currentTributeValue: short(card.body),
        referencePattern: `Curio avg scenario depth ~${Math.round(referenceDepthStats([curioModel], "valueScenarios", "cards").avg || 40)} words`,
        defectType: "thin_copy_vs_reference",
        severity: "medium",
        fixRequiresCopy: true,
        fixRequiresImage: false,
        fixRequiresSourceEvidence: true,
        fixRequiresDisplayMapping: false,
        proposedCorrection: "Expand scenario body with approved consumer/FDD narrative — no invented performance claims.",
        safeForFutureWriter: true,
      });
    }
  }

  // 2. Why Value blank bullets
  if (tribute.whyValueStrongest.emptyIndices.length > 0) {
    addDefect(defects, {
      section: tribute.whyValueStrongest.label,
      slotKey: "overview.why_value",
      displayPath: `overview.why_value.bullets[${tribute.whyValueStrongest.emptyIndices.join(",")}]`,
      currentTributeValue: `${tribute.whyValueStrongest.filledCount}/5 bullets filled`,
      referencePattern: `Curio has ${curio.whyValueStrongest?.filledCount || "5"}/5 bullets on overview.why_value`,
      defectType: "empty_bullet",
      severity: tribute.whyValueStrongest.emptyIndices.length >= 2 ? "high" : "medium",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: true,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Add line-broken bullets to overview.why_value Body (splitBullets uses newlines/semicolons).",
      safeForFutureWriter: true,
      verifiedKnownIssue: true,
    });
  }

  // 3. Value Creation Scenarios thin/title-only
  for (const card of tribute.valueCreationScenarios.cards) {
    if (!hasVal(card.body)) {
      addDefect(defects, {
        section: tribute.valueCreationScenarios.label,
        slotKey: card.slotKey,
        displayPath: `${card.slotKey}.body`,
        currentTributeValue: card.title,
        referencePattern: "Curio valueOwners.scenario.* cards include body paragraphs",
        defectType: "title_only_card",
        severity: "high",
        fixRequiresCopy: true,
        fixRequiresImage: false,
        fixRequiresSourceEvidence: true,
        fixRequiresDisplayMapping: false,
        proposedCorrection: "Populate valueOwners.scenario.N Body with owner-education copy from approved sources.",
        safeForFutureWriter: true,
        verifiedKnownIssue: true,
      });
    } else if (card.wordCount < 15) {
      addDefect(defects, {
        section: tribute.valueCreationScenarios.label,
        slotKey: card.slotKey,
        displayPath: `${card.slotKey}.body`,
        currentTributeValue: short(card.body),
        referencePattern: "Completed brands use 2–4 sentence scenario bodies",
        defectType: "thin_copy_vs_reference",
        severity: "medium",
        fixRequiresCopy: true,
        fixRequiresImage: false,
        fixRequiresSourceEvidence: true,
        fixRequiresDisplayMapping: false,
        proposedCorrection: "Strengthen scenario body copy after source review.",
        safeForFutureWriter: true,
        verifiedKnownIssue: true,
      });
    }
  }

  // 4. Key Watchouts blank bullets
  if (tribute.keyWatchouts.emptyIndices.length > 0) {
    addDefect(defects, {
      section: tribute.keyWatchouts.label,
      slotKey: "valueOwners.watchouts",
      displayPath: `valueOwners.watchouts.bullets[${tribute.keyWatchouts.emptyIndices.join(",")}]`,
      currentTributeValue: `${tribute.keyWatchouts.filledCount}/5 bullets filled`,
      referencePattern: `Curio watchouts filled: ${curio.keyWatchouts?.filledCount ?? "n/a"}/5`,
      defectType: "empty_bullet",
      severity: "high",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: true,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Add owner watchout bullets to valueOwners.watchouts — considerations, not alarms.",
      safeForFutureWriter: true,
      verifiedKnownIssue: true,
    });
  }

  // 5. Standards table missing
  if (!tribute.standardsDetail.table.hasCompletedBrandTable) {
    addDefect(defects, {
      section: tribute.standardsDetail.label,
      slotKey: "standards.requirement",
      displayPath: "standards.requirement.table",
      currentTributeValue: `${tribute.standardsDetail.table.rowCount} rows (${tribute.standardsDetail.table.structuredRowCount} structured)`,
      referencePattern: `Curio has ${curio.standardsDetail?.table?.rowCount || 6}+ structured standards.requirement rows forming 5-column table`,
      defectType: "missing_table_structure",
      severity: "critical",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: true,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Add standards.requirement rows using Typical consideration / Owner planning / Typical status / Notes to confirm body format.",
      safeForFutureWriter: true,
      verifiedKnownIssue: true,
    });
    addDefect(defects, {
      section: tribute.standardsDetail.label,
      slotKey: "standards.requirement",
      displayPath: "renderStandardsOwnerConsiderations fallback",
      currentTributeValue: "Placeholder: No owner planning checklist is published…",
      referencePattern: "Curio renders be-standards-owner-table five-column grid",
      defectType: "generic_placeholder_copy",
      severity: "critical",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: true,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Populate structured standards.requirement rows — do not rely on Brand Setup narrative fallback.",
      safeForFutureWriter: true,
      verifiedKnownIssue: true,
    });
  }

  // 6. Portfolio Context generic / missing parent ladder mapping
  const parentCompany = tribute.portfolioContext.parentCompany;
  const isMarriott = isMarriottParent(parentCompany);
  const isIhg = isIhgParent(parentCompany);
  const marriottPortfolioReady = tribute.portfolioContext.marriottLadderMappingReady;
  const ihgPortfolioReady = tribute.portfolioContext.ihgLadderMappingReady;
  const parentPortfolioReady =
    tribute.portfolioContext.parentPortfolioReady ||
    marriottPortfolioReady ||
    ihgPortfolioReady ||
    isHiltonParent(parentCompany) ||
    isChoiceParent(parentCompany);
  const missingPeerPortfolioContext =
    !tribute.portfolioContext.portfolioContextReady &&
    (!tribute.portfolioContext.narrativeRenders ||
      (!parentPortfolioReady &&
        (tribute.portfolioContext.usesGenericScaleLabels ||
          !tribute.portfolioContext.usesParentStaticLadder)));
  if (missingPeerPortfolioContext) {
    addDefect(defects, {
      section: tribute.portfolioContext.label,
      slotKey: "overview.portfolio_context",
      displayPath: "buildPortfolioLadderCellsHtml",
      currentTributeValue: short(tribute.portfolioContext.body || tribute.portfolioContext.title) || "generic scale ladder labels",
      referencePattern: isMarriott
        ? "Marriott portfolio should show sibling collection flags (like Hilton ladder for Curio), not Lower-scale/Mid-scale generics"
        : isIhg
          ? "IHG portfolio should show IHG luxury/lifestyle sibling flags (InterContinental, Regent, Kimpton), not generic scale labels"
          : "Curio uses Hilton static ladder with named sibling brands per tier",
      defectType: "missing_peer_portfolio_context",
      severity: "high",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: true,
      proposedCorrection: isMarriott
        ? "Set overview.portfolio_context tier + add Marriott static ladder mapping in frontend OR populate tier body with collection ladder labels."
        : "Align portfolio_context slot with parent-company ladder pattern.",
      safeForFutureWriter: false,
      verifiedKnownIssue: true,
    });
    if (isGenericPlaceholder(tribute.portfolioContext.body)) {
      addDefect(defects, {
        section: tribute.portfolioContext.label,
        slotKey: "overview.portfolio_context",
        displayPath: "overview.portfolio_context.body",
        currentTributeValue: short(tribute.portfolioContext.body),
        referencePattern: "Curio portfolio_context uses tier index + Hilton sibling names",
        defectType: "generic_placeholder_copy",
        severity: "medium",
        fixRequiresCopy: true,
        fixRequiresImage: false,
        fixRequiresSourceEvidence: false,
        fixRequiresDisplayMapping: true,
        proposedCorrection: "Replace generic scale labels with Marriott collection ladder context.",
        safeForFutureWriter: true,
        verifiedKnownIssue: true,
      });
    }
  }

  // 7. Featured Application truncated/weak
  const feat = tribute.featuredApplication;
  const hasDedicatedFeaturedSlot = feat.sourceFields?.includes("overview.featured_application");
  if (!feat.displayed) {
    addDefect(defects, {
      section: feat.label || tribute.featuredApplication.label,
      slotKey: "overview.featured_application",
      displayPath: "featured-case-preview__sub",
      currentTributeValue: short(feat.body || feat.lead) || "(empty)",
      referencePattern: "Curio uses substantive brandTaglineMotto + brandPositioning in featured preview",
      defectType: "empty_card",
      severity: "medium",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: true,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Populate overview.featured_application or Basics positioning fields.",
      safeForFutureWriter: true,
      verifiedKnownIssue: true,
    });
  } else if (feat.truncatedInUi) {
    addDefect(defects, {
      section: tribute.featuredApplication.label,
      slotKey: "overview.featured_application",
      displayPath: "brandPositioning.slice(0,220)",
      currentTributeValue: `${feat.body.length} chars (UI truncates at 220)`,
      referencePattern: "Curio featured preview shows fuller positioning lead",
      defectType: "truncated_copy",
      severity: "low",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: true,
      proposedCorrection: "Shorten lead copy for featured block or extend UI truncation threshold.",
      safeForFutureWriter: false,
      verifiedKnownIssue: true,
    });
  } else if (feat.wordCount < 20 && !hasDedicatedFeaturedSlot) {
    addDefect(defects, {
      section: feat.label || tribute.featuredApplication.label,
      slotKey: "overview.featured_application",
      displayPath: "featured-case-preview__sub",
      currentTributeValue: short(feat.body || feat.lead) || "(empty)",
      referencePattern: "Curio uses substantive brandTaglineMotto + brandPositioning in featured preview",
      defectType: "thin_copy_vs_reference",
      severity: "medium",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: true,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Strengthen Basics positioning fields or add dedicated presentation slot if UI truncation hides copy.",
      safeForFutureWriter: true,
      verifiedKnownIssue: true,
    });
  } else if (hasDedicatedFeaturedSlot && feat.wordCount < 25) {
    addDefect(defects, {
      section: tribute.featuredApplication.label,
      slotKey: "overview.featured_application",
      displayPath: "overview.featured_application.body",
      currentTributeValue: short(feat.body),
      referencePattern: "Curio featured preview shows fuller positioning lead",
      defectType: "thin_copy_vs_reference",
      severity: "low",
      cosmeticNonBlocking: true,
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Optional cosmetic polish — dedicated featured slot is populated and not truncated.",
      safeForFutureWriter: false,
      verifiedKnownIssue: true,
    });
  }

  // Best At thin cards
  for (const card of tribute.bestAt.cards) {
    if (!hasVal(card.body)) {
      addDefect(defects, {
        section: tribute.bestAt.label,
        slotKey: card.slotKey,
        displayPath: `${card.slotKey}.body`,
        currentTributeValue: card.title,
        referencePattern: "Curio overview.bestAt.* has body copy per card",
        defectType: "empty_card",
        severity: "medium",
        fixRequiresCopy: true,
        fixRequiresImage: false,
        fixRequiresSourceEvidence: true,
        fixRequiresDisplayMapping: false,
        proposedCorrection: "Populate overview.bestAt.N Body with pillar-specific copy.",
        safeForFutureWriter: true,
      });
    }
  }

  // Differentiators empty bullets
  for (const side of ["identity", "commercial"]) {
    const block = tribute.keyDifferentiators[side];
    if (block.emptyIndices.length > 0) {
      addDefect(defects, {
        section: tribute.keyDifferentiators.label,
        slotKey: `overview.differentiators.${side}`,
        displayPath: `overview.differentiators.${side}.bullets`,
        currentTributeValue: `${block.filledCount}/4 bullets`,
        referencePattern: `Curio ${side} differentiators fully populated`,
        defectType: "empty_bullet",
        severity: "medium",
        fixRequiresCopy: true,
        fixRequiresImage: false,
        fixRequiresSourceEvidence: true,
        fixRequiresDisplayMapping: false,
        proposedCorrection: `Fill overview.differentiators.${side} with line-broken bullets.`,
        safeForFutureWriter: true,
      });
    }
  }

  // Brand materials gallery — active profile requires six visible API image cards (v33H)
  const gallerySlots = tribute.brandMaterials.gallery;
  const visibleGallery = gallerySlots.filter((g) => g.visibleInApi);
  const visibleWithImage = visibleGallery.filter((g) => g.hasImage);
  const missingGallery = visibleGallery.filter((g) => !g.hasImage);
  const deferredGallery = gallerySlots.filter((g) => g.deferredHidden);

  if (visibleGallery.length < 6) {
    addDefect(defects, {
      section: tribute.brandMaterials.label,
      slotKey:
        deferredGallery.map((g) => g.slotKey).join(", ") ||
        gallerySlots.map((g) => g.slotKey).join(", "),
      displayPath: "materials.gallery visible count",
      currentTributeValue: `${visibleGallery.length}/6 visible gallery cards in Brand Library API`,
      referencePattern:
        "Active-profile brands show six materials.gallery image cards with imageUrl in API/UI",
      defectType: "insufficient_visible_gallery_images",
      severity: "high",
      fixRequiresCopy: false,
      fixRequiresImage: true,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: true,
      proposedCorrection:
        "Unhide and materialize six gallery slots with hotel/property photography on presentation Image fields — hidden rows and registry-only sources do not count.",
      safeForFutureWriter: false,
    });
  }

  if (missingGallery.length > 0) {
    addDefect(defects, {
      section: tribute.brandMaterials.label,
      slotKey: missingGallery.map((g) => g.slotKey).join(", "),
      displayPath: "materials.gallery.*.imageUrl",
      currentTributeValue: `${missingGallery.length} visible gallery slots without imageUrl`,
      referencePattern: "Curio promotes gallery images for materials tab parity",
      defectType: "missing_card_image",
      severity: missingGallery.length >= 3 ? "high" : "medium",
      fixRequiresCopy: false,
      fixRequiresImage: true,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: false,
      proposedCorrection:
        "Attach durable hotel/property images to visible gallery presentation rows — registry Source URL alone does not render in Explorer UI.",
      safeForFutureWriter: false,
    });
  }

  // Dealality Insight thin
  if (tribute.dealalityInsight.wordCount < 30) {
    addDefect(defects, {
      section: tribute.dealalityInsight.label,
      slotKey: "insight.summary",
      displayPath: "insight.summary.body",
      currentTributeValue: short(tribute.dealalityInsight.body) || "(empty)",
      referencePattern: "Completed brands include insight.summary caveats and sourcing notes",
      defectType: tribute.dealalityInsight.wordCount === 0 ? "empty_card" : "thin_copy_vs_reference",
      severity: "medium",
      fixRequiresCopy: true,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: true,
      fixRequiresDisplayMapping: false,
      proposedCorrection: "Expand insight.summary with governance-safe caveats — no unsupported claims.",
      safeForFutureWriter: true,
    });
  }

  // Parent-aware wrong-brand copy scan (v28F)
  if (brandApi && brandTarget) {
    detectWrongBrandCopyDefects(brandApi, brandTarget, defects);
  }

  // Sort Order defects from prior audit
  if (sortOrderContext?.likelyWriterDefaultCount > 0) {
    addDefect(defects, {
      section: "Cross-section",
      slotKey: "(multi-row slots)",
      displayPath: "presentation.sort",
      currentTributeValue: `${sortOrderContext.likelyWriterDefaultCount} rows with index×10 Sort Order defaults`,
      referencePattern: "Curio completed brands predominantly use Sort Order 0 per slot family",
      defectType: "bad_sort_order",
      severity: sortOrderContext.likelyWriterDefaultCount >= 50 ? "high" : "medium",
      fixRequiresCopy: false,
      fixRequiresImage: false,
      fixRequiresSourceEvidence: false,
      fixRequiresDisplayMapping: true,
      proposedCorrection: "Future v24D Sort Order correction writer — normalize to reference pattern after content stabilization.",
      safeForFutureWriter: false,
      verifiedKnownIssue: true,
    });
  }

  // Duplicated bullets
  for (const key of ["whyValueStrongest", "keyWatchouts"]) {
    const bullets = tribute[key].bullets.filter(hasVal);
    const seen = new Set();
    for (const b of bullets) {
      const norm = b.toLowerCase();
      if (seen.has(norm)) {
        addDefect(defects, {
          section: tribute[key].label,
          slotKey: tribute[key].slotKey,
          displayPath: `${tribute[key].slotKey}.bullets`,
          currentTributeValue: short(b),
          referencePattern: "Completed brands use distinct bullets per line",
          defectType: "duplicated_bullet",
          severity: "low",
          fixRequiresCopy: true,
          fixRequiresImage: false,
          fixRequiresSourceEvidence: false,
          fixRequiresDisplayMapping: false,
          proposedCorrection: "Deduplicate bullet copy in presentation Body.",
          safeForFutureWriter: true,
        });
      }
      seen.add(norm);
    }
  }

  return defects;
}

function groupDefectsBySeverity(defects) {
  return {
    critical: defects.filter((d) => d.severity === "critical"),
    high: defects.filter((d) => d.severity === "high"),
    medium: defects.filter((d) => d.severity === "medium"),
    low: defects.filter((d) => d.severity === "low"),
  };
}

function buildRemediationBatches(defects) {
  const copyOnly = defects.filter((d) => d.fixRequiresCopy && !d.fixRequiresImage && !d.fixRequiresSourceEvidence);
  const media = defects.filter((d) => d.fixRequiresImage);
  const evidence = defects.filter((d) => d.fixRequiresSourceEvidence);
  const sortOrder = defects.filter((d) => d.defectType === "bad_sort_order");
  const displayMapping = defects.filter((d) => d.fixRequiresDisplayMapping);

  return {
    v24A_copy_cleanup_writer: {
      description: "Editorial/copy cleanup for bullets, scenario bodies, bestAt, watchouts, featured lead — no new claims.",
      defectCount: copyOnly.length,
      slotKeys: [...new Set(copyOnly.map((d) => d.slotKey))],
      safeToAutomate: copyOnly.filter((d) => d.safeForFutureWriter).length,
    },
    v24B_media_asset_fix: {
      description: "Attach approved registry images to scenario.3 and gallery slots — no invented assets.",
      defectCount: media.length,
      slotKeys: [...new Set(media.map((d) => d.slotKey))],
      safeToAutomate: 0,
    },
    v24C_source_evidence_work: {
      description: "Standards table rows, scenario proof copy, insight summary — requires approved facts/sources.",
      defectCount: evidence.length,
      slotKeys: [...new Set(evidence.map((d) => d.slotKey))],
      safeToAutomate: evidence.filter((d) => d.safeForFutureWriter).length,
    },
    v24D_sort_order_correction: {
      description: "Normalize presentation Sort Order after content batches land.",
      defectCount: sortOrder.length,
      slotKeys: sortOrder.map((d) => d.slotKey),
      safeToAutomate: 0,
    },
    displayMappingFollowUp: {
      description: "Frontend/portfolio ladder mapping fixes (Marriott static ladder, truncation).",
      defectCount: displayMapping.length,
    },
  };
}

function pickRecommendedNextBatch(defects, remediation) {
  const blocking = defects.filter(
    (d) => !d.cosmeticNonBlocking && (d.severity === "critical" || d.severity === "high")
  );
  if (!blocking.length) {
    const cosmeticOnly = defects.length > 0 && defects.every((d) => d.cosmeticNonBlocking || d.severity === "low");
    if (!defects.length || cosmeticOnly) return "none_active_profile_ready";
  }
  if (remediation.v24B_media_asset_fix.defectCount > 0) return "v24B_media_asset_fix";
  if (remediation.v24C_source_evidence_work.defectCount > 0) return "v24C_source_evidence_work";
  if (remediation.v24A_copy_cleanup_writer.defectCount > 0) return "v24A_copy_cleanup_writer";
  if (remediation.v24D_sort_order_correction.defectCount > 0) return "v24D_sort_order_correction";
  return "none_active_profile_ready";
}

function assessVisualComparability(defects, tributeModel, curioModel) {
  const bySev = groupDefectsBySeverity(defects);
  const criticalHigh = bySev.critical.length + bySev.high.length;
  const comparable =
    criticalHigh === 0 &&
    tributeModel.blockCount >= (curioModel?.blockCount || 0) * 0.85 &&
    tributeModel.sections.standardsDetail?.table?.hasCompletedBrandTable;
  return {
    visuallyComparableToCurioToday: comparable,
    score: Math.max(0, 100 - bySev.critical.length * 15 - bySev.high.length * 8 - bySev.medium.length * 3),
    rationale:
      criticalHigh > 0
        ? `${bySev.critical.length} critical + ${bySev.high.length} high display defects block Curio-level parity.`
        : "Residual polish gaps remain.",
  };
}

function countLiveLikelyWriterSortDefaults(brand) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => {
    const sort = typeof b.sort === "number" ? b.sort : b.sortOrder;
    return isLikelyWriterBatchSortOrder(sort);
  }).length;
}

function isLikelyWriterBatchSortOrder(sortOrder) {
  if (sortOrder == null || Number.isNaN(Number(sortOrder))) return false;
  const n = Number(sortOrder);
  return n >= 10 && n % 10 === 0;
}

function readSortOrderContext(tributeBrand = null) {
  const liveCount = tributeBrand ? countLiveLikelyWriterSortDefaults(tributeBrand) : null;
  const p = path.join(ROOT, "reports/brand-explorer-presentation-sort-order-audit.json");
  let fileCount = 0;
  if (fs.existsSync(p)) {
    try {
      const data = JSON.parse(fs.readFileSync(p, "utf8"));
      fileCount = data.sortOrderAuditSummary?.likelyWriterDefaultCount ?? 0;
    } catch {
      fileCount = 0;
    }
  }
  return {
    likelyWriterDefaultCount: liveCount != null ? liveCount : fileCount,
    source: liveCount != null ? "live_api_blocks" : "sort_order_audit_report",
    tributeRowCount: tributeBrand?.brandExplorer?.blocks?.length ?? null,
    futureSortOrderWriterNeeded: (liveCount != null ? liveCount : fileCount) > 0,
  };
}

export async function buildBrandExplorerVisualDisplayDefectAuditReport(options = {}) {
  const brandId = await resolveVisualAuditBrandRecordId(options.brandIdOrName);
  const tributeBrand = await fetchBrandApiShape(brandId);
  if (!tributeBrand) throw new Error(`Could not load Tribute brand API shape for ${brandId}`);

  const curioBrand = await fetchBrandApiShape(CURIO_BRAND_ID);
  const referenceBrands = [];
  for (const name of REFERENCE_BRANDS) {
    const b = await fetchBrandApiShape(name);
    if (b) referenceBrands.push({ name: nz(b.name) || name, brand: b });
  }

  const brandTarget = resolveAuditBrandTarget(brandId, tributeBrand);
  const tributeModel = reconstructVisibleModel(tributeBrand);
  const curioModel = curioBrand ? reconstructVisibleModel(curioBrand) : null;
  const referenceModels = referenceBrands.map((r) => reconstructVisibleModel(r.brand));
  const sortOrderContext = readSortOrderContext(tributeBrand);
  const carryoverScan = detectWrongBrandCopyDefects(tributeBrand, brandTarget, []);

  const defects = detectDefects(
    tributeModel,
    curioModel,
    referenceModels,
    sortOrderContext,
    tributeBrand,
    brandTarget
  );
  const discoveryConfig = getDiscoveryBrandConfig(brandTarget.slug);
  if (discoveryConfig) {
    const registryAssets = await listRegistryAssetsForBrand(brandId).catch(() => []);
    const imageGovDefects = detectBrandAssetImageGovernanceDefects(
      tributeBrand,
      registryAssets,
      discoveryConfig,
      brandTarget
    );
    for (const d of imageGovDefects) {
      addDefect(defects, {
        section: d.surface || d.slotKey,
        slotKey: d.slotKey,
        recordId: d.recordId,
        defectType: d.type,
        severity: d.severity,
        category: d.category || "data",
        proposedCorrection: d.message,
        fixRequiresImage: true,
        fixRequiresSourceEvidence: d.type === "wrong_brand_image",
        remediationBatch: d.recommendedFixBatch || "v31B_brand_asset_registry_discovery",
        cosmeticNonBlocking: Boolean(d.cosmeticNonBlocking),
      });
    }
    const quarantineAssessments = [];
    for (const block of tributeBrand?.brandExplorer?.blocks || []) {
      if (!isOpeningsEvidenceSlot(block?.slotKey)) continue;
      const registryMatch = findRegistryAssetForPresentationRow(registryAssets, block);
      const imageAssessment = assessPresentationRowImageGovernance(
        block,
        discoveryConfig,
        registryAssets
      );
      const q = assessOpeningsRowQuarantine(block, imageAssessment, registryMatch);
      if (q) quarantineAssessments.push(q);
    }
    const quarantineDefects = detectOpeningsUiQuarantineDefects(
      [],
      quarantineAssessments,
      brandTarget
    );
    for (const d of quarantineDefects) {
      addDefect(defects, {
        section: d.surface || d.slotKey,
        slotKey: d.slotKey,
        recordId: d.recordId,
        defectType: d.type,
        severity: d.severity,
        category: d.category || "data",
        proposedCorrection: d.message,
        fixRequiresDisplayMapping: true,
        remediationBatch: d.recommendedFixBatch || "v31C_radisson_individuals_openings_suppression",
      });
    }
  }
  const bySeverity = groupDefectsBySeverity(defects);
  const remediation = buildRemediationBatches(defects);
  const comparability = assessVisualComparability(defects, tributeModel, curioModel);
  const recommendedNextBatch = pickRecommendedNextBatch(defects, remediation);

  const sectionsAudited = Object.values(tributeModel.sections).map((s) => s.label || s.note).filter(Boolean);

  return {
    auditVersion: AUDIT_VERSION,
    carryoverAuditVersion: CARRYOVER_AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    brand: {
      recordId: brandId,
      name: nz(tributeBrand.name) || BRAND_NAME,
      slug: brandTarget.slug || null,
      parentFamily: carryoverScan.targetFamily,
    },
    parentAwareCarryover: {
      version: CARRYOVER_AUDIT_VERSION,
      targetFamily: carryoverScan.targetFamily,
      blockingFindings: carryoverScan.blockingCount,
      allowedReferences: carryoverScan.allowedCount,
    },
    filesRead: [
      "AGENTS.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "docs/brand-explorer-presentation-slots.md",
      "reports/brand-explorer-slot-completion-writer.md",
      "reports/brand-explorer-remaining-editorial-slot-completion-writer.md",
      "reports/brand-explorer-presentation-slot-coverage-audit.md",
      "reports/brand-explorer-slot-standard-manifest.md",
      "reports/brand-explorer-slot-completion-remaining-plan.md",
      "reports/brand-explorer-presentation-sort-order-audit.md",
      "fixtures/brand-explorer-presentation-*.json",
      "live Brand Explorer Presentation rows (API)",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
      "lib/partner-intelligence/brand-explorer-parent-aware-carryover.js",
      "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
      "scripts/brand-explorer-visual-display-defect-audit.mjs",
      `docs/data-intelligence/${DOC_MD_NAME}`,
      `reports/${REPORT_MD_NAME}`,
      `reports/${REPORT_JSON_NAME}`,
      "package.json",
    ],
    v24VisualDisplayDefectAuditExists: true,
    referenceBrandsInspected: [
      { name: PRIMARY_REFERENCE, recordId: CURIO_BRAND_ID, blockCount: curioModel?.blockCount || 0 },
      ...referenceBrands.map((r) => ({
        name: r.name,
        recordId: r.brand?.id,
        blockCount: r.brand?.brandExplorer?.blocks?.length || 0,
      })),
    ],
    sectionsAudited,
    tributePresentationRowCount: tributeModel.blockCount,
    curioPresentationRowCount: curioModel?.blockCount || 0,
    tributeVisibleModel: tributeModel,
    curioVisibleModelSummary: curioModel
      ? {
          blockCount: curioModel.blockCount,
          standardsTableRows: curioModel.sections.standardsDetail.table.rowCount,
          scenario3HasImage: curioModel.sections.valueScenarios.cards[2]?.hasImage,
        }
      : null,
    defects,
    defectsBySeverity: bySeverity,
    defectCounts: {
      total: defects.length,
      critical: bySeverity.critical.length,
      high: bySeverity.high.length,
      medium: bySeverity.medium.length,
      low: bySeverity.low.length,
      missingImage: defects.filter((d) => d.defectType === "missing_card_image" || d.defectType === "blank_image_placeholder").length,
      emptyBullet: defects.filter((d) => d.defectType === "empty_bullet").length,
      titleOnlyOrThin: defects.filter((d) =>
        ["title_only_card", "thin_copy_vs_reference", "empty_card"].includes(d.defectType)
      ).length,
      placeholderGeneric: defects.filter((d) => d.defectType === "generic_placeholder_copy").length,
      sortOrder: defects.filter((d) => d.defectType === "bad_sort_order").length,
    },
    sectionsRequiringSourceEvidence: [
      ...new Set(defects.filter((d) => d.fixRequiresSourceEvidence).map((d) => d.section)),
    ],
    sectionsRequiringMediaAssets: [
      ...new Set(defects.filter((d) => d.fixRequiresImage).map((d) => d.section)),
    ],
    sectionsRequiringCopyCleanupOnly: [
      ...new Set(
        defects
          .filter((d) => d.fixRequiresCopy && !d.fixRequiresImage && !d.fixRequiresSourceEvidence)
          .map((d) => d.section)
      ),
    ],
    proposedRemediationBatches: remediation,
    visualComparability: comparability,
    knownIssuesVerified: defects.filter((d) => d.verifiedKnownIssue).map((d) => d.defectType + ":" + d.slotKey),
    exactNextCommand:
      "npm run brand-explorer-visual-display-defect-audit -- --brand tribute-portfolio --dry-run",
    recommendedNextBatch,
  };
}

export function buildBrandExplorerVisualDisplayDefectAuditMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Visual Display Defect Audit v24");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** · Airtable modified: **no** · Images untouched: **yes**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- Visually comparable to Curio today: **${report.visualComparability.visuallyComparableToCurioToday ? "yes" : "no"}** (${report.visualComparability.score}/100)`);
  lines.push(`- Defects: **${report.defectCounts.total}** (critical ${report.defectCounts.critical}, high ${report.defectCounts.high}, medium ${report.defectCounts.medium}, low ${report.defectCounts.low})`);
  lines.push("");
  lines.push("## Reference brands");
  for (const r of report.referenceBrandsInspected) {
    lines.push(`- ${r.name} (\`${r.recordId}\`) — ${r.blockCount} presentation blocks`);
  }
  lines.push("");
  lines.push("## Critical defects");
  for (const d of report.defectsBySeverity.critical) {
    lines.push(`- **${d.section}** · \`${d.slotKey}\` · ${d.defectType}: ${short(d.currentTributeValue, 120)}`);
  }
  if (!report.defectsBySeverity.critical.length) lines.push("- none");
  lines.push("");
  lines.push("## High-priority defects");
  for (const d of report.defectsBySeverity.high) {
    lines.push(`- **${d.section}** · \`${d.slotKey}\` · ${d.defectType}`);
  }
  lines.push("");
  lines.push("## Remediation batches");
  for (const [key, batch] of Object.entries(report.proposedRemediationBatches)) {
    if (key === "displayMappingFollowUp") continue;
    lines.push(`- **${key}**: ${batch.defectCount} defects`);
  }
  lines.push("");
  lines.push(`## Recommended next batch: **${report.recommendedNextBatch}**`);
  lines.push("");
  lines.push("## Exact next command");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}
