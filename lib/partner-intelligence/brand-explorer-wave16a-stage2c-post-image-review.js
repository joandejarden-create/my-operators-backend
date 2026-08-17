/**
 * Wave 16A Stage 2C — LOW-risk post-image review + narrow openings remediation.
 *
 * Allowed: Presentation Title/Body/Case Summary (and captions) on target brands only.
 * Forbidden: Brand Status, release, CV, Census, Recent Momentum, Flex, Active 62,
 * remaining Wave 16A, Wave 16B, Founder Visual Review Pass writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import {
  WAVE16A_VERSION,
  WAVE16A_STAGE2B_APPROVED_SLUGS,
  WAVE16A_FLEX_HOLD,
  WAVE16A_IDENTITIES,
  WAVE16A_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave16a-factory-plan.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import {
  buildOpeningsPropertyCardTitle,
  buildOpeningsPropertyCardBody,
} from "./brand-explorer-openings-property-card-contract.js";
import { runWave16aStage2bIdentityPreflight } from "./brand-explorer-wave16a-stage2b-image-materialization.js";

export const WAVE16A_STAGE2C_VERSION = "wave16a-stage2c-post-image-review-v1";
export const READY_PASS = "wave16a_stage2c_low_risk_profiles_ready_for_founder_review";
export const READY_REMEDIATE = "wave16a_stage2c_low_risk_targeted_remediation_required";
export const READY_DRY = "wave16a_stage2c_post_image_review_dry_run_ready";

export const WAVE16A_STAGE2C_APPLY_FLAGS = Object.freeze([
  "--approve-wave16a-stage2c-post-image-review",
  "--confirm-three-brand-scope",
  "--confirm-all-three-under-review",
  "--confirm-active-62-protected",
  "--confirm-no-brand-status-writes",
  "--confirm-no-release-writes",
  "--confirm-no-company-validation-writes",
  "--confirm-no-brand-verified-writes",
  "--confirm-no-census-writes",
  "--confirm-no-recent-momentum-writes",
  "--confirm-no-source-library-writes",
  "--confirm-no-registry-writes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-wave16b-writes",
  "--confirm-no-remaining-wave16a-writes",
  "--confirm-no-non-target-writes",
  "--confirm-no-founder-visual-review-pass-writes",
  "--confirm-openings-public-copy-safety",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
  "Partner Intelligence - Source Library",
  "Partner Intelligence - Brand Asset Registry",
  "Image",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function parseWave16aStage2cFlags(argv = []) {
  const missing = WAVE16A_STAGE2C_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    ok: argv.includes("--apply") && missing.length === 0,
    missing,
  };
}

function stripTrailingUrl(body) {
  return nz(body)
    .split(/\n+/)
    .filter((line) => !/^https?:\/\/\S+$/i.test(line.trim()))
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function ownerFacingInterpretation() {
  return "Validate current brand affiliation, property product, and capital scope with the brand before underwriting.";
}

function ownerFacingBrandRelevance({ isIntl }) {
  return isIntl
    ? "Official International Reference property photography for Brand Explorer openings — not a CALA operating claim."
    : "Official CALA property photography used as a Brand Explorer property example for this brand.";
}

function ownerFacingObjective() {
  return "Use as a directional property reference when underwriting product fit, capital scope, and platform participation.";
}

function buildOwnerTeaser({ propertyName, brandName, marketCity, geographyLabel }) {
  const isIntl = !/^cala/i.test(nz(geographyLabel));
  const place = nz(marketCity) || (isIntl ? "an International Reference market" : "a CALA market");
  return `${propertyName} is an official ${
    isIntl ? "International Reference" : "CALA"
  } example for ${brandName} in ${place}. Use it to benchmark product standards, service delivery, and capital scope for similar assets.`;
}

function openingsChipList(row) {
  return nz(row.caseSummaryTags)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function openingsIsCala(row) {
  return openingsChipList(row).some((c) => /^CALA$/i.test(c)) || /—\s*CALA\s*$/i.test(nz(row.title));
}

function openingsMarketCity(row, brandName) {
  const chips = openingsChipList(row);
  const fromChips = chips.find((c) => !/^(CALA|International Reference|Property example)$/i.test(c));
  if (fromChips) return fromChips;
  const afterEm = nz(row.title).split("—")[1]?.trim();
  if (afterEm && !/^CALA$/i.test(afterEm) && !/^International Reference$/i.test(afterEm)) return afterEm;
  return "this market";
}

function openingsPropertyName(row, brandName) {
  let name = nz(row.title).split("—")[0].trim();
  if (brandName) {
    const esc = String(brandName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    name = name.replace(new RegExp(`\\s+${esc}\\s*$`, "i"), "").trim();
  }
  return name;
}

function sanitizeOwnerTeaser(teaser, brandName) {
  let t = nz(teaser);
  if (!brandName) return t;
  const esc = String(brandName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  // "…Cancun Airport Fairfield by Marriott is an official Fairfield by Marriott…"
  t = t.replace(new RegExp(`\\s+${esc}\\s+(is an official\\s+${esc})\\b`, "i"), " $1");
  return t;
}

function rebuildOpeningsStructuredBody(row, brandName, teaser) {
  const chips = openingsChipList(row);
  const isCala = openingsIsCala(row);
  const marketCity = openingsMarketCity(row, brandName);
  const chipList =
    chips.length >= 2
      ? chips
      : [
          isCala ? "CALA" : "International Reference",
          marketCity,
          "Property example",
        ];
  const locationLine = isCala
    ? `${marketCity} (CALA)`
    : `${marketCity} (International Reference)`;
  const metaLine = isCala
    ? `CALA · ${marketCity}`
    : `International Reference · ${marketCity}`;
  const scenarioLine = chipList.join(" / ").toUpperCase();
  return buildOpeningsPropertyCardBody({
    chips: chipList,
    locationLine,
    metaLine,
    scenarioLine,
    teaser,
    sourceUrl: "",
  });
}

function openingsBodyCollapsed(body) {
  const text = nz(body);
  const blocks = text.split(/\n\n+/).filter(Boolean);
  const wordCount = text.split(/\s+/).filter(Boolean).length;
  return blocks.length < 3 || wordCount < 40;
}

/** Curated Stage 2C openings remediations keyed by recordId. */
function plannedOpeningsPatches(slug, rows) {
  const brandName = WAVE16A_IDENTITIES[slug].exactBrandBasicsName;
  const patches = [];
  const defects = [];

  for (const row of rows.filter((r) => r.slotKey === "footprint.openings")) {
    const body = nz(row.body);
    const title = nz(row.title);
    const cs = nz(row.caseSummaryOverview);
    const interp = nz(row.caseSummaryInterpretation);
    const hasUrl = /https?:\/\/\S+/i.test(body);
    const hasConfirm = /Confirm live affiliation/i.test(
      `${body}\n${cs}\n${interp}\n${nz(row.caseSummaryBrandRelevance)}\n${nz(row.caseSummaryOwnerObjective)}`
    );
    const roleTeaser = /—\s*(property setting|public space lobby|exterior arrival|guest room|guest room suite|amenity[_ ]?\w*)\b/i.test(
      `${body}\n${cs}`
    );
    const titleDoubledBrand =
      /Fairfield Inn & Suites New York Manhattan\/Central Park Fairfield by Marriott/i.test(title) ||
      /Fairfield Inn & Suites New York Manhattan\/Times Square South Fairfield by Marriott/i.test(title);

    // CRITICAL: Times Square title/body with Cancun CS metadata
    const timesSquareTitle = /Times Square South/i.test(title);
    const cancunCs = /Cancun Airport/i.test(cs) || (/CALA/i.test(nz(row.caseSummaryTags)) && /Cancún|Cancun/i.test(nz(row.caseSummaryTags)));
    if (slug === "fairfield-by-marriott" && timesSquareTitle && cancunCs) {
      const propertyName = "Fairfield Inn & Suites Cancun Airport";
      const marketCity = "Cancún";
      const geographyLabel = "CALA";
      const chips = "CALA, Cancún, Property example";
      const newTitle = `${propertyName} — ${marketCity}`;
      const teaser = buildOwnerTeaser({
        propertyName,
        brandName,
        marketCity,
        geographyLabel,
      });
      const newBody = buildOpeningsPropertyCardBody({
        chips: chips.split(",").map((s) => s.trim()),
        locationLine: "Cancún, Mexico (CALA)",
        metaLine: "CALA · Mexico",
        scenarioLine: "CALA / CANCÚN / PROPERTY EXAMPLE",
        teaser,
        sourceUrl: "", // Stage 2C: no raw URL in visible Body
      });
      defects.push({
        brand: slug,
        tab: "footprint",
        section: "footprint.openings",
        component: row.recordId,
        field: "Title/Body/Case Summary",
        currentValue: `title=${title.slice(0, 80)}; cs=${cs.slice(0, 80)}`,
        defectType: "wrong_property_metadata",
        severity: "CRITICAL",
        proposedFix: "Unify openings card to Cancun Airport CALA identity matching Stage 2B image assignment",
        sourceSupport: "stage2b_image_assignment",
        patchAllowed: true,
      });
      patches.push({
        recordId: row.recordId,
        slotKey: "footprint.openings",
        fields: {
          Title: newTitle,
          Body: newBody,
          "Case Summary Overview": teaser,
          "Case Summary Tags": chips,
          "Case Summary Brand Relevance": ownerFacingBrandRelevance({ isIntl: false }),
          "Case Summary Owner Objective": ownerFacingObjective(),
          "Case Summary Interpretation": ownerFacingInterpretation(),
        },
        reason: "wrong_property_metadata_cancun_unify",
      });
      continue;
    }

    const fields = {};
    let reasons = [];

    const propertyName = openingsPropertyName(row, brandName);
    const marketCity = openingsMarketCity(row, brandName);
    const geographyLabel = openingsIsCala(row) ? "CALA" : "International Reference";
    const defaultTeaser = buildOwnerTeaser({
      propertyName: propertyName || brandName,
      brandName,
      marketCity,
      geographyLabel,
    });
    const brandEsc = String(brandName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const doubledBrandInCopy =
      new RegExp(`${brandEsc}\\s+is an official\\s+${brandEsc}`, "i").test(`${body}\n${cs}`) ||
      /Fairfield Inn & Suites Cancun Airport Fairfield by Marriott/i.test(title) ||
      /Fairfield Inn & Suites New York Manhattan\/(?:Central Park|Times Square South)\s+Fairfield by Marriott\s+is an official/i.test(
        `${body}\n${cs}`
      );

    if (doubledBrandInCopy) {
      defects.push({
        brand: slug,
        tab: "footprint",
        section: "footprint.openings",
        component: row.recordId,
        field: "Title/Body/Case Summary Overview",
        currentValue: `${title.slice(0, 60)} | ${cs.slice(0, 60)}`,
        defectType: "awkward_title_brand_duplication",
        severity: "LOW",
        proposedFix: "Remove duplicated brand fragment from openings title/teaser",
        sourceSupport: "openings_title_contract",
        patchAllowed: true,
      });
      reasons.push("dedupe_brand_copy");
      fields["Case Summary Overview"] = defaultTeaser;
      fields.Body = rebuildOpeningsStructuredBody(row, brandName, defaultTeaser);
      if (/Fairfield Inn & Suites Cancun Airport Fairfield by Marriott/i.test(title)) {
        fields.Title = "Fairfield Inn & Suites Cancun Airport — Cancún";
      }
    }

    if (hasUrl) {
      defects.push({
        brand: slug,
        tab: "footprint",
        section: "footprint.openings",
        component: row.recordId,
        field: "Body",
        currentValue: body.slice(0, 120),
        defectType: "prohibited_language:raw_url",
        severity: "HIGH",
        proposedFix: "Strip trailing raw URL from openings Body",
        sourceSupport: "public_copy_safety",
        patchAllowed: true,
      });
      reasons.push("strip_raw_url");
    }

    if (hasConfirm) {
      defects.push({
        brand: slug,
        tab: "footprint",
        section: "footprint.openings",
        component: row.recordId,
        field: "Case Summary Interpretation",
        currentValue: interp.slice(0, 120),
        defectType: "internal_workflow_language",
        severity: "HIGH",
        proposedFix: "Replace Confirm live affiliation steward wording",
        sourceSupport: "public_copy_safety",
        patchAllowed: true,
      });
      fields["Case Summary Interpretation"] = ownerFacingInterpretation();
      reasons.push("fix_confirm_live");
    }

    if (roleTeaser) {
      defects.push({
        brand: slug,
        tab: "footprint",
        section: "footprint.openings",
        component: row.recordId,
        field: "Body/Case Summary Overview",
        currentValue: (cs || body).slice(0, 120),
        defectType: "role_label_as_owner_teaser",
        severity: "MEDIUM",
        proposedFix: "Replace image-role teaser with owner-facing property teaser",
        sourceSupport: "owner_facing_clarity",
        patchAllowed: true,
      });
      fields["Case Summary Overview"] = defaultTeaser;
      reasons.push("owner_teaser");
    }

    if (titleDoubledBrand) {
      defects.push({
        brand: slug,
        tab: "footprint",
        section: "footprint.openings",
        component: row.recordId,
        field: "Title",
        currentValue: title.slice(0, 120),
        defectType: "awkward_title_brand_duplication",
        severity: "LOW",
        proposedFix: "Remove duplicated brand fragment from openings Title",
        sourceSupport: "openings_title_contract",
        patchAllowed: true,
      });
      fields.Title = title
        .replace(/\s+Fairfield by Marriott\s+—/i, " —")
        .replace(/\s{2,}/g, " ")
        .trim();
      reasons.push("title_dedupe");
    }

    const needsStructuredRebuild =
      hasUrl ||
      roleTeaser ||
      openingsBodyCollapsed(body) ||
      (reasons.includes("owner_teaser") && openingsBodyCollapsed(body));

    if (openingsBodyCollapsed(body) && !roleTeaser && !hasUrl) {
      defects.push({
        brand: slug,
        tab: "footprint",
        section: "footprint.openings",
        component: row.recordId,
        field: "Body",
        currentValue: body.slice(0, 120),
        defectType: "collapsed_openings_body",
        severity: "HIGH",
        proposedFix: "Rebuild structured openings Body (chips/location/meta/teaser) without raw URL",
        sourceSupport: "openings_card_contract",
        patchAllowed: true,
      });
      reasons.push("rebuild_structured_body");
    }

    if (needsStructuredRebuild || reasons.includes("rebuild_structured_body")) {
      const teaserForBody =
        fields["Case Summary Overview"] ||
        (reasons.includes("dedupe_brand_copy") ||
        /—\s*(property setting|public space lobby|exterior arrival|guest room)/i.test(cs) ||
        new RegExp(`${brandEsc}\\s+is an official\\s+${brandEsc}`, "i").test(cs)
          ? defaultTeaser
          : nz(cs) || defaultTeaser);
      fields.Body = rebuildOpeningsStructuredBody(row, brandName, sanitizeOwnerTeaser(teaserForBody, brandName));
      if (!fields["Case Summary Overview"] && /—\s*(property setting|public space lobby|exterior arrival|guest room)/i.test(cs)) {
        fields["Case Summary Overview"] = defaultTeaser;
      }
      if (!reasons.includes("rebuild_structured_body") && !reasons.includes("strip_raw_url") && !reasons.includes("owner_teaser") && !reasons.includes("dedupe_brand_copy")) {
        reasons.push("rebuild_structured_body");
      }
    } else if (hasUrl) {
      fields.Body = stripTrailingUrl(body);
    }

    // Ensure interpretation fixed even if only other fields change
    if (Object.keys(fields).length) {
      if (!fields["Case Summary Interpretation"] && /Confirm live affiliation/i.test(interp)) {
        fields["Case Summary Interpretation"] = ownerFacingInterpretation();
      }
      patches.push({
        recordId: row.recordId,
        slotKey: "footprint.openings",
        fields,
        reason: reasons.join("+") || "openings_fix",
      });
    }
  }

  // Delta duplicate Toronto openings — differentiate second card teaser (MEDIUM)
  if (slug === "delta-hotels-by-marriott") {
    const toronto = rows.filter(
      (r) =>
        r.slotKey === "footprint.openings" &&
        /Toronto Airport & Conference Centre/i.test(nz(r.title))
    );
    if (toronto.length >= 2) {
      const second = toronto[1];
      const first = toronto[0];
      const secondBlob = `${nz(second.body)}\n${nz(second.caseSummaryOverview)}`;
      const firstBlob = `${nz(first.body)}\n${nz(first.caseSummaryOverview)}`;
      const alreadyDiff =
        /meetings and conference demand|conference centre value|group and meetings/i.test(secondBlob) &&
        !/meetings and conference demand|conference centre value|group and meetings/i.test(firstBlob);
      const nearDuplicate =
        nz(second.caseSummaryOverview) &&
        nz(first.caseSummaryOverview) &&
        nz(second.caseSummaryOverview) === nz(first.caseSummaryOverview);
      const secondStructuredOk = !openingsBodyCollapsed(nz(second.body));
      if (!(alreadyDiff && secondStructuredOk) || nearDuplicate) {
        defects.push({
          brand: slug,
          tab: "footprint",
          section: "footprint.openings",
          component: second.recordId,
          field: "Body",
          currentValue: nz(second.body).slice(0, 120),
          defectType: "duplicate_property_card_weak_differentiation",
          severity: "MEDIUM",
          proposedFix: "Differentiate second Toronto card toward meetings/conference value",
          sourceSupport: "owner_facing_clarity",
          patchAllowed: true,
        });
        const teaser =
          "Delta Hotels Toronto Airport & Conference Centre is a useful International Reference for owners underwriting airport-adjacent full-service assets with meaningful meetings and conference demand. Use it to benchmark product standards, service delivery, meetings capacity, and capital scope for similar assets.";
        const body = rebuildOpeningsStructuredBody(second, brandName, teaser);
        const existing = patches.find((p) => p.recordId === second.recordId);
        if (existing) {
          existing.fields.Body = body;
          existing.fields["Case Summary Overview"] = teaser;
          existing.reason += "+toronto_meetings_diff";
        } else {
          patches.push({
            recordId: second.recordId,
            slotKey: "footprint.openings",
            fields: {
              Body: body,
              "Case Summary Overview": teaser,
              "Case Summary Interpretation": ownerFacingInterpretation(),
            },
            reason: "toronto_meetings_diff",
          });
        }
      }
    }
  }

  return { defects, patches };
}

function scanFlexContamination(slug, rows) {
  // Contrastive peer language is allowed (Stage 2A intentionally separates classic Four Points from Flex).
  // CRITICAL only when Flex is presented as self / proof / substitute without contrast framing.
  const hits = [];
  const contrastiveNear = new RegExp(
    [
      String.raw`\b(?:never|not|no|none|non[- ]target|held|outside)`,
      String.raw`|\b(?:distinct|distinctly|distinguish(?:es|ed|ing)?|differenti(?:ate|ates|ated|ation)|separation|separate|separates)`,
      String.raw`|\b(?:reject|wrong|do not|don't|unlike|versus|vs\.?|rather than|instead of|against|over|than|from)`,
      String.raw`|\b(?:alternatives?|peers?|compare|comparison|compared|choice|credibly|explicitly)`,
      String.raw`|\b(?:substitut(?:es|e|ing|ion)?|import(?:s|ed|ing)?|weaker|fails?|collapses?|CRITICAL)`,
    ].join(""),
    "i"
  );
  for (const row of rows) {
    const text = [
      row.title,
      row.body,
      row.caseSummaryOverview,
      row.caseSummaryBrandRelevance,
      row.caseSummaryOwnerObjective,
      row.caseSummaryInterpretation,
      row.caseSummaryTags,
    ]
      .map(nz)
      .join("\n");
    for (const m of text.matchAll(/\bFour Points Flex\b|\bFlex[- ]light\b/gi)) {
      const idx = m.index ?? 0;
      const window = text.slice(Math.max(0, idx - 140), idx + Math.min(m[0].length + 100, 180));
      if (contrastiveNear.test(window)) continue;
      hits.push({
        brand: slug,
        section: row.slotKey,
        sample: window.replace(/\s+/g, " ").trim().slice(0, 160),
      });
    }
    if (/fpx-|\/xf-|fourpointsexpress/i.test(nz(row.imageUrl))) {
      hits.push({ brand: slug, section: row.slotKey, sample: "flex image url" });
    }
  }
  return hits;
}

function crossBrandReview(loaded) {
  const distinctive = {
    "fairfield-by-marriott": [/fairfield/i, /select-service|select service/i, /courtyard/i],
    "four-points-by-sheraton": [/four points/i, /flex/i, /best available rate|reliable/i],
    "delta-hotels-by-marriott": [/delta hotels/i, /streamlined|simple made perfect|meetings|conference/i],
  };
  const pairs = [
    ["fairfield-by-marriott", "four-points-by-sheraton"],
    ["fairfield-by-marriott", "delta-hotels-by-marriott"],
    ["four-points-by-sheraton", "delta-hotels-by-marriott"],
  ];
  return pairs.map(([a, b]) => {
    const textA = loaded[a].map((r) => `${r.title}\n${r.body}`).join("\n");
    const textB = loaded[b].map((r) => `${r.title}\n${r.body}`).join("\n");
    const aHas = distinctive[a].filter((re) => re.test(textA)).length;
    const bHas = distinctive[b].filter((re) => re.test(textB)).length;
    const pass = aHas >= 1 && bHas >= 1;
    return {
      brandPair: `${a} vs ${b}`,
      semanticSimilarityRisk: pass ? "LOW" : "MEDIUM",
      genericReuseFindings: pass
        ? []
        : ["One brand lacks distinctive identity signals in Presentation corpus"],
      requiredFix: pass ? "None" : "Strengthen brand-specific owner language",
      distinctiveSignalCounts: { [a]: aHas, [b]: bHas },
      pass,
    };
  });
}

async function airtablePatch({ baseId, apiKey, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const payload = { ...fields };
  for (const f of FORBIDDEN_WRITE_FIELDS) delete payload[f];
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: payload, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH failed ${res.status}`);
  return json;
}

function writeReports(basename, json, md) {
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.writeFileSync(path.join(REPORTS, `${basename}.json`), JSON.stringify(json, null, 2));
  if (md) fs.writeFileSync(path.join(REPORTS, `${basename}.md`), md);
}

export async function runWave16aStage2cPostImageReview({ dryRun = true, argv = [] } = {}) {
  const flagCheck = parseWave16aStage2cFlags(argv);
  const apply = argv.includes("--apply") && !dryRun;

  const preflight = await runWave16aStage2bIdentityPreflight();
  if (!preflight.pass) {
    const stopped = {
      version: WAVE16A_STAGE2C_VERSION,
      pass: false,
      stopRecommended: true,
      readyStatement:
        preflight.liveActiveCount !== WAVE16A_PROTECTED_BASELINE_COUNT
          ? "wave16a_stage2c_blocked_active_baseline_regression"
          : READY_REMEDIATE,
      preflight,
    };
    writeReports("brand-explorer-wave16a-stage2c-post-image-review", stopped, `# STOPPED\n`);
    return stopped;
  }

  const loaded = {};
  const brandResults = [];
  const allDefects = [];
  const allPatches = [];

  for (const slug of WAVE16A_STAGE2B_APPROVED_SLUGS) {
    const id = WAVE16A_IDENTITIES[slug];
    const { rows } = await listPresentationRowsLight(id.recordId, id.exactBrandBasicsName);
    loaded[slug] = rows;

    const gallery = rows.filter((r) => String(r.slotKey || "").startsWith("materials.gallery."));
    const scenario = rows.filter((r) => String(r.slotKey || "").startsWith("overview.scenario."));
    const openings = rows.filter((r) => r.slotKey === "footprint.openings");
    const imageRows = [...gallery, ...scenario, ...openings].map((r) => ({
      slotKey: r.slotKey,
      title: r.title,
      imageUrl: r.imageUrl,
      recordId: r.recordId,
    }));
    const uniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: imageRows });
    const roleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: imageRows });
    const flexHits = scanFlexContamination(slug, rows);
    for (const h of flexHits) {
      allDefects.push({
        brand: slug,
        tab: "multiple",
        section: h.section,
        component: h.section,
        field: "visible_copy",
        currentValue: h.sample,
        defectType: "sibling_contamination:four_points_flex",
        severity: "CRITICAL",
        proposedFix: "Remove non-contrastive Flex framing",
        sourceSupport: "identity_hold",
        patchAllowed: true,
      });
    }

    const planned = plannedOpeningsPatches(slug, rows);
    allDefects.push(...planned.defects);
    allPatches.push(...planned.patches.map((p) => ({ ...p, brandSlug: slug })));

    const brandDefects = allDefects.filter((d) => d.brand === slug);
    const unpatchedBlocking = brandDefects.filter(
      (d) =>
        (d.severity === "CRITICAL" || d.severity === "HIGH") &&
        d.patchAllowed !== true
    );
    brandResults.push({
      brandSlug: slug,
      brandName: id.exactBrandBasicsName,
      recordId: id.recordId,
      brandStatus: preflight.targets.find((t) => t.slug === slug)?.brandStatus || "Under Review",
      gallery: gallery.filter((r) => r.imageUrl).length,
      scenario: scenario.filter((r) => r.imageUrl).length,
      openings: openings.filter((r) => r.imageUrl).length,
      uniquenessPass: uniqueness?.pass === true,
      roleMatchPass: roleMatch?.pass === true,
      flexContaminationCount: flexHits.length,
      defectCounts: {
        CRITICAL: brandDefects.filter((d) => d.severity === "CRITICAL").length,
        HIGH: brandDefects.filter((d) => d.severity === "HIGH").length,
        MEDIUM: brandDefects.filter((d) => d.severity === "MEDIUM").length,
        LOW: brandDefects.filter((d) => d.severity === "LOW").length,
      },
      patchesPlanned: planned.patches.length,
      // Projected readiness after allowed Stage 2C patches (not a Brand Status write).
      founderReviewReady:
        flexHits.length === 0 &&
        uniqueness?.pass === true &&
        roleMatch?.pass === true &&
        unpatchedBlocking.length === 0,
    });
  }

  const crossBrand = crossBrandReview(loaded);

  // Apply patches
  const writeAuditByBrand = Object.fromEntries(
    WAVE16A_STAGE2B_APPROVED_SLUGS.map((s) => [
      s,
      { contentPatches: 0, imageCaptionPatches: 0, visibilityPatches: 0, other: 0 },
    ])
  );
  const applyResults = [];
  if (apply) {
    if (!flagCheck.ok) {
      return {
        version: WAVE16A_STAGE2C_VERSION,
        pass: false,
        readyStatement: READY_REMEDIATE,
        reason: "missing_apply_flags",
        flagCheck,
      };
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    for (const patch of allPatches) {
      if (!WAVE16A_STAGE2B_APPROVED_SLUGS.includes(patch.brandSlug)) {
        throw new Error(`Refuse non-target patch ${patch.brandSlug}`);
      }
      if (patch.recordId === WAVE16A_FLEX_HOLD.recordId) {
        throw new Error("Refuse Flex write");
      }
      await airtablePatch({
        baseId,
        apiKey,
        recordId: patch.recordId,
        fields: patch.fields,
      });
      writeAuditByBrand[patch.brandSlug].contentPatches += 1;
      applyResults.push({
        brandSlug: patch.brandSlug,
        recordId: patch.recordId,
        reason: patch.reason,
        fields: Object.keys(patch.fields),
      });
      await sleep(280);
    }
  }

  // Post-apply re-check openings for remaining raw URL / confirm live
  let remainingUnsafe = 0;
  if (apply) {
    for (const slug of WAVE16A_STAGE2B_APPROVED_SLUGS) {
      const id = WAVE16A_IDENTITIES[slug];
      const { rows } = await listPresentationRowsLight(id.recordId, id.exactBrandBasicsName);
      for (const r of rows.filter((x) => x.slotKey === "footprint.openings")) {
        const blob = `${r.body}\n${r.caseSummaryInterpretation}\n${r.caseSummaryOverview}`;
        if (/https?:\/\/\S+/i.test(nz(r.body))) remainingUnsafe += 1;
        if (/Confirm live affiliation/i.test(blob)) remainingUnsafe += 1;
      }
      // refresh founder readiness after patches
      const br = brandResults.find((b) => b.brandSlug === slug);
      if (br) {
        const plannedLeft = plannedOpeningsPatches(slug, rows);
        br.founderReviewReady =
          plannedLeft.defects.filter((d) => d.severity === "CRITICAL" || d.severity === "HIGH").length === 0 &&
          br.uniquenessPass &&
          br.roleMatchPass &&
          br.flexContaminationCount === 0;
        br.postPatchDefects = plannedLeft.defects.length;
      }
    }
  }

  const universeAfter = await loadActiveUniverse({ includeDetails: false });
  const severityCounts = { CRITICAL: 0, HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const d of allDefects) severityCounts[d.severity] = (severityCounts[d.severity] || 0) + 1;

  const allFounderReady = brandResults.every((b) => b.founderReviewReady);
  const unresolvedCritical = allDefects.filter(
    (d) => d.severity === "CRITICAL" && d.patchAllowed !== true
  ).length;
  const flexTotal = brandResults.reduce((n, b) => n + b.flexContaminationCount, 0);
  const pass =
    preflight.pass &&
    universeAfter.totalCount === WAVE16A_PROTECTED_BASELINE_COUNT &&
    flexTotal === 0 &&
    crossBrand.every((c) => c.pass) &&
    unresolvedCritical === 0 &&
    brandResults.every((b) => b.uniquenessPass && b.roleMatchPass) &&
    (apply ? remainingUnsafe === 0 && allFounderReady : allFounderReady);

  const summary = {
    version: WAVE16A_STAGE2C_VERSION,
    wave16aVersion: WAVE16A_VERSION,
    stage: "post-image-review",
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    flagCheck,
    preflight,
    activeUniverseBefore: preflight.liveActiveCount,
    activeUniverseAfter: universeAfter.totalCount,
    defectCountsBySeverity: severityCounts,
    defects: allDefects,
    patchesPlanned: allPatches.map((p) => ({
      brandSlug: p.brandSlug,
      recordId: p.recordId,
      reason: p.reason,
      fields: Object.keys(p.fields),
    })),
    applyResults,
    writeAuditByBrand,
    writeAudit: {
      recentMomentumWrites: 0,
      brandStatusWrites: 0,
      releaseWrites: 0,
      companyValidatedWrites: 0,
      companyValidationDateWrites: 0,
      brandVerifiedWrites: 0,
      censusWrites: 0,
      active62Writes: 0,
      fourPointsFlexWrites: 0,
      wave16bWrites: 0,
      remainingWave16aWrites: 0,
      nonTargetWrites: 0,
      founderVisualReviewPassWrites: 0,
    },
    brands: brandResults,
    crossBrand,
    imageAudits: {
      uniqueness: brandResults.every((b) => b.uniquenessPass),
      roleMatch: brandResults.every((b) => b.roleMatchPass),
      galleryScenarioOpenings: brandResults.every(
        (b) => b.gallery >= 6 && b.scenario >= 3 && b.openings >= 3
      ),
    },
    remainingUnsafeOpeningsLanguage: remainingUnsafe,
    deferred: ["Recent Momentum intentionally deferred"],
    recommendedNextStage: "founder visual review (no Momentum / no promote / no release)",
    pass,
    readyStatement: !apply
      ? READY_DRY
      : pass
        ? READY_PASS
        : READY_REMEDIATE,
  };

  // Outputs
  writeReports("brand-explorer-wave16a-stage2c-rendered-defects", {
    generatedAt: summary.generatedAt,
    defectCountsBySeverity: severityCounts,
    defects: allDefects,
  });
  writeReports("brand-explorer-wave16a-stage2c-write-audit", {
    dryRun: !apply,
    writeAudit: summary.writeAudit,
    writeAuditByBrand,
    applyResults,
  });

  const crossMd = [
    `# Wave 16A Stage 2C — Cross-brand semantic review`,
    ``,
    ...crossBrand.map(
      (c) =>
        `- **${c.brandPair}**: risk=${c.semanticSimilarityRisk} · pass=${c.pass} · fix=${c.requiredFix}`
    ),
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave16a-stage2c-cross-brand-semantic-review.md"), crossMd);

  for (const b of brandResults) {
    const md = [
      `# Wave 16A Stage 2C — ${b.brandName}`,
      ``,
      `- Brand Status: **${b.brandStatus}**`,
      `- Gallery/Scenario/Openings images: **${b.gallery}/6 · ${b.scenario}/3 · ${b.openings}/3**`,
      `- Uniqueness: **${b.uniquenessPass}** · Role-match: **${b.roleMatchPass}**`,
      `- Flex contamination: **${b.flexContaminationCount}**`,
      `- Defects C/H/M/L: **${b.defectCounts.CRITICAL}/${b.defectCounts.HIGH}/${b.defectCounts.MEDIUM}/${b.defectCounts.LOW}**`,
      `- Patches planned: **${b.patchesPlanned}**`,
      `- Founder-review ready: **${b.founderReviewReady}**`,
      ``,
    ].join("\n");
    fs.writeFileSync(
      path.join(REPORTS, `brand-explorer-wave16a-stage2c-${b.brandSlug}.md`),
      md
    );
  }

  const mainMd = [
    `# Wave 16A Stage 2C — Post-Image Review`,
    ``,
    `- Ready: \`${summary.readyStatement}\``,
    `- Mode: **${apply ? "APPLY" : "DRY-RUN"}**`,
    `- Active universe: **${summary.activeUniverseBefore} → ${summary.activeUniverseAfter}**`,
    `- Defects C/H/M/L: **${severityCounts.CRITICAL}/${severityCounts.HIGH}/${severityCounts.MEDIUM}/${severityCounts.LOW}**`,
    `- Patches ${apply ? "applied" : "planned"}: **${allPatches.length}**`,
    `- Recent Momentum writes: **0**`,
    `- Flex contamination: **${brandResults.reduce((n, b) => n + b.flexContaminationCount, 0)}**`,
    `- Cross-brand pass: **${crossBrand.every((c) => c.pass)}**`,
    ``,
    `## Brands`,
    ``,
    ...brandResults.map(
      (b) =>
        `- **${b.brandName}**: founderReady=${b.founderReviewReady} · patches=${b.patchesPlanned} · defects C/H/M/L=${b.defectCounts.CRITICAL}/${b.defectCounts.HIGH}/${b.defectCounts.MEDIUM}/${b.defectCounts.LOW}`
    ),
    ``,
    `## Deferred`,
    ``,
    `- Recent Momentum`,
    ``,
    `## Next`,
    ``,
    `- Founder visual review`,
    `- Do not write Momentum / promote / release / start other Wave 16A brands`,
    ``,
  ].join("\n");

  writeReports("brand-explorer-wave16a-stage2c-post-image-review", summary, mainMd);
  fs.mkdirSync(DOCS, { recursive: true });
  fs.writeFileSync(path.join(DOCS, "brand-explorer-wave16a-stage2c-post-image-review.md"), mainMd);

  return summary;
}
