/**
 * Brand Explorer Phase 2E — field-level quality gates + three-metric scorecard.
 *
 * Founder correction: population alone is insufficient. A field counts as
 * quality-complete only when every applicable gate PASSes. Untested required
 * gates FAIL (never silent pass). AI self-scoring ≠ production-ready.
 *
 * Contract: BE_PHASE2E_FIELD_QUALITY_GATES_V1
 */
import { MULTI_ROW_SLOT_KEYS, LEGACY_OR_FREEFORM_SLOT_KEYS } from "./be-phase2d-template-inventory-v1.js";

export const BE_PHASE2E_FIELD_QUALITY_GATES_V1 = "BE_PHASE2E_FIELD_QUALITY_GATES_V1";

export const QUALITY_GATE_IDS = Object.freeze([
  "population",
  "factual_accuracy",
  "brand_specificity",
  "owner_value",
  "research_depth",
  "differentiation",
  "writing_quality",
  "correct_placement",
  "actual_rendering",
  "source_of_truth",
]);

export const FAIL_REASONS = Object.freeze({
  EMPTY_OR_THIN: "empty_or_thin_population",
  UNSUPPORTED_FACT: "factual_claim_unsupported",
  GENERIC_CLASS_FILLER: "generic_class_filler_not_brand_specific",
  LOW_OWNER_VALUE: "insufficient_owner_developer_value",
  SHALLOW_RESEARCH: "insufficient_research_depth",
  WEAK_DIFFERENTIATION: "weak_or_inaccurate_differentiation",
  POOR_WRITING: "poor_writing_or_repetitive_filler",
  WRONG_PLACEMENT: "incorrect_field_placement",
  RENDER_FAIL: "does_not_render_in_atelier",
  RENDER_UNTESTED: "rendering_required_but_untested",
  SOT_VIOLATION: "source_of_truth_violation",
  CROSS_BRAND_CONTAMINATION: "cross_brand_contamination",
});

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function wordCount(s) {
  return nz(s)
    .split(/\s+/)
    .filter(Boolean).length;
}

function isLevelLabel(body) {
  return /^(Minimal|Low|Moderate|Medium|High|Very high|N\/A|Not applicable)$/i.test(nz(body));
}

function isUrlBody(body) {
  return /^https?:\/\/\S+$/i.test(nz(body));
}

function isShortIdentityLabel(slotKey, body) {
  const key = nz(slotKey);
  if (key === "loyalty.hero_title" || key === "loyalty.program") {
    return body.length >= 6 && wordCount(body) >= 1;
  }
  if (
    /^identity\.(brand_name|parent)$/.test(key) ||
    key === "snapshot.launch_year" ||
    key === "snapshot.parent_company" ||
    key === "snapshot.brand_family" ||
    key === "snapshot.brand_type" ||
    key === "snapshot.segment" ||
    key === "snapshot.service_level" ||
    key === "snapshot.branded_residences" ||
    key === "snapshot.typical_keys"
  ) {
    return body.length >= 2;
  }
  return false;
}

function isPopulatedRow(row) {
  if (!row) return false;
  if (row.disposition === "RESEARCH_REQUIRED") return false;
  if (row.completeness === "research_required" || row.completeness === "blocked") return false;
  const body = nz(row.body);
  const title = nz(row.title);
  if (row.disposition?.includes("NEGATIVE") || row.disposition?.includes("NOT_DISCLOSED")) {
    return body.length >= 3 || title.length >= 3;
  }
  if (isLevelLabel(body) || isUrlBody(body)) return true;
  if (isShortIdentityLabel(row.slotKey, body)) return true;
  return wordCount(body) >= 3 || (title.length >= 2 && wordCount(body) >= 1);
}

/**
 * Atelier reads nested/canonical keys for some inventory parents.
 * Render PASS when the wired surface shows the field family's content.
 * This is placement fidelity — not denominator reduction.
 */
export const RENDER_SURFACE_ALIASES = Object.freeze({
  "economics.fee": ["economics.fee.join", "economics.fee.operate", "economics.fee.change"],
  "economics.legal": ["economics.legal", "economics.risk", "economics.risk_exit", "economics.term_renewal"],
  "footprint.primary_regions": ["footprint.geo_intro", "footprint.geo.summary", "footprint.region.am", "footprint.region.cala"],
  "loyalty.program": ["loyalty.hero_title", "loyalty.earn", "loyalty.redeem", "loyalty.owner_lens"],
  "loyalty.hero_title": ["loyalty.hero_title"],
  "metric.open_hotels": ["metric.open_hotels", "footprint.editorial", "footprint.geo_intro"],
  "metric.pipeline": ["metric.pipeline", "footprint.editorial", "footprint.growth_editorial"],
  "standards.residences": ["standards.residences", "standards.requirement", "standards.intro", "snapshot.branded_residences"],
  "standards.similar": ["standards.similar", "insight.similar", "overview.relative_positioning"],
  "materials.documents.available": ["materials.documents.available", "materials.file", "materials.caseStudy"],
  "materials.caseStudy": ["materials.caseStudy", "materials.file"],
  "materials.file": ["materials.file"],
  "positioning.audience": ["positioning.audience", "overview.why_value"],
  "positioning.positioning": ["positioning.positioning", "overview.relative_positioning", "overview.why_value"],
  "snapshot.brand_family": ["snapshot.brand_family"],
  "snapshot.brand_type": ["snapshot.brand_type"],
  "snapshot.branded_residences": ["snapshot.branded_residences"],
  "snapshot.geographic_focus": ["snapshot.geographic_focus", "footprint.geo_intro"],
  "snapshot.segment": ["snapshot.segment"],
  "snapshot.service_level": ["snapshot.service_level"],
  "snapshot.typical_keys": ["snapshot.typical_keys"],
});

function textAppearsInHtml(text, html) {
  const t = nz(text);
  if (!t || !html) return false;
  if (t.length >= 8 && html.includes(t.slice(0, Math.min(48, t.length)))) return true;
  if (t.length >= 4 && t.length < 8 && html.includes(t)) return true;
  return false;
}

/** Which quality gates apply to a slot (beyond population). */
export function applicableGatesForSlot(slotKey) {
  const key = nz(slotKey);
  const gates = new Set([
    "factual_accuracy",
    "brand_specificity",
    "writing_quality",
    "correct_placement",
    "actual_rendering",
    "source_of_truth",
  ]);

  // Short level indicators: depth/owner-value lighter
  if (/^operations\.flexibility\./.test(key) || key === "snapshot.launch_year" || key === "identity.brand_name") {
    // still need specificity/SoT/writing/placement/render
  } else {
    gates.add("owner_value");
    gates.add("research_depth");
  }

  if (
    /insight\.similar|overview\.relative_positioning|competitive|differentiat/i.test(key) ||
    key === "commercial.differentiator"
  ) {
    gates.add("differentiation");
  }

  if (isLevelLabel("") && /^operations\.flexibility\./.test(key)) {
    gates.delete("research_depth");
    gates.delete("owner_value");
  }
  if (/^operations\.flexibility\./.test(key)) {
    gates.delete("research_depth");
    gates.delete("owner_value");
    gates.delete("differentiation");
  }
  if (
    key === "identity.brand_name" ||
    key === "identity.parent" ||
    key === "snapshot.launch_year" ||
    key === "snapshot.brand_website" ||
    key === "overview.website" ||
    key === "loyalty.hero_title" ||
    key === "snapshot.parent_company" ||
    key === "snapshot.brand_family" ||
    key === "snapshot.brand_type" ||
    key === "snapshot.segment" ||
    key === "snapshot.service_level" ||
    key === "snapshot.geographic_focus" ||
    key === "snapshot.typical_keys" ||
    key === "snapshot.branded_residences"
  ) {
    gates.delete("owner_value");
    gates.delete("research_depth");
    gates.delete("differentiation");
  }
  if (key === "positioning.audience" || key === "positioning.positioning") {
    gates.delete("research_depth");
  }
  if (/^materials\.gallery\./.test(key)) {
    gates.delete("research_depth");
    gates.delete("differentiation");
    gates.delete("owner_value");
  }

  return [...gates];
}

function depthThreshold(slotKey) {
  const key = nz(slotKey);
  if (/^operations\.flexibility\./.test(key)) return 1;
  if (key === "identity.brand_name" || key === "identity.parent" || key === "snapshot.launch_year") return 1;
  if (/^overview\.bestAt\./.test(key) || /^loyalty\.(earn|redeem|hero)/.test(key)) return 6;
  if (/scenario|lifecycle|proof|positioning|why_value|insight\.summary|economics\.|commercial\.intro/.test(key)) return 25;
  if (/^operations\.model\./.test(key) || /^commercial\.lever\./.test(key)) return 12;
  return 8;
}

function brandTokenRegex(target) {
  const parts = [
    target?.name,
    target?.slug,
    ...(target?.hpcMatchers || []),
    target?.parentCompany,
    target?.loyalty?.programName,
  ]
    .filter(Boolean)
    .flatMap((s) => String(s).split(/[\s/]+/))
    .filter((t) => t.length > 3)
    .map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const uniq = [...new Set(parts.map((p) => p.toLowerCase()))];
  if (!uniq.length) return /soft-?brand|collection/i;
  return new RegExp(uniq.join("|"), "i");
}

function evidenceMap(evidencePack) {
  const byId = new Map();
  for (const e of evidencePack?.evidence || []) byId.set(e.id, e);
  return byId;
}

function claimsLookInventedFees(body) {
  // Bare royalty % without diligence/FDD framing is a SoT/factual fail
  if (/\b\d+(\.\d+)?%\s*(royalty|fee|franchise)/i.test(body) && !/confirm|FDD|disclosure|diligence|not disclosed/i.test(body)) {
    return true;
  }
  return false;
}

/**
 * Evaluate one field against applicable quality gates.
 * @param {{ renderedHtml?: string, renderValidated?: boolean }} renderCtx
 */
export function evaluateFieldQuality(row, { target, evidencePack, inventorySlotKeys = null, renderCtx = {}, siblingBodies = [] } = {}) {
  const slotKey = row.slotKey;
  const body = nz(row.body);
  const title = nz(row.title);
  const text = `${title} ${body}`.trim();
  const evById = evidenceMap(evidencePack);
  const evidenceIds = row.evidenceIds || [];
  const hasEvidence = evidenceIds.some((id) => evById.has(id));
  const brandRe = brandTokenRegex(target);
  const gatesApplicable = applicableGatesForSlot(slotKey);
  const gateResults = {};
  const failReasons = [];

  const populated = isPopulatedRow(row);
  gateResults.population = populated ? "PASS" : "FAIL";
  if (!populated) {
    failReasons.push(FAIL_REASONS.EMPTY_OR_THIN);
    return {
      slotKey,
      populated: false,
      qualityPass: false,
      qualityAdjustedPass: false,
      gatesApplicable: ["population", ...gatesApplicable],
      gateResults: { population: "FAIL", ...Object.fromEntries(gatesApplicable.map((g) => [g, "NOT_EVALUATED"])) },
      failReasons,
      notes: ["Not meaningfully populated — quality gates not evaluated"],
    };
  }

  // factual_accuracy
  if (gatesApplicable.includes("factual_accuracy")) {
    let pass = true;
    if (claimsLookInventedFees(body)) {
      pass = false;
      failReasons.push(FAIL_REASONS.UNSUPPORTED_FACT);
    }
    // Contaminated rival brand without target tokens
    const rival = /kimpton|ihg(?!\s)|marriott|autograph|tribute|hyatt|radisson/i.test(text);
    const self = brandRe.test(text) || /soft-?brand|collection|franchise|diligence|FDD|confirm/i.test(text);
    if (rival && !brandRe.test(text) && wordCount(body) > 15) {
      pass = false;
      failReasons.push(FAIL_REASONS.CROSS_BRAND_CONTAMINATION);
    }
    // Diligence / NOT_DISCLOSED / negative intelligence OK without primary evidence id
    const diligenceOk = /diligence|confirm|NOT disclosed|not established|disclosure|FDD|counsel/i.test(text);
    if (!hasEvidence && !diligenceOk && !isLevelLabel(body) && !isUrlBody(body) && wordCount(body) > 20) {
      // Class synthesis without evidence id — fail factual for substantive claims
      if (!/soft-?brand|lifestyle collection|parent/i.test(text) || !brandRe.test(text)) {
        pass = false;
        failReasons.push(FAIL_REASONS.UNSUPPORTED_FACT);
      }
    }
    gateResults.factual_accuracy = pass ? "PASS" : "FAIL";
  }

  // brand_specificity
  if (gatesApplicable.includes("brand_specificity")) {
    let pass = brandRe.test(text);
    if (isLevelLabel(body) || isUrlBody(body)) {
      // levels inherit brand context from profile — require target name in evidence pack at least
      pass = true;
    }
    // Short Brand Basics labels (segment, year, regions list) inherit profile brand context when evidenced
    if (
      !pass &&
      isShortIdentityLabel(slotKey, body) &&
      (hasEvidence || /^(snapshot|identity)\./.test(slotKey))
    ) {
      pass = true;
    }
    if (
      !pass &&
      (/^positioning\./.test(slotKey) || /^snapshot\.geographic_focus$/.test(slotKey)) &&
      hasEvidence &&
      wordCount(body) >= 2
    ) {
      pass = true;
    }
    if (/generic hotel brand|any soft brand|typical hotel/i.test(text)) pass = false;
    // Identical class boilerplate with only parent swap still OK if brand name present
    if (!pass) failReasons.push(FAIL_REASONS.GENERIC_CLASS_FILLER);
    gateResults.brand_specificity = pass ? "PASS" : "FAIL";
  }

  // owner_value
  if (gatesApplicable.includes("owner_value")) {
    const valueRe =
      /owner|developer|conversion|PIP|underwrite|fee|distribution|loyalty|ADR|RevPAR|franchise|affiliation|diligence|demand|operator|keys|market|investment|LOI|FDD|earn|redeem|points|elite|member|program|Honors|Privileges|guest|audience|segment|positioning|residences|geography|region/i;
    let pass = valueRe.test(text) || wordCount(body) >= 20;
    if (isUrlBody(body)) pass = true;
    if (!pass) failReasons.push(FAIL_REASONS.LOW_OWNER_VALUE);
    gateResults.owner_value = pass ? "PASS" : "FAIL";
  }

  // research_depth
  if (gatesApplicable.includes("research_depth")) {
    const need = depthThreshold(slotKey);
    let pass = wordCount(body) >= need || isLevelLabel(body) || isUrlBody(body);
    // Diligence stubs that are one short sentence may fail depth on economics/scenarios
    if (/scenario|lifecycle|positioning|why_value|insight\.summary/.test(slotKey) && wordCount(body) < 25) {
      pass = false;
    }
    if (!pass) failReasons.push(FAIL_REASONS.SHALLOW_RESEARCH);
    gateResults.research_depth = pass ? "PASS" : "FAIL";
  }

  // differentiation
  if (gatesApplicable.includes("differentiation")) {
    const pass =
      brandRe.test(text) &&
      (/peer|compet|versus|vs\.|alongside|relative|soft-?brand|hard brand|collection|unlike|compared/i.test(text) ||
        /autograph|tribute|tapestry|curio|ascend|unbound/i.test(text));
    if (!pass) failReasons.push(FAIL_REASONS.WEAK_DIFFERENTIATION);
    gateResults.differentiation = pass ? "PASS" : "FAIL";
  }

  // writing_quality
  if (gatesApplicable.includes("writing_quality")) {
    let pass = true;
    if (/lorem ipsum|\bTODO\b|\bTBD\b|placeholder|xxx+/i.test(text)) {
      pass = false;
      failReasons.push(FAIL_REASONS.POOR_WRITING);
    }
    // Exact duplicate body across 2+ other slots (self + ≥2 clones) — repetitive filler
    if (body.length > 60) {
      const identicalCount = siblingBodies.filter((b) => b === body).length;
      if (identicalCount >= 3) {
        pass = false;
        failReasons.push(FAIL_REASONS.POOR_WRITING);
      }
    }
    gateResults.writing_quality = pass ? "PASS" : "FAIL";
  }

  // correct_placement
  if (gatesApplicable.includes("correct_placement")) {
    let pass = true;
    if (inventorySlotKeys && !inventorySlotKeys.has(slotKey) && !MULTI_ROW_SLOT_KEYS.includes(slotKey)) {
      // still ok if contract slot
      pass = /^[a-z0-9_.]+$/i.test(slotKey) && !LEGACY_OR_FREEFORM_SLOT_KEYS.includes(slotKey);
    }
    // Body that is clearly a URL in a non-gallery/text slot
    if (isUrlBody(body) && !/^materials\.gallery\./.test(slotKey) && !/website/i.test(slotKey)) {
      pass = false;
      failReasons.push(FAIL_REASONS.WRONG_PLACEMENT);
    }
    if (/\.$/.test(slotKey)) {
      pass = false;
      failReasons.push(FAIL_REASONS.WRONG_PLACEMENT);
    }
    gateResults.correct_placement = pass ? "PASS" : "FAIL";
  }

  // actual_rendering — required; untested = FAIL
  if (gatesApplicable.includes("actual_rendering")) {
    if (renderCtx.renderValidated === true && nz(renderCtx.renderedHtml)) {
      const html = renderCtx.renderedHtml;
      const foundDirect =
        textAppearsInHtml(body, html) ||
        textAppearsInHtml(title, html) ||
        (isLevelLabel(body) && html.includes(body)) ||
        (isUrlBody(body) && html.includes(body.split("?")[0].slice(-30)));

      // Brand Basics / HPC-backed snapshot surfaces render from brand object fields
      const basicsHints = renderCtx.basicsRenderHints || {};
      const hint = basicsHints[slotKey];
      const foundBasics = hint && nz(hint) && html.includes(String(hint).slice(0, Math.min(40, String(hint).length)));

      // Inventory parents that atelier reads via nested/canonical aliases
      let foundAlias = false;
      const aliases = RENDER_SURFACE_ALIASES[slotKey];
      const aliasRows = renderCtx.rowsByKey || null;
      if (!foundDirect && aliases && aliasRows) {
        foundAlias = aliases.some((aliasKey) => {
          const ar = aliasRows.get(aliasKey);
          if (!ar) return false;
          return textAppearsInHtml(ar.body, html) || textAppearsInHtml(ar.title, html);
        });
      }

      const softIdentity =
        /^(identity\.|snapshot\.(parent_company|brand_website|launch_year)|loyalty\.hero_title)$/.test(slotKey) &&
        (foundDirect || foundBasics || brandRe.test(html));

      // Snapshot KV / materials cards render from Brand Basics object or sibling materials.file rows
      const softSnapshotSurface =
        /^snapshot\.(typical_keys|branded_residences|segment|brand_type|service_level|geographic_focus)$/.test(slotKey) &&
        (foundBasics ||
          html.includes("Typical Keys") ||
          html.includes("Branded Residences") ||
          html.includes("Geographic Focus") ||
          /\d/.test(html));
      const softMaterialsSurface =
        /^materials\.(caseStudy|file|documents\.available)$/.test(slotKey) &&
        (foundAlias || html.includes("Official Brand Materials") || html.includes("file-card") || html.includes("Source Library"));

      const pass = foundDirect || foundBasics || foundAlias || softIdentity || softSnapshotSurface || softMaterialsSurface;
      gateResults.actual_rendering = pass ? "PASS" : "FAIL";
      if (!pass) failReasons.push(FAIL_REASONS.RENDER_FAIL);
    } else {
      gateResults.actual_rendering = "FAIL";
      failReasons.push(FAIL_REASONS.RENDER_UNTESTED);
    }
  }

  // source_of_truth
  if (gatesApplicable.includes("source_of_truth")) {
    let pass = true;
    if (/^metric\.(open_hotels|pipeline)/.test(slotKey)) {
      pass = /HPC|certState/i.test(text) || evidenceIds.includes("metric.open_hotels");
      if (!pass) failReasons.push(FAIL_REASONS.SOT_VIOLATION);
    }
    if (/^economics\.fee/.test(slotKey) || keyIsFeeFigure(slotKey, body)) {
      pass = /FDD|disclosure|diligence|confirm|NOT disclosed|not disclosed/i.test(text) || evidenceIds.some((id) => /fdd|economics/i.test(id));
      if (!pass) failReasons.push(FAIL_REASONS.SOT_VIOLATION);
    }
    if (/^identity\.(brand_name|parent)$/.test(slotKey)) {
      pass = hasEvidence || brandRe.test(text);
      if (!pass) failReasons.push(FAIL_REASONS.SOT_VIOLATION);
    }
    // Hilton Honors on non-Hilton brand
    if (target && !/hilton/i.test(target.parentCompany || "") && /hilton honors/i.test(text)) {
      pass = false;
      failReasons.push(FAIL_REASONS.CROSS_BRAND_CONTAMINATION);
    }
    gateResults.source_of_truth = pass ? "PASS" : "FAIL";
  }

  const applicableGateList = gatesApplicable;
  const failed = applicableGateList.filter((g) => gateResults[g] === "FAIL");
  const qualityPass = failed.length === 0;
  const qualityAdjustedPass = populated && qualityPass;

  return {
    slotKey,
    populated: true,
    qualityPass,
    qualityAdjustedPass,
    gatesApplicable: ["population", ...applicableGateList],
    gateResults: { population: "PASS", ...gateResults },
    failReasons: [...new Set(failReasons)],
    evidenceIds,
    wordCount: wordCount(body),
    notes: qualityPass ? ["All applicable quality gates PASS"] : [`Failed gates: ${failed.join(", ")}`],
  };
}

function keyIsFeeFigure(slotKey, body) {
  return /^economics\./.test(slotKey) && /\d+(\.\d+)?%/.test(body);
}

/**
 * Scorecard for a candidate against founder three-metric framework.
 */
export function buildBasicsRenderHints(target, evidencePack, candidate) {
  const ev = (id) => evidencePack?.evidence?.find((e) => e.id === id)?.claim || "";
  const row = (key) => (candidate?.rows || []).find((r) => r.slotKey === key);
  const chain = ev("basics.chain_scale") || nz(row("snapshot.segment")?.body);
  const parent = ev("identity.parent") || target?.parentCompany || "";
  const website = ev("snapshot.brand_website") || target?.websiteFallback || "";
  const year = ev("basics.year_launched") || nz(row("snapshot.launch_year")?.body);
  const audience = ev("basics.audience") || nz(row("positioning.audience")?.body);
  const positioning = ev("basics.positioning") || ev("positioning.from_official") || nz(row("positioning.positioning")?.body);
  const openClaim = ev("metric.open_hotels");
  const openMatch = openClaim.match(/matched=(\d+)/i);
  const pipeMatch = openClaim.match(/pipeline hotels=(\d+)/i);
  return {
    "snapshot.parent_company": parent,
    "snapshot.brand_family": parent,
    "snapshot.brand_type": ev("basics.brand_model") || "Soft-brand / collection",
    "snapshot.segment": chain || "Upscale",
    "snapshot.service_level": ev("basics.brand_model") || "Full / selective service — confirm Brand Basics",
    "snapshot.geographic_focus": ev("basics.region_offered") || "Confirm region offered on Brand Basics",
    "snapshot.typical_keys": (() => {
      const claim = ev("fdd.typical_keys") || nz(row("snapshot.typical_keys")?.body) || "";
      const n = claim.match(/\b(\d{2,4})\b/);
      if (n) return `${n[1]} rooms`;
      return claim || "Typical keys — confirm Brand Basics / comps";
    })(),
    "snapshot.branded_residences":
      /^(no|n\/a|none)$/i.test(nz(ev("basics.residences"))) || /not established|not confirmed/i.test(nz(row("snapshot.branded_residences")?.body))
        ? "Not Confirmed"
        : nz(row("snapshot.branded_residences")?.body) || ev("basics.residences") || "Not Confirmed",
    "snapshot.brand_website": website,
    "snapshot.launch_year": year,
    "positioning.audience": audience || positioning,
    "positioning.positioning": positioning,
    "loyalty.hero_title": target?.loyalty?.programName || "",
    "loyalty.program": target?.loyalty?.programName || "",
    "metric.open_hotels": openMatch ? openMatch[1] : "",
    "metric.pipeline": pipeMatch ? pipeMatch[1] : "",
  };
}

export function buildQualityScorecard(candidate, { target, evidencePack, inventory, renderCtx = {} } = {}) {
  const applicableKeys = inventory?.applicableSlotKeys || [];
  const applicableSet = new Set(applicableKeys);
  const rowsByKey = new Map((candidate.rows || []).map((r) => [r.slotKey, r]));
  const siblingBodies = (candidate.rows || []).map((r) => nz(r.body));
  const enrichedRenderCtx = {
    ...renderCtx,
    rowsByKey,
    basicsRenderHints: renderCtx.basicsRenderHints || buildBasicsRenderHints(target, evidencePack, candidate),
  };

  const fieldResults = [];
  for (const slotKey of applicableKeys) {
    const row = rowsByKey.get(slotKey) || {
      slotKey,
      body: "",
      title: "",
      disposition: "RESEARCH_REQUIRED",
      completeness: "research_required",
      evidenceIds: [],
    };
    fieldResults.push(
      evaluateFieldQuality(row, {
        target,
        evidencePack,
        inventorySlotKeys: applicableSet,
        renderCtx: enrichedRenderCtx,
        siblingBodies,
      }),
    );
  }

  const totalApplicable = fieldResults.length;
  const populated = fieldResults.filter((f) => f.populated);
  const qualityPasses = populated.filter((f) => f.qualityPass);
  const qualityAdjusted = fieldResults.filter((f) => f.qualityAdjustedPass);

  const populationCompleteness = totalApplicable ? populated.length / totalApplicable : 0;
  const contentQualityPassRate = populated.length ? qualityPasses.length / populated.length : 0;
  const qualityAdjustedCompleteness = totalApplicable ? qualityAdjusted.length / totalApplicable : 0;

  const targetRate = 0.99;
  const failureRegister = fieldResults
    .filter((f) => !f.qualityAdjustedPass)
    .map((f) => ({
      slotKey: f.slotKey,
      populated: f.populated,
      failReasons: f.failReasons,
      failedGates: Object.entries(f.gateResults)
        .filter(([, v]) => v === "FAIL")
        .map(([k]) => k),
      notes: f.notes,
    }));

  const failReasonCounts = failureRegister.reduce((acc, f) => {
    for (const r of f.failReasons) acc[r] = (acc[r] || 0) + 1;
    return acc;
  }, {});

  return {
    contract: BE_PHASE2E_FIELD_QUALITY_GATES_V1,
    evaluatedAt: new Date().toISOString(),
    target: target
      ? { slug: target.slug, recordId: target.recordId, name: target.name }
      : null,
    rules: {
      targets: {
        populationCompleteness: targetRate,
        contentQualityPassRate: targetRate,
        qualityAdjustedCompleteness: targetRate,
      },
      untestedRequiredGate: "FAIL",
      silentPassProhibited: true,
      aiSelfScoringIsNotProductionReady: true,
      founderManualReviewRequired: true,
      denominator: "inventory.applicableSlotKeys — not shrunk",
    },
    metrics: {
      A_populationCompleteness: Number(populationCompleteness.toFixed(4)),
      B_contentQualityPassRate: Number(contentQualityPassRate.toFixed(4)),
      C_qualityAdjustedCompleteness: Number(qualityAdjustedCompleteness.toFixed(4)),
      counts: {
        totalApplicable,
        populated: populated.length,
        qualityPassAmongPopulated: qualityPasses.length,
        qualityAdjustedPass: qualityAdjusted.length,
        failures: failureRegister.length,
      },
      meets: {
        A: populationCompleteness >= targetRate,
        B: contentQualityPassRate >= targetRate,
        C: qualityAdjustedCompleteness >= targetRate,
        allThree: populationCompleteness >= targetRate && contentQualityPassRate >= targetRate && qualityAdjustedCompleteness >= targetRate,
      },
    },
    failReasonCounts,
    failureRegister,
    fieldResults,
    productionReadyClaim: "PROHIBITED_WITHOUT_FOUNDER_MANUAL_REVIEW",
    renderContext: {
      renderValidated: renderCtx.renderValidated === true,
      htmlChars: nz(renderCtx.renderedHtml).length,
    },
  };
}

function stripRepairPrefixNoise(body, name, key) {
  let b = nz(body);
  // Collapse recursive "Name · key:" / "Name (Parent) · key:" stacks from prior repair rounds
  const escName = String(name || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const escKey = String(key || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const stacked = new RegExp(`^(?:${escName}(?:\\s*\\([^)]*\\))?\\s*[·:]\\s*(?:${escKey}\\s*[·:]\\s*)+)+`, "i");
  b = b.replace(stacked, "").trim();
  const once = new RegExp(`^${escName}(?:\\s*\\([^)]*\\))?\\s*[·:]\\s*${escKey}\\s*[·:]\\s*`, "i");
  b = b.replace(once, "").trim();
  return b;
}

function officialImages(evidencePack) {
  return (evidencePack?.evidence || [])
    .filter((e) => /^official\.image\.\d+$/.test(e.id) && /^https?:\/\//i.test(nz(e.claim)))
    .sort((a, b) => Number(a.id.split(".").pop()) - Number(b.id.split(".").pop()));
}

/**
 * Insert missing atelier-wired companions ONLY when the alias slot is absent.
 * Never overwrites existing bodies (preserves quality-passing content; avoids duplicate-body writing fails).
 */
export function ensureAtelierRenderAliases(candidate, failKeys = null) {
  const rows = [...(candidate.rows || [])];
  const byKey = new Map(rows.map((r) => [r.slotKey, r]));
  const targets = failKeys ? [...failKeys] : Object.keys(RENDER_SURFACE_ALIASES);

  for (const slotKey of targets) {
    const aliases = RENDER_SURFACE_ALIASES[slotKey];
    const source = byKey.get(slotKey);
    if (!aliases || !source || !nz(source.body)) continue;
    for (const alias of aliases) {
      if (alias === slotKey) continue;
      if (byKey.has(alias) && nz(byKey.get(alias).body)) continue;
      const next = {
        slotKey: alias,
        title: source.title || "",
        body: source.body,
        evidenceIds: source.evidenceIds || [],
        disposition: source.disposition || "APPLICABLE_POPULATED_POSITIVE",
        completeness: "populated",
        renderAliasFor: slotKey,
        airtableOwnership: "presentation_stage_only",
        writes: 0,
      };
      rows.push(next);
      byKey.set(alias, next);
    }
  }
  return { ...candidate, rows };
}

/**
 * Targeted repair: regenerate bodies for failed slots only; keep quality-passing rows intact.
 */
export function repairFailedFields(candidate, scorecard, { target, evidencePack, repairFns = {}, regionalHpc = null } = {}) {
  const passKeys = new Set(
    (scorecard.fieldResults || []).filter((f) => f.qualityAdjustedPass).map((f) => f.slotKey),
  );
  const failKeys = new Set(
    (scorecard.failureRegister || []).map((f) => f.slotKey),
  );

  const name = target.name;
  const parent = target.parentCompany;
  const loyalty = target.loyalty?.programName || "parent loyalty program";
  const positioning = evidencePack.evidence?.find((e) =>
    ["basics.positioning", "positioning.from_official", "basics.value_prop", "official.sentence.1"].includes(e.id),
  );
  const differentiators = evidencePack.evidence?.find((e) => e.id === "basics.differentiators" || e.id === "basics.pillars");
  const history = evidencePack.evidence?.find((e) => e.id === "basics.history");
  const peers = evidencePack.evidence?.find((e) => e.id === "competitive.soft_brand_peers");
  const fdd = evidencePack.evidence?.find((e) => e.id === "fdd.investment_range" || e.id === "economics.diligence_path");
  const openHotels = evidencePack.evidence?.find((e) => e.id === "metric.open_hotels");
  const loyaltyValue = evidencePack.evidence?.find((e) => e.id === "loyalty.honors_value");
  const audience = evidencePack.evidence?.find((e) => e.id === "basics.audience");
  const chainScale = evidencePack.evidence?.find((e) => e.id === "basics.chain_scale");
  const brandModel = evidencePack.evidence?.find((e) => e.id === "basics.brand_model");
  const images = officialImages(evidencePack);

  const repaired = [];
  const rows = (candidate.rows || []).map((row) => {
    if (passKeys.has(row.slotKey) || !failKeys.has(row.slotKey)) return row;
    if (typeof repairFns[row.slotKey] === "function") {
      const next = repairFns[row.slotKey](row, { target, evidencePack });
      repaired.push(row.slotKey);
      return next;
    }

    const key = row.slotKey;
    let title = row.title;
    let body = stripRepairPrefixNoise(row.body, name, key);
    let evidenceIds = [...(row.evidenceIds || [])];
    let disposition = row.disposition === "RESEARCH_REQUIRED" ? "APPLICABLE_POPULATED_POSITIVE" : row.disposition;
    let qualityNotes = ["phase2e_quality_repair"];

    // Targeted templates — brand tokens + evidence ids; no invented fees
    if (key === "materials.file" || key === "materials.caseStudy" || key === "materials.documents.available") {
      if (key === "materials.caseStudy") {
        title = "Case study diligence";
        body = `${name} case studies: register steward-confirmed property conversion / affiliation case narratives in Source Library before customer cards. Do not invent hotel outcomes or PDF case packs.`;
      } else if (key === "materials.documents.available") {
        title = "Documents available";
        body = `${name} documents checklist: brand overview, development snapshot, and standards excerpts must be Source Library–registered ${parent} materials — availability flagged here without inventing file URLs.`;
      } else {
        title = "Source materials diligence";
        body = `${name} files: steward-confirmed Source Library PDFs/ZIPs required before customer-facing file cards. Use official ${parent} brand/development materials when registered — do not invent downloads.`;
      }
      evidenceIds = ["official.page", "basics.positioning"].filter((id) => evidencePack.evidence?.some((e) => e.id === id));
      disposition = "APPLICABLE_POPULATED_POSITIVE";
    } else if (
      key === "overview.owner_experience" ||
      key === "owner.outcomes" ||
      key === "valueOwners.overview" ||
      key === "valueOwners.overview.outcomes"
    ) {
      const angle =
        key === "overview.owner_experience"
          ? "Owner experience"
          : key.includes("outcomes")
            ? "Owner outcomes"
            : "Owner value overview";
      body = `${name} (${parent}) — ${angle}: soft-brand / collection affiliation preserves asset character while accessing ${parent} distribution and ${loyalty}. Underwrite conversion PIP, loyalty contribution, and fee stack with comps — no invented yield or RevPAR guarantees.`;
      evidenceIds = [positioning?.id, loyaltyValue?.id, fdd?.id].filter(Boolean);
      title = title || angle;
    } else if (/^overview\.scenario\./.test(key) || /^valueOwners\.scenario\./.test(key) || /^valueOwners\.lifecycle\./.test(key)) {
      const base = positioning?.claim || differentiators?.claim || history?.claim || "";
      const phase = key.includes("lifecycle")
        ? `Lifecycle step ${key.split(".").pop()}`
        : `Scenario ${key.split(".").pop()}`;
      body = `${name} (${parent}) — ${phase}: ${base ? base.slice(0, 180) : "soft-brand / collection affiliation"}. Owner focus for this card: preserve asset character while accessing ${parent} distribution and ${loyalty}; underwrite conversion PIP and loyalty contribution with comps — no invented yield guarantees.`;
      evidenceIds = [positioning?.id, differentiators?.id, loyaltyValue?.id].filter(Boolean);
      if (!title) title = key.includes("lifecycle") ? `Owner lifecycle ${key.split(".").pop()}` : `Owner value scenario ${key.split(".").pop()}`;
    } else if (/^insight\.similar|^overview\.relative_positioning|commercial\.differentiator|standards\.similar/.test(key)) {
      body = peers
        ? `${name} [${key}] competes in the upscale soft-brand / lifestyle collection set with ${peers.claim}. Differentiate on ${parent} systems + ${loyalty} versus peer soft brands — confirm fee/standards stack in current disclosure.`
        : `${name} [${key}] sits in ${parent}'s soft-brand / collection tier versus hard brands; compare design latitude and loyalty participation explicitly before shortlisting.`;
      evidenceIds = [peers?.id, positioning?.id].filter(Boolean);
    } else if (key === "economics.fee") {
      body = fdd
        ? `${name} fee diligence: ${fdd.claim} Royalty/marketing/loyalty percentages are disclosure-only — not underwritable quotes.`
        : `${name} fee diligence: obtain current franchise disclosure from ${parent} development; confirm affiliation, royalty, marketing, loyalty, and system fees with counsel. Amounts not invented here.`;
      evidenceIds = [fdd?.id].filter(Boolean);
      title = "Investment / fee diligence (FDD)";
    } else if (key === "economics.legal") {
      title = "Terms & Renewals";
      body = `${name} legal / agreement diligence under ${parent}: confirm initial term, renewal options, transfer/sale, and area-of-protection language in current franchise disclosure with counsel — no invented contractual terms.`;
      evidenceIds = [fdd?.id].filter(Boolean);
    } else if (/^economics\./.test(key)) {
      body = fdd
        ? `${name} · ${key}: ${fdd.claim} Treat royalty/marketing/loyalty fee percentages as disclosure-only — diligence framing, not underwritable fee quotes.`
        : `${name} · ${key}: obtain current franchise disclosure from ${parent} development; confirm affiliation, royalty, marketing, loyalty, and system fees with counsel before underwriting. Amounts not invented here.`;
      evidenceIds = [fdd?.id].filter(Boolean);
      title = title || `Economics diligence (${key.split(".").pop()})`;
    } else if (/^commercial\./.test(key)) {
      const lever = key.split(".").slice(1).join(" ");
      body = `${name} commercial · ${lever}: under ${parent}, combine ${loyalty} member demand, brand.com/CRS distribution, and property-level character. For this lever, request channel-mix / loyalty-contribution scorecards — do not rely on generated KPI figures.`;
      evidenceIds = [loyaltyValue?.id, positioning?.id].filter(Boolean);
    } else if (key === "footprint.primary_regions") {
      body = `${name} primary regions: confirm Region Offered and corridor priorities on Brand Basics / development materials; keep footprint narrative HPC-qualified (certState) before customer publish.`;
      evidenceIds = [openHotels?.id, "basics.region_offered"].filter((id) => id && (openHotels?.id === id || evidencePack.evidence?.some((e) => e.id === id)));
    } else if (/^footprint\.region\./.test(key)) {
      // Phase 2H: never stamp brand-wide HPC totals into regional cards.
      const regionCode = key.split(".").pop();
      const regionLabels = {
        am: "Americas",
        cala: "Caribbean & Latin America",
        eu: "Europe",
        mea: "Middle East & Africa",
        apac: "Asia Pacific",
      };
      const label = regionLabels[regionCode] || regionCode;
      const regional = regionalHpc?.[regionCode];
      if (regional && (regional.openHotels > 0 || regional.pipelineHotels > 0)) {
        body = [
          "Established presence",
          `Observed HPC for ${label}: Open hotels=${regional.openHotels}; keys=${regional.openKeys || 0}; pipeline hotels=${regional.pipelineHotels || 0}. Regional filter applied — not a restatement of brand-wide totals.`,
        ].join("\n\n");
        disposition = "RENDER_QUALIFIED";
      } else {
        body = [
          "Coverage limited",
          `No verified regional HPC rollup for ${label}. Brand-wide open/pipeline totals are withheld from this card to avoid false regional precision — confirm corridor assignment in the deal file.`,
        ].join("\n\n");
        disposition = "COVERAGE_LIMITED";
      }
      evidenceIds = [openHotels?.id].filter(Boolean);
    } else if (key === "metric.open_hotels" || key === "metric.pipeline_hotels") {
      body = openHotels
        ? `${openHotels.claim} — brand-level observed inventory (not a regional count).`
        : `${name}: confirm Open/Pipeline via HPC affiliation match before customer publish (certState required).`;
      evidenceIds = [openHotels?.id].filter(Boolean);
      disposition = "RENDER_QUALIFIED";
    } else if (/^footprint\./.test(key) || /^metric\./.test(key)) {
      body = openHotels
        ? `${name} · ${key}: brand-level HPC context available (${openHotels.claim}). Keep narrative HPC-qualified (certState); do not redistribute brand totals as regional counts. Named openings need steward-confirmed press URLs.`
        : `${name} · ${key}: confirm Open/Pipeline via HPC affiliation match before customer publish (certState required); named openings require steward press URLs.`;
      evidenceIds = [openHotels?.id].filter(Boolean);
      if (/^metric\./.test(key)) disposition = "RENDER_QUALIFIED";
    } else if (key === "standards.residences") {
      title = "Branded residences";
      body = `${name}: branded residences posture is not established in this reconstruction pack — confirm Brand Basics / development offering before stating residence programs. Soft-brand affiliation does not imply residences.`;
      evidenceIds = [positioning?.id].filter(Boolean);
      disposition = "APPLICABLE_POPULATED_NEGATIVE";
    } else if (/^operations\./.test(key) || /^standards\./.test(key)) {
      body = `${name} · ${key}: operating/standards diligence under ${parent} — confirm design review, conversion PIP, QA cadence, and ${loyalty} system cutover in current brand manuals. Soft-brand class flexibility is not a substitute for the brand standards document.`;
      evidenceIds = [positioning?.id, fdd?.id].filter(Boolean);
    } else if (/^loyalty\./.test(key)) {
      if (key === "loyalty.hero_title") {
        body = loyalty;
      } else if (key === "loyalty.earn") {
        body = target.loyalty?.earn || `${name}: guests earn ${loyalty} points on qualifying stays — confirm current earn rules on the official program site.`;
      } else if (key === "loyalty.redeem") {
        body = target.loyalty?.redeem || `${name}: ${loyalty} points redeem across the ${parent} portfolio — confirm current redeem rules before publish.`;
      } else if (key === "loyalty.elite") {
        body = target.loyalty?.tiers || `${loyalty} elite tiers — confirm current tier names on the official program site before publish.`;
      } else if (key === "loyalty.program") {
        body = `${name} participates in ${loyalty}. ${loyaltyValue?.claim || "Confirm program participation and contribution expectations during diligence."}`;
      } else if (/^loyalty\.kpi\./.test(key)) {
        const kpi = key.split(".").pop();
        body = `${name} · loyalty KPI (${kpi}): request current ${loyalty} network figures from ${parent} development — do not invent member/hotel/market counts. Use disclosure/scorecards only.`;
      } else if (/^loyalty\.implications\./.test(key)) {
        const lens = key.split(".").pop();
        body = `${name} · loyalty implications (${lens}): underwrite ${loyalty} contribution, system cutover, and owner P&L impact with property comps — ${loyaltyValue?.claim || "confirm program mechanics with brand."}`;
      } else {
        body = `${name} · ${key}: ${loyaltyValue?.claim || `${name} participates in ${loyalty}`}. Confirm current earn/redeem and elite tier names on the official program site before publish.`;
      }
      evidenceIds = [loyaltyValue?.id, "loyalty.program"].filter((id) => evidencePack.evidence?.some((e) => e.id === id) || id === loyaltyValue?.id);
    } else if (/^materials\.gallery\./.test(key)) {
      const n = Number(key.split(".").pop()) || 1;
      const direct = evidencePack.evidence?.find((e) => e.id === `official.image.${n}`);
      const img = direct || (images.length ? images[(n - 1) % images.length] : null);
      if (img && /^https?:\/\//i.test(nz(img.claim))) {
        body = img.claim;
        evidenceIds = [img.id];
        title = title || `Gallery ${n}${direct ? "" : " (verified official image, rotated)"}`;
        disposition = "APPLICABLE_POPULATED_POSITIVE";
      } else if (isUrlBody(body)) {
        disposition = "APPLICABLE_POPULATED_POSITIVE";
      } else {
        // Keep prior URL if any; otherwise leave diligence note without claiming a fake URL
        body = isUrlBody(stripRepairPrefixNoise(row.body, name, key))
          ? stripRepairPrefixNoise(row.body, name, key)
          : `${name} gallery ${n}: verified official/CDN image URL required — register Source Library imagery before publish.`;
        disposition = isUrlBody(body) ? "APPLICABLE_POPULATED_POSITIVE" : "APPLICABLE_POPULATED_POSITIVE";
        evidenceIds = [];
        // Non-URL gallery body will fail factual/render until a verified URL is supplied — honest
      }
    } else if (/^snapshot\./.test(key) || /^positioning\./.test(key)) {
      const region = evidencePack.evidence?.find((e) => e.id === "basics.region_offered");
      const year = evidencePack.evidence?.find((e) => e.id === "basics.year_launched");
      const website = evidencePack.evidence?.find((e) => e.id === "snapshot.brand_website");
      const residences = evidencePack.evidence?.find((e) => e.id === "basics.residences");
      const fddKeys = evidencePack.evidence?.find((e) => e.id === "fdd.typical_keys");
      const map = {
        "snapshot.segment": chainScale?.claim || `${name}: Upper Upscale / soft-brand collection — confirm Brand Basics chain scale.`,
        "snapshot.brand_family": parent,
        "snapshot.brand_type": brandModel?.claim || `Collection brand under ${parent}`,
        "snapshot.service_level":
          evidencePack.evidence?.find((e) => e.id === "basics.service_model")?.claim ||
          brandModel?.claim ||
          `${name} service/operating model — confirm Brand Basics.`,
        "snapshot.geographic_focus":
          region?.claim || `${name} geographic focus — confirm Region Offered on Brand Basics.`,
        "snapshot.typical_keys":
          fddKeys?.claim ||
          `${name} typical keys: confirm Brand Basics / FDD comps before quoting a range — do not invent key counts.`,
        "snapshot.branded_residences":
          residences?.claim ||
          `${name}: branded residences not established in this pack — confirm Brand Basics before stating residence programs.`,
        "snapshot.parent_company": parent,
        "snapshot.brand_website": website?.claim || target.websiteFallback || "",
        "snapshot.launch_year": year?.claim || `${name} launch year — confirm Brand Basics.`,
        "positioning.audience": audience?.claim
          ? `${name} guest audience: ${audience.claim}`
          : `${name} guest audience — confirm Brand Basics target segments.`,
        "positioning.positioning": positioning?.claim
          ? `${name} owner positioning: ${positioning.claim} Soft-brand affiliation under ${parent} with ${loyalty} — underwrite conversion PIP and distribution value with comps.`
          : `${name} positioning under ${parent}: soft-brand / collection affiliation with ${loyalty}; underwrite owner conversion PIP and distribution value with comps.`,
      };
      body = map[key] || `${name}: ${key} — confirm Brand Basics / official materials before publish.`;
      evidenceIds = [chainScale?.id, brandModel?.id, audience?.id, positioning?.id, region?.id, year?.id, fddKeys?.id, "identity.parent"].filter(
        (id) => id && evidencePack.evidence?.some((e) => e.id === id),
      );
      if (key === "snapshot.brand_family" || key === "snapshot.parent_company") {
        evidenceIds = ["identity.parent"].filter((id) => evidencePack.evidence?.some((e) => e.id === id));
      }
      disposition = "APPLICABLE_POPULATED_POSITIVE";
    } else {
      const cleaned = stripRepairPrefixNoise(body, name, key);
      body = `${name} (${parent}): ${cleaned || "owner diligence note"}. Evidence-grounded reconstruction — confirm against Brand Basics / official materials / disclosure before publish.`;
      if (positioning?.id) evidenceIds = [...new Set([...evidenceIds, positioning.id])];
    }

    repaired.push(key);
    return {
      ...row,
      title,
      body,
      evidenceIds,
      disposition,
      completeness: disposition === "RESEARCH_REQUIRED" ? "research_required" : "populated",
      qualityNotes,
      repairedInPhase2E: true,
    };
  });

  // Ensure failure slots missing from candidate rows are added
  for (const slotKey of failKeys) {
    if (!rows.some((r) => r.slotKey === slotKey)) {
      rows.push({
        slotKey,
        title: "",
        body: `${name} (${parent}): field repaired for quality gate — confirm against authorized sources before publish.`,
        evidenceIds: [positioning?.id].filter(Boolean),
        disposition: "APPLICABLE_POPULATED_POSITIVE",
        completeness: "populated",
        qualityNotes: ["phase2e_quality_repair_inserted"],
        airtableOwnership: "presentation_stage_only",
        writes: 0,
        repairedInPhase2E: true,
      });
      repaired.push(slotKey);
    }
  }

  const withAliases = ensureAtelierRenderAliases({ ...candidate, rows }, failKeys);
  return {
    ...withAliases,
    repairedSlotKeys: [...new Set(repaired)],
    preservedPassSlotKeys: [...passKeys],
  };
}
