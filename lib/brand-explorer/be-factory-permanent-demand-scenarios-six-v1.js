/**
 * Brand Explorer Factory — permanent Demand Scenario Fit (exact 6).
 *
 * Slot: commercial.demand
 * Lesson: BE-QL-DEMAND-SCENARIOS-SIX-001
 * Contract: BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1
 *
 * Inherited by research generation, cumulative assembly, atelier render,
 * Comparison Studio, Factory 1.0 / 2.0, and publication quality gates.
 */

export const BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1 =
  "BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1";

export const DEMAND_SCENARIO_SLOT = "commercial.demand";
export const DEMAND_SCENARIO_REQUIRED_COUNT = 6;
export const DEMAND_SCENARIO_LESSON_ID = "BE-QL-DEMAND-SCENARIOS-SIX-001";

/** Canonical fit levels (internal enum). */
export const DEMAND_FIT_LEVELS = Object.freeze({
  STRONG_FIT: "STRONG_FIT",
  MODERATE_FIT: "MODERATE_FIT",
  LIMITED_FIT: "LIMITED_FIT",
  NOT_A_FIT: "NOT_A_FIT",
});

/** Customer-facing labels (Title Case). */
export const DEMAND_FIT_LABELS = Object.freeze({
  STRONG_FIT: "Strong Fit",
  MODERATE_FIT: "Moderate Fit",
  LIMITED_FIT: "Limited Fit",
  NOT_A_FIT: "Not a Fit",
});

/**
 * Shared Demand Scenario taxonomy (candidate categories — brand picks six).
 * Reuses titles already present across fixtures / Tribute / atelier fallbacks.
 */
export const CANONICAL_DEMAND_SCENARIO_TAXONOMY = Object.freeze([
  { themeKey: "gateway_urban", title: "Gateway Urban", aliases: ["Gateway / Urban Hub", "Gateway City"] },
  { themeKey: "corporate_led_urban", title: "Corporate-Led Urban", aliases: ["Corporate Urban", "Urban Corporate"] },
  { themeKey: "regional_secondary", title: "Regional & Secondary Markets", aliases: ["Regional & Secondary Upscale", "Secondary Market", "Regional Markets"] },
  { themeKey: "resort_coastal", title: "Resort & Coastal Leisure", aliases: ["Resort / Coastal Leisure", "Resort / Leisure Repositioning", "Coastal Leisure"] },
  { themeKey: "conversion_repositioning", title: "Conversion & Repositioning", aliases: ["Conversion / Repositioning", "Urban Lifestyle Conversion", "Independent / Soft-Brand Conversion", "Heritage & Experiential Repositioning"] },
  { themeKey: "highway_interstate", title: "Highway & Interstate Transient", aliases: ["Pure Economy / Highway", "Highway Transient", "Interstate Transient"] },
  { themeKey: "airport_transport", title: "Airport & Transportation", aliases: ["Airport", "Airport / Transport"] },
  { themeKey: "extended_stay_corporate", title: "Extended-Stay Corporate", aliases: ["Extended Stay", "Corporate Extended Stay"] },
  { themeKey: "project_workforce", title: "Project & Workforce Demand", aliases: ["Workforce", "Project Demand"] },
  { themeKey: "association_convention", title: "Association & Convention", aliases: ["Convention", "Association Demand"] },
  { themeKey: "university_medical", title: "University & Medical", aliases: ["University", "Medical Demand", "Campus & Medical"] },
  { themeKey: "leisure_destination", title: "Leisure Destination", aliases: ["Leisure Destination Demand"] },
  { themeKey: "luxury_urban_flagship", title: "Luxury Urban Flagship", aliases: ["Luxury Flagship", "Urban Flagship"] },
  { themeKey: "meetings_group", title: "Meetings & Group Intensive", aliases: ["Meetings & Groups", "Group Intensive"] },
  { themeKey: "suburban_business_leisure", title: "Suburban Business & Leisure", aliases: ["Suburban", "Suburban Transient"] },
  { themeKey: "economy_value_transient", title: "Economy Value-Oriented Transient", aliases: ["Economy Transient", "Value Transient", "Economy Limited Service"] },
  { themeKey: "independent_distribution", title: "Independent Soft-Brand Distribution", aliases: ["Independent Marriott Distribution", "Soft-Brand Distribution"] },
  { themeKey: "mixed_use_destination", title: "Mixed-Use Destination", aliases: ["Mixed Use"] },
  { themeKey: "operator_led_reflag", title: "Operator-Led Reflag", aliases: ["Operator Led Reflag", "Reflag"] },
]);

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function normalizeKey(s) {
  return nz(s)
    .toLowerCase()
    .replace(/[–—]/g, "-")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const THEME_BY_ALIAS = (() => {
  const map = new Map();
  for (const t of CANONICAL_DEMAND_SCENARIO_TAXONOMY) {
    map.set(normalizeKey(t.title), t.themeKey);
    map.set(normalizeKey(t.themeKey.replace(/_/g, " ")), t.themeKey);
    for (const a of t.aliases || []) map.set(normalizeKey(a), t.themeKey);
  }
  return map;
})();

export function resolveDemandScenarioThemeKey(title) {
  const k = normalizeKey(title);
  if (!k) return null;
  if (THEME_BY_ALIAS.has(k)) return THEME_BY_ALIAS.get(k);
  // Fuzzy contains match against taxonomy titles
  for (const t of CANONICAL_DEMAND_SCENARIO_TAXONOMY) {
    const tk = normalizeKey(t.title);
    if (k.includes(tk) || tk.includes(k)) return t.themeKey;
    for (const a of t.aliases || []) {
      const ak = normalizeKey(a);
      if (k === ak || (k.length >= 8 && (k.includes(ak) || ak.includes(k)))) return t.themeKey;
    }
  }
  return `custom:${k}`;
}

/**
 * Normalize free-text / legacy pills → canonical fit level + customer label.
 * @param {string} raw
 * @param {{ allowLegacyCompounds?: boolean }} [opts]
 *   allowLegacyCompounds — map Moderate–strong → Moderate Fit during CURRENT migration only.
 *   New generation / publication validation keeps compounds rejected.
 */
export function normalizeDemandFit(raw, opts = {}) {
  const s = nz(raw);
  if (!s) return { ok: false, level: null, label: "", reason: "missing_fit" };

  const compound =
    /very\s*strong|moderate\s*[–—-]?\s*strong|moderate\s+to\s+strong|medium\s*[–—-]?\s*high|weak\s*[–—-]?\s*moderate/i.test(
      s
    );

  if (compound && opts.allowLegacyCompounds) {
    if (/very\s*strong/i.test(s)) {
      return { ok: true, level: DEMAND_FIT_LEVELS.STRONG_FIT, label: DEMAND_FIT_LABELS.STRONG_FIT, legacyMapped: true };
    }
    if (/moderate/i.test(s) || /medium/i.test(s)) {
      return { ok: true, level: DEMAND_FIT_LEVELS.MODERATE_FIT, label: DEMAND_FIT_LABELS.MODERATE_FIT, legacyMapped: true };
    }
    if (/weak/i.test(s)) {
      return { ok: true, level: DEMAND_FIT_LEVELS.LIMITED_FIT, label: DEMAND_FIT_LABELS.LIMITED_FIT, legacyMapped: true };
    }
  }

  // Reject inconsistent compounds the founder banned (new generation / strict validate)
  if (compound) {
    return { ok: false, level: null, label: s, reason: "invalid_fit_label_compound" };
  }

  const k = normalizeKey(s);

  // Exact / near-exact legacy → enum (canonical four only)
  if (/^(strong fit|strong|high fit)$/.test(k) || k === "strongfit") {
    return { ok: true, level: DEMAND_FIT_LEVELS.STRONG_FIT, label: DEMAND_FIT_LABELS.STRONG_FIT };
  }
  if (/^(moderate fit|moderate|selective|medium)$/.test(k) || k === "moderatefit") {
    return { ok: true, level: DEMAND_FIT_LEVELS.MODERATE_FIT, label: DEMAND_FIT_LABELS.MODERATE_FIT };
  }
  if (/^(limited fit|limited|weak|low)$/.test(k) || k === "limitedfit") {
    return { ok: true, level: DEMAND_FIT_LEVELS.LIMITED_FIT, label: DEMAND_FIT_LABELS.LIMITED_FIT };
  }
  if (/^(not a fit|notafit|no fit|poor fit|unsuitable)$/.test(k)) {
    return { ok: true, level: DEMAND_FIT_LEVELS.NOT_A_FIT, label: DEMAND_FIT_LABELS.NOT_A_FIT };
  }

  // Enum already
  if (DEMAND_FIT_LEVELS[s]) {
    return { ok: true, level: s, label: DEMAND_FIT_LABELS[s] };
  }

  return { ok: false, level: null, label: s, reason: "invalid_fit_label" };
}

export function customerFacingFitLabel(levelOrRaw) {
  const n = normalizeDemandFit(levelOrRaw);
  return n.ok ? n.label : nz(levelOrRaw);
}

function slotOf(b) {
  return nz(b && (b.slotKey || b.slot_key));
}

function titleOf(b) {
  return nz(b && b.title);
}

function bodyOf(b) {
  return nz(b && (b.body || b.value || b.text));
}

function explanationOf(b) {
  return nz(
    b &&
      (b.demandScenarioDescription ||
        b.caseSummaryOverview ||
        b.explanation ||
        b.ownerImplication ||
        b.demandLogic ||
        "")
  );
}

/**
 * A substantive demand card needs a distinct scenario title + valid fit.
 * Prefer explanation when present; body alone as fit pill is allowed if explanation
 * lives in extended fields, but empty title/fit fail.
 */
export function isSubstantiveDemandScenario(block) {
  const title = titleOf(block);
  const fit = normalizeDemandFit(bodyOf(block), { allowLegacyCompounds: true });
  if (!title || title.length < 3) return false;
  if (/^scenario$/i.test(title) || /^demand\s*pattern$/i.test(title)) return false;
  if (!fit.ok) return false;
  const expl = explanationOf(block);
  const body = bodyOf(block);
  // Fit-only body is OK when we have extended explanation; otherwise require
  // either a known fit label body OR longer narrative body (≥40) with fit elsewhere.
  if (expl.length >= 40) return true;
  if (fit.ok && body.length <= 32) return true; // classic title + pill
  if (body.length >= 40 && fit.ok) return true;
  return fit.ok && title.length >= 3;
}

function scoreDemandCard(block, usedThemes) {
  let score = 0;
  const title = titleOf(block);
  const theme = resolveDemandScenarioThemeKey(title);
  const fit = normalizeDemandFit(bodyOf(block), { allowLegacyCompounds: true });
  const expl = explanationOf(block);
  if (fit.ok) score += 20;
  if (fit.level === DEMAND_FIT_LEVELS.STRONG_FIT) score += 4;
  if (fit.level === DEMAND_FIT_LEVELS.MODERATE_FIT) score += 3;
  if (fit.level === DEMAND_FIT_LEVELS.LIMITED_FIT) score += 2;
  if (fit.level === DEMAND_FIT_LEVELS.NOT_A_FIT) score += 3; // honest negative is valuable
  if (expl.length >= 80) score += 15;
  else if (expl.length >= 40) score += 8;
  if (title.length >= 8) score += 4;
  if (theme && usedThemes.has(theme)) score -= 40; // synonym / duplicate theme
  if (/^custom:/.test(theme || "")) score += 1; // brand-specific title still valuable
  // Prefer taxonomy-aligned titles slightly
  if (theme && !/^custom:/.test(theme)) score += 3;
  return { score, theme, fit };
}

/**
 * Select exactly six strongest differentiated cards. Never silent-truncate without scoring.
 * Merges explanation fragments from excluded same-theme cards into retained ones when useful.
 */
export function selectBestSixDemandScenarios(blocks) {
  const list = (Array.isArray(blocks) ? blocks : []).filter(
    (b) => slotOf(b) === DEMAND_SCENARIO_SLOT || !slotOf(b)
  );
  const substantive = list.filter(isSubstantiveDemandScenario);
  const ranked = substantive
    .map((b, index) => {
      const used = new Set();
      const meta = scoreDemandCard(b, used);
      return { block: b, index, ...meta };
    })
    .sort((a, b) => b.score - a.score || a.index - b.index);

  const retained = [];
  const excluded = [];
  const usedThemes = new Set();
  const mergeNotes = [];

  for (const row of ranked) {
    if (retained.length >= DEMAND_SCENARIO_REQUIRED_COUNT) {
      excluded.push({
        title: titleOf(row.block),
        themeKey: row.theme,
        reason: "exceeds_six_after_stronger_selection",
      });
      continue;
    }
    if (row.theme && usedThemes.has(row.theme)) {
      // Merge useful explanation into the retained same-theme card when present
      const target = retained.find((r) => r.theme === row.theme);
      const extra = explanationOf(row.block);
      if (target && extra.length >= 40) {
        const prev = explanationOf(target.block);
        if (!prev.includes(extra.slice(0, 48))) {
          target.block = {
            ...target.block,
            demandScenarioDescription: [prev, extra].filter(Boolean).join(" "),
          };
          mergeNotes.push({
            from: titleOf(row.block),
            into: titleOf(target.block),
            themeKey: row.theme,
          });
        }
      }
      excluded.push({
        title: titleOf(row.block),
        themeKey: row.theme,
        reason: "synonym_or_duplicate_theme",
      });
      continue;
    }
    if (row.theme) usedThemes.add(row.theme);
    retained.push(row);
  }

  const selected = retained.slice(0, DEMAND_SCENARIO_REQUIRED_COUNT).map((r, i) => {
    const fit = normalizeDemandFit(bodyOf(r.block), { allowLegacyCompounds: true });
    return {
      ...r.block,
      slotKey: DEMAND_SCENARIO_SLOT,
      title: titleOf(r.block),
      body: fit.ok ? fit.label : bodyOf(r.block),
      sortOrder: i,
      themeKey: r.theme,
      fitLevel: fit.level,
    };
  });

  return {
    selected,
    excluded,
    mergeNotes,
    inputCount: substantive.length,
    selectedCount: selected.length,
    canCertify: selected.length === DEMAND_SCENARIO_REQUIRED_COUNT,
  };
}

/**
 * Validate commercial.demand card set — EXACT 6 distinct scenarios + valid fits.
 */
export function validateDemandScenariosExactSix(blocks, opts = {}) {
  const list = (Array.isArray(blocks) ? blocks : []).filter(
    (b) => slotOf(b) === DEMAND_SCENARIO_SLOT
  );
  const substantive = list.filter(isSubstantiveDemandScenario);
  const reasons = [];

  if (opts.allowEmpty === true && substantive.length === 0) {
    return {
      ok: true,
      count: 0,
      reasons: ["no_demand_scenarios_optional_empty"],
      blocked: false,
      distinctThemeCount: 0,
      invalidFits: [],
    };
  }

  if (substantive.length < DEMAND_SCENARIO_REQUIRED_COUNT) {
    reasons.push("demand_scenarios_requires_exact_six_cards");
  }
  if (substantive.length > DEMAND_SCENARIO_REQUIRED_COUNT) {
    reasons.push("demand_scenarios_exceeds_six_cards");
  }

  const themes = substantive.map((b) => resolveDemandScenarioThemeKey(titleOf(b)));
  const distinctThemes = new Set(themes.filter(Boolean));
  if (
    substantive.length === DEMAND_SCENARIO_REQUIRED_COUNT &&
    distinctThemes.size < DEMAND_SCENARIO_REQUIRED_COUNT
  ) {
    reasons.push("demand_scenarios_synonymous_or_duplicate");
  }

  const titles = substantive.map((b) => normalizeKey(titleOf(b)));
  if (new Set(titles.filter(Boolean)).size < Math.min(substantive.length, DEMAND_SCENARIO_REQUIRED_COUNT)) {
    reasons.push("demand_scenarios_titles_not_distinct");
  }

  const invalidFits = [];
  const emptyExpl = [];
  for (const b of substantive) {
    const fit = normalizeDemandFit(bodyOf(b));
    if (!fit.ok) invalidFits.push({ title: titleOf(b), body: bodyOf(b), reason: fit.reason });
    const expl = explanationOf(b);
    // Classic title+pill rows pass without long explanation; narrative-only stubs fail
    if (!fit.ok && expl.length < 24 && bodyOf(b).length < 24) {
      emptyExpl.push(titleOf(b));
    }
  }
  if (invalidFits.length) reasons.push("demand_scenarios_invalid_fit_labels");
  if (emptyExpl.length) reasons.push("demand_scenarios_empty_explanations");

  const genericBodies = substantive.filter((b) =>
    /^(suitable for business travelers|good for leisure|works well in many markets)$/i.test(
      explanationOf(b) || bodyOf(b)
    )
  );
  if (genericBodies.length) reasons.push("demand_scenarios_generic_repeated_text");

  const ok =
    substantive.length === DEMAND_SCENARIO_REQUIRED_COUNT &&
    distinctThemes.size === DEMAND_SCENARIO_REQUIRED_COUNT &&
    invalidFits.length === 0 &&
    !reasons.includes("demand_scenarios_titles_not_distinct") &&
    !reasons.includes("demand_scenarios_generic_repeated_text");

  return {
    ok,
    count: substantive.length,
    distinctThemeCount: distinctThemes.size,
    reasons: [...new Set(reasons)],
    blocked: !ok,
    invalidFits,
    lessonId: DEMAND_SCENARIO_LESSON_ID,
  };
}

/**
 * Decide whether a commercial.demand candidate group may replace CURRENT.
 */
export function decideDemandScenarioCandidateApply(currentBlocks, candidateBlocks) {
  const cur = validateDemandScenariosExactSix(currentBlocks, { allowEmpty: true });
  const candRaw = (Array.isArray(candidateBlocks) ? candidateBlocks : []).filter(
    (b) => slotOf(b) === DEMAND_SCENARIO_SLOT
  );

  // Auto-normalize candidate >6 via scored selection before decide
  let candBlocks = candRaw;
  let selection = null;
  if (candRaw.filter(isSubstantiveDemandScenario).length > DEMAND_SCENARIO_REQUIRED_COUNT) {
    selection = selectBestSixDemandScenarios(candRaw);
    if (selection.canCertify) candBlocks = selection.selected;
  }

  const cand = validateDemandScenariosExactSix(candBlocks, { allowEmpty: true });

  if (cand.count > 0 && cand.count < DEMAND_SCENARIO_REQUIRED_COUNT) {
    return {
      applyCandidate: false,
      reason: "demand_scenarios_requires_exact_six_cards",
      retainCurrent: cur.count === DEMAND_SCENARIO_REQUIRED_COUNT && cur.ok,
      needsRemediation: true,
      candidate: cand,
      current: cur,
      selection,
    };
  }
  if (cand.count > 0 && cand.blocked) {
    return {
      applyCandidate: false,
      reason: cand.reasons[0] || "demand_scenarios_blocked",
      retainCurrent: cur.ok,
      needsRemediation: true,
      candidate: cand,
      current: cur,
      selection,
    };
  }
  if (cur.ok && cand.count !== DEMAND_SCENARIO_REQUIRED_COUNT) {
    return {
      applyCandidate: false,
      reason: "demand_scenarios_cardinality_regression",
      retainCurrent: true,
      candidate: cand,
      current: cur,
      selection,
    };
  }
  if (cand.ok && cand.count === DEMAND_SCENARIO_REQUIRED_COUNT) {
    return {
      applyCandidate: true,
      reason: "demand_scenarios_exact_six_ok",
      retainCurrent: false,
      candidate: cand,
      current: cur,
      selection,
      normalizedBlocks: candBlocks,
    };
  }
  return {
    applyCandidate: false,
    reason: "demand_scenarios_no_candidate",
    retainCurrent: cur.count > 0,
    candidate: cand,
    current: cur,
    selection,
  };
}

/**
 * Normalize a block list's commercial.demand rows to exact six when possible.
 * Returns { blocks, audit } — other slots preserved.
 */
export function enforceDemandScenariosExactSixOnBlocks(blocks) {
  const all = Array.isArray(blocks) ? blocks : [];
  const demand = all.filter((b) => slotOf(b) === DEMAND_SCENARIO_SLOT);
  const other = all.filter((b) => slotOf(b) !== DEMAND_SCENARIO_SLOT);
  if (!demand.length) {
    return {
      blocks: all,
      audit: { action: "none", reason: "no_demand_rows" },
      validation: validateDemandScenariosExactSix([], { allowEmpty: true }),
    };
  }
  const selection = selectBestSixDemandScenarios(demand);
  if (!selection.canCertify) {
    return {
      blocks: all,
      audit: {
        action: "needs_remediation",
        inputCount: selection.inputCount,
        selectedCount: selection.selectedCount,
        excluded: selection.excluded,
        mergeNotes: selection.mergeNotes,
      },
      validation: validateDemandScenariosExactSix(demand),
      needsRemediation: true,
    };
  }
  // Normalize fit labels on selected
  const normalized = selection.selected.map((b, i) => {
    const fit = normalizeDemandFit(bodyOf(b), { allowLegacyCompounds: true });
    return {
      ...b,
      slotKey: DEMAND_SCENARIO_SLOT,
      body: fit.ok ? fit.label : bodyOf(b),
      sortOrder: i,
    };
  });
  return {
    blocks: [...other, ...normalized],
    audit: {
      action: "enforced_exact_six",
      inputCount: selection.inputCount,
      selectedCount: 6,
      excluded: selection.excluded,
      mergeNotes: selection.mergeNotes,
    },
    validation: validateDemandScenariosExactSix(normalized),
    needsRemediation: false,
  };
}

export function evaluateDemandScenariosPermanentGate(input = {}) {
  const blocks = input.blocks || input.proposedBlocks || [];
  const demand = validateDemandScenariosExactSix(blocks, {
    allowEmpty: input.allowEmpty === true,
  });
  // Profiles with any commercial.demand content must be exact-six clean
  const hasAny = (blocks || []).some((b) => slotOf(b) === DEMAND_SCENARIO_SLOT);
  let ok = demand.ok;
  if (!hasAny && input.requireDemandScenarios === true) {
    ok = false;
    demand.blocked = true;
    demand.reasons = [...demand.reasons, "demand_scenarios_missing"];
  }
  if (!hasAny && input.requireDemandScenarios !== true) {
    ok = true;
  }
  return {
    contract: BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1,
    lessons: [DEMAND_SCENARIO_LESSON_ID],
    factoryInheritance: ["1.0", "2.0"],
    ok,
    demand,
    blocked: !ok,
  };
}
