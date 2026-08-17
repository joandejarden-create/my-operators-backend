/**
 * Scan rendered Brand Explorer HTML for empty UI shells.
 * Empty cards / phase boxes / bars / metrics / chips / modals are hard fails for active profiles.
 */
function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * @returns {{ pass: boolean, failFindings: number, findings: object[] }}
 */
export function scanNoEmptyRenderedComponents(html = "", { brandSlug = null } = {}) {
  const h = nz(html);
  const findings = [];

  const push = (id, detail, count = 1) => {
    findings.push({
      brandSlug,
      id,
      status: "blocked_empty_render",
      detail,
      count,
      recommendedAction: "suppress_component_or_fill",
    });
  };

  const emptyDd = (h.match(/oe-dd--empty/gi) || []).length;
  if (emptyDd > 0) push("empty_dd_nodes", `oe-dd--empty count=${emptyDd}`, emptyDd);

  const emptyBars = (h.match(/indicator-bar__fill--empty/gi) || []).length;
  if (emptyBars > 0) push("empty_flexibility_bars", `empty bars=${emptyBars}`, emptyBars);

  const emptyChips = (h.match(/tag-chip oe-dd--empty/gi) || []).length;
  if (emptyChips > 0) push("empty_chips", `empty chips=${emptyChips}`, emptyChips);

  const emptyScenarioVisual = (h.match(/scenario-card__visual--empty/gi) || []).length;
  if (emptyScenarioVisual > 0) {
    push("empty_scenario_visuals", `empty scenario visuals=${emptyScenarioVisual}`, emptyScenarioVisual);
  }

  const emptyLis = (h.match(/<li>\s*(?:&nbsp;)?\s*<\/li>/gi) || []).length;
  if (emptyLis > 0) push("empty_bullet_rows", `empty <li>=${emptyLis}`, emptyLis);

  // Timeline phase with only &nbsp; detail
  const emptyPhases = (
    h.match(/<div class="timeline__phase"><strong>[^<]+<\/strong><span>(?:&nbsp;|\s)*<\/span><\/div>/gi) || []
  ).length;
  if (emptyPhases > 0) push("empty_phase_boxes", `empty timeline phases=${emptyPhases}`, emptyPhases);

  const emptyMomentum = /momentum-feed__label oe-dd--empty|momentum-feed oe-dd--empty/i.test(h);
  if (emptyMomentum) push("empty_momentum_panel", "Recent Momentum empty shell");

  const emptyModalBodies = (h.match(/case-summary[^>]*>\s*(?:&nbsp;)?\s*</gi) || []).length;
  if (emptyModalBodies > 0) push("empty_modal_bodies", `suspicious empty case-summary=${emptyModalBodies}`);

  const failFindings = findings.reduce((n, f) => n + (f.count || 1), 0);
  return {
    pass: failFindings === 0,
    failFindings,
    findings,
    brandSlug,
  };
}
