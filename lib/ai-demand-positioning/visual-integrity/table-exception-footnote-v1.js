/**
 * TABLE_EXCEPTION_TO_FOOTNOTE — reusable table visual principle.
 * Exception detail must not inflate one row vs peer rows.
 * Coverage footnotes append to the Provider Presence methodology note below the table —
 * never as a standalone block inside the table container.
 */

export const TABLE_EXCEPTION_TO_FOOTNOTE = "TABLE_EXCEPTION_TO_FOOTNOTE";
export const DEFECT_INLINE_EXCEPTION_INFLATES_ROW = "INLINE_EXCEPTION_INFLATES_ROW";

export const PROVIDER_TERRITORY_METHODOLOGY_NOTE =
  "Territory totals can be higher than any single model because a traveler need counts if at least one model mentioned your hotel.";

/**
 * Compose shared note below the provider table: methodology first, then coverage footnotes inline.
 */
export function composeProviderTerritoryNote(footnotes) {
  const base = PROVIDER_TERRITORY_METHODOLOGY_NOTE;
  if (!footnotes || !footnotes.length) {
    return { text: base, hasCoverageFootnotes: false };
  }
  const suffix = footnotes.map((fn) => `(${fn.num}) ${fn.text}`).join(" ");
  return {
    text: `${base} ${suffix}`,
    hasCoverageFootnotes: true,
  };
}

/**
 * Build compact monitored cell + shared footnotes for incomplete provider coverage.
 * Denominator semantics unchanged: present / comparable; scheduled only in footnote.
 */
export function buildProviderMonitoredFootnotes(providers) {
  const footnoteByNote = new Map();
  const footnotes = [];
  const rows = [];

  for (const p of providers || []) {
    const unavailable = p.presenceUnavailable === true || p.presence == null;
    const denom = p.comparable != null ? p.comparable : p.total;
    const scheduled =
      p.scheduled != null
        ? p.scheduled
        : p.scenariosScheduled != null
          ? p.scenariosScheduled
          : denom;
    let monitoredLabel = unavailable ? "—" : `${p.mentioned} / ${denom}`;
    let footnoteNum = null;

    if (!unavailable && scheduled != null && denom != null && Number(scheduled) > Number(denom)) {
      const note = `${denom} of ${scheduled} observations captured.`;
      if (!footnoteByNote.has(note)) {
        const num = footnotes.length + 1;
        footnoteByNote.set(note, num);
        footnotes.push({ num, text: note });
      }
      footnoteNum = footnoteByNote.get(note);
      monitoredLabel = `${p.mentioned} / ${denom} (${footnoteNum})`;
    }

    rows.push({
      provider: p.provider,
      monitoredLabel,
      footnoteNum,
      comparable: denom,
      scheduled,
      mentioned: p.mentioned,
    });
  }

  return { rows, footnotes };
}

/**
 * Detect inline exception prose inside table Monitored cells (legacy defect).
 */
export function auditMonitoredCellsForInlineCoverageProse(cellTexts) {
  const defects = [];
  for (const text of cellTexts || []) {
    const t = String(text || "");
    if (/\d+\s+of\s+\d+\s+observations\s+captured/i.test(t) && !/^\d+\s*\/\s*\d+(\s*\(\d+\))?$/.test(t.trim())) {
      defects.push({
        code: DEFECT_INLINE_EXCEPTION_INFLATES_ROW,
        rule: TABLE_EXCEPTION_TO_FOOTNOTE,
        detail: t.slice(0, 120),
      });
    }
  }
  return {
    status: defects.length ? "FAIL" : "PASS",
    defects,
    rule: TABLE_EXCEPTION_TO_FOOTNOTE,
  };
}
