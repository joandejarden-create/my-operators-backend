/**
 * Measure Manual Process section heights at multiple viewports via CDP-friendly
 * one-shot evaluate (called after Emulation.setDeviceMetricsOverride).
 */
(() => {
  const r = document.querySelector("#dealality-manual-process");
  if (r) {
    r.classList.add("is-drawn");
    r.classList.remove("is-animating");
    r.scrollIntoView({ block: "start" });
  }
  const cardRow = r && r.querySelector(".dmp-problems");
  const cards = r ? [...r.querySelectorAll(".dmp-problem")] : [];
  const layout =
    cards[0] &&
    (() => {
      const cs = getComputedStyle(cards[0]);
      return {
        display: cs.display,
        gridTemplateColumns: cs.gridTemplateColumns,
        flexDirection: cs.flexDirection,
      };
    })();
  return JSON.stringify({
    vw: innerWidth,
    sectionH: r ? Math.round(r.getBoundingClientRect().height) : null,
    cardRowDisplay: cardRow ? getComputedStyle(cardRow).display : null,
    card0: layout,
    titles: [...document.querySelectorAll(".dmp-problem-h")].map((e) =>
      e.textContent.trim()
    ),
    overflowX: document.documentElement.scrollWidth > innerWidth + 2,
  });
})();
