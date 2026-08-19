/**
 * AI Demand Positioning — consistent percentage formatting (##.#%).
 */

export function roundAdpPercent(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 10) / 10;
}

/** Format numeric percent value as ##.#% (e.g. 75.4%). */
export function formatAdpPercent(value) {
  return `${roundAdpPercent(value).toFixed(1)}%`;
}

/** Format ratio 0–1 as ##.#%. */
export function formatAdpPercentFromRatio(ratio) {
  return formatAdpPercent(Number(ratio) * 100);
}
