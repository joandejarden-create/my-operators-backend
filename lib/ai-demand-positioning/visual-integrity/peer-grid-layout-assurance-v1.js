/**
 * ADP Visual Integrity — peer-grid / dead-space structural QA.
 * Rule: VISUAL_BALANCE_BEFORE_DECORATION
 * Defect: PEER_GRID_ORPHAN / DEAD_SPACE_LAYOUT_DEFECT / ORPHANED_PEER_COMPONENT
 */

export const VISUAL_INTEGRITY_RULE = "VISUAL_BALANCE_BEFORE_DECORATION";
export const VISUAL_DEFECT_PEER_ORPHAN = "PEER_GRID_ORPHAN";
export const VISUAL_DEFECT_DEAD_SPACE = "DEAD_SPACE_LAYOUT_DEFECT";
export const VISUAL_DEFECT_ORPHANED_PEER = "ORPHANED_PEER_COMPONENT";
export const VISUAL_DEFECT_UNEVEN_HEIGHTS = "UNEVEN_PEER_CARD_HEIGHTS";
export const VISUAL_DEFECT_Y_MISALIGN = "PEER_ROW_Y_MISALIGNMENT";

export const TRENDS_KPI_LAYOUT_CONTRACT = Object.freeze({
  peerGridId: "trends-kpi",
  peerCount: 4,
  expectedColumnsByViewport: Object.freeze({
    1440: 4,
    1280: 4,
    768: 2,
    390: 1,
  }),
});

/**
 * Cluster cards into rows by approximate y (tolerance px).
 */
export function clusterPeerRows(boxes, yTolerance = 12) {
  const sorted = [...boxes].sort((a, b) => a.y - b.y || a.x - b.x);
  const rows = [];
  for (const box of sorted) {
    const row = rows.find((r) => Math.abs(r.y - box.y) <= yTolerance);
    if (row) {
      row.cards.push(box);
      row.y = (row.y * (row.cards.length - 1) + box.y) / row.cards.length;
    } else {
      rows.push({ y: box.y, cards: [box] });
    }
  }
  return rows;
}

/**
 * Analyze peer card geometry for orphan / dead-space defects.
 */
export function analyzePeerGridLayout({
  containerBox,
  cardBoxes,
  expectedColumns,
  viewportWidth,
  yTolerance = 12,
  heightTolerance = 24,
  deadSpaceRatioMax = 0.42,
}) {
  const defects = [];
  if (!cardBoxes?.length) {
    return { status: "FAIL", defects: ["NO_PEER_CARDS"], rows: [], utilization: null };
  }

  const rows = clusterPeerRows(cardBoxes, yTolerance);
  const colsOnFirst = rows[0]?.cards.length || 0;

  if (expectedColumns != null && colsOnFirst !== expectedColumns && rows.length === 1) {
    // single row but wrong count
    if (cardBoxes.length === expectedColumns) {
      defects.push({
        code: VISUAL_DEFECT_Y_MISALIGN,
        detail: `expected ${expectedColumns} same-row peers; first-row count ${colsOnFirst}`,
      });
    }
  }

  // Orphan: last row has 1 card while prior rows are fuller and total peers would fit expected columns
  if (rows.length >= 2) {
    const last = rows[rows.length - 1];
    const prior = rows[rows.length - 2];
    if (last.cards.length === 1 && prior.cards.length >= 2) {
      const total = cardBoxes.length;
      if (expectedColumns && total <= expectedColumns) {
        defects.push({
          code: VISUAL_DEFECT_ORPHANED_PEER,
          class: VISUAL_DEFECT_PEER_ORPHAN,
          detail: `viewport ${viewportWidth}: ${prior.cards.length}+1 orphan layout; expected ${expectedColumns} columns`,
        });
      } else if (!expectedColumns && prior.cards.length === 3 && last.cards.length === 1) {
        defects.push({
          code: VISUAL_DEFECT_ORPHANED_PEER,
          class: VISUAL_DEFECT_PEER_ORPHAN,
          detail: `viewport ${viewportWidth}: classic 3+1 orphan peer wrap`,
        });
      }
    }
  }

  // Same-row y alignment
  for (const row of rows) {
    if (row.cards.length < 2) continue;
    const ys = row.cards.map((c) => c.y);
    if (Math.max(...ys) - Math.min(...ys) > yTolerance) {
      defects.push({
        code: VISUAL_DEFECT_Y_MISALIGN,
        detail: `row y spread ${Math.max(...ys) - Math.min(...ys)}px`,
      });
    }
    const hs = row.cards.map((c) => c.height);
    if (Math.max(...hs) - Math.min(...hs) > heightTolerance) {
      defects.push({
        code: VISUAL_DEFECT_UNEVEN_HEIGHTS,
        detail: `row height spread ${Math.max(...hs) - Math.min(...hs)}px`,
      });
    }
  }

  // Container utilization — large unused region under a short orphan row
  let utilization = null;
  if (containerBox && containerBox.height > 0 && containerBox.width > 0) {
    const cardArea = cardBoxes.reduce((s, b) => s + b.width * b.height, 0);
    utilization = cardArea / (containerBox.width * containerBox.height);
    if (
      rows.length >= 2 &&
      rows[rows.length - 1].cards.length === 1 &&
      utilization < 1 - deadSpaceRatioMax
    ) {
      defects.push({
        code: "EXCESSIVE_UNUSED_LAYOUT_SPACE",
        class: VISUAL_DEFECT_DEAD_SPACE,
        detail: `utilization=${utilization.toFixed(2)} with orphan row`,
      });
    }
  }

  // Expected column contract
  if (expectedColumns != null) {
    const ok =
      (expectedColumns === 4 && rows.length === 1 && colsOnFirst === 4) ||
      (expectedColumns === 2 && rows.length === 2 && rows.every((r) => r.cards.length === 2)) ||
      (expectedColumns === 2 && rows.length === 1 && colsOnFirst === 2) ||
      (expectedColumns === 1 && rows.length === cardBoxes.length && rows.every((r) => r.cards.length === 1));
    // 2x2 may be two rows of 2
    const ok2x2 =
      expectedColumns === 2 &&
      cardBoxes.length === 4 &&
      rows.length === 2 &&
      rows[0].cards.length === 2 &&
      rows[1].cards.length === 2;
    if (!(ok || ok2x2)) {
      defects.push({
        code: "UNEXPECTED_PEER_COLUMN_LAYOUT",
        detail: `viewport ${viewportWidth}: expected ~${expectedColumns} cols; rows=${rows.map((r) => r.cards.length).join("+")}`,
      });
    }
  }

  return {
    status: defects.length ? "FAIL" : "PASS",
    defects,
    rows: rows.map((r) => ({ y: r.y, count: r.cards.length, cards: r.cards })),
    utilization,
    rule: VISUAL_INTEGRITY_RULE,
  };
}

export function expectedTrendKpiColumns(viewportWidth) {
  if (viewportWidth >= 1280) return 4;
  if (viewportWidth >= 600) return 2; // includes 768 intentional 2×2
  return 1; // 390 mobile
}
