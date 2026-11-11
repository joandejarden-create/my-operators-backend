/**
 * ADP_BETHESDA_MASTER_TEMPLATE_PARITY_V1
 *
 * Permanent Full E2E structural gate: every hotel must match Bethesda Marriott's
 * approved component hierarchy (not Bethesda's metrics or narrative text).
 */

export const ADP_BETHESDA_MASTER_TEMPLATE_PARITY_V1 =
  "ADP_BETHESDA_MASTER_TEMPLATE_PARITY_V1";

export const BETHESDA_STRUCTURAL_REFERENCE_PROPERTY_ID = "adp_bethesda_marriott";

export const BETHESDA_EXEC_LEFT_LABELS_V1 = Object.freeze([
  "BIGGEST STRENGTH",
  "BIGGEST CONSTRAINT",
  "CHANGE SINCE LAST COMPARABLE RUN",
]);

export const BETHESDA_EXEC_V3_SECTION_ORDER_V1 = Object.freeze([
  "headline",
  "keyInsight",
  "whyItMatters",
  "focusNow",
  "whatToReview",
]);

export const BETHESDA_EXEC_FORBIDDEN_TITLES_V1 = Object.freeze([
  "What The Data Says",
  "What the Data Says",
  "WHAT THE DATA SAYS",
]);

export const BETHESDA_EXEC_REQUIRED_RENDERER_V1 =
  "ADP_EXECUTIVE_STRUCTURED_RENDERER_V3";

export const BETHESDA_BPP_FORBIDDEN_SELECTORS_V1 = Object.freeze([
  ".bpp-additional-portfolio-analysis",
  ".bpp-demand-territory-dropdown",
  "#bpp-provider-presence",
  "[data-bpp-forbidden]",
  "#adpBrandPortfolioStatus",
  ".adp-bpp-status--rank-blocked",
]);

/** Negative text patterns that must never appear in customer BPP DOM. */
export const BETHESDA_BPP_FORBIDDEN_TEXT_V1 = Object.freeze([
  /PORTFOLIO MEASUREMENT COMPLETE\s*[—\-–]\s*PEER RANKING PENDING/i,
  /Among 1 relevant peer set hotels/i,
]);

/**
 * @param {object} sig — browser structural signature for one hotel
 * @param {object} master — Bethesda signature
 */
export function compareHotelSignatureToBethesda(sig, master) {
  const failures = [];
  if (!sig || !master) {
    return { pass: false, failures: ["MISSING_SIGNATURE"], structurePass: false };
  }

  if (sig.exec?.hasWhatTheDataSays) {
    failures.push("EXEC_LEGACY_WHAT_THE_DATA_SAYS");
  }
  if (
    sig.exec?.title &&
    BETHESDA_EXEC_FORBIDDEN_TITLES_V1.some((t) =>
      String(sig.exec.title).toLowerCase().includes(t.toLowerCase())
    )
  ) {
    failures.push("EXEC_FORBIDDEN_TITLE");
  }
  if (sig.exec?.mode !== master.exec?.mode && sig.exec?.mode !== "V3_HYBRID") {
    failures.push(`EXEC_MODE_${sig.exec?.mode}_EXPECTED_${master.exec?.mode}`);
  }
  if (sig.exec?.renderer !== BETHESDA_EXEC_REQUIRED_RENDERER_V1) {
    // Unavailable structured shell may omit renderer attribute — still require V3 sections
    if (sig.exec?.renderer && sig.exec.renderer !== BETHESDA_EXEC_REQUIRED_RENDERER_V1) {
      failures.push(`EXEC_RENDERER_${sig.exec.renderer}`);
    }
  }
  const expectedSections = (master.exec?.v3Sections || [...BETHESDA_EXEC_V3_SECTION_ORDER_V1]).filter(
    (s) => s !== "watch"
  );
  const actualSections = (sig.exec?.v3Sections || []).filter((s) => s !== "watch");
  for (const key of expectedSections) {
    if (!actualSections.includes(key)) failures.push(`EXEC_MISSING_SECTION_${key}`);
  }
  const left = sig.exec?.leftLabels || [];
  if (left.length !== 3) failures.push(`EXEC_LEFT_CARD_COUNT_${left.length}`);
  for (let i = 0; i < BETHESDA_EXEC_LEFT_LABELS_V1.length; i++) {
    if ((left[i] || "").toUpperCase() !== BETHESDA_EXEC_LEFT_LABELS_V1[i]) {
      failures.push(`EXEC_LEFT_LABEL_${i + 1}_${left[i] || "missing"}`);
    }
  }

  if (sig.bpp?.forbidden) failures.push("BPP_FORBIDDEN_COMPONENT");
  if (sig.bpp?.statusPanelPresent) failures.push("BPP_UNAPPROVED_STATUS_PANEL");
  if (sig.bpp?.peerRankingPendingText) {
    failures.push("BPP_PEER_RANKING_PENDING_STATUS_COPY");
  }
  if (sig.bpp?.subjectOnlyPeerTable) {
    failures.push("BPP_SUBJECT_ONLY_PEER_TABLE");
  }
  if (sig.bpp?.amongOnePeerHotelLanguage) {
    failures.push("BPP_AMONG_1_PEER_SET_HOTELS_LANGUAGE");
  }
  if (sig.bpp?.status === "READY" && Number(sig.bpp?.kpiCount || 0) !== 4) {
    failures.push(`BPP_READY_KPI_COUNT_${sig.bpp?.kpiCount}`);
  }
  if (
    sig.bpp?.rankCertified === true &&
    Number(sig.bpp?.peerTableRows || 0) < 4
  ) {
    // subject + ≥3 peers = ≥4 rows
    failures.push(`BPP_READY_PEER_TABLE_TOO_SMALL_${sig.bpp?.peerTableRows}`);
  }

  if (master.trends?.present && sig.trends?.present === false) {
    failures.push("TRENDS_SECTION_MISSING");
  }
  if (
    master.trends?.kpiCount != null &&
    sig.trends?.kpiCount != null &&
    Number(sig.trends.kpiCount) !== Number(master.trends.kpiCount) &&
    Number(master.trends.kpiCount) > 0
  ) {
    // Allow NO_BASELINE hotels to still expose four slots (including unavailable)
    if (Number(sig.trends.kpiCount) !== 4 && Number(master.trends.kpiCount) === 4) {
      failures.push(`TRENDS_KPI_COUNT_${sig.trends.kpiCount}`);
    }
  }

  if (
    sig.asset &&
    !/adp-v11[1-9]|adp-v1[2-9]\d|v115-bpp-peer-table-structure/i.test(String(sig.asset))
  ) {
    failures.push("STALE_OR_WRONG_ADP_ASSET");
  }

  return {
    pass: failures.length === 0,
    structurePass: failures.length === 0,
    failures,
    gate: ADP_BETHESDA_MASTER_TEMPLATE_PARITY_V1,
  };
}

/**
 * Negative-test helpers: synthetic signatures that MUST fail the gate.
 */
export function buildNegativeRegressionCases(master) {
  return [
    {
      id: "legacy_what_the_data_says",
      sig: {
        ...master,
        exec: {
          ...master.exec,
          mode: "LEGACY_OR_OTHER",
          title: "What The Data Says",
          hasWhatTheDataSays: true,
          renderer: null,
          v3Sections: [],
        },
      },
      expectFailCodes: ["EXEC_LEGACY_WHAT_THE_DATA_SAYS", "EXEC_MISSING_SECTION_headline"],
    },
    {
      id: "missing_focus_now",
      sig: {
        ...master,
        exec: {
          ...master.exec,
          v3Sections: ["headline", "keyInsight", "whyItMatters", "whatToReview"],
        },
      },
      expectFailCodes: ["EXEC_MISSING_SECTION_focusNow"],
    },
    {
      id: "bpp_forbidden_extra",
      sig: {
        ...master,
        bpp: { ...(master.bpp || {}), forbidden: true, kpiCount: 4, status: "READY" },
      },
      expectFailCodes: ["BPP_FORBIDDEN_COMPONENT"],
    },
    {
      id: "bpp_peer_ranking_pending_status_panel",
      sig: {
        ...master,
        bpp: {
          ...(master.bpp || {}),
          statusPanelPresent: true,
          peerRankingPendingText: true,
          kpiCount: 4,
          status: "READY",
        },
      },
      expectFailCodes: [
        "BPP_UNAPPROVED_STATUS_PANEL",
        "BPP_PEER_RANKING_PENDING_STATUS_COPY",
      ],
    },
    {
      id: "bpp_subject_only_peer_table",
      sig: {
        ...master,
        bpp: {
          ...(master.bpp || {}),
          subjectOnlyPeerTable: true,
          amongOnePeerHotelLanguage: true,
          rankCertified: true,
          peerTableRows: 1,
          kpiCount: 4,
          status: "READY",
        },
      },
      expectFailCodes: [
        "BPP_SUBJECT_ONLY_PEER_TABLE",
        "BPP_AMONG_1_PEER_SET_HOTELS_LANGUAGE",
        "BPP_READY_PEER_TABLE_TOO_SMALL_1",
      ],
    },
    {
      id: "wrong_left_order",
      sig: {
        ...master,
        exec: {
          ...master.exec,
          leftLabels: [
            "BIGGEST CONSTRAINT",
            "BIGGEST STRENGTH",
            "CHANGE SINCE LAST COMPARABLE RUN",
          ],
        },
      },
      expectFailCodes: ["EXEC_LEFT_LABEL_1_BIGGEST CONSTRAINT"],
    },
  ];
}

export function runNegativeRegressionSuite(master) {
  const cases = buildNegativeRegressionCases(master);
  return cases.map((c) => {
    const result = compareHotelSignatureToBethesda(c.sig, master);
    const hit = (c.expectFailCodes || []).some((code) =>
      result.failures.some((f) => String(f).includes(code) || f === code)
    );
    return {
      id: c.id,
      pass: !result.pass && hit,
      failures: result.failures,
      expectFailCodes: c.expectFailCodes,
    };
  });
}
