/**
 * Prepends Operator DNA blocks onto Explorer (gold-mock) panel HTML where needed.
 * Explorer panels retain all Setup data: footprint tables, leadership photos, case images, etc.
 */
(function (global) {
  "use strict";

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function pick(ex, p, key, fallback) {
    if (ex && nz(ex[key])) return ex[key];
    if (p && nz(p[key])) return p[key];
    return fallback != null ? fallback : "";
  }

  function parseAchievementLines(p) {
    var raw = p.achievements;
    if (Array.isArray(raw)) return raw.map(nz).filter(Boolean);
    return String(raw || "")
      .split(/\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
  }

  var ASSET_KPI_META = {
    "RevPAR index lift": {
      kind: "revpar",
      note: "Representative improvement after revenue strategy reset",
    },
    "NOI improvement": {
      kind: "noi",
      note: "Portfolio-level NOI uplift from operating discipline and mix",
    },
    "Occupancy improvement": {
      kind: "occupancy",
      note: "Stabilized occupancy gains vs. prior-year baseline",
    },
    "Owner retention / renewal": {
      kind: "retention",
      note: "Renewal and retention across managed owner relationships",
    },
    "ADR stabilization": {
      kind: "adr",
      note: "Rate positioning after repositioning or market reset",
    },
    "Performance lift signal": {
      kind: "lift",
      note: "Directional performance signal from operator track record",
    },
    "Guest satisfaction": {
      kind: "guest",
      note: "Consistent service scores across stabilized resort assets",
    },
    "Revenue performance": {
      kind: "revpar",
      note: "Revenue and rate performance from recent operating cycles",
    },
    "Operating margin": {
      kind: "margin",
      note: "Illustrative portfolio-level operating discipline",
    },
    "Direct booking contribution": {
      kind: "direct",
      note: "Targeted digital and loyalty capture strategy",
    },
    "Avg. GOP margin": {
      kind: "margin",
      note: "Illustrative portfolio-level operating discipline",
    },
  };

  var TRACK_RECORD_KPI_META = {
    "Occupancy recovery window": {
      note: "Typical window to recover occupancy after transition or repositioning",
    },
    "ADR stabilization": {
      note: "Typical window to stabilize rate positioning after strategy reset",
    },
    "Case repeatability": {
      note: "How consistently this operator repeats outcomes across similar asset types",
    },
  };

  /** Two-line KPI headers (matches Brand / Engagement / Infrastructure snapshot pattern). */
  var KPI_LABEL_LINES = {
    "RevPAR index lift": ["RevPAR Index", "Lift"],
    "NOI improvement": ["NOI", "Improvement"],
    "Occupancy improvement": ["Occupancy", "Improvement"],
    "Owner retention / renewal": ["Owner Retention", "/ Renewal"],
    "Guest satisfaction": ["Guest", "Satisfaction"],
    "Direct booking contribution": ["Direct Booking", "Contribution"],
    "Operating margin": ["Operating", "Margin"],
    "Avg. GOP margin": ["Avg. GOP", "Margin"],
    "Occupancy recovery window": ["Occupancy Recovery", "Window"],
    "ADR stabilization": ["ADR", "Stabilization"],
    "Case repeatability": ["Case", "Repeatability"],
  };

  function labelLinesFor(label) {
    if (KPI_LABEL_LINES[label]) return KPI_LABEL_LINES[label].slice();
    var slash = String(label || "").match(/^(.+?)\s*\/\s*(.+)$/);
    if (slash) return [slash[1].trim(), slash[2].trim()];
    var words = String(label || "")
      .trim()
      .split(/\s+/)
      .filter(Boolean);
    if (words.length <= 1) return [label || "", ""];
    var mid = Math.ceil(words.length / 2);
    return [words.slice(0, mid).join(" "), words.slice(mid).join(" ")];
  }

  function metricWithLabelLines(metric) {
    var lines = labelLinesFor(metric.label);
    metric.labelLines = lines;
    metric.label = lines.join(" ");
    return metric;
  }

  function parseMetricNumber(raw) {
    var s = nz(raw);
    if (!s || s === "✓" || s === "—") return null;
    var m = s.match(/[-+]?\d+(?:\.\d+)?/);
    return m ? parseFloat(m[0]) : null;
  }

  function formatMetricNumber(n) {
    if (n == null || isNaN(n)) return "";
    var abs = Math.abs(n);
    if (abs % 1 === 0) return String(Math.round(abs));
    return abs.toFixed(1).replace(/\.0$/, "");
  }

  function formatSignedPercent(n) {
    var body = formatMetricNumber(n) + "%";
    if (n > 0) return "+" + body;
    if (n < 0) return "-" + body;
    return body;
  }

  function formatAssetMetricValue(raw, kind) {
    var s = nz(raw);
    if (!s || s === "—") return "—";
    if (/^not yet/i.test(s)) return "—";
    if (/\d\s*[-–]\s*\d/.test(s)) return s;

    if (/%/.test(s) || /pts?\.?|points/i.test(s) || /pp\b/i.test(s)) {
      var n0 = parseMetricNumber(s);
      if (n0 != null && kind === "occupancy" && !/pts?|points|pp/i.test(s)) {
        return (n0 > 0 ? "+" : n0 < 0 ? "-" : "") + formatMetricNumber(n0) + " pts";
      }
      if (n0 != null && /%\+/.test(s)) {
        return formatMetricNumber(n0) + "%+";
      }
      if (n0 != null && !/[-+]/.test(s) && /revpar|noi|margin|lift|adr|direct|revenue/i.test(kind || "")) {
        if (n0 > 0) return "+" + s.replace(/^\+/, "");
      }
      return s;
    }

    var n = parseMetricNumber(s);
    if (n == null) return s;

    if (kind === "occupancy") {
      return (n > 0 ? "+" : n < 0 ? "-" : "") + formatMetricNumber(n) + " pts";
    }
    if (kind === "retention") {
      if (n > 0 && n <= 1) n = n * 100;
      return formatMetricNumber(n) + "%";
    }
    if (kind === "guest") {
      if (n >= 85 && n <= 100) return formatMetricNumber(n) + "%+";
      if (n > 0 && n <= 100) return formatSignedPercent(n);
      return s;
    }
    if (kind === "revpar" || kind === "noi" || kind === "adr" || kind === "margin" || kind === "direct" || kind === "lift") {
      if (n > 0 && n <= 100) return formatSignedPercent(n);
      return s;
    }
    if (n > 0 && n <= 100) return formatSignedPercent(n);
    return s;
  }

  function achievementToMetric(line, label, kind) {
    var meta = ASSET_KPI_META[label] || {};
    var pct = line.match(/(\d+(?:\.\d+)?)\s*%\+?|\+?\s*(\d+(?:\.\d+)?)\s*%/);
    if (!pct) return null;
    var value = formatAssetMetricValue(pct[1] || pct[2], kind || meta.kind);
    var note = line
      .replace(/^[+\s]*\d+(?:\.\d+)?\s*%\+?\s*[-–—:]?\s*/i, "")
      .replace(/^\d+(?:\.\d+)?\s*%\+?\s*[-–—:]?\s*/i, "")
      .trim();
    if (!note || note.length < 12) note = meta.note || line;
    return metricWithLabelLines({ value: value, label: label, note: note });
  }

  function deriveAssetValueMetrics(vm) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var metrics = [];

    function add(value, label, note) {
      var v = nz(value);
      if (!v || v === "—" || /^not yet/i.test(v)) return;
      var meta = ASSET_KPI_META[label] || {};
      metrics.push(
        metricWithLabelLines({
          value: formatAssetMetricValue(v, meta.kind),
          label: label,
          note: nz(note) || meta.note || "",
        })
      );
    }

    add(p.revparImprovement, "RevPAR index lift", "");
    add(p.noiImprovement || pick(ex, p, "noiImprovement", ""), "NOI improvement", "");
    add(p.occupancyImprovement, "Occupancy improvement", "");
    add(p.ownerRetention || p.renewalRate, "Owner retention / renewal", "");

    var achievements = parseAchievementLines(p);
    achievements.forEach(function (line) {
      if (metrics.length >= 4) return;
      var lower = line.toLowerCase();
      var parsed;
      if (/guest|satisfaction|review|reputation/i.test(lower)) {
        parsed = achievementToMetric(line, "Guest satisfaction", "guest");
      } else if (/revpar|adr|revenue/i.test(lower) && metrics.length < 4) {
        parsed = achievementToMetric(line, "RevPAR index lift", "revpar");
      } else if (/noi|gop|margin|profit/i.test(lower) && metrics.length < 4) {
        var marginLabel = /gop/i.test(lower) ? "Avg. GOP margin" : "Operating margin";
        parsed = achievementToMetric(line, marginLabel, "margin");
      } else if (/direct|booking|loyalty|digital/i.test(lower) && metrics.length < 4) {
        parsed = achievementToMetric(line, "Direct booking contribution", "direct");
      }
      if (parsed) metrics.push(parsed);
    });

    return metrics.slice(0, 4);
  }

  function deriveTrackRecordSignalMetrics(vm) {
    var ex = (vm && vm.ex) || {};
    var p = (vm && vm.prefill) || {};
    var rows = [];

    function add(key, label) {
      var v = nz(pick(ex, p, key, ""));
      if (!v || v === "—" || /^not yet/i.test(v)) return;
      var meta = TRACK_RECORD_KPI_META[label] || {};
      rows.push(
        metricWithLabelLines({
          value: v,
          label: label,
          note: meta.note || "",
        })
      );
    }

    add("tr_signal_occ", "Occupancy recovery window");
    add("tr_signal_adr", "ADR stabilization");
    add("tr_signal_repeat", "Case repeatability");
    return rows;
  }

  function deriveAssetValueCreationSnapshotMetrics(vm) {
    return deriveAssetValueMetrics(vm).concat(deriveTrackRecordSignalMetrics(vm)).slice(0, 8);
  }

  var VALUE_CREATION_LEVERS = [
    [
      "Revenue Growth",
      "Turns demand into rate and mix gains through disciplined pricing, channel rebalancing, and direct-booking programs. Focuses on who books, what they pay, and which segments drive the strongest contribution—not occupancy alone.",
    ],
    [
      "Margin Protection",
      "Protects owner cash flow by aligning labor to demand, tightening procurement, and tracking energy and controllable costs with weekly rigor. Builds operating discipline that holds up when revenue softens or input costs rise.",
    ],
    [
      "Reputation Lift",
      "Treats guest experience and online reputation as revenue assets—service standards, recovery protocols, review response, and on-property programming that lift scores and support rate integrity over time.",
    ],
    [
      "Exit Value",
      "Prepares the asset for refinance or sale with clean financials, stabilized operations, and a credible story for lenders and buyers. Stronger documentation and performance narrative can support valuation and reduce uncertainty at exit.",
    ],
    [
      "CapEx Discipline",
      "Sequences renovation and FF&E so spend ties to measurable guest and financial impact. Prioritizes work that supports repositioning and brand standards—avoiding scattershot projects owners cannot underwrite.",
    ],
    [
      "Owner Visibility",
      "Keeps owners informed with timely reporting, actionable dashboards, and plain-language commentary on performance, risks, and decisions—not backward-looking statements alone.",
    ],
  ];

  function buildValueCreationLeversBlock() {
    return (
      '<section class="section">' +
      '<h2 class="section-title">Value Creation Levers</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">The main ways an operator can improve owner outcomes across the asset lifecycle.</p>' +
      '<div class="grid-3">' +
      VALUE_CREATION_LEVERS.map(function (pair) {
        return (
          '<div class="card"><h3>' +
          escapeHtml(pair[0]) +
          "</h3><p>" +
          escapeHtml(pair[1]) +
          "</p></div>"
        );
      }).join("") +
      "</div></section>"
    );
  }

  function buildAssetValueCreationBlock(vm) {
    var Gold = typeof window !== "undefined" ? window.OperatorExplorerGoldMock : null;
    var metrics = deriveAssetValueCreationSnapshotMetrics(vm);
    var intro =
      '<p class="gold-mock-tab-empty odna-subsection-intro">How this operator describes improving asset value on past engagements—not as a forecast or guarantee for your deal.</p>';
    var emptyHtml =
      '<p class="gold-mock-tab-empty">Asset value metrics not yet provided in Operator Setup (e.g. RevPAR improvement, NOI, track record signals, notable achievements).</p>';

    if (!metrics.length) {
      return (
        '<section class="section oe-asset-value-snapshot-section odna-asset-value-section">' +
        '<h2 class="section-title">Asset Value Creation</h2>' +
        intro +
        emptyHtml +
        "</section>"
      );
    }

    if (!Gold || typeof Gold.buildValueKpiSnapshotSection !== "function") {
      return (
        '<section class="section oe-asset-value-snapshot-section odna-asset-value-section">' +
        '<h2 class="section-title">Asset Value Creation</h2>' +
        intro +
        emptyHtml +
        "</section>"
      );
    }

    return Gold.buildValueKpiSnapshotSection({
      title: "Asset Value Creation",
      intro: intro,
      metrics: metrics,
      sectionClass: "oe-asset-value-snapshot-section odna-asset-value-section",
      kpiClass: "oe-asset-value-snapshot-kpis",
    });
  }

  /**
   * @param {Record<string, string>} panels - gold-mock panel HTML map
   * @param {object} vm - gold-mock view model
   */
  function enhancePanels(panels, vm) {
    var out = Object.assign({}, panels || {});
    var keyProof = "Proof & Track Record";
    if (out[keyProof] != null) {
      out[keyProof] =
        buildAssetValueCreationBlock(vm) + buildValueCreationLeversBlock() + out[keyProof];
    }
    return out;
  }

  global.OperatorDnaProfileConsolidate = {
    enhancePanels: enhancePanels,
  };
})(typeof window !== "undefined" ? window : global);
