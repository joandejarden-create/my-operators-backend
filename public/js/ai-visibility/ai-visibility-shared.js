/**
 * Shared AI Visibility UI helpers (Brand now; Operator later).
 */
(function (global) {
  function availabilityClass(availability) {
    return "aiv-avail-" + String(availability || "unavailable");
  }

  function formatMetricCell(metric) {
    if (!metric) return '<span class="aiv-avail-unavailable">Unavailable</span>';
    var display = metric.display != null ? metric.display : "—";
    if (metric.rank != null && metric.peerCount != null) {
      display = "#" + metric.rank + " of " + metric.peerCount;
    } else if (metric.rank != null && metric.display) {
      display = metric.display;
    } else if (typeof metric.value === "number" && metric.display == null) {
      // Counts (Questions Won / Missing) must never be shown as rates.
      if (metric.unit === "count") {
        display = String(Math.round(metric.value));
      } else {
        display =
          metric.value <= 1
            ? (Math.round(metric.value * 1000) / 10).toFixed(1) + "%"
            : Number(metric.value).toFixed(1) + "%";
      }
    }
    return (
      '<span class="' +
      availabilityClass(metric.availability) +
      '">' +
      escapeHtml(String(display)) +
      "</span>"
    );
  }

  function formatDeltaCell(metric) {
    if (!metric) {
      return '<span class="aiv-avail-not_monitored aiv-delta-none">—</span>';
    }
    var avail = metric.availability || "unavailable";
    if (
      avail === "not_monitored" ||
      avail === "unavailable" ||
      avail === "not_comparable" ||
      avail === "insufficient_history" ||
      avail === "provider_error" ||
      avail === "partial_monitoring"
    ) {
      var label =
        metric.display ||
        (avail === "not_comparable"
          ? "Not Comparable"
          : avail === "insufficient_history"
            ? "Insufficient History"
            : avail === "provider_error"
              ? "Provider Error"
              : avail === "partial_monitoring"
                ? "Partial Monitoring"
                : "—");
      return (
        '<span class="' +
        availabilityClass(avail) +
        ' aiv-delta-none">' +
        escapeHtml(String(label)) +
        "</span>"
      );
    }
    // Prefer explicit display (including "0 pp"); do not treat numeric 0 as missing.
    var hasNumeric =
      (typeof metric.value === "number" && isFinite(metric.value)) ||
      (typeof metric.absoluteDeltaPp === "number" && isFinite(metric.absoluteDeltaPp));
    var display =
      metric.display != null && String(metric.display).trim() !== ""
        ? String(metric.display)
        : null;
    if (!display && !hasNumeric) {
      return '<span class="aiv-avail-not_monitored aiv-delta-none">—</span>';
    }
    if (!display && hasNumeric) {
      var pp =
        typeof metric.absoluteDeltaPp === "number"
          ? metric.absoluteDeltaPp
          : Math.round(metric.value * 1000) / 10;
      display = pp === 0 ? "0 pp" : (pp > 0 ? "+" : "−") + Math.abs(pp) + " pp";
    }
    var dir = metric.direction || "flat";
    if (!metric.direction && hasNumeric) {
      var n =
        typeof metric.absoluteDeltaPp === "number"
          ? metric.absoluteDeltaPp
          : metric.value;
      dir = n > 0 ? "up" : n < 0 ? "down" : "flat";
    }
    var dirClass =
      dir === "up" ? "aiv-delta-up" : dir === "down" ? "aiv-delta-down" : "aiv-delta-flat";
    return (
      '<span class="' +
      availabilityClass(avail) +
      " " +
      dirClass +
      '">' +
      escapeHtml(display) +
      "</span>"
    );
  }

  function statusBadge(status) {
    var s = String(status || "Missing");
    var cls = "aiv-badge-mention";
    if (s === "Missing") cls = "aiv-badge-missing";
    else if (s === "Present") cls = "aiv-badge-present";
    else if (s === "First Recommended") cls = "aiv-badge-won";
    else if (s === "Recommended") cls = "aiv-badge-rec";
    return '<span class="aiv-badge ' + cls + '">' + escapeHtml(s) + "</span>";
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function renderTrend(container, points, helpEl, ranges) {
    if (!container) return;
    if (!points || !points.length) {
      container.innerHTML =
        '<div class="aiv-empty">Not Monitored for this geography yet.</div>';
      if (helpEl) helpEl.textContent = "";
      return;
    }
    var max = Math.max.apply(
      null,
      points.map(function (p) {
        return typeof p.value === "number" ? p.value : 0;
      }).concat([0.01])
    );
    container.innerHTML = points
      .map(function (p) {
        var pct = typeof p.value === "number" ? Math.max(4, Math.round((p.value / max) * 100)) : 4;
        var label = (p.date || "").slice(0, 10);
        var val =
          typeof p.value === "number"
            ? p.value <= 1
              ? Math.round(p.value * 1000) / 10 + "%"
              : String(p.value)
            : "—";
        return (
          '<div class="aiv-trend-point">' +
          '<div class="aiv-trend-bar" style="height:' +
          pct +
          '%" title="' +
          escapeHtml(val) +
          '"></div>' +
          '<div class="aiv-trend-label">' +
          escapeHtml(label) +
          "<br>" +
          escapeHtml(val) +
          "</div></div>"
        );
      })
      .join("");
    if (helpEl) {
      helpEl.textContent =
        points.length <= 1
          ? "Trend will develop as additional monitoring periods are completed."
          : "Points are actual completed monitoring periods only.";
    }
    var pills = document.getElementById("aivRangePills");
    if (pills && ranges) {
      pills.innerHTML = ["30D", "90D", "6M", "1Y"]
        .map(function (key) {
          var supported = ranges[key] === true;
          return (
            '<span class="' +
            (supported ? "is-on" : "") +
            '" title="' +
            escapeHtml(String(ranges[key])) +
            '">' +
            key +
            (supported ? "" : " · n/a") +
            "</span>"
          );
        })
        .join("");
    }
  }

  global.AiVisibilityUi = {
    availabilityClass: availabilityClass,
    formatMetricCell: formatMetricCell,
    formatDeltaCell: formatDeltaCell,
    statusBadge: statusBadge,
    escapeHtml: escapeHtml,
    renderTrend: renderTrend,
  };
})(window);
