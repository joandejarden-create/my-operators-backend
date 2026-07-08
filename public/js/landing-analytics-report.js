(function () {
  "use strict";

  var TOKEN_KEY = "dl_la_report_token_v1";
  var ACCESS_KEY_STORAGE = "dl_la_report_key_v1";
  var activeSessionId = null;

  function $(id) {
    return document.getElementById(id);
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtTime(iso) {
    if (!iso) return "—";
    try {
      var d = new Date(iso);
      return d.toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
    } catch (_e) {
      return iso;
    }
  }

  function fmtRange(window) {
    if (!window || !window.firstEventAt || !window.lastEventAt) return "No events in this window";
    return fmtTime(window.firstEventAt) + " → " + fmtTime(window.lastEventAt);
  }

  function setBanner(msg, kind) {
    var el = $("laBanner");
    el.textContent = msg || "";
    el.className = "banner" + (kind ? " " + kind : "");
  }

  function extractToken(raw) {
    if (!raw) return null;
    var s = String(raw).trim();
    if (!s) return null;
    try {
      if (s.indexOf("msToken=") >= 0) {
        var u = new URL(s.indexOf("http") === 0 ? s : "https://x.test/?" + s.replace(/^\?/, ""));
        s = u.searchParams.get("msToken") || u.searchParams.get("memberstackToken") || s;
      }
    } catch (_e) {}
    if (s.indexOf("Bearer ") === 0) s = s.slice(7).trim();
    if (s.indexOf("eyJ") !== 0) return null;
    return s;
  }

  function storeToken(token) {
    try {
      sessionStorage.setItem(TOKEN_KEY, token);
    } catch (_e) {}
    if (window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.acceptEmbedJwt) {
      window.DealalityMemberstackAuth.acceptEmbedJwt(token);
    }
  }

  function reportKeyFromPage() {
    try {
      var params = new URLSearchParams(window.location.search);
      var fromUrl = params.get("key");
      if (fromUrl && String(fromUrl).trim()) return String(fromUrl).trim();
    } catch (_e) {}
    try {
      var saved = sessionStorage.getItem(ACCESS_KEY_STORAGE);
      if (saved) return String(saved).trim();
    } catch (_e2) {}
    return null;
  }

  function storeReportKey(key) {
    if (!key) return;
    try {
      sessionStorage.setItem(ACCESS_KEY_STORAGE, key);
    } catch (_e) {}
  }

  function appendReportKey(path) {
    var key = reportKeyFromPage();
    if (!key) return path;
    var sep = path.indexOf("?") >= 0 ? "&" : "?";
    return path + sep + "key=" + encodeURIComponent(key);
  }

  function tokenFromPage() {
    try {
      var params = new URLSearchParams(window.location.search);
      var fromUrl = params.get("msToken") || params.get("memberstackToken");
      if (fromUrl) return extractToken(fromUrl);
    } catch (_e) {}
    try {
      var saved = sessionStorage.getItem(TOKEN_KEY);
      if (saved) return extractToken(saved);
    } catch (_e2) {}
    return null;
  }

  function selectedDays() {
    return $("laDays").value || "7";
  }

  function selectedExcludeInternal() {
    return Boolean($("laExcludeInternal") && $("laExcludeInternal").checked);
  }

  function syncFiltersToUrl(days, excludeInternal) {
    try {
      var url = new URL(window.location.href);
      url.searchParams.set("days", String(days));
      if (excludeInternal) {
        url.searchParams.set("excludeInternal", "1");
      } else {
        url.searchParams.delete("excludeInternal");
      }
      window.history.replaceState(null, "", url.toString());
    } catch (_e) {}
  }

  function applyFiltersFromUrl() {
    try {
      var params = new URLSearchParams(window.location.search);
      var days = params.get("days");
      if (days && $("laDays")) {
        var option = $("laDays").querySelector('option[value="' + days + '"]');
        if (option) $("laDays").value = days;
      }
      if ($("laExcludeInternal")) {
        $("laExcludeInternal").checked = params.get("excludeInternal") === "1";
      }
    } catch (_e2) {}
  }

  async function apiFetch(path) {
    var headers = { "Content-Type": "application/json" };
    var reportKey = reportKeyFromPage();
    var fetchPath = appendReportKey(path);

    if (!reportKey) {
      var pre = tokenFromPage();
      if (pre) storeToken(pre);
      if (window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.getAuthHeaders) {
        var auth = await window.DealalityMemberstackAuth.getAuthHeaders(null, {
          waitForLogin: true,
          maxWaitMs: 3000,
        });
        if (auth.error) throw new Error(auth.error);
        Object.assign(headers, auth.headers);
      }
    }

    var res = await fetch(fetchPath, {
      headers: headers,
      cache: "no-store",
    });
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok) {
      var msg = (data && data.message) || (data && data.error) || res.statusText;
      if (res.status === 401 && !data.message) {
        msg = reportKey
          ? "Access key rejected — it must match LANDING_ANALYTICS_REPORT_KEY on Railway exactly."
          : "Access required — open with ?key=YOUR_KEY or use Advanced admin token.";
      }
      if (res.status === 503 && data.error === "report_key_not_configured") {
        msg = data.message;
      }
      if (res.status === 403) msg = "Admin access required for this report.";
      throw new Error(msg);
    }
    return data;
  }

  function renderInsights(insights) {
    var el = $("laInsights");
    if (!insights || !insights.length) {
      el.innerHTML = "";
      el.hidden = true;
      return;
    }
    el.hidden = false;
    el.innerHTML = insights
      .map(function (item) {
        return (
          '<article class="insight insight--' +
          esc(item.tone || "info") +
          '">' +
          '<h3>' +
          esc(item.title) +
          "</h3>" +
          "<p>" +
          esc(item.body) +
          "</p>" +
          "</article>"
        );
      })
      .join("");
  }

  function renderTrafficSegments(segments) {
    var el = $("laTrafficSegments");
    if (!el) return;
    if (!segments) {
      el.innerHTML = '<p class="empty">No segment data yet.</p>';
      return;
    }

    var rows = [
      {
        label: "External Sessions",
        value: segments.externalSessions || 0,
      },
      {
        label: "Internal Sessions (" + (segments.internalLocationLabel || "Barcelona") + ")",
        value: segments.internalSessions || 0,
      },
      {
        label: "External Events",
        value: segments.externalEvents || 0,
      },
      {
        label: "Internal Events",
        value: segments.internalEvents || 0,
      },
    ];

    el.innerHTML =
      rows
        .map(function (row) {
          return (
            '<div class="segment-row"><span>' +
            esc(row.label) +
            '</span><span class="segment-row__value">' +
            esc(row.value) +
            "</span></div>"
          );
        })
        .join("") +
      '<p class="panel-sub" style="margin:10px 0 0;">' +
      (segments.excludedInternal
        ? "Internal traffic is excluded from charts and KPIs."
        : "Toggle exclude to remove internal traffic from all charts and KPIs.") +
      "</p>";
  }

  function renderHeroKpis(totals, funnel) {
    var sessions = totals.sessions || 0;
    var ctaStep = (funnel && funnel.steps && funnel.steps.find(function (s) {
      return s.key === "cta_click";
    })) || { rate: 0, count: 0 };
    var deepStep = (funnel && funnel.steps && funnel.steps.find(function (s) {
      return s.key === "deep_engagement";
    })) || { rate: 0 };

    var cards = [
      {
        label: "Visitors",
        value: sessions,
        sub:
          (totals.embedSessions || 0) +
          " From Homepage Embed" +
          (totals.insightsSessions
            ? " · " + totals.insightsSessions + " On /insights"
            : ""),
        accent: "primary",
      },
      {
        label: "Deep Engagement",
        value: deepStep.rate + "%",
        sub: "Reached FAQ / Why Section",
        accent: "teal",
      },
      {
        label: "CTA Clicks",
        value: ctaStep.count,
        sub: ctaStep.rate + "% of Sessions",
        accent: "violet",
      },
      {
        label: "Email Signups",
        value: totals.emailCaptures || 0,
        sub: "Successful Captures",
        accent: "green",
      },
    ];

    if (totals.videoOpens > 0 && totals.videoCompletionRate != null) {
      cards.push({
        label: "Video Completion Rate",
        value: totals.videoCompletionRate + "%",
        sub: (totals.videoCompletes || 0) + " of " + totals.videoOpens + " Opens",
        accent: "teal",
      });
    }

    cards.push(
      {
        label: "Avg Events / Visit",
        value: totals.eventsPerSession != null ? totals.eventsPerSession : "—",
        sub: (totals.events || 0) + " Total Events",
        accent: "muted",
      },
      {
        label: "Time to First Scroll",
        value:
          totals.medianFirstScrollSeconds != null ? totals.medianFirstScrollSeconds + "s" : "—",
        sub: "Median Across Sessions",
        accent: "muted",
      }
    );

    $("laCards").innerHTML = cards
      .map(function (c) {
        return (
          '<div class="kpi kpi--' +
          esc(c.accent) +
          '">' +
          '<div class="kpi__label">' +
          esc(c.label) +
          "</div>" +
          '<div class="kpi__value">' +
          esc(c.value) +
          "</div>" +
          '<div class="kpi__sub">' +
          esc(c.sub) +
          "</div>" +
          "</div>"
        );
      })
      .join("");
    $("laCards").hidden = false;
  }

  function renderFunnel(funnel) {
    var el = $("laFunnel");
    if (!funnel || !funnel.steps || !funnel.steps.length) {
      el.innerHTML = '<p class="empty">No Funnel Data Yet</p>';
      return;
    }
    var maxCount = funnel.steps[0].count || 1;
    el.innerHTML = funnel.steps
      .map(function (step, i) {
        var width = maxCount ? Math.max(8, Math.round((step.count / maxCount) * 100)) : 8;
        var drop =
          i > 0 && funnel.steps[i - 1].count
            ? Math.max(0, funnel.steps[i - 1].count - step.count)
            : 0;
        var dropHtml =
          drop > 0
            ? '<span class="funnel__drop">−' + drop + " From Prior Step</span>"
            : "";
        return (
          '<div class="funnel__row">' +
          '<div class="funnel__meta">' +
          '<span class="funnel__label">' +
          esc(step.label) +
          "</span>" +
          '<span class="funnel__stats">' +
          esc(step.count) +
          " · " +
          esc(step.rate) +
          "%</span>" +
          dropHtml +
          "</div>" +
          '<div class="funnel__track"><div class="funnel__bar" style="width:' +
          width +
          '%"></div></div>' +
          "</div>"
        );
      })
      .join("");
  }

  function renderSectionJourney(journey) {
    var el = $("laJourney");
    if (!journey || !journey.length) {
      el.innerHTML = '<p class="empty">No Section Journey Yet</p>';
      return;
    }
    el.innerHTML = journey
      .map(function (row, i) {
        return (
          '<div class="journey__step">' +
          '<div class="journey__num">' +
          (i + 1) +
          "</div>" +
          '<div class="journey__body">' +
          '<div class="journey__head">' +
          '<span class="journey__label">' +
          esc(row.label) +
          "</span>" +
          '<span class="journey__pct">' +
          esc(row.rate) +
          "%</span>" +
          "</div>" +
          '<div class="journey__track"><div class="journey__bar" style="width:' +
          Math.max(4, row.rate) +
          '%"></div></div>' +
          '<div class="journey__count">' +
          esc(row.sessions) +
          " session" +
          (row.sessions === 1 ? "" : "s") +
          "</div>" +
          "</div>" +
          (i < journey.length - 1 ? '<div class="journey__connector" aria-hidden="true"></div>' : "") +
          "</div>"
        );
      })
      .join("");
  }

  function renderBarList(containerId, rows, emptyLabel) {
    var el = $(containerId);
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="empty">' + esc(emptyLabel || "No Data") + "</p>";
      return;
    }
    var max = rows[0].count || 1;
    el.innerHTML = rows
      .map(function (row) {
        var label = row.label || row.key;
        var width = Math.max(6, Math.round((row.count / max) * 100));
        return (
          '<div class="barlist__row">' +
          '<div class="barlist__meta">' +
          '<span class="barlist__label">' +
          esc(label) +
          "</span>" +
          '<span class="barlist__count">' +
          esc(row.count) +
          "</span>" +
          "</div>" +
          '<div class="barlist__track"><div class="barlist__bar" style="width:' +
          width +
          '%"></div></div>' +
          "</div>"
        );
      })
      .join("");
  }

  function renderDevices(rows) {
    var mapped = (rows || []).map(function (row) {
      return { key: row.key, label: row.label || row.key, count: row.count };
    });
    renderBarList("laDevices", mapped, "No Device Data Yet");
  }

  function renderEngagementMilestones(rows) {
    renderBarList(
      "laEngagement",
      rows,
      "No time-on-page milestones yet — fires at 30s, 60s, and 120s."
    );
  }

  function renderInteractions(interactions) {
    var el = $("laInteractions");
    if (!interactions || !interactions.items || !interactions.items.length) {
      el.innerHTML =
        '<p class="empty">No content interactions yet — open the hero video, expand FAQ items, or switch audience tabs on the homepage.</p>';
      return;
    }
    var html = "";
    html += '<div class="barlist">';
    var max = interactions.items[0].count || 1;
    html += interactions.items
      .map(function (row) {
        var width = Math.max(6, Math.round((row.count / max) * 100));
        return (
          '<div class="barlist__row">' +
          '<div class="barlist__meta">' +
          '<span class="barlist__label">' +
          esc(row.label) +
          "</span>" +
          '<span class="barlist__count">' +
          esc(row.count) +
          "</span>" +
          "</div>" +
          '<div class="barlist__track"><div class="barlist__bar" style="width:' +
          width +
          '%"></div></div>' +
          "</div>"
        );
      })
      .join("");
    html += "</div>";

    if (interactions.audienceTabs && interactions.audienceTabs.length) {
      html +=
        '<p class="panel-sub" style="margin:14px 0 8px;">Audience Tabs</p>';
      html += interactions.audienceTabs
        .map(function (row) {
          return (
            '<div class="barlist__row">' +
            '<div class="barlist__meta">' +
            '<span class="barlist__label">' +
            esc(row.label) +
            "</span>" +
            '<span class="barlist__count">' +
            esc(row.count) +
            "</span>" +
            "</div></div>"
          );
        })
        .join("");
    }

    if (interactions.navClicks && interactions.navClicks.length) {
      html +=
        '<p class="panel-sub" style="margin:14px 0 8px;">Top Nav Clicks</p>';
      html += interactions.navClicks
        .slice(0, 6)
        .map(function (row) {
          return (
            '<div class="barlist__row">' +
            '<div class="barlist__meta">' +
            '<span class="barlist__label">' +
            esc(row.label) +
            "</span>" +
            '<span class="barlist__count">' +
            esc(row.count) +
            "</span>" +
            "</div></div>"
          );
        })
        .join("");
    }

    el.innerHTML = html;
  }

  function fmtDuration(seconds) {
    if (seconds == null || seconds === 0) return "—";
    var n = Math.round(seconds);
    if (n < 60) return n + "s";
    var m = Math.floor(n / 60);
    var s = n % 60;
    return m + "m" + (s ? " " + s + "s" : "");
  }

  function renderDashboardLegend(channels) {
    if (!channels || !channels.length) return "";
    return (
      '<div class="dash-legend">' +
      channels
        .map(function (ch) {
          return (
            '<span class="dash-legend__item">' +
            '<span class="dash-legend__swatch" style="background:' +
            esc(ch.color) +
            '"></span>' +
            esc(ch.label) +
            (ch.count != null ? " (" + esc(ch.count) + ")" : "") +
            "</span>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function renderAudienceOverview(audience, elId) {
    var el = $(elId || "laAudienceOverview");
    if (!el) return;
    if (!audience || !audience.days || !audience.days.length) {
      el.innerHTML =
        '<p class="empty">No session data yet — visits appear after homepage traffic is recorded.</p>';
      return;
    }

    var maxSessions = 1;
    audience.days.forEach(function (row) {
      if (row.sessions > maxSessions) maxSessions = row.sessions;
    });

    var html = '<div class="overview-metrics">';
    html +=
      '<div class="overview-metric"><span class="overview-metric__value">' +
      esc(audience.totalSessions) +
      '</span><span class="overview-metric__label">Sessions</span></div>';
    html +=
      '<div class="overview-metric"><span class="overview-metric__value">' +
      esc(fmtDuration(audience.medianDurationSeconds)) +
      '</span><span class="overview-metric__label">Median session</span></div>';
    html += "</div>";

    var w = 400;
    var h = 148;
    var pad = { l: 12, r: 12, t: 22, b: 30 };
    var innerW = w - pad.l - pad.r;
    var innerH = h - pad.t - pad.b;
    var days = audience.days;
    var points = days.map(function (row, i) {
      var x =
        pad.l +
        (days.length === 1 ? innerW / 2 : (i / (days.length - 1)) * innerW);
      var y = pad.t + innerH - (row.sessions / maxSessions) * innerH;
      return {
        x: x,
        y: y,
        label: row.label,
        sessions: row.sessions,
        duration: row.medianDurationSeconds,
      };
    });
    var polyline = points
      .map(function (p) {
        return p.x.toFixed(1) + "," + p.y.toFixed(1);
      })
      .join(" ");

    html +=
      '<svg class="line-chart" viewBox="0 0 ' +
      w +
      " " +
      h +
      '" role="img" aria-label="Sessions over time">';
    html +=
      '<polyline fill="none" stroke="#5b8cff" stroke-width="2.5" stroke-linejoin="round" points="' +
      polyline +
      '"/>';
    points.forEach(function (p) {
      var tip =
        esc(p.label) +
        ": " +
        esc(p.sessions) +
        " sessions" +
        (p.duration != null ? ", median " + esc(fmtDuration(p.duration)) : "");
      html +=
        '<circle cx="' +
        p.x +
        '" cy="' +
        p.y +
        '" r="4" fill="#5b8cff"><title>' +
        tip +
        "</title></circle>";
      html +=
        '<text x="' +
        p.x +
        '" y="' +
        Math.max(12, p.y - 9) +
        '" text-anchor="middle" class="line-chart__value">' +
        esc(p.sessions) +
        "</text>";
      html +=
        '<text x="' +
        p.x +
        '" y="' +
        (h - 8) +
        '" text-anchor="middle" class="line-chart__label">' +
        esc(p.label) +
        "</text>";
    });
    html += "</svg>";

    html += '<table class="dash-table line-chart__table" aria-label="Sessions by day data table">';
    html += "<thead><tr><th>Day</th><th>Sessions</th><th>Median Session</th></tr></thead><tbody>";
    html += days
      .map(function (row) {
        return (
          "<tr><td>" +
          esc(row.label) +
          "</td><td>" +
          esc(row.sessions) +
          "</td><td>" +
          esc(fmtDuration(row.medianDurationSeconds)) +
          "</td></tr>"
        );
      })
      .join("");
    html += "</tbody></table>";
    el.innerHTML = html;
  }

  function renderAcquisitionReport(acquisition, elId) {
    var el = $(elId || "laAcquisition");
    if (!el) return;
    if (!acquisition || !acquisition.days || !acquisition.days.length) {
      el.innerHTML =
        '<p class="empty">No acquisition data yet — channel splits appear after landing visits.</p>';
      return;
    }

    var maxTotal = 1;
    acquisition.days.forEach(function (row) {
      if (row.total > maxTotal) maxTotal = row.total;
    });

    var colHeight = 110;
    var html =
      '<div class="stacked-chart" role="img" aria-label="Sessions by acquisition channel">';
    html += acquisition.days
      .map(function (day) {
        var stackHeight = Math.max(
          6,
          Math.round((day.total / maxTotal) * colHeight)
        );
        var segments = "";
        day.channels.forEach(function (ch) {
          var segH = Math.max(
            2,
            Math.round((ch.count / Math.max(1, day.total)) * stackHeight)
          );
          segments +=
            '<div class="stacked-chart__seg" style="height:' +
            segH +
            "px;background:" +
            esc(ch.color) +
            '" title="' +
            esc(ch.label) +
            ": " +
            esc(ch.count) +
            '"></div>';
        });
        return (
          '<div class="stacked-chart__col">' +
          '<div class="stacked-chart__value">' +
          esc(day.total) +
          "</div>" +
          '<div class="stacked-chart__stack" style="height:' +
          stackHeight +
          'px">' +
          segments +
          "</div>" +
          '<div class="stacked-chart__label">' +
          esc(day.label) +
          "</div>" +
          "</div>"
        );
      })
      .join("");
    html += "</div>";
    html += renderDashboardLegend(acquisition.totals || []);
    el.innerHTML = html;
  }

  function renderPopularPages(rows, elId) {
    var el = $(elId || "laPopularPages");
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="empty">No page views yet.</p>';
      return;
    }
    el.innerHTML =
      '<table class="dash-table"><thead><tr><th>Page</th><th>Pageviews</th></tr></thead><tbody>' +
      rows
        .map(function (row) {
          return (
            "<tr><td class=\"dash-table__page\">" +
            esc(row.label) +
            "</td><td>" +
            esc(row.pageviews) +
            "</td></tr>"
          );
        })
        .join("") +
      "</tbody></table>";
  }

  function renderSessionsByCountry(rows, elId) {
    var el = $(elId || "laSessionsByCountry");
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML =
        '<p class="empty">No country data yet — captured on new visits after deploy.</p>';
      return;
    }
    var max = rows[0].sessions || 1;
    el.innerHTML =
      '<div class="barlist">' +
      rows
        .map(function (row) {
          var width = Math.max(6, Math.round((row.sessions / max) * 100));
          return (
            '<div class="barlist__row">' +
            '<div class="barlist__meta">' +
            '<span class="barlist__label">' +
            esc(row.label) +
            "</span>" +
            '<span class="barlist__count">' +
            esc(row.sessions) +
            "</span>" +
            "</div>" +
            '<div class="barlist__track"><div class="barlist__bar barlist__bar--scroll" style="width:' +
            width +
            '%"></div></div>' +
            "</div>"
          );
        })
        .join("") +
      "</div>";
  }

  function renderSessionsByDevice(rows, elId) {
    var el = $(elId || "laSessionsByDevice");
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="empty">No device data yet.</p>';
      return;
    }

    var gradientParts = [];
    var start = 0;
    rows.forEach(function (row) {
      var end = start + row.rate;
      gradientParts.push(row.color + " " + start + "% " + end + "%");
      start = end;
    });

    var html = '<div class="device-donut-wrap">';
    html +=
      '<div class="device-donut" style="background:conic-gradient(' +
      gradientParts.join(", ") +
      ')" role="img" aria-label="Sessions by device"></div>';
    html += "<div style=\"flex:1;min-width:140px\">";
    html += rows
      .map(function (row) {
        return (
          '<div class="device-row">' +
          '<div class="device-row__meta">' +
          '<span class="device-row__swatch" style="background:' +
          esc(row.color) +
          '"></span>' +
          "<span>" +
          esc(row.label) +
          "</span>" +
          "</div>" +
          '<span class="device-row__pct">' +
          esc(row.rate) +
          "%</span>" +
          "<span>" +
          esc(row.sessions) +
          "</span>" +
          "</div>"
        );
      })
      .join("");
    html += "</div></div>";
    el.innerHTML = html;
  }

  function renderInsightsArticles(rows) {
    var el = $("laInsightsArticles");
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="empty">No article clicks yet.</p>';
      return;
    }
    el.innerHTML =
      '<table class="dash-table"><thead><tr><th>Article</th><th>Clicks</th></tr></thead><tbody>' +
      rows
        .map(function (row) {
          var lang = row.languageLabel
            ? ' <span class="raw-key">' + esc(row.languageLabel) + "</span>"
            : "";
          return (
            "<tr><td class=\"dash-table__page\">" +
            esc(row.label) +
            lang +
            "</td><td>" +
            esc(row.count) +
            "</td></tr>"
          );
        })
        .join("") +
      "</tbody></table>";
  }

  function renderInsightsHub(hub) {
    var section = $("laInsightsHubSection");
    if (!section) return;
    if (!hub || !hub.hasData) {
      section.hidden = true;
      return;
    }
    section.hidden = false;

    var kpis = $("laInsightsKpis");
    if (kpis) {
      var cards = [
        {
          label: "Insights Sessions",
          value: hub.totals.sessions,
          sub: hub.totals.pageviews + " Pageviews",
          accent: "primary",
        },
        {
          label: "Article Clicks",
          value: hub.totals.articleClicks,
          sub: "Card clicks to read",
          accent: "teal",
        },
        {
          label: "CTA Clicks",
          value: hub.totals.ctaClicks,
          sub: "Request Early Access / signup",
          accent: "violet",
        },
      ];
      kpis.innerHTML = cards
        .map(function (c) {
          return (
            '<div class="kpi kpi--' +
            esc(c.accent) +
            '"><div class="kpi__label">' +
            esc(c.label) +
            '</div><div class="kpi__value">' +
            esc(c.value) +
            '</div><div class="kpi__sub">' +
            esc(c.sub) +
            "</div></div>"
          );
        })
        .join("");
    }

    var dash = hub.dashboard || {};
    renderAudienceOverview(dash.audience, "laInsightsAudience");
    renderAcquisitionReport(dash.acquisition, "laInsightsAcquisition");
    renderInsightsArticles(hub.articleClicks);
    renderSessionsByCountry(dash.sessionsByCountry, "laInsightsCountries");
    renderSessionsByDevice(dash.sessionsByDevice, "laInsightsDevices");
    renderBarList(
      "laInsightsLanguages",
      (hub.languages || []).map(function (row) {
        return { key: row.key, label: row.label, count: row.count };
      }),
      "No language data yet — detected from article titles and lang attributes."
    );
    renderBarList(
      "laInsightsCtas",
      (hub.ctaLocations || []).map(function (row) {
        return { key: row.key, label: row.label, count: row.count };
      }),
      "No CTA clicks on /insights yet."
    );
    renderBarList(
      "laInsightsScroll",
      hub.scrollDepths || [],
      "No scroll milestones yet."
    );
  }

  function renderDashboard(dashboard) {
    if (!dashboard) {
      renderAudienceOverview(null);
      renderAcquisitionReport(null);
      renderPopularPages(null);
      renderSessionsByCountry(null);
      renderSessionsByDevice(null);
      return;
    }
    renderAudienceOverview(dashboard.audience);
    renderAcquisitionReport(dashboard.acquisition);
    renderPopularPages(dashboard.popularPages);
    renderSessionsByCountry(dashboard.sessionsByCountry);
    renderSessionsByDevice(dashboard.sessionsByDevice);
  }

  function renderBenchmarks(rows) {
    var el = $("laBenchmarks");
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="empty">No Benchmark Data Yet</p>';
      return;
    }
    el.innerHTML = rows
      .map(function (row) {
        return (
          '<div class="benchmark-row benchmark--' +
          esc(row.status) +
          '">' +
          '<div><div>' +
          esc(row.label) +
          '</div><div class="benchmark__target">Target ' +
          esc(row.targetRate) +
          "% · Floor " +
          esc(row.goodMin) +
          "%</div></div>" +
          '<div class="benchmark__rate">' +
          esc(row.actualRate) +
          "%</div></div>"
        );
      })
      .join("");
  }

  function renderDailyUniqueUsers(data) {
    var el = $("laDailyUsers");
    if (!el) return;
    if (!data || !data.days || !data.days.length) {
      el.innerHTML =
        '<p class="empty">No Daily User Data Yet — visits appear after homepage traffic is recorded.</p>';
      return;
    }

    var max = 1;
    data.days.forEach(function (row) {
      if (row.uniqueUsers > max) max = row.uniqueUsers;
    });

    var html = '<div class="daily-summary">';
    html +=
      "<span><strong>" +
      esc(data.totalUniqueUsers) +
      "</strong> Unique Users in Window</span>";
    if (data.latestDay) {
      html +=
        "<span>Latest Day: <strong>" +
        esc(data.latestDay.uniqueUsers) +
        "</strong> (" +
        esc(data.latestDay.label) +
        ")</span>";
    }
    if (data.changeVsPriorDay != null) {
      var trendClass =
        data.changeVsPriorDay > 0
          ? "daily-trend--up"
          : data.changeVsPriorDay < 0
            ? "daily-trend--down"
            : "daily-trend--flat";
      var sign = data.changeVsPriorDay > 0 ? "+" : "";
      html +=
        '<span class="' +
        trendClass +
        '">Vs Prior Day: ' +
        sign +
        esc(data.changeVsPriorDay) +
        "</span>";
    }
    if (data.weekOverWeekChange != null && (data.windowDays == null || data.windowDays >= 14)) {
      var wowClass =
        data.weekOverWeekChange > 0
          ? "daily-trend--up"
          : data.weekOverWeekChange < 0
            ? "daily-trend--down"
            : "daily-trend--flat";
      var wowSign = data.weekOverWeekChange > 0 ? "+" : "";
      html +=
        '<span class="' +
        wowClass +
        '">Last 7 Days vs Prior 7: ' +
        wowSign +
        esc(data.weekOverWeekChange) +
        " users</span>";
    }
    html += "</div>";

    html += '<div class="daily-chart" role="img" aria-label="Unique users per day chart">';
    html += data.days
      .map(function (row) {
        var height = Math.max(6, Math.round((row.uniqueUsers / max) * 110));
        return (
          '<div class="daily-chart__col">' +
          '<div class="daily-chart__value">' +
          esc(row.uniqueUsers) +
          "</div>" +
          '<div class="daily-chart__bar" style="height:' +
          height +
          'px" title="' +
          esc(row.label) +
          ": " +
          esc(row.uniqueUsers) +
          " unique users, " +
          esc(row.sessions) +
          ' sessions"></div>' +
          '<div class="daily-chart__label">' +
          esc(row.label) +
          "</div>" +
          "</div>"
        );
      })
      .join("");
    html += "</div>";

    html += '<div class="barlist">';
    html += data.days
      .slice()
      .reverse()
      .map(function (row) {
        var width = Math.max(6, Math.round((row.uniqueUsers / max) * 100));
        return (
          '<div class="barlist__row">' +
          '<div class="barlist__meta">' +
          '<span class="barlist__label">' +
          esc(row.label) +
          "</span>" +
          '<span class="barlist__count">' +
          esc(row.uniqueUsers) +
          " Users · " +
          esc(row.sessions) +
          " Sessions</span>" +
          "</div>" +
          '<div class="barlist__track"><div class="barlist__bar barlist__bar--scroll" style="width:' +
          width +
          '%"></div></div>' +
          "</div>"
        );
      })
      .join("");
    html += "</div>";

    if (data.peakDay && data.peakDay.uniqueUsers > 0) {
      html +=
        '<p class="panel-sub" style="margin:10px 0 0;">Peak day: ' +
        esc(data.peakDay.label) +
        " (" +
        esc(data.peakDay.uniqueUsers) +
        " users)</p>";
    }
    if (data.note) {
      html +=
        '<p class="panel-sub" style="margin:6px 0 0;">' + esc(data.note) + "</p>";
    }

    el.innerHTML = html;
  }

  function renderCtaPaths(ctaPaths) {
    renderBarList(
      "laCtaPaths",
      ctaPaths && ctaPaths.paths,
      "No CTA Path Data Yet"
    );
  }

  function renderFaqHeatmap(rows) {
    renderBarList("laFaq", rows, "No FAQ Expansions Yet");
  }

  function renderReturnVisitors(data) {
    var el = $("laReturnVisitors");
    if (!data) {
      el.innerHTML = '<p class="empty">No Return Visitor Data Yet</p>';
      return;
    }
    if (!data.totalVisitors) {
      var missing =
        data.sessionsWithoutVisitorId > 0
          ? esc(data.sessionsWithoutVisitorId) +
            " Sessions Recorded Without Visitor Id — Deploy the Latest Backend, Then Revisit the Homepage (Close Tab First for a Return Visit Test)."
          : "No Return Visitor Data Yet — Visit the Homepage, Then Close the Tab and Visit Again to Test a Return.";
      el.innerHTML = '<p class="empty">' + missing + "</p>";
      if (data.note) {
        el.innerHTML += '<p class="panel-sub">' + esc(data.note) + "</p>";
      }
      return;
    }
    var summary =
      '<p class="panel-sub" style="margin-top:0;">' +
      esc(data.returningVisitors) +
      " Returning · " +
      esc(data.firstTimeVisitors != null ? data.firstTimeVisitors : data.totalVisitors - data.returningVisitors) +
      " First-Time · " +
      esc(data.totalVisitors) +
      " Known Visitors (" +
      esc(data.returningRate) +
      "% Return Rate)</p>";
    if (!data.visitors || !data.visitors.length) {
      el.innerHTML =
        summary +
        '<p class="empty">No Repeat Visitors in This Window Yet — Close Your Browser Tab and Revisit the Homepage to Count as a Return.</p>';
      if (data.note) {
        el.innerHTML += '<p class="panel-sub">' + esc(data.note) + "</p>";
      }
      return;
    }
    el.innerHTML =
      summary +
      data.visitors
        .map(function (v) {
          return (
            '<div class="return-row"><div><code>' +
            esc(v.visitorId ? v.visitorId.slice(0, 14) + "…" : "—") +
            "</code><div>" +
            esc(v.sessionCount) +
            " Sessions · " +
            esc(v.visitDayCount) +
            " Days</div></div><div>" +
            esc(v.ctaSessions) +
            " CTA</div></div>"
          );
        })
        .join("") +
      (data.note ? '<p class="panel-sub">' + esc(data.note) + "</p>" : "");
  }

  function renderTiming(timing) {
    if (!timing) {
      $("laTimingHours").innerHTML = '<p class="empty">No Timing Data Yet</p>';
      $("laTimingDays").innerHTML = "";
      return;
    }
    var hours = (timing.hours || []).filter(function (h) {
      return h.count > 0;
    });
    renderBarList(
      "laTimingHours",
      hours.length ? hours : timing.hours,
      "No Hour Data Yet"
    );
    renderBarList("laTimingDays", timing.days, "No Day Data Yet");
    if (timing.peakHour && timing.peakHour.count > 0) {
      $("laTimingHours").insertAdjacentHTML(
        "beforeend",
        '<p class="panel-sub">Peak hour: ' +
          esc(timing.peakHour.label) +
          " (" +
          esc(timing.peakHour.count) +
          " sessions, " +
          esc(timing.timezone) +
          ")</p>"
      );
    }
  }

  function renderSessionStrip(sessions) {
    var el = $("laSessionStrip");
    if (!sessions || !sessions.length) {
      el.innerHTML = '<p class="empty">No Sessions to Replay Yet</p>';
      $("laSessionTimeline").innerHTML = "";
      return;
    }
    el.innerHTML = sessions
      .map(function (s) {
        var short = s.sessionId ? s.sessionId.slice(0, 12) + "…" : "—";
        var active = s.sessionId === activeSessionId ? " is-active" : "";
        return (
          '<button type="button" class="session-chip' +
          active +
          '" data-session-id="' +
          esc(s.sessionId) +
          '">' +
          esc(short) +
          '<span class="session-chip__meta">' +
          esc(s.embedLabel) +
          " · " +
          esc(s.eventCount) +
          " ev</span></button>"
        );
      })
      .join("");
    el.querySelectorAll(".session-chip").forEach(function (btn) {
      btn.addEventListener("click", function () {
        loadSessionReplay(btn.getAttribute("data-session-id"));
      });
    });
  }

  function renderSessionTimeline(data) {
    var el = $("laSessionTimeline");
    if (!data || !data.timeline || !data.timeline.length) {
      el.innerHTML = '<p class="empty">Select a Session Above</p>';
      return;
    }
    el.innerHTML =
      '<p class="panel-sub" style="margin-top:0;">' +
      esc(data.eventCount) +
      " Events · Session " +
      esc(data.sessionId ? data.sessionId.slice(0, 18) + "…" : "") +
      "</p>" +
      data.timeline
        .map(function (ev) {
          return (
            '<div class="timeline__event"><div class="timeline__time">' +
            esc(fmtTime(ev.ts)) +
            '</div><div><div class="timeline__label">' +
            esc(ev.label) +
            "</div>" +
            (ev.detail
              ? '<div class="timeline__detail">' + esc(ev.detail) + "</div>"
              : "") +
            "</div></div>"
          );
        })
        .join("");
  }

  async function loadSessionReplay(sessionId) {
    if (!sessionId) return;
    activeSessionId = sessionId;
    var chips = $("laSessionStrip").querySelectorAll(".session-chip");
    chips.forEach(function (btn) {
      btn.classList.toggle(
        "is-active",
        btn.getAttribute("data-session-id") === sessionId
      );
    });
    $("laSessionTimeline").innerHTML = '<p class="empty">Loading Session…</p>';
    try {
      var days = selectedDays();
      var excludeInternal = selectedExcludeInternal();
      var data = await apiFetch(
        "/api/marketing/landing-events/session?sessionId=" +
          encodeURIComponent(sessionId) +
          "&days=" +
          encodeURIComponent(days) +
          "&excludeInternal=" +
          (excludeInternal ? "1" : "0") +
          "&_=" +
          Date.now()
      );
      renderSessionTimeline(data);
    } catch (err) {
      $("laSessionTimeline").innerHTML =
        '<p class="empty">' + esc(err.message || "Could not load session") + "</p>";
    }
  }

  function downloadExport(format) {
    var days = selectedDays();
    var excludeInternal = selectedExcludeInternal();
    var url =
      appendReportKey(
        "/api/marketing/landing-events/export?days=" +
          encodeURIComponent(days) +
          "&excludeInternal=" +
          (excludeInternal ? "1" : "0") +
          "&format=" +
          encodeURIComponent(format)
      );
    window.location.href = url;
  }

  function formatStorageBanner(storage, totals, window) {
    var parts = [];
    if (window && window.label) {
      parts.push(window.label);
    } else if (window && window.days) {
      parts.push(
        window.days === 1 ? "Last 24 Hours" : "Last " + window.days + " Days"
      );
    }
    parts.push(
      (totals && totals.sessions ? totals.sessions : 0) + " Landing Sessions"
    );
    if (totals && totals.insightsSessions) {
      parts.push(totals.insightsSessions + " Insights Sessions");
    }
    if (totals && totals.locatedSessions != null) {
      parts.push(totals.locatedSessions + " With Location");
    }
    if (window && window.eventCount != null) {
      parts.push(window.eventCount + " Events in Window");
    }
    parts.push(fmtRange(window));
    if (storage) {
      if (storage.lineCount != null) {
        parts.push(storage.lineCount + " Stored Events");
      }
      if (!storage.persistent) {
        parts.push("Ephemeral Storage — Mount a Railway Volume to Keep History");
      }
    }
    return parts.join(" · ");
  }

  function renderScrollDepths(rows) {
    var el = $("laScroll");
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="empty">No scroll milestones yet — fires at 25%, 50%, 75%, 100%.</p>';
      return;
    }
    var sorted = rows.slice().sort(function (a, b) {
      return Number(a.key) - Number(b.key);
    });
    var max = sorted[sorted.length - 1].count || 1;
    el.innerHTML = sorted
      .map(function (row) {
        var width = Math.max(6, Math.round((row.count / max) * 100));
        return (
          '<div class="barlist__row">' +
          '<div class="barlist__meta">' +
          '<span class="barlist__label">' +
          esc(row.key) +
          "% Down Page</span>" +
          '<span class="barlist__count">' +
          esc(row.count) +
          "</span>" +
          "</div>" +
          '<div class="barlist__track"><div class="barlist__bar barlist__bar--scroll" style="width:' +
          width +
          '%"></div></div>' +
          "</div>"
        );
      })
      .join("");
  }

  function renderRecent(rows) {
    var tbody = $("laRecent").querySelector("tbody");
    tbody.innerHTML = "";
    if (!rows || !rows.length) {
      tbody.innerHTML = '<tr><td colspan="5">No Recent Activity</td></tr>';
      return;
    }
    rows.forEach(function (e) {
      var tr = document.createElement("tr");
      var sessionCell = "—";
      if (e.sessionId) {
        sessionCell =
          '<code class="session-link" data-session-id="' +
          esc(e.sessionId) +
          '" title="View session replay">' +
          esc(e.sessionId.slice(0, 10) + "…") +
          "</code>";
      }
      tr.innerHTML =
        "<td>" +
        esc(fmtTime(e.ts)) +
        "</td>" +
        "<td>" +
        esc(e.label || e.event) +
        "</td>" +
        "<td>" +
        esc(e.visitorLocation || "—") +
        "</td>" +
        "<td>" +
        sessionCell +
        "</td>" +
        "<td>" +
        esc(e.detail || "—") +
        "</td>";
      tbody.appendChild(tr);
    });
    tbody.querySelectorAll(".session-link").forEach(function (link) {
      link.addEventListener("click", function () {
        loadSessionReplay(link.getAttribute("data-session-id"));
        var panel = $("laSessionTimeline");
        if (panel && panel.scrollIntoView) {
          panel.scrollIntoView({ behavior: "smooth", block: "nearest" });
        }
      });
    });
  }

  function renderRawTable(tableId, rows, emptyLabel, useLabel) {
    var tbody = $(tableId).querySelector("tbody");
    tbody.innerHTML = "";
    if (!rows || !rows.length) {
      tbody.innerHTML =
        '<tr><td colspan="2">' + esc(emptyLabel || "No Data") + "</td></tr>";
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement("tr");
      var name = useLabel ? row.label || row.key : row.key;
      tr.innerHTML =
        "<td>" +
        esc(name) +
        (useLabel && row.label && row.key !== row.label
          ? ' <span class="raw-key">' + esc(row.key) + "</span>"
          : "") +
        "</td><td>" +
        esc(row.count) +
        "</td>";
      tbody.appendChild(tr);
    });
  }

  async function loadReport() {
    var days = selectedDays();
    var excludeInternal = selectedExcludeInternal();
    syncFiltersToUrl(days, excludeInternal);
    setBanner("Loading " + (days === "1" ? "last 24 hours" : "last " + days + " days") + "…", "info");
    $("laReport").hidden = true;
    try {
      var data = await apiFetch(
        "/api/marketing/landing-events/report?days=" +
          encodeURIComponent(days) +
          "&excludeInternal=" +
          (excludeInternal ? "1" : "0") +
          "&_=" +
          Date.now()
      );
      renderInsights(data.insights);
      renderHeroKpis(data.totals || {}, data.funnel);
      renderTrafficSegments(data.trafficSegments);
      renderDashboard(data.dashboard);
      renderInsightsHub(data.insightsHub);
      renderBenchmarks(data.benchmarks);
      renderDailyUniqueUsers(data.dailyUniqueUsers);
      renderFunnel(data.funnel);
      renderSectionJourney(data.funnel && data.funnel.sectionJourney);
      renderCtaPaths(data.ctaPaths);
      renderFaqHeatmap(data.faqHeatmap);
      renderReturnVisitors(data.returnVisitors);
      renderTiming(data.timing);
      renderEngagementMilestones(data.engagementMilestones);
      renderInteractions(data.interactions);
      renderSessionStrip(data.sessionIndex);
      if (activeSessionId) {
        loadSessionReplay(activeSessionId);
      } else {
        renderSessionTimeline(null);
      }
      renderBarList("laCta", data.ctaLocations, "No CTA Clicks Yet");
      renderBarList("laGeoCountries", data.geography && data.geography.countries, "No Country Data Yet — Captured on New Visits After Deploy");
      renderBarList("laGeoCities", data.geography && data.geography.cities, "No City Data Yet — Captured on New Visits After Deploy");
      renderScrollDepths(data.scrollDepths);
      renderDevices(data.devices);
      renderBarList("laUtm", data.utmSources, "No Campaign Tags Yet");
      renderRecent(data.recent);
      renderRawTable("laByEvent", data.byEvent, "No Events", true);
      renderRawTable("laSections", data.sections, "No Section Views", true);
      $("laReport").hidden = false;
      var bannerKind = data.storage && !data.storage.persistent ? "warn" : "info";
      setBanner(
        formatStorageBanner(data.storage, data.totals, data.window) +
          (data.storage && data.storage.retentionNote && !data.storage.persistent
            ? " — " + data.storage.retentionNote
            : ""),
        bannerKind
      );
      hideAuthPanel();
    } catch (err) {
      setBanner(err.message || "Could not load report", "error");
      $("laReport").hidden = true;
      $("laAuthPanel").hidden = false;
    }
  }

  function hideAuthPanel() {
    var panel = $("laAuthPanel");
    if (panel) panel.hidden = true;
  }

  function clearReportKey() {
    try {
      sessionStorage.removeItem(ACCESS_KEY_STORAGE);
    } catch (_e) {}
    if ($("laKeyInput")) $("laKeyInput").value = "";
    setBanner("Saved key cleared. Set LANDING_ANALYTICS_REPORT_KEY on Railway, then paste the same value.", "info");
  }

  function usePastedKey() {
    var key = ($("laKeyInput") && $("laKeyInput").value) || reportKeyFromPage();
    key = key ? String(key).trim() : "";
    if (!key) {
      setBanner("Paste your report access key, or add ?key=… to the URL.", "error");
      return;
    }
    storeReportKey(key);
    hideAuthPanel();
    loadReport();
  }

  function usePastedToken() {
    var token = extractToken($("laTokenInput").value) || extractToken(tokenFromPage());
    if (!token) {
      setBanner("Paste a valid eyJ… token from dealality.com after signing in.", "error");
      return;
    }
    storeToken(token);
    loadReport();
  }

  async function boot() {
    var key = reportKeyFromPage();
    if (key) {
      storeReportKey(key);
      hideAuthPanel();
      loadReport();
      return;
    }
    var token = tokenFromPage();
    if (token) {
      storeToken(token);
      hideAuthPanel();
      loadReport();
      return;
    }
    $("laAuthPanel").hidden = false;
    setBanner(
      "Bookmark this page as /landing-analytics-report?key=YOUR_KEY (set LANDING_ANALYTICS_REPORT_KEY on Railway).",
      "info"
    );
  }

  applyFiltersFromUrl();
  $("laRefresh").addEventListener("click", loadReport);
  $("laDays").addEventListener("change", loadReport);
  if ($("laExcludeInternal")) {
    $("laExcludeInternal").addEventListener("change", loadReport);
  }
  if ($("laExportEvents")) {
    $("laExportEvents").addEventListener("click", function () {
      downloadExport("events");
    });
  }
  if ($("laExportFunnel")) {
    $("laExportFunnel").addEventListener("click", function () {
      downloadExport("funnel");
    });
  }
  if ($("laUseKey")) $("laUseKey").addEventListener("click", usePastedKey);
  if ($("laClearKey")) $("laClearKey").addEventListener("click", clearReportKey);
  if ($("laUseToken")) $("laUseToken").addEventListener("click", usePastedToken);
  if ($("laPasteHelp")) {
    $("laPasteHelp").addEventListener("click", function () {
      alert(
        "After signing in at dealality.com:\n\n" +
          "1. Stay on dealality.com\n" +
          "2. Open browser DevTools → Console\n" +
          "3. Run: copy(await $memberstackDom.getToken())\n" +
          "4. Paste in Advanced sign-in below and click Load with token"
      );
    });
  }
  boot();
})();
