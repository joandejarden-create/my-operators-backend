(function () {
  "use strict";

  var TOKEN_KEY = "dl_la_report_token_v1";
  var ACCESS_KEY_STORAGE = "dl_la_report_key_v1";

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

    var res = await fetch(fetchPath, { headers: headers });
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok) {
      var msg = (data && (data.message || data.error)) || res.statusText;
      if (res.status === 401) {
        msg = reportKey
          ? "Invalid access key — check LANDING_ANALYTICS_REPORT_KEY on Railway."
          : "Access required — open this page with ?key=YOUR_KEY in the URL (set LANDING_ANALYTICS_REPORT_KEY on Railway).";
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
        sub: (totals.embedSessions || 0) + " from homepage embed",
        accent: "primary",
      },
      {
        label: "Deep engagement",
        value: deepStep.rate + "%",
        sub: "Reached FAQ / why section",
        accent: "teal",
      },
      {
        label: "CTA clicks",
        value: ctaStep.count,
        sub: ctaStep.rate + "% of sessions",
        accent: "violet",
      },
      {
        label: "Email signups",
        value: totals.emailCaptures || 0,
        sub: "Successful captures",
        accent: "green",
      },
      {
        label: "Avg events / visit",
        value: totals.eventsPerSession != null ? totals.eventsPerSession : "—",
        sub: (totals.events || 0) + " total events",
        accent: "muted",
      },
      {
        label: "Time to first scroll",
        value:
          totals.medianFirstScrollSeconds != null ? totals.medianFirstScrollSeconds + "s" : "—",
        sub: "Median across sessions",
        accent: "muted",
      },
    ];

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
      el.innerHTML = '<p class="empty">No funnel data yet.</p>';
      return;
    }
    var maxCount = funnel.steps[0].count || 1;
    el.innerHTML = funnel.steps
      .map(function (step, i) {
        var width = maxCount ? Math.max(8, Math.round((step.count / maxCount) * 100)) : 8;
        var drop =
          i > 0 && funnel.steps[i - 1].count
            ? funnel.steps[i - 1].count - step.count
            : 0;
        var dropHtml =
          drop > 0
            ? '<span class="funnel__drop">−' + drop + " from prior step</span>"
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
      el.innerHTML = '<p class="empty">No section journey yet.</p>';
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
      el.innerHTML = '<p class="empty">' + esc(emptyLabel || "No data") + "</p>";
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
    renderBarList("laDevices", rows, "No device data yet");
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
          "% down page</span>" +
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
      tbody.innerHTML = '<tr><td colspan="5">No recent activity</td></tr>';
      return;
    }
    rows.forEach(function (e) {
      var tr = document.createElement("tr");
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
        "<td><code>" +
        esc(e.sessionId ? e.sessionId.slice(0, 10) + "…" : "—") +
        "</code></td>" +
        "<td>" +
        esc(e.detail || "—") +
        "</td>";
      tbody.appendChild(tr);
    });
  }

  function renderRawTable(tableId, rows, emptyLabel, useLabel) {
    var tbody = $(tableId).querySelector("tbody");
    tbody.innerHTML = "";
    if (!rows || !rows.length) {
      tbody.innerHTML =
        '<tr><td colspan="2">' + esc(emptyLabel || "No data") + "</td></tr>";
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
    var days = $("laDays").value || "7";
    setBanner("Loading report…", "info");
    $("laReport").hidden = true;
    try {
      var data = await apiFetch(
        "/api/marketing/landing-events/report?days=" + encodeURIComponent(days)
      );
      renderInsights(data.insights);
      renderHeroKpis(data.totals || {}, data.funnel);
      renderFunnel(data.funnel);
      renderSectionJourney(data.funnel && data.funnel.sectionJourney);
      renderBarList("laCta", data.ctaLocations, "No CTA clicks yet");
      renderBarList("laGeoCountries", data.geography && data.geography.countries, "No country data yet — captured on new visits after deploy");
      renderBarList("laGeoCities", data.geography && data.geography.cities, "No city data yet — captured on new visits after deploy");
      renderScrollDepths(data.scrollDepths);
      renderDevices(data.devices);
      renderBarList("laUtm", data.utmSources, "No campaign tags yet");
      renderRecent(data.recent);
      renderRawTable("laByEvent", data.byEvent, "No events", true);
      renderRawTable("laSections", data.sections, "No section views", true);
      $("laReport").hidden = false;
      setBanner(
        "Showing " +
          (data.totals && data.totals.sessions ? data.totals.sessions : 0) +
          " sessions" +
          (data.totals && data.totals.locatedSessions != null
            ? " · " + data.totals.locatedSessions + " with location"
            : "") +
          " · " +
          fmtRange(data.window),
        "info"
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

  $("laRefresh").addEventListener("click", loadReport);
  $("laDays").addEventListener("change", loadReport);
  if ($("laUseKey")) $("laUseKey").addEventListener("click", usePastedKey);
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
