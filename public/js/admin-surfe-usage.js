/**
 * Admin — Surfe Usage dashboard V1.3 (Dealality Admin Resources shell).
 * Readable stacked layout + dual-line hotel / opportunity / request-origin cells.
 * Never renders or requests personal email/phone values.
 */
(function () {
  "use strict";

  var summary = null;
  var activeTrend = "daily";

  function authFetch(url, opts) {
    opts = opts || {};
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      return auth.authFetch(url, opts);
    }
    // Fail closed — bare fetch caused shell/embed auth split-brain on admin APIs.
    return Promise.reject(
      new Error(
        "DealalityMemberstackAuth.authFetch unavailable — cannot call Surfe Usage admin APIs"
      )
    );
  }

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function money(n) {
    var x = Number(n);
    if (!Number.isFinite(x)) return "—";
    return "$" + x.toFixed(2);
  }

  function num(n) {
    var x = Number(n);
    if (!Number.isFinite(x)) return "—";
    return String(x);
  }

  function setStatus(text) {
    var el = document.getElementById("asuStatus");
    if (el) el.textContent = text || "";
  }

  function queryParams() {
    var from = document.getElementById("asuFrom").value;
    var to = document.getElementById("asuTo").value;
    var hotelId = document.getElementById("asuHotel").value;
    var portfolioId = document.getElementById("asuPortfolio").value.trim();
    var opportunityId = document.getElementById("asuOpportunity").value.trim();
    var requestingOrganization = document
      .getElementById("asuRequestingOrg")
      .value.trim();
    var outcome = document.getElementById("asuOutcome").value;
    var environment = document.getElementById("asuEnvironment").value;
    var hideFixtures = document.getElementById("asuHideFixtures").checked;
    var q = [];
    if (from) q.push("from=" + encodeURIComponent(from));
    if (to) q.push("to=" + encodeURIComponent(to + "T23:59:59.999Z"));
    if (hotelId) q.push("hotelId=" + encodeURIComponent(hotelId));
    if (portfolioId) q.push("portfolioId=" + encodeURIComponent(portfolioId));
    if (opportunityId) q.push("opportunityId=" + encodeURIComponent(opportunityId));
    if (requestingOrganization) {
      q.push(
        "requestingOrganization=" + encodeURIComponent(requestingOrganization),
      );
    }
    if (outcome) q.push("status=" + encodeURIComponent(outcome));
    if (environment) q.push("environment=" + encodeURIComponent(environment));
    if (hideFixtures) q.push("hideFixtures=1");
    return q.length ? "?" + q.join("&") : "";
  }

  function clearFilters() {
    document.getElementById("asuFrom").value = "";
    document.getElementById("asuTo").value = "";
    document.getElementById("asuHotel").value = "";
    document.getElementById("asuPortfolio").value = "";
    document.getElementById("asuOpportunity").value = "";
    document.getElementById("asuRequestingOrg").value = "";
    document.getElementById("asuOutcome").value = "";
    document.getElementById("asuEnvironment").value = "";
    // Prefer production / non-fixture by default
    document.getElementById("asuHideFixtures").checked = true;
  }

  function renderKpis(totals, remainingCredits) {
    var remainingLabel = "—";
    if (remainingCredits) {
      remainingLabel =
        "E " +
        num(remainingCredits.totalEmail) +
        " · M " +
        num(remainingCredits.totalMobile);
    }
    var successLike =
      (totals.successfulLookups || 0) + (totals.partialLookups || 0);
    var creditsLabel =
      "C " +
      num(totals.creditsConfirmed) +
      " · E " +
      num(totals.creditsEstimated) +
      " · U " +
      num(totals.creditsUnknownCount);
    var items = [
      { label: "Total Lookups", value: num(totals.totalLookups) },
      { label: "Successful Lookups", value: num(successLike) },
      { label: "Success Rate", value: num(totals.successRate) + "%" },
      { label: "Email Contacts Found", value: num(totals.emailFoundCount) },
      { label: "Phone Contacts Found", value: num(totals.phoneFoundCount) },
      { label: "Credits (confirmed · estimated · unknown)", value: creditsLabel },
      {
        label: "Estimated Cost (known only)",
        value: money(totals.totalCostEstimateUsd),
      },
      { label: "Remaining Surfe Credits", value: remainingLabel },
      {
        label: "Accounting failures",
        value: num(totals.accountingFailureCount),
      },
      {
        label: "Fixture / test (not adoption)",
        value: num(totals.fixtureTestCount),
      },
      {
        label: "Production class lookups",
        value: num(totals.productionCount),
      },
    ];
    var root = document.getElementById("asuKpis");
    root.innerHTML = items
      .map(function (it) {
        return (
          '<div class="asu-kpi"><span class="asu-kpi__label">' +
          esc(it.label) +
          '</span><span class="asu-kpi__value">' +
          esc(it.value) +
          "</span></div>"
        );
      })
      .join("");
  }

  function emptyRow(colspan, message) {
    return (
      '<tr><td colspan="' +
      colspan +
      '"><div class="asu-empty">' +
      esc(message) +
      "</div></td></tr>"
    );
  }

  function identityCell(opts) {
    opts = opts || {};
    var primary = opts.primary || "UNKNOWN";
    var secondary = opts.secondary || "";
    var meta = opts.meta || "";
    var href = opts.href || null;
    var flag = opts.flag === true;
    var primaryHtml =
      href && primary !== "UNKNOWN"
        ? '<a href="' + esc(href) + '" target="_top">' + esc(primary) + "</a>"
        : esc(primary);
    var html =
      '<div class="asu-identity"><span class="asu-identity__primary">' +
      primaryHtml +
      "</span>";
    if (secondary) {
      html +=
        '<span class="asu-identity__secondary">' + esc(secondary) + "</span>";
    }
    if (meta) {
      html += '<span class="asu-identity__meta">' + esc(meta) + "</span>";
    }
    if (flag) {
      html += '<span class="asu-flag">Investigate</span>';
    }
    html += "</div>";
    return html;
  }

  function locationLineFrom(row) {
    if (row.hotelLocationLine && row.hotelLocationLine !== "UNKNOWN") {
      return row.hotelLocationLine;
    }
    var parts = [];
    if (row.hotelCity && row.hotelCity !== "UNKNOWN") parts.push(row.hotelCity);
    else if (row.hotelMarket && row.hotelMarket !== "UNKNOWN") {
      parts.push(row.hotelMarket);
    }
    if (row.hotelCountry && row.hotelCountry !== "UNKNOWN") {
      parts.push(row.hotelCountry);
    }
    return parts.length ? parts.join(", ") : "UNKNOWN";
  }

  function marketLineFrom(row) {
    var parts = [];
    if (row.hotelCity && row.hotelCity !== "UNKNOWN") parts.push(row.hotelCity);
    if (row.hotelMarket && row.hotelMarket !== "UNKNOWN") {
      parts.push(row.hotelMarket);
    }
    if (row.hotelCountry && row.hotelCountry !== "UNKNOWN") {
      parts.push(row.hotelCountry);
    }
    return parts.length ? parts.join(" / ") : "UNKNOWN";
  }

  function renderTrends() {
    var rows = (summary && summary.trends && summary.trends[activeTrend]) || [];
    var tbody = document.querySelector("#asuTrendsTable tbody");
    if (!rows.length) {
      tbody.innerHTML = emptyRow(
        6,
        "No tracked Surfe usage in this range yet.",
      );
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        return (
          "<tr><td>" +
          esc(r.key) +
          "</td><td>" +
          esc(r.lookups) +
          "</td><td>" +
          esc(r.success) +
          "</td><td>" +
          esc(r.unsuccessful) +
          "</td><td>" +
          esc(r.credits) +
          "</td><td>" +
          esc(money(r.costUsd)) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function fillHotelFilter(hotels) {
    var sel = document.getElementById("asuHotel");
    var prev = sel.value;
    sel.innerHTML = '<option value="">All hotels</option>';
    (hotels || []).forEach(function (h) {
      var opt = document.createElement("option");
      opt.value = h.hotelId;
      opt.textContent =
        (h.hotelName && h.hotelName !== "UNKNOWN" ? h.hotelName : h.hotelId) +
        (h.hotelCity && h.hotelCity !== "UNKNOWN" ? " — " + h.hotelCity : "");
      sel.appendChild(opt);
    });
    if (prev) sel.value = prev;
  }

  function renderHotels(hotels) {
    var tbody = document.querySelector("#asuHotelsTable tbody");
    if (!hotels || !hotels.length) {
      tbody.innerHTML = emptyRow(
        9,
        "No hotel-level Surfe usage recorded for these filters. Uncheck “Hide fixture/test” to inspect development/fixture rows.",
      );
      return;
    }
    tbody.innerHTML = hotels
      .map(function (h) {
        var phoneSearches =
          Number(h.phoneSearches || 0) + Number(h.bothSearches || 0);
        var emailSearches =
          Number(h.emailSearches || 0) + Number(h.bothSearches || 0);
        var classBadge =
          h.usageClass === "FIXTURE_TEST"
            ? '<span class="asu-badge asu-badge--fixture">' +
              esc(h.usageClass) +
              "</span>"
            : esc(h.usageClass || "—");
        return (
          "<tr><td>" +
          identityCell({
            primary: h.hotelName || "UNKNOWN",
            secondary: locationLineFrom(h),
            meta: h.hotelId ? "ID: " + h.hotelId : "",
            flag: h.identityInvestigationFlag === true,
          }) +
          "</td><td>" +
          esc(marketLineFrom(h)) +
          "</td><td>" +
          classBadge +
          "</td><td>" +
          esc(h.lookups) +
          "</td><td>" +
          esc(emailSearches) +
          "</td><td>" +
          esc(phoneSearches) +
          "</td><td>" +
          esc(h.successfulLookups) +
          "</td><td>" +
          esc(h.creditsConsumed) +
          "</td><td>" +
          esc(money(h.costEstimateUsd)) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function originLocation(o) {
    var parts = [];
    if (o.requestingOfficeCity && o.requestingOfficeCity !== "UNKNOWN") {
      parts.push(o.requestingOfficeCity);
    }
    if (o.requestingOfficeMarket && o.requestingOfficeMarket !== "UNKNOWN") {
      parts.push(o.requestingOfficeMarket);
    }
    return parts.length ? parts.join(", ") : "UNKNOWN";
  }

  function renderOrgs(orgs) {
    var tbody = document.querySelector("#asuOrgsTable tbody");
    if (!tbody) return;
    if (!orgs || !orgs.length) {
      tbody.innerHTML = emptyRow(
        6,
        "No requesting-organization attribution yet for these filters.",
      );
      return;
    }
    tbody.innerHTML = orgs
      .map(function (o) {
        var office =
          o.requestingHotelOrOfficeId &&
          o.requestingHotelOrOfficeId !== "UNKNOWN"
            ? o.requestingHotelOrOfficeId
            : "";
        return (
          "<tr><td>" +
          identityCell({
            primary: o.requestingOrganizationName || "UNKNOWN",
            secondary: office ? "Office: " + office : "Office: UNKNOWN",
            meta:
              o.requestingPortfolioId &&
              o.requestingPortfolioId !== "UNKNOWN"
                ? "Portfolio: " + o.requestingPortfolioId
                : "",
            flag:
              !o.requestingOrganizationName ||
              o.requestingOrganizationName === "UNKNOWN",
          }) +
          "</td><td>" +
          esc(originLocation(o)) +
          "</td><td>" +
          esc(o.lookups) +
          "</td><td>" +
          esc(o.hotelsResearchedCount) +
          "</td><td>" +
          esc(o.creditsConsumed) +
          "</td><td>" +
          esc(money(o.costEstimateUsd)) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderTopOpps(rows) {
    var tbody = document.querySelector("#asuOppsTable tbody");
    if (!tbody) return;
    if (!rows || !rows.length) {
      tbody.innerHTML = emptyRow(
        6,
        "No opportunity-level usage yet for these filters.",
      );
      return;
    }
    tbody.innerHTML = rows
      .map(function (o) {
        var title = o.opportunityTitle || "UNKNOWN";
        return (
          "<tr><td>" +
          identityCell({
            primary: title,
            secondary:
              o.opportunityCategory && o.opportunityCategory !== "UNKNOWN"
                ? o.opportunityCategory
                : "",
            meta: o.opportunityId ? "Opportunity ID: " + o.opportunityId : "",
            href: o.drawerHref || null,
            flag: title === "UNKNOWN",
          }) +
          "</td><td>" +
          esc(o.opportunityOrganizationName || "UNKNOWN") +
          "</td><td>" +
          identityCell({
            primary: o.hotelName || "UNKNOWN",
            secondary: locationLineFrom(o),
            meta: o.hotelId ? "ID: " + o.hotelId : "",
          }) +
          "</td><td>" +
          esc(
            o.hotelMarket && o.hotelMarket !== "UNKNOWN"
              ? o.hotelMarket
              : marketLineFrom(o),
          ) +
          "</td><td>" +
          esc(o.lookups) +
          "</td><td>" +
          esc(money(o.costEstimateUsd)) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function statusBadge(status) {
    var s = String(status || "unknown");
    var cls = "asu-badge";
    if (s === "success") cls += " asu-badge--ok";
    else if (s === "partial") cls += " asu-badge--partial";
    else if (s === "no_match" || s === "error") cls += " asu-badge--bad";
    return '<span class="' + cls + '">' + esc(s) + "</span>";
  }

  function renderLatest(rows) {
    var tbody = document.querySelector("#asuLatestTable tbody");
    if (!rows || !rows.length) {
      tbody.innerHTML = emptyRow(
        9,
        "No lookup activity for these filters. Fixture/test rows are hidden by default — uncheck “Hide fixture/test” to review them.",
      );
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        var creditsDisplay =
          r.creditsCharged == null
            ? "—"
            : String(r.creditsCharged) +
              (r.creditCertainty ? " · " + r.creditCertainty : "");
        var oppTitle = r.opportunityTitle || "UNKNOWN";
        var office =
          r.requestingHotelOrOfficeId &&
          r.requestingHotelOrOfficeId !== "UNKNOWN"
            ? r.requestingHotelOrOfficeId
            : "UNKNOWN";
        var originLoc =
          r.requestingOfficeMarket && r.requestingOfficeMarket !== "UNKNOWN"
            ? r.requestingOfficeMarket
            : "UNKNOWN";
        var envLabel = r.dataEnvironment || "—";
        var classBadge =
          r.usageClass === "FIXTURE_TEST"
            ? ' <span class="asu-badge asu-badge--fixture">' +
              esc(r.usageClass) +
              "</span>"
            : r.usageClass
              ? " · " + esc(r.usageClass)
              : "";
        return (
          "<tr><td>" +
          esc(r.at) +
          "</td><td>" +
          identityCell({
            primary: r.hotelName || "UNKNOWN",
            secondary: locationLineFrom(r),
            meta: r.hotelId ? "ID: " + r.hotelId : "",
            flag: r.hotelIdentityInvestigationFlag === true,
          }) +
          "</td><td>" +
          identityCell({
            primary: r.requestingOrganizationName || "UNKNOWN",
            secondary: "Office: " + office + " · " + originLoc,
            flag:
              !r.requestingOrganizationName ||
              r.requestingOrganizationName === "UNKNOWN",
          }) +
          "</td><td>" +
          identityCell({
            primary: oppTitle,
            secondary: r.opportunityOrganizationName || "",
            meta: r.opportunityId ? "Opportunity ID: " + r.opportunityId : "",
            href: r.opportunityDrawerHref || null,
            flag: r.opportunityIdentityInvestigationFlag === true,
          }) +
          "</td><td>" +
          esc(r.lookupType || "—") +
          "</td><td>" +
          statusBadge(r.status) +
          "</td><td>" +
          esc(creditsDisplay) +
          "</td><td>" +
          esc(
            r.costEstimateUsd == null
              ? "—"
              : money(r.costEstimateUsd) +
                  (r.costCertainty ? " · " + r.costCertainty : ""),
          ) +
          "</td><td>" +
          esc(envLabel) +
          classBadge +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderOps(summaryData) {
    var t = summaryData.totals || {};
    var durability = summaryData.durability || {};
    var list = document.getElementById("asuOpsList");
    var items = [
      {
        label: "No-match frequency",
        value: num(summaryData.noMatchFrequency) + "%",
      },
      { label: "Error lookups", value: num(t.errorLookups) },
      {
        label: "Avg provider latency",
        value: t.avgLatencyMs == null ? "—" : t.avgLatencyMs + " ms",
      },
      {
        label: "Accounting complete",
        value:
          (t.accountingComplete === false ? "No" : "Yes") +
          " (" +
          num(t.accountingFailureCount) +
          " failures)",
      },
      {
        label: "Credits unknown rows",
        value: num(t.creditsUnknownCount),
      },
      {
        label: "Persistence",
        value:
          (durability.store || "filesystem") +
          " · " +
          (durability.mechanism || "lock + atomic write"),
      },
      {
        label: "Airtable sync",
        value:
          ((summaryData.airtableSyncHealth &&
            summaryData.airtableSyncHealth.tableStatus) ||
            "PROPOSED") +
          " · pending " +
          (summaryData.airtableSyncHealth
            ? summaryData.airtableSyncHealth.pending
            : "—") +
          " · synced " +
          (summaryData.airtableSyncHealth
            ? summaryData.airtableSyncHealth.synced
            : "—"),
      },
      {
        label: "Adoption note",
        value:
          (summaryData.totals && summaryData.totals.adoptionNote) ||
          "Fixture/test usage is labeled and is not customer adoption.",
      },
      {
        label: "Privacy",
        value: "Personal contact details are not stored in this ledger.",
      },
    ];
    list.innerHTML = items
      .map(function (it) {
        return (
          "<li><span class=\"asu-ops__label\">" +
          esc(it.label) +
          '</span><span class="asu-ops__value">' +
          esc(it.value) +
          "</span></li>"
        );
      })
      .join("");

    var errors = summaryData.recentErrors || [];
    var tbody = document.querySelector("#asuErrorsTable tbody");
    if (!errors.length) {
      tbody.innerHTML = emptyRow(4, "No recent provider errors.");
    } else {
      tbody.innerHTML = errors
        .map(function (e) {
          return (
            "<tr><td>" +
            esc(e.at) +
            "</td><td>" +
            esc(e.hotelName || e.hotelId || "—") +
            "</td><td>" +
            esc(e.errorCategory || "—") +
            "</td><td>" +
            esc(e.opportunityTitle || e.opportunityId || "—") +
            "</td></tr>"
          );
        })
        .join("");
    }

    var failures = summaryData.recentAccountingFailures || [];
    var fbody = document.querySelector("#asuAccountingFailures tbody");
    if (!fbody) return;
    if (!failures.length) {
      fbody.innerHTML = emptyRow(4, "No accounting write failures recorded.");
      return;
    }
    fbody.innerHTML = failures
      .map(function (e) {
        return (
          "<tr><td>" +
          esc(e.at) +
          "</td><td>" +
          esc(e.category || "—") +
          "</td><td>" +
          esc(e.hotelId || "—") +
          "</td><td>" +
          esc(e.opportunityId || "—") +
          "</td></tr>"
        );
      })
      .join("");
  }

  async function loadSummary() {
    setStatus("Loading…");
    var res = await authFetch("/api/admin/surfe-usage/summary" + queryParams(), {
      cache: "no-store",
    });
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok || !data.ok) {
      setStatus((data && (data.message || data.error)) || "Failed to load usage");
      return;
    }
    summary = data;
    if (data.trackingStartedNote) {
      document.getElementById("asuHistoryNote").textContent =
        data.trackingStartedNote +
        " Fixture/test rows stay classified and are hidden by default.";
    }
    renderKpis(data.totals || {}, data.remainingCredits);
    fillHotelFilter(data.hotels || []);
    renderTrends();
    renderHotels(data.hotels || []);
    renderOrgs(data.requestingOrganizations || []);
    renderTopOpps(data.topOpportunities || []);
    renderLatest(data.latestActivity || []);
    renderOps(data);
    setStatus("Updated " + (data.generatedAt || ""));
  }

  function bind() {
    document.getElementById("asuApplyFilters").addEventListener("click", function () {
      loadSummary().catch(function (err) {
        setStatus(err && err.message ? err.message : "Load failed");
      });
    });
    document.getElementById("asuClearFilters").addEventListener("click", function () {
      clearFilters();
      loadSummary().catch(function (err) {
        setStatus(err && err.message ? err.message : "Load failed");
      });
    });
    document.getElementById("asuRefresh").addEventListener("click", function () {
      Promise.all([loadSummary(), loadFeBenchmark()]).catch(function (err) {
        setStatus(err && err.message ? err.message : "Load failed");
      });
    });
    var feBtn = document.getElementById("asuFeBenchmarkRefresh");
    if (feBtn) {
      feBtn.addEventListener("click", function () {
        loadFeBenchmark().catch(function (err) {
          setStatus(err && err.message ? err.message : "Benchmark load failed");
        });
      });
    }
    document.querySelectorAll(".asu-trend-tab").forEach(function (btn) {
      btn.addEventListener("click", function () {
        document.querySelectorAll(".asu-trend-tab").forEach(function (b) {
          b.classList.remove("active");
          b.setAttribute("aria-selected", "false");
        });
        btn.classList.add("active");
        btn.setAttribute("aria-selected", "true");
        activeTrend = btn.getAttribute("data-trend") || "daily";
        renderTrends();
      });
    });
  }

  function renderFeBenchmark(data) {
    var summary = (data && data.summary) || {};
    var kpis = [
      {
        label: "Eligible Surfe miss events",
        value: num(summary.genuineEligibleMissEvents),
      },
      {
        label: "Distinct verified contacts",
        value: num(summary.distinctVerifiedContacts),
      },
      {
        label: "Associated hotels",
        value: num(summary.associatedHotels),
      },
      {
        label: "Associated opportunities",
        value: num(summary.associatedOpportunities),
      },
      {
        label: "Identity-ready contacts",
        value: num(summary.fullEnrichIdentityReadyCount),
      },
      {
        label: "Prior FullEnrich attempts (excluded)",
        value: num(summary.priorFullEnrichAttemptCount),
      },
      {
        label: "Est. benchmark credit exposure",
        value: num(summary.estimatedBenchmarkCreditExposure),
      },
      {
        label: "Fixtures excluded",
        value: num(summary.excluded && summary.excluded.fixture),
      },
    ];
    var kpiRoot = document.getElementById("asuFeBenchmarkKpis");
    if (kpiRoot) {
      kpiRoot.innerHTML = kpis
        .map(function (it) {
          return (
            '<div class="asu-kpi"><span class="asu-kpi__label">' +
            esc(it.label) +
            '</span><span class="asu-kpi__value">' +
            esc(it.value) +
            "</span></div>"
          );
        })
        .join("");
    }
    var tbody = document.querySelector("#asuFeBenchmarkTable tbody");
    if (!tbody) return;
    var rows = (data && data.shortlist) || [];
    if (!rows.length) {
      tbody.innerHTML = emptyRow(
        7,
        "No genuine eligible Surfe work-email misses yet. Fixture/test rows are excluded."
      );
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        var hotels = (r.hotels || []).join(", ") || "—";
        var oppCount = num(r.opportunityCount);
        var ready =
          r.identityReadiness && r.identityReadiness.ready ? "Yes" : "No";
        return (
          "<tr>" +
          "<td>" +
          identityCell({
            primary: r.contactDisplayName || "—",
            secondary: r.contactRole || "",
            meta: r.personIdentityKey
              ? "person " + String(r.personIdentityKey).slice(0, 8)
              : "",
          }) +
          "</td>" +
          "<td>" +
          esc(r.organizationName || "—") +
          "</td>" +
          "<td>" +
          esc(hotels) +
          " (" +
          num(r.hotelCount) +
          ")</td>" +
          "<td>" +
          esc(oppCount) +
          "</td>" +
          "<td>" +
          esc(ready) +
          "</td>" +
          "<td>" +
          esc(num(r.estimatedCreditsIfBenchmarked)) +
          "</td>" +
          "<td>" +
          esc(r.latestMissAt || "—") +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  async function loadFeBenchmark() {
    var data = await authFetch(
      "/api/admin/surfe-usage/fullenrich-benchmark-eligibility"
    ).then(function (r) {
      if (!r.ok) throw new Error("Benchmark eligibility HTTP " + r.status);
      return r.json();
    });
    renderFeBenchmark(data);
    var note = document.getElementById("asuFeBenchmarkNote");
    if (note && data && data.controls) {
      note.textContent =
        "Genuine Surfe work-email misses only. Fixtures excluded. FullEnrich OFF · paidLookupsTriggered=" +
        String(data.controls.paidLookupsTriggered === true) +
        " · customerFacing=" +
        String(data.privacy && data.privacy.customerFacing === true) +
        " · " +
        (data.generatedAt || "");
    }
    return data;
  }

  async function boot() {
    var gate = window.SupportAdminGate;
    var deniedEl = document.getElementById("supportGateDenied");
    var loadingEl = document.getElementById("supportGateLoading");
    if (!gate || typeof gate.requireAdmin !== "function") {
      if (loadingEl) loadingEl.hidden = true;
      if (deniedEl) {
        deniedEl.hidden = false;
        var msg = deniedEl.querySelector("[data-gate-message]");
        if (msg) {
          msg.textContent =
            "Authentication unavailable. Sign in through the Dealality app to view this page.";
        }
      }
      return;
    }
    if (!window.DealalityMemberstackAuth || typeof window.DealalityMemberstackAuth.authFetch !== "function") {
      if (loadingEl) loadingEl.hidden = true;
      if (deniedEl) {
        deniedEl.hidden = false;
        var authMsg = deniedEl.querySelector("[data-gate-message]");
        if (authMsg) {
          authMsg.textContent =
            "Authentication unavailable. Sign in through the Dealality app to view this page.";
        }
      }
      return;
    }
    var ok = await gate.requireAdmin({
      loadingId: "supportGateLoading",
      deniedId: "supportGateDenied",
      contentId: "supportGateContent",
      loadingMessage: "Verifying admin access…",
    });
    if (!ok) return;
    bind();
    try {
      await loadSummary();
      await loadFeBenchmark();
    } catch (err) {
      setStatus(err && err.message ? err.message : "Failed to load Surfe Usage");
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
