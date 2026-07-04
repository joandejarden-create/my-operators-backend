/**

 * Capital Provider Explorer — profile detail (Operator Explorer presentation parity).

 */

(function (global) {

  "use strict";



  var DEFAULT_RAILWAY = "https://my-operators-backend-production.up.railway.app";

  var raw =

    global.DEALITY_API_BASE && String(global.DEALITY_API_BASE).replace(/\/$/, "").trim();

  if (!raw && global.location && global.location.hostname) {

    var h = global.location.hostname;

    var local = h === "localhost" || h === "127.0.0.1" || h === "[::1]";

    if (!local) raw = DEFAULT_RAILWAY;

  }

  var base = raw || "";

  global.__dealityApiUrl = function (path) {

    var p = path.charAt(0) === "/" ? path : "/" + path;

    return base ? base + p : p;

  };

})(typeof window !== "undefined" ? window : this);



(function () {

  "use strict";



  var INSTITUTION_STRIPE = {
    bank: "#3498db",
    "regional bank": "#1abc9c",
    "national bank": "#2e86de",
    "development finance institution": "#16a085",
    "multilateral institution": "#8e44ad",
    "export credit / government finance": "#e67e22",
    "commercial bank": "#3498db",
    "debt fund": "#9b59b6",
    "cmbs lender": "#d4af37",
    "life company": "#2ecc71",
    "hud / agency lender": "#e67e22",
    "private credit": "#e74c3c",
  };

  function normalizeWebsiteUrl(url) {
    if (!url) return "";
    var raw = String(url).trim();
    if (!raw) return "";
    if (/^https?:\/\//i.test(raw)) return raw;
    return "https://" + raw;
  }

  function websiteLabel(url) {
    if (!url) return "";
    return String(url)
      .replace(/^https?:\/\//i, "")
      .replace(/^www\./i, "")
      .replace(/\/$/, "");
  }

  function resolveLogoUrl(p) {
    var logoUrl = p.logoUrl && String(p.logoUrl).trim();
    if (logoUrl && /^https?:\/\//i.test(logoUrl)) return logoUrl;
    var websiteUrl = normalizeWebsiteUrl(p.website);
    if (!websiteUrl) return "";
    try {
      var host = new URL(websiteUrl).hostname.replace(/^www\./i, "");
      if (!host) return "";
      return (
        "https://www.google.com/s2/favicons?domain=" + encodeURIComponent(host) + "&sz=128"
      );
    } catch (e) {
      return "";
    }
  }

  function visibilityBadgeLabel(visibility) {
    var v = String(visibility || "Public").toLowerCase();
    if (v === "limited") return "Public Summary";
    if (v === "private") return "Restricted";
    if (v === "invite only") return "Invite Only";
    if (v === "admin only") return "Internal";
    return "";
  }



  function escapeHtml(s) {

    return String(s ?? "")

      .replace(/&/g, "&amp;")

      .replace(/</g, "&lt;")

      .replace(/>/g, "&gt;")

      .replace(/"/g, "&quot;");

  }



  function apiUrl(path) {

    if (typeof window.__dealityApiUrl === "function") {

      return window.__dealityApiUrl(path);

    }

    return path.charAt(0) === "/" ? path : "/" + path;

  }



  function formatUsd(n) {

    var num = Number(n);

    if (!num || Number.isNaN(num)) return "—";

    if (num >= 1000000) {

      return "$" + (num / 1000000).toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",") + "M";

    }

    return "$" + num.toLocaleString("en-US");

  }



  function visibilityBadgeClass(visibility) {

    var v = String(visibility || "Public").toLowerCase();

    if (v === "invite only") return "cpe-card-badge--invite-only";

    if (v === "admin only") return "cpe-card-badge--admin-only";

    if (v === "limited") return "cpe-card-badge--limited";

    if (v === "private") return "cpe-card-badge--private";

    return "cpe-card-badge--public";

  }



  function institutionStripe(type) {

    return INSTITUTION_STRIPE[String(type || "").trim().toLowerCase()] || "#6c72ff";

  }



  function section(title, body, extraClass) {

    return (

      '<section class="section' +

      (extraClass ? " " + extraClass : "") +

      '"><h2 class="section-title">' +

      escapeHtml(title) +

      "</h2>" +

      body +

      "</section>"

    );

  }



  function kpi(label, value) {

    return (

      '<div class="kpi"><div class="label">' +

      escapeHtml(label) +

      '</div><div class="value">' +

      escapeHtml(value || "—") +

      "</div></div>"

    );

  }



  function valueKpi(value, label, note) {

    return (

      '<div class="card oe-value-kpi"><div class="oe-value-kpi__value">' +

      escapeHtml(value || "—") +

      '</div><h3 class="oe-value-kpi__label">' +

      escapeHtml(label) +

      "</h3><p>" +

      escapeHtml(note || "") +

      "</p></div>"

    );

  }



  function card(title, body) {

    return (

      '<div class="card"><h3>' +

      escapeHtml(title) +

      "</h3><p>" +

      body +

      "</p></div>"

    );

  }



  function cluster(title, items) {

    var list = Array.isArray(items) ? items.filter(Boolean) : [];

    return (

      '<div class="cluster"><h3>' +

      escapeHtml(title) +

      "</h3><ul>" +

      list.map(function (i) {

        return "<li>" + escapeHtml(i) + "</li>";

      }).join("") +

      "</ul></div>"

    );

  }



  function tagList(arr) {

    var list = Array.isArray(arr) ? arr.filter(Boolean) : [];

    if (!list.length) return '<p class="gold-mock-tab-empty">Not provided.</p>';

    return (

      '<div class="cpe-tag-grid">' +

      list

        .map(function (t) {

          return '<span class="cpe-tag">' + escapeHtml(t) + "</span>";

        })

        .join("") +

      "</div>"

    );

  }



  function bulletList(arr) {

    var list = Array.isArray(arr) ? arr.filter(Boolean) : [];

    if (!list.length) return '<p class="gold-mock-tab-empty">Not provided.</p>';

    return (

      '<ul class="cpe-check-list">' +

      list

        .map(function (i) {

          return "<li>" + escapeHtml(i) + "</li>";

        })

        .join("") +

      "</ul>"

    );

  }



  function leaderCard(leader) {

    var img = leader.imageUrl

      ? '<img src="' + escapeHtml(leader.imageUrl) + '" alt="" loading="lazy" referrerpolicy="no-referrer">'

      : "";

    return (

      '<div class="leader-card leader-card--with-profile"><div class="leader-image-wrap">' +

      img +

      '<div class="leader-bio-overlay">' +

      escapeHtml(leader.bio || "") +

      '</div></div><div class="leader-body"><div class="leader-name">' +

      escapeHtml(leader.name) +

      '</div><div class="leader-meta">' +

      escapeHtml(leader.title || "") +

      '</div><div class="leader-summary">' +

      escapeHtml(leader.role || "") +

      "</div></div></div>"

    );

  }



  function proofCard(deal) {

    var img = deal.imageUrl

      ? '<img src="' + escapeHtml(deal.imageUrl) + '" alt="" loading="lazy" referrerpolicy="no-referrer">'

      : "";

    return (

      '<div class="proof-card">' +

      img +

      '<div class="proof-body"><div class="proof-title">' +

      escapeHtml(deal.name) +

      '</div><div class="proof-meta">' +

      escapeHtml([deal.location, deal.dealType, deal.loanAmount, deal.year].filter(Boolean).join(" · ")) +

      "</div><p>" +

      escapeHtml(deal.summary || "") +

      "</p>" +

      (deal.sourceUrl
        ? '<p class="meta-muted"><a href="' +
          escapeHtml(deal.sourceUrl) +
          '" target="_blank" rel="noopener noreferrer">' +
          escapeHtml(deal.sourceName || "Source") +
          "</a></p>"
        : "") +

      "</div></div>"

    );

  }



  function renderOwnerFacingNotes(p) {
    if (!p.ownerFacingNotes) return "";
    return section(
      "Owner Guidance",
      '<div class="card"><p>' + escapeHtml(p.ownerFacingNotes) + "</p></div>"
    );
  }



  function renderCriteriaRecords(criteria) {
    var rows = Array.isArray(criteria) ? criteria : [];
    if (!rows.length) return "";

    return section(
      "Product-Level Lending Criteria",
      '<div class="quant-grid">' +
        rows
          .map(function (c) {
            var bullets = [
              c.loanProduct ? "Loan product: " + c.loanProduct : "",
              c.appetiteStatus ? "Appetite: " + c.appetiteStatus : "",
              c.termRange ? "Term range: " + c.termRange : "",
              c.recoursePreference ? "Recourse: " + c.recoursePreference : "",
              c.minimumLoanSize != null ? "Minimum loan: " + formatUsd(c.minimumLoanSize) : "",
              c.maximumLoanSize != null ? "Maximum loan: " + formatUsd(c.maximumLoanSize) : "",
              (c.dealTypes || []).length ? "Deal types: " + c.dealTypes.join(", ") : "",
              (c.rateType || []).length ? "Rate type: " + c.rateType.join(", ") : "",
              (c.currency || []).length ? "Currency: " + c.currency.join(", ") : "",
              c.sponsorRequirements ? "Sponsor: " + c.sponsorRequirements : "",
              c.equityRequirements ? "Equity: " + c.equityRequirements : "",
              c.collateralRequirements ? "Collateral: " + c.collateralRequirements : "",
              c.brandRequirements ? "Brand / flag: " + c.brandRequirements : "",
              c.operatorRequirements ? "Operator: " + c.operatorRequirements : "",
              c.marketRequirements ? "Market: " + c.marketRequirements : "",
            ].filter(Boolean);

            return (
              '<div class="card"><h3>' +
              escapeHtml(c.criteriaName || "Lending criteria") +
              "</h3>" +
              (c.ownerVisibleSummary
                ? "<p>" + escapeHtml(c.ownerVisibleSummary) + "</p>"
                : "") +
              (bullets.length
                ? '<ul class="cpe-check-list">' +
                  bullets
                    .map(function (b) {
                      return "<li>" + escapeHtml(b) + "</li>";
                    })
                    .join("") +
                  "</ul>"
                : "") +
              (c.sourceConfidence
                ? '<p class="meta-muted">Source confidence: ' +
                  escapeHtml(c.sourceConfidence) +
                  "</p>"
                : "") +
              "</div>"
            );
          })
          .join("") +
        "</div>"
    );
  }



  function renderSourceReferencesFooter(p) {
    var sources = Array.isArray(p.sourceReferences) ? p.sourceReferences : [];
    var el = document.getElementById("cpeProfileSourcesFooter");
    if (!el) return;
    if (!sources.length) {
      el.innerHTML = "";
      el.hidden = true;
      return;
    }

    var items = sources
      .map(function (s) {
        var title = s.name || "Source";
        var link = s.url
          ? '<a href="' +
            escapeHtml(s.url) +
            '" target="_blank" rel="noopener noreferrer">' +
            escapeHtml(title) +
            "</a>"
          : escapeHtml(title);
        var meta = [s.sourceType, s.confidence, s.sourceDate]
          .filter(Boolean)
          .join(" · ");
        return (
          "<li><strong>" +
          link +
          "</strong>" +
          (meta ? '<span class="meta-muted"> — ' + escapeHtml(meta) + "</span>" : "") +
          (s.summary ? "<p>" + escapeHtml(s.summary) + "</p>" : "") +
          "</li>"
        );
      })
      .join("");

    el.innerHTML =
      '<details class="cpe-sources-panel" open>' +
      '<summary>Sources &amp; references (' +
      sources.length +
      ")</summary>" +
      '<ul class="cpe-sources-list">' +
      items +
      "</ul></details>";
    el.hidden = false;
  }



  function metaCard(label, value) {

    return (

      '<div class="meta-card"><div class="label">' +

      escapeHtml(label) +

      '</div><div class="value">' +

      escapeHtml(value || "—") +

      "</div></div>"

    );

  }



  function metaCardLink(label, href, text) {

    if (!href || !text) return metaCard(label, "");

    return (

      '<div class="meta-card"><div class="label">' +

      escapeHtml(label) +

      '</div><div class="value"><a href="' +

      escapeHtml(href) +

      '" target="_blank" rel="noopener noreferrer">' +

      escapeHtml(text) +

      "</a></div></div>"

    );

  }



  function renderHeroMeta(p) {

    var stats = p.portfolioStats || {};

    var geo = (p.geographicCoverage || []).slice(0, 2).join(", ");

    if ((p.geographicCoverage || []).length > 2) geo += " +";

    return (

      metaCard("Institution Type", p.institutionType) +

      metaCard("Typical Loan Size", p.loanSizeLabel) +

      metaCard("Lending Appetite", p.currentLendingAppetiteOwner || "—") +

      metaCard("Deals Financed", stats.dealsFinanced != null ? String(stats.dealsFinanced) : "—") +

      metaCard("Volume Financed", stats.totalVolumeLabel || "—") +

      metaCard("Geographic Focus", geo || "—") +

      (p.website
        ? metaCardLink(
            "Website",
            normalizeWebsiteUrl(p.website),
            websiteLabel(p.website)
          )
        : "")

    );

  }



  function renderPortfolioSnapshot(p) {

    var stats = p.portfolioStats || {};

    if (!stats.dealsFinanced && !stats.totalVolumeLabel) return "";

    return section(

      "Lending Platform Snapshot",

      '<div class="kpi-grid-4 oe-snapshot-kpi-row oe-tab-snapshot-kpis--single-row">' +

        valueKpi(

          stats.dealsFinanced != null ? String(stats.dealsFinanced) : "—",

          "Deals Financed",

          "Representative hotel financings completed"

        ) +

        valueKpi(stats.totalVolumeLabel || "—", "Volume Financed", "Cumulative lending volume") +

        valueKpi(

          stats.activeMarkets != null ? String(stats.activeMarkets) : "—",

          "Active Markets",

          "Geographies with recent lending activity"

        ) +

        valueKpi(

          stats.yearsLending != null ? String(stats.yearsLending) + "+" : "—",

          "Years Lending",

          "Hospitality lending experience"

        ) +

        "</div>",

      "oe-bf-snapshot-section"

    );

  }



  function renderProfileTab(p) {

    var html = "";

    html += renderPortfolioSnapshot(p);

    html += section(

      "Institution Overview",

      '<div class="card"><p>' + escapeHtml(p.institutionOverview || "Not provided.") + "</p></div>"

    );

    html += renderOwnerFacingNotes(p);

    if ((p.keyDifferentiators || []).length) {

      html += section(

        "Key Differentiators",

        '<div class="grid-2">' + cluster("What Sets This Lender Apart", p.keyDifferentiators) + "</div>"

      );

    }

    if ((p.ownerValueProps || []).length) {

      html += section(

        "Why Owners Consider This Capital Provider",

        '<div class="grid-3">' +

          p.ownerValueProps

            .map(function (v) {

              return card(v.title, escapeHtml(v.body));

            })

            .join("") +

          "</div>"

      );

    }

    if ((p.leadership || []).length) {

      html += section(

        "Leadership Snapshot",

        '<p class="gold-mock-tab-empty odna-subsection-intro">Key leaders behind hospitality origination, credit, and portfolio strategy.</p><div class="proof-grid oe-leader-profile-grid" style="--oe-leader-profile-cols: 3;">' +

          p.leadership

            .slice(0, 3)

            .map(leaderCard)

            .join("") +

          "</div>"

      );

    }

    if ((p.trackRecord || []).length) {

      html += section(

        "Recent Hotel Financings",

        '<p class="gold-mock-tab-empty odna-subsection-intro">Representative transactions illustrating lending focus and execution. See Track Record for the full list.</p><div class="proof-grid">' +

          p.trackRecord

            .slice(0, 3)

            .map(proofCard)

            .join("") +

          "</div>"

      );

    }

    html += section(

      "At a Glance",

      '<div class="quant-grid">' +

        cluster("Lending Profile", [

          "Headquarters: " + (p.headquarters || "—"),

          "Typical loan size: " + (p.loanSizeLabel || "—"),

          "Contact pathway: " + (p.contactPathway || "—"),

          "Sponsor preference: " + (p.sponsorPreference || "—"),

        ]) +

        cluster("Coverage & Products", [

          "Geography: " + ((p.geographicCoverage || []).slice(0, 4).join(", ") || "—"),

          "Loan products: " + ((p.loanProducts || []).slice(0, 4).join(", ") || "—"),

          "Asset focus: " + (p.assetTypeAppetite || "—"),

        ]) +

        "</div>"

    );

    return html;

  }



  function renderLendingFocus(p) {

    return (

      section(

        "Hotel Lending Focus",

        '<div class="card"><p>' + escapeHtml(p.hotelLendingFocus || "Not provided.") + "</p></div>"

      ) +

      renderOwnerFacingNotes(p) +

      section("Geographic Coverage", tagList(p.geographicCoverage)) +

      section("Loan Products Offered", tagList(p.loanProducts)) +

      section(

        "Current Lending Appetite",

        '<div class="card"><p>' +

          escapeHtml(p.currentLendingAppetiteOwner || "Contact pathway will confirm current appetite.") +

          "</p></div>"

      ) +

      section(

        "Lending Platform Signals",

        '<div class="kpi-grid-4">' +

          kpi("Institution Type", p.institutionType) +

          kpi("Typical Loan Size", p.loanSizeLabel) +

          kpi("Minimum Loan", formatUsd(p.loanSizeMinUsd)) +

          kpi("Maximum Loan", formatUsd(p.loanSizeMaxUsd)) +

          "</div>"

      )

    );

  }



  function renderDealCriteria(p) {

    var stages = Array.isArray(p.projectStages) ? p.projectStages : [];

    var brands = Array.isArray(p.brandPreferences) ? p.brandPreferences : [];

    var operators = Array.isArray(p.operatorPreferences) ? p.operatorPreferences : [];

    return (

      section(

        "Deal & Asset Criteria",

        '<div class="grid-2">' +

          '<div class="card"><h3>Typical Deal Types</h3><p>' +

          escapeHtml(p.typicalDealTypes || "Not provided.") +

          "</p></div>" +

          '<div class="card"><h3>Asset Type Appetite</h3><p>' +

          escapeHtml(p.assetTypeAppetite || "Not provided.") +

          "</p></div></div>"

      ) +

      section("Preferred Asset Types", tagList(p.preferredAssetTypes)) +

      section(

        "Loan Size Range",

        '<div class="kpi-grid-4">' +

          kpi("Minimum", formatUsd(p.loanSizeMinUsd)) +

          kpi("Maximum", formatUsd(p.loanSizeMaxUsd)) +

          kpi("Typical Range", p.loanSizeLabel) +

          kpi("Sponsor Preference", p.sponsorPreference || "—") +

          "</div>"

      ) +

      section(

        "Project Stage & Relationships",

        '<div class="quant-grid">' +

          cluster("Project Stage Appetite", stages) +

          cluster("Brand Preference", brands) +

          cluster("Operator Preference", operators) +

          "</div>"

      ) +

      renderCriteriaRecords(p.criteria)

    );

  }



  function renderTrackRecord(p) {

    var deals = p.trackRecord || [];

    if (!deals.length) {

      return (

        '<p class="gold-mock-tab-empty">Representative hotel financings will appear here when available for this capital provider.</p>'

      );

    }

    var tableRows = deals

      .map(function (d) {

        return (

          "<tr><td>" +

          escapeHtml(d.name) +

          "</td><td>" +

          escapeHtml(d.location || "—") +

          "</td><td>" +

          escapeHtml(d.dealType || "—") +

          "</td><td>" +

          escapeHtml(d.loanAmount || "—") +

          "</td><td>" +

          escapeHtml(d.year || "—") +

          "</td></tr>"

        );

      })

      .join("");

    return (

      section(

        "Representative Hotel Financings",

        '<p class="gold-mock-tab-empty odna-subsection-intro">Illustrative transactions showing markets, deal types, and loan sizes this lender has supported.</p>' +

          '<div class="proof-grid proof-grid--case-studies">' +

          deals.map(proofCard).join("") +

          "</div>"

      ) +

      section(

        "Transaction Summary",

        '<div class="cpe-footprint-table-wrap"><table class="cpe-footprint-table"><thead><tr><th>Project</th><th>Market</th><th>Deal Type</th><th>Loan Amount</th><th>Year</th></tr></thead><tbody>' +

          tableRows +

          "</tbody></table></div>"

      )

    );

  }



  function renderRequiredInfo(p) {

    var docs = p.requiredDocuments || [];

    var docsHtml = "";

    var defaultNote = p.usesDefaultRequiredDocuments
      ? '<p class="gold-mock-tab-empty odna-subsection-intro">Standard hospitality financing checklist — confirm provider-specific requirements before submission.</p>'
      : "";

    if (docs.length) {

      docsHtml =

        defaultNote +

        '<table class="cpe-doc-table"><thead><tr><th>Document</th><th>Category</th><th>Required</th><th>Guidance</th></tr></thead><tbody>' +

        docs

          .map(function (d) {

            return (

              "<tr><td>" +

              escapeHtml(d.name) +

              (d.isDefault ? ' <span class="meta-muted">(standard)</span>' : "") +

              "</td><td>" +

              escapeHtml(d.category || "—") +

              "</td><td>" +

              escapeHtml(d.required || "—") +

              "</td><td>" +

              escapeHtml(d.notes || "—") +

              "</td></tr>"

            );

          })

          .join("") +

        "</tbody></table>";

    } else {

      docsHtml = '<p class="gold-mock-tab-empty">No document checklist on file.</p>';

    }

    return (

      section("Required Information Checklist", bulletList(p.requiredInformation)) +

      section("Required Documents", docsHtml)

    );

  }



  function renderProcess(p) {

    return (

      section(

        "Process Overview",

        '<div class="card"><p>' + escapeHtml(p.processOverview || "Not provided.") + "</p></div>"

      ) +

      section(

        "Typical Timeline & Engagement",

        '<div class="grid-2">' +

          cluster("What to Expect", [

            "Initial screening against lending appetite and deal criteria",

            "Term sheet or indicative structure after package review",

            "Third-party reports and credit diligence",

            "Committee approval and closing coordination",

          ]) +

          cluster("Owner Preparation Tips", [

            "Prepare sources & uses and sponsor equity summary early",

            "Include operator and brand agreements where applicable",

            "Share trailing financials and market context upfront",

            "Align contact pathway before sharing sensitive materials",

          ]) +

          "</div>"

      )

    );

  }



  function renderContact(p) {

    var contacts = p.contacts || [];

    var contactsHtml = "";

    if (contacts.length) {

      contactsHtml =

        '<div class="quant-grid">' +

        contacts

          .map(function (c) {

            return cluster(c.name + (c.title ? " — " + c.title : ""), [

              c.email ? "Email: " + c.email : "",

              c.phone ? "Phone: " + c.phone : "",

              c.notes || "",

            ].filter(Boolean));

          })

          .join("") +

        "</div>";

    } else {

      contactsHtml =

        '<p class="gold-mock-tab-empty">Public contact details are shared through the pathway below. Leadership contacts may be available after introduction.</p>';

    }

    if ((p.leadership || []).length) {

      contactsHtml +=

        section(

          "Hospitality Leadership Team",

          '<div class="proof-grid oe-leader-profile-grid" style="--oe-leader-profile-cols: 3;">' +

            p.leadership.map(leaderCard).join("") +

            "</div>"

        );

    }

    return (

      section(

        "Contact Pathway",

        '<div class="card"><p><strong>' +

          escapeHtml(p.contactPathway || "—") +

          "</strong></p><p>" +

          escapeHtml(p.contactPathwayDetail || "Not provided.") +

          "</p></div>"

      ) +

      section("How to Engage", contactsHtml)

    );

  }



  function renderInternal(internal) {

    if (!internal) {

      return '<div class="cpe-internal-banner">Internal notes are not available for your account.</div>';

    }

    return (

      '<div class="cpe-internal-banner">Admin / demo internal view — not visible to owner-facing users.</div>' +

      section(

        "Credit Box & Appetite",

        '<div class="grid-2">' +

          '<div class="card"><h3>Current Lending Appetite</h3><p>' +

          escapeHtml(internal.currentLendingAppetite || "—") +

          "</p></div>" +

          '<div class="card"><h3>Relationship Sensitivity</h3><p>' +

          escapeHtml(internal.relationshipSensitivity || "—") +

          "</p></div></div>"

      ) +

      section(

        "Risk & Structure Guidance",

        '<div class="quant-grid">' +

          cluster("Risk Limits", [internal.riskLimits || "—"]) +

          cluster("Leverage Ranges", [internal.leverageRanges || "—"]) +

          cluster("Pricing Guidance", [internal.pricingGuidance || "—"]) +

          cluster("Markets Paused", internal.marketsPaused || ["—"]) +

          "</div>"

      ) +

      section("Credit Box Notes", '<div class="card"><p>' + escapeHtml(internal.creditBoxNotes || "—") + "</p></div>") +

      section(

        "Deal Decline Patterns",

        '<div class="card"><p>' + escapeHtml(internal.dealDeclinePatterns || "—") + "</p></div>"

      ) +

      section(

        "Internal Notes",

        '<div class="card"><p>' +

          escapeHtml(internal.internalNotes || "—") +

          "</p><p><strong>Internal contacts:</strong> " +

          escapeHtml(internal.internalContacts || "—") +

          "</p><p><strong>Last verified:</strong> " +

          escapeHtml(internal.lastVerifiedDate || "—") +

          " · <strong>Confidence:</strong> " +

          escapeHtml(internal.sourceConfidence || "—") +

          "</p></div>"

      )

    );

  }



  function activateTab(tabId) {

    document.querySelectorAll(".tabs-section .section-nav-item[data-tab]").forEach(function (btn) {

      btn.classList.toggle("active", btn.getAttribute("data-tab") === tabId);

    });

    document.querySelectorAll("#cpeProfilePanels .tab-content").forEach(function (panel) {

      panel.classList.toggle("active", panel.id === "tab-" + tabId);

    });

  }



  function getPopupToken() {

    var params = new URLSearchParams(window.location.search);

    var token = params.get("popupToken");

    return token ? decodeURIComponent(token) : "";

  }



  function notifyParentProfileReady(live) {

    if (window === window.top) return;

    var token = getPopupToken();

    if (!token) return;

    try {

      window.parent.postMessage(

        {

          type: "capital-provider-profile-ready",

          popupToken: token,

          live: live === true,

        },

        window.location.origin

      );

    } catch (e) {

      console.warn("[CapitalProviderExplorerDetail] postMessage failed:", e);

    }

  }



  function showProfileState(state, message) {

    document.getElementById("cpeProfileLoading").classList.toggle("hidden", state !== "loading");

    document.getElementById("cpeProfileError").classList.toggle("hidden", state !== "error");

    document.getElementById("cpeProfileContent").classList.toggle("hidden", state !== "success");

    if (state === "error" && message) {

      document.getElementById("cpeProfileErrorMessage").textContent = message;

    }

    if (state === "success") notifyParentProfileReady(true);

    if (state === "error") notifyParentProfileReady(false);

  }



  /** Hero badge + data-source line (Explorer Hero Verification / Data Source). */
  function buildHeroVerificationLineHtml(p) {
    p = p || {};
    var verification = String(p.explorerHeroVerification || "").trim();
    var dataSource = String(p.explorerHeroDataSource || "").trim();

    if (!dataSource) {
      dataSource = "Live Airtable / Capital Setup data";
    }

    if (!verification && !dataSource) return "";

    var html = "";
    if (verification) {
      html +=
        '<span class="oe-hero-badge-verified" title="' +
        escapeHtml(verification) +
        '">' +
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">' +
        '<path d="M9 12l2 2 4-4"/><circle cx="12" cy="12" r="9"/>' +
        "</svg>" +
        escapeHtml(verification) +
        "</span>";
    }
    if (dataSource) {
      html += '<span class="meta-muted">' + escapeHtml(dataSource) + "</span>";
    }
    return html;
  }



  function renderHeroChips(p) {

    var chips = [];

    if (p.institutionType) chips.push(p.institutionType);

    if (p.loanSizeLabel) chips.push(p.loanSizeLabel);

    var visLabel = visibilityBadgeLabel(p.visibility);

    if (visLabel) chips.push(visLabel);

    if (String(p.profileStatus || "").trim() === "Needs Review") chips.push("Needs Review");

    var el = document.getElementById("cpeHeroChips");

    if (!el) return;

    el.innerHTML = chips

      .map(function (c) {

        return '<span class="oe-hero-chip">' + escapeHtml(c) + "</span>";

      })

      .join("");

  }



  function renderHero(p) {

    document.getElementById("cpeProfileName").textContent = p.name || "Capital Provider";

    document.getElementById("cpeBreadcrumbName").textContent = p.name || "Profile";

    var verifiedEl = document.getElementById("cpeHeroVerifiedLine");
    if (verifiedEl) {
      var heroLineHtml = buildHeroVerificationLineHtml(p);
      verifiedEl.innerHTML = heroLineHtml;
      verifiedEl.hidden = !heroLineHtml;
    }

    document.getElementById("cpeProfileTagline").textContent =

      p.shortDescription || [p.institutionType, p.loanSizeLabel].filter(Boolean).join(" · ");

    document.getElementById("cpeProfileStatement").textContent = p.hotelLendingFocus || "";



    var logoImg = document.getElementById("cpeProfileLogoImg");

    var logoInitial = document.getElementById("cpeProfileLogoInitial");

    var logoUrl = resolveLogoUrl(p);

    if (logoImg && logoUrl) {

      logoImg.src = logoUrl;

      logoImg.alt = p.name || "Logo";

      logoImg.hidden = false;

      logoImg.onerror = function () {

        logoImg.hidden = true;

        if (logoInitial) logoInitial.style.display = "flex";

      };

      if (logoInitial) logoInitial.style.display = "none";

    } else {

      if (logoImg) logoImg.hidden = true;

      if (logoInitial) {

        logoInitial.style.display = "flex";

        logoInitial.textContent = (p.name || "C").charAt(0).toUpperCase();

      }

    }



    var hero = document.getElementById("cpeProfileHero");

    if (hero) hero.style.setProperty("--cpe-hero-stripe", institutionStripe(p.institutionType));



    var badgesEl = document.getElementById("cpeProfileBadges");

    if (badgesEl) {
      var badgeParts = [];
      var visLabel = visibilityBadgeLabel(p.visibility);
      if (visLabel) {
        badgeParts.push(
          '<span class="cpe-card-badge ' +
            visibilityBadgeClass(p.visibility) +
            '">' +
            escapeHtml(visLabel) +
            "</span>"
        );
      }
      if (String(p.profileStatus || "").trim() === "Needs Review") {
        badgeParts.push(
          '<span class="cpe-card-badge cpe-card-badge--needs-review">Needs Review</span>'
        );
      }
      badgesEl.innerHTML = badgeParts.join("");
    }



    var metaEl = document.getElementById("cpeProfileHeroMeta");

    if (metaEl) {

      var cards = renderHeroMeta(p);

      var count = (cards.match(/meta-card/g) || []).length;

      metaEl.className = "hero-meta oe-hero-meta-single-row oe-hero-meta-single-row--count-" + count;

      metaEl.innerHTML = cards;

    }



    renderHeroChips(p);

  }



  async function authHeaders() {

    var auth = window.DealalityMemberstackAuth;

    if (!auth || typeof auth.getMemberstackJwtWhenReady !== "function") return {};

    try {

      var jwt = await auth.getMemberstackJwtWhenReady(8000);

      if (jwt) return { Authorization: "Bearer " + jwt };

    } catch (e) {

      console.warn("[CapitalProviderExplorerDetail] auth headers skipped:", e);

    }

    return {};

  }



  async function loadProfile(providerId) {

    showProfileState("loading");

    try {

      var headers = await authHeaders();

      var res = await fetch(

        apiUrl("/api/capital-provider-explorer/provider?id=" + encodeURIComponent(providerId)),

        { headers: headers }

      );

      var data = await res.json();

      if (!res.ok || !data.success) {

        var msg = data.error || data.message || "Failed to load profile";

        if (res.status === 404 && msg === "API route not found") {

          msg =

            "API route not found — restart the backend (npm start) so /api/capital-provider-explorer is registered.";

        }

        throw new Error(msg);

      }



      var p = data.provider;

      var internal = data.internal || null;

      var canInternal = !!(data.meta && data.meta.canViewInternal);



      renderHero(p);



      document.getElementById("tab-profile").innerHTML = renderProfileTab(p);

      document.getElementById("tab-lending-focus").innerHTML = renderLendingFocus(p);

      document.getElementById("tab-deal-criteria").innerHTML = renderDealCriteria(p);

      document.getElementById("tab-track-record").innerHTML = renderTrackRecord(p);

      document.getElementById("tab-required-info").innerHTML = renderRequiredInfo(p);

      document.getElementById("tab-process").innerHTML = renderProcess(p);

      document.getElementById("tab-contact").innerHTML = renderContact(p);

      document.getElementById("tab-internal-notes").innerHTML = renderInternal(internal);

      renderSourceReferencesFooter(p);



      var internalTab = document.getElementById("cpeInternalNotesTab");

      if (internalTab) internalTab.classList.toggle("hidden", !canInternal);



      document.title = (p.name || "Capital Explorer Profile") + " - Dealality";

      var saveBtn = document.getElementById("cpeSaveToList");
      if (saveBtn) {
        saveBtn.setAttribute("data-cpe-provider-id", p.id || providerId);
        saveBtn.setAttribute("data-cpe-provider-name", p.name || "");
      }

      var favReady =
        window.CapitalExplorerFavorites && window.CapitalExplorerFavorites.ready
          ? window.CapitalExplorerFavorites.ready()
          : Promise.resolve();
      favReady
        .then(function () {
          if (window.CapitalExplorerFavorites && window.CapitalExplorerFavorites.wireSaveButtons) {
            window.CapitalExplorerFavorites.wireSaveButtons(document);
          }
        })
        .catch(function () {
          if (window.CapitalExplorerFavorites && window.CapitalExplorerFavorites.wireSaveButtons) {
            window.CapitalExplorerFavorites.wireSaveButtons(document);
          }
        });

      showProfileState("success");

    } catch (err) {

      console.error("[CapitalProviderExplorerDetail] load failed:", err);

      showProfileState("error", err.message || "Unknown error");

    }

  }



  function wirePlaceholderCtAs() {
    var prepareBtn = document.getElementById("cpePrepareProfile");
    if (prepareBtn) {
      prepareBtn.addEventListener("click", function () {
        window.alert(
          "Prepare Financing Profile is coming soon. You will be able to assemble a financing package from here."
        );
      });
    }
  }



  document.addEventListener("DOMContentLoaded", function () {

    var params = new URLSearchParams(window.location.search);

    var providerId = params.get("id") ? decodeURIComponent(params.get("id")) : "";

    if (!providerId) {

      showProfileState("error", "Capital provider ID is required.");

      return;

    }



    document.querySelectorAll(".tabs-section .section-nav-item[data-tab]").forEach(function (btn) {

      btn.addEventListener("click", function () {

        activateTab(btn.getAttribute("data-tab"));

      });

    });



    wirePlaceholderCtAs();

    loadProfile(providerId);

  });

})();

