/**
 * Capital Provider Explorer — owner-facing profile tab renderers (live API data).
 */
(function (global) {
  "use strict";

  var FALLBACK_DETAIL =
    "Additional public-source profile detail has not yet been verified.";

  var DOC_CATEGORY_ORDER = [
    "Deal Summary",
    "Financing Request",
    "Sources & Uses",
    "Property Financials",
    "Market Data",
    "Sponsor Information",
    "Brand Information",
    "Operator Information",
    "Capex / PIP",
    "Legal / Ownership",
    "Existing Debt",
    "Development / Construction",
    "Environmental / Technical",
    "Insurance",
    "Tax / Compliance",
    "Other",
  ];

  var CONTACT_PATHWAY_COPY = {
    "Request Introduction":
      "This provider may require a relationship-led introduction.",
    "Submit Financing Profile":
      "Owners should prepare a structured financing profile before outreach.",
    "Invite to Review Deal":
      "Future functionality may allow owners to invite providers to review a deal teaser.",
    "Direct Contact Available":
      "Public contact information may be available through the provider's website.",
    "Internal Review First":
      "Dealality can help organize the financing profile before determining whether outreach is appropriate.",
    "Private / Invite Only":
      "This provider should not be contacted directly through the platform without review.",
    Unknown:
      "Contact approach should be confirmed before sharing sensitive deal materials.",
  };

  function escapeHtml(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function textOrFallback(value, fallback) {
    var t = String(value || "").trim();
    return t ? escapeHtml(t) : '<span class="meta-muted">' + escapeHtml(fallback || FALLBACK_DETAIL) + "</span>";
  }

  function tagList(items) {
    var list = (items || []).filter(Boolean);
    if (!list.length) return '<p class="gold-mock-tab-empty">' + escapeHtml(FALLBACK_DETAIL) + "</p>";
    return (
      '<div class="cpe-chip-row">' +
      list.map(function (x) {
        return '<span class="cpe-chip">' + escapeHtml(x) + "</span>";
      }).join("") +
      "</div>"
    );
  }

  function badge(label, kind) {
    return '<span class="cpe-badge cpe-badge--' + escapeHtml(kind || "neutral") + '">' + escapeHtml(label) + "</span>";
  }

  function section(title, body) {
    return '<section class="cpe-section"><h2 class="cpe-section__title">' + escapeHtml(title) + "</h2>" + body + "</section>";
  }

  function callout(kind, html) {
    return '<div class="cpe-callout cpe-callout--' + kind + '" role="note">' + html + "</div>";
  }

  function formatUsd(n) {
    if (n == null || !Number.isFinite(Number(n))) return "";
    return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(n);
  }

  function metaCardLink(label, url) {
    if (!url) return "";
    var display = String(url).replace(/^https?:\/\//i, "").replace(/^www\./i, "").replace(/\/$/, "");
    return (
      '<div class="cpe-meta-card"><div class="cpe-meta-card__label">' +
      escapeHtml(label) +
      '</div><a class="cpe-meta-card__value" href="' +
      escapeHtml(url) +
      '" target="_blank" rel="noopener noreferrer">' +
      escapeHtml(display) +
      "</a></div>"
    );
  }

  function groupDocuments(docs) {
    var groups = {};
    (docs || []).forEach(function (d) {
      var cat = d.category || "Other";
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push(d);
    });
    var order = DOC_CATEGORY_ORDER.concat(Object.keys(groups).filter(function (k) {
      return DOC_CATEGORY_ORDER.indexOf(k) === -1;
    }));
    return order
      .filter(function (cat) {
        return groups[cat] && groups[cat].length;
      })
      .map(function (cat) {
        return { category: cat, items: groups[cat] };
      });
  }

  function renderCriteriaCard(c) {
    var warn =
      c.isTransactionExample || (c.maximumLoanSize != null && String(c.ownerVisibleSummary || "").toLowerCase().indexOf("transaction") !== -1)
        ? callout(
            "warning",
            "<strong>Transaction example.</strong> This reflects a public transaction example and should not be interpreted as a published lending limit or product criterion."
          )
        : "";

    function row(label, value) {
      if (value == null || value === "" || (Array.isArray(value) && !value.length)) return "";
      var display = Array.isArray(value) ? value.join(", ") : value;
      if (typeof display === "number") display = formatUsd(display);
      return "<li><strong>" + escapeHtml(label) + ":</strong> " + escapeHtml(display) + "</li>";
    }

    var rows = [
      row("Loan Product", c.loanProduct),
      row("Deal Types", c.dealTypes),
      row("Minimum Loan Size", c.minimumLoanSize),
      row("Maximum Loan Size", c.maximumLoanSize),
      row("Minimum Total Project Cost", c.minimumTotalProjectCost),
      row("Maximum Total Project Cost", c.maximumTotalProjectCost),
      row("Term Range", c.termRange),
      row("Recourse Preference", c.recoursePreference),
      row("Rate Type", c.rateType),
      row("Currency", c.currency),
      row("Sponsor Requirements", c.sponsorRequirements),
      row("Equity Requirements", c.equityRequirements),
      row("Collateral Requirements", c.collateralRequirements),
      row("Brand / Flag Requirements", c.brandRequirements),
      row("Operator Requirements", c.operatorRequirements),
      row("Market Requirements", c.marketRequirements),
      row("Appetite Status", c.appetiteStatus),
      row("Source Confidence", c.sourceConfidence),
      row("Last Verified", c.lastVerifiedDate),
    ].join("");

    return (
      '<article class="cpe-criteria-card">' +
      "<h3>" +
      escapeHtml(c.criteriaName || "Lending criteria") +
      "</h3>" +
      warn +
      (c.ownerVisibleSummary ? "<p>" + escapeHtml(c.ownerVisibleSummary) + "</p>" : "") +
      (rows ? '<ul class="cpe-check-list">' + rows + "</ul>" : "") +
      "</article>"
    );
  }

  function renderSidebar(p) {
    var rows = [
      ["Institution Type", p.institutionType],
      ["Primary Region", p.primaryRegion],
      ["Geographic Coverage", (p.geographicCoverage || []).join(", ")],
      ["Loan Products", (p.loanProducts || []).join(", ")],
      ["Typical Deal Types", p.typicalDealTypes],
      ["Preferred Asset Types", (p.preferredAssetTypes || []).join(", ")],
      ["Project Stage Appetite", (p.projectStages || []).join(", ")],
      ["Contact Pathway", p.contactPathway],
      ["Source Confidence", p.sourceConfidence],
      ["Last Verified", p.lastVerifiedDate],
    ];

    var html =
      '<div class="cpe-sidebar-card"><h2 class="cpe-sidebar-card__title">At a Glance</h2><dl class="cpe-sidebar-dl">';
    rows.forEach(function (pair) {
      var val = String(pair[1] || "").trim();
      if (!val) return;
      html +=
        "<dt>" + escapeHtml(pair[0]) + "</dt><dd>" + escapeHtml(val) + "</dd>";
    });
    html +=
      '</dl><div class="cpe-sidebar-actions">' +
      '<button type="button" class="btn cpe-save-btn" data-cpe-provider-id="' +
      escapeHtml(p.id) +
      '" data-cpe-provider-name="' +
      escapeHtml(p.name || "") +
      '">Save to Financing List</button>' +
      '<button type="button" class="btn btn--primary" id="cpePrepareProfileSidebar" disabled title="Coming soon">Prepare Financing Profile</button>' +
      '<p class="cpe-sidebar-cta-note">Coming soon: save to a deal-specific financing list and prepare a lender review package.</p>' +
      "</div></div>";
    return html;
  }

  function renderOverview(p) {
    return (
      section("Institution Overview", '<div class="card"><p>' + textOrFallback(p.institutionOverview) + "</p></div>") +
      section("Short Description", '<div class="card"><p>' + textOrFallback(p.shortDescription) + "</p></div>") +
      section("Hotel Lending Focus", '<div class="card"><p>' + textOrFallback(p.hotelLendingFocus) + "</p></div>") +
      '<div class="cpe-meta-grid">' +
      metaCardLink("Website", p.website) +
      '<div class="cpe-meta-card"><div class="cpe-meta-card__label">Headquarters</div><div class="cpe-meta-card__value">' +
      textOrFallback(p.headquarters) +
      "</div></div>" +
      '<div class="cpe-meta-card"><div class="cpe-meta-card__label">Primary Region</div><div class="cpe-meta-card__value">' +
      textOrFallback(p.primaryRegion) +
      "</div></div>" +
      "</div>" +
      section("Geographic Coverage", tagList(p.geographicCoverage)) +
      section("Source Confidence", '<p>' + textOrFallback(p.sourceConfidence) + " · Last verified: " + textOrFallback(p.lastVerifiedDate, "—") + "</p>") +
      (p.ownerFacingNotes
        ? section("Owner-Facing Notes", '<div class="card"><p>' + escapeHtml(p.ownerFacingNotes) + "</p></div>")
        : "") +
      callout("info", escapeHtml(p.disclaimer || "Capital provider information is for organizational and informational purposes only."))
    );
  }

  function renderLendingFocus(p) {
    var hasBrandOps =
      String(p.brandPreference || "").trim() ||
      String(p.operatorPreference || "").trim() ||
      String(p.sponsorPreference || "").trim() ||
      (p.brandPreferences || []).length ||
      (p.operatorPreferences || []).length;

    return (
      section("Hotel Lending Focus", '<div class="card"><p>' + textOrFallback(p.hotelLendingFocus) + "</p></div>") +
      section("Preferred Markets", tagList(p.preferredMarkets)) +
      section("Geographic Coverage", tagList(p.geographicCoverage)) +
      section("Preferred Asset Types", tagList(p.preferredAssetTypes)) +
      section("Typical Deal Types", tagList(Array.isArray(p.typicalDealTypes) ? p.typicalDealTypes : String(p.typicalDealTypes || "").split(",").map(function (s) { return s.trim(); }).filter(Boolean))) +
      section("Project Stage Appetite", tagList(p.projectStages)) +
      (hasBrandOps
        ? section(
            "Relationship Preferences",
            tagList(
              [p.brandPreference || (p.brandPreferences || [])[0], p.operatorPreference || (p.operatorPreferences || [])[0], p.sponsorPreference]
                .filter(Boolean)
            )
          )
        : section(
            "Relationship Preferences",
            '<p class="gold-mock-tab-empty">Brand, operator, and sponsor preferences have not been publicly verified for this provider.</p>'
          ))
    );
  }

  function renderDealCriteria(p) {
    var criteria = p.criteria || [];
    if (!criteria.length) {
      return (
        '<p class="gold-mock-tab-empty">No provider-specific deal criteria have been verified from public sources yet. Review the Lending Focus and Required Info tabs for broader financing-readiness context.</p>'
      );
    }
    return (
      '<div class="cpe-criteria-grid">' + criteria.map(renderCriteriaCard).join("") + "</div>"
    );
  }

  function renderRequiredInfo(p) {
    var docs = p.requiredDocuments || [];
    if (!docs.length) {
      return '<p class="gold-mock-tab-empty">No required information checklist is available for this provider yet.</p>';
    }

    var groups = groupDocuments(docs);
    var html = "";
    if (p.requiredInformation && p.requiredInformation.length) {
      html += section(
        "Required Information Summary",
        '<div class="card"><p>' + escapeHtml(p.requiredInformation[0].detail || "") + "</p></div>"
      );
    }

    groups.forEach(function (g) {
      html += section(
        g.category,
        g.items
          .map(function (d) {
            var badges = d.isGeneralReadiness
              ? badge("General Readiness", "general")
              : badge("Provider-Specific", "provider");
            var note = d.isGeneralReadiness
              ? '<p class="cpe-doc-note">This item is a general hotel financing readiness item and is not confirmed as a provider-specific requirement unless supported by source references.</p>'
              : "";
            return (
              '<article class="cpe-doc-card">' +
              "<h3>" +
              escapeHtml(d.name) +
              " " +
              badges +
              "</h3>" +
              note +
              "<p><strong>Required level:</strong> " +
              escapeHtml(d.required || "—") +
              "</p>" +
              (d.appliesToDealTypes && d.appliesToDealTypes.length
                ? "<p><strong>Applies to:</strong> " + escapeHtml(d.appliesToDealTypes.join(", ")) + "</p>"
                : "") +
              (d.description ? "<p>" + escapeHtml(d.description) + "</p>" : "") +
              (d.notes ? '<p class="meta-muted">' + escapeHtml(d.notes) + "</p>" : "") +
              "</article>"
            );
          })
          .join("")
      );
    });
    return html;
  }

  function renderProcess(p) {
    var steps = [
      "Prepare financing profile",
      "Confirm potential fit",
      "Organize required documents",
      "Decide whether to request introduction or share a teaser",
      "Track lender feedback and next steps",
    ];
    return (
      section("Process Overview", '<div class="card"><p>' + textOrFallback(p.processOverview) + "</p></div>") +
      section(
        "Required Information Summary",
        '<div class="card"><p>' +
          textOrFallback(
            p.requiredInformation && p.requiredInformation[0] ? p.requiredInformation[0].detail : "",
            FALLBACK_DETAIL
          ) +
          "</p></div>"
      ) +
      (p.ownerFacingNotes
        ? section("Owner-Facing Notes", '<div class="card"><p>' + escapeHtml(p.ownerFacingNotes) + "</p></div>")
        : "") +
      section(
        "Suggested Owner Process Steps",
        '<ol class="cpe-process-steps">' +
          steps
            .map(function (s) {
              return "<li>" + escapeHtml(s) + "</li>";
            })
            .join("") +
          "</ol>" +
          '<p class="meta-muted">Process steps are Dealality organizational guidance unless a provider-specific process is publicly documented.</p>'
      )
    );
  }

  function renderContactPathway(p) {
    var pathway = String(p.contactPathway || "").trim();
    var explanation = CONTACT_PATHWAY_COPY[pathway] || CONTACT_PATHWAY_COPY.Unknown;
    return (
      section(
        "Contact Pathway",
        '<div class="card"><p><strong>' +
          escapeHtml(pathway || "Not specified") +
          "</strong></p><p>" +
          escapeHtml(explanation) +
          "</p></div>"
      ) +
      (p.ownerFacingNotes
        ? section("Owner-Facing Notes", '<div class="card"><p>' + escapeHtml(p.ownerFacingNotes) + "</p></div>")
        : "") +
      section(
        "Next Steps",
        '<div class="btn-row">' +
          '<button type="button" class="btn cpe-save-btn" data-cpe-provider-id="' +
          escapeHtml(p.id) +
          '" data-cpe-provider-name="' +
          escapeHtml(p.name || "") +
          '">Save to Financing List</button>' +
          '<button type="button" class="btn btn--primary" disabled title="Coming soon">Prepare Financing Profile</button>' +
          '<button type="button" class="btn" disabled title="Coming soon">Request Introduction Review</button>' +
          "</div>" +
          '<p class="meta-muted">Request Introduction Review is not yet available in this MVP.</p>'
      )
    );
  }

  function renderSources(p) {
    var sources = p.sourceReferences || [];
    if (!sources.length) {
      return '<p class="gold-mock-tab-empty">No source references are on file for this provider.</p>';
    }
    var items = sources
      .map(function (s) {
        var link = s.url
          ? '<a href="' + escapeHtml(s.url) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(s.name || "Source") + "</a>"
          : escapeHtml(s.name || "Source");
        return (
          '<article class="cpe-source-card"><h3>' +
          link +
          "</h3>" +
          "<p>" +
          badge(s.sourceType || "Public Source", "neutral") +
          " " +
          (s.confidence ? badge(s.confidence, "confidence") : "") +
          "</p>" +
          (s.sourceDate ? "<p><strong>Source date:</strong> " + escapeHtml(s.sourceDate) + "</p>" : "") +
          (s.retrievedDate ? "<p><strong>Retrieved / reviewed:</strong> " + escapeHtml(s.retrievedDate) + "</p>" : "") +
          (s.summary ? "<p>" + escapeHtml(s.summary) + "</p>" : "") +
          (s.relevantFields
            ? '<p class="meta-muted"><strong>Relevant fields:</strong> ' +
              escapeHtml(Array.isArray(s.relevantFields) ? s.relevantFields.join(", ") : s.relevantFields) +
              "</p>"
            : "") +
          "</article>"
        );
      })
      .join("");
    return (
      items +
      callout(
        "info",
        "Source references are used to support public profile information. They may not reflect current credit appetite, pricing, availability, or approval criteria."
      )
    );
  }

  global.CpeProfilePresentations = {
    renderSidebar: renderSidebar,
    renderOverview: renderOverview,
    renderLendingFocus: renderLendingFocus,
    renderDealCriteria: renderDealCriteria,
    renderRequiredInfo: renderRequiredInfo,
    renderProcess: renderProcess,
    renderContactPathway: renderContactPathway,
    renderSources: renderSources,
    badge: badge,
  };
})(typeof window !== "undefined" ? window : this);
