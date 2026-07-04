/**

 * Operator Alignment Snapshot — profile-level standalone document renderer.

 * Data: GET /api/operator-alignment-snapshot/:dealId/profile

 */

(function (global) {

  "use strict";



  var PAGE_DISCLAIMER =

    "This snapshot is not a recommendation or advisory conclusion. It highlights potential alignment signals, review considerations, and data gaps based on the information available in Dealality.";



  var OUTPUT_NOTE_PROFILE_ONLY =

    "This snapshot currently shows profile-level operator alignment. Company-level alignment will appear once Operator Setup profiles are complete enough for this deal. " +

    "The output organizes owner/advisor review and does not indicate endorsement, approval, availability, or commercial terms.";



  var OUTPUT_NOTE_WITH_COMPANIES =

    "This snapshot includes profile-level alignment and company-level alignment signals based on available Operator Setup data. " +

    "Company-level results do not indicate endorsement, availability, approval, or commercial terms.";



  var COMPANIES_GATED_PRIMARY =

    "Company-level operator alignment will appear here once Operator Setup profiles are complete enough for this deal.";



  var COMPANIES_GATED_SUPPORT =

    "Profile-level alignment remains available above. Named company alignment depends on live Operator Setup records and sufficient structured data.";



  var REVIEW_SIGNAL_INTRO =

    "Operator review may be relevant based on the available deal profile.";



  var DEALALITY_LOGO_URL =

    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";

  /** Printable cover height for OAS PDF (A4 minus margins; tuned below full 273mm). */
  var OAS_PRINT_COVER_HEIGHT_MM = 265;




  var NOT_PROVIDED = "Not provided";



  /** Owner-facing labels for internal deal signal keys (API unchanged). */

  var SIGNAL_KEY_LABELS = {

    new_build_project: "New-build project",

    conversion_or_reflag: "Conversion or reflag",

    repositioning_project: "Repositioning project",

    preopening_or_reopening_phase: "Pre-opening or reopening phase",

    brand_path_active: "Brand path under review",

    brand_affiliation_under_review: "Brand affiliation under review",

    preferred_future_third_party: "Third-party management path indicated",

    preferred_future_third_party_only: "Third-party management only",

    preferred_future_brand_managed: "Brand-managed path indicated",

    preferred_future_brand_managed_only: "Brand-managed only",

    preferred_future_owner_operated: "Owner-operated path indicated",

    preferred_future_owner_operated_only: "Owner-operated only",

    preferred_future_owner_operated_path: "Owner-operated path indicated",

    operator_in_scope: "Operator review in scope",

    fb_outlets_present: "F&B outlets present",

    commercial_support_priority: "Commercial support priority",

    revenue_management_priority: "Revenue management priority",

    cala_market: "CALA market exposure",

    operator_strategy_exploring_or_ready: "Operator strategy under review",

    operator_strategy_closed: "Operator strategy closed to input",

    select_service_only_scale: "Select-service operating model",

    services_partial_not_full_management: "Partial services, not full management",

    services_require_partial_management: "Partial management services required",

    full_service_or_upper_scale: "Full-service or upper-scale positioning",

    lifestyle_positioning: "Lifestyle positioning",

    country_or_market_provided: "Country or market provided",

    project_type_known: "Project type recorded",

    preferred_profile_regional: "Regional operator profile preference",

    preferred_profile_international: "International operator profile preference",

    preferred_profile_independent_boutique: "Independent or boutique profile preference",

    preferred_profile_no_preference: "No operator profile preference stated",

    operator_capability_full_management_only: "Full management capability priority",

    chain_scale_upper: "Upper-upscale chain scale",

    legacy_profile_option_match: "Legacy operator profile option maps to this category",

  };



  var WORKFLOW_PREFIX_RE = /^Suggested workflow action:\s*/i;



  function esc(t) {

    return String(t == null ? "" : t)

      .replace(/&/g, "&amp;")

      .replace(/</g, "&lt;")

      .replace(/>/g, "&gt;")

      .replace(/"/g, "&quot;");

  }



  function titleCaseWords(s) {

    return String(s)

      .replace(/_/g, " ")

      .replace(/\s+/g, " ")

      .trim()

      .replace(/\b\w/g, function (c) {

        return c.toUpperCase();

      });

  }



  /**

   * Map internal signal key to owner-facing label.

   * @param {string} key

   * @returns {string}

   */

  function humanizeSignalKey(key) {

    var k = String(key == null ? "" : key).trim();

    if (!k) return "";

    if (SIGNAL_KEY_LABELS[k]) return SIGNAL_KEY_LABELS[k];

    return titleCaseWords(k);

  }



  /**

   * @param {string[]|null|undefined} keys

   * @returns {string[]}

   */

  function humanizeSignalKeys(keys) {

    if (!keys || !keys.length) return [];

    var seen = {};

    var out = [];

    keys.forEach(function (k) {

      var label = humanizeSignalKey(k);

      if (label && !seen[label]) {

        seen[label] = true;

        out.push(label);

      }

    });

    return out;

  }



  function stripWorkflowActionPrefix(text) {

    return String(text == null ? "" : text).replace(WORKFLOW_PREFIX_RE, "").trim();

  }



  function normalizeListItems(items, stripWorkflow) {

    if (!items || !items.length) return [];

    return items.map(function (item) {

      var s = String(item == null ? "" : item).trim();

      return stripWorkflow ? stripWorkflowActionPrefix(s) : s;

    }).filter(Boolean);

  }



  function extractExplanationSignals(explanation, prefix) {

    var text = String(explanation || "");

    var needle = prefix + ":";

    var idx = text.indexOf(needle);

    if (idx < 0) return [];

    var rest = text.slice(idx + needle.length).trim();

    var dot = rest.indexOf(".");

    if (dot >= 0) rest = rest.slice(0, dot);

    return rest

      .split(",")

      .map(function (s) {

        return s.trim();

      })

      .filter(Boolean);

  }



  function profileSupplementalNote(explanation) {

    var text = String(explanation || "").trim();

    if (!text) return "";

    var note = text;

    ["Some required deal signals are not present:", "Matched deal signals include:", "Conditional or offsetting signals:"].forEach(

      function (prefix) {

        var re = new RegExp(prefix + "[^.]*\\.?\\s*", "gi");

        note = note.replace(re, "");

      }

    );

    note = note.trim();

    if (!note) return "";

    return humanizeExplanationText(note);

  }



  function humanizeExplanationText(text) {

    var out = String(text || "");

    Object.keys(SIGNAL_KEY_LABELS).forEach(function (key) {

      var re = new RegExp("\\b" + key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\b", "g");

      out = out.replace(re, SIGNAL_KEY_LABELS[key]);

    });

    return out.replace(/\s{2,}/g, " ").trim();

  }



  function displayVal(v) {

    if (v == null || v === "") return NOT_PROVIDED;

    if (typeof v === "number" && Number.isFinite(v)) return String(v);

    var s = String(v).trim();

    return s || NOT_PROVIDED;

  }



  function formatDate(iso) {

    if (!iso) return "";

    try {

      var d = new Date(iso);

      if (Number.isNaN(d.getTime())) return String(iso).slice(0, 10);

      return d.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });

    } catch (_) {

      return String(iso).slice(0, 10);

    }

  }

  var OAS_COVER_NOTE =
    "This output organizes operator alignment signals based on current deal inputs. " +
    "It is intended to support internal owner/advisor review and does not constitute a recommendation, endorsement, operator approval, " +
    "legal advice, franchise advice, or investment advice.";

  var OAS_OUTPUT_NOTE =
    "This Operator Alignment Snapshot organizes potential operator alignment signals based on current deal inputs and available Operator Setup data. " +
    "It is intended to support structured owner/advisor review and does not constitute a recommendation, endorsement, operator approval, " +
    "franchise advice, valuation, legal advice, or investment advice.";

  var OAS_METHODOLOGY_NOTE =
    "Alignment scores are generated from current deal inputs and available Operator Setup data using Dealality's server-side operator alignment logic. " +
    "Scores are intended to organize owner/advisor review and may change as deal inputs, owner priorities, operator profile data, or readiness information are updated. " +
    "An alignment signal does not indicate operator approval, operator interest, availability, commercial terms, or final suitability.";

  var OAS_TABLE_COMPANY_LIMIT = 8;
  var OAS_DETAIL_CARD_LIMIT = 5;

  function renderTable(headers, rows, keepTogether) {
    var wrapClass = "bas-table-wrap" + (keepTogether ? " bas-table-wrap--keep" : "");
    var html = '<div class="' + wrapClass + '"><table class="bas-brief-table"><thead><tr>';
    headers.forEach(function (h) {
      html += "<th>" + esc(h) + "</th>";
    });
    html += "</tr></thead><tbody>";
    rows.forEach(function (row) {
      html += "<tr>";
      row.forEach(function (cell) {
        html += "<td>" + esc(cell) + "</td>";
      });
      html += "</tr>";
    });
    html += "</tbody></table></div>";
    return html;
  }

  function wrapBookPage(index, innerHtml, active) {
    return (
      '<div class="bas-book-page' +
      (active ? " active" : "") +
      '" data-bas-page="' +
      index +
      '" role="region" aria-hidden="' +
      (active ? "false" : "true") +
      '">' +
      innerHtml +
      "</div>"
    );
  }

  function oasMetaLine(ctx) {
    ctx = ctx || {};
    var parts = [];
    if (ctx.roomCount != null && ctx.roomCount !== "") parts.push(ctx.roomCount + " keys");
    if (ctx.projectType && ctx.projectType !== NOT_PROVIDED) parts.push(ctx.projectType);
    if (ctx.chainScale && ctx.chainScale !== NOT_PROVIDED) parts.push(ctx.chainScale);
    else if (ctx.desiredOperatingModel && ctx.desiredOperatingModel !== NOT_PROVIDED) {
      parts.push(ctx.desiredOperatingModel);
    }
    return parts.length ? parts.join(" · ") : "—";
  }

  function oasLocationLine(ctx) {
    ctx = ctx || {};
    var m =
      ctx.cityOrMarket && ctx.cityOrMarket !== NOT_PROVIDED ? String(ctx.cityOrMarket) : "";
    var c = ctx.country && ctx.country !== NOT_PROVIDED ? String(ctx.country) : "";
    if (!m && !c) return "—";
    if (!m) return c;
    if (!c) return m;
    if (m.toLowerCase().indexOf(c.toLowerCase()) >= 0) return m;
    return m + ", " + c;
  }

  function buildOperatorSummaryParagraphs(data) {
    if (data.operatorAlignmentSummaryParagraphs && data.operatorAlignmentSummaryParagraphs.length) {
      return data.operatorAlignmentSummaryParagraphs.slice();
    }
    var snap = data.companiesSnapshot || {};
    if (snap.operatorAlignmentSummaryParagraphs && snap.operatorAlignmentSummaryParagraphs.length) {
      return snap.operatorAlignmentSummaryParagraphs.slice();
    }
    if (
      snap.operatorAlignmentExecutiveSummary &&
      snap.operatorAlignmentExecutiveSummary.operatorAlignmentSummaryParagraphs
    ) {
      return snap.operatorAlignmentExecutiveSummary.operatorAlignmentSummaryParagraphs.slice();
    }
    var ctx = data.dealContext || {};
    var profiles = data.profilesForReview || [];
    var companiesPack = resolveCompaniesPayload(data);
    var paras = [];
    paras.push(
      "The current inputs describe " +
        displayVal(ctx.dealName) +
        " as a hospitality opportunity in " +
        oasLocationLine(ctx) +
        ". Company-level alignment will appear once Operator Setup records are complete enough for this deal."
    );
    paras.push(
      "The current snapshot shows " +
        profiles.length +
        " operator profile pathways only. Company-level alignment is not yet available for this review set."
    );
    paras.push(
      "This snapshot should be used as an internal screening and discussion tool. " +
        "It does not determine final operator selection, operator interest, approval, availability, or commercial terms."
    );
    return paras;
  }

  function buildPathwayViewRows(profiles) {
    return (profiles || []).map(function (p) {
      var why =
        p.bestUseCase ||
        p.description ||
        (p.alignmentSignals && p.alignmentSignals[0]) ||
        "May merit review against current deal inputs.";
      var clar =
        (p.questionsToClarify && p.questionsToClarify[0]) ||
        (p.dataGaps && p.dataGaps[0]) ||
        "Confirm deal inputs against this pathway.";
      return [p.displayLabel || p.shortLabel || "Operator profile", why, clar];
    });
  }

  function companyReviewStatus(company) {
    var c = company || {};
    if (c.reviewStatusLabel) return String(c.reviewStatusLabel);
    var band = String(c.alignmentBand || "");
    if (/insufficient/i.test(band)) return "Needs more operator profile data";
    if (/conditional/i.test(band)) return "Review if owner confirms operating path";
    if (/limited/i.test(band)) return "Review if management structure aligns";
    if (/moderate|strong/i.test(band)) return "May merit review based on available Operator Setup data";
    return "Review if owner confirms operating path";
  }

  function companyKeyConsideration(company) {
    var c = company || {};
    if (c.keyConsideration) return String(c.keyConsideration);
    var review = mapHumanizedList(c.reviewConsiderations || [], humanizeCompanyReviewConsideration, 1);
    if (review.length) return review[0];
    var sig = mapHumanizedList(c.alignmentSignals || [], humanizeCompanyAlignmentSignal, 1);
    return sig[0] || "Validate alignment signals against deal intake before outreach.";
  }

  function buildPrimaryReviewConsiderations(data) {
    var items = [
      "Market coverage should be validated against active operations, not only pipeline interest.",
      "Management structure and scope should be confirmed before outreach.",
      "Service platform depth should be reviewed against owner must-haves.",
      "Brand/operator responsibility split should be clarified if brand affiliation is in scope.",
      "Reporting cadence and owner governance expectations should be confirmed.",
      "F&B, staffing, and pre-opening support should be validated if relevant.",
    ];
    return items.slice(0, 8);
  }

  function buildClarificationAreas(data) {
    var fromGaps = (data.dataGaps || []).slice();
    var defaults = [
      "Preferred Future Operating Model",
      "Operator Strategy Status",
      "Required Services / Must-Haves",
      "Primary Market Region",
      "Owner Reporting Frequency",
      "Brand / Operator Responsibility Split",
      "Management Structure Preference",
      "Pre-Opening or Transition Support",
    ];
    var seen = {};
    var out = [];
    fromGaps.forEach(function (g) {
      var t = String(g || "").trim();
      if (t && !seen[t]) {
        seen[t] = true;
        out.push(t);
      }
    });
    defaults.forEach(function (d) {
      if (out.length >= 8) return;
      if (!seen[d]) {
        seen[d] = true;
        out.push(d);
      }
    });
    return out.slice(0, 8);
  }

  function buildCurrentReviewStatus(data) {
    var signal = data.operatorReviewSignal || {};
    var companiesPack = resolveCompaniesPayload(data);
    var companyCount = companiesPack.companiesAvailable
      ? (companiesPack.companiesForConsideration || []).length
      : 0;
    var level = String(signal.level || "").trim();
    var highOrMedium = level === "High" || level === "Medium";
    if (companiesPack.companiesAvailable && companyCount >= 3 && highOrMedium) {
      return "Ready for controlled operator review after owner/advisor validation";
    }
    if (level === "Insufficient Data" || !companiesPack.companiesAvailable || companyCount < 3) {
      return "Additional deal and operator setup information needed before controlled operator review";
    }
    return "Ready for controlled operator review after owner/advisor validation";
  }

  function buildCommonQuestions(data) {
    var base = [
      "Which operating functions must be managed by the operator versus retained by the owner?",
      "Is the owner seeking full management, commercial support, or brand-managed structure?",
      "What market coverage must be active versus aspirational?",
      "What reporting package and governance cadence does the owner expect?",
      "Are F&B, staffing, and pre-opening responsibilities clearly defined?",
      "Does the operator need direct experience with the preferred brand or chain scale?",
    ];
    return base;
  }

  function renderListSection(title, items) {
    var html = '<h4 class="bas-brand-card-h4">' + esc(title) + "</h4>";
    if (!(items || []).length) {
      html += '<p class="bas-muted">Not available for current inputs.</p>';
      return html;
    }
    html += '<ul class="bas-detail-list">';
    items.forEach(function (item) {
      html += "<li>" + esc(item) + "</li>";
    });
    html += "</ul>";
    return html;
  }

  function renderOperatorDetailCard(company) {
    var c = company || {};
    var display = resolveCompanyDisplayName(c);
    var score = formatInformationalScore(c.alignmentScoreOptional);
    var tier = c.alignmentBand || "—";
    var html = '<article class="bas-brand-card bas-avoid-break bas-section--keep oas-operator-detail-card">';
    html +=
      '<h3 class="oas-operator-detail-title" data-oas-company-name>' + esc(display.name) + "</h3>";
    if (c.parentCompany) {
      html += '<p class="oas-operator-detail-sub">' + esc(c.parentCompany) + "</p>";
    }
    if (display.missing) {
      html +=
        '<p class="oas-operator-detail-gap bas-muted">Company name missing from Operator Setup profile.</p>';
    }
    html +=
      '<p class="oas-operator-detail-meta">Alignment score: <strong>' +
      esc(score ? score + " / 100" : "Not enough data") +
      "</strong> · " +
      esc(tier) +
      "</p>";
    html +=
      '<h4 class="bas-brand-card-h4">Owner-Facing Alignment Rationale</h4><p class="bas-brand-card-text">' +
      esc(buildCompanyOwnerRationale(c)) +
      "</p>";
    html += renderListSection("What Supports Review", buildCompanyWhatSupports(c));
    html += renderListSection("What Needs Validation", buildCompanyWhatNeedsValidation(c));
    html += renderListSection("What Could Weaken Alignment", buildCompanyWhatCouldWeaken(c));
    html += renderListSection("Owner Questions This Operator Raises", buildCompanyOwnerQuestions(c));
    html += '<h4 class="bas-brand-card-h4 bas-brand-card-h4--technical">Alignment Factors Reviewed</h4>';
    html +=
      '<p class="bas-muted bas-technical-hint">Informational factors from Operator Setup and deal intake (not the primary owner rationale).</p>';
    html += '<ul class="bas-signal-list bas-signal-list--technical">';
    buildCompanyFactorsReviewed(c).forEach(function (s) {
      html += "<li>" + esc(s) + "</li>";
    });
    html += "</ul></article>";
    return html;
  }

  function renderPage1OperatorNarrative(data) {
    var profiles = data.profilesForReview || [];
    var companiesPack = resolveCompaniesPayload(data);
    var companyTotal = companiesPack.companiesAvailable
      ? (companiesPack.companiesForConsideration || []).length
      : 0;
    var companies = companiesPack.companiesAvailable
      ? sortCompaniesForDisplay(companiesPack.companiesForConsideration).slice(0, OAS_TABLE_COMPANY_LIMIT)
      : [];
    var signal = data.operatorReviewSignal || {};
    var html = '<div class="bas-book-page-inner bas-content-page bas-page-narrative">';

    html += '<div class="bas-brief-highlights">';
    html += '<p class="bas-brief-kicker">Operator Alignment Narrative</p>';
    html += '<div class="bas-brief-score-cards">';
    html +=
      '<div class="bas-brief-card"><div class="bas-brief-card-title">Operator Review Signal</div><div class="bas-brief-card-body">' +
      esc(signal.level || "—") +
      "</div></div>";
    html +=
      '<div class="bas-brief-card"><div class="bas-brief-card-title">Operating Companies in Review Set</div><div class="bas-brief-card-body">' +
      esc(companiesPack.companiesAvailable ? String(companyTotal) : "0") +
      "</div></div>";
    html += "</div></div>";

    html += '<div class="bas-brief-panel">';
    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">1. Operator Alignment Summary</h2>';
    buildOperatorSummaryParagraphs(data).forEach(function (p) {
      html += '<p class="bas-summary">' + esc(p) + "</p>";
    });
    html += "</section>";

    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">2. Operator Pathway View</h2>';
    var pathwayRows = buildPathwayViewRows(profiles);
    if (pathwayRows.length) {
      html += renderTable(
        ["Operator pathway", "Why it may merit review", "Clarification needed"],
        pathwayRows,
        true
      );
    } else {
      html += '<p class="bas-muted">No operator pathways available.</p>';
    }
    html += "</section>";

    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">3. Operating Companies for Owner Review</h2>';
    if (!companiesPack.companiesAvailable || !companies.length) {
      html += '<p class="bas-muted">' + esc(companiesPack.gatingReason || COMPANIES_GATED_PRIMARY) + "</p>";
    } else {
      var companyRows = companies.map(function (c) {
        var display = resolveCompanyDisplayName(c);
        return [
          display.name,
          c.parentCompany || "—",
          c.alignmentBand || "—",
          companyReviewStatus(c),
          companyKeyConsideration(c),
        ];
      });
      html += renderTable(
        ["Operating company", "Parent / platform", "Alignment signal", "Review status", "Key consideration"],
        companyRows,
        true
      );
    }
    html += "</section>";

    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">4. Primary Review Considerations</h2><ul class="bas-detail-list">';
    buildPrimaryReviewConsiderations(data).forEach(function (item) {
      html += "<li>" + esc(item) + "</li>";
    });
    html +=
      '</ul><p class="bas-muted">These considerations should be validated before controlled operator outreach.</p></section>';

    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">5. Clarification Areas Before Outreach</h2><ul class="bas-detail-list">';
    buildClarificationAreas(data).forEach(function (item) {
      html += "<li>" + esc(item) + "</li>";
    });
    html += "</ul></section>";

    html += '<div class="bas-narrative-tail">';
    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">6. Current Review Status</h2>';
    html += '<p class="bas-review-status-label">' + esc(buildCurrentReviewStatus(data)) + "</p></section>";
    html +=
      '<footer class="bas-output-note bas-output-note--brief bas-section--keep"><p><strong>Output Note.</strong> ' +
      esc(OAS_OUTPUT_NOTE) +
      "</p></footer>";
    html += "</div></div></div>";
    return html;
  }

  function renderPage2OperatorDetail(data) {
    var companiesPack = resolveCompaniesPayload(data);
    var companies = companiesPack.companiesAvailable
      ? sortCompaniesForDisplay(companiesPack.companiesForConsideration).slice(0, OAS_DETAIL_CARD_LIMIT)
      : [];
    var html = '<div class="bas-book-page-inner bas-content-page bas-page-technical">';

    html += '<div class="bas-brief-highlights">';
    html += '<p class="bas-brief-kicker">Operator Alignment Detail</p>';
    html += '<p class="bas-brief-lead">Supporting operator-level alignment signals and review notes</p>';
    html += "</div>";

    html += '<div class="bas-brief-panel">';
    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">1. Operator Alignment Snapshot Table</h2>';
    if (!companiesPack.companiesAvailable || !companies.length) {
      html +=
        '<p class="bas-muted">Company-level alignment will appear once Operator Setup profiles are complete enough for this deal.</p>';
    } else {
      var rows = companies.map(function (c) {
        var display = resolveCompanyDisplayName(c);
        return [
          display.name,
          c.parentCompany || "—",
          formatInformationalScore(c.alignmentScoreOptional) || "—",
          c.alignmentBand || "—",
          companyKeyConsideration(c),
        ];
      });
      html += renderTable(
        [
          "Operating company",
          "Parent / platform",
          "Numeric score / 100",
          "Alignment tier",
          "Key consideration (business rationale)",
        ],
        rows,
        true
      );
    }
    html += "</section>";

    html += '<section class="bas-section bas-section--brief"><h2 class="bas-section-title">2. Operator-by-Operator Review Cards</h2>';
    if (!companies.length) {
      html += '<p class="bas-muted">No operating companies available for detailed review cards.</p>';
    } else {
      companies.forEach(function (c) {
        html += renderOperatorDetailCard(c);
      });
    }
    html += "</section>";

    html +=
      '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">3. Common Questions to Clarify Before Outreach</h2><ul class="bas-detail-list">';
    buildCommonQuestions(data).forEach(function (q) {
      html += "<li>" + esc(q) + "</li>";
    });
    html += "</ul></section>";

    html += '<section class="bas-section bas-section--brief bas-section--technical-note bas-section--keep"><h2 class="bas-section-title">Methodology Note</h2>';
    html += '<p class="bas-summary">' + esc(OAS_METHODOLOGY_NOTE) + "</p></section>";
    html += "</div></div>";
    return html;
  }

  function bindPageFlip(root) {
    if (!root) return;
    var viewport = root.querySelector("[data-bas-book-viewport]");
    var pages = viewport ? Array.prototype.slice.call(viewport.querySelectorAll(".bas-book-page")) : [];
    if (!viewport || pages.length < 2) return;
    var current = 0;
    var prevBtn = root.querySelector("[data-bas-turn-prev]");
    var nextBtn = root.querySelector("[data-bas-turn-next]");
    var indicator = root.querySelector("[data-bas-page-indicator]");
    var animating = false;
    var flipMs = 750;
    function updateControls() {
      if (indicator) indicator.textContent = current + 1 + " of " + pages.length;
      if (prevBtn) prevBtn.disabled = current === 0 || animating;
      if (nextBtn) nextBtn.disabled = current === pages.length - 1 || animating;
    }
    function clearFlipClasses() {
      pages.forEach(function (p) {
        p.classList.remove("flip-out-forward", "flip-out-back", "flip-in-forward", "flip-in-back");
      });
    }
    function goTo(nextIndex) {
      if (animating || nextIndex === current) return;
      if (nextIndex < 0 || nextIndex >= pages.length) return;
      animating = true;
      updateControls();
      var outPage = pages[current];
      var inPage = pages[nextIndex];
      var forward = nextIndex > current;
      clearFlipClasses();
      outPage.classList.add(forward ? "flip-out-forward" : "flip-out-back");
      inPage.classList.add(forward ? "flip-in-forward" : "flip-in-back");
      inPage.classList.add("active");
      inPage.setAttribute("aria-hidden", "false");
      global.setTimeout(function () {
        outPage.classList.remove("active", "flip-out-forward", "flip-out-back");
        outPage.setAttribute("aria-hidden", "true");
        inPage.classList.remove("flip-in-forward", "flip-in-back");
        pages.forEach(function (p, i) {
          if (i !== nextIndex) {
            p.classList.remove("active");
            p.setAttribute("aria-hidden", "true");
          }
        });
        current = nextIndex;
        animating = false;
        updateControls();
      }, flipMs);
    }
    if (prevBtn) prevBtn.addEventListener("click", function () { goTo(current - 1); });
    if (nextBtn) nextBtn.addEventListener("click", function () { goTo(current + 1); });
    viewport.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight" || e.key === "PageDown") { e.preventDefault(); goTo(current + 1); }
      else if (e.key === "ArrowLeft" || e.key === "PageUp") { e.preventDefault(); goTo(current - 1); }
    });
    updateControls();
  }

  function bandClass(band) {

    var b = String(band || "").toLowerCase();

    if (b.indexOf("strong") >= 0) return "oas-band oas-band--strong";

    if (b.indexOf("moderate") >= 0) return "oas-band oas-band--moderate";

    if (b.indexOf("conditional") >= 0) return "oas-band oas-band--conditional";

    if (b.indexOf("limited") >= 0) return "oas-band oas-band--limited";

    if (b.indexOf("insufficient") >= 0) return "oas-band oas-band--insufficient";

    return "oas-band";

  }



  function renderChipList(title, labels) {

    if (!labels || !labels.length) return "";

    var html = '<div class="oas-chip-block"><h4 class="oas-chip-block__title">' + esc(title) + "</h4>";

    html += '<ul class="oas-chip-list" role="list">';

    labels.forEach(function (label) {

      html += '<li class="oas-chip">' + esc(label) + "</li>";

    });

    html += "</ul></div>";

    return html;

  }



  function renderList(title, items, options) {

    options = options || {};

    var normalized = normalizeListItems(items, options.stripWorkflowPrefix);

    if (!normalized.length) {

      return (

        '<div class="oas-list-block"><h4 class="oas-list-title">' +

        esc(title) +

        '</h4><p class="bas-muted">None listed for this profile.</p></div>'

      );

    }

    var html =

      '<div class="oas-list-block"><h4 class="oas-list-title">' +

      esc(title) +

      '</h4><ul class="bas-detail-list';

    if (options.listClass) html += " " + options.listClass;

    html += '">';

    normalized.forEach(function (item) {

      html += "<li>" + esc(item) + "</li>";

    });

    html += "</ul></div>";

    return html;

  }



  function renderProfileCard(profile) {
    var p = profile || {};
    var title = p.displayLabel || p.shortLabel || "Operator profile";
    var html = '<article class="oas-profile-card bas-brand-card bas-avoid-break bas-section--keep">';
    html += '<header class="oas-card-header">';
    html += '<div class="oas-card-header__title-row">';
    html += '<h3 class="oas-card-title oas-profile-card__title">' + esc(title) + "</h3>";
    html += '<span class="' + bandClass(p.alignmentBand) + ' oas-card-band">' + esc(p.alignmentBand || "—") + "</span>";
    html += "</div>";
    if (p.bestUseCase) {
      html += '<p class="oas-card-lead">' + esc(p.bestUseCase) + "</p>";
    } else if (p.description) {
      html += '<p class="oas-card-lead">' + esc(p.description) + "</p>";
    }
    var chipHtml = "";
    var matchedLabels = humanizeSignalKeys(p.matchedDealSignals).slice(0, 4);
    if (matchedLabels.length) {
      chipHtml += renderChipList("Matched deal signals", matchedLabels);
    }
    var conditionalLabels = humanizeSignalKeys(
      extractExplanationSignals(p.explanation, "Conditional or offsetting signals")
    ).slice(0, 3);
    if (conditionalLabels.length) {
      chipHtml += renderChipList("Conditional signals", conditionalLabels);
    }
    var missingLabels = humanizeSignalKeys(
      extractExplanationSignals(p.explanation, "Some required deal signals are not present")
    ).slice(0, 2);
    if (missingLabels.length) {
      chipHtml += renderChipList("Missing signals", missingLabels);
    }
    if (chipHtml) {
      html += '<div class="oas-card-header__chips">' + chipHtml + "</div>";
    }
    html += "</header>";

    var left =
      renderCompactBullets(
        "Alignment Signals",
        mapHumanizedList(p.alignmentSignals, function (x) {
          return String(x || "").trim();
        }, OAS_PROFILE_BULLET_MAX),
        OAS_PROFILE_BULLET_MAX
      ) +
      renderCompactBullets(
        "Review Considerations",
        (p.reviewConsiderations || []).slice(0, OAS_PROFILE_BULLET_MAX),
        OAS_PROFILE_BULLET_MAX
      );
    var right =
      renderCompactBullets(
        "Questions to Clarify",
        (p.questionsToClarify || []).slice(0, OAS_PROFILE_BULLET_MAX),
        OAS_PROFILE_BULLET_MAX
      ) +
      renderCompactBullets("Data Gaps", (p.dataGaps || []).slice(0, OAS_PROFILE_BULLET_MAX), OAS_PROFILE_BULLET_MAX);
    html += renderCardColumns(left, right);
    html += "</article>";
    return html;
  }



  function renderDealContext(ctx) {
    ctx = ctx || {};
    var ind = ctx.indicators || {};
    var indicatorParts = [];
    if (ind.conversionOrReflag) indicatorParts.push("Conversion / reflag");
    if (ind.repositioning) indicatorParts.push("Repositioning");
    if (ind.newBuild) indicatorParts.push("New build");
    if (ind.preopeningOrReopening) indicatorParts.push("Pre-opening / reopening");
    if (ind.operatorInScope) indicatorParts.push("Third-party operator path in scope");
    var indicatorLine = indicatorParts.length ? indicatorParts.join(" · ") : NOT_PROVIDED;

    var locationParts = [];
    if (ctx.cityOrMarket && ctx.cityOrMarket !== NOT_PROVIDED) locationParts.push(ctx.cityOrMarket);
    if (ctx.country && ctx.country !== NOT_PROVIDED) locationParts.push(ctx.country);
    if (ctx.primaryMarketRegion && ctx.primaryMarketRegion !== NOT_PROVIDED) {
      locationParts.push("Region: " + ctx.primaryMarketRegion);
    }
    var locationLine = locationParts.length ? locationParts.join(" · ") : NOT_PROVIDED;

    return renderKvGrid([
      ["Deal name", displayVal(ctx.dealName)],
      ["Country / city / market", locationLine],
      ["Room count", ctx.roomCount != null ? String(ctx.roomCount) : NOT_PROVIDED],
      ["Project type", displayVal(ctx.projectType)],
      ["Desired operating model", displayVal(ctx.desiredOperatingModel)],
      ["Chain scale", displayVal(ctx.chainScale)],
      ["Project indicators", indicatorLine],
    ]);
  }

  function renderDealContextCard(ctx) {
    return (
      '<div class="oas-brief-card oas-summary-card oas-summary-card--deal">' +
      '<div class="oas-brief-card-title">Deal Context</div>' +
      '<div class="oas-brief-card-body">' +
      renderDealContextCompact(ctx) +
      "</div></div>"
    );
  }

  function renderReviewSignalCard(signal) {
    return (
      '<div class="oas-brief-card oas-summary-card oas-summary-card--signal">' +
      '<div class="oas-brief-card-title">Operator Review Signal</div>' +
      '<div class="oas-brief-card-body">' +
      renderReviewSignal(signal) +
      "</div></div>"
    );
  }

  function renderSummaryGrid(data) {
    var html = '<section class="bas-section bas-section--brief bas-section--keep oas-section oas-section--summary">';
    html += '<h2 class="bas-section-title">Snapshot Summary</h2>';
    html += '<div class="oas-summary-grid">';
    html += renderDealContextCard(data.dealContext);
    html += renderReviewSignalCard(data.operatorReviewSignal);
    html += "</div></section>";
    return html;
  }



  function renderKvGrid(pairs) {

    var html = '<dl class="oas-deal-grid">';

    pairs.forEach(function (p) {

      html += '<div class="oas-deal-kv"><dt>' + esc(p[0]) + "</dt><dd>" + esc(p[1]) + "</dd></div>";

    });

    html += "</dl>";

    return html;

  }



  function renderReviewSignal(signal) {
    signal = signal || {};
    var level = signal.level || "Insufficient Data";
    var html = '<div class="oas-review-signal bas-avoid-break">';
    html += '<div class="oas-review-signal__row">';
    html +=
      '<span class="oas-review-level oas-review-level--' +
      esc(level.replace(/\s+/g, "-").toLowerCase()) +
      '">' +
      esc(level) +
      "</span>";
    html += "</div>";
    if (signal.rationale) {
      html += '<p class="oas-review-signal__rationale">' + esc(signal.rationale) + "</p>";
    }
    var chipLabels = humanizeSignalKeys(signal.matchedSignals);
    if (chipLabels.length) {
      html += renderChipList("Key relevance signals", chipLabels.slice(0, 8));
    }
    html += "</div>";
    return html;
  }



  function renderCover(data, options) {
    var ctx = data.dealContext || {};
    var generatedAt = options.generatedAt || data.generatedAt || new Date().toISOString();
    var html = '<section class="bas-cover-page bas-book-page-surface bas-avoid-break" aria-label="Cover">';
    html += '<div class="bas-cover-geometric" aria-hidden="true"></div>';
    html += '<p class="bas-cover-confidential">Draft for validation · Internal owner/advisor review</p>';
    html += '<div class="bas-cover-block">';
    html += '<p class="bas-cover-doc-type">DEALALITY OPERATOR ALIGNMENT SNAPSHOT</p>';
    html += '<h1 class="bas-cover-title">' + esc(displayVal(ctx.dealName)) + "</h1>";
    html += '<p class="bas-cover-location">' + esc(oasLocationLine(ctx)) + "</p>";
    html += '<div class="bas-cover-accent-line" aria-hidden="true"></div>';
    html += '<p class="bas-cover-sub">' + esc(oasMetaLine(ctx)) + "</p>";
    html += '<p class="bas-cover-date">Generated ' + esc(formatDate(generatedAt)) + " · current deal inputs</p>";
    html += "</div>";
    html += '<p class="bas-cover-disclaimer">' + esc(OAS_COVER_NOTE) + "</p>";
    html +=
      '<div class="bas-cover-hero"><div class="bas-cover-logo-block"><img src="' +
      esc(DEALALITY_LOGO_URL) +
      '" alt="Dealality" class="bas-cover-logo-img" width="140" height="auto"></div></div>';
    html += "</section>";
    return html;
  }



  function renderDocumentBody(data, options) {
    options = options || {};
    var html = '<div class="oas-scroll-document bas-book-page-inner bas-content-page">';
    html += renderSummaryGrid(data);

    html += '<section class="bas-section bas-section--brief oas-section oas-section--profiles bas-section--keep">';
    html += '<h2 class="bas-section-title">Operator Profiles for Review</h2>';
    var profiles = data.profilesForReview || [];
    if (!profiles.length) {
      html += '<p class="bas-muted">No operator profile categories are available for this deal. Check deal inputs and try again.</p>';
    } else {
      html += '<div class="oas-profile-grid oas-card-grid--two-col">';
      profiles.forEach(function (p) {
        html += renderProfileCard(p);
      });
      html += "</div>";
    }
    html += "</section>";

    html += renderCompaniesForConsiderationSection(resolveCompaniesPayload(data), options);

    var gaps = data.dataGaps || [];
    var actions = normalizeListItems(data.suggestedWorkflowActions, true);
    if (gaps.length || actions.length) {
      html += '<section class="bas-section bas-section--brief bas-section--keep oas-section oas-section--closing">';
      html += '<h2 class="bas-section-title">Key Follow-Ups</h2>';
      html += '<div class="oas-closing-grid">';
      if (gaps.length) {
        html += renderKeyItemsPanel("Key Data Gaps", gaps, OAS_PRINT_GAP_LIMIT);
      }
      if (actions.length) {
        html += renderKeyItemsPanel("Suggested Workflow Actions", actions, OAS_PRINT_ACTION_LIMIT);
      }
      html += "</div>";
      if (gaps.length > OAS_PRINT_GAP_LIMIT || actions.length > OAS_PRINT_ACTION_LIMIT) {
        html +=
          '<p class="oas-closing-more bas-muted">Additional data gaps and workflow actions may be available in the platform view.</p>';
      }
      html += "</section>";
    }



    html +=

      '<footer class="bas-output-note bas-output-note--brief oas-footer-disclaimer"><p>' +

      esc(buildOutputNote(data)) +

      "</p></footer>";

    html += "</div>";

    return html;

  }



  function buildFullPageNav(options) {
    if (!options.fullPage || !options.backHref) return "";
    return (
      '<nav class="snapshot-page-nav bas-no-print" aria-label="Page">' +
      '<a class="snapshot-page-back" href="' +
      esc(options.backHref) +
      '">' +
      esc(options.backLabel || "\u2190 Back to My Deals") +
      "</a></nav>"
    );
  }



  function buildHtml(data, options) {
    options = options || {};
    var snapClass = "operator-alignment-snapshot brand-alignment-snapshot";
    if (options.embed) snapClass += " bas--embed";
    if (options.fullPage) snapClass += " bas--full-page";

    var html = '<div class="' + snapClass + '">';
    html += buildFullPageNav(options);
    html += '<div class="bas-toolbar bas-no-print"><div class="bas-toolbar-actions">';
    html +=
      '<span class="bas-print-tip bas-no-print">Turn off <strong>Headers and footers</strong> and enable <strong>Background graphics</strong> in the print dialog.</span>';
    html += '<div class="bas-toolbar-buttons bas-no-print">';
    html +=
      '<button type="button" class="bas-btn bas-btn-primary bas-toolbar-print" data-bas-print>Print / Save as PDF</button>';
    html += "</div></div></div>";

    html += '<div class="bas-book-shell"><article class="bas-document bas-book-document">';
    html += '<div class="bas-book-viewport" data-bas-book-viewport tabindex="0"><div class="bas-book-stage">';
    html += wrapBookPage(0, renderCover(data, options), true);
    html += wrapBookPage(1, renderPage1OperatorNarrative(data), false);
    html += wrapBookPage(2, renderPage2OperatorDetail(data), false);
    html += "</div>";
    html +=
      '<button type="button" class="bas-turn-btn bas-turn-prev bas-no-print" data-bas-turn-prev aria-label="Previous page" disabled>‹</button>';
    html +=
      '<button type="button" class="bas-turn-btn bas-turn-next bas-no-print" data-bas-turn-next aria-label="Next page">›</button>';
    html += '<span class="bas-page-indicator bas-no-print" data-bas-page-indicator>1 of 3</span>';
    html += "</div></article></div></div>";
    return html;
  }



  function getSnapshotRoot(container) {

    if (!container) return null;

    if (container.classList && container.classList.contains("operator-alignment-snapshot")) {

      return container;

    }

    return container.querySelector(".operator-alignment-snapshot");

  }



  /**
   * Print-only cover: minimal DOM (no geometric layer / absolute BAS footer).
   * Disclaimer + logo share one footer row inside a max-height sheet.
   */
  function rebuildOasPrintCover(cover) {
    if (!cover || cover.getAttribute("data-oas-print-cover") === "1") return;
    var tag = cover.querySelector(".bas-cover-confidential");
    var block = cover.querySelector(".bas-cover-block");
    var disclaimer = cover.querySelector(".bas-cover-disclaimer");
    var img = cover.querySelector(".bas-cover-logo-img");
    if (!block) return;

    cover.setAttribute("data-oas-print-cover", "1");
    cover.className = "bas-cover-page oas-print-cover-sheet";
    cover.setAttribute("aria-label", "Cover");

    var html = "";
    html +=
      '<p class="bas-cover-confidential">' +
      esc(tag ? tag.textContent : "") +
      "</p>";
    html += '<div class="bas-cover-block">' + block.innerHTML + "</div>";
    html += '<div class="oas-print-cover-foot">';
    html +=
      '<p class="bas-cover-disclaimer">' +
      esc(disclaimer ? disclaimer.textContent : "") +
      "</p>";
    html +=
      '<div class="bas-cover-hero"><div class="bas-cover-logo-block"><img src="' +
      esc(img && img.getAttribute("src") ? img.getAttribute("src") : DEALALITY_LOGO_URL) +
      '" alt="Dealality" class="bas-cover-logo-img" width="120" height="auto"></div></div>';
    html += "</div>";
    cover.innerHTML = html;
  }

  /** Shrink cover to fit one printable sheet if layout overflows (Chrome print). */
  function fitOasPrintCoverToOnePage(printHost) {
    var cover = printHost.querySelector(".oas-print-cover-sheet");
    if (!cover) return;
    cover.classList.remove("oas-print-cover-sheet--scaled");
    cover.style.transform = "";
    cover.style.width = "";
    cover.style.height = "";

    var maxMm = OAS_PRINT_COVER_HEIGHT_MM;
    var test = document.createElement("div");
    test.style.cssText =
      "position:absolute;visibility:hidden;height:" + maxMm + "mm;width:1px;left:-9999px;";
    document.body.appendChild(test);
    var maxPx = test.offsetHeight || 1030;
    document.body.removeChild(test);

    cover.style.height = maxMm + "mm";
    cover.style.minHeight = maxMm + "mm";
    cover.style.maxHeight = maxMm + "mm";
    cover.style.overflow = "hidden";
    var guard = 0;
    while (cover.scrollHeight > maxPx + 2 && guard < 12) {
      guard += 1;
      var scale = (maxPx - 2) / cover.scrollHeight;
      if (scale >= 0.99) break;
      cover.classList.add("oas-print-cover-sheet--scaled");
      cover.style.transform = "scale(" + scale + ")";
      cover.style.transformOrigin = "top left";
      cover.style.width = (100 / scale).toFixed(3) + "%";
      cover.style.height = maxMm + "mm";
    }
  }

  /** Print: flat document — cover sheet, then narrative/detail (no flip-book viewport). */
  function flattenOasBookForPrint(root) {
    var stage = root.querySelector(".bas-book-stage");
    if (!stage) return;

    var pages = Array.prototype.slice.call(stage.querySelectorAll(":scope > .bas-book-page"));
    if (pages[0]) {
      var coverEl = pages[0].querySelector(".bas-cover-page");
      if (coverEl) {
        rebuildOasPrintCover(coverEl);
        stage.insertBefore(coverEl, pages[0]);
      }
      pages[0].remove();
    }

    Array.prototype.slice
      .call(stage.querySelectorAll(":scope > .bas-book-page"))
      .forEach(function (page) {
        var inner = page.querySelector(".bas-book-page-inner");
        if (inner) stage.appendChild(inner);
        page.remove();
      });

    var cover = stage.querySelector(".oas-print-cover-sheet");
    var inners = Array.prototype.slice.call(stage.querySelectorAll(":scope > .bas-book-page-inner"));
    var doc = document.createElement("div");
    doc.className = "oas-print-document";
    if (cover) doc.appendChild(cover);
    inners.forEach(function (inner) {
      doc.appendChild(inner);
    });

    var viewport = root.querySelector(".bas-book-viewport");
    if (viewport) {
      viewport.innerHTML = "";
      viewport.classList.add("oas-print-viewport-flat");
      viewport.appendChild(doc);
    }

    root.classList.add("oas-print-flattened");
  }

  function runPrintWhenReady(printHost, printFn) {
    var imgs = printHost.querySelectorAll("img");
    var pending = 0;
    var doneCalled = false;
    function done() {
      if (doneCalled) return;
      doneCalled = true;
      printFn();
    }
    imgs.forEach(function (img) {
      if (img.complete && img.naturalWidth > 0) return;
      pending += 1;
      img.addEventListener("load", function () {
        pending -= 1;
        if (pending <= 0) done();
      }, { once: true });
      img.addEventListener("error", function () {
        pending -= 1;
        if (pending <= 0) done();
      }, { once: true });
    });
    if (pending === 0) done();
    else window.setTimeout(done, 3000);
  }

  function printSnapshot(root) {
    var snapshot = getSnapshotRoot(root);
    if (!snapshot) {
      window.print();
      return;
    }
    var printHost = document.getElementById("bas-print-host");
    if (!printHost) {
      printHost = document.createElement("div");
      printHost.id = "bas-print-host";
      printHost.setAttribute("aria-hidden", "true");
      document.body.appendChild(printHost);
    }
    var clone = snapshot.cloneNode(true);
    clone.classList.add("bas-printing");
    clone.classList.remove("bas--embed");
    flattenOasBookForPrint(clone);
    clone.querySelectorAll(".bas-book-page").forEach(function (page) {
      page.classList.add("active");
      page.setAttribute("aria-hidden", "false");
    });
    printHost.innerHTML = "";
    printHost.appendChild(clone);
    document.body.classList.add("bas-print-active");
    function cleanup() {
      document.body.classList.remove("bas-print-active");
      printHost.innerHTML = "";
    }
    function onAfterPrint() {
      cleanup();
      window.removeEventListener("afterprint", onAfterPrint);
    }
    window.addEventListener("afterprint", onAfterPrint);
    window.setTimeout(function () {
      if (document.body.classList.contains("bas-print-active")) cleanup();
    }, 3000);
    runPrintWhenReady(printHost, function () {
      fitOasPrintCoverToOnePage(printHost);
      window.requestAnimationFrame(function () {
        window.setTimeout(function () {
          window.print();
        }, 80);
      });
    });
  }



  function bindCompaniesExpand(root) {
    if (!root) return;
    var btn = root.querySelector("[data-oas-show-all-companies]");
    var grid = root.querySelector(".oas-company-grid");
    if (!btn || !grid || btn._oasExpandBound) return;
    btn._oasExpandBound = true;
    btn.addEventListener("click", function () {
      grid.classList.add("oas-companies--expanded");
      btn.setAttribute("hidden", "hidden");
    });
  }

  function bindPrint(root) {
    if (!root) return;
    var btn = root.querySelector("[data-bas-print]");
    if (btn && !btn._basPrintBound) {
      btn._basPrintBound = true;
      btn.addEventListener("click", function () {
        printSnapshot(root);
      });
    }
    bindCompaniesExpand(root);
  }



  var BAND_SORT_RANK = {
    "Strong Alignment Signals": 1,
    "Moderate Alignment Signals": 2,
    "Conditional Alignment Signals": 3,
    "Limited Alignment Signals": 4,
    "Insufficient Data": 5,
  };

  function bandSortRank(band) {
    var b = String(band || "").trim();
    if (BAND_SORT_RANK[b] != null) return BAND_SORT_RANK[b];
    var lower = b.toLowerCase();
    if (lower.indexOf("strong") >= 0) return 1;
    if (lower.indexOf("moderate") >= 0) return 2;
    if (lower.indexOf("conditional") >= 0) return 3;
    if (lower.indexOf("limited") >= 0) return 4;
    if (lower.indexOf("insufficient") >= 0) return 5;
    return 99;
  }

  function sortProfilesForPreview(profiles) {
    return (profiles || []).slice().sort(function (a, b) {
      var bandDiff = bandSortRank(a.alignmentBand) - bandSortRank(b.alignmentBand);
      if (bandDiff !== 0) return bandDiff;
      return (a.sortPriority != null ? a.sortPriority : 100) - (b.sortPriority != null ? b.sortPriority : 100);
    });
  }

  function pickTopProfilesForPreview(profiles, limit) {
    return sortProfilesForPreview(profiles).slice(0, limit == null ? 3 : limit);
  }

  function renderPreviewChipList(labels) {
    if (!labels || !labels.length) return "";
    var html = '<ul class="oas-preview-chip-list" role="list">';
    labels.forEach(function (label) {
      html += '<li class="oas-preview-chip">' + esc(label) + "</li>";
    });
    html += "</ul>";
    return html;
  }

  var OAS_PRINT_COMPANY_LIMIT = 5;
  var OAS_MY_DEALS_COMPANY_LIMIT = 3;
  var OAS_MARKET_CHIP_MAX = 5;
  var OAS_PRINT_GAP_LIMIT = 5;
  var OAS_PRINT_ACTION_LIMIT = 5;
  var OAS_PROFILE_BULLET_MAX = 2;
  var OAS_COMPANY_SIGNAL_MAX = 2;
  var OAS_COMPANY_REVIEW_MAX = 1;

  var OAS_COMPANY_SECTION_NOTE =
    "Company-level alignment uses available Operator Setup profile data. These results are informational and do not indicate endorsement, availability, approval, or commercial terms.";

  function stripTechnicalScoringTail(text) {
    return String(text || "")
      .replace(/\s+before advancing\s*[—–-]\s*.+$/i, ".")
      .replace(/\s+[—–-]\s*Measures\s+.+$/i, ".")
      .replace(/\s+[—–-]\s*Compares\s+.+$/i, ".")
      .replace(/: data may be needed on both sides\s*[—–-]\s*/i, ": ")
      .replace(/\s{2,}/g, " ")
      .trim();
  }

  function humanizeCompanyAlignmentSignal(text) {
    var s = stripTechnicalScoringTail(String(text || "").trim());
    if (!s) return "";
    var lower = s.toLowerCase();
    if (/^fee\s*\/\s*commercial$/i.test(lower) || /^fee\s*\/\s*commercial\b/i.test(lower)) {
      return "Fee / commercial assumptions may need validation.";
    }
    if (/brand\s*\/\s*portfolio/i.test(lower)) {
      return "Brand or portfolio relevance may need confirmation.";
    }
    if (/service offerings|service platform/i.test(lower)) {
      return "Service platform overlap should be confirmed.";
    }
    if (/deal structure|assignment/i.test(lower)) {
      return "Deal structure alignment may need validation.";
    }
    if (/geography\s*&\s*markets|geography|markets/i.test(lower)) return "Market overlap indicated";
    if (/chain scale/i.test(lower)) return "Chain-scale overlap indicated";
    if (/asset\s*\/\s*project|project.stage|stage fit/i.test(lower)) {
      return "Project-stage fit may be relevant based on current inputs";
    }
    if (/deal breakers|offsetting/i.test(lower)) return "Offsetting factors may apply";
    if (/owner must-haves|must.haves/i.test(lower)) return "Owner requirements need cross-check";
    s = s
      .replace(/^Potential alignment on /i, "")
      .replace(/ based on available inputs\.?$/i, "")
      .replace(/^Conditional alignment signal on /i, "")
      .replace(/ — validation may be needed\.?$/i, "")
      .replace(/^Limited alignment signal on /i, "")
      .replace(/ with current inputs\.?$/i, "")
      .trim();
    if (/^fee\s*\/\s*commercial$/i.test(s.toLowerCase())) {
      return "Fee / commercial assumptions may need validation.";
    }
    if (/before advancing|measures overlap|compares owner/i.test(s)) return "";
    return s.charAt(0).toUpperCase() + s.slice(1);
  }

  function humanizeCompanyReviewConsideration(text) {
    var s = stripTechnicalScoringTail(String(text || "").trim());
    if (!s) return "";
    var lower = s.toLowerCase();
    if (/fee\s*\/\s*commercial/i.test(lower)) {
      return "Fee / commercial assumptions may need validation.";
    }
    if (/brand\s*\/\s*portfolio/i.test(lower)) {
      return "Brand or portfolio relevance may need confirmation.";
    }
    if (/service offerings|service platform/i.test(lower)) {
      return "Required services should be validated against the operator's documented platform.";
    }
    if (/deal structure|assignment|management structure/i.test(lower)) {
      return "Management structure and assignment model should be confirmed.";
    }
    if (/geography|markets/i.test(lower)) return "Active market coverage should be confirmed";
    if (/chain scale/i.test(lower)) return "Chain-scale fit should be confirmed";
    if (/before advancing|measures overlap|compares owner/i.test(lower)) return "";
    return s.replace(/^Review /i, "").trim();
  }

  function mergeUniqueBullets(primary, fallback, minCount, maxCount) {
    var out = [];
    var seen = {};
    function add(item) {
      var line = String(item || "").trim();
      if (!line || seen[line]) return;
      seen[line] = true;
      out.push(line);
    }
    (primary || []).forEach(add);
    (fallback || []).forEach(function (item) {
      if (out.length >= (maxCount == null ? 99 : maxCount)) return;
      add(item);
    });
    minCount = minCount == null ? 0 : minCount;
    maxCount = maxCount == null ? 99 : maxCount;
    while (out.length < minCount && fallback && fallback.length) {
      fallback.forEach(add);
      if (out.length >= minCount) break;
    }
    return out.slice(0, maxCount);
  }

  function extractAlignmentThemePhrase(company) {
    var themes = [];
    var blob = []
      .concat(company.alignmentSignals || [])
      .concat(company.reviewConsiderations || [])
      .join(" ")
      .toLowerCase();
    if (/geograph|market/i.test(blob)) themes.push("market overlap");
    if (/chain/i.test(blob)) themes.push("chain-scale overlap");
    if (/project|stage|asset/i.test(blob)) themes.push("project-stage fit");
    if (/service/i.test(blob)) themes.push("service platform fit");
    if (/deal structure|assignment/i.test(blob)) themes.push("deal structure alignment");
    if (/brand|portfolio/i.test(blob)) themes.push("brand or portfolio relevance");
    if (!themes.length) {
      return "market overlap, chain-scale overlap, and project-stage fit";
    }
    if (themes.length === 1) return themes[0];
    if (themes.length === 2) return themes[0] + " and " + themes[1];
    return themes.slice(0, themes.length - 1).join(", ") + ", and " + themes[themes.length - 1];
  }

  function buildCompanyOwnerRationale(company) {
    var c = company || {};
    if (c.ownerFacingRationale) return String(c.ownerFacingRationale);
    var display = resolveCompanyDisplayName(c);
    var band = c.alignmentBand || "alignment signals";
    return (
      display.name +
      " currently shows " +
      band +
      " based on available Operator Setup data. Stronger alignment signals appear around " +
      extractAlignmentThemePhrase(c) +
      ". Before outreach, validate the highest-priority open items for this operator profile."
    );
  }

  function buildCompanyWhatSupports(company) {
    var c = company || {};
    if (c.whatSupportsReview && c.whatSupportsReview.length) {
      return mergeUniqueBullets(c.whatSupportsReview, [], 2, 4);
    }
    var fromSignals = mapHumanizedList(c.alignmentSignals, humanizeCompanyAlignmentSignal, 6);
    var defaults =
      fromSignals.length >= 2
        ? []
        : [
            "Operator profile has sufficient structured data for company-level review.",
          ];
    return mergeUniqueBullets(fromSignals, defaults, 2, 6);
  }

  function buildCompanyWhatNeedsValidation(company) {
    var c = company || {};
    if (c.whatNeedsValidation && c.whatNeedsValidation.length) {
      return mergeUniqueBullets(c.whatNeedsValidation, [], 2, 6);
    }
    var fromReview = mapHumanizedList(c.reviewConsiderations, humanizeCompanyReviewConsideration, 6);
    var defaults = fromReview.length >= 2 ? [] : ["Confirm service delivery scope and market coverage before external sharing."];
    return mergeUniqueBullets(fromReview, defaults, 2, 4);
  }

  function buildCompanyWhatCouldWeaken(company) {
    var c = company || {};
    if (c.whatCouldWeakenAlignment && c.whatCouldWeakenAlignment.length) {
      return mergeUniqueBullets(c.whatCouldWeakenAlignment, [], 2, 5);
    }
    var fromGaps = mapHumanizedList(c.dataGaps, function (g) {
      var s = String(g || "").trim();
      if (!s) return "";
      if (/market/i.test(s)) {
        return "Alignment may weaken if the operator lacks active operations in the target market.";
      }
      if (/management|structure/i.test(s)) {
        return "Alignment may weaken if the operator's preferred management structure does not match the deal path.";
      }
      if (/service/i.test(s)) {
        return "Alignment may weaken if service platform depth is not sufficient for owner must-haves.";
      }
      if (/brand|chain/i.test(s)) {
        return "Alignment may weaken if brand relationship or chain-scale experience is not confirmed.";
      }
      return "Alignment may weaken if " + s.charAt(0).toLowerCase() + s.slice(1) + " is not confirmed.";
    }, 4);
    return mergeUniqueBullets(
      fromGaps,
      [
        "Alignment may weaken if the operator lacks active operations in the target market.",
        "Alignment may weaken if the operator's preferred management structure does not match the deal path.",
        "Alignment may weaken if service platform depth is not sufficient for owner must-haves.",
        "Alignment may weaken if brand relationship or chain-scale experience is not confirmed.",
      ],
      3,
      5
    );
  }

  function buildCompanyOwnerQuestions(company) {
    var c = company || {};
    if (c.ownerQuestions && c.ownerQuestions.length) {
      return c.ownerQuestions.slice(0, 5);
    }
    return [
      "Does this operator's market coverage reflect active operations for this deal?",
      "Are management structures and service scope aligned with owner must-haves?",
      "Does the operator have experience with similar chain scale, service model, or brand path?",
      "What reporting cadence and owner governance structure would apply?",
    ].slice(0, 4);
  }

  function buildCompanyFactorsReviewed(company) {
    var c = company || {};
    if (c.factorsReviewed && c.factorsReviewed.length) {
      return c.factorsReviewed.slice(0, 8);
    }
    var blob = []
      .concat(company.alignmentSignals || [])
      .concat(company.reviewConsiderations || [])
      .join(" ")
      .toLowerCase();
    var catalog = [
      { test: /geograph|market/i, label: "Geography / market alignment" },
      { test: /chain/i, label: "Chain-scale alignment" },
      { test: /project|stage|asset/i, label: "Project / stage alignment" },
      { test: /deal structure|assignment/i, label: "Deal structure alignment" },
      { test: /service/i, label: "Service platform alignment" },
      { test: /brand|portfolio/i, label: "Brand / portfolio relevance" },
      { test: /fee|commercial/i, label: "Fee / commercial assumptions" },
    ];
    var out = [];
    catalog.forEach(function (entry) {
      if (entry.test.test(blob) && out.indexOf(entry.label) < 0) out.push(entry.label);
    });
    if (!out.length) {
      return catalog.map(function (e) {
        return e.label;
      });
    }
    return out;
  }

  function resolveCompanyDisplayName(company) {
    var c = company || {};
    var name = String(c.operatorName || c.companyName || c.company_name || "").trim();
    if (!name || name === "—" || name === "Not provided") {
      return { name: "Operating Company", missing: true };
    }
    return { name: name, missing: false };
  }

  function mapHumanizedList(items, mapper, max) {
    var out = [];
    (items || []).forEach(function (item) {
      var line = mapper(item);
      if (line && out.indexOf(line) < 0) out.push(line);
    });
    return out.slice(0, max == null ? 99 : max);
  }

  function formatMarketsCompact(markets, maxVisible) {
    var list = (markets || []).filter(Boolean).map(function (m) {
      return String(m).trim();
    });
    maxVisible = maxVisible == null ? OAS_MARKET_CHIP_MAX : maxVisible;
    if (!list.length) return { chips: [], moreCount: 0, fullTitle: "" };
    var shown = list.slice(0, maxVisible);
    var more = Math.max(0, list.length - maxVisible);
    return {
      chips: shown,
      moreCount: more,
      fullTitle: list.join(", "),
    };
  }

  function renderMarketChips(markets) {
    var m = formatMarketsCompact(markets, OAS_MARKET_CHIP_MAX);
    if (!m.chips.length) return "";
    var html = '<div class="oas-market-chips"';
    if (m.fullTitle) html += ' title="' + esc(m.fullTitle) + '"';
    html += ">";
    m.chips.forEach(function (chip) {
      html += '<span class="oas-market-chip">' + esc(chip) + "</span>";
    });
    if (m.moreCount > 0) {
      html += '<span class="oas-market-chip oas-market-chip--more">+' + esc(String(m.moreCount)) + " more</span>";
    }
    html += "</div>";
    return html;
  }

  function formatInformationalScore(score) {
    if (score == null || score === "") return "";
    var n = Number(score);
    if (Number.isNaN(n)) return "";
    return String(Math.round(n));
  }

  function collectCommonDataGaps(companies) {
    var counts = {};
    var total = (companies || []).length;
    if (!total) return [];
    (companies || []).forEach(function (c) {
      (c.dataGaps || []).forEach(function (g) {
        var key = String(g || "").trim();
        if (!key) return;
        counts[key] = (counts[key] || 0) + 1;
      });
    });
    var threshold = Math.max(2, Math.ceil(total * 0.5));
    return Object.keys(counts).filter(function (g) {
      return counts[g] >= threshold;
    });
  }

  function filterUniqueGaps(gaps, commonGaps, max) {
    var common = commonGaps || [];
    var unique = [];
    (gaps || []).forEach(function (g) {
      var key = String(g || "").trim();
      if (!key || common.indexOf(key) >= 0) return;
      if (unique.indexOf(key) < 0) unique.push(key);
    });
    if (!unique.length) return [];
    return unique.slice(0, max == null ? 1 : max);
  }

  function renderCompactBullets(title, items, max) {
    if (!items || !items.length) return "";
    var html =
      '<div class="oas-compact-block"><h4 class="oas-compact-block__title">' + esc(title) + "</h4><ul class=\"oas-compact-list\">";
    items.slice(0, max).forEach(function (item) {
      html += "<li>" + esc(item) + "</li>";
    });
    html += "</ul></div>";
    return html;
  }

  function renderCardColumns(leftBlocks, rightBlocks) {
    var html = '<div class="oas-card-columns">';
    html += '<div class="oas-card-columns__col">' + (leftBlocks || "") + "</div>";
    html += '<div class="oas-card-columns__col">' + (rightBlocks || "") + "</div>";
    html += "</div>";
    return html;
  }

  function renderKeyItemsPanel(title, items, max) {
    var list = (items || []).slice(0, max);
    if (!list.length) return "";
    var html = '<div class="oas-key-panel bas-avoid-break">';
    html += '<h3 class="oas-key-panel__title">' + esc(title) + "</h3>";
    html += '<ul class="oas-compact-list">';
    list.forEach(function (item) {
      html += "<li>" + esc(item) + "</li>";
    });
    html += "</ul></div>";
    return html;
  }

  function renderDealContextCompact(ctx) {
    ctx = ctx || {};
    var ind = ctx.indicators || {};
    var indicatorParts = [];
    if (ind.conversionOrReflag) indicatorParts.push("Conversion / reflag");
    if (ind.repositioning) indicatorParts.push("Repositioning");
    if (ind.newBuild) indicatorParts.push("New build");
    if (ind.preopeningOrReopening) indicatorParts.push("Pre-opening / reopening");
    if (ind.operatorInScope) indicatorParts.push("Operator in scope");
    var locationParts = [];
    if (ctx.cityOrMarket && ctx.cityOrMarket !== NOT_PROVIDED) locationParts.push(ctx.cityOrMarket);
    if (ctx.country && ctx.country !== NOT_PROVIDED) locationParts.push(ctx.country);
    var facts = [
      ["Location", locationParts.length ? locationParts.join(" · ") : NOT_PROVIDED],
      ["Room count", ctx.roomCount != null ? String(ctx.roomCount) : NOT_PROVIDED],
      ["Project type", displayVal(ctx.projectType)],
      ["Desired operating model", displayVal(ctx.desiredOperatingModel)],
      ["Chain scale", displayVal(ctx.chainScale)],
      ["Key indicators", indicatorParts.length ? indicatorParts.join(" · ") : NOT_PROVIDED],
    ];
    var html = '<ul class="oas-summary-facts">';
    facts.forEach(function (row) {
      html +=
        '<li class="oas-summary-facts__row"><span class="oas-summary-facts__label">' +
        esc(row[0]) +
        '</span><span class="oas-summary-facts__value">' +
        esc(row[1]) +
        "</span></li>";
    });
    html += "</ul>";
    return html;
  }

  function renderCompletenessBadge(company) {
    var c = company || {};
    var label = c.dataCompleteness || c.sourceStatus || "";
    if (!label) return "";
    var cls = "oas-meta-badge";
    var lower = String(label).toLowerCase();
    if (lower.indexOf("sufficient") >= 0 || lower === "live") cls += " oas-meta-badge--ok";
    else if (lower.indexOf("incomplete") >= 0) cls += " oas-meta-badge--warn";
    return '<span class="' + cls + '">' + esc(String(label)) + "</span>";
  }

  function pickCompaniesForDisplay(companies, limit) {
    return sortCompaniesForDisplay(companies).slice(0, limit == null ? 99 : limit);
  }

  function sortCompaniesForDisplay(companies) {
    return (companies || []).slice().sort(function (a, b) {
      var bandDiff = bandSortRank(a.alignmentBand) - bandSortRank(b.alignmentBand);
      if (bandDiff !== 0) return bandDiff;
      var sa = a.alignmentScoreOptional != null ? a.alignmentScoreOptional : -1;
      var sb = b.alignmentScoreOptional != null ? b.alignmentScoreOptional : -1;
      return sb - sa;
    });
  }

  function renderCompanyCardFull(company, cardOptions) {
    cardOptions = cardOptions || {};
    var c = company || {};
    var display = resolveCompanyDisplayName(c);
    var extraClass = cardOptions.isExtra ? " oas-company-card--limited-extra" : "";
    var html =
      '<article class="oas-company-card bas-brand-card bas-avoid-break bas-section--keep' +
      extraClass +
      '" data-oas-company-id="' +
      esc(c.operatorId || "") +
      '">';
    html += '<header class="oas-card-header oas-company-card__header">';
    html += '<div class="oas-card-header__title-row">';
    html +=
      '<h3 class="oas-card-title oas-company-card__title" data-oas-company-name>' +
      esc(display.name) +
      "</h3>";
    html +=
      '<span class="' +
      bandClass(c.alignmentBand) +
      ' oas-card-band">' +
      esc(c.alignmentBand || "—") +
      "</span>";
    html += "</div>";
    html += '<div class="oas-company-card__meta-row">';
    html += renderCompletenessBadge(c);
    var scoreStr = formatInformationalScore(c.alignmentScoreOptional);
    if (scoreStr) {
      html += '<span class="oas-company-card__score">Informational score: ' + esc(scoreStr) + "</span>";
    }
    html += "</div>";
    if (c.parentCompany) {
      html += '<p class="oas-company-card__sub">' + esc(c.parentCompany) + "</p>";
    }
    html += "</header>";

    html += '<div class="oas-company-card__body">';
    if (display.missing) {
      html += '<p class="oas-company-card__gap-note">Company name missing from Operator Setup profile.</p>';
    }
    if (c.countriesMarkets && c.countriesMarkets.length) {
      html += '<div class="oas-company-card__markets">' + renderMarketChips(c.countriesMarkets) + "</div>";
    }
    html += renderCompactBullets(
      "Alignment Signals",
      mapHumanizedList(c.alignmentSignals, humanizeCompanyAlignmentSignal, OAS_COMPANY_SIGNAL_MAX),
      OAS_COMPANY_SIGNAL_MAX
    );
    html += renderCompactBullets(
      "Review Considerations",
      mapHumanizedList(c.reviewConsiderations, humanizeCompanyReviewConsideration, OAS_COMPANY_REVIEW_MAX),
      OAS_COMPANY_REVIEW_MAX
    );
    var uniqueGaps = filterUniqueGaps(c.dataGaps, cardOptions.commonGaps, 1);
    if (uniqueGaps.length) {
      html += renderCompactBullets("Data Gaps", uniqueGaps, 1);
    }
    html += "</div></article>";
    return html;
  }

  function defaultCompaniesPayload(gatingReason) {
    return {
      mode: "companies",
      sectionName: "Operating Companies for Consideration",
      companiesAvailable: false,
      gatingReason: gatingReason || COMPANIES_GATED_PRIMARY,
      companiesForConsideration: [],
      dataCompletenessSummary: {},
    };
  }

  function resolveCompaniesPayload(data) {
    var p = data && data.companiesSnapshot;
    if (p && p.mode === "companies") return p;
    return defaultCompaniesPayload(
      (p && p.gatingReason) ||
        "Company-level alignment could not be loaded. Profile-level alignment remains available above."
    );
  }

  function buildOutputNote(data) {
    var companies = resolveCompaniesPayload(data || {});
    if (companies.companiesAvailable && (companies.companiesForConsideration || []).length) {
      return OUTPUT_NOTE_WITH_COMPANIES;
    }
    return OUTPUT_NOTE_PROFILE_ONLY;
  }

  function shouldLogOasCompaniesDebug() {
    try {
      if (!global.location || !global.location.hostname) return false;
      if (/localhost|127\.0\.0\.1/i.test(global.location.hostname)) return true;
      return (global.location.search || "").indexOf("oasDebug=1") >= 0;
    } catch (_) {
      return false;
    }
  }

  function logCompaniesQaState(data, options) {
    if (!shouldLogOasCompaniesDebug()) return;
    var c = resolveCompaniesPayload(data || {});
    var summary = c.dataCompletenessSummary || {};
    try {
      console.info("[OAS companies QA]", {
        dealId: options && options.dealId,
        companiesAvailable: c.companiesAvailable,
        companyCards: (c.companiesForConsideration || []).length,
        scorableOperators: summary.scorableOperators,
        activeOperatorRecords: summary.activeOperatorRecords,
        gatingReason: c.gatingReason || null,
      });
    } catch (_) {}
  }

  function companiesFetchUserMessage(companiesPack) {
    if (!companiesPack) {
      return "Company-level operator alignment could not be loaded. Please refresh and try again.";
    }
    var body = companiesPack.body || {};
    if (companiesPack.networkError || companiesPack.status === 0) {
      return "Company-level operator alignment could not be loaded. Please refresh and try again.";
    }
    if (companiesPack.ok && body.success && body.mode === "companies") {
      if (body.companiesAvailable === false && body.gatingReason) {
        return body.gatingReason;
      }
      return null;
    }
    if (body.mode === "companies" && body.gatingReason && body.companiesAvailable === false) {
      return body.gatingReason;
    }
    var status = companiesPack.status;
    if (status === 401 || status === 403) {
      return "Company-level operator alignment requires a signed-in session with access to this deal.";
    }
    if (status === 404) {
      return "Company-level operator alignment endpoint was not found. Restart the server or confirm the route is deployed.";
    }
    if (status >= 500) {
      return "Company-level operator alignment could not be loaded due to a server error.";
    }
    if (body.gatingReason && body.companiesAvailable === false) {
      return body.gatingReason;
    }
    return COMPANIES_GATED_PRIMARY;
  }

  function fetchCompaniesApiPack(fetchFn, dealId) {
    var url =
      "/api/operator-alignment-snapshot/" + encodeURIComponent(dealId) + "/companies";
    return fetchFn(url, { method: "GET" })
      .then(function (r) {
        return r.json().then(function (body) {
          return { ok: r.ok, status: r.status, body: body };
        });
      })
      .catch(function () {
        return { ok: false, status: 0, body: null, networkError: true };
      });
  }

  function copyExecutiveSummaryToData(data, body) {
    if (!body) return;
    if (body.operatorAlignmentSummaryParagraphs && body.operatorAlignmentSummaryParagraphs.length) {
      data.operatorAlignmentSummaryParagraphs = body.operatorAlignmentSummaryParagraphs;
    }
    if (body.operatorAlignmentExecutiveSummary) {
      data.operatorAlignmentExecutiveSummary = body.operatorAlignmentExecutiveSummary;
    }
  }

  function attachCompaniesSnapshot(data, companiesPack) {
    if (!data) return data;
    if (companiesPack && companiesPack.ok && companiesPack.body && companiesPack.body.success) {
      data.companiesSnapshot = companiesPack.body;
      copyExecutiveSummaryToData(data, companiesPack.body);
      return data;
    }
    if (companiesPack && companiesPack.body && companiesPack.body.mode === "companies") {
      data.companiesSnapshot = companiesPack.body;
      copyExecutiveSummaryToData(data, companiesPack.body);
      return data;
    }
    var userMsg = companiesFetchUserMessage(companiesPack);
    data.companiesSnapshot = defaultCompaniesPayload(
      userMsg || "Company-level operator alignment could not be loaded. Profile-level alignment remains available above."
    );
    return data;
  }

  function renderCompaniesGatedBlock(payload) {
    var html = '<div class="oas-companies-gated">';
    html +=
      '<p class="bas-muted oas-companies-gated__primary">' +
      esc(payload.gatingReason || COMPANIES_GATED_PRIMARY) +
      "</p>";
    html += '<p class="bas-muted oas-companies-gated__support">' + esc(COMPANIES_GATED_SUPPORT) + "</p>";
    if (payload.dataCompletenessSummary && payload.dataCompletenessSummary.activeOperatorRecords != null) {
      html +=
        '<p class="bas-muted oas-companies-gated__meta">Active Operator Setup records: ' +
        esc(String(payload.dataCompletenessSummary.activeOperatorRecords)) +
        "; scorable for company-level alignment: " +
        esc(String(payload.dataCompletenessSummary.scorableOperators != null ? payload.dataCompletenessSummary.scorableOperators : "—")) +
        ".</p>";
    }
    html += "</div>";
    return html;
  }

  function renderCompaniesForConsiderationSection(companiesPayload, options) {
    options = options || {};
    var payload =
      companiesPayload && companiesPayload.mode === "companies" ? companiesPayload : defaultCompaniesPayload();
    var html =
      '<section class="bas-section bas-section--brief oas-section oas-section--companies bas-section--keep">';
    html += '<h2 class="bas-section-title">Operating Companies for Consideration</h2>';
    if (!payload.companiesAvailable || !(payload.companiesForConsideration || []).length) {
      html += renderCompaniesGatedBlock(payload);
      html += "</section>";
      return html;
    }
    html += '<p class="oas-company-level-note bas-muted">' + esc(OAS_COMPANY_SECTION_NOTE) + "</p>";
    var companies = sortCompaniesForDisplay(payload.companiesForConsideration);
    var total = companies.length;
    var printLimit = OAS_PRINT_COMPANY_LIMIT;
    var commonGaps = collectCommonDataGaps(companies);
    if (commonGaps.length) {
      html += '<div class="oas-common-gaps bas-avoid-break">';
      html += '<h3 class="oas-common-gaps__title">Common Data Gaps</h3>';
      html += '<ul class="oas-compact-list">';
      commonGaps.forEach(function (g) {
        html += "<li>" + esc(g) + "</li>";
      });
      html += "</ul></div>";
    }
    if (total > printLimit) {
      html +=
        '<p class="oas-companies-count bas-muted">Showing ' +
        esc(String(printLimit)) +
        " of " +
        esc(String(total)) +
        " companies with sufficient Operator Setup data.</p>";
    }
    html += '<div class="oas-company-grid oas-card-grid--two-col">';
    companies.forEach(function (c, idx) {
      html += renderCompanyCardFull(c, {
        isExtra: idx >= printLimit,
        commonGaps: commonGaps,
      });
    });
    html += "</div>";
    if (total > printLimit) {
      html +=
        '<button type="button" class="bas-btn bas-btn-secondary oas-companies-show-all bas-no-print" data-oas-show-all-companies>Show all ' +
        esc(String(total)) +
        " companies</button>";
    }
    html += "</section>";
    return html;
  }

  function renderMyDealsPreviewCompanyCard(company) {
    var c = company || {};
    var display = resolveCompanyDisplayName(c);
    var signals = mapHumanizedList(c.alignmentSignals, humanizeCompanyAlignmentSignal, 2);
    var html = '<article class="oas-preview-profile-card oas-preview-company-card">';
    html += '<div class="oas-preview-profile-card__head">';
    html +=
      '<h4 class="oas-preview-profile-card__title" data-oas-company-name>' + esc(display.name) + "</h4>";
    html +=
      '<span class="' +
      bandClass(c.alignmentBand) +
      ' oas-preview-band">' +
      esc(c.alignmentBand || "—") +
      "</span>";
    html += "</div>";
    if (c.countriesMarkets && c.countriesMarkets.length) {
      html += '<div class="oas-preview-markets">' + renderMarketChips(c.countriesMarkets) + "</div>";
    }
    if (signals.length) {
      html += '<ul class="oas-preview-bullets">';
      signals.forEach(function (s) {
        html += "<li>" + esc(s) + "</li>";
      });
      html += "</ul>";
    }
    if (c.dataCompleteness) {
      html += '<p class="oas-preview-gap-note">' + esc(String(c.dataCompleteness)) + "</p>";
    }
    html += "</article>";
    return html;
  }

  function renderCompaniesPreviewBlock(companiesPayload, fullHref) {
    var payload =
      companiesPayload && companiesPayload.mode === "companies" ? companiesPayload : defaultCompaniesPayload();
    var html = '<section class="oas-preview-section oas-preview-section--companies">';
    html += '<h3 class="oas-preview-section__title">Operating companies for consideration</h3>';
    if (!payload.companiesAvailable || !(payload.companiesForConsideration || []).length) {
      html += '<div class="oas-preview-future">';
      html +=
        '<p class="oas-preview-future__primary">' + esc(payload.gatingReason || COMPANIES_GATED_PRIMARY) + "</p>";
      html += '<p class="oas-preview-future__support">' + esc(COMPANIES_GATED_SUPPORT) + "</p>";
      html += "</div>";
      html += "</section>";
      return html;
    }
    var top = pickCompaniesForDisplay(payload.companiesForConsideration, OAS_MY_DEALS_COMPANY_LIMIT);
    html += '<div class="oas-preview-profile-grid">';
    top.forEach(function (c) {
      html += renderMyDealsPreviewCompanyCard(c);
    });
    html += "</div>";
    if (payload.companiesForConsideration.length > 3 && fullHref) {
      html +=
        '<p class="oas-preview-more"><a class="oas-preview-link" href="' +
        esc(fullHref) +
        '">View all company-level alignment in the full snapshot</a></p>';
    }
    html += "</section>";
    return html;
  }

  function renderMyDealsPreviewProfileCard(profile) {
    var p = profile || {};
    var signals = (p.alignmentSignals || []).slice(0, 3);
    var gapCount = (p.dataGaps || []).length;
    var gapNote = gapCount
      ? gapCount + " data gap" + (gapCount === 1 ? "" : "s") + " noted for this profile"
      : "";
    var html = '<article class="oas-preview-profile-card">';
    html += '<div class="oas-preview-profile-card__head">';
    html +=
      '<h4 class="oas-preview-profile-card__title">' +
      esc(p.displayLabel || p.shortLabel || "Operator profile") +
      "</h4>";
    html += '<span class="' + bandClass(p.alignmentBand) + ' oas-preview-band">' + esc(p.alignmentBand || "—") + "</span>";
    html += "</div>";
    if (p.bestUseCase) {
      html += '<p class="oas-preview-profile-card__lead">' + esc(p.bestUseCase) + "</p>";
    } else if (p.description) {
      html += '<p class="oas-preview-profile-card__lead">' + esc(p.description) + "</p>";
    }
    if (signals.length) {
      html += '<ul class="oas-preview-bullets">';
      signals.forEach(function (s) {
        html += "<li>" + esc(s) + "</li>";
      });
      html += "</ul>";
    }
    if (gapNote) {
      html += '<p class="oas-preview-gap-note">' + esc(gapNote) + "</p>";
    }
    html += "</article>";
    return html;
  }

  function buildMyDealsPreviewHtml(data, options) {
    options = options || {};
    var dealId = options.dealId || "";
    var ctx = data.dealContext || {};
    var signal = data.operatorReviewSignal || {};
    var profiles = data.profilesForReview || [];
    var topProfiles = pickTopProfilesForPreview(profiles, 3);
    var fullHref =
      options.fullPageHref ||
      (dealId ? "/operator-alignment-snapshot.html?dealId=" + encodeURIComponent(dealId) : "");
    var printHref = fullHref
      ? fullHref + (fullHref.indexOf("?") >= 0 ? "&" : "?") + "print=1"
      : "";

    var html = '<div class="oas-my-deals-preview">';
    html += '<p class="oas-preview-disclaimer">' + esc(PAGE_DISCLAIMER) + "</p>";
    html += '<p class="oas-preview-helper">Profile-level alignment signals based on the available deal profile.</p>';

    if (ctx.dealName) {
      html += '<p class="oas-preview-deal"><span class="oas-preview-deal__label">Deal</span> ' + esc(displayVal(ctx.dealName)) + "</p>";
    }

    html += '<section class="oas-preview-section">';
    html += '<h3 class="oas-preview-section__title">Operator review signal</h3>';
    html += '<div class="oas-preview-review">';
    html += '<div class="oas-preview-review__row">';
    html += '<span class="oas-preview-review__label">Signal level</span>';
    html +=
      '<span class="oas-review-level oas-review-level--' +
      esc(String(signal.level || "Insufficient Data").replace(/\s+/g, "-").toLowerCase()) +
      '">' +
      esc(signal.level || "Insufficient Data") +
      "</span>";
    html += "</div>";
    html += '<p class="oas-preview-review__intro">' + esc(REVIEW_SIGNAL_INTRO) + "</p>";
    if (signal.rationale) {
      html += '<p class="oas-preview-review__rationale">' + esc(signal.rationale) + "</p>";
    }
    var relevance = humanizeSignalKeys(signal.matchedSignals);
    if (relevance.length) {
      html += '<p class="oas-preview-chip-kicker">Key relevance signals</p>';
      html += renderPreviewChipList(relevance);
    }
    html += "</div></section>";

    html += '<section class="oas-preview-section">';
    html += '<h3 class="oas-preview-section__title">Operator profiles for review</h3>';
    if (!topProfiles.length) {
      html +=
        '<p class="oas-preview-empty">Additional deal information may be needed before operator profile alignment can be assessed.</p>';
    } else {
      html += '<div class="oas-preview-profile-grid">';
      topProfiles.forEach(function (p) {
        html += renderMyDealsPreviewProfileCard(p);
      });
      html += "</div>";
      if (profiles.length > topProfiles.length && fullHref) {
        html +=
          '<p class="oas-preview-more"><a class="oas-preview-link" href="' +
          esc(fullHref) +
          '">View all ' +
          esc(String(profiles.length)) +
          " operator profiles in the full snapshot</a></p>";
      }
    }
    html += "</section>";

    var gaps = data.dataGaps || [];
    if (gaps.length) {
      html += '<section class="oas-preview-section oas-preview-section--compact">';
      html += '<h3 class="oas-preview-section__title">Data gaps</h3>';
      html += '<ul class="oas-preview-bullets oas-preview-bullets--compact">';
      gaps.slice(0, 3).forEach(function (g) {
        html += "<li>" + esc(g) + "</li>";
      });
      html += "</ul>";
      if (gaps.length > 3) {
        html += '<p class="oas-preview-muted">' + esc("+" + (gaps.length - 3) + " more in full snapshot") + "</p>";
      }
      html += "</section>";
    }

    var actions = normalizeListItems(data.suggestedWorkflowActions, true);
    if (actions.length) {
      html += '<section class="oas-preview-section oas-preview-section--compact">';
      html += '<h3 class="oas-preview-section__title">Suggested workflow actions</h3>';
      html += '<ul class="oas-preview-bullets oas-preview-bullets--compact">';
      actions.slice(0, 3).forEach(function (a) {
        html += "<li>" + esc(a) + "</li>";
      });
      html += "</ul>";
      if (actions.length > 3) {
        html += '<p class="oas-preview-muted">' + esc("+" + (actions.length - 3) + " more in full snapshot") + "</p>";
      }
      html += "</section>";
    }

    html += renderCompaniesPreviewBlock(resolveCompaniesPayload(data), fullHref);

    html += '<footer class="oas-preview-footer">';
    if (fullHref) {
      html +=
        '<a class="btn btn-primary oas-preview-btn" href="' +
        esc(fullHref) +
        '">Open full snapshot</a>';
    }
    if (printHref) {
      html +=
        '<a class="btn btn-secondary oas-preview-btn" href="' +
        esc(printHref) +
        '" target="_blank" rel="noopener noreferrer">Print / Save as PDF</a>';
    }
    html += "</footer></div>";
    return html;
  }

  function renderMyDealsPreview(container, data, options) {
    if (!container || !data) return null;
    options = options || {};
    logCompaniesQaState(data, options);
    container.innerHTML = buildMyDealsPreviewHtml(data, options);
    return data;
  }

  function render(container, data, options) {

    if (!container || !data) return null;

    options = options || {};

    logCompaniesQaState(data, options);

    container.innerHTML = buildHtml(data, options);

    bindPrint(container);
    bindPageFlip(container);

    return data;

  }



  global.OperatorAlignmentSnapshot = {

    render: render,

    renderMyDealsPreview: renderMyDealsPreview,

    buildHtml: buildHtml,

    buildMyDealsPreviewHtml: buildMyDealsPreviewHtml,

    pickTopProfilesForPreview: pickTopProfilesForPreview,

    bandSortRank: bandSortRank,

    humanizeSignalKey: humanizeSignalKey,

    humanizeSignalKeys: humanizeSignalKeys,

    stripWorkflowActionPrefix: stripWorkflowActionPrefix,

    PAGE_DISCLAIMER: PAGE_DISCLAIMER,

    OUTPUT_NOTE: OUTPUT_NOTE_PROFILE_ONLY,
    OUTPUT_NOTE_PROFILE_ONLY: OUTPUT_NOTE_PROFILE_ONLY,
    OUTPUT_NOTE_WITH_COMPANIES: OUTPUT_NOTE_WITH_COMPANIES,
    buildOutputNote: buildOutputNote,
    resolveCompaniesPayload: resolveCompaniesPayload,
    attachCompaniesSnapshot: attachCompaniesSnapshot,
    fetchCompaniesApiPack: fetchCompaniesApiPack,
    companiesFetchUserMessage: companiesFetchUserMessage,
    defaultCompaniesPayload: defaultCompaniesPayload,
    resolveCompanyDisplayName: resolveCompanyDisplayName,
    OAS_PRINT_COMPANY_LIMIT: OAS_PRINT_COMPANY_LIMIT,
    OAS_PRINT_GAP_LIMIT: OAS_PRINT_GAP_LIMIT,

  };

})(typeof window !== "undefined" ? window : globalThis);


