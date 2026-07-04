/**
 * Dealality Insights — owner-facing tab on Operator DNA profile.
 * Profile Map, Discussion Prompts, Conversation Prep (Dealality + operator framing; non-advisory).
 */
(function (global) {
  "use strict";

  var TAB_NAME = "Dealality Insights";

  var DEFAULT_DISCUSSION_PROMPTS = [
    "How would this operator staff and support my market after signing?",
    "What experience does the team have with my asset type, guest mix, and seasonality?",
    "How do they approach conversion, reflag, or transition without disrupting asset value?",
    "What owner reporting, cadence, and escalation should I expect?",
    "How do they plan to improve revenue, margin, and guest experience at my property?",
    "Which brand relationships are relevant to my strategy, if any?",
    "Who are my day-to-day counterparts, and how are decisions made with ownership?",
    "How do they define success and protect long-term asset value for owners like me?",
  ];

  var TAB_ICON =
    '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 18h6"></path><path d="M10 22h4"></path><path d="M12 2a7 7 0 0 0-4 12.7V17h8v-2.3A7 7 0 0 0 12 2z"></path></svg>';

  var TAB_LABEL = "Dealality<br>Insights";

  var TAB_PROFILE = "Profile & Positioning";
  var TAB_MARKETS = "Markets & Footprint";
  var TAB_LEADERSHIP = "Leadership";
  var TAB_BRAND = "Brand & Relationships";
  var TAB_ENGAGEMENT = "Owner Engagement & Reporting";
  var TAB_INFRA = "Infrastructure & Data";
  var TAB_PROJECT_FIT = "Project Fit & Deal Profile";
  var TAB_PROOF = "Proof & Track Record";

  /** Profile Map row labels — Title Case; keep in sync with deriveProfileMap / byArea lookups. */
  var AREA_PROFILE = "Profile & Positioning";
  var AREA_MARKETS = "Markets & Footprint";
  var AREA_LEADERSHIP = "Leadership & Team";
  var AREA_BRAND = "Brand & Relationships";
  var AREA_ENGAGEMENT = "Engagement & Reporting";
  var AREA_INFRA = "Infrastructure & Data";
  var AREA_PROJECT_FIT = "Project Fit & Deal Profile";
  var AREA_PROOF = "Proof & Track Record";

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function arrayish(v) {
    if (Array.isArray(v)) return v.filter(Boolean).map(String);
    if (v == null || v === "") return [];
    return String(v)
      .split(/[,;|\n]+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
  }

  function unique(arr) {
    var seen = {};
    return (arr || []).filter(function (x) {
      var k = String(x).toLowerCase();
      if (!k || seen[k]) return false;
      seen[k] = true;
      return true;
    });
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

  function hasSignal(v) {
    var s = nz(v);
    return !!s && s !== "—" && !/^not yet/i.test(s);
  }

  function countSignals(values) {
    return (values || []).filter(hasSignal).length;
  }

  function coverageStatus(score, partialMin) {
    partialMin = partialMin == null ? 1 : partialMin;
    if (score <= 0) return "empty";
    if (score >= partialMin + 2) return "documented";
    return "partial";
  }

  function statusLabel(status) {
    if (status === "documented") return "Published";
    if (status === "partial") return "Partial";
    return "Not published";
  }

  function caseStudyHasContent(cs) {
    var Gold = global.OperatorExplorerGoldMock;
    if (Gold && typeof Gold.caseStudyHasContent === "function") {
      return Gold.caseStudyHasContent(cs);
    }
    if (!cs || typeof cs !== "object") return false;
    return !!(
      nz(cs.title) ||
      nz(cs.propertyName) ||
      nz(cs.headline) ||
      nz(cs.summary) ||
      nz(cs.challenge) ||
      nz(cs.result)
    );
  }

  function leadershipHasContent(L) {
    if (!L || typeof L !== "object") return false;
    return !!(nz(L.name) || nz(L.fullName) || nz(L.title) || nz(L.role));
  }

  function deriveMarketLayers(vm) {
    var p = (vm && vm.prefill) || {};
    return {
      current: unique(
        []
          .concat(arrayish(p.activeCountries))
          .concat(arrayish(p.activeMarkets))
          .concat(arrayish(p.regions))
          .concat(arrayish(p.regionsSupported))
      ),
      team: unique(arrayish(p.teamExperienceMarkets)),
      target: unique(
        []
          .concat(arrayish(p.targetGrowthMarkets))
          .concat(arrayish(p.priorityMarkets))
          .concat(arrayish(p.specificMarkets))
      ),
    };
  }

  function deriveProfileMap(vm) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var fields = (vm && vm.fields) || {};
    var me = deriveMarketLayers(vm);
    var leaders = (vm && vm.leadership) || [];
    var leadersDoc = leaders.filter(leadershipHasContent);
    var cases = ((vm && vm.caseStudies) || []).filter(caseStudyHasContent);
    var brands = unique(
      []
        .concat(arrayish(p.brandFamiliesOperated))
        .concat(arrayish(p.brandsOperated))
        .concat(arrayish(p.brands))
    );

    var profileScore = countSignals([
      vm && vm.statement,
      vm && vm.tagline,
      pick(ex, p, "managementPhilosophy", ""),
      arrayish(p.differentiators).join(", "),
      arrayish(p.propertyTypes).join(", "),
      arrayish(p.chainScalesSupported).join(", "),
    ]);

    var marketScore =
      me.current.length +
      me.target.length +
      (hasSignal(pick(ex, p, "geo_cala_total_hotels", "")) ? 1 : 0) +
      (hasSignal(pick(ex, p, "mkt_signal_years", "")) ? 1 : 0);

    var leadershipScore = leadersDoc.length + (hasSignal(p.leadershipPhilosophy) ? 1 : 0);

    var brandScore =
      brands.length +
      countSignals([
        p.softBrandLifestyleExperience,
        p.brandsPortfolioDetail,
        p.brand_narrative_compliance,
        fields["Branded vs Independent Mix"],
      ]);

    var engagementScore = countSignals([
      pick(ex, p, "eng_signal_reporting", ""),
      pick(ex, p, "eng_signal_cadence", ""),
      pick(ex, p, "ov_q_touchpoints", ""),
      arrayish(p.reportTypes).join(", "),
      p.ownerEngagementNarrative,
    ]);

    var infraScore = countSignals([
      pick(ex, p, "infra_signal_uptime", ""),
      pick(ex, p, "infra_signal_adoption", ""),
      pick(ex, p, "risk_signal_audit", ""),
      pick(ex, p, "platformTechnology", ""),
    ]);

    var projectFitScore = countSignals([
      arrayish(p.bf_selected_asset_types).join(", "),
      arrayish(p.bf_selected_situation_types).join(", "),
      arrayish(p.bf_selected_deal_structures).join(", "),
      p.bf_evaluation_path,
    ]);

    var proofScore =
      cases.length +
      countSignals([
        p.revparImprovement,
        p.noiImprovement,
        p.occupancyImprovement,
        p.ownerRetention,
        pick(ex, p, "tr_signal_occ", ""),
        pick(ex, p, "tr_signal_adr", ""),
        pick(ex, p, "tr_signal_repeat", ""),
        p.achievements,
      ]);

    function row(area, tabName, score, summary, partialMin) {
      var status = coverageStatus(score, partialMin);
      return {
        area: area,
        tabName: tabName,
        status: status,
        statusLabel: statusLabel(status),
        summary: summary,
      };
    }

    return [
      row(
        AREA_PROFILE,
        TAB_PROFILE,
        profileScore,
        profileScore
          ? "You can review this operator's positioning and operating focus on Dealality."
          : "This operator has published limited positioning detail on Dealality.",
        1
      ),
      row(
        AREA_MARKETS,
        TAB_MARKETS,
        marketScore,
        marketScore
          ? unique(me.current.concat(me.target)).slice(0, 4).join(", ") +
            (marketScore > 4
              ? " and additional markets on the Markets & Footprint tab."
              : " on the Markets & Footprint tab.")
          : "This operator has not yet published market footprint detail on Dealality.",
        1
      ),
      row(
        AREA_LEADERSHIP,
        TAB_LEADERSHIP,
        leadershipScore,
        leadersDoc.length
          ? "This operator has published " +
            leadersDoc.length +
            " leadership profile" +
            (leadersDoc.length === 1 ? "" : "s") +
            " on Dealality."
          : "This operator has not yet published leadership profiles on Dealality.",
        1
      ),
      row(
        AREA_BRAND,
        TAB_BRAND,
        brandScore,
        brandScore
          ? brands.length
            ? "This operator references " +
              brands.length +
              " brand relationship" +
              (brands.length === 1 ? "" : "s") +
              " on Dealality."
            : "This operator has published brand and portfolio context on Dealality."
          : "This operator has not yet published brand relationship detail on Dealality.",
        1
      ),
      row(
        AREA_ENGAGEMENT,
        TAB_ENGAGEMENT,
        engagementScore,
        engagementScore
          ? "You can review this operator's owner communication and reporting detail on Dealality."
          : "This operator has not yet published reporting cadence or owner-touchpoint detail on Dealality.",
        1
      ),
      row(
        AREA_INFRA,
        TAB_INFRA,
        infraScore,
        infraScore
          ? "You can review this operator's platform, data, and control signals on Dealality."
          : "This operator has not yet published infrastructure or control detail on Dealality.",
        1
      ),
      row(
        AREA_PROJECT_FIT,
        TAB_PROJECT_FIT,
        projectFitScore,
        projectFitScore
          ? "You can review project types, situations, and deal structures on the Project Fit & Deal Profile tab."
          : "This operator has not yet published structured project and deal-profile detail on Dealality.",
        1
      ),
      row(
        AREA_PROOF,
        TAB_PROOF,
        proofScore,
        proofScore
          ? cases.length
            ? "This operator has published " +
              cases.length +
              " case " +
              (cases.length === 1 ? "study" : "studies") +
              " and performance signals on Dealality."
            : "You can review performance and track-record signals this operator published on Dealality."
          : "This operator has not yet published case studies or performance proof on Dealality.",
        1
      ),
    ];
  }

  function deriveDiscussionPrompts() {
    return DEFAULT_DISCUSSION_PROMPTS.slice();
  }

  function deriveConversationPrep(vm, profileMap) {
    var company = nz(vm && vm.companyName) || "this operator";
    var byArea = {};
    (profileMap || []).forEach(function (row) {
      byArea[row.area] = row;
    });

    function topic(title, tabName, gapNote, prompts) {
      return {
        title: title,
        tabName: tabName,
        gapNote: gapNote,
        prompts: prompts || [],
      };
    }

    var topics = [];

    var marketsRow = byArea[AREA_MARKETS];
    topics.push(
      topic(
        "Market footprint & resourcing",
        TAB_MARKETS,
        !marketsRow || marketsRow.status === "empty"
          ? "This operator has not published geographic footprint detail on Dealality yet."
          : marketsRow.status === "partial"
            ? "Published footprint may be light—you can confirm local leadership and owner support for your market with the operator."
            : "",
        [
          "How is the on-the-ground team structured for owners in my market?",
          "What does mobilization and ongoing owner support look like in the first 12 months?",
        ]
      )
    );

    var proofRow = byArea[AREA_PROOF];
    topics.push(
      topic(
        "Performance proof & case examples",
        TAB_PROOF,
        !proofRow || proofRow.status === "empty"
          ? "This operator has not published case studies or performance signals on Dealality yet."
          : proofRow.status === "partial"
            ? "Some performance context is on Dealality—you can ask the operator how it compares to your asset type and deal stage."
            : "",
        [
          "Which published examples are closest to my asset type and operating situation?",
          "How does " + company + " measure and report outcomes during transition vs. stabilized operations?",
        ]
      )
    );

    var brandRow = byArea[AREA_BRAND];
    topics.push(
      topic(
        "Brand execution & conversions",
        TAB_BRAND,
        !brandRow || brandRow.status === "empty"
          ? "This operator has not published brand relationship detail on Dealality yet."
          : "",
        [
          "Which brand families has the team operated under for assets similar to mine?",
          "How does the operator handle conversion, reflag, or soft-brand transitions?",
        ]
      )
    );

    var engRow = byArea[AREA_ENGAGEMENT];
    topics.push(
      topic(
        "Owner reporting & communication",
        TAB_ENGAGEMENT,
        !engRow || engRow.status === "empty"
          ? "This operator has not published owner reporting format or cadence on Dealality yet."
          : engRow.status === "partial"
            ? "Some reporting detail is on Dealality—you can confirm deliverables, timing, and escalation with the operator."
            : "",
        [
          "What owner reporting package should I expect monthly and at budget season?",
          "How are variances, capex, and major operating decisions communicated?",
        ]
      )
    );

    var infraRow = byArea[AREA_INFRA];
    topics.push(
      topic(
        "Systems, data & controls",
        TAB_INFRA,
        !infraRow || infraRow.status === "empty"
          ? "This operator has not published infrastructure or control detail on Dealality yet."
          : "",
        [
          "Which systems power revenue management, reporting, and owner dashboards?",
          "How often is owner-facing data refreshed, and who owns data quality?",
        ]
      )
    );

    var leadRow = byArea[AREA_LEADERSHIP];
    topics.push(
      topic(
        "Leadership bench & accountability",
        TAB_LEADERSHIP,
        !leadRow || leadRow.status === "empty"
          ? "This operator has not published leadership profiles on Dealality yet."
          : "",
        [
          "Who would be the day-to-day owner counterpart and regional executive for my asset?",
          "How does leadership continuity work through transition and stabilization?",
        ]
      )
    );

    return topics;
  }

  /**
   * @param {object} vm - gold-mock view model
   * @returns {object} insights data contract for panel render
   */
  function deriveInsightsData(vm) {
    var profileMap = deriveProfileMap(vm);
    return {
      profileMap: profileMap,
      discussionPrompts: deriveDiscussionPrompts(),
      conversationPrep: deriveConversationPrep(vm, profileMap),
    };
  }

  function gotoTabButton(tabName, label) {
    return (
      '<button type="button" class="odna-insights-goto-tab" data-tab="' +
      escapeHtml(tabName) +
      '">' +
      escapeHtml(label || "View tab") +
      "</button>"
    );
  }

  function statusBadgeHtml(status) {
    return (
      '<span class="odna-insights-status odna-insights-status--' +
      escapeHtml(status) +
      '">' +
      escapeHtml(statusLabel(status)) +
      "</span>"
    );
  }

  function buildProfileMapSection(rows) {
    if (!rows.length) {
      return (
        '<section class="section odna-insights-section">' +
        '<h2 class="section-title">Profile Map</h2>' +
        '<p class="gold-mock-tab-empty odna-subsection-intro">Dealality shows how much this operator has published across the main profile areas—so you know what you can review before your conversations.</p>' +
        '<p class="gold-mock-tab-empty">Dealality does not have enough published detail from this operator to map profile areas yet.</p></section>'
      );
    }

    var tableRows = rows
      .map(function (row) {
        return (
          "<tr>" +
          "<th scope=\"row\">" +
          escapeHtml(row.area) +
          "</th>" +
          "<td>" +
          statusBadgeHtml(row.status) +
          "</td>" +
          "<td>" +
          escapeHtml(row.summary) +
          "</td>" +
          "<td class=\"odna-insights-map-actions\">" +
          gotoTabButton(row.tabName, "Open tab") +
          "</td></tr>"
        );
      })
      .join("");

    return (
      '<section class="section odna-insights-section">' +
      '<h2 class="section-title">Profile Map</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">Dealality maps what this operator has published on their profile—by area—and links you to the tabs where you can read it. This is not a fit score or a recommendation.</p>' +
      '<div class="odna-insights-map-wrap">' +
      '<table class="odna-insights-map-table">' +
      "<thead><tr><th scope=\"col\">Profile Area</th><th scope=\"col\">Coverage</th><th scope=\"col\">Summary</th><th scope=\"col\"><span class=\"sr-only\">Navigate</span></th></tr></thead>" +
      "<tbody>" +
      tableRows +
      "</tbody></table></div></section>"
    );
  }

  function buildDiscussionPromptsSection(prompts) {
    var cards = (prompts || [])
      .map(function (q) {
        return (
          '<div class="card odna-insights-question-card">' +
          '<span class="odna-insights-question-card__icon" aria-hidden="true">' +
          '<svg viewBox="0 0 24 24"><path d="M20 6L9 17l-5-5"></path></svg>' +
          "</span>" +
          "<p>" +
          escapeHtml(q) +
          "</p></div>"
        );
      })
      .join("");

    return (
      '<section class="section odna-insights-section">' +
      '<h2 class="section-title">Discussion Prompts</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">Questions you may want to explore with this operator in your own conversations.</p>' +
      '<div class="grid-2 odna-insights-questions-grid">' +
      cards +
      "</div></section>"
    );
  }

  function buildConversationPrepSection(topics) {
    var cards = (topics || [])
      .map(function (t) {
        var gap = nz(t.gapNote);
        var prompts = (t.prompts || [])
          .map(function (q) {
            return "<li>" + escapeHtml(q) + "</li>";
          })
          .join("");
        return (
          '<div class="card odna-insights-prep-card">' +
          "<h3>" +
          escapeHtml(t.title) +
          "</h3>" +
          (gap
            ? '<p class="odna-insights-prep-gap"><span class="odna-insights-prep-label">Worth confirming with the operator:</span> ' +
              escapeHtml(gap) +
              "</p>"
            : "") +
          (prompts
            ? '<p class="odna-insights-prep-label">Discussion prompts</p><ul class="odna-insights-prep-prompts">' +
              prompts +
              "</ul>"
            : "") +
          '<div class="odna-insights-prep-actions">' +
          gotoTabButton(t.tabName, "Related tab") +
          "</div></div>"
        );
      })
      .join("");

    return (
      '<section class="section odna-insights-section">' +
      '<h2 class="section-title">Conversation Prep</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">Topics where this operator&apos;s published profile is light or incomplete—so you know what to confirm with them directly.</p>' +
      '<div class="grid-2 odna-insights-prep-grid">' +
      cards +
      "</div></section>"
    );
  }

  function buildPanelHtml(vm) {
    var data = deriveInsightsData(vm);
    return (
      '<div class="odna-insights-tab">' +
      '<p class="gold-mock-tab-empty odna-subsection-intro odna-insights-tab-intro">Dealality helps you navigate this operator&apos;s published profile—what they have shared, where to find it, and questions you may want to discuss. This is not legal, financial, or investment advice, and it is not a substitute for your own diligence.</p>' +
      buildProfileMapSection(data.profileMap || []) +
      buildDiscussionPromptsSection(data.discussionPrompts || []) +
      buildConversationPrepSection(data.conversationPrep || []) +
      "</div>"
    );
  }

  function wireTabNavigation(root) {
    if (!root) return;
    root.querySelectorAll(".odna-insights-goto-tab").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var tab = btn.getAttribute("data-tab");
        var Mount = global.OperatorDnaProfileMount;
        if (Mount && Mount.activateTab && tab) Mount.activateTab(tab);
      });
    });
  }

  function mountTab(vm) {
    var Mount = global.OperatorDnaProfileMount;
    if (!Mount || !Mount.appendCustomTab) return;
    Mount.appendCustomTab({
      tabName: TAB_NAME,
      iconHtml: TAB_ICON,
      labelHtml: TAB_LABEL,
      panelHtml: buildPanelHtml(vm),
      activateOnAppend: false,
    });
    var panel = document.querySelector('[data-panel="' + TAB_NAME + '"]');
    wireTabNavigation(panel);
  }

  global.OperatorDnaDealalityInsights = {
    TAB_NAME: TAB_NAME,
    deriveInsightsData: deriveInsightsData,
    deriveProfileMap: deriveProfileMap,
    deriveDiscussionPrompts: deriveDiscussionPrompts,
    deriveConversationPrep: deriveConversationPrep,
    DEFAULT_DISCUSSION_PROMPTS: DEFAULT_DISCUSSION_PROMPTS,
    buildPanelHtml: buildPanelHtml,
    mountTab: mountTab,
    wireTabNavigation: wireTabNavigation,
  };
})(typeof window !== "undefined" ? window : global);
