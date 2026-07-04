/**
 * Best Fit & Deal Profile — owner-facing subsections (Explorer / DNA).
 * Subsection intros and KPI notes are owner-facing: inform and compare operators,
 * not encourage owners to self-eliminate or gatekeep themselves out of outreach.
 * JSON via prefill / explorerProfileJson (bf_* keys) with defaults.
 */
(function (global) {
  "use strict";

  var FIELD = {
    fitCriteria: "bf_fit_criteria_json",
    bestFitProjectTypes: "bf_best_fit_project_types_json",
    preferredDealProfile: "bf_preferred_deal_profile_json",
    evaluationPath: "bf_evaluation_path_json",
    redFlags: "bf_red_flags_json",
    evaluationTimeline: "bf_evaluation_timeline",
  };

  var DEFAULTS = {
    bf_fit_criteria_json: [
      {
        fitCriteria: "Market Fit",
        operatorLooksFor:
          "Coastal, resort, island, leisure-led, resort-adjacent, or mixed leisure/business demand markets.",
        importance: "High",
      },
      {
        fitCriteria: "Asset Type Fit",
        operatorLooksFor:
          "Full-service resort, lifestyle hotel, condo-hotel, soft brand, independent hotel, or conversion/reflag.",
        importance: "High",
      },
      {
        fitCriteria: "Ownership Fit",
        operatorLooksFor:
          "Owners seeking transparent reporting, active operating support, and a hands-on management partner.",
        importance: "High",
      },
      {
        fitCriteria: "Brand / Flag Fit",
        operatorLooksFor:
          "Projects where the operator can support brand onboarding, standards, conversion, or independent positioning.",
        importance: "Medium / High",
      },
      {
        fitCriteria: "Operating Complexity",
        operatorLooksFor:
          "Complex assets where the operator's resort, F&B, condo-hotel, or transition experience creates value.",
        importance: "High",
      },
      {
        fitCriteria: "Commercial Upside",
        operatorLooksFor:
          "Assets with revenue, distribution, direct booking, repositioning, or market penetration opportunities.",
        importance: "High",
      },
      {
        fitCriteria: "Capital / CapEx Readiness",
        operatorLooksFor:
          "Owners with realistic capex plans, PIP awareness, funding path, and willingness to invest appropriately.",
        importance: "Medium / High",
      },
      {
        fitCriteria: "Timing & Readiness",
        operatorLooksFor:
          "Projects with clear decision timing, access to information, and a real path to engagement.",
        importance: "High",
      },
    ],
    bf_best_fit_project_types_json: [
      {
        fitLevel: "Best Fit",
        projectType: "Conversion / Reflag",
        ownerContext:
          "Existing hotel with brand transition, repositioning, or operating reset needs.",
      },
      {
        fitLevel: "Best Fit",
        projectType: "Resort / Leisure Asset",
        ownerContext:
          "Beach, island, coastal, spa, F&B-heavy, lifestyle, or destination-driven hotel.",
      },
      {
        fitLevel: "Best Fit",
        projectType: "Condo-Hotel / Mixed Ownership",
        ownerContext:
          "Complex ownership structures requiring reporting discipline and owner communication.",
      },
      {
        fitLevel: "Selective Fit",
        projectType: "New Build",
        ownerContext:
          "Best when owner has clear capital plan, brand path, and pre-opening support needs.",
      },
      {
        fitLevel: "Selective Fit",
        projectType: "Urban Full Service",
        ownerContext:
          "Potential fit when leisure, lifestyle, F&B, or owner reporting complexity is meaningful.",
      },
      {
        fitLevel: "Limited Fit",
        projectType: "Pure Economy / Low-Touch Asset",
        ownerContext:
          "May not fully leverage operator's resort, commercial, and owner-support strengths.",
      },
    ],
    bf_preferred_deal_profile_json: [
      {
        label: "Preferred Owner Type",
        value:
          "Family offices, long-term holders, developers, institutional owners, and owners seeking active operator involvement.",
      },
      {
        label: "Preferred Agreement Type",
        value:
          "Third-party management agreement, transition management, pre-opening support, or owner/operator partnership depending on asset.",
      },
      {
        label: "Preferred Market Position",
        value:
          "Upper-midscale through upscale, upper-upscale, lifestyle, resort, independent, and soft-brand opportunities.",
      },
      {
        label: "Ideal Situation",
        value:
          "Owner has a real project, clear objectives, access to asset information, and willingness to evaluate operator fit seriously.",
      },
      {
        label: "Less Ideal Situation",
        value:
          "No real transaction, unclear ownership authority, unrealistic economics, insufficient capex, or purely passive owner expectations.",
      },
      {
        label: "Important Deal Signals",
        value:
          "Owner responsiveness, data availability, brand path, capex readiness, market demand, and decision timeline.",
      },
    ],
    bf_evaluation_path_json: [
      {
        title: "Initial Screen",
        description:
          "Review market, asset type, ownership profile, size, timing, and basic operating needs.",
      },
      {
        title: "Fit Qualification",
        description:
          "Assess alignment with operator DNA, regional experience, brand requirements, and operating complexity.",
      },
      {
        title: "Information Request",
        description:
          "Collect operating data, financials, capex/PIP context, brand status, owner priorities, and project timeline.",
      },
      {
        title: "Internal Review",
        description:
          "Operations, commercial, finance, pre-opening, and leadership review project feasibility and support needs.",
      },
      {
        title: "Owner Discussion",
        description:
          "Clarify objectives, reporting expectations, capex readiness, brand direction, and decision process.",
      },
      {
        title: "Proposal Path",
        description:
          "Move to management proposal, transition plan, or decline/defer if fit is not strong enough.",
      },
    ],
    bf_red_flags_json: [
      {
        title: "Unclear Ownership Authority",
        description:
          "Decision-maker is not identified or owner group is not aligned.",
      },
      {
        title: "Insufficient CapEx Readiness",
        description:
          "PIP, renovation, or operating needs are not realistically funded or scoped.",
      },
      {
        title: "Unrealistic Performance Expectations",
        description:
          "Owner assumptions do not match market reality, brand requirements, or operating economics.",
      },
      {
        title: "No Clear Timeline",
        description:
          "Project is exploratory with no decision path, financing path, or operating transition need.",
      },
      {
        title: "Poor Strategic Fit",
        description:
          "Asset type, market, or owner expectations do not align with the operator's strengths.",
      },
      {
        title: "Limited Data Access",
        description:
          "Operator cannot review enough information to assess risk, staffing, capex, or commercial upside.",
      },
    ],
    bf_evaluation_timeline: "60-90 days",
  };

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

  function pick(ex, p, key) {
    if (ex && nz(ex[key])) return ex[key];
    if (p && nz(p[key])) return p[key];
    return "";
  }

  function parseJsonArray(raw) {
    if (raw == null || raw === "") return null;
    if (Array.isArray(raw)) return raw;
    try {
      var parsed = JSON.parse(String(raw));
      return Array.isArray(parsed) ? parsed : null;
    } catch (e) {
      return null;
    }
  }

  function sectionData(vm, fieldKey) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var fromRecord = parseJsonArray(pick(ex, p, fieldKey));
    if (fromRecord && fromRecord.length) return fromRecord;
    return DEFAULTS[fieldKey] || [];
  }

  function scalarData(vm, fieldKey) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    return nz(pick(ex, p, fieldKey)) || nz(DEFAULTS[fieldKey]);
  }

  function parseList(v) {
    if (Array.isArray(v)) return v.map(nz).filter(Boolean);
    if (v == null || v === "") return [];
    return String(v)
      .split(/[,;|\n]+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
  }

  function uniqueList(arr) {
    var seen = {};
    return (arr || []).filter(function (x) {
      var k = String(x).toLowerCase().trim();
      if (!k || seen[k]) return false;
      seen[k] = true;
      return true;
    });
  }

  function joinList(arr) {
    var items = uniqueList(arr);
    return items.length ? items.join(", ") : "";
  }

  function fieldList(vm, prefillKeys, airtableNames) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var fields = (vm && vm.fields) || {};
    var out = [];
    (prefillKeys || []).forEach(function (key) {
      out = out.concat(parseList(pick(ex, p, key)));
      out = out.concat(parseList(p[key]));
    });
    (airtableNames || []).forEach(function (name) {
      if (fields[name] != null && fields[name] !== "") {
        out = out.concat(parseList(fields[name]));
      }
    });
    return uniqueList(out);
  }

  /** Same JSON rows as Operator Fit Criteria subsection on Project Fit tab. */
  function fitCriteriaLooksFor(vm, criteriaMatch) {
    var needle = nz(criteriaMatch).toLowerCase();
    if (!needle) return "";
    var rows = sectionData(vm, FIELD.fitCriteria);
    for (var i = 0; i < rows.length; i++) {
      var name = nz(rows[i].fitCriteria || rows[i].criteria).toLowerCase();
      if (name.indexOf(needle) >= 0) {
        return nz(rows[i].operatorLooksFor || rows[i].looksFor || rows[i].description);
      }
    }
    return "";
  }

  /** Same JSON rows as Preferred Deal Profile subsection on Project Fit tab. */
  function preferredDealProfileValue(vm, labelMatch) {
    var needle = nz(labelMatch).toLowerCase();
    if (!needle) return "";
    var rows = sectionData(vm, FIELD.preferredDealProfile);
    for (var i = 0; i < rows.length; i++) {
      var label = nz(rows[i].label || rows[i].key).toLowerCase();
      if (label.indexOf(needle) >= 0) {
        return nz(rows[i].value || rows[i].description);
      }
    }
    return "";
  }

  /** Project type names from Best-Fit Project Types table (excludes Limited Fit). */
  function bestFitProjectTypeSummary(vm) {
    var rows = sectionData(vm, FIELD.bestFitProjectTypes);
    var names = [];
    rows.forEach(function (row) {
      if (nz(row.fitLevel).toLowerCase().indexOf("limited") >= 0) return;
      var name = nz(row.projectType || row.type);
      if (name) names.push(name);
    });
    return joinList(names);
  }

  /**
   * Profile & Positioning summary — mirrors Project Fit & Deal Profile tab content
   * (fit criteria, best-fit project types, preferred deal profile), not raw Setup dumps.
   * @returns {Array<[string, string]>}
   */
  function deriveOverviewBestFitRows(vm) {
    var p = (vm && vm.prefill) || {};

    var idealMarkets =
      fitCriteriaLooksFor(vm, "market fit") ||
      joinList(
        fieldList(vm, ["bestFitGeographies"], ["Best Fit Geographies"]).concat(
          fieldList(vm, ["priorityMarkets"], ["Priority Markets"])
        )
      ) ||
      nz(p.priorityMarketsOther);

    var idealAssetTypes =
      fitCriteriaLooksFor(vm, "asset type") ||
      joinList(
        fieldList(vm, ["bf_selected_asset_types"], ["Best Fit Asset Types"])
      );

    var idealOwnership =
      preferredDealProfileValue(vm, "preferred owner type") ||
      joinList(fieldList(vm, ["bestFitOwnerTypes"], ["Best Fit Owner Types"]));

    var projectStages =
      joinList(fieldList(vm, ["bf_selected_situation_types"], [])) ||
      bestFitProjectTypeSummary(vm) ||
      fitCriteriaLooksFor(vm, "timing");

    var lessIdeal =
      preferredDealProfileValue(vm, "less ideal") ||
      joinList(
        fieldList(vm, ["bf_not_ideal_for", "lessIdealSituations"], [
          "Not Ideal for",
          "Less Ideal Situations",
        ])
      );

    return [
      ["Ideal Markets", idealMarkets || "—"],
      ["Ideal Asset Types", idealAssetTypes || "—"],
      ["Ideal Ownership Profile", idealOwnership || "—"],
      ["Project Stages", projectStages || "—"],
      ["Less Ideal For", lessIdeal || "—"],
    ];
  }

  function overviewBestFitHasData(rows) {
    return (rows || []).some(function (pair) {
      return nz(pair[1]) && pair[1] !== "—";
    });
  }

  function buildOverviewBestFitOwnerProjectSection(vm) {
    var rows = deriveOverviewBestFitRows(vm);
    if (!overviewBestFitHasData(rows)) return "";
    var body = rows
      .map(function (pair) {
        return (
          "<tr><th scope=\"row\">" +
          escapeHtml(pair[0]) +
          "</th><td>" +
          escapeHtml(pair[1]) +
          "</td></tr>"
        );
      })
      .join("");
    return wrapSection(
      "Best-Fit Owner / Project Profile",
      "Where this operator is most likely to create value and where fit may be limited.",
      '<div class="oe-bf-table-wrap"><table class="oe-bf-table oe-bf-table--two-col"><tbody>' +
        body +
        "</tbody></table></div>",
      "oe-bf-section--profile oe-bf-section--overview-fit"
    );
  }

  function wrapSection(title, intro, bodyHtml, extraClass) {
    if (!bodyHtml) return "";
    return (
      '<section class="section oe-bf-section' +
      (extraClass ? " " + extraClass : "") +
      '">' +
      '<h2 class="section-title">' +
      escapeHtml(title) +
      "</h2>" +
      (intro
        ? '<p class="gold-mock-tab-empty odna-subsection-intro">' + escapeHtml(intro) + "</p>"
        : "") +
      bodyHtml +
      "</section>"
    );
  }

  function importanceClass(v) {
    var s = nz(v).toLowerCase();
    if (s.indexOf("medium") >= 0) return "oe-bf-pill oe-bf-pill--medium";
    if (s.indexOf("limited") >= 0) return "oe-bf-pill oe-bf-pill--limited";
    return "oe-bf-pill oe-bf-pill--high";
  }

  function fitLevelClass(v) {
    var s = nz(v).toLowerCase();
    if (s.indexOf("limited") >= 0) return "oe-bf-pill oe-bf-pill--limited";
    if (s.indexOf("selective") >= 0) return "oe-bf-pill oe-bf-pill--medium";
    return "oe-bf-pill oe-bf-pill--high";
  }

  function cardIconCard(title, description, cardClass, iconClass, step) {
    if (!nz(title)) return "";
    var displayTitle =
      global.OperatorExplorerCardTitle && typeof global.OperatorExplorerCardTitle.formatCardTitle === "function"
        ? global.OperatorExplorerCardTitle.formatCardTitle(title)
        : title;
    return (
      '<div class="card ' +
      cardClass +
      '">' +
      '<span class="' +
      iconClass +
      '" aria-hidden="true"></span>' +
      '<div class="oe-bf-card__body">' +
      (step ? '<div class="oe-bf-step">' + escapeHtml(step) + "</div>" : "") +
      "<h3>" +
      escapeHtml(displayTitle) +
      "</h3><p>" +
      escapeHtml(description || "") +
      "</p></div></div>"
    );
  }

  function buildFitCriteriaSection(vm) {
    var rows = sectionData(vm, FIELD.fitCriteria);
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        return (
          "<tr><th scope=\"row\">" +
          escapeHtml(row.fitCriteria || row.criteria || "") +
          "</th><td>" +
          escapeHtml(row.operatorLooksFor || row.looksFor || row.description || "") +
          "</td><td><span class=\"" +
          importanceClass(row.importance) +
          "\">" +
          escapeHtml(row.importance || "") +
          "</span></td></tr>"
        );
      })
      .join("");
    return wrapSection(
      "Operator Fit Criteria",
      "What this operator typically weighs when learning about a new owner partnership—so you know what matters in early conversations.",
      '<div class="oe-bf-table-wrap"><table class="oe-bf-table"><thead><tr><th scope="col">Fit criteria</th><th scope="col">What matters for your project</th><th scope="col">Importance</th></tr></thead><tbody>' +
        body +
        "</tbody></table></div>",
      "oe-bf-section--criteria"
    );
  }

  function buildBestFitProjectTypesSection(vm) {
    var rows = sectionData(vm, FIELD.bestFitProjectTypes);
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        return (
          "<tr><td><span class=\"" +
          fitLevelClass(row.fitLevel) +
          "\">" +
          escapeHtml(row.fitLevel || "") +
          "</span></td><td>" +
          escapeHtml(row.projectType || row.type || "") +
          "</td><td>" +
          escapeHtml(row.ownerContext || row.context || "") +
          "</td></tr>"
        );
      })
      .join("");
    return wrapSection(
      "Best-Fit Project Types",
      "Project types where this operator tends to bring the deepest experience—and where your asset may involve more tailored discussion.",
      '<div class="oe-bf-table-wrap"><table class="oe-bf-table"><thead><tr><th scope="col">Fit level</th><th scope="col">Project type</th><th scope="col">Owner-relevant context</th></tr></thead><tbody>' +
        body +
        "</tbody></table></div>",
      "oe-bf-section--types"
    );
  }

  function buildPreferredDealProfileSection(vm) {
    var rows = sectionData(vm, FIELD.preferredDealProfile);
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        return (
          "<tr><th scope=\"row\">" +
          escapeHtml(row.label || row.key || "") +
          "</th><td>" +
          escapeHtml(row.value || row.description || "") +
          "</td></tr>"
        );
      })
      .join("");
    return wrapSection(
      "Preferred Deal Profile",
      "Typical owner, agreement, and project characteristics that align well with how this operator partners with ownership.",
      '<div class="oe-bf-table-wrap"><table class="oe-bf-table oe-bf-table--two-col"><tbody>' +
        body +
        "</tbody></table></div>",
      "oe-bf-section--profile"
    );
  }

  function buildEvaluationPathSection(vm) {
    var rows = sectionData(vm, FIELD.evaluationPath);
    if (!rows.length) return "";
    return wrapSection(
      "Evaluation Path",
      "How conversations with this operator often progress from first introduction through proposal and agreed next steps.",
      '<div class="grid-3 oe-bf-eval-grid">' +
        rows
          .map(function (row, i) {
            return cardIconCard(
              row.title || row.step,
              row.description,
              "oe-bf-eval-card",
              "oe-bf-eval-card__icon",
              String(i + 1) + "."
            );
          })
          .join("") +
        "</div>",
      "oe-bf-section--evaluation"
    );
  }

  function buildRedFlagsSection(vm) {
    var rows = sectionData(vm, FIELD.redFlags);
    if (!rows.length) return "";
    return wrapSection(
      "Potential Red Flags / Poor-Fit Signals",
      "Topics that often deserve early clarity in the conversation—not reasons to avoid outreach on their own.",
      '<div class="grid-3 oe-bf-red-grid">' +
        rows
          .map(function (row) {
            return cardIconCard(
              row.title,
              row.description,
              "oe-bf-red-card",
              "oe-bf-red-card__icon",
              ""
            );
          })
          .join("") +
        "</div>",
      "oe-bf-section--red-flags"
    );
  }

  function toRoomRange(vm) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var min = nz(pick(ex, p, "minPropertySize"));
    var max = nz(pick(ex, p, "maxPropertySize"));
    if (!min && !max) return "120-350";
    if (min && max) return min + "-" + max;
    return min || max;
  }

  function parseMillions(v) {
    var s = nz(v);
    var m = s.match(/(\$?\s*\d+(?:\.\d+)?\s*[Mm]?)/);
    return m ? m[1].replace(/\s+/g, "") + "+" : "";
  }

  function bestFitTypeCount(vm) {
    var rows = sectionData(vm, FIELD.bestFitProjectTypes);
    if (!rows.length) return "";
    var n = rows.filter(function (r) {
      return nz(r.fitLevel).toLowerCase().indexOf("limited") < 0;
    }).length;
    return String(n || rows.length);
  }

  function deriveSnapshotMetrics(vm) {
    vm = vm || {};
    var ex = vm.ex || {};
    var p = vm.prefill || {};
    var criteriaCount = String(sectionData(vm, FIELD.fitCriteria).length || 0);
    var geos = (p.bestFitGeographies || p.priorityMarkets || []).length;
    var timeline =
      scalarData(vm, FIELD.evaluationTimeline) ||
      nz(pick(ex, p, "bf_signal_transition")) ||
      "60-90 days";
    var dealSize =
      parseMillions(pick(ex, p, "bf_signal_dealsize")) ||
      parseMillions(nz(p.portfolioValue)) ||
      "$15M+";
    var geosVal = geos ? String(geos) : "3";

    function row(labelLines, value, note) {
      var s = nz(value);
      if (!s) return null;
      var lines = Array.isArray(labelLines) ? labelLines : [labelLines];
      return { label: lines.join(" "), labelLines: lines, value: s, note: note };
    }

    return [
      row(
        ["Preferred", "Room Count"],
        toRoomRange(vm),
        "Room-count range most common in this operator's portfolio and representative deal experience"
      ),
      row(
        ["Indicative", "Project Size"],
        dealSize,
        "Deal sizes where this operator usually staffs full operating and owner-support resources"
      ),
      row(
        ["Best-Fit", "Project Types"],
        bestFitTypeCount(vm),
        "Project types most represented in this operator's experience and market positioning"
      ),
      row(
        ["Primary Fit", "Criteria"],
        criteriaCount,
        "Topics this operator usually explores when understanding a new owner partnership"
      ),
      row(
        ["Priority", "Geographies"],
        geosVal,
        "Markets where this operator reports the strongest bench and local operating depth"
      ),
      row(
        ["Typical", "Evaluation Path"],
        timeline,
        "Typical timing from first conversation to proposal and alignment on next steps"
      ),
    ].filter(Boolean);
  }

  function buildAllSectionsHtml(vm) {
    return (
      buildFitCriteriaSection(vm) +
      buildBestFitProjectTypesSection(vm) +
      buildPreferredDealProfileSection(vm) +
      buildEvaluationPathSection(vm) +
      buildRedFlagsSection(vm)
    );
  }

  global.OperatorBestFitDealProfileSections = {
    buildAllSectionsHtml: buildAllSectionsHtml,
    buildOverviewBestFitOwnerProjectSection: buildOverviewBestFitOwnerProjectSection,
    deriveOverviewBestFitRows: deriveOverviewBestFitRows,
    deriveSnapshotMetrics: deriveSnapshotMetrics,
    FIELD: FIELD,
    DEFAULTS: DEFAULTS,
  };
})(typeof globalThis !== "undefined" ? globalThis : global);
