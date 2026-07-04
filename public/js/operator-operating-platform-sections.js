/**
 * Operating Platform — capability pillar subsections (Explorer / DNA).
 * Up to six custom title + description tiles per pillar from operator Setup (op_* JSON or fallbacks).
 * See docs/operator-operating-platform-airtable-fields.md
 */
(function (global) {
  "use strict";

  var FIELD = {
    commercialEngine: "op_commercial_engine_json",
    ownerReporting: "op_owner_reporting_json",
    preOpening: "op_preopening_transition_json",
    conversion: "op_conversion_repositioning_json",
    fbResort: "op_fb_lifestyle_resort_json",
  };

  var FIELD_BY_ID = {
    commercial: FIELD.commercialEngine,
    reporting: FIELD.ownerReporting,
    preopening: FIELD.preOpening,
    conversion: FIELD.conversion,
    fb: FIELD.fbResort,
  };

  var PILLAR_ORDER = ["commercial", "reporting", "preopening", "conversion", "fb"];

  /** API pillar keys (child table) → Explorer UI pillar ids */
  var SECTION_KEY_BY_UI_ID = {
    commercial: "commercialEngine",
    reporting: "ownerReporting",
    preopening: "preOpeningTransition",
    conversion: "conversionRepositioning",
    fb: "fbLifestyleResort",
    operational: "operationalExecutionLabor",
    procurement: "procurementCostControl",
    sales: "salesMarketingActivation",
    engineering: "engineeringPropertyCare",
    portfolio: "portfolioMultiProperty",
    technology: "technologyEnabledOperations",
  };

  var MAX_PILLAR_ITEMS = 6;

  /** Read order: op_* JSON first, then legacy multiline cap_* mirrors. */
  var PILLAR_RAW_FIELD_KEYS = {
    commercial: ["op_commercial_engine_json", "cap_profile_commercial"],
    reporting: ["op_owner_reporting_json", "cap_card_governance"],
    preopening: ["op_preopening_transition_json", "cap_profile_transition"],
    conversion: ["op_conversion_repositioning_json", "cap_deep_revenue_systems"],
    fb: ["op_fb_lifestyle_resort_json", "cap_card_service_diff"],
  };

  var SMALL_WORDS = {
    a: true,
    an: true,
    the: true,
    and: true,
    or: true,
    of: true,
    for: true,
    to: true,
    in: true,
    on: true,
    at: true,
    by: true,
  };

  var DEFAULTS = {
    commercial: {
      id: "commercial",
      title: "Commercial Engine",
      description:
        "Drives top-line performance through disciplined revenue strategy, pricing, distribution, and direct-booking focus.",
      items: [
        {
          title: "Revenue Management",
          description:
            "Pricing, demand forecasting, and yield strategies that align rate, channel, and occupancy with market opportunity.",
        },
        {
          title: "Sales Strategy",
          description:
            "Group, corporate, and leisure sales plans that build productive demand and protect rate integrity.",
        },
        {
          title: "Distribution Strategy",
          description:
            "Channel mix, parity controls, and connectivity that balance reach, cost, and brand standards.",
        },
        {
          title: "Pricing Discipline",
          description:
            "Rate fences, BAR integrity, and restriction management that protect positioning while driving conversion.",
        },
        {
          title: "Direct Booking",
          description:
            "Website, CRM, and loyalty levers that grow owned-channel share and reduce third-party dependency.",
        },
        {
          title: "Forecasting",
          description:
            "Rolling forecasts and pickup reviews that connect commercial plans to staffing, purchasing, and owner expectations.",
        },
      ],
    },
    reporting: {
      id: "reporting",
      title: "Owner Reporting & Communication",
      description:
        "Transparent, proactive governance and reporting that keeps owners informed and in control.",
      items: [
        {
          title: "Reporting Cadence",
          description:
            "Predictable rhythm of financial and operating reports aligned to your approval and review needs.",
        },
        {
          title: "Monthly Business Reviews",
          description:
            "Structured performance discussions covering results, risks, actions, and forward outlook.",
        },
        {
          title: "Dashboards & KPIs",
          description:
            "Owner-facing views of the metrics that matter most for asset health and decision-making.",
        },
        {
          title: "CapEx Visibility",
          description:
            "Clear tracking of capital plans, approvals, spend, and project status against owner priorities.",
        },
        {
          title: "Budget Process",
          description:
            "Annual and rolling budget development, owner review, and variance explanation.",
        },
        {
          title: "Responsiveness Standards",
          description:
            "Defined response times and escalation paths for owner questions and material issues.",
        },
      ],
    },
    preopening: {
      id: "preopening",
      title: "Pre-Opening & Transition Support",
      description:
        "End-to-end opening and transition support from planning to stabilized operations.",
      items: [
        {
          title: "Recruiting",
          description:
            "Sourcing and hiring qualified leadership and hourly staff so the property is ready for launch.",
        },
        {
          title: "Procurement",
          description:
            "Strategic sourcing and purchase of operating supplies, equipment, and opening inventories.",
        },
        {
          title: "Systems Setup",
          description:
            "Implementation and configuration of PMS, RMS, POS, finance, and reporting technology.",
        },
        {
          title: "Transition Planning",
          description:
            "Roadmap from handover through soft opening to stabilized operations with clear milestones.",
        },
        {
          title: "Training",
          description:
            "Structured onboarding for management and staff on standards, systems, and service delivery.",
        },
        {
          title: "Opening Support",
          description:
            "On-site operational guidance during launch to resolve issues and protect guest experience.",
        },
      ],
    },
    conversion: {
      id: "conversion",
      title: "Conversion & Repositioning",
      description:
        "Proven ability to reposition assets and unlock value through thoughtful operational change.",
      items: [
        {
          title: "Brand Transitions",
          description:
            "Management of conversion timelines, brand standards, and owner communication through a reflag.",
        },
        {
          title: "PIP Execution",
          description:
            "Oversight of property improvement plans to meet brand requirements on schedule and budget.",
        },
        {
          title: "Renovation Coordination",
          description:
            "Scheduling and operational planning that limits disruption while work is in progress.",
        },
        {
          title: "Operational Turnaround",
          description:
            "Targeted changes to service, staffing, and commercial levers on underperforming assets.",
        },
        {
          title: "Reopening Ramp",
          description:
            "Phased staffing, marketing, and rate strategy after closure or major renovation.",
        },
        {
          title: "Stabilization",
          description:
            "Consistent operating rhythm and KPI discipline once the asset reaches steady state.",
        },
      ],
    },
    fb: {
      id: "fb",
      title: "F&B, Lifestyle & Resort Capability",
      description:
        "Elevated resort programming and F&B that drive guest satisfaction, spend, and local relevance.",
      items: [
        {
          title: "Restaurant Concepts",
          description:
            "Development and operation of dining venues aligned to positioning, demand, and margin goals.",
        },
        {
          title: "Beach & Pool Operations",
          description:
            "Safe, service-oriented outdoor programming that supports resort positioning and guest spend.",
        },
        {
          title: "Programming & Activities",
          description:
            "Curated guest activities and events that differentiate the stay and support ancillary revenue.",
        },
        {
          title: "Local Partnerships",
          description:
            "Collaborations with area vendors and experiences that add authenticity and guest value.",
        },
        {
          title: "Spa & Wellness",
          description:
            "Treatment menus, staffing, and retail that fit the asset profile and owner return expectations.",
        },
        {
          title: "Guest Experience Design",
          description:
            "End-to-end journey design across touchpoints so the property feels cohesive and memorable.",
        },
      ],
    },
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

  function arrayish(v) {
    if (Array.isArray(v)) return v.map(nz).filter(Boolean);
    if (v == null || v === "") return [];
    return String(v)
      .split(/[,;|\n]+/)
      .map(function (s) {
        return nz(s);
      })
      .filter(Boolean);
  }

  function itemKey(title) {
    return nz(title)
      .toLowerCase()
      .replace(/[^a-z0-9&]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function isJunkCapabilityLabel(label) {
    var t = nz(label);
    if (!t || t.length < 4) return true;
    if (/^\d+(\.\d+)?$/.test(t)) return true;
    if (!/[a-zA-Z]{2,}/.test(t)) return true;
    if (
      /^(not\s*measured|n\/a|unknown|none\s*documented|not\s*yet\s*provided)$/i.test(
        t
      )
    ) {
      return true;
    }
    if (
      /^(moderate|strong|limited|advanced|excellent|proven|emerging|developing|basic|standard|low|high|very\s*strong|institutional|property[-\s]level|centralized|partner[-\s]led)$/i.test(
        t
      )
    ) {
      return true;
    }
    return false;
  }

  function toProperCaseTitle(title) {
    var t = nz(title);
    if (!t) return "";
    var parts = t.split(/\s+/);
    return parts
      .map(function (word, index) {
        if (!word) return "";
        var lower = word.toLowerCase();
        if (index > 0 && SMALL_WORDS[lower]) return lower;
        if (word.indexOf("&") >= 0) {
          return word
            .split("&")
            .map(function (seg) {
              var s = nz(seg);
              if (!s) return "";
              return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
            })
            .join(" & ");
        }
        if (/^[fF]&[bB]$/.test(word)) return "F&B";
        if (word.length <= 3 && word === word.toUpperCase()) return word;
        return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
      })
      .join(" ");
  }

  function linesFromText(t) {
    var fromArray = arrayish(t);
    if (fromArray.length) return fromArray;
    return String(t || "")
      .split(/\r?\n/)
      .map(function (s) {
        return nz(s);
      })
      .filter(Boolean);
  }

  function normalizeCustomItem(raw) {
    if (!raw) return null;
    if (typeof raw === "object" && !Array.isArray(raw)) {
      var objTitle = toProperCaseTitle(raw.title || raw.label || raw.name || "");
      if (!objTitle || isJunkCapabilityLabel(objTitle)) return null;
      return {
        title: objTitle,
        description: nz(raw.description || raw.detail || raw.body || raw.text),
      };
    }
    var line = nz(raw);
    if (!line || isJunkCapabilityLabel(line)) return null;
    var colon = line.indexOf(":");
    if (colon > 2 && colon < 72) {
      var left = toProperCaseTitle(line.slice(0, colon).trim());
      var right = nz(line.slice(colon + 1));
      if (left && !isJunkCapabilityLabel(left)) {
        return { title: left, description: right };
      }
    }
    return { title: toProperCaseTitle(line), description: "" };
  }

  function uniqueItems(items) {
    var seen = {};
    return (items || []).filter(function (row) {
      if (!row || !nz(row.title)) return false;
      var k = itemKey(row.title);
      if (!k || seen[k]) return false;
      seen[k] = true;
      return true;
    });
  }

  function parseJsonObject(raw) {
    if (!raw) return null;
    if (typeof raw === "object" && !Array.isArray(raw)) return raw;
    try {
      var parsed = JSON.parse(String(raw));
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
    } catch (e) {
      return null;
    }
  }

  function itemsFromJsonPayload(obj) {
    if (!obj) return [];
    if (Array.isArray(obj.items) && obj.items.length) {
      return uniqueItems(
        obj.items.map(normalizeCustomItem).filter(Boolean).slice(0, MAX_PILLAR_ITEMS)
      );
    }
    return uniqueItems(
      arrayish(obj.bullets || obj.capabilities)
        .map(normalizeCustomItem)
        .filter(Boolean)
        .slice(0, MAX_PILLAR_ITEMS)
    );
  }

  function multilineFieldToItems(raw) {
    return uniqueItems(
      linesFromText(raw).map(normalizeCustomItem).filter(Boolean).slice(0, MAX_PILLAR_ITEMS)
    );
  }

  function offeredCustomItemsForPillar(p, pillarId) {
    var offered = arrayish(p.offeredServices);
    var patterns = {
      commercial: /revenue|sales|marketing|commercial|distribution|pricing|forecast/i,
      reporting: /report|governance|owner|budget|capex|dashboard|review/i,
      preopening: /pre-?opening|transition|opening|recruit|procurement|systems|training/i,
      conversion: /conversion|reflag|reposition|turnaround|pip|renovation|stabiliz/i,
      fb: /f&b|food|beverage|resort|lifestyle|spa|restaurant|pool|programming|wellness/i,
    };
    var re = patterns[pillarId];
    if (!re) return [];
    return uniqueItems(
      offered
        .filter(function (s) {
          return re.test(s);
        })
        .map(normalizeCustomItem)
        .filter(Boolean)
        .slice(0, MAX_PILLAR_ITEMS)
    );
  }

  function capabilityById(vm, id) {
    var caps = vm && vm.capabilities;
    if (!Array.isArray(caps)) return null;
    for (var i = 0; i < caps.length; i++) {
      if (caps[i] && caps[i].id === id) return caps[i];
    }
    return null;
  }

  function pickPillarRaw(vm, pillarId) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var keys = PILLAR_RAW_FIELD_KEYS[pillarId] || [];
    var i;
    for (i = 0; i < keys.length; i++) {
      var raw = pick(ex, p, keys[i], "");
      if (nz(raw)) return raw;
    }
    return "";
  }

  function parsePillarSetupPayload(raw) {
    if (!nz(raw)) return { intro: "", items: [] };
    var obj = parseJsonObject(raw);
    if (obj) {
      return {
        intro: nz(obj.description) || nz(obj.intro) || "",
        items: itemsFromJsonPayload(obj),
        sectionTitle: toProperCaseTitle(obj.title || ""),
      };
    }
    return {
      intro: "",
      items: multilineFieldToItems(raw),
      sectionTitle: "",
    };
  }

  function itemsFromCapability(cap) {
    if (!cap) return [];
    return uniqueItems(
      arrayish(cap.bullets).map(normalizeCustomItem).filter(Boolean).slice(0, MAX_PILLAR_ITEMS)
    );
  }

  function resolveCustomPillarItems(vm, pillarId) {
    var p = (vm && vm.prefill) || {};
    var items = [];
    var setup = parsePillarSetupPayload(pickPillarRaw(vm, pillarId));
    if (setup.items.length) items = setup.items;

    if (!items.length) {
      var cap = capabilityById(vm, pillarId);
      if (cap) items = itemsFromCapability(cap);
    }

    if (!items.length) {
      items = offeredCustomItemsForPillar(p, pillarId);
    }

    return {
      intro: setup.intro,
      sectionTitle: setup.sectionTitle,
      items: items.slice(0, MAX_PILLAR_ITEMS),
    };
  }

  function operatingPlatformFromVm(vm) {
    if (vm && vm.operatingPlatform && typeof vm.operatingPlatform === "object") {
      return vm.operatingPlatform;
    }
    if (vm && vm.prefill && vm.prefill.operatingPlatform && typeof vm.prefill.operatingPlatform === "object") {
      return vm.prefill.operatingPlatform;
    }
    return null;
  }

  function pillarFromOperatingPlatform(platform, uiId) {
    if (!platform || !platform.pillars) return null;
    var sectionKey = SECTION_KEY_BY_UI_ID[uiId];
    if (!sectionKey) return null;
    var pillar = platform.pillars[sectionKey];
    if (!pillar || typeof pillar !== "object") return null;
    var items = Array.isArray(pillar.items)
      ? pillar.items
          .map(function (item) {
            return normalizeCustomItem({
              title: item && item.title,
              description: item && item.description,
            });
          })
          .filter(Boolean)
          .slice(0, MAX_PILLAR_ITEMS)
      : [];
    if (!nz(pillar.description) && !items.length) return null;
    return {
      intro: nz(pillar.description),
      sectionTitle: nz(pillar.title),
      items: items,
    };
  }

  function mergePillar(vm, id) {
    var base = DEFAULTS[id] || { id: id, title: "", description: "", items: [] };
    var fromCap = capabilityById(vm, id);
    var fromPlatform = pillarFromOperatingPlatform(operatingPlatformFromVm(vm), id);
    var custom = fromPlatform || resolveCustomPillarItems(vm, id);
    var description =
      custom.intro ||
      (fromCap && (nz(fromCap.description) || nz(fromCap.evidence))) ||
      base.description ||
      "";
    var title =
      custom.sectionTitle ||
      (fromCap && toProperCaseTitle(fromCap.title)) ||
      base.title;
    var items = custom.items && custom.items.length ? custom.items : [];

    if (!items.length && !(fromPlatform && fromPlatform.intro)) {
      items = (base.items || []).slice(0, MAX_PILLAR_ITEMS);
    }

    return {
      id: id,
      title: toProperCaseTitle(title),
      description: description,
      items: items,
    };
  }

  function wrapSection(title, intro, bodyHtml, extraClass) {
    if (!bodyHtml) return "";
    return (
      '<section class="section oe-op-section' +
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

  function capabilityTile(row) {
    if (!row || !nz(row.title)) return "";
    var title =
      global.OperatorExplorerCardTitle && typeof global.OperatorExplorerCardTitle.formatCardTitle === "function"
        ? global.OperatorExplorerCardTitle.formatCardTitle(row.title)
        : row.title;
    return (
      '<div class="card oe-op-check-tile">' +
      '<span class="oe-op-check-tile__icon" aria-hidden="true"></span>' +
      '<div class="oe-op-check-tile__body">' +
      "<h3>" +
      escapeHtml(title) +
      "</h3>" +
      (nz(row.description)
        ? "<p>" + escapeHtml(row.description) + "</p>"
        : '<p class="oe-op-check-tile__empty-desc">Add a short description in Operator Setup.</p>') +
      "</div></div>"
    );
  }

  function buildPillarSection(vm, id) {
    var pillar = mergePillar(vm, id);
    if (!pillar || !nz(pillar.title)) return "";
    var tiles = (pillar.items || []).map(capabilityTile).filter(Boolean).join("");
    if (!tiles) {
      return wrapSection(
        pillar.title,
        pillar.description,
        '<p class="gold-mock-tab-empty">Add up to six custom capability titles and descriptions for this pillar in Operator Setup (see docs/operator-operating-platform-airtable-fields.md).</p>',
        "oe-op-section--" + id
      );
    }
    return wrapSection(
      pillar.title,
      pillar.description,
      '<div class="grid-2 oe-op-pillar-grid">' + tiles + "</div>",
      "oe-op-section--" + id
    );
  }

  function buildAllSectionsHtml(vm) {
    return PILLAR_ORDER.map(function (id) {
      return buildPillarSection(vm, id);
    }).join("");
  }

  global.OperatorOperatingPlatformSections = {
    buildAllSectionsHtml: buildAllSectionsHtml,
    FIELD: FIELD,
    FIELD_BY_ID: FIELD_BY_ID,
    DEFAULTS: DEFAULTS,
    PILLAR_ORDER: PILLAR_ORDER,
    PILLAR_RAW_FIELD_KEYS: PILLAR_RAW_FIELD_KEYS,
    MAX_PILLAR_ITEMS: MAX_PILLAR_ITEMS,
    mergePillar: mergePillar,
    resolveCustomPillarItems: resolveCustomPillarItems,
    toProperCaseTitle: toProperCaseTitle,
  };
})(typeof window !== "undefined" ? window : global);
