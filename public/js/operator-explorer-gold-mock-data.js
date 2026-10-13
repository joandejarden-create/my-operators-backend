/**
 * Operator Explorer Gold Mock — view model + panel HTML from finalized Operator Setup
 * (prefill + explorerProfileJson + Master-linked tables via /api/intake/third-party-operators/:id).
 */
(function (global) {
  var TABS = [
    "Profile & Positioning",
    "Operating Platform",
    "Brand & Relationships",
    "Markets & Footprint",
    "Owner Engagement & Reporting",
    "Infrastructure & Data",
    "Leadership",
    "Project Fit & Deal Profile",
    "Proof & Track Record",
  ];

  var TAB_ICONS = {
    "Profile & Positioning":
      '<svg viewBox="0 0 24 24"><path d="M3 9.5L12 3l9 6.5"></path><path d="M5 10v10h14V10"></path></svg>',
    "Operating Platform":
      '<svg viewBox="0 0 24 24"><path d="M4 6h16"></path><path d="M4 12h16"></path><path d="M4 18h10"></path></svg>',
    "Brand & Relationships":
      '<svg viewBox="0 0 24 24"><path d="M7 4h10l3 4-8 12L4 8z"></path></svg>',
    "Markets & Footprint":
      '<svg viewBox="0 0 24 24"><path d="M21 10c0 7-9 11-9 11s-9-4-9-11a9 9 0 0 1 18 0z"></path><circle cx="12" cy="10" r="2.5"></circle></svg>',
    "Owner Engagement & Reporting":
      '<svg viewBox="0 0 24 24"><path d="M12 1v22"></path><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 1 1 1 0 7H6"></path></svg>',
    "Infrastructure & Data":
      '<svg viewBox="0 0 24 24"><path d="M3 21h18"></path><path d="M6 21V7l6-4 6 4v14"></path><path d="M10 11h4"></path><path d="M10 15h4"></path></svg>',
    Leadership:
      '<svg viewBox="0 0 24 24"><circle cx="12" cy="8" r="4"></circle><path d="M4 21c1.5-4 5-6 8-6s6.5 2 8 6"></path></svg>',
    "Project Fit & Deal Profile":
      '<svg viewBox="0 0 24 24"><path d="M3 12l6 6 12-12"></path></svg>',
    "Proof & Track Record":
      '<svg viewBox="0 0 24 24"><path d="M4 19h16"></path><path d="M7 16V9"></path><path d="M12 16V5"></path><path d="M17 16v-4"></path></svg>',
  };

  /** Display HTML for tab labels — matches Operator Setup section nav (UPPERCASE + explicit line breaks). */
  var TAB_LABEL_HTML = {
    "Profile & Positioning": "Profile &<br>Positioning",
    "Operating Platform": "Operating<br>Platform",
    "Brand & Relationships": "Brand &<br>Relationships",
    "Markets & Footprint": "Markets &<br>Footprint",
    "Owner Engagement & Reporting": "Engagement &<br>Reporting",
    "Infrastructure & Data": "Infrastructure<br>&amp; Data",
    Leadership: "Leadership<br>&amp; Team",
    "Project Fit & Deal Profile": "Project Fit &<br>Deal Profile",
    "Proof & Track Record": "Proof &<br>Track Record",
  };

  var PLACEHOLDER_PROOF =
    "https://images.unsplash.com/photo-1566073771259-6a8506099945?auto=format&fit=crop&w=1200&q=80";
  var PLACEHOLDER_LEADER =
    "https://images.unsplash.com/photo-1560250097-0b93528c311a?auto=format&fit=crop&w=900&q=80";

  function escapeAttr(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function escapeHtml(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function nz(v) {
    return v != null && String(v).trim() !== "" ? String(v).trim() : "";
  }

  function isInternalFillPlaceholder(v) {
    var t = nz(v);
    if (!t) return false;
    return (
      /^Select deal types Arbor pursues in CALA/i.test(t) ||
      /^Choose values that match the CALA deals you pursue/i.test(t) ||
      /^\[Internal fill guidance/i.test(t) ||
      /^Mirror CALA leadership/i.test(t) ||
      /^mirror your top three leaders/i.test(t) ||
      (/Leadership Team Members:/i.test(t) && t.length > 72)
    );
  }

  function pickOwnerFacing(ex, p, key) {
    var v = pick(ex, p, key);
    return isInternalFillPlaceholder(v) ? "" : nz(v);
  }

  var CROSS_BRAND_SELECT_OPTIONS = [
    "Very Strong",
    "Strong",
    "High",
    "Moderate-High",
    "Medium",
    "Low",
    "Emerging",
  ];

  var HOTEL_BRAND_TOKEN_RE =
    /\b(marriott|hilton|hyatt|ihg|choice|accor|starwood|sheraton|westin|curio|aloft|hampton|holiday inn|hotel indigo|radisson|four seasons|kimpton|independent)\b/gi;

  function normalizeCrossBrandSignal(raw, leaders, vm) {
    var s = isInternalFillPlaceholder(raw) ? "" : nz(raw);
    if (s) {
      var match = CROSS_BRAND_SELECT_OPTIONS.find(function (opt) {
        return opt.toLowerCase() === s.toLowerCase();
      });
      if (match) return match;
      if (s.length <= 24) return s;
    }
    var brands = {};
    (leaders || []).forEach(function (L) {
      var text = [L.priorBackground, L.coreExpertise, L.relevantAssetTypes]
        .map(nz)
        .join(" ");
      var m;
      HOTEL_BRAND_TOKEN_RE.lastIndex = 0;
      while ((m = HOTEL_BRAND_TOKEN_RE.exec(text)) !== null) {
        brands[m[1].toLowerCase()] = true;
      }
    });
    var brandCount = Object.keys(brands).length;
    if (brandCount >= 4) return "Very Strong";
    if (brandCount >= 3) return "High";
    if (brandCount >= 2) return "Moderate-High";
    var p = (vm && vm.prefill) || {};
    var brandList = nz(p.brands || p.additionalBrands);
    if (brandList) {
      var parts = brandList.split(/[,;|]+/).filter(function (x) {
        return nz(x);
      });
      if (parts.length >= 5) return "High";
      if (parts.length >= 2) return "Moderate-High";
    }
    return "—";
  }

  function arrayishWithoutPlaceholders(v) {
    return arrayish(v).filter(function (item) {
      return !isInternalFillPlaceholder(item);
    });
  }

  /** Same mapping as operator-explorer.html `getChainScaleColor`. */
  function chainScaleLabelToColor(scale) {
    if (!scale) return null;
    var s = String(scale).toLowerCase();
    if (s.indexOf("luxury") !== -1) return "#d4af37";
    if (s.indexOf("upper upscale") !== -1) return "#9b59b6";
    if (s.indexOf("upscale") !== -1 && s.indexOf("upper") === -1) return "#3498db";
    if (s.indexOf("upper midscale") !== -1) return "#2ecc71";
    if (s.indexOf("midscale") !== -1) return "#1abc9c";
    if (s.indexOf("economy") !== -1) return "#e67e22";
    if (s.indexOf("independent") !== -1) return "#94a3b8";
    return null;
  }

  /** Same rules as operator-explorer.html `getChainScaleStripeBackground` (vertical bar on the tile). */
  function chainScaleStripeBackgroundFromScales(chainScales) {
    var colors = [];
    (chainScales || []).forEach(function (scale) {
      var c = chainScaleLabelToColor(scale);
      if (c && colors.indexOf(c) === -1) colors.push(c);
    });
    if (colors.length === 0) return null;
    if (colors.length === 1) return colors[0];
    var step = 100 / colors.length;
    var stops = colors.map(function (color, index) {
      var start = (index * step).toFixed(3);
      var end = ((index + 1) * step).toFixed(3);
      return color + " " + start + "%, " + color + " " + end + "%";
    }).join(", ");
    return "linear-gradient(to bottom, " + stops + ")";
  }

  function applyHeroStripeFromChainScales(chainScales) {
    var root = document.documentElement;
    var bg = chainScaleStripeBackgroundFromScales(chainScales);
    if (!bg) {
      root.style.removeProperty("--hero-stripe-bg");
      return;
    }
    root.style.setProperty("--hero-stripe-bg", bg);
  }

  function applyHeroStripeFromHex(hex) {
    var h = String(hex || "").replace(/^#/, "");
    var root = document.documentElement;
    if (!/^[0-9a-fA-F]{6}$/.test(h)) {
      root.style.removeProperty("--hero-stripe-bg");
      return;
    }
    root.style.setProperty("--hero-stripe-bg", "#" + h);
  }

  function linesFromText(t) {
    if (!t) return [];
    return String(t)
      .split(/\r?\n/)
      .map(function (x) {
        return x.trim();
      })
      .filter(Boolean);
  }

  function pick(ex, prefill, key, fallback) {
    var a = nz(ex[key]);
    if (a) return a;
    var b = nz(prefill[key]);
    if (b) return b;
    return fallback != null ? String(fallback) : "";
  }

  function mergeExplorerPrefill(prefill) {
    var p = prefill || {};
    var json = {};
    if (p.explorerProfileJson && typeof p.explorerProfileJson === "string") {
      try {
        json = JSON.parse(p.explorerProfileJson) || {};
      } catch (e) {
        json = {};
      }
    }
    var out = Object.assign({}, json);
    var prefixes = [
      "overview_",
      "cap_",
      "brand_",
      "mkt_",
      "ov_",
      "infra_",
      "risk_",
      "lead_",
      "bf_",
      "tr_",
      "systems_",
      "exec_",
      "op_",
    ];
    Object.keys(p).forEach(function (k) {
      if (k === "explorerProfileJson") return;
      if (
        k === "marketDepthOptIn" ||
        k === "displayLeadershipOnExplorer" ||
        k === "ownerEngagementNarrative" ||
        prefixes.some(function (pr) {
          return k.indexOf(pr) === 0;
        })
      ) {
        if (p[k] != null && p[k] !== "") out[k] = p[k];
      }
    });
    if (global.OperatorExplorerNewBaseProfile && global.OperatorExplorerNewBaseProfile.mergeNewBaseKeysIntoExplorer) {
      return global.OperatorExplorerNewBaseProfile.mergeNewBaseKeysIntoExplorer(out, p);
    }
    return out;
  }

  function firstAttachmentUrl(raw) {
    if (!raw) return "";
    if (typeof raw === "string" && /^https?:\/\//i.test(raw)) return raw;
    if (Array.isArray(raw) && raw[0] && raw[0].url) return String(raw[0].url);
    return "";
  }

  function arrayish(val) {
    if (val == null) return [];
    if (Array.isArray(val)) return val.map(String).filter(nz);
    return String(val)
      .split(/[,;\n]/)
      .map(function (x) {
        return x.trim();
      })
      .filter(Boolean);
  }

  function formatInt(v) {
    if (v == null || v === "") return "";
    var n = Number(String(v).replace(/,/g, ""));
    return Number.isFinite(n) ? n.toLocaleString() : String(v);
  }

  /** Multi-select or comma-separated lists from Operator Setup */
  function formatMulti(val) {
    if (val == null) return "";
    if (Array.isArray(val)) return val.map(nz).filter(Boolean).join(", ");
    return nz(val);
  }

  /** Fee structure: show category labels only; hide values that look like raw percentages */
  function feeCategoryOnly(v) {
    var s = nz(v);
    if (!s) return "";
    if (/^\s*\d+(\.\d+)?\s*%\s*$/.test(s)) return "";
    if (/^\s*\d+(\.\d+)?\s*-\s*\d+(\.\d+)?\s*%\s*$/.test(s)) return "";
    return s;
  }

  function propertySizeRange(minV, maxV) {
    var a = nz(minV);
    var b = nz(maxV);
    if (!a && !b) return "";
    if (a && b) return a + " – " + b;
    return a || b;
  }

  function numOrEmpty(v) {
    if (v == null || v === "") return null;
    var n = Number(String(v).replace(/,/g, ""));
    return Number.isFinite(n) ? n : null;
  }

  /** Sum geo existing vs pipeline from totals row or regional fields (Operator Setup footprint). */
  function sumGeoExistingPipeline(p) {
    var exH = numOrEmpty(p.geo_total_existing_hotels);
    var exR = numOrEmpty(p.geo_total_existing_rooms);
    var piH = numOrEmpty(p.geo_total_pipeline_hotels);
    var piR = numOrEmpty(p.geo_total_pipeline_rooms);
    if (exH != null || exR != null || piH != null || piR != null) {
      return { exH: exH, exR: exR, piH: piH, piR: piR };
    }
    var regions = ["na", "cala", "eu", "mea", "apac"];
    var sxH = 0;
    var sxR = 0;
    var spH = 0;
    var spR = 0;
    var any = false;
    regions.forEach(function (reg) {
      var a = numOrEmpty(p["geo_" + reg + "_existing_hotels"]);
      var b = numOrEmpty(p["geo_" + reg + "_existing_rooms"]);
      var c = numOrEmpty(p["geo_" + reg + "_pipeline_hotels"]);
      var d = numOrEmpty(p["geo_" + reg + "_pipeline_rooms"]);
      if (a != null) {
        sxH += a;
        any = true;
      }
      if (b != null) {
        sxR += b;
        any = true;
      }
      if (c != null) {
        spH += c;
        any = true;
      }
      if (d != null) {
        spR += d;
        any = true;
      }
    });
    if (!any) return null;
    return { exH: sxH, exR: sxR, piH: spH, piR: spR };
  }

  var CHAIN_SCALE_TIERS = [
    { id: "luxury", label: "Luxury", staffKey: "luxuryAvgStaff" },
    { id: "upperUpscale", label: "Upper Upscale", staffKey: "upperUpscaleAvgStaff" },
    { id: "upscale", label: "Upscale", staffKey: "upscaleAvgStaff" },
    { id: "upperMidscale", label: "Upper Midscale", staffKey: "upperMidscaleAvgStaff" },
    { id: "midscale", label: "Midscale", staffKey: "midscaleAvgStaff" },
    { id: "economy", label: "Economy", staffKey: "economyAvgStaff" },
  ];

  var GEO_REGIONS = [
    { id: "na", label: "North America" },
    { id: "cala", label: "Caribbean & Latin America" },
    { id: "eu", label: "Europe" },
    { id: "mea", label: "Middle East & Africa" },
    { id: "apac", label: "Asia Pacific" },
  ];

  function formatStaffForTable(v) {
    var n = numOrEmpty(v);
    if (n == null) return "—";
    if (Math.abs(n - Math.round(n)) < 1e-9) return formatInt(n);
    return n.toFixed(1);
  }

  /** Operator Setup — Technology Stack & Service Offerings multi-selects (same keys as intake / prefill). */
  var SERVICE_OFFERING_GROUPS = [
    {
      title: "Revenue Management Services",
      key: "revenueManagementServices",
      otherKey: "revenueManagementOther",
    },
    {
      title: "Sales & Marketing Support",
      key: "salesMarketingSupport",
      otherKey: "salesMarketingOther",
    },
    {
      title: "Accounting & Financial Reporting",
      key: "accountingReporting",
      otherKey: "accountingReportingOther",
    },
    {
      title: "Procurement Services",
      key: "procurementServices",
      otherKey: "procurementServicesOther",
    },
    {
      title: "HR & Training Services",
      key: "hrTrainingServices",
      otherKey: "hrTrainingServicesOther",
    },
    {
      title: "Technology Services",
      key: "technologyServices",
      otherKey: "technologyServicesOther",
    },
    {
      title: "Design & Renovation Support",
      key: "designRenovationSupport",
      otherKey: "designRenovationSupportOther",
    },
    {
      title: "Development Services",
      key: "developmentServices",
      otherKey: "developmentServicesOther",
    },
  ];

  function parseMultiSelectList(v) {
    if (v == null || v === "") return [];
    if (Array.isArray(v)) {
      return v
        .map(function (x) {
          return String(x).trim();
        })
        .filter(Boolean);
    }
    if (typeof v === "string") {
      var t = v.trim();
      if (!t) return [];
      if (t.charAt(0) === "[" || t.charAt(0) === "{") {
        try {
          var j = JSON.parse(t);
          if (Array.isArray(j)) {
            return j
              .map(function (x) {
                return String(x).trim();
              })
              .filter(Boolean);
          }
        } catch (e) {}
      }
      return t
        .split(/[,\n]/)
        .map(function (x) {
          return x.trim();
        })
        .filter(Boolean);
    }
    return [String(v)];
  }

  function normalizeServiceOfferingLabel(s) {
    s = String(s || "").trim();
    if (!s) return "";
    var m = s.match(/^(.+)\s+-\s+(.+)$/);
    if (!m) return s;
    var tail = m[2].trim();
    if (
      /revenue management services|sales marketing support|sales & marketing support|accounting reporting|accounting & financial reporting|procurement services|hr training services|hr & training services|technology services|design renovation support|design & renovation support|development services$/i.test(
        tail
      )
    ) {
      return m[1].trim();
    }
    return s;
  }

  function serviceOfferingsSectionHtml(p) {
    var cards = [];
    SERVICE_OFFERING_GROUPS.forEach(function (g) {
      var raw = parseMultiSelectList(p[g.key]);
      var seen = {};
      var labels = [];
      raw.forEach(function (item) {
        var lab = normalizeServiceOfferingLabel(item);
        if (!lab) return;
        var low = lab.toLowerCase();
        if (seen[low]) return;
        seen[low] = true;
        labels.push(lab);
      });
      var otherText = nz(p[g.otherKey]);
      var hasOther = labels.some(function (x) {
        return /^other$/i.test(x);
      });
      if (hasOther) {
        labels = labels.filter(function (x) {
          return !/^other$/i.test(x);
        });
        if (otherText) labels.push("Other: " + otherText);
        else labels.push("Other");
      }
      if (!labels.length) return;
      cards.push(
        '<div class="gold-service-category">' +
          '<h4 class="gold-service-category-title">' +
          escapeHtml(g.title) +
          "</h4>" +
          '<ul class="gold-service-list">' +
          labels
            .map(function (lab) {
              return "<li>" + escapeHtml(lab) + "</li>";
            })
            .join("") +
          "</ul></div>"
      );
    });
    if (!cards.length) return "";
    return (
      '<section class="section">' +
      '<h2 class="section-title">Service Offerings</h2>' +
      '<div class="gold-service-offerings-grid">' +
      cards.join("") +
      "</div></section>"
    );
  }

  function chainScaleRowData(p) {
    var rows = [];
    CHAIN_SCALE_TIERS.forEach(function (t) {
      var ep = numOrEmpty(p[t.id + "ExistingProperties"]) || 0;
      var er = numOrEmpty(p[t.id + "ExistingRooms"]) || 0;
      var pp = numOrEmpty(p[t.id + "PipelineProperties"]) || 0;
      var pr = numOrEmpty(p[t.id + "PipelineRooms"]) || 0;
      var activity = ep + er + pp + pr;
      if (!activity) return;
      var st = t.staffKey ? numOrEmpty(p[t.staffKey]) : null;
      rows.push({
        label: t.label,
        ep: ep,
        er: er,
        pp: pp,
        pr: pr,
        st: st,
      });
    });
    return rows;
  }

  function geoRegionRowData(p) {
    var rows = [];
    GEO_REGIONS.forEach(function (reg) {
      var ep = numOrEmpty(p["geo_" + reg.id + "_existing_hotels"]) || 0;
      var er = numOrEmpty(p["geo_" + reg.id + "_existing_rooms"]) || 0;
      var pp = numOrEmpty(p["geo_" + reg.id + "_pipeline_hotels"]) || 0;
      var pr = numOrEmpty(p["geo_" + reg.id + "_pipeline_rooms"]) || 0;
      var activity = ep + er + pp + pr;
      if (!activity) return;
      rows.push({
        label: reg.label,
        ep: ep,
        er: er,
        pp: pp,
        pr: pr,
        st: null,
      });
    });
    return rows;
  }

  function brandPortfolioRowData(p) {
    var raw = parseBrandsPortfolioDetail(p.brandsPortfolioDetail);
    var rows = [];
    raw.forEach(function (r) {
      var name = nz(r.brand_name) || nz(r.brand_key) || "Brand";
      var ep = numOrEmpty(r.existing_properties) || 0;
      var er = numOrEmpty(r.existing_rooms) || 0;
      var pp = numOrEmpty(r.pipeline_properties) || 0;
      var pr = numOrEmpty(r.pipeline_rooms) || 0;
      var st = numOrEmpty(r.avg_staff);
      var activity = ep + er + pp + pr;
      if (!activity && st == null) return;
      rows.push({
        label: name,
        ep: ep,
        er: er,
        pp: pp,
        pr: pr,
        st: st,
      });
    });
    return rows;
  }

  function footprintNumericBreakdownTableHtml(firstColHeader, rows, emptyMessage) {
    var thead =
      "<thead><tr>" +
      '<th scope="col">' +
      escapeHtml(firstColHeader) +
      "</th>" +
      '<th scope="col">Existing Hotels</th>' +
      '<th scope="col">Existing Rooms</th>' +
      '<th scope="col">Pipeline Hotels</th>' +
      '<th scope="col">Pipeline Rooms</th>' +
      '<th scope="col">Total Hotels</th>' +
      '<th scope="col">Total Rooms</th>' +
      '<th scope="col">Avg Staff / Property</th>' +
      "</tr></thead>";
    if (!rows.length) {
      return (
        "<table class=\"gold-footprint-table\">" +
        thead +
        '<tbody><tr><td colspan="8">' +
        escapeHtml(emptyMessage || "No data for this view.") +
        "</td></tr></tbody></table>"
      );
    }
    var sumEp = 0;
    var sumEr = 0;
    var sumPp = 0;
    var sumPr = 0;
    var body = rows
      .map(function (r) {
        sumEp += r.ep;
        sumEr += r.er;
        sumPp += r.pp;
        sumPr += r.pr;
        var totH = r.ep + r.pp;
        var totR = r.er + r.pr;
        return (
          "<tr>" +
          '<th scope="row">' +
          escapeHtml(r.label) +
          "</th>" +
          "<td>" +
          formatInt(r.ep) +
          "</td><td>" +
          formatInt(r.er) +
          "</td><td>" +
          formatInt(r.pp) +
          "</td><td>" +
          formatInt(r.pr) +
          "</td><td>" +
          formatInt(totH) +
          "</td><td>" +
          formatInt(totR) +
          "</td><td>" +
          formatStaffForTable(r.st) +
          "</td></tr>"
        );
      })
      .join("");
    var totHt = sumEp + sumPp;
    var totRt = sumEr + sumPr;
    var foot =
      '<tr class="gold-ft-total-row">' +
      '<th scope="row">Total</th>' +
      "<td>" +
      formatInt(sumEp) +
      "</td><td>" +
      formatInt(sumEr) +
      "</td><td>" +
      formatInt(sumPp) +
      "</td><td>" +
      formatInt(sumPr) +
      "</td><td>" +
      formatInt(totHt) +
      "</td><td>" +
      formatInt(totRt) +
      '</td><td>—</td></tr>';
    return (
      '<table class="gold-footprint-table">' +
      thead +
      "<tbody>" +
      body +
      foot +
      "</tbody></table>"
    );
  }

  function footprintDistributionBlockHtml(p) {
    var regionRows = geoRegionRowData(p);
    var chainRows = chainScaleRowData(p);
    var brandAll = brandPortfolioRowData(p);
    var brandRows = brandAll.slice(0, 12);
    var brandMore = brandAll.length > 12 ? brandAll.length - 12 : 0;
    if (
      !regionRows.length &&
      !chainRows.length &&
      !brandAll.length
    ) {
      return "";
    }
    var defaultView = "chain";
    if (!chainRows.length && regionRows.length) defaultView = "region";
    else if (!chainRows.length && !regionRows.length && brandAll.length)
      defaultView = "brand";
    var chk = function (v) {
      return defaultView === v ? " checked" : "";
    };
    var regionTable = footprintNumericBreakdownTableHtml(
      "Region",
      regionRows,
      "No regional footprint breakdown in profile."
    );
    var chainTable = footprintNumericBreakdownTableHtml(
      "Chain Scale",
      chainRows,
      "No chain scale breakdown in profile."
    );
    var brandTable = footprintNumericBreakdownTableHtml(
      "Brand",
      brandRows,
      "No brand-level footprint in profile."
    );
    var brandNote = "";
    if (brandMore > 0) {
      brandNote =
        '<p class="gold-footprint-table-note">+' +
        brandMore +
        " more brands in Operator Setup.</p>";
    }
    if (nz(p.footprintPortfolioSource) === "hotel_census") {
      brandNote +=
        '<p class="gold-footprint-table-note">Portfolio distribution from Hotel Census' +
        (nz(p.footprintPortfolioManagementCompany)
          ? " (" + escapeHtml(p.footprintPortfolioManagementCompany) + ")"
          : "") +
        ". Operator Setup values used only when census has no matching properties.</p>";
    }
    return (
      '<div class="gold-footprint-subsection gold-footprint-distribution">' +
      '<div class="gold-footprint-distribution-head">' +
      '<h3 class="gold-footprint-table-title">Portfolio Distribution</h3>' +
      '<p class="gold-footprint-view-hint">View by region, chain scale, or brand.</p>' +
      "</div>" +
      '<input type="radio" name="goldFpView" id="goldFpViewRegion" class="gold-fp-view-input"' +
      chk("region") +
      ">" +
      '<input type="radio" name="goldFpView" id="goldFpViewChain" class="gold-fp-view-input"' +
      chk("chain") +
      ">" +
      '<input type="radio" name="goldFpView" id="goldFpViewBrand" class="gold-fp-view-input"' +
      chk("brand") +
      ">" +
      '<div class="gold-footprint-view-toggle" role="tablist" aria-label="Portfolio distribution view">' +
      '<label class="gold-fp-view-label" for="goldFpViewRegion">Region</label>' +
      '<label class="gold-fp-view-label" for="goldFpViewChain">Chain Scale</label>' +
      '<label class="gold-fp-view-label" for="goldFpViewBrand">Brand</label>' +
      "</div>" +
      '<div class="gold-footprint-table-wrap gold-fp-panel gold-fp-panel-region" role="tabpanel" aria-label="By region">' +
      regionTable +
      "</div>" +
      '<div class="gold-footprint-table-wrap gold-fp-panel gold-fp-panel-chain" role="tabpanel" aria-label="By chain scale">' +
      chainTable +
      "</div>" +
      '<div class="gold-footprint-table-wrap gold-fp-panel gold-fp-panel-brand" role="tabpanel" aria-label="By brand">' +
      brandTable +
      brandNote +
      "</div>" +
      "</div>"
    );
  }

  function existingVsPipelineTableHtml(p) {
    var geo = sumGeoExistingPipeline(p);
    if (!geo) return "";
    var exSum = (geo.exH || 0) + (geo.exR || 0);
    var piSum = (geo.piH || 0) + (geo.piR || 0);
    if (exSum <= 0 && piSum <= 0) return "";
    var eh = formatInt(geo.exH != null ? geo.exH : 0);
    var er = formatInt(geo.exR != null ? geo.exR : 0);
    var ph = formatInt(geo.piH != null ? geo.piH : 0);
    var pr = formatInt(geo.piR != null ? geo.piR : 0);
    return (
      '<div class="gold-footprint-subsection">' +
      '<h3 class="gold-footprint-table-title">Existing vs. Pipeline (Portfolio)</h3>' +
      '<div class="gold-footprint-table-wrap" role="region" aria-label="Existing versus pipeline portfolio totals">' +
      '<table class="gold-footprint-table">' +
      "<thead><tr>" +
      '<th scope="col"></th>' +
      '<th scope="col">Existing Hotels</th>' +
      '<th scope="col">Existing Rooms</th>' +
      '<th scope="col">Pipeline Hotels</th>' +
      '<th scope="col">Pipeline Rooms</th>' +
      "</tr></thead>" +
      '<tbody><tr class="gold-ft-data-row">' +
      '<th scope="row">Total</th>' +
      "<td>" +
      eh +
      "</td><td>" +
      er +
      "</td><td>" +
      ph +
      "</td><td>" +
      pr +
      "</td>" +
      "</tr></tbody></table></div></div>"
    );
  }

  function unitsStaffingTableHtml(p) {
    var te = numOrEmpty(p.totalEmployees);
    var av = numOrEmpty(p.avgOnSiteStaff);
    var rt = nz(p.regionalTeams);
    var pairs = [];
    if (te != null) pairs.push(["Total Employees (Reported)", formatInt(te)]);
    if (av != null) pairs.push(["Avg On-Site Staff per Property", formatInt(av)]);
    if (rt) pairs.push(["Regional Teams", rt]);
    if (!pairs.length) return "";
    var rows = pairs
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
    return (
      '<div class="gold-footprint-subsection">' +
      '<h3 class="gold-footprint-table-title">Units &amp; Staffing</h3>' +
      '<div class="gold-footprint-table-wrap">' +
      '<table class="gold-footprint-table units-staffing-table">' +
      "<tbody>" +
      rows +
      "</tbody></table></div></div>"
    );
  }

  function parseBrandsPortfolioDetail(raw) {
    if (raw == null || raw === "") return [];
    if (Array.isArray(raw)) return raw;
    if (typeof raw === "string") {
      try {
        var j = JSON.parse(raw);
        return Array.isArray(j) ? j : [];
      } catch (e) {
        return [];
      }
    }
    return [];
  }

  /**
   * Full-width tile: per-brand hotels, rooms, and % of portfolio (by hotels, or by rooms if all hotel counts are 0).
   */
  function brandPortfolioUnitsShareTileHtml(p) {
    var raw = parseBrandsPortfolioDetail(p.brandsPortfolioDetail);
    if (!raw.length) return "";
    var rows = [];
    raw.forEach(function (r) {
      var name = nz(r.brand_name) || nz(r.brand_key) || "";
      var ep = numOrEmpty(r.existing_properties) || 0;
      var er = numOrEmpty(r.existing_rooms) || 0;
      var pp = numOrEmpty(r.pipeline_properties) || 0;
      var pr = numOrEmpty(r.pipeline_rooms) || 0;
      var totH = ep + pp;
      var totR = er + pr;
      if (!name) name = "Brand";
      rows.push({ name: name, hotels: totH, rooms: totR });
    });
    if (!rows.length) return "";
    rows.sort(function (a, b) {
      return b.hotels - a.hotels || b.rooms - a.rooms;
    });
    var sumH = rows.reduce(function (acc, r) {
      return acc + r.hotels;
    }, 0);
    var sumR = rows.reduce(function (acc, r) {
      return acc + r.rooms;
    }, 0);
    var basis = sumH > 0 ? "hotels" : sumR > 0 ? "rooms" : null;
    if (!basis) return "";
    var denom = basis === "hotels" ? sumH : sumR;
    var note =
      basis === "hotels"
        ? "% of portfolio is by total hotel count (existing + pipeline) per brand."
        : "% of portfolio is by total room count (existing + pipeline) per brand — hotel counts were zero.";

    var body = rows
      .map(function (r) {
        var u = basis === "hotels" ? r.hotels : r.rooms;
        var pct = denom > 0 ? ((100 * u) / denom).toFixed(1) + "%" : "—";
        return (
          "<tr><th scope=\"row\">" +
          escapeHtml(r.name) +
          "</th><td>" +
          escapeHtml(formatInt(r.hotels)) +
          "</td><td>" +
          escapeHtml(formatInt(r.rooms)) +
          "</td><td>" +
          escapeHtml(pct) +
          "</td></tr>"
        );
      })
      .join("");

    return (
      '<div class="cluster cluster--brand-share cluster--full-row">' +
      "<h3>Units &amp; share by brand</h3>" +
      '<table class="brand-share-table">' +
      "<thead><tr>" +
      '<th scope="col">Brand</th>' +
      '<th scope="col">Hotels</th>' +
      '<th scope="col">Rooms</th>' +
      '<th scope="col">% of portfolio</th>' +
      "</tr></thead><tbody>" +
      body +
      "</tbody></table>" +
      '<p class="brand-share-note">' +
      escapeHtml(note) +
      "</p></div>"
    );
  }

  function footprintMetricsSection(p) {
    var evo = existingVsPipelineTableHtml(p);
    var dist = footprintDistributionBlockHtml(p);
    var staff = unitsStaffingTableHtml(p);
    var inner = evo + dist + staff;
    if (!inner) return "";
    return (
      '<section class="section"><h2 class="section-title">Footprint Metrics</h2><div class="gold-footprint-metrics">' +
      inner +
      "</div></section>"
    );
  }

  /** Signals / KPI values: hide N/A bands and empty placeholders */
  function meaningfulSignal(v) {
    var s = nz(v);
    if (!s) return false;
    if (s === "—") return false;
    if (/^not measured\s*\/\s*n\/a$/i.test(s)) return false;
    if (/^n\/a$/i.test(s)) return false;
    return true;
  }

  function meaningfulMetaValue(v) {
    return nz(v) && nz(v) !== "—";
  }

  /** Hero "Brand Mix" must be a short label (e.g. 62% branded), not a narrative paragraph. */
  function heroBrandMixValue(raw) {
    var s = nz(raw);
    if (!s) return "";
    if (/^not measured\s*\/\s*n\/a$/i.test(s) || /^n\/a$/i.test(s)) return "Not Measured / N/A";
    if (s.length > 48 || /\)\s*\.|—/.test(s) || (s.indexOf(".") >= 0 && s.split(/\s+/).length > 8)) {
      return "";
    }
    return s;
  }

  /** Resort, Lifestyle, etc. — for hero "Asset Focus" card */
  function assetFocusHeroValue(p, fields) {
    var parts = [];
    var seen = {};
    function addSource(val) {
      arrayish(val).forEach(function (t) {
        var k = String(t).toLowerCase();
        if (!k || seen[k]) return;
        seen[k] = true;
        parts.push(String(t).trim());
      });
    }
    addSource(p.propertyTypes);
    addSource(p.bestFitAssetTypes);
    addSource(p.bf_selected_asset_types);
    addSource(p.idealBuildingTypes);
    addSource(fields["Property Types"]);
    if (!parts.length) {
      var raw = formatMulti(p.propertyTypes) || formatMulti(fields["Property Types"]) || "";
      return meaningfulMetaValue(raw) ? raw : "";
    }
    return parts.join(", ");
  }

  function caseStudyHasContent(cs) {
    if (!cs) return false;
    if (nz(cs.property_name)) return true;
    if (nz(cs.outcome) || nz(cs.owner_relevance)) return true;
    if (nz(cs.hotel_type) && (nz(cs.region) || nz(cs.situation) || nz(cs.services))) return true;
    return false;
  }

  function caseStudyIsStructuredCase(cs) {
    return !!(nz(cs && cs.property_name) && (nz(cs.outcome) || nz(cs.services)));
  }

  /** True when Setup case study should use Before / Operator Action / After layout. */
  function caseStudyUsesNarrativeLayout(cs) {
    if (!caseStudyHasContent(cs)) return false;
    if (nz(cs.before) || nz(cs.operator_action) || nz(cs.after)) return true;
    return caseStudyIsStructuredCase(cs);
  }

  function formatCaseStudyMeta(cs) {
    return [nz(cs.region), nz(cs.hotel_type), nz(cs.branded_independent)]
      .filter(Boolean)
      .map(function (part) {
        return String(part).toUpperCase();
      })
      .join(" · ");
  }

  /**
   * When operators enter one Outcome block, split action vs. results sentences for B/A/A columns.
   */
  function splitCaseStudyOutcomeNarrative(outcome) {
    var text = nz(outcome);
    if (!text) return { before: "", action: "", after: "" };

    var paras = text
      .split(/\n\s*\n/)
      .map(function (p) {
        return p.trim();
      })
      .filter(Boolean);
    if (paras.length >= 3) {
      return { before: paras[0], action: paras[1], after: paras.slice(2).join(" ") };
    }
    if (paras.length === 2) {
      return { before: "", action: paras[0], after: paras[1] };
    }

    var sentences = text.match(/[^.!?]+[.!?]+|[^.!?]+$/g) || [text];
    sentences = sentences
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);

    var metricRe =
      /\d+\s*%|\d+\s*(?:bps|basis\s+points?)|\+\s*\d|\$\d|grew\s+\d|improved\s+\d|rose\s+\d|expanded\s+\d|increased\s+\d|lift\s+of\s+\d/i;
    var actionParts = [];
    var afterParts = [];

    sentences.forEach(function (s) {
      if (
        metricRe.test(s) ||
        /margin|revpar|gop|occupancy|satisfaction|profit dollars|revenue grew|repeat-stay/i.test(s)
      ) {
        afterParts.push(s);
      } else {
        actionParts.push(s);
      }
    });

    return {
      before: "",
      action: actionParts.join(" "),
      after: afterParts.join(" ") || text,
    };
  }

  /**
   * Maps Operator Setup case study fields to owner-facing narrative columns.
   * Explicit before/operator_action/after keys supported when present on the record.
   */
  function deriveCaseStudyNarrative(cs) {
    cs = cs || {};
    var before =
      nz(cs.before) || nz(cs.before_state) || nz(cs.challenge) || "";
    var action =
      nz(cs.operator_action) || nz(cs.operatorAction) || nz(cs.services) || "";
    var after = nz(cs.after) || nz(cs.after_state) || "";
    var outcome = nz(cs.outcome);
    var situation = nz(cs.situation);
    var split = outcome
      ? splitCaseStudyOutcomeNarrative(outcome)
      : { before: "", action: "", after: "" };

    if (!action && split.action) action = split.action;
    if (!after) after = split.after || outcome;

    if (!before && situation) {
      before =
        "At engagement start, the asset was in a " +
        situation.toLowerCase() +
        " phase—fragmented performance signals and limited owner visibility into priorities.";
    }

    return {
      before: before,
      action: action,
      after: after,
      relevance: nz(cs.owner_relevance),
    };
  }

  /** Owner-facing five-part case study (Challenge → Data Status). */
  function deriveCaseStudyDataStatus(cs) {
    var explicit = nz(cs.data_status) || nz(cs.dataStatus) || nz(cs.proof_status);
    if (explicit) return explicit;
    var hasOutcome = nz(cs.outcome);
    var hasAction = nz(cs.services) || nz(cs.operator_action);
    var hasWhy = nz(cs.owner_relevance);
    if (hasOutcome && hasAction && hasWhy) return "Operator-provided proof in setup.";
    if (hasOutcome || hasAction) return "Partial detail in operator setup.";
    return "Representative case format — confirm with operator before reliance.";
  }

  function formatRichChallengeFromContext(cs) {
    cs = cs || {};
    var prop = nz(cs.property_name) || "the property";
    var region = nz(cs.region) || "the market";
    var type = nz(cs.hotel_type) || "hotel";
    var sit = nz(cs.situation);
    if (!sit) return "";
    var sitKey = sit.toLowerCase();

    if (sitKey.indexOf("stabiliz") !== -1) {
      return (
        "When we began at " +
        prop +
        " in " +
        region +
        ", the " +
        type.toLowerCase() +
        " asset was nominally stabilized but underperforming on mix and margin. Group and transient demand were misaligned, secondary revenue was under-captured, and the owner lacked a reliable weekly view of revenue actions and profit levers."
      );
    }
    if (sitKey.indexOf("reposition") !== -1 || sitKey.indexOf("conversion") !== -1) {
      return (
        prop +
        " in " +
        region +
        " required renewal of aging systems and public areas without sacrificing peak demand. The owner needed capex and operations sequencing that protected guest safety, cash flow, and lender confidence—not cosmetic reopen dates."
      );
    }
    if (sitKey.indexOf("turnaround") !== -1 || sitKey.indexOf("transition") !== -1) {
      return (
        prop +
        " faced a turnaround in " +
        region +
        ": inconsistent execution, weak outlet economics, and limited owner visibility into which dayparts and service moments were eroding guest satisfaction and margin despite a strong location."
      );
    }
    if (sitKey.indexOf("pre-opening") !== -1) {
      return (
        "Before opening, " +
        prop +
        " in " +
        region +
        " needed a disciplined pre-opening plan—staffing, systems, and commercial ramp—so the " +
        type.toLowerCase() +
        " asset did not open into fragmented reporting and unclear owner priorities."
      );
    }
    return (
      "At engagement start, " +
      prop +
      " in " +
      region +
      " was in a " +
      sitKey +
      " phase with fragmented performance signals and limited owner visibility into operating priorities."
    );
  }

  function formatSituationAsChallenge(situation) {
    return formatRichChallengeFromContext({ situation: situation });
  }

  function deriveCaseStudyFivePart(cs) {
    cs = cs || {};
    var n = deriveCaseStudyNarrative(cs);
    var challenge = nz(cs.challenge) || nz(cs.before) || nz(cs.before_state) || "";
    if (!challenge && nz(cs.situation)) {
      challenge = formatRichChallengeFromContext(cs);
    }
    if (!challenge && n.before && !/^At engagement start/i.test(n.before)) {
      challenge = n.before;
    }
    var operatorAction =
      nz(cs.services) || nz(cs.operator_action) || nz(cs.operatorAction) || n.action;
    var outcome = nz(cs.outcome) || n.after;
    var whyItMatters = nz(cs.owner_relevance) || n.relevance;
    return {
      challenge: challenge,
      operatorAction: operatorAction,
      outcome: outcome,
      whyItMatters: whyItMatters,
      dataStatus: nz(cs.data_status) || nz(cs.dataStatus) || deriveCaseStudyDataStatus(cs),
    };
  }

  function proofFromCaseStudy(cs, gridOpts) {
    gridOpts = gridOpts || {};
    if (!caseStudyHasContent(cs)) return null;
    var img = nz(cs.image_url);
    var title = nz(cs.property_name) || nz(cs.hotel_type) || "Case study";
    var meta = formatCaseStudyMeta(cs) || nz(cs.situation);
    if (gridOpts.useCaseStudyFivePart) {
      return {
        img: img,
        title: title,
        meta: meta,
        fivePart: deriveCaseStudyFivePart(cs),
      };
    }
    if (caseStudyUsesNarrativeLayout(cs)) {
      return {
        img: img,
        title: title,
        meta: meta,
        narrative: deriveCaseStudyNarrative(cs),
      };
    }
    var lines = [nz(cs.outcome), nz(cs.owner_relevance)].filter(Boolean);
    if (!lines.length) lines = [nz(cs.services), nz(cs.situation)].filter(Boolean);
    return { img: img, title: title, meta: meta, lines: lines };
  }

  function kpiOptional(label, value) {
    if (!meaningfulSignal(value)) return "";
    return kpi(label, value);
  }

  function kpiGridFromPairs(pairs) {
    var cells = (pairs || [])
      .map(function (pair) {
        return kpiOptional(pair[0], pair[1]);
      })
      .filter(Boolean);
    if (!cells.length) return "";
    var count = cells.length;
    return (
      '<div class="oe-tab-snapshot-kpis ' +
      snapshotKpiRowWrapperClass(count) +
      '" style="grid-template-columns:' +
      snapshotKpiGridInlineStyle(count) +
      '">' +
      cells.join("") +
      "</div>"
    );
  }

  var OPERATOR_QUICK_FACT_LABELS = [
    "Year Founded",
    "Company Website",
    "Team Members",
    "Avg. Hotel Size",
    "Management Style",
    "Typical Agreement",
    "Data Confidence",
  ];

  function looksLikeFourDigitYear(v) {
    return /^(19|20)\d{2}$/.test(nz(v));
  }

  function resolveYearFounded(vm) {
    var p = vm.prefill || {};
    var f = vm.fields || {};
    var ex = vm.ex || {};
    var candidates = [
      pick(ex, p, "yearEstablished", ""),
      nz(p.yearEstablished),
      nz(f["Year Established"]),
      nz(p.yearsInBusiness),
      nz(f["Years in Business"]),
    ];
    var i;
    for (i = 0; i < candidates.length; i++) {
      if (looksLikeFourDigitYear(candidates[i])) return candidates[i];
    }
    for (i = 0; i < candidates.length; i++) {
      var v = nz(candidates[i]);
      if (v) {
        var m = v.match(/(19|20)\d{2}/);
        if (m) return m[0];
      }
    }
    return "";
  }

  function resolveCompanyWebsite(vm) {
    var p = vm.prefill || {};
    var f = vm.fields || {};
    var ex = vm.ex || {};
    return (
      nz(p.website) ||
      pick(ex, p, "website", "") ||
      nz(f["Website"]) ||
      ""
    );
  }

  function normalizeWebsiteHref(raw) {
    var u = nz(raw);
    if (!u) return "";
    if (!/^https?:\/\//i.test(u)) return "https://" + u;
    return u;
  }

  function websiteDisplayText(raw) {
    var u = nz(raw);
    if (!u) return "";
    return u.replace(/^https?:\/\//i, "").replace(/^www\./i, "").replace(/\/+$/, "");
  }

  function kpiCompanyWebsite(label, rawUrl) {
    var href = normalizeWebsiteHref(rawUrl);
    var display = websiteDisplayText(rawUrl);
    if (!href || !display) {
      return kpi(label, "");
    }
    return (
      '<div class="kpi"><div class="label">' +
      escapeHtml(label) +
      '</div><div class="value"><a class="oe-quick-fact-website" href="' +
      escapeAttr(href) +
      '" target="_blank" rel="noopener noreferrer">' +
      escapeHtml(display) +
      "</a></div></div>"
    );
  }

  function resolveTeamMembers(vm) {
    var p = vm.prefill || {};
    var f = vm.fields || {};
    var te = numOrEmpty(p.totalEmployees);
    if (te != null) {
      if (te >= 200) return formatInt(te) + "+";
      return formatInt(te);
    }
    return nz(p.companySize) || nz(f["Company Size"]) || "";
  }

  function resolveAvgHotelSize(vm) {
    var p = vm.prefill || {};
    var hotels = numOrEmpty(p.totalProperties);
    var rooms = numOrEmpty(p.totalRooms);
    if (hotels != null && hotels > 0 && rooms != null && rooms > 0) {
      return formatInt(Math.round(rooms / hotels)) + " rooms";
    }
    var min = numOrEmpty(p.minPropertySize);
    var max = numOrEmpty(p.maxPropertySize);
    if (min != null && max != null) return formatInt(min) + "–" + formatInt(max) + " rooms";
    if (min != null) return formatInt(min) + "+ rooms";
    if (max != null) return "Up to " + formatInt(max) + " rooms";
    return "";
  }

  function resolveManagementStyle(vm) {
    var p = vm.prefill || {};
    var f = vm.fields || {};
    var ex = vm.ex || {};
    var primary =
      nz(p.primaryServiceModel) ||
      pick(ex, p, "primaryServiceModel", "") ||
      "";
    if (primary) return primary;
    var models = arrayish(p.serviceModelsSupported);
    if (!models.length) {
      models = arrayish(f["Service Models Supported"]);
    }
    return models.slice(0, 2).join(" · ");
  }

  function resolveTypicalAgreement(vm) {
    var p = vm.prefill || {};
    var f = vm.fields || {};
    var ex = vm.ex || {};
    var direct =
      nz(p.typicalAgreement) ||
      pick(ex, p, "typicalAgreement", "") ||
      nz(f["Typical Agreement"]) ||
      "";
    if (direct && !isInternalFillPlaceholder(direct)) return direct;
    var structures = arrayishWithoutPlaceholders(p.managementStructuresSupported);
    if (!structures.length) {
      structures = arrayishWithoutPlaceholders(f["Management Structures Supported"]);
    }
    return structures.slice(0, 2).join(" · ");
  }

  function formatDataStatusLabel(raw) {
    var v = nz(raw);
    if (!v) return "";
    var lower = v.toLowerCase();
    if (lower === "inferred") return "Operator profile draft";
    if (lower.indexOf("operator-provided") !== -1 || lower.indexOf("operator provided") !== -1) {
      return "Operator verified";
    }
    return v;
  }

  function resolveDataStatus(vm) {
    var p = vm.prefill || {};
    var f = vm.fields || {};
    var ex = vm.ex || {};
    var conf =
      nz(p.dataConfidenceLevel) ||
      pick(ex, p, "dataConfidenceLevel", "") ||
      nz(f["Data Confidence Level"]) ||
      "";
    if (conf) return formatDataStatusLabel(conf);
    var status = nz(p.submission_status) || nz(f["submission_status"]) || "";
    if (status) return formatDataStatusLabel(status);
    return "";
  }

  function buildOperatorQuickFacts(vm) {
    vm = vm || {};
    return [
      ["Year Founded", resolveYearFounded(vm)],
      ["Company Website", resolveCompanyWebsite(vm)],
      ["Team Members", resolveTeamMembers(vm)],
      ["Avg. Hotel Size", resolveAvgHotelSize(vm)],
      ["Management Style", resolveManagementStyle(vm)],
      ["Typical Agreement", resolveTypicalAgreement(vm)],
      ["Data Confidence", resolveDataStatus(vm)],
    ];
  }

  function operatorQuickFactsSectionHtml(vm) {
    var pairs = buildOperatorQuickFacts(vm);
    var cells = pairs
      .map(function (pair) {
        if (pair[0] === "Company Website") {
          return kpiCompanyWebsite(pair[0], pair[1]);
        }
        return kpi(pair[0], pair[1]);
      })
      .join("");
    var qCount = pairs.length;
    return (
      '<section class="section oe-operator-quick-facts-section">' +
      '<h2 class="section-title">Operator Quick Facts</h2>' +
      '<div class="oe-operator-quick-facts-kpis oe-tab-snapshot-kpis ' +
      snapshotKpiRowWrapperClass(qCount) +
      '" style="grid-template-columns:' +
      snapshotKpiGridInlineStyle(qCount) +
      '">' +
      cells +
      "</div></section>"
    );
  }

  function decisionStripFiltered(items, opts) {
    opts = opts || {};
    var sectionTitle = nz(opts.sectionTitle) || "Decision Signals";
    var pairs = (items || []).filter(function (pair) {
      return meaningfulSignal(pair[1]);
    });
    if (!pairs.length) return "";
    return (
      '<section class="section"><h2 class="section-title">' +
      escapeHtml(sectionTitle) +
      '</h2><div class="kpi-grid-4">' +
      pairs
        .map(function (pair) {
          return kpi(pair[0], pair[1]);
        })
        .join("") +
      "</div></section>"
    );
  }

  function uniqueStrings(arr) {
    var seen = {};
    return (arr || []).filter(function (x) {
      var k = String(x).toLowerCase().trim();
      if (!k || seen[k]) return false;
      seen[k] = true;
      return true;
    });
  }

  function leadersForSnapshot(vm) {
    return (vm.leadership || []).filter(function (L) {
      if (!nz(L.name)) return false;
      if (L.displayOnExplorer === false) return false;
      return true;
    });
  }

  function parseYearsFromText(text) {
    var years = [];
    var re = /(\d+(?:\.\d+)?)\s*\+?\s*years?/gi;
    var m;
    var s = String(text || "");
    while ((m = re.exec(s)) !== null) {
      var n = parseFloat(m[1]);
      if (!isNaN(n) && n > 0 && n < 80) years.push(n);
    }
    return years;
  }

  function formatYearsDisplay(raw) {
    var s = nz(raw);
    if (!s) return "";
    if (/yrs?\.?$/i.test(s)) return s;
    if (/\d/.test(s) && !/year/i.test(s)) return s + " yrs";
    return s.replace(/\s*years?/i, " yrs");
  }

  /** Compact KPI value (e.g. "8.4 yrs") — avoids wrapping on messy Setup strings. */
  function compactYearsKpi(raw) {
    var s = nz(raw);
    if (!s) return "";
    var m = s.match(/(\d+(?:\.\d+)?)\s*\+?/);
    if (m) {
      var n = parseFloat(m[1]);
      if (!isNaN(n) && n >= 0 && n < 80) {
        var rounded = Math.round(n * 10) / 10;
        var display = Number.isInteger(rounded) ? String(rounded) : String(rounded);
        return display + " yrs";
      }
    }
    if (/^\s*\d+(\.\d+)?\s*yrs?\.?\s*$/i.test(s)) return s.replace(/\s*years?/i, " yrs");
    return "";
  }

  /** First percentage in a signal string (e.g. "30% gateway…" → "30%"). */
  function compactPercentKpi(raw) {
    var s = nz(raw);
    if (!s) return "";
    var m = s.match(/(\d+(?:\.\d+)?)\s*%/);
    return m ? m[1] + "%" : "";
  }

  /** Urban / resort style mix → "46% / 31%". */
  function compactMixKpi(raw) {
    var s = nz(raw);
    if (!s) return "";
    var matches = s.match(/(\d+(?:\.\d+)?)\s*%/g);
    if (!matches || !matches.length) return "";
    if (matches.length >= 2) {
      return matches[0].replace(/\s/g, "") + " / " + matches[1].replace(/\s/g, "");
    }
    return matches[0].replace(/\s/g, "");
  }

  /** Short display for gateway / qualitative signals. */
  function compactGatewayKpi(raw) {
    var pct = compactPercentKpi(raw);
    if (pct) return pct;
    var s = nz(raw);
    if (!s) return "";
    if (s.length <= 14) return s;
    var words = s.split(/\s+/).slice(0, 2);
    return words.join(" ");
  }

  function marketKpiNote(shortDefault, raw, compactDisplay) {
    var full = nz(raw);
    if (full && compactDisplay && full !== compactDisplay) return full;
    return shortDefault;
  }

  function looksLikeLanguageName(s) {
    var t = nz(s);
    if (!t || t.length > 36) return false;
    if (/hub|regional|across|cala|market|portfolio|owner-facing/i.test(t)) return false;
    return true;
  }

  function collectLeadershipLanguages(leaders) {
    var langs = [];
    (leaders || []).forEach(function (L) {
      arrayish(L.languages).forEach(function (lang) {
        langs.push(lang);
      });
      var fluency = nz(L.languageFluencyLevel);
      if (fluency) {
        fluency.split(/[,;|/]+/).forEach(function (part) {
          var t = part.trim();
          if (t) langs.push(t);
        });
      }
    });
    return uniqueStrings(langs);
  }

  /** Unique `languages` multiselect values from visible executives only. */
  function collectExecutiveLanguagesFromField(leaders) {
    var langs = [];
    (leaders || []).forEach(function (L) {
      arrayish(L.languages).forEach(function (lang) {
        var t = nz(lang);
        if (t) langs.push(t);
      });
    });
    return uniqueStrings(langs.filter(looksLikeLanguageName));
  }

  function parseExecutiveHospitalityYears(L) {
    if (!L) return null;
    var raw =
      L.hospitalityExperienceYears != null && L.hospitalityExperienceYears !== ""
        ? L.hospitalityExperienceYears
        : L.hospitality_experience_years;
    if (raw == null || raw === "") return null;
    var n = typeof raw === "number" ? raw : parseFloat(String(raw).replace(/,/g, ""));
    if (isNaN(n) || n <= 0 || n >= 80) return null;
    return n;
  }

  /** Mean `hospitalityExperienceYears` for visible executives (Operator Setup child rows). */
  function averageExecutiveHospitalityYears(leaders) {
    var nums = [];
    (leaders || []).forEach(function (L) {
      var n = parseExecutiveHospitalityYears(L);
      if (n != null) nums.push(n);
    });
    if (!nums.length) return "";
    var avg =
      nums.reduce(function (a, b) {
        return a + b;
      }, 0) / nums.length;
    return compactYearsKpi(String(Math.round(avg * 10) / 10));
  }

  /** Mean `companyTenureYears` for visible executives (Operator Setup child rows). */
  function averageExecutiveCompanyTenureYears(leaders) {
    var nums = [];
    (leaders || []).forEach(function (L) {
      var raw =
        L.companyTenureYears != null && L.companyTenureYears !== ""
          ? L.companyTenureYears
          : L.company_tenure_years;
      if (raw == null || raw === "") return;
      var n = typeof raw === "number" ? raw : parseFloat(String(raw).replace(/,/g, ""));
      if (isNaN(n) || n <= 0 || n >= 80) return;
      nums.push(n);
    });
    if (!nums.length) return "";
    var avg =
      nums.reduce(function (a, b) {
        return a + b;
      }, 0) / nums.length;
    return compactYearsKpi(String(Math.round(avg * 10) / 10));
  }

  function regionalCoverageKpi(ex, p) {
    var raw = pick(ex, p, "lead_narrative_regional", "");
    var lines = linesFromText(raw);
    if (!lines.length) {
      return { value: "—", note: "Regional leadership coverage" };
    }
    if (lines.length === 1) {
      var one = lines[0];
      var hubMatch = one.match(/^(\d+)\s+(.+)$/);
      if (hubMatch) {
        return { value: hubMatch[1], note: hubMatch[2].replace(/\.\s*$/, "") };
      }
      if (one.length <= 28) {
        return { value: one, note: "CALA and priority markets" };
      }
      return { value: "—", note: one };
    }
    return {
      value: String(lines.length),
      note: lines.slice(0, 5).join(", "),
    };
  }

  /**
   * Owner-facing leadership snapshot KPIs (top row — six cards, one line).
   * @param {object} vm
   * @returns {{ label: string, value: string, note: string }[]}
   */
  function deriveLeadershipSnapshotMetrics(vm) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var leaders = leadersForSnapshot(vm);
    var execCount = leaders.length;

    var leadSections = global.OperatorLeadershipTeamSections;
    var hospValue = averageExecutiveHospitalityYears(leaders);

    var tenureRaw = pickOwnerFacing(ex, p, "lead_signal_tenure");
    var tenureValue = compactYearsKpi(tenureRaw) || compactYearsKpi(formatYearsDisplay(tenureRaw));
    if (!tenureValue) tenureValue = averageExecutiveCompanyTenureYears(leaders);
    if (!tenureValue && leadSections && leadSections.scalarSnapshotField) {
      tenureValue = compactYearsKpi(leadSections.scalarSnapshotField(vm, "lead_signal_tenure"));
    }

    var langUnique = collectExecutiveLanguagesFromField(leaders);
    if (
      leadSections &&
      leadSections.languageSnapshot &&
      (!langUnique.length || langUnique.length < 2)
    ) {
      var langSnap = leadSections.languageSnapshot(vm);
      if (langSnap.names && langSnap.names.length) langUnique = langSnap.names;
    }
    var langCount = langUnique.length ? String(langUnique.length) : "";

    var crossBrand = normalizeCrossBrandSignal(
      pick(ex, p, "lead_signal_crossbrand", ""),
      leaders,
      vm
    );
    var regional = regionalCoverageKpi(ex, p);

    return [
      {
        label: "Executive Leaders",
        value: execCount ? String(execCount) : "—",
        note: "Owner-facing leadership bench",
      },
      {
        label: "Avg. Hospitality Experience",
        value: hospValue || "—",
        note: "Across senior team",
      },
      {
        label: "Avg. Company Tenure",
        value: tenureValue || "—",
        note: "Leadership continuity",
      },
      {
        label: "Languages Covered",
        value: langCount || "—",
        note: langUnique.length
          ? langUnique.slice(0, 8).join(", ")
          : "Languages represented on the leadership team",
      },
      {
        label: "Cross-Brand Experience",
        value: crossBrand || "—",
        note: "Multi-brand leadership depth",
      },
      {
        label: "Regional Coverage",
        value: regional.value || "—",
        note: regional.note || "Regional leadership coverage",
      },
    ];
  }

  function buildKpiLabelHtml(m) {
    if (m.labelLines && m.labelLines.length) {
      return m.labelLines.map(escapeHtml).join("<br>");
    }
    return escapeHtml(m.label);
  }

  function buildLeadershipSnapshotMetricCard(m) {
    return (
      '<div class="card oe-value-kpi">' +
      '<div class="oe-value-kpi__value">' +
      escapeHtml(m.value) +
      '</div><h3 class="oe-value-kpi__label">' +
      buildKpiLabelHtml(m) +
      "</h3><p>" +
      escapeHtml(m.note) +
      "</p></div>"
    );
  }

  /**
   * Leadership tab wrapper — same value-first KPI row as other Explorer tabs.
   * @param {object} vm
   */
  function buildLeadershipSnapshotSection(vm) {
    var metrics = deriveLeadershipSnapshotMetrics(vm);
    var intro =
      '<p class="gold-mock-tab-empty odna-subsection-intro">Owner-facing view of the people and organizational depth behind the operator.</p>';
    if (!metrics.length) {
      return (
        '<section class="section oe-leadership-snapshot-section">' +
        '<h2 class="section-title">Leadership Snapshot</h2>' +
        intro +
        '<p class="gold-mock-tab-empty">Leadership snapshot metrics not yet available from Operator Setup.</p></section>'
      );
    }
    return buildValueKpiSnapshotSection({
      title: "Leadership Snapshot",
      intro: intro,
      metrics: metrics,
      sectionClass: "oe-leadership-snapshot-section",
      kpiClass: "oe-leadership-snapshot-kpis",
    });
  }

  function deriveMarketsFootprintSnapshotMetrics(vm, m, ex, p) {
    m = m || marketsDerivedMetrics(vm);
    ex = ex || (vm && vm.ex) || {};
    p = p || (vm && vm.prefill) || {};
    var yearsRaw = nz(pick(ex, p, "mkt_signal_years", ""));
    var gatewayRaw = nz(pick(ex, p, "mkt_signal_gateway", ""));
    var mixRaw = nz(pick(ex, p, "mkt_signal_mix", ""));
    var yearsVal = compactYearsKpi(yearsRaw);
    var gatewayVal = compactGatewayKpi(gatewayRaw);
    var mixVal = compactMixKpi(mixRaw);
    var coverageVal = nz(m.coverage);
    if (coverageVal.length > 16) {
      coverageVal = coverageVal.split(/\s+/).slice(0, 2).join(" ");
    }
    var metrics = [
      {
        label: "Regions",
        value: nz(m.regions),
        note: nz(m.regionNames) || "Active operating regions",
      },
      {
        label: m.countriesIsMarketsFallback ? "Markets Operated" : "Countries",
        value: nz(m.countries),
        note: m.countriesIsMarketsFallback
          ? "From markets operated count in Operator Setup"
          : nz(m.countryNames) || "Countries with operating presence",
      },
      {
        label: "Cities / Markets",
        value: nz(m.cities),
        note: nz(m.cityNames) || "Cities in footprint list",
      },
      {
        label: "Coverage Model",
        value: coverageVal,
        note: marketKpiNote(
          "Regional management and coverage model",
          nz(p.regionalManagementTeams) || nz(p.primaryServiceModel),
          coverageVal
        ),
      },
      {
        label: "Years in Core Markets",
        value: yearsVal,
        note: marketKpiNote("Average tenure in primary markets", yearsRaw, yearsVal),
      },
      {
        label: "Gateway Concentration",
        value: gatewayVal,
        note: marketKpiNote("Gateway vs secondary market mix", gatewayRaw, gatewayVal),
      },
      {
        label: "Urban / Resort Mix",
        value: mixVal,
        note: marketKpiNote("Urban versus resort portfolio balance", mixRaw, mixVal),
      },
    ];
    return metrics.filter(function (row) {
      return meaningfulSignal(row.value);
    });
  }

  /** Inline grid-template-columns for tab snapshot KPI rows (always one row). */
  function snapshotKpiGridInlineStyle(count) {
    var n = Math.max(1, Number(count) || 1);
    var colMin = n >= 7 ? "120px" : n >= 6 ? "128px" : "";
    return colMin
      ? "repeat(" + n + ",minmax(" + colMin + ",1fr))"
      : "repeat(" + n + ",minmax(0,1fr))";
  }

  function snapshotKpiRowWrapperClass(count) {
    var n = Math.max(1, Number(count) || 1);
    return (
      "oe-snapshot-kpi-row oe-snapshot-kpi-row--count-" +
      n +
      " oe-tab-snapshot-kpis--single-row"
    );
  }

  function compactSignalDisplay(raw) {
    var s = nz(raw);
    if (!s) return "";
    var pct = compactPercentKpi(s);
    if (pct) return pct;
    var timeMatch = s.match(/<\s*(\d+(?:\.\d+)?)\s*(min|mins|minutes|hr|hrs|hours)/i);
    if (timeMatch) {
      var unit = timeMatch[2].toLowerCase();
      if (unit.indexOf("min") === 0) return "<" + timeMatch[1] + " min";
      return "<" + timeMatch[1] + " hr";
    }
    if (s.length <= 18) return s;
    return compactGatewayKpi(s) || s.slice(0, 18);
  }

  /**
   * Value-first KPI row — always one horizontal row per tab (equal columns).
   * @param {{ title: string, intro: string, metrics: Array<{label:string,value:string,note:string,labelLines?:string[]}>, sectionClass?: string, kpiClass?: string }} opts
   */
  function buildValueKpiSnapshotSection(opts) {
    opts = opts || {};
    var metrics = (opts.metrics || []).filter(function (row) {
      return meaningfulSignal(row && row.value);
    });
    if (!metrics.length) return "";
    var count = metrics.length;
    var sectionClass = opts.sectionClass || "oe-tab-snapshot-section";
    var kpiClass = opts.kpiClass || "oe-tab-snapshot-kpis";
    var metricsHtml =
      '<div class="' +
      kpiClass +
      " oe-tab-snapshot-kpis " +
      snapshotKpiRowWrapperClass(count) +
      '" style="grid-template-columns:' +
      snapshotKpiGridInlineStyle(count) +
      '">' +
      metrics.map(buildLeadershipSnapshotMetricCard).join("") +
      "</div>";
    return (
      '<section class="section ' +
      sectionClass +
      '">' +
      '<h2 class="section-title">' +
      escapeHtml(opts.title || "Snapshot") +
      "</h2>" +
      (opts.intro || "") +
      metricsHtml +
      "</section>"
    );
  }

  /**
   * Markets tab — footprint KPIs + decision signals in one value-first row (Leadership Snapshot pattern).
   */
  function buildMarketsFootprintSnapshotSection(vm, m, ex, p) {
    var metrics = deriveMarketsFootprintSnapshotMetrics(vm, m, ex, p);
    var intro =
      '<p class="gold-mock-tab-empty odna-subsection-intro">A concise view of geographic reach, operating depth, and market signals to use when you are evaluating this operator.</p>';
    if (!metrics.length) {
      return (
        '<section class="section oe-markets-snapshot-section">' +
        '<h2 class="section-title">Markets &amp; Footprint</h2>' +
        intro +
        '<p class="gold-mock-tab-empty">Markets snapshot metrics not yet available from Operator Setup.</p></section>'
      );
    }
    return buildValueKpiSnapshotSection({
      title: "Markets & Footprint",
      intro: intro,
      metrics: metrics,
      sectionClass: "oe-markets-snapshot-section",
      kpiClass: "oe-markets-snapshot-kpis",
    });
  }

  function deriveInfrastructureDecisionSignalsMetrics(ex, p) {
    ex = ex || {};
    p = p || {};
    function row(labelLines, raw, defaultNote) {
      var display = compactSignalDisplay(raw) || nz(raw);
      if (!meaningfulSignal(display)) return null;
      var lines = Array.isArray(labelLines) ? labelLines : [labelLines];
      return {
        label: lines.join(" "),
        labelLines: lines,
        value: display,
        note: defaultNote,
      };
    }
    return [
      row(
        ["Platform", "Uptime"],
        pick(ex, p, "infra_signal_uptime", ""),
        "Platform availability and SLA posture"
      ),
      row(
        ["Incident", "Response"],
        pick(ex, p, "infra_signal_incident", ""),
        "Critical incident response window"
      ),
      row(
        ["Portfolio", "Adoption"],
        pick(ex, p, "infra_signal_adoption", ""),
        "System adoption across the portfolio"
      ),
      row(
        ["Data Refresh", "Cadence"],
        pick(ex, p, "infra_signal_refresh", ""),
        "How often owner-facing data is refreshed"
      ),
      row(
        ["Audit", "Consistency"],
        pick(ex, p, "risk_signal_audit", ""),
        "Audit pass rate and consistency"
      ),
      row(
        ["BCP Test", "Frequency"],
        pick(ex, p, "risk_signal_bcp", ""),
        "Business continuity testing cadence"
      ),
      row(
        ["Control Closure", "Rate"],
        pick(ex, p, "risk_signal_control", ""),
        "Internal control closure within cycle"
      ),
      row(
        ["Insurance", "Adequacy"],
        pick(ex, p, "risk_signal_insurance", ""),
        "Insurance adequacy review rhythm"
      ),
    ].filter(Boolean);
  }

  /**
   * Infrastructure tab — platform, data, and controls snapshot (top KPI row).
   */
  function buildInfrastructureSnapshotSection(ex, p) {
    var intro =
      '<p class="gold-mock-tab-empty odna-subsection-intro">Platform reliability, data discipline, and control posture to use when you are evaluating this operator.</p>';
    return buildValueKpiSnapshotSection({
      title: "Infrastructure & Data",
      intro: intro,
      metrics: deriveInfrastructureDecisionSignalsMetrics(ex, p),
      sectionClass: "oe-infra-snapshot-section",
      kpiClass: "oe-infra-snapshot-kpis",
    });
  }

  function compactEngagementKpi(raw) {
    var s = nz(raw);
    if (!s) return "";
    var pct = compactPercentKpi(s);
    if (pct) return pct;
    if (/^\d+(\.\d+)?$/.test(s)) return s;
    return compactSignalDisplay(s);
  }

  function compactReportingLevelKpi(raw) {
    var s = nz(raw).toLowerCase();
    if (!s) return "";
    if (s.indexOf("institutional") >= 0) return "Institutional";
    if (s.indexOf("lender") >= 0 || s.indexOf("investor") >= 0) return "Lender-grade";
    if (s.indexOf("monthly") >= 0) return "Monthly review";
    if (s.indexOf("basic") >= 0) return "Basic";
    if (s.indexOf("custom") >= 0) return "Custom";
    if (s.indexOf("structured") >= 0) return "Structured";
    if (s.indexOf("advanced") >= 0) return "Advanced";
    return compactCapabilityLevelKpi(raw) || compactEngagementKpi(raw);
  }

  function compactOwnerPortalKpi(raw) {
    var s = nz(raw);
    if (!s) return "";
    if (/^(yes|y|true|available|included)/i.test(s)) return "Yes";
    if (/^(no|n|false|none|not available)/i.test(s)) return "No";
    var first = s.split(/\s*\/\s*/)[0].trim();
    if (first.length <= 18) return compactEngagementKpi(first);
    return compactSignalDisplay(first) || compactEngagementKpi(first.slice(0, 18));
  }

  function reportingOutputsSnapshotValue(ex, p) {
    var rt = (p && p.reportTypes) || (ex && ex.reportTypes);
    if (Array.isArray(rt) && rt.length) return String(rt.length);
    var n = nz(pick(ex, p, "infra_kpi_reporting", ""));
    if (n && /^\d+/.test(n)) return n;
    return "";
  }

  /**
   * Engagement & Reporting tab — owner communication and reporting snapshot (top KPI row).
   */
  function deriveEngagementSnapshotMetrics(ex, p) {
    ex = ex || {};
    p = p || {};
    function row(labelLines, raw, defaultNote, compactFn) {
      var display = compactFn ? compactFn(raw) : compactEngagementKpi(raw);
      if (!meaningfulSignal(display)) return null;
      var lines = Array.isArray(labelLines) ? labelLines : [labelLines];
      return {
        label: lines.join(" "),
        labelLines: lines,
        value: display,
        note: defaultNote,
      };
    }
    var touchpoints = nz(pick(ex, p, "ov_q_touchpoints", ""));
    var touchDisplay = touchpoints;
    if (touchpoints && /^\d+(\.\d+)?$/.test(touchpoints)) {
      touchDisplay = touchpoints.indexOf(".") >= 0 ? touchpoints : touchpoints;
    }
    return [
      row(
        ["Owner Reporting", "Level"],
        pick(ex, p, "ownerReportingLevel", ""),
        "Depth and structure of owner reporting packages",
        compactReportingLevelKpi
      ),
      row(
        ["Financial", "Reporting"],
        pick(ex, p, "reportingFrequency", "") || pick(ex, p, "ownerReportingCadence", ""),
        "How often financial and operating reports are delivered"
      ),
      row(
        ["Owner", "Response"],
        pick(ex, p, "ownerResponseTime", ""),
        "Typical response window for owner inquiries"
      ),
      row(
        ["Review", "Touchpoints"],
        touchDisplay,
        "Scheduled owner reviews per quarter"
      ),
      row(
        ["Reporting", "Outputs"],
        reportingOutputsSnapshotValue(ex, p),
        "Report types or systems in the owner cadence"
      ),
      row(
        ["Owner", "Portal"],
        pick(ex, p, "ownerPortalFeatures", "") ||
          pick(ex, p, "ownerPortal", "") ||
          pick(ex, p, "owner_portal", ""),
        "Owner portal, dashboard, or secure owner access",
        compactOwnerPortalKpi
      ),
      row(
        ["Collaboration", "Model"],
        pick(ex, p, "operatingCollaborationMode", "") || pick(ex, p, "ownerInvolvement", ""),
        "How the operator partners with ownership day to day"
      ),
    ].filter(Boolean);
  }

  function buildEngagementSnapshotSection(ex, p) {
    var intro =
      '<p class="gold-mock-tab-empty odna-subsection-intro">Quick signals for how often you will hear from this operator, what you are likely to receive, and how quickly they respond—use these when you compare operators for your asset.</p>';
    return buildValueKpiSnapshotSection({
      title: "Engagement & Reporting",
      intro: intro,
      metrics: deriveEngagementSnapshotMetrics(ex, p),
      sectionClass: "oe-eng-snapshot-section",
      kpiClass: "oe-eng-snapshot-kpis",
    });
  }

  function pickSetupField(ex, p, fields, keys, airtableNames) {
    var i;
    var keyList = Array.isArray(keys) ? keys : keys ? [keys] : [];
    var nameList = Array.isArray(airtableNames)
      ? airtableNames
      : airtableNames
        ? [airtableNames]
        : [];
    for (i = 0; i < keyList.length; i++) {
      var v = pick(ex, p, keyList[i], "");
      if (nz(v)) return v;
    }
    for (i = 0; i < nameList.length; i++) {
      if (fields && nz(fields[nameList[i]])) return fields[nameList[i]];
    }
    for (i = 0; i < keyList.length; i++) {
      if (fields && nz(fields[keyList[i]])) return fields[keyList[i]];
    }
    return "";
  }

  function compactCapabilityLevelKpi(raw) {
    var s = nz(raw);
    if (!s) return "";
    if (/not\s*measured|n\/a|none\s*documented|^unknown$/i.test(s)) return "";
    if (/excellent|institutional/i.test(s)) return "Excellent";
    if (/very\s*strong/i.test(s)) return "Very Strong";
    if (/^strong$/i.test(s)) return "Strong";
    if (/^moderate$/i.test(s)) return "Moderate";
    if (/^limited$/i.test(s)) return "Limited";
    if (/^standard$/i.test(s)) return "Standard";
    if (/^advanced$/i.test(s)) return "Advanced";
    if (/^proven$/i.test(s)) return "Proven";
    if (/^developing$/i.test(s)) return "Developing";
    if (/^basic$/i.test(s)) return "Basic";
    if (/advanced\s*centralized/i.test(s)) return "Advanced";
    if (/centralized\s*support/i.test(s)) return "Centralized";
    if (/property[-\s]level/i.test(s)) return "Property-level";
    if (/third[-\s]party/i.test(s)) return "Partner-led";
    if (/lifestyle\s*\/\s*experiential/i.test(s)) return "Experiential F&B";
    if (/significant\s*f&b/i.test(s)) return "Significant F&B";
    if (/moderate\s*f&b/i.test(s)) return "Moderate F&B";
    if (/limited\s*f&b/i.test(s)) return "Limited F&B";
    if (/rooms[-\s]only/i.test(s)) return "Rooms-only";
    if (/lender|investor[-\s]grade/i.test(s)) return "Institutional";
    if (/monthly\s*operating/i.test(s)) return "Monthly review";
    if (/basic\s*owner/i.test(s)) return "Basic";
    if (/custom\s*\/\s*project/i.test(s)) return "Custom";
    if (/structured/i.test(s)) return "Structured";
    if (/owner[-\s]aligned\s*partnership/i.test(s)) return "Owner-Aligned";
    if (/hybrid\s*platform/i.test(s)) return "Hybrid";
    if (/resort\s*and\s*lifestyle/i.test(s)) return "Resort/Lifestyle";
    if (/full[-\s]service\s*institutional/i.test(s)) return "Full-Service";
    if (/conversion\s*and\s*transition/i.test(s)) return "Conversion";
    if (/regional\s*hub/i.test(s)) return "Regional Hub";
    return compactSignalDisplay(s) || compactEngagementKpi(s);
  }

  function operatingSnapshotKpiValue(operatingPlatform, rowKey) {
    var list =
      operatingPlatform && Array.isArray(operatingPlatform.snapshotKpis)
        ? operatingPlatform.snapshotKpis
        : [];
    var i;
    for (i = 0; i < list.length; i++) {
      if (list[i] && list[i].rowKey === rowKey && nz(list[i].value)) return list[i].value;
    }
    return "";
  }

  function displayOperatingPlatformKpi(raw, compactFn) {
    if (!nz(raw)) return "—";
    var display = compactFn ? compactFn(raw) : compactCapabilityLevelKpi(raw);
    if (!nz(display) || !meaningfulSignal(display)) {
      display = compactSignalDisplay(raw) || nz(raw);
    }
    if (!nz(display)) return "—";
    if (display.length > 24) {
      display = compactSignalDisplay(display) || display.slice(0, 24);
    }
    return display;
  }

  /**
   * Operating Platform tab — five pillar strength signals (value-first KPI row).
   * Always emits five tiles (value "—" when Setup has no data).
   */
  function deriveOperatingPlatformSnapshotMetrics(ex, p, fields, operatingPlatform) {
    ex = ex || {};
    p = p || {};
    fields = fields || {};
    operatingPlatform = operatingPlatform || p.operatingPlatform || null;
    function row(labelLines, raw, defaultNote, compactFn) {
      var lines = Array.isArray(labelLines) ? labelLines : [labelLines];
      return {
        label: lines.join(" "),
        labelLines: lines,
        value: displayOperatingPlatformKpi(raw, compactFn),
        note: defaultNote,
      };
    }
    var reporting =
      pickSetupField(ex, p, fields, ["ownerReportingLevel"], [
        "Owner Reporting Level",
        "owner_reporting_level",
      ]) || pick(ex, p, "cap_kpi_reporting", "");
    var preOpening =
      pickSetupField(ex, p, fields, ["preOpeningSupportCapability"], [
        "Pre-Opening Support Capability",
        "pre_opening_support_capability",
      ]) || pick(ex, p, "cap_kpi_transition", "");
    var commercial =
      operatingSnapshotKpiValue(operatingPlatform, "revenue_management_capability") ||
      pickSetupField(ex, p, fields, ["revenueManagementCapability"], [
        "Revenue Management Capability",
        "revenue_management_capability",
      ]) ||
      pick(ex, p, "cap_kpi_operating_model", "");
    var conversion =
      operatingSnapshotKpiValue(operatingPlatform, "conversion_reflag") ||
      pickSetupField(ex, p, fields, ["conversionReflagExperience"], [
        "Conversion / Reflag Experience",
        "conversion_reflag_experience",
      ]) ||
      pick(ex, p, "conversionReflagExperience", "");
    var fbResort =
      operatingSnapshotKpiValue(operatingPlatform, "fb_capability") ||
      pickSetupField(ex, p, fields, ["fbCapabilityLevel", "fBCapabilityLevel"], [
        "F&B Capability Level",
        "f_b_capability_level",
        "fb_capability_level",
      ]) ||
      pick(ex, p, "cap_kpi_execution_strength", "");

    return [
      row(
        ["Commercial", "Engine"],
        commercial,
        "Revenue, pricing, distribution, and commercial execution strength",
        compactCapabilityLevelKpi
      ),
      row(
        ["Owner", "Reporting"],
        reporting ||
          operatingSnapshotKpiValue(operatingPlatform, "owner_reporting_level"),
        "Depth and structure of owner reporting and governance",
        compactReportingLevelKpi
      ),
      row(
        ["Pre-Opening", "Support"],
        preOpening ||
          operatingSnapshotKpiValue(operatingPlatform, "pre_opening_support"),
        "New-build, takeover, and transition readiness",
        compactCapabilityLevelKpi
      ),
      row(
        ["Conversion", "Capability"],
        conversion,
        "Reflag, PIP, repositioning, and turnaround experience",
        compactCapabilityLevelKpi
      ),
      row(
        ["F&B &", "Resort"],
        fbResort,
        "Resort, lifestyle, and food & beverage operating depth",
        compactCapabilityLevelKpi
      ),
    ];
  }

  function buildOperatingPlatformSnapshotSection(ex, p, fields, operatingPlatform) {
    var intro =
      '<p class="gold-mock-tab-empty odna-subsection-intro">Quick signals on day-to-day operating strength—from commercial execution and owner reporting through transitions and resort programming—use these when you compare operators for your asset.</p>';
    var metrics = deriveOperatingPlatformSnapshotMetrics(ex, p, fields, operatingPlatform);
    var count = metrics.length;
    var metricsHtml =
      '<div class="oe-op-snapshot-kpis oe-tab-snapshot-kpis ' +
      snapshotKpiRowWrapperClass(count) +
      '" style="grid-template-columns:' +
      snapshotKpiGridInlineStyle(count) +
      '">' +
      metrics.map(buildLeadershipSnapshotMetricCard).join("") +
      "</div>";
    return (
      '<section class="section oe-op-snapshot-section">' +
      '<h2 class="section-title">Operating Platform</h2>' +
      intro +
      metricsHtml +
      "</section>"
    );
  }

  function buildBrandSnapshotSection(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var B = root.OperatorBrandRelationshipsSections;
    var metrics =
      B && typeof B.deriveSnapshotMetrics === "function" ? B.deriveSnapshotMetrics(vm) : [];
    var scopeNotice =
      B && typeof B.buildScopeNoticeHtml === "function" ? B.buildScopeNoticeHtml(vm) : "";
    var intro =
      scopeNotice +
      '<p class="gold-mock-tab-empty odna-subsection-intro">Quick signals on brand depth, portfolio mix, and conversion experience—use these when you compare operators for your asset.</p>';
    return buildValueKpiSnapshotSection({
      title: "Brand & Relationship Snapshot",
      intro: intro,
      metrics: metrics,
      sectionClass: "oe-brand-snapshot-section",
      kpiClass: "oe-brand-snapshot-kpis",
    });
  }

  function buildBestFitSnapshotSection(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var B = root.OperatorBestFitDealProfileSections;
    var metrics =
      B && typeof B.deriveSnapshotMetrics === "function" ? B.deriveSnapshotMetrics(vm) : [];
    var intro =
      '<p class="gold-mock-tab-empty odna-subsection-intro">Quick signals on the kinds of projects and deals this operator is built to support—helpful context when you compare operators for your asset.</p>';
    return buildValueKpiSnapshotSection({
      title: "Project Fit Snapshot",
      intro: intro,
      metrics: metrics,
      sectionClass: "oe-bf-snapshot-section",
      kpiClass: "oe-bf-snapshot-kpis",
    });
  }

  function buildProofDecisionSignalsSection(vm, sectionTitle) {
    var ex = (vm && vm.ex) || {};
    var p = (vm && vm.prefill) || {};
    return decisionStripFiltered(
      [
        ["RevPAR lift range", pick(ex, p, "tr_signal_revpar", "")],
        ["Occupancy recovery window", pick(ex, p, "tr_signal_occ", "")],
        ["ADR stabilization", pick(ex, p, "tr_signal_adr", "")],
        ["Case repeatability", pick(ex, p, "tr_signal_repeat", "")],
      ],
      { sectionTitle: sectionTitle || "Track Record Signals" }
    );
  }

  function clusterContent(title, items) {
    var list = (items || []).map(nz).filter(Boolean);
    if (!list.length) return "";
    return cluster(title, list);
  }

  /** Same as clusterContent but spans 2 columns in `.quant-grid` (e.g. long brand lists). */
  function clusterContentWide(title, items) {
    var list = (items || []).map(nz).filter(Boolean);
    if (!list.length) return "";
    return cluster(title, list, "cluster--span-2");
  }

  /** Explorer headline + story: omit incomplete pairs to avoid dash-heavy cards */
  function insightCard(headline, story) {
    var h = nz(headline);
    var st = nz(story);
    if (!h && !st) return "";
    if (!st) return "";
    if (!h) return card("Highlight", st);
    return card(h, st);
  }

  /** Fixed section title + body (capabilities / owner cards) */
  function titledCard(title, body) {
    var b = nz(body);
    if (!b) return "";
    return card(title, b);
  }

  function normalizeSetupFieldKey(key) {
    return String(key || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "");
  }

  function formatSetupFieldValue(val) {
    if (val == null || val === "") return "";
    if (Array.isArray(val)) {
      return val
        .map(function (x) {
          return nz(x);
        })
        .filter(Boolean)
        .join("\n");
    }
    return nz(val);
  }

  /** Resolve a Setup / Master field from explorer JSON, prefill, or Airtable fields (fuzzy key match). */
  function pickSetupField(vm, prefillKeys, airtableNames) {
    var p = (vm && vm.prefill) || {};
    var f = (vm && vm.fields) || {};
    var ex = (vm && vm.ex) || {};
    var i;
    var want = {};
    var keys = prefillKeys || [];
    for (i = 0; i < keys.length; i++) {
      want[normalizeSetupFieldKey(keys[i])] = true;
      var fromEx = pick(ex, p, keys[i], "");
      if (fromEx) return formatSetupFieldValue(fromEx);
    }
    var names = airtableNames || [];
    for (i = 0; i < names.length; i++) {
      want[normalizeSetupFieldKey(names[i])] = true;
      var fromField = formatSetupFieldValue(f[names[i]]);
      if (fromField) return fromField;
    }
    var bags = [p, f, ex];
    for (var b = 0; b < bags.length; b++) {
      var bag = bags[b] || {};
      for (var key in bag) {
        if (!Object.prototype.hasOwnProperty.call(bag, key)) continue;
        if (!want[normalizeSetupFieldKey(key)]) continue;
        var formatted = formatSetupFieldValue(bag[key]);
        if (formatted) return formatted;
      }
    }
    return "";
  }

  function recognitionCard(title, body) {
    return card(title, nz(body) || "—");
  }

  function pickRecognitionNotableAchievements(vm) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var direct = pickSetupField(vm, ["notableAchievements", "achievements"], ["Notable Achievements"]);
    if (nz(direct)) return direct;
    var signals = [
      pick(ex, p, "overview_signal_1_value", ""),
      pick(ex, p, "overview_signal_2_value", ""),
      pick(ex, p, "overview_signal_3_value", ""),
    ].filter(function (s) {
      return nz(s);
    });
    if (signals.length) return signals.join("\n");
    return "";
  }

  /** Overview — always three cards: certifications, industry recognition, notable achievements. */
  function buildRecognitionSectionHtml(vm) {
    var industryRecognition =
      pickSetupField(vm, ["industryRecognition"], ["Industry Recognition"]) ||
      pickOwnerFacing(vm.ex, vm.prefill, "lead_narrative_functional");
    var cards = [
      recognitionCard(
        "Certifications & Standards",
        pickSetupField(vm, ["certifications"], ["Certifications", "Certifications Held"])
      ),
      recognitionCard("Industry Recognition", industryRecognition),
      recognitionCard("Notable Achievements", pickRecognitionNotableAchievements(vm)),
    ];
    return (
      '<section class="section oe-recognition-section">' +
      '<h2 class="section-title">Recognition</h2>' +
      '<div class="oe-recognition-grid ' +
      snapshotKpiRowWrapperClass(3) +
      '" style="grid-template-columns:' +
      snapshotKpiGridInlineStyle(3) +
      '">' +
      cards.join("") +
      "</div></section>"
    );
  }

  /** Company Story & Positioning: one card with labeled blocks (history, differentiators, philosophy, mission). */
  function profileDepthConsolidatedCard(prefill, vm) {
    var blocks = [];
    function addBlock(title, body) {
      var b = nz(body);
      if (!b) return;
      blocks.push("<h3>" + escapeHtml(title) + "</h3><p>" + escapeHtml(b) + "</p>");
    }
    addBlock("Company History", prefill.companyHistory);
    addBlock("Key Differentiators", prefill.differentiators);
    addBlock("Management Philosophy", prefill.managementPhilosophy);
    var ms = nz(prefill.missionStatement);
    if (ms && ms !== nz(vm && vm.statement)) addBlock("Mission Statement", ms);
    if (!blocks.length) return "";
    return '<div class="card oe-profile-depth-consolidated">' + blocks.join("") + "</div>";
  }

  function tabEmptyHint() {
    return (
      '<section class="section"><p class="gold-mock-tab-empty">' +
      "Nothing added for this area yet. Complete the matching sections in Operator Setup to populate this tab." +
      "</p></section>"
    );
  }

  function ensureTabBody(html) {
    var t = String(html || "").replace(/\s/g, "");
    if (!t) return tabEmptyHint();
    return html;
  }

  /**
   * Client-side fallback when detail API has censusFootprint but prefill was not merged (stale server).
   */
  function applyCensusFootprintFromDetailPayload(detailPayload, prefill, fields) {
    if (!prefill || nz(prefill.footprintPortfolioSource) === "hotel_census") return;
    var cf = detailPayload && detailPayload.censusFootprint;
    if (!cf || !cf.ok || !(cf.totals && cf.totals.totalHotels > 0)) return;

    if (cf.brandsPortfolioDetail && cf.brandsPortfolioDetail.length) {
      prefill.brandsPortfolioDetail = cf.brandsPortfolioDetail;
      if (fields) {
        try {
          fields["Brands Portfolio Detail"] = JSON.stringify(cf.brandsPortfolioDetail);
        } catch (e) {
          fields["Brands Portfolio Detail"] = "";
        }
      }
    }
    Object.keys(cf.geoFields || {}).forEach(function (k) {
      prefill[k] = cf.geoFields[k];
    });
    Object.keys(cf.chainScaleFields || {}).forEach(function (k) {
      prefill[k] = cf.chainScaleFields[k];
    });
    if (cf.totals) {
      prefill.geo_total_existing_hotels = String(cf.totals.totalExistingHotels || "");
      prefill.geo_total_existing_rooms = String(cf.totals.totalExistingRooms || "");
      prefill.geo_total_pipeline_hotels = String(cf.totals.totalPipelineHotels || "");
      prefill.geo_total_pipeline_rooms = String(cf.totals.totalPipelineRooms || "");
      prefill.totalProperties = String(cf.totals.totalHotels || "");
      prefill.totalRooms = String(
        (Number(cf.totals.totalExistingRooms) || 0) + (Number(cf.totals.totalPipelineRooms) || 0)
      );
    }
    prefill.footprintPortfolioSource = "hotel_census";
    prefill.footprintPortfolioManagementCompany = nz(cf.managementCompany);
  }

  function buildViewModel(detailPayload, listRow) {
    var prefill = (detailPayload && detailPayload.prefill) || {};
    var fields = (detailPayload && detailPayload.fields) || {};
    applyCensusFootprintFromDetailPayload(detailPayload, prefill, fields);
    var ex = mergeExplorerPrefill(prefill);
    var caseStudies = Array.isArray(detailPayload.caseStudiesDetail)
      ? detailPayload.caseStudiesDetail
      : [];
    var leadership = Array.isArray(detailPayload.leadershipTeam)
      ? detailPayload.leadershipTeam
      : [];
    var brandProfiles = Array.isArray(detailPayload.brandProfiles)
      ? detailPayload.brandProfiles
      : [];

    var ownerDiligenceQa = Array.isArray(detailPayload.ownerDiligenceQa)
      ? detailPayload.ownerDiligenceQa
      : Array.isArray(prefill.ownerDiligenceQa)
        ? prefill.ownerDiligenceQa
        : [];

    var operatorExplorerMaterials =
      detailPayload &&
      detailPayload.operatorExplorerMaterials &&
      typeof detailPayload.operatorExplorerMaterials === "object"
        ? detailPayload.operatorExplorerMaterials
        : { version: 1, blocks: [] };

    var leadershipPlatform =
      detailPayload &&
      detailPayload.leadershipPlatform &&
      typeof detailPayload.leadershipPlatform === "object"
        ? detailPayload.leadershipPlatform
        : prefill.leadershipPlatform && typeof prefill.leadershipPlatform === "object"
          ? prefill.leadershipPlatform
          : null;

    var infrastructurePlatform =
      detailPayload &&
      detailPayload.infrastructurePlatform &&
      typeof detailPayload.infrastructurePlatform === "object"
        ? detailPayload.infrastructurePlatform
        : prefill.infrastructurePlatform && typeof prefill.infrastructurePlatform === "object"
          ? prefill.infrastructurePlatform
          : null;

    var engagementReporting =
      detailPayload &&
      (detailPayload.engagementReporting || detailPayload.engagementPlatform) &&
      typeof (detailPayload.engagementReporting || detailPayload.engagementPlatform) === "object"
        ? detailPayload.engagementReporting || detailPayload.engagementPlatform
        : prefill.engagementReporting && typeof prefill.engagementReporting === "object"
          ? prefill.engagementReporting
          : prefill.engagementPlatform && typeof prefill.engagementPlatform === "object"
            ? prefill.engagementPlatform
            : null;

    var operatingPlatform =
      detailPayload &&
      detailPayload.operatingPlatform &&
      typeof detailPayload.operatingPlatform === "object"
        ? detailPayload.operatingPlatform
        : prefill.operatingPlatform && typeof prefill.operatingPlatform === "object"
          ? prefill.operatingPlatform
          : null;

    var brandRelationships =
      detailPayload &&
      detailPayload.brandRelationships &&
      typeof detailPayload.brandRelationships === "object"
        ? detailPayload.brandRelationships
        : prefill.brandRelationships && typeof prefill.brandRelationships === "object"
          ? prefill.brandRelationships
          : null;

    var companyName =
      nz(fields["Company Name"]) ||
      nz(prefill.companyName) ||
      nz(listRow && listRow.companyName) ||
      "Operator";

    var logoUrl =
      firstAttachmentUrl(fields["Company Logo"]) ||
      firstAttachmentUrl(prefill.companyLogo) ||
      nz(listRow && listRow.logo) ||
      "";

    var tagline =
      nz(prefill.companyTagline) ||
      nz(fields["Company Tagline"]) ||
      "";

    var statement =
      nz(prefill.companyDescription) ||
      nz(fields["Company Description"]) ||
      nz(prefill.missionStatement) ||
      "";

    var hq =
      nz(prefill.headquarters) ||
      nz(fields["Headquarters"]) ||
      nz(fields["Headquarters Location"]) ||
      "";

    var yearsBiz = nz(prefill.yearsInBusiness);
    var totalHotels = pick(ex, prefill, "totalProperties", prefill.totalProperties);
    var totalRooms = pick(ex, prefill, "totalRooms", prefill.totalRooms);
    var brandCount =
      nz(prefill.numberOfBrands) ||
      (Array.isArray(prefill.brands) ? String(prefill.brands.length) : "");

    var brandedMix =
      heroBrandMixValue(
        nz(fields["Branded vs Independent Mix"]) || nz(prefill.brandedVsIndependentMix) || ""
      );

    var heroMeta = [];
    if (meaningfulMetaValue(hq)) heroMeta.push(["Headquarters", hq]);
    if (meaningfulMetaValue(yearsBiz)) heroMeta.push(["Years in Business", yearsBiz]);
    var hotelsStr = formatInt(totalHotels);
    if (meaningfulMetaValue(hotelsStr)) heroMeta.push(["Hotels Managed", hotelsStr]);
    var roomsStr = formatInt(totalRooms);
    if (meaningfulMetaValue(roomsStr)) heroMeta.push(["Rooms Managed", roomsStr]);
    var assetFocusStr = assetFocusHeroValue(prefill, fields);
    if (meaningfulMetaValue(assetFocusStr)) heroMeta.push(["Asset Focus", assetFocusStr]);
    if (meaningfulMetaValue(brandCount)) heroMeta.push(["Brand Relationships", brandCount]);
    if (meaningfulMetaValue(brandedMix)) heroMeta.push(["Brand Mix", brandedMix]);

    return {
      ex: ex,
      prefill: prefill,
      fields: fields,
      companyName: companyName,
      logoUrl: logoUrl,
      tagline: tagline,
      statement: statement,
      heroMeta: heroMeta,
      brandMixDisplay: brandedMix,
      caseStudies: caseStudies,
      leadership: leadership,
      brandProfiles: brandProfiles,
      listRow: listRow || {},
      ownerDiligenceQa: ownerDiligenceQa,
      operatorExplorerMaterials: operatorExplorerMaterials,
      leadershipPlatform: leadershipPlatform,
      infrastructurePlatform: infrastructurePlatform,
      engagementReporting: engagementReporting,
      engagementPlatform: engagementReporting,
      operatingPlatform: operatingPlatform,
      brandRelationships: brandRelationships,
      explorerHeroVerification:
        nz(detailPayload.explorerHeroVerification) ||
        nz(prefill.explorerHeroVerification) ||
        nz(listRow && listRow.explorerHeroVerification),
      explorerHeroDataSource:
        nz(detailPayload.explorerHeroDataSource) ||
        nz(prefill.explorerHeroDataSource) ||
        nz(listRow && listRow.explorerHeroDataSource),
      governance: detailPayload.governance || null,
    };
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

  function cluster(title, items, extraClass) {
    var cls = "cluster" + (extraClass ? " " + String(extraClass).trim() : "");
    return (
      '<div class="' +
      escapeHtml(cls) +
      '"><h3>' +
      escapeHtml(title) +
      "</h3><ul>" +
      items
        .map(function (i) {
          return "<li>" + escapeHtml(i) + "</li>";
        })
        .join("") +
      "</ul></div>"
    );
  }

  function card(title, body) {
    return (
      '<div class="card"><h3>' +
      escapeHtml(title) +
      "</h3><p>" +
      escapeHtml(body) +
      "</p></div>"
    );
  }

  function proofCard(img, title, meta, lines) {
    var src = img || PLACEHOLDER_PROOF;
    return (
      '<article class="proof-card"><img src="' +
      escapeHtml(src) +
      '" alt="' +
      escapeHtml(title) +
      '"' +
      exportPdfImgAttrs() +
      '><div class="proof-body"><div class="proof-title">' +
      escapeHtml(title) +
      '</div><div class="proof-meta">' +
      escapeHtml(meta) +
      "</div>" +
      (lines || [])
        .map(function (l) {
          return '<div class="proof-line">' + escapeHtml(l) + "</div>";
        })
        .join("") +
      "</div></article>"
    );
  }

  function caseStudyNarrativeColumn(heading, body) {
    if (!nz(body)) return "";
    return (
      '<div class="case-study-narrative__col">' +
      "<h4>" +
      escapeHtml(heading) +
      "</h4><p>" +
      escapeHtml(body) +
      "</p></div>"
    );
  }

  var CASE_STUDY_FIVE_PART_HEADINGS = [
    ["Challenge", "challenge"],
    ["Operator Action", "operatorAction"],
    ["Outcome", "outcome"],
    ["Why It Matters", "whyItMatters"],
    ["Data Confidence", "dataStatus"],
  ];

  function proofCardFivePart(img, title, meta, fivePart) {
    var src = img || PLACEHOLDER_PROOF;
    fivePart = fivePart || {};
    var cols = CASE_STUDY_FIVE_PART_HEADINGS.map(function (pair) {
      var body = nz(fivePart[pair[1]]);
      return caseStudyNarrativeColumn(pair[0], body || "\u2014");
    }).join("");
    return (
      '<article class="proof-card proof-card--narrative proof-card--five-part"><img src="' +
      escapeHtml(src) +
      '" alt="' +
      escapeHtml(title) +
      '"' +
      exportPdfImgAttrs() +
      '><div class="proof-body"><div class="proof-title">' +
      escapeHtml(title) +
      '</div><div class="proof-meta">' +
      escapeHtml(meta) +
      '</div><div class="case-study-narrative case-study-narrative--cols-5">' +
      cols +
      "</div></div></article>"
    );
  }

  function proofCardNarrative(img, title, meta, narrative) {
    var src = img || PLACEHOLDER_PROOF;
    narrative = narrative || {};
    var colDefs = [
      ["Before", narrative.before],
      ["Operator Action", narrative.action],
      ["After", narrative.after],
    ].filter(function (pair) {
      return nz(pair[1]);
    });
    if (!colDefs.length) return proofCard(img, title, meta, [narrative.after, narrative.action].filter(Boolean));
    var colCount = Math.min(3, Math.max(colDefs.length, 1));
    var cols = colDefs
      .map(function (pair) {
        return caseStudyNarrativeColumn(pair[0], pair[1]);
      })
      .join("");
    var relevance = nz(narrative.relevance)
      ? '<p class="case-study-relevance">' + escapeHtml(narrative.relevance) + "</p>"
      : "";
    return (
      '<article class="proof-card proof-card--narrative"><img src="' +
      escapeHtml(src) +
      '" alt="' +
      escapeHtml(title) +
      '"' +
      exportPdfImgAttrs() +
      '><div class="proof-body"><div class="proof-title">' +
      escapeHtml(title) +
      '</div><div class="proof-meta">' +
      escapeHtml(meta) +
      '</div><div class="case-study-narrative case-study-narrative--cols-' +
      colCount +
      '">' +
      cols +
      "</div>" +
      relevance +
      "</div></article>"
    );
  }

  function truncateCaseStudyText(text, max) {
    var t = nz(text);
    if (!t || t.length <= max) return t;
    return t.slice(0, max - 1) + "\u2026";
  }

  function caseStudyBeChips(cs) {
    var seen = {};
    var out = [];
    [cs.hotel_type, cs.region, cs.situation, cs.branded_independent].forEach(function (p) {
      var t = nz(p);
      if (!t) return;
      var key = t.toLowerCase();
      if (seen[key]) return;
      seen[key] = true;
      out.push(t);
    });
    return out;
  }

  function buildCaseStudyBePayload(cs) {
    var fp = deriveCaseStudyFivePart(cs);
    var title = nz(cs.property_name) || nz(cs.hotel_type) || "Case study";
    var loc = nz(cs.region);
    var situation = nz(cs.situation);
    var subtitle = loc
      ? loc + (situation ? " \u00b7 " + situation : "")
      : situation || "Case study";
    var chips = caseStudyBeChips(cs);
    return {
      title: title,
      subtitle: subtitle,
      challenge: nz(fp.challenge) || "\u2014",
      operatorAction: nz(fp.operatorAction) || "\u2014",
      outcome: nz(fp.outcome) || "\u2014",
      whyItMatters: nz(fp.whyItMatters) || "\u2014",
      dataStatus: nz(fp.dataStatus) || "\u2014",
      tags: chips,
      externalUrl: "",
    };
  }

  function buildCaseStudyBeCard(cs, index, payload) {
    if (!payload) return "";
    var title = payload.title;
    var loc = nz(cs.region);
    var imgUrl = nz(cs.image_url);
    var badge = nz(cs.situation) || "Case Study";
    var metaLine = nz(cs.hotel_type) || "\u2014";
    var scenario = payload.tags.length
      ? payload.tags
          .slice(0, 3)
          .map(function (t) {
            return String(t).toUpperCase();
          })
          .join(" / ")
      : "";
    var teaser = truncateCaseStudyText(
      payload.challenge !== "\u2014" ? payload.challenge : payload.operatorAction,
      220
    );
    var tagsHtml = payload.tags.length
      ? payload.tags
          .map(function (t) {
            return '<span class="tag-chip">' + escapeHtml(String(t).toUpperCase()) + "</span>";
          })
          .join("")
      : '<span class="tag-chip">&nbsp;</span>';

    var topInner = "";
    if (imgUrl && /^https?:\/\//i.test(imgUrl)) {
      topInner =
        '<img src="' +
        escapeHtml(imgUrl) +
        '" alt="' +
        escapeHtml(title) +
        '"' +
        (isExportPdfMode()
          ? ' loading="eager" decoding="sync" referrerpolicy="no-referrer"'
          : ' loading="lazy"') +
        " />";
    }

    return (
      '<article class="property-example-card">' +
      '<div class="property-example-card__top">' +
      topInner +
      '<span class="property-example-card__badge">' +
      escapeHtml(badge) +
      "</span>" +
      '<div class="property-example-card__titles">' +
      "<h4>" +
      escapeHtml(title) +
      "</h4><span>" +
      escapeHtml(loc || "\u2014") +
      "</span></div></div>" +
      '<div class="property-example-card__mid">' +
      '<div class="property-example-card__meta">' +
      escapeHtml(metaLine) +
      "</div>" +
      (scenario
        ? '<div class="property-example-card__scenario">' + escapeHtml(scenario) + "</div>"
        : "") +
      "<p>" +
      escapeHtml(teaser) +
      "</p></div>" +
      '<div class="property-example-card__bottom">' +
      '<div class="property-example-card__tags">' +
      tagsHtml +
      "</div>" +
      '<button type="button" class="btn" data-odna-case-study="' +
      index +
      '">View Property</button></div></article>'
    );
  }

  var CASE_STUDY_INITIAL_VISIBLE = 3;

  function caseStudiesExpanderButtonHtml(total, initial) {
    initial = initial || CASE_STUDY_INITIAL_VISIBLE;
    if (!total || total <= initial) return "";
    return (
      '<div class="odna-case-studies-be__actions">' +
      '<button type="button" class="btn odna-case-studies-expand" data-odna-case-studies-expand ' +
      'aria-expanded="false" data-odna-case-studies-initial="' +
      initial +
      '">' +
      escapeHtml("View all " + total + " case studies") +
      "</button></div>"
    );
  }

  function markCaseStudyCardCollapsed(cardHtml, index, initial) {
    if (!cardHtml || index < initial) return cardHtml;
    return cardHtml.replace(
      '<article class="property-example-card"',
      '<article class="property-example-card property-example-card--collapsed"'
    );
  }

  function markProofCardCollapsed(cardHtml, index, initial) {
    if (!cardHtml || index < initial) return cardHtml;
    return cardHtml.replace('<article class="proof-card', '<article class="proof-card proof-card--collapsed');
  }

  /** Brand Explorer property-example-card grid (Operator DNA profile). */
  function buildBrandExplorerCaseStudiesGridHtml(cases) {
    var list = (cases || []).filter(caseStudyHasContent);
    if (!list.length) return "";

    var initial = CASE_STUDY_INITIAL_VISIBLE;
    var payloads = [];
    var sourceCases = [];
    var cards = list
      .map(function (cs, index) {
        var payload = buildCaseStudyBePayload(cs);
        if (!payload) return "";
        var payloadIdx = payloads.length;
        payloads.push(payload);
        sourceCases.push(cs);
        return markCaseStudyCardCollapsed(
          buildCaseStudyBeCard(cs, payloadIdx, payload),
          index,
          initial
        );
      })
      .filter(Boolean)
      .join("");

    if (!cards) return "";

    global._odnaCaseStudyPayloads = payloads;
    global._odnaCaseStudySourceCases = sourceCases;

    return (
      '<div class="be-atelier-oe odna-case-studies-be" data-odna-case-study-total="' +
      list.length +
      '">' +
      '<div class="property-example-grid">' +
      cards +
      "</div>" +
      caseStudiesExpanderButtonHtml(list.length, initial) +
      "</div>"
    );
  }

  function proofGridFromCasesFivePart(cases) {
    var initial = CASE_STUDY_INITIAL_VISIBLE;
    var list = (cases || [])
      .map(function (cs) {
        return proofFromCaseStudy(cs, { useCaseStudyFivePart: true });
      })
      .filter(Boolean);
    if (!list.length) return "";
    var html = list
      .map(function (p, index) {
        return markProofCardCollapsed(
          proofCardFivePart(p.img, p.title, p.meta, p.fivePart),
          index,
          initial
        );
      })
      .join("");
    return (
      '<div class="odna-proof-studies-expander" data-odna-case-study-total="' +
      list.length +
      '">' +
      '<div class="proof-grid proof-grid--case-studies proof-grid--case-studies-five-part">' +
      html +
      "</div>" +
      caseStudiesExpanderButtonHtml(list.length, initial) +
      "</div>"
    );
  }

  function resolveCaseStudiesGridHtml(proof, panelOpts) {
    panelOpts = panelOpts || {};
    var preferBrandExplorerCards =
      panelOpts.useBrandExplorerCaseStudies === true ||
      panelOpts.ownerFacingProofKpis === true;

    if (panelOpts.useCaseStudyFivePart) {
      return proofGridFromCasesFivePart(proof);
    }
    if (preferBrandExplorerCards) {
      var beHtml = buildBrandExplorerCaseStudiesGridHtml(proof);
      if (beHtml) return beHtml;
      if (
        global.OperatorDnaCaseStudiesBe &&
        global.OperatorDnaCaseStudiesBe.buildCaseStudiesExplorerGridHtml
      ) {
        var extHtml = global.OperatorDnaCaseStudiesBe.buildCaseStudiesExplorerGridHtml(proof);
        if (extHtml) return extHtml;
      }
      if (typeof console !== "undefined" && console.warn) {
        console.warn(
          "[gold-mock] property-example case studies empty; using compact cards (not narrative columns)"
        );
      }
      return proofGridFromCases(proof, {
        caseStudiesSection: true,
        omitNarrativeLayout: true,
      });
    }
    return proofGridFromCases(proof, { caseStudiesSection: true });
  }

  function proofGridFromCases(cases, gridOpts) {
    gridOpts = gridOpts || {};
    var initial = CASE_STUDY_INITIAL_VISIBLE;
    var list = (cases || [])
      .map(function (cs) {
        var p = proofFromCaseStudy(cs);
        if (!p || !gridOpts.omitNarrativeLayout || !p.narrative) return p;
        var n = p.narrative;
        var lines = [nz(n.before), nz(n.action), nz(n.after), nz(n.relevance)].filter(Boolean);
        return { img: p.img, title: p.title, meta: p.meta, lines: lines.length ? lines : [nz(cs.outcome)] };
      })
      .filter(Boolean);
    if (!list.length) return "";
    var useNarrative =
      !gridOpts.omitNarrativeLayout &&
      list.some(function (p) {
        return p.narrative;
      });
    var html = list
      .map(function (p, index) {
        var card;
        if (p.narrative) {
          card = proofCardNarrative(p.img, p.title, p.meta, p.narrative);
        } else {
          card = proofCard(p.img, p.title, p.meta, p.lines || []);
        }
        return markProofCardCollapsed(card, index, initial);
      })
      .join("");
    var gridClass = "proof-grid";
    if (useNarrative) {
      gridClass += gridOpts.caseStudiesSection
        ? " proof-grid--case-studies"
        : " proof-grid--case-narrative";
    }
    if (list.length <= initial) {
      return '<div class="' + gridClass + '">' + html + "</div>";
    }
    return (
      '<div class="odna-proof-studies-expander" data-odna-case-study-total="' +
      list.length +
      '">' +
      '<div class="' +
      gridClass +
      '">' +
      html +
      "</div>" +
      caseStudiesExpanderButtonHtml(list.length, initial) +
      "</div>"
    );
  }

  /** Card body uses `summary` (exec_*_summary); image hover uses `bioHover` (exec_*_bio). */
  function leaderCard(img, name, titleLine, summary, roleLine, bioHover, leader) {
    var summaryText = nz(summary);
    var bioText = nz(bioHover);
    var src = img || PLACEHOLDER_LEADER;
    var overlayHtml = bioText
      ? '<div class="leader-bio-overlay"><strong>Executive Bio:</strong> ' +
        escapeHtml(bioText) +
        "</div>"
      : "";
    var profileApi =
      (typeof global !== "undefined" && global.OperatorLeadershipProfileDetail) ||
      (typeof window !== "undefined" && window.OperatorLeadershipProfileDetail);
    var profileDetail =
      profileApi && profileApi.buildLeaderProfileDetailHtml
        ? profileApi.buildLeaderProfileDetailHtml(leader || {})
        : "";
    var summaryHtml = summaryText
      ? '<div class="leader-summary">' + escapeHtml(summaryText) + "</div>"
      : "";
    return (
      '<article class="leader-card' +
      (profileDetail ? " leader-card--with-profile" : "") +
      '"><div class="leader-image-wrap"><img src="' +
      escapeHtml(src) +
      '" alt="' +
      escapeHtml(name) +
      '"' +
      exportPdfImgAttrs() +
      ">" +
      overlayHtml +
      '</div><div class="leader-body"><div class="leader-name">' +
      escapeHtml(name) +
      '</div><div class="leader-meta">' +
      escapeHtml(titleLine) +
      " · " +
      escapeHtml(roleLine) +
      "</div>" +
      summaryHtml +
      profileDetail +
      "</div></article>"
    );
  }

  function leaderCardFromLeader(L) {
    return leaderCard(
      L && L.headshotUrl,
      nz(L && L.name),
      nz(L && L.title) || "—",
      nz(L && L.summary) || "",
      nz(L && L.function) || "—",
      nz(L && L.bio) || "",
      L || {}
    );
  }

  function marketsDerivedMetrics(vm) {
    var p = vm.prefill || {};
    var ex = (vm && vm.ex) || {};
    var regionList = uniqueStrings(
      []
        .concat(arrayish(p.regions || p.regionsSupported))
        .concat(arrayish(ex.regions))
    );
    var countryList = uniqueStrings(
      []
        .concat(arrayish(p.activeCountries))
        .concat(arrayish(ex.activeCountries))
        .concat(arrayish(p.priorityCountries))
        .concat(arrayish(p.countriesServed))
    );
    var cityList = uniqueStrings(
      []
        .concat(linesFromText(p.specificMarkets || ""))
        .concat(arrayish(p.activeMarkets))
        .concat(arrayish(ex.activeMarkets))
    );
    var marketsOperated = nz(p.numberOfMarkets) || nz(ex.numberOfMarkets);
    var countriesIsMarketsFallback = false;
    var countriesDisplay = "";
    if (countryList.length) {
      countriesDisplay = String(countryList.length);
    } else if (marketsOperated) {
      countriesDisplay = marketsOperated;
      countriesIsMarketsFallback = true;
    }
    return {
      regions: regionList.length ? String(regionList.length) : "",
      regionNames: regionList.slice(0, 6).join(", "),
      countries: countriesDisplay,
      countriesIsMarketsFallback: countriesIsMarketsFallback,
      countryNames: countryList.slice(0, 6).join(", "),
      cities: cityList.length ? String(cityList.length) : "",
      cityNames: cityList.slice(0, 6).join(", "),
      coverage: nz(p.regionalManagementTeams) || nz(p.primaryServiceModel) || "",
    };
  }

  function marketExperienceThreeLayerHtml(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var M = root.OperatorMarketExperienceSection;
    if (!M || typeof M.buildThreeLayerSectionHtml !== "function") return "";
    return M.buildThreeLayerSectionHtml(vm) || "";
  }

  function marketsFootprintSubsectionsHtml(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var S = root.OperatorMarketsFootprintSections;
    if (!S || typeof S.buildAllSectionsHtml !== "function") return "";
    return S.buildAllSectionsHtml(vm) || "";
  }

  function infrastructureDetailSectionsHtml(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var I = root.OperatorInfrastructureSections;
    if (!I || typeof I.buildAllSectionsHtml !== "function") return "";
    return I.buildAllSectionsHtml(vm) || "";
  }

  function engagementReportingSectionsHtml(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var E = root.OperatorEngagementReportingSections;
    if (!E || typeof E.buildAllSectionsHtml !== "function") return "";
    return E.buildAllSectionsHtml(vm) || "";
  }

  function brandRelationshipsSectionsHtml(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var B = root.OperatorBrandRelationshipsSections;
    if (!B || typeof B.buildAllSectionsHtml !== "function") return "";
    return B.buildAllSectionsHtml(vm) || "";
  }

  function bestFitDealProfileSectionsHtml(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var B = root.OperatorBestFitDealProfileSections;
    if (!B || typeof B.buildAllSectionsHtml !== "function") return "";
    return B.buildAllSectionsHtml(vm) || "";
  }

  function operatingPlatformSectionsHtml(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var O = root.OperatorOperatingPlatformSections;
    if (!O || typeof O.buildAllSectionsHtml !== "function") return "";
    return O.buildAllSectionsHtml(vm) || "";
  }

  function bestFitOwnerProjectProfileOverviewHtml(vm) {
    var root =
      typeof globalThis !== "undefined"
        ? globalThis
        : typeof window !== "undefined"
          ? window
          : {};
    var B = root.OperatorBestFitDealProfileSections;
    if (!B || typeof B.buildOverviewBestFitOwnerProjectSection !== "function") {
      return "";
    }
    return B.buildOverviewBestFitOwnerProjectSection(vm) || "";
  }

  function buildPanels(vm, panelOpts) {
    panelOpts = panelOpts || {};
    var ex = vm.ex;
    var p = vm.prefill;
    var proof = (vm.caseStudies || []).filter(caseStudyHasContent);
    var leadersAllRaw = vm.leadership || [];
    var leadersNamed = leadersAllRaw.filter(function (L) {
      return nz(L.name);
    });
    var m = marketsDerivedMetrics(vm);

    var leadersOverview = leadersNamed.slice(0, 3);
    var leadersAll = leadersNamed;

    var caseStudiesGridHtml = resolveCaseStudiesGridHtml(proof, panelOpts);

    function heroSummarySection() {
      return operatorQuickFactsSectionHtml(vm);
    }

    var bestAt =
      insightCard(
        pick(ex, p, "overview_bestat_1_headline", ""),
        pick(ex, p, "overview_bestat_1_story", "")
      ) +
      insightCard(
        pick(ex, p, "overview_bestat_2_headline", ""),
        pick(ex, p, "overview_bestat_2_story", "")
      ) +
      insightCard(
        pick(ex, p, "overview_bestat_3_headline", ""),
        pick(ex, p, "overview_bestat_3_story", "")
      );

    var whyOwners =
      insightCard(
        pick(ex, p, "overview_why_1_headline", ""),
        pick(ex, p, "overview_why_1_story", "")
      ) +
      insightCard(
        pick(ex, p, "overview_why_2_headline", ""),
        pick(ex, p, "overview_why_2_story", "")
      ) +
      insightCard(
        pick(ex, p, "overview_why_3_headline", ""),
        pick(ex, p, "overview_why_3_story", "")
      );

    var leadershipSnap =
      leadersOverview
        .map(function (L) {
          return leaderCardFromLeader(L);
        })
        .join("") || "";

    var profileDepthHtml = profileDepthConsolidatedCard(p, vm);
    var recognitionSectionHtml = buildRecognitionSectionHtml(vm);
    var bestFitOwnerOverviewHtml = bestFitOwnerProjectProfileOverviewHtml(vm);

    var ProfilePositioning =
      heroSummarySection() +
      (profileDepthHtml
        ? '<section class="section"><h2 class="section-title">Company Story &amp; Positioning</h2>' +
          profileDepthHtml +
          "</section>"
        : "") +
      (bestAt
        ? '<section class="section"><h2 class="section-title">What They Are Best At</h2><div class="grid-3">' +
          bestAt +
          "</div></section>"
        : "") +
      (whyOwners
        ? '<section class="section"><h2 class="section-title">Why Owners Consider This Operator</h2><div class="grid-3">' +
          whyOwners +
          "</div></section>"
        : "") +
      bestFitOwnerOverviewHtml +
      (leadershipSnap
        ? '<section class="section"><h2 class="section-title">Leadership Snapshot</h2><div class="proof-grid proof-grid--leadership-aligned">' +
          leadershipSnap +
          "</div></section>"
        : "") +
      recognitionSectionHtml;

    var operatingPlatformSnapshotHtml = buildOperatingPlatformSnapshotSection(
      ex,
      p,
      vm.fields || {},
      vm.operatingPlatform
    );

    var operatingPlatformPillarsHtml = operatingPlatformSectionsHtml(vm);

    var OperatingPlatform = operatingPlatformSnapshotHtml + operatingPlatformPillarsHtml;

    var BrandRelationships =
      buildBrandSnapshotSection(vm) + brandRelationshipsSectionsHtml(vm);

    var marketsSnapshotHtml = buildMarketsFootprintSnapshotSection(vm, m, ex, p);

    var footprintMetricsHtml = footprintMetricsSection(p);

    var MarketsFootprint =
      marketsSnapshotHtml +
      marketExperienceThreeLayerHtml(vm) +
      marketsFootprintSubsectionsHtml(vm) +
      (footprintMetricsHtml || "");

    var portfolioScaleLine = (function () {
      var h = formatInt(pick(ex, p, "totalProperties", p.totalProperties));
      var r = formatInt(pick(ex, p, "totalRooms", p.totalRooms));
      if (!meaningfulMetaValue(h) && !meaningfulMetaValue(r)) return "";
      if (meaningfulMetaValue(h) && meaningfulMetaValue(r)) return h + " hotels / " + r + " rooms";
      return meaningfulMetaValue(h) ? h + " hotels" : r + " rooms";
    })();

    var OwnerEngagement =
      buildEngagementSnapshotSection(ex, p) + engagementReportingSectionsHtml(vm);

    var InfrastructureData =
      buildInfrastructureSnapshotSection(ex, p) + infrastructureDetailSectionsHtml(vm);

    var leadProfiles =
      leadersAll
        .map(function (L) {
          return leaderCardFromLeader(L);
        })
        .join("") || "";

    var leadProfileGridAttrs =
      global.OperatorLeadershipProfileDetail &&
      global.OperatorLeadershipProfileDetail.leadershipProfileGridAttrs
        ? global.OperatorLeadershipProfileDetail.leadershipProfileGridAttrs(
            leadersAll.length
          )
        : 'class="oe-leader-profile-grid oe-leader-profile-grid--aligned" style="--oe-leader-profile-cols:2"';

    var leadershipTeamSections =
      global.OperatorLeadershipTeamSections &&
      global.OperatorLeadershipTeamSections.buildLeadershipTeamSectionsHtml
        ? global.OperatorLeadershipTeamSections.buildLeadershipTeamSectionsHtml(vm)
        : "";

    var Leadership =
      buildLeadershipSnapshotSection(vm) +
      (leadProfiles
        ? '<section class="section oe-leadership-profiles-section"><h2 class="section-title">Leadership Profiles</h2>' +
          '<p class="gold-mock-tab-empty odna-subsection-intro">Structured experience, markets, and expertise from Operator Setup profile fields (or inferred from bios when fields are empty).</p>' +
          "<div " +
          leadProfileGridAttrs +
          ">" +
          leadProfiles +
          "</div></section>"
        : "") +
      leadershipTeamSections;

    var BestFitDealProfile =
      buildBestFitSnapshotSection(vm) + bestFitDealProfileSectionsHtml(vm);

    function lenderRefSignal(v) {
      var s = nz(v).toLowerCase();
      if (!s) return "";
      if (s === "yes" || s === "available" || s === "true") return "Available";
      if (s === "no" || s === "none" || s === "false") return "Not highlighted";
      return nz(v);
    }

    function ownerRefDisplay() {
      var v = p.ownerReferences;
      if (v == null || v === "") return "";
      var n = Number(String(v).replace(/,/g, ""));
      if (Number.isFinite(n)) return formatInt(n);
      return nz(v);
    }

    function diligenceClusterBlock(category, pairs) {
      if (!pairs || !pairs.length) return "";
      var lis = pairs
        .map(function (pair) {
          var q = nz(pair.q);
          var a = nz(pair.a);
          if (!q && !a) return "";
          var qHtml = q
            ? '<div class="diligence-q"><span class="diligence-q-label" aria-hidden="true">Q</span><span class="diligence-q-text">' +
              escapeHtml(q) +
              "</span></div>"
            : "";
          var aHtml = a
            ? '<div class="diligence-a"><span class="diligence-a-label" aria-hidden="true">A</span><span class="diligence-a-text">' +
              escapeHtml(a) +
              "</span></div>"
            : "";
          return "<li>" + qHtml + aHtml + "</li>";
        })
        .filter(Boolean)
        .join("");
      if (!lis) return "";
      return (
        '<div class="cluster diligence-cluster"><h3>' +
        escapeHtml(category) +
        '</h3><ul class="diligence-qa-list">' +
        lis +
        "</ul></div>"
      );
    }

    function diligenceLightSection(rows) {
      if (!rows || !rows.length) return "";
      var byCat = {};
      rows.forEach(function (r) {
        var cat = nz(r && r.category) || "General";
        var q = nz(r && r.question);
        var a = nz(r && r.answer);
        if (!q && !a) return;
        if (!byCat[cat]) byCat[cat] = [];
        if (byCat[cat].length < 3) byCat[cat].push({ q: q, a: a });
      });
      var inner = Object.keys(byCat)
        .map(function (cat) {
          return diligenceClusterBlock(cat, byCat[cat]);
        })
        .join("");
      if (!inner) return "";
      return (
        '<section class="section"><h2 class="section-title">Owner Diligence Highlights</h2><div class="grid-2">' +
        inner +
        "</div></section>"
      );
    }

    var trHotels = formatInt(pick(ex, p, "totalProperties", p.totalProperties));
    var trYears = nz(p.yearsInBusiness);

    function buildTrackRecordKpiSection(proofList, kpiOpts) {
      kpiOpts = kpiOpts || {};
      var ownerFacing = !!kpiOpts.ownerFacingProofKpis;
      var pairs = ownerFacing
        ? [
            ["Properties managed", trHotels],
            ["Operating markets", nz(p.numberOfMarkets)],
            ["Years in business", trYears],
            ["Owner references", ownerRefDisplay()],
            ["Lender references", lenderRefSignal(p.lenderReferences)],
          ]
        : [
            ["Properties", trHotels],
            ["Markets operated", nz(p.numberOfMarkets)],
            ["Years in business", trYears],
            ["Case Studies on profile", proofList.length ? String(proofList.length) : ""],
            ["Owner references", ownerRefDisplay()],
            ["Lender references", lenderRefSignal(p.lenderReferences)],
          ];
      if (ownerFacing && kpiOpts.includeCaseStudyCountKpi) {
        pairs.splice(3, 0, [
          "Case studies",
          proofList.length ? String(proofList.length) : "",
        ]);
      }
      var metrics = pairs
        .map(function (pair) {
          return {
            label: pair[0],
            value: nz(pair[1]),
            note: "",
          };
        })
        .filter(function (row) {
          return meaningfulSignal(row.value);
        });
      if (!metrics.length) return "";

      var count = metrics.length;
      var metricsHtml =
        '<div class="oe-tab-snapshot-kpis oe-snapshot-kpi-row oe-tab-snapshot-kpis--single-row" style="grid-template-columns:' +
        snapshotKpiGridInlineStyle(count) +
        '">' +
        metrics.map(buildLeadershipSnapshotMetricCard).join("") +
        "</div>";

      if (ownerFacing && kpiOpts.omitProofTrackRecordKpiSectionTitle) {
        return '<section class="section odna-track-record-kpis">' + metricsHtml + "</section>";
      }
      return buildValueKpiSnapshotSection({
        title: "Proof & Track Record",
        intro: "",
        metrics: metrics,
        sectionClass: "oe-proof-track-record-section",
        kpiClass: "oe-proof-track-record-kpis",
      });
    }

    var trHeader = buildTrackRecordKpiSection(proof, panelOpts);

    var ProofTrackRecord =
      (trHeader || "") +
      (caseStudiesGridHtml
        ? '<section class="section"><h2 class="section-title">Case Studies</h2>' +
          caseStudiesGridHtml +
          "</section>"
        : "") +
      diligenceLightSection(vm.ownerDiligenceQa || []) +
      (panelOpts.omitProofDecisionSignals
        ? ""
        : buildProofDecisionSignalsSection(vm, "Decision Signals"));

    return {
      "Profile & Positioning": ensureTabBody(ProfilePositioning),
      "Operating Platform": ensureTabBody(OperatingPlatform),
      "Brand & Relationships": ensureTabBody(BrandRelationships),
      "Markets & Footprint": ensureTabBody(MarketsFootprint),
      "Owner Engagement & Reporting": ensureTabBody(OwnerEngagement),
      "Infrastructure & Data": ensureTabBody(InfrastructureData),
      Leadership: ensureTabBody(Leadership),
      "Project Fit & Deal Profile": ensureTabBody(BestFitDealProfile),
      "Proof & Track Record": ensureTabBody(ProofTrackRecord),
    };
  }

  async function fetchOperatorBundle(recordId) {
    var idLower = String(recordId || "").toLowerCase();
    var listPromise = fetch("/api/third-party-operators?activeOnly=1")
      .then(function (listRes) {
        return listRes.ok ? listRes.json().catch(function () { return {}; }) : {};
      })
      .catch(function () {
        return {};
      });
    var detailPromise = fetch(
      "/api/intake/third-party-operators/" + encodeURIComponent(recordId)
    );

    var listData = await listPromise;
    var rows = Array.isArray(listData.operators) ? listData.operators : [];
    var listRow =
      rows.find(function (r) {
        return String((r && r.id) || "").toLowerCase() === idLower;
      }) || null;

    var detailRes = await detailPromise;
    if (!detailRes.ok) {
      var err = await detailRes.json().catch(function () { return ({}); });
      throw new Error((err && err.error) || "Failed to load operator detail");
    }
    var detailData = await detailRes.json().catch(function () { return {}; });
    if (!detailData || !detailData.success || !detailData.operator) {
      throw new Error("Invalid detail response");
    }
    var operator = detailData.operator;
    // Always reconcile from census endpoint when available so share/staging hosts match
    // local Explorer even if the detail API build predates server-side census merge.
    try {
      var cfRes = await fetch(
        "/api/intake/third-party-operators/" +
          encodeURIComponent(recordId) +
          "/census-footprint"
      );
      if (cfRes.ok) {
        var cfData = await cfRes.json().catch(function () { return {}; });
        if (cfData && cfData.censusFootprint && cfData.censusFootprint.ok) {
          operator.censusFootprint = cfData.censusFootprint;
        }
      }
    } catch (cfErr) {
      console.warn("[gold-mock] census-footprint reconcile failed", cfErr);
    }
    return { detail: operator, listRow: listRow };
  }

  function heroMetaCardHtml(label, value) {
    var isAssetFocus = label === "Asset Focus";
    var valueClass = "value" + (isAssetFocus ? " meta-card__value--clamp-2" : "");
    var titleAttr =
      isAssetFocus && nz(value) ? ' title="' + escapeAttr(value) + '" aria-label="' + escapeAttr(value) + '"' : "";
    return (
      '<div class="meta-card' +
      (isAssetFocus ? " meta-card--asset-focus" : "") +
      '"><div class="label">' +
      escapeHtml(label) +
      '</div><div class="' +
      valueClass +
      '"' +
      titleAttr +
      ">" +
      escapeHtml(value) +
      "</div></div>"
    );
  }

  function tabLabelPlain(html) {
    return String(html || "")
      .replace(/<br\s*\/?>/gi, " ")
      .replace(/<[^>]+>/g, "")
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/\s+/g, " ")
      .trim();
  }

  function isExportPdfMode() {
    return !!(
      global.document &&
      global.document.documentElement &&
      global.document.documentElement.classList.contains("oe-export-pdf")
    );
  }

  function exportPdfImgAttrs() {
    return isExportPdfMode()
      ? ' loading="eager" decoding="sync" referrerpolicy="no-referrer"'
      : "";
  }

  /** PDF export shows every tab at once — prime lazy/off-screen images before print. */
  function primeExportImages(scopeEl) {
    if (!isExportPdfMode() || !global.document) return;
    var root = scopeEl || global.document.body;
    if (!root || !root.querySelectorAll) return;
    var imgs = root.querySelectorAll("img[src]");
    for (var i = 0; i < imgs.length; i++) {
      var img = imgs[i];
      try {
        img.loading = "eager";
        img.removeAttribute("loading");
        img.decoding = "sync";
      } catch (_) {}
      try {
        img.scrollIntoView({ block: "nearest", inline: "nearest" });
      } catch (_) {}
      if (!img.complete || img.naturalWidth === 0) {
        try {
          var src = img.getAttribute("src");
          if (src) {
            img.src = src;
            if (typeof img.load === "function") img.load();
          }
        } catch (_) {}
      }
    }
  }

  function waitForExportImages(scopeEl, maxMs) {
    if (!isExportPdfMode()) return Promise.resolve();
    var root = scopeEl || global.document.body;
    if (!root || !root.querySelectorAll) return Promise.resolve();
    primeExportImages(root);
    var imgs = root.querySelectorAll("img[src]");
    var pending = [];
    for (var i = 0; i < imgs.length; i++) {
      var img = imgs[i];
      if (img.complete && img.naturalWidth > 0) {
        if (typeof img.decode === "function") {
          pending.push(
            img.decode().catch(function () {
              return undefined;
            })
          );
        }
        continue;
      }
      pending.push(
        new Promise(function (resolve) {
          function finish() {
            resolve();
          }
          img.addEventListener("load", finish, { once: true });
          img.addEventListener("error", finish, { once: true });
          if (typeof img.decode === "function") {
            img.decode().then(finish).catch(finish);
          }
        })
      );
    }
    var allDone = pending.length ? Promise.all(pending) : Promise.resolve();
    var cap = typeof maxMs === "number" && maxMs > 0 ? maxMs : 28000;
    var timeout = new Promise(function (r) {
      global.setTimeout(r, cap);
    });
    return Promise.race([allDone, timeout]);
  }

  function waitForExportImagesWithRetry(scopeEl) {
    return waitForExportImages(scopeEl, 28000).then(function () {
      primeExportImages(scopeEl);
      return waitForExportImages(scopeEl, 12000);
    }).then(function () {
      return new Promise(function (resolve) {
        global.setTimeout(resolve, 350);
      });
    });
  }

  function prepareAllPanelsForExport() {
    if (!isExportPdfMode() || !global.document) return;
    var tabsEl = global.document.getElementById("tabs");
    if (tabsEl) tabsEl.setAttribute("hidden", "hidden");

    var labelByPanel = {};
    TABS.forEach(function (t) {
      labelByPanel[t] = tabLabelPlain(TAB_LABEL_HTML[t] || t);
    });
    global.document.querySelectorAll(".section-nav-item").forEach(function (btn) {
      var tab = btn.getAttribute("data-tab");
      if (!tab || labelByPanel[tab]) return;
      var labelEl = btn.querySelector(".section-nav-label");
      labelByPanel[tab] = tabLabelPlain(labelEl ? labelEl.innerHTML : tab);
    });

    global.document.querySelectorAll(".tab-panel").forEach(function (panel) {
      panel.classList.add("active");
      panel.style.display = "block";
      panel.style.visibility = "visible";
      var panelName = panel.getAttribute("data-panel") || "";
      var titleText = labelByPanel[panelName] || panelName;
      if (!panel.querySelector(".oe-export-section-title")) {
        var heading = global.document.createElement("h2");
        heading.className = "oe-export-section-title";
        heading.textContent = titleText;
        panel.insertBefore(heading, panel.firstChild);
      }
    });

    var alignEl = global.document.getElementById("alignmentContext");
    if (alignEl && String(alignEl.innerHTML || "").trim()) {
      alignEl.hidden = false;
      if (!alignEl.querySelector(".oe-export-section-title")) {
        var alignHeading = global.document.createElement("h2");
        alignHeading.className = "oe-export-section-title";
        alignHeading.textContent = "Alignment Context";
        alignEl.insertBefore(alignHeading, alignEl.firstChild);
      }
    }
    primeExportImages(global.document.body);
  }

  var exportReadyPromise = null;

  function notifyExportReady() {
    if (!isExportPdfMode()) return exportReadyPromise || Promise.resolve();
    if (exportReadyPromise) return exportReadyPromise;
    exportReadyPromise = waitForExportImagesWithRetry(global.document.body).then(function () {
      try {
        global.dispatchEvent(new CustomEvent("operator-explorer-export-ready"));
      } catch (_) {}
      try {
        if (global.parent && global.parent !== global) {
          global.parent.postMessage({ type: "operator-explorer-export-ready" }, "*");
        }
      } catch (_) {}
      var p = new URLSearchParams(global.location.search || "");
      if (p.get("print") === "1" || p.get("print") === "true") {
        global.setTimeout(function () {
          try {
            global.print();
          } catch (_) {}
        }, 400);
      }
    });
    return exportReadyPromise;
  }

  function finalizeExportIfNeeded() {
    if (!isExportPdfMode()) return;
    prepareAllPanelsForExport();
    global.requestAnimationFrame(function () {
      global.requestAnimationFrame(function () {
        notifyExportReady();
      });
    });
  }

  function mount(vm, panels) {
    document.documentElement.classList.remove("gold-profile--loading");
    var nameEl = document.getElementById("heroName");
    var logoEl = document.getElementById("heroLogo");
    var tagEl = document.querySelector(".hero .tag");
    var stmtEl = document.querySelector(".hero .statement");
    var metaEl = document.getElementById("heroMeta");
    var tabsEl = document.getElementById("tabs");
    var panelsRoot = document.getElementById("panels");

    if (nameEl) nameEl.textContent = vm.companyName;
    if (logoEl) {
      if (vm.logoUrl) {
        logoEl.src = vm.logoUrl;
        logoEl.removeAttribute("hidden");
        logoEl.style.display = "block";
        if (isExportPdfMode()) {
          try {
            logoEl.loading = "eager";
            logoEl.decoding = "sync";
            logoEl.removeAttribute("loading");
          } catch (_) {}
        }
        if (vm.companyName) logoEl.alt = vm.companyName + " logo";
      } else {
        logoEl.style.display = "none";
        logoEl.setAttribute("hidden", "");
      }
    }
    if (tagEl) {
      if (nz(vm.tagline)) {
        tagEl.textContent = vm.tagline;
        tagEl.style.display = "";
      } else {
        tagEl.style.display = "none";
      }
    }
    if (stmtEl) {
      if (nz(vm.statement)) {
        stmtEl.textContent = vm.statement;
        stmtEl.style.display = "";
      } else {
        stmtEl.style.display = "none";
      }
    }
    if (metaEl) {
      var hm = vm.heroMeta || [];
      if (hm.length) {
        metaEl.className =
          "hero-meta oe-hero-meta-single-row oe-hero-meta-single-row--count-" + hm.length;
        metaEl.style.display = "grid";
        metaEl.style.gridTemplateColumns = snapshotKpiGridInlineStyle(hm.length);
        metaEl.innerHTML = hm
          .map(function (pair) {
            return heroMetaCardHtml(pair[0], pair[1]);
          })
          .join("");
      } else {
        metaEl.className = "hero-meta";
        metaEl.style.gridTemplateColumns = "";
        metaEl.innerHTML = "";
        metaEl.style.display = "none";
      }
    }
    if (tabsEl) {
      tabsEl.innerHTML = TABS.map(function (t, i) {
        return (
          '<button type="button" class="section-nav-item ' +
          (i === 0 ? "active" : "") +
          '" data-tab="' +
          escapeHtml(t) +
          '"><div class="section-nav-icon">' +
          TAB_ICONS[t] +
          '</div><div class="section-nav-label">' +
          (TAB_LABEL_HTML[t] || escapeHtml(t)) +
          "</div></button>"
        );
      }).join("");
    }
    if (panelsRoot) {
      panelsRoot.innerHTML = TABS.map(function (tab, i) {
        return (
          '<section class="tab-panel ' +
          (i === 0 ? "active" : "") +
          '" data-panel="' +
          escapeHtml(tab) +
          '">' +
          (panels[tab] || "") +
          "</section>"
        );
      }).join("");
    }
    if (tabsEl && panelsRoot) {
      tabsEl.addEventListener("click", function (e) {
        var btn = e.target.closest(".section-nav-item");
        if (!btn) return;
        var tab = btn.getAttribute("data-tab");
        document.querySelectorAll(".section-nav-item").forEach(function (b) {
          b.classList.toggle("active", b.getAttribute("data-tab") === tab);
        });
        document.querySelectorAll(".tab-panel").forEach(function (p) {
          p.classList.toggle("active", p.getAttribute("data-panel") === tab);
        });
      });
    }
    if (global.OperatorExplorerNewBaseProfile) {
      global.OperatorExplorerNewBaseProfile.mountProfileChrome(vm);
    }
    var DnaMount = global.OperatorDnaProfileMount;
    if (DnaMount && DnaMount.mountDnaExtensionTabs) {
      DnaMount.mountDnaExtensionTabs(vm);
    }
    var CaseStudiesBe = global.OperatorDnaCaseStudiesBe;
    if (CaseStudiesBe) {
      if (typeof CaseStudiesBe.attachCaseStudyPayloadsToDom === "function") {
        CaseStudiesBe.attachCaseStudyPayloadsToDom();
      }
      if (typeof CaseStudiesBe.wireCaseStudyModal === "function") {
        CaseStudiesBe.wireCaseStudyModal();
      }
    }
  }

  async function bootstrap(options) {
    options = options || {};
    var params = new URLSearchParams(global.location.search || "");
    var id =
      params.get("id") ||
      params.get("operatorId") ||
      params.get("recordId") ||
      options.recordId ||
      "";

    var accentParam = (params.get("accent") || "").replace(/^#/, "");

    if (id) {
      try {
        var bundle = await fetchOperatorBundle(id);
        var scalesStr =
          (bundle.listRow && bundle.listRow.chainScale) ||
          (bundle.listRow && bundle.listRow.chainScalesSupported) ||
          "";
        var scales = String(scalesStr)
          .split(",")
          .map(function (s) {
            return s.trim();
          })
          .filter(Boolean);
        if (!scales.length && bundle.detail && Array.isArray(bundle.detail.chainScalesSupported)) {
          scales = bundle.detail.chainScalesSupported
            .map(function (s) {
              return String(s || "").trim();
            })
            .filter(Boolean);
        }
        if (scales.length) {
          applyHeroStripeFromChainScales(scales);
        } else if (/^[0-9a-fA-F]{6}$/.test(accentParam)) {
          applyHeroStripeFromHex(accentParam);
        } else {
          document.documentElement.style.removeProperty("--hero-stripe-bg");
        }
        var vm = buildViewModel(bundle.detail, bundle.listRow);
        var panels = buildPanels(vm, {
          useBrandExplorerCaseStudies: true,
          ownerFacingProofKpis: true,
          omitProofDecisionSignals: true,
        });
        mount(vm, panels);
        var dealId = params.get("dealId") || "";
        if (dealId && global.OperatorExplorerNewBaseProfile) {
          var profileId =
            (bundle.detail &&
              (bundle.detail.operatorId ||
                (bundle.detail.prefill && bundle.detail.prefill.operatorId))) ||
            "";
          await global.OperatorExplorerNewBaseProfile.mountAlignmentContext(
            dealId,
            id,
            profileId || id
          );
        }
        finalizeExportIfNeeded();
        return { mode: "live", recordId: id, vm: vm, dealId: dealId };
      } catch (e) {
        console.warn("[gold-mock] Live load failed, falling back to demo", e);
        if (typeof options.onDemoFallback === "function") options.onDemoFallback(e);
        return { mode: "error", error: e };
      }
    }

    if (typeof options.onDemoFallback === "function") options.onDemoFallback(null);
    return { mode: "demo" };
  }

  global.OperatorExplorerGoldMock = {
    TABS: TABS,
    TAB_LABEL_HTML: TAB_LABEL_HTML,
    mergeExplorerPrefill: mergeExplorerPrefill,
    buildViewModel: buildViewModel,
    buildPanels: buildPanels,
    bootstrap: bootstrap,
    mount: mount,
    isExportPdfMode: isExportPdfMode,
    exportPdfImgAttrs: exportPdfImgAttrs,
    primeExportImages: primeExportImages,
    prepareAllPanelsForExport: prepareAllPanelsForExport,
    finalizeExportIfNeeded: finalizeExportIfNeeded,
    applyHeroStripeFromHex: applyHeroStripeFromHex,
    applyHeroStripeFromChainScales: applyHeroStripeFromChainScales,
    chainScaleStripeBackgroundFromScales: chainScaleStripeBackgroundFromScales,
    chainScaleLabelToColor: chainScaleLabelToColor,
    buildProofDecisionSignalsSection: buildProofDecisionSignalsSection,
    buildValueKpiSnapshotSection: buildValueKpiSnapshotSection,
    buildLeadershipSnapshotSection: buildLeadershipSnapshotSection,
    deriveLeadershipSnapshotMetrics: deriveLeadershipSnapshotMetrics,
    caseStudyHasContent: caseStudyHasContent,
    CASE_STUDY_INITIAL_VISIBLE: CASE_STUDY_INITIAL_VISIBLE,
    caseStudiesExpanderButtonHtml: caseStudiesExpanderButtonHtml,
    markCaseStudyCardCollapsed: markCaseStudyCardCollapsed,
    deriveCaseStudyNarrative: deriveCaseStudyNarrative,
    deriveCaseStudyFivePart: deriveCaseStudyFivePart,
    buildBrandExplorerCaseStudiesGridHtml: buildBrandExplorerCaseStudiesGridHtml,
    buildCaseStudyBePayload: buildCaseStudyBePayload,
    buildOperatorQuickFacts: buildOperatorQuickFacts,
    operatorQuickFactsSectionHtml: operatorQuickFactsSectionHtml,
    OPERATOR_QUICK_FACT_LABELS: OPERATOR_QUICK_FACT_LABELS,
  };
})(typeof window !== "undefined" ? window : this);
