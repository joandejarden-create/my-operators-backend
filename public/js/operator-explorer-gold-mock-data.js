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
    "Risk, Compliance & ESG",
    "Leadership",
    "Best Fit & Deal Profile",
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
    "Risk, Compliance & ESG":
      '<svg viewBox="0 0 24 24"><path d="M12 2l9 16H3z"></path><path d="M12 9v5"></path><circle cx="12" cy="17" r="1"></circle></svg>',
    Leadership:
      '<svg viewBox="0 0 24 24"><circle cx="12" cy="8" r="4"></circle><path d="M4 21c1.5-4 5-6 8-6s6.5 2 8 6"></path></svg>',
    "Best Fit & Deal Profile":
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
    "Risk, Compliance & ESG": "Risk, Compliance<br>&amp; ESG",
    Leadership: "Leadership<br>&amp; Team",
    "Best Fit & Deal Profile": "Best Fit &<br>Deal Profile",
    "Proof & Track Record": "Proof &<br>Track Record",
  };

  var PLACEHOLDER_PROOF =
    "https://images.unsplash.com/photo-1566073771259-6a8506099945?auto=format&fit=crop&w=1200&q=80";
  var PLACEHOLDER_LEADER =
    "https://images.unsplash.com/photo-1560250097-0b93528c311a?auto=format&fit=crop&w=900&q=80";

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
    var brandNote =
      brandMore > 0
        ? '<p class="gold-footprint-table-note">+' +
          brandMore +
          " more brands in Operator Setup.</p>"
        : "";
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

  function caseStudyHasContent(cs) {
    if (!cs) return false;
    if (nz(cs.property_name)) return true;
    if (nz(cs.outcome) || nz(cs.owner_relevance)) return true;
    if (nz(cs.hotel_type) && (nz(cs.region) || nz(cs.situation) || nz(cs.services))) return true;
    return false;
  }

  function proofFromCaseStudy(cs) {
    if (!caseStudyHasContent(cs)) return null;
    var img = nz(cs.image_url);
    var title = nz(cs.property_name) || nz(cs.hotel_type) || "Case study";
    var meta = [nz(cs.region), nz(cs.hotel_type)].filter(Boolean).join(" · ");
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
    return '<div class="kpi-grid-4">' + cells.join("") + "</div>";
  }

  function decisionStripFiltered(items) {
    var pairs = (items || []).filter(function (pair) {
      return meaningfulSignal(pair[1]);
    });
    if (!pairs.length) return "";
    return (
      '<section class="section"><h2 class="section-title">Decision Signals</h2><div class="kpi-grid-4">' +
      pairs
        .map(function (pair) {
          return kpi(pair[0], pair[1]);
        })
        .join("") +
      "</div></section>"
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

  function buildViewModel(detailPayload, listRow) {
    var prefill = (detailPayload && detailPayload.prefill) || {};
    var fields = (detailPayload && detailPayload.fields) || {};
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
      nz(fields["Branded vs Independent Mix"]) ||
      nz(prefill.brandedVsIndependentMix) ||
      "";

    var heroMeta = [];
    if (meaningfulMetaValue(hq)) heroMeta.push(["Headquarters", hq]);
    if (meaningfulMetaValue(yearsBiz)) heroMeta.push(["Years in Business", yearsBiz]);
    var hotelsStr = formatInt(totalHotels);
    if (meaningfulMetaValue(hotelsStr)) heroMeta.push(["Hotels Managed", hotelsStr]);
    var roomsStr = formatInt(totalRooms);
    if (meaningfulMetaValue(roomsStr)) heroMeta.push(["Rooms Managed", roomsStr]);
    if (meaningfulMetaValue(brandCount)) heroMeta.push(["Brands Supported", brandCount]);
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
      '"><div class="proof-body"><div class="proof-title">' +
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

  function proofGridFromCases(cases) {
    return (cases || [])
      .slice(0, 3)
      .map(proofFromCaseStudy)
      .filter(Boolean)
      .map(function (p) {
        return proofCard(p.img, p.title, p.meta, p.lines.length ? p.lines : []);
      })
      .join("");
  }

  /** Card body uses `summary` (exec_*_summary); image hover uses `bioHover` (exec_*_bio). */
  function leaderCard(img, name, titleLine, summary, roleLine, bioHover) {
    var summaryText = nz(summary);
    var bioText = nz(bioHover);
    var src = img || PLACEHOLDER_LEADER;
    var overlayHtml = bioText
      ? '<div class="leader-bio-overlay"><strong>Executive Bio:</strong> ' +
        escapeHtml(bioText) +
        "</div>"
      : "";
    return (
      '<article class="leader-card"><div class="leader-image-wrap"><img src="' +
      escapeHtml(src) +
      '" alt="' +
      escapeHtml(name) +
      '">' +
      overlayHtml +
      '</div><div class="leader-body"><div class="leader-name">' +
      escapeHtml(name) +
      '</div><div class="leader-meta">' +
      escapeHtml(titleLine) +
      " · " +
      escapeHtml(roleLine) +
      '</div><div class="leader-summary">' +
      escapeHtml(summaryText) +
      "</div></div></article>"
    );
  }

  function marketsDerivedMetrics(vm) {
    var p = vm.prefill || {};
    var regions = arrayish(p.regions || p.regionsSupported || p.priorityMarkets);
    var cities = linesFromText(p.specificMarkets || "");
    var countries = arrayish(p.priorityCountries || p.countriesServed);
    var r = regions.length ? String(regions.length) : "";
    var c = countries.length ? String(countries.length) : "";
    var ct = cities.length ? String(cities.length) : "";
    return {
      regions: r,
      countries: c,
      cities: ct,
      coverage: nz(p.regionalManagementTeams) || nz(p.primaryServiceModel) || "",
    };
  }

  function buildPanels(vm) {
    var ex = vm.ex;
    var p = vm.prefill;
    var proof = (vm.caseStudies || []).filter(caseStudyHasContent);
    var leadersAllRaw = vm.leadership || [];
    var leadersNamed = leadersAllRaw.filter(function (L) {
      return nz(L.name);
    });
    var m = marketsDerivedMetrics(vm);

    var leadersOverview = leadersNamed.slice(0, 3);
    var leadersAll = leadersNamed.slice(0, 6);

    var proofGridHtml = proofGridFromCases(proof);

    function heroSummarySection() {
      var pairs = [];
      var hm = vm.heroMeta || [];
      for (var i = 0; i < hm.length; i++) {
        if (hm[i] && meaningfulMetaValue(hm[i][1])) pairs.push([hm[i][0], hm[i][1]]);
      }
      if (pairs.length >= 4) {
        return (
          '<section class="section"><h2 class="section-title">Operator Summary</h2>' +
          kpiGridFromPairs(pairs.slice(0, 4)) +
          "</section>"
        );
      }
      if (pairs.length) {
        return (
          '<section class="section"><h2 class="section-title">Operator Summary</h2>' +
          kpiGridFromPairs(pairs) +
          "</section>"
        );
      }
      return "";
    }

    var overviewDecision = [
      ["Repeat-owner / relationship signal", pick(ex, p, "overview_signal_1_value", "")],
      ["Average contract duration", pick(ex, p, "overview_signal_2_value", "")],
      ["Typical stabilization window", pick(ex, p, "overview_signal_3_value", "")],
    ];

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
          return leaderCard(
            L.headshotUrl,
            nz(L.name),
            nz(L.title) || "—",
            nz(L.summary) || "",
            nz(L.function) || "—",
            nz(L.bio) || ""
          );
        })
        .join("") || "";

    var profileDepthCards =
      titledCard("Company History", p.companyHistory) +
      titledCard("Key Differentiators", p.differentiators) +
      titledCard("Management Philosophy", p.managementPhilosophy) +
      (function () {
        var ms = nz(p.missionStatement);
        if (!ms || ms === nz(vm.statement)) return "";
        return titledCard("Mission Statement", ms);
      })();

    var profileClustersBasics =
      clusterContent("Property Types", arrayish(p.propertyTypes)) +
      clusterContent("Chain Scales Supported", arrayish(p.chainScalesSupported || p.chainScale)) +
      clusterContentWide("Brands Supported", arrayish(p.brands));

    var profileRecognition =
      titledCard("Certifications & Standards", p.certifications) +
      titledCard("Industry Recognition", p.industryRecognition) +
      titledCard("Notable Achievements", p.achievements);

    var ProfilePositioning =
      heroSummarySection() +
      (nz(vm.statement)
        ? '<section class="section"><h2 class="section-title">Company Background</h2>' +
          card("Positioning", vm.statement) +
          "</section>"
        : "") +
      (profileDepthCards
        ? '<section class="section"><h2 class="section-title">Profile Depth</h2><div class="grid-2">' +
          profileDepthCards +
          "</div></section>"
        : "") +
      (profileClustersBasics
        ? '<section class="section"><h2 class="section-title">Property & Portfolio Profile</h2><div class="quant-grid">' +
          profileClustersBasics +
          "</div></section>"
        : "") +
      (profileRecognition
        ? '<section class="section"><h2 class="section-title">Recognition</h2><div class="grid-2">' +
          profileRecognition +
          "</div></section>"
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
      (leadershipSnap
        ? '<section class="section"><h2 class="section-title">Leadership Snapshot</h2><div class="proof-grid">' +
          leadershipSnap +
          "</div></section>"
        : "") +
      decisionStripFiltered(overviewDecision);

    var capKpis = kpiGridFromPairs([
      ["Operating Model", pick(ex, p, "cap_kpi_operating_model", "")],
      ["Execution Strength", pick(ex, p, "cap_kpi_execution_strength", "")],
      ["Transition Capability", pick(ex, p, "cap_kpi_transition", "")],
      ["Reporting Strength", pick(ex, p, "cap_kpi_reporting", "")],
    ]);

    var capProfileRow =
      clusterContent(
        "Operational Execution",
        linesFromText(pick(ex, p, "cap_profile_operational", "")).slice(0, 8)
      ) +
      clusterContent(
        "Commercial Engine",
        linesFromText(pick(ex, p, "cap_profile_commercial", "")).slice(0, 8)
      ) +
      clusterContent(
        "Transition Capability",
        linesFromText(pick(ex, p, "cap_profile_transition", "")).slice(0, 8)
      );

    var capAssetCards =
      titledCard("Asset Positioning", pick(ex, p, "cap_card_asset_positioning", "")) +
      titledCard("Service Differentiation", pick(ex, p, "cap_card_service_diff", "")) +
      titledCard("Execution Reliability", pick(ex, p, "cap_card_execution_rel", "")) +
      titledCard("Governance & Reporting", pick(ex, p, "cap_card_governance", ""));

    var capDeep =
      clusterContent(
        "Revenue Systems",
        linesFromText(pick(ex, p, "cap_deep_revenue_systems", "")).slice(0, 8)
      ) +
      clusterContent(
        "Execution Infrastructure",
        linesFromText(pick(ex, p, "cap_deep_execution_infra", "")).slice(0, 8)
      );

    var OperatingPlatform =
      (capKpis
        ? '<section class="section"><h2 class="section-title">Operating Platform</h2>' + capKpis + "</section>"
        : "") +
      (capProfileRow
        ? '<section class="section"><h2 class="section-title">Hotel Operating Profile</h2><div class="cap-profile-3-2"><div class="row-3">' +
          capProfileRow +
          "</div></div></section>"
        : "") +
      (capAssetCards
        ? '<section class="section"><h2 class="section-title">Asset Support Capabilities</h2><div class="grid-2">' +
          capAssetCards +
          "</div></section>"
        : "") +
      (capDeep
        ? '<section class="section"><h2 class="section-title">Capability Deep Dive</h2><div class="grid-2">' +
          capDeep +
          "</div></section>"
        : "") +
      (proofGridHtml
        ? '<section class="section"><h2 class="section-title">Sample Properties</h2><div class="proof-grid">' +
          proofGridHtml +
          "</div></section>"
        : "") +
      decisionStripFiltered([
        ["Budget accuracy", pick(ex, p, "cap_signal_budget", "")],
        ["Time to first performance lift", pick(ex, p, "cap_signal_lift", "")],
        ["Transitions on schedule", pick(ex, p, "cap_signal_trans", "")],
      ]);

    var brandKpi1 = nz(p.numberOfBrands) || (arrayish(p.brands).length ? String(arrayish(p.brands).length) : "");
    var brandKpi2 = nz(vm.brandMixDisplay) || "";

    var brandKpiHtml = kpiGridFromPairs([
      ["Brands Supported", brandKpi1],
      ["Brand Mix", brandKpi2],
      ["Franchise alignment", pick(ex, p, "brand_signal_franchise_align", "")],
      ["Soft-brand retention", pick(ex, p, "brand_signal_soft_retention", "")],
    ]);

    var brandModules =
      clusterContent(
        "Compliance + Commercial Balance",
        linesFromText(pick(ex, p, "brand_narrative_compliance", ""))
      ) +
      clusterContent(
        "Brand Relationship Model",
        linesFromText(pick(ex, p, "brand_narrative_relationship", ""))
      );

    var brandUnitsShareTile = brandPortfolioUnitsShareTileHtml(p);

    var BrandRelationships =
      (brandKpiHtml
        ? '<section class="section"><h2 class="section-title">Brand & Relationships</h2>' + brandKpiHtml + "</section>"
        : "") +
      (brandUnitsShareTile
        ? '<section class="section"><h2 class="section-title">Portfolio by Brand</h2><div class="quant-grid">' +
          brandUnitsShareTile +
          "</div></section>"
        : "") +
      (brandModules
        ? '<section class="section"><h2 class="section-title">Brand Operating Modules</h2><div class="grid-2">' +
          brandModules +
          "</div></section>"
        : "") +
      decisionStripFiltered([
        ["Brand audit pass rate", pick(ex, p, "brand_signal_audit", "")],
        ["Reflag readiness lead time", pick(ex, p, "brand_signal_reflag", "")],
        ["Franchise alignment", pick(ex, p, "brand_signal_franchise_align", "")],
        ["Soft-brand retention", pick(ex, p, "brand_signal_soft_retention", "")],
      ]);

    var coreMarketsList =
      linesFromText(p.specificMarkets || "").slice(0, 10).length
        ? linesFromText(p.specificMarkets || "").slice(0, 10)
        : arrayish(p.priorityMarkets).slice(0, 10);

    var marketsKpiHtml = kpiGridFromPairs([
      ["Regions (count)", m.regions],
      ["Countries (count)", m.countries],
      ["Cities (markets list)", m.cities],
      ["Coverage / depth", m.coverage],
    ]);

    var footprintInner =
      clusterContent("Core Markets / Cities", coreMarketsList) +
      clusterContent(
        "Market Depth Narrative",
        linesFromText(pick(ex, p, "mkt_narrative_depth", "")).slice(0, 8)
      );

    var footprintMetricsHtml = footprintMetricsSection(p);

    var MarketsFootprint =
      (marketsKpiHtml
        ? '<section class="section"><h2 class="section-title">Markets & Footprint</h2>' + marketsKpiHtml + "</section>"
        : "") +
      (footprintMetricsHtml || "") +
      (footprintInner
        ? '<section class="section"><h2 class="section-title">Market Footprint</h2><div class="grid-2">' +
          footprintInner +
          "</div></section>"
        : "") +
      decisionStripFiltered([
        ["Avg. years in core markets", pick(ex, p, "mkt_signal_years", "")],
        ["Gateway concentration", pick(ex, p, "mkt_signal_gateway", "")],
        ["Urban / resort mix", pick(ex, p, "mkt_signal_mix", "")],
      ]);

    var portfolioScaleLine = (function () {
      var h = formatInt(pick(ex, p, "totalProperties", p.totalProperties));
      var r = formatInt(pick(ex, p, "totalRooms", p.totalRooms));
      if (!meaningfulMetaValue(h) && !meaningfulMetaValue(r)) return "";
      if (meaningfulMetaValue(h) && meaningfulMetaValue(r)) return h + " hotels / " + r + " rooms";
      return meaningfulMetaValue(h) ? h + " hotels" : r + " rooms";
    })();

    var ovKpiHtml = kpiGridFromPairs([
      ["Communication style", nz(p.communicationStyle)],
      ["Owner involvement", nz(p.ownerInvolvement)],
      ["Operating collaboration", nz(p.operatingCollaborationMode)],
      ["Reporting frequency", nz(p.reportingFrequency)],
      ["Primary service model", nz(p.primaryServiceModel)],
      ["Owner reporting cadence", nz(p.ownerReportingCadence)],
      ["Portfolio scale", portfolioScaleLine],
      ["Markets operated", nz(p.numberOfMarkets)],
    ]);

    var ovCadence =
      titledCard("Report Types", formatMulti(p.reportTypes)) +
      titledCard("Budget Process", p.budgetProcess) +
      titledCard("CapEx planning", p.capexPlanning) +
      titledCard("Performance Reviews", p.performanceReviews) +
      titledCard("Owner Response Time", p.ownerResponseTime) +
      titledCard("Concern Resolution Time", p.concernResolutionTime) +
      titledCard("Decision-making", p.decisionMaking) +
      titledCard("Dispute Resolution", p.disputeResolution) +
      titledCard("Owner Portal", p.ownerPortalFeatures) +
      titledCard("Owner Advisory Board", p.ownerAdvisoryBoard) +
      titledCard("Owner Education", p.ownerEducation);

    var ovNarrative = titledCard("Owner Engagement Narrative", p.ownerEngagementNarrative);

    var ovInsights =
      titledCard("Discipline & Controls", pick(ex, p, "ov_card_discipline", "")) +
      titledCard("Commercial Engine", pick(ex, p, "ov_card_commercial", "")) +
      titledCard("Insight to Action", pick(ex, p, "ov_card_communication", "")) +
      titledCard("Flexibility & Tradeoffs", pick(ex, p, "ov_card_flexibility", "")) +
      titledCard("Continuity & Escalation", pick(ex, p, "ov_card_risk", ""));

    var ovExperience =
      clusterContent("Interaction Rhythm", linesFromText(pick(ex, p, "ov_cluster_interaction", ""))) +
      clusterContent("Deliverables", linesFromText(pick(ex, p, "ov_cluster_deliverables", "")));

    var OwnerEngagement =
      (ovKpiHtml
        ? '<section class="section"><h2 class="section-title">Engagement &amp; Reporting Snapshot</h2>' +
          ovKpiHtml +
          "</section>"
        : "") +
      (ovCadence
        ? '<section class="section"><h2 class="section-title">Cadence, Controls &amp; Tools</h2><div class="grid-2">' +
          ovCadence +
          "</div></section>"
        : "") +
      (ovNarrative
        ? '<section class="section"><h2 class="section-title">Engagement Narrative</h2><div class="grid-2">' +
          ovNarrative +
          "</div></section>"
        : "") +
      (ovInsights
        ? '<section class="section"><h2 class="section-title">Strategic Owner Value</h2><div class="grid-3">' +
          ovInsights +
          "</div></section>"
        : "") +
      (ovExperience
        ? '<section class="section"><h2 class="section-title">Interaction &amp; Deliverables (Explorer)</h2><div class="grid-2">' +
          ovExperience +
          "</div></section>"
        : "") +
      decisionStripFiltered([
        ["Review touchpoints / quarter", pick(ex, p, "ov_q_touchpoints", "")],
        ["Owner response window", nz(p.ownerResponseTime)],
        ["Concern resolution", nz(p.concernResolutionTime)],
      ]);

    var infraKpiHtml = kpiGridFromPairs([
      ["Reporting systems", pick(ex, p, "infra_kpi_reporting", "")],
      ["Revenue systems", pick(ex, p, "infra_kpi_revenue", "")],
      ["Execution platform", pick(ex, p, "infra_kpi_exec", "")],
      ["Owner tools", pick(ex, p, "infra_kpi_tools", "")],
    ]);

    var infraReporting =
      clusterContent(
        "Asset Management & Reporting",
        linesFromText(pick(ex, p, "infra_asset_management_reporting", ""))
      ) +
      clusterContent("Systems & Technology", linesFromText(pick(ex, p, "infra_systems_technology", "")));

    var infraInventory = clusterContent(
      "Additional Systems / Integrations",
      linesFromText(pick(ex, p, "systems_inventory_lines", ""))
    );

    var serviceOfferingsBlock = serviceOfferingsSectionHtml(p);
    var InfrastructureData =
      (serviceOfferingsBlock || "") +
      (infraKpiHtml
        ? '<section class="section"><h2 class="section-title">Infrastructure &amp; Data</h2>' + infraKpiHtml + "</section>"
        : "") +
      (infraReporting
        ? '<section class="section"><h2 class="section-title">Reporting &amp; Technology</h2><div class="grid-2">' +
          infraReporting +
          "</div></section>"
        : "") +
      (infraInventory
        ? '<section class="section"><h2 class="section-title">Systems Inventory</h2>' + infraInventory + "</section>"
        : "") +
      decisionStripFiltered([
        ["Platform uptime", pick(ex, p, "infra_signal_uptime", "")],
        ["Critical incident response", pick(ex, p, "infra_signal_incident", "")],
        ["Portfolio adoption", pick(ex, p, "infra_signal_adoption", "")],
        ["Data refresh cadence", pick(ex, p, "infra_signal_refresh", "")],
      ]);

    var esgLines = []
      .concat(linesFromText(nz(p.sustainabilityPrograms)))
      .concat(linesFromText(nz(p.esgReporting)))
      .concat(linesFromText(nz(p.esgExpectations)))
      .filter(Boolean);
    if (!esgLines.length && nz(p.carbonFootprintTracking)) {
      esgLines.push(nz(p.carbonFootprintTracking));
    }

    var riskPrograms = clusterContent(
      "Programs & Narrative",
      linesFromText(pick(ex, p, "risk_programs_narrative", ""))
    );

    var RiskComplianceEsg =
      (riskPrograms
        ? '<section class="section"><h2 class="section-title">Risk, Compliance &amp; ESG</h2>' + riskPrograms + "</section>"
        : "") +
      (esgLines.length
        ? '<section class="section"><h2 class="section-title">ESG &amp; Sustainability</h2>' +
          cluster("From Operator Setup (Preferences)", esgLines.slice(0, 12)) +
          "</section>"
        : "") +
      decisionStripFiltered([
        ["Audit consistency", pick(ex, p, "risk_signal_audit", "")],
        ["BCP test frequency", pick(ex, p, "risk_signal_bcp", "")],
        ["Control closure rate", pick(ex, p, "risk_signal_control", "")],
        ["Insurance adequacy review", pick(ex, p, "risk_signal_insurance", "")],
      ]);

    var leadKpiHtml = kpiGridFromPairs([
      [
        "Team size (displayed)",
        leadersAll.length ? String(leadersAll.length) : "",
      ],
      ["Avg. tenure signal", pick(ex, p, "lead_signal_tenure", "")],
      ["Cross-brand experience", pick(ex, p, "lead_signal_crossbrand", "")],
      ["Regional leadership density", pick(ex, p, "lead_signal_density", "")],
    ]);

    var leadProfiles =
      leadersAll
        .map(function (L) {
          return leaderCard(
            L.headshotUrl,
            nz(L.name),
            nz(L.title) || "—",
            nz(L.summary) || "",
            nz(L.function) || "—",
            nz(L.bio) || ""
          );
        })
        .join("") || "";

    var benchInner =
      clusterContent("Leadership Model", linesFromText(pick(ex, p, "lead_narrative_model", ""))) +
      clusterContent("Platform Resilience", linesFromText(pick(ex, p, "lead_narrative_resilience", ""))) +
      clusterContent("Industry Recognition", linesFromText(pick(ex, p, "lead_narrative_functional", ""))) +
      clusterContent("Regional Coverage", linesFromText(pick(ex, p, "lead_narrative_regional", "")));

    var Leadership =
      (leadKpiHtml
        ? '<section class="section"><h2 class="section-title">Leadership</h2>' + leadKpiHtml + "</section>"
        : "") +
      (leadProfiles
        ? '<section class="section"><h2 class="section-title">Leadership Profiles</h2><div class="proof-grid">' +
          leadProfiles +
          "</div></section>"
        : "") +
      (benchInner
        ? '<section class="section"><h2 class="section-title">Bench Strength</h2><div class="grid-2">' +
          benchInner +
          "</div></section>"
        : "") +
      decisionStripFiltered([
        ["Avg. leadership tenure", pick(ex, p, "lead_signal_tenure", "")],
        ["Cross-brand experience", pick(ex, p, "lead_signal_crossbrand", "")],
        ["Succession coverage", pick(ex, p, "lead_signal_succession", "")],
        ["Regional density", pick(ex, p, "lead_signal_density", "")],
      ]);

    var bfQuant = function (v) {
      var s = nz(v);
      if (!s || s === "0") return "";
      return s;
    };

    /** Count + bullet list of selected labels (prefill arrays) for Fit Quantifiers */
    function kpiQuant(label, countKey, itemsSource) {
      var c = bfQuant(pick(ex, p, countKey, ""));
      var list = arrayish(itemsSource);
      if (!c && !list.length) return "";
      var numShown = c || (list.length ? String(list.length) : "—");
      var detailHtml = list.length
        ? '<ul class="kpi-item-list">' +
          list
            .map(function (x) {
              return "<li>" + escapeHtml(nz(x)) + "</li>";
            })
            .join("") +
          "</ul>"
        : "";
      return (
        '<div class="kpi kpi--quant">' +
        '<div class="label">' +
        escapeHtml(label) +
        '</div><div class="value">' +
        escapeHtml(numShown) +
        "</div>" +
        detailHtml +
        "</div>"
      );
    }

    var bfCommercialHtml = kpiGridFromPairs([
      ["Fee approach (category)", feeCategoryOnly(p.feeStructure)],
      ["Typical agreement length", nz(p.avgContractTerm)],
      ["Property size range", propertySizeRange(p.minPropertySize, p.maxPropertySize)],
      ["Portfolio scale (directional)", nz(p.portfolioValue)],
      ["Revenue scale (directional)", nz(p.annualRevenueManaged)],
    ]);

    var bfKpiHtmlInner = [
      kpiQuant("Asset Types (Quant)", "bf_q_assets", p.bf_selected_asset_types),
      kpiQuant("Situation Types (Quant)", "bf_q_situations", p.bf_selected_situation_types),
      kpiQuant("Deal Structures (Quant)", "bf_q_deals", p.bf_selected_deal_structures),
      kpiQuant("Leadership Functions (Quant)", "lead_kpi_functions", p.lead_functions_selected),
    ]
      .filter(Boolean)
      .join("");

    var bfKpiHtml = bfKpiHtmlInner ? '<div class="kpi-grid-4">' + bfKpiHtmlInner + "</div>" : "";

    var idealAssets = arrayish(p.bestFitAssetTypes || p.idealBuildingTypes);
    var idealGeos = arrayish(p.bestFitGeographies || p.priorityMarkets);
    var ownerLevels = arrayish(p.ownerInvolvementLevel);

    var fitClusters =
      clusterContent("Operating Situations", linesFromText(pick(ex, p, "bf_operating_situations", ""))) +
      clusterContent("Not Ideal For", linesFromText(pick(ex, p, "bf_not_ideal_for", ""))) +
      clusterContent("Markets to Avoid", arrayish(p.marketsToAvoid)) +
      (idealAssets.length
        ? cluster("Ideal Asset Types", idealAssets)
        : "") +
      (idealGeos.length ? cluster("Ideal Geographies", idealGeos) : "") +
      (ownerLevels.length ? cluster("Acceptable Owner Involvement", ownerLevels) : "");

    var BestFitDealProfile =
      (bfCommercialHtml
        ? '<section class="section"><h2 class="section-title">Commercial Fit Signals</h2>' +
          bfCommercialHtml +
          "</section>"
        : "") +
      (bfKpiHtml
        ? '<section class="section"><h2 class="section-title">Fit Quantifiers</h2>' + bfKpiHtml + "</section>"
        : "") +
      (fitClusters
        ? '<section class="section"><h2 class="section-title">Fit Clusters</h2><div class="grid-2">' +
          fitClusters +
          "</div></section>"
        : "") +
      decisionStripFiltered([
        ["Best-fit deal size", pick(ex, p, "bf_signal_dealsize", "")],
        ["Transition intensity fit", pick(ex, p, "bf_signal_transition", "")],
        ["Owner governance fit", pick(ex, p, "bf_signal_governance", "")],
        ["Capital plan complexity", pick(ex, p, "bf_signal_capital", "")],
      ]);

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
          if (q.length > 240) q = q.slice(0, 237) + "…";
          if (a.length > 320) a = a.slice(0, 317) + "…";
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

    var proofCredibilityKpis = kpiGridFromPairs([
      ["Owner references", ownerRefDisplay()],
      ["Lender references", lenderRefSignal(p.lenderReferences)],
      ["Renewal / retention signal", nz(p.renewalRate) || nz(p.ownerRetention)],
    ]);

    var proofLenderCard = titledCard("Major Lender Relationships", p.majorLenders);

    var trHeader =
      kpiGridFromPairs([
        ["Properties", trHotels],
        ["Markets operated", nz(p.numberOfMarkets)],
        ["Years in business", trYears],
        ["Case Studies on profile", proof.length ? String(proof.length) : ""],
      ]);

    var ProofTrackRecord =
      (trHeader
        ? '<section class="section"><h2 class="section-title">Proof &amp; Track Record</h2>' + trHeader + "</section>"
        : "") +
      (proofCredibilityKpis
        ? '<section class="section"><h2 class="section-title">Credibility &amp; References</h2>' +
          proofCredibilityKpis +
          "</section>"
        : "") +
      (proofLenderCard
        ? '<section class="section"><h2 class="section-title">Lender Context</h2><div class="grid-2">' +
          proofLenderCard +
          "</div></section>"
        : "") +
      (proofGridHtml
        ? '<section class="section"><h2 class="section-title">Case Studies</h2><div class="proof-grid">' +
          proofGridHtml +
          "</div></section>"
        : "") +
      diligenceLightSection(vm.ownerDiligenceQa || []) +
      decisionStripFiltered([
        ["RevPAR lift range", pick(ex, p, "tr_signal_revpar", "")],
        ["Occupancy recovery window", pick(ex, p, "tr_signal_occ", "")],
        ["ADR stabilization", pick(ex, p, "tr_signal_adr", "")],
        ["Case repeatability", pick(ex, p, "tr_signal_repeat", "")],
      ]);

    return {
      "Profile & Positioning": ensureTabBody(ProfilePositioning),
      "Operating Platform": ensureTabBody(OperatingPlatform),
      "Brand & Relationships": ensureTabBody(BrandRelationships),
      "Markets & Footprint": ensureTabBody(MarketsFootprint),
      "Owner Engagement & Reporting": ensureTabBody(OwnerEngagement),
      "Infrastructure & Data": ensureTabBody(InfrastructureData),
      "Risk, Compliance & ESG": ensureTabBody(RiskComplianceEsg),
      Leadership: ensureTabBody(Leadership),
      "Best Fit & Deal Profile": ensureTabBody(BestFitDealProfile),
      "Proof & Track Record": ensureTabBody(ProofTrackRecord),
    };
  }

  async function fetchOperatorBundle(recordId) {
    var listRes = await fetch("/api/third-party-operators?activeOnly=1");
    var listData = listRes.ok ? await listRes.json().catch(function () { return {}; }) : {};
    var rows = Array.isArray(listData.operators) ? listData.operators : [];
    var idLower = String(recordId || "").toLowerCase();
    var listRow =
      rows.find(function (r) {
        return String((r && r.id) || "").toLowerCase() === idLower;
      }) || null;

    var detailRes = await fetch(
      "/api/intake/third-party-operators/" + encodeURIComponent(recordId)
    );
    if (!detailRes.ok) {
      var err = await detailRes.json().catch(function () { return ({}); });
      throw new Error((err && err.error) || "Failed to load operator detail");
    }
    var detailData = await detailRes.json().catch(function () { return {}; });
    if (!detailData || !detailData.success || !detailData.operator) {
      throw new Error("Invalid detail response");
    }
    return { detail: detailData.operator, listRow: listRow };
  }

  function mount(vm, panels) {
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
        logoEl.style.display = "block";
      } else {
        logoEl.style.display = "none";
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
        metaEl.style.display = "grid";
        metaEl.innerHTML = hm
          .map(function (pair) {
            return (
              '<div class="meta-card"><div class="label">' +
              escapeHtml(pair[0]) +
              '</div><div class="value">' +
              escapeHtml(pair[1]) +
              "</div></div>"
            );
          })
          .join("");
      } else {
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
        var scalesStr = (bundle.listRow && bundle.listRow.chainScale) || "";
        var scales = String(scalesStr)
          .split(",")
          .map(function (s) {
            return s.trim();
          })
          .filter(Boolean);
        if (scales.length) {
          applyHeroStripeFromChainScales(scales);
        } else if (/^[0-9a-fA-F]{6}$/.test(accentParam)) {
          applyHeroStripeFromHex(accentParam);
        } else {
          document.documentElement.style.removeProperty("--hero-stripe-bg");
        }
        var vm = buildViewModel(bundle.detail, bundle.listRow);
        var panels = buildPanels(vm);
        mount(vm, panels);
        return { mode: "live", recordId: id, vm: vm };
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
    applyHeroStripeFromHex: applyHeroStripeFromHex,
    applyHeroStripeFromChainScales: applyHeroStripeFromChainScales,
    chainScaleStripeBackgroundFromScales: chainScaleStripeBackgroundFromScales,
  };
})(typeof window !== "undefined" ? window : this);
