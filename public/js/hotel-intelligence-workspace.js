/**
 * Dealality Hotel Intelligence Workspace — Packet 2.6
 * Golden Demo UX + feature completion (KGPV reference product standard).
 * Research-native overlay over Radar. Ownership = long-form report.
 * Does not invent ownership; preserves operator ≠ owner semantics.
 * UI consumes canonical ownership payload — no hotel-specific presentation forks.
 */
(function () {
  "use strict";

  var state = {
    open: false,
    hotel: null,
    ownership: null,
    org: null,
    tab: "ownership",
    radarSnapshot: null,
    evidenceOpen: null,
    relFilter: "all",
    marketAlertsCitations: [],
    guestSentimentCitations: [],
    contactCitations: [],
    sourceFilter: "all",
    sourceFocusN: null,
  };

  var SOURCE_CATEGORY_DEFS = [
    { id: "all", label: "All sources" },
    { id: "property", label: "Property & Facilities" },
    { id: "ownership", label: "Ownership & Organizations" },
    { id: "contacts", label: "Contacts & People" },
    { id: "financing", label: "Financing & Development" },
    { id: "brand", label: "Brand & Operator" },
    { id: "news", label: "News & Market Alerts" },
    { id: "sentiment", label: "Guest Sentiment" },
  ];

  var asyncApi = typeof window !== "undefined" ? window.HeAsyncSection : null;
  var newsAsync =
    asyncApi && asyncApi.createController
      ? asyncApi.createController({ key: "hotel-news", cacheTtlMs: 5 * 60 * 1000, timeoutMs: 20000 })
      : null;
  var sentimentAsync =
    asyncApi && asyncApi.createController
      ? asyncApi.createController({
          key: "guest-sentiment",
          cacheTtlMs: 5 * 60 * 1000,
          timeoutMs: 20000,
        })
      : null;
  var sourcesMaAsync =
    asyncApi && asyncApi.createController
      ? asyncApi.createController({
          key: "sources-market-alerts",
          cacheTtlMs: 5 * 60 * 1000,
          timeoutMs: 20000,
        })
      : null;

  function heWaveCompact(text, sub) {
    if (window.HeLoadingUx && window.HeLoadingUx.toastHtml) {
      return window.HeLoadingUx.toastHtml(text || "Loading…", { subText: sub || "" });
    }
    if (asyncApi && asyncApi.waveCompact) return asyncApi.waveCompact(text, sub);
    if (window.DealalityWaveLoader && window.DealalityWaveLoader.toastHtml) {
      return window.DealalityWaveLoader.toastHtml(text || "Loading…", { subText: sub || "" });
    }
    if (window.DealalityWaveLoader && window.DealalityWaveLoader.compactHtml) {
      return window.DealalityWaveLoader.compactHtml(text || "Loading…", {
        subText: sub || "",
        progress: false,
      });
    }
    return (
      '<div class="he-async-fallback" role="status" aria-live="polite">' +
      esc(text || "Loading…") +
      "</div>"
    );
  }

  function heAsyncError(message, retryKey) {
    if (asyncApi && asyncApi.errorHtml) return asyncApi.errorHtml(message, retryKey);
    return (
      '<div class="he-async-error" role="status"><p class="hiw-prose hiw-prose--quiet">' +
      esc(message || "This information is temporarily unavailable.") +
      "</p></div>"
    );
  }

  var ALL_TABS = [
    { id: "overview", label: "Overview" },
    { id: "ownership", label: "Ownership Intelligence" },
    { id: "organization", label: "Organization & Portfolio" },
    { id: "relationships", label: "Relationships" },
    { id: "market", label: "Market Intelligence" },
    { id: "demand", label: "Demand & Access" },
    { id: "sentiment", label: "Guest Sentiment" },
    { id: "sources", label: "Sources & Evidence" },
  ];

  function guestSentimentFlag() {
    return typeof window !== "undefined" ? window.HeGuestSentimentFeatureFlag : null;
  }

  function isGuestSentimentEnabled() {
    var f = guestSentimentFlag();
    return !!(f && typeof f.isEnabled === "function" && f.isEnabled());
  }

  var TABS = (function () {
    var f = guestSentimentFlag();
    if (f && typeof f.filterActiveTabs === "function") return f.filterActiveTabs(ALL_TABS);
    return ALL_TABS.filter(function (t) {
      return t.id !== "sentiment";
    });
  })();

  var TAB_ALIASES = {
    brand: "sentiment",
    "brand-operator": "sentiment",
    "guest-sentiment": "sentiment",
  };

  var PERSON_GROUP_ORDER = [
    { key: "principals", label: "Principals / Ownership Leadership", match: /beneficial|principal|ubo|shareholder|owner/i },
    { key: "corporate", label: "Corporate Leadership", match: /operator executive|ceo|director general|commercial leadership|corporate/i },
    { key: "development", label: "Development / Growth", match: /development|growth|franchise/i },
    { key: "operations", label: "Operations", match: /asset management|operations|operator(?! executive)/i },
    { key: "property", label: "Property Leadership", match: /property leadership|general manager|gm|director.*property/i },
    { key: "legal", label: "Legal / Governance", match: /legal|governance|board|signator/i },
    { key: "other", label: "Other Relevant People", match: /.*/ },
  ];

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  /** Whole numbers with thousands separators (2,696); non-numeric passthrough. */
  function formatCount(n) {
    if (n == null || n === "" || n === "—") return "—";
    var num = typeof n === "number" ? n : Number(String(n).replace(/,/g, ""));
    if (!Number.isFinite(num)) return String(n);
    return Math.round(num).toLocaleString("en-US");
  }

  function hexFact(value, label) {
    return (
      '<div class="hex-fact"><div class="label">' +
      esc(label) +
      '</div><div class="value">' +
      esc(value == null || value === "" ? "—" : value) +
      "</div></div>"
    );
  }

  function recordId(hotel) {
    return (hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || null;
  }

  function isRichCase(ownership) {
    return !!(
      ownership &&
      (ownership.intelligence_case === "A" ||
        (ownership.deep_research && ownership.deep_research.ownership_chain))
    );
  }

  /**
   * Packet 2.7-R4 — rich Case A must NEVER inject another hotel's prose.
   * All rich sections are built from the current ownership report / ctrl payload.
   */
  var FOREIGN_HOTEL_TOKEN_RE =
    /\b(Krystal Grand|Puerto Vallarta|Breathless|IHVSF|Grupo Chartwell|Grupo Hotelero Santa Fe|Hilton Vallarta|Francisco Medina|Francisco Zinser|Ancira|GSF Hotels|recUNycnMwOVFX0hc)\b|\bKGPV\b|(?<![A-Za-z])GSF(?![A-Za-z])/i;

  function assertNoForeignHotelTokens(html, context) {
    var text = String(html || "");
    var m = text.match(FOREIGN_HOTEL_TOKEN_RE);
    if (m && typeof console !== "undefined" && console.error) {
      console.error("[HotelIntelligenceWorkspace] CROSS_HOTEL_CONTAMINATION", {
        context: context || "render",
        token: m[0],
      });
    }
    return !m;
  }

  function truncateEntityLabel(name, maxLen) {
    var s = String(name || "").trim();
    if (!s) return "Not verified";
    maxLen = maxLen || 72;
    if (s.length <= maxLen) return s;
    var short = s.split("(")[0].trim();
    return short.length <= maxLen ? short : short.slice(0, maxLen - 1) + "…";
  }

  function brandChronologyRows(brandChrono, brand, brandStatus) {
    var rows = Array.isArray(brandChrono) ? brandChrono.slice() : [];
    if (!rows.length && brand && brand !== "—") {
      rows.push({ date: "CURRENT", label: brand, brand: brand, status: brandStatus || "CURRENT" });
    }
    return rows;
  }

  function buildDataDrivenStructureChart(hotel, ctrl, opName) {
    return buildOwnershipStructureChart(hotel, ctrl, opName, {
      ariaLabel: "Ownership structure",
      forceEcon: true,
    });
  }

  function shortCompare(a, b) {
    function norm(s) {
      return String(s || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();
    }
    var x = norm(a);
    var y = norm(b);
    if (!x || !y) return false;
    return x === y || x.indexOf(y) >= 0 || y.indexOf(x) >= 0;
  }

  function buildDataDrivenCommercialRels(ctrl, brandChrono, brand, brandStatus, opName, chain) {
    var charts = "";
    var hist =
      (Array.isArray(chain) ? chain : []).filter(function (n) {
        return (
          /former|historical|seller|lender|investor/i.test(n.role || "") ||
          /FORMER|HISTORICAL/i.test(n.status || "")
        );
      }) || [];
    var lender = ctrl.lender && ctrl.lender.name ? ctrl.lender : null;
    var histNodes = hist.slice(0, 4);
    if (lender && !histNodes.some(function (n) {
      return shortCompare(n.name, lender.name);
    })) {
      histNodes.push({
        name: lender.name,
        role: "lender",
        status: ctrl.lender.status || "HIGH",
        note: ctrl.lender.note || null,
      });
    }
    if (histNodes.length || (opName && opName !== "—")) {
      charts += orgChart(
        (histNodes.length
          ? histNodes
              .map(function (n) {
                return chainNode(
                  truncateEntityLabel(n.name),
                  roleLabel(n.role) || "Historical / Commercial",
                  friendlyStatus(n.confidence || n.status || "HIGH"),
                  /former|seller|historical/i.test(n.role || "") ? "former" : "org"
                );
              })
              .join(chainEdge("Related"))
          : "") +
          (opName && opName !== "—"
            ? (histNodes.length ? chainEdge("Operates / manages") : "") +
              chainNode(opName, "Operator / Management Company", "High", "org")
            : ""),
        "Historical and operator relationships"
      );
    }
    var chrono = brandChronologyRows(brandChrono, brand, brandStatus);
    if (chrono.length) {
      charts += orgChart(
        chrono
          .slice(0, 4)
          .map(function (b, i) {
            return (
              (i ? chainEdge(String(b.status || "Brand")) : "") +
              chainNode(
                truncateEntityLabel(b.label || b.brand),
                friendlyStatus(b.status || "CURRENT") + " brand / affiliation",
                b.date || b.status || "",
                /FORMER|HISTOR/i.test(b.status || "")
                  ? "former"
                  : /ANNOUNCED|PLANNED/i.test(b.status || "")
                    ? "announced"
                    : "brand"
              )
            );
          })
          .join(""),
        "Brand chronology"
      );
    }
    return charts ? '<div class="hiw-org-charts">' + charts + "</div>" : "";
  }

  function buildDataDrivenControlRows(ctrl, org, opName, brand, brandStatus, brandChrono) {
    var propco = ctrl.legal_property_owner_propco || {};
    var econ = ctrl.economic_owner_or_group || {};
    var parent = ctrl.parent_sponsor || {};
    var developer = ctrl.developer || {};
    var lender = ctrl.lender || {};
    var chrono = brandChronologyRows(brandChrono, brand, brandStatus);
    var former = chrono
      .filter(function (b) {
        return /FORMER/i.test(b.status || "");
      })
      .map(function (b) {
        return b.label || b.brand;
      })
      .filter(Boolean);
    var announced = chrono
      .filter(function (b) {
        return /ANNOUNCED|PLANNED/i.test(b.status || "");
      })
      .map(function (b) {
        return b.label || b.brand;
      })
      .filter(Boolean);
    var rows =
      rowStatus(
        "Legal property owner / PropCo",
        propco.name || "Not yet verified",
        propco.known === false ? "gap" : statusClass(propco.status || "high")
      ) +
      rowStatus(
        "Economic owner",
        econ.name || "Not yet verified",
        econ.known === false ? "gap" : statusClass(econ.status || "high")
      ) +
      rowStatus(
        "Parent / sponsor",
        parent.name || org.display_name || "Not yet verified",
        parent.known === false && !org.display_name ? "gap" : statusClass(parent.status || "high")
      );
    if (lender.name) {
      rows += rowStatus("Lender / financier", lender.name, statusClass(lender.status || "probable"));
    }
    rows +=
      rowStatus(
        "Operator / management company",
        opName +
          (ctrl.operator && ctrl.operator.status
            ? " (" + friendlyStatus(ctrl.operator.status) + ")"
            : ""),
        statusClass((ctrl.operator && ctrl.operator.status) || "high")
      );
    if (developer.name) {
      rows += rowStatus("Developer", developer.name, statusClass(developer.status || "high"));
    }
    rows += rowStatus(
      "Current brand",
      brand + (brandStatus ? " (" + friendlyStatus(brandStatus) + ")" : ""),
      statusClass(brandStatus || "high")
    );
    if (former.length) {
      rows += rowStatus("Former brands / affiliations", former.join(" · "), "high");
    }
    if (announced.length) {
      rows += rowStatus("Announced brand / operator relationships", announced.join(" · "), "probable");
    }
    return rows;
  }

  function buildDataDrivenUbo(ctrl, org, people) {
    var econ = ctrl.economic_owner_or_group || {};
    var principals = (people || [])
      .filter(function (p) {
        return /principal|ownership|founder|ceo|sponsor/i.test(
          String(p.decision_authority_category || p.role_category || p.category || p.title || "")
        );
      })
      .map(function (p) {
        return p.name + (p.title ? " (" + p.title + ")" : "");
      })
      .slice(0, 4);
    return (
      wrapKvTable(
        row("Corporate economic owner / sponsor", econ.name || org.display_name || "Not yet verified") +
          row(
            "Ultimate controlling organization",
            org.display_name || org.legal_name || econ.name || "Not yet verified"
          ) +
          row(
            "Named principals (titles only — not deed UBO)",
            principals.length ? principals.join(" · ") : "Not independently verified as deed UBO"
          )
      ) +
      '<p class="hiw-prose hiw-prose--quiet">Natural-person beneficial ownership has not been independently verified at deed level.</p>'
    );
  }

  function buildDataDrivenAssetProfile(ctrl, ownership, hotel, brand, opName) {
    var profile =
      (ownership && ownership.hotel && ownership.hotel.property_profile) ||
      (ownership && ownership.deep_research && ownership.deep_research.property_profile) ||
      {};
    var redev =
      (ownership && ownership.report && ownership.report.redevelopment) ||
      (ownership && ownership.deep_research && ownership.deep_research.redevelopment) ||
      {};
    var structure =
      (ctrl.ownership_structure && ctrl.ownership_structure.structure) ||
      (ctrl.economic_owner_or_group && ctrl.economic_owner_or_group.name
        ? "Economic owner / sponsor via property company"
        : "Operator known; ownership structure unresolved");
    var brandLine =
      brand && brand !== "—"
        ? brand +
          ((ownership && ownership.hotel && ownership.hotel.brand_display_status)
            ? " (" + friendlyStatus(ownership.hotel.brand_display_status) + ")"
            : "")
        : "—";
    var rooms =
      (ownership && ownership.hotel && ownership.hotel.rooms) ||
      hotel.rooms ||
      (profile.rooms_suites != null ? profile.rooms_suites : null);
    return wrapKvTable(
      row("Ownership / management structure", structure) +
        row("Franchise / brand arrangement", brandLine) +
        row(
          "Known rooms / suites",
          rooms != null && Number(rooms) > 0 ? String(rooms) : "—"
        ) +
        row(
          "Redevelopment / renovation",
          redev.summary ||
            redev.scope ||
            redev.note ||
            (redev.tourism_investment_order ? "Tourism Investment Order / renovation frame supported" : "—")
        ) +
        row("Operator", opName || "—") +
        row(
          "Lender",
          (ctrl.lender && ctrl.lender.name) || "—"
        )
    );
  }

  function buildDataDrivenPursuit(pursuit, brandChrono, brand, brandStatus, ctrl, opName) {
    pursuit = pursuit || {};
    var chrono = brandChronologyRows(brandChrono, brand, brandStatus);
    var brandSummary =
      pursuit.brand_relationships_summary ||
      chrono
        .map(function (b) {
          return friendlyStatus(b.status || "CURRENT") + " " + (b.label || b.brand);
        })
        .join("; ") ||
      (brand && brand !== "—" ? "CURRENT " + brand : "Brand relationships not yet promoted.");
    var opSummary =
      pursuit.operator_relationships_summary ||
      (opName && opName !== "—"
        ? opName +
          (ctrl.operator && ctrl.operator.status
            ? " (" + friendlyStatus(ctrl.operator.status) + ")"
            : "")
        : "Operator relationship not yet promoted.");
    return (
      '<div class="hiw-qa">' +
      '<div class="hiw-qa__q">Why this property?</div><p class="hiw-qa__a">' +
      esc(pursuit.why_matters || "Ownership intelligence supports commercial diligence for this asset.") +
      "</p>" +
      '<div class="hiw-qa__q">Why this owner / organization?</div><p class="hiw-qa__a">' +
      esc(pursuit.who_controls_relationship || opName || "Control path still being resolved.") +
      "</p>" +
      '<div class="hiw-qa__q">Portfolio leverage</div><p class="hiw-qa__a">' +
      esc(pursuit.portfolio_leverage || "Review owned versus managed relationships before outreach.") +
      "</p>" +
      '<div class="hiw-qa__q">Brand relationships</div><p class="hiw-qa__a">' +
      esc(brandSummary) +
      "</p>" +
      '<div class="hiw-qa__q">Operator relationships</div><p class="hiw-qa__a">' +
      esc(opSummary) +
      "</p>" +
      '<div class="hiw-qa__q">What has changed?</div><p class="hiw-qa__a">' +
      esc(pursuit.timing_signals || "See property history for supported change events.") +
      "</p>" +
      '<div class="hiw-qa__q">Outreach path</div><p class="hiw-qa__a">' +
      esc(pursuit.approach_organization || "Use verified owner / operator / property channels only.") +
      "</p>" +
      '<div class="hiw-qa__q">Research gaps before outreach</div><p class="hiw-qa__a">' +
      esc(pursuit.missing_before_outreach || "Resolve ownership / operator / signatory gaps before outreach.") +
      "</p></div>"
    );
  }

  function normalizeResearchGaps(gaps, rich) {
    if (Array.isArray(gaps) && gaps.length) {
      return gaps.map(function (g) {
        if (typeof g === "string") {
          return {
            what: g,
            why: "Affects outreach confidence or legal diligence",
            state: "Unresolved",
          };
        }
        return {
          what: g.what || g.text || g.gap || g.title || "Unresolved item",
          why: g.why || g.why_it_matters || "Affects commercial or legal diligence",
          state: g.state || g.evidence_state || "Unresolved",
        };
      });
    }
    if (rich) {
      return [
        {
          what: "Natural-person beneficial ownership",
          why: "Ultimate control diligence beyond the listed sponsor / company",
          state: "Not independently verified",
        },
        {
          what: "Legal / franchise signing authority",
          why: "Required before treating any individual as a qualified signatory",
          state: "Titles verified; authority not established",
        },
      ];
    }
    return [
      {
        what: "Economic owner",
        why: "Defines who controls the asset commercially",
        state: "Unresolved",
      },
      {
        what: "Property company / PropCo",
        why: "Defines the legal holding vehicle",
        state: "Unresolved",
      },
    ];
  }

  function friendlyStatus(raw) {
    var s = String(raw || "").toUpperCase();
    if (s === "HIGH" || s === "VERIFIED") return s === "HIGH" ? "High" : "Verified";
    if (s === "MEDIUM") return "Medium";
    if (s === "PROBABLE") return "Probable";
    if (s === "CONTESTED") return "Contested";
    if (s === "PREVIEW") return "Display Only";
    if (s === "CURRENT") return "Current";
    if (s === "FORMER") return "Former";
    if (s === "ANNOUNCED") return "Announced";
    if (s === "PLANNED") return "Planned";
    if (s === "CANCELLED") return "Cancelled";
    if (s === "LIVE") return "Verified";
    if (s === "PROPOSED" || s === "PROPOSED_STAGED") return "Proposed · not persisted";
    if (s === "UNRESOLVED") return "Unresolved";
    if (s === "OPERATOR_PRESENT_OWNER_UNRESOLVED") {
      return "Operator identified · economic owner unresolved";
    }
    if (s === "ECONOMIC_OWNER_PROPOSED_UBO_UNRESOLVED") {
      return "Economic sponsor proposed · UBO unresolved";
    }
    if (s === "PARTIAL") return "Partial graph";
    if (s === "VERIFIED_HPC_ASSOCIATION") return "Verified HPC association";
    if (s === "PROPERTY_MATCH_READ") return "Property match (read)";
    if (
      s === "UNKNOWN" ||
      s === "NOT_YET_VERIFIED" ||
      s === "NOT_VERIFIED" ||
      s === "AUTHORITY NOT VERIFIED" ||
      !s
    ) {
      return "Not Yet Verified";
    }
    return toProperCase(s.replace(/_/g, " ").toLowerCase());
  }

  function statusClass(raw) {
    var s = String(raw || "").toLowerCase();
    if (s === "high" || s === "verified" || s === "current" || s === "live")
      return s === "current" || s === "live" ? "verified" : s;
    if (s === "contested" || s === "probable" || s === "announced" || s === "former")
      return s === "announced" || s === "former" ? "probable" : s;
    return "gap";
  }

  function relTypeLabel(type) {
    var t = String(type || "").toUpperCase();
    var map = {
      OWNED_BY: "Owned / Controlled By",
      CONTROLLED_BY: "Controlled By",
      OPERATED_BY: "Operated / Managed By",
      ASSET_MANAGED_BY: "Asset-Managed By",
      BRANDED_BY: "Branded As",
      DEVELOPED_BY: "Developed By",
      SPONSORED_BY: "Sponsored By",
      JV_WITH: "Joint Venture With",
      LEASED_FROM: "Leased From",
      FORMER_OWNED_BY: "Formerly Owned By",
      ANNOUNCED_BRAND: "Announced Brand",
      "LEGAL OWNER": "Legal Owner / PropCo",
      "ECONOMIC OWNER": "Economic Owner / Sponsor",
      OPERATOR: "Operated / Managed By",
      BRAND: "Branded As",
    };
    if (map[t]) return map[t];
    return toProperCase(
      String(type || "")
        .replace(/_/g, " ")
        .toLowerCase()
    );
  }

  function toProperCase(label) {
    var acronyms = {
      propco: "PropCo",
      ubo: "UBO",
      ir: "IR",
      bmv: "BMV",
      gsf: "GSF",
      jv: "JV",
      hq: "HQ",
      ceo: "CEO",
      gm: "GM",
      cfo: "CFO",
      "f&b": "F&B",
      fnb: "F&B",
    };
    return String(label || "")
      .replace(/_/g, " ")
      .split(/(\s+|\/|·|&amp;|&|\(|\))/)
      .map(function (part) {
        if (!part || /^(\s+|\/|·|&amp;|&|\(|\))$/.test(part)) return part;
        var lower = part.toLowerCase();
        if (acronyms[lower]) return acronyms[lower];
        if (/^[A-Z0-9]{2,}$/.test(part) && part.length <= 6) return part;
        return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
      })
      .join("");
  }

  function sourceTypeLabel(provider) {
    var p = String(provider || "").toLowerCase();
    if (/bmv|filing|annual|issuer|securities/.test(p)) return "Corporate Filing";
    if (/hyatt|hilton|brand|newsroom|press/.test(p)) return "Brand / Press";
    if (/census|dealality/.test(p)) return "Dealality Census";
    if (/operator|management/.test(p)) return "Operator";
    if (/linkedin|tripadvisor|people|listing/.test(p)) return "People / Listing";
    if (/transaction|acquisition/.test(p)) return "Transaction / History";
    if (/market.?alert/.test(p)) return "Market Alerts";
    if (/guest|reputation|booking|expedia|agoda|trivago|hotels\.com|google/.test(p))
      return "Guest Reputation";
    return "Source";
  }

  function citationCategory(s) {
    if (!s) return "ownership";
    if (s.category && SOURCE_CATEGORY_DEFS.some(function (c) { return c.id === s.category; })) {
      return s.category;
    }
    var supports = String(s.supports || "").toLowerCase();
    var type = String(s.source_type || "").toLowerCase();
    var provider = String(s.provider || s.publisher || "").toLowerCase();
    var blob = supports + " " + type + " " + provider + " " + String(s.title || "").toLowerCase();
    if (/guest_sentiment|guest reputation|tripadvisor|booking\.com|expedia|agoda|trivago|hotels\.com|google reviews/.test(blob))
      return "sentiment";
    if (/market_alerts|market alerts|news/.test(blob)) return "news";
    if (/contact|people|linkedin|person/.test(blob)) return "contacts";
    if (/financ|transaction|acquisition|develop|capex|debt|loan/.test(blob)) return "financing";
    if (/brand_census|brand \/ press|brand|operator|franchise|management company/.test(blob) && !/ownership/.test(supports))
      return /operator|management|brand/.test(supports + " " + type) ? "brand" : "brand";
    if (/property|facilit|amenit|rooms|meeting|adp|hpc|census/.test(blob) && /property|facilit|amenit|rooms|meeting|adp/.test(blob))
      return "property";
    if (/dealality census/.test(type) || supports === "brand_census") return "brand";
    if (/ownership|corporate filing|propco|sponsor|ubo|legal/.test(blob)) return "ownership";
    if (supports === "operator" || supports === "brand_census") return "brand";
    if (supports === "ownership") return "ownership";
    return "ownership";
  }

  function formatSourceDate(value) {
    if (value == null || value === "") return "—";
    var s = String(value).trim();
    if (!s) return "—";
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
    if (/^\d{4}$/.test(s)) return s;
    try {
      var d = new Date(s);
      if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
    } catch (_e) {
      /* keep raw */
    }
    return s.length > 12 ? s.slice(0, 10) : s;
  }

  /** Normalize citation dates — never promote access/verify time to publication. */
  function normalizeCitationDates(raw) {
    var s = raw || {};
    var publication =
      s.publication_date ||
      s.published_at ||
      s.publishedAt ||
      s.document_date ||
      s.transaction_date ||
      s.source_date ||
      null;
    var verified = s.verified_at || s.last_verified || s.lastVerifiedAt || null;
    var accessed = s.accessed_at || s.accessedAt || s.retrieved_at || null;
    var type = String(s.source_type || s.canonicalSource || "").toLowerCase();
    var blob =
      type +
      " " +
      String(s.supports || "").toLowerCase() +
      " " +
      String(s.title || "").toLowerCase() +
      " " +
      String(s.provider || "").toLowerCase();
    var isNews =
      s.supports === "market_alerts_hotel_fact" ||
      type.indexOf("market alert") >= 0 ||
      s.canonicalSource === "MarketAlerts";
    var looksPublishedSource =
      isNews ||
      /press|journal|news|business|transaction|acquisition|filing|securities|bmv|deed|registry/.test(
        blob
      );
    // Legacy payloads often stuffed publication into observed_date.
    if (!publication && s.observed_date && (s._observed_is_publication === true || looksPublishedSource)) {
      publication = s.observed_date;
    } else if (!publication && s.observed_date) {
      var year = parseInt(String(s.observed_date).slice(0, 4), 10);
      if (year && year < 2025) {
        // Historical calendar date on a research citation is almost always publication/document date.
        publication = s.observed_date;
      } else if (!accessed && !verified) {
        accessed = s.observed_date;
      }
    }
    return {
      publication_date: publication || null,
      verified_at: verified || null,
      accessed_at: accessed || null,
    };
  }

  function citationDedupeKey(c) {
    if (!c) return "";
    if (c.market_alerts_id) return "ma:" + c.market_alerts_id;
    if (c.dedupe_id) return "d:" + c.dedupe_id;
    if (c.url) return "u:" + String(c.url).toLowerCase().replace(/\/$/, "");
    return (
      "t:" +
      String(c.title || "")
        .toLowerCase()
        .trim() +
      "|" +
      String(c.provider || c.publisher || "")
        .toLowerCase()
        .trim()
    );
  }

  function dedupeCitations(list) {
    var out = [];
    var seen = {};
    (list || []).forEach(function (c) {
      var key = citationDedupeKey(c);
      if (!key || seen[key]) return;
      seen[key] = true;
      out.push(c);
    });
    return out;
  }

  function enrichCitation(raw) {
    var s = Object.assign({}, raw || {});
    var dates = normalizeCitationDates(s);
    s.publication_date = dates.publication_date;
    s.verified_at = dates.verified_at;
    s.accessed_at = dates.accessed_at;
    s.publisher = s.publisher || s.provider || "";
    s.source_type = s.source_type || sourceTypeLabel(s.provider || s.title);
    s.category = citationCategory(s);
    s.verification_status =
      s.verification_status ||
      (s.supports === "ownership" ? "EVIDENCE_BACKED" : s.url ? "LINKED" : "REFERENCED");
    return s;
  }

  function navLinkHtml(label, attrs) {
    attrs = attrs || {};
    var parts = ['type="button"', 'class="hiw-navlink"'];
    Object.keys(attrs).forEach(function (k) {
      if (attrs[k] == null || attrs[k] === "") return;
      parts.push(k + '="' + esc(String(attrs[k])) + '"');
    });
    return (
      "<button " +
      parts.join(" ") +
      ">" +
      esc(label) +
      '<span class="hiw-navlink__icon" aria-hidden="true">↗</span></button>'
    );
  }

  function sectionHeadHtml(title, navHtml, headingClass) {
    var hClass = headingClass || "hiw-section__h";
    if (!navHtml) {
      return '<h3 class="' + hClass + '">' + title + "</h3>";
    }
    return (
      '<div class="hiw-section__head">' +
      '<h3 class="' +
      hClass +
      '">' +
      title +
      "</h3>" +
      '<div class="hiw-section__nav">' +
      navHtml +
      "</div></div>"
    );
  }

  function citeBtn(n) {
    return (
      '<button type="button" class="hiw-cite" data-hiw-cite="' +
      n +
      '" aria-label="Open citation ' +
      n +
      '"><sup>[' +
      n +
      "]</sup></button>"
    );
  }

  function shortUrlLabel(url) {
    if (!url) return "";
    try {
      var u = new URL(url);
      return u.hostname.replace(/^www\./, "") + (u.pathname.length > 1 ? "…" : "");
    } catch (e) {
      return "Open source";
    }
  }

  function ensureRoot() {
    var root = document.getElementById("hiwRoot");
    if (root) return root;
    root = document.createElement("div");
    root.id = "hiwRoot";
    root.className = "hiw-root";
    root.setAttribute("role", "dialog");
    root.setAttribute("aria-modal", "true");
    root.innerHTML =
      '<div class="hiw-topbar">' +
      '<button type="button" class="hiw-back" data-hiw-back>← Back to Radar</button>' +
      '<div class="hiw-topbar__title">Hotel Intelligence Workspace</div>' +
      '<div class="hiw-topbar__crumb" id="hiwCrumb"></div>' +
      '<div class="hiw-topbar__demo">Golden Demo</div>' +
      "</div>" +
      '<header class="hiw-header" id="hiwHeader"></header>' +
      '<nav class="hiw-tabs" id="hiwTabs" role="tablist"></nav>' +
      '<div class="hiw-body">' +
      '<main class="hiw-main" id="hiwMain"></main>' +
      '<aside class="hiw-rail" id="hiwRail"></aside>' +
      "</div>";
    document.body.appendChild(root);
    root.addEventListener("click", onRootClick);
    return root;
  }

  function snapshotRadar() {
    return {
      scrollX: window.scrollX,
      scrollY: window.scrollY,
      hash: window.location.hash,
      search: window.location.search,
      country: (document.getElementById("countryFilter") || {}).value || null,
      market: (document.getElementById("marketFilter") || {}).value || null,
    };
  }

  function buildCitations(hotel, ownership) {
    var list = [];
    var deepSources =
      (ownership && ownership.report && ownership.report.evidence && ownership.report.evidence.sources) ||
      (ownership && ownership.deep_research && ownership.deep_research.sources) ||
      [];
    var ev = ownership && ownership.report && ownership.report.evidence;
    var sources = deepSources.length ? deepSources : (ev && ev.sources) || [];
    var defaultSupports = deepSources.length ? "ownership" : "operator";
    sources.forEach(function (s) {
      list.push(
        enrichCitation({
          title: s.title || s.provider || "Source",
          url: s.url || null,
          provider: s.provider || "",
          publisher: s.publisher || s.provider || "",
          publication_date: s.publication_date || s.published_at || s.document_date || null,
          verified_at: s.verified_at || s.last_verified || null,
          accessed_at: s.accessed_at || s.retrieved_at || null,
          observed_date: s.observed_date || null,
          excerpt: s.evidence_excerpt || s.note || "",
          claim:
            (ev && ev.claim) ||
            (ownership &&
              ownership.report &&
              ownership.report.evidence &&
              ownership.report.evidence.claim) ||
            "",
          relationship:
            (ev && ev.relationship) ||
            (ownership &&
              ownership.report &&
              ownership.report.evidence &&
              ownership.report.evidence.relationship),
          proves:
            (ev && ev.what_it_proves) ||
            (ownership &&
              ownership.report &&
              ownership.report.evidence &&
              ownership.report.evidence.what_it_proves),
          does_not:
            (ev && ev.what_it_does_not_prove) ||
            (ownership &&
              ownership.report &&
              ownership.report.evidence &&
              ownership.report.evidence.what_it_does_not_prove),
          supports: s.supports || defaultSupports,
          source_type: sourceTypeLabel(s.provider || s.title),
          verification_status: s.verification_status || null,
          dedupe_id: s.id || s.source_id || null,
        })
      );
    });
    list.push(
      enrichCitation({
        title: "Dealality Hotel Census — affiliation & management fields",
        url: null,
        provider: "dealality_hotel_census",
        publisher: "Dealality Hotel Census",
        publication_date: null,
        verified_at: null,
        accessed_at: null,
        observed_date: null,
        excerpt:
          (hotel && hotel.name ? hotel.name + ": " : "") +
          "Affiliation / brand field and Management Company as stored in Hotel Census.",
        claim: "Census identity fields for brand affiliation and management company",
        proves:
          "Documents how Dealality currently stores trading name, affiliation, and management company for this hotel.",
        does_not:
          "Does not by itself prove completed reflag, franchise contract, or economic ownership.",
        supports: "brand_census",
        source_type: "Dealality Census",
        category: "brand",
        dedupe_id: "census:affiliation-management",
      })
    );
    list = mergeMarketAlertsCitationsLocal(list, state.marketAlertsCitations || []);
    list = mergeMarketAlertsCitationsLocal(list, state.guestSentimentCitations || []);
    list = mergeMarketAlertsCitationsLocal(list, state.contactCitations || []);
    return dedupeCitations(list.map(enrichCitation));
  }

  function applyContactEvidenceCitations(evidenceList) {
    var cites = (evidenceList || [])
      .filter(function (e) {
        // Never index Surfe / provider-only payloads in the canonical Sources register.
        var blob = String(e.provider || e.provenance || e.verificationStatus || e.note || "").toLowerCase();
        if (/surfe|provider_returned|ephemeral|session-only/.test(blob)) return false;
        if (e.surfeDerived === true) return false;
        return !!(e.title || e.claim || e.sourceUrl || e.sourceTitle);
      })
      .map(function (e) {
        return enrichCitation({
          title: e.sourceTitle || e.title || e.claim || "Contact research source",
          url: e.sourceUrl || e.profileUrl || e.url || null,
          provider: e.publisher || e.provider || "Contact research",
          publisher: e.publisher || null,
          publication_date: e.documentDate || e.publication_date || null,
          verified_at: e.retrievedAt || e.lastVerified || null,
          accessed_at: e.retrievedAt || null,
          observed_date: null,
          excerpt: e.note || e.claim || "",
          claim: e.claim || e.title || "",
          proves: Array.isArray(e.proves) ? e.proves.join("; ") : e.proves || e.claim || "",
          does_not:
            e.does_not ||
            "Does not by itself prove signing authority or property-specific assignment without independent confirmation.",
          supports: "contact_research",
          source_type: "Contacts & People",
          category: "contacts",
          verification_status: e.verificationStatus || "VERIFIED_PUBLIC",
          dedupe_id: e.id || e.dedupe_id || null,
        });
      });
    state.contactCitations = cites;
    state.citations = buildCitations(state.hotel, state.ownership);
  }

  /** Client-side dedupe merge — MarketAlerts remain canonical; no HI Evidence copy. */
  function mergeMarketAlertsCitationsLocal(base, ma) {
    var out = (base || []).slice();
    var seen = {};
    function mark(c) {
      if (c && c.market_alerts_id) seen["id:" + c.market_alerts_id] = true;
      if (c && c.dedupe_id) seen["dedupe:" + c.dedupe_id] = true;
      if (c && c.url) seen["url:" + String(c.url).toLowerCase()] = true;
    }
    out.forEach(mark);
    (ma || []).forEach(function (c) {
      var keys = [];
      if (c.market_alerts_id) keys.push("id:" + c.market_alerts_id);
      if (c.dedupe_id) keys.push("dedupe:" + c.dedupe_id);
      if (c.url) keys.push("url:" + String(c.url).toLowerCase());
      if (keys.some(function (k) {
        return seen[k];
      }))
        return;
      keys.forEach(function (k) {
        seen[k] = true;
      });
      out.push(c);
    });
    return out;
  }

  function applyMarketAlertsEvidencePayload(payload) {
    var cites = (payload && payload.evidenceCitations) || [];
    if ((!cites || !cites.length) && payload && payload.articles && payload.articles.length) {
      cites = payload.articles.map(function (a) {
        return enrichCitation({
          title: a.headline || "Market Alerts article",
          url: a.url || null,
          provider: a.publisher || "Market Alerts",
          publisher: a.publisher || null,
          // Publication date = article date; never substitute research/access time.
          publication_date: a.publishedAt || a.publicationDate || null,
          verified_at: a.verifiedAt || a.lastVerifiedAt || null,
          accessed_at: a.accessedAt || null,
          observed_date: null,
          excerpt: a.supportedFact || a.relevanceSummary || "",
          claim: a.supportedFact || a.relevanceSummary || a.headline || "",
          proves:
            "Supports the hotel-specific Market Alerts fact summarized for this property.",
          does_not:
            "Does not by itself prove completed ownership transfer, CapEx execution, or franchise contracting without independent verification.",
          supports: "market_alerts_hotel_fact",
          source_type: "Market Alerts",
          category: "news",
          verification_status: a.verificationStatus || "PROPERTY_MATCH_READ",
          market_alerts_id: a.id || null,
          dedupe_id: a.dedupeKey || null,
          canonicalSource: "MarketAlerts",
        });
      });
    } else {
      cites = (cites || []).map(function (c) {
        return enrichCitation(
          Object.assign({}, c, {
            publication_date:
              c.publication_date ||
              c.published_at ||
              c.publishedAt ||
              (c._observed_is_publication ? c.observed_date : null) ||
              null,
            verified_at: c.verified_at || c.last_verified || null,
            accessed_at: c.accessed_at || c.accessedAt || null,
            // If legacy payload stuffed publication into observed_date, recover it.
            _observed_is_publication: !!(c.publication_date || c.publishedAt || c.published_at),
            category: "news",
            supports: c.supports || "market_alerts_hotel_fact",
            source_type: c.source_type || "Market Alerts",
            canonicalSource: c.canonicalSource || "MarketAlerts",
          })
        );
      });
    }
    state.marketAlertsCitations = cites || [];
    state.citations = buildCitations(state.hotel, state.ownership);
  }

  function applyGuestSentimentEvidencePayload(payload) {
    var overview = reputationOverviewCards(payload) || [];
    var cites = overview
      .filter(function (p) {
        return p && p.available === true && p.listingUrl;
      })
      .map(function (p) {
        return enrichCitation({
          title: (p.platformLabel || p.platform || "Guest listing") + " reputation listing",
          url: p.listingUrl,
          provider: p.platformLabel || p.platform || "Guest Reputation",
          publisher: p.platformLabel || p.platform || "Guest Reputation",
          publication_date: null,
          verified_at: p.observedAt || null,
          accessed_at: p.observedAt || null,
          observed_date: null,
          excerpt:
            p.rating != null
              ? "Native listing score " +
                p.rating +
                (p.scale ? " / " + p.scale : "") +
                (p.reviewCount != null ? " · " + p.reviewCount + " reviews" : "")
              : "Guest reputation listing observation",
          claim: "Guest reputation listing observation for this property",
          proves: "Documents the observed guest-listing URL and native score metadata.",
          does_not:
            "Does not prove CapEx needs, operational KPIs, or ownership. Platform scores are never averaged across sites.",
          supports: "guest_sentiment",
          source_type: "Guest Reputation",
          category: "sentiment",
          verification_status: "OBSERVED_LISTING",
          dedupe_id: "guest:" + String(p.platform || p.platformLabel || p.listingUrl),
        });
      });
    state.guestSentimentCitations = cites;
    state.citations = buildCitations(state.hotel, state.ownership);
  }

  function getControl(ownership) {
    var ctrl =
      (ownership && ownership.report && ownership.report.ownership_and_control) || {};
    // Normalize HI-graph overlay keys to golden-demo control vocabulary.
    if (!ctrl.economic_owner_or_group && ctrl.economic_owner) {
      ctrl = Object.assign({}, ctrl, { economic_owner_or_group: ctrl.economic_owner });
    }
    if (!ctrl.legal_property_owner_propco && ctrl.property_company) {
      ctrl = Object.assign({}, ctrl, {
        legal_property_owner_propco: ctrl.property_company,
      });
    }
    return ctrl;
  }

  function controlPartyKnown(party) {
    return !!(party && party.known === true && party.name);
  }

  function controlPartyMeta(party, fallbackWhenUnknown) {
    if (controlPartyKnown(party)) {
      return friendlyStatus(party.status || party.confidence || "HIGH");
    }
    return fallbackWhenUnknown || "Not verified";
  }

  function buildOwnershipStructureChart(hotel, ctrl, opName, opts) {
    opts = opts || {};
    var propco = ctrl.legal_property_owner_propco || {};
    var econ = ctrl.economic_owner_or_group || {};
    var propcoKnown = controlPartyKnown(propco);
    var econKnown = controlPartyKnown(econ);
    var propcoLabel = propcoKnown
      ? truncateEntityLabel(propco.name)
      : "Property company (name not verified)";
    var econLabel = econKnown
      ? truncateEntityLabel(econ.name)
      : "Economic owner (name not verified)";
    var op =
      opName && opName !== "—"
        ? opName
        : (ctrl.operator && ctrl.operator.name) || null;
    var brand = opts.brand || null;
    var brandStatus = opts.brandStatus || null;
    var ownedOperated =
      econKnown &&
      op &&
      shortCompare(econ.name || econLabel, op);

    var html = chainNode(hotel.name, "Hotel / Property", null, "focus");
    html += chainEdge(propcoKnown ? "Held through" : "Title vehicle");
    html += chainNode(
      propcoLabel,
      "Legal Property Owner",
      controlPartyMeta(propco, "Unknown"),
      propcoKnown ? "org" : "gap"
    );
    if (econKnown || opts.forceEcon) {
      html += chainEdge("Controlled / sponsored by");
      html += chainNode(
        econLabel,
        ownedOperated ? "Economic Owner / Sponsor · Owned & Operated" : "Economic Owner / Sponsor",
        controlPartyMeta(econ, "Not verified"),
        econKnown ? "org" : "gap"
      );
    }
    if (opts.includeOperator && op) {
      html += chainEdge("Operated / managed by");
      html += chainNode(
        op,
        "Hotel Operator",
        friendlyStatus((ctrl.operator && (ctrl.operator.status || ctrl.operator.verification_bucket)) || "HIGH"),
        "org"
      );
    }
    if (opts.includeBrand && brand) {
      html += chainEdge("Current brand");
      html += chainNode(brand, "Brand Affiliation", brandStatus || "Current", "brand");
    }
    return orgChart(html, opts.ariaLabel || "Ownership & Control Hierarchy");
  }

  function renderHeader(hotel, ownership) {
    var ctrl = getControl(ownership);
    var econ = ctrl.economic_owner_or_group || {};
    var propco = ctrl.legal_property_owner_propco || {};
    var op = (ctrl.operator && ctrl.operator.name) || hotel.managementCompany || "—";
    var opBucket = (ctrl.operator && ctrl.operator.verification_bucket) || "UNKNOWN";
    var brand =
      (ownership && ownership.hotel && ownership.hotel.brand_display) ||
      hotel.brand ||
      hotel.affiliation ||
      "—";
    var brandStatus =
      (ownership && ownership.hotel && ownership.hotel.brand_display_status) || "";
    var cites = state.citations || [];
    var rich = isRichCase(ownership);
    var ownerKnown = controlPartyKnown(econ);
    var propcoKnown = controlPartyKnown(propco);
    var ownerValue = ownerKnown ? econ.name : "Not yet verified";
    var ownerMeta = ownerKnown
      ? friendlyStatus(econ.status || "HIGH")
      : "Research gap";
    var propcoValue = propcoKnown ? propco.name : "Not yet verified";
    var propcoMeta = propcoKnown
      ? friendlyStatus(propco.status || propco.confidence || "HIGH")
      : "Research gap";
    var researchMeta = rich
      ? ownerKnown || propcoKnown
        ? "Primary ownership evidence reviewed"
        : "Case A · ownership gaps remain"
      : ownership && ownership.in_cohort
        ? "Operator-known · owner unresolved"
        : "Limited coverage";

    return (
      '<div class="hiw-header__row">' +
      "<div>" +
      '<p class="hiw-kicker">Dealality Hotel Intelligence</p>' +
      '<h1 class="hiw-name">' +
      esc(hotel.name || "Hotel") +
      "</h1>" +
      '<div class="hiw-meta">' +
      "<span><strong>Market</strong> " +
      esc(hotel.market || hotel.city || "—") +
      "</span>" +
      "<span><strong>Rooms</strong> " +
      esc(hotel.rooms != null ? hotel.rooms : "—") +
      "</span>" +
      "<span><strong>Chain scale</strong> " +
      esc(hotel.chainScale || hotel.chain_scale || "—") +
      "</span>" +
      "<span><strong>Evidence</strong> " +
      esc(researchMeta) +
      (cites.length ? " · " + cites.length + " sources" : "") +
      "</span>" +
      "</div></div>" +
      '<div class="hiw-indicators">' +
      '<button type="button" class="hiw-ind" data-hiw-tab="ownership">' +
      '<span class="hiw-ind__label">Economic Owner</span>' +
      '<span class="hiw-ind__value">' +
      esc(ownerValue) +
      "</span>" +
      '<span class="hiw-ind__meta">' +
      esc(ownerMeta) +
      "</span></button>" +
      '<button type="button" class="hiw-ind" data-hiw-tab="ownership">' +
      '<span class="hiw-ind__label">Property Company</span>' +
      '<span class="hiw-ind__value">' +
      esc(propcoValue.length > 42 ? propcoValue.slice(0, 40) + "…" : propcoValue) +
      "</span>" +
      '<span class="hiw-ind__meta">' +
      esc(propcoMeta) +
      "</span></button>" +
      '<button type="button" class="hiw-ind" data-hiw-tab="ownership">' +
      '<span class="hiw-ind__label">Operator</span>' +
      '<span class="hiw-ind__value">' +
      esc(op) +
      "</span>" +
      '<span class="hiw-ind__meta">' +
      esc(friendlyStatus(opBucket) + (cites.length ? " · " + Math.min(cites.length, 9) + " sources" : "")) +
      "</span></button>" +
      '<button type="button" class="hiw-ind" data-hiw-tab="brand">' +
      '<span class="hiw-ind__label">Current Brand</span>' +
      '<span class="hiw-ind__value">' +
      esc(brand) +
      "</span>" +
      '<span class="hiw-ind__meta">' +
      esc(brandStatus ? friendlyStatus(brandStatus) : "Affiliation") +
      "</span></button>" +
      "</div></div>"
    );
  }

  function renderTabs() {
    return TABS.map(function (t) {
      return (
        '<button type="button" class="hiw-tab' +
        (state.tab === t.id ? " is-active" : "") +
        '" role="tab" data-hiw-tab="' +
        t.id +
        '" aria-selected="' +
        (state.tab === t.id ? "true" : "false") +
        '">' +
        esc(t.label) +
        "</button>"
      );
    }).join("");
  }

  function row(label, value) {
    return (
      "<tr><td>" +
      esc(toProperCase(label)) +
      "</td><td>" +
      esc(value == null || value === "" ? "—" : value) +
      "</td></tr>"
    );
  }

  function roleLabel(role) {
    var map = {
      hotel: "Hotel",
      propco: "Property Company / PropCo",
      economic_owner: "Economic Owner",
      former_co_owner: "Historical Owner / Investor",
      public_company_principal_shareholder_group: "Principal Shareholder / Control Group",
      ubo: "Natural-Person UBO",
      operator: "Operator / Management Company",
      parent: "Parent / Public Company",
      sponsor: "Sponsor",
      developer: "Developer",
      asset_manager: "Asset Manager",
      lease_lessor: "Lease / Lessor",
      brand: "Brand",
    };
    var key = String(role || "").toLowerCase();
    if (map[key]) return map[key];
    return toProperCase(String(role || "").replace(/_/g, " "));
  }

  function insightCertaintyClass(meta) {
    var s = String(meta || "").toLowerCase();
    if (/high|verified/.test(s)) return "high";
    if (/partial|probable|announced|current/.test(s)) return "probable";
    if (/not verified|gap|unknown/.test(s)) return "gap";
    return "gap";
  }

  function insightRow(title, bodyHtml, meta) {
    var certainty = meta || "";
    var titleHtml = esc(title).replace(/\n/g, "<br>");
    return (
      '<div class="hiw-insight-card">' +
      '<div class="hiw-insight-card__head">' +
      '<h4 class="hiw-insight-card__title">' +
      titleHtml +
      "</h4>" +
      (certainty
        ? '<span class="hiw-status hiw-status--' +
          esc(insightCertaintyClass(certainty)) +
          '">' +
          esc(certainty) +
          "</span>"
        : "") +
      "</div>" +
      '<p class="hiw-insight-card__body">' +
      bodyHtml +
      "</p></div>"
    );
  }

  var HISTORY_ICONS = {
    message:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>',
    file:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/><path d="M16 13H8"/><path d="M16 17H8"/></svg>',
    "trending-up":
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M23 6l-9.5 9.5-5-5L1 18"/><path d="M17 6h6v6"/></svg>',
    eye:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>',
    building:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21h18"/><path d="M9 8h1"/><path d="M9 12h1"/><path d="M9 16h1"/><path d="M14 8h1"/><path d="M14 12h1"/><path d="M14 16h1"/><path d="M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16"/></svg>',
    clipboard:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><path d="M15 2H9a1 1 0 0 0-1 1v2a1 1 0 0 0 1 1h6a1 1 0 0 0 1-1V3a1 1 0 0 0-1-1z"/><path d="M16 13H8"/><path d="M16 17H8"/></svg>',
  };

  function historyEventMeta(h) {
    var detail = String((h && (h.event || h.text)) || "").trim();
    var lower = detail.toLowerCase();
    var title = "Property Event";
    var iconKey = "building";
    if (/renovation|renovate/.test(lower)) {
      title = "Renovations";
      iconKey = "clipboard";
    } else if (/announce|breathless|collaboration/.test(lower)) {
      title = "Brand Announcement";
      iconKey = "message";
    } else if (/brand change|reflag|altitude|krystal grand/.test(lower)) {
      title = "Brand Change";
      iconKey = "eye";
    } else if (/acquires|acquisition|ownership|stake/.test(lower)) {
      title = "Ownership Change";
      iconKey = "file";
    } else if (/expansion|hacienda|parcels|suites/.test(lower) && /open/.test(lower)) {
      title = "Expansion Opens";
      iconKey = "trending-up";
    } else if (/expansion|parcels|hacienda/.test(lower)) {
      title = "Expansion";
      iconKey = "trending-up";
    } else if (/opens as|hotel opens|opening/.test(lower)) {
      title = "Hotel Opens";
      iconKey = "building";
    } else if (/development|begins|ground/.test(lower)) {
      title = "Development Starts";
      iconKey = "building";
    }
    return {
      title: title,
      iconKey: iconKey,
      detail: detail,
      date: String((h && (h.date || h.theme)) || "—"),
      confidence: friendlyStatus((h && h.confidence) || "HIGH"),
    };
  }

  /** Recent Activities–style chronology (circle + SVG + scroll). */
  function renderPropertyHistoryFeed(history, opts) {
    opts = opts || {};
    if (!Array.isArray(history) || !history.length) {
      return opts.emptyHtml ||
        '<div class="hiw-gap-box"><h4>No Chronological Events Promoted Yet</h4></div>';
    }
    var limit = opts.limit > 0 ? opts.limit : history.length;
    var items = history.slice(0, limit);
    var list =
      '<ul class="hiw-activity-feed">' +
      items
        .map(function (h) {
          var meta = historyEventMeta(h);
          var iconSvg = HISTORY_ICONS[meta.iconKey] || HISTORY_ICONS.building;
          var tag = "Property History · " + meta.confidence;
          return (
            '<li class="hiw-activity-feed__item">' +
            '<span class="hiw-activity-feed__icon" aria-hidden="true">' +
            iconSvg +
            "</span>" +
            '<div class="hiw-activity-feed__content">' +
            '<span class="hiw-activity-feed__time">' +
            esc(meta.date) +
            "</span>" +
            '<div class="hiw-activity-feed__event">' +
            esc(meta.title) +
            "</div>" +
            '<span class="hiw-activity-feed__tag">' +
            esc(tag) +
            "</span>" +
            (meta.detail
              ? '<div class="hiw-activity-feed__detail">' + esc(meta.detail) + "</div>"
              : "") +
            "</div></li>"
          );
        })
        .join("") +
      "</ul>";
    var wrapClass =
      "hiw-activity-feed-wrap" +
      (opts.compact ? " hiw-activity-feed-wrap--compact" : "");
    return '<div class="' + wrapClass + '">' + list + "</div>";
  }

  function th(text) {
    var raw = String(text || "");
    var lines = raw.split(" / ");
    if (lines.length === 1 && raw.indexOf(" ") > 0 && raw.indexOf("&") < 0) {
      var words = raw.split(" ");
      if (words.length === 2) lines = words;
    }
    if (lines.length > 1) {
      return (
        '<th><span class="hiw-th">' +
        lines
          .map(function (line) {
            return esc(line);
          })
          .join("<br>") +
        "</span></th>"
      );
    }
    return "<th>" + esc(raw) + "</th>";
  }

  function wrapTable(inner, tableClass) {
    return (
      '<div class="hiw-table-wrap"><table class="hiw-table' +
      (tableClass ? " " + tableClass : "") +
      '">' +
      inner +
      "</table></div>"
    );
  }

  function wrapKvTable(rowsHtml) {
    return wrapTable(
      "<thead><tr>" +
        th("Field") +
        th("Detail") +
        "</tr></thead><tbody>" +
        rowsHtml +
        "</tbody>",
      "hiw-table--kv"
    );
  }

  function wrapStatusTable(rowsHtml) {
    return wrapTable(
      "<thead><tr>" +
        th("Field") +
        th("Detail") +
        th("Status") +
        "</tr></thead><tbody>" +
        rowsHtml +
        "</tbody>",
      "hiw-table--kv hiw-table--status"
    );
  }

  /** Subsection card: title + body inside one bordered wrapper */
  function snapSub(title, bodyHtml, opts) {
    opts = opts || {};
    var head = "";
    if (title) {
      head = opts.navHtml
        ? sectionHeadHtml(title, opts.navHtml, "hiw-snap-sub__title")
        : '<h3 class="hiw-snap-sub__title">' + title + "</h3>";
    }
    return (
      '<section class="hiw-snap-sub">' +
      head +
      '<div class="hiw-snap-sub__body">' +
      (bodyHtml || "") +
      "</div></section>"
    );
  }

  function orgNode(name, label, meta, modifier) {
    return (
      '<div class="hiw-org-level">' +
      '<div class="hiw-org-node' +
      (modifier ? " hiw-org-node--" + modifier : "") +
      '">' +
      (label
        ? '<div class="hiw-org-node__label">' + esc(toProperCase(label)) + "</div>"
        : "") +
      '<div class="hiw-org-node__name">' +
      esc(name || "—") +
      "</div>" +
      (meta ? '<div class="hiw-org-node__meta">' + esc(meta) + "</div>" : "") +
      "</div></div>"
    );
  }

  function orgConnector(label) {
    return (
      '<div class="hiw-org-connector" aria-hidden="true">' +
      '<span class="hiw-org-connector__line"></span>' +
      '<span class="hiw-org-connector__label">' +
      esc(toProperCase(label || "")) +
      "</span>" +
      '<span class="hiw-org-connector__line"></span>' +
      "</div>"
    );
  }

  function orgChart(innerHtml, ariaLabel) {
    return (
      '<div class="hiw-org-chart" aria-label="' +
      esc(ariaLabel || "Organization structure") +
      '">' +
      (innerHtml || "") +
      "</div>"
    );
  }

  /** Franchise-style node (kept name for call sites) */
  function chainNode(name, role, meta, modifier) {
    return orgNode(name, role, meta, modifier);
  }

  function chainEdge(label) {
    return orgConnector(label);
  }

  function renderOverview(hotel, ownership) {
    var ctrl = getControl(ownership);
    var econ = ctrl.economic_owner_or_group || {};
    var propco = ctrl.legal_property_owner_propco || {};
    var op = (ctrl.operator && ctrl.operator.name) || hotel.managementCompany || "—";
    var brand =
      (ownership && ownership.hotel && ownership.hotel.brand_display) ||
      hotel.brand ||
      hotel.affiliation ||
      "—";
    var rich = isRichCase(ownership);
    var ownerKnown = econ.known === true && econ.name;
    var gaps = (ownership && ownership.report && ownership.report.research_gaps) || [];
    var history = (ownership && ownership.report && ownership.report.property_history) || [];
    var orgName =
      (state.org && state.org.group && state.org.group.display_name) ||
      (ownership && ownership.organization && ownership.organization.display_name) ||
      (ownerKnown ? econ.name : null);

    var signals;
    var exec = (ownership && ownership.report && ownership.report.executive_summary) || {};
    var whatWeKnow = exec.what_we_know || [];
    var whatUncertain = exec.what_remains_uncertain || [];
    var whyMatters = exec.why_this_hotel_matters || "";

    // Data-driven signals for ALL hotels (no hotel-identity presentation fork).
    if (whatWeKnow.length || (exec.text && whatUncertain.length)) {
      signals =
        (whatWeKnow.length
          ? whatWeKnow
              .slice(0, 4)
              .map(function (line, i) {
                return insightRow("Established finding", esc(line) + citeBtn(Math.min(i + 1, 3)), "High");
              })
              .join("")
          : insightRow("Ownership intelligence", esc(exec.text || "") + citeBtn(1), "High")) +
        (whyMatters
          ? insightRow("Why this hotel matters", esc(whyMatters), "Evidence-supported")
          : "") +
        (whatUncertain.length
          ? insightRow(
              "Still open",
              esc(whatUncertain.slice(0, 3).join(" · ")),
              "Research gap"
            )
          : "");
    } else if (rich) {
      signals =
        insightRow(
          "Owner-operator control",
          (econ.name || "Economic owner") +
            " both owns and operates this hotel — outreach is an owner conversation, not a third-party management pitch." +
            citeBtn(1),
          "High"
        ) +
        insightRow(
          "Property holding structure",
          "The asset is held through a dedicated property company" +
            (propco.name ? " (" + esc(propco.name.split("(")[0].trim()) + ")" : "") +
            ", controlled by the listed economic owner." +
            citeBtn(1),
          "High"
        ) +
        insightRow(
          "Brand chronology",
          "Current brand is " +
            esc(brand) +
            ". Former and announced brand identities are tracked separately — announced is not current." +
            citeBtn(3),
          "Evidence-supported"
        ) +
        insightRow(
          "Adjacent-asset distinction",
          "Do not confuse this hotel with adjacent similarly named assets under different ownership.",
          "High"
        );
    } else {
      signals =
        insightRow(
          "Operator known",
          esc(op) + " is supported as operator / management company." + citeBtn(1),
          "High"
        ) +
        insightRow(
          "Ownership unresolved",
          "Economic owner and property company are not yet verified. Operator evidence is not promoted to ownership.",
          "Research gap"
        );
    }

    var timelinePreview =
      Array.isArray(history) && history.length
        ? renderPropertyHistoryFeed(history, { limit: 4, compact: true }) +
          '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--ghost" data-hiw-tab="ownership">Full property history →</button></div>'
        : '<p class="hiw-empty">No chronology promoted yet.</p>';

    var gapPreview =
      gaps.length || rich
        ? renderCompactResearchGaps(
            "Open research gaps",
            (gaps.length
              ? gaps
              : [
                  {
                    what: "Natural-person beneficial ownership",
                    why: "Matters for ultimate control diligence",
                    state: "Not independently verified",
                  },
                  {
                    what: "Legal / franchise signing authority",
                    why: "Required before treating any individual as a qualified signatory",
                    state: "Title only — authority not established",
                  },
                ]
            ).slice(0, 3),
            null
          ) +
          '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--ghost" data-hiw-tab="ownership">All research gaps →</button></div>'
        : "";

    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">' +
      esc(hotel.name || "Hotel") +
      "</h2>" +
      '<p class="hiw-report__subtitle">Orientation — what matters, then drill into Ownership, Organization, and Evidence</p>' +
      '<section class="hiw-section"><h3 class="hiw-section__h">Hotel Snapshot</h3>' +
      '<div class="hiw-snap-grid">' +
      '<div class="hiw-snap"><span class="hiw-snap__l">Location</span><span class="hiw-snap__v">' +
      esc(
        [hotel.city, hotel.market && hotel.market !== hotel.city ? hotel.market : null, hotel.country]
          .filter(Boolean)
          .join(" · ") || "—"
      ) +
      "</span></div>" +
      '<div class="hiw-snap"><span class="hiw-snap__l">Rooms</span><span class="hiw-snap__v">' +
      esc(hotel.rooms != null ? hotel.rooms : "—") +
      "</span></div>" +
      '<div class="hiw-snap"><span class="hiw-snap__l">Chain scale</span><span class="hiw-snap__v">' +
      esc(hotel.chainScale || hotel.chain_scale || "—") +
      "</span></div>" +
      '<div class="hiw-snap"><span class="hiw-snap__l">Status</span><span class="hiw-snap__v">' +
      esc(hotel.status || "—") +
      "</span></div></div></section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Ownership · Operator · Brand</h3>' +
      wrapTable(
        "<tbody>" +
          row("Economic owner", ownerKnown ? econ.name : "Not yet verified") +
          row("Property company", propco.name || "Not yet verified") +
          row("Operator", op) +
          row("Current brand", brand) +
          "</tbody>"
      ) +
      '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--primary" data-hiw-tab="ownership">Open Ownership Intelligence →</button></div>' +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Strategic Signals</h3>' +
      '<div class="hiw-insights">' +
      signals +
      "</div></section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Property Timeline Preview</h3>' +
      timelinePreview +
      "</section>" +
      (orgName
        ? '<section class="hiw-section"><h3 class="hiw-section__h">Organization Preview</h3>' +
          '<div class="hiw-prose"><p><strong>' +
          esc(orgName) +
          "</strong> is the related organization for this hotel. Review owned versus managed relationships separately in Organization &amp; Portfolio.</p></div>" +
          '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--ghost" data-hiw-tab="organization">Organization &amp; Portfolio →</button></div></section>'
        : "") +
      '<section class="hiw-section"><h3 class="hiw-section__h">Market · Demand Preview</h3>' +
      '<div class="hiw-prose"><p>Market geography and demand / access intelligence remain available in-tab from Radar context — Ownership does not replace location diligence.</p></div>' +
      '<div class="hiw-actions">' +
      '<button type="button" class="hiw-btn hiw-btn--ghost" data-hiw-tab="market">Market Intelligence →</button>' +
      '<button type="button" class="hiw-btn hiw-btn--ghost" data-hiw-tab="demand">Demand &amp; Access →</button>' +
      "</div></section>" +
      (gapPreview
        ? '<section class="hiw-section"><h3 class="hiw-section__h">Research Gaps Preview</h3>' +
          gapPreview +
          "</section>"
        : "") +
      "</article>"
    );
  }

  function ownershipMetaStrip(hotel, ownership, ctrl, opName) {
    ctrl = ctrl || {};
    var econ = ctrl.economic_owner_or_group || {};
    var propco = ctrl.legal_property_owner_propco || {};
    var cites = state.citations || [];
    function shortOrg(name) {
      if (!name || name === "—") return "";
      return String(name).split("(")[0].split(",")[0].trim();
    }
    function fact(value, label) {
      return (
        '<div class="hex-fact"><div class="label">' +
        esc(toProperCase(label)) +
        '</div><div class="value">' +
        esc(value || "—") +
        "</div></div>"
      );
    }
    var facts =
      fact(
        econ.known && econ.name ? shortOrg(econ.name) : "Not Yet Verified",
        "Economic owner"
      ) +
      fact(shortOrg(propco.name) || "—", "Property company") +
      fact(shortOrg(opName) || "—", "Operator") +
      fact(
        econ.known ? "Owner Context Established" : "Owner Unresolved",
        "Control status"
      ) +
      fact(cites.length ? cites.length + " sources" : "Limited", "Source coverage");
    return (
      '<div class="hex-facts hex-facts--5 hex-facts--own" aria-label="Ownership snapshot">' +
      facts +
      "</div>"
    );
  }

  function renderCompactDecisionMakers(people) {
    var list = Array.isArray(people) ? people : [];
    var contactsLink = navLinkHtml("View Contacts", { "data-hiw-tab": "contacts" });
    if (!list.length) {
      return (
        '<section class="hiw-section" data-ocn-section="decision-makers">' +
        sectionHeadHtml("Relevant Decision-Makers", contactsLink) +
        '<p class="hiw-prose hiw-prose--quiet">No owner-side decision-makers verified yet. Use View Contacts for property-level channels.</p>' +
        "</section>"
      );
    }
    return (
      '<section class="hiw-section" data-ocn-section="decision-makers">' +
      sectionHeadHtml("Relevant Decision-Makers", contactsLink) +
      '<p class="hiw-prose hiw-prose--quiet">Owner-side contacts only — full cards live in Contact Intelligence.</p>' +
      '<ul class="hiw-list hiw-list--compact">' +
      list
        .slice(0, 6)
        .map(function (p) {
          return (
            "<li><strong>" +
            esc(p.display_name || p.name) +
            "</strong> — " +
            esc(p.title || p.role || "Role pending") +
            (p.preview_state
              ? ' <span class="hiw-chip hiw-chip--proposed">' +
                esc(friendlyStatus(p.preview_state)) +
                "</span>"
              : "") +
            "</li>"
          );
        })
        .join("") +
      "</ul>" +
      "</section>"
    );
  }

  function ownershipSourcesNavLink(focusN) {
    var attrs = { "data-hiw-tab": "sources", "data-hiw-source-filter": "ownership" };
    if (focusN != null) attrs["data-hiw-source-focus"] = String(focusN);
    return navLinkHtml("View Sources", attrs);
  }

  function renderOwnershipNetworkSection() {
    return (
      '<section class="hiw-section hiw-section--ocn" data-ocn-section="network">' +
      sectionHeadHtml(
        "Hotel Stakeholder &amp; Deal Relationship Network",
        ownershipSourcesNavLink(1)
      ) +
      '<p class="hiw-prose hiw-prose--quiet">Hotel-centered default view. Ownership above · Operations &amp; Brand below. Explore people and related hotels progressively — operator/brand are never drawn as ownership.</p>' +
      '<div class="ocn-host" data-ocn-host="1" aria-label="Hotel stakeholder and deal relationship network"></div>' +
      "</section>"
    );
  }

  function mountOwnershipControlNetwork() {
    var host = document.querySelector("[data-ocn-host]");
    if (!host) return;
    if (!window.HeOwnershipControlNetwork || typeof window.HeOwnershipControlNetwork.mount !== "function") {
      host.innerHTML =
        '<p class="hiw-empty">Ownership network module unavailable.</p>';
      return;
    }
    if (!state.ownership) {
      host.innerHTML = heWaveCompact("Loading ownership network…");
      return;
    }
    window.HeOwnershipControlNetwork.mount(host, state.hotel, state.ownership, {
      includePortfolioPreview: false,
    });
  }

  function formatNewsDate(iso) {
    if (!iso) return "";
    var d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "";
    try {
      return d.toLocaleDateString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
      });
    } catch (e) {
      return String(iso).slice(0, 10);
    }
  }

  function renderHotelNewsSection(payload, opts) {
    opts = opts || {};
    var articles = (payload && payload.articles) || [];
    if (!articles.length) return "";
    var list = articles
      .map(function (a) {
        var meta = [a.publisher, formatNewsDate(a.publishedAt), a.category]
          .filter(Boolean)
          .join(" · ");
        return (
          '<article class="hiw-news-card">' +
          '<a class="hiw-news-card__title" href="' +
          esc(a.url || "#") +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(a.headline || "Article") +
          "</a>" +
          (meta ? '<p class="hiw-news-card__meta">' + esc(meta) + "</p>" : "") +
          (a.relevanceSummary
            ? '<p class="hiw-news-card__relevance">' + esc(a.relevanceSummary) + "</p>"
            : "") +
          "</article>"
        );
      })
      .join("");
    var more =
      payload.hasMore || opts.viewAll
        ? '<div class="hiw-section__nav hiw-section__nav--below">' +
          navLinkHtml("View all articles", { "data-hiw-news-view-all": "1" }) +
          "</div>"
        : "";
    var sourcesLink = navLinkHtml("View Sources", {
      "data-hiw-tab": "sources",
      "data-hiw-source-filter": "news",
    });
    return (
      '<section class="hiw-section hiw-section--hotel-news" data-hiw-hotel-news="1">' +
      sectionHeadHtml("Hotel News &amp; Articles", sourcesLink) +
      '<p class="hiw-prose hiw-prose--quiet">Property-specific coverage from Dealality Market Alerts.</p>' +
      '<div class="hiw-news-list">' +
      list +
      "</div>" +
      more +
      "</section>"
    );
  }

  function buildHotelNewsQuery(opts) {
    opts = opts || {};
    var params = new URLSearchParams();
    if (state.hotel && state.hotel.name) params.set("name", state.hotel.name);
    if (state.hotel && state.hotel.brand) params.set("brand", state.hotel.brand);
    if (state.hotel && state.hotel.managementCompany)
      params.set("operator", state.hotel.managementCompany);
    if (state.hotel && state.hotel.city) params.set("city", state.hotel.city);
    var econ =
      (state.ownership &&
        state.ownership.report &&
        state.ownership.report.ownership_and_control &&
        state.ownership.report.ownership_and_control.economic_owner_or_group &&
        state.ownership.report.ownership_and_control.economic_owner_or_group.name) ||
      "";
    if (econ) params.set("economicOwner", econ);
    params.set("limit", opts.viewAll ? "25" : "5");
    if (opts.viewAll) params.set("view", "all");
    return params;
  }

  function fetchHotelNewsArticles(opts, fetchOpts) {
    opts = opts || {};
    fetchOpts = fetchOpts || {};
    var id = state.hotel && recordId(state.hotel);
    if (!id) {
      return Promise.resolve(null);
    }
    var url =
      "/api/hotel-explorer/hotels/" +
      encodeURIComponent(id) +
      "/news-articles?" +
      buildHotelNewsQuery(opts).toString();
    var init = { headers: { "ngrok-skip-browser-warning": "true" } };
    if (fetchOpts.signal) init.signal = fetchOpts.signal;
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () {
        return null;
      });
    });
  }

  function paintHotelNewsHost(host, result, opts) {
    if (!host || !host.isConnected) return;
    var st = result && result.state;
    if (st === "LOADING") {
      host.hidden = false;
      host.innerHTML =
        '<section class="hiw-section hiw-section--hotel-news he-async-loading" data-hiw-hotel-news-loading="1">' +
        '<h3 class="hiw-section__h">Hotel News &amp; Articles</h3>' +
        heWaveCompact("Loading hotel news…", "Market Alerts") +
        "</section>";
      return;
    }
    if (st === "ERROR") {
      host.hidden = false;
      host.innerHTML =
        '<section class="hiw-section hiw-section--hotel-news" data-hiw-hotel-news-error="1">' +
        '<h3 class="hiw-section__h">Hotel News &amp; Articles</h3>' +
        heAsyncError(
          result.message || "Hotel news is temporarily unavailable.",
          "hotel-news"
        ) +
        "</section>";
      return;
    }
    if (st === "STALE" && result.payload && result.payload.visible === true) {
      applyMarketAlertsEvidencePayload(result.payload);
      host.hidden = false;
      host.innerHTML =
        renderHotelNewsSection(result.payload, opts) +
        '<p class="hiw-prose hiw-prose--quiet he-async-stale" data-he-async-stale="1">Showing last available coverage while refresh continues.</p>';
      return;
    }
    var payload = result && result.payload;
    applyMarketAlertsEvidencePayload(
      payload && payload.visible === true ? payload : { articles: [], evidenceCitations: [] }
    );
    if (!payload || payload.visible !== true || !(payload.articles || []).length) {
      host.hidden = true;
      host.innerHTML = "";
      return;
    }
    host.hidden = false;
    host.innerHTML = renderHotelNewsSection(payload, opts);
  }

  function mountHotelNewsArticles(opts) {
    opts = opts || {};
    var host = document.querySelector("[data-hiw-hotel-news-host]");
    if (!state.hotel) return;
    var id = recordId(state.hotel);
    if (!id) {
      if (host) {
        host.hidden = true;
        host.innerHTML = "";
      }
      state.marketAlertsCitations = [];
      if (newsAsync) newsAsync.cancel();
      return;
    }

    function applyResult(result) {
      paintHotelNewsHost(host, result, opts);
      if (state.tab === "sources" && result && result.state !== "LOADING") {
        var main = document.getElementById("hiwMain");
        if (main) main.innerHTML = renderSourcesTab();
        var rail = document.getElementById("hiwRail");
        if (rail) rail.innerHTML = renderRail(state.hotel, state.ownership);
      }
    }

    if (!newsAsync) {
      if (host) {
        host.hidden = false;
        host.innerHTML =
          '<section class="hiw-section">' + heWaveCompact("Loading hotel news…") + "</section>";
      }
      fetchHotelNewsArticles(opts)
        .then(function (payload) {
          applyResult({
            state:
              payload && payload.visible === true && (payload.articles || []).length
                ? "READY"
                : "EMPTY",
            payload: payload,
          });
        })
        .catch(function () {
          applyResult({ state: "ERROR", message: "Hotel news is temporarily unavailable." });
        });
      return;
    }

    newsAsync.load(
      id,
      function (ticket) {
        return fetchHotelNewsArticles(opts, { signal: ticket && ticket.signal });
      },
      {
        force: opts.force === true,
        isEmpty: function (payload) {
          return !(payload && payload.visible === true && (payload.articles || []).length);
        },
        onLoading: function () {
          applyResult({ state: "LOADING" });
        },
        onResult: applyResult,
      }
    );
  }

  /** Sources tab: refresh Market Alerts evidence via same GET-only news path (no writes). */
  function mountMarketAlertsEvidenceForSources() {
    if (!state.hotel || !recordId(state.hotel)) return;
    var id = recordId(state.hotel);

    function finish(result) {
      if (result && result.payload) {
        applyMarketAlertsEvidencePayload(
          result.payload.visible === true
            ? result.payload
            : { articles: [], evidenceCitations: [] }
        );
      } else if (result && result.state === "ERROR") {
        /* keep prior citations */
      } else {
        applyMarketAlertsEvidencePayload({ articles: [], evidenceCitations: [] });
      }
      if (state.tab !== "sources") return;
      var main = document.getElementById("hiwMain");
      if (main) {
        main.innerHTML = renderSourcesTab();
        if (result && result.state === "LOADING") {
          var slot = main.querySelector("[data-hiw-sources-ma-slot]");
          if (slot) {
            slot.hidden = false;
            slot.innerHTML = heWaveCompact("Updating Market Alerts sources…");
          }
        }
      }
      var rail = document.getElementById("hiwRail");
      if (rail) rail.innerHTML = renderRail(state.hotel, state.ownership);
    }

    if (!sourcesMaAsync) {
      fetchHotelNewsArticles({ viewAll: true })
        .then(function (payload) {
          finish({ state: "READY", payload: payload });
        })
        .catch(function () {
          finish({ state: "ERROR" });
        });
      return;
    }

    sourcesMaAsync.load(
      id,
      function (ticket) {
        return fetchHotelNewsArticles({ viewAll: true }, { signal: ticket && ticket.signal });
      },
      {
        onLoading: function () {
          finish({ state: "LOADING", payload: sourcesMaAsync.getCached(id) });
        },
        onResult: finish,
      }
    );
  }

  function renderOwnershipReport(hotel, ownership) {
    if (!ownership) {
      return (
        '<article class="hiw-report"><h2 class="hiw-report__title">' +
        esc(hotel.name || "Hotel") +
        "</h2>" +
        '<p class="hiw-empty">Loading ownership intelligence…</p></article>'
      );
    }

    var inCohort = ownership && ownership.in_cohort;
    var ctrl =
      (ownership && ownership.report && ownership.report.ownership_and_control) || {};
    var org = (ownership && (ownership.organization || ownership.ownership_group)) || {};
    var opName = (ctrl.operator && ctrl.operator.name) || hotel.managementCompany || "—";
    var brand =
      (ownership && ownership.hotel && ownership.hotel.brand_display) ||
      hotel.brand ||
      "—";
    var brandStatus =
      (ownership && ownership.hotel && ownership.hotel.brand_display_status) || "";
    var contested = String(brandStatus) === "CONTESTED";
    var rich =
      ownership.intelligence_case === "A" ||
      (ownership.deep_research && ownership.deep_research.ownership_chain);
    var gaps = (ownership.report && ownership.report.research_gaps) ||
      (ownership.report &&
        ownership.report.research_status &&
        ownership.report.research_status.gaps) ||
      [];
    var people =
      (ownership.report &&
        ownership.report.decision_authority &&
        ownership.report.decision_authority.people) ||
      [];
    var chain = (ownership.report && ownership.report.ownership_chain) || [];
    var pursuit = (ownership.report && ownership.report.commercial_pursuit) || {};
    var brandChrono = (ownership.report && ownership.report.brand_chronology) || [];
    var history = (ownership.report && ownership.report.property_history) || [];
    if (!history.length && ownership.deep_research && Array.isArray(ownership.deep_research.property_history)) {
      history = ownership.deep_research.property_history;
    }
    if (!history.length && brandChrono.length) {
      history = brandChrono.map(function (b) {
        return {
          date: b.date || b.label || "—",
          event: b.event || b.label || b.brand || "Brand / identity event",
          status: b.status || "HIGH",
          confidence: "HIGH",
        };
      });
    }

    var title =
      esc(hotel.name || "Hotel") +
      '<div class="hiw-report__subtitle">Ownership &amp; Control Network</div>';

    var airtableGraphPartial =
      ownership.data_status === "AIRTABLE_GRAPH_PARTIAL" ||
      (ownership.airtable_graph && ownership.airtable_graph.relationshipCount > 0);

    if (!inCohort && !airtableGraphPartial) {
      return (
        '<article class="hiw-report"><h2 class="hiw-report__title">' +
        title +
        "</h2>" +
        '<section class="hiw-section"><h3 class="hiw-section__h">Executive Ownership Summary</h3>' +
        '<div class="hiw-prose"><p>Ownership Intelligence is not yet available for this hotel in the controlled demo cohort. Economic ownership is not verified. No fabricated owner is shown.</p></div>' +
        "</section></article>"
      );
    }

    if (!inCohort && airtableGraphPartial) {
      var graphCtrl = getControl(ownership) || ctrl || {};
      var graphExec =
        (ownership.report &&
          ownership.report.executive_summary &&
          ownership.report.executive_summary.text) ||
        "Airtable HI graph relationships are available for this hotel. Economic ownership remains unresolved unless a Validated Economic Owner relationship exists. Operator and brand are shown as separate operating relationships — not as ownership. No fabricated owner is shown.";
      var proposedMode =
        ownership.preview_mode === "PHASE3_PROPOSED" ||
        (ownership.report &&
          ownership.report.research_status &&
          ownership.report.research_status.preview_mode === "PHASE3_PROPOSED");
      var graphPeople =
        (ownership.report &&
          ownership.report.decision_authority &&
          ownership.report.decision_authority.people) ||
        [];
      var graphRelRows = renderRelationshipEdgesTable(ownership, state.relFilter);
      return (
        '<article class="hiw-report hiw-report--ownership" data-ocn-tab="1"><h2 class="hiw-report__title">' +
        title +
        "</h2>" +
        '<section class="hiw-section" data-ocn-section="executive"><h3 class="hiw-section__h">Executive Ownership Summary</h3>' +
        '<div class="hiw-prose"><p>' +
        esc(graphExec) +
        "</p>" +
        (proposedMode
          ? '<p class="hiw-chip hiw-chip--proposed">Phase 3 Proposed Preview — staged ownership overlay. Not Airtable-persisted. Not customer-approved.</p>'
          : "") +
        '<p class="hiw-muted">' +
        (proposedMode
          ? "Proposed entities are staged research overlays on live Airtable operator/brand edges. Medium-confidence sponsors are not shown as validated legal owners."
          : "Source: HI Organizations + HI Hotel–Org Relationships (live Airtable).") +
        "</p></div></section>" +
        renderOwnershipNetworkSection() +
        renderCompactDecisionMakers(graphPeople) +
        '<div data-ocn-section="relationships">' +
        graphRelRows +
        "</div>" +
        "</article>"
      );
    }

    /* ——— Executive summary (prose) ——— */
    var execObj = (ownership.report && ownership.report.executive_summary) || {};
    var exec;
    if (execObj.text) {
      exec =
        "<p>" +
        esc(execObj.text) +
        citeBtn(1) +
        "</p>" +
        (Array.isArray(execObj.what_remains_uncertain) && execObj.what_remains_uncertain.length
          ? "<p><strong>Still open:</strong> " +
            esc(execObj.what_remains_uncertain.slice(0, 4).join("; ")) +
            "</p>"
          : "");
    } else if (rich) {
      exec =
        "<p><strong>" +
        esc((ctrl.economic_owner_or_group && ctrl.economic_owner_or_group.name) || org.display_name || "Economic owner") +
        "</strong> is supported as economic owner" +
        (opName && opName !== "—" ? " with operator / management <strong>" + esc(opName) + "</strong>" : "") +
        "." +
        citeBtn(1) +
        " Current brand display: <strong>" +
        esc(brand) +
        "</strong>" +
        (contested ? " (contested — requires brand-history review)." : ".") +
        citeBtn(2) +
        "</p>";
    } else {
      exec =
        "<p>Dealality has verified that <strong>" +
        esc(opName) +
        "</strong> operates / manages <strong>" +
        esc(hotel.name) +
        "</strong>." +
        citeBtn(1) +
        " Economic ownership and the property-holding company have <strong>not</strong> been verified from product-safe evidence, so Dealality abstains rather than treating the operator as the owner.</p>" +
        (brand && brand !== "—"
          ? "<p>Current brand display: <strong>" +
            esc(brand) +
            "</strong>" +
            (contested ? " (contested affiliation — requires brand-history review)." : ".") +
            "</p>"
          : "");
    }

    /* ——— Key strategic insights ——— */
    var insights;
    if (Array.isArray(execObj.what_we_know) && execObj.what_we_know.length) {
      insights = execObj.what_we_know
        .slice(0, 5)
        .map(function (line, i) {
          return insightRow(
            i === 0 ? "Ownership intelligence" : "Established finding",
            esc(line) + citeBtn(Math.min(i + 1, 3)),
            "High"
          );
        })
        .join("");
      if (Array.isArray(execObj.what_remains_uncertain) && execObj.what_remains_uncertain.length) {
        insights += insightRow(
          "Still open",
          esc(execObj.what_remains_uncertain.slice(0, 3).join(" · ")),
          "Research gap"
        );
      }
    } else if (rich) {
      insights =
        insightRow(
          "Direct owner-operator\ncontrol",
          esc((ctrl.economic_owner_or_group && ctrl.economic_owner_or_group.name) || "Economic owner") +
            " is supported as both economic owner and operator." +
            citeBtn(1),
          "High"
        ) +
        insightRow(
          "Property\nstructure",
          "The asset is held through a dedicated property company" +
            (ctrl.legal_property_owner_propco && ctrl.legal_property_owner_propco.name
              ? " (" + esc(ctrl.legal_property_owner_propco.name) + ")"
              : "") +
            "." +
            citeBtn(1),
          "High"
        ) +
        insightRow(
          "Brand\ncontext",
          "Current brand display: " + esc(brand) + ". Former and announced identities are tracked separately.",
          "Evidence-supported"
        );
    } else {
      insights =
        insightRow(
          "Operating\nrelationship",
          esc(opName) + " is supported as operator / management company." + citeBtn(1),
          "High"
        ) +
        insightRow(
          "Ownership\ngap",
          "Economic owner and PropCo remain unverified — operator evidence is not promoted to ownership.",
          "Not verified"
        );
    }

    /* ——— Gaps (compact) ——— */
    var gapItems = normalizeResearchGaps(gaps, rich);
    var gapsHtml = renderCompactResearchGaps(
      "Research Gaps",
      gapItems,
      rich
        ? "Registry filing, deed, or audited ownership / signatory disclosure."
        : "Issuer filing, deed, or first-party ownership disclosure."
    );

    var pageTitle = "Ownership Intelligence";
    var pageLede =
      "Interactive ownership and control network for " +
      (hotel.name || "this hotel") +
      ".";
    var cohortRelRows = renderRelationshipEdgesTable(ownership, state.relFilter);

    var ownershipHtml =
      '<article class="hiw-report hiw-report--ownership" data-ocn-tab="1">' +
      '<h2 class="hiw-report__title">' +
      esc(pageTitle) +
      "</h2>" +
      '<p class="hiw-report__subtitle">' +
      esc(pageLede) +
      "</p>" +
      '<section class="hiw-section" id="hiw-exec" data-ocn-section="executive"><h3 class="hiw-section__h">Executive Ownership Summary</h3>' +
      '<div class="hiw-prose">' +
      exec +
      "</div></section>" +
      ownershipMetaStrip(hotel, ownership, ctrl, opName) +
      '<section class="hiw-section"><h3 class="hiw-section__h">Key Strategic Insights</h3>' +
      '<div class="hiw-insights">' +
      insights +
      "</div></section>" +
      renderOwnershipNetworkSection() +
      renderCompactDecisionMakers(people) +
      '<div data-ocn-section="relationships">' +
      cohortRelRows +
      "</div>" +
      (gapsHtml
        ? '<section class="hiw-section"><h3 class="hiw-section__h">Research Gaps</h3>' +
          gapsHtml +
          "</section>"
        : "") +
      "</article>";
    assertNoForeignHotelTokens(ownershipHtml, "renderOwnershipReport:" + recordId(hotel));
    return ownershipHtml;
  }

  function personProfessionalProfile(p) {
    if (!p || typeof p !== "object") {
      return { type: "", url: "", verified: false };
    }
    var nested = p.professional_profile && typeof p.professional_profile === "object" ? p.professional_profile : null;
    var profileUrl0 =
      Array.isArray(p.profileUrls) && p.profileUrls[0] && p.profileUrls[0].url
        ? p.profileUrls[0].url
        : "";
    var url = String(
      (nested && nested.url) ||
        p.professional_profile_url ||
        p.linkedinUrl ||
        profileUrl0 ||
        ""
    ).trim();
    var type = String(
      (nested && nested.type) || p.professional_profile_type || ""
    ).trim();
    var verified =
      nested && typeof nested.verified === "boolean"
        ? nested.verified
        : p.professional_profile_verified === true ||
          (Array.isArray(p.profileUrls) &&
            p.profileUrls.some(function (u) {
              return u && u.verified !== false;
            })) ||
          Boolean(p.linkedinUrl);
    // Only person-level LinkedIn /in/ profiles — never company pages or guesses
    var isPersonLinkedIn =
      /^https?:\/\/((www|[a-z]{2})\.)?linkedin\.com\/in\//i.test(url);
    if (!url || !verified || !isPersonLinkedIn) {
      return { type: "", url: "", verified: false };
    }
    return {
      type: type || "LINKEDIN",
      url: url,
      verified: true,
    };
  }

  function normalizeOrgKeyPeopleRows(people) {
    var rows = [];
    var seen = {};
    (people || []).forEach(function (raw) {
      if (!raw || typeof raw !== "object") return;
      var name = String(raw.display_name || raw.name || "").trim();
      if (!name) return;
      var key = name.toLowerCase();
      if (seen[key]) return;
      seen[key] = true;
      var relevance = String(
        raw.whyRelevant ||
          raw.strategic_relevance ||
          raw.relationship_to_hotel ||
          raw.recommendationRationale ||
          raw.verificationNotes ||
          ""
      ).trim();
      var commercial = String(
        raw.commercialRole ||
          raw.contactClassLabel ||
          raw.role_group ||
          raw.stakeholderClass ||
          raw.title ||
          ""
      ).trim();
      var propLabel = String(
        raw.propertyResponsibilityLabel ||
          (raw.propertyResponsibility === "CONFIRMED"
            ? "Confirmed property responsibility"
            : raw.propertyResponsibility === "INFERRED"
              ? "Inferred portfolio responsibility"
              : raw.propertyResponsibility === "UNKNOWN"
                ? "Unknown property responsibility"
                : "") ||
          ""
      ).trim();
      var verification = String(
        raw.verificationStatus ||
          raw.identityVerification ||
          raw.preview_state ||
          raw.confidence ||
          ""
      ).trim();
      if (/VERIFIED|HIGH/i.test(verification)) verification = "Verified identity";
      else if (/PROPOSED|STAGED|MEDIUM/i.test(verification)) verification = "Proposed";
      rows.push({
        name: name,
        title: String(raw.title || raw.role || "").trim() || "—",
        organization: String(raw.organization || raw.organizationName || "").trim() || "—",
        relevance: relevance || "Role verified; hotel-specific relevance not yet documented.",
        commercialRole: commercial || "Relevant contact",
        propertyResponsibilityLabel: propLabel || "Unknown property responsibility",
        verificationStatus: verification || "Proposed",
        confidence: raw.confidence,
        professional_profile: raw.professional_profile,
        professional_profile_url: raw.professional_profile_url || raw.linkedinUrl,
        professional_profile_verified: raw.professional_profile_verified,
        linkedinUrl: raw.linkedinUrl,
        profileUrls: raw.profileUrls,
        decision_authority_category:
          raw.decision_authority_category || raw.role_group || raw.contactClass || raw.category,
        category: raw.role_group || raw.contactClass || raw.category,
        strategic_relevance: relevance,
        relationship_to_hotel: relevance,
      });
    });
    return rows;
  }

  function collectOrgKeyPeople(ownership, orgPayload) {
    var bags = [];
    if (
      ownership &&
      ownership.report &&
      ownership.report.decision_authority &&
      ownership.report.decision_authority.people
    ) {
      bags = bags.concat(ownership.report.decision_authority.people);
    }
    if (ownership && ownership.report && ownership.report.decision_makers) {
      bags = bags.concat(ownership.report.decision_makers);
    }
    if (orgPayload && orgPayload.key_people) bags = bags.concat(orgPayload.key_people);
    if (
      ownership &&
      ownership.organization &&
      ownership.organization.key_people
    ) {
      bags = bags.concat(ownership.organization.key_people);
    }
    return normalizeOrgKeyPeopleRows(bags);
  }

  function resolveStrategicContext(hotel, ownership, orgPayload) {
    var sc =
      (ownership && ownership.report && ownership.report.strategic_context) ||
      (orgPayload && orgPayload.strategic_context) ||
      (ownership && ownership.organization && ownership.organization.strategic_context) ||
      null;
    if (sc && Array.isArray(sc.sections) && sc.sections.length) return sc;
    var ctrl =
      (ownership &&
        ownership.report &&
        ownership.report.ownership_and_control) ||
      {};
    var propco = ctrl.legal_property_owner_propco || ctrl.property_company || {};
    var econ = ctrl.economic_owner_or_group || ctrl.economic_owner || {};
    var operator = ctrl.operator || {};
    var brand =
      (ownership && ownership.hotel && ownership.hotel.brand_display) ||
      (hotel && (hotel.brand_display || hotel.brand || hotel.affiliation)) ||
      "";
    var hotelName = (hotel && hotel.name) || "This hotel";
    var city = (hotel && hotel.city) || "";
    var roomsRaw =
      (hotel && (hotel.rooms || hotel.Rooms || hotel.room_count)) ||
      (hotel &&
        hotel.guestRoomsMeetingSpace &&
        hotel.guestRoomsMeetingSpace.guestRooms &&
        hotel.guestRoomsMeetingSpace.guestRooms.totalGuestrooms &&
        hotel.guestRoomsMeetingSpace.guestRooms.totalGuestrooms.value) ||
      null;
    var rooms = roomsRaw != null && roomsRaw !== "" ? String(roomsRaw) : "";
    var uboUnresolved =
      !ctrl.ubo_group ||
      ctrl.ubo_group.known === false ||
      /UNRESOLVED/i.test(String((ctrl.ubo_group && ctrl.ubo_group.status) || ""));
    var owned =
      (ownership &&
        ownership.organization &&
        ownership.organization.owned_controlled_hotels) ||
      (orgPayload && orgPayload.owned_controlled_hotels) ||
      [];
    var portfolioCount = Array.isArray(owned)
      ? owned.filter(function (h) {
          var n = String((h && (h.hotel_name || h.name)) || "").trim();
          return n && n.toLowerCase() !== String(hotelName).toLowerCase();
        }).length
      : 0;
    var scaleBits = [];
    if (rooms) scaleBits.push(rooms + "-room");
    scaleBits.push(brand || "branded");
    return {
      version: "he-org-portfolio-key-people-v1-client",
      generated: true,
      sections: [
        {
          id: "asset_market",
          title: "Asset & Market Position",
          body:
            hotelName +
            " is a " +
            scaleBits.join(" ") +
            " lodging asset" +
            (city ? " in " + city : "") +
            ". " +
            (operator.name
              ? "Day-to-day operations are associated with " +
                operator.name +
                (brand ? " under the " + brand + " brand. " : ". ")
              : "") +
            "Scale, brand distribution, and management structure frame outreach; they do not by themselves establish ownership economics.",
        },
        {
          id: "ownership_portfolio",
          title: "Ownership & Portfolio Context",
          body:
            (propco.name
              ? "Legal property ownership is associated with " + propco.name + ". "
              : "Legal property company remains incompletely verified. ") +
            (econ.name
              ? "Economic sponsor context points to " +
                econ.name +
                (portfolioCount
                  ? ", with " +
                    portfolioCount +
                    " additional related hotel listing(s) in the selected portfolio scope. "
                  : ". ")
              : "Economic sponsor is not yet verified. ") +
            (uboUnresolved
              ? "Ultimate beneficial ownership remains unresolved — seniority alone is not treated as UBO proof."
              : ""),
        },
        {
          id: "strengths",
          title: "Commercial Strengths",
          body:
            (brand
              ? "Established " + brand + " affiliation supports distribution and commercial recognition. "
              : "") +
            (operator.name
              ? "Management relationship with " +
                operator.name +
                " separates operating execution from ownership decision rights. "
              : "") +
            (rooms
              ? "Published inventory scale (" +
                rooms +
                " rooms) is material for meeting, group, and brand conversations."
              : "Verified operating relationships provide a commercial baseline while ownership research continues."),
        },
        {
          id: "strategic",
          title: "Brand Development & Investment Considerations",
          body:
            "Brand positioning and management agreements should be discussed with the economic sponsor and operator as separate counterparties. Current lender and capital structure remain priority diligence items when not publicly verified. No public evidence in this packet authorizes claims that the owner is actively selling, rebranding, refinancing, or changing management.",
        },
        {
          id: "follow_ups",
          title: "Priority Follow-Ups",
          body:
            (uboUnresolved
              ? "Resolve ultimate beneficial ownership if it becomes material to counterparty mapping. "
              : "") +
            "Confirm any property-assigned asset manager without inferring from portfolio titles alone. Verify personal professional channels for priority owner-side people via Contact Intelligence (Surfe on explicit click only).",
        },
      ],
    };
  }

  function renderStrategicContext(hotel, ownership, orgPayload) {
    var sc = resolveStrategicContext(hotel, ownership, orgPayload);
    var preferred = ["asset_market", "strengths", "strategic", "follow_ups", "ownership_portfolio"];
    var byId = {};
    (sc.sections || []).forEach(function (s) {
      if (s && s.id) byId[s.id] = s;
    });
    var ordered = [];
    preferred.forEach(function (id) {
      if (byId[id]) ordered.push(byId[id]);
    });
    (sc.sections || []).forEach(function (s) {
      if (!s || !s.id) return;
      if (preferred.indexOf(s.id) >= 0) return;
      ordered.push(s);
    });
    // Presentation: Asset & Market · Commercial Strengths · Strategic Considerations · Priority Follow-Ups
    // Keep Ownership & Portfolio as fifth when present (founder required content).
    var html =
      '<div class="hiw-strategic" data-hiw-strategic-context="1">' +
      '<p class="hiw-muted hiw-strategic__lede">Executive assessment synthesized from verified and appropriately qualified proposed intelligence. Evidence lives in Sources &amp; Evidence.</p>';
    ordered.forEach(function (s) {
      var title = s.title || "";
      if (s.id === "strategic") title = "Strategic Considerations";
      if (s.id === "follow_ups") title = "Priority Follow-Ups";
      html +=
        '<div class="hiw-strategic__block">' +
        "<h4>" +
        esc(title) +
        "</h4>" +
        '<p class="hiw-prose">' +
        esc(s.body || "") +
        "</p></div>";
    });
    html += "</div>";
    return html;
  }

  function renderPeopleProfileCell(p) {
    var profile = personProfessionalProfile(p);
    if (!profile.verified || !profile.url) {
      return '<td class="hiw-td-profile"><span class="hiw-profile-empty">—</span></td>';
    }
    var name = String(p.name || "person");
    return (
      '<td class="hiw-td-profile">' +
      '<a class="hiw-profile-link" href="' +
      esc(profile.url) +
      '" target="_blank" rel="noopener noreferrer" aria-label="Open ' +
      esc(name) +
      ' LinkedIn profile">View LinkedIn</a></td>'
    );
  }

  function renderKeyPeopleNameCell(p) {
    return (
      '<td><a class="hiw-person-link" href="#contacts" data-hiw-tab="contacts" data-hiw-person="' +
      esc(p.name) +
      '">' +
      esc(p.name) +
      "</a></td>"
    );
  }

  function renderPeopleGrouped(people) {
    var rows = normalizeOrgKeyPeopleRows(people);
    if (!rows.length) {
      return '<div class="hiw-gap-box"><h4>Decision authority not yet verified</h4><p>No named individuals verified for this hotel.</p></div>';
    }
    var claimed = {};
    var html = "";
    PERSON_GROUP_ORDER.forEach(function (group, gi) {
      var members = [];
      rows.forEach(function (p, idx) {
        if (claimed[idx]) return;
        var cat = String(p.decision_authority_category || p.role_category || p.category || "");
        var title = String(p.title || "");
        var isLast = gi === PERSON_GROUP_ORDER.length - 1;
        if (isLast || group.match.test(cat) || group.match.test(title)) {
          claimed[idx] = true;
          members.push(p);
        }
      });
      if (!members.length) return;
      html += snapSub(
        group.label,
        wrapTable(
          "<thead><tr>" +
            th("Name") +
            th("Title") +
            th("Organization") +
            th("Relevance") +
            th("Commercial role") +
            th("Property responsibility") +
            th("Profile") +
            th("Verification") +
            "</tr></thead><tbody>" +
            members
              .map(function (p) {
                return (
                  "<tr>" +
                  renderKeyPeopleNameCell(p) +
                  "<td>" +
                  esc(p.title) +
                  "</td><td>" +
                  esc(p.organization) +
                  "</td><td>" +
                  esc((p.relevance || "").slice(0, 160)) +
                  "</td><td>" +
                  esc(p.commercialRole) +
                  "</td><td>" +
                  esc(p.propertyResponsibilityLabel) +
                  "</td>" +
                  renderPeopleProfileCell(p) +
                  "<td>" +
                  esc(p.verificationStatus) +
                  "</td></tr>"
                );
              })
              .join("") +
            "</tbody>",
          "hiw-table--people hiw-table--key-people"
        )
      );
    });
    return html || '<p class="hiw-empty">No people verified.</p>';
  }

  function finding(title, bodyHtml, status) {
    return insightRow(title, bodyHtml, status);
  }

  function renderCompactResearchGaps(heading, items, resolverHint) {
    var list = (items || [])
      .map(function (g) {
        if (typeof g === "string") {
          return (
            '<li class="hiw-gap-item"><div class="hiw-gap-item__what">' +
            esc(g) +
            "</div></li>"
          );
        }
        return (
          '<li class="hiw-gap-item">' +
          '<div class="hiw-gap-item__what"><strong>What is unknown</strong> — ' +
          esc(g.what || g.text || g.gap || "") +
          "</div>" +
          (g.why
            ? '<div class="hiw-gap-item__why"><strong>Why it matters</strong> — ' +
              esc(g.why) +
              "</div>"
            : "") +
          (g.state
            ? '<div class="hiw-gap-item__state"><strong>Evidence state</strong> — ' +
              esc(g.state) +
              "</div>"
            : "") +
          "</li>"
        );
      })
      .join("");
    return (
      '<div class="hiw-research-gaps">' +
      "<h4>" +
      esc(heading || "Research Gaps") +
      "</h4>" +
      "<ul>" +
      list +
      "</ul>" +
      (resolverHint
        ? '<p class="hiw-research-gaps__hint">' +
          esc(resolverHint) +
          "</p>"
        : "") +
      '<p class="hiw-research-gaps__dossier">' +
      '<button type="button" class="hiw-btn hiw-btn--ghost hiw-btn--sm" data-hex-deep-research>Investigated in Full Hotel Intelligence Investigation</button> ' +
      '<button type="button" class="hiw-btn hiw-btn--ghost hiw-btn--sm" disabled title="Future capability — research run not in Packet 2.6B">Research this question</button>' +
      "</p>" +
      "</div>"
    );
  }

  function rowStatus(label, value, status) {
    return (
      "<tr><td>" +
      esc(toProperCase(label)) +
      "</td><td>" +
      esc(value) +
      '</td><td><span class="hiw-status hiw-status--' +
      esc(statusClass(status)) +
      '">' +
      esc(friendlyStatus(status === "gap" ? "NOT_YET_VERIFIED" : status)) +
      "</span></td></tr>"
    );
  }

  function sourceTableRow(s, n, opts) {
    opts = opts || {};
    var dates = normalizeCitationDates(s);
    var pub = formatSourceDate(dates.publication_date);
    var verified = formatSourceDate(dates.verified_at || dates.accessed_at);
    var claim = s.claim || s.proves || s.supports || "—";
    if (String(claim).length > 120) claim = String(claim).slice(0, 117) + "…";
    var catLabel =
      (SOURCE_CATEGORY_DEFS.find(function (c) {
        return c.id === citationCategory(s);
      }) || {}).label || "Source";
    var focusClass =
      opts.focusN != null && Number(opts.focusN) === Number(n) ? " is-source-focus" : "";
    var openLink = s.url
      ? '<a class="hiw-linkish" href="' +
        esc(s.url) +
        '" target="_blank" rel="noopener noreferrer">Open</a>'
      : "—";
    return (
      '<tr class="hiw-source-row' +
      focusClass +
      '" data-hiw-source-row="' +
      n +
      '" id="hiw-source-' +
      n +
      '">' +
      "<td>" +
      n +
      "</td>" +
      "<td>" +
      esc(s.title || "Untitled source") +
      "</td>" +
      "<td>" +
      esc(s.publisher || s.provider || "—") +
      "</td>" +
      "<td>" +
      esc(catLabel) +
      "</td>" +
      "<td>" +
      esc(pub) +
      "</td>" +
      "<td>" +
      esc(verified) +
      "</td>" +
      "<td>" +
      esc(claim) +
      "</td>" +
      "<td>" +
      esc(friendlyStatus(s.verification_status || "Referenced")) +
      "</td>" +
      "<td>" +
      openLink +
      ' · <button type="button" class="hiw-linkish" data-hiw-cite="' +
      n +
      '">Evidence</button>' +
      "</td>" +
      "</tr>"
    );
  }

  function sourcesTableHead() {
    return (
      "<thead><tr>" +
      th("#") +
      th("Source title") +
      th("Publisher") +
      th("Category") +
      th("Published") +
      th("Verified / accessed") +
      th("Supports") +
      th("Status") +
      th("Link") +
      "</tr></thead>"
    );
  }

  /** One sorted Sources-style table (References chapter). */
  function renderSourcesFlatTable(cites, opts) {
    opts = opts || {};
    if (!cites || !cites.length) {
      return '<p class="hiw-empty">No sources in this cohort.</p>';
    }
    var entries = cites
      .map(function (s, i) {
        return { s: s, n: opts.indexOffset != null ? opts.indexOffset + i + 1 : i + 1 };
      })
      .sort(function (a, b) {
        return a.n - b.n;
      });
    var rows = entries
      .map(function (row) {
        return sourceTableRow(row.s, row.n, { focusN: opts.focusN });
      })
      .join("");
    return wrapTable(sourcesTableHead() + "<tbody>" + rows + "</tbody>", "hiw-table--sources");
  }

  function renderSourcesCategoryFilters(cites) {
    var counts = { all: (cites || []).length };
    (cites || []).forEach(function (s) {
      var cat = citationCategory(s);
      counts[cat] = (counts[cat] || 0) + 1;
    });
    var active = state.sourceFilter || "all";
    var pills = SOURCE_CATEGORY_DEFS.filter(function (c) {
      if (c.id === "all") return counts.all > 0;
      return (counts[c.id] || 0) > 0;
    })
      .map(function (c) {
        return (
          '<button type="button" class="hiw-filter' +
          (active === c.id ? " is-active" : "") +
          '" data-hiw-source-filter="' +
          esc(c.id) +
          '">' +
          esc(c.label) +
          ' <span class="hiw-filter__count">(' +
          (counts[c.id] || 0) +
          ")</span></button>"
        );
      })
      .join("");
    if (!pills) return "";
    return (
      '<div class="hiw-filter-group hiw-filter-group--sources">' +
      '<h4 class="hiw-filter-group__title">Source category</h4>' +
      '<div class="hiw-filters">' +
      pills +
      "</div></div>"
    );
  }

  function renderSourcesList() {
    var all = state.citations || [];
    if (!all.length) return '<p class="hiw-empty">No sources in this cohort.</p>';

    var filter = state.sourceFilter || "all";
    var indexed = all.map(function (s, i) {
      return { s: s, n: i + 1 };
    });
    var filtered =
      filter === "all"
        ? indexed
        : indexed.filter(function (row) {
            return citationCategory(row.s) === filter;
          });

    if (!filtered.length) {
      return (
        renderSourcesCategoryFilters(all) +
        '<p class="hiw-empty">No sources in this category.</p>'
      );
    }

    var rows = filtered
      .map(function (row) {
        return sourceTableRow(row.s, row.n, { focusN: state.sourceFocusN });
      })
      .join("");

    return (
      renderSourcesCategoryFilters(all) +
      '<section class="hiw-section hiw-section--sources-register" data-hiw-sources-register="1">' +
      sectionHeadHtml(
        "Hotel source register",
        '<span class="hiw-prose hiw-prose--quiet">' +
          filtered.length +
          (filter === "all" ? " sources" : " in filter") +
          "</span>"
      ) +
      wrapTable(sourcesTableHead() + "<tbody>" + rows + "</tbody>", "hiw-table--sources") +
      "</section>"
    );
  }

  function resolveOwnerPortfolioScope(orgPayload, ownership) {
    var api = window.HeOwnerPortfolioOperatorView;
    var scopes = (api && api.PORTFOLIO_SCOPE) || {
      VERIFIED_ONLY: "verified_only",
      VERIFIED_AND_PROPOSED: "verified_and_proposed",
    };
    var stored = "";
    try {
      stored = sessionStorage.getItem("he-owner-portfolio-scope-v1") || "";
    } catch (_err) {
      stored = "";
    }
    if (stored === scopes.VERIFIED_ONLY || stored === scopes.VERIFIED_AND_PROPOSED) {
      return stored;
    }
    var proposedMode =
      (ownership &&
        ownership.founder_preview &&
        String(ownership.founder_preview.mode || "").toLowerCase() === "proposed") ||
      (orgPayload && orgPayload._from_proposed_owner_portfolio) ||
      (ownership &&
        ownership.organization &&
        ownership.organization.preview_state === "PROPOSED_STAGED");
    return proposedMode ? scopes.VERIFIED_AND_PROPOSED : scopes.VERIFIED_ONLY;
  }

  function hotelExplorerDeepLinkButton(h, focusId) {
    var id = String(h.airtable_record_id || h.id || "").trim();
    var name = h.hotel_name || h.name || "Hotel";
    if (!id) {
      return (
        esc(name) +
        ' <span class="hiw-status hiw-status--med">Pending HPC Match</span>'
      );
    }
    return (
      '<button type="button" class="hiw-linkish" data-hiw-hotel="' +
      esc(id) +
      '" data-hex-open-hotel="' +
      esc(id) +
      '">' +
      esc(name) +
      "</button>" +
      (id === focusId ? ' <span class="hiw-status hiw-status--high">Focus</span>' : "")
    );
  }

  function ownedPortfolioRow(h, focusId) {
    var market = h.market_display || h.market || h.city || "—";
    var rooms =
      h.rooms_known && h.rooms != null
        ? formatCount(h.rooms)
        : h.rooms != null && Number(h.rooms) > 0
          ? formatCount(h.rooms)
          : "—";
    var brand = h.brand_display || h.brand || "—";
    var operator = h.operator || "—";
    var rel =
      h.ownership_relationship_label ||
      (window.HeOwnerPortfolioOperatorView &&
        window.HeOwnerPortfolioOperatorView.formatOwnershipRelationshipLabel(h)) ||
      "Owned / controlled";
    var status =
      h.ownership_evidence_label ||
      (window.HeOwnerPortfolioOperatorView &&
        window.HeOwnerPortfolioOperatorView.classifyPortfolioHotel &&
        window.HeOwnerPortfolioOperatorView.classifyPortfolioHotel(h).label) ||
      "Proposed";
    return (
      "<tr>" +
      "<td>" +
      hotelExplorerDeepLinkButton(h, focusId) +
      "</td>" +
      "<td>" +
      esc(market) +
      "</td>" +
      '<td class="dl-td--num">' +
      esc(rooms) +
      "</td>" +
      "<td>" +
      esc(brand) +
      "</td>" +
      "<td>" +
      esc(operator === "—" ? "Not verified" : operator) +
      "</td>" +
      "<td>" +
      esc(rel) +
      "</td>" +
      "<td>" +
      esc(status) +
      "</td></tr>"
    );
  }

  function buildSharedHotelAttrs(hotel, ownership) {
    var inv =
      (hotel && hotel.guestRoomsMeetingSpace) ||
      (ownership && ownership.guestRoomsMeetingSpace) ||
      null;
    var roomsFromInv =
      inv &&
      inv.guestRooms &&
      inv.guestRooms.totalGuestrooms &&
      (inv.guestRooms.totalGuestrooms.value != null
        ? inv.guestRooms.totalGuestrooms.value
        : inv.guestRooms.totalGuestrooms);
    var rooms = Number(roomsFromInv || (hotel && hotel.rooms) || (ownership && ownership.hotel && ownership.hotel.rooms));
    var control =
      (ownership &&
        ownership.report &&
        ownership.report.ownership_and_control) ||
      {};
    var partyName = function (p) {
      if (!p) return null;
      if (typeof p === "string") return p;
      return p.tradingName || p.legalName || p.name || p.display_name || null;
    };
    return {
      hotelId: recordId(hotel),
      name: (hotel && hotel.name) || null,
      rooms: Number.isFinite(rooms) && rooms > 0 ? rooms : null,
      brand: (hotel && (hotel.brand || hotel.currentBrand)) || partyName(control.brand) || null,
      market: (hotel && hotel.market) || null,
      operator:
        (hotel && (hotel.managementCompany || hotel.operator)) ||
        partyName(control.operator) ||
        null,
      status: (hotel && (hotel.status || hotel.radarStatus)) || null,
      chainScale: (hotel && hotel.chainScale) || null,
      sources: { rooms: roomsFromInv != null ? "physical_product_or_hpc" : "hotel_dto" },
    };
  }

  function buildPortfolioOperatorView(ownedRows, ownerName, coverageMeta, scope, sharedHotelView) {
    var api = window.HeOwnerPortfolioOperatorView;
    if (api && typeof api.buildOwnerPortfolioOperatorView === "function") {
      return api.buildOwnerPortfolioOperatorView(ownedRows, {
        ownerDisplayName: ownerName,
        // Coverage title is derived from selected scope (verified vs verified+proposed).
        completeness: (coverageMeta && coverageMeta.completeness) || "PARTIAL",
        scope: scope,
        sharedHotelView: sharedHotelView || null,
        focusHotelId: sharedHotelView && sharedHotelView.hotelId ? sharedHotelView.hotelId : null,
      });
    }
    return {
      ownedHotels: ownedRows,
      scope: scope,
      totals: {
        hotels: ownedRows.length,
        roomsKnown: ownedRows.reduce(function (s, h) {
          return s + (Number(h.rooms) || 0);
        }, 0),
        roomsKnownHotelCount: 0,
        roomsUnknownHotelCount: 0,
      },
      operatorGroups: [],
      reconciliation: { ok: false, hotelsMatch: false, roomsMatch: false },
      stats: {},
      partition: { verified: 0, proposed: 0, unresolved_match: 0, historical: 0 },
      coverageLabel: "Selected Portfolio Coverage",
    };
  }

  function renderPortfolioScopeToggle(scope, partition) {
    var api = window.HeOwnerPortfolioOperatorView;
    var scopes = (api && api.PORTFOLIO_SCOPE) || {
      VERIFIED_ONLY: "verified_only",
      VERIFIED_AND_PROPOSED: "verified_and_proposed",
    };
    var verified = (partition && partition.verified) || 0;
    var proposed =
      ((partition && partition.proposed) || 0);
    return (
      '<div class="hex-portfolio-scope" role="group" aria-label="Portfolio evidence scope">' +
      '<span class="hex-portfolio-scope__label">Portfolio scope</span>' +
      '<button type="button" class="hex-portfolio-scope__btn' +
      (scope === scopes.VERIFIED_ONLY ? " is-active" : "") +
      '" data-hex-portfolio-scope="' +
      esc(scopes.VERIFIED_ONLY) +
      '" aria-pressed="' +
      (scope === scopes.VERIFIED_ONLY ? "true" : "false") +
      '">Verified only (' +
      esc(String(verified)) +
      ")</button>" +
      '<button type="button" class="hex-portfolio-scope__btn' +
      (scope === scopes.VERIFIED_AND_PROPOSED ? " is-active" : "") +
      '" data-hex-portfolio-scope="' +
      esc(scopes.VERIFIED_AND_PROPOSED) +
      '" aria-pressed="' +
      (scope === scopes.VERIFIED_AND_PROPOSED ? "true" : "false") +
      '">Verified + proposed (' +
      esc(String(verified + proposed)) +
      ")</button>" +
      '<p class="hiw-muted hex-portfolio-scope__note">A published portfolio listing is association evidence — not verified current title or economic control. Both sections below use this same selected set.</p>' +
      "</div>"
    );
  }

  function renderOperatorGroupsHtml(view, focusId) {
    var groups = (view && view.operatorGroups) || [];
    if (!groups.length) {
      return '<p class="hiw-empty">No operator groupings available for this ownership portfolio.</p>';
    }
    return (
      '<div class="hex-op-groups" data-hex-op-groups>' +
      groups
        .map(function (g, idx) {
          var open = idx === 0;
          return (
            '<div class="hex-op-group' +
            (open ? " is-open" : "") +
            '" data-hex-op-group>' +
            '<button type="button" class="hex-op-group__head" data-hex-op-toggle aria-expanded="' +
            (open ? "true" : "false") +
            '">' +
            '<span class="hex-op-group__arrow" aria-hidden="true">' +
            (open ? "▾" : "▸") +
            "</span>" +
            '<span class="hex-op-group__name">' +
            esc(g.operatorName) +
            "</span>" +
            '<span class="hex-op-group__meta">' +
            esc(String(g.hotelCount)) +
            " hotels · " +
            esc(formatCount(g.roomsKnown)) +
            " rooms" +
            (g.roomsUnknownHotelCount
              ? " · " + g.roomsUnknownHotelCount + " rooms unresolved"
              : "") +
            "</span></button>" +
            '<div class="hex-op-group__body"' +
            (open ? "" : " hidden") +
            ">" +
            '<ul class="hex-op-group__hotels">' +
            (g.hotels || [])
              .map(function (h) {
                return (
                  "<li>" +
                  hotelExplorerDeepLinkButton(h, focusId) +
                  ' <span class="hex-op-group__rooms">' +
                  (h.rooms_known && h.rooms != null ? esc(formatCount(h.rooms)) + " rooms" : "rooms unresolved") +
                  "</span></li>"
                );
              })
              .join("") +
            "</ul></div></div>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function synthesizeOrgPayloadFromOwnership(ownership, hotel) {
    if (!ownership) return null;
    if (!ownership.report && !(ownership.airtable_graph && ownership.airtable_graph.relationshipCount)) {
      return null;
    }
    var org = ownership.organization || ownership.ownership_group || {};
    if (
      !org.display_name &&
      ownership.airtable_graph &&
      ownership.airtable_graph.primaryOrganization
    ) {
      var po = ownership.airtable_graph.primaryOrganization;
      org = {
        display_name: po.tradingName || po.legalName,
        legal_name: po.legalName || po.tradingName,
        website: po.officialWebsite || null,
        known_hotel_count: (ownership.airtable_graph.portfolioCount || 0),
      };
    }
    var portfolioRows =
      (ownership.report &&
        ownership.report.organization_and_portfolio &&
        ownership.report.organization_and_portfolio.hotels) ||
      [];
    var assets = (ownership.deep_research &&
      ownership.deep_research.portfolio_notes &&
      ownership.deep_research.portfolio_notes.assets) ||
      [];
    var portfolio = portfolioRows.length
      ? portfolioRows.map(function (h) {
          return {
            name: h.hotel || h.name,
            city: h.market || null,
            market: h.market || null,
            rooms: h.rooms,
            rooms_display: h.rooms,
            brand_display: h.brand || null,
            relationship_type: h.relationship || "RELATED",
            verification_bucket: h.confidence || "HIGH",
            economic_owner_verified: /OWNED/i.test(String(h.relationship || "")),
          };
        })
      : assets.map(function (a) {
          var rels = Array.isArray(a.relationships) ? a.relationships : [];
          return {
            name: a.name,
            city: a.market || null,
            rooms: a.rooms,
            rooms_display: a.rooms,
            brand_display: a.brand || null,
            relationship_type: rels.some(function (r) {
              return /OWNED/i.test(String(r));
            })
              ? "OWNED_BY"
              : "OPERATED_BY",
            verification_bucket: a.confidence || "HIGH",
            economic_owner_verified: rels.some(function (r) {
              return /OWNED/i.test(String(r));
            }),
          };
        });
    if (!org.display_name && !portfolio.length) return null;
    return {
      group: {
        display_name: org.display_name || org.legal_name || "Related organization",
        legal_name: org.legal_name || org.display_name || null,
        website: org.website || null,
        headquarters: org.headquarters || null,
        known_current_hotel_count: portfolio.length || org.known_hotel_count || 0,
        known_hotel_relationships: portfolio.length || org.known_hotel_count || 0,
        relationship_summary: {
          total_relationships: portfolio.length,
          owned_or_controlled_verified: portfolio.filter(function (h) {
            return h.economic_owner_verified || h.relationship_type === "OWNED_BY";
          }).length,
          operated_or_managed: portfolio.length,
          ownership_unknown: 0,
        },
      },
      portfolio: portfolio,
      page_framing: {
        title: "Known hotel relationships",
        kicker: "Organization profile",
      },
      why_this_matters: {
        text:
          (ownership.report.why_this_matters && ownership.report.why_this_matters.text) ||
          (ownership.report.commercial_pursuit &&
            ownership.report.commercial_pursuit.why_matters) ||
          (ownership.report.executive_summary && ownership.report.executive_summary.why_this_hotel_matters) ||
          "",
      },
      strategic_context:
        (ownership.report && ownership.report.strategic_context) ||
        (ownership.organization && ownership.organization.strategic_context) ||
        null,
      key_people:
        (ownership.organization && ownership.organization.key_people) ||
        (ownership.report &&
          ownership.report.decision_authority &&
          ownership.report.decision_authority.people) ||
        [],
      portfolio_kpis:
        (ownership.organization && ownership.organization.portfolio_kpis) ||
        (ownership.report.organization_and_portfolio &&
          ownership.report.organization_and_portfolio.kpis) ||
        null,
      owned_controlled_hotels:
        (ownership.organization && ownership.organization.owned_controlled_hotels) ||
        portfolio.filter(function (h) {
          return h.economic_owner_verified || h.relationship_type === "OWNED_BY";
        }),
      portfolio_source:
        (ownership.organization && ownership.organization.portfolio_source) || null,
      operated_managed_hotels:
        (ownership.organization && ownership.organization.operated_managed_hotels) ||
        [],
      ticker:
        org.ticker ||
        org.registry_id ||
        (ownership.organization && ownership.organization.ticker) ||
        null,
      _synthesized_from_ownership: true,
    };
  }

  function renderOrganization(hotel, ownership, orgPayload) {
    if (orgPayload && !orgPayload.group && orgPayload.organization) {
      orgPayload = Object.assign({}, orgPayload, { group: orgPayload.organization });
    }
    if ((!orgPayload || !orgPayload.group) && ownership) {
      orgPayload = synthesizeOrgPayloadFromOwnership(ownership, hotel);
    }
    if (!orgPayload || !orgPayload.group) {
      return '<div class="hiw-empty">Organization profile not available for this hotel.</div>';
    }
    var g = orgPayload.group;
    var summary = (orgPayload.page_framing && orgPayload.page_framing.summary) || g.relationship_summary || {};
    var hotelName = hotel.name || "This hotel";
    var focusId = recordId(hotel);
    var ownedFocus = isRichCase(ownership) || (ownership && ownership.hotel && ownership.hotel.economic_owner_verified === true);
    var opName =
      (ownership &&
        ownership.report &&
        ownership.report.ownership_and_control &&
        ownership.report.ownership_and_control.operator &&
        ownership.report.ownership_and_control.operator.name) ||
      "";
    var orgNameLower = String(g.display_name || "").toLowerCase();
    var opNameLower = String(opName || "").toLowerCase();
    var ownerOperatorSame =
      ownedFocus &&
      opNameLower &&
      orgNameLower &&
      (orgNameLower.indexOf(opNameLower) >= 0 || opNameLower.indexOf(orgNameLower.slice(0, 12)) >= 0);
    var portfolio = orgPayload.portfolio || [];
    var ownedFromOrg =
      (orgPayload.owned_controlled_hotels && orgPayload.owned_controlled_hotels.length
        ? orgPayload.owned_controlled_hotels
        : null) ||
      (ownership &&
        ownership.organization &&
        ownership.organization.owned_controlled_hotels) ||
      [];
    var ownedSeed = ownedFromOrg.length
      ? ownedFromOrg
      : portfolio.filter(function (h) {
          return (
            h.include_as_owned === true ||
            h.economic_owner_verified ||
            /OWNED|CONTROLLED|ECONOMIC_OWNER|LEGAL_OWNER/i.test(
              String(h.relationship_type || h.relationship || "")
            )
          );
        });
    var coverageMeta =
      (ownership && ownership.organization && ownership.organization.portfolio_kpis) ||
      orgPayload.portfolio_kpis ||
      null;
    var scope = resolveOwnerPortfolioScope(orgPayload, ownership);
    var sharedHotelView = buildSharedHotelAttrs(hotel, ownership);
    var view = buildPortfolioOperatorView(
      ownedSeed,
      g.display_name,
      coverageMeta,
      scope,
      sharedHotelView
    );
    var ownedRows = view.ownedHotels || [];
    var partition = view.partition || {};

    var orgPeople = collectOrgKeyPeople(ownership, orgPayload);

    var framingNote = g.evidence_summary || "";
    var portfolioSourceNote =
      (ownership &&
        ownership.organization &&
        ownership.organization.portfolio_source &&
        ownership.organization.portfolio_source.limitation) ||
      (orgPayload.portfolio_source && orgPayload.portfolio_source.limitation) ||
      "";
    var historicalNote =
      partition.historical > 0
        ? partition.historical +
          " historical / disposed listing(s) are excluded from current portfolio totals."
        : "";

    return (
      '<article class="hiw-report hex-owner-portfolio" data-hex-owner-portfolio="1" data-hex-portfolio-scope="' +
      esc(scope) +
      '">' +
      '<h2 class="hiw-report__title">' +
      esc(g.display_name) +
      "</h2>" +
      '<p class="hiw-report__subtitle">Owner portfolio intelligence · related hotel: ' +
      esc(hotelName) +
      (ownedFocus
        ? ownerOperatorSame
          ? " · Economic owner &amp; operator"
          : " · Economic owner · operator relationships shown per hotel" +
            (opName ? " (this hotel: " + esc(opName) + ")" : "")
        : " · Organization profile · ownership not implied by brand/management alone") +
      (orgPayload.owner_explorer_path
        ? ' · <a href="' +
          esc(orgPayload.owner_explorer_path) +
          '">Open Owner Explorer</a>'
        : "") +
      "</p>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Organization Overview</h3>' +
      '<div class="hiw-prose"><p>' +
      esc(g.display_name) +
      (g.legal_name ? " (" + esc(g.legal_name) + ")" : "") +
      (g.ticker ? " · " + esc(g.ticker) : "") +
      ". Sections below show this owner's owned–controlled hotels for the selected evidence scope, then group <em>those same hotels</em> by management company. Brand affiliation and third-party management contracts are not treated as real-estate ownership." +
      (framingNote ? "</p><p>" + esc(framingNote) : "") +
      (portfolioSourceNote ? "</p><p>" + esc(portfolioSourceNote) : "") +
      (historicalNote ? "</p><p>" + esc(historicalNote) : "") +
      "</p></div>" +
      renderPortfolioScopeToggle(scope, partition) +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">' +
      esc(view.coverageLabel || "Selected Portfolio Coverage") +
      "</h3>" +
      '<div class="hex-facts hex-facts--5 hex-facts--own" aria-label="Owner portfolio coverage">' +
      hexFact(formatCount(view.totals.hotels), "Selected hotels") +
      hexFact(
        formatCount(view.totals.roomsKnownHotelCount || 0),
        "Hotels with known rooms"
      ) +
      hexFact(formatCount(view.totals.roomsKnown), "Known rooms") +
      hexFact(
        formatCount(view.totals.roomsUnknownHotelCount || 0),
        "Rooms unresolved"
      ) +
      hexFact(
        formatCount((view.stats && view.stats.hotelsOperatorUnresolved) || 0),
        "Operator unresolved"
      ) +
      "</div>" +
      '<p class="hiw-muted">Universe: ' +
      esc(String(partition.verified || 0)) +
      " verified · " +
      esc(String(partition.proposed || 0)) +
      " proposed/unresolved · " +
      esc(String(partition.historical || 0)) +
      " historical excluded. Known-room totals are not a complete portfolio-room figure.</p>" +
      (view.reconciliation && view.reconciliation.ok
        ? '<p class="hiw-muted">Totals reconcile for this scope: selected hotels = operator-group hotels; known rooms match.</p>'
        : '<p class="hiw-muted">Reconciliation check pending or incomplete for this portfolio payload.</p>') +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Owned / Controlled Hotels</h3>' +
      '<p class="hiw-muted">Selected hotels: ' +
      esc(formatCount(view.totals.hotels)) +
      " · Hotels with known rooms: " +
      esc(formatCount(view.totals.roomsKnownHotelCount || 0)) +
      " · Known rooms: " +
      esc(formatCount(view.totals.roomsKnown)) +
      " · Rooms unresolved: " +
      esc(formatCount(view.totals.roomsUnknownHotelCount || 0)) +
      "</p>" +
      (ownedRows.length
        ? '<div class="dl-table-wrap">' +
          wrapTable(
            "<thead><tr>" +
              th("Hotel") +
              th("Market") +
              th("Rooms") +
              th("Brand") +
              th("Operator") +
              th("Ownership relationship") +
              th("Status") +
              "</tr></thead><tbody>" +
              ownedRows
                .map(function (h) {
                  return ownedPortfolioRow(h, focusId);
                })
                .join("") +
              "</tbody>",
            "hiw-table--portfolio dl-table ada-table"
          ) +
          "</div>"
        : '<div class="hiw-gap-box"><h4>No hotels in this scope</h4><p>' +
          (scope === "verified_only"
            ? "No verified current owned/controlled hotels yet. Switch to Verified + proposed to review staged candidates. A portfolio website listing alone does not certify ownership."
            : "No owned/controlled hotels in the selected scope. Research the organization once, link matched HPC hotels, and reuse the graph.") +
          "</p></div>") +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Who Manages These Hotels?</h3>' +
      '<p class="hiw-muted">Same hotel set as Owned / Controlled for the selected scope, grouped by current management company. Owner-operated properties are labeled separately from third-party managers.</p>' +
      renderOperatorGroupsHtml(view, focusId) +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Key People</h3>' +
      '<p class="hiw-muted">Organization-level summary from the shared contact intelligence model. Names open Contact Intelligence; LinkedIn opens verified profiles. Surfe enrichment stays on Contact Intelligence.</p>' +
      renderPeopleGrouped(orgPeople.slice(0, 12)) +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Strategic Context</h3>' +
      renderStrategicContext(hotel, ownership, orgPayload) +
      "</section>" +
      "</article>"
    );
  }

  function edgeEndpoint(e, side) {
    if (!e || typeof e !== "object") return "";
    if (side === "from") {
      return (
        e.from ||
        e.subject ||
        e.from_label ||
        e.source ||
        e.hpcHotelId ||
        (e.source === "HI_AIRTABLE_GRAPH" ? "Hotel" : "") ||
        ""
      );
    }
    return (
      e.to ||
      e.object ||
      e.to_label ||
      e.target ||
      e.organization_id ||
      e.organizationId ||
      ""
    );
  }

  function edgeTemporal(e) {
    if (!e || typeof e !== "object") return "CURRENT";
    return e.temporal_status || e.temporal || e.status || "CURRENT";
  }

  function edgeCategory(edgeOrType) {
    var e = edgeOrType && typeof edgeOrType === "object" ? edgeOrType : null;
    var t = String((e && (e.type || e.relationship_type)) || edgeOrType || "").toUpperCase();
    var temporal = String(e ? edgeTemporal(e) : "").toUpperCase();
    // Historical first: former edges and disposition events (SOLD), even when type is OPERATED_BY/BRANDED_BY.
    if (
      /FORMER|HISTORICAL|PAST/.test(temporal) ||
      /FORMER|HISTOR|SOLD|DISPOS|ACQUIRED_FROM/.test(t) ||
      (/SOLD/.test(t) && temporal && temporal !== "CURRENT" && temporal !== "ANNOUNCED")
    ) {
      return "historical";
    }
    if (/OWN|CONTROL|SPONSOR/.test(t)) return "ownership";
    if (/OPERAT|MANAGE|ASSET/.test(t)) return "operator";
    if (/BRAND|FRANCHIS/.test(t)) return "brand";
    if (/DEVELOP/.test(t)) return "developed";
    if (/JV|JOINT/.test(t)) return "jv";
    if (/PERSON|PEOPLE/.test(t)) return "people";
    return "other";
  }

  function resolveOrgDisplayName(rel, ownership) {
    if (!rel) return "Organization";
    if (rel.organization_name) return rel.organization_name;
    if (rel.organizationName) return rel.organizationName;
    if (rel.display_name) return rel.display_name;
    if (rel.tradingName || rel.trading_name) return rel.tradingName || rel.trading_name;
    if (rel.legalName || rel.legal_name) return rel.legalName || rel.legal_name;
    if (rel.to_label) return rel.to_label;
    if (
      rel.to &&
      !/^rec[A-Za-z0-9]{14}$/.test(String(rel.to)) &&
      !/^dle_[A-Za-z0-9]+$/i.test(String(rel.to)) &&
      !/^dor_[A-Za-z0-9]+$/i.test(String(rel.to))
    ) {
      return rel.to;
    }
    var orgs =
      (ownership && ownership.airtable_graph && ownership.airtable_graph.organizations) ||
      (ownership && ownership.report && ownership.report.organizations) ||
      [];
    var id = rel.organization_id || rel.organizationId || rel.airtable_id || rel.to;
    var linkIds = [].concat(rel.organizationLink || rel.organization_link || []);
    if (id && Array.isArray(orgs)) {
      for (var i = 0; i < orgs.length; i++) {
        var o = orgs[i] || {};
        var candidates = [
          o.airtableId,
          o.orgId,
          o.organizationId,
          o.id,
          o.organization_id,
        ];
        var hit =
          candidates.indexOf(id) >= 0 ||
          linkIds.some(function (lid) {
            return candidates.indexOf(lid) >= 0;
          });
        if (hit) {
          return (
            o.tradingName ||
            o.trading_name ||
            o.display_name ||
            o.legalName ||
            o.legal_name ||
            o.name ||
            "Organization"
          );
        }
      }
    }
    if (id && (/^rec[A-Za-z0-9]{14}$/.test(String(id)) || /^dle_/i.test(String(id)))) {
      return "Organization (name pending)";
    }
    return id || "Organization";
  }

  function normalizeGraphEdges(ownership) {
    var fromReport = (ownership && ownership.report && ownership.report.relationships) || [];
    if (Array.isArray(fromReport) && fromReport.length && fromReport[0] && fromReport[0].relationship_type) {
      return fromReport.map(function (rel) {
        return {
          type: rel.relationship_type,
          relationship_type: rel.relationship_type,
          from: "Focus hotel",
          to: resolveOrgDisplayName(rel, ownership),
          temporal_status: rel.preview_state === "PROPOSED_STAGED" ? "PROPOSED" : "CURRENT",
          confidence: rel.confidence || rel.verification_state || "MEDIUM",
          source: rel.source || "HI_AIRTABLE_GRAPH",
          preview_state: rel.preview_state || null,
        };
      });
    }
    var fromGraph =
      (ownership && ownership.airtable_graph && ownership.airtable_graph.relationships) || [];
    if (Array.isArray(fromGraph) && fromGraph.length) {
      return fromGraph.map(function (rel) {
        return {
          type: rel.relationshipType || rel.relationship_type,
          relationship_type: rel.relationshipType || rel.relationship_type,
          from: "Focus hotel",
          to: resolveOrgDisplayName(rel, ownership),
          temporal_status: "CURRENT",
          confidence: rel.confidence || rel.verificationState || "MEDIUM",
          source: "HI_AIRTABLE_GRAPH",
        };
      });
    }
    return (
      (ownership && ownership.deep_research && ownership.deep_research.relationships) ||
      (ownership && ownership.report && ownership.report.relationship_edges) ||
      (ownership &&
        ownership.report &&
        ownership.report.relationships &&
        ownership.report.relationships.edges) ||
      []
    );
  }

  function renderRelationshipEdgesTable(ownership, filter) {
    var edges = normalizeGraphEdges(ownership);
    if (!edges.length) return "";
    var f = filter || state.relFilter || "all";
    var filters = [
      { id: "all", label: "All" },
      { id: "ownership", label: "Owned / Controlled" },
      { id: "operator", label: "Operated" },
      { id: "brand", label: "Branded" },
      { id: "historical", label: "Historical" },
      { id: "jv", label: "JV / Sponsored" },
    ];
    var counts = { all: edges.length };
    edges.forEach(function (e) {
      var cat = edgeCategory(e);
      counts[cat] = (counts[cat] || 0) + 1;
    });
    var filtered = edges.filter(function (e) {
      if (f === "all") return true;
      return edgeCategory(e) === f;
    });
    var pills =
      '<div class="hiw-filter-group">' +
      '<h4 class="hiw-filter-group__title">Relationship Type</h4>' +
      '<div class="hiw-filters">' +
      filters
        .map(function (x) {
          var count = counts[x.id] || 0;
          return (
            '<button type="button" class="hiw-filter' +
            (f === x.id ? " is-active" : "") +
            '" data-hiw-rel-filter="' +
            x.id +
            '">' +
            esc(x.label) +
            ' <span class="hiw-filter__count">(' +
            count +
            ")</span></button>"
          );
        })
        .join("") +
      "</div></div>";
    var tableHtml = filtered.length
      ? wrapTable(
          "<thead><tr>" +
            th("From") +
            th("Relationship") +
            th("To") +
            th("State") +
            th("Confidence") +
            "</tr></thead><tbody>" +
            filtered
              .map(function (e) {
                var rowKey =
                  String(e.id || "") ||
                  [
                    edgeEndpoint(e, "from"),
                    relTypeLabel(e.type || e.relationship_type),
                    edgeEndpoint(e, "to"),
                  ].join("|");
                return (
                  '<tr class="hiw-rel-row" data-hiw-rel-row="' +
                  esc(rowKey) +
                  '" tabindex="0"><td>' +
                  esc(edgeEndpoint(e, "from")) +
                  "</td><td>" +
                  esc(relTypeLabel(e.type || e.relationship_type)) +
                  "</td><td>" +
                  esc(edgeEndpoint(e, "to")) +
                  "</td><td>" +
                  esc(friendlyStatus(edgeTemporal(e))) +
                  "</td><td>" +
                  esc(friendlyStatus(e.confidence || "HIGH")) +
                  "</td></tr>"
                );
              })
              .join("") +
            "</tbody>",
          "hiw-table--relationships"
        )
      : '<p class="hiw-empty">No relationships in this filter.</p>';
    return snapSub("Relationship Detail", pills + tableHtml, {
      navHtml: ownershipSourcesNavLink(1),
    });
  }

  function renderRelationships(hotel, ownership) {
    var ctrl = getControl(ownership);
    var op =
      (ctrl.operator && ctrl.operator.name) ||
      hotel.managementCompany ||
      "—";
    var brand =
      (ownership && ownership.hotel && ownership.hotel.brand_display) || hotel.brand || "—";
    var brandStatus =
      (ownership && ownership.hotel && ownership.hotel.brand_display_status) || "";
    var contested = String(brandStatus) === "CONTESTED";
    var propco = ctrl.legal_property_owner_propco || {};
    var econ = ctrl.economic_owner_or_group || {};
    var chain = (ownership && ownership.report && ownership.report.ownership_chain) ||
      (ownership && ownership.deep_research && ownership.deep_research.ownership_chain) ||
      [];
    var propcoKnown = controlPartyKnown(propco);
    var econKnown =
      controlPartyKnown(econ) ||
      !!(
        chain.find(function (n) {
          return n.role === "economic_owner" && n.name;
        }) || {}
      ).name;

    var hotelDiagram = econKnown || propcoKnown
      ? '<section class="hiw-section"><h3 class="hiw-section__h">Hotel Structure</h3>' +
        buildOwnershipStructureChart(hotel, ctrl, op, {
          includeOperator: true,
          includeBrand: true,
          brand: brand,
          brandStatus: brandStatus,
          forceEcon: true,
          ariaLabel: "Hotel structure",
        }) +
        '<div class="hiw-prose"><p>Ownership, operations, and brand are separate relationships. Package / economic owner naming is not the same as a verified Mexican PropCo or deed vehicle. Operator or brand is never drawn as owner. Stakeholder exploration lives in Ownership Intelligence.</p></div>' +
        renderRelationshipEdgesTable(ownership, state.relFilter) +
        "</section>"
      : '<section class="hiw-section"><h3 class="hiw-section__h">Hotel Structure</h3>' +
        orgChart(
          chainNode(op, "Operator / Manager", "High", "org") +
            chainEdge("Operates / manages") +
            chainNode(hotel.name, "Focus Hotel", "Owner not verified", "focus") +
            chainEdge("Brand affiliation" + (contested ? " · Contested" : "")) +
            chainNode(brand, "Brand", contested ? "Contested" : brandStatus || "Affiliation", "brand"),
          "Hotel structure"
        ) +
        '<div class="hiw-prose"><p>Economic owner and property company are held back when unverified — the operator is not relabeled as owner. Stakeholder exploration lives in Ownership Intelligence.</p></div>' +
        renderRelationshipEdgesTable(ownership, state.relFilter) +
        "</section>";

    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Relationships &amp; Structure</h2>' +
      '<p class="hiw-report__subtitle">Hotel structure · relationship detail · sources — stakeholder network lives under Ownership Intelligence</p>' +
      hotelDiagram +
      "</article>"
    );
  }

  function getMarketIntel(ownership, hotel) {
    return (
      (ownership && ownership.hotel && ownership.hotel.market_intelligence) ||
      (ownership && ownership.deep_research && ownership.deep_research.market_intelligence) ||
      (ownership && ownership.report && ownership.report.market_intelligence) ||
      (hotel && hotel.market_intelligence) ||
      null
    );
  }

  function getPropertyProfile(ownership, hotel) {
    return (
      (ownership && ownership.hotel && ownership.hotel.property_profile) ||
      (ownership && ownership.deep_research && ownership.deep_research.property_profile) ||
      null
    );
  }

  function renderMarket(hotel, ownership) {
    var mi = getMarketIntel(ownership, hotel);
    var sub = (mi && mi.submarket) || {};
    var areas = (mi && mi.area_hotels) || [];
    var drivers = (mi && mi.demand_drivers) || [];
    var access = (mi && mi.access) || {};
    if (!mi) {
      return (
        '<article class="hiw-report">' +
        '<h2 class="hiw-report__title">Market intelligence</h2>' +
        '<p class="hiw-report__subtitle">Radar geography context for this hotel</p>' +
        '<section class="hiw-section"><h3 class="hiw-section__h">Location</h3>' +
        '<table class="hiw-table"><tbody>' +
        row("Country", hotel.country || "—") +
        row("Market", hotel.market || "—") +
        row("Submarket", hotel.submarket || "—") +
        row("City", hotel.city || "—") +
        row("Latitude", hotel.lat != null ? hotel.lat : hotel.latitude != null ? hotel.latitude : "—") +
        row("Longitude", hotel.lng != null ? hotel.lng : hotel.longitude != null ? hotel.longitude : "—") +
        "</tbody></table>" +
        '<div class="hiw-prose"><p>Submarket snapshot, area hotels, and corridor intelligence remain available from Radar layers.</p></div>' +
        "</section></article>"
      );
    }
    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Submarket Snapshot</h2>' +
      '<p class="hiw-report__subtitle">' +
      esc(sub.label || hotel.submarket || hotel.market || "Market context") +
      "</p>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Hotel-relevant context</h3>' +
      '<div class="hiw-prose"><p>' +
      esc(sub.summary || "—") +
      "</p></div>" +
      (Array.isArray(sub.demand_segments) && sub.demand_segments.length
        ? "<ul>" +
          sub.demand_segments.map(function (s) {
            return "<li>" + esc(s) + "</li>";
          }).join("") +
          "</ul>"
        : "") +
      "</section>" +
      (areas.length
        ? '<section class="hiw-section"><h3 class="hiw-section__h">Area hotels (preview)</h3>' +
          '<table class="hiw-table hiw-table--portfolio"><thead><tr><th>Hotel</th><th>Relation</th><th>Market</th></tr></thead><tbody>' +
          areas
            .map(function (a) {
              return (
                "<tr><td>" +
                esc(a.name) +
                "</td><td>" +
                esc(a.relation || "—") +
                "</td><td>" +
                esc(a.market || "—") +
                "</td></tr>"
              );
            })
            .join("") +
          "</tbody></table></section>"
        : "") +
      (drivers.length
        ? '<section class="hiw-section"><h3 class="hiw-section__h">Demand drivers (preview)</h3><ul>' +
          drivers
            .map(function (d) {
              return (
                "<li><strong>" +
                esc(d.driver || d.name || "Driver") +
                "</strong> — " +
                esc(d.tie || d.note || "") +
                "</li>"
              );
            })
            .join("") +
          "</ul></section>"
        : "") +
      (access.airport || access.notes
        ? '<section class="hiw-section"><h3 class="hiw-section__h">Access (preview)</h3><div class="hiw-prose"><p>' +
          esc([access.airport, access.notes].filter(Boolean).join(" — ")) +
          "</p></div></section>"
        : "") +
      "</article>"
    );
  }

  function renderSubmarketTab(hotel, ownership) {
    return renderMarket(hotel, ownership);
  }

  function renderAreaHotelsTab(hotel, ownership) {
    var mi = getMarketIntel(ownership, hotel);
    var areas = (mi && mi.area_hotels) || [];
    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Area Hotels</h2>' +
      '<p class="hiw-report__subtitle">Nearby · market · aspirational comps</p>' +
      (areas.length
        ? '<section class="hiw-section">' +
          '<table class="hiw-table hiw-table--portfolio"><thead><tr><th>Hotel</th><th>Relation</th><th>Market</th></tr></thead><tbody>' +
          areas
            .map(function (a) {
              return (
                "<tr><td>" +
                esc(a.name) +
                "</td><td>" +
                esc(a.relation || "—") +
                "</td><td>" +
                esc(a.market || "—") +
                "</td></tr>"
              );
            })
            .join("") +
          "</tbody></table></section>"
        : '<p class="hiw-empty">No area hotel set promoted yet.</p>') +
      "</article>"
    );
  }

  function renderDemandTab(hotel, ownership) {
    var mi = getMarketIntel(ownership, hotel);
    var drivers = (mi && mi.demand_drivers) || [];
    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Demand Drivers</h2>' +
      '<p class="hiw-report__subtitle">Tied to this hotel’s positioning</p>' +
      (drivers.length
        ? '<section class="hiw-section"><ul>' +
          drivers
            .map(function (d) {
              return (
                "<li><strong>" +
                esc(d.driver || d.name || "Driver") +
                "</strong><div class=\"hiw-prose\"><p>" +
                esc(d.tie || d.note || "") +
                "</p></div></li>"
              );
            })
            .join("") +
          "</ul></section>"
        : renderDemand(hotel)) +
      "</article>"
    );
  }

  function renderAccessTab(hotel, ownership) {
    var mi = getMarketIntel(ownership, hotel);
    var access = (mi && mi.access) || {};
    var nodes = access.local_nodes || [];
    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Access &amp; Connectivity</h2>' +
      '<p class="hiw-report__subtitle">Airport · ferry · road · marina context</p>' +
      '<section class="hiw-section"><table class="hiw-table"><tbody>' +
      row("Airport", access.airport || "—") +
      row("Notes", access.notes || "—") +
      row("Local nodes", nodes.length ? nodes.join(" · ") : "—") +
      "</tbody></table></section></article>"
    );
  }

  function renderHotelOverviewTab(hotel, ownership) {
    var profile = getPropertyProfile(ownership, hotel) || {};
    var acre = profile.acreage || {};
    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Hotel Overview</h2>' +
      '<p class="hiw-report__subtitle">Identity · product · amenities</p>' +
      '<section class="hiw-section"><h3 class="hiw-section__h">Property</h3>' +
      '<table class="hiw-table"><tbody>' +
      row("Name", profile.canonical_name || hotel.name || "—") +
      row("Address", profile.address || hotel.address || "—") +
      row("Parish / locality", [profile.parish, profile.locality].filter(Boolean).join(" · ") || hotel.city || "—") +
      row("Country", profile.country || hotel.country || "—") +
      row("Coordinates", [profile.latitude || hotel.latitude, profile.longitude || hotel.longitude].filter(function (v) { return v != null; }).join(", ") || "—") +
      row("Rooms / suites", profile.rooms_suites || hotel.rooms || "—") +
      row(
        "Acreage",
        acre.best_supported_current != null
          ? String(acre.best_supported_current) +
              " acres (" +
              (acre.confidence || "PROBABLE") +
              ")" +
              (acre.conflict ? " — conflict noted: " + (acre.conflict.values || []).join(" vs ") : "")
          : "—"
      ) +
      row("Established", profile.established_year || "—") +
      row("Type", profile.hotel_type || "—") +
      row("Positioning", profile.positioning || "—") +
      row("Website", profile.website || hotel.website || "—") +
      row("Phone", profile.phone || hotel.phone || "—") +
      "</tbody></table></section>" +
      (Array.isArray(profile.amenities) && profile.amenities.length
        ? '<section class="hiw-section"><h3 class="hiw-section__h">Amenities</h3><ul>' +
          profile.amenities.map(function (a) {
            return "<li>" + esc(a) + "</li>";
          }).join("") +
          "</ul></section>"
        : "") +
      (acre.conflict && acre.conflict.notes
        ? '<section class="hiw-section"><div class="hiw-gap-box"><h4>Acreage conflict</h4><p>' +
          esc(acre.conflict.notes) +
          "</p></div></section>"
        : "") +
      "</article>"
    );
  }

  function renderDemand(hotel) {
    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Demand &amp; access</h2>' +
      '<p class="hiw-report__subtitle">Demand drivers and connectivity — Radar context</p>' +
      '<section class="hiw-section"><div class="hiw-prose">' +
      "<p>Demand generators, demand drivers, and access &amp; connectivity intelligence for this market are modeled in The Radar. The Hotel Intelligence Workspace preserves those domains as first-class tabs so ownership research sits beside location intelligence rather than in a separate app.</p>" +
      "<p>For " +
      esc(hotel.name || "this hotel") +
      " in " +
      esc(hotel.market || hotel.city || "this market") +
      ", use Radar map layers (Demand Anchors, Travel Infrastructure) while reading Ownership Intelligence.</p>" +
      "</div>" +
      '<div class="hiw-gap-box"><h4>Unified depth — next iteration</h4><p>Full demand/access narrative pull-through into this tab is architected; content depth expands as Radar section payloads are federated into the workspace.</p></div>' +
      "</section></article>"
    );
  }

  /**
   * Guest Sentiment tab shell (formerly Brand / Operator).
   * Franchise & Current Operator removed — brand/operator facts live on Overview,
   * Ownership Intelligence, and Relationships (HE-QL-038).
   */
  function renderGuestSentimentTab(_hotel, _ownership) {
    if (!isGuestSentimentEnabled()) {
      var f = guestSentimentFlag();
      return f && typeof f.unavailableHtml === "function"
        ? f.unavailableHtml()
        : '<article class="hiw-report" data-he-guest-sentiment-disabled="1"><h2 class="hiw-report__title">Guest Sentiment unavailable</h2><p class="hiw-prose">Guest Sentiment is temporarily disabled pending licensing review.</p></article>';
    }
    return (
      '<article class="hiw-report hiw-report--guest-sentiment" data-hiw-guest-sentiment-tab="1">' +
      '<h2 class="hiw-report__title">Guest Sentiment</h2>' +
      '<p class="hiw-report__subtitle">Guest reputation · category performance · experience insights · hotel news</p>' +
      '<div class="hiw-hotel-news-host" data-hiw-hotel-news-host="1" hidden></div>' +
      '<div class="hiw-guest-sentiment-host" data-hiw-guest-sentiment-host="1" hidden></div>' +
      "</article>"
    );
  }

  /** @deprecated Use renderGuestSentimentTab — kept as alias for embed callers. */
  function renderBrandOperator(hotel, ownership) {
    return renderGuestSentimentTab(hotel, ownership);
  }

  function isFounderProposedPreview() {
    try {
      var q = new URLSearchParams(window.location.search || "");
      var mode = String(q.get("previewMode") || q.get("mode") || "")
        .trim()
        .toLowerCase();
      if (mode === "proposed" || mode === "phase3") return true;
    } catch (_e) {}
    try {
      var attr = String(
        document.documentElement.getAttribute("data-he-founder-preview") || ""
      )
        .trim()
        .toLowerCase();
      if (attr === "proposed" || attr === "phase3") return true;
    } catch (_e2) {}
    return !!(
      state.ownership &&
      state.ownership.founder_preview &&
      String(state.ownership.founder_preview.mode || "").toLowerCase() === "proposed"
    );
  }

  function renderSentimentScorecards(overview) {
    if (!overview || !overview.length) return "";
    return (
      '<div class="hiw-rep-scorecards" data-hiw-rep-scorecards="1" data-hiw-rep-overview-count="' +
      esc(String(overview.length)) +
      '">' +
      overview
        .map(function (p) {
          var available =
            p.available === true ||
            (p.available !== false && (p.rating != null || p.listingUrl));
          if (!available) {
            return (
              '<article class="hiw-rep-scorecard hiw-rep-scorecard--unavailable" data-platform="' +
              esc(p.platform || "") +
              '" data-available="0">' +
              '<header class="hiw-rep-scorecard__head">' +
              '<h4 class="hiw-rep-scorecard__platform">' +
              esc(p.platformLabel || p.platform) +
              "</h4>" +
              "</header>" +
              '<p class="hiw-rep-scorecard__status" role="status">' +
              esc(p.statusLabel || "Data unavailable") +
              "</p>" +
              "</article>"
            );
          }
          var rating =
            p.rating != null
              ? '<span class="hiw-rep-scorecard__rating">' +
                esc(Number(p.rating).toFixed(1)) +
                "</span>"
              : '<span class="hiw-rep-scorecard__rating hiw-rep-scorecard__rating--na">—</span>';
          var scale =
            p.rating != null && p.scaleMax != null
              ? '<span class="hiw-rep-scorecard__scale">/ ' + esc(String(p.scaleMax)) + "</span>"
              : "";
          var link = p.listingUrl
            ? '<a class="hiw-sentiment-link" href="' +
              esc(p.listingUrl) +
              '" target="_blank" rel="noopener noreferrer">Open listing</a>'
            : "";
          return (
            '<article class="hiw-rep-scorecard" data-platform="' +
            esc(p.platform || "") +
            '" data-available="1">' +
            '<header class="hiw-rep-scorecard__head">' +
            '<h4 class="hiw-rep-scorecard__platform">' +
            esc(p.platformLabel || p.platform) +
            "</h4>" +
            link +
            "</header>" +
            '<div class="hiw-rep-scorecard__score" aria-label="Native rating">' +
            rating +
            scale +
            "</div>" +
            '<div class="hiw-rep-scorecard__meta">' +
            '<span class="hiw-rep-scorecard__reviews">' +
            (p.reviewCount != null
              ? esc(formatCount(p.reviewCount)) + " reviews"
              : "Review count unavailable") +
            "</span>" +
            '<span class="hiw-rep-scorecard__fresh">' +
            (p.observedAt ? "Observed " + esc(p.observedAt) : "Freshness unknown") +
            "</span>" +
            "</div>" +
            "</article>"
          );
        })
        .join("") +
      "</div>" +
      '<p class="hiw-prose hiw-prose--quiet hiw-rep-scale-note">Native platform scales and review populations differ — scores are never averaged into a combined index.</p>'
    );
  }

  function renderCategoryBars(categories) {
    if (!categories || !categories.length) {
      return '<p class="hiw-prose hiw-prose--quiet">No category scores for this platform.</p>';
    }
    var sorted = categories.slice().sort(function (a, b) {
      var pa = a.pct != null ? a.pct : -1;
      var pb = b.pct != null ? b.pct : -1;
      return pb - pa;
    });
    return (
      '<ul class="hiw-rep-bars" role="list">' +
      sorted
        .map(function (c) {
          var pct = c.pct != null ? Math.max(0, Math.min(100, c.pct)) : 0;
          return (
            '<li class="hiw-rep-bar">' +
            '<div class="hiw-rep-bar__label">' +
            '<span>' +
            esc(c.label) +
            "</span>" +
            '<span class="hiw-rep-bar__score">' +
            esc(c.scoreDisplay || "—") +
            "</span>" +
            "</div>" +
            '<div class="hiw-rep-bar__track" role="presentation">' +
            '<div class="hiw-rep-bar__fill" style="width:' +
            pct +
            '%"></div>' +
            "</div>" +
            "</li>"
          );
        })
        .join("") +
      "</ul>"
    );
  }

  function renderCategoryPerformance(cp) {
    if (!cp || cp.available !== true || !(cp.platforms || []).length) return "";
    var active =
      cp.defaultPlatform ||
      (cp.platforms[0] && cp.platforms[0].platform) ||
      "";
    var tabs = cp.platforms
      .map(function (p) {
        var isOn = p.platform === active;
        return (
          '<button type="button" class="hiw-rep-cat-tab' +
          (isOn ? " is-active" : "") +
          '" data-hiw-rep-cat-platform="' +
          esc(p.platform) +
          '" aria-pressed="' +
          (isOn ? "true" : "false") +
          '">' +
          esc(p.platformLabel || p.platform) +
          "</button>"
        );
      })
      .join("");
    var panels = cp.platforms
      .map(function (p) {
        var isOn = p.platform === active;
        return (
          '<div class="hiw-rep-cat-panel" data-hiw-rep-cat-panel="' +
          esc(p.platform) +
          '"' +
          (isOn ? "" : " hidden") +
          ">" +
          renderCategoryBars(p.categories) +
          "</div>"
        );
      })
      .join("");
    return (
      '<div class="hiw-rep-block" data-hiw-rep-category="1">' +
      '<div class="hiw-rep-block__head">' +
      '<h4 class="hiw-section__h hiw-section__h--sub">Category Performance</h4>' +
      '<p class="hiw-prose hiw-prose--quiet">' +
      esc(
        cp.note ||
          "Platform-published category scores — not review-theme frequency counts."
      ) +
      "</p>" +
      "</div>" +
      '<div class="hiw-rep-cat-tabs" role="tablist" aria-label="Category platform">' +
      tabs +
      "</div>" +
      panels +
      "</div>"
    );
  }

  function renderExecutiveInsightItems(items) {
    if (!items || !items.length) return "";
    return (
      '<ul class="hiw-rep-insight__items">' +
      items
        .slice(0, 3)
        .map(function (it) {
          return (
            "<li>" +
            '<span class="hiw-rep-insight__item-title">' +
            esc(it.title || it.label || "") +
            "</span>" +
            (it.detail || it.explanation
              ? '<span class="hiw-rep-insight__item-detail">' +
                esc(it.detail || it.explanation) +
                "</span>"
              : "") +
            (it.metric
              ? '<span class="hiw-rep-insight__item-metric">' + esc(it.metric) + "</span>"
              : "") +
            "</li>"
          );
        })
        .join("") +
      "</ul>"
    );
  }

  function renderExecutiveInsights(cards) {
    if (!cards || !cards.length) return "";
    return (
      '<div class="hiw-rep-block" data-hiw-rep-insights="1">' +
      '<h4 class="hiw-section__h hiw-section__h--sub">Guest Experience Insights</h4>' +
      '<div class="hiw-rep-insights hiw-rep-insights--executive hiw-rep-insights--compact">' +
      cards
        .map(function (c) {
          var metric = c.metric || null;
          var evidenceBody = c.evidence && c.evidence !== metric ? c.evidence : null;
          var itemsHtml = renderExecutiveInsightItems(c.items);
          return (
            '<article class="hiw-rep-insight" data-insight="' +
            esc(c.id || "") +
            '"' +
            (c.role ? ' data-role="' + esc(c.role) + '"' : "") +
            ">" +
            '<h5 class="hiw-rep-insight__title">' +
            esc(c.title) +
            "</h5>" +
            (itemsHtml ||
              '<p class="hiw-rep-insight__finding">' + esc(c.finding || "") + "</p>") +
            (!itemsHtml && metric
              ? '<p class="hiw-rep-insight__metric">' + esc(metric) + "</p>"
              : "") +
            (c.implication
              ? '<p class="hiw-rep-insight__impl">' + esc(c.implication) + "</p>"
              : "") +
            (evidenceBody
              ? '<details class="hiw-rep-insight__more"><summary>Evidence</summary><p>' +
                esc(evidenceBody) +
                "</p></details>"
              : "") +
            (c.confidence
              ? '<p class="hiw-rep-insight__conf">' + esc(c.confidence) + "</p>"
              : "") +
            "</article>"
          );
        })
        .join("") +
      "</div></div>"
    );
  }

  function renderReviewThemes(rt) {
    if (!rt || rt.available !== true || !(rt.items || []).length) return "";
    return (
      '<div class="hiw-rep-block" data-hiw-rep-themes="1">' +
      '<h4 class="hiw-section__h hiw-section__h--sub">Review Theme Analysis</h4>' +
      '<ul class="hiw-sentiment-theme-list">' +
      rt.items
        .map(function (t) {
          return (
            "<li><strong>" +
            esc(t.label || t.theme) +
            "</strong>" +
            (t.sampleCount != null
              ? ' <span class="hiw-sentiment-theme-ev">(' +
                esc(String(t.sampleCount)) +
                ")</span>"
              : "") +
            (t.summary ? " — " + esc(t.summary) : "") +
            "</li>"
          );
        })
        .join("") +
      "</ul></div>"
    );
  }

  function renderReputationTrends(trends) {
    var available = trends && trends.available === true;
    return (
      '<div class="hiw-rep-block" data-hiw-rep-trends="1">' +
      '<h4 class="hiw-section__h hiw-section__h--sub">Reputation Trends</h4>' +
      (available
        ? '<p class="hiw-prose">Historical observation windows are available for trend comparison.</p>'
        : '<p class="hiw-rep-trend-status" role="status">Trend data not yet available — 30 / 90 / 180 / 365-day windows require additional historical observations. This snapshot is not projected into a chart.</p>') +
      "</div>"
    );
  }

  function renderSentimentMethodology(payload) {
    var lim = (payload.limitations || [])
      .slice(0, 8)
      .map(function (l) {
        return "<li>" + esc(l) + "</li>";
      })
      .join("");
    var overview = payload.platformOverview || [];
    var links = overview
      .filter(function (p) {
        return p && p.listingUrl;
      })
      .map(function (p) {
        return (
          "<li><a class=\"hiw-sentiment-link\" href=\"" +
          esc(p.listingUrl) +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(p.platformLabel || p.platform) +
          "</a>" +
          (p.observedAt ? " · observed " + esc(p.observedAt) : "") +
          "</li>"
        );
      })
      .join("");
    var coverage =
      overview.length
        ? '<p class="hiw-prose hiw-prose--quiet">Coverage: ' +
          esc(
            String(
              overview.filter(function (p) {
                return p.available === true;
              }).length
            )
          ) +
          " of " +
          esc(String(overview.length)) +
          " catalog platforms with native listing scores. Unavailable cards are not zero ratings.</p>"
        : "";
    if (!lim && !links && !coverage) return "";
    return (
      '<details class="hiw-sentiment-details hiw-rep-methodology">' +
      "<summary>Sources, Methodology &amp; Limitations</summary>" +
      '<div class="hiw-sentiment-insights">' +
      coverage +
      (payload.insights && payload.insights.interpretationNote
        ? '<p class="hiw-prose hiw-prose--quiet">' +
          esc(payload.insights.interpretationNote) +
          "</p>"
        : "") +
      '<p class="hiw-prose hiw-prose--quiet">Platform scores use native scales and review populations — they are never averaged. Category scores are platform-published aggregates, not review-theme frequency. Trend windows require historical observations.</p>' +
      (links
        ? '<h5 class="hiw-rep-methodology__h">Review source links</h5><ul class="hiw-sentiment-followups">' +
          links +
          "</ul>"
        : "") +
      (lim
        ? '<h5 class="hiw-rep-methodology__h">Limitations</h5><ul class="hiw-sentiment-limits">' +
          lim +
          "</ul>"
        : "") +
      '<p class="hiw-prose hiw-prose--quiet">Listing URLs also appear in Sources &amp; Evidence under Guest Sentiment. Licensing notes above stay secondary to the executive assessment.</p>' +
      "</div></details>"
    );
  }

  function reputationOverviewCards(payload) {
    if (!payload) return [];
    if (payload.platformOverview && payload.platformOverview.length) {
      return payload.platformOverview;
    }
    return payload.platforms || [];
  }

  function hasReputationOverview(payload) {
    return !!(
      payload &&
      payload.visible === true &&
      reputationOverviewCards(payload).length
    );
  }

  function renderGuestSentimentSection(payload) {
    if (!payload || payload.visible !== true) return "";
    var overview = reputationOverviewCards(payload);
    if (!overview.length) return "";

    return (
      '<section class="hiw-section hiw-section--guest-sentiment hiw-section--rep-dashboard" data-hiw-guest-sentiment="1" data-hiw-rep-dashboard="1">' +
      '<div class="hiw-rep-dash-head">' +
      sectionHeadHtml(
        "Guest Reputation &amp; Experience",
        navLinkHtml("View Sources", {
          "data-hiw-tab": "sources",
          "data-hiw-source-filter": "sentiment",
        })
      ) +
      (payload.rightsChip
        ? '<p class="hiw-chip hiw-chip--proposed">' + esc(payload.rightsChip) + "</p>"
        : "") +
      "</div>" +
      renderSentimentScorecards(overview) +
      renderCategoryPerformance(payload.categoryPerformance) +
      renderExecutiveInsights(payload.executiveInsights) +
      renderReviewThemes(payload.reviewThemes) +
      renderSentimentMethodology(payload) +
      "</section>"
    );
  }

  function bindGuestSentimentDashboard(root) {
    if (!root || root.__hiwRepBound) return;
    root.__hiwRepBound = true;
    root.addEventListener("click", function (ev) {
      var btn = ev.target.closest("[data-hiw-rep-cat-platform]");
      if (!btn || !root.contains(btn)) return;
      var plat = btn.getAttribute("data-hiw-rep-cat-platform");
      if (!plat) return;
      var tabs = root.querySelectorAll("[data-hiw-rep-cat-platform]");
      for (var i = 0; i < tabs.length; i++) {
        var on = tabs[i].getAttribute("data-hiw-rep-cat-platform") === plat;
        tabs[i].classList.toggle("is-active", on);
        tabs[i].setAttribute("aria-pressed", on ? "true" : "false");
      }
      var panels = root.querySelectorAll("[data-hiw-rep-cat-panel]");
      for (var j = 0; j < panels.length; j++) {
        panels[j].hidden = panels[j].getAttribute("data-hiw-rep-cat-panel") !== plat;
      }
    });
  }

  function fetchGuestSentiment(opts, fetchOpts) {
    opts = opts || {};
    fetchOpts = fetchOpts || {};
    var id = state.hotel && recordId(state.hotel);
    if (!id) return Promise.resolve(null);
    var params = new URLSearchParams();
    if (state.hotel.name) params.set("name", state.hotel.name);
    if (opts.founderPreview || isFounderProposedPreview()) {
      params.set("mode", "proposed");
      params.set("founderPreview", "1");
    }
    var url =
      "/api/hotel-explorer/hotels/" +
      encodeURIComponent(id) +
      "/guest-sentiment?" +
      params.toString();
    var init = { headers: { "ngrok-skip-browser-warning": "true" } };
    if (fetchOpts.signal) init.signal = fetchOpts.signal;
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () {
        return null;
      });
    });
  }

  function paintGuestSentimentHost(host, result) {
    if (!host || !host.isConnected) return;
    var st = result && result.state;
    if (st === "LOADING") {
      host.hidden = false;
      host.innerHTML =
        '<section class="hiw-section he-async-loading" data-hiw-guest-sentiment-loading="1">' +
        '<h3 class="hiw-section__h">Guest Reputation &amp; Experience</h3>' +
        heWaveCompact("Loading guest reputation…") +
        "</section>";
      return;
    }
    if (st === "ERROR") {
      host.hidden = false;
      host.innerHTML =
        '<section class="hiw-section" data-hiw-guest-sentiment-error="1">' +
        '<h3 class="hiw-section__h">Guest Reputation &amp; Experience</h3>' +
        heAsyncError(
          result.message || "Guest reputation is temporarily unavailable.",
          "guest-sentiment"
        ) +
        "</section>";
      return;
    }
    if (st === "STALE" && result.payload && result.payload.visible === true) {
      applyGuestSentimentEvidencePayload(result.payload);
      host.hidden = false;
      host.innerHTML =
        renderGuestSentimentSection(result.payload) +
        '<p class="hiw-prose hiw-prose--quiet he-async-stale">Showing last available sentiment while refresh continues.</p>';
      bindGuestSentimentDashboard(host.querySelector("[data-hiw-rep-dashboard]"));
      return;
    }
    var payload = result && result.payload;
    if (!hasReputationOverview(payload)) {
      host.hidden = true;
      host.innerHTML = "";
      return;
    }
    applyGuestSentimentEvidencePayload(payload);
    host.hidden = false;
    host.innerHTML = renderGuestSentimentSection(payload);
    bindGuestSentimentDashboard(host.querySelector("[data-hiw-rep-dashboard]"));
  }

  function mountGuestSentiment(opts) {
    opts = opts || {};
    var host = document.querySelector("[data-hiw-guest-sentiment-host]");
    if (!isGuestSentimentEnabled()) {
      if (host) {
        host.hidden = true;
        host.innerHTML = "";
      }
      if (sentimentAsync) sentimentAsync.cancel();
      return;
    }
    if (!host || !state.hotel) return;
    var id = recordId(state.hotel);
    if (!id) {
      host.hidden = true;
      host.innerHTML = "";
      if (sentimentAsync) sentimentAsync.cancel();
      return;
    }

    if (!sentimentAsync) {
      host.hidden = false;
      host.innerHTML =
        '<section class="hiw-section">' + heWaveCompact("Loading guest reputation…") + "</section>";
      fetchGuestSentiment(opts)
        .then(function (payload) {
          paintGuestSentimentHost(host, {
            state: hasReputationOverview(payload) ? "READY" : "EMPTY",
            payload: payload,
          });
        })
        .catch(function () {
          paintGuestSentimentHost(host, {
            state: "ERROR",
            message: "Guest reputation is temporarily unavailable.",
          });
        });
      return;
    }

    if (opts.force === true && opts.bypassStale === true) {
      sentimentAsync.clearCache(id);
    }

    sentimentAsync.load(
      id,
      function (ticket) {
        return fetchGuestSentiment(opts, { signal: ticket && ticket.signal });
      },
      {
        force: opts.force === true,
        isEmpty: function (payload) {
          return !hasReputationOverview(payload);
        },
        onLoading: function () {
          paintGuestSentimentHost(host, { state: "LOADING" });
        },
        onResult: function (result) {
          paintGuestSentimentHost(host, result);
        },
      }
    );
  }

  function renderSourcesTab() {
    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Sources &amp; Evidence</h2>' +
      '<p class="hiw-report__subtitle">One hotel-specific register for research evidence. Published dates stay distinct from verification or access dates. Open a source link or Evidence to inspect what it supports.</p>' +
      '<div class="he-async-slot" data-hiw-sources-ma-slot="1" hidden></div>' +
      renderSourcesList() +
      "</article>"
    );
  }

  function renderRail(hotel, ownership) {
    var cites = state.citations || [];
    var ctrl =
      (ownership && ownership.report && ownership.report.ownership_and_control) || {};
    var econ = ctrl.economic_owner_or_group || {};
    var ownerKnown = econ.known === true && econ.name;
    var evidenceHtml =
      '<div class="hiw-evidence' +
      (state.evidenceOpen ? " is-open" : "") +
      '" id="hiwEvidence">' +
      (state.evidenceOpen ? renderEvidenceDetail(state.evidenceOpen) : "<p>Select a citation to inspect evidence.</p>") +
      "</div>";

    return (
      '<div class="hiw-rail__block"><h3>Hotel</h3>' +
      '<div class="hiw-rail__row"><span>Name</span><span>' +
      esc(hotel.name || "—") +
      "</span></div>" +
      '<div class="hiw-rail__row"><span>Market</span><span>' +
      esc(hotel.market || "—") +
      "</span></div>" +
      '<div class="hiw-rail__row"><span>Rooms</span><span>' +
      esc(hotel.rooms != null ? hotel.rooms : "—") +
      "</span></div></div>" +
      '<div class="hiw-rail__block"><h3>Key relationships</h3>' +
      '<div class="hiw-rail__row"><span>Owner</span><span>' +
      esc(ownerKnown ? econ.name : "Not yet verified") +
      "</span></div>" +
      '<div class="hiw-rail__row"><span>Operator</span><span>' +
      esc(
        (ctrl.operator && ctrl.operator.name) ||
          hotel.managementCompany ||
          "—"
      ) +
      "</span></div>" +
      '<div class="hiw-rail__row"><span>Brand</span><span>' +
      esc(
        (ownership && ownership.hotel && ownership.hotel.brand_display) ||
          hotel.brand ||
          "—"
      ) +
      "</span></div></div>" +
      '<div class="hiw-rail__block"><h3>Source status</h3>' +
      '<div class="hiw-rail__row"><span>Citations</span><span>' +
      cites.length +
      "</span></div>" +
      '<div class="hiw-rail__row"><span>Management</span><span>High</span></div>' +
      '<div class="hiw-rail__row"><span>Ownership</span><span>' +
      esc(ownerKnown ? friendlyStatus(econ.status || "HIGH") : "Not verified") +
      "</span></div></div>" +
      "<h3>Evidence</h3>" +
      evidenceHtml
    );
  }

  function renderEvidenceDetail(cite) {
    if (!cite) return "<p>No citation selected.</p>";
    var rel = cite.relationship;
    var relLabel =
      rel && rel.relationship_type
        ? relTypeLabel(rel.relationship_type)
        : "";
    return (
      "<h4>Evidence [" +
      (state.evidenceOpenIndex || "") +
      "]</h4>" +
      "<p><strong>Claim</strong> — " +
      esc(cite.claim || cite.title) +
      "</p>" +
      (rel
        ? "<p><strong>Relationship</strong> — " +
          esc(rel.subject || "") +
          " → " +
          esc(relLabel) +
          " → " +
          esc(rel.object || "") +
          "</p>"
        : "") +
      "<p><strong>Source</strong> — " +
      esc(cite.title) +
      "</p>" +
      (cite.url
        ? '<p><a href="' +
          esc(cite.url) +
          '" target="_blank" rel="noopener noreferrer">Open source · ' +
          esc(shortUrlLabel(cite.url)) +
          "</a></p>"
        : "") +
      (cite.publication_date
        ? "<p><strong>Published</strong> — " + esc(formatSourceDate(cite.publication_date)) + "</p>"
        : "") +
      (cite.verified_at
        ? "<p><strong>Last verified</strong> — " + esc(formatSourceDate(cite.verified_at)) + "</p>"
        : "") +
      (cite.accessed_at && cite.accessed_at !== cite.verified_at
        ? "<p><strong>Accessed</strong> — " + esc(formatSourceDate(cite.accessed_at)) + "</p>"
        : "") +
      (cite.publisher || cite.provider
        ? "<p><strong>Publisher</strong> — " + esc(cite.publisher || cite.provider) + "</p>"
        : "") +
      (cite.verification_status
        ? "<p><strong>Verification</strong> — " + esc(friendlyStatus(cite.verification_status)) + "</p>"
        : "") +
      (cite.excerpt ? "<p>" + esc(cite.excerpt) + "</p>" : "") +
      "<p><strong>Supports</strong> — " +
      esc(cite.proves || "") +
      "</p>" +
      '<p class="hiw-prove-not"><strong>Does not establish</strong> — ' +
      esc(cite.does_not || "") +
      "</p>"
    );
  }

  function prepareTabNav(opts) {
    opts = opts || {};
    if (opts.sourceFilter) state.sourceFilter = opts.sourceFilter;
    if (opts.sourceFocus != null && opts.sourceFocus !== "") {
      state.sourceFocusN = Number(opts.sourceFocus) || opts.sourceFocus;
    }
  }

  function normalizeHiwTab(tabId) {
    var raw = String(tabId || "").trim();
    if (TAB_ALIASES[raw]) raw = TAB_ALIASES[raw];
    var f = guestSentimentFlag();
    if (f && typeof f.normalizeTabOrFallback === "function") {
      return f.normalizeTabOrFallback(raw, "ownership");
    }
    if (!isGuestSentimentEnabled() && (raw === "sentiment" || raw === "brand")) {
      return "ownership";
    }
    return raw || "ownership";
  }

  function setTab(tabId, opts) {
    opts = opts || {};
    prepareTabNav(opts);
    state.tab = normalizeHiwTab(tabId);
    paint();
  }

  function highlightFocusedSourceRow() {
    if (state.tab !== "sources" || state.sourceFocusN == null) return;
    var n = state.sourceFocusN;
    var row =
      document.getElementById("hiw-source-" + n) ||
      document.querySelector('[data-hiw-source-row="' + n + '"]');
    if (!row) return;
    row.classList.add("is-source-focus");
    try {
      row.scrollIntoView({ block: "center", behavior: "smooth" });
    } catch (_err) {
      row.scrollIntoView(true);
    }
    openCitation(n);
  }

  function openCitation(n) {
    var idx = Number(n) - 1;
    var cite = (state.citations || [])[idx];
    if (!cite) return;
    state.evidenceOpen = cite;
    state.evidenceOpenIndex = n;
    var rail = document.getElementById("hiwRail");
    if (rail) {
      rail.innerHTML = renderRail(state.hotel, state.ownership);
      rail.scrollTop = Math.max(0, rail.scrollHeight - rail.clientHeight);
    }
    var ev = document.getElementById("hiwEvidence");
    if (ev) {
      ev.classList.add("is-open");
      ev.innerHTML = renderEvidenceDetail(cite);
      ev.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
  }

  function paint() {
    var root = ensureRoot();
    root.classList.add("is-open");
    var crumb = document.getElementById("hiwCrumb");
    if (crumb) {
      crumb.textContent = state.hotel && state.hotel.name
        ? "Radar · " + state.hotel.name
        : "Radar · Hotel Intelligence";
    }
    document.getElementById("hiwHeader").innerHTML = renderHeader(state.hotel, state.ownership);
    document.getElementById("hiwTabs").innerHTML = renderTabs();
    var main = "";
    if (state.tab === "overview") main = renderOverview(state.hotel, state.ownership);
    else if (state.tab === "ownership") main = renderOwnershipReport(state.hotel, state.ownership);
    else if (state.tab === "organization")
      main = renderOrganization(state.hotel, state.ownership, state.org);
    else if (state.tab === "relationships")
      main = renderRelationships(state.hotel, state.ownership);
    else if (state.tab === "market") main = renderMarket(state.hotel);
    else if (state.tab === "demand") main = renderDemand(state.hotel);
    else if (state.tab === "sentiment" || state.tab === "brand")
      main = renderGuestSentimentTab(state.hotel, state.ownership);
    else if (state.tab === "sources") main = renderSourcesTab();
    document.getElementById("hiwMain").innerHTML = main;
    document.getElementById("hiwRail").innerHTML = renderRail(state.hotel, state.ownership);
    var mainEl = document.getElementById("hiwMain");
    if (mainEl) mainEl.scrollTop = 0;
    if (state.tab === "ownership") mountOwnershipControlNetwork();
    if ((state.tab === "sentiment" || state.tab === "brand") && isGuestSentimentEnabled()) {
      mountHotelNewsArticles();
      mountGuestSentiment();
    }
    if (state.tab === "sources") {
      mountMarketAlertsEvidenceForSources();
      window.setTimeout(highlightFocusedSourceRow, 60);
    }
  }

  function onRootClick(e) {
    var opToggle = e.target.closest("[data-hex-op-toggle]");
    if (opToggle) {
      var group = opToggle.closest("[data-hex-op-group]");
      if (group) {
        var openNow = !group.classList.contains("is-open");
        group.classList.toggle("is-open", openNow);
        opToggle.setAttribute("aria-expanded", openNow ? "true" : "false");
        var arrow = opToggle.querySelector(".hex-op-group__arrow");
        if (arrow) arrow.textContent = openNow ? "▾" : "▸";
        var body = group.querySelector(".hex-op-group__body");
        if (body) {
          if (openNow) body.removeAttribute("hidden");
          else body.setAttribute("hidden", "");
        }
      }
      return;
    }
    var back = e.target.closest("[data-hiw-back]");
    if (back) {
      close();
      return;
    }
    var tab = e.target.closest("[data-hiw-tab]");
    if (tab) {
      setTab(tab.getAttribute("data-hiw-tab"), {
        sourceFilter: tab.getAttribute("data-hiw-source-filter") || undefined,
        sourceFocus: tab.getAttribute("data-hiw-source-focus") || undefined,
      });
      return;
    }
    var sourceFilt = e.target.closest("[data-hiw-source-filter]");
    if (sourceFilt && !sourceFilt.getAttribute("data-hiw-tab")) {
      state.sourceFilter = sourceFilt.getAttribute("data-hiw-source-filter") || "all";
      state.sourceFocusN = null;
      paint();
      return;
    }
    var cite = e.target.closest("[data-hiw-cite]");
    if (cite) {
      openCitation(cite.getAttribute("data-hiw-cite"));
      return;
    }
    var newsAll = e.target.closest("[data-hiw-news-view-all]");
    if (newsAll) {
      mountHotelNewsArticles({ viewAll: true });
      return;
    }
    var asyncRetry = e.target.closest("[data-he-async-retry]");
    if (asyncRetry) {
      var which = asyncRetry.getAttribute("data-he-async-retry") || "";
      if (which === "hotel-news") mountHotelNewsArticles({ force: true });
      else if (which === "guest-sentiment") mountGuestSentiment({ force: true });
      return;
    }
    var filt = e.target.closest("[data-hiw-rel-filter]");
    if (filt) {
      state.relFilter = filt.getAttribute("data-hiw-rel-filter") || "all";
      if (state.tab === "relationships" || state.tab === "ownership") paint();
      return;
    }
    var relRow = e.target.closest("[data-hiw-rel-row]");
    if (relRow && state.tab === "ownership") {
      var edgeKey = relRow.getAttribute("data-hiw-rel-row") || "";
      document.querySelectorAll(".ocn-edge").forEach(function (el) {
        el.classList.toggle("is-highlighted", false);
      });
      document.querySelectorAll("[data-hiw-rel-row]").forEach(function (el) {
        el.classList.toggle("is-highlighted", el === relRow);
      });
      if (edgeKey) {
        var matchEdge = Array.prototype.find.call(
          document.querySelectorAll(".ocn-edge"),
          function (el) {
            var id = el.getAttribute("data-ocn-edge") || "";
            return id.indexOf(edgeKey) !== -1 || edgeKey.indexOf(id) !== -1;
          }
        );
        if (matchEdge) matchEdge.classList.add("is-highlighted");
      }
      return;
    }
    var hotelBtn = e.target.closest("[data-hiw-hotel]");
    if (hotelBtn) {
      var hid = hotelBtn.getAttribute("data-hiw-hotel");
      var ownedList =
        (state.org && state.org.owned_controlled_hotels) ||
        (state.ownership &&
          state.ownership.organization &&
          state.ownership.organization.owned_controlled_hotels) ||
        [];
      var match =
        ownedList.find(function (h) {
          return (h.airtable_record_id || h.id) === hid;
        }) ||
        (state.org &&
          state.org.portfolio &&
          state.org.portfolio.find(function (h) {
            return h.airtable_record_id === hid;
          })) ||
        null;
      if (window.HotelExplorer && typeof window.HotelExplorer.open === "function") {
        window.HotelExplorer.open({
          id: hid,
          recordId: hid,
          name: (match && (match.hotel_name || match.name)) || hid,
          city: match && (match.city || match.market),
          market: match && (match.market || match.city),
          rooms: match && match.rooms,
          brand: match && (match.brand_display || match.brand),
        });
        return;
      }
      if (match) {
        open(
          {
            id: match.airtable_record_id,
            name: match.name,
            city: match.city,
            market: match.market || match.city,
            rooms: match.rooms_display != null ? match.rooms_display : match.rooms,
            brand: match.brand_display,
            managementCompany:
              (state.ownership &&
                state.ownership.organization &&
                state.ownership.organization.display_name) ||
              undefined,
          },
          { tab: "overview" }
        );
      }
      return;
    }
  }

  function fetchOwnership(hotel) {
    var id = recordId(hotel);
    if (!id) return Promise.resolve(null);
    return fetch("/api/golden-demo/ownership/hotel/" + encodeURIComponent(id), {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (r) {
        return r.json();
      })
      .catch(function () {
        return null;
      });
  }

  function fetchOrg(slug) {
    return fetch("/api/golden-demo/ownership/groups/" + encodeURIComponent(slug || "grupo-hotelero-santa-fe"), {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (r) {
        return r.json();
      })
      .catch(function () {
        return null;
      });
  }

  function fetchHotelOwnerAnchor(hotelId) {
    return fetch(
      "/api/owner-intelligence/hotels/" + encodeURIComponent(hotelId) + "/owner-anchor",
      { headers: { "ngrok-skip-browser-warning": "true" } }
    )
      .then(function (r) {
        return r.json();
      })
      .catch(function () {
        return null;
      });
  }

  function fetchOwnerPortfolio(ownerId, focusHotelId) {
    if (!ownerId) return Promise.resolve(null);
    var q = focusHotelId ? "?focusHotel=" + encodeURIComponent(focusHotelId) : "";
    return fetch(
      "/api/owner-intelligence/owners/" + encodeURIComponent(ownerId) + "/portfolio" + q,
      { headers: { "ngrok-skip-browser-warning": "true" } }
    )
      .then(function (r) {
        return r.json();
      })
      .catch(function () {
        return null;
      });
  }

  /** Prefer org-level owner portfolio over cohort arrays when available. */
  function orgPayloadFromOwnerPortfolio(ownerApi, focusHotelId) {
    if (!ownerApi || !ownerApi.success || !ownerApi.profile) return null;
    var p = ownerApi.profile;
    var owned = (ownerApi.buckets && ownerApi.buckets.owned_controlled) || [];
    var managed = (ownerApi.buckets && ownerApi.buckets.operated_managed) || [];
    function mapRow(h) {
      return {
        name: h.hotel_name,
        hotel: h.hotel_name,
        id: h.airtable_record_id || null,
        airtable_record_id: h.airtable_record_id || null,
        market: h.market,
        market_display: h.market,
        rooms: h.rooms,
        brand: h.brand,
        brand_display: h.brand,
        relationship_type: h.relationship_type,
        economic_owner_verified: Boolean(h.include_as_owned),
        verification_bucket: h.confidence,
        why_in_portfolio: h.why_in_portfolio,
        census_match_status: h.census_match_status,
        focus_hotel: Boolean(h.focus_hotel) || h.airtable_record_id === focusHotelId,
      };
    }
    return {
      ok: true,
      source: "owner_portfolio_org_level",
      group: {
        display_name: p.owner_display_name,
        legal_name: p.owner_legal_name,
        entity_id: p.owner_entity_id,
        known_hotel_relationships: (p.hotel_relationships || []).length,
        known_rooms: (p.hotel_relationships || []).reduce(function (s, h) {
          return s + (Number(h.rooms) || 0);
        }, 0),
        markets: p.markets || [],
        evidence_summary:
          (p.customer_label || "Known Portfolio") +
          " · owned/controlled " +
          owned.length +
          " · managed " +
          managed.length,
        relationship_summary: {
          total_relationships: (p.hotel_relationships || []).length,
          owned_or_controlled_verified: owned.length,
          operated_or_managed: managed.length,
        },
      },
      portfolio: owned.map(mapRow),
      owned_controlled_hotels: owned.map(mapRow),
      portfolio_kpis: {
        coverage_label: p.customer_label || "Verified Portfolio Coverage",
        completeness: p.completeness || p.portfolio_status || "PARTIAL",
      },
      owner_explorer_path:
        "/owner-explorer.html?owner=" +
        encodeURIComponent(p.owner_entity_id) +
        (focusHotelId ? "&focusHotel=" + encodeURIComponent(focusHotelId) : ""),
      related_entities: p.related_entities || [],
      why_this_matters: {
        text:
          "Organization-level portfolio from Owner Control Graph. Who Manages These Hotels? groups only the owned/controlled set — operator-only hotels are excluded from these totals.",
      },
      _from_owner_portfolio: true,
    };
  }

  function open(hotel, opts) {
    opts = opts || {};
    if (!hotel) return;
    if (newsAsync) newsAsync.cancel();
    if (sentimentAsync) sentimentAsync.cancel();
    if (sourcesMaAsync) sourcesMaAsync.cancel();
    state.marketAlertsCitations = [];
    state.radarSnapshot = snapshotRadar();
    state.hotel = hotel;
    state.tab = normalizeHiwTab(opts.tab || "ownership");
    state.ownership = null;
    state.org = null;
    state.citations = [];
    state.evidenceOpen = null;
    state.open = true;
    ensureRoot();
    paint();
    document.body.style.overflow = "hidden";
    // Close compact modal if open — workspace is the deep layer
    if (window.HotelDetailPanel && typeof window.HotelDetailPanel.close === "function") {
      try {
        window.HotelDetailPanel.close();
      } catch (err) {
        console.warn("[HotelIntelligenceWorkspace] close compact panel failed", err);
      }
    }

    var hotelId = recordId(hotel);
    Promise.all([
      fetchOwnership(hotel),
      fetchOrg("grupo-hotelero-santa-fe"),
      fetchHotelOwnerAnchor(hotelId),
    ]).then(function (triple) {
      if (!state.open) return;
      state.ownership = triple[0];
      var legacyOrg = triple[1];
      var anchor = triple[2];
      var ownerId =
        (anchor &&
          anchor.anchor &&
          anchor.anchor.primary_owner_entity_id) ||
        null;
      return fetchOwnerPortfolio(ownerId, hotelId).then(function (ownerPort) {
        if (!state.open) return;
        state.org =
          orgPayloadFromOwnerPortfolio(ownerPort, hotelId) || legacyOrg;
        if (anchor && anchor.owner_explorer_path && state.org) {
          state.org.owner_explorer_path =
            state.org.owner_explorer_path || anchor.owner_explorer_path;
        }
        state.citations = buildCitations(hotel, state.ownership);
        paint();
      });
    });
  }

  function close() {
    var root = document.getElementById("hiwRoot");
    if (root) root.classList.remove("is-open");
    state.open = false;
    document.body.style.overflow = "";
    var snap = state.radarSnapshot;
    if (snap) {
      try {
        window.scrollTo(snap.scrollX || 0, snap.scrollY || 0);
      } catch (err) {
        console.warn("[HotelIntelligenceWorkspace] restore scroll failed", err);
      }
    }
  }

  function patchHotelDetailPanel() {
    if (window.__DEALALITY_HOTEL_EXPLORER_PRIMARY) return;
    if (!window.HotelDetailPanel || window.HotelDetailPanel.__hiwPatched) return;
    var originalOpen = window.HotelDetailPanel.open;
    window.HotelDetailPanel.open = function (hotel) {
      // Open workspace as primary Hotel Intelligence experience
      open(hotel, { tab: "ownership" });
    };
    window.HotelDetailPanel.openCompact = originalOpen;
    window.HotelDetailPanel.__hiwPatched = true;
  }

  // Disable Packet 2.2 modal ownership injection to avoid dual UIs
  if (window.OwnershipIntelligenceUI) {
    window.OwnershipIntelligenceUI.enhancePanel = function () {};
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", patchHotelDetailPanel);
  } else {
    patchHotelDetailPanel();
  }

  window.HotelIntelligenceWorkspace = {
    open: open,
    close: close,
    setTab: setTab,
    prepareTabNav: prepareTabNav,
    setRelFilter: function (value) {
      state.relFilter = value || "all";
    },
    setSourceFilter: function (value) {
      state.sourceFilter = value || "all";
    },
    getCitations: function () {
      return state.citations || [];
    },
    renderTabHtml: function (tabId, hotel, ownership, org) {
      state.hotel = hotel;
      state.ownership = ownership;
      state.org = org || state.org;
      state.citations = buildCitations(hotel, ownership);
      if (tabId === "overview" || tabId === "intelligence") return renderOverview(hotel, ownership);
      if (tabId === "hotel") return renderHotelOverviewTab(hotel, ownership);
      if (tabId === "ownership") return renderOwnershipReport(hotel, ownership);
      if (tabId === "organization") return renderOrganization(hotel, ownership, org);
      if (tabId === "relationships") return renderRelationships(hotel, ownership);
      if (tabId === "sentiment" || tabId === "brand")
        return renderGuestSentimentTab(hotel, ownership);
      if (tabId === "sources") return renderSourcesTab();
      if (tabId === "market" || tabId === "submarket") return renderSubmarketTab(hotel, ownership);
      if (tabId === "area") return renderAreaHotelsTab(hotel, ownership);
      if (tabId === "demand") return renderDemandTab(hotel, ownership);
      if (tabId === "access") return renderAccessTab(hotel, ownership);
      return "";
    },
    mountOwnershipControlNetwork: mountOwnershipControlNetwork,
    mountHotelNewsArticles: mountHotelNewsArticles,
    mountGuestSentiment: mountGuestSentiment,
    mountMarketAlertsEvidenceForSources: mountMarketAlertsEvidenceForSources,
    openCitation: openCitation,
    highlightFocusedSourceRow: highlightFocusedSourceRow,
    applyContactEvidenceCitations: applyContactEvidenceCitations,
  };
})();
