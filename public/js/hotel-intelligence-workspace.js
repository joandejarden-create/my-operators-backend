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
  };

  var TABS = [
    { id: "overview", label: "Overview" },
    { id: "ownership", label: "Ownership Intelligence" },
    { id: "organization", label: "Organization & Portfolio" },
    { id: "relationships", label: "Relationships" },
    { id: "market", label: "Market Intelligence" },
    { id: "demand", label: "Demand & Access" },
    { id: "brand", label: "Brand / Operator" },
    { id: "sources", label: "Sources & Evidence" },
  ];

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
    return "Source";
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
    if (deepSources.length) {
      deepSources.forEach(function (s) {
        list.push({
          title: s.title || s.provider || "Source",
          url: s.url || null,
          provider: s.provider || "",
          observed_date: s.observed_date || null,
          excerpt: s.evidence_excerpt || s.note || "",
          claim: (ownership.report && ownership.report.evidence && ownership.report.evidence.claim) || "",
          relationship: ownership.report && ownership.report.evidence && ownership.report.evidence.relationship,
          proves: ownership.report && ownership.report.evidence && ownership.report.evidence.what_it_proves,
          does_not: ownership.report && ownership.report.evidence && ownership.report.evidence.what_it_does_not_prove,
          supports: "ownership",
          source_type: sourceTypeLabel(s.provider || s.title),
        });
      });
      return list;
    }
    var ev = ownership && ownership.report && ownership.report.evidence;
    var sources = (ev && ev.sources) || [];
    sources.forEach(function (s) {
      list.push({
        title: s.title || s.provider || "Source",
        url: s.url || null,
        provider: s.provider || "",
        observed_date: s.observed_date || null,
        excerpt: s.evidence_excerpt || "",
        claim: (ev && ev.claim) || "",
        relationship: ev && ev.relationship,
        proves: ev && ev.what_it_proves,
        does_not: ev && ev.what_it_does_not_prove,
        supports: "operator",
        source_type: sourceTypeLabel(s.provider || s.title),
      });
    });
    list.push({
      title: "Dealality Hotel Census — affiliation & management fields",
      url: null,
      provider: "dealality_hotel_census",
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
    });
    return list;
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
  function snapSub(title, bodyHtml) {
    return (
      '<section class="hiw-snap-sub">' +
      (title ? '<h3 class="hiw-snap-sub__title">' + title + "</h3>" : "") +
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
    if (!list.length) {
      return (
        '<section class="hiw-section" data-ocn-section="decision-makers">' +
        '<h3 class="hiw-section__h">Relevant Decision-Makers</h3>' +
        '<p class="hiw-prose hiw-prose--quiet">No owner-side decision-makers verified yet. Open Contact Intelligence for property-level channels.</p>' +
        '<div class="hiw-actions"><button type="button" class="hiw-btn" data-hiw-tab="contacts">Open Contact Intelligence →</button></div>' +
        "</section>"
      );
    }
    return (
      '<section class="hiw-section" data-ocn-section="decision-makers">' +
      '<h3 class="hiw-section__h">Relevant Decision-Makers</h3>' +
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
      '<div class="hiw-actions"><button type="button" class="hiw-btn" data-hiw-tab="contacts">Open Contact Intelligence →</button></div>' +
      "</section>"
    );
  }

  function renderOwnershipSourcesStrip(ownership) {
    var cites = state.citations || [];
    if (!cites.length && ownership && ownership.airtable_graph) {
      return (
        '<section class="hiw-section" data-ocn-section="sources">' +
        '<h3 class="hiw-section__h">Sources and Verification</h3>' +
        '<p class="hiw-prose">Relationships use HI Graph Evidence. Open Sources &amp; Evidence for citations — internal source IDs are not shown here.</p>' +
        '<div class="hiw-actions"><button type="button" class="hiw-btn" data-hiw-tab="sources">Open Sources &amp; Evidence →</button></div>' +
        "</section>"
      );
    }
    return (
      '<section class="hiw-section" data-ocn-section="sources">' +
      '<h3 class="hiw-section__h">Sources and Verification</h3>' +
      (cites.length
        ? renderSourcesFlatTable(cites.slice(0, 8))
        : '<p class="hiw-prose hiw-prose--quiet">No citations attached yet.</p>') +
      '<div class="hiw-actions"><button type="button" class="hiw-btn" data-hiw-tab="sources">Open Sources &amp; Evidence →</button></div>' +
      "</section>"
    );
  }

  function renderOwnershipNetworkSection() {
    return (
      '<section class="hiw-section hiw-section--ocn" data-ocn-section="network">' +
      '<h3 class="hiw-section__h">Interactive Ownership &amp; Control Network</h3>' +
      '<p class="hiw-prose hiw-prose--quiet">Typed relationships only. Operator and brand are never drawn as ownership. Expand organizations on demand.</p>' +
      '<div class="ocn-host" data-ocn-host="1" aria-label="Ownership and control network"></div>' +
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
    window.HeOwnershipControlNetwork.mount(host, state.hotel, state.ownership, {
      includePortfolioPreview: false,
    });
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
        renderOwnershipSourcesStrip(ownership) +
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
      renderOwnershipSourcesStrip(ownership) +
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
    var url = String(
      (nested && nested.url) ||
        p.professional_profile_url ||
        ""
    ).trim();
    var type = String(
      (nested && nested.type) || p.professional_profile_type || ""
    ).trim();
    var verified =
      nested && typeof nested.verified === "boolean"
        ? nested.verified
        : p.professional_profile_verified === true;
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

  function renderPeopleGrouped(people) {
    if (!people || !people.length) {
      return '<div class="hiw-gap-box"><h4>Decision authority not yet verified</h4><p>No named individuals verified for this hotel.</p></div>';
    }
    var claimed = {};
    var html = "";
    PERSON_GROUP_ORDER.forEach(function (group, gi) {
      var members = [];
      people.forEach(function (p, idx) {
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
            th("Confidence") +
            th("Profile") +
            "</tr></thead><tbody>" +
            members
              .map(function (p) {
                var authNote = "";
                if (
                  /ceo|president|director general|chairman|vice.?chairman|development/i.test(
                    p.title || ""
                  )
                ) {
                  authNote =
                    '<div class="hiw-auth-subtle">Decision authority: not established</div>';
                }
                return (
                  "<tr><td>" +
                  esc(p.name) +
                  "</td><td>" +
                  esc(p.title) +
                  "</td><td>" +
                  esc(p.organization) +
                  "</td><td>" +
                  esc(
                    (p.strategic_relevance || p.relationship_to_hotel || "—").slice(0, 120)
                  ) +
                  authNote +
                  "</td><td>" +
                  esc(friendlyStatus(p.confidence)) +
                  "</td>" +
                  renderPeopleProfileCell(p) +
                  "</tr>"
                );
              })
              .join("") +
            "</tbody>",
          "hiw-table--people"
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

  function sourceTableRow(s, n) {
    return (
      "<tr>" +
      "<td>" +
      n +
      "</td>" +
      "<td>" +
      esc(s.title || "Untitled source") +
      "</td>" +
      "<td>" +
      esc(s.provider || "—") +
      "</td>" +
      "<td>" +
      esc(s.observed_date || "—") +
      "</td>" +
      "<td>" +
      '<button type="button" class="hiw-linkish" data-hiw-cite="' +
      n +
      '">Open Evidence</button>' +
      "</td>" +
      "</tr>"
    );
  }

  function sourcesTableHead() {
    return (
      "<thead><tr>" +
      th("#") +
      th("Source") +
      th("Publisher") +
      th("Date") +
      th("Evidence") +
      "</tr></thead>"
    );
  }

  /** One sorted Sources-style table (References chapter). */
  function renderSourcesFlatTable(cites) {
    if (!cites || !cites.length) {
      return '<p class="hiw-empty">No sources in this cohort.</p>';
    }
    var entries = cites
      .map(function (s, i) {
        return { s: s, n: i + 1 };
      })
      .sort(function (a, b) {
        return a.n - b.n;
      });
    var rows = entries
      .map(function (row) {
        return sourceTableRow(row.s, row.n);
      })
      .join("");
    return wrapTable(sourcesTableHead() + "<tbody>" + rows + "</tbody>", "hiw-table--sources");
  }

  function renderSourcesList() {
    var cites = state.citations || [];
    if (!cites.length) return '<p class="hiw-empty">No sources in this cohort.</p>';

    var buckets = {};
    var order = [];
    cites.forEach(function (s, i) {
      var key = toProperCase(s.source_type || sourceTypeLabel(s.provider || s.title));
      if (!buckets[key]) {
        buckets[key] = [];
        order.push(key);
      }
      buckets[key].push({ s: s, n: i + 1 });
    });

    order.sort(function (a, b) {
      return String(a).localeCompare(String(b));
    });

    return order
      .map(function (key) {
        var rows = buckets[key]
          .slice()
          .sort(function (a, b) {
            return a.n - b.n;
          })
          .map(function (row) {
            return sourceTableRow(row.s, row.n);
          })
          .join("");

        return (
          '<section class="hiw-section">' +
          '<h3 class="hiw-section__h">' +
          esc(key) +
          "</h3>" +
          wrapTable(sourcesTableHead() + "<tbody>" + rows + "</tbody>", "hiw-table--sources") +
          "</section>"
        );
      })
      .join("");
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

  function buildPortfolioOperatorView(ownedRows, ownerName, coverageMeta, scope) {
    var api = window.HeOwnerPortfolioOperatorView;
    if (api && typeof api.buildOwnerPortfolioOperatorView === "function") {
      return api.buildOwnerPortfolioOperatorView(ownedRows, {
        ownerDisplayName: ownerName,
        // Coverage title is derived from selected scope (verified vs verified+proposed).
        completeness: (coverageMeta && coverageMeta.completeness) || "PARTIAL",
        scope: scope,
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
    var view = buildPortfolioOperatorView(ownedSeed, g.display_name, coverageMeta, scope);
    var ownedRows = view.ownedHotels || [];
    var partition = view.partition || {};

    var orgPeople =
      (ownership &&
        ownership.report &&
        ownership.report.decision_authority &&
        ownership.report.decision_authority.people) ||
      [];

    var framingNote =
      (orgPayload.why_this_matters && orgPayload.why_this_matters.text) ||
      (g.evidence_summary || "");
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
      renderPeopleGrouped(orgPeople.slice(0, 10)) +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Strategic Context</h3>' +
      '<div class="hiw-prose"><p>' +
      esc(
        (orgPayload.why_this_matters && orgPayload.why_this_matters.text) ||
          "Use Hotel → Owner → Portfolio and Owner → Hotels → Operators. Research one owner; reuse verified relationships across every connected hotel."
      ) +
      "</p></div></section>" +
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
    return snapSub("Relationship Detail", pills + tableHtml);
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
        '<div class="hiw-prose"><p>Ownership, operations, and brand are separate relationships. Package / economic owner naming is not the same as a verified Mexican PropCo or deed vehicle. Operator or brand is never drawn as owner.</p></div>' +
        renderRelationshipEdgesTable(ownership, state.relFilter) +
        '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--primary" data-hiw-org-network>Show Organization Network</button></div>' +
        '<div id="hiwOrgNetwork" hidden></div>' +
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
        '<div class="hiw-prose"><p>Economic owner and property company are held back when unverified — the operator is not relabeled as owner.</p></div>' +
        renderRelationshipEdgesTable(ownership, state.relFilter) +
        '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--primary" data-hiw-org-network>Show Organization Network</button></div>' +
        '<div id="hiwOrgNetwork" hidden></div>' +
        "</section>";

    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Relationships &amp; Structure</h2>' +
      '<p class="hiw-report__subtitle">Hotel structure first · organization network on demand</p>' +
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

  function renderBrandOperator(hotel, ownership) {
    var brand =
      (ownership && ownership.hotel && ownership.hotel.brand_display) ||
      hotel.brand ||
      hotel.affiliation ||
      "—";
    var brandStatus =
      (ownership && ownership.hotel && ownership.hotel.brand_display_status) || "";
    var contested = String(brandStatus) === "CONTESTED";
    var op =
      (ownership &&
        ownership.report &&
        ownership.report.ownership_and_control &&
        ownership.report.ownership_and_control.operator &&
        ownership.report.ownership_and_control.operator.name) ||
      hotel.managementCompany ||
      "—";
    var chrono = (ownership && ownership.report && ownership.report.brand_chronology) || [];
    var byStatus = function (st) {
      return chrono.filter(function (b) {
        return String(b.status || "").toUpperCase() === st;
      });
    };
    var brandCards = "";
    ["CURRENT", "FORMER", "ANNOUNCED", "PLANNED", "CONTESTED", "CANCELLED"].forEach(function (st) {
      var items = byStatus(st);
      if (!items.length && st === "CURRENT" && brand && brand !== "—") {
        items = [{ label: brand, brand: brand, date: "", status: "CURRENT" }];
      }
      if (!items.length) return;
      brandCards +=
        '<div class="hiw-brand-card hiw-brand-card--' +
        esc(st.toLowerCase()) +
        '"><div class="hiw-brand-card__status">' +
        esc(friendlyStatus(st)) +
        "</div><ul>" +
        items
          .map(function (b) {
            return (
              "<li><strong>" +
              esc(b.label || b.brand) +
              "</strong>" +
              (b.date ? " · " + esc(b.date) : "") +
              "</li>"
            );
          })
          .join("") +
        "</ul></div>";
    });

    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Brand / Operator</h2>' +
      '<p class="hiw-report__subtitle">Current, former, and announced brand states · operating relationship</p>' +
      '<section class="hiw-section"><h3 class="hiw-section__h">Franchise &amp; Brand Status</h3>' +
      (brandCards ||
        wrapTable(
          "<tbody>" +
            row("Current affiliation", brand + (brandStatus ? " · " + brandStatus : "")) +
            "</tbody>"
        )) +
      (contested
        ? '<div class="hiw-prose"><p>Trading presentation and affiliation evidence remain unresolved. Announced projects are not treated as completed reflags.' +
          citeBtn(2) +
          citeBtn(3) +
          "</p></div>"
        : '<div class="hiw-prose"><p>Announced brand relationships are shown separately from current affiliation.</p></div>') +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Current Operator</h3>' +
      wrapTable(
        "<tbody>" +
          row("Management company", op) +
          row("Relationship", "Operated / Managed By") +
          row("Confidence", "High") +
          "</tbody>"
      ) +
      '<div class="hiw-prose"><p>Operator evidence is tracked separately from economic ownership.' +
      citeBtn(1) +
      "</p></div></section></article>"
    );
  }

  function renderSourcesTab() {
    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Sources &amp; Evidence</h2>' +
      '<p class="hiw-report__subtitle">Click a citation in Ownership or Open Evidence here to inspect what the source supports and what it does not establish.</p>' +
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
      (cite.observed_date ? "<p>Observed " + esc(cite.observed_date) + "</p>" : "") +
      (cite.excerpt ? "<p>" + esc(cite.excerpt) + "</p>" : "") +
      "<p><strong>Supports</strong> — " +
      esc(cite.proves || "") +
      "</p>" +
      '<p class="hiw-prove-not"><strong>Does not establish</strong> — ' +
      esc(cite.does_not || "") +
      "</p>"
    );
  }

  function setTab(tabId) {
    state.tab = tabId;
    paint();
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
    else if (state.tab === "brand")
      main = renderBrandOperator(state.hotel, state.ownership);
    else if (state.tab === "sources") main = renderSourcesTab();
    document.getElementById("hiwMain").innerHTML = main;
    document.getElementById("hiwRail").innerHTML = renderRail(state.hotel, state.ownership);
    var mainEl = document.getElementById("hiwMain");
    if (mainEl) mainEl.scrollTop = 0;
    if (state.tab === "ownership") mountOwnershipControlNetwork();
  }

  function loadOrgNetwork() {
    var box = document.getElementById("hiwOrgNetwork");
    if (!box) return;
    box.hidden = false;
    box.innerHTML = '<p class="hiw-empty">Loading organization network…</p>';
    var org =
      (state.ownership && (state.ownership.organization || state.ownership.ownership_group)) ||
      {};
    var orgId = org.entity_id || org.slug || "dle_06G6AB1VK0BCCD94DNN7W8DRWZ";
    fetch(
      "/api/golden-demo/ownership/graph/neighbors?node_id=" +
        encodeURIComponent(orgId) +
        "&node_type=organization&limit=12&relationship_categories=ownership,operator,brand,other&include_probable=1",
      { headers: { "ngrok-skip-browser-warning": "true" } }
    )
      .then(function (r) {
        return r.json().then(function (data) {
          return { okHttp: r.ok, data: data };
        });
      })
      .then(function (pack) {
        var data = pack && pack.data;
        if (!pack.okHttp || !data || data.success === false || !Array.isArray(data.nodes) || !data.nodes.length) {
          box.innerHTML = '<p class="hiw-empty">Unable to load network.</p>';
          return;
        }
        box.innerHTML =
          '<h3 class="hiw-section__h" style="margin-top:18px">Organization Network</h3>' +
          '<div class="hiw-prose"><p>Organization-centered hotel relationships. Edge labels use product language (Operates / Owns) — not graph schema keys.</p></div>' +
          drawMiniNetwork(data);
      })
      .catch(function () {
        box.innerHTML = '<p class="hiw-empty">Network failed to load.</p>';
      });
  }

  function drawMiniNetwork(payload) {
    var width = 640;
    var height = 380;
    var cx = width / 2;
    var cy = height / 2;
    var nodes = payload.nodes || [];
    var edges = payload.edges || [];
    var center = nodes.find(function (n) {
      return n.type === "organization";
    }) || nodes[0];
    var positions = {};
    if (center) positions[center.id] = { x: cx, y: cy, r: 36 };
    var hotels = nodes.filter(function (n) {
      return n.type === "hotel";
    });
    hotels.forEach(function (n, i) {
      var angle = (Math.PI * 2 * i) / Math.max(hotels.length, 1) - Math.PI / 2;
      positions[n.id] = {
        x: cx + Math.cos(angle) * 140,
        y: cy + Math.sin(angle) * 140,
        r: 22,
      };
    });
    var edgeSvg = edges
      .map(function (e) {
        var a = positions[e.from];
        var b = positions[e.to];
        if (!a || !b) return "";
        return (
          '<g class="ofg-edge"><line x1="' +
          a.x +
          '" y1="' +
          a.y +
          '" x2="' +
          b.x +
          '" y2="' +
          b.y +
          '"></line>' +
          '<text class="ofg-edge-label" x="' +
          (a.x + b.x) / 2 +
          '" y="' +
          ((a.y + b.y) / 2 - 6) +
          '" text-anchor="middle">' +
          esc(
            e.relationship_label ||
              (/OWN|CONTROL/i.test(String(e.relationship_type || ""))
                ? "Owns"
                : /BRAND|FRANCHIS/i.test(String(e.relationship_type || ""))
                  ? "Brands"
                  : "Operates")
          ) +
          "</text></g>"
        );
      })
      .join("");
    var nodeSvg = nodes
      .filter(function (n) {
        return positions[n.id];
      })
      .map(function (n) {
        var p = positions[n.id];
        var label = String(n.label || "").slice(0, 16);
        return (
          '<g class="ofg-node ofg-node--' +
          esc(n.type) +
          '" transform="translate(' +
          p.x +
          "," +
          p.y +
          ')"><circle r="' +
          p.r +
          '"></circle><text text-anchor="middle" y="' +
          (p.r + 12) +
          '">' +
          esc(label) +
          "</text></g>"
        );
      })
      .join("");
    return (
      '<svg class="hiw-graph-svg" viewBox="0 0 ' +
      width +
      " " +
      height +
      '">' +
      edgeSvg +
      nodeSvg +
      "</svg>" +
      (payload.pagination && payload.pagination.truncated
        ? '<p class="hiw-teaser">Showing first relationships — expand in-tab for more.</p>'
        : "")
    );
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
      setTab(tab.getAttribute("data-hiw-tab"));
      return;
    }
    var cite = e.target.closest("[data-hiw-cite]");
    if (cite) {
      openCitation(cite.getAttribute("data-hiw-cite"));
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
    var net = e.target.closest("[data-hiw-org-network]");
    if (net) {
      loadOrgNetwork();
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
    state.radarSnapshot = snapshotRadar();
    state.hotel = hotel;
    state.tab = opts.tab || "ownership";
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
    loadOrgNetwork: loadOrgNetwork,
    setRelFilter: function (value) {
      state.relFilter = value || "all";
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
      if (tabId === "brand") return renderBrandOperator(hotel, ownership);
      if (tabId === "sources") return renderSourcesTab();
      if (tabId === "market" || tabId === "submarket") return renderSubmarketTab(hotel, ownership);
      if (tabId === "area") return renderAreaHotelsTab(hotel, ownership);
      if (tabId === "demand") return renderDemandTab(hotel, ownership);
      if (tabId === "access") return renderAccessTab(hotel, ownership);
      return "";
    },
    mountOwnershipControlNetwork: mountOwnershipControlNetwork,
    openCitation: openCitation,
  };
})();
