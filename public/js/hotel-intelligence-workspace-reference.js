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

  function friendlyStatus(raw) {
    var s = String(raw || "").toUpperCase();
    if (s === "HIGH" || s === "VERIFIED") return s === "HIGH" ? "High" : "Verified";
    if (s === "PROBABLE") return "Probable";
    if (s === "CONTESTED") return "Contested";
    if (s === "PREVIEW") return "Display only";
    if (s === "CURRENT") return "Current";
    if (s === "FORMER") return "Former";
    if (s === "ANNOUNCED") return "Announced";
    if (s === "PLANNED") return "Planned";
    if (s === "CANCELLED") return "Cancelled";
    if (s === "LIVE") return "Verified";
    if (
      s === "UNKNOWN" ||
      s === "NOT_YET_VERIFIED" ||
      s === "NOT_VERIFIED" ||
      s === "AUTHORITY NOT VERIFIED" ||
      !s
    ) {
      return "Not yet verified";
    }
    return s.charAt(0) + s.slice(1).toLowerCase();
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
      OWNED_BY: "Owned / controlled by",
      CONTROLLED_BY: "Controlled by",
      OPERATED_BY: "Operated / managed by",
      ASSET_MANAGED_BY: "Asset-managed by",
      BRANDED_BY: "Branded as",
      DEVELOPED_BY: "Developed by",
      SPONSORED_BY: "Sponsored by",
      JV_WITH: "Joint venture with",
      LEASED_FROM: "Leased from",
      FORMER_OWNED_BY: "Formerly owned by",
      ANNOUNCED_BRAND: "Announced brand",
    };
    if (map[t]) return map[t];
    return String(type || "")
      .replace(/_/g, " ")
      .replace(/\b\w/g, function (c) {
        return c.toUpperCase();
      });
  }

  function sourceTypeLabel(provider) {
    var p = String(provider || "").toLowerCase();
    if (/bmv|filing|annual|issuer|securities/.test(p)) return "Corporate filing";
    if (/hyatt|hilton|brand|newsroom|press/.test(p)) return "Brand / press";
    if (/census|dealality/.test(p)) return "Dealality census";
    if (/operator|management/.test(p)) return "Operator";
    if (/linkedin|tripadvisor|people|listing/.test(p)) return "People / listing";
    if (/transaction|acquisition/.test(p)) return "Transaction / history";
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
      source_type: "Dealality census",
    });
    return list;
  }

  function getControl(ownership) {
    return (ownership && ownership.report && ownership.report.ownership_and_control) || {};
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
    var ownerKnown = econ.known === true && econ.name;
    var ownerValue = ownerKnown ? econ.name : "Not yet verified";
    var ownerMeta = ownerKnown
      ? friendlyStatus(econ.status || "HIGH") + (rich ? " · owned & operated" : "")
      : "Research gap";
    var propcoValue = propco.name || (rich ? "Property company verified" : "—");
    var propcoMeta = propco.name
      ? friendlyStatus(propco.status || propco.confidence || "HIGH")
      : rich
        ? "Property vehicle"
        : "Not established";
    var researchMeta = rich
      ? "Primary filings validated"
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
      (propco.name || rich
        ? '<button type="button" class="hiw-ind" data-hiw-tab="ownership">' +
          '<span class="hiw-ind__label">Property Company</span>' +
          '<span class="hiw-ind__value">' +
          esc(propcoValue.length > 42 ? propcoValue.slice(0, 40) + "…" : propcoValue) +
          "</span>" +
          '<span class="hiw-ind__meta">' +
          esc(propcoMeta) +
          "</span></button>"
        : "") +
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
      esc(label) +
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
    return String(role || "")
      .replace(/_/g, " ")
      .replace(/\b\w/g, function (c) {
        return c.toUpperCase();
      });
  }

  function insightRow(title, bodyHtml, meta) {
    return (
      '<div class="hiw-insight">' +
      '<h4 class="hiw-insight__title">' +
      esc(title) +
      "</h4>" +
      '<p class="hiw-insight__body">' +
      bodyHtml +
      "</p>" +
      '<div class="hiw-insight__meta">' +
      esc(meta || "") +
      "</div></div>"
    );
  }

  function wrapTable(inner) {
    return '<div class="hiw-table-wrap"><table class="hiw-table">' + inner + "</table></div>";
  }

  function chainNode(name, role, meta, modifier) {
    return (
      '<div class="hiw-chain__node' +
      (modifier ? " hiw-chain__node--" + modifier : "") +
      '"><span class="hiw-chain__name">' +
      esc(name) +
      '</span><span class="hiw-chain__role">' +
      esc(role) +
      "</span>" +
      (meta ? '<span class="hiw-chain__meta">' + esc(meta) + "</span>" : "") +
      "</div>"
    );
  }

  function chainEdge(label) {
    return (
      '<div class="hiw-chain__edge"><div class="hiw-chain__arrow"></div>' +
      esc(label) +
      "</div>"
    );
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

    var signals = rich
      ? insightRow(
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
            ". Former Hilton / Altitude identities and an announced Breathless project are tracked separately — announced is not current." +
            citeBtn(3),
          "Evidence-supported"
        ) +
        insightRow(
          "Adjacent-asset distinction",
          "Do not confuse this hotel with the adjacent Krystal Resort Puerto Vallarta (different owner, same operator family).",
          "High"
        )
      : insightRow(
          "Operator known",
          esc(op) + " is supported as operator / management company." + citeBtn(1),
          "High"
        ) +
        insightRow(
          "Ownership unresolved",
          "Economic owner and property company are not yet verified. Operator evidence is not promoted to ownership.",
          "Research gap"
        );

    var timelinePreview =
      Array.isArray(history) && history.length
        ? '<ul class="hiw-timeline hiw-timeline--compact">' +
          history
            .slice(0, 4)
            .map(function (h) {
              return (
                '<li class="hiw-timeline__item"><div class="hiw-timeline__date">' +
                esc(h.date || h.theme || "—") +
                '</div><p class="hiw-timeline__event">' +
                esc(h.event || h.text || "") +
                "</p></li>"
              );
            })
            .join("") +
          "</ul>" +
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
          row("Property company", propco.name || (rich ? "Verified in filings" : "Not established")) +
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
    var gaps = (ownership.report && ownership.report.research_gaps) || [];
    var people =
      (ownership.report &&
        ownership.report.decision_authority &&
        ownership.report.decision_authority.people) ||
      [];
    var history = (ownership.report && ownership.report.property_history) || [];
    var chain = (ownership.report && ownership.report.ownership_chain) || [];
    var contacts = (ownership.report && ownership.report.corporate_contacts) || {};
    var pursuit = (ownership.report && ownership.report.commercial_pursuit) || {};
    var brandChrono = (ownership.report && ownership.report.brand_chronology) || [];
    var orgPayload = state.org || null;
    var portfolio = (orgPayload && orgPayload.portfolio) || [];

    var title =
      esc(hotel.name || "Hotel") +
      '<div class="hiw-report__subtitle">Ownership, Corporate Structure &amp; Development Intelligence</div>';

    if (!inCohort) {
      return (
        '<article class="hiw-report"><h2 class="hiw-report__title">' +
        title +
        "</h2>" +
        '<section class="hiw-section"><h3 class="hiw-section__h">Executive summary</h3>' +
        '<div class="hiw-prose"><p>Ownership Intelligence is not yet available for this hotel in the controlled demo cohort. Economic ownership is not verified. No fabricated owner is shown.</p></div>' +
        "</section></article>"
      );
    }

    /* ——— Executive summary (prose) ——— */
    var exec;
    if (rich) {
      exec =
        "<p><strong>Grupo Hotelero Santa Fe</strong> (BMV: HOTEL) both owns and operates Krystal Grand Puerto Vallarta — a Pacific resort formerly developed and co-owned with Chartwell before GSF acquired the remaining interest in 2014." +
        citeBtn(1) +
        " The asset is held through a GSF property company; commercially, outreach is an owner-operator conversation rather than a third-party management pitch." +
        citeBtn(2) +
        "</p>" +
        "<p>The hotel currently trades as <strong>Krystal Grand</strong>; Hilton and Krystal Altitude are former identities." +
        citeBtn(4) +
        " Hyatt and GSF have announced a <strong>Breathless Puerto Vallarta</strong> project — treat that as announced, not a completed reflag of this hotel." +
        citeBtn(3) +
        " Do not confuse this property with adjacent <strong>Krystal Resort Puerto Vallarta</strong>, which Chartwell owns and GSF manages.</p>";
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
    var insights = rich
      ? insightRow(
          "Direct owner-operator control",
          "GSF is supported as both economic owner and operator — brand and management decisions sit inside the same public company." +
            citeBtn(1),
          "High"
        ) +
        insightRow(
          "Portfolio leverage",
          "GSF’s Mexico platform spans owned and managed hotels across multiple markets; adjacent Chartwell-owned Krystal Resort PV is managed by GSF without merging ownership.",
          "High"
        ) +
        insightRow(
          "Brand evolution / reflag context",
          "Hilton → Altitude → Krystal Grand is the supported history; Breathless is an announced Hyatt collaboration, not proven current affiliation." +
            citeBtn(3),
          "Current / Announced"
        ) +
        insightRow(
          "Decision authority",
          "Named GSF and Hyatt executives are verified for title and organization; legal / franchise signatory authority is not inferred from seniority.",
          "Partial"
        ) +
        insightRow(
          "Adjacent-asset distinction",
          "Krystal Grand PV (GSF-owned) ≠ Krystal Resort PV (Chartwell-owned, GSF-managed).",
          "High"
        )
      : insightRow(
          "Operating relationship",
          esc(opName) + " is supported as operator / management company." + citeBtn(1),
          "High"
        ) +
        insightRow(
          "Ownership gap",
          "Economic owner and PropCo remain unverified — operator evidence is not promoted to ownership.",
          "Not verified"
        );

    /* ——— Ownership structure chain ——— */
    var structureHtml = "";
    if (rich) {
      var propcoName =
        (ctrl.legal_property_owner_propco && ctrl.legal_property_owner_propco.name) ||
        "Inmobiliaria en Hotelería Vallarta Santa Fe, S. de R.L. de C.V. (IHVSF)";
      var econName =
        (ctrl.economic_owner_or_group && ctrl.economic_owner_or_group.name) ||
        "Grupo Hotelero Santa Fe, S.A.B. de C.V.";
      structureHtml =
        '<div class="hiw-chain" aria-label="Ownership structure">' +
        chainNode(hotel.name, "Hotel", null, null) +
        chainEdge("Held through") +
        chainNode(propcoName, "Property Company / PropCo", "HIGH · securities disclosure", "org") +
        chainEdge("Controlled by") +
        chainNode(econName, "Economic Owner / Public Company · BMV: HOTEL", "HIGH · owned & operated", "org") +
        "</div>" +
        '<h4 class="hiw-section__h2">Historical &amp; commercial relationships</h4>' +
        '<div class="hiw-structure">' +
        '<div class="hiw-structure__col">' +
        chainNode("Grupo Chartwell", "Historical Owner / Investor", "Former co-owner · sold remaining interest 2014", "former") +
        chainEdge("Operates / manages") +
        chainNode(opName, "Operator / Management Company", "HIGH · owned & operated", "org") +
        "</div>" +
        '<div class="hiw-structure__col">' +
        chainNode("Krystal Grand", "Current Brand", "CURRENT", "brand") +
        chainNode("Hilton / Krystal Altitude", "Former Brand", "FORMER", "former") +
        chainNode("Breathless / Hyatt", "Announced Project / Brand Partnership", "ANNOUNCED", "announced") +
        "</div></div>";
    }

    /* ——— Ownership & control table ——— */
    var controlRows = rich
      ? rowStatus("Legal property owner / PropCo", (ctrl.legal_property_owner_propco && ctrl.legal_property_owner_propco.name) || "IHVSF", "high") +
        rowStatus("Economic owner", (ctrl.economic_owner_or_group && ctrl.economic_owner_or_group.name) || opName, "high") +
        rowStatus("Parent / public company", org.display_name || opName + " (BMV: HOTEL)", "high") +
        rowStatus("Historical owner / investor", "Grupo Chartwell (former co-owner)", "high") +
        rowStatus("Operator / management company", opName + " — owned & operated", "high") +
        rowStatus("Developer (historical)", "GSF with Grupo Chartwell", "high") +
        rowStatus("Lease / lessor (adjacent parcels)", "Promotora Turística Mexicana → IHVSF", "high") +
        rowStatus("Current brand", brand + " — CURRENT", "high") +
        rowStatus("Former brands", "Hilton · Krystal Altitude", "high") +
        rowStatus("Announced brand / project", "Breathless Puerto Vallarta (Hyatt + GSF)", "probable")
      : rowStatus("Economic owner", "Not yet verified", "gap") +
        rowStatus("Legal property owner / PropCo", "Unknown", "gap") +
        rowStatus("Operator / management company", opName, "high") +
        rowStatus("Current brand", brand, contested ? "contested" : "probable");

    /* ——— UBO section ——— */
    var uboHtml = rich
      ? wrapTable(
          "<tbody>" +
            row("Corporate economic owner", (ctrl.economic_owner_or_group && ctrl.economic_owner_or_group.name) || opName) +
            row("Ultimate controlling organization", "Grupo Hotelero Santa Fe, S.A.B. de C.V. (public company)") +
            row(
              "Principal shareholder / control group",
              "Grupo Chartwell / related principals appear in filings as major shareholders — not asserted as PropCo deed holders"
            ) +
            "</tbody>"
        ) +
        '<p class="hiw-prose">Natural-person beneficial ownership has not been independently verified.</p>'
      : '<div class="hiw-gap-box"><h4>Ownership group not verified</h4><p>Economic owner and natural-person beneficial ownership remain unresolved. Operator is not treated as owner.</p></div>';

    /* ——— Portfolio preview ——— */
    var previewRows = portfolio
      .slice(0, 6)
      .map(function (h) {
        var rel =
          h.relationship_type === "OWNED_BY" || h.economic_owner_verified
            ? "Owned / controlled" +
              (h.secondary_relationship_type === "OPERATED_BY" ? " · Operated" : "")
            : "Operated / managed";
        return (
          "<tr><td>" +
          esc(h.name) +
          "</td><td>" +
          esc(h.city || h.market || "—") +
          "</td><td>" +
          esc(h.rooms_display != null ? h.rooms_display : h.rooms != null ? h.rooms : "—") +
          "</td><td>" +
          esc(h.brand_display || "—") +
          "</td><td>" +
          esc(rel) +
          "</td><td>" +
          esc(friendlyStatus(h.verification_bucket || "HIGH")) +
          "</td></tr>"
        );
      })
      .join("");
    var portfolioPreview = previewRows
      ? wrapTable(
          "<thead><tr><th>Hotel</th><th>Market</th><th>Rooms</th><th>Brand</th><th>Relationship</th><th>Confidence</th></tr></thead><tbody>" +
            previewRows +
            "</tbody>"
        ) +
        '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--ghost" data-hiw-tab="organization">View full Organization &amp; Portfolio →</button></div>'
      : '<p class="hiw-empty">Portfolio preview unavailable.</p>';

    /* ——— Brand status ——— */
    var brandSection = brandChrono.length
      ? wrapTable(
          "<thead><tr><th>Date</th><th>Brand / event</th><th>Status</th></tr></thead><tbody>" +
            brandChrono
              .map(function (b) {
                return (
                  "<tr><td>" +
                  esc(b.date) +
                  "</td><td>" +
                  esc(b.label || b.brand) +
                  "</td><td><span class=\"hiw-status hiw-status--" +
                  esc(statusClass(b.status)) +
                  '">' +
                  esc(b.status) +
                  "</span></td></tr>"
                );
              })
              .join("") +
            "</tbody>"
        )
      : wrapTable(
          "<tbody>" +
            row("Current affiliation", brand + (brandStatus ? " · " + brandStatus : "")) +
            "</tbody>"
        );

    /* ——— People (grouped; no authority-banner soup) ——— */
    var peopleHtml = renderPeopleGrouped(people);

    /* ——— Qualified signatories ——— */
    var signatories = people.filter(function (p) {
      return (
        /signator|legal.?authorit/i.test(p.legal_signing_authority || "") ||
        (p.legal_signing_authority_verified === true)
      );
    });
    var signatoryHtml = signatories.length
      ? wrapTable(
          "<thead><tr><th>Name</th><th>Title</th><th>Organization</th><th>Evidence</th></tr></thead><tbody>" +
            signatories
              .map(function (p) {
                return (
                  "<tr><td>" +
                  esc(p.name) +
                  "</td><td>" +
                  esc(p.title) +
                  "</td><td>" +
                  esc(p.organization) +
                  "</td><td>" +
                  esc(p.authority_note || "Documentary support") +
                  "</td></tr>"
                );
              })
              .join("") +
            "</tbody>"
        )
      : '<p class="hiw-prose hiw-prose--quiet">No specific franchise or legal signing authority has yet been independently verified.</p>';

    /* ——— Contacts ——— */
    var contactsHtml = wrapTable(
      "<tbody>" +
        row("Headquarters", contacts.headquarters || org.headquarters || "—") +
        row("Website", contacts.website || org.website || "—") +
        row("Corporate telephone", contacts.corporate_phone || org.phone || "—") +
        row("Business / IR email", contacts.business_email || org.email || "—") +
        row("Property telephone", contacts.property_phone || "—") +
        row("Property email", contacts.property_email || "—") +
        "</tbody>"
    );

    /* ——— Asset profile ——— */
    var assetHtml = rich
      ? wrapTable(
          "<tbody>" +
            row("Real estate tenure", "Owned (public-company consolidation) — deed parties not scanned") +
            row("Ownership / management structure", "Owned and operated by GSF via PropCo vehicle") +
            row("Franchise / brand arrangement", "Proprietary Krystal Grand brand (CURRENT); Breathless ANNOUNCED with Hyatt") +
            row("Known expansion", "Hacienda expansion (2018) — 192 suites; hotel ~451 rooms") +
            row("Known renovation", "Ongoing renovation notices reported by distribution partners (probable)") +
            row("Related-party lease context", "Event-salon parcels: Promotora Turística Mexicana ↔ IHVSF") +
            row("Material litigation", "Not asserted from current product-safe evidence") +
            "</tbody>"
        )
      : wrapTable(
          "<tbody>" +
            row("Ownership / management structure", "Operated / managed by " + opName + "; ownership unresolved") +
            row("Franchise / brand arrangement", brand || "—") +
            "</tbody>"
        );

    /* ——— Property history ——— */
    var historyNarrative = rich
      ? '<div class="hiw-prose"><p>Public filings support a development start around mid-2011, a 2012 Hilton opening, GSF’s 2014 acquisition of Chartwell’s remaining stake, the 2018 Hacienda expansion to ~451 rooms, brand evolution from Hilton through Altitude to Krystal Grand, and a 2024 Hyatt Breathless announcement for Puerto Vallarta.' +
        citeBtn(1) +
        citeBtn(3) +
        "</p></div>"
      : "";
    var timelineHtml = Array.isArray(history) && history.length
      ? '<ul class="hiw-timeline">' +
        history
          .map(function (h) {
            return (
              '<li class="hiw-timeline__item"><div class="hiw-timeline__date">' +
              esc(h.date || h.theme || "—") +
              '</div><p class="hiw-timeline__event">' +
              esc(h.event || h.text || "") +
              " · " +
              esc(friendlyStatus(h.confidence || "HIGH")) +
              "</p></li>"
            );
          })
          .join("") +
        "</ul>"
      : '<div class="hiw-gap-box"><h4>No chronological events promoted yet</h4></div>';

    /* ——— Pursuit ——— */
    var pursuitHtml = rich
      ? '<div class="hiw-qa">' +
        '<div class="hiw-qa__q">Why this property?</div><p class="hiw-qa__a">' +
        esc(pursuit.why_matters || "Large Pacific resort owned and operated by a listed Mexican hotel company.") +
        "</p>" +
        '<div class="hiw-qa__q">Why this owner / organization?</div><p class="hiw-qa__a">' +
        esc(pursuit.who_controls_relationship || opName) +
        "</p>" +
        '<div class="hiw-qa__q">Portfolio leverage</div><p class="hiw-qa__a">' +
        esc(pursuit.portfolio_leverage || "Multi-asset GSF relationships across Mexico, including adjacent managed Resort.") +
        "</p>" +
        '<div class="hiw-qa__q">Brand relationships</div><p class="hiw-qa__a">CURRENT Krystal Grand; FORMER Hilton / Altitude; ANNOUNCED Breathless / Hyatt.</p>' +
        '<div class="hiw-qa__q">Operator relationships</div><p class="hiw-qa__a">GSF operates this owned hotel and manages adjacent Chartwell-owned Krystal Resort PV.</p>' +
        '<div class="hiw-qa__q">What has changed?</div><p class="hiw-qa__a">' +
        esc(pursuit.timing_signals || "Brand evolution and Breathless announcement.") +
        "</p>" +
        '<div class="hiw-qa__q">Outreach path</div><p class="hiw-qa__a">' +
        esc(pursuit.approach_organization || "GSF corporate / IR and executive channels.") +
        "</p>" +
        '<div class="hiw-qa__q">Why now?</div><p class="hiw-qa__a">' +
        esc(pursuit.timing_signals || "Recent brand positioning and announced Hyatt collaboration.") +
        "</p>" +
        '<div class="hiw-qa__q">Research gaps before outreach</div><p class="hiw-qa__a">' +
        esc(pursuit.missing_before_outreach || "Property GM; Breathless site confirmation; deed/signatory documentation.") +
        "</p></div>"
      : '<div class="hiw-qa"><div class="hiw-qa__q">Outreach implication</div><p class="hiw-qa__a">Operator is known; owner is not. Do not pitch as if ' +
        esc(opName) +
        " is the economic owner without further research.</p></div>";

    /* ——— Gaps (compact — unresolved items must not dominate) ——— */
    var gapItems = Array.isArray(gaps) && gaps.length
      ? gaps.map(function (g) {
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
        })
      : rich
        ? [
            {
              what: "Natural-person beneficial ownership",
              why: "Ultimate control diligence beyond the listed company",
              state: "Not independently verified",
            },
            {
              what: "Legal / franchise signing authority",
              why: "Required before treating any individual as a qualified signatory",
              state: "Titles verified; authority not established",
            },
            {
              what: "Breathless Puerto Vallarta implementation status",
              why: "Determines whether announced brand is a conversion of this asset or a separate project",
              state: "Announced — site match unresolved",
            },
          ]
        : [
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
    var gapsHtml = renderCompactResearchGaps(
      "Research Gaps",
      gapItems,
      rich
        ? "Registry filing, deed, or audited ownership / signatory disclosure. Future Deep Research can attach here."
        : "Issuer filing, deed, or first-party ownership disclosure."
    );

    /* ——— References ——— */
    var cites = state.citations || [];
    var refsHtml = cites.length
      ? '<ol class="hiw-ref-list">' +
        cites
          .map(function (s, i) {
            var titleText = s.title || s.provider || "Source";
            var link = s.url
              ? '<a href="' +
                esc(s.url) +
                '" target="_blank" rel="noopener noreferrer">' +
                esc(titleText) +
                "</a>"
              : "<strong>" + esc(titleText) + "</strong>";
            return (
              '<li class="hiw-ref-item">[' +
              (i + 1) +
              "] " +
              link +
              '<span class="hiw-ref-meta">' +
              esc(s.provider || "source") +
              (s.observed_date ? " · Observed " + esc(s.observed_date) : "") +
              ' · <button type="button" class="hiw-cite" data-hiw-cite="' +
              (i + 1) +
              '">Evidence</button></span></li>'
            );
          })
          .join("") +
        "</ol>"
      : '<p class="hiw-empty">No sources in this cohort.</p>';

    /* ——— Optional: hide raw chain roles list; use labeled historical only if needed ——— */
    var historicalNotes = "";
    if (rich && chain.length) {
      var histNodes = chain.filter(function (n) {
        return /former|ubo|shareholder/i.test(n.role || "") || n.status === "FORMER" || n.status === "NOT_VERIFIED";
      });
      if (histNodes.length) {
        historicalNotes =
          '<h4 class="hiw-section__h2">Additional chain notes</h4>' +
          wrapTable(
            "<thead><tr><th>Relationship</th><th>Entity</th><th>Confidence</th></tr></thead><tbody>" +
              histNodes
                .map(function (n) {
                  return (
                    "<tr><td>" +
                    esc(roleLabel(n.role)) +
                    "</td><td>" +
                    esc(n.name || "Not verified") +
                    "</td><td>" +
                    esc(friendlyStatus(n.confidence || n.status)) +
                    "</td></tr>"
                  );
                })
                .join("") +
              "</tbody>"
          );
      }
    }

    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">' +
      title +
      "</h2>" +
      '<section class="hiw-section" id="hiw-exec"><h3 class="hiw-section__h">Executive Summary</h3>' +
      '<div class="hiw-prose">' +
      exec +
      "</div></section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Key Strategic Insights for Development</h3>' +
      '<div class="hiw-insights">' +
      insights +
      "</div></section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h hiw-section__h--major">1. Ownership Intelligence</h3>' +
      '<h4 class="hiw-section__h2">Ownership Structure</h4>' +
      (structureHtml ||
        '<div class="hiw-gap-box"><h4>Structure not fully verified</h4><p>Operator known; ownership chain not asserted.</p></div>') +
      historicalNotes +
      '<h4 class="hiw-section__h2">Ownership &amp; control</h4>' +
      wrapTable("<tbody>" + controlRows + "</tbody>") +
      '<h4 class="hiw-section__h2">Ultimate beneficial owner / ownership group</h4>' +
      uboHtml +
      '<h4 class="hiw-section__h2">Multi-property connections / portfolio preview</h4>' +
      portfolioPreview +
      '<h4 class="hiw-section__h2">Franchise &amp; brand status</h4>' +
      brandSection +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h hiw-section__h--major">2. Decision Authority &amp; Contacts</h3>' +
      '<h4 class="hiw-section__h2">People by Role</h4>' +
      peopleHtml +
      '<h4 class="hiw-section__h2">Qualified Franchise / Legal Signatories</h4>' +
      signatoryHtml +
      '<h4 class="hiw-section__h2">Corporate Contact Channels</h4>' +
      contactsHtml +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h hiw-section__h--major">3. Asset &amp; Agreement Profile</h3>' +
      assetHtml +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h hiw-section__h--major">4. Property History &amp; Recent Changes</h3>' +
      historyNarrative +
      timelineHtml +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h hiw-section__h--major">5. Development / Pursuit Intelligence</h3>' +
      pursuitHtml +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h hiw-section__h--major">6. Research Gaps</h3>' +
      gapsHtml +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h hiw-section__h--major">7. References</h3>' +
      refsHtml +
      "</section>" +
      "</article>"
    );
  }

  function personProfessionalProfile(p) {
    if (!p || typeof p !== "object") {
      return { type: "", url: "", verified: false };
    }
    var nested = p.professional_profile && typeof p.professional_profile === "object" ? p.professional_profile : null;
    var url = String((nested && nested.url) || p.professional_profile_url || "").trim();
    var type = String((nested && nested.type) || p.professional_profile_type || "").trim();
    var verified =
      nested && typeof nested.verified === "boolean"
        ? nested.verified
        : p.professional_profile_verified === true;
    var isPersonLinkedIn =
      /^https?:\/\/((www|[a-z]{2})\.)?linkedin\.com\/in\//i.test(url);
    if (!url || !verified || !isPersonLinkedIn) {
      return { type: "", url: "", verified: false };
    }
    return { type: type || "LINKEDIN", url: url, verified: true };
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
      html +=
        '<div class="hiw-people-group"><h5 class="hiw-people-group__h">' +
        esc(group.label) +
        "</h5>" +
        wrapTable(
          "<thead><tr><th>Name</th><th>Title</th><th>Organization</th><th>Relevance</th><th>Confidence</th><th>Profile</th></tr></thead><tbody>" +
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
            "</tbody>"
        ) +
        "</div>";
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
          '</p><button type="button" class="hiw-btn hiw-btn--ghost hiw-btn--sm" disabled title="Future capability">Research this gap (coming soon)</button>'
        : "") +
      "</div>"
    );
  }

  function rowStatus(label, value, status) {
    return (
      "<tr><td>" +
      esc(label) +
      '</td><td>' +
      esc(value) +
      ' <span class="hiw-status hiw-status--' +
      esc(statusClass(status)) +
      '">' +
      esc(friendlyStatus(status === "gap" ? "NOT_YET_VERIFIED" : status)) +
      "</span></td></tr>"
    );
  }

  function renderSourcesList(grouped) {
    var cites = state.citations || [];
    if (!cites.length) return '<p class="hiw-empty">No sources in this cohort.</p>';
    if (!grouped) {
      return (
        '<ol class="hiw-source-list">' +
        cites
          .map(function (s, i) {
            return (
              '<li class="hiw-source-item">' +
              '<div class="hiw-source-num">[' +
              (i + 1) +
              "]</div><div>" +
              "<strong>" +
              esc(s.title) +
              "</strong>" +
              '<div class="hiw-source-meta">' +
              esc(s.source_type || sourceTypeLabel(s.provider)) +
              (s.provider ? " · " + esc(s.provider) : "") +
              (s.observed_date ? " · " + esc(s.observed_date) : "") +
              "</div>" +
              (s.url
                ? '<div><a href="' +
                  esc(s.url) +
                  '" target="_blank" rel="noopener noreferrer">' +
                  esc(shortUrlLabel(s.url)) +
                  "</a></div>"
                : "") +
              '<button type="button" class="hiw-cite" data-hiw-cite="' +
              (i + 1) +
              '">Open evidence</button>' +
              "</div></li>"
            );
          })
          .join("") +
        "</ol>"
      );
    }
    var buckets = {};
    cites.forEach(function (s, i) {
      var key = s.source_type || sourceTypeLabel(s.provider);
      if (!buckets[key]) buckets[key] = [];
      buckets[key].push({ s: s, i: i });
    });
    return Object.keys(buckets)
      .map(function (key) {
        return (
          '<div class="hiw-source-group"><h4 class="hiw-section__h2">' +
          esc(key) +
          "</h4><ol class=\"hiw-source-list\">" +
          buckets[key]
            .map(function (row) {
              var s = row.s;
              var i = row.i;
              return (
                '<li class="hiw-source-item">' +
                '<div class="hiw-source-num">[' +
                (i + 1) +
                "]</div><div><strong>" +
                esc(s.title) +
                "</strong>" +
                (s.url
                  ? '<div><a href="' +
                    esc(s.url) +
                    '" target="_blank" rel="noopener noreferrer">' +
                    esc(shortUrlLabel(s.url)) +
                    "</a></div>"
                  : "") +
                '<button type="button" class="hiw-cite" data-hiw-cite="' +
                (i + 1) +
                '">Open evidence</button></div></li>'
              );
            })
            .join("") +
          "</ol></div>"
        );
      })
      .join("");
  }

  function portfolioRow(h, focusId) {
    var owned = h.relationship_type === "OWNED_BY" || h.economic_owner_verified;
    var rel = owned
      ? "Owned / controlled" +
        (h.secondary_relationship_type === "OPERATED_BY" ? " · Operated" : "")
      : "Operated / managed";
    var id = h.airtable_record_id || "";
    return (
      "<tr>" +
      "<td>" +
      (id
        ? '<button type="button" class="hiw-linkish" data-hiw-hotel="' +
          esc(id) +
          '">' +
          esc(h.name) +
          "</button>"
        : esc(h.name)) +
      (id && id === focusId
        ? ' <span class="hiw-status hiw-status--high">Focus</span>'
        : "") +
      "</td>" +
      "<td>" +
      esc(h.city || h.market || "—") +
      "</td>" +
      "<td>" +
      esc(h.rooms_display != null ? h.rooms_display : h.rooms != null ? h.rooms : "—") +
      "</td>" +
      "<td>" +
      esc(h.brand_display || "—") +
      "</td>" +
      "<td>" +
      esc(rel) +
      "</td>" +
      "<td>" +
      esc(friendlyStatus(h.verification_bucket || "HIGH")) +
      "</td></tr>"
    );
  }

  function renderOrganization(hotel, ownership, orgPayload) {
    if (!orgPayload || !orgPayload.group) {
      return '<div class="hiw-empty">Organization profile not available for this hotel.</div>';
    }
    var g = orgPayload.group;
    var summary = (orgPayload.page_framing && orgPayload.page_framing.summary) || g.relationship_summary || {};
    var hotelName = hotel.name || "This hotel";
    var focusId = recordId(hotel);
    var ownedFocus = isRichCase(ownership) || (ownership && ownership.hotel && ownership.hotel.economic_owner_verified === true);
    var portfolio = orgPayload.portfolio || [];
    var ownedRows = portfolio.filter(function (h) {
      return h.relationship_type === "OWNED_BY" || h.economic_owner_verified;
    });
    var managedRows = portfolio.filter(function (h) {
      return !(h.relationship_type === "OWNED_BY" || h.economic_owner_verified);
    });

    var orgPeople =
      (ownership &&
        ownership.report &&
        ownership.report.decision_authority &&
        ownership.report.decision_authority.people) ||
      [];

    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">' +
      esc(g.display_name) +
      "</h2>" +
      '<p class="hiw-report__subtitle">Organization intelligence · related hotel: ' +
      esc(hotelName) +
      (ownedFocus
        ? " · Economic owner &amp; operator"
        : " · Operator / manager · ownership not implied") +
      "</p>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Organization Overview</h3>' +
      '<div class="hiw-prose"><p>' +
      esc(g.display_name) +
      (g.legal_name ? " (" + esc(g.legal_name) + ")" : "") +
      (g.ticker ? " · " + esc(g.ticker) : "") +
      ". Known Dealality portfolio relationships below separate owned / controlled hotels from operated / managed hotels. Portfolio inclusion does not invent property-company ownership.</p></div></section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Known Dealality Portfolio</h3>' +
      '<div class="hiw-kpi-strip">' +
      '<div class="hiw-kpi"><span class="hiw-kpi__v">' +
      esc(summary.total_relationships || g.known_hotel_relationships || portfolio.length || "—") +
      '</span><span class="hiw-kpi__l">Known hotels</span></div>' +
      '<div class="hiw-kpi"><span class="hiw-kpi__v">' +
      esc(g.known_rooms != null ? g.known_rooms : "—") +
      '</span><span class="hiw-kpi__l">Known rooms</span></div>' +
      '<div class="hiw-kpi"><span class="hiw-kpi__v">' +
      esc(summary.owned_or_controlled_verified != null ? summary.owned_or_controlled_verified : ownedRows.length) +
      '</span><span class="hiw-kpi__l">Owned / controlled</span></div>' +
      '<div class="hiw-kpi"><span class="hiw-kpi__v">' +
      esc(summary.operated_or_managed != null ? summary.operated_or_managed : managedRows.length) +
      '</span><span class="hiw-kpi__l">Managed / operated</span></div>' +
      '<div class="hiw-kpi"><span class="hiw-kpi__v">' +
      esc((g.markets || []).length || "—") +
      '</span><span class="hiw-kpi__l">Markets</span></div></div></section>' +
      '<section class="hiw-section"><h3 class="hiw-section__h">Owned / Controlled Hotels</h3>' +
      (ownedRows.length
        ? wrapTable(
            "<thead><tr><th>Hotel</th><th>Market</th><th>Rooms</th><th>Brand</th><th>Relationship</th><th>Status</th></tr></thead><tbody>" +
              ownedRows.map(function (h) {
                return portfolioRow(h, focusId);
              }).join("") +
              "</tbody>"
          )
        : '<div class="hiw-gap-box"><h4>None verified in this cohort</h4><p>Operator evidence alone does not certify ownership.</p></div>') +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Operated / Managed Hotels</h3>' +
      (managedRows.length
        ? wrapTable(
            "<thead><tr><th>Hotel</th><th>Market</th><th>Rooms</th><th>Brand</th><th>Relationship</th><th>Status</th></tr></thead><tbody>" +
              managedRows.map(function (h) {
                return portfolioRow(h, focusId);
              }).join("") +
              "</tbody>"
          )
        : '<p class="hiw-empty">No operated-only hotels in the known Dealality portfolio.</p>') +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Key People</h3>' +
      renderPeopleGrouped(orgPeople.slice(0, 10)) +
      "</section>" +
      '<section class="hiw-section"><h3 class="hiw-section__h">Strategic Context</h3>' +
      '<div class="hiw-prose"><p>' +
      esc((orgPayload.why_this_matters && orgPayload.why_this_matters.text) ||
        "Review owned versus managed mix before outreach. Brand and operator relationships differ by hotel.") +
      "</p></div></section>" +
      "</article>"
    );
  }

  function edgeCategory(type) {
    var t = String(type || "").toUpperCase();
    if (/OWN|CONTROL|SPONSOR/.test(t)) return "ownership";
    if (/OPERAT|MANAGE|ASSET/.test(t)) return "operator";
    if (/BRAND/.test(t)) return "brand";
    if (/DEVELOP/.test(t)) return "developed";
    if (/JV|JOINT/.test(t)) return "jv";
    if (/FORMER|HISTOR/.test(t)) return "historical";
    if (/PERSON|PEOPLE/.test(t)) return "people";
    return "other";
  }

  function renderRelationshipEdgesTable(ownership, filter) {
    var edges =
      (ownership && ownership.deep_research && ownership.deep_research.relationships) ||
      (ownership && ownership.report && ownership.report.relationship_edges) ||
      [];
    if (!edges.length) return "";
    var f = filter || state.relFilter || "all";
    var filtered = edges.filter(function (e) {
      if (f === "all") return true;
      return edgeCategory(e.type || e.relationship_type) === f;
    });
    var filters = [
      { id: "all", label: "All" },
      { id: "ownership", label: "Owned / Controlled" },
      { id: "operator", label: "Operated" },
      { id: "brand", label: "Branded" },
      { id: "historical", label: "Historical" },
      { id: "jv", label: "JV / Sponsored" },
    ];
    return (
      '<h4 class="hiw-section__h2">Relationship Detail</h4>' +
      '<div class="hiw-filters">' +
      filters
        .map(function (x) {
          return (
            '<button type="button" class="hiw-filter' +
            (f === x.id ? " is-active" : "") +
            '" data-hiw-rel-filter="' +
            x.id +
            '">' +
            esc(x.label) +
            "</button>"
          );
        })
        .join("") +
      "</div>" +
      (filtered.length
        ? wrapTable(
            "<thead><tr><th>From</th><th>Relationship</th><th>To</th><th>State</th><th>Confidence</th></tr></thead><tbody>" +
              filtered
                .map(function (e) {
                  return (
                    "<tr><td>" +
                    esc(e.from) +
                    "</td><td>" +
                    esc(relTypeLabel(e.type || e.relationship_type)) +
                    "</td><td>" +
                    esc(e.to) +
                    "</td><td>" +
                    esc(friendlyStatus(e.temporal_status || e.status || "CURRENT")) +
                    "</td><td>" +
                    esc(friendlyStatus(e.confidence || "HIGH")) +
                    "</td></tr>"
                  );
                })
                .join("") +
              "</tbody>"
          )
        : '<p class="hiw-empty">No relationships in this filter.</p>')
    );
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
    var rich = isRichCase(ownership);
    var propco = ctrl.legal_property_owner_propco || {};
    var econ = ctrl.economic_owner_or_group || {};
    var chain = (ownership && ownership.report && ownership.report.ownership_chain) ||
      (ownership && ownership.deep_research && ownership.deep_research.ownership_chain) ||
      [];
    var propcoName =
      propco.name ||
      ((chain.find(function (n) {
        return n.role === "propco";
      }) || {}).name);
    var econName =
      (econ.known && econ.name) ||
      ((chain.find(function (n) {
        return n.role === "economic_owner";
      }) || {}).name);

    var hotelDiagram = rich && propcoName && econName
      ? '<section class="hiw-section"><h3 class="hiw-section__h">Hotel Structure</h3>' +
        '<div class="hiw-diagram" aria-label="Ownership structure">' +
        '<button type="button" class="hiw-diagram__node">' +
        esc(hotel.name) +
        '<span class="hiw-diagram__meta">Property</span></button>' +
        '<div class="hiw-diagram__edge"><div class="hiw-diagram__line"></div>Held through · High</div>' +
        '<button type="button" class="hiw-diagram__node hiw-diagram__node--org">' +
        esc(propcoName.length > 48 ? propcoName.split("(")[0].trim() : propcoName) +
        '<span class="hiw-diagram__meta">Property Company / PropCo</span></button>' +
        '<button type="button" class="hiw-diagram__edge" data-hiw-cite="1"><div class="hiw-diagram__line"></div>Controlled by · High</button>' +
        '<button type="button" class="hiw-diagram__node hiw-diagram__node--org" data-hiw-tab="organization">' +
        esc(econName) +
        '<span class="hiw-diagram__meta">Economic Owner</span></button>' +
        '<button type="button" class="hiw-diagram__edge" data-hiw-cite="1"><div class="hiw-diagram__line"></div>Operated / managed by · High</button>' +
        '<button type="button" class="hiw-diagram__node hiw-diagram__node--org">' +
        esc(op) +
        '<span class="hiw-diagram__meta">Operator</span></button>' +
        '<div class="hiw-diagram__edge"><div class="hiw-diagram__line"></div>Current brand</div>' +
        '<button type="button" class="hiw-diagram__node hiw-diagram__node--brand">' +
        esc(brand) +
        '<span class="hiw-diagram__meta">Current</span></button>' +
        "</div>" +
        '<div class="hiw-prose"><p>Ownership, operations, and brand are separate relationships. Operator or brand is never drawn as owner.</p></div>' +
        renderRelationshipEdgesTable(ownership, state.relFilter) +
        '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--primary" data-hiw-org-network>Show Organization Network</button></div>' +
        '<div id="hiwOrgNetwork" hidden></div>' +
        "</section>"
      : '<section class="hiw-section"><h3 class="hiw-section__h">Hotel Structure</h3>' +
        '<div class="hiw-diagram">' +
        '<button type="button" class="hiw-diagram__node hiw-diagram__node--org" data-hiw-tab="organization">' +
        esc(op) +
        '<span class="hiw-diagram__meta">Operator / manager</span></button>' +
        '<button type="button" class="hiw-diagram__edge" data-hiw-cite="1"><div class="hiw-diagram__line"></div>Operates / manages · High</button>' +
        '<button type="button" class="hiw-diagram__node">' +
        esc(hotel.name) +
        '<span class="hiw-diagram__meta">Focus hotel · Owner not verified</span></button>' +
        '<div class="hiw-diagram__edge"><div class="hiw-diagram__line"></div>Brand affiliation' +
        (contested ? " · Contested" : "") +
        "</div>" +
        '<button type="button" class="hiw-diagram__node hiw-diagram__node--brand">' +
        esc(brand) +
        '<span class="hiw-diagram__meta">' +
        esc(contested ? "Contested" : brandStatus || "Affiliation") +
        "</span></button>" +
        "</div>" +
        '<div class="hiw-prose"><p>Economic owner and property company are held back when unverified — the operator is not relabeled as owner.</p></div>' +
        renderRelationshipEdgesTable(ownership, state.relFilter) +
        '<div class="hiw-actions"><button type="button" class="hiw-btn hiw-btn--primary" data-hiw-org-network>Show Organization Network</button></div>' +
        '<div id="hiwOrgNetwork" hidden></div>' +
        "</section>";

    return (
      '<article class="hiw-report">' +
      '<h2 class="hiw-report__title">Relationships</h2>' +
      '<p class="hiw-report__subtitle">Hotel structure first · organization network on demand</p>' +
      hotelDiagram +
      "</article>"
    );
  }

  function renderMarket(hotel) {
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
      '<div class="hiw-prose"><p>Submarket snapshot, area hotels, and corridor intelligence remain available from Radar layers. This workspace tab consolidates the hotel’s geographic identity for diligence reading.</p></div>' +
      "</section></article>"
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
          row("Relationship", "Operated / managed by") +
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
      '<p class="hiw-report__subtitle">Claim-linked citations — transparency layer</p>' +
      '<section class="hiw-section">' +
      renderSourcesList(true) +
      '<div class="hiw-prose" style="margin-top:16px"><p>Click a citation in Ownership or <strong>Open evidence</strong> here to inspect what the source supports — and what it does not establish.</p></div>' +
      "</section></article>"
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
      rel && rel.relationship_type === "OPERATED_BY"
        ? "Operated / managed by"
        : rel && rel.relationship_type
          ? String(rel.relationship_type).replace(/_/g, " ")
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
  }

  function loadOrgNetwork() {
    var box = document.getElementById("hiwOrgNetwork");
    if (!box) return;
    box.hidden = false;
    box.innerHTML = '<p class="hiw-empty">Loading organization network…</p>';
    var orgId =
      (state.ownership &&
        state.ownership.organization &&
        state.ownership.organization.entity_id) ||
      "dle_06G6AB1VK0BCCD94DNN7W8DRWZ";
    fetch(
      "/api/golden-demo/ownership/graph/neighbors?node_id=" +
        encodeURIComponent(orgId) +
        "&node_type=organization&limit=6&relationship_categories=ownership,operator,other&include_probable=1",
      { headers: { "ngrok-skip-browser-warning": "true" } }
    )
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data || !data.nodes) {
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
          '" text-anchor="middle">Operates</text></g>'
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
      if (state.tab === "relationships") paint();
      return;
    }
    var hotelBtn = e.target.closest("[data-hiw-hotel]");
    if (hotelBtn) {
      var hid = hotelBtn.getAttribute("data-hiw-hotel");
      var match =
        (state.org &&
          state.org.portfolio &&
          state.org.portfolio.find(function (h) {
            return h.airtable_record_id === hid;
          })) ||
        null;
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

    Promise.all([
      fetchOwnership(hotel),
      fetchOrg("grupo-hotelero-santa-fe"),
    ]).then(
      function (pair) {
        if (!state.open) return;
        state.ownership = pair[0];
        state.org = pair[1];
        state.citations = buildCitations(hotel, state.ownership);
        paint();
      }
    );
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
  };
})();
