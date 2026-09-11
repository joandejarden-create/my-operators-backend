/**
 * Ownership Intelligence — Radar Hotel Detail Panel (Packet 2.2 UX).
 * Semantics unchanged. Client-facing labels + progressive disclosure.
 */
(function () {
  "use strict";

  var CACHE = {};

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function recordIdOf(hotel) {
    return (hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || null;
  }

  function fetchOwnership(recordId) {
    if (!recordId) return Promise.resolve(null);
    if (CACHE[recordId]) return Promise.resolve(CACHE[recordId]);
    return fetch("/api/golden-demo/ownership/hotel/" + encodeURIComponent(recordId), {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (payload) {
        CACHE[recordId] = payload;
        return payload;
      })
      .catch(function (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[OwnershipIntel] fetch failed", err);
        }
        return null;
      });
  }

  function friendlyStatus(raw) {
    var s = String(raw || "").toUpperCase();
    if (s === "VERIFIED") return "Verified";
    if (s === "HIGH") return "High";
    if (s === "PROBABLE") return "Probable";
    if (s === "CONTESTED") return "Contested";
    if (s === "LIVE") return "Live";
    if (s === "PREVIEW") return "Display only";
    if (s === "GAP") return "Research gap";
    if (s === "UNKNOWN" || s === "NOT_YET_VERIFIED") return "Not yet verified";
    return raw ? String(raw) : "Not yet verified";
  }

  function badgeClass(raw) {
    var s = String(raw || "").toLowerCase();
    if (s === "verified" || s === "high") return s;
    if (s === "probable" || s === "contested" || s === "preview") return s === "preview" ? "probable" : s;
    return "gap";
  }

  function evidenceBadge(label, sourceCount, opts) {
    opts = opts || {};
    var count =
      sourceCount != null && sourceCount > 0
        ? " · " + sourceCount + " source" + (sourceCount === 1 ? "" : "s")
        : "";
    var open = opts.openEvidence !== false && sourceCount > 0;
    var tag = open ? "button" : "span";
    var attrs = open
      ? ' type="button" data-oi-open-evidence="1"'
      : ' aria-disabled="true"';
    return (
      "<" +
      tag +
      ' class="oi-badge oi-badge--' +
      esc(badgeClass(label)) +
      '"' +
      attrs +
      ">" +
      esc(friendlyStatus(label)) +
      esc(count) +
      "</" +
      tag +
      ">"
    );
  }

  function renderEvidencePanel(evidence) {
    if (!evidence) return "";
    var sources = (evidence.sources || [])
      .map(function (s) {
        return (
          '<li class="oi-source">' +
          '<div class="oi-source__title">' +
          esc(s.title || s.provider) +
          "</div>" +
          (s.url
            ? '<a class="oi-source__url" href="' +
              esc(s.url) +
              '" target="_blank" rel="noopener noreferrer">' +
              esc(s.url) +
              "</a>"
            : "") +
          '<div class="oi-source__meta">' +
          esc(s.provider || "") +
          (s.observed_date ? " · Observed " + esc(s.observed_date) : "") +
          "</div>" +
          (s.evidence_excerpt
            ? '<p class="oi-source__excerpt">' + esc(s.evidence_excerpt) + "</p>"
            : "") +
          "</li>"
        );
      })
      .join("");

    var relType = evidence.relationship && evidence.relationship.relationship_type;
    var relLabel =
      relType === "OPERATED_BY"
        ? "Operated / managed by"
        : relType === "BRANDED_BY"
          ? "Brand affiliation"
          : relType
            ? String(relType).replace(/_/g, " ")
            : "";

    return (
      '<div class="oi-evidence-drawer" hidden data-oi-evidence-drawer>' +
      '<div class="oi-evidence-drawer__head">' +
      "<h4>Evidence</h4>" +
      '<button type="button" class="oi-evidence-drawer__close" data-oi-close-evidence aria-label="Close evidence">&times;</button>' +
      "</div>" +
      '<p class="oi-evidence-drawer__claim"><strong>Claim</strong> — ' +
      esc(evidence.claim) +
      "</p>" +
      '<p class="oi-evidence-drawer__rel"><strong>Relationship</strong> — ' +
      esc(evidence.relationship && evidence.relationship.subject) +
      " → " +
      esc(relLabel) +
      " → " +
      esc(evidence.relationship && evidence.relationship.object) +
      "</p>" +
      "<h5 style=\"margin:10px 0 4px;color:var(--oi-text);font-size:12px\">Sources</h5>" +
      '<ul class="oi-source-list">' +
      sources +
      "</ul>" +
      '<p class="oi-prove"><strong>What it proves</strong> — ' +
      esc(evidence.what_it_proves) +
      "</p>" +
      '<p class="oi-prove oi-prove--not"><strong>What it does not prove</strong> — ' +
      esc(evidence.what_it_does_not_prove) +
      "</p>" +
      "</div>"
    );
  }

  function partyCard(label, value, hint, badgeHtml, clickable) {
    var tag = clickable ? "button" : "div";
    var attrs = clickable ? ' type="button" data-oi-open-evidence="1"' : "";
    return (
      "<" +
      tag +
      ' class="oi-party"' +
      attrs +
      ">" +
      '<div class="oi-party__label">' +
      esc(label) +
      "</div>" +
      '<div class="oi-party__value">' +
      esc(value) +
      "</div>" +
      (hint ? '<div class="oi-party__hint">' + esc(hint) + "</div>" : "") +
      (badgeHtml ? '<div class="oi-party__badge-row">' + badgeHtml + "</div>" : "") +
      "</" +
      tag +
      ">"
    );
  }

  function buildExecutiveSummary(payload) {
    if (!payload || !payload.in_cohort) {
      return "Ownership for this hotel is not yet verified. No fabricated owner is shown.";
    }
    var op =
      payload.report &&
      payload.report.ownership_and_control &&
      payload.report.ownership_and_control.operator &&
      payload.report.ownership_and_control.operator.name;
    var brand =
      payload.hotel && payload.hotel.brand_display
        ? payload.hotel.brand_display
        : null;
    var contested =
      payload.hotel && String(payload.hotel.brand_display_status || "") === "CONTESTED";
    var parts = [
      "Economic ownership is not yet verified.",
      op
        ? op + " is evidenced as the operator / management company."
        : "Operator information is limited.",
    ];
    if (contested && brand) {
      parts.push(
        "Brand affiliation (" + brand + ") is contested against the hotel’s trading name."
      );
    } else if (brand) {
      parts.push("Brand: " + brand + ".");
    }
    return parts.join(" ");
  }

  function insightCards(payload) {
    var r = (payload && payload.report) || {};
    var strat = r.strategic_development || {};
    var g = payload.organization || payload.ownership_group || {};
    var cards = [];

    if (strat.portfolio_leverage && strat.portfolio_leverage.signal) {
      cards.push({
        title: "Multi-property relationship",
        body:
          (g.display_name || "This organization") +
          " is linked through management relationships to " +
          (g.known_hotel_count || "multiple") +
          " matched hotels across Mexican markets.",
      });
    }
    if (
      (payload.hotel && payload.hotel.brand_display_status === "CONTESTED") ||
      (strat.conversion_development_signal && strat.conversion_development_signal.signal)
    ) {
      cards.push({
        title: "Brand complexity",
        body: "Current Krystal / Breathless identity evidence is conflicting — treated as contested, not auto-resolved.",
      });
    }
    cards.push({
      title: "Ownership gap",
      body: "Economic owner and legal PropCo remain unresolved. Management alone is not treated as ownership.",
    });
    if (strat.multi_asset_potential && strat.multi_asset_potential.signal) {
      cards.push({
        title: "Portfolio leverage",
        body: "An organization-level operating relationship may provide multi-property commercial relevance.",
      });
    }
    return cards.slice(0, 4);
  }

  function renderReport(payload) {
    if (!payload) {
      return (
        '<div class="oi-empty"><p>Ownership intelligence failed to load.</p>' +
        '<p class="oi-empty__sub">Reopen the hotel to retry.</p></div>'
      );
    }

    var r = payload.report || {};
    var ctrl = r.ownership_and_control || {};
    var evidence = r.evidence || null;
    var sourceCount = (evidence && evidence.source_count) || 0;
    var g = payload.organization || payload.ownership_group;
    var hotelName = (payload.hotel && payload.hotel.name) || "This hotel";
    var operatorName =
      (ctrl.operator && ctrl.operator.name) ||
      (payload.in_cohort ? "Not identified" : "Not identified");
    var operatorBucket =
      (ctrl.operator && ctrl.operator.verification_bucket) || "UNKNOWN";
    var brandName =
      (ctrl.brand && ctrl.brand.name) ||
      (payload.hotel && payload.hotel.brand_display) ||
      null;
    var brandStatus =
      (ctrl.brand && ctrl.brand.status) ||
      (payload.hotel && payload.hotel.brand_display_status) ||
      "UNKNOWN";

    var operatorClickable = payload.in_cohort && sourceCount > 0;
    var brandContested = String(brandStatus).toUpperCase() === "CONTESTED";

    var parties =
      '<div class="oi-party-grid">' +
      partyCard("Economic owner", "Not yet verified", "Research gap", "", false) +
      partyCard(
        "Operator",
        operatorName,
        payload.in_cohort ? "Operated / managed by" : null,
        payload.in_cohort
          ? evidenceBadge(operatorBucket, sourceCount, { openEvidence: true })
          : "",
        operatorClickable
      ) +
      partyCard(
        "Brand",
        brandName || "Not identified",
        brandContested ? "Affiliation contested" : brandName ? null : "Research gap",
        brandName && brandContested
          ? evidenceBadge("contested", 0, { openEvidence: false })
          : brandName
            ? evidenceBadge(brandStatus, 0, { openEvidence: false })
            : "",
        false
      ) +
      partyCard("Legal PropCo", "Not yet verified", "Research gap", "", false) +
      "</div>";

    var structure = payload.in_cohort
      ? '<section class="oi-section">' +
        '<h3 class="oi-section__title">Ownership structure</h3>' +
        '<div class="oi-structure">' +
        '<div class="oi-structure__flow">' +
        '<div class="oi-structure__node">' +
        esc(hotelName) +
        "</div>" +
        '<div class="oi-structure__edge">Operated / managed by</div>' +
        '<div class="oi-structure__node oi-structure__node--org">' +
        esc(operatorName) +
        "</div>" +
        "</div>" +
        '<ul class="oi-structure__gaps">' +
        "<li><strong>Economic owner:</strong> Not yet verified</li>" +
        "<li><strong>PropCo:</strong> Not yet verified</li>" +
        "</ul>" +
        '<p class="oi-structure__note">Dealality does not treat the management company as the asset owner without supporting evidence.</p>' +
        "</div></section>"
      : "";

    var insights = insightCards(payload)
      .map(function (c) {
        return (
          '<article class="oi-insight">' +
          '<h4 class="oi-insight__title">' +
          esc(c.title) +
          "</h4>" +
          '<p class="oi-insight__body">' +
          esc(c.body) +
          "</p></article>"
        );
      })
      .join("");

    var actions = payload.in_cohort
      ? '<div class="oi-actions">' +
        (g
          ? '<a class="oi-btn oi-btn--primary" href="/app#/ownership-group?slug=' +
            encodeURIComponent(g.slug) +
            '" target="_parent">View GSF relationships</a>'
          : "") +
        (g
          ? '<a class="oi-btn oi-btn--secondary" href="/app#/ownership-graph?node_id=' +
            encodeURIComponent((payload.hotel && payload.hotel.hotel_id) || g.entity_id || "") +
            '&node_type=hotel" target="_parent">View relationship graph</a>'
          : "") +
        '<span class="oi-teaser" title="Coming soon">Deep Research · Coming soon</span>' +
        "</div>"
      : "";

    return (
      '<div class="oi-intel" data-oi-report>' +
      '<div class="oi-intel__head">' +
      '<h2 class="oi-intel__title">Ownership intelligence</h2>' +
      (payload.data_status === "CONTROLLED_DEMO"
        ? '<span class="oi-demo-tag">Demo cohort</span>'
        : "") +
      "</div>" +
      '<p class="oi-exec">' +
      esc(buildExecutiveSummary(payload)) +
      "</p>" +
      parties +
      structure +
      (insights
        ? '<section class="oi-section"><h3 class="oi-section__title">Strategic intelligence</h3>' +
          '<div class="oi-insight-grid">' +
          insights +
          "</div></section>"
        : "") +
      actions +
      renderEvidencePanel(evidence) +
      "</div>"
    );
  }

  function ensureOwnershipTab(panelEl) {
    if (!panelEl) return null;
    var nav = panelEl.querySelector(".hdp-tabs-section");
    var panels = panelEl.querySelector(".hdp-tab-panels");
    if (!nav || !panels) return null;
    if (nav.querySelector('[data-tab="ownership"]')) {
      return panels.querySelector('[data-panel="ownership"]');
    }

    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "section-nav-item";
    btn.setAttribute("data-tab", "ownership");
    btn.setAttribute("aria-selected", "false");
    btn.innerHTML =
      '<div class="section-nav-icon" aria-hidden="true">' +
      '<svg viewBox="0 0 24 24"><path d="M12 3l8 4v5c0 5-3.5 8.5-8 10-4.5-1.5-8-5-8-10V7l8-4z"/></svg>' +
      "</div>" +
      '<span class="section-nav-label">Ownership</span>';

    var first = nav.querySelector(".section-nav-item");
    if (first && first.nextSibling) nav.insertBefore(btn, first.nextSibling);
    else nav.appendChild(btn);

    var section = document.createElement("section");
    section.className = "tab-panel";
    section.setAttribute("data-panel", "ownership");
    section.setAttribute("data-state", "loading");
    section.innerHTML =
      '<p class="hdp-empty hdp-context-state">Loading ownership intelligence…</p>';
    panels.appendChild(section);
    return section;
  }

  function clearHeaderChips(panelEl) {
    if (!panelEl) return;
    panelEl.querySelectorAll(".oi-rel-card, .oi-header-chip").forEach(function (el) {
      el.remove();
    });
  }

  function injectIdentityChips(panelEl, hotel, payload) {
    if (!panelEl) return;
    var footer = panelEl.querySelector(".hdp-header__footer");
    if (!footer) return;
    clearHeaderChips(panelEl);

    var ctrl =
      (payload && payload.report && payload.report.ownership_and_control) || {};
    var evidence = payload && payload.report && payload.report.evidence;
    var sourceCount = (evidence && evidence.source_count) || 0;
    var org = payload && (payload.organization || payload.ownership_group);

    function card(opts) {
      var el = document.createElement(opts.href || opts.openEvidence ? (opts.href ? "a" : "button") : "div");
      el.className = "oi-rel-card oi-rel-card--" + opts.kind;
      if (opts.href) {
        el.href = opts.href;
        el.target = "_parent";
      }
      if (opts.openEvidence) {
        el.type = "button";
        el.setAttribute("data-oi-open-evidence", "1");
      }
      el.innerHTML =
        '<span class="oi-rel-card__label">' +
        esc(opts.label) +
        "</span>" +
        '<span class="oi-rel-card__value">' +
        esc(opts.value) +
        "</span>" +
        '<span class="oi-rel-card__meta">' +
        esc(opts.meta) +
        "</span>";
      footer.appendChild(el);
    }

    card({
      kind: "owner",
      label: "Owner",
      value: "Not yet verified",
      meta: "Research gap",
    });

    var operatorName =
      (ctrl.operator && ctrl.operator.name) ||
      (hotel && (hotel.managementCompany || hotel.management_company)) ||
      null;
    if (operatorName) {
      var opBucket = (ctrl.operator && ctrl.operator.verification_bucket) || "HIGH";
      var opMeta =
        friendlyStatus(opBucket) +
        (sourceCount ? " · " + sourceCount + " source" + (sourceCount === 1 ? "" : "s") : "");
      card({
        kind: "operator",
        label: "Operator",
        value: operatorName,
        meta: opMeta,
        href: org ? "/app#/ownership-group?slug=" + encodeURIComponent(org.slug) : null,
        openEvidence: !org && sourceCount > 0,
      });
    }

    var brandName =
      (payload && payload.hotel && payload.hotel.brand_display) ||
      (hotel && (hotel.brand || hotel.affiliation)) ||
      null;
    if (brandName) {
      var brandStatus =
        (payload && payload.hotel && payload.hotel.brand_display_status) || "";
      card({
        kind: "brand",
        label: "Brand",
        value: brandName,
        meta: brandStatus
          ? friendlyStatus(brandStatus) + (String(brandStatus).toUpperCase() === "CONTESTED" ? "" : "")
          : "Affiliation",
      });
    }
  }

  function bindEvidence(panelEl) {
    if (!panelEl || panelEl.__oiEvidenceBound) return;
    panelEl.__oiEvidenceBound = true;
    panelEl.addEventListener("click", function (event) {
      var openBtn = event.target.closest("[data-oi-open-evidence]");
      var closeBtn = event.target.closest("[data-oi-close-evidence]");
      var drawer = panelEl.querySelector("[data-oi-evidence-drawer]");
      if (openBtn) {
        // Ensure Ownership tab content is active so drawer is visible
        var ownTab = panelEl.querySelector('[data-tab="ownership"]');
        if (ownTab && !ownTab.classList.contains("active")) ownTab.click();
        setTimeout(function () {
          var d = panelEl.querySelector("[data-oi-evidence-drawer]");
          if (d) d.hidden = false;
        }, 50);
      }
      if (closeBtn && drawer) drawer.hidden = true;
    });
  }

  function enhancePanel(hotel) {
    var panelEl = document.getElementById("hotelDetailPanel");
    if (!panelEl || !panelEl.classList.contains("is-open")) return;
    var ownershipPanel = ensureOwnershipTab(panelEl);
    if (!ownershipPanel) return;
    bindEvidence(panelEl);

    var rid = recordIdOf(hotel);
    fetchOwnership(rid).then(function (payload) {
      if (!panelEl.classList.contains("is-open")) return;
      injectIdentityChips(panelEl, hotel, payload);
      ownershipPanel.setAttribute("data-state", "ready");
      ownershipPanel.innerHTML = renderReport(payload);
    });
  }

  function patchHotelDetailPanel() {
    // Packet 2.3: Hotel Intelligence Workspace owns hotel open; skip modal Ownership injection.
    if (window.HotelIntelligenceWorkspace) return;
    if (!window.HotelDetailPanel || window.HotelDetailPanel.__oiPatched) return;
    var originalOpen = window.HotelDetailPanel.open;
    window.HotelDetailPanel.open = function (hotel) {
      originalOpen(hotel);
      var attempts = 0;
      var timer = setInterval(function () {
        attempts += 1;
        var panelEl = document.getElementById("hotelDetailPanel");
        if (panelEl && panelEl.querySelector(".hdp-tabs-section")) {
          clearInterval(timer);
          enhancePanel(hotel);
        } else if (attempts > 40) {
          clearInterval(timer);
        }
      }, 100);
    };
    window.HotelDetailPanel.__oiPatched = true;
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", patchHotelDetailPanel);
  } else {
    patchHotelDetailPanel();
  }
  window.OwnershipIntelligenceUI = {
    enhancePanel: enhancePanel,
    fetchOwnership: fetchOwnership,
  };
})();
