/**
 * Operator DNA — Case Studies grid + modal (Brand Explorer property-example-card parity).
 */
(function (global) {
  "use strict";

  var Gold = function () {
    return global.OperatorExplorerGoldMock;
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

  function truncate(text, max) {
    var t = nz(text);
    if (!t || t.length <= max) return t;
    return t.slice(0, max - 1) + "…";
  }

  function uniqueTags(parts) {
    var seen = {};
    var out = [];
    (parts || []).forEach(function (p) {
      var t = nz(p);
      if (!t) return;
      var key = t.toLowerCase();
      if (seen[key]) return;
      seen[key] = true;
      out.push(t);
    });
    return out;
  }

  function caseStudyChips(cs) {
    return uniqueTags([
      cs.hotel_type,
      cs.region,
      cs.situation,
      cs.branded_independent,
    ]);
  }

  function buildPayload(cs) {
    var g = Gold();
    if (!g || typeof g.deriveCaseStudyNarrative !== "function") {
      return null;
    }
    var n = g.deriveCaseStudyNarrative(cs);
    var title = nz(cs.property_name) || nz(cs.hotel_type) || "Case study";
    var loc = nz(cs.region);
    var situation = nz(cs.situation);
    var subtitle = loc
      ? loc + (situation ? " · " + situation : "")
      : situation || "Case study";
    var chips = caseStudyChips(cs);
    var scenario = chips
      .slice(0, 3)
      .map(function (c) {
        return String(c).toUpperCase();
      })
      .join(" / ");
    var fp =
      g && typeof g.deriveCaseStudyFivePart === "function"
        ? g.deriveCaseStudyFivePart(cs)
        : {
            challenge: nz(n.before),
            operatorAction: nz(n.action),
            outcome: nz(n.after),
            whyItMatters: nz(n.relevance),
            dataStatus: "",
          };
    var teaser = truncate(
      fp.challenge || fp.operatorAction || fp.outcome,
      280
    );
    return {
      title: title,
      subtitle: subtitle,
      challenge: nz(fp.challenge) || "—",
      operatorAction: nz(fp.operatorAction) || "—",
      outcome: nz(fp.outcome) || "—",
      whyItMatters: nz(fp.whyItMatters) || "—",
      dataStatus: nz(fp.dataStatus) || "—",
      tags: chips,
      externalUrl: "",
    };
  }

  function buildCard(cs, index) {
    var g = Gold();
    if (!g || !g.caseStudyHasContent || !g.caseStudyHasContent(cs)) return "";
    var payload = buildPayload(cs);
    if (!payload) return "";

    var title = payload.title;
    var loc = nz(cs.region);
    var imgUrl = nz(cs.image_url);
    var badge = nz(cs.situation) || "Case Study";
    var metaLine = nz(cs.hotel_type) || "—";
    var scenario = payload.tags.length
      ? payload.tags
          .slice(0, 3)
          .map(function (t) {
            return String(t).toUpperCase();
          })
          .join(" / ")
      : "";
    var teaser = truncate(
      payload.challenge !== "—" ? payload.challenge : payload.operatorAction,
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
        (global.document &&
        global.document.documentElement &&
        global.document.documentElement.classList.contains("oe-export-pdf")
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
      escapeHtml(loc || "—") +
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

  function caseStudyInitialVisible() {
    var g = Gold();
    return g && g.CASE_STUDY_INITIAL_VISIBLE ? g.CASE_STUDY_INITIAL_VISIBLE : 3;
  }

  function caseStudiesExpanderButtonHtml(total, initial) {
    var g = Gold();
    if (g && typeof g.caseStudiesExpanderButtonHtml === "function") {
      return g.caseStudiesExpanderButtonHtml(total, initial);
    }
    if (!total || total <= initial) return "";
    return (
      '<div class="odna-case-studies-be__actions">' +
      '<button type="button" class="btn odna-case-studies-expand" data-odna-case-studies-expand ' +
      'aria-expanded="false" data-odna-case-studies-initial="' +
      initial +
      '">View all ' +
      total +
      " case studies</button></div>"
    );
  }

  function markCardCollapsed(cardHtml, index, initial) {
    var g = Gold();
    if (g && typeof g.markCaseStudyCardCollapsed === "function") {
      return g.markCaseStudyCardCollapsed(cardHtml, index, initial);
    }
    if (!cardHtml || index < initial) return cardHtml;
    return cardHtml.replace(
      '<article class="property-example-card"',
      '<article class="property-example-card property-example-card--collapsed"'
    );
  }

  function buildCaseStudiesExplorerGridHtml(cases) {
    var g = Gold();
    if (g && typeof g.buildBrandExplorerCaseStudiesGridHtml === "function") {
      return g.buildBrandExplorerCaseStudiesGridHtml(cases);
    }
    var list = (cases || []).filter(function (cs) {
      return g && g.caseStudyHasContent && g.caseStudyHasContent(cs);
    });
    if (!list.length) return "";

    var initial = caseStudyInitialVisible();
    var payloads = [];
    var sourceCases = [];
    var cards = list
      .map(function (cs, index) {
        var payload = buildPayload(cs);
        if (!payload) return "";
        var payloadIdx = payloads.length;
        payloads.push(payload);
        sourceCases.push(cs);
        return markCardCollapsed(buildCard(cs, payloadIdx), index, initial);
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

  var CASE_STUDY_MODAL_COLUMNS = [
    ["Challenge", "challenge"],
    ["Operator Action", "operatorAction"],
    ["Outcome", "outcome"],
    ["Why It Matters", "whyItMatters"],
    ["Data Confidence", "dataStatus"],
  ];

  function caseStudyPayloadFivePart(payload) {
    payload = payload || {};
    return {
      challenge:
        nz(payload.challenge) ||
        nz(payload.before) ||
        nz(payload.overview) ||
        "—",
      operatorAction:
        nz(payload.operatorAction) ||
        nz(payload.suggests) ||
        "—",
      outcome: nz(payload.outcome) || nz(payload.after) || nz(payload.dealalityTakeaway) || "—",
      whyItMatters:
        nz(payload.whyItMatters) ||
        nz(payload.relevance) ||
        "—",
      dataStatus: nz(payload.dataStatus) || "—",
    };
  }

  function buildCaseStudyDetailBlock(heading, body) {
    return (
      '<div class="be-case-detail-block"><h4>' +
      escapeHtml(heading) +
      "</h4><p>" +
      escapeHtml(body || "—") +
      "</p></div>"
    );
  }

  /** Vertical modal — Brand Explorer parity; operator five-part field mapping. */
  function buildCaseStudyModalInnerHtml(payload) {
    var fp = caseStudyPayloadFivePart(payload);
    var tagsHtml = (payload.tags || [])
      .map(function (t) {
        return '<span class="tag-chip">' + escapeHtml(String(t).toUpperCase()) + "</span>";
      })
      .join("");

    var blocks = CASE_STUDY_MODAL_COLUMNS.map(function (pair) {
      return buildCaseStudyDetailBlock(pair[0], fp[pair[1]]);
    }).join("");

    return (
      '<p class="odna-cs-modal__subtitle">' +
      escapeHtml(payload.subtitle || "") +
      "</p>" +
      blocks +
      '<div class="be-case-detail-block"><h4>Similar property types</h4><div class="be-case-modal__tags">' +
      (tagsHtml || '<span class="be-case-modal__tags-empty">—</span>') +
      "</div></div>"
    );
  }

  function openModal(payload) {
    var modal = document.getElementById("odnaCaseStudyModal");
    var titleEl = document.getElementById("odnaCsModalTitle");
    var innerEl = document.getElementById("odnaCsModalInner");
    if (!modal || !titleEl || !innerEl || !payload) return;

    titleEl.textContent = payload.title || "Case study";
    innerEl.innerHTML = buildCaseStudyModalInnerHtml(payload);

    modal.classList.add("be-case-modal-overlay--open");
    document.body.style.overflow = "hidden";
  }

  function closeModal() {
    var modal = document.getElementById("odnaCaseStudyModal");
    if (!modal) return;
    modal.classList.remove("be-case-modal-overlay--open");
    document.body.style.overflow = "";
  }

  function rebuildCaseStudyPayload(cs) {
    var g = Gold();
    if (g && typeof g.buildCaseStudyBePayload === "function") {
      return g.buildCaseStudyBePayload(cs);
    }
    return buildPayload(cs);
  }

  function resolveCaseStudyPayload(btn) {
    var idx = parseInt(btn.getAttribute("data-odna-case-study"), 10);
    if (isNaN(idx) || idx < 0) return null;

    var grid = btn.closest(".property-example-grid");
    var payloads =
      (grid && grid._odnaCaseStudyPayloads) || global._odnaCaseStudyPayloads;
    if (payloads && payloads[idx]) return payloads[idx];

    var sources =
      (grid && grid._odnaCaseStudySourceCases) || global._odnaCaseStudySourceCases;
    if (sources && sources[idx]) return rebuildCaseStudyPayload(sources[idx]);

    return null;
  }

  function attachCaseStudyPayloadsToDom() {
    var grid = document.querySelector(
      "#panels .odna-case-studies-be .property-example-grid"
    );
    if (!grid || !global._odnaCaseStudyPayloads) return;
    grid._odnaCaseStudyPayloads = global._odnaCaseStudyPayloads;
    grid._odnaCaseStudySourceCases = global._odnaCaseStudySourceCases || [];
  }

  function wireCaseStudyExpander() {
    if (global._odnaCaseStudyExpanderWired) return;
    global._odnaCaseStudyExpanderWired = true;

    document.addEventListener("click", function (e) {
      var expandBtn = e.target.closest("[data-odna-case-studies-expand]");
      if (!expandBtn) return;
      e.preventDefault();
      var root = expandBtn.closest(".odna-case-studies-be, .odna-proof-studies-expander");
      if (!root) return;
      var expanded = expandBtn.getAttribute("aria-expanded") === "true";
      var total = parseInt(root.getAttribute("data-odna-case-study-total"), 10) || 0;
      if (expanded) {
        root.classList.remove("is-case-studies-expanded");
        expandBtn.setAttribute("aria-expanded", "false");
        expandBtn.textContent = total
          ? "View all " + total + " case studies"
          : "View all case studies";
      } else {
        root.classList.add("is-case-studies-expanded");
        expandBtn.setAttribute("aria-expanded", "true");
        expandBtn.textContent = "Show fewer case studies";
      }
    });
  }

  function wireCaseStudyModal() {
    if (global._odnaCaseStudyModalWired) return;
    global._odnaCaseStudyModalWired = true;
    wireCaseStudyExpander();

    document.addEventListener("click", function (e) {
      var btn = e.target.closest("[data-odna-case-study]");
      if (btn) {
        var payload = resolveCaseStudyPayload(btn);
        if (payload) {
          e.preventDefault();
          openModal(payload);
        } else if (
          typeof console !== "undefined" &&
          console.warn &&
          (global.location.hostname === "localhost" ||
            global.location.hostname === "127.0.0.1")
        ) {
          console.warn(
            "[OperatorDnaCaseStudiesBe] Case study modal payload missing for index",
            btn.getAttribute("data-odna-case-study")
          );
        }
        return;
      }
      if (e.target.closest("#odnaCsModalClose")) {
        e.preventDefault();
        closeModal();
        return;
      }
      if (e.target.id === "odnaCaseStudyModal") {
        closeModal();
      }
    });

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") closeModal();
    });
  }

  global.OperatorDnaCaseStudiesBe = {
    buildCaseStudiesExplorerGridHtml: buildCaseStudiesExplorerGridHtml,
    attachCaseStudyPayloadsToDom: attachCaseStudyPayloadsToDom,
    wireCaseStudyExpander: wireCaseStudyExpander,
    wireCaseStudyModal: wireCaseStudyModal,
  };
})(typeof window !== "undefined" ? window : global);
