(function () {
  "use strict";

  var DEFAULT_BRIEF_QUESTION = "What should the sales team pursue?";
  var root = document.getElementById("gdiShareRoot");
  var loading = document.getElementById("gdiShareLoading");
  var drawer = document.getElementById("gdiShareDrawer");
  var drawerTitle = document.getElementById("gdiShareDrawerTitle");
  var drawerBody = document.getElementById("gdiShareDrawerBody");
  var drawerClose = document.getElementById("gdiShareDrawerClose");
  var params = new URLSearchParams(window.location.search || "");
  var share = params.get("share") || "";

  var state = {
    hotelId: null,
    resolve: null,
    brief: null,
    opportunities: [],
    validationEnums: null,
    tab: "brief",
    filters: { priority: "", segment: "", booking: "", territory: "" },
  };

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtVal(v) {
    if (v == null || v === "" || v === "UNKNOWN") return "—";
    return v;
  }

  function showError(msg) {
    if (loading) loading.style.display = "none";
    root.innerHTML = '<div class="gdi-error" role="alert"><p>' + esc(msg) + "</p></div>";
  }

  function api(path) {
    var sep = path.indexOf("?") >= 0 ? "&" : "?";
    return fetch(path + sep + "share=" + encodeURIComponent(share)).then(function (r) {
      return r.json().then(function (data) {
        if (!r.ok || data.ok === false) {
          throw new Error(data.message || data.error || "Request failed");
        }
        return data;
      });
    });
  }

  function apiPost(path, body) {
    var sep = path.indexOf("?") >= 0 ? "&" : "?";
    return fetch(path + sep + "share=" + encodeURIComponent(share), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      return r.json().then(function (data) {
        if (!r.ok || data.ok === false) {
          throw new Error(data.message || data.error || "Request failed");
        }
        return data;
      });
    });
  }

  function enumOptions(keys, labels, selected) {
    return (
      '<option value="">—</option>' +
      (keys || [])
        .map(function (k) {
          return (
            '<option value="' +
            esc(k) +
            '"' +
            (selected === k ? " selected" : "") +
            ">" +
            esc((labels && labels[k]) || k) +
            "</option>"
          );
        })
        .join("")
    );
  }

  function nextUnvalidatedId(currentId) {
    var rows = state.opportunities || [];
    var start = -1;
    for (var i = 0; i < rows.length; i++) {
      if (rows[i].id === currentId) {
        start = i;
        break;
      }
    }
    for (var j = start + 1; j < rows.length; j++) {
      if (!rows[j].shareValidation) return rows[j].id;
    }
    for (var k = 0; k < start; k++) {
      if (!rows[k].shareValidation) return rows[k].id;
    }
    return null;
  }

  function validationFormHtml(o) {
    var v = o.shareValidation || {};
    var e = state.validationEnums || {};
    return (
      '<div class="gdi-section gdi-validation-panel"><h3>15. Hotel Validation</h3>' +
      '<p class="gdi-lede">Fast review for sales. Stored separately — does not overwrite research.</p>' +
      '<div class="gdi-feedback" id="gdiShareValidation">' +
      "<label>Familiarity / Status<select id=\"gdiSvFamiliarity\">" +
      enumOptions(e.familiarityStatus, e.familiarityLabels, v.familiarityStatus) +
      "</select></label>" +
      "<label>Commercial Value<select id=\"gdiSvCommercial\">" +
      enumOptions(e.commercialValue, e.commercialValueLabels, v.commercialValue) +
      "</select></label>" +
      "<label>Contact Person<select id=\"gdiSvPerson\">" +
      enumOptions(e.contactPerson, e.contactPersonLabels, v.contactPersonAssessment) +
      "</select></label>" +
      "<label>Email Quality<select id=\"gdiSvEmail\">" +
      enumOptions(e.emailAssessment, e.emailAssessmentLabels, v.emailAssessment) +
      "</select></label>" +
      "<label>Phone Quality<select id=\"gdiSvPhone\">" +
      enumOptions(e.phoneAssessment, e.phoneAssessmentLabels, v.phoneAssessment) +
      "</select></label>" +
      '<textarea id="gdiSvNote" placeholder="Optional note" rows="2">' +
      esc(v.note || "") +
      "</textarea>" +
      '<div class="gdi-validation-actions">' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiSvSave" data-id="' +
      esc(o.id) +
      '">Save</button>' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiSvSaveNext" data-id="' +
      esc(o.id) +
      '">Save + Next</button>' +
      '<span class="gdi-validation-status" id="gdiSvStatus"></span></div></div></div>'
    );
  }

  function wireValidationForm(opportunityId) {
    function collect() {
      return {
        familiarityStatus: (document.getElementById("gdiSvFamiliarity") || {}).value || null,
        commercialValue: (document.getElementById("gdiSvCommercial") || {}).value || null,
        contactPersonAssessment: (document.getElementById("gdiSvPerson") || {}).value || null,
        emailAssessment: (document.getElementById("gdiSvEmail") || {}).value || null,
        phoneAssessment: (document.getElementById("gdiSvPhone") || {}).value || null,
        note: (document.getElementById("gdiSvNote") || {}).value || "",
        validator: "SHARE_REVIEWER",
      };
    }
    function save(thenNext) {
      var statusEl = document.getElementById("gdiSvStatus");
      if (statusEl) statusEl.textContent = "Saving…";
      var payload = collect();
      if (!payload.familiarityStatus && !payload.commercialValue) {
        if (statusEl) statusEl.textContent = "Pick familiarity or commercial value.";
        return;
      }
      apiPost(
        "/api/group-demand-intelligence/share/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/opportunities/" +
          encodeURIComponent(opportunityId) +
          "/validation",
        payload
      )
        .then(function (data) {
          for (var i = 0; i < state.opportunities.length; i++) {
            if (state.opportunities[i].id === opportunityId) {
              state.opportunities[i].shareValidation = data.validation;
              break;
            }
          }
          if (state.resolve && data.summary) {
            state.resolve.summary = Object.assign({}, state.resolve.summary, {
              validated: data.summary.validated,
              pendingValidation: data.summary.pendingValidation,
              confirmedNew: data.summary.confirmedNew,
              alreadyKnown: data.summary.alreadyKnown,
              worthPursuingNow: data.summary.worthPursuingNow,
              notRelevant: data.summary.notRelevant,
            });
            state.resolve.validationSummary = data.summary;
          }
          if (statusEl) statusEl.textContent = "Saved";
          if (thenNext) {
            var nid = nextUnvalidatedId(opportunityId);
            if (nid) openDetail(nid);
            else if (statusEl) statusEl.textContent = "Saved — all reviewed";
          }
        })
        .catch(function (err) {
          if (statusEl) statusEl.textContent = err.message || "Save failed";
        });
    }
    var saveBtn = document.getElementById("gdiSvSave");
    var nextBtn = document.getElementById("gdiSvSaveNext");
    if (saveBtn) saveBtn.addEventListener("click", function () { save(false); });
    if (nextBtn) nextBtn.addEventListener("click", function () { save(true); });
  }

  function priorityPill(p) {
    if (p === "HIGH_PRIORITY") return '<span class="gdi-pill gdi-pill-high">High</span>';
    if (p === "MEDIUM_PRIORITY") return '<span class="gdi-pill gdi-pill-med">Medium</span>';
    return '<span class="gdi-pill gdi-pill-watch">Watchlist</span>';
  }

  function contactLabel(contact) {
    if (!contact) return "—";
    if (contact.name && contact.name !== "UNKNOWN") {
      return contact.name + (contact.role ? " (" + contact.role + ")" : "");
    }
    if (contact.email) return contact.email;
    return "—";
  }

  function briefContactBlock(it, linked) {
    var c = (linked && linked.primaryContact) || it.primaryContact || null;
    var grade =
      (linked && (linked.contactGradeLabel || linked.contactGrade)) ||
      it.contactGradeLabel ||
      it.contactGrade ||
      (c && (c.contactGradeLabel || c.contactGrade)) ||
      it.contactQualityLabel ||
      "—";
    if (!c) {
      return (
        '<div><span class="gdi-meta-label">Primary Contact</span><span class="gdi-meta-value">—</span></div>' +
        '<div><span class="gdi-meta-label">Contact Quality</span><span class="gdi-meta-value">' +
        esc(grade) +
        "</span></div>"
      );
    }
    return (
      '<div><span class="gdi-meta-label">Primary Contact</span><span class="gdi-meta-value">' +
      esc(c.name || "—") +
      '</span></div><div><span class="gdi-meta-label">Role</span><span class="gdi-meta-value">' +
      esc(c.role || c.title || "—") +
      '</span></div><div><span class="gdi-meta-label">Email</span><span class="gdi-meta-value">' +
      esc(c.email || "—") +
      '</span></div><div><span class="gdi-meta-label">Phone</span><span class="gdi-meta-value">' +
      esc(c.phone || "—") +
      '</span></div><div><span class="gdi-meta-label">Contact Quality</span><span class="gdi-meta-value">' +
      esc(grade) +
      "</span></div>"
    );
  }

  function whoShouldSalesContactHtml(o) {
    var c = o.primaryContact;
    if (!c) return "<p>No usable contact resolved yet.</p>";
    var backups = (o.backupContacts || [])
      .map(function (b) {
        return (
          "<li><strong>" +
          esc(b.name || "—") +
          "</strong> · " +
          esc(b.role || "") +
          " · " +
          esc(b.email || "") +
          (b.phone ? " · " + esc(b.phone) : "") +
          (b.whyThisContact ? "<br/><em>" + esc(b.whyThisContact) + "</em>" : "") +
          "</li>"
        );
      })
      .join("");
    return (
      "<p><strong>" +
      esc(c.name || "—") +
      "</strong></p>" +
      "<p>Title: " +
      esc(c.title || c.role || "—") +
      "</p>" +
      "<p>Relationship to event: " +
      esc(c.relationshipToEvent || o.relationshipToEvent || "—") +
      "</p>" +
      "<p><strong>Why this contact:</strong> " +
      esc(c.whyThisContact || o.whyThisContact || "—") +
      "</p>" +
      "<p>Email: " +
      esc(c.email || "—") +
      " · " +
      esc(c.emailVerificationStatusLabel || c.emailVerificationStatus || "") +
      "</p>" +
      "<p>Phone: " +
      esc(c.phone || "—") +
      (c.phoneTypeLabel || c.phoneType
        ? " (" + esc(c.phoneTypeLabel || c.phoneType) + ")"
        : "") +
      "</p>" +
      "<p>Contact Quality: <strong>" +
      esc(o.contactGradeLabel || c.contactGradeLabel || o.contactQualityLabel || "—") +
      "</strong> · Contact Confidence: <strong>" +
      esc(o.contactConfidence ?? c.contactConfidence ?? "—") +
      "</strong></p>" +
      "<p>Last verified: " +
      esc(c.lastVerifiedAt || "—") +
      "</p>" +
      (backups ? "<h4>Backup contacts</h4><ul>" + backups + "</ul>" : "")
    );
  }

  function trunc(s, n) {
    s = s == null ? "" : String(s);
    if (s.length <= n) return s;
    return s.slice(0, n) + "…";
  }

  function oppById(id) {
    for (var i = 0; i < state.opportunities.length; i++) {
      if (state.opportunities[i].id === id) return state.opportunities[i];
    }
    return null;
  }

  function briefHeading() {
    return DEFAULT_BRIEF_QUESTION;
  }

  function entityName() {
    return (state.resolve && state.resolve.hotelName) || "Hotel";
  }

  function entityMeta() {
    if (!state.resolve) return "";
    var parts = [];
    if (state.resolve.city) parts.push(state.resolve.city);
    if (state.resolve.state) parts.push(state.resolve.state);
    return parts.join(", ");
  }

  function qualifiedCount() {
    var s = (state.resolve && state.resolve.summary) || {};
    if (s.qualifiedCount != null) return s.qualifiedCount;
    return state.opportunities.length;
  }

  function fitLabel(key) {
    var map = {
      physicalFit: "Physical Fit",
      geographyFit: "Demand Territory Fit",
      timing: "Timing / Winnability",
      commercialValue: "Commercial Potential",
      historicalFit: "Historical Hotel / Brand Fit",
      competitiveAccessibility: "Competitive Accessibility",
      contactability: "Contactability",
    };
    return map[key] || key;
  }

  function openDetail(id) {
    api(
      "/api/group-demand-intelligence/share/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/opportunities/" +
        encodeURIComponent(id)
    ).then(function (data) {
      var o = data.opportunity;
      var audit = data.scoreAudit || {};
      var comps = (audit.hotelFit && audit.hotelFit.components) || {};
      var labels = o.hotelFitComponentLabels || {};
      var sources = (o.sources || [])
        .map(function (s) {
          var link = s.url
            ? '<a href="' + esc(s.url) + '" target="_blank" rel="noopener">' + esc(s.name) + "</a>"
            : esc(s.name);
          return (
            "<li>" +
            link +
            (s.supportsFact ? " — " + esc(s.supportsFact) : "") +
            "</li>"
          );
        })
        .join("");
      drawerTitle.textContent = o.title;
      drawerBody.innerHTML =
        '<div class="gdi-section"><h3>1. Opportunity Summary</h3><p>' +
        esc(o.summaryWhat) +
        "</p></div>" +
        '<div class="gdi-section"><h3>2. Opportunity Type</h3><p><strong>' +
        esc(o.opportunityTypeLabel || o.opportunityType || "—") +
        "</strong></p></div>" +
        '<div class="gdi-section"><h3>3. Venue / Sourcing Status</h3><p><strong>' +
        esc(o.venueSourcingStatusLabel || o.venueSourcingStatus || "—") +
        "</strong></p><p>" +
        esc(o.venueSourcingRationale || "") +
        "</p></div>" +
        '<div class="gdi-section"><h3>4. Hotel Opportunity Thesis</h3><p>' +
        esc(o.hotelOpportunityThesis || o.bethesdaWinThesis || "—") +
        "</p></div>" +
        '<div class="gdi-section"><h3>5. Room Demand</h3><p><strong>' +
        esc(o.roomDemandStatusLabel || o.roomDemandStatus || "—") +
        "</strong></p><p>" +
        esc(o.roomDemandRationale || "") +
        "</p><p>Attendees: " +
        esc(fmtVal(o.estimatedAttendance)) +
        " · Peak rooms: " +
        esc(fmtVal(o.estimatedPeakRooms)) +
        "</p></div>" +
        '<div class="gdi-section"><h3>6. Why this hotel?</h3><p>' +
        esc(o.summaryWhyHotel || o.fitExplanation) +
        "</p>" +
        '<div class="gdi-score-grid">' +
        '<div class="gdi-score-cell"><div class="n">' +
        esc(o.hotelFitScore) +
        '</div><div class="l">Hotel Fit</div></div>' +
        Object.keys(comps)
          .map(function (k) {
            return (
              '<div class="gdi-score-cell"><div class="n">' +
              esc(comps[k]) +
              '</div><div class="l">' +
              esc(labels[k] || fitLabel(k)) +
              "</div></div>"
            );
          })
          .join("") +
        "</div><p>" +
        esc((audit.hotelFit && audit.hotelFit.explanation) || o.fitExplanation || "") +
        "</p></div>" +
        '<div class="gdi-section"><h3>7. Why Now?</h3><p><strong>' +
        esc(o.bookingWindowLabel || o.bookingWindowStatus) +
        "</strong></p><p>" +
        esc(o.whyNow) +
        "</p></div>" +
        '<div class="gdi-section"><h3>8–9. Qualification & Evidence</h3><p>Qualification: <strong>' +
        esc(o.opportunityQualificationLabel || o.opportunityQualification || "—") +
        "</strong></p><p>Evidence Confidence: <strong>" +
        esc(o.evidenceConfidence) +
        "</strong></p><p>" +
        esc(o.evidenceConfidenceExplanation || audit.evidenceConfidenceExplanation || "") +
        "</p></div>" +
        '<div class="gdi-section"><h3>10. Event Location</h3><p>' +
        esc(o.eventLocationStatusLabel || o.eventLocationStatus || "—") +
        " · " +
        esc(o.eventLocationSummary || o.destinationStatus || "—") +
        "</p><p>Demand Territory: " +
        esc(o.demandTerritoryFitLabel || "—") +
        "</p></div>" +
        '<div class="gdi-section"><h3>11. Who Should Sales Contact?</h3>' +
        whoShouldSalesContactHtml(o) +
        "</div>" +
        '<div class="gdi-section"><h3>12. Competitive Context</h3><p>STR: ' +
        esc(o.likelyStrCompetitor || "—") +
        " · Group alternative: " +
        esc(o.likelyGroupCompetitor || "—") +
        "</p></div>" +
        '<div class="gdi-section"><h3>13. Sources</h3><ul>' +
        (sources || "<li>See opportunity evidence.</li>") +
        "</ul></div>" +
        '<div class="gdi-section"><h3>14. Recommended Action</h3><p>' +
        esc(o.recommendedAction) +
        "</p></div>" +
        '<div class="gdi-section"><h3>Why This Matters</h3><p>' +
        esc(o.summaryWhyMatters) +
        "</p></div>" +
        validationFormHtml(o);
      if (typeof drawer.showModal === "function") drawer.showModal();
      else drawer.setAttribute("open", "open");
      wireValidationForm(o.id);
    });
  }

  function renderBriefCard(it) {
    var linked = oppById(it.opportunityId);
    var priority = linked ? linked.priority : it.priority || "WATCHLIST";
    var contact = contactLabel(it.primaryContact);
    return (
      '<article class="gdi-brief-card">' +
      '<div class="gdi-brief-card__top">' +
      '<div class="gdi-brief-card__title-row">' +
      priorityPill(priority) +
      "<h3>" +
      esc(it.title) +
      "</h3></div>" +
      '<div class="gdi-brief-card__date">' +
      esc(it.eventTiming || "Dates TBD") +
      "</div></div>" +
      '<div class="gdi-brief-meta">' +
      "<span>" +
      esc(it.organizationName) +
      "</span><span>·</span><span>" +
      esc(it.segment) +
      '</span><span class="gdi-score-chip">Hotel Fit <strong>' +
      esc(it.hotelFitScore) +
      '</strong></span><span class="gdi-score-chip">Qualification <strong>' +
      esc(
        (linked && (linked.opportunityQualificationLabel || linked.opportunityQualification)) ||
          it.opportunityQualificationLabel ||
          it.opportunityQualification ||
          "—"
      ) +
      '</strong></span><span class="gdi-score-chip">Evidence Confidence <strong>' +
      esc(it.evidenceConfidence) +
      "</strong></span></div>" +
      '<div class="gdi-brief-grid">' +
      '<div><span class="gdi-meta-label">Opportunity Type</span><span class="gdi-meta-value">' +
      esc(
        (linked && (linked.opportunityTypeLabel || linked.opportunityType)) ||
          it.opportunityTypeLabel ||
          it.opportunityType ||
          "—"
      ) +
      '</span></div><div><span class="gdi-meta-label">Size</span><span class="gdi-meta-value">' +
      esc(it.estimatedSize) +
      '</span></div><div><span class="gdi-meta-label">Demand Territory</span><span class="gdi-meta-value">' +
      esc(it.demandTerritoryFitLabel || "—") +
      '</span></div><div><span class="gdi-meta-label">Sourcing Status</span><span class="gdi-meta-value">' +
      esc(it.sourcingStatusLabel || "Unknown — requires hotel validation") +
      '</span></div><div><span class="gdi-meta-label">Why Now</span><span class="gdi-meta-value">' +
      esc(it.whyNow) +
      '</span></div><div><span class="gdi-meta-label">Why This Matters</span><span class="gdi-meta-value">' +
      esc(it.whyItMatters) +
      "</span></div>" +
      briefContactBlock(it, linked) +
      "</div>" +
      '<div class="gdi-brief-action">' +
      '<div class="gdi-brief-action__label">Recommended Action</div>' +
      '<p class="gdi-brief-action__text">' +
      esc(it.recommendedNextStep) +
      '</p><div class="gdi-brief-footer">' +
      '<span class="gdi-brief-contact">' +
      esc(it.bookingWindowLabel || "") +
      '</span><button type="button" class="gdi-btn gdi-btn-primary" data-open="' +
      esc(it.opportunityId) +
      '">View Details</button></div></div></article>'
    );
  }

  function renderBrief() {
    var items = (state.brief && state.brief.items) || [];
    if (!items.length) return '<div class="gdi-empty">No weekly brief items available.</div>';
    return (
      '<p class="gdi-lede">' +
      esc(briefHeading()) +
      '</p><div class="gdi-brief-list">' +
      items.map(renderBriefCard).join("") +
      "</div>"
    );
  }

  function filteredOpps() {
    var rows = state.opportunities.slice();
    if (state.filters.priority) {
      rows = rows.filter(function (o) {
        return o.priority === state.filters.priority;
      });
    }
    if (state.filters.segment) {
      rows = rows.filter(function (o) {
        return o.segment === state.filters.segment;
      });
    }
    if (state.filters.booking) {
      rows = rows.filter(function (o) {
        return o.bookingWindowStatus === state.filters.booking;
      });
    }
    if (state.filters.territory) {
      rows = rows.filter(function (o) {
        return o.demandTerritoryFit === state.filters.territory;
      });
    }
    return rows;
  }

  function renderFilters(segments) {
    return (
      '<div class="gdi-filters">' +
      '<select id="gdiFilterPriority" aria-label="Filter by priority">' +
      '<option value="">All priorities</option>' +
      '<option value="HIGH_PRIORITY">High</option>' +
      '<option value="MEDIUM_PRIORITY">Medium</option>' +
      '<option value="WATCHLIST">Watchlist</option>' +
      "</select>" +
      '<select id="gdiFilterSegment" aria-label="Filter by segment">' +
      '<option value="">All segments</option>' +
      segments
        .map(function (seg) {
          return (
            '<option value="' +
            esc(seg) +
            '"' +
            (state.filters.segment === seg ? " selected" : "") +
            ">" +
            esc(seg) +
            "</option>"
          );
        })
        .join("") +
      "</select>" +
      '<select id="gdiFilterBooking" aria-label="Filter by booking window">' +
      '<option value="">All booking windows</option>' +
      '<option value="CONTACT_NOW">Contact Now</option>' +
      '<option value="QUALIFY_NOW">Qualify Now</option>' +
      '<option value="WATCH">Watch</option>' +
      '<option value="TOO_EARLY">Too Early</option>' +
      "</select>" +
      '<select id="gdiFilterTerritory" aria-label="Filter by territory">' +
      '<option value="">All territories</option>' +
      '<option value="BETHESDA_MONTGOMERY_CORE">Bethesda / Montgomery Core</option>' +
      '<option value="NORTH_DC_MEDICAL_CORRIDOR">North DC / Medical Corridor</option>' +
      '<option value="DMV_COMPETITIVE">DMV Competitive</option>' +
      '<option value="DMV_STRETCH">DMV Stretch</option>' +
      "</select></div>"
    );
  }

  function renderOpps() {
    if (!state.opportunities.length) return '<div class="gdi-empty">No opportunities.</div>';
    var segments = Array.from(
      new Set(
        state.opportunities.map(function (o) {
          return o.segment;
        })
      )
    ).filter(Boolean);
    var rows = filteredOpps();
    var table =
      rows.length === 0
        ? '<div class="gdi-empty">No opportunities match the current filters.</div>'
        : '<div class="gdi-table-wrap"><table class="gdi-table gdi-table--adp"><thead><tr>' +
          "<th scope=\"col\">Priority</th><th scope=\"col\">Opportunity</th><th scope=\"col\">Type</th><th scope=\"col\">Organization</th><th scope=\"col\">Segment</th>" +
          "<th scope=\"col\">Event Date</th><th scope=\"col\">Size</th><th scope=\"col\">Booking Window</th><th scope=\"col\">Hotel Fit</th>" +
          "<th scope=\"col\">Qualification</th><th scope=\"col\">Room Demand</th><th scope=\"col\">Venue Status</th>" +
          "<th scope=\"col\">Evidence</th><th scope=\"col\">Why Now</th><th scope=\"col\">Contact</th><th scope=\"col\">Validation</th><th scope=\"col\">Action</th>" +
          "</tr></thead><tbody>" +
          rows
            .map(function (o) {
              var v = o.shareValidation;
              var vLabel = v
                ? (state.validationEnums &&
                    state.validationEnums.familiarityLabels &&
                    state.validationEnums.familiarityLabels[v.familiarityStatus]) ||
                  v.familiarityStatus ||
                  "Validated"
                : "Pending";
              return (
                "<tr><td>" +
                priorityPill(o.priority) +
                '</td><td class="gdi-col-opportunity"><button type="button" class="gdi-link" data-open="' +
                esc(o.id) +
                '">' +
                esc(o.title) +
                "</button></td><td>" +
                esc(o.opportunityTypeLabel || o.opportunityType || "—") +
                "</td><td>" +
                esc(o.organizationName) +
                "</td><td>" +
                esc(o.segment) +
                "</td><td>" +
                esc(o.eventStartDate || "TBD") +
                "</td><td>" +
                esc(fmtVal(o.estimatedAttendance)) +
                "</td><td>" +
                esc(o.bookingWindowStatus) +
                "</td><td>" +
                esc(o.hotelFitScore) +
                "</td><td>" +
                esc(o.opportunityQualificationLabel || o.opportunityQualification || "—") +
                "</td><td>" +
                esc(o.roomDemandStatusLabel || o.roomDemandStatus || "—") +
                "</td><td>" +
                esc(o.venueSourcingStatusLabel || o.venueSourcingStatus || "—") +
                "</td><td>" +
                esc(o.evidenceConfidence) +
                '</td><td class="gdi-why-cell">' +
                esc(trunc(o.whyNow, 90)) +
                "</td><td>" +
                esc(contactLabel(o.primaryContact)) +
                "</td><td>" +
                esc(vLabel) +
                '</td><td><button type="button" class="gdi-btn" data-open="' +
                esc(o.id) +
                '">Open</button></td></tr>'
              );
            })
            .join("") +
          "</tbody></table></div>";
    return renderFilters(segments) + table;
  }

  function territoryCounts() {
    var counts = {
      BETHESDA_MONTGOMERY_CORE: 0,
      NORTH_DC_MEDICAL_CORRIDOR: 0,
      DMV_COMPETITIVE: 0,
      DMV_STRETCH: 0,
    };
    (state.opportunities || []).forEach(function (o) {
      if (counts[o.demandTerritoryFit] != null) counts[o.demandTerritoryFit] += 1;
    });
    return counts;
  }

  function render() {
    if (loading) loading.style.display = "none";
    var s = (state.resolve && state.resolve.summary) || {};
    var tc = territoryCounts();
    root.innerHTML =
      '<div class="gdi-share-brand">' +
      '<div class="gdi-share-brand__mark"><span class="gdi-share-brand__logo">D</span>Dealality</div>' +
      '<div class="gdi-share-brand__meta"><span class="gdi-badge">PILOT</span><span class="gdi-badge gdi-badge--readonly">Read-only</span></div>' +
      "</div>" +
      '<header class="gdi-header">' +
      '<div class="gdi-header__eyebrow"><span class="gdi-badge">PILOT</span></div>' +
      "<h1>Group Demand Intelligence</h1>" +
      '<p class="gdi-header__subtitle">Identifies group demand the hotel may be positioned to pursue, why the opportunity matters, and the recommended sales action.</p>' +
      '<div class="gdi-entity"><span class="gdi-entity__name">' +
      esc(entityName()) +
      '</span><span class="gdi-entity__meta">' +
      esc(entityMeta()) +
      "</span></div></header>" +
      '<div class="gdi-kpis">' +
      [
        ["Opportunities", qualifiedCount()],
        ["High", s.highPriorityCount || 0],
        ["Medium", s.mediumPriorityCount || 0],
        ["Watchlist", s.watchlistCount || 0],
        ["Validated", s.validated || 0],
        ["Pending", s.pendingValidation != null ? s.pendingValidation : qualifiedCount()],
        ["Confirmed new", s.confirmedNew || 0],
        ["Already known", s.alreadyKnown || 0],
        ["Worth now", s.worthPursuingNow || 0],
        ["Not relevant", s.notRelevant || 0],
      ]
        .map(function (k) {
          return (
            '<div class="gdi-kpi"><div class="val">' +
            esc(k[1]) +
            '</div><div class="lab">' +
            esc(k[0]) +
            "</div></div>"
          );
        })
        .join("") +
      "</div>" +
      '<div class="gdi-kpis gdi-kpis--territory">' +
      [
        ["Bethesda / Montgomery Core", tc.BETHESDA_MONTGOMERY_CORE],
        ["North DC / Medical Corridor", tc.NORTH_DC_MEDICAL_CORRIDOR],
        ["DMV Competitive", tc.DMV_COMPETITIVE],
        ["DMV Stretch", tc.DMV_STRETCH],
      ]
        .map(function (k) {
          return (
            '<div class="gdi-kpi"><div class="val">' +
            esc(k[1]) +
            '</div><div class="lab">' +
            esc(k[0]) +
            "</div></div>"
          );
        })
        .join("") +
      "</div>" +
      '<div class="gdi-tabs" role="tablist">' +
      '<button type="button" class="gdi-tab" data-tab="brief" aria-selected="' +
      (state.tab === "brief") +
      '">WEEKLY BRIEF</button>' +
      '<button type="button" class="gdi-tab" data-tab="opportunities" aria-selected="' +
      (state.tab === "opportunities") +
      '">OPPORTUNITIES</button></div>' +
      (state.tab === "brief" ? renderBrief() : renderOpps()) +
      '<div class="gdi-disclaimer">Pilot brief for review only. Findings are research-assisted and should be validated by the hotel sales team before outreach.</div>';

    root.querySelectorAll(".gdi-tab").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.tab = btn.getAttribute("data-tab");
        render();
      });
    });

    var fp = document.getElementById("gdiFilterPriority");
    if (fp) {
      fp.value = state.filters.priority;
      fp.addEventListener("change", function () {
        state.filters.priority = fp.value;
        render();
      });
    }
    var fs = document.getElementById("gdiFilterSegment");
    if (fs) {
      fs.addEventListener("change", function () {
        state.filters.segment = fs.value;
        render();
      });
    }
    var fb = document.getElementById("gdiFilterBooking");
    if (fb) {
      fb.value = state.filters.booking;
      fb.addEventListener("change", function () {
        state.filters.booking = fb.value;
        render();
      });
    }
    var ft = document.getElementById("gdiFilterTerritory");
    if (ft) {
      ft.value = state.filters.territory;
      ft.addEventListener("change", function () {
        state.filters.territory = ft.value;
        render();
      });
    }

    root.querySelectorAll("[data-open]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        openDetail(btn.getAttribute("data-open"));
      });
    });
  }

  if (!share) {
    showError("A valid share link is required.");
    return;
  }

  if (drawerClose) {
    drawerClose.addEventListener("click", function () {
      if (typeof drawer.close === "function") drawer.close();
      else drawer.removeAttribute("open");
    });
  }

  api("/api/group-demand-intelligence/share/resolve")
    .then(function (resolve) {
      state.resolve = resolve;
      state.hotelId = resolve.hotelId;
      return Promise.all([
        api(
          "/api/group-demand-intelligence/share/hotels/" +
            encodeURIComponent(resolve.hotelId) +
            "/weekly-brief"
        ),
        api(
          "/api/group-demand-intelligence/share/hotels/" +
            encodeURIComponent(resolve.hotelId) +
            "/opportunities"
        ),
      ]);
    })
    .then(function (parts) {
      state.brief = parts[0].brief;
      state.opportunities = parts[1].opportunities || [];
      state.validationEnums = parts[1].validationEnums || null;
      render();
    })
    .catch(function (err) {
      showError(err.message || "Unable to open share link.");
    });
})();
