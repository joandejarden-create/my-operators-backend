(function () {
  "use strict";

  var PILOT_HOTEL_ID = "recLuxvwwxID7U2B8";
  var DEFAULT_BRIEF_QUESTION = "What should the sales team pursue?";
  var root = document.getElementById("gdiRoot");
  var loading = document.getElementById("gdiLoading");
  var drawer = document.getElementById("gdiDrawer");
  var drawerTitle = document.getElementById("gdiDrawerTitle");
  var drawerBody = document.getElementById("gdiDrawerBody");
  var drawerClose = document.getElementById("gdiDrawerClose");

  var state = {
    hotelId: PILOT_HOTEL_ID,
    summary: null,
    opportunities: [],
    brief: null,
    runs: [],
    tab: "brief",
    sortKey: "priority",
    sortDir: 1,
    filters: { priority: "", segment: "", booking: "", territory: "" },
    isAdmin: false,
    flag: null,
  };

  // Prefer plain fetch for GDI pilot reads (PILOT_READ allows unauth).
  // Memberstack is used only when a session exists and for admin actions.
  function plainFetch(url, opts) {
    return fetch(url, opts);
  }

  var memberFetch =
    window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.fetchMyDealsApi
      ? window.DealalityMemberstackAuth.fetchMyDealsApi.bind(window.DealalityMemberstackAuth)
      : null;

  function fetchFn(url, opts) {
    opts = opts || {};
    var method = String(opts.method || "GET").toUpperCase();
    var needsAuthWrite = method !== "GET" && method !== "HEAD";
    if (needsAuthWrite && memberFetch) return memberFetch(url, opts);
    return plainFetch(url, opts).then(function (r) {
      if (r.status === 401 && memberFetch) return memberFetch(url, opts);
      return r;
    });
  }

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

  function formatDate(iso) {
    if (!iso) return "Never";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso);
    return d.toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }

  function applyEmbedMode() {
    var params = new URLSearchParams(window.location.search || "");
    var inFrame = false;
    try {
      inFrame = window !== window.top;
    } catch (e) {
      inFrame = true;
    }
    if (inFrame || params.get("embed") === "1") {
      document.body.classList.add("embed-mode");
    }
  }

  function showError(msg) {
    if (loading) loading.style.display = "none";
    root.innerHTML =
      '<div class="gdi-error" role="alert"><p>' +
      esc(msg) +
      "</p><p>Enable GROUP_DEMAND_INTELLIGENCE_V1 or GROUP_DEMAND_INTELLIGENCE_PILOT_READ for local pilot read. Admin actions still require sign-in.</p></div>";
  }

  function priorityPill(p) {
    if (p === "HIGH_PRIORITY") return '<span class="gdi-pill gdi-pill-high">High</span>';
    if (p === "MEDIUM_PRIORITY") return '<span class="gdi-pill gdi-pill-med">Medium</span>';
    return '<span class="gdi-pill gdi-pill-watch">Watchlist</span>';
  }

  function countEnteringWindow() {
    return state.opportunities.filter(function (o) {
      return (
        o.bookingWindowStatus === "CONTACT_NOW" ||
        o.bookingWindowStatus === "QUALIFY_NOW" ||
        o.bookingWindowStatus === "RESEARCH_FURTHER" ||
        (o.labels || []).indexOf("entering_booking_window") >= 0
      );
    }).length;
  }

  function qualifiedCount() {
    var s = state.summary || {};
    if (s.qualifiedCount != null) return s.qualifiedCount;
    return state.opportunities.length;
  }

  function oppById(id) {
    for (var i = 0; i < state.opportunities.length; i++) {
      if (state.opportunities[i].id === id) return state.opportunities[i];
    }
    return null;
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
    if (!c) {
      return "<p>No usable contact resolved yet.</p>";
    }
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
      (backups
        ? "<h4>Backup contacts</h4><ul>" + backups + "</ul>"
        : "")
    );
  }

  function trunc(s, n) {
    s = s == null ? "" : String(s);
    if (s.length <= n) return s;
    return s.slice(0, n) + "…";
  }

  function briefHeading() {
    return DEFAULT_BRIEF_QUESTION;
  }

  function api(path, opts) {
    return fetchFn(path, opts).then(function (r) {
      return r.json().then(function (data) {
        if (!r.ok || data.ok === false) {
          var err = new Error(data.message || data.error || "Request failed");
          err.status = r.status;
          err.data = data;
          throw err;
        }
        return data;
      });
    });
  }

  function loadAll() {
    var id = encodeURIComponent(state.hotelId);
    return Promise.all([
      api("/api/group-demand-intelligence/flag"),
      api("/api/group-demand-intelligence/hotels/" + id + "/summary"),
      api("/api/group-demand-intelligence/hotels/" + id + "/opportunities"),
      api("/api/group-demand-intelligence/hotels/" + id + "/weekly-brief"),
      api("/api/group-demand-intelligence/hotels/" + id + "/research-runs"),
    ]).then(function (parts) {
      state.flag = parts[0].flag;
      state.summary = parts[1].summary;
      state.opportunities = parts[2].opportunities || [];
      state.brief = parts[3].brief;
      state.runs = parts[4].runs || [];
      render();
    });
  }

  function runResearch() {
    if (
      !window.confirm(
        "Run Group Demand research for this hotel? This does not auto-spend Webhound unless configured in the run payload."
      )
    ) {
      return;
    }
    if (loading) {
      loading.style.display = "block";
      loading.textContent = "Running research…";
    }
    api("/api/group-demand-intelligence/hotels/" + encodeURIComponent(state.hotelId) + "/research/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    })
      .then(function () {
        return loadAll();
      })
      .catch(function (err) {
        showError(err.message || "Research run failed");
      });
  }

  function filteredSorted() {
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
    var key = state.sortKey;
    var dir = state.sortDir;
    rows.sort(function (a, b) {
      var av = a[key];
      var bv = b[key];
      if (key === "priority") {
        var rank = { HIGH_PRIORITY: 0, MEDIUM_PRIORITY: 1, WATCHLIST: 2 };
        av = rank[a.priority] != null ? rank[a.priority] : 9;
        bv = rank[b.priority] != null ? rank[b.priority] : 9;
      }
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
      return String(av || "").localeCompare(String(bv || "")) * dir;
    });
    return rows;
  }

  function openDetail(id) {
    api(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/opportunities/" +
        encodeURIComponent(id)
    ).then(function (data) {
      var o = data.opportunity;
      drawerTitle.textContent = o.title;
      drawerBody.innerHTML = renderDetail(o, data.scoreAudit);
      if (typeof drawer.showModal === "function") drawer.showModal();
      else drawer.setAttribute("open", "open");
    });
  }

  function claimLabel(kind) {
    if (kind === "FACT") return "VERIFIED";
    if (kind === "INFERENCE") return "INFERRED";
    if (kind === "ESTIMATED") return "ESTIMATED";
    return kind || "UNKNOWN";
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

  function incrementalBadge(o) {
    var s = o.incrementalValueStatus || "";
    if (s === "NEW_TO_HOTEL" || s === "NEW_OPPORTUNITY") {
      return '<span class="gdi-pill gdi-pill-high">NEW TO HOTEL</span>';
    }
    if (s.indexOf("ALREADY_KNOWN") === 0) {
      return '<span class="gdi-pill gdi-pill-med">ALREADY KNOWN</span>';
    }
    if ((o.sourcingStatus || "").indexOf("HOTEL_CONFIRMED_ALREADY") === 0) {
      return '<span class="gdi-pill gdi-pill-watch">ALREADY SOURCED</span>';
    }
    return "";
  }

  function renderDetail(o, audit) {
    var hist = (o.meetingHistory || [])
      .map(function (h) {
        return "<li>" + esc(h.year) + " — " + esc(h.city) + " (" + esc(claimLabel(h.claimKind)) + ")</li>";
      })
      .join("");
    var comps = (audit && audit.hotelFit && audit.hotelFit.components) || {};
    var labels = (o.hotelFitComponentLabels) || {};
    var sources = (o.sources || [])
      .map(function (s) {
        var link = s.url
          ? '<a href="' + esc(s.url) + '" target="_blank" rel="noopener">' + esc(s.name) + "</a>"
          : esc(s.name);
        return (
          "<li>" +
          link +
          (s.sourceType ? " · " + esc(s.sourceType) : "") +
          (s.supportsFact ? " — supports " + esc(s.supportsFact) : "") +
          (s.date ? " · " + esc(s.date) : "") +
          "</li>"
        );
      })
      .join("");
    var verified = ((o.knownVsEstimated && o.knownVsEstimated.verified) || [])
      .map(function (x) {
        return "<li><strong>" + esc(x.field) + ":</strong> " + esc(x.value) + " <em>(" + esc(x.status) + ")</em></li>";
      })
      .join("");
    var estimated = ((o.knownVsEstimated && o.knownVsEstimated.estimated) || [])
      .map(function (x) {
        return "<li><strong>" + esc(x.field) + ":</strong> " + esc(x.value) + " <em>(" + esc(x.status) + ")</em></li>";
      })
      .join("");
    var inferred = ((o.knownVsEstimated && o.knownVsEstimated.inferred) || [])
      .map(function (x) {
        return "<li><strong>" + esc(x.field) + ":</strong> " + esc(x.value) + " <em>(" + esc(x.status) + ")</em></li>";
      })
      .join("");
    var compsList = (o.competitors || [])
      .map(function (c) {
        return (
          "<li><strong>" +
          esc(c.name) +
          "</strong> · " +
          esc(c.classLabel || c.class) +
          "<br/><span class='gdi-meta-value'>" +
          esc(c.reason || "") +
          "</span></li>"
        );
      })
      .join("");

    return (
      '<div class="gdi-section"><h3>1. Opportunity Summary</h3><p>' +
      esc(o.summaryWhat) +
      "</p><p>" +
      esc(o.organizationName) +
      " · " +
      esc(o.segment) +
      " · " +
      esc(o.eventStartDate || "TBD") +
      " → " +
      esc(o.eventEndDate || "TBD") +
      "</p></div>" +
      '<div class="gdi-section"><h3>2. Opportunity Type</h3><p><strong>' +
      esc(o.opportunityTypeLabel || o.opportunityType || "—") +
      "</strong></p></div>" +
      '<div class="gdi-section"><h3>3. Venue / Sourcing Status</h3><p><strong>' +
      esc(o.venueSourcingStatusLabel || o.venueSourcingStatus || "—") +
      "</strong></p><p>" +
      esc(o.venueSourcingRationale || "") +
      "</p><p>Venue note: " +
      esc(o.venueStatus || "—") +
      "</p></div>" +
      '<div class="gdi-section"><h3>4. Hotel Opportunity Thesis</h3><p>' +
      esc(o.hotelOpportunityThesis || o.bethesdaWinThesis || "—") +
      "</p></div>" +
      '<div class="gdi-section"><h3>5. Room Demand</h3><p><strong>' +
      esc(o.roomDemandStatusLabel || o.roomDemandStatus || "—") +
      "</strong>" +
      (o.roomDemandConfidence != null ? " · confidence " + esc(o.roomDemandConfidence) : "") +
      "</p><p>" +
      esc(o.roomDemandRationale || "") +
      "</p><p>Attendees: " +
      esc(fmtVal(o.estimatedAttendance)) +
      " · Peak rooms: " +
      esc(fmtVal(o.estimatedPeakRooms)) +
      " · Published peak: " +
      esc(fmtVal(o.publishedPeakRooms)) +
      "</p></div>" +
      '<div class="gdi-section"><h3>6. Why Bethesda Marriott?</h3><p>' +
      esc(o.summaryWhyHotel || o.fitExplanation) +
      "</p>" +
      (o.bethesdaWinThesis
        ? '<p><strong>Bethesda Win Thesis:</strong> ' + esc(o.bethesdaWinThesis) + "</p>"
        : "") +
      "</div>" +
      '<div class="gdi-section"><h3>7. Why Now?</h3><p><strong>' +
      esc(o.bookingWindowLabel || o.bookingWindowStatus) +
      "</strong></p><p>" +
      esc(o.whyNow) +
      "</p></div>" +
      '<div class="gdi-section"><h3>8. Hotel Fit</h3>' +
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
      "</div><p class='gdi-lede'>" +
      esc((audit && audit.hotelFit && audit.hotelFit.explanation) || o.fitExplanation || "") +
      "</p><p class='gdi-lede'>Hotel Fit answers whether the hotel could serve the business — not whether the opportunity is still open.</p></div>" +
      '<div class="gdi-section"><h3>9. Opportunity Qualification</h3><p><strong>' +
      esc(o.opportunityQualificationLabel || o.opportunityQualification || "—") +
      "</strong></p><p>" +
      esc(o.qualificationNotes || "") +
      "</p>" +
      (o.qualificationFailureReasonLabel
        ? "<p>Failure reason: " + esc(o.qualificationFailureReasonLabel) + "</p>"
        : "") +
      "</div>" +
      '<div class="gdi-section"><h3>10. Evidence Confidence <span class="gdi-help" title="' +
      esc(o.evidenceConfidenceTooltip || "How confident Dealality is in the facts underlying the opportunity based on source quality, recency, corroboration and how much is verified versus inferred.") +
      '">?</span></h3><div class="gdi-score-grid"><div class="gdi-score-cell"><div class="n">' +
      esc(o.evidenceConfidence) +
      '</div><div class="l">Evidence Confidence</div></div></div><p>' +
      esc(o.evidenceConfidenceExplanation || (audit && audit.evidenceConfidenceExplanation) || "") +
      "</p></div>" +
      '<div class="gdi-section"><h3>11. Event Location</h3><p><strong>' +
      esc(o.eventLocationStatusLabel || o.eventLocationStatus || "—") +
      "</strong></p><p>" +
      esc(o.eventLocationSummary || o.destinationStatus || "—") +
      "</p><p>Demand Territory: " +
      esc(o.demandTerritoryFitLabel || "—") +
      "</p><p>" +
      esc(o.demandTerritoryRationale || "") +
      "</p></div>" +
      '<div class="gdi-section"><h3>12. Meeting / Venue History</h3><ul>' +
      (hist || "<li>None captured</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>13. Reactivation Signals</h3><p><strong>' +
      esc(o.reactivationSignalLabel || o.reactivationSignal || "Unknown") +
      "</strong></p><p>" +
      esc(o.reactivationThesis || "No reactivation thesis.") +
      "</p></div>" +
      '<div class="gdi-section"><h3>14. Competitive Context</h3><p><strong>Likely STR competitor:</strong> ' +
      esc(o.likelyStrCompetitor || "—") +
      "</p><p><strong>Likely group alternative:</strong> " +
      esc(o.likelyGroupCompetitor || "—") +
      "</p><p>" +
      esc(o.competitorRationale || "") +
      "</p><ul>" +
      (compsList || "<li>See hotel STR set and group alternatives in configuration.</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>15. Who Should Sales Contact?</h3>' +
      whoShouldSalesContactHtml(o) +
      "</div>" +
      '<div class="gdi-section"><h3>16. Sources</h3><ul>' +
      (sources || "<li>No sources listed.</li>") +
      "</ul><h4>What we know</h4><ul>" +
      (verified || "<li>See evidence table for verified fields.</li>") +
      "</ul><h4>Estimated</h4><ul>" +
      (estimated || "<li>No estimated fields highlighted.</li>") +
      "</ul><h4>Inferred</h4><ul>" +
      (inferred || "<li>No inferred fields highlighted.</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>17. Recommended Action</h3><p>' +
      esc(o.recommendedAction) +
      "</p><p>" +
      esc(o.summaryWhyMatters || "") +
      "</p></div>" +
      '<div class="gdi-section"><h3>18. Hotel Validation</h3><div class="gdi-feedback">' +
      '<label>Familiarity<select id="gdiFbFamiliarity"><option value="">—</option><option>Never Seen</option><option>Familiar</option><option>Already Received</option><option>Already Pursuing</option></select></label>' +
      '<label>Commercial Status<select id="gdiFbCommercial"><option value="">—</option><option>Worth Pursuing</option><option>Not a Fit</option><option>Already Lost</option><option>Already Won</option><option>Already Booked Elsewhere</option></select></label>' +
      '<label>Value<select id="gdiFbValue"><option value="">—</option><option>Excellent</option><option>Useful</option><option>Marginal</option><option>No Incremental Value</option></select></label>' +
      '<label>Incremental?<select id="gdiFbIncremental"><option value="">—</option><option value="NEW_TO_HOTEL">New to hotel</option><option value="ALREADY_KNOWN_USEFUL_ADDITIONAL">Already known / useful additional</option><option value="ALREADY_KNOWN_NO_INCREMENTAL">Already known / no incremental value</option><option value="DUPLICATE_OF_EXISTING_SALES_LEAD">Duplicate of existing sales lead</option><option value="NEW_TIMING_CONTACT_COMPETITIVE_INTEL">New timing / contact / competitive intel</option></select></label>' +
      '<label>Qualification failure<select id="gdiFbFailReason"><option value="">—</option><option value="VENUE_ALREADY_SELECTED">Venue already selected</option><option value="HOTEL_ALREADY_SELECTED">Hotel already selected</option><option value="NO_OVERFLOW_OPPORTUNITY">No overflow opportunity</option><option value="EVENT_LOCATION_POOR_FIT">Event location poor fit</option><option value="ROOM_DEMAND_TOO_SMALL">Room demand too small</option><option value="MOST_ATTENDEES_LOCAL">Most attendees local</option><option value="WRONG_EVENT_CYCLE">Wrong event cycle</option><option value="CONTACT_NOT_RELEVANT">Contact not relevant</option><option value="DUPLICATE">Duplicate</option><option value="TOO_EARLY">Too early</option><option value="TOO_LATE">Too late</option><option value="WEAK_EVIDENCE">Weak evidence</option><option value="NOT_HOTEL_DEMAND">Not hotel demand</option><option value="OTHER">Other</option></select></label>' +
      '<textarea id="gdiFbComment" placeholder="Optional comment"></textarea>' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiFbSave" data-id="' +
      esc(o.id) +
      '">Save Hotel Validation</button></div><p class="gdi-lede">Validation is stored as hotel feedback. It does not overwrite public research facts. Failure labels are for evaluation, not automatic retraining.</p></div>'
    );
  }

  function renderTable() {
    var rows = filteredSorted();
    if (!rows.length) {
      return '<div class="gdi-empty">No opportunities match the current filters. Run research or clear filters.</div>';
    }
    return (
      '<div class="gdi-table-wrap"><table class="gdi-table"><thead><tr>' +
      [
        ["priority", "Priority"],
        ["title", "Opportunity"],
        ["opportunityType", "Type"],
        ["organizationName", "Organization"],
        ["segment", "Segment"],
        ["eventStartDate", "Event Date"],
        ["estimatedAttendance", "Size"],
        ["bookingWindowStatus", "Booking Window"],
        ["hotelFitScore", "Hotel Fit"],
        ["opportunityQualification", "Qualification"],
        ["roomDemandStatus", "Room Demand"],
        ["venueSourcingStatus", "Venue Status"],
        ["evidenceConfidence", "Evidence Confidence"],
        ["demandTerritoryFit", "Demand Territory"],
        ["whyNow", "Why Now"],
        ["primaryContact", "Contact"],
      ]
        .map(function (c) {
          return '<th data-sort="' + c[0] + '">' + c[1] + "</th>";
        })
        .join("") +
      "<th>Action</th></tr></thead><tbody>" +
      rows
        .map(function (o) {
          return (
            "<tr><td>" +
            priorityPill(o.priority) +
            '</td><td><button type="button" class="gdi-link" data-open="' +
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
            esc(o.bookingWindowLabel || o.bookingWindowStatus) +
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
            "</td><td>" +
            esc(o.demandTerritoryFitLabel || o.demandTerritoryFit || "—") +
            '</td><td class="gdi-why-cell">' +
            esc(trunc(o.whyNow, 90)) +
            "</td><td>" +
            esc(contactLabel(o.primaryContact)) +
            '</td><td><button type="button" class="gdi-btn" data-open="' +
            esc(o.id) +
            '">Open</button></td></tr>'
          );
        })
        .join("") +
      "</tbody></table></div>"
    );
  }

  function renderBriefCard(it) {
    var linked = oppById(it.opportunityId);
    var priority = linked ? linked.priority : it.priority || "WATCHLIST";
    var contact = contactLabel(it.primaryContact);
    var badge = incrementalBadge(linked || it);
    return (
      '<article class="gdi-brief-card">' +
      '<div class="gdi-brief-card__top">' +
      '<div class="gdi-brief-card__title-row">' +
      priorityPill(priority) +
      badge +
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
      '</strong></span><span class="gdi-score-chip" title="How confident Dealality is in the facts based on source quality, recency, corroboration, and verified vs inferred.">Evidence Confidence <strong>' +
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
    if (!items.length) {
      return '<div class="gdi-empty">No weekly brief items yet. Run research first.</div>';
    }
    return (
      '<p class="gdi-lede">' +
      esc(briefHeading()) +
      '</p><div class="gdi-brief-list">' +
      items.map(renderBriefCard).join("") +
      "</div>"
    );
  }

  function renderAudit() {
    var latest = state.runs[0];
    if (!latest) {
      return '<div class="gdi-empty">No research runs yet.</div>';
    }
    var cost = latest.cost || {};
    return (
      '<div class="gdi-audit">' +
      "<p>Run <strong>" +
      esc(latest.id) +
      "</strong> · " +
      esc(latest.status) +
      "</p>" +
      "<p>Total cost $" +
      esc(cost.totalUsd || 0) +
      " · Webhound $" +
      esc(cost.webhoundUsd || 0) +
      " / hard cap $" +
      esc(cost.webhoundHardCapUsd || 5) +
      "</p><pre>" +
      esc(JSON.stringify(latest, null, 2)) +
      "</pre></div>"
    );
  }

  function territoryCounts() {
    var counts = {
      BETHESDA_MONTGOMERY_CORE: 0,
      NORTH_DC_MEDICAL_CORRIDOR: 0,
      DMV_COMPETITIVE: 0,
      DMV_STRETCH: 0,
    };
    state.opportunities.forEach(function (o) {
      if (counts[o.demandTerritoryFit] != null) counts[o.demandTerritoryFit] += 1;
    });
    return counts;
  }

  function render() {
    if (loading) loading.style.display = "none";
    var s = state.summary || {};
    var segments = Array.from(
      new Set(
        state.opportunities.map(function (o) {
          return o.segment;
        })
      )
    ).filter(Boolean);

    var body =
      state.tab === "brief"
        ? renderBrief()
        : state.tab === "audit"
          ? renderAudit()
          : '<div class="gdi-filters">' +
            '<select id="gdiFilterPriority"><option value="">All priorities</option><option value="HIGH_PRIORITY">High</option><option value="MEDIUM_PRIORITY">Medium</option><option value="WATCHLIST">Watchlist</option></select>' +
            '<select id="gdiFilterSegment"><option value="">All segments</option>' +
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
            '</select><select id="gdiFilterBooking"><option value="">All booking windows</option><option value="CONTACT_NOW">Contact Now</option><option value="QUALIFY_NOW">Qualify Now</option><option value="WATCH">Watch</option><option value="TOO_EARLY">Too Early</option></select>' +
            '<select id="gdiFilterTerritory"><option value="">All territories</option><option value="BETHESDA_MONTGOMERY_CORE">Bethesda / Montgomery Core</option><option value="NORTH_DC_MEDICAL_CORRIDOR">North DC / Medical Corridor</option><option value="DMV_COMPETITIVE">DMV Competitive</option><option value="DMV_STRETCH">DMV Stretch</option></select></div>' +
            renderTable();

    var tc = territoryCounts();
    root.innerHTML =
      '<header class="gdi-header">' +
      '<div class="gdi-header__eyebrow"><span class="gdi-badge">PILOT</span></div>' +
      "<h1>Group Demand Intelligence</h1>" +
      '<p class="gdi-header__subtitle">Identifies group demand the hotel may be positioned to pursue, why the opportunity matters, and the recommended sales action.</p>' +
      '<div class="gdi-entity"><span class="gdi-entity__name">Bethesda Marriott</span><span class="gdi-entity__meta">Bethesda, Maryland · DMV demand territory</span></div>' +
      "</header>" +
      '<div class="gdi-toolbar"><div class="gdi-field"><label for="gdiHotel">Hotel</label><select id="gdiHotel"><option value="' +
      esc(PILOT_HOTEL_ID) +
      '">Bethesda Marriott (Bethesda, MD)</option></select></div>' +
      '<div class="gdi-field"><label>Last research</label><div>' +
      esc(formatDate(s.lastResearchAt)) +
      "</div></div>" +
      '<div class="gdi-field"><label>Run status</label><div>' +
      esc(s.runStatus || "NEVER_RUN") +
      "</div></div>" +
      '<div class="gdi-toolbar-actions">' +
      (memberFetch
        ? '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiRunBtn">Run Research</button>'
        : '<span class="gdi-muted">Read-only pilot view · sign in for research</span>') +
      "</div></div>" +
      '<div class="gdi-kpis">' +
      [
        ["High Priority", s.highPriorityCount || 0],
        ["Medium Priority", s.mediumPriorityCount || 0],
        ["Watchlist", s.watchlistCount || 0],
        ["Qualified", qualifiedCount()],
        ["Entering Booking Window", countEnteringWindow()],
        ["Sources", s.sourceCount || 0],
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
      '">OPPORTUNITIES</button>' +
      '<button type="button" class="gdi-tab" data-tab="audit" aria-selected="' +
      (state.tab === "audit") +
      '">RESEARCH AUDIT</button></div>' +
      '<div id="gdiTabBody">' +
      body +
      "</div>";

    var runBtn = document.getElementById("gdiRunBtn");
    if (runBtn) runBtn.addEventListener("click", runResearch);

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

    root.querySelectorAll("th[data-sort]").forEach(function (th) {
      th.addEventListener("click", function () {
        var k = th.getAttribute("data-sort");
        if (state.sortKey === k) state.sortDir *= -1;
        else {
          state.sortKey = k;
          state.sortDir = 1;
        }
        render();
      });
    });

    root.querySelectorAll("[data-open]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        openDetail(btn.getAttribute("data-open"));
      });
    });
  }

  applyEmbedMode();

  if (drawerClose) {
    drawerClose.addEventListener("click", function () {
      if (typeof drawer.close === "function") drawer.close();
      else drawer.removeAttribute("open");
    });
  }

  drawerBody &&
    drawerBody.addEventListener("click", function (e) {
      var t = e.target;
      if (t && t.id === "gdiFbSave") {
        var id = t.getAttribute("data-id");
        var familiarityEl = document.getElementById("gdiFbFamiliarity");
        var commercialEl = document.getElementById("gdiFbCommercial");
        var valueEl = document.getElementById("gdiFbValue");
        var incrementalEl = document.getElementById("gdiFbIncremental");
        var failReasonEl = document.getElementById("gdiFbFailReason");
        var comment = document.getElementById("gdiFbComment").value;
        var familiarity = familiarityEl ? familiarityEl.value : "";
        var commercialStatus = commercialEl ? commercialEl.value : "";
        var value = valueEl ? valueEl.value : "";
        var incrementalValueStatus = incrementalEl ? incrementalEl.value : "";
        var qualificationFailureReason = failReasonEl ? failReasonEl.value : "";
        api(
          "/api/group-demand-intelligence/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/opportunities/" +
            encodeURIComponent(id) +
            "/feedback",
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              familiarity: familiarity || null,
              commercialStatus: commercialStatus || null,
              value: value || null,
              quality: value || null,
              salesOutcome: commercialStatus || "Not Reviewed",
              incrementalValueStatus: incrementalValueStatus || null,
              qualificationFailureReason: qualificationFailureReason || null,
              comment: comment,
            }),
          }
        )
          .then(function () {
            t.textContent = "Saved";
          })
          .catch(function (err) {
            t.textContent = err.message || "Failed";
          });
      }
    });

  loadAll().catch(function (err) {
    showError(err.message || "Failed to load Group Demand Intelligence");
  });
})();
