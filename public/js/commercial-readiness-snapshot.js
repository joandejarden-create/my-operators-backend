(function () {
  var root = document.getElementById("commercialReadinessRoot");
  if (!root) return;

  var params = new URLSearchParams(window.location.search || "");
  var dealId = (params.get("dealId") || "").trim();
  var standaloneMode = !dealId || dealId.indexOf("rec") !== 0;
  var STANDALONE_STORAGE_KEY = "dealality.commercialReadinessStandalone.v1";
  var state = { status: "Not started", inputs: {}, snapshot: null, labels: {}, enrichNarrative: false };

  function fetchFn(url, opts) {
    if (!standaloneMode && window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.fetchMyDealsApi) {
      return window.DealalityMemberstackAuth.fetchMyDealsApi(url, opts);
    }
    return fetch(url, opts);
  }

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function requiredMark() {
    return ' <span class="crs-required">*</span>';
  }

  function hasValue(value) {
    return String(value == null ? "" : value).trim() !== "";
  }

  function loadStandaloneState() {
    try {
      var raw = window.localStorage.getItem(STANDALONE_STORAGE_KEY);
      if (!raw) return false;
      var parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return false;
      state.status = parsed.status || "Not started";
      state.inputs = parsed.inputs || {};
      state.snapshot = parsed.snapshot || null;
      state.labels = parsed.labels || {};
      state.enrichNarrative = !!parsed.enrichNarrative;
      return true;
    } catch (_) {
      return false;
    }
  }

  function saveStandaloneState() {
    try {
      window.localStorage.setItem(
        STANDALONE_STORAGE_KEY,
        JSON.stringify({
          status: state.status,
          inputs: state.inputs,
          snapshot: state.snapshot,
          labels: state.labels,
          enrichNarrative: !!state.enrichNarrative,
        })
      );
    } catch (_) {}
  }

  function renderChips(labels) {
    var chips = [
      ["Commercial Readiness Level", labels.readinessLevel],
      ["Input Completeness", labels.inputCompleteness],
      ["Performance Data Confidence", labels.performanceDataConfidence],
      ["Observable Evidence Confidence", labels.observableEvidenceConfidence],
      ["OTA Dependency Risk", labels.otaRisk],
      ["Direct Booking Capability", labels.directCapability],
      ["Strategic Diagnosis", labels.strategicDiagnosis],
    ];
    return chips
      .map(function (pair) {
        return '<div class="crs-chip"><span class="crs-chip-label">' + esc(pair[0]) + ":</span> " + esc(pair[1] || "—") + "</div>";
      })
      .join("");
  }

  function renderStatusRow(snapshot) {
    var generatedAt = snapshot && snapshot.generatedAt ? snapshot.generatedAt : "";
    var generatedChip = generatedAt ? '<div class="crs-chip"><span class="crs-chip-label">Last generated</span>' + esc(generatedAt) + "</div>" : "";
    return (
      '<div class="crs-chip-row">' +
      '<div class="crs-chip"><span class="crs-chip-label">Mode</span>' + esc(standaloneMode ? "Standalone" : "Deal-linked") + "</div>" +
      '<div class="crs-chip crs-chip-status"><span class="crs-chip-label">Status</span>' + esc(state.status || "Not started") + "</div>" +
      generatedChip +
      "</div>"
    );
  }

  function renderInputSections(inputs) {
    return (
      '<div class="crs-form-group"><h3 class="crs-section-title">Hotel & Channel Links</h3><div class="crs-grid">' +
      field("Hotel Website URL", "hotelWebsiteUrl", inputs.hotelWebsiteUrl, true) +
      field("Booking.com URL", "bookingComUrl", inputs.bookingComUrl, false) +
      field("Expedia URL", "expediaUrl", inputs.expediaUrl, false) +
      field("Google Business Profile URL", "googleBusinessProfileUrl", inputs.googleBusinessProfileUrl, false) +
      "</div></div>" +
      '<div class="crs-form-group"><h3 class="crs-section-title">Current Commercial Setup</h3><div class="crs-grid">' +
      selectField("Current Brand Status", "currentBrandStatus", inputs.currentBrandStatus, ["Independent", "Branded", "Soft branded", "Unknown"], true) +
      selectField("Current Operator Status", "currentOperatorStatus", inputs.currentOperatorStatus, ["Self-managed", "Third-party operator", "Brand-managed", "Unknown"], true) +
      field("Booking Engine Provider", "bookingEngineProvider", inputs.bookingEngineProvider, false) +
      selectField("CRM / Guest Email Capture", "crmGuestEmailCapture", inputs.crmGuestEmailCapture, ["Yes", "No", "Unknown"], true) +
      "</div></div>" +
      '<div class="crs-form-group"><h3 class="crs-section-title">Channel Mix Assumptions</h3><div class="crs-grid">' +
      selectField("Estimated OTA Share", "estimatedOtaShare", inputs.estimatedOtaShare, ["Unknown", "Low", "Moderate", "High", "Very high"], true) +
      selectField("Estimated Direct Booking Share", "estimatedDirectBookingShare", inputs.estimatedDirectBookingShare, ["Unknown", "Low", "Moderate", "High"], true) +
      field("Actual OTA Booking Share", "actualOtaBookingShare", inputs.actualOtaBookingShare, false) +
      field("Actual Direct Booking Share", "actualDirectBookingShare", inputs.actualDirectBookingShare, false) +
      field("Estimated OTA Commission", "estimatedOtaCommission", inputs.estimatedOtaCommission, false) +
      field("Website Conversion Rate", "websiteConversionRate", inputs.websiteConversionRate, false) +
      field("Repeat Guest Percentage", "repeatGuestPercentage", inputs.repeatGuestPercentage, false) +
      "</div></div>" +
      '<div class="crs-form-group"><h3 class="crs-section-title">Commercial Context</h3><div class="crs-grid">' +
      selectField("Main Commercial Concern", "mainCommercialConcern", inputs.mainCommercialConcern, ["OTA dependency", "Weak direct bookings", "Low demand", "Poor conversion", "Repositioning", "Brand distribution", "Operator capability", "Unsure"], true) +
      selectField("Owner Goal", "ownerGoal", inputs.ownerGoal, ["Reduce OTA dependency", "Improve direct bookings", "Evaluate brand need", "Evaluate operator need", "Improve commercial story before outreach", "Other"], true) +
      field("Top Source Markets", "topSourceMarkets", inputs.topSourceMarkets, false) +
      field("Primary Guest Segments", "primaryGuestSegments", inputs.primaryGuestSegments, false) +
      textAreaField("Additional Owner Notes", "additionalOwnerNotes", inputs.additionalOwnerNotes) +
      "</div></div>"
    );
  }

  function render() {
    var snapshot = state.snapshot || {};
    var labels = state.labels || {};
    var basis = snapshot.snapshotBasis || {};
    var inputs = state.inputs || {};
    var primaryLabel = state.status === "Draft" ? "Continue Snapshot" : "Start Snapshot";
    if (state.status === "Snapshot ready") primaryLabel = "Regenerate Snapshot";

    var outputHtml = renderSnapshotOutput(snapshot, basis);
    root.innerHTML =
      '<div class="crs-card crs-header">' +
      "<h1>Commercial Readiness Snapshot</h1>" +
      '<p class="crs-header-subtitle">Assess how much control this hotel has over demand before choosing a brand, operator, or commercial path.</p>' +
      renderStatusRow(snapshot) +
      "</div>" +
      '<div class="crs-card crs-context-card">' +
      '<p class="crs-note">This snapshot is designed to support deal decisioning. It does not replace a full revenue management or digital marketing audit.</p>' +
      "</div>" +
      '<div class="crs-card crs-form-wrap"><h2 class="crs-section-title">Input Form</h2>' +
      renderInputSections(inputs) +
      '<div class="crs-actions">' +
      '<label class="crs-toggle"><input type="checkbox" data-key="enrichNarrative"' + (state.enrichNarrative ? " checked" : "") + '> Generate enriched narrative (optional)</label>' +
      '<button class="crs-btn crs-btn-secondary" data-action="save-inputs">Continue Snapshot</button>' +
      '<button class="crs-btn crs-btn-primary" data-action="generate">' + esc(primaryLabel === "Regenerate Snapshot" && state.status !== "Snapshot ready" ? "Start Snapshot" : primaryLabel) + "</button>" +
      (state.status === "Snapshot ready" ? '<button class="crs-btn crs-btn-secondary" data-action="regenerate">Regenerate Snapshot</button>' : "") +
      (standaloneMode
        ? '<button class="crs-btn crs-btn-secondary" data-action="clear-standalone">Clear Standalone Snapshot</button>'
        : '<button class="crs-btn crs-btn-secondary" data-action="back">Back to My Deals</button>') +
      "</div></div>" +
      (outputHtml || '<div class="crs-card crs-empty"><h2>No commercial snapshot generated yet.</h2><p class="crs-note">Add the hotel\'s commercial context and generate a snapshot to assess direct booking readiness, OTA dependency signals, and commercial capability needs.</p><div class="crs-actions"><button class="crs-btn crs-btn-primary" data-action="generate">Start Snapshot</button></div></div>');
  }

  function field(label, key, value, required) {
    return '<div class="crs-field"><label for="' + key + '">' + esc(label) + (required ? requiredMark() : "") + '</label><input id="' + key + '" data-key="' + key + '" value="' + esc(value || "") + '"></div>';
  }
  function textAreaField(label, key, value) {
    return '<div class="crs-field crs-field-full"><label for="' + key + '">' + esc(label) + '</label><textarea id="' + key + '" data-key="' + key + '" rows="3">' + esc(value || "") + "</textarea></div>";
  }
  function selectField(label, key, value, options, required) {
    var opts = ['<option value="">Select</option>']
      .concat((options || []).map(function (o) { return '<option value="' + esc(o) + '"' + (o === value ? " selected" : "") + ">" + esc(o) + "</option>"; }))
      .join("");
    return '<div class="crs-field"><label for="' + key + '">' + esc(label) + (required ? requiredMark() : "") + '</label><select id="' + key + '" data-key="' + key + '">' + opts + "</select></div>";
  }

  function section(title, body, className) {
    return '<div class="crs-card crs-output-section' + (className ? " " + className : "") + '"><h2>' + esc(title) + "</h2>" + body + "</div>";
  }

  function para(text) {
    return text ? '<p class="crs-prose">' + esc(text) + "</p>" : "";
  }
  function sectionNarrative(block, fallback) {
    if (block && block.enrichedNarrative) return block.enrichedNarrative;
    return fallback || "";
  }

  function list(items) {
    if (!items || !items.length) return "";
    return "<ul>" + items.map(function (x) { return "<li>" + esc(x) + "</li>"; }).join("") + "</ul>";
  }

  function labelBlock(label, value) {
    return '<p><strong>' + esc(label) + ":</strong> " + esc(value || "—") + "</p>";
  }

  function renderSnapshotOutput(snapshot, basis) {
    if (!snapshot || (!snapshot.executiveCommercialInterpretation && !snapshot.commercialReadiness)) return "";

    var exec = snapshot.executiveCommercialInterpretation || {};
    var readiness = snapshot.commercialReadinessLevel || snapshot.commercialReadiness || {};
    var ota = snapshot.otaDependencyRisk || {};
    var direct = snapshot.directBookingCapability || {};
    var owned = snapshot.ownedChannelVsOtaContentGap || snapshot.ownedChannelGap || {};
    var brand = snapshot.brandSystemContribution || snapshot.brandDistributionNeed || {};
    var operator = snapshot.operatorCommercialExecutionNeed || {};
    var economic = snapshot.economicSensitivity || {};
    var diagnosis = snapshot.strategicDiagnosis || {};
    var path = snapshot.recommendedPath || snapshot.recommendedStrategicPath || {};
    var actions = snapshot.suggestedNextActions || {};
    var missing = snapshot.dataNeededToConfirm || basis.missingData || basis.dataNeededToConfirm || [];
    var questions = snapshot.questionsToResolve || [];
    var enrichment = snapshot.enrichment || {};
    var urlEvidence = snapshot.urlEvidence || null;

    var html = "";
    html +=
      '<div class="crs-card crs-report-header"><h2 class="crs-section-title">Commercial Readiness Snapshot</h2><p class="crs-note">Generated from structured commercial inputs and available evidence.</p><div class="crs-chip-row">' +
      renderChips(state.labels || {}) +
      "</div></div>";
    html += section(
      "Snapshot Basis",
      para(sectionNarrative(basis, basis.note || "This snapshot uses owner-provided inputs and structured commercial logic.")) +
        labelBlock("Input Completeness", basis.inputCompleteness) +
        labelBlock("Performance Data Confidence", basis.performanceDataConfidence) +
        labelBlock("Observable Evidence Confidence", basis.observableEvidenceConfidence) +
        labelBlock("Confirmed performance data provided", basis.confirmedPerformanceDataProvided ? "Yes" : "No") +
        (missing.length ? "<h3>Data needed to confirm</h3>" + list(missing) : "")
    );
    if (urlEvidence && urlEvidence.sources) {
      var src = urlEvidence.sources;
      var sourceRows = [
        ["Hotel website", src.hotelWebsite],
        ["Booking.com", src.bookingCom],
        ["Expedia", src.expedia],
        ["Google Business Profile", src.googleBusinessProfile],
      ];
      var sourceHtml = sourceRows
        .map(function (row) {
          var info = row[1] || {};
          var reason = info.reason || info.notes || "";
          return (
            "<li><strong>" +
            esc(row[0]) +
            ":</strong> " +
            esc(info.status || "not_provided") +
            (reason ? " — " + esc(reason) : "") +
            "</li>"
          );
        })
        .join("");
      html += section("Evidence Sources", "<ul>" + sourceHtml + "</ul>");
    }

    html += section(
      "Executive Commercial Interpretation",
      para(sectionNarrative(exec, exec.summary || readiness.summary)) +
        labelBlock("Primary commercial question", exec.primaryCommercialQuestion) +
        labelBlock("Deal implication", exec.dealImplication)
    );

    html += section(
      "Commercial Readiness Level",
      labelBlock("Level", readiness.label || readiness.level) +
        labelBlock("Confidence", readiness.confidence) +
        para(sectionNarrative(readiness, readiness.rationale)) +
        (readiness.keyDrivers && readiness.keyDrivers.length ? "<h3>Key drivers</h3>" + list(readiness.keyDrivers) : "")
    );

    html += section(
      "OTA Dependency Risk",
      labelBlock("Assessment", ota.label || ota.assessment) +
        labelBlock("Confirmed by numbers", ota.confirmedByNumbers ? "Yes" : "No") +
        para(sectionNarrative(ota, ota.rationale || ota.interpretation)) +
        labelBlock("Deal implication", ota.dealImplication) +
        (ota.dataNeeded && ota.dataNeeded.length ? "<h3>Data needed</h3>" + list(ota.dataNeeded) : "")
    );

    html += section(
      "Direct Booking Capability",
      labelBlock("Assessment", direct.label || direct.assessment) +
        para(sectionNarrative(direct, direct.rationale)) +
        (direct.infrastructureSignals && direct.infrastructureSignals.length ? "<h3>Infrastructure signals</h3>" + list(direct.infrastructureSignals) : "") +
        (direct.conversionRisks && direct.conversionRisks.length ? "<h3>Conversion risks</h3>" + list(direct.conversionRisks) : "") +
        labelBlock("Deal implication", direct.dealImplication)
    );

    html += section(
      "Owned Channel vs OTA Content Gap",
      labelBlock("Assessment", owned.assessment) +
        labelBlock("Confidence", owned.confidence) +
        para(sectionNarrative(owned, owned.rationale)) +
        (owned.itemsToCompare && owned.itemsToCompare.length ? "<h3>Items to compare</h3>" + list(owned.itemsToCompare) : "") +
        (owned.ownedChannelStrengths && owned.ownedChannelStrengths.length ? "<h3>Owned channel strengths</h3>" + list(owned.ownedChannelStrengths) : "") +
        (owned.otaStrengths && owned.otaStrengths.length ? "<h3>OTA strengths</h3>" + list(owned.otaStrengths) : "") +
        (owned.contentGaps && owned.contentGaps.length ? "<h3>Content gaps</h3>" + list(owned.contentGaps) : "") +
        (owned.directBookingGaps && owned.directBookingGaps.length ? "<h3>Direct-booking gaps</h3>" + list(owned.directBookingGaps) : "") +
        (owned.guestReassuranceGaps && owned.guestReassuranceGaps.length ? "<h3>Guest reassurance gaps</h3>" + list(owned.guestReassuranceGaps) : "") +
        labelBlock("Deal implication", owned.dealImplication)
    );

    html += section(
      "Brand / System Contribution",
      labelBlock("Assessment", brand.assessment) +
        para(sectionNarrative(brand, brand.rationale || brand.note)) +
        (brand.questions && brand.questions.length ? "<h3>Questions</h3>" + list(brand.questions) : "") +
        labelBlock("Deal implication", brand.dealImplication)
    );

    html += section(
      "Operator / Commercial Execution Need",
      labelBlock("Assessment", operator.assessment) +
        para(sectionNarrative(operator, operator.rationale)) +
        (operator.capabilitiesToReview && operator.capabilitiesToReview.length ? "<h3>Capabilities to review</h3>" + list(operator.capabilitiesToReview) : "") +
        labelBlock("Deal implication", operator.dealImplication)
    );

    html += section(
      "Economic Sensitivity",
      labelBlock("Assessment", economic.assessment) +
        para(sectionNarrative(economic, economic.rationale)) +
        (economic.cannotCalculateBecause && economic.cannotCalculateBecause.length ? "<h3>Cannot calculate because</h3>" + list(economic.cannotCalculateBecause) : "") +
        (economic.dataNeeded && economic.dataNeeded.length ? "<h3>Data needed</h3>" + list(economic.dataNeeded) : "")
    );

    html += section(
      "Strategic Diagnosis",
      labelBlock("Primary diagnosis", diagnosis.primaryDiagnosis) +
        labelBlock("Secondary diagnosis", diagnosis.secondaryDiagnosis) +
      para(sectionNarrative(diagnosis, diagnosis.rationale)) +
        (diagnosis.notYetConfirmed && diagnosis.notYetConfirmed.length ? "<h3>Not yet confirmed</h3>" + list(diagnosis.notYetConfirmed) : "")
    );

    html += section(
      "Recommended Path",
      labelBlock("Headline", path.headline) +
        (path.recommendedSteps ? "<h3>Recommended steps</h3>" + list(path.recommendedSteps) : path.recommendedPath ? "<h3>Recommended steps</h3>" + list(path.recommendedPath) : "") +
        para(sectionNarrative(path, path.rationale || path.why)) +
        labelBlock("Before external outreach", "Use this path to align conversion priorities before brand or operator conversations."),
      "crs-recommended-path"
    );

    if (questions.length) {
      html += section(
        "Questions to Resolve Before Outreach",
        para(snapshot.questionsToResolveNarrative || "") + list(questions)
      );
    }

    if (actions.immediateActions || actions.dealalityNextSteps) {
      html += section(
        "Suggested Next Actions",
        (snapshot.suggestedNextActionsNarrative ? para(snapshot.suggestedNextActionsNarrative) : "") +
        (actions.immediateActions ? "<h3>Immediate Actions</h3>" + list(actions.immediateActions) : "") +
          (actions.dealalityNextSteps ? "<h3>Dealality Next Steps</h3>" + list(actions.dealalityNextSteps) : "")
      );
    }
    if (enrichment.enabled || enrichment.fallbackUsed) {
      html += section(
        "Narrative Generation Note",
        para(
          enrichment.fallbackUsed
            ? "Narrative enrichment was requested but unavailable. Deterministic narrative is shown. Scores and labels remain deterministic."
            : "Narrative enriched from structured Dealality analysis. Scores and labels are generated from deterministic rules."
        ) +
          labelBlock("Enrichment status", enrichment.fallbackUsed ? "Fallback used" : "Enabled") +
          labelBlock("Provider", enrichment.provider || "none")
      );
    }

    return '<div class="crs-output">' + html + "</div>";
  }

  function collectInputs() {
    var out = {};
    root.querySelectorAll("[data-key]").forEach(function (el) {
      if (el.type === "checkbox") out[el.getAttribute("data-key")] = !!el.checked;
      else out[el.getAttribute("data-key")] = el.value || "";
    });
    return out;
  }

  function applyGenerateResult(data) {
    var out = data.commercialReadiness || {};
    state.status = out.status || "Snapshot ready";
    state.snapshot = out.snapshot || null;
    state.labels = out.labels || {};
  }

  async function loadExisting() {
    if (standaloneMode) {
      loadStandaloneState();
      return;
    }
    var response = await fetchFn("/api/deals/" + encodeURIComponent(dealId) + "/commercial-readiness-snapshot", { method: "GET" });
    var data = await response.json();
    if (!response.ok || !data.ok) throw new Error((data && data.error) || "Could not load Commercial Readiness Snapshot.");
    state.status = data.commercialReadiness.status || "Not started";
    state.inputs = data.commercialReadiness.inputs || {};
    state.snapshot = data.commercialReadiness.snapshot || null;
    state.labels = {
      readinessLevel: data.commercialReadiness.level || "",
      confidence: data.commercialReadiness.evidenceConfidence || "",
      inputCompleteness: "",
      performanceDataConfidence: "",
      observableEvidenceConfidence: "",
      otaRisk: data.commercialReadiness.otaDependencyRisk || "",
      directCapability: data.commercialReadiness.directBookingCapability || "",
      strategicDiagnosis: (data.commercialReadiness.snapshot && data.commercialReadiness.snapshot.strategicDiagnosis && data.commercialReadiness.snapshot.strategicDiagnosis.primaryDiagnosis) || "",
    };
  }

  async function saveInputs() {
    state.inputs = collectInputs();
    state.enrichNarrative = !!state.inputs.enrichNarrative;
    delete state.inputs.enrichNarrative;
    if (standaloneMode) {
      state.status = "Draft";
      saveStandaloneState();
      return;
    }
    var response = await fetchFn("/api/ai/commercial-readiness-snapshot/save-inputs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ dealId: dealId, inputs: state.inputs }),
    });
    var data = await response.json();
    if (!response.ok || !data.ok) throw new Error((data && data.error) || "Could not save draft.");
    state.status = data.status || "Draft";
  }

  async function generateSnapshot() {
    state.inputs = collectInputs();
    state.enrichNarrative = !!state.inputs.enrichNarrative;
    delete state.inputs.enrichNarrative;
    var url = standaloneMode
      ? "/api/ai/commercial-readiness-snapshot/generate-standalone"
      : "/api/ai/commercial-readiness-snapshot/generate";
    var body = standaloneMode
      ? { inputs: state.inputs, enrichNarrative: state.enrichNarrative }
      : { dealId: dealId, inputs: state.inputs, enrichNarrative: state.enrichNarrative };

    var response = await fetchFn(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    var data = await response.json();
    if (!response.ok || !data.ok) {
      if (response.status === 400) state.status = "Needs inputs";
      if (standaloneMode) saveStandaloneState();
      throw new Error((data && data.error) || "Could not generate snapshot.");
    }
    applyGenerateResult(data);
    if (standaloneMode) saveStandaloneState();
  }

  root.addEventListener("click", function (event) {
    var button = event.target.closest("button[data-action]");
    if (!button) return;
    var action = button.getAttribute("data-action");
    if (action === "back") {
      window.location.href = "/my-deals.html";
      return;
    }
    if (action === "clear-standalone") {
      state = { status: "Not started", inputs: {}, snapshot: null, labels: {}, enrichNarrative: false };
      try { window.localStorage.removeItem(STANDALONE_STORAGE_KEY); } catch (_) {}
      render();
      return;
    }
    if (action === "save-inputs") {
      saveInputs().then(render).catch(function (err) { alert(err.message || "Save failed."); });
      return;
    }
    if (action === "generate" || action === "regenerate") {
      generateSnapshot().then(render).catch(function (err) { alert(err.message || "Generate failed."); });
    }
  });

  function boot() {
    loadExisting()
      .catch(function () {
        state = { status: "Not started", inputs: {}, snapshot: null, labels: {}, enrichNarrative: false };
      })
      .finally(render);
  }

  if (!standaloneMode && window.DealalityMemberstackAuth && typeof window.DealalityMemberstackAuth.whenReady === "function") {
    window.DealalityMemberstackAuth.whenReady(boot);
  } else {
    boot();
  }
})();
