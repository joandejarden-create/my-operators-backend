(function () {
  "use strict";

  var selectedFactId = null;
  var facts = [];
  var extractionContext = null;

  function $(id) {
    return document.getElementById(id);
  }

  function setPageBanner(msg, kind) {
    var el = $("piPageBanner");
    if (!el) return;
    if (!msg) {
      el.hidden = true;
      el.textContent = "";
      el.className = "pi-banner";
      return;
    }
    el.hidden = false;
    el.textContent = msg;
    el.className = "pi-banner" + (kind ? " " + kind : "");
  }

  function setStatus(msg, kind) {
    var el = $("piStatus");
    if (!el) return;
    el.textContent = msg || "";
    el.className = "pi-status" + (kind ? " " + kind : "");
  }

  async function apiFetch(path, opts) {
    opts = opts || {};
    var headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
    if (window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.getAuthHeaders) {
      var auth = await window.DealalityMemberstackAuth.getAuthHeaders(null, { waitForLogin: false });
      if (auth.error) throw new Error(auth.error);
      Object.assign(headers, auth.headers);
    }
    var res = await fetch(path, {
      method: opts.method || "GET",
      headers: headers,
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    });
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok) {
      var msg = (data && (data.message || data.error)) || res.statusText;
      if (res.status === 404 && msg === "API route not found") {
        msg = "API route not found — restart the backend (npm start).";
      }
      throw new Error(msg);
    }
    return data;
  }

  function renderSourceSummary() {
    var el = $("piSourceSummary");
    if (!el || !extractionContext) return;
    var urlCount = (extractionContext.sources || []).filter(function (s) {
      return s.sourceUrl && !s.localFilePath;
    }).length;
    var localCount = (extractionContext.folderFiles || []).length;
    var registeredLocal = (extractionContext.sources || []).filter(function (s) {
      return s.localFilePath;
    }).length;
    el.textContent =
      "Sources: " +
      urlCount +
      " public URL(s) in Airtable · " +
      localCount +
      " file(s) in reference folder (" +
      (extractionContext.referenceFolder || "not mapped") +
      ")" +
      (registeredLocal ? " · " + registeredLocal + " registered locally" : "") +
      ". Extraction merges all sources into one Pending row per field (" +
      (window.location.hostname === "localhost" ? "set OPENAI_API_KEY + PARTNER_INTELLIGENCE_LLM_EXTRACTION_ENABLED=1 for LLM" : "LLM when enabled") +
      ").";
  }

  function renderFactList() {
    var list = $("piFactList");
    var empty = $("piFactEmpty");
    if (!list) return;
    list.innerHTML = "";
    if (!facts.length) {
      if (empty) empty.hidden = false;
      return;
    }
    if (empty) empty.hidden = true;
    facts.forEach(function (f) {
      var li = document.createElement("li");
      li.className = f.id === selectedFactId ? "active" : "";
      li.innerHTML =
        "<strong>" +
        escapeHtml(f.fieldName) +
        "</strong><div class=\"meta\">" +
        escapeHtml(f.humanReviewStatus || "Pending") +
        " · " +
        escapeHtml(f.confidenceLevel || "") +
        (f.dataGap === "Yes" ? " · gap" : "") +
        (f.extractionRunId ? " · " + escapeHtml(f.extractionRunId) : "") +
        "</div>";
      li.addEventListener("click", function () {
        selectFact(f.id);
      });
      list.appendChild(li);
    });
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function isBrandProfile() {
    return $("piProfileType") && $("piProfileType").value === "brand";
  }

  function activeProfileId() {
    return isBrandProfile() ? $("piBrandSelect").value : $("piOperatorSelect").value;
  }

  function syncProfileControls() {
    var brand = isBrandProfile();
    $("piOperatorLabel").hidden = brand;
    $("piBrandLabel").hidden = !brand;
  }

  async function loadExtractionContext() {
    var id = activeProfileId();
    var q = isBrandProfile()
      ? "?brandId=" + encodeURIComponent(id)
      : "?operatorId=" + encodeURIComponent(id);
    extractionContext = await apiFetch("/api/partner-intelligence/extraction/context" + q);
    renderSourceSummary();
  }

  async function selectFact(id) {
    selectedFactId = id;
    renderFactList();
    setStatus("");
    var data = await apiFetch("/api/partner-intelligence/facts/" + encodeURIComponent(id));
    var f = data.fact;
    $("piDetailEmpty").hidden = true;
    $("piDetail").hidden = false;
    $("piFieldName").value = f.fieldName || "";
    $("piSection").value = f.explorerSection || "";
    $("piExtracted").value = f.extractedValue || "";
    $("piEvidence").value = f.evidenceText || "";
    $("piApproved").value = f.approvedValue || f.extractedValue || "";
    $("piNotes").value = f.reviewerNotes || "";
    $("piVisibility").value = f.publicVisibility || "Public";
    if (data.source && data.source.sourceTitle) {
      setStatus("Source: " + data.source.sourceTitle, "ok");
    }
  }

  async function loadFacts() {
    setPageBanner("Loading facts…", "info");
    var id = activeProfileId();
    var status = $("piStatusFilter").value;
    var q = isBrandProfile()
      ? "/api/partner-intelligence/facts?brandId=" + encodeURIComponent(id)
      : "/api/partner-intelligence/facts?operatorId=" + encodeURIComponent(id);
    if (status) q += "&humanReviewStatus=" + encodeURIComponent(status);
    var data = await apiFetch(q);
    facts = data.facts || [];
    if (selectedFactId && !facts.some(function (f) { return f.id === selectedFactId; })) {
      selectedFactId = null;
      $("piDetail").hidden = true;
      $("piDetailEmpty").hidden = false;
    }
    renderFactList();
    setPageBanner(
      facts.length
        ? facts.length + " fact(s) loaded."
        : "No facts for this filter. Run extraction or change the status filter.",
      facts.length ? "ok" : "info"
    );
  }

  async function patchReview(payload) {
    if (!selectedFactId) return;
    var data = await apiFetch(
      "/api/partner-intelligence/facts/" + encodeURIComponent(selectedFactId) + "/review",
      { method: "PATCH", body: payload }
    );
    setStatus("Saved: " + (data.fact && data.fact.humanReviewStatus), "ok");
    await loadFacts();
    if (selectedFactId) await selectFact(selectedFactId);
  }

  async function publishSelected() {
    if (!selectedFactId) return;
    var data = await apiFetch("/api/partner-intelligence/publish", {
      method: "POST",
      body: { factId: selectedFactId },
    });
    setStatus("Published field: " + (data.published && data.published.fieldName), "ok");
  }

  async function runExtraction() {
    setPageBanner("Syncing reference folder and running all sources…", "info");
    var id = activeProfileId();
    var body = { allSources: true, syncFolder: true, force: true, mode: isBrandProfile() ? "brand" : "operator" };
    if (isBrandProfile()) body.brandId = id;
    else body.operatorId = id;
    var data = await apiFetch("/api/partner-intelligence/extraction/run", {
      method: "POST",
      body: body,
    });
    var synced = (data.folderSync && data.folderSync.synced && data.folderSync.synced.length) || 0;
    setPageBanner(
      "Run " +
        data.runId +
        ": " +
        data.mergedFieldCount +
        " merged field(s), " +
        data.factsWithValues +
        " with values (" +
        (data.extractor || "rules") +
        (data.usedLlm ? " + evidence validation" : "") +
        ") from " +
        data.sourcesConsidered +
        " source(s).",
      "ok"
    );
    $("piStatusFilter").value = "Pending";
    await loadExtractionContext();
    await loadFacts();
  }

  function wire() {
    $("piRefreshBtn").addEventListener("click", function () {
      Promise.all([loadExtractionContext(), loadFacts()]).catch(function (e) {
        setPageBanner(e.message, "error");
      });
    });
    $("piStatusFilter").addEventListener("change", loadFacts);
    if ($("piProfileType")) {
      $("piProfileType").addEventListener("change", function () {
        syncProfileControls();
        selectedFactId = null;
        $("piDetail").hidden = true;
        $("piDetailEmpty").hidden = false;
        Promise.all([loadExtractionContext(), loadFacts()]).catch(function (e) {
          setPageBanner(e.message, "error");
        });
      });
    }
    $("piApproveBtn").addEventListener("click", function () {
      patchReview({
        humanReviewStatus: "Approved",
        approvedValue: $("piApproved").value,
        reviewerNotes: $("piNotes").value,
        publicVisibility: $("piVisibility").value,
      }).catch(function (e) {
        setPageBanner(e.message, "error");
      });
    });
    $("piEditBtn").addEventListener("click", function () {
      patchReview({
        humanReviewStatus: "Edited",
        approvedValue: $("piApproved").value,
        reviewerNotes: $("piNotes").value,
        publicVisibility: $("piVisibility").value,
      }).catch(function (e) {
        setPageBanner(e.message, "error");
      });
    });
    $("piRejectBtn").addEventListener("click", function () {
      patchReview({
        humanReviewStatus: "Rejected",
        reviewerNotes: $("piNotes").value,
      }).catch(function (e) {
        setPageBanner(e.message, "error");
      });
    });
    $("piPublishBtn").addEventListener("click", function () {
      publishSelected().catch(function (e) {
        setPageBanner(e.message, "error");
      });
    });
    $("piExtractBtn").addEventListener("click", function () {
      runExtraction().catch(function (e) {
        setPageBanner(e.message, "error");
      });
    });
  }

  function boot() {
    syncProfileControls();
    wire();
    Promise.all([loadExtractionContext(), loadFacts()]).catch(function (e) {
      setPageBanner(e.message, "error");
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (window.DealalityMemberstackAuth && typeof window.DealalityMemberstackAuth.whenReady === "function") {
      window.DealalityMemberstackAuth.whenReady(boot);
      return;
    }
    boot();
  });
})();
