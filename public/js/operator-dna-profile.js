/**
 * Operator DNA Profile — consolidated with Operator Explorer (gold-mock).
 * Uses the same panel builders as operator-explorer-gold-mock.html so ALL Setup data
 * (leadership photos, footprint by region/brand/scale, case study images, etc.) is shown.
 * Does not modify operator-explorer.html or the list → popup flow.
 */
(function () {
  "use strict";

  var Gold = function () {
    return window.OperatorExplorerGoldMock;
  };
  var Consolidate = function () {
    return window.OperatorDnaProfileConsolidate;
  };
  var Mount = function () {
    return window.OperatorDnaProfileMount;
  };

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function showLoadError(msg) {
    document.documentElement.classList.remove("gold-profile--loading");
    var el = document.getElementById("odnaLoadError");
    if (el) {
      el.hidden = false;
      el.innerHTML = "<strong>Unable to load profile</strong><p>" + msg + "</p>";
    }
    var nameEl = document.getElementById("heroName");
    if (nameEl) nameEl.textContent = "Operator DNA Profile";
  }

  function parseOperatorId() {
    var params = new URLSearchParams(window.location.search);
    return (
      params.get("operatorId") ||
      params.get("recordId") ||
      params.get("id") ||
      ""
    );
  }

  function parseDealId() {
    return nz(new URLSearchParams(window.location.search).get("dealId"));
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
      var err = await detailRes.json().catch(function () { return {}; });
      throw new Error((err && err.error) || "Failed to load operator detail");
    }
    var detailData = await detailRes.json().catch(function () { return {}; });
    if (!detailData || !detailData.success || !detailData.operator) {
      throw new Error("Invalid operator detail response");
    }
    return { detail: detailData.operator, listRow: listRow, raw: detailData };
  }

  async function fetchAlignment(dealId, operatorId) {
    if (!dealId || !operatorId) return null;
    try {
      var res = await fetch(
        "/api/operator-alignment-snapshot/" + encodeURIComponent(dealId) + "/companies"
      );
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok || !data.success) return null;
      var companies = data.companiesForConsideration || data.companies || [];
      var oid = String(operatorId).toLowerCase();
      var match =
        companies.find(function (c) {
          return [c.operatorId, c.operatorRecordId, c.id, c.recordId]
            .filter(Boolean)
            .some(function (id) {
              return String(id).toLowerCase() === oid;
            });
        }) || null;
      if (!match) return null;
      return {
        band: nz(match.alignmentBand),
        score: match.alignmentScoreOptional != null ? String(match.alignmentScoreOptional) : "",
        signals: (match.alignmentSignals || []).slice(0, 6),
        validation: (match.whatNeedsValidation || match.reviewConsiderations || []).slice(0, 6),
        keyConsideration: nz(match.keyConsideration),
      };
    } catch (e) {
      console.warn("[operator-dna-profile] alignment fetch failed", e);
      return null;
    }
  }

  function applyHeroStripe(listRow, detail) {
    var g = Gold();
    if (!g) return;
    var scalesStr = (listRow && listRow.chainScale) || "";
    var scales = String(scalesStr)
      .split(",")
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    if (scales.length) {
      g.applyHeroStripeFromChainScales(scales);
      return;
    }
    var p = (detail && detail.prefill) || {};
    var scalesFromPrefill = []
      .concat(p.chainScalesSupported || [])
      .filter(Boolean);
    if (scalesFromPrefill.length) g.applyHeroStripeFromChainScales(scalesFromPrefill);
  }

  async function mountAlignmentContext(dealId, operatorId, profileId) {
    var mountEl = document.getElementById("alignmentContext");
    if (!mountEl || !dealId) return;

    if (window.OperatorExplorerNewBaseProfile && window.OperatorExplorerNewBaseProfile.mountAlignmentContext) {
      mountEl.hidden = false;
      await window.OperatorExplorerNewBaseProfile.mountAlignmentContext(
        dealId,
        operatorId,
        profileId || operatorId
      );
      return;
    }

    var alignment = await fetchAlignment(dealId, operatorId);
    if (!alignment) return;

    if (window.OperatorExplorerNewBaseProfile && window.OperatorExplorerNewBaseProfile.buildAlignmentContextHtml) {
      var html = window.OperatorExplorerNewBaseProfile.buildAlignmentContextHtml(
        {
          alignmentBand: alignment.band,
          alignmentScoreOptional: alignment.score,
          alignmentSignals: alignment.signals,
          whatNeedsValidation: alignment.validation,
          keyConsideration: alignment.keyConsideration,
        },
        dealId
      );
      mountEl.hidden = false;
      mountEl.innerHTML = html;
      var m = Mount();
      if (m) m.appendAlignmentTab('<div class="odna-alignment-panel-wrap">' + html + "</div>");
    }
  }

  async function init() {
    var g = Gold();
    var c = Consolidate();
    if (!g || !c) {
      showLoadError("Profile scripts failed to load. Refresh the page.");
      return;
    }

    var operatorId = parseOperatorId();
    var dealId = parseDealId();

    if (!operatorId || operatorId.indexOf("rec") !== 0) {
      showLoadError(
        "This consolidated profile requires an operator record id. Open Operator Explorer, click an operator, and copy the <code>id=rec…</code> from the popup URL into <code>?operatorId=rec…</code> on this page."
      );
      return;
    }

    try {
      var bundle = await fetchOperatorBundle(operatorId);
      applyHeroStripe(bundle.listRow, bundle.detail);

      var vm = g.buildViewModel(bundle.detail, bundle.listRow);
      var panels = g.buildPanels(vm, {
        omitProofDecisionSignals: true,
        useBrandExplorerCaseStudies: true,
        ownerFacingProofKpis: true,
        omitProofTrackRecordKpiSectionTitle: true,
      });
      panels = c.enhancePanels(panels, vm);
      g.mount(vm, panels);

      if (window.OperatorDnaCaseStudiesBe) {
        if (window.OperatorDnaCaseStudiesBe.attachCaseStudyPayloadsToDom) {
          window.OperatorDnaCaseStudiesBe.attachCaseStudyPayloadsToDom();
        }
        if (window.OperatorDnaCaseStudiesBe.wireCaseStudyModal) {
          window.OperatorDnaCaseStudiesBe.wireCaseStudyModal();
        }
      }

      var profileId =
        bundle.detail.operatorId ||
        (bundle.detail.prefill && bundle.detail.prefill.operatorId) ||
        operatorId;

      if (dealId) {
        await mountAlignmentContext(dealId, operatorId, profileId);
      }

      var MountApi = Mount();
      if (MountApi && MountApi.mountDnaExtensionTabs) {
        MountApi.mountDnaExtensionTabs(vm);
      }

      if (window.OperatorExplorerFavorites && window.OperatorExplorerFavorites.wireSaveButtons) {
        window.OperatorExplorerFavorites.wireSaveButtons(document);
      }

      if (window.parent && window.parent !== window) {
        try {
          window.parent.postMessage(
            {
              type: "operator-gold-mock-ready",
              operatorId: operatorId,
              live: true,
              source: "operator-dna-profile",
            },
            window.location.origin
          );
        } catch (e) {
          /* ignore */
        }
      }
    } catch (e) {
      console.error("[operator-dna-profile] load failed", e);
      showLoadError(
        (e && e.message) ||
          "Could not load operator. Verify the record id or try again from Operator Explorer."
      );
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
