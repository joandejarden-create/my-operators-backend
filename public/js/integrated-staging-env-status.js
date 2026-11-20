/**
 * Dealality Integrated Staging — Environment Status panel (internal).
 * Shows merged vs staging-only vs not-integrated. Never claims production-ready
 * for unmerged features.
 */
(function () {
  "use strict";

  var PANEL_ID = "dealality-integrated-staging-status";
  var ENDPOINT = "/api/admin/integrated-staging/status";

  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) {
        if (k === "className") node.className = attrs[k];
        else if (k === "text") node.textContent = attrs[k];
        else node.setAttribute(k, attrs[k]);
      });
    }
    (children || []).forEach(function (c) {
      if (c) node.appendChild(c);
    });
    return node;
  }

  function short(sha) {
    if (!sha) return "—";
    return String(sha).slice(0, 10);
  }

  function listBlock(title, items, tone) {
    var wrap = el("div", { className: "isis-block isis-" + tone });
    wrap.appendChild(el("div", { className: "isis-block-title", text: title }));
    if (!items || !items.length) {
      wrap.appendChild(el("div", { className: "isis-empty", text: "None" }));
      return wrap;
    }
    var ul = el("ul", { className: "isis-list" });
    items.forEach(function (item) {
      var label =
        typeof item === "string"
          ? item
          : item.label || item.id || JSON.stringify(item);
      var li = el("li", { text: label });
      if (item && item.productionReady === false) {
        li.appendChild(
          el("span", {
            className: "isis-badge",
            text: " STAGING ONLY — NOT PRODUCTION",
          }),
        );
      }
      ul.appendChild(li);
    });
    wrap.appendChild(ul);
    return wrap;
  }

  function render(data) {
    var existing = document.getElementById(PANEL_ID);
    if (existing) existing.remove();

    var panel = el("aside", {
      id: PANEL_ID,
      className: "isis-panel",
      "aria-label": "Integrated Staging Environment Status",
    });

    panel.appendChild(
      el("div", {
        className: "isis-env",
        text: "Environment: " + (data.environment || "INTEGRATED_STAGING"),
      }),
    );
    panel.appendChild(
      el("div", {
        className: "isis-row",
        text: "Base main: " + short(data.baseMainCommit || data.baseMainShort),
      }),
    );
    panel.appendChild(
      el("div", {
        className: "isis-row",
        text: "HEAD: " + short(data.headCommit || data.headShort),
      }),
    );

    var extras = data.additionalStagedFeatureCommits || [];
    panel.appendChild(
      el("div", {
        className: "isis-row",
        text:
          "Staged feature commits: " +
          (extras.length
            ? extras
                .map(function (c) {
                  return short(c.sha) + (c.label ? " (" + c.label + ")" : "");
                })
                .join(", ")
            : "none"),
      }),
    );
    panel.appendChild(
      el("div", {
        className: "isis-row",
        text: "Last refresh: " + (data.lastStagingRefresh || "—"),
      }),
    );
    panel.appendChild(
      el("div", {
        className: "isis-row",
        text: "Server: " + (data.serverStatus || "UNKNOWN"),
      }),
    );
    panel.appendChild(
      el("div", {
        className: "isis-row",
        text: "Validation: " + (data.validationStatus || "NOT_RUN"),
      }),
    );

    var features = data.features || {};
    panel.appendChild(
      listBlock("Merged into main", features.mergedIntoMain, "merged"),
    );
    panel.appendChild(
      listBlock(
        "Included only in local staging",
        features.includedOnlyInLocalStaging,
        "staging",
      ),
    );
    panel.appendChild(
      listBlock("Not yet integrated", features.notYetIntegrated, "pending"),
    );

    if (data.disclaimer) {
      panel.appendChild(
        el("div", { className: "isis-disclaimer", text: data.disclaimer }),
      );
    }

    document.body.appendChild(panel);
  }

  function shouldShow() {
    try {
      var q = new URLSearchParams(window.location.search || "");
      if (q.get("stagingStatus") === "1") return true;
      if (window.localStorage.getItem("dealality_show_staging_status") === "1")
        return true;
      // Auto-show on integrated staging host when API confirms staging checkout
      return true;
    } catch (_) {
      return false;
    }
  }

  function boot() {
    if (!shouldShow()) return;
    fetch(ENDPOINT, { credentials: "same-origin" })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data || data.ok === false) return;
        // Only inject when serving staging or explicitly requested
        var q = new URLSearchParams(window.location.search || "");
        var force = q.get("stagingStatus") === "1";
        if (!force && data.isServingFromStagingCheckout === false) return;
        render(data);
      })
      .catch(function () {
        /* non-fatal */
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
