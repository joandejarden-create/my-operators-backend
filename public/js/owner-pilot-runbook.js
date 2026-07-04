/**
 * Fetch admin-protected runbook content and render collapsible sections.
 */
(function (global) {
  "use strict";

  var RUNBOOK_API = "/api/support/owner-pilot-provisioning-runbook";

  function el(tag, className, html) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (html != null) node.innerHTML = html;
    return node;
  }

  function renderTable(block) {
    var wrap = el("div", "runbook-table-wrap");
    var table = el("table", "runbook-table");
    if (Array.isArray(block.headers) && block.headers.length) {
      var thead = document.createElement("thead");
      var hr = document.createElement("tr");
      block.headers.forEach(function (h) {
        hr.appendChild(el("th", null, h));
      });
      thead.appendChild(hr);
      table.appendChild(thead);
    }
    var tbody = document.createElement("tbody");
    (block.rows || []).forEach(function (row) {
      var tr = document.createElement("tr");
      (row || []).forEach(function (cell) {
        tr.appendChild(el("td", null, cell));
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    wrap.appendChild(table);
    return wrap;
  }

  function renderBlock(block) {
    if (!block || !block.type) return document.createDocumentFragment();
    switch (block.type) {
      case "paragraph":
        return el("p", null, block.html || "");
      case "heading":
        return el(block.level === 4 ? "h4" : "h3", null, block.text || "");
      case "table":
        return renderTable(block);
      case "orderedList":
        return el(
          "ol",
          block.className || "runbook-checklist",
          (block.items || []).map(function (item) {
            return "<li>" + item + "</li>";
          }).join("")
        );
      case "unorderedList":
        return el(
          "ul",
          block.className || null,
          (block.items || []).map(function (item) {
            return "<li>" + item + "</li>";
          }).join("")
        );
      case "code":
        return el("pre", null, "<code>" + (block.text || "") + "</code>");
      case "alert":
        return el("div", "support-alert", block.html || "");
      case "priority":
        return renderPriority(block);
      default:
        return document.createDocumentFragment();
    }
  }

  function renderPriority(block) {
    var variant = block.variant === "critical" ? " runbook-priority--critical"
      : block.variant === "soon" ? " runbook-priority--soon" : "";
    var box = el("div", "runbook-priority" + variant);
    if (block.title) box.appendChild(el("h4", null, block.title));
    var ul = el("ul", null, (block.items || []).map(function (item) {
      return "<li>" + item + "</li>";
    }).join(""));
    box.appendChild(ul);
    return box;
  }

  function renderSection(section, index) {
    var details = el("details", "runbook-section");
    if (section.defaultOpen || index === 0) details.open = true;
    details.id = section.id || "runbook-section-" + index;
    var summary = document.createElement("summary");
    summary.textContent = section.title || "Section";
    details.appendChild(summary);
    var body = el("div", "runbook-section-body");
    (section.contentBlocks || []).forEach(function (block) {
      body.appendChild(renderBlock(block));
    });
    details.appendChild(body);
    return details;
  }

  function bindToolbar(root) {
    var expandBtn = document.getElementById("runbookExpandAll");
    var collapseBtn = document.getElementById("runbookCollapseAll");
    function allSections() {
      return root ? Array.prototype.slice.call(root.querySelectorAll(".runbook-section")) : [];
    }
    if (expandBtn) {
      expandBtn.addEventListener("click", function () {
        allSections().forEach(function (node) { node.open = true; });
      });
    }
    if (collapseBtn) {
      collapseBtn.addEventListener("click", function () {
        allSections().forEach(function (node) { node.open = false; });
      });
    }
  }

  function renderRunbook(payload, rootId) {
    var root = document.getElementById(rootId || "runbookRoot");
    if (!root || !payload) return;
    root.innerHTML = "";

    var header = el("header", "support-page-header");
    var badgeRow = el("div", "support-badge-row");
    (payload.badges || []).forEach(function (badge) {
      var cls = "support-badge" + (badge.variant === "internal" ? " support-badge--internal" : "");
      badgeRow.appendChild(el("span", cls, badge.label));
    });
    header.appendChild(badgeRow);
    header.appendChild(el("h1", null, payload.title || "Owner Pilot Provisioning Runbook"));
    if (payload.subtitle) header.appendChild(el("p", null, payload.subtitle));
    root.appendChild(header);

    if (payload.warning) {
      var alert = el("div", "support-alert");
      alert.setAttribute("role", "note");
      alert.innerHTML = payload.warning;
      root.appendChild(alert);
    }

    var toolbar = el("div", "runbook-toolbar");
    toolbar.setAttribute("aria-label", "Runbook controls");
    toolbar.innerHTML =
      '<button type="button" id="runbookExpandAll">Expand all</button>' +
      '<button type="button" id="runbookCollapseAll">Collapse all</button>';
    root.appendChild(toolbar);

    var sectionsWrap = el("div", "runbook-sections");
    (payload.sections || []).forEach(function (section, index) {
      sectionsWrap.appendChild(renderSection(section, index));
    });
    root.appendChild(sectionsWrap);

    bindToolbar(root);
  }

  async function fetchRunbook(authFetch, apiUrl) {
    var res = await authFetch(apiUrl || RUNBOOK_API, { maxWaitMs: 20000 });
    return res;
  }

  async function loadAndRender(options) {
    options = options || {};
    var auth = global.DealalityMemberstackAuth;
    if (!auth || typeof auth.authFetch !== "function") {
      return { ok: false, status: 0, reason: "auth_unavailable" };
    }
    try {
      var res = await fetchRunbook(auth.authFetch, options.apiUrl);
      if (res.status === 401 || res.status === 403) {
        return { ok: false, status: res.status, reason: "forbidden" };
      }
      if (!res.ok) {
        return { ok: false, status: res.status, reason: "http_" + res.status };
      }
      var data = await res.json();
      if (data && data.error === "API route not found") {
        return { ok: false, status: 404, reason: "route_not_found" };
      }
      renderRunbook(data, options.rootId || "runbookRoot");
      return { ok: true, data: data };
    } catch (err) {
      return {
        ok: false,
        status: 0,
        reason: err && err.message ? err.message : "network_error",
      };
    }
  }

  global.OwnerPilotRunbook = {
    RUNBOOK_API: RUNBOOK_API,
    renderRunbook: renderRunbook,
    loadAndRender: loadAndRender,
  };
})(window);
