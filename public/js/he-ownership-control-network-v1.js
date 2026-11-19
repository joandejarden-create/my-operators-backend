/**
 * Interactive Ownership & Control Network (browser).
 * Hierarchical SVG layout — no force-directed drift.
 * Typed edges only; operator/brand never drawn as ownership.
 */
(function (global) {
  "use strict";

  var VERSION = "ownership-control-network-v1";
  var MAX_EXPANSION_HOTELS = 12;

  var EDGE_LABELS = {
    LEGALLY_OWNED_BY: "Legally owned by",
    ECONOMICALLY_CONTROLLED_BY: "Economically controlled by",
    MANAGED_BY: "Managed by",
    BRANDED_AS: "Branded as",
    ASSOCIATED_PERSON: "Key person",
    PORTFOLIO_HOTEL: "Associated hotel",
  };

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function text(v) {
    var s = String(v == null ? "" : v).trim();
    return s || null;
  }

  function partyKnown(party) {
    if (!party || typeof party !== "object") return false;
    if (party.known === false) return false;
    return Boolean(text(party.name) || text(party.display_name) || text(party.legal_name));
  }

  function partyName(party) {
    if (!party || typeof party !== "object") return null;
    return (
      text(party.name) ||
      text(party.display_name) ||
      text(party.legal_name) ||
      text(party.trading_name) ||
      null
    );
  }

  function verificationOf(party, fallback) {
    var raw = String(
      (party && (party.preview_state || party.verification_bucket || party.status || party.confidence)) ||
        fallback ||
        ""
    ).toUpperCase();
    if (/PROPOSED|STAGED|MEDIUM|PROBABLE/.test(raw)) return "proposed";
    if (/HISTOR|FORMER|PAST/.test(raw)) return "historical";
    if (/UNRESOLVED|UNKNOWN|GAP|NOT.?YET/.test(raw)) return "unresolved";
    if (/VERIFIED|HIGH|VALIDATED|LIVE|CURRENT/.test(raw)) return "verified";
    if (!partyKnown(party)) return "unresolved";
    return "proposed";
  }

  function friendlyVerification(v) {
    if (v === "verified") return "Verified";
    if (v === "proposed") return "Proposed";
    if (v === "historical") return "Historical";
    return "Unresolved";
  }

  function buildOwnershipControlNetwork(hotel, ownership, opts) {
    opts = opts || {};
    var includePeople = opts.includePeople !== false;
    var includePortfolioPreview = opts.includePortfolioPreview === true;
    var maxPortfolio = Math.min(Number(opts.maxPortfolio) || MAX_EXPANSION_HOTELS, MAX_EXPANSION_HOTELS);
    var ctrl = (ownership && ownership.report && ownership.report.ownership_and_control) || {};
    var propco = ctrl.legal_property_owner_propco || ctrl.property_company || {};
    var econ = ctrl.economic_owner_or_group || ctrl.economic_owner || {};
    var ubo = ctrl.ubo_group || {};
    var operator = ctrl.operator || {};
    var brandParty = ctrl.brand || {};

    var hotelId = text(hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || "focus";
    var hotelName = text(hotel && hotel.name) || "Hotel";
    // Prefer brand affiliation / hotel brand over parent org name on the brand node.
    var brandName =
      text(ownership && ownership.hotel && ownership.hotel.brand_display) ||
      text(hotel && hotel.brand) ||
      text(brandParty.name) ||
      null;
    var operatorName = text(operator.name) || text(hotel && hotel.managementCompany) || null;

    var nodes = [];
    var edges = [];
    var byId = {};

    function addNode(node) {
      if (!node || !node.id) return null;
      if (byId[node.id]) return byId[node.id];
      byId[node.id] = node;
      nodes.push(node);
      return node;
    }

    function addEdge(edge) {
      if (!edge || !edge.from || !edge.to || !edge.type) return;
      var id = edge.id || edge.from + "|" + edge.type + "|" + edge.to;
      for (var i = 0; i < edges.length; i++) if (edges[i].id === id) return;
      edges.push({
        id: id,
        from: edge.from,
        to: edge.to,
        type: edge.type,
        label: edge.label || EDGE_LABELS[edge.type] || edge.type,
        verification: edge.verification || "unresolved",
        temporal: edge.temporal || "CURRENT",
      });
    }

    var hotelNodeId = "hotel:" + hotelId;
    addNode({
      id: hotelNodeId,
      kind: "hotel",
      label: hotelName,
      role: "Hotel / Property",
      verification: "verified",
      hpcId: /^rec[a-zA-Z0-9]{14}$/.test(hotelId) ? hotelId : null,
      focus: true,
    });

    var propcoId = null;
    var econId = null;

    if (partyKnown(propco)) {
      propcoId = "org:propco:" + (text(propco.entity_id) || partyName(propco));
      addNode({
        id: propcoId,
        kind: "org",
        label: partyName(propco),
        role: "Legal property owner",
        verification: verificationOf(propco),
        expandKey: text(propco.entity_id) || null,
      });
      addEdge({
        from: hotelNodeId,
        to: propcoId,
        type: "LEGALLY_OWNED_BY",
        verification: verificationOf(propco),
      });
    }

    if (partyKnown(econ)) {
      econId = "org:econ:" + (text(econ.entity_id) || partyName(econ));
      addNode({
        id: econId,
        kind: "org",
        label: partyName(econ),
        role: "Economic owner / sponsor",
        verification: verificationOf(econ),
        expandKey: text(econ.entity_id) || null,
      });
      if (propcoId) {
        addEdge({
          from: propcoId,
          to: econId,
          type: "ECONOMICALLY_CONTROLLED_BY",
          verification: verificationOf(econ),
        });
      } else {
        addEdge({
          from: hotelNodeId,
          to: econId,
          type: "ECONOMICALLY_CONTROLLED_BY",
          verification: verificationOf(econ),
        });
      }
    }

    if (operatorName) {
      var opId = "org:operator:" + (text(operator.entity_id) || operatorName);
      addNode({
        id: opId,
        kind: "org",
        label: operatorName,
        role: "Hotel operator",
        verification: verificationOf(operator, "HIGH"),
        expandKey: text(operator.entity_id) || null,
      });
      addEdge({
        from: hotelNodeId,
        to: opId,
        type: "MANAGED_BY",
        verification: verificationOf(operator, "HIGH"),
      });
    }

    if (brandName) {
      var brandId = "org:brand:" + brandName;
      addNode({
        id: brandId,
        kind: "org",
        label: brandName,
        role: "Brand",
        verification: verificationOf(brandParty, "HIGH"),
      });
      addEdge({
        from: hotelNodeId,
        to: brandId,
        type: "BRANDED_AS",
        verification: verificationOf(brandParty, "HIGH"),
      });
    }

    if (includePeople) {
      var people =
        (ownership &&
          ownership.report &&
          ownership.report.decision_authority &&
          ownership.report.decision_authority.people) ||
        [];
      people.slice(0, 6).forEach(function (p, idx) {
        var name = text(p.display_name || p.name);
        if (!name) return;
        var personId = "person:" + (text(p.personId || p.person_id || p.entity_id) || idx) + ":" + name;
        addNode({
          id: personId,
          kind: "person",
          label: name,
          role: text(p.title || p.role) || "Decision-maker",
          verification: "proposed",
          personKey: text(p.personId || p.person_id) || null,
          linkedIn: text(p.linkedinUrl || (p.professional_profile && p.professional_profile.url)),
        });
        var anchor = econId || propcoId;
        if (anchor) {
          addEdge({
            from: anchor,
            to: personId,
            type: "ASSOCIATED_PERSON",
            verification: "proposed",
          });
        }
      });
    }

    if (includePortfolioPreview && econId) {
      var owned =
        (ownership && ownership.organization && ownership.organization.owned_controlled_hotels) ||
        [];
      owned.slice(0, maxPortfolio).forEach(function (h) {
        var hid = text(h.airtable_record_id || h.id);
        var hname = text(h.hotel_name || h.name);
        if (!hname) return;
        var nid = hid ? "hotel:" + hid : "hotel:portfolio:" + hname;
        if (nid === hotelNodeId) return;
        var bucket = String(h.verification_bucket || h.ownership_evidence_class || "").toUpperCase();
        var ver = /VERIFIED/.test(bucket)
          ? "verified"
          : /HISTOR/.test(bucket)
            ? "historical"
            : /UNRESOLVED|PENDING/.test(bucket)
              ? "unresolved"
              : "proposed";
        addNode({
          id: nid,
          kind: "hotel",
          label: hname,
          role: "Portfolio hotel",
          verification: ver,
          hpcId: hid && /^rec[a-zA-Z0-9]{14}$/.test(hid) ? hid : null,
          pendingHpc: !hid,
        });
        addEdge({
          from: econId,
          to: nid,
          type: "PORTFOLIO_HOTEL",
          verification: ver,
          temporal: /HISTOR/.test(String(h.current_historical || "")) ? "HISTORICAL" : "CURRENT",
        });
      });
    }

    return {
      version: VERSION,
      focusHotelId: hotelNodeId,
      nodes: nodes,
      edges: edges,
      summary: {
        legalOwner: partyKnown(propco) ? partyName(propco) : "Not yet verified",
        legalOwnerVerification: partyKnown(propco) ? verificationOf(propco) : "unresolved",
        economicSponsor: partyKnown(econ) ? partyName(econ) : "Not yet verified",
        economicSponsorVerification: partyKnown(econ) ? verificationOf(econ) : "unresolved",
        operator: operatorName || "Not yet verified",
        brand: brandName || "—",
        ubo:
          partyKnown(ubo) && ubo.status !== "UNRESOLVED" ? partyName(ubo) : "Unresolved",
        ownershipStatus: text(ctrl.control_status) || "PARTIAL",
      },
      legend: [
        { key: "verified", label: "Verified current" },
        { key: "proposed", label: "Proposed" },
        { key: "historical", label: "Historical" },
        { key: "unresolved", label: "Unresolved" },
      ],
      meta: {
        proposedMode:
          (ownership && ownership.preview_mode === "PHASE3_PROPOSED") ||
          (ownership &&
            ownership.report &&
            ownership.report.research_status &&
            ownership.report.research_status.preview_mode === "PHASE3_PROPOSED"),
      },
    };
  }

  function layout(graph, opts) {
    opts = opts || {};
    var width = Number(opts.width) || 920;
    var height = Number(opts.height) || 480;
    var nodeW = 172;
    var nodeH = 62;
    var nodes = graph.nodes || [];
    var edges = graph.edges || [];
    var focus = nodes.filter(function (n) {
      return n.focus;
    })[0] || nodes[0];
    var positions = {};
    var cx = width / 2;
    var econ = nodes.filter(function (n) {
      return n.role === "Economic owner / sponsor";
    })[0];
    var propco = nodes.filter(function (n) {
      return n.role === "Legal property owner";
    })[0];
    var operator = nodes.filter(function (n) {
      return n.role === "Hotel operator";
    })[0];
    var brand = nodes.filter(function (n) {
      return n.role === "Brand";
    })[0];
    var people = nodes.filter(function (n) {
      return n.kind === "person";
    });
    var portfolio = nodes.filter(function (n) {
      return n.role === "Portfolio hotel";
    });

    if (focus) positions[focus.id] = { x: cx, y: 230 };
    if (propco) positions[propco.id] = { x: cx, y: 125 };
    if (econ) positions[econ.id] = { x: cx, y: 30 };
    if (operator) positions[operator.id] = { x: cx - 230, y: 340 };
    if (brand) positions[brand.id] = { x: cx + 230, y: 340 };
    people.forEach(function (p, i) {
      positions[p.id] = { x: cx + 250, y: (econ ? 30 : 125) + i * 74 };
    });
    portfolio.forEach(function (h, i) {
      positions[h.id] = { x: 90 + (i % 4) * 200, y: 430 + Math.floor(i / 4) * 74 };
    });
    var leftover = 0;
    nodes.forEach(function (n) {
      if (!positions[n.id]) {
        positions[n.id] = {
          x: 90 + (leftover % 4) * 200,
          y: 430 + Math.floor(leftover / 4) * 74,
        };
        leftover += 1;
      }
    });
    return {
      width: width,
      height: Math.max(height, portfolio.length ? 430 + Math.ceil(portfolio.length / 4) * 74 + 50 : height),
      nodeW: nodeW,
      nodeH: nodeH,
      positions: positions,
      nodes: nodes,
      edges: edges,
    };
  }

  function strokeDash(ver) {
    if (ver === "proposed") return "6 4";
    if (ver === "historical") return "2 5";
    if (ver === "unresolved") return "1 4";
    return "none";
  }

  function renderShell(graph, layoutData) {
    var L = layoutData;
    var edgeSvg = (L.edges || [])
      .map(function (e) {
        var a = L.positions[e.from];
        var b = L.positions[e.to];
        if (!a || !b) return "";
        var mx = (a.x + b.x) / 2;
        var my = (a.y + b.y) / 2;
        return (
          '<g class="ocn-edge ocn-edge--' +
          esc(e.verification) +
          '" data-ocn-edge="' +
          esc(e.id) +
          '">' +
          '<line x1="' +
          a.x +
          '" y1="' +
          a.y +
          '" x2="' +
          b.x +
          '" y2="' +
          b.y +
          '" stroke-dasharray="' +
          strokeDash(e.verification) +
          '"></line>' +
          '<text class="ocn-edge-label" x="' +
          mx +
          '" y="' +
          (my - 6) +
          '">' +
          esc(e.label) +
          "</text></g>"
        );
      })
      .join("");

    var nodeSvg = (L.nodes || [])
      .map(function (n) {
        var p = L.positions[n.id];
        if (!p) return "";
        var x = p.x - L.nodeW / 2;
        var y = p.y - L.nodeH / 2;
        return (
          '<g class="ocn-node ocn-node--' +
          esc(n.kind) +
          " ocn-node--" +
          esc(n.verification) +
          (n.focus ? " is-focus" : "") +
          '" data-ocn-node="' +
          esc(n.id) +
          '" transform="translate(' +
          x +
          " " +
          y +
          ')">' +
          '<rect width="' +
          L.nodeW +
          '" height="' +
          L.nodeH +
          '" rx="10"></rect>' +
          '<text class="ocn-node__role" x="12" y="20">' +
          esc(n.role) +
          "</text>" +
          '<text class="ocn-node__label" x="12" y="40">' +
          esc((n.label || "").slice(0, 34)) +
          "</text>" +
          '<text class="ocn-node__meta" x="12" y="54">' +
          esc(friendlyVerification(n.verification)) +
          "</text></g>"
        );
      })
      .join("");

    var legend = (graph.legend || [])
      .map(function (item) {
        return (
          '<span class="ocn-legend__item ocn-legend__item--' +
          esc(item.key) +
          '"><i></i>' +
          esc(item.label) +
          "</span>"
        );
      })
      .join("");

    var s = graph.summary || {};
    return (
      '<div class="ocn" data-ocn="1">' +
      '<div class="ocn-toolbar">' +
      '<button type="button" class="ocn-btn" data-ocn-fit>Fit</button>' +
      '<button type="button" class="ocn-btn" data-ocn-reset>Reset</button>' +
      '<button type="button" class="ocn-btn" data-ocn-expand-sponsor>Expand sponsor hotels</button>' +
      "</div>" +
      '<div class="ocn-legend" aria-label="Relationship legend">' +
      legend +
      '<span class="ocn-legend__note">Solid = verified · Dashed = proposed · Dotted = historical</span>' +
      "</div>" +
      '<div class="ocn-stage">' +
      '<svg class="ocn-svg" viewBox="0 0 ' +
      L.width +
      " " +
      L.height +
      '" role="img" aria-label="Ownership and control network">' +
      '<g class="ocn-viewport">' +
      edgeSvg +
      nodeSvg +
      "</g></svg>" +
      '<aside class="ocn-detail" data-ocn-detail hidden>' +
      '<button type="button" class="ocn-detail__close" data-ocn-detail-close aria-label="Close detail">&times;</button>' +
      '<div class="ocn-detail__body"></div>' +
      "</aside></div>" +
      '<div class="ocn-summary" aria-label="Ownership status">' +
      '<div><span class="ocn-summary__k">Legal owner</span><span class="ocn-summary__v">' +
      esc(s.legalOwner) +
      " · " +
      esc(friendlyVerification(s.legalOwnerVerification)) +
      "</span></div>" +
      '<div><span class="ocn-summary__k">Economic sponsor</span><span class="ocn-summary__v">' +
      esc(s.economicSponsor) +
      " · " +
      esc(friendlyVerification(s.economicSponsorVerification)) +
      "</span></div>" +
      '<div><span class="ocn-summary__k">Operator</span><span class="ocn-summary__v">' +
      esc(s.operator) +
      "</span></div>" +
      '<div><span class="ocn-summary__k">Brand</span><span class="ocn-summary__v">' +
      esc(s.brand) +
      "</span></div>" +
      '<div><span class="ocn-summary__k">UBO</span><span class="ocn-summary__v">' +
      esc(s.ubo) +
      "</span></div>" +
      "</div></div>"
    );
  }

  function detailHtml(node, graph) {
    if (!node) return "";
    var bits = [];
    bits.push("<h4>" + esc(node.label) + "</h4>");
    bits.push('<p class="ocn-detail__role">' + esc(node.role) + "</p>");
    bits.push(
      '<p><strong>Status:</strong> ' + esc(friendlyVerification(node.verification)) + "</p>"
    );
    if (node.kind === "hotel" && node.hpcId) {
      bits.push(
        '<p><button type="button" class="ocn-btn ocn-btn--primary" data-ocn-open-hotel="' +
          esc(node.hpcId) +
          '">Open Hotel Explorer</button></p>'
      );
    } else if (node.kind === "hotel" && node.pendingHpc) {
      bits.push("<p>Pending HPC match — deep link unavailable.</p>");
    }
    if (node.kind === "person") {
      bits.push(
        '<p><button type="button" class="ocn-btn" data-ocn-open-contacts>Open Contact Intelligence</button></p>'
      );
      if (node.linkedIn) {
        bits.push(
          '<p><a class="ocn-ext" href="' +
            esc(node.linkedIn) +
            '" target="_blank" rel="noopener noreferrer">LinkedIn profile</a></p>'
        );
      }
    }
    if (node.kind === "org" && node.expandKey) {
      bits.push(
        '<p><button type="button" class="ocn-btn" data-ocn-expand-org="' +
          esc(node.id) +
          '">Expand related hotels</button></p>'
      );
    }
    var related = (graph.edges || []).filter(function (e) {
      return e.from === node.id || e.to === node.id;
    });
    if (related.length) {
      bits.push("<p><strong>Connections</strong></p><ul>");
      related.forEach(function (e) {
        bits.push("<li>" + esc(e.label) + "</li>");
      });
      bits.push("</ul>");
    }
    return bits.join("");
  }

  function mount(host, hotel, ownership, opts) {
    if (!host) return null;
    opts = opts || {};
    var graph = buildOwnershipControlNetwork(hotel, ownership, {
      includePeople: true,
      includePortfolioPreview: !!opts.includePortfolioPreview,
    });
    var laid = layout(graph, { width: opts.width || 920, height: opts.height || 480 });
    host.innerHTML = renderShell(graph, laid);
    host._ocn = { graph: graph, laid: laid, hotel: hotel, ownership: ownership, scale: 1, tx: 0, ty: 0 };

    var svg = host.querySelector(".ocn-svg");
    var viewport = host.querySelector(".ocn-viewport");
    var detail = host.querySelector("[data-ocn-detail]");
    var detailBody = host.querySelector(".ocn-detail__body");

    function applyTransform() {
      if (!viewport) return;
      var s = host._ocn;
      viewport.setAttribute(
        "transform",
        "translate(" + s.tx + " " + s.ty + ") scale(" + s.scale + ")"
      );
    }

    function showDetail(node) {
      if (!detail || !detailBody) return;
      detail.hidden = false;
      detailBody.innerHTML = detailHtml(node, host._ocn.graph);
      host.querySelectorAll(".ocn-node").forEach(function (el) {
        el.classList.toggle("is-selected", el.getAttribute("data-ocn-node") === node.id);
      });
    }

    host.addEventListener("click", function (ev) {
      var close = ev.target.closest("[data-ocn-detail-close]");
      if (close && detail) {
        detail.hidden = true;
        return;
      }
      if (ev.target.closest("[data-ocn-fit]")) {
        host._ocn.scale = 1;
        host._ocn.tx = 0;
        host._ocn.ty = 0;
        applyTransform();
        return;
      }
      if (ev.target.closest("[data-ocn-reset]")) {
        mount(host, hotel, ownership, opts);
        return;
      }
      if (ev.target.closest("[data-ocn-expand-sponsor]")) {
        var expandSlot = host.querySelector("[data-ocn-expand-loading]") || host;
        if (global.HeAsyncSection && global.HeAsyncSection.waveCompact) {
          var banner = document.createElement("div");
          banner.className = "ocn-expand-loading";
          banner.setAttribute("data-ocn-expand-loading", "1");
          banner.innerHTML = global.HeAsyncSection.waveCompact(
            "Expanding portfolio…",
            "Ownership network"
          );
          host.insertBefore(banner, host.firstChild);
        }
        // Sync remount — yield so the compact wave paints before work
        setTimeout(function () {
          mount(
            host,
            hotel,
            ownership,
            Object.assign({}, opts, { includePortfolioPreview: true })
          );
        }, 0);
        return;
      }
      var openHotel = ev.target.closest("[data-ocn-open-hotel]");
      if (openHotel) {
        var hid = openHotel.getAttribute("data-ocn-open-hotel");
        if (hid && global.HotelExplorer && typeof global.HotelExplorer.open === "function") {
          global.HotelExplorer.open({ id: hid, recordId: hid });
        }
        return;
      }
      var openContacts = ev.target.closest("[data-ocn-open-contacts]");
      if (openContacts) {
        if (global.HotelExplorer && typeof global.HotelExplorer.setTab === "function") {
          global.HotelExplorer.setTab("contacts");
        } else {
          var btn = document.querySelector('[data-hex-tab="contacts"], [data-hiw-tab="contacts"]');
          if (btn) btn.click();
        }
        return;
      }
      var nodeEl = ev.target.closest("[data-ocn-node]");
      if (nodeEl) {
        var nid = nodeEl.getAttribute("data-ocn-node");
        var node = (host._ocn.graph.nodes || []).filter(function (n) {
          return n.id === nid;
        })[0];
        if (node) showDetail(node);
      }
    });

    // Pan via drag on SVG background
    if (svg) {
      var dragging = false;
      var lastX = 0;
      var lastY = 0;
      svg.addEventListener("pointerdown", function (ev) {
        if (ev.target.closest("[data-ocn-node]")) return;
        dragging = true;
        lastX = ev.clientX;
        lastY = ev.clientY;
        svg.setPointerCapture(ev.pointerId);
      });
      svg.addEventListener("pointermove", function (ev) {
        if (!dragging) return;
        host._ocn.tx += ev.clientX - lastX;
        host._ocn.ty += ev.clientY - lastY;
        lastX = ev.clientX;
        lastY = ev.clientY;
        applyTransform();
      });
      svg.addEventListener("pointerup", function () {
        dragging = false;
      });
      svg.addEventListener(
        "wheel",
        function (ev) {
          ev.preventDefault();
          var delta = ev.deltaY > 0 ? 0.92 : 1.08;
          host._ocn.scale = Math.max(0.55, Math.min(1.8, host._ocn.scale * delta));
          applyTransform();
        },
        { passive: false }
      );
    }

    return host._ocn;
  }

  function renderMountHtml() {
    return '<div class="ocn-host" data-ocn-host="1"></div>';
  }

  global.HeOwnershipControlNetwork = {
    VERSION: VERSION,
    buildOwnershipControlNetwork: buildOwnershipControlNetwork,
    layout: layout,
    mount: mount,
    renderMountHtml: renderMountHtml,
    EDGE_LABELS: EDGE_LABELS,
  };
})(typeof window !== "undefined" ? window : globalThis);
