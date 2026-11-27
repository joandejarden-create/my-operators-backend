/**
 * Hotel Stakeholder & Deal Relationship Network (browser).
 * Layered SVG layout — content-measured nodes, progressive disclosure, bounded expansion.
 * Operator/brand never drawn as ownership.
 */
(function (global) {
  "use strict";

  var VERSION = "ownership-control-network-v1";
  var MAX_EXPANSION_HOTELS = 8;
  var MAX_VISIBLE_PEOPLE = 5;
  var MIN_LABEL_PX = 13;

  var EDGE_LABELS = {
    LEGALLY_OWNED_BY: "Legal Ownership",
    ECONOMICALLY_CONTROLLED_BY: "Economic Sponsorship",
    ULTIMATE_CONTROL: "Ultimate Control",
    MANAGED_BY: "Hotel Management",
    BRANDED_AS: "Brand Affiliation",
    ASSOCIATED_PERSON: "Key Person",
    PORTFOLIO_HOTEL: "Related Hotel",
    ASSET_MANAGED_BY: "Asset Management",
    FINANCED_BY: "Financing",
    DEVELOPED_BY: "Developer",
    ADVISED_BY: "Advisor",
    EQUITY_PARTNER: "Equity Partner",
  };

  /** Compact fallbacks only when connector gap cannot fit the full customer label. */
  var SHORT_EDGE = {
    LEGALLY_OWNED_BY: "Legal Ownership",
    ECONOMICALLY_CONTROLLED_BY: "Economic Sponsorship",
    ULTIMATE_CONTROL: "Ultimate Control",
    MANAGED_BY: "Hotel Management",
    BRANDED_AS: "Brand Affiliation",
    ASSOCIATED_PERSON: "Role",
    PORTFOLIO_HOTEL: "Related Hotel",
    ASSET_MANAGED_BY: "Asset Mgmt",
    FINANCED_BY: "Financing",
    DEVELOPED_BY: "Developer",
    ADVISED_BY: "Advisor",
    EQUITY_PARTNER: "Equity",
  };

  var EDGE_LABEL_PAD_X = 8;
  var EDGE_LABEL_PAD_Y = 4;
  var EDGE_LABEL_FONT = 11;
  var EDGE_LABEL_MIN_CLEARANCE = 10;
  var PRIMARY_CHAIN_GAP_Y = 72;

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

  function wrapNodeLabel(label, maxChars, maxLines) {
    maxChars = maxChars || 22;
    maxLines = maxLines || 3;
    var raw = String(label || "").trim();
    if (!raw) return [""];
    var words = raw.split(/\s+/);
    var lines = [];
    var cur = "";
    for (var i = 0; i < words.length; i++) {
      var w = words[i];
      var next = cur ? cur + " " + w : w;
      if (next.length <= maxChars) {
        cur = next;
      } else {
        if (cur) lines.push(cur);
        cur = w.length > maxChars ? w.slice(0, maxChars - 1) + "…" : w;
        if (lines.length >= maxLines - 1) break;
      }
    }
    if (cur && lines.length < maxLines) lines.push(cur);
    return lines.length ? lines : [raw.slice(0, maxChars)];
  }

  function measureNodeBox(node) {
    var focus = !!(node && node.focus);
    var width = focus ? 248 : 212;
    var lines = wrapNodeLabel(node && node.label, focus ? 26 : 22, 3);
    var height = 14 + 14 + lines.length * 16 + 14 + 10;
    return {
      width: width,
      height: Math.max(focus ? 78 : 70, height),
      labelLines: lines,
    };
  }

  function buildOwnershipControlNetwork(hotel, ownership, opts) {
    opts = opts || {};
    var activeGroup = text(opts.activeGroup) || null;
    var includePeople = opts.includePeople === true || activeGroup === "asset_people";
    var includePortfolioPreview = opts.includePortfolioPreview === true || activeGroup === "related_hotels";
    var includeHistoricalCapital = opts.includeHistoricalCapital === true || activeGroup === "capital_financing";
    var includeHistoricalAdvisors = opts.includeHistoricalAdvisors === true || activeGroup === "development_advisors";
    var portfolioAsCluster = opts.portfolioAsCluster !== false;
    var maxPortfolio = Math.min(Number(opts.maxPortfolio) || MAX_EXPANSION_HOTELS, MAX_EXPANSION_HOTELS);
    var maxPeople = Math.min(Number(opts.maxPeople) || MAX_VISIBLE_PEOPLE, MAX_VISIBLE_PEOPLE);
    var brandDevFilter = opts.brandDevelopmentFilter === true;

    var ctrl = (ownership && ownership.report && ownership.report.ownership_and_control) || {};
    var propco = ctrl.legal_property_owner_propco || ctrl.property_company || {};
    var econ = ctrl.economic_owner_or_group || ctrl.economic_owner || {};
    var ubo = ctrl.ubo_group || {};
    var operator = ctrl.operator || {};
    var brandParty = ctrl.brand || {};
    var assetMgr = ctrl.asset_manager || ctrl.asset_management_company || {};
    var lender = ctrl.lender || ctrl.financing_institution || {};
    var developer = ctrl.developer || {};
    var advisor = ctrl.advisor || ctrl.broker || {};

    var hotelId = text(hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || "focus";
    var hotelName = text(hotel && hotel.name) || "Hotel";
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
      var box = measureNodeBox(node);
      node.labelLines = box.labelLines;
      node.boxW = box.width;
      node.boxH = box.height;
      node.fullName = node.fullName || node.label;
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
        shortLabel: edge.shortLabel || SHORT_EDGE[edge.type] || EDGE_LABELS[edge.type] || edge.type,
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
      group: "focus",
    });

    var propcoId = null;
    var econId = null;

    if (partyKnown(propco)) {
      propcoId = "org:propco:" + (text(propco.entity_id) || partyName(propco));
      addNode({
        id: propcoId,
        kind: "org",
        label: partyName(propco),
        fullName: text(propco.legal_name) || partyName(propco),
        role: "Legal property owner",
        verification: verificationOf(propco),
        expandKey: text(propco.entity_id) || null,
        group: "ownership_control",
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
        fullName: text(econ.legal_name) || partyName(econ),
        role: "Economic owner / sponsor",
        verification: verificationOf(econ),
        expandKey: text(econ.entity_id) || null,
        group: "ownership_control",
      });
      addEdge({
        from: propcoId || hotelNodeId,
        to: econId,
        type: "ECONOMICALLY_CONTROLLED_BY",
        verification: verificationOf(econ),
      });
    }

    if (partyKnown(ubo) && String(ubo.status || "").toUpperCase() !== "UNRESOLVED") {
      var uboId = "org:ubo:" + (text(ubo.entity_id) || partyName(ubo));
      addNode({
        id: uboId,
        kind: "org",
        label: partyName(ubo),
        role: "Ultimate beneficial owner",
        verification: verificationOf(ubo),
        group: "ownership_control",
      });
      addEdge({
        from: econId || propcoId || hotelNodeId,
        to: uboId,
        type: "ULTIMATE_CONTROL",
        verification: verificationOf(ubo),
      });
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
        group: "operations_brand",
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
        group: "operations_brand",
      });
      addEdge({
        from: hotelNodeId,
        to: brandId,
        type: "BRANDED_AS",
        verification: verificationOf(brandParty, "HIGH"),
      });
    }

    if (partyKnown(assetMgr)) {
      var amId = "org:asset:" + (text(assetMgr.entity_id) || partyName(assetMgr));
      addNode({
        id: amId,
        kind: "org",
        label: partyName(assetMgr),
        role: "Asset manager",
        verification: verificationOf(assetMgr),
        group: "asset_people",
      });
      addEdge({
        from: hotelNodeId,
        to: amId,
        type: "ASSET_MANAGED_BY",
        verification: verificationOf(assetMgr),
      });
    }
    if (partyKnown(lender)) {
      var lenderVer = verificationOf(lender);
      var lenderHistorical = lenderVer === "historical" || lender.current === false;
      if (!lenderHistorical || includeHistoricalCapital) {
        var lid = "org:lender:" + (text(lender.entity_id) || partyName(lender));
        addNode({
          id: lid,
          kind: "org",
          label: partyName(lender),
          role: lenderHistorical ? "Historical lender" : "Lender / financing",
          verification: lenderVer,
          group: "capital_financing",
          amountUsd: lender.amount_usd || lender.amountUsd || null,
          financingDate: text(lender.financing_date_approx || lender.effective_date),
          evidenceUrls: lender.evidence_urls || lender.evidenceUrls || [],
          note: text(lender.note),
        });
        addEdge({
          from: hotelNodeId,
          to: lid,
          type: "FINANCED_BY",
          verification: lenderVer,
          temporal: lenderHistorical ? "HISTORICAL" : "CURRENT",
        });
      }
    }
    if (partyKnown(developer) && String(developer.status || "").toUpperCase() !== "NOT_APPLICABLE") {
      var did = "org:dev:" + (text(developer.entity_id) || partyName(developer));
      addNode({
        id: did,
        kind: "org",
        label: partyName(developer),
        role: "Developer",
        verification: verificationOf(developer),
        group: "development_advisors",
      });
      addEdge({
        from: hotelNodeId,
        to: did,
        type: "DEVELOPED_BY",
        verification: verificationOf(developer),
      });
    }
    if (partyKnown(advisor)) {
      var advVer = verificationOf(advisor);
      var advHistorical = advVer === "historical" || advisor.current === false;
      if (!advHistorical || includeHistoricalAdvisors) {
        var aid = "org:adv:" + (text(advisor.entity_id) || partyName(advisor));
        addNode({
          id: aid,
          kind: "org",
          label: partyName(advisor),
          role: advHistorical ? "Historical advisor" : "Advisor / broker",
          verification: advVer,
          group: "development_advisors",
          evidenceUrls: advisor.evidence_urls || advisor.evidenceUrls || [],
          note: text(advisor.note),
          effectiveDate: text(advisor.effective_date),
        });
        addEdge({
          from: hotelNodeId,
          to: aid,
          type: "ADVISED_BY",
          verification: advVer,
          temporal: advHistorical ? "HISTORICAL" : "CURRENT",
        });
      }
    }

    var peopleHidden = 0;
    if (includePeople) {
      var people =
        (ownership &&
          ownership.report &&
          ownership.report.decision_authority &&
          ownership.report.decision_authority.people) ||
        [];
      var pSlice = people.slice(0, maxPeople);
      peopleHidden = Math.max(0, people.length - pSlice.length);
      pSlice.forEach(function (p, idx) {
        var name = text(p.display_name || p.name);
        if (!name) return;
        var personId = "person:" + (text(p.personId || p.person_id || p.entity_id) || idx) + ":" + name;
        var title = text(p.title || p.role) || "Relevant contact";
        var orgName = text(p.organization || p.company || p.org) || partyName(econ) || "Sponsor organization";
        addNode({
          id: personId,
          kind: "person",
          label: name,
          role: title,
          title: title,
          orgName: orgName,
          propertyResponsibility: text(p.propertyResponsibility || p.property_responsibility) || "UNKNOWN",
          verification: "proposed",
          personKey: text(p.personId || p.person_id) || null,
          linkedIn: text(p.linkedinUrl || (p.professional_profile && p.professional_profile.url)),
          group: "asset_people",
        });
        var anchor = econId || propcoId;
        if (anchor) {
          addEdge({
            from: anchor,
            to: personId,
            type: "ASSOCIATED_PERSON",
            verification: "proposed",
            shortLabel: title.length > 18 ? title.slice(0, 16) + "…" : title,
            label: title + " at " + orgName,
          });
        }
      });
    }

    var portfolioHidden = 0;
    if (includePortfolioPreview && econId) {
      var owned =
        (ownership && ownership.organization && ownership.organization.owned_controlled_hotels) ||
        [];
      var eligible = owned.filter(function (h) {
        var hname = text(h.hotel_name || h.name);
        var hid = text(h.airtable_record_id || h.id);
        if (!hname) return false;
        var nid = hid ? "hotel:" + hid : "hotel:portfolio:" + hname;
        return nid !== hotelNodeId;
      });
      var hSlice = eligible.slice(0, maxPortfolio);
      portfolioHidden = Math.max(0, eligible.length - hSlice.length);
      var mapped = hSlice.map(function (h) {
        var hid = text(h.airtable_record_id || h.id);
        var hname = text(h.hotel_name || h.name);
        var bucket = String(h.verification_bucket || h.ownership_evidence_class || "").toUpperCase();
        var ver = /VERIFIED/.test(bucket)
          ? "verified"
          : /HISTOR/.test(bucket)
            ? "historical"
            : /UNRESOLVED|PENDING/.test(bucket)
              ? "unresolved"
              : "proposed";
        return {
          id: hid ? "hotel:" + hid : "hotel:portfolio:" + hname,
          name: hname,
          brand: text(h.brand || h.brand_display),
          location: text(h.city || h.location || h.market),
          rooms: h.rooms || h.keys || null,
          verification: ver,
          hpcId: hid && /^rec[a-zA-Z0-9]{14}$/.test(hid) ? hid : null,
          pendingHpc: !hid,
        };
      });
      if (portfolioAsCluster && mapped.length) {
        var clusterId = "cluster:related-hotels";
        addNode({
          id: clusterId,
          kind: "cluster",
          label: "Related Hotels (" + eligible.length + ")",
          role: "Portfolio cluster",
          verification: "proposed",
          group: "related_hotels",
          clusterHotels: mapped,
          clusterTotal: eligible.length,
        });
        addEdge({
          from: econId,
          to: clusterId,
          type: "PORTFOLIO_HOTEL",
          verification: "proposed",
          shortLabel: "Portfolio",
          label: "Related hotels (bounded cluster — expand in detail)",
        });
      } else {
        mapped.forEach(function (h) {
          addNode({
            id: h.id,
            kind: "hotel",
            label: h.name,
            role: "Related hotel",
            verification: h.verification,
            hpcId: h.hpcId,
            pendingHpc: h.pendingHpc,
            group: "related_hotels",
          });
          addEdge({
            from: econId,
            to: h.id,
            type: "PORTFOLIO_HOTEL",
            verification: h.verification,
          });
        });
      }
    }

    var groupResearchStatus = null;
    var STAKEHOLDER_GROUPS = [
      { id: "ownership_control", label: "Ownership & Control", edgeTypes: ["LEGALLY_OWNED_BY", "ECONOMICALLY_CONTROLLED_BY", "ULTIMATE_CONTROL", "EQUITY_PARTNER"] },
      { id: "asset_people", label: "Asset Management & People", edgeTypes: ["ASSET_MANAGED_BY", "ASSOCIATED_PERSON"] },
      { id: "capital_financing", label: "Capital & Financing", edgeTypes: ["FINANCED_BY", "EQUITY_PARTNER"] },
      { id: "operations_brand", label: "Operations & Brand", edgeTypes: ["MANAGED_BY", "BRANDED_AS"] },
      { id: "development_advisors", label: "Development & Advisors", edgeTypes: ["DEVELOPED_BY", "ADVISED_BY"] },
      { id: "related_hotels", label: "Related Hotels", edgeTypes: ["PORTFOLIO_HOTEL"] },
    ];
    if (activeGroup) {
      var groupDef = STAKEHOLDER_GROUPS.filter(function (g) { return g.id === activeGroup; })[0];
      var allowedTypes = {};
      ((groupDef && groupDef.edgeTypes) || []).forEach(function (t) { allowedTypes[t] = true; });
      var keepG = {};
      keepG[hotelNodeId] = true;
      if (activeGroup === "asset_people" || activeGroup === "related_hotels" || activeGroup === "capital_financing") {
        nodes.forEach(function (n) {
          if (n.group === "ownership_control") keepG[n.id] = true;
        });
      }
      edges.forEach(function (e) {
        if (allowedTypes[e.type]) {
          keepG[e.from] = true;
          keepG[e.to] = true;
        }
      });
      nodes.forEach(function (n) {
        if (n.group === activeGroup || n.focus) keepG[n.id] = true;
      });
      nodes = nodes.filter(function (n) { return keepG[n.id]; });
      edges = edges.filter(function (e) { return keepG[e.from] && keepG[e.to]; });
      var groupHasContent = nodes.some(function (n) { return n.group === activeGroup; });
      if (!groupHasContent) {
        groupResearchStatus = {
          groupId: activeGroup,
          label: (groupDef && groupDef.label) || activeGroup,
          message: "No verified records in this category yet. Research status is tracked for future refresh.",
        };
      }
    } else if (brandDevFilter) {
      var keep = {};
      keep[hotelNodeId] = true;
      nodes.forEach(function (n) {
        if (n.group === "ownership_control" || n.group === "operations_brand" || n.focus) keep[n.id] = true;
      });
      edges.forEach(function (e) {
        if (
          /LEGALLY|ECONOMIC|ULTIMATE|MANAGED|BRANDED|ASSET_MANAGED|ASSOCIATED_PERSON|DEVELOPED|FINANCED|EQUITY/.test(
            e.type
          )
        ) {
          keep[e.from] = true;
          keep[e.to] = true;
        }
      });
      nodes = nodes.filter(function (n) {
        return keep[n.id];
      });
      edges = edges.filter(function (e) {
        return keep[e.from] && keep[e.to];
      });
    }

    return {
      version: VERSION,
      networkTitle: "Hotel Stakeholder & Deal Relationship Network",
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
          partyKnown(ubo) && String(ubo.status || "").toUpperCase() !== "UNRESOLVED"
            ? partyName(ubo)
            : "Unresolved",
        ownershipStatus: text(ctrl.control_status) || "PARTIAL",
      },
      expansion: {
        peopleVisible: includePeople,
        peopleHidden: peopleHidden,
        portfolioVisible: includePortfolioPreview,
        portfolioHidden: portfolioHidden,
      },
      legend: [
        { key: "verified", label: "Verified relationship" },
        { key: "proposed", label: "Proposed relationship" },
        { key: "historical", label: "Historical relationship" },
        { key: "unresolved", label: "Unresolved" },
      ],
      meta: {
        proposedMode:
          (ownership && ownership.preview_mode === "PHASE3_PROPOSED") ||
          (ownership &&
            ownership.report &&
            ownership.report.research_status &&
            ownership.report.research_status.preview_mode === "PHASE3_PROPOSED"),
        brandDevelopmentFilter: brandDevFilter,
        activeGroup: activeGroup,
        defaultView: !includePeople && !includePortfolioPreview && !activeGroup,
      },
      stakeholderGroups: STAKEHOLDER_GROUPS,
      groupResearchStatus: groupResearchStatus,
      researchCoverage: (ctrl && ctrl.stakeholder_research_coverage) || null,
    };
  }

  function layout(graph, opts) {
    opts = opts || {};
    var width = Number(opts.width) || 960;
    var nodes = (graph.nodes || []).map(function (n) {
      var box = measureNodeBox(n);
      return Object.assign({}, n, {
        boxW: n.boxW || box.width,
        boxH: n.boxH || box.height,
        labelLines: n.labelLines || box.labelLines,
      });
    });
    var edges = graph.edges || [];
    var positions = {};
    var boxes = {};
    nodes.forEach(function (n) {
      boxes[n.id] = { w: n.boxW, h: n.boxH };
    });

    var focus = nodes.filter(function (n) {
      return n.focus;
    })[0] || nodes[0];
    var econ = nodes.filter(function (n) {
      return n.role === "Economic owner / sponsor";
    })[0];
    var propco = nodes.filter(function (n) {
      return n.role === "Legal property owner";
    })[0];
    var uboN = nodes.filter(function (n) {
      return n.role === "Ultimate beneficial owner";
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
      return (
        n.kind === "cluster" ||
        n.role === "Related hotel" ||
        n.role === "Portfolio hotel" ||
        n.role === "Portfolio cluster"
      );
    });
    var extras = nodes.filter(function (n) {
      return (
        n.group === "capital_financing" ||
        n.group === "development_advisors" ||
        (n.group === "asset_people" && n.kind === "org")
      );
    });

    var cx = width / 2;
    var gapY = PRIMARY_CHAIN_GAP_Y;
    var opsGapX = Math.max(260, Math.min(320, width * 0.28));
    var y = 36;

    if (uboN) {
      positions[uboN.id] = { x: cx, y: y + boxes[uboN.id].h / 2 };
      y += boxes[uboN.id].h + gapY;
    }
    if (econ) {
      positions[econ.id] = { x: cx, y: y + boxes[econ.id].h / 2 };
      y += boxes[econ.id].h + gapY;
    }
    if (propco) {
      positions[propco.id] = { x: cx, y: y + boxes[propco.id].h / 2 };
      y += boxes[propco.id].h + gapY;
    }
    if (focus) {
      y += 12;
      positions[focus.id] = { x: cx, y: y + boxes[focus.id].h / 2 };
      y += boxes[focus.id].h + gapY + 18;
    }

    var opsY =
      y +
      Math.max(
        (operator && boxes[operator.id].h) || 70,
        (brand && boxes[brand.id].h) || 70
      ) /
        2;
    if (operator) positions[operator.id] = { x: cx - opsGapX, y: opsY };
    if (brand) positions[brand.id] = { x: cx + opsGapX, y: opsY };
    if (operator || brand) {
      y =
        opsY +
        Math.max((operator && boxes[operator.id].h) || 0, (brand && boxes[brand.id].h) || 0) / 2 +
        gapY;
    }

    if (extras.length) {
      var span = Math.min(extras.length, 4);
      var startX = cx - ((span - 1) * 230) / 2;
      extras.slice(0, 4).forEach(function (n, i) {
        positions[n.id] = { x: startX + i * 230, y: y + boxes[n.id].h / 2 };
      });
      var maxH = 0;
      extras.slice(0, 4).forEach(function (n) {
        maxH = Math.max(maxH, boxes[n.id].h);
      });
      y += maxH + gapY;
    }

    var peopleAnchor = econ || propco || focus;
    var peopleBaseX = Math.min(
      width - 130,
      (peopleAnchor && positions[peopleAnchor.id] ? positions[peopleAnchor.id].x : cx) + 280
    );
    var peopleBaseY =
      (peopleAnchor && positions[peopleAnchor.id] ? positions[peopleAnchor.id].y : y) -
      ((people.length - 1) * 92) / 2;
    people.forEach(function (p, i) {
      positions[p.id] = { x: peopleBaseX, y: peopleBaseY + i * 92 };
    });

    if (portfolio.length) {
      var cols = Math.min(3, portfolio.length);
      var peopleBottom = y;
      people.forEach(function (p) {
        peopleBottom = Math.max(peopleBottom, positions[p.id].y + boxes[p.id].h / 2 + 28);
      });
      var gridTop = peopleBottom;
      portfolio.forEach(function (h, i) {
        var col = i % cols;
        var row = Math.floor(i / cols);
        var gridWidth = cols * 230;
        var left = cx - gridWidth / 2 + 115;
        positions[h.id] = {
          x: left + col * 230,
          y: gridTop + row * 100 + boxes[h.id].h / 2,
        };
      });
      y = gridTop + Math.ceil(portfolio.length / cols) * 100 + 28;
    }

    var leftover = 0;
    nodes.forEach(function (n) {
      if (!positions[n.id]) {
        positions[n.id] = {
          x: 130 + (leftover % 3) * 230,
          y: y + Math.floor(leftover / 3) * 100,
        };
        leftover += 1;
      }
    });

    // Horizontal collision nudge
    var sorted = nodes.slice().sort(function (a, b) {
      return positions[a.id].y - positions[b.id].y;
    });
    for (var i = 0; i < sorted.length; i++) {
      for (var j = i + 1; j < sorted.length; j++) {
        var a = sorted[i];
        var b = sorted[j];
        if (Math.abs(positions[a.id].y - positions[b.id].y) > 42) continue;
        var minDx = (boxes[a.id].w + boxes[b.id].w) / 2 + 32;
        var dx = positions[b.id].x - positions[a.id].x;
        if (Math.abs(dx) < minDx) {
          var push = (minDx - Math.abs(dx)) / 2 + 2;
          if (dx >= 0) {
            positions[b.id].x += push;
            positions[a.id].x -= push;
          } else {
            positions[a.id].x += push;
            positions[b.id].x -= push;
          }
        }
      }
    }

    var maxY = 440;
    nodes.forEach(function (n) {
      maxY = Math.max(maxY, positions[n.id].y + boxes[n.id].h / 2 + 48);
    });

    return {
      width: width,
      height: Math.max(Number(opts.height) || 520, maxY),
      positions: positions,
      boxes: boxes,
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

  function connectorAnchors(a, b, aw, ah, bw, bh) {
    var x1 = a.x;
    var y1 = a.y;
    var x2 = b.x;
    var y2 = b.y;
    var vertical = Math.abs(y2 - y1) > Math.abs(x2 - x1);
    if (vertical) {
      if (y2 > y1) {
        y1 += ah / 2;
        y2 -= bh / 2;
      } else {
        y1 -= ah / 2;
        y2 += bh / 2;
      }
    } else if (x2 > x1) {
      x1 += aw / 2;
      x2 -= bw / 2;
    } else {
      x1 -= aw / 2;
      x2 += bw / 2;
    }
    return { x1: x1, y1: y1, x2: x2, y2: y2, vertical: vertical };
  }

  function elbowPath(a, b, aw, ah, bw, bh) {
    // Connect measured node borders with a vertical-then-horizontal elbow.
    var c = connectorAnchors(a, b, aw, ah, bw, bh);
    if (c.vertical) {
      var midY = (c.y1 + c.y2) / 2;
      return (
        "M " +
        c.x1 +
        " " +
        c.y1 +
        " L " +
        c.x1 +
        " " +
        midY +
        " L " +
        c.x2 +
        " " +
        midY +
        " L " +
        c.x2 +
        " " +
        c.y2
      );
    }
    var midX = (c.x1 + c.x2) / 2;
    return (
      "M " +
      c.x1 +
      " " +
      c.y1 +
      " L " +
      midX +
      " " +
      c.y1 +
      " L " +
      midX +
      " " +
      c.y2 +
      " L " +
      c.x2 +
      " " +
      c.y2
    );
  }

  function estimateLabelWidth(label) {
    return Math.ceil(String(label || "").length * 6.55) + EDGE_LABEL_PAD_X * 2;
  }

  function pickEdgeDisplayLabel(edge, gapPx, spanPx) {
    var full =
      EDGE_LABELS[edge.type] ||
      edge.label ||
      edge.shortLabel ||
      SHORT_EDGE[edge.type] ||
      "";
    var compact = edge.shortLabel || SHORT_EDGE[edge.type] || full;
    var fullW = estimateLabelWidth(full);
    var canFitGap = gapPx >= EDGE_LABEL_FONT + EDGE_LABEL_PAD_Y * 2 + EDGE_LABEL_MIN_CLEARANCE * 2;
    var canFitSpan = spanPx >= fullW + 8;
    if (canFitGap && canFitSpan) return full;
    if (estimateLabelWidth(compact) <= spanPx) return compact;
    return compact;
  }

  function edgeLabelPlacement(a, b, aw, ah, bw, bh, labelText) {
    var c = connectorAnchors(a, b, aw, ah, bw, bh);
    var labelW = estimateLabelWidth(labelText);
    var labelH = EDGE_LABEL_FONT + EDGE_LABEL_PAD_Y * 2;
    var lx;
    var ly;
    var gapPx;
    var spanPx;
    if (c.vertical) {
      gapPx = Math.abs(c.y2 - c.y1);
      spanPx = Math.max(aw, bw, 220);
      lx = (c.x1 + c.x2) / 2;
      ly = (c.y1 + c.y2) / 2;
      // Keep chip centered in the open connector gap, never on node borders.
      var minY = Math.min(c.y1, c.y2) + EDGE_LABEL_MIN_CLEARANCE + labelH / 2;
      var maxY = Math.max(c.y1, c.y2) - EDGE_LABEL_MIN_CLEARANCE - labelH / 2;
      if (maxY > minY) {
        ly = Math.max(minY, Math.min(maxY, ly));
      }
    } else {
      gapPx = Math.abs(c.x2 - c.x1);
      spanPx = gapPx;
      lx = (c.x1 + c.x2) / 2;
      ly = (c.y1 + c.y2) / 2;
      var minX = Math.min(c.x1, c.x2) + EDGE_LABEL_MIN_CLEARANCE + labelW / 2;
      var maxX = Math.max(c.x1, c.x2) - EDGE_LABEL_MIN_CLEARANCE - labelW / 2;
      if (maxX > minX) {
        lx = Math.max(minX, Math.min(maxX, lx));
      }
      // Lift slightly above the horizontal segment so text does not sit on the stroke.
      ly -= labelH / 2 + 2;
    }
    return {
      x: lx,
      y: ly,
      w: labelW,
      h: labelH,
      gapPx: gapPx,
      spanPx: spanPx,
    };
  }

  function renderEdgeLabelChip(labelText, place) {
    var x = place.x - place.w / 2;
    var y = place.y - place.h / 2;
    return (
      '<g class="ocn-edge-label-host" transform="translate(' +
      x +
      " " +
      y +
      ')" data-ocn-edge-chip="1">' +
      '<rect class="ocn-edge-label-bg" width="' +
      place.w +
      '" height="' +
      place.h +
      '" rx="4"></rect>' +
      '<text class="ocn-edge-label" x="' +
      place.w / 2 +
      '" y="' +
      (place.h / 2 + 1) +
      '" text-anchor="middle" dominant-baseline="middle">' +
      esc(labelText) +
      "</text></g>"
    );
  }

  function renderNodeSvg(n, L) {
    var p = L.positions[n.id];
    var box = L.boxes[n.id] || { w: n.boxW || 212, h: n.boxH || 70 };
    if (!p) return "";
    var x = p.x - box.w / 2;
    var y = p.y - box.h / 2;
    var role = n.nodeRole || n.role || "";
    var lines = n.labelLines || wrapNodeLabel(n.label, n.focus ? 26 : 22, 3);
    var tspans = lines
      .map(function (line, idx) {
        return (
          '<tspan x="14" dy="' +
          (idx === 0 ? 0 : 16) +
          '">' +
          esc(line) +
          "</tspan>"
        );
      })
      .join("");
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
      box.w +
      '" height="' +
      box.h +
      '" rx="10"></rect>' +
      '<text class="ocn-node__role" x="14" y="18">' +
      esc(role) +
      "</text>" +
      '<text class="ocn-node__label" x="14" y="38" font-size="' +
      MIN_LABEL_PX +
      '">' +
      tspans +
      "</text>" +
      '<text class="ocn-node__meta" x="14" y="' +
      (box.h - 10) +
      '">' +
      esc(friendlyVerification(n.verification)) +
      "</text></g>"
    );
  }

  function renderShell(graph, layoutData, opts) {
    opts = opts || {};
    var L = layoutData;
    var edgeSvg = (L.edges || [])
      .map(function (e) {
        var a = L.positions[e.from];
        var b = L.positions[e.to];
        var aw = (L.boxes[e.from] && L.boxes[e.from].w) || 212;
        var ah = (L.boxes[e.from] && L.boxes[e.from].h) || 70;
        var bw = (L.boxes[e.to] && L.boxes[e.to].w) || 212;
        var bh = (L.boxes[e.to] && L.boxes[e.to].h) || 70;
        if (!a || !b) return "";
        var d = elbowPath(a, b, aw, ah, bw, bh);
        var anchors = connectorAnchors(a, b, aw, ah, bw, bh);
        var provisionalGap = anchors.vertical
          ? Math.abs(anchors.y2 - anchors.y1)
          : Math.abs(anchors.x2 - anchors.x1);
        var provisionalSpan = anchors.vertical ? Math.max(aw, bw, 220) : provisionalGap;
        var labelText = pickEdgeDisplayLabel(e, provisionalGap, provisionalSpan);
        var place = edgeLabelPlacement(a, b, aw, ah, bw, bh, labelText);
        return (
          '<g class="ocn-edge ocn-edge--' +
          esc(e.verification) +
          '" data-ocn-edge="' +
          esc(e.id) +
          '" data-ocn-edge-label="' +
          esc(labelText) +
          '" data-ocn-edge-type="' +
          esc(e.type) +
          '">' +
          '<path d="' +
          d +
          '" fill="none" stroke-dasharray="' +
          strokeDash(e.verification) +
          '"></path>' +
          renderEdgeLabelChip(labelText, place) +
          "</g>"
        );
      })
      .join("");

    var nodeSvg = (L.nodes || [])
      .map(function (n) {
        return renderNodeSvg(n, L);
      })
      .join("");

    var legendItems = (graph.legend || []).length
      ? graph.legend
      : [
          { key: "verified", label: "Verified relationship" },
          { key: "proposed", label: "Proposed relationship" },
          { key: "historical", label: "Historical relationship" },
          { key: "unresolved", label: "Unresolved" },
        ];
    var legend = legendItems
      .map(function (item) {
        return (
          '<li class="ocn-legend__item ocn-legend__item--' +
          esc(item.key) +
          '"><i aria-hidden="true"></i><span>' +
          esc(item.label) +
          "</span></li>"
        );
      })
      .join("");

    var s = graph.summary || {};
    var exp = graph.expansion || {};
    var groups = graph.stakeholderGroups || [];
    var activeGroup = (graph.meta && graph.meta.activeGroup) || opts.activeGroup || "";
    var groupChips = groups
      .map(function (g) {
        return (
          '<button type="button" class="ocn-chip' +
          (activeGroup === g.id ? " is-active" : "") +
          '" data-ocn-group="' +
          esc(g.id) +
          '" aria-pressed="' +
          (activeGroup === g.id ? "true" : "false") +
          '">' +
          esc(g.label) +
          "</button>"
        );
      })
      .join("");
    var researchBanner = "";
    if (graph.groupResearchStatus && graph.groupResearchStatus.message) {
      researchBanner =
        '<div class="ocn-research-status" role="status"><strong>' +
        esc(graph.groupResearchStatus.label || "Research status") +
        ":</strong> " +
        esc(graph.groupResearchStatus.message) +
        "</div>";
    }
    var aboutStructure =
      '<details class="ocn-about-structure" data-ocn-about-structure="1">' +
      "<summary>About This Structure</summary>" +
      '<div class="ocn-about-structure__body">' +
      "<p>Ownership relationships appear above the hotel. Hotel management and brand affiliation appear below and are not ownership claims.</p>" +
      "<p><strong>Verified</strong> means the relationship is confirmed in the current ownership record. <strong>Proposed</strong> means the relationship is indicated but not yet fully certified — Proposed is never promoted to Verified by the graph layout.</p>" +
      "<p>Connector labels describe relationship type only. Open a node for entity details, evidence, and related contacts.</p>" +
      "</div></details>";
    return (
      '<div class="ocn" data-ocn="1" data-ocn-label-layout="v2" data-ocn-default="' +
      (graph.meta && graph.meta.defaultView ? "1" : "0") +
      '">' +
      '<div class="ocn-toolbar">' +
      '<button type="button" class="ocn-btn" data-ocn-fit>Fit</button>' +
      '<button type="button" class="ocn-btn" data-ocn-reset>Reset</button>' +
      '<button type="button" class="ocn-btn' +
      (opts.brandDevelopmentFilter ? " is-active" : "") +
      '" data-ocn-brand-dev aria-pressed="' +
      (opts.brandDevelopmentFilter ? "true" : "false") +
      '">Brand Development view</button>' +
      "</div>" +
      '<div class="ocn-groups" role="toolbar" aria-label="Relationship groups">' +
      '<button type="button" class="ocn-chip' +
      (!activeGroup ? " is-active" : "") +
      '" data-ocn-group="" aria-pressed="' +
      (!activeGroup ? "true" : "false") +
      '">Default</button>' +
      groupChips +
      "</div>" +
      researchBanner +
      '<ul class="ocn-legend" aria-label="Relationship legend">' +
      legend +
      "</ul>" +
      '<div class="ocn-stage">' +
      '<svg class="ocn-svg" viewBox="0 0 ' +
      L.width +
      " " +
      L.height +
      '" role="img" aria-label="Hotel stakeholder and deal relationship network">' +
      '<g class="ocn-viewport">' +
      edgeSvg +
      nodeSvg +
      "</g></svg>" +
      '<aside class="ocn-detail" data-ocn-detail hidden>' +
      '<button type="button" class="ocn-detail__close" data-ocn-detail-close aria-label="Close detail">&times;</button>' +
      '<div class="ocn-detail__body"></div>' +
      "</aside></div>" +
      aboutStructure +
      (exp.portfolioHidden
        ? '<p class="ocn-bound-note" role="status">' +
          esc(String(exp.portfolioHidden)) +
          " additional related hotels are available — open the sponsor detail panel for Show more.</p>"
        : "") +
      (exp.peopleHidden
        ? '<p class="ocn-bound-note" role="status">' +
          esc(String(exp.peopleHidden)) +
          " additional people are available in Contact Intelligence.</p>"
        : "") +
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

  function detailHtml(node, graph, opts) {
    if (!node) return "";
    opts = opts || {};
    var bits = [];
    bits.push("<h4>" + esc(node.fullName || node.label) + "</h4>");
    bits.push('<p class="ocn-detail__role">' + esc(node.title || node.role) + "</p>");
    if (node.orgName) {
      bits.push("<p><strong>Organization:</strong> " + esc(node.orgName) + "</p>");
    }
    bits.push(
      "<p><strong>Type:</strong> " +
        esc(
          node.kind === "hotel"
            ? "Hotel"
            : node.kind === "person"
              ? "Person"
              : node.kind === "cluster"
                ? "Related hotels cluster"
                : "Organization"
        ) +
        "</p>"
    );
    bits.push(
      "<p><strong>Status:</strong> " + esc(friendlyVerification(node.verification)) + "</p>"
    );
    if (node.note) {
      bits.push('<p class="ocn-detail__hint">' + esc(node.note) + "</p>");
    }
    if (node.kind === "hotel" && node.hpcId) {
      bits.push(
        '<p><button type="button" class="ocn-btn ocn-btn--primary" data-ocn-open-hotel="' +
          esc(node.hpcId) +
          '">Open Hotel Explorer</button></p>'
      );
    } else if (node.kind === "hotel" && node.pendingHpc) {
      bits.push("<p>Pending HPC match — deep link unavailable.</p>");
    }
    if (node.kind === "cluster" && Array.isArray(node.clusterHotels)) {
      bits.push(
        "<p><strong>Related hotels</strong> (" +
          esc(String(node.clusterTotal || node.clusterHotels.length)) +
          ') — verification status and Hotel Explorer links where HPC-matched.</p><ul class="ocn-cluster-list">'
      );
      node.clusterHotels.forEach(function (h) {
        bits.push(
          "<li><strong>" +
            esc(h.name) +
            "</strong> · " +
            esc(friendlyVerification(h.verification)) +
            (h.brand ? " · " + esc(h.brand) : "") +
            (h.location ? " · " + esc(h.location) : "") +
            (h.hpcId
              ? ' · <button type="button" class="ocn-btn ocn-btn--primary" data-ocn-open-hotel="' +
                esc(h.hpcId) +
                '">Open</button>'
              : " · Pending HPC match") +
            "</li>"
        );
      });
      bits.push("</ul>");
    }
    if (node.kind === "person") {
      bits.push(
        "<p><strong>Property responsibility:</strong> " +
          esc(node.propertyResponsibility || "UNKNOWN") +
          " (confirmed vs inferred — never invent signing authority)</p>"
      );
      bits.push(
        '<p><button type="button" class="hiw-navlink ocn-navlink" data-ocn-open-contacts>View Contacts / Surfe actions<span class="hiw-navlink__icon" aria-hidden="true">↗</span></button></p>'
      );
      if (node.linkedIn) {
        bits.push(
          '<p><a class="ocn-ext" href="' +
            esc(node.linkedIn) +
            '" target="_blank" rel="noopener noreferrer">LinkedIn profile</a></p>'
        );
      }
    }
    if (node.role === "Historical lender" || node.group === "capital_financing") {
      if (node.amountUsd) {
        bits.push(
          "<p><strong>Documented amount:</strong> $" +
            esc(Number(node.amountUsd).toLocaleString("en-US")) +
            "</p>"
        );
      }
      if (node.financingDate) {
        bits.push("<p><strong>Approx. date:</strong> " + esc(node.financingDate) + "</p>");
      }
      bits.push(
        "<p><strong>Temporal:</strong> Historical — not proof of the hotel's current lender.</p>"
      );
      (node.evidenceUrls || []).forEach(function (u) {
        bits.push(
          '<p><a class="ocn-ext" href="' + esc(u) + '" target="_blank" rel="noopener noreferrer">Evidence</a></p>'
        );
      });
    }
    if (node.role === "Historical advisor" || (node.group === "development_advisors" && node.verification === "historical")) {
      if (node.effectiveDate) {
        bits.push("<p><strong>Effective:</strong> " + esc(node.effectiveDate) + "</p>");
      }
      bits.push("<p><strong>Temporal:</strong> Historical — not a current decision-maker.</p>");
      (node.evidenceUrls || []).forEach(function (u) {
        bits.push(
          '<p><a class="ocn-ext" href="' + esc(u) + '" target="_blank" rel="noopener noreferrer">Evidence</a></p>'
        );
      });
    }
    if (node.kind === "org" && node.kind !== "cluster") {
      bits.push('<div class="ocn-detail__actions">');
      bits.push(
        '<button type="button" class="ocn-btn" data-ocn-group="asset_people">Asset Management &amp; People</button>'
      );
      bits.push(
        '<button type="button" class="ocn-btn" data-ocn-group="related_hotels">Related Hotels</button>'
      );
      bits.push("</div>");
      bits.push(
        '<p class="ocn-detail__hint">Use relationship-group selectors for Capital, Advisors, and Operations. Historical parties stay historical.</p>'
      );
    }
    var related = (graph.edges || []).filter(function (e) {
      return e.from === node.id || e.to === node.id;
    });
    if (related.length) {
      bits.push("<p><strong>Connections</strong></p><ul>");
      related.forEach(function (e) {
        var otherId = e.from === node.id ? e.to : e.from;
        var other = (graph.nodes || []).filter(function (n) {
          return n.id === otherId;
        })[0];
        bits.push(
          "<li>" +
            esc(e.label) +
            (other ? " → " + esc(other.label) : "") +
            " · " +
            esc(friendlyVerification(e.verification)) +
            "</li>"
        );
      });
      bits.push("</ul>");
    }
    return bits.join("");
  }

  function remount(host, hotel, ownership, opts) {
    return mount(host, hotel, ownership, opts);
  }

  function mount(host, hotel, ownership, opts) {
    if (!host) return null;
    opts = opts || {};
    var graph = buildOwnershipControlNetwork(hotel, ownership, {
      includePeople: opts.includePeople === true,
      includePortfolioPreview: opts.includePortfolioPreview === true,
      includeHistoricalCapital: opts.includeHistoricalCapital === true,
      includeHistoricalAdvisors: opts.includeHistoricalAdvisors === true,
      portfolioAsCluster: opts.portfolioAsCluster !== false,
      activeGroup: opts.activeGroup || null,
      brandDevelopmentFilter: opts.brandDevelopmentFilter === true,
      maxPortfolio: opts.maxPortfolio,
      maxPeople: opts.maxPeople,
    });
    if (graph.groupResearchStatus && graph.groupResearchStatus.message) {
      // Ensure detail panel surfaces research honesty when a group has no edges.
      setTimeout(function () {
        var d = host.querySelector("[data-ocn-detail]");
        var b = host.querySelector(".ocn-detail__body");
        if (!d || !b) return;
        d.hidden = false;
        b.innerHTML =
          "<h4>" +
          esc(graph.groupResearchStatus.label || "Research status") +
          "</h4><p>" +
          esc(graph.groupResearchStatus.message) +
          "</p>";
      }, 0);
    }
    var laid = layout(graph, { width: opts.width || 960, height: opts.height || 520 });
    host.innerHTML = renderShell(graph, laid, opts);
    host._ocn = {
      graph: graph,
      laid: laid,
      hotel: hotel,
      ownership: ownership,
      opts: opts,
      scale: 1,
      tx: 0,
      ty: 0,
    };

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
      detailBody.innerHTML = detailHtml(node, host._ocn.graph, host._ocn.opts);
      host.querySelectorAll(".ocn-node").forEach(function (el) {
        el.classList.toggle("is-selected", el.getAttribute("data-ocn-node") === node.id);
      });
    }

    function showEdgeDetail(edge) {
      if (!detail || !detailBody || !edge) return;
      detail.hidden = false;
      detailBody.innerHTML =
        "<h4>" +
        esc(edge.label) +
        "</h4>" +
        '<p class="ocn-detail__role">' +
        esc(edge.shortLabel || "") +
        "</p>" +
        "<p><strong>Status:</strong> " +
        esc(friendlyVerification(edge.verification)) +
        "</p>" +
        "<p><strong>Temporal:</strong> " +
        esc(edge.temporal || "CURRENT") +
        "</p>" +
        "<p>Selecting an edge reveals precise relationship meaning. Unsupported ownership or financing is never inferred.</p>";
    }

    function withWave(label, run) {
      var banner = document.createElement("div");
      banner.className = "ocn-expand-loading";
      banner.setAttribute("data-ocn-expand-loading", "1");
      if (global.HeLoadingUx && global.HeLoadingUx.inlineHtml) {
        banner.innerHTML = global.HeLoadingUx.inlineHtml(label, {
          subText: "Stakeholder network",
        });
      } else if (global.HeAsyncSection && global.HeAsyncSection.waveCompact) {
        banner.innerHTML = global.HeAsyncSection.waveCompact(label, "Stakeholder network");
      } else {
        banner.textContent = label || "Loading…";
      }
      host.insertBefore(banner, host.firstChild);
      setTimeout(run, 0);
    }

    host.onclick = function (ev) {
      if (ev.target.closest("[data-ocn-detail-close]")) {
        if (detail) detail.hidden = true;
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
        remount(host, hotel, ownership, {
          brandDevelopmentFilter: !!host._ocn.opts.brandDevelopmentFilter,
        });
        return;
      }
      if (ev.target.closest("[data-ocn-brand-dev]")) {
        var next = !host._ocn.opts.brandDevelopmentFilter;
        remount(
          host,
          hotel,
          ownership,
          Object.assign({}, host._ocn.opts, {
            brandDevelopmentFilter: next,
            includePeople: host._ocn.opts.includePeople,
            includePortfolioPreview: host._ocn.opts.includePortfolioPreview,
          })
        );
        return;
      }
      var groupBtn = ev.target.closest("[data-ocn-group]");
      if (groupBtn) {
        var gid = groupBtn.getAttribute("data-ocn-group") || "";
        withWave(gid ? "Loading relationship group…" : "Resetting network…", function () {
          remount(
            host,
            hotel,
            ownership,
            Object.assign({}, host._ocn.opts, {
              activeGroup: gid || null,
              includePeople: gid === "asset_people",
              includePortfolioPreview: gid === "related_hotels",
              includeHistoricalCapital: gid === "capital_financing",
              includeHistoricalAdvisors: gid === "development_advisors",
              brandDevelopmentFilter: false,
            })
          );
        });
        return;
      }
      if (ev.target.closest("[data-ocn-expand-people]")) {
        withWave("Loading key people…", function () {
          remount(
            host,
            hotel,
            ownership,
            Object.assign({}, host._ocn.opts, {
              activeGroup: "asset_people",
              includePeople: true,
            })
          );
        });
        return;
      }
      if (ev.target.closest("[data-ocn-expand-hotels]")) {
        withWave("Loading related hotels…", function () {
          remount(
            host,
            hotel,
            ownership,
            Object.assign({}, host._ocn.opts, {
              activeGroup: "related_hotels",
              includePortfolioPreview: true,
              portfolioAsCluster: true,
            })
          );
        });
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
      var edgeEl = ev.target.closest("[data-ocn-edge]");
      if (edgeEl) {
        var eid = edgeEl.getAttribute("data-ocn-edge");
        var edge = (host._ocn.graph.edges || []).filter(function (e) {
          return e.id === eid;
        })[0];
        if (edge) showEdgeDetail(edge);
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
    };

    if (svg) {
      var dragging = false;
      var lastX = 0;
      var lastY = 0;
      svg.addEventListener("pointerdown", function (ev) {
        if (ev.target.closest("[data-ocn-node],[data-ocn-edge]")) return;
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
          host._ocn.scale = Math.max(0.6, Math.min(1.6, host._ocn.scale * delta));
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
    wrapNodeLabel: wrapNodeLabel,
    measureNodeBox: measureNodeBox,
    MIN_LABEL_PX: MIN_LABEL_PX,
  };
})(typeof window !== "undefined" ? window : globalThis);
