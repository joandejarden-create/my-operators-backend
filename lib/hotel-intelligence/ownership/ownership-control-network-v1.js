/**
 * Hotel Stakeholder & Deal Relationship Network — typed graph model (v1.1).
 * Presentation only. Does not invent PropCo→sponsor control, financing terms, or brand availability.
 * Operator/brand edges are never ownership edges.
 */

export const OWNERSHIP_CONTROL_NETWORK_V1 = "ownership-control-network-v1";
export const OCN_MAX_EXPANSION_HOTELS = 8;
export const OCN_MAX_VISIBLE_PEOPLE = 5;
export const OCN_MAX_DEPTH = 2;
export const OCN_MIN_NODE_LABEL_PX = 13;

export const OCN_EDGE_TYPES = Object.freeze({
  LEGALLY_OWNED_BY: "LEGALLY_OWNED_BY",
  ECONOMICALLY_CONTROLLED_BY: "ECONOMICALLY_CONTROLLED_BY",
  ULTIMATE_CONTROL: "ULTIMATE_CONTROL",
  MANAGED_BY: "MANAGED_BY",
  BRANDED_AS: "BRANDED_AS",
  ASSOCIATED_PERSON: "ASSOCIATED_PERSON",
  PORTFOLIO_HOTEL: "PORTFOLIO_HOTEL",
  ASSET_MANAGED_BY: "ASSET_MANAGED_BY",
  FINANCED_BY: "FINANCED_BY",
  DEVELOPED_BY: "DEVELOPED_BY",
  ADVISED_BY: "ADVISED_BY",
  EQUITY_PARTNER: "EQUITY_PARTNER",
});

export const OCN_EDGE_LABELS = Object.freeze({
  LEGALLY_OWNED_BY: "Legally owned by",
  ECONOMICALLY_CONTROLLED_BY: "Economically controlled by",
  ULTIMATE_CONTROL: "Ultimate control",
  MANAGED_BY: "Managed by",
  BRANDED_AS: "Branded as",
  ASSOCIATED_PERSON: "Key person",
  PORTFOLIO_HOTEL: "Related hotel",
  ASSET_MANAGED_BY: "Asset managed by",
  FINANCED_BY: "Financed by",
  DEVELOPED_BY: "Developed by",
  ADVISED_BY: "Advised by",
  EQUITY_PARTNER: "Equity partner",
});

export const OCN_SHORT_EDGE_LABELS = Object.freeze({
  LEGALLY_OWNED_BY: "Legal",
  ECONOMICALLY_CONTROLLED_BY: "Economic",
  ULTIMATE_CONTROL: "UBO",
  MANAGED_BY: "Managed",
  BRANDED_AS: "Brand",
  ASSOCIATED_PERSON: "Person",
  PORTFOLIO_HOTEL: "Hotel",
  ASSET_MANAGED_BY: "Asset mgmt",
  FINANCED_BY: "Financing",
  DEVELOPED_BY: "Developer",
  ADVISED_BY: "Advisor",
  EQUITY_PARTNER: "Equity",
});

export const OCN_STAKEHOLDER_GROUPS = Object.freeze([
  {
    id: "ownership_control",
    label: "Ownership & Control",
    edgeTypes: ["LEGALLY_OWNED_BY", "ECONOMICALLY_CONTROLLED_BY", "ULTIMATE_CONTROL", "EQUITY_PARTNER"],
  },
  {
    id: "operations_brand",
    label: "Operations & Brand",
    edgeTypes: ["MANAGED_BY", "BRANDED_AS"],
  },
  {
    id: "capital_financing",
    label: "Capital & Financing",
    edgeTypes: ["FINANCED_BY", "EQUITY_PARTNER"],
  },
  {
    id: "asset_people",
    label: "Asset Management & Key People",
    edgeTypes: ["ASSET_MANAGED_BY", "ASSOCIATED_PERSON"],
  },
  {
    id: "development_advisors",
    label: "Development & Advisors",
    edgeTypes: ["DEVELOPED_BY", "ADVISED_BY"],
  },
  {
    id: "related_hotels",
    label: "Related Hotels & Portfolios",
    edgeTypes: ["PORTFOLIO_HOTEL"],
  },
]);

export const OCN_BRAND_DEV_EDGE_TYPES = Object.freeze([
  "LEGALLY_OWNED_BY",
  "ECONOMICALLY_CONTROLLED_BY",
  "ULTIMATE_CONTROL",
  "MANAGED_BY",
  "BRANDED_AS",
  "ASSET_MANAGED_BY",
  "ASSOCIATED_PERSON",
  "DEVELOPED_BY",
  "EQUITY_PARTNER",
  "FINANCED_BY",
]);

function text(v) {
  const s = String(v == null ? "" : v).trim();
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
  const raw = String(
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

function edgeCategoryFromType(type) {
  const t = String(type || "").toUpperCase();
  if (/OWN|CONTROL|SPONSOR|LEGALLY|ECONOMIC|ULTIMATE|EQUITY/.test(t)) return "ownership";
  if (/MANAGE|OPERAT/.test(t) && !/ASSET/.test(t)) return "operator";
  if (/BRAND|FRANCHIS/.test(t)) return "brand";
  if (/PERSON|ASSET_MANAGED/.test(t)) return "people";
  if (/PORTFOLIO|HOTEL/.test(t)) return "portfolio";
  if (/FINANC|LEND/.test(t)) return "capital";
  if (/DEVELOP|ADVIS/.test(t)) return "development";
  return "other";
}

export function shortEdgeLabel(type) {
  return OCN_SHORT_EDGE_LABELS[type] || OCN_EDGE_LABELS[type] || String(type || "");
}

/** Word-wrap for SVG tspans — never truncate mid-word into unreadability. */
export function wrapNodeLabel(label, maxChars = 22, maxLines = 3) {
  const raw = String(label || "").trim();
  if (!raw) return [""];
  const words = raw.split(/\s+/);
  const lines = [];
  let cur = "";
  for (const w of words) {
    const next = cur ? `${cur} ${w}` : w;
    if (next.length <= maxChars) {
      cur = next;
    } else {
      if (cur) lines.push(cur);
      cur = w.length > maxChars ? `${w.slice(0, maxChars - 1)}…` : w;
      if (lines.length >= maxLines - 1) break;
    }
  }
  if (cur && lines.length < maxLines) lines.push(cur);
  const used = lines.join(" ").replace(/…$/, "");
  if (raw.length > used.replace(/…/g, "").length && !lines[lines.length - 1].endsWith("…")) {
    lines[lines.length - 1] = `${lines[lines.length - 1].replace(/\s+\S*$/, "")}…`.replace(/^…$/, "…");
  }
  return lines.length ? lines : [raw.slice(0, maxChars)];
}

export function measureNodeBox(node, opts = {}) {
  const focus = node && node.focus;
  const width = focus ? Number(opts.focusWidth) || 248 : Number(opts.nodeWidth) || 212;
  const lines = wrapNodeLabel(node && node.label, focus ? 26 : 22, 3);
  const lineH = 16;
  const padY = 14;
  const roleH = 14;
  const metaH = 14;
  const height = padY + roleH + lines.length * lineH + metaH + 10;
  return {
    width,
    height: Math.max(focus ? 78 : 70, height),
    labelLines: lines,
  };
}

/**
 * Build hotel-centered stakeholder network from HI ownership payload.
 * Default view: hotel + legal + economic + operator + brand (+ UBO when known).
 * People and portfolio require explicit expansion flags.
 */
export function buildOwnershipControlNetwork(hotel, ownership, opts = {}) {
  const includePeople = opts.includePeople === true;
  const includePortfolioPreview = opts.includePortfolioPreview === true;
  const maxPortfolio = Math.min(
    Number(opts.maxPortfolio) || OCN_MAX_EXPANSION_HOTELS,
    OCN_MAX_EXPANSION_HOTELS
  );
  const maxPeople = Math.min(
    Number(opts.maxPeople) || OCN_MAX_VISIBLE_PEOPLE,
    OCN_MAX_VISIBLE_PEOPLE
  );
  const brandDevFilter = opts.brandDevelopmentFilter === true;

  const ctrl =
    (ownership && ownership.report && ownership.report.ownership_and_control) || {};
  const propco = ctrl.legal_property_owner_propco || ctrl.property_company || {};
  const econ = ctrl.economic_owner_or_group || ctrl.economic_owner || {};
  const ubo = ctrl.ubo_group || {};
  const operator = ctrl.operator || {};
  const brandParty = ctrl.brand || {};
  const assetMgr = ctrl.asset_manager || ctrl.asset_management_company || {};
  const lender = ctrl.lender || ctrl.financing_institution || {};
  const developer = ctrl.developer || {};
  const advisor = ctrl.advisor || ctrl.broker || {};

  const hotelId = text(hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || "hotel:focus";
  const hotelName = text(hotel && hotel.name) || "Hotel";
  const brandName =
    text(ownership && ownership.hotel && ownership.hotel.brand_display) ||
    text(hotel && hotel.brand) ||
    text(brandParty.name) ||
    null;
  const operatorName =
    text(operator.name) || text(hotel && hotel.managementCompany) || null;

  const nodes = [];
  const edges = [];
  const byId = new Map();

  function addNode(node) {
    if (!node || !node.id) return null;
    if (byId.has(node.id)) return byId.get(node.id);
    const box = measureNodeBox(node);
    const enriched = {
      ...node,
      displayLabel: node.label,
      labelLines: box.labelLines,
      boxW: box.width,
      boxH: box.height,
      fullName: node.fullName || node.label,
    };
    byId.set(node.id, enriched);
    nodes.push(enriched);
    return enriched;
  }

  function addEdge(edge) {
    if (!edge || !edge.from || !edge.to || !edge.type) return;
    const id = edge.id || `${edge.from}|${edge.type}|${edge.to}`;
    if (edges.some((e) => e.id === id)) return;
    edges.push({
      id,
      from: edge.from,
      to: edge.to,
      type: edge.type,
      label: edge.label || OCN_EDGE_LABELS[edge.type] || edge.type,
      shortLabel: edge.shortLabel || shortEdgeLabel(edge.type),
      verification: edge.verification || "unresolved",
      temporal: edge.temporal || "CURRENT",
      category: edgeCategoryFromType(edge.type),
      evidenceIds: edge.evidenceIds || [],
    });
  }

  const hotelNodeId = `hotel:${hotelId}`;
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

  let propcoId = null;
  let econId = null;

  if (partyKnown(propco)) {
    propcoId = `org:propco:${text(propco.entity_id) || partyName(propco)}`;
    addNode({
      id: propcoId,
      kind: "org",
      label: partyName(propco),
      fullName: text(propco.legal_name) || partyName(propco),
      role: "Legal property owner",
      verification: verificationOf(propco),
      orgKey: text(propco.entity_id) || partyName(propco),
      expandKey: text(propco.entity_id) || null,
      group: "ownership_control",
    });
    addEdge({
      from: hotelNodeId,
      to: propcoId,
      type: OCN_EDGE_TYPES.LEGALLY_OWNED_BY,
      verification: verificationOf(propco),
    });
  }

  if (partyKnown(econ)) {
    econId = `org:econ:${text(econ.entity_id) || partyName(econ)}`;
    addNode({
      id: econId,
      kind: "org",
      label: partyName(econ),
      fullName: text(econ.legal_name) || partyName(econ),
      role: "Economic owner / sponsor",
      verification: verificationOf(econ),
      orgKey: text(econ.entity_id) || partyName(econ),
      expandKey: text(econ.entity_id) || null,
      group: "ownership_control",
    });
    if (propcoId && opts.forceControlEdge !== false) {
      addEdge({
        from: propcoId,
        to: econId,
        type: OCN_EDGE_TYPES.ECONOMICALLY_CONTROLLED_BY,
        verification: verificationOf(econ),
      });
    } else {
      addEdge({
        from: hotelNodeId,
        to: econId,
        type: OCN_EDGE_TYPES.ECONOMICALLY_CONTROLLED_BY,
        verification: verificationOf(econ),
      });
    }
  }

  // UBO only when a named party is present — never invent.
  if (partyKnown(ubo) && String(ubo.status || "").toUpperCase() !== "UNRESOLVED") {
    const uboId = `org:ubo:${text(ubo.entity_id) || partyName(ubo)}`;
    addNode({
      id: uboId,
      kind: "org",
      label: partyName(ubo),
      role: "Ultimate beneficial owner",
      verification: verificationOf(ubo),
      group: "ownership_control",
    });
    const anchor = econId || propcoId || hotelNodeId;
    addEdge({
      from: anchor,
      to: uboId,
      type: OCN_EDGE_TYPES.ULTIMATE_CONTROL,
      verification: verificationOf(ubo),
    });
  }

  if (operatorName) {
    const opId = `org:operator:${text(operator.entity_id) || operatorName}`;
    addNode({
      id: opId,
      kind: "org",
      label: operatorName,
      role: "Hotel operator",
      verification: verificationOf(operator, "HIGH"),
      orgKey: text(operator.entity_id) || operatorName,
      expandKey: text(operator.entity_id) || null,
      group: "operations_brand",
    });
    addEdge({
      from: hotelNodeId,
      to: opId,
      type: OCN_EDGE_TYPES.MANAGED_BY,
      verification: verificationOf(operator, "HIGH"),
    });
  }

  if (brandName) {
    const brandId = `org:brand:${brandName}`;
    addNode({
      id: brandId,
      kind: "org",
      label: brandName,
      role: "Brand",
      verification: verificationOf(brandParty, "HIGH"),
      orgKey: brandName,
      group: "operations_brand",
    });
    addEdge({
      from: hotelNodeId,
      to: brandId,
      type: OCN_EDGE_TYPES.BRANDED_AS,
      verification: verificationOf(brandParty, "HIGH"),
    });
  }

  // Optional evidence-backed extended stakeholders — only when present (never invent).
  if (partyKnown(assetMgr)) {
    const amId = `org:asset:${text(assetMgr.entity_id) || partyName(assetMgr)}`;
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
      type: OCN_EDGE_TYPES.ASSET_MANAGED_BY,
      verification: verificationOf(assetMgr),
    });
  }
  if (partyKnown(lender)) {
    const lid = `org:lender:${text(lender.entity_id) || partyName(lender)}`;
    addNode({
      id: lid,
      kind: "org",
      label: partyName(lender),
      role: "Lender / financing",
      verification: verificationOf(lender),
      group: "capital_financing",
    });
    addEdge({
      from: hotelNodeId,
      to: lid,
      type: OCN_EDGE_TYPES.FINANCED_BY,
      verification: verificationOf(lender),
    });
  }
  if (partyKnown(developer)) {
    const did = `org:dev:${text(developer.entity_id) || partyName(developer)}`;
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
      type: OCN_EDGE_TYPES.DEVELOPED_BY,
      verification: verificationOf(developer),
    });
  }
  if (partyKnown(advisor)) {
    const aid = `org:adv:${text(advisor.entity_id) || partyName(advisor)}`;
    addNode({
      id: aid,
      kind: "org",
      label: partyName(advisor),
      role: "Advisor / broker",
      verification: verificationOf(advisor),
      group: "development_advisors",
    });
    addEdge({
      from: hotelNodeId,
      to: aid,
      type: OCN_EDGE_TYPES.ADVISED_BY,
      verification: verificationOf(advisor),
    });
  }

  let peopleHidden = 0;
  if (includePeople) {
    const people =
      (ownership &&
        ownership.report &&
        ownership.report.decision_authority &&
        ownership.report.decision_authority.people) ||
      [];
    const slice = people.slice(0, maxPeople);
    peopleHidden = Math.max(0, people.length - slice.length);
    slice.forEach((p, idx) => {
      const name = text(p.display_name || p.name);
      if (!name) return;
      const personId = `person:${text(p.personId || p.person_id || p.entity_id) || idx}:${name}`;
      const anchorOrg =
        nodes.find((n) => n.role === "Economic owner / sponsor") ||
        nodes.find((n) => n.role === "Legal property owner") ||
        null;
      addNode({
        id: personId,
        kind: "person",
        label: name,
        role: "Key person",
        title: text(p.title || p.role) || "Decision-maker",
        verification: "proposed",
        personKey: text(p.personId || p.person_id) || null,
        linkedIn: text(p.linkedinUrl || (p.professional_profile && p.professional_profile.url)),
        group: "asset_people",
      });
      if (anchorOrg) {
        addEdge({
          from: anchorOrg.id,
          to: personId,
          type: OCN_EDGE_TYPES.ASSOCIATED_PERSON,
          verification: "proposed",
        });
      }
    });
  }

  let portfolioHidden = 0;
  if (includePortfolioPreview) {
    const owned =
      (ownership && ownership.organization && ownership.organization.owned_controlled_hotels) ||
      [];
    const sponsor = nodes.find((n) => n.role === "Economic owner / sponsor");
    if (sponsor) {
      const eligible = owned.filter((h) => {
        const hname = text(h.hotel_name || h.name);
        const hid = text(h.airtable_record_id || h.id);
        if (!hname) return false;
        const nid = hid ? `hotel:${hid}` : `hotel:portfolio:${hname}`;
        return nid !== hotelNodeId;
      });
      const slice = eligible.slice(0, maxPortfolio);
      portfolioHidden = Math.max(0, eligible.length - slice.length);
      slice.forEach((h) => {
        const hid = text(h.airtable_record_id || h.id);
        const hname = text(h.hotel_name || h.name);
        const nid = hid ? `hotel:${hid}` : `hotel:portfolio:${hname}`;
        const bucket = String(h.verification_bucket || h.ownership_evidence_class || "").toUpperCase();
        const ver = /VERIFIED/.test(bucket)
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
          role: "Related hotel",
          verification: ver,
          hpcId: hid && /^rec[a-zA-Z0-9]{14}$/.test(hid) ? hid : null,
          pendingHpc: !hid,
          group: "related_hotels",
        });
        addEdge({
          from: sponsor.id,
          to: nid,
          type: OCN_EDGE_TYPES.PORTFOLIO_HOTEL,
          verification: ver,
          temporal: /HISTOR/.test(String(h.current_historical || "")) ? "HISTORICAL" : "CURRENT",
        });
      });
    }
  }

  const unresolvedGroups = [];
  if (!partyKnown(lender)) {
    unresolvedGroups.push({
      id: "capital_financing",
      label: "Capital & Financing",
      status: "unresolved",
      note: "No verified lender or financing institution linked to this hotel yet.",
    });
  }
  if (!partyKnown(developer) && !partyKnown(advisor)) {
    unresolvedGroups.push({
      id: "development_advisors",
      label: "Development & Advisors",
      status: "unresolved",
      note: "No verified developer or advisor relationship in the current packet.",
    });
  }
  if (!partyKnown(assetMgr) && !includePeople) {
    unresolvedGroups.push({
      id: "asset_people",
      label: "Asset Management & Key People",
      status: "collapsed",
      note: "Explore Key People from the economic owner or PropCo node.",
    });
  }

  let outNodes = nodes;
  let outEdges = edges;
  if (brandDevFilter) {
    const allowed = new Set(OCN_BRAND_DEV_EDGE_TYPES);
    outEdges = edges.filter((e) => allowed.has(e.type) || e.from === hotelNodeId || e.to === hotelNodeId);
    const keep = new Set([hotelNodeId]);
    outEdges.forEach((e) => {
      keep.add(e.from);
      keep.add(e.to);
    });
    // Always keep core default entities
    nodes.forEach((n) => {
      if (n.focus || n.group === "ownership_control" || n.group === "operations_brand") keep.add(n.id);
    });
    outNodes = nodes.filter((n) => keep.has(n.id));
    outEdges = outEdges.filter((e) => keep.has(e.from) && keep.has(e.to));
  }

  const summary = {
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
  };

  return {
    version: OWNERSHIP_CONTROL_NETWORK_V1,
    networkTitle: "Hotel Stakeholder & Deal Relationship Network",
    focusHotelId: hotelNodeId,
    nodes: outNodes,
    edges: outEdges,
    summary,
    stakeholderGroups: OCN_STAKEHOLDER_GROUPS,
    unresolvedGroups,
    expansion: {
      peopleVisible: includePeople,
      peopleHidden,
      portfolioVisible: includePortfolioPreview,
      portfolioHidden,
      maxPeople,
      maxPortfolio,
    },
    legend: [
      { key: "verified", label: "Verified current" },
      { key: "proposed", label: "Proposed" },
      { key: "historical", label: "Historical" },
      { key: "unresolved", label: "Unresolved" },
    ],
    limits: {
      maxExpansionHotels: OCN_MAX_EXPANSION_HOTELS,
      maxVisiblePeople: OCN_MAX_VISIBLE_PEOPLE,
      maxDepth: OCN_MAX_DEPTH,
      minLabelPx: OCN_MIN_NODE_LABEL_PX,
    },
    meta: {
      proposedMode:
        ownership?.preview_mode === "PHASE3_PROPOSED" ||
        ownership?.report?.research_status?.preview_mode === "PHASE3_PROPOSED",
      brandDevelopmentFilter: brandDevFilter,
      defaultView: !includePeople && !includePortfolioPreview,
    },
  };
}

/**
 * Layered hotel-centered layout with content-measured boxes and cluster lanes.
 * Ownership above · Hotel center · Operations/Brand below · expansions to the side/below.
 */
export function layoutOwnershipControlNetwork(graph, opts = {}) {
  const width = Number(opts.width) || 960;
  const nodes = (graph.nodes || []).map((n) => {
    const box = measureNodeBox(n);
    return { ...n, boxW: n.boxW || box.width, boxH: n.boxH || box.height, labelLines: n.labelLines || box.labelLines };
  });
  const edges = graph.edges || [];
  const positions = {};
  const boxes = {};
  nodes.forEach((n) => {
    boxes[n.id] = { w: n.boxW, h: n.boxH };
  });

  const focus = nodes.find((n) => n.focus) || nodes[0];
  const econ = nodes.find((n) => n.role === "Economic owner / sponsor");
  const propco = nodes.find((n) => n.role === "Legal property owner");
  const ubo = nodes.find((n) => n.role === "Ultimate beneficial owner");
  const operator = nodes.find((n) => n.role === "Hotel operator");
  const brand = nodes.find((n) => n.role === "Brand");
  const capital = nodes.filter((n) => n.group === "capital_financing");
  const development = nodes.filter((n) => n.group === "development_advisors");
  const assetOrgs = nodes.filter(
    (n) => n.group === "asset_people" && n.kind === "org"
  );
  const people = nodes.filter((n) => n.kind === "person");
  const portfolio = nodes.filter((n) => n.role === "Related hotel" || n.role === "Portfolio hotel");

  const cx = width / 2;
  const gapY = 36;
  let y = 28;

  // Ownership cluster (vertical chain when both PropCo + Econ; UBO above econ)
  if (ubo) {
    positions[ubo.id] = { x: cx, y: y + boxes[ubo.id].h / 2 };
    y += boxes[ubo.id].h + gapY;
  }
  if (econ) {
    positions[econ.id] = { x: cx, y: y + boxes[econ.id].h / 2 };
    y += boxes[econ.id].h + gapY;
  }
  if (propco) {
    positions[propco.id] = { x: cx, y: y + boxes[propco.id].h / 2 };
    y += boxes[propco.id].h + gapY;
  }

  // Focus hotel
  if (focus) {
    y += 8;
    positions[focus.id] = { x: cx, y: y + boxes[focus.id].h / 2 };
    y += boxes[focus.id].h + gapY + 12;
  }

  // Operations & Brand lane — separate from ownership, no cross-link implication
  const opsY = y + Math.max(boxes[operator?.id]?.h || 70, boxes[brand?.id]?.h || 70) / 2;
  if (operator) positions[operator.id] = { x: cx - 230, y: opsY };
  if (brand) positions[brand.id] = { x: cx + 230, y: opsY };
  if (operator || brand) {
    y = opsY + Math.max(boxes[operator?.id]?.h || 0, boxes[brand?.id]?.h || 0) / 2 + gapY;
  }

  // Extended orgs (asset / capital / development) in a lower row when present
  const extraOrgs = [...assetOrgs, ...capital, ...development];
  if (extraOrgs.length) {
    const span = Math.min(extraOrgs.length, 4);
    const startX = cx - ((span - 1) * 220) / 2;
    extraOrgs.slice(0, 4).forEach((n, i) => {
      positions[n.id] = { x: startX + i * 220, y: y + boxes[n.id].h / 2 };
    });
    y += Math.max(...extraOrgs.slice(0, 4).map((n) => boxes[n.id].h)) + gapY;
  }

  // People: right of economic/propco anchor
  const peopleAnchor = econ || propco || focus;
  const peopleBaseX = (peopleAnchor ? positions[peopleAnchor.id].x : cx) + 270;
  const peopleBaseY = peopleAnchor ? positions[peopleAnchor.id].y - ((people.length - 1) * 88) / 2 : y;
  people.forEach((p, i) => {
    positions[p.id] = {
      x: Math.min(width - 120, peopleBaseX),
      y: peopleBaseY + i * 88,
    };
  });

  // Related hotels grid below
  if (portfolio.length) {
    const cols = Math.min(3, portfolio.length);
    const gridTop = Math.max(y, ...(people.map((p) => positions[p.id].y + boxes[p.id].h / 2 + 24)));
    portfolio.forEach((h, i) => {
      const col = i % cols;
      const row = Math.floor(i / cols);
      const gridWidth = cols * 220;
      const left = cx - gridWidth / 2 + 110;
      positions[h.id] = {
        x: left + col * 220,
        y: gridTop + row * 96 + boxes[h.id].h / 2,
      };
    });
    y =
      gridTop +
      Math.ceil(portfolio.length / cols) * 96 +
      24;
  }

  // Leftovers
  let leftover = 0;
  nodes.forEach((n) => {
    if (!positions[n.id]) {
      positions[n.id] = {
        x: 120 + (leftover % 3) * 220,
        y: y + Math.floor(leftover / 3) * 96,
      };
      leftover += 1;
    }
  });

  const maxY = Math.max(
    420,
    ...nodes.map((n) => (positions[n.id]?.y || 0) + (boxes[n.id]?.h || 70) / 2 + 40)
  );

  // Collision nudge: ensure horizontal separation for same-row nodes
  const sorted = [...nodes].sort(
    (a, b) => (positions[a.id].y || 0) - (positions[b.id].y || 0)
  );
  for (let i = 0; i < sorted.length; i++) {
    for (let j = i + 1; j < sorted.length; j++) {
      const a = sorted[i];
      const b = sorted[j];
      const dy = Math.abs(positions[a.id].y - positions[b.id].y);
      if (dy > 40) continue;
      const minDx = (boxes[a.id].w + boxes[b.id].w) / 2 + 28;
      const dx = positions[b.id].x - positions[a.id].x;
      if (Math.abs(dx) < minDx) {
        const push = (minDx - Math.abs(dx)) / 2 + 2;
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

  return {
    width,
    height: Math.max(Number(opts.height) || 520, maxY),
    positions,
    boxes,
    nodes,
    edges,
    lanes: {
      ownershipY: econ || propco ? (positions[propco?.id || econ.id]?.y ?? null) : null,
      hotelY: focus ? positions[focus.id].y : null,
      opsY: operator || brand ? opsY : null,
    },
  };
}

export function friendlyVerificationLabel(v) {
  return friendlyVerification(v);
}

export function filterGraphForBrandDevelopment(graph) {
  return buildOwnershipControlNetwork(
    { id: graph?.focusHotelId?.replace(/^hotel:/, ""), name: "Hotel" },
    {},
    { brandDevelopmentFilter: true }
  );
}
