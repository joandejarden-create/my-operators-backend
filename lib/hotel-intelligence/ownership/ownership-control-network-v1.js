/**
 * Ownership & Control Network — typed graph model (v1).
 * Presentation only. Does not invent PropCo→sponsor control edges.
 * Operator/brand edges are never ownership edges.
 */

export const OWNERSHIP_CONTROL_NETWORK_V1 = "ownership-control-network-v1";
export const OCN_MAX_EXPANSION_HOTELS = 12;
export const OCN_MAX_DEPTH = 2;

export const OCN_EDGE_TYPES = Object.freeze({
  LEGALLY_OWNED_BY: "LEGALLY_OWNED_BY",
  ECONOMICALLY_CONTROLLED_BY: "ECONOMICALLY_CONTROLLED_BY",
  MANAGED_BY: "MANAGED_BY",
  BRANDED_AS: "BRANDED_AS",
  ASSOCIATED_PERSON: "ASSOCIATED_PERSON",
  PORTFOLIO_HOTEL: "PORTFOLIO_HOTEL",
});

export const OCN_EDGE_LABELS = Object.freeze({
  LEGALLY_OWNED_BY: "Legally owned by",
  ECONOMICALLY_CONTROLLED_BY: "Economically controlled by",
  MANAGED_BY: "Managed by",
  BRANDED_AS: "Branded as",
  ASSOCIATED_PERSON: "Key person",
  PORTFOLIO_HOTEL: "Associated hotel",
});

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
  if (/OWN|CONTROL|SPONSOR|LEGALLY|ECONOMIC/.test(t)) return "ownership";
  if (/MANAGE|OPERAT/.test(t)) return "operator";
  if (/BRAND|FRANCHIS/.test(t)) return "brand";
  if (/PERSON/.test(t)) return "people";
  if (/PORTFOLIO|HOTEL/.test(t)) return "portfolio";
  return "other";
}

/**
 * Build hotel-centered ownership/control network from HI ownership payload.
 * @param {object} hotel
 * @param {object} ownership
 * @param {{ includePeople?: boolean, includePortfolioPreview?: boolean, maxPortfolio?: number }} [opts]
 */
export function buildOwnershipControlNetwork(hotel, ownership, opts = {}) {
  const includePeople = opts.includePeople !== false;
  const includePortfolioPreview = opts.includePortfolioPreview === true;
  const maxPortfolio = Math.min(
    Number(opts.maxPortfolio) || OCN_MAX_EXPANSION_HOTELS,
    OCN_MAX_EXPANSION_HOTELS
  );

  const ctrl =
    (ownership && ownership.report && ownership.report.ownership_and_control) || {};
  const propco = ctrl.legal_property_owner_propco || ctrl.property_company || {};
  const econ = ctrl.economic_owner_or_group || ctrl.economic_owner || {};
  const ubo = ctrl.ubo_group || {};
  const operator = ctrl.operator || {};
  const brandParty = ctrl.brand || {};

  const hotelId = text(hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || "hotel:focus";
  const hotelName = text(hotel && hotel.name) || "Hotel";
  // Prefer brand affiliation / hotel brand over parent org name on the brand node.
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
    byId.set(node.id, node);
    nodes.push(node);
    return node;
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
      verification: edge.verification || "unresolved",
      temporal: edge.temporal || "CURRENT",
      category: edgeCategoryFromType(edge.type),
      evidenceIds: edge.evidenceIds || [],
    });
  }

  addNode({
    id: `hotel:${hotelId}`,
    kind: "hotel",
    label: hotelName,
    role: "Hotel / Property",
    verification: "verified",
    hpcId: /^rec[a-zA-Z0-9]{14}$/.test(hotelId) ? hotelId : null,
    focus: true,
  });

  const hotelNodeId = `hotel:${hotelId}`;

  if (partyKnown(propco)) {
    const propcoId = `org:propco:${text(propco.entity_id) || partyName(propco)}`;
    addNode({
      id: propcoId,
      kind: "org",
      label: partyName(propco),
      role: "Legal property owner",
      verification: verificationOf(propco),
      orgKey: text(propco.entity_id) || partyName(propco),
      expandKey: text(propco.entity_id) || null,
    });
    addEdge({
      from: hotelNodeId,
      to: propcoId,
      type: OCN_EDGE_TYPES.LEGALLY_OWNED_BY,
      verification: verificationOf(propco),
      temporal: "CURRENT",
    });

    // Control edge only when economic sponsor is separately established (not inferred).
    if (partyKnown(econ) && opts.forceControlEdge !== false) {
      const established =
        econ.known === true ||
        /VERIFIED|HIGH|VALIDATED|PROPOSED|MEDIUM/.test(
          String(econ.preview_state || econ.verification_bucket || econ.status || "").toUpperCase()
        );
      if (established) {
        const econId = `org:econ:${text(econ.entity_id) || partyName(econ)}`;
        addNode({
          id: econId,
          kind: "org",
          label: partyName(econ),
          role: "Economic owner / sponsor",
          verification: verificationOf(econ),
          orgKey: text(econ.entity_id) || partyName(econ),
          expandKey: text(econ.entity_id) || null,
        });
        addEdge({
          from: propcoId,
          to: econId,
          type: OCN_EDGE_TYPES.ECONOMICALLY_CONTROLLED_BY,
          verification: verificationOf(econ),
          temporal: "CURRENT",
        });
      }
    }
  } else if (partyKnown(econ)) {
    // Sponsor known without PropCo — show sponsor linked from hotel as economic control (proposed/verified),
    // never as legal owner.
    const econId = `org:econ:${text(econ.entity_id) || partyName(econ)}`;
    addNode({
      id: econId,
      kind: "org",
      label: partyName(econ),
      role: "Economic owner / sponsor",
      verification: verificationOf(econ),
      orgKey: text(econ.entity_id) || partyName(econ),
      expandKey: text(econ.entity_id) || null,
    });
    addEdge({
      from: hotelNodeId,
      to: econId,
      type: OCN_EDGE_TYPES.ECONOMICALLY_CONTROLLED_BY,
      verification: verificationOf(econ),
      temporal: "CURRENT",
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
    });
    addEdge({
      from: hotelNodeId,
      to: opId,
      type: OCN_EDGE_TYPES.MANAGED_BY,
      verification: verificationOf(operator, "HIGH"),
      temporal: "CURRENT",
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
    });
    addEdge({
      from: hotelNodeId,
      to: brandId,
      type: OCN_EDGE_TYPES.BRANDED_AS,
      verification: verificationOf(brandParty, "HIGH"),
      temporal: "CURRENT",
    });
  }

  if (includePeople) {
    const people =
      (ownership &&
        ownership.report &&
        ownership.report.decision_authority &&
        ownership.report.decision_authority.people) ||
      [];
    people.slice(0, 6).forEach((p, idx) => {
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
        role: text(p.title || p.role) || "Decision-maker",
        verification: /PROPOSED|STAGED/.test(String(p.preview_state || "").toUpperCase())
          ? "proposed"
          : "proposed",
        personKey: text(p.personId || p.person_id) || null,
        linkedIn: text(p.linkedinUrl || (p.professional_profile && p.professional_profile.url)),
      });
      if (anchorOrg) {
        addEdge({
          from: anchorOrg.id,
          to: personId,
          type: OCN_EDGE_TYPES.ASSOCIATED_PERSON,
          verification: "proposed",
          temporal: "CURRENT",
        });
      }
    });
  }

  if (includePortfolioPreview) {
    const owned =
      (ownership && ownership.organization && ownership.organization.owned_controlled_hotels) ||
      [];
    const sponsor = nodes.find((n) => n.role === "Economic owner / sponsor");
    if (sponsor) {
      owned.slice(0, maxPortfolio).forEach((h) => {
        const hid = text(h.airtable_record_id || h.id);
        const hname = text(h.hotel_name || h.name);
        if (!hname) return;
        const nid = hid ? `hotel:${hid}` : `hotel:portfolio:${hname}`;
        if (nid === hotelNodeId) return;
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
          role: "Portfolio hotel",
          verification: ver,
          hpcId: hid && /^rec[a-zA-Z0-9]{14}$/.test(hid) ? hid : null,
          pendingHpc: !hid,
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

  const summary = {
    legalOwner: partyKnown(propco) ? partyName(propco) : "Not yet verified",
    legalOwnerVerification: partyKnown(propco) ? verificationOf(propco) : "unresolved",
    economicSponsor: partyKnown(econ) ? partyName(econ) : "Not yet verified",
    economicSponsorVerification: partyKnown(econ) ? verificationOf(econ) : "unresolved",
    operator: operatorName || "Not yet verified",
    brand: brandName || "—",
    ubo:
      partyKnown(ubo) && ubo.status !== "UNRESOLVED"
        ? partyName(ubo)
        : "Unresolved",
    ownershipStatus: text(ctrl.control_status) || "PARTIAL",
  };

  return {
    version: OWNERSHIP_CONTROL_NETWORK_V1,
    focusHotelId: hotelNodeId,
    nodes,
    edges,
    summary,
    legend: [
      { key: "verified", label: "Verified current" },
      { key: "proposed", label: "Proposed" },
      { key: "historical", label: "Historical" },
      { key: "unresolved", label: "Unresolved" },
    ],
    limits: {
      maxExpansionHotels: OCN_MAX_EXPANSION_HOTELS,
      maxDepth: OCN_MAX_DEPTH,
    },
    meta: {
      proposedMode:
        ownership?.preview_mode === "PHASE3_PROPOSED" ||
        ownership?.report?.research_status?.preview_mode === "PHASE3_PROPOSED",
    },
  };
}

export function layoutOwnershipControlNetwork(graph, opts = {}) {
  const width = Number(opts.width) || 920;
  const height = Number(opts.height) || 520;
  const nodeW = 168;
  const nodeH = 64;
  const nodes = graph.nodes || [];
  const edges = graph.edges || [];
  const focus = nodes.find((n) => n.focus) || nodes[0];
  const positions = {};

  const econ = nodes.find((n) => n.role === "Economic owner / sponsor");
  const propco = nodes.find((n) => n.role === "Legal property owner");
  const operator = nodes.find((n) => n.role === "Hotel operator");
  const brand = nodes.find((n) => n.role === "Brand");
  const people = nodes.filter((n) => n.kind === "person");
  const portfolio = nodes.filter((n) => n.role === "Portfolio hotel");

  const cx = width / 2;
  if (focus) positions[focus.id] = { x: cx, y: 250 };
  if (propco) positions[propco.id] = { x: cx, y: 140 };
  if (econ) positions[econ.id] = { x: cx, y: 40 };
  if (operator) positions[operator.id] = { x: cx - 220, y: 360 };
  if (brand) positions[brand.id] = { x: cx + 220, y: 360 };

  people.forEach((p, i) => {
    const baseX = econ ? cx + 240 : cx + 240;
    const baseY = econ ? 40 : 140;
    positions[p.id] = { x: baseX, y: baseY + i * 78 };
  });

  portfolio.forEach((h, i) => {
    const col = i % 4;
    const row = Math.floor(i / 4);
    positions[h.id] = {
      x: 80 + col * 200,
      y: 460 + row * 78,
    };
  });

  // Place any leftover nodes
  let leftover = 0;
  nodes.forEach((n) => {
    if (!positions[n.id]) {
      positions[n.id] = { x: 80 + (leftover % 4) * 200, y: 460 + Math.floor(leftover / 4) * 78 };
      leftover += 1;
    }
  });

  return {
    width,
    height: Math.max(height, portfolio.length ? 460 + Math.ceil(portfolio.length / 4) * 78 + 40 : height),
    nodeW,
    nodeH,
    positions,
    nodes,
    edges,
  };
}

export function friendlyVerificationLabel(v) {
  return friendlyVerification(v);
}
