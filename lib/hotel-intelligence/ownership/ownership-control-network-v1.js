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
});

/** Compact fallbacks only when connector gap cannot fit the full customer label. */
export const OCN_SHORT_EDGE_LABELS = Object.freeze({
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
});

/** Vertical gap between primary ownership / ops chain nodes (label chip clearance). */
export const OCN_PRIMARY_CHAIN_GAP_Y = 72;

export const OCN_STAKEHOLDER_GROUPS = Object.freeze([
  {
    id: "ownership_control",
    label: "Ownership & Control",
    edgeTypes: ["LEGALLY_OWNED_BY", "ECONOMICALLY_CONTROLLED_BY", "ULTIMATE_CONTROL", "EQUITY_PARTNER"],
  },
  {
    id: "asset_people",
    label: "Asset Management & People",
    edgeTypes: ["ASSET_MANAGED_BY", "ASSOCIATED_PERSON"],
  },
  {
    id: "capital_financing",
    label: "Capital & Financing",
    edgeTypes: ["FINANCED_BY", "EQUITY_PARTNER"],
  },
  {
    id: "operations_brand",
    label: "Operations & Brand",
    edgeTypes: ["MANAGED_BY", "BRANDED_AS"],
  },
  {
    id: "development_advisors",
    label: "Development & Advisors",
    edgeTypes: ["DEVELOPED_BY", "ADVISED_BY"],
  },
  {
    id: "related_hotels",
    label: "Related Hotels",
    edgeTypes: ["PORTFOLIO_HOTEL"],
  },
]);

/** Separate research completeness from graph rendering completeness. */
export function assessStakeholderResearchCoverage(ownership = {}) {
  const ctrl =
    (ownership && ownership.report && ownership.report.ownership_and_control) || {};
  const covered = ctrl.stakeholder_research_coverage || {};
  const categories = [
    "legal_ownership",
    "economic_ownership",
    "asset_management",
    "financing",
    "equity_jv",
    "operator",
    "brand",
    "development",
    "advisors",
    "key_people",
    "related_hotels",
  ];
  const rows = categories.map((id) => ({
    id,
    status: text(covered[id]) || "NOT_ASSESSED",
  }));
  return {
    version: "stakeholder-research-coverage-v1",
    rows,
    matrixReport: text(covered.matrix_report) || null,
    note: "Research coverage ≠ visualization coverage",
  };
}

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
  const activeGroup = text(opts.activeGroup) || null;
  const includePeople =
    opts.includePeople === true || activeGroup === "asset_people";
  const includePortfolioPreview =
    opts.includePortfolioPreview === true || activeGroup === "related_hotels";
  const includeHistoricalCapital =
    opts.includeHistoricalCapital === true || activeGroup === "capital_financing";
  const includeHistoricalAdvisors =
    opts.includeHistoricalAdvisors === true ||
    activeGroup === "development_advisors";
  const portfolioAsCluster = opts.portfolioAsCluster !== false;
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
    const lenderVer = verificationOf(lender);
    const lenderHistorical = lenderVer === "historical" || lender.current === false;
    if (!lenderHistorical || includeHistoricalCapital) {
      const lid = `org:lender:${text(lender.entity_id) || partyName(lender)}`;
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
        type: OCN_EDGE_TYPES.FINANCED_BY,
        verification: lenderVer,
        temporal: lenderHistorical ? "HISTORICAL" : "CURRENT",
      });
    }
  }
  if (partyKnown(developer) && String(developer.status || "").toUpperCase() !== "NOT_APPLICABLE") {
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
    const advVer = verificationOf(advisor);
    const advHistorical = advVer === "historical" || advisor.current === false;
    if (!advHistorical || includeHistoricalAdvisors) {
      const aid = `org:adv:${text(advisor.entity_id) || partyName(advisor)}`;
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
        type: OCN_EDGE_TYPES.ADVISED_BY,
        verification: advVer,
        temporal: advHistorical ? "HISTORICAL" : "CURRENT",
      });
    }
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
      const title = text(p.title || p.role) || "Relevant contact";
      const orgName =
        text(p.organization || p.company || p.org) ||
        partyName(econ) ||
        "Sponsor organization";
      const propResp = text(p.propertyResponsibility || p.property_responsibility) || "UNKNOWN";
      const anchorOrg =
        nodes.find((n) => n.role === "Economic owner / sponsor") ||
        nodes.find((n) => n.role === "Legal property owner") ||
        null;
      addNode({
        id: personId,
        kind: "person",
        label: name,
        role: title,
        title,
        orgName,
        propertyResponsibility: propResp,
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
          shortLabel: title.length > 18 ? `${title.slice(0, 16)}…` : title,
          label: `${title} at ${orgName}`,
        });
      }
    });
  }

  let portfolioHidden = 0;
  let portfolioCluster = null;
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
      const mapped = slice.map((h) => {
        const hid = text(h.airtable_record_id || h.id);
        const hname = text(h.hotel_name || h.name);
        const bucket = String(h.verification_bucket || h.ownership_evidence_class || "").toUpperCase();
        const ver = /VERIFIED/.test(bucket)
          ? "verified"
          : /HISTOR/.test(bucket)
            ? "historical"
            : /UNRESOLVED|PENDING/.test(bucket)
              ? "unresolved"
              : "proposed";
        return {
          id: hid ? `hotel:${hid}` : `hotel:portfolio:${hname}`,
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
        const clusterId = "cluster:related-hotels";
        portfolioCluster = {
          id: clusterId,
          count: eligible.length,
          hotels: mapped,
        };
        addNode({
          id: clusterId,
          kind: "cluster",
          label: `Related Hotels (${eligible.length})`,
          role: "Portfolio cluster",
          verification: "proposed",
          group: "related_hotels",
          clusterHotels: mapped,
          clusterTotal: eligible.length,
        });
        addEdge({
          from: sponsor.id,
          to: clusterId,
          type: OCN_EDGE_TYPES.PORTFOLIO_HOTEL,
          verification: "proposed",
          shortLabel: "Portfolio",
          label: "Related hotels (bounded cluster — expand in detail)",
        });
      } else {
        mapped.forEach((h) => {
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
            from: sponsor.id,
            to: h.id,
            type: OCN_EDGE_TYPES.PORTFOLIO_HOTEL,
            verification: h.verification,
          });
        });
      }
    }
  }

  const unresolvedGroups = [];
  const lenderKnown = partyKnown(lender);
  const lenderIsHistorical =
    lenderKnown &&
    (verificationOf(lender) === "historical" || lender.current === false);
  if (!lenderKnown) {
    unresolvedGroups.push({
      id: "capital_financing",
      label: "Capital & Financing",
      status: "unresolved",
      note: "No public financing party verified for this hotel yet.",
    });
  } else if (lenderIsHistorical) {
    unresolvedGroups.push({
      id: "capital_financing",
      label: "Capital & Financing",
      status: "historical_only",
      note: "Historical financing is documented; current lender remains unresolved.",
    });
  }
  const advisorKnown = partyKnown(advisor);
  const advisorIsHistorical =
    advisorKnown &&
    (verificationOf(advisor) === "historical" || advisor.current === false);
  if (!partyKnown(developer) && !advisorKnown) {
    unresolvedGroups.push({
      id: "development_advisors",
      label: "Development & Advisors",
      status: "unresolved",
      note: "No verified developer or advisor relationship in the current packet.",
    });
  } else if (!partyKnown(developer) && advisorIsHistorical) {
    unresolvedGroups.push({
      id: "development_advisors",
      label: "Development & Advisors",
      status: "historical_only",
      note: "Historical transaction advisor documented; current advisors unresolved. Original developer not established.",
    });
  }
  if (!partyKnown(assetMgr) && !includePeople) {
    unresolvedGroups.push({
      id: "asset_people",
      label: "Asset Management & People",
      status: "collapsed",
      note: "Open Asset Management & People to review firm and key contacts.",
    });
  }

  let outNodes = nodes;
  let outEdges = edges;
  let groupResearchStatus = null;

  if (activeGroup) {
    const groupDef = OCN_STAKEHOLDER_GROUPS.find((g) => g.id === activeGroup);
    const allowedTypes = new Set((groupDef && groupDef.edgeTypes) || []);
    const keep = new Set([hotelNodeId]);
    // Keep ownership anchors when focusing people / portfolio / capital
    if (
      activeGroup === "asset_people" ||
      activeGroup === "related_hotels" ||
      activeGroup === "capital_financing"
    ) {
      nodes.forEach((n) => {
        if (n.group === "ownership_control") keep.add(n.id);
      });
    }
    edges.forEach((e) => {
      if (allowedTypes.has(e.type)) {
        keep.add(e.from);
        keep.add(e.to);
      }
    });
    nodes.forEach((n) => {
      if (n.group === activeGroup || n.focus) keep.add(n.id);
    });
    outNodes = nodes.filter((n) => keep.has(n.id));
    outEdges = edges.filter((e) => keep.has(e.from) && keep.has(e.to));
    const groupHasContent = outNodes.some((n) => n.group === activeGroup);
    if (!groupHasContent) {
      const unresolved = unresolvedGroups.find((g) => g.id === activeGroup);
      groupResearchStatus = {
        groupId: activeGroup,
        label: (groupDef && groupDef.label) || activeGroup,
        message:
          (unresolved && unresolved.note) ||
          "No verified records in this category yet. Research status is tracked for future refresh.",
      };
    }
  } else if (brandDevFilter) {
    const allowed = new Set(OCN_BRAND_DEV_EDGE_TYPES);
    outEdges = edges.filter((e) => allowed.has(e.type) || e.from === hotelNodeId || e.to === hotelNodeId);
    const keep = new Set([hotelNodeId]);
    outEdges.forEach((e) => {
      keep.add(e.from);
      keep.add(e.to);
    });
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

  const researchCoverage = assessStakeholderResearchCoverage(ownership);
  const vizCoverage = {
    version: "network-visualization-coverage-v1",
    renderedEdgeTypes: [...new Set(outEdges.map((e) => e.type))],
    renderedNodeKinds: [...new Set(outNodes.map((n) => n.kind))],
    activeGroup: activeGroup || "default",
    note: "Visualization coverage counts rendered verified/historical edges only",
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
    groupResearchStatus,
    portfolioCluster,
    researchCoverage,
    visualizationCoverage: vizCoverage,
    expansion: {
      peopleVisible: includePeople,
      peopleHidden,
      portfolioVisible: includePortfolioPreview,
      portfolioHidden,
      portfolioAsCluster: portfolioAsCluster && includePortfolioPreview,
      maxPeople,
      maxPortfolio,
    },
    legend: [
      { key: "verified", label: "Verified relationship" },
      { key: "proposed", label: "Proposed relationship" },
      { key: "historical", label: "Historical relationship" },
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
      activeGroup,
      defaultView: !includePeople && !includePortfolioPreview && !activeGroup,
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
  const portfolio = nodes.filter(
    (n) => n.kind === "cluster" || n.role === "Related hotel" || n.role === "Portfolio hotel" || n.role === "Portfolio cluster"
  );

  const cx = width / 2;
  const gapY = OCN_PRIMARY_CHAIN_GAP_Y;
  const opsGapX = Math.max(260, Math.min(320, width * 0.28));
  let y = 36;

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
    y += 12;
    positions[focus.id] = { x: cx, y: y + boxes[focus.id].h / 2 };
    y += boxes[focus.id].h + gapY + 18;
  }

  // Operations & Brand lane — separate from ownership, no cross-link implication
  const opsY = y + Math.max(boxes[operator?.id]?.h || 70, boxes[brand?.id]?.h || 70) / 2;
  if (operator) positions[operator.id] = { x: cx - opsGapX, y: opsY };
  if (brand) positions[brand.id] = { x: cx + opsGapX, y: opsY };
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
