/**
 * Market opportunity graph index (in-memory / report artifact).
 * Prefer relational records over heavy graph infra.
 */

export function buildMarketOpportunityGraph({
  marketPackets = [],
  hotelLinks = [],
  hotels = [],
} = {}) {
  const nodes = [];
  const edges = [];

  const metroId = "market:nyc";
  nodes.push({ id: metroId, type: "Market", label: "New York City" });

  const subSeen = new Set();
  for (const h of hotels) {
    nodes.push({
      id: `hotel:${h.hotelId}`,
      type: "Hotel",
      label: h.displayName,
      submarket: h.submarket,
      microArea: h.microArea,
    });
    if (h.submarket && !subSeen.has(h.submarket)) {
      subSeen.add(h.submarket);
      const sid = `submarket:${h.submarket}`;
      nodes.push({ id: sid, type: "Submarket", label: h.submarket });
      edges.push({ from: sid, to: metroId, type: "LOCATED_IN" });
    }
    if (h.submarket) {
      edges.push({
        from: `hotel:${h.hotelId}`,
        to: `submarket:${h.submarket}`,
        type: "LOCATED_IN",
      });
    }
  }

  const mktSeen = new Set();
  for (const p of marketPackets) {
    if (mktSeen.has(p.marketOpportunityId)) continue;
    mktSeen.add(p.marketOpportunityId);
    nodes.push({
      id: `opp:${p.marketOpportunityId}`,
      type: "Opportunity",
      label: p.title,
      organization: p.organizationName,
    });
    if (p.organizationName) {
      const oid = `org:${p.organizationName.slice(0, 48)}`;
      if (!nodes.some((n) => n.id === oid)) {
        nodes.push({ id: oid, type: "Organization", label: p.organizationName });
      }
      edges.push({
        from: oid,
        to: `opp:${p.marketOpportunityId}`,
        type: "GENERATES",
      });
    }
    if (p.geography?.submarket) {
      const sid = `submarket:${p.geography.submarket}`;
      if (!nodes.some((n) => n.id === sid)) {
        nodes.push({ id: sid, type: "Submarket", label: p.geography.submarket });
        edges.push({ from: sid, to: metroId, type: "LOCATED_IN" });
      }
      edges.push({
        from: `opp:${p.marketOpportunityId}`,
        to: sid,
        type: "OCCURS_AT",
      });
    } else {
      edges.push({
        from: `opp:${p.marketOpportunityId}`,
        to: metroId,
        type: "OCCURS_AT",
      });
    }
  }

  for (const link of hotelLinks) {
    const edgeType =
      link.finalState === "NOT_APPLICABLE" || link.finalState === "NOT_FIT"
        ? "NOT_FIT_FOR"
        : link.geographicApplicability === "DIRECT" ||
            link.geographicApplicability === "STRONG"
          ? "APPLICABLE_TO"
          : "MATCHED_TO";
    edges.push({
      from: `opp:${link.marketOpportunityId}`,
      to: `hotel:${link.hotelId}`,
      type: edgeType,
      finalState: link.finalState,
      fit: link.hotelFitScore,
    });
  }

  return {
    schemaVersion: "gdi_market_opportunity_graph_v1",
    marketOpportunities: mktSeen.size,
    hotelOpportunityLinks: hotelLinks.length,
    duplicateMarketOppsMerged: Math.max(0, marketPackets.length - mktSeen.size),
    nodes,
    edges,
  };
}
