/**
 * AVA Claim ↔ Evidence relationship graph V1
 *
 * Every substantive finding/recommendation keeps claimId, supporting URLs,
 * verification status, and related recommendation IDs.
 */
export const CLAIM_EVIDENCE_GRAPH_CONTRACT = "AVA_CLAIM_EVIDENCE_GRAPH_V1";

function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function pushUrl(list, url, meta = {}) {
  const u = String(url || "").trim();
  if (!/^https?:\/\//i.test(u)) return;
  list.push({
    url: u,
    label: meta.label || null,
    kind: meta.kind || null,
    note: meta.note || meta.claimNotes || null,
  });
}

/**
 * Build claim nodes from an AVA payload.
 * @returns {{ contract:string, propertyId:string, claims: object[] }}
 */
export function buildClaimEvidenceGraph(payload) {
  const propertyId = payload?.property?.propertyId || null;
  const claims = [];

  for (const f of payload?.keyFindings || []) {
    const related = f.relatedActionId || null;
    const claimId =
      f.claimId ||
      `FINDING_${slug(related || f.title || claims.length)}`;
    const evidence = [];
    for (const e of f.evidence || []) {
      pushUrl(evidence, e.url || e, e);
    }
    claims.push({
      claimId,
      claimType: "FINDING",
      propertyId,
      claimText: f.summary || f.title || "",
      title: f.title || null,
      supportingEvidence: evidence,
      relatedRecommendationIds: related ? [related] : [],
      verificationStatus: "PENDING",
      verificationTimestamp: null,
      resolutionAttempts: [],
      finalSupportingSources: [],
      material: true,
    });
  }

  for (const a of payload?.actions || []) {
    const claimId = a.claimId || `ACTION_${slug(a.actionId || a.headline)}`;
    const evidence = [];
    for (const e of a.supportingEvidence || []) pushUrl(evidence, e.url || e, e);
    for (const e of a.technicalDetails?.urls || []) pushUrl(evidence, e.url || e, e);
    for (const e of a.technicalDetails?.evidence || []) {
      if (e && typeof e === "object") pushUrl(evidence, e.url, e);
    }
    for (const ref of a.evidenceRefs || []) pushUrl(evidence, ref, { label: "evidenceRef" });
    claims.push({
      claimId,
      claimType: "RECOMMENDATION",
      propertyId,
      claimText: a.whatWeFound || a.headline || "",
      title: a.headline || null,
      actionId: a.actionId || null,
      founderDecision: a.hotelDecision || a.founderDecision || null,
      supportingEvidence: evidence,
      relatedRecommendationIds: a.actionId ? [a.actionId] : [],
      verificationStatus: "PENDING",
      verificationTimestamp: null,
      resolutionAttempts: [],
      finalSupportingSources: [],
      material: a.claimType !== "OPTIONAL_COMMERCIAL_OPPORTUNITY",
    });
  }

  // Executive themes (material commercial statements)
  let themeIdx = 0;
  for (const t of payload?.executiveSummary?.themes || []) {
    themeIdx += 1;
    const claimId = t.claimId || `THEME_${themeIdx}`;
    claims.push({
      claimId,
      claimType: "EXECUTIVE_THEME",
      propertyId,
      claimText: t.what || "",
      title: t.whyItMatters || null,
      supportingEvidence: [],
      relatedRecommendationIds: [],
      verificationStatus: "PENDING",
      verificationTimestamp: null,
      resolutionAttempts: [],
      finalSupportingSources: [],
      material: true,
    });
  }

  const da = payload?.digitalAuthority;
  if (da) {
    for (const o of da.opportunities || []) {
      const claimId = `DA_OPP_${slug(o.title)}`;
      const evidence = [];
      if (o.targetOrganizationUrl) pushUrl(evidence, o.targetOrganizationUrl, o);
      for (const e of o.evidenceLinks || []) pushUrl(evidence, e.url || e, e);
      claims.push({
        claimId,
        claimType: "DIGITAL_AUTHORITY_OPPORTUNITY",
        propertyId,
        claimText: o.whyItMatters || o.title || "",
        title: o.title || null,
        supportingEvidence: evidence,
        relatedRecommendationIds: [],
        verificationStatus: "PENDING",
        verificationTimestamp: null,
        resolutionAttempts: [],
        finalSupportingSources: [],
        material: false,
      });
    }
  }

  return {
    contract: CLAIM_EVIDENCE_GRAPH_CONTRACT,
    propertyId,
    hotelName: payload?.property?.name || null,
    claimCount: claims.length,
    claims,
    builtAt: new Date().toISOString(),
  };
}

/**
 * Deduplicate evidence URLs across the graph.
 */
export function uniqueEvidenceUrlsFromGraph(graph) {
  const urls = new Set();
  for (const c of graph?.claims || []) {
    for (const e of c.supportingEvidence || []) {
      if (e?.url) urls.add(e.url);
    }
  }
  return [...urls];
}

/**
 * Attach per-URL verification outcomes back onto claims.
 */
export function applyUrlVerificationToClaims(graph, urlOutcomes = {}) {
  const now = new Date().toISOString();
  return {
    ...graph,
    claims: (graph.claims || []).map((c) => {
      const outcomes = (c.supportingEvidence || []).map((e) => {
        const o = urlOutcomes[e.url] || urlOutcomes[normalizeLoose(e.url)];
        return { url: e.url, ...(o || { status: "UNCHECKED" }) };
      });
      const blocking = outcomes.some(
        (o) =>
          o.gateBlocking === true ||
          ["BROKEN_LINK", "UNSUPPORTED_CLAIM", "REQUIRES_MANUAL_REVIEW"].includes(
            o.classification
          )
      );
      const allOk =
        outcomes.length === 0 ||
        outcomes.every((o) =>
          ["VERIFIED_CURRENT", "VERIFIED_WITH_QUALIFICATION", "HTTP_OK"].includes(
            o.classification || o.status
          )
        );
      return {
        ...c,
        evidenceOutcomes: outcomes,
        verificationStatus: allOk
          ? "VERIFIED"
          : blocking
            ? "REQUIRES_MANUAL_REVIEW"
            : "PARTIAL",
        verificationTimestamp: now,
        finalSupportingSources: outcomes
          .filter((o) => o.finalUrl || o.url)
          .filter((o) =>
            ["VERIFIED_CURRENT", "VERIFIED_WITH_QUALIFICATION"].includes(
              o.classification
            )
          )
          .map((o) => o.finalUrl || o.url),
      };
    }),
  };
}

function normalizeLoose(u) {
  try {
    const p = new URL(u);
    p.hash = "";
    p.hostname = p.hostname.replace(/^www\./i, "").toLowerCase();
    return p.toString().replace(/\/$/, "");
  } catch {
    return String(u || "");
  }
}

export { normalizeLoose as normalizeClaimEvidenceUrl };
