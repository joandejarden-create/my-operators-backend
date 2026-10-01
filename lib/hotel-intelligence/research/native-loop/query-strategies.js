/**
 * Dynamic query strategies by lane — Iteration 1.
 */

export const QUERY_STRATEGY_VERSION = "native-query-strategy-v1";

function hotelLabel(state) {
  return state.entity?.name || state.identity_lock?.subject?.name || "hotel";
}

function cityCountry(state) {
  const city = state.entity?.city || state.identity_lock?.subject?.city || "";
  const country = state.entity?.country || state.identity_lock?.subject?.country || "";
  return [city, country].filter(Boolean).join(" ");
}

export function generateQueriesForStep(state, step) {
  const hotel = hotelLabel(state);
  const loc = cityCountry(state);
  const base = `${hotel} ${loc}`.trim();
  const ownerCandidates = state.claims
    .filter((c) => /OWNED_BY|ECONOMIC_OWNER|PROPCO/.test(c.relationship || ""))
    .map((c) => c.object)
    .filter(Boolean);

  const field = step?.field || "ownership";
  let queries = [];

  if (field === "identity" || field === "address") {
    queries = [`${base} official site`, `${base} address`, `${hotel} ${loc} hotel`];
  } else if (field === "economic_owner" || field === "owner" || field === "ultimate_owner") {
    queries = [
      `${base} owner`,
      `${base} ownership`,
      `${base} acquisition`,
      `${base} sold`,
      `${base} LLC`,
      `${base} PropCo`,
      `${base} portfolio`,
    ];
  } else if (field === "propco") {
    queries = [`${base} inmobiliaria`, `${base} property owner`, `${base} fideicomiso`, `${base} PropCo`];
  } else if (field === "operator" || field === "management" || field === "residual" || field === "dispute") {
    queries = [
      `${base} operator`,
      `${base} managed by`,
      `${base} management company`,
      `${base} stewarded by`,
    ];
  } else if (field === "transaction") {
    queries = [`${base} acquisition`, `${base} sold to`, `${base} transaction`, `${base} financing`];
  } else if (field === "portfolio" || field === "subsidiaries" || field === "operating_mix") {
    const org = state.entity?.name || ownerCandidates[0] || hotel;
    queries = [`${org} hotel portfolio`, `${org} owned hotels`, `${org} subsidiaries`];
  } else if (field === "person" || field === "role" || field === "email" || field === "domain" || field === "phone") {
    const org = state.entity?.name || ownerCandidates[0] || hotel;
    queries = [`${org} leadership`, `${org} CEO`, `${org} about`, `${org} contact`];
  } else if (field === "facts" || field === "conflicts") {
    queries = [`${base} rooms`, `${base} keys`, `${base} acreage`, `${base} official`];
  } else if (field === "contradiction" || field === "verification" || field === "alternatives") {
    queries = ownerCandidates.length
      ? ownerCandidates.flatMap((o) => [`${o} ${hotel}`, `${o} acquisition ${hotel}`, `${o} portfolio`])
      : [`${base} ownership verify`, `${base} operator vs owner`];
  } else {
    queries = [`${base} ${field}`];
  }

  // Candidate follow-ups
  for (const o of ownerCandidates.slice(0, 2)) {
    queries.push(`${o} ${hotel}`, `${o} portfolio ${loc}`);
  }

  const deduped = [];
  const seen = new Set();
  for (const q of queries) {
    const k = q.toLowerCase().replace(/\s+/g, " ").trim();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    deduped.push(q);
  }

  for (const q of deduped) {
    state.queries.push({
      query: q,
      step_id: step?.id || null,
      iteration: state.iteration,
      at: new Date().toISOString(),
    });
  }
  return deduped;
}
