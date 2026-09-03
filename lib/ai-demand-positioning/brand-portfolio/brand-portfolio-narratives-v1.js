/**
 * Customer Brand & Portfolio narratives — PORTFOLIO_KPI_CONTRACT_V1_1.
 * Absolute visibility vs relative portfolio position must stay distinct.
 */

function pct(rate) {
  if (rate == null) return null;
  return `${(rate * 100).toFixed(1)}%`;
}

function providerSpreadLine(byProvider) {
  if (!byProvider) return null;
  const order = ["openai", "gemini", "perplexity", "claude"];
  const labels = { openai: "OpenAI", gemini: "Gemini", perplexity: "Perplexity", claude: "Claude" };
  return order
    .map((p) => {
      const r = byProvider[p]?.presenceRate;
      return `${labels[p]} ${r == null ? "—" : pct(r)}`;
    })
    .join(" · ");
}

function indexAboveBenchmarkPct(index) {
  if (index == null || index < 100) return null;
  return Math.round(index - 100);
}

/**
 * Founder-approved customer narratives for the five first-cycle properties.
 */
export function buildBrandPortfolioNarrativesV1({ profile, metrics, lens }) {
  const propertyId = profile.propertyId || profile.id;
  const presence = metrics.portfolioAiPresence;
  const rank = metrics.portfolioRank;
  const rankOf = metrics.portfolioRankOf;
  const index = metrics.portfolioPresenceIndex;
  const benchmark = metrics.portfolioBenchmark;
  const top3 = metrics.top3Appearance;
  const spread = providerSpreadLine(metrics.byProvider);
  const lensLabel = lens?.label || metrics.lens?.label || "portfolio";
  const name = profile.name;
  const above = indexAboveBenchmarkPct(index);

  const headlineParts = [];
  if (presence != null) headlineParts.push(`${pct(presence)} Portfolio AI Presence`);
  if (rank != null) headlineParts.push(`#${rank} of ${rankOf}`);
  if (index != null) headlineParts.push(`Index ${index}`);
  const headline = headlineParts.join(" · ");

  let body = "";
  let actions = [];

  if (propertyId === "adp_waterstone_boca_raton") {
    body =
      `${name} ranks #${rank} of ${rankOf} within the governed Hilton Honors competitive set, ` +
      `with ${pct(presence)} cross-model Portfolio AI Presence — moderate absolute visibility, not broadly dominant coverage. ` +
      `Presence Index ${index} is about ${above}% above the peer benchmark (${pct(benchmark)}), so relative portfolio position is strong. ` +
      `Provider inconsistency remains material: Gemini and Perplexity are stronger; OpenAI and Claude are weaker (${spread}). ` +
      `Index and rank do not imply uniformly strong AI visibility across every provider.`;
    actions = [
      {
        priority: "medium",
        title: "Close OpenAI and Claude gaps",
        description:
          "Relative #2 standing and Index 137 sit atop 50% absolute presence. Strengthen signals that OpenAI and Claude cite so cross-model coverage catches up to Gemini/Perplexity.",
      },
      {
        priority: "medium",
        title: "Keep absolute and relative reads separate",
        description:
          "Above-benchmark Index measures ecosystem standing vs peers — it does not mean travelers see you consistently across all major AI providers.",
      },
    ];
  } else if (propertyId === "adp_renaissance_times_square") {
    body =
      `${name} ranks #${rank} of ${rankOf} within the governed Marriott Bonvoy competitive set, ` +
      `with ${pct(presence)} cross-model Portfolio AI Presence. ` +
      `Presence Index ${index} is modestly above the peer benchmark (${pct(benchmark)}) — do not overstate that edge. ` +
      `There remains meaningful room to strengthen consistency across AI providers (${spread}).`;
    actions = [
      {
        priority: "medium",
        title: "Improve cross-provider consistency",
        description:
          "Near-parity Index 115 with ~47% absolute presence leaves clear headroom, especially where Claude and Perplexity under-index.",
      },
    ];
  } else if (propertyId === "adp_hotel_phillips_kansas_city") {
    body =
      `${name} ranks #${rank} of ${rankOf} within the governed Hilton Honors competitive set, ` +
      `with ${pct(presence)} cross-model Portfolio AI Presence and above-benchmark Index ${index} (peer mean ${pct(benchmark)}). ` +
      `Scenario-level Top-3 Appearance is strong (${pct(top3)}), but #2 ranking must not obscure weak OpenAI presence. ` +
      `Substantial provider inconsistency remains (${spread}).`;
    actions = [
      {
        priority: "high",
        title: "Address OpenAI absence",
        description:
          "OpenAI presence is near zero while Gemini/Perplexity carry most visibility. Ranking #2 does not mean consistent cross-model discovery.",
      },
      {
        priority: "medium",
        title: "Preserve Top-3 scenario strength",
        description:
          "Strong Top-3 scenario outcomes are a relative advantage — convert them into broader provider-observation coverage.",
      },
    ];
  } else if (propertyId === "adp_cambridge_beaches_bermuda") {
    body =
      `${name} ranks #${rank} of ${rankOf} within the governed independent peer set, ` +
      `with ${pct(presence)} Portfolio AI Presence. ` +
      `The peer set is too thin for governed Portfolio Benchmark and Presence Index — those KPIs are not shown (not zero). ` +
      `Provider mix: ${spread}.`;
    actions = [
      {
        priority: "low",
        title: "Monitor independent peer-set adequacy",
        description:
          "Benchmark and Index stay suppressed until the governed independent peer set meets the minimum threshold.",
      },
    ];
  } else if (propertyId === "adp_now_now_noho") {
    body =
      `${name} shows very weak visibility within the measured Independent Positioning universe: ` +
      `${pct(presence)} Portfolio AI Presence, rank #${rank} of ${rankOf}, and Presence Index ${index} vs peer benchmark ${pct(benchmark)}. ` +
      `This is a measured outcome for the governed scenario and provider set — not a technical failure, and not a claim about broader market demand beyond that universe. ` +
      `Provider mix: ${spread}.`;
    actions = [
      {
        priority: "high",
        title: "Build independent AI discoverability",
        description:
          "Absolute observation-grain presence is 3.1%. Prioritize consistent naming and citeable independent signals across providers.",
      },
    ];
  } else {
    // Generic fallback
    body =
      `${name} shows ${pct(presence)} Portfolio AI Presence` +
      (rank != null ? `, ranking #${rank} of ${rankOf}` : "") +
      ` under ${lensLabel}.`;
    if (index != null) {
      body += ` Presence Index ${index} vs peer benchmark ${pct(benchmark)} — relative position is separate from absolute cross-model visibility.`;
    } else {
      body += ` Peer Benchmark / Index are suppressed for this peer set (not zero).`;
    }
    if (spread) body += ` Provider mix: ${spread}.`;
    actions = [
      {
        priority: "low",
        title: "Monitor portfolio presence continuity",
        description: "Maintain observation-grain presence and peer rank across the next certified cycle.",
      },
    ];
  }

  return {
    version: "ADP_BRAND_PORTFOLIO_NARRATIVE_V1_1_CUSTOMER",
    grain: "PROVIDER_OBSERVATION",
    headline,
    body,
    absoluteBand:
      presence == null
        ? "unknown"
        : presence < 0.1
          ? "very_weak"
          : presence < 0.35
            ? "weak"
            : presence < 0.55
              ? "moderate"
              : presence < 0.75
                ? "solid"
                : "strong",
    providerSpread: spread,
    actions: actions.slice(0, 4),
    customerFacing: true,
    distinguishesAbsoluteVsRelative: true,
  };
}
