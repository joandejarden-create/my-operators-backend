/**
 * AI Demand Positioning — Multi-Provider Execution Runner.
 * Executes prompt universe across all providers with rate limiting and cost tracking.
 */

import { PROVIDERS, createObservation, createPeriod, savePeriod } from "../data-model.js";

const PROVIDER_CONFIGS = {
  openai: {
    model: "gpt-4o",
    maxTokens: 2000,
    costPerCall: 0.03,
    delayMs: 1000,
  },
  gemini: {
    model: "gemini-3.6-flash",
    maxTokens: 2000,
    costPerCall: 0.02,
    delayMs: 800,
  },
  perplexity: {
    model: "sonar",
    maxTokens: 2000,
    costPerCall: 0.05,
    delayMs: 1500,
  },
  claude: {
    model: "claude-sonnet-4-6",
    maxTokens: 2000,
    costPerCall: 0.03,
    delayMs: 1000,
  },
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function callProvider(provider, query, config) {
  const apiKey = process.env[`${provider.toUpperCase()}_API_KEY`] || process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return { error: `No API key for ${provider}`, response: null };
  }

  try {
    if (provider === "openai") {
      const res = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
        body: JSON.stringify({
          model: config.model,
          messages: [{ role: "user", content: query }],
          max_tokens: config.maxTokens,
          temperature: 0.7,
        }),
      });
      const data = await res.json();
      return { error: null, response: data.choices?.[0]?.message?.content || "" };
    }

    if (provider === "gemini") {
      const geminiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_GENAI_API_KEY || process.env.GOOGLE_GENERATIVE_AI_API_KEY || process.env.FDD_GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
      if (!geminiKey) return { error: "No Gemini API key", response: null };
      const res = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${config.model}:generateContent?key=${geminiKey}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ contents: [{ parts: [{ text: query }] }] }),
        }
      );
      const data = await res.json();
      if (data.error) return { error: `Gemini ${data.error.code}: ${data.error.message}`, response: null };
      return { error: null, response: data.candidates?.[0]?.content?.parts?.[0]?.text || "" };
    }

    if (provider === "perplexity") {
      const pplxKey = process.env.PERPLEXITY_API_KEY;
      if (!pplxKey) return { error: "No Perplexity API key", response: null };
      const res = await fetch("https://api.perplexity.ai/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${pplxKey}` },
        body: JSON.stringify({
          model: config.model,
          messages: [{ role: "user", content: query }],
          max_tokens: config.maxTokens,
        }),
      });
      const data = await res.json();
      const content = data.choices?.[0]?.message?.content || "";
      const citations = data.citations || [];
      return { error: null, response: content, citations };
    }

    if (provider === "claude") {
      const claudeKey = process.env.ANTHROPIC_API_KEY || process.env.CLAUDE_API_KEY || process.env.FDD_ANTHROPIC_API_KEY;
      if (!claudeKey) return { error: "No Anthropic API key", response: null };
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": claudeKey,
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify({
          model: config.model,
          max_tokens: config.maxTokens,
          messages: [{ role: "user", content: query }],
        }),
      });
      const data = await res.json();
      if (data.error) return { error: `Claude ${data.error.type}: ${data.error.message}`, response: null };
      return { error: null, response: data.content?.[0]?.text || "" };
    }

    return { error: `Unknown provider: ${provider}`, response: null };
  } catch (err) {
    return { error: err.message, response: null };
  }
}

/**
 * Execute the full prompt universe for a property.
 * @param {object} options
 * @param {string} options.propertyId
 * @param {Array} options.scenarios - merged scenario universe
 * @param {boolean} [options.dryRun=true] - if true, don't make actual API calls
 * @param {string[]} [options.providers] - subset of providers to run
 * @param {Function} [options.onProgress] - callback(completed, total)
 */
export async function executeMonitoringPeriod({
  propertyId,
  scenarios,
  dryRun = true,
  providers = PROVIDERS,
  onProgress,
}) {
  const period = createPeriod(propertyId, scenarios);
  const total = scenarios.length * providers.length;
  let completed = 0;
  const costEstimate = { total: 0, byProvider: {} };

  for (const provider of providers) {
    const config = PROVIDER_CONFIGS[provider];
    costEstimate.byProvider[provider] = 0;

    for (const scenario of scenarios) {
      if (dryRun) {
        const obs = createObservation({
          propertyId,
          scenarioId: scenario.scenarioId,
          provider,
          periodId: period.periodId,
          response: "[DRY RUN — no actual call]",
        });
        obs.dryRun = true;
        period.observations.push(obs);
      } else {
        await sleep(config.delayMs);
        const result = await callProvider(provider, scenario.query, config);
        const obs = createObservation({
          propertyId,
          scenarioId: scenario.scenarioId,
          provider,
          periodId: period.periodId,
          response: result.response || "",
        });
        if (result.error) {
          obs.error = result.error;
        } else {
          obs.rawResponse = result.response;
          if (result.citations && result.citations.length) {
            obs.providerCitations = result.citations;
          }
        }
        period.observations.push(obs);
      }

      costEstimate.byProvider[provider] += config.costPerCall;
      costEstimate.total += config.costPerCall;
      completed += 1;
      if (onProgress) onProgress(completed, total);
    }
  }

  period.status = dryRun ? "DRY_RUN_COMPLETE" : "EXECUTION_COMPLETE";
  period.costEstimate = costEstimate;
  period.completedAt = new Date().toISOString();
  savePeriod(period);

  return period;
}

export function estimateCost(scenarioCount, providerList = PROVIDERS) {
  let total = 0;
  const byProvider = {};
  for (const p of providerList) {
    const cost = scenarioCount * (PROVIDER_CONFIGS[p]?.costPerCall || 0.03);
    byProvider[p] = Math.round(cost * 100) / 100;
    total += cost;
  }
  return { total: Math.round(total * 100) / 100, byProvider, scenarioCount, providerCount: providerList.length };
}
