/**
 * Optional LLM enrichment for Commercial Readiness Snapshot prose.
 * Deterministic labels/scores remain source of truth and are never modified.
 */

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function enabledByEnv() {
  return process.env.COMMERCIAL_READINESS_LLM_ENRICHMENT_ENABLED === "1";
}

function providerConfigured() {
  return !!nz(process.env.OPENAI_API_KEY);
}

function resolveModel() {
  return process.env.COMMERCIAL_READINESS_LLM_MODEL || process.env.PARTNER_INTELLIGENCE_LLM_MODEL || "gpt-4o-mini";
}

function boolParam(v, fallback = false) {
  if (typeof v === "boolean") return v;
  const s = nz(v).toLowerCase();
  if (!s) return fallback;
  return s === "1" || s === "true" || s === "yes";
}

async function callOpenAiJson(systemPrompt, userPrompt) {
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: resolveModel(),
      temperature: 0.2,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
    }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json?.error?.message || `OpenAI API error (${res.status})`);
  }
  const content = json?.choices?.[0]?.message?.content;
  if (!content) throw new Error("OpenAI returned empty content.");
  return JSON.parse(content);
}

const SYSTEM_PROMPT = `You are writing a premium owner-facing Dealality Commercial Readiness narrative.
RULES (MANDATORY):
- Deterministic labels and scores are fixed and cannot be changed.
- Do NOT invent numbers, channel shares, conversion rates, or website audit findings.
- If performance data is missing, state that clearly and avoid definitive claims.
- Do NOT claim URL pages were programmatically analyzed unless explicitly provided.
- Keep tone strategic, calm, premium, practical, honest about uncertainty.
- Focus on what this means for deal strategy and owner decisions.
- Do not use chatbot language or mention AI.
- Output valid JSON only.
`;

function buildUserPrompt({ deterministicResult, inputs, mode }) {
  return `Generation mode: ${mode}

Original owner inputs (JSON):
${JSON.stringify(inputs, null, 2)}

Deterministic source-of-truth output (JSON):
${JSON.stringify(deterministicResult, null, 2)}

Task:
Return enriched prose for these sections while preserving deterministic labels/scores:
1) snapshotBasis
2) executiveCommercialInterpretation
3) commercialReadinessLevel
4) otaDependencyRisk
5) directBookingCapability
6) ownedChannelVsOtaContentGap
7) brandSystemContribution
8) operatorCommercialExecutionNeed
9) economicSensitivity
10) strategicDiagnosis
11) recommendedPath
12) dataNeededToConfirmNarrative
13) questionsToResolveNarrative
14) suggestedNextActionsNarrative

Requirements:
- Total prose target: 900 to 1500 words when sufficient input detail exists.
- Keep all deterministic labels unchanged.
- Mention URL extraction limitation explicitly.
- Keep case-specific and owner-goal-specific interpretation.

Return JSON:
{
  "sections": {
    "snapshotBasis": "...",
    "executiveCommercialInterpretation": "...",
    "commercialReadinessLevel": "...",
    "otaDependencyRisk": "...",
    "directBookingCapability": "...",
    "ownedChannelVsOtaContentGap": "...",
    "brandSystemContribution": "...",
    "operatorCommercialExecutionNeed": "...",
    "economicSensitivity": "...",
    "strategicDiagnosis": "...",
    "recommendedPath": "...",
    "dataNeededToConfirmNarrative": "...",
    "questionsToResolveNarrative": "...",
    "suggestedNextActionsNarrative": "..."
  },
  "wordCountEstimate": 0
}`;
}

function mergeEnrichedNarrative(baseResult, sections, meta) {
  const output = {
    ...baseResult,
    snapshot: {
      ...baseResult.snapshot,
      enrichment: {
        enabled: true,
        provider: meta.provider,
        model: meta.model,
        generatedAt: new Date().toISOString(),
        fallbackUsed: false,
      },
      snapshotBasis: {
        ...baseResult.snapshot.snapshotBasis,
        enrichedNarrative: nz(sections.snapshotBasis),
      },
      executiveCommercialInterpretation: {
        ...baseResult.snapshot.executiveCommercialInterpretation,
        enrichedNarrative: nz(sections.executiveCommercialInterpretation),
      },
      commercialReadinessLevel: {
        ...baseResult.snapshot.commercialReadinessLevel,
        enrichedNarrative: nz(sections.commercialReadinessLevel),
      },
      otaDependencyRisk: {
        ...baseResult.snapshot.otaDependencyRisk,
        enrichedNarrative: nz(sections.otaDependencyRisk),
      },
      directBookingCapability: {
        ...baseResult.snapshot.directBookingCapability,
        enrichedNarrative: nz(sections.directBookingCapability),
      },
      ownedChannelVsOtaContentGap: {
        ...baseResult.snapshot.ownedChannelVsOtaContentGap,
        enrichedNarrative: nz(sections.ownedChannelVsOtaContentGap),
      },
      brandSystemContribution: {
        ...baseResult.snapshot.brandSystemContribution,
        enrichedNarrative: nz(sections.brandSystemContribution),
      },
      operatorCommercialExecutionNeed: {
        ...baseResult.snapshot.operatorCommercialExecutionNeed,
        enrichedNarrative: nz(sections.operatorCommercialExecutionNeed),
      },
      economicSensitivity: {
        ...baseResult.snapshot.economicSensitivity,
        enrichedNarrative: nz(sections.economicSensitivity),
      },
      strategicDiagnosis: {
        ...baseResult.snapshot.strategicDiagnosis,
        enrichedNarrative: nz(sections.strategicDiagnosis),
      },
      recommendedPath: {
        ...baseResult.snapshot.recommendedPath,
        enrichedNarrative: nz(sections.recommendedPath),
      },
      dataNeededToConfirmNarrative: nz(sections.dataNeededToConfirmNarrative),
      questionsToResolveNarrative: nz(sections.questionsToResolveNarrative),
      suggestedNextActionsNarrative: nz(sections.suggestedNextActionsNarrative),
    },
  };

  // Labels remain deterministic source of truth.
  output.labels = { ...baseResult.labels };
  return output;
}

function applyFallback(baseResult, reason) {
  return {
    ...baseResult,
    snapshot: {
      ...baseResult.snapshot,
      enrichment: {
        enabled: false,
        provider: "none",
        model: "",
        generatedAt: new Date().toISOString(),
        fallbackUsed: true,
        reason,
      },
    },
  };
}

/**
 * @param {object} params
 * @param {object} params.deterministicResult output of buildCommercialReadinessSnapshot
 * @param {object} params.inputs original sanitized inputs
 * @param {"standalone"|"deal-linked"} params.mode generation mode
 * @param {boolean|string|number} params.enrichNarrative request toggle
 */
export async function enrichCommercialReadinessSnapshot({
  deterministicResult,
  inputs,
  mode = "standalone",
  enrichNarrative = false,
}) {
  const requested = boolParam(enrichNarrative, false);
  if (!requested) return applyFallback(deterministicResult, "enrichment not requested");
  if (!enabledByEnv()) return applyFallback(deterministicResult, "enrichment disabled by env");
  if (!providerConfigured()) return applyFallback(deterministicResult, "OPENAI_API_KEY not configured");

  try {
    const parsed = await callOpenAiJson(
      SYSTEM_PROMPT,
      buildUserPrompt({ deterministicResult, inputs, mode })
    );
    const sections = parsed?.sections || {};
    return mergeEnrichedNarrative(deterministicResult, sections, {
      provider: "openai",
      model: resolveModel(),
    });
  } catch (error) {
    return applyFallback(
      deterministicResult,
      `enrichment failed: ${nz(error?.message) || "unknown error"}`
    );
  }
}

