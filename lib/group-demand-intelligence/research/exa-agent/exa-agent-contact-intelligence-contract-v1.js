/**
 * Exa Agent contact-intelligence contract V1.
 *
 * Structured people leads for post-ladder escalation. Candidates only —
 * never auto-finalize. Do not reuse scaling discovery schema.
 */
export const EXA_AGENT_CONTACT_INTELLIGENCE_CONTRACT_V1 =
  "gdi-exa-agent-contact-intelligence-contract-v1";

export const EXA_AGENT_CONTACT_OUTPUT_SCHEMA_V1 = Object.freeze({
  type: "object",
  required: ["people", "unresolvedQuestions"],
  additionalProperties: false,
  properties: {
    eventOrganizerSummary: { type: "string" },
    logisticsControllerSummary: { type: "string" },
    people: {
      type: "array",
      maxItems: 6,
      items: {
        type: "object",
        required: [
          "fullName",
          "currentEmployer",
          "professionalRole",
          "relevantOrganization",
          "relationshipToOpportunity",
          "classification",
          "sourceUrls",
          "evidenceExcerpts",
          "confidence",
        ],
        additionalProperties: false,
        properties: {
          fullName: { type: "string" },
          currentEmployer: { type: "string" },
          professionalRole: { type: "string" },
          relevantOrganization: { type: "string" },
          relationshipToOpportunity: { type: "string" },
          classification: {
            type: "string",
            enum: ["BUYER", "ALTERNATIVE_PERSON", "UNKNOWN"],
          },
          sourceUrls: {
            type: "array",
            items: { type: "string" },
            maxItems: 8,
          },
          evidenceExcerpts: {
            type: "array",
            items: { type: "string" },
            maxItems: 6,
          },
          confidence: {
            type: "string",
            enum: ["HIGH", "MEDIUM", "LOW"],
          },
          unresolvedForPerson: { type: "string" },
        },
      },
    },
    unresolvedQuestions: {
      type: "array",
      items: { type: "string" },
      maxItems: 10,
    },
  },
});

/**
 * Build Agent query for contact research. Must not include known person seeds.
 */
export function buildExaAgentContactQuery({
  organizationName,
  eventTitle,
  hotelName,
  destination,
  evidenceUrls = [],
  hotelFitNote = "",
} = {}) {
  const org = String(organizationName || "").trim();
  const event = String(eventTitle || "").trim();
  const hotel = String(hotelName || "").trim();
  const dest = String(destination || "").trim();
  const urls = (Array.isArray(evidenceUrls) ? evidenceUrls : [])
    .map((u) => String(u || "").trim())
    .filter((u) => /^https?:\/\//i.test(u))
    .slice(0, 4);
  const fit = String(hotelFitNote || "").trim();

  return [
    `Investigate hotel group-demand contact intelligence for this specific opportunity.`,
    `Organization: ${org}`,
    `Event / project: ${event}`,
    dest ? `Destination: ${dest}` : null,
    hotel ? `Assigned hotel (overflow/partner lodging context): ${hotel}` : null,
    fit ? `Hotel fit note: ${fit}` : null,
    urls.length ? `Primary evidence URLs:\n${urls.map((u) => `- ${u}`).join("\n")}` : null,
    ``,
    `Answer independently:`,
    `1. Who actually organizes the event?`,
    `2. Which organization controls event logistics?`,
    `3. Who is responsible for accommodation, hotel sourcing, or partnerships?`,
    `4. What named person has the strongest evidenced relationship to this decision?`,
    `5. If the direct buyer is unknown, who is the best viable alternative person?`,
    `6. Is there a second commercially relevant named person?`,
    `7. Which primary sources verify these individuals?`,
    ``,
    `Rules:`,
    `- Return only named people you can support with source URLs.`,
    `- Prefer organizers, officers, meetings/events/housing roles over speakers.`,
    `- Do not invent emails or phones.`,
    `- Classify each person as BUYER or ALTERNATIVE_PERSON.`,
    `- If uncertain, leave people empty and list unresolvedQuestions.`,
  ]
    .filter((line) => line != null)
    .join("\n");
}

export function buildExaAgentContactCreatePayload({
  organizationName,
  eventTitle,
  hotelName,
  destination,
  evidenceUrls,
  hotelFitNote,
  effort = "medium",
} = {}) {
  return {
    query: buildExaAgentContactQuery({
      organizationName,
      eventTitle,
      hotelName,
      destination,
      evidenceUrls,
      hotelFitNote,
    }),
    effort,
    outputSchema: EXA_AGENT_CONTACT_OUTPUT_SCHEMA_V1,
  };
}

/**
 * Map Agent structured people → candidate person objects for verification.
 */
export function mapExaAgentContactPeopleToCandidates(structured = {}, opts = {}) {
  const people = Array.isArray(structured?.people) ? structured.people : [];
  const orgHint = String(opts.organizationName || "").trim() || null;
  return people.map((p) => {
    const urls = Array.isArray(p?.sourceUrls)
      ? p.sourceUrls.map((u) => String(u || "").trim()).filter((u) => /^https?:\/\//i.test(u))
      : [];
    const excerpts = Array.isArray(p?.evidenceExcerpts)
      ? p.evidenceExcerpts.map((e) => String(e || "").trim()).filter(Boolean)
      : [];
    return {
      name: String(p?.fullName || "").trim() || null,
      title: String(p?.professionalRole || "").trim() || null,
      role: String(p?.professionalRole || "").trim() || null,
      currentOrganization:
        String(p?.currentEmployer || p?.relevantOrganization || "").trim() || orgHint,
      organization:
        String(p?.relevantOrganization || p?.currentEmployer || "").trim() || orgHint,
      relevanceEvidence: [
        String(p?.relationshipToOpportunity || "").trim(),
        ...excerpts,
      ]
        .filter(Boolean)
        .join(" | ")
        .slice(0, 600),
      sourceUrl: urls[0] || null,
      sourceUrls: urls,
      buyerClassification: p?.classification || "UNKNOWN",
      confidence: p?.confidence || null,
      discoveryMethod: "EXA_AGENT_CONTACT",
      agentUnresolved: p?.unresolvedForPerson || null,
    };
  });
}
