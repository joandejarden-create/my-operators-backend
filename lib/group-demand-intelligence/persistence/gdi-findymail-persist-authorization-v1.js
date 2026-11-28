/**
 * Hotel-agnostic Findymail → Airtable persistence authorization + patch builder.
 *
 * Fail-closed. Paid research authorization (authorizePaidResearch /
 * authorizeFindymail) NEVER unlocks Airtable writes. Persistence needs ONE of:
 *
 *   A. authorizeResearchPersistence + exactWriteSetAuthorization (text)
 *   B. authorizeFindymailPersist + authorizeAirtableProductionWrites
 *
 * Actual apply additionally requires the process env gate
 * GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES=1 (set for the live process only).
 *
 * Surfe-sourced channels are never persisted. No hotel-specific defaults.
 */

export const GDI_FINDYMAIL_PERSIST_AUTHORIZATION_V1 =
  "gdi-findymail-persist-authorization-v1";

export const FINDYMAIL_PERSIST_PRODUCTION_ENV_GATE =
  "GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function persistError(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

/**
 * Pure authorization probe. Never throws.
 */
export function resolveFindymailPersistAuthorization(opts = {}) {
  const viaResearchPersistence =
    opts.authorizeResearchPersistence === true &&
    Boolean(text(opts.exactWriteSetAuthorization));
  const viaDedicated =
    opts.authorizeFindymailPersist === true &&
    opts.authorizeAirtableProductionWrites === true;
  const envGateOpen =
    String(process.env[FINDYMAIL_PERSIST_PRODUCTION_ENV_GATE] || "") === "1";
  return {
    contract: GDI_FINDYMAIL_PERSIST_AUTHORIZATION_V1,
    authorized: viaResearchPersistence || viaDedicated,
    via: viaResearchPersistence
      ? "RESEARCH_PERSISTENCE_EXACT_WRITE_SET"
      : viaDedicated
        ? "DEDICATED_FINDYMAIL_PERSIST_PLUS_AIRTABLE_PRODUCTION_WRITES"
        : null,
    envGateOpen,
    // Paid-research flags are intentionally ignored here.
    paidResearchAuthorizationIgnored: true,
  };
}

/**
 * Throws a fail-closed error unless Findymail persistence is separately
 * authorized. `apply` additionally requires the production env gate.
 */
export function assertFindymailPersistAuthorized(opts = {}, { apply = false } = {}) {
  const auth = resolveFindymailPersistAuthorization(opts);
  if (!auth.authorized) {
    throw persistError(
      "FINDYMAIL_PERSIST_REQUIRES_AUTH",
      "Findymail persistence requires authorizeResearchPersistence + exactWriteSetAuthorization, OR authorizeFindymailPersist + authorizeAirtableProductionWrites (paid research authorization never unlocks Airtable writes)",
    );
  }
  if (apply && !auth.envGateOpen) {
    throw persistError(
      "FINDYMAIL_PERSIST_PRODUCTION_ENV_GATE_REQUIRED",
      `Applying Findymail persistence requires process env ${FINDYMAIL_PERSIST_PRODUCTION_ENV_GATE}=1`,
    );
  }
  return auth;
}

/**
 * Build persist patches from Stage 4 contact results. Only opportunities whose
 * primary contact carries a FINDYMAIL-sourced email channel are eligible.
 * Surfe and public-source emails are never persisted by this path.
 */
export function buildFindymailPersistPatchesFromContactResults(results = []) {
  const patches = [];
  const skipped = [];
  for (const r of Array.isArray(results) ? results : []) {
    const opportunity = r?.opportunity || r;
    if (!opportunity || typeof opportunity !== "object") continue;
    const id = text(opportunity.id || opportunity.opportunityId || opportunity.candidateId);
    const person = opportunity.primaryContact || {};
    let channel = (Array.isArray(person.channels) ? person.channels : []).find(
      (c) => text(c?.source).toUpperCase() === "FINDYMAIL" && text(c?.value),
    );
    // Recover Findymail emails when Stage 4 attached emailSource /
    // FINDYMAIL_PROVIDER_RETURNED but channels were rebuilt without the
    // FINDYMAIL source marker (pre-persist-channel-merge builds).
    if (
      !channel &&
      text(person.email).includes("@") &&
      (text(person.emailSource).toUpperCase() === "FINDYMAIL" ||
        text(person.emailVerificationStatus).toUpperCase() ===
          "FINDYMAIL_PROVIDER_RETURNED")
    ) {
      channel = {
        channelType: "PROFESSIONAL_EMAIL",
        value: text(person.email),
        source: "FINDYMAIL",
        verificationStatus: "UNVERIFIED",
        personDirect: true,
        organizational: false,
        researchProvenance: {
          provider: "FINDYMAIL",
          operation: "FIND_FROM_NAME",
          recoveredFromPersonEmail: true,
        },
        personName: text(person.fullName || person.name) || null,
      };
    }
    if (!channel) {
      skipped.push({ opportunityId: id || null, reason: "NO_FINDYMAIL_CHANNEL" });
      continue;
    }
    if (!id) {
      skipped.push({ opportunityId: null, reason: "NO_OPPORTUNITY_ID" });
      continue;
    }
    patches.push({
      opportunity: { ...opportunity, id: opportunity.id || id },
      enrichment: { person, channel },
    });
  }
  return { patches, skipped };
}
