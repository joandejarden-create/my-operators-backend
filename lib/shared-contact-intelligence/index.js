/**
 * Shared Canonical Contact Intelligence V1 — public entry.
 * GDI and Hotel Explorer must consume this resolver for durable channels.
 */

export {
  SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
  CANONICAL_PERSON_SOT,
  CANONICAL_CHANNEL_SOT,
  CONTACT_CONSISTENCY_GATE,
  GATE_STATUS,
  BETHESDA_GOLDEN,
} from "./architecture-constants-v1.js";

export {
  CANONICAL_PERSON_IDENTITY_V1,
  buildPersonIdentityLink,
  scoreIdentityEvidence,
  loadPersonIdMap,
  upsertPersonIdMapEntry,
  resolveCanonicalPersonId,
  gdiPersonIdentityKey,
} from "./canonical-person-identity-v1.js";

export {
  CANONICAL_CHANNEL_RESOLVER_V1,
  resolveCanonicalPersonChannels,
  resolveCanonicalHotelContacts,
  toGdiContactPresentation,
  toHeContactDirectoryRow,
  assertGdiHeChannelParity,
  toSharedChannelDto,
} from "./canonical-channel-resolver-v1.js";

export { hydrateGdiPrimaryContact } from "./gdi-canonical-hydrate-v1.js";

export {
  CANONICAL_CONTACTABILITY_V1,
  computeSharedContactability,
  dedupePeopleForContactability,
} from "./canonical-contactability-v1.js";

export {
  CANONICAL_CONTACT_CONSISTENCY_GATES_V1,
  runCanonicalContactConsistencyGates,
} from "./canonical-contact-consistency-gates-v1.js";

export { FINDYMAIL_REUSE_POLICY_V1, planFindymailLookup } from "./findymail-reuse-policy-v1.js";

export {
  SURFE_TRANSIENT_POLICY_V1,
  SURFE_POLICY,
  assertSurfeTransientOnly,
} from "./surfe-transient-policy-v1.js";
