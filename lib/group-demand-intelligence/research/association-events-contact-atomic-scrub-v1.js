/**
 * Atomic contact scrub V1 (run-050) — re-exports authoritative scrub from buyer integrity.
 */
export {
  scrubAssociationEventsContactFields as scrubContactFieldsAtomically,
  assertRejectedContactAbsent,
  ASSOCIATION_EVENTS_BUYER_INTEGRITY_V1 as ASSOCIATION_EVENTS_CONTACT_ATOMIC_SCRUB_V1,
} from "./association-events-buyer-integrity-v1.js";
