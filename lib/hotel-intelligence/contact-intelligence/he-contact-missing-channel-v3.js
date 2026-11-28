/**
 * HE re-export — canonical implementation lives in shared-contact-intelligence
 * so Hotel Explorer and GDI share identical missing-channel / Surfe include logic.
 */
export {
  MISSING_CHANNEL_ENRICHMENT_V3,
  HE_CONTACT_MISSING_CHANNEL_V3,
  isEligibleVerifiedChannel,
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
  surfeIncludeForChannel,
  resolveEnrichmentChannelRequest,
} from "../../shared-contact-intelligence/missing-channel-enrichment-v3.js";

export { default } from "../../shared-contact-intelligence/missing-channel-enrichment-v3.js";
