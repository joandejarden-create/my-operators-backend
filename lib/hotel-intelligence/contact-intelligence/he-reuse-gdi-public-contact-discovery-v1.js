/**
 * Hotel Explorer reuse shim for GDI Public Contact Discovery 2.0.
 * Do not build a second contact research engine — re-export GDI.
 *
 * Surfe is never invoked here. Paid Exa search requires explicit opts.
 */
export {
  GDI_PUBLIC_CONTACT_DISCOVERY_V1,
  PUBLIC_CONTACT_RESEARCH_STATUS,
  PUBLIC_PHONE_KIND,
  buildPublicPersonContactQueryPlan,
  classifyPublicPhoneKind,
  enrichRecoveredChannelsWithOfficeMetadata,
  mergeCandidateSourceUrls,
  discoverPeerNamedContactsOnPages,
  discoverPublicContactChannels,
} from "../../group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";

export {
  createPublicContactSearchProvider,
  inspectPublicContactSearchCredentials,
} from "../../group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js";

export const HE_REUSES_GDI_PUBLIC_CONTACT_DISCOVERY_V1 =
  "he-reuse-gdi-public-contact-discovery-v1";
