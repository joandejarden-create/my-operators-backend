/**
 * Factory build queue for Wyndham soft brands (Dazzler, Trademark).
 * Not Active/Live. Not public-full. Use Factory Preview until promotion.
 */
export const WYNDHAM_FACTORY_BUILD_QUEUE = Object.freeze([
  Object.freeze({
    slug: "dazzler-by-wyndham",
    name: "Dazzler by Wyndham",
    recordId: "rec5CNMM4ZUD7ZHlM",
    parent: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    recommendedStatusWhileInFactory: "Under Review",
    status: "promoted_active_pending_public_full_visuals",
    notes:
      "Presentation pack applied (94 rows). Brand Status Active + release fields + intentional restore. Public-full still blocked on gallery/openings visual gates.",
  }),
  Object.freeze({
    slug: "trademark-collection-by-wyndham",
    name: "Trademark Collection by Wyndham",
    recordId: "recob7tgHRryRSbeO",
    parent: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    recommendedStatusWhileInFactory: "Under Review",
    status: "promoted_active_pending_public_full_visuals",
    notes:
      "Presentation pack applied (94 rows). Brand Status Active + release fields + intentional restore. Public-full still blocked on gallery/openings visual gates.",
  }),
]);

export function getWyndhamFactoryQueueEntry(slug) {
  const s = String(slug || "").trim().toLowerCase();
  return WYNDHAM_FACTORY_BUILD_QUEUE.find((b) => b.slug === s) || null;
}

export default {
  WYNDHAM_FACTORY_BUILD_QUEUE,
  getWyndhamFactoryQueueEntry,
};
