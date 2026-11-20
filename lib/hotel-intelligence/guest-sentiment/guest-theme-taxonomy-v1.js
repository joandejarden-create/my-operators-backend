/**
 * Reusable hospitality guest-experience theme taxonomy (HE Guest Sentiment).
 * Themes may carry both positive and negative evidence — not complaint-only.
 */

export const GUEST_THEME_TAXONOMY_V1 = "guest-theme-taxonomy-v1";

/** @type {ReadonlyArray<{ id: string, label: string, adpRelated?: string[] }>} */
export const GUEST_THEME_TAXONOMY = Object.freeze([
  { id: "staff_service", label: "Staff and service", adpRelated: ["service"] },
  { id: "guestroom_condition", label: "Guestroom condition", adpRelated: ["rooms"] },
  { id: "cleanliness", label: "Cleanliness", adpRelated: ["cleanliness"] },
  { id: "sleep_noise", label: "Sleep quality and noise", adpRelated: ["sleep"] },
  { id: "location_access", label: "Location and accessibility", adpRelated: ["location"] },
  { id: "food_beverage", label: "Food and beverage", adpRelated: ["fb"] },
  { id: "breakfast", label: "Breakfast", adpRelated: ["breakfast"] },
  { id: "value", label: "Value for money", adpRelated: ["value"] },
  { id: "amenities", label: "Amenities and facilities", adpRelated: ["amenities", "pool", "fitness"] },
  { id: "meetings_events", label: "Meetings and events", adpRelated: ["meetings"] },
  { id: "maintenance", label: "Maintenance", adpRelated: ["maintenance"] },
  { id: "accessibility", label: "Accessibility", adpRelated: ["accessibility"] },
  { id: "arrival_departure", label: "Arrival, check-in, and departure", adpRelated: ["checkin"] },
]);

export const GUEST_THEME_BY_ID = Object.freeze(
  Object.fromEntries(GUEST_THEME_TAXONOMY.map((t) => [t.id, t]))
);

/** Map common platform category labels → taxonomy ids */
export const PLATFORM_CATEGORY_TO_THEME = Object.freeze({
  location: "location_access",
  rooms: "guestroom_condition",
  room: "guestroom_condition",
  value: "value",
  "value for money": "value",
  cleanliness: "cleanliness",
  service: "staff_service",
  staff: "staff_service",
  "sleep quality": "sleep_noise",
  sleep: "sleep_noise",
  facilities: "amenities",
  comfort: "guestroom_condition",
  breakfast: "breakfast",
  food: "food_beverage",
});

export function mapPlatformCategoryToTheme(label) {
  const key = String(label || "")
    .toLowerCase()
    .trim();
  return PLATFORM_CATEGORY_TO_THEME[key] || null;
}

/** Minimum supporting observations before showing precise % claims */
export const MIN_SAMPLE_FOR_PERCENT = 30;
