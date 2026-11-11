/**
 * Hotel Commercial Priorities V1 — selection-first portal contracts.
 * Customer-facing labels only; map to GDI enums server-side.
 */

export const HOTEL_COMMERCIAL_PRIORITIES_V1 =
  "gdi-hotel-commercial-priorities-v1";

export const TABLE_GDI_HOTEL_COMMERCIAL_PROFILES =
  "GDI Hotel Commercial Profiles";

export const TABLE_GDI_COMMERCIAL_PROFILE_CHANGE_HISTORY =
  "GDI Commercial Profile Change History";

/** Revenue priority chips → NEED_REVENUE_TYPE / research hints */
export const REVENUE_PRIORITY_OPTIONS = Object.freeze([
  { id: "GROUP_ROOM_NIGHTS", label: "Group Room Nights", revenueTypes: ["GROUP_GUESTROOMS"] },
  { id: "CORPORATE_TRANSIENT", label: "Corporate Transient", revenueTypes: ["CORPORATE_TRANSIENT_GUESTROOMS"] },
  { id: "MEETINGS_CONFERENCES", label: "Meetings & Conferences", revenueTypes: ["MEETING_SPACE"] },
  { id: "LOCAL_CATERING", label: "Local Catering", revenueTypes: ["LOCAL_CATERING"] },
  { id: "BANQUETS_CELEBRATIONS", label: "Banquets & Celebrations", revenueTypes: ["BANQUETS"] },
  { id: "WEDDINGS_SOCIAL", label: "Weddings & Social Groups", revenueTypes: ["WEDDING_GROUP_GUESTROOMS", "BANQUETS"] },
  { id: "EXTENDED_STAY", label: "Extended Stay", revenueTypes: ["EXTENDED_STAY"] },
  { id: "SPORTS_GROUPS", label: "Sports Groups", revenueTypes: ["GROUP_GUESTROOMS"], strategyBoost: ["sports_entertainment"] },
  { id: "ASSOCIATION_BUSINESS", label: "Association Business", revenueTypes: ["GROUP_GUESTROOMS", "MEETING_SPACE"], strategyBoost: ["association_events"] },
  { id: "GOVERNMENT_BUSINESS", label: "Government Business", revenueTypes: ["GROUP_GUESTROOMS", "MEETING_SPACE"], strategyBoost: ["government_public_sector"] },
  { id: "LEISURE_GROUPS", label: "Leisure Groups", revenueTypes: ["RESORT_GROUP", "GROUP_GUESTROOMS"] },
  { id: "OTHER", label: "Other", revenueTypes: ["OTHER_ANCILLARY"] },
]);

export const PEAK_ROOMS_PRESETS = Object.freeze([
  { id: "UNDER_10", label: "Under 10", min: 1, max: 9 },
  { id: "10_25", label: "10–25", min: 10, max: 25 },
  { id: "26_50", label: "26–50", min: 26, max: 50 },
  { id: "51_100", label: "51–100", min: 51, max: 100 },
  { id: "101_200", label: "101–200", min: 101, max: 200 },
  { id: "200_PLUS", label: "200+", min: 201, max: null },
  { id: "FLEXIBLE", label: "Flexible", min: null, max: null },
]);

export const BOOKING_WINDOW_PRESETS = Object.freeze([
  { id: "UNDER_30_DAYS", label: "Under 30 days" },
  { id: "1_3_MONTHS", label: "1–3 months" },
  { id: "3_6_MONTHS", label: "3–6 months" },
  { id: "6_12_MONTHS", label: "6–12 months" },
  { id: "12_PLUS_MONTHS", label: "12+ months" },
  { id: "FLEXIBLE", label: "Flexible" },
]);

export const MEETING_SPACE_NEED = Object.freeze([
  { id: "ROOMS_ONLY", label: "Rooms Only" },
  { id: "ROOMS_AND_MEETINGS", label: "Rooms + Meetings" },
  { id: "MEETINGS_CATERING_ONLY", label: "Meetings / Catering Only" },
  { id: "ANY", label: "Any" },
]);

export const STAY_DURATION_PRESETS = Object.freeze([
  { id: "1_NIGHT", label: "1 night" },
  { id: "2_3_NIGHTS", label: "2–3 nights" },
  { id: "4_7_NIGHTS", label: "4–7 nights" },
  { id: "7_PLUS_NIGHTS", label: "7+ nights" },
  { id: "FLEXIBLE", label: "Flexible" },
]);

export const DAY_PREFERENCE = Object.freeze([
  { id: "WEEKDAYS", label: "Weekdays" },
  { id: "WEEKENDS", label: "Weekends" },
  { id: "BOTH", label: "Both" },
  { id: "SPECIFIC", label: "Specific days" },
]);

export const WEEKDAYS = Object.freeze([
  "Mon",
  "Tue",
  "Wed",
  "Thu",
  "Fri",
  "Sat",
  "Sun",
]);

/** Hotel-friendly → GDI demand segment ids */
export const PRIORITY_SEGMENT_OPTIONS = Object.freeze([
  { id: "association_events", label: "Associations & Conferences" },
  { id: "scaling_companies", label: "Corporate Accounts" },
  { id: "meetings_incentives", label: "Corporate Meetings" },
  { id: "government_public_sector", label: "Government" },
  { id: "sports_entertainment", label: "Sports & Entertainment" },
  { id: "education_scientific", label: "Education & Universities" },
  { id: "projects_infrastructure", label: "Projects & Construction" },
  { id: "social_special_events", label: "Weddings & Social Groups" },
  { id: "local_catering_day_meetings", label: "Local Catering" },
  { id: "corporate_celebrations", label: "Corporate Celebrations" },
  { id: "travel_trade_group_intermediaries", label: "Travel Trade" },
  { id: "relocation_extended_stay", label: "Relocation & Extended Stay" },
  { id: "preferred_lodging_partnerships", label: "Venue Partnerships" },
  { id: "professional_event_planners", label: "Event Planners" },
]);

export const SEGMENT_MODE = Object.freeze({
  ALL_RELEVANT: "ALL_RELEVANT",
  SELECT_SPECIFIC: "SELECT_SPECIFIC",
  NO_PREFERENCE: "NO_PREFERENCE",
});

export const DEMAND_GENERATOR_CATEGORIES = Object.freeze([
  { id: "HOSPITALS_MEDICAL", label: "Hospitals / Medical" },
  { id: "UNIVERSITIES", label: "Universities" },
  { id: "CORPORATE_OFFICES", label: "Corporate Offices" },
  { id: "GOVERNMENT_AGENCIES", label: "Government Agencies" },
  { id: "CONVENTION_CENTERS", label: "Convention Centers" },
  { id: "WEDDING_VENUES", label: "Wedding Venues" },
  { id: "SPORTS_FACILITIES", label: "Sports Facilities" },
  { id: "ENTERTAINMENT_VENUES", label: "Entertainment Venues" },
  { id: "BUSINESS_PARKS", label: "Business Parks" },
  { id: "AIRPORTS_TRANSPORT", label: "Airports / Transportation" },
  { id: "OTHER", label: "Other" },
]);

export const KNOWN_ACCOUNT_STATUS = Object.freeze({
  EXISTING_CUSTOMER: "Existing Customer",
  ACTIVELY_PURSUING: "Actively Pursuing",
  FORMER_CUSTOMER: "Former Customer",
  COMPETITOR_CUSTOMER: "Competitor Customer",
  DO_NOT_PURSUE: "Do Not Pursue",
});

export const BUSINESS_TO_AVOID = Object.freeze([
  { id: "LARGE_ROOM_BLOCKS", label: "Large room blocks" },
  { id: "SMALL_GROUPS", label: "Small groups" },
  { id: "LATE_NIGHT_EVENTS", label: "Late-night events" },
  { id: "WEEKEND_HEAVY", label: "Weekend-heavy groups" },
  { id: "LONG_STAY", label: "Long-stay groups" },
  { id: "MEETING_INTENSIVE", label: "Meeting-space intensive groups" },
  { id: "CERTAIN_EVENT_TYPES", label: "Certain event types" },
  { id: "NO_RESTRICTIONS", label: "No restrictions" },
]);

export const HOTEL_CONSIDERATIONS = Object.freeze([
  { id: "LIMITED_MEETING_SPACE", label: "Limited meeting space" },
  { id: "LIMITED_COACH_PARKING", label: "Limited coach/bus parking" },
  { id: "LIMITED_GROUP_INVENTORY", label: "Limited group inventory" },
  { id: "SEASONAL_RESTRICTIONS", label: "Seasonal restrictions" },
  { id: "NOISE_RESTRICTIONS", label: "Venue/event noise restrictions" },
  { id: "ACCESSIBILITY", label: "Accessibility considerations" },
  { id: "RENOVATION_PERIODS", label: "Renovation periods" },
  { id: "NO_SPECIAL", label: "No special restrictions" },
]);

export const COMMERCIAL_TEAM_ROLES = Object.freeze([
  { id: "SALES_LEAD", label: "Sales Lead" },
  { id: "REVENUE_CONTACT", label: "Revenue Contact" },
  { id: "GM_SPONSOR", label: "GM / Executive Sponsor" },
  { id: "MARKETING_DIGITAL", label: "Marketing / Digital Contact" },
  { id: "HOTEL_COORDINATOR", label: "Hotel Coordinator" },
]);

export const PREFERENCE_STRENGTH = Object.freeze({
  PREFERENCE: "Preference",
  CONSTRAINT: "Constraint",
});

export const DATA_PROVENANCE = Object.freeze({
  HOTEL_CONFIRMED: "Hotel Confirmed",
  VERIFIED_PROPERTY_DATA: "Existing Verified Property Data",
  SUGGESTED_FROM_RESEARCH: "Suggested From Research",
  NOT_PROVIDED: "Not Provided",
  SYNTHETIC_TEST: "Synthetic Test",
});

export const MONTH_NEED_CHIPS = Object.freeze([
  { id: "GROUP_GUESTROOMS", label: "Guestrooms / Groups" },
  { id: "MEETING_SPACE", label: "Meetings" },
  { id: "LOCAL_CATERING", label: "Catering" },
  { id: "BANQUETS", label: "Banquets" },
  { id: "PRIVATE_DINING", label: "Private Dining" },
  { id: "EXTENDED_STAY", label: "Extended Stay" },
]);

export const PRIORITY_CHIPS = Object.freeze([
  { id: "CRITICAL", label: "Critical" },
  { id: "HIGH", label: "High" },
  { id: "MEDIUM", label: "Medium" },
]);

export function emptyCommercialProfile(hotelId) {
  return {
    version: HOTEL_COMMERCIAL_PRIORITIES_V1,
    hotelId,
    profileId: null,
    airtableRecordId: null,
    currentVersion: 1,
    revenuePriorities: [],
    peakRoomsPreset: null,
    peakRoomsCustom: null,
    peakRoomsStrength: PREFERENCE_STRENGTH.PREFERENCE,
    bookingWindow: null,
    bookingWindowStrength: PREFERENCE_STRENGTH.PREFERENCE,
    meetingSpaceNeed: null,
    stayDuration: null,
    dayPreference: null,
    specificDays: [],
    segmentMode: SEGMENT_MODE.NO_PREFERENCE,
    prioritySegments: [],
    excludedSegments: [],
    demandGeneratorCategories: [],
    knownOrganizations: [],
    knownAccounts: [],
    businessToAvoid: [],
    hotelConsiderations: [],
    restrictionNotes: "",
    commercialTeam: {},
    provenance: DATA_PROVENANCE.NOT_PROVIDED,
    isSyntheticTest: false,
    lastConfirmedAt: null,
    updatedAt: null,
    updatedBy: null,
  };
}

export function getCommercialPrioritiesEnums() {
  return {
    version: HOTEL_COMMERCIAL_PRIORITIES_V1,
    revenuePriorities: REVENUE_PRIORITY_OPTIONS,
    peakRooms: PEAK_ROOMS_PRESETS,
    bookingWindow: BOOKING_WINDOW_PRESETS,
    meetingSpaceNeed: MEETING_SPACE_NEED,
    stayDuration: STAY_DURATION_PRESETS,
    dayPreference: DAY_PREFERENCE,
    weekdays: WEEKDAYS,
    prioritySegments: PRIORITY_SEGMENT_OPTIONS,
    segmentMode: Object.values(SEGMENT_MODE),
    demandGenerators: DEMAND_GENERATOR_CATEGORIES,
    knownAccountStatus: Object.values(KNOWN_ACCOUNT_STATUS),
    businessToAvoid: BUSINESS_TO_AVOID,
    hotelConsiderations: HOTEL_CONSIDERATIONS,
    commercialTeamRoles: COMMERCIAL_TEAM_ROLES,
    preferenceStrength: Object.values(PREFERENCE_STRENGTH),
    provenance: Object.values(DATA_PROVENANCE),
    monthNeedChips: MONTH_NEED_CHIPS,
    priorityChips: PRIORITY_CHIPS,
  };
}

/**
 * Merge hotel-wide commercial profile with optional need-period overrides.
 * Period overrides never mutate the hotel-wide profile.
 */
export function resolveEffectiveCommercialProfile(hotelProfile, needPeriod = null) {
  const base = hotelProfile && typeof hotelProfile === "object" ? { ...hotelProfile } : {};
  const overrides =
    needPeriod?.preferenceOverrides && typeof needPeriod.preferenceOverrides === "object"
      ? needPeriod.preferenceOverrides
      : null;
  if (!overrides) {
    return {
      ...base,
      effectiveSource: "HOTEL_WIDE",
      periodOverridesApplied: false,
    };
  }
  const merged = { ...base };
  for (const [k, v] of Object.entries(overrides)) {
    if (v !== undefined && v !== null && v !== "") merged[k] = v;
  }
  return {
    ...merged,
    effectiveSource: "PERIOD_OVERRIDE",
    periodOverridesApplied: true,
    periodOverrideKeys: Object.keys(overrides),
  };
}
