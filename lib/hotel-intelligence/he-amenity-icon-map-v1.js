/**
 * Canonical Hotel Explorer amenity icon map.
 * attribute_id → display_label → icon_id → category → display_priority
 *
 * Presentation policy: horizontal strip, max 20, never invent amenities.
 */

export const HE_AMENITY_ICON_MAP_VERSION = "he-amenity-icon-map-v1";

/** @typedef {{ id: string, label: string, iconId: string, category: string, priority: number }} AmenityIconMapping */

/** @type {AmenityIconMapping[]} */
export const HE_AMENITY_ICON_MAP = Object.freeze([
  { id: "meetingEventSpace", label: "Meeting & Event Space", iconId: "meetingEventSpace", category: "meetings", priority: 100 },
  { id: "meetingRooms", label: "Meeting Rooms", iconId: "meetingRooms", category: "meetings", priority: 98 },
  { id: "ballroom", label: "Ballroom", iconId: "ballroom", category: "meetings", priority: 96 },
  { id: "dining", label: "Restaurant", iconId: "dining", category: "f_and_b", priority: 90 },
  { id: "barLounge", label: "Bar / Lounge", iconId: "barLounge", category: "f_and_b", priority: 89 },
  { id: "spa", label: "Spa", iconId: "spa", category: "wellness", priority: 88 },
  { id: "outdoorPool", label: "Outdoor Pool", iconId: "outdoorPool", category: "recreation", priority: 86 },
  { id: "indoorPool", label: "Indoor Pool", iconId: "indoorPool", category: "recreation", priority: 85 },
  { id: "fitnessCenter", label: "Fitness Center", iconId: "fitnessCenter", category: "wellness", priority: 82 },
  { id: "executiveLounge", label: "Executive Lounge", iconId: "executiveLounge", category: "service", priority: 78 },
  { id: "concierge", label: "Concierge", iconId: "concierge", category: "service", priority: 76 },
  { id: "businessCenter", label: "Business Center", iconId: "businessCenter", category: "business", priority: 74 },
  { id: "freeParking", label: "Parking", iconId: "freeParking", category: "access", priority: 72 },
  { id: "on_site_parking", label: "On-Site Parking", iconId: "freeParking", category: "access", priority: 72 },
  { id: "valetParking", label: "Valet Parking", iconId: "valetParking", category: "access", priority: 71 },
  { id: "airportShuttle", label: "Airport Shuttle", iconId: "airportShuttle", category: "access", priority: 70 },
  { id: "metro_shuttle", label: "Complimentary Metro Shuttle", iconId: "airportShuttle", category: "access", priority: 69 },
  { id: "metroAccess", label: "Metro Access", iconId: "metroAccess", category: "access", priority: 68 },
  { id: "roomAccessibility", label: "Accessibility", iconId: "roomAccessibility", category: "accessibility", priority: 66 },
  { id: "room_accessibility", label: "Accessible Rooms & Facilities", iconId: "roomAccessibility", category: "accessibility", priority: 66 },
  { id: "beach", label: "Beach Access", iconId: "beach", category: "recreation", priority: 64 },
  { id: "roomService", label: "Room Service", iconId: "roomService", category: "f_and_b", priority: 55 },
  { id: "room_service", label: "Room Service", iconId: "roomService", category: "f_and_b", priority: 55 },
  { id: "freeWifi", label: "Wi-Fi", iconId: "freeWifi", category: "connectivity", priority: 54 },
  { id: "free_wifi", label: "Complimentary Wi-Fi", iconId: "freeWifi", category: "connectivity", priority: 54 },
  { id: "petsAllowed", label: "Pets Allowed", iconId: "petsAllowed", category: "service", priority: 50 },
  { id: "pet_friendly", label: "Pet-Friendly", iconId: "petsAllowed", category: "service", priority: 50 },
  { id: "business_center", label: "Business Center", iconId: "businessCenter", category: "business", priority: 74 },
  { id: "concierge", label: "Concierge Service", iconId: "concierge", category: "service", priority: 76 },
  { id: "evCharging", label: "EV Charging", iconId: "evCharging", category: "access", priority: 48 },
]);

export function iconIdsMustBeDistinctForUnrelatedAttributes(map = HE_AMENITY_ICON_MAP) {
  const criticalPairs = [
    ["barLounge", "dining"],
    ["ballroom", "meetingRooms"],
    ["metroAccess", "airportShuttle"],
    ["roomAccessibility", "freeWifi"],
    ["beach", "outdoorPool"],
  ];
  const byId = new Map(map.map((m) => [m.id, m.iconId]));
  const collisions = [];
  for (const [a, b] of criticalPairs) {
    if (byId.get(a) && byId.get(b) && byId.get(a) === byId.get(b)) {
      collisions.push(`${a}/${b} share icon ${byId.get(a)}`);
    }
  }
  return collisions;
}
