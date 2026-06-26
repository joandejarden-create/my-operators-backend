/**
 * Hotel amenity icons for the detail panel.
 * Primary set: SVGRepo assets in /icons/hotel-amenities/ (user-provided).
 * Fallback: inline SVG for amenities without a bundled asset.
 */
(function () {
  "use strict";

  var ICON_BASE = "/icons/hotel-amenities/";
  var SVG_ATTR =
    'viewBox="0 0 32 32" aria-hidden="true" focusable="false"';

  /** amenityId → image filename (SVGRepo exports) */
  var IMAGE_ICONS = {
    allInclusive: "all-inclusive.png",
    beach: "beach.png",
    boutique: "boutique.png",
    businessCenter: "business-center.png",
    concierge: "concierge.png",
    cribs: "cribs.png",
    digitalKey: "digital-key.png",
    dining: "restaurant.png",
    evCharging: "ev-charging.svg",
    executiveLounge: "executive-lounge.png",
    fitnessCenter: "fitness.png",
    freeBreakfast: "free-breakfast.svg",
    freeParking: "parking.png",
    indoorPool: "pool.png",
    meetingRooms: "meeting-rooms.png",
    meetingEventSpace: "meeting-event-space.png",
    weddingServices: "wedding-services.png",
    coffeeTeaInRoom: "coffee-tea-in-room.png",
    newHotel: "new-built.png",
    nonSmoking: "non-smoking.png",
    outdoorPool: "pool.png",
    petsAllowed: "pet-friendly.png",
    petsNotAllowed: "pets-not-allowed.png",
    pool: "pool.png",
    resort: "resort.png",
    roomService: "room-service.png",
    spa: "spa.png",
    valetParking: "parking.png",
    wakeUpCall: "wake-up-call.png",
    convenienceStore: "convenience-store-v2.png",
    kidsRecreation: "kids-recreation.png",
    sustainability: "sustainability.png",
    serviceRequest: "service-bell.png",
  };

  /** Normalized label → amenityId for icon lookup when id is missing */
  var LABEL_TO_AMENITY_ID = {
    "business center": "businessCenter",
    "ev charging": "evCharging",
    "electric car charging": "evCharging",
    "electric car charging station": "evCharging",
    "free breakfast": "freeBreakfast",
    "complimentary breakfast": "freeBreakfast",
    "pet friendly": "petsAllowed",
    "pet-friendly": "petsAllowed",
    "pet friendly rooms": "petsAllowed",
    "complimentary wi fi": "freeWifi",
    "complimentary wi-fi": "freeWifi",
    "complimentary wifi": "freeWifi",
    "free wifi": "freeWifi",
    "restaurant": "dining",
    "bar": "dining",
    "outdoor pool": "outdoorPool",
    "indoor pool": "indoorPool",
    "fitness center": "fitnessCenter",
    "room service": "roomService",
    "meeting space": "meetingRooms",
    "meeting rooms": "meetingRooms",
    "meeting event space": "meetingEventSpace",
    "event space": "meetingEventSpace",
    "wedding services": "weddingServices",
    "wedding service": "weddingServices",
    "coffee tea in room": "coffeeTeaInRoom",
    "coffee tea maker": "coffeeTeaInRoom",
    "coffee maker": "coffeeTeaInRoom",
    "new hotel": "newHotel",
    "newly built": "newHotel",
    "mobile key": "digitalKey",
    "digital key": "digitalKey",
    "wake up call": "wakeUpCall",
    "wake up calls": "wakeUpCall",
    "wake-up call": "wakeUpCall",
    "wake-up calls": "wakeUpCall",
    "convenience store": "convenienceStore",
    "market": "convenienceStore",
    "pantry": "convenienceStore",
    "gift shop": "convenienceStore",
    "children s recreation": "kidsRecreation",
    "children recreation": "kidsRecreation",
    "kids club": "kidsRecreation",
    "kids recreation": "kidsRecreation",
    "sustainability": "sustainability",
    "eco friendly": "sustainability",
    "service request": "serviceRequest",
    "guest services": "serviceRequest",
  };

  function normalizeLabelKey(label) {
    return String(label || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  /** Inline fallbacks when no image asset exists */
  var SVG_FALLBACKS = {
    freeWifi:
      '<svg ' + SVG_ATTR + '><path fill="none" stroke="currentColor" stroke-width="1.5" d="M4 14c6-5 18-5 24 0"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M8 18c4-3 12-3 16 0"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M12 22c2-1 6-1 8 0"/><circle fill="currentColor" cx="16" cy="25" r="1.2"/></svg>',
    petsAllowed:
      '<svg ' + SVG_ATTR + '><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="14" cy="15" r="4"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M8 11l-2-2M20 11l2-2M8 19l-2 2M20 19l2 2"/></svg>',
    businessCenter:
      '<svg ' + SVG_ATTR + '><rect fill="none" stroke="currentColor" stroke-width="1.5" x="6" y="8" width="20" height="14" rx="2"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M6 14h20"/></svg>',
    adjoiningRooms:
      '<svg ' + SVG_ATTR + '><rect fill="none" stroke="currentColor" stroke-width="1.5" x="5" y="10" width="9" height="14" rx="1"/><rect fill="none" stroke="currentColor" stroke-width="1.5" x="18" y="10" width="9" height="14" rx="1"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M14 17h4"/></svg>',
    airportShuttle:
      '<svg ' + SVG_ATTR + '><rect fill="none" stroke="currentColor" stroke-width="1.5" x="5" y="12" width="22" height="10" rx="2"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M9 12V9h14v3"/><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="10" cy="24" r="2"/><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="22" cy="24" r="2"/></svg>',
    evCharging:
      '<svg ' + SVG_ATTR + '><path fill="none" stroke="currentColor" stroke-width="1.5" d="M18 6l-4 8h4l-2 8"/><rect fill="none" stroke="currentColor" stroke-width="1.5" x="6" y="10" width="14" height="16" rx="2"/></svg>',
    golf:
      '<svg ' + SVG_ATTR + '><path fill="none" stroke="currentColor" stroke-width="1.5" d="M16 6v14"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M16 6l6 3-6 3"/><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="10" cy="24" r="2"/></svg>',
    casino:
      '<svg ' + SVG_ATTR + '><rect fill="none" stroke="currentColor" stroke-width="1.5" x="6" y="8" width="20" height="16" rx="2"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M16 12v8M12 16h8"/></svg>',
    conventionCenter:
      '<svg ' + SVG_ATTR + '><path fill="none" stroke="currentColor" stroke-width="1.5" d="M6 26V12l10-6 10 6v14"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M12 26v-8h8v8"/></svg>',
    tennis:
      '<svg ' + SVG_ATTR + '><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="16" cy="16" r="9"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M8 8c4 4 4 12 0 16"/></svg>',
    ski:
      '<svg ' + SVG_ATTR + '><path fill="none" stroke="currentColor" stroke-width="1.5" d="M6 24l10-12"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M16 12l10 12"/><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="22" cy="8" r="2"/></svg>',
    adultsOnly:
      '<svg ' + SVG_ATTR + '><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="16" cy="11" r="4"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M8 26v-3a8 8 0 0 1 16 0v3"/></svg>',
    roomAccessibility:
      '<svg ' + SVG_ATTR + '><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="16" cy="8" r="3"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M10 14h12v3H10z"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M16 17v7"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M12 24h8"/></svg>',
    default:
      '<svg ' + SVG_ATTR + '><circle fill="none" stroke="currentColor" stroke-width="1.5" cx="16" cy="16" r="10"/><path fill="none" stroke="currentColor" stroke-width="1.5" d="M11 16l3 3 7-7"/></svg>',
  };

  function iconImgHtml(filename) {
    return (
      '<img class="hdp-amenity-icon-img" src="' +
      ICON_BASE +
      filename +
      '" alt="" loading="lazy" decoding="async" />'
    );
  }

  function iconSvgForAmenityId(id) {
    var key = String(id || "").trim();
    if (IMAGE_ICONS[key]) return iconImgHtml(IMAGE_ICONS[key]);
    return SVG_FALLBACKS[key] || SVG_FALLBACKS.default;
  }

  function amenityIdFromLabel(label) {
    return LABEL_TO_AMENITY_ID[normalizeLabelKey(label)] || null;
  }

  function iconSvgForAmenityLabel(label) {
    var id = amenityIdFromLabel(label);
    if (!id) return null;
    return iconSvgForAmenityId(id);
  }

  window.HiltonAmenityIcons = {
    amenityIdFromLabel: amenityIdFromLabel,
    iconSvgForAmenityId: iconSvgForAmenityId,
    iconSvgForAmenityLabel: iconSvgForAmenityLabel,
  };
})();
