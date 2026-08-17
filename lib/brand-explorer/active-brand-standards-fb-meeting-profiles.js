/**
 * Brand Standards — F&B Outlets Required + Meeting Space Required profiles for active brands.
 * Select values must match Brand Setup form: Yes | No | Preferred.
 */

/** @typedef {{
 *   fbOutletsRequired: "Yes" | "No" | "Preferred",
 *   meetingSpaceRequired: "Yes" | "No" | "Preferred",
 *   typicalFbOutlets?: number | null,
 *   typicalMeetingRooms?: number | null,
 * }} BrandStandardsFbMeetingProfile */

const BRAND_NAME_OVERRIDES = Object.freeze({
  "Ascend Hotel Collection": { fbOutletsRequired: "Preferred", meetingSpaceRequired: "Preferred", typicalFbOutlets: 1, typicalMeetingRooms: 1 },
  "Comfort Inn & Suites": { fbOutletsRequired: "Preferred", meetingSpaceRequired: "Yes", typicalFbOutlets: 1, typicalMeetingRooms: 2 },
  "Design Hotels": { fbOutletsRequired: "Yes", meetingSpaceRequired: "Preferred", typicalFbOutlets: 2, typicalMeetingRooms: 1 },
  "Everhome Suites": { fbOutletsRequired: "No", meetingSpaceRequired: "No", typicalFbOutlets: 0, typicalMeetingRooms: 0 },
  "Hampton by Hilton": { fbOutletsRequired: "Preferred", meetingSpaceRequired: "Yes", typicalFbOutlets: 1, typicalMeetingRooms: 2 },
  "Hilton Garden Inn": { fbOutletsRequired: "Yes", meetingSpaceRequired: "Yes", typicalFbOutlets: 1, typicalMeetingRooms: 2 },
  "Kimpton Hotels": { fbOutletsRequired: "Yes", meetingSpaceRequired: "Preferred", typicalFbOutlets: 1, typicalMeetingRooms: 2 },
  "Mr & Mrs Smith": { fbOutletsRequired: "Preferred", meetingSpaceRequired: "No", typicalFbOutlets: 1, typicalMeetingRooms: 0 },
  "Quality Inn": { fbOutletsRequired: "Preferred", meetingSpaceRequired: "Preferred", typicalFbOutlets: 1, typicalMeetingRooms: 1 },
  "Radisson Individuals by Choice": { fbOutletsRequired: "Preferred", meetingSpaceRequired: "Preferred", typicalFbOutlets: 1, typicalMeetingRooms: 1 },
  "Spark by Hilton": { fbOutletsRequired: "Preferred", meetingSpaceRequired: "Preferred", typicalFbOutlets: 1, typicalMeetingRooms: 1 },
  "Suburban Studios": { fbOutletsRequired: "No", meetingSpaceRequired: "No", typicalFbOutlets: 0, typicalMeetingRooms: 0 },
  "WoodSpring Suites": { fbOutletsRequired: "No", meetingSpaceRequired: "No", typicalFbOutlets: 0, typicalMeetingRooms: 0 },
});

function norm(value) {
  return String(value || "").trim();
}

function isExtendedStay(serviceModel, brandModel) {
  const svc = norm(serviceModel);
  const model = norm(brandModel);
  return svc === "Extended Stay" || model === "Extended Stay Brand" || model === "Serviced Apartments";
}

function isAllInclusive(serviceModel, brandModel) {
  const svc = norm(serviceModel);
  const model = norm(brandModel);
  return svc === "All-Inclusive" || model === "All-Inclusive Brand";
}

function isCollectionOrSoft(architecture, brandModel) {
  const arch = norm(architecture);
  const model = norm(brandModel);
  return arch === "Soft/Collection Brand" || model === "Collection Brand";
}

function isEconomy(chainScale) {
  return norm(chainScale) === "Economy";
}

function isSelectService(serviceModel) {
  return norm(serviceModel) === "Select-Service";
}

function isFullService(serviceModel) {
  const svc = norm(serviceModel);
  return svc === "Full-Service" || svc === "Lifestyle / Boutique";
}

/**
 * @param {{
 *   name?: string,
 *   parentCompany?: string,
 *   chainScale?: string,
 *   brandModel?: string,
 *   serviceModel?: string,
 *   architecture?: string,
 * }} brand
 * @returns {BrandStandardsFbMeetingProfile}
 */
export function buildBrandStandardsFbMeetingProfile(brand = {}) {
  const name = norm(brand.name);
  if (BRAND_NAME_OVERRIDES[name]) return { ...BRAND_NAME_OVERRIDES[name] };

  const chainScale = norm(brand.chainScale);
  const brandModel = norm(brand.brandModel);
  const serviceModel = norm(brand.serviceModel);
  const architecture = norm(brand.architecture);

  if (isAllInclusive(serviceModel, brandModel)) {
    return {
      fbOutletsRequired: "Yes",
      meetingSpaceRequired: "Preferred",
      typicalFbOutlets: 4,
      typicalMeetingRooms: 1,
    };
  }

  if (isExtendedStay(serviceModel, brandModel)) {
    if (isEconomy(chainScale)) {
      return {
        fbOutletsRequired: "No",
        meetingSpaceRequired: "No",
        typicalFbOutlets: 0,
        typicalMeetingRooms: 0,
      };
    }
    return {
      fbOutletsRequired: "Preferred",
      meetingSpaceRequired: "Preferred",
      typicalFbOutlets: 1,
      typicalMeetingRooms: 1,
    };
  }

  if (isCollectionOrSoft(architecture, brandModel)) {
    return {
      fbOutletsRequired: "Preferred",
      meetingSpaceRequired: "Preferred",
      typicalFbOutlets: 2,
      typicalMeetingRooms: 1,
    };
  }

  if (isSelectService(serviceModel)) {
    if (isEconomy(chainScale) || norm(chainScale) === "Midscale") {
      return {
        fbOutletsRequired: "Preferred",
        meetingSpaceRequired: "Preferred",
        typicalFbOutlets: 1,
        typicalMeetingRooms: 1,
      };
    }
    return {
      fbOutletsRequired: "Preferred",
      meetingSpaceRequired: "Yes",
      typicalFbOutlets: 1,
      typicalMeetingRooms: 2,
    };
  }

  if (isFullService(serviceModel) || norm(chainScale) === "Luxury") {
    return {
      fbOutletsRequired: "Yes",
      meetingSpaceRequired: "Yes",
      typicalFbOutlets: 2,
      typicalMeetingRooms: 2,
    };
  }

  return {
    fbOutletsRequired: "Preferred",
    meetingSpaceRequired: "Yes",
    typicalFbOutlets: 1,
    typicalMeetingRooms: 2,
  };
}

export function brandStandardsFbMeetingProfileToAirtableFields(profile) {
  const fields = {
    "F&B Outlets Required": profile.fbOutletsRequired,
    "Meeting Space Required": profile.meetingSpaceRequired,
  };
  if (profile.typicalFbOutlets != null) {
    fields["Typical Number of F&B Outlets"] = profile.typicalFbOutlets;
  }
  if (profile.typicalMeetingRooms != null) {
    fields["Typical Number of Meeting Rooms"] = profile.typicalMeetingRooms;
  }
  return fields;
}
