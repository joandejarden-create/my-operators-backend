/**
 * CoStar Contact Data Export → GTM Airtable field mapping.
 */
export const GTM_CONTACT_TABLE =
  process.env.AIRTABLE_GTM_COSTAR_CONTACTS_TABLE || "Contacts";

/** Excel / Airtable column names (1:1). */
export const MAP_GTM_CONTACT = {
  name: "Name",
  email: "Email",
  linkedIn: "LinkedIn",
  company: "Company",
  title: "Title",
  specialty: "Specialty",
  propertyTypeFocus: "Property Type Focus",
  phone: "Phone",
  businessPhone: "Business Phone",
  mobilePhone: "Mobile Phone",
  phoneVerificationTier: "Phone Verification Tier",
  buildingName: "Building Name",
  address: "Address",
  city: "City",
  state: "State",
  postalCode: "Postal Code",
  country: "Country",
  website: "Website",
  leaseTransactions3Y: "Lease Transactions (3Y)",
  leaseTransactionsSf3Y: "Lease Transactions SF (3Y)",
  leaseListings: "Lease Listings",
  leaseListingsPortfolioSf: "Lease Listings Portfolio SF",
  leaseListingsAvailableSf: "Lease Listings Available SF",
  saleTransactions3Y: "Sale Transactions (3Y)",
  saleTransactionsSf3Y: "Sale Transactions SF (3Y)",
  saleTransactionsVolume3Y: "Sale Transactions Volume (3Y)",
  saleListings: "Sale Listings",
  saleListingsSf: "Sale Listings SF",
  outreachStatus: "Outreach Status",
  internalNotes: "Internal Notes",
  sourceFile: "Source File",
  contactDedupeKey: "Contact Dedupe Key",
  calaHotelContact: "CALA Hotel Contact",
  calaPropertyCount: "CALA Property Count",
  calaCountriesSummary: "CALA Countries",
  matchedOwnerName: "Matched Owner Name",
  calaMatchType: "CALA Match Type",
  ownerTargets: "Owner Targets",
  contactRelevance: "Contact Relevance",
  verificationTier: "Verification Tier",
  verificationSource: "Verification Source",
  registrySystem: "Registry System",
  registryCountry: "Registry Country",
  registryEntityName: "Registry Entity Name",
  registryEntityId: "Registry Entity ID",
  legalRepresentativeName: "Legal Representative Name",
  verificationUrl: "Verification URL",
  verifiedAt: "Verified At",
};

export const VAL_GTM_CONTACT_VERIFICATION_TIER = ["V1", "V1R", "V2", "V3"];

/** VP2=verified mobile; VP1=verified direct business line; VP3=entity switchboard/toll-free. */
export const VAL_GTM_PHONE_VERIFICATION_TIER = ["VP1", "VP2", "VP3"];

export const VAL_GTM_CONTACT_VERIFICATION_SOURCE = [
  "costar_contact",
  "public_registry",
  "company_website",
  "linkedin",
  "manual",
];

export const VAL_GTM_CONTACT_RELEVANCE = ["hospitality", "broker", "other", "unknown"];

export const VAL_GTM_CONTACT_CALA_HOTEL = ["yes", "no", "unknown"];

export const VAL_GTM_CONTACT_OUTREACH_STATUS = [
  "not_contacted",
  "researching",
  "intro_sent",
  "responded",
  "meeting_scheduled",
  "passed",
  "parked",
];

/** Ordered headers from CoStar ContactDataExport sheet. */
export const COSTAR_CONTACT_HEADERS = [
  MAP_GTM_CONTACT.name,
  MAP_GTM_CONTACT.email,
  MAP_GTM_CONTACT.linkedIn,
  MAP_GTM_CONTACT.company,
  MAP_GTM_CONTACT.title,
  MAP_GTM_CONTACT.specialty,
  MAP_GTM_CONTACT.propertyTypeFocus,
  MAP_GTM_CONTACT.phone,
  MAP_GTM_CONTACT.buildingName,
  MAP_GTM_CONTACT.address,
  MAP_GTM_CONTACT.city,
  MAP_GTM_CONTACT.state,
  MAP_GTM_CONTACT.postalCode,
  MAP_GTM_CONTACT.country,
  MAP_GTM_CONTACT.website,
  MAP_GTM_CONTACT.leaseTransactions3Y,
  MAP_GTM_CONTACT.leaseTransactionsSf3Y,
  MAP_GTM_CONTACT.leaseListings,
  MAP_GTM_CONTACT.leaseListingsPortfolioSf,
  MAP_GTM_CONTACT.leaseListingsAvailableSf,
  MAP_GTM_CONTACT.saleTransactions3Y,
  MAP_GTM_CONTACT.saleTransactionsSf3Y,
  MAP_GTM_CONTACT.saleTransactionsVolume3Y,
  MAP_GTM_CONTACT.saleListings,
  MAP_GTM_CONTACT.saleListingsSf,
];
