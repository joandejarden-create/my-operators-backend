/**
 * International source families — public/legal sources only. No Apify.
 */

export const INTERNATIONAL_SOURCE_FAMILIES = Object.freeze([
  {
    id: "PCO_SITE",
    label: "PCO websites",
    useFor: ["controller_discovery", "housing_authority", "contact_path"],
  },
  {
    id: "DMC_SITE",
    label: "DMC websites",
    useFor: ["controller_discovery", "destination_logistics"],
  },
  {
    id: "ASSOCIATION_SECRETARIAT",
    label: "Association secretariats",
    useFor: ["controller_discovery", "cycle_timing", "contact_path"],
  },
  {
    id: "EVENT_AGENCY",
    label: "Event agencies",
    useFor: ["controller_discovery"],
  },
  {
    id: "REGISTRATION_PORTAL",
    label: "Registration portals",
    useFor: ["housing_step", "decision_window"],
  },
  {
    id: "CONFERENCE_PLATFORM",
    label: "Conference platforms",
    useFor: ["generator_discovery", "program_pdf"],
  },
  {
    id: "CVB_CALENDAR",
    label: "Convention / tourism bureau calendars",
    useFor: ["generator_discovery", "controller_discovery", "process_intelligence"],
    notAutoReady: true,
  },
  {
    id: "VENUE_CALENDAR",
    label: "Official venue calendars",
    useFor: ["generator_discovery", "process_intelligence"],
    notAutoReady: true,
  },
  {
    id: "UNIVERSITY_CONGRESS",
    label: "University congress offices",
    useFor: ["controller_discovery", "generator_discovery"],
  },
  {
    id: "PUBLIC_PROCUREMENT",
    label: "Public procurement portals",
    useFor: ["rfp", "lodging_selection_active", "buyer_path"],
  },
  {
    id: "EU_PROCUREMENT",
    label: "EU / national / regional procurement",
    useFor: ["rfp", "lodging_selection_active"],
  },
  {
    id: "MEDICAL_SOCIETY",
    label: "Medical society sites",
    useFor: ["generator_discovery", "secretariat"],
  },
  {
    id: "PROFESSIONAL_ASSOCIATION",
    label: "Professional / trade associations",
    useFor: ["generator_discovery", "secretariat", "cycle"],
  },
  {
    id: "CORPORATE_PRESSROOM",
    label: "Corporate pressrooms",
    useFor: ["account_first_triggers"],
  },
  {
    id: "PROJECT_TENDER",
    label: "Project tender / infrastructure pages",
    useFor: ["project_workforce_demand"],
  },
  {
    id: "EVENT_MANUAL_PDF",
    label: "PDF programs / manuals / circulars",
    useFor: ["document_first_extraction", "housing_guide", "exhibitor_manual"],
    note: "PDF existence ≠ extracted evidence; prefer structured extraction",
  },
]);

export const DOCUMENT_FIRST_TARGETS = Object.freeze([
  "PDF_PROGRAM",
  "DELEGATE_GUIDE",
  "EXHIBITOR_MANUAL",
  "SPONSORSHIP_PROSPECTUS",
  "REGISTRATION_BROCHURE",
  "HOUSING_GUIDE",
  "PROCUREMENT_NOTICE",
  "TENDER_DOCUMENT",
  "ASSOCIATION_CIRCULAR",
]);

export function listInternationalSourceFamilies() {
  return INTERNATIONAL_SOURCE_FAMILIES;
}
