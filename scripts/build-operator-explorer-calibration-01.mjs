#!/usr/bin/env node
/**
 * Operator Explorer Calibration-01 dry-run builder.
 * Harvests existing Operator Intelligence + Airtable read snapshot + curated Track 2 seeds.
 * NO Airtable writes.
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { classifyExplorerReadiness } from "../lib/operator-explorer/readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const outRoot = join(root, "data/operator-explorer/calibration-01");
const reports = join(root, "reports");

function ensureDir(p) {
  mkdirSync(p, { recursive: true });
}
function writeJson(p, data) {
  ensureDir(dirname(p));
  writeFileSync(p, JSON.stringify(data, null, 2));
}
function readJson(p) {
  return JSON.parse(readFileSync(p, "utf8"));
}

const ENTITIES = [
  // Track 1
  { id: "recF5Z87OAqFgndoq", provisionalId: null, canonicalName: "Arbor Lodging (CALA)", track: 1, aliases: ["Arbor Lodging", "Arbor Lodging Partners"], parent: "Arbor Lodging Partners", website: "https://arborlodging.com", operatingModel: "Third-Party", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "recWPKu5laVZxsvpn", provisionalId: null, canonicalName: "Hotel Equities (CALA)", track: 1, aliases: ["Hotel Equities"], parent: "Hotel Equities", website: "https://hotelequities.com/cala.htm", operatingModel: "Third-Party", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "reciI2tYQBfMoMK9G", provisionalId: null, canonicalName: "GHL Hoteles (GHL Holding)", track: 1, aliases: ["GHL Hoteles", "GHL"], parent: "GHL Holding", website: "https://www.ghlhoteles.com", operatingModel: "Third-Party", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "recGWxIJqnYHkJZFD", provisionalId: null, canonicalName: "Aimbridge Hospitality (LATAM)", track: 1, aliases: ["Aimbridge", "Aimbridge Hospitality"], parent: "Aimbridge Hospitality", website: "https://www.aimbridge.com", operatingModel: "Third-Party", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "rec3TUHT9Z4AnFp5P", provisionalId: null, canonicalName: "Playa Hotels & Resorts", track: 1, aliases: ["Playa"], parent: "Playa Hotels & Resorts", website: "https://www.playaresorts.com", operatingModel: "Integrated Owner / Brand / Operator", managementAvailability: "Conditional / Scoped", masterStatus: "existing" },
  { id: "reckyv9O0Y3auYpJJ", provisionalId: null, canonicalName: "Grupo Hotelero Santa Fe", track: 1, aliases: ["Santa Fe", "GHSF"], parent: "Grupo Hotelero Santa Fe", website: "https://www.gsf-hoteles.com", operatingModel: "Hybrid", managementAvailability: "Conditional / Scoped", masterStatus: "existing" },
  { id: "recLjxtxIIVJaGbXK", provisionalId: null, canonicalName: "Highgate", track: 1, aliases: ["Highgate Hotels"], parent: "Highgate", website: "https://highgate.com", operatingModel: "Third-Party", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "recKVILWcRLqrQlWs", provisionalId: null, canonicalName: "Driftwood Hospitality Management", track: 1, aliases: ["Driftwood"], parent: "Driftwood Hospitality", website: "https://www.driftwoodhospitality.com", operatingModel: "Third-Party", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "recfwDdU5t9h4uFnZ", provisionalId: null, canonicalName: "Atlantica Hotels International (AHI)", track: 1, aliases: ["Atlantica", "AHI"], parent: "Atlantica Hospitality International", website: "https://www.ahi.com.br", operatingModel: "Hybrid", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "recQ6Cf8O2z0tiqBz", provisionalId: null, canonicalName: "Cenote Azul Operadores", track: 1, aliases: ["Cenote Azul"], parent: null, website: "https://cenoteazul.mx", operatingModel: "Third-Party", managementAvailability: "Conditional / Scoped", masterStatus: "existing", researchNote: "Public footprint weak historically" },
  { id: "recwEHUotSGpfkZEJ", provisionalId: null, canonicalName: "Grupo Iberostar", track: 1, aliases: ["Iberostar", "Iberostar Hotels & Resorts"], parent: "Grupo Iberostar", website: "https://www.iberostar.com", operatingModel: "Integrated Owner / Brand / Operator", managementAvailability: "Conditional / Scoped", masterStatus: "existing" },
  { id: "recjgHXqTJktijFUR", provisionalId: null, canonicalName: "Álvarez Argüelles Hoteles", track: 1, aliases: ["Alvarez Arguelles"], parent: null, website: null, operatingModel: "Third-Party", managementAvailability: "Confirmed Direct Management", masterStatus: "existing", researchStatusHint: "Research Stage" },
  // Track 2
  { id: "recGmiPhRt6hiayd9", provisionalId: null, canonicalName: "Marriott International (Managed)", track: 2, aliases: ["Marriott", "MxM", "Managed by Marriott", "Marriott International, Inc."], parent: "Marriott International", website: "https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott", operatingModel: "Hybrid", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "rec3Uwxe6ovpiokuN", provisionalId: null, canonicalName: "Hilton (Managed)", track: 2, aliases: ["Hilton", "Hilton Worldwide", "Hilton Management Services", "HMS"], parent: "Hilton Worldwide", website: "https://www.hilton.com", operatingModel: "Hybrid", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "recF2WqLqNVyKGz9E", provisionalId: null, canonicalName: "Accor (Managed)", track: 2, aliases: ["Accor", "AccorHotels", "Accor Group"], parent: "Accor", website: "https://group.accor.com/en/hotel-development", operatingModel: "Hybrid", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: "rec7IXYQYpKMYsrDl", provisionalId: null, canonicalName: "IHG Hotels & Resorts (Managed)", track: 2, aliases: ["IHG", "InterContinental Hotels Group"], parent: "IHG", website: "https://www.ihg.com", operatingModel: "Hybrid", managementAvailability: "Conditional / Scoped", masterStatus: "existing" },
  { id: null, provisionalId: "provisional_operator_hyatt", canonicalName: "Hyatt (Managed)", track: 2, aliases: ["Hyatt", "Hyatt Hotels Corporation"], parent: "Hyatt Hotels Corporation", website: "https://www.hyatt.com", operatingModel: "Hybrid", managementAvailability: "Confirmed Direct Management", masterStatus: "provisional" },
  { id: "rec8SrT3VjRkkYTxm", provisionalId: null, canonicalName: "Minor Hotels (Managed)", track: 2, aliases: ["Minor Hotels", "Minor Hotel Group", "NH Hotels"], parent: "Minor International", website: "https://www.minorhotels.com", operatingModel: "Hybrid", managementAvailability: "Confirmed Direct Management", masterStatus: "existing" },
  { id: null, provisionalId: "provisional_operator_sonesta", canonicalName: "Sonesta International", track: 2, aliases: ["Sonesta"], parent: "Sonesta International Hotels Corporation", website: "https://www.sonesta.com", operatingModel: "Brand / Operator", managementAvailability: "Confirmed Direct Management", masterStatus: "provisional" },
  { id: null, provisionalId: "provisional_operator_four_seasons", canonicalName: "Four Seasons Hotels and Resorts", track: 2, aliases: ["Four Seasons"], parent: "Four Seasons Hotels and Resorts", website: "https://www.fourseasons.com", operatingModel: "Brand / Operator", managementAvailability: "Confirmed Direct Management", masterStatus: "provisional" },
  { id: null, provisionalId: "provisional_operator_rosewood", canonicalName: "Rosewood Hotel Group", track: 2, aliases: ["Rosewood"], parent: "Rosewood Hotel Group", website: "https://www.rosewoodhotels.com", operatingModel: "Brand / Operator", managementAvailability: "Confirmed Direct Management", masterStatus: "provisional" },
  { id: null, provisionalId: "provisional_operator_mandarin_oriental", canonicalName: "Mandarin Oriental Hotel Group", track: 2, aliases: ["Mandarin Oriental"], parent: "Mandarin Oriental Hotel Group", website: "https://www.mandarinoriental.com", operatingModel: "Integrated Brand / Operator", managementAvailability: "Conditional / Scoped", masterStatus: "provisional" },
  { id: null, provisionalId: "provisional_operator_radisson", canonicalName: "Radisson Hotel Group", track: 2, aliases: ["Radisson", "Radisson Hotels"], parent: "Radisson Hotel Group", website: "https://www.radissonhotels.com", operatingModel: "Hybrid", managementAvailability: "Conditional / Scoped", masterStatus: "provisional" },
  { id: null, provisionalId: "provisional_operator_melia", canonicalName: "Meliá Hotels International", track: 2, aliases: ["Melia", "Meliá"], parent: "Meliá Hotels International", website: "https://www.melia.com", operatingModel: "Hybrid", managementAvailability: "Conditional / Scoped", masterStatus: "provisional" },
  { id: null, provisionalId: "provisional_operator_auberge", canonicalName: "Auberge Resorts Collection", track: 2, aliases: ["Auberge"], parent: "Auberge Resorts Collection", website: "https://aubergeresorts.com", operatingModel: "Brand / Operator", managementAvailability: "Confirmed Direct Management", masterStatus: "provisional" },
  { id: null, provisionalId: "provisional_operator_shangri_la", canonicalName: "Shangri-La Group", track: 2, aliases: ["Shangri-La"], parent: "Shangri-La Asia", website: "https://www.shangri-la.com", operatingModel: "Integrated Brand / Operator", managementAvailability: "Conditional / Scoped", masterStatus: "provisional" },
  { id: null, provisionalId: "provisional_operator_barcelo", canonicalName: "Barceló Hotel Group", track: 2, aliases: ["Barcelo", "Barceló"], parent: "Barceló Hotel Group", website: "https://www.barcelo.com", operatingModel: "Integrated Owner / Brand / Operator", managementAvailability: "Conditional / Scoped", masterStatus: "provisional" },
];

function entityKey(e) {
  return e.id || e.provisionalId;
}

// --- curated Track 2 assignment / BM capability seeds (official-source anchored; dry-run) ---
const TRACK2_CURATED = {
  provisional_operator_hyatt: {
    sources: [
      { id: "src_c01_hyatt_01", title: "Hyatt Development", url: "https://about.hyatt.com/en/hyatt-development.html", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Park Hyatt New York", country: "United States", city: "New York", brand: "Park Hyatt", brandParent: "Hyatt", assignmentStatus: "Current", urbanOrResort: "Urban", managementStructure: "Brand-managed", developmentContext: "Existing Operation / Takeover", sourceIds: ["src_c01_hyatt_01"] },
      { propertyName: "Grand Hyatt São Paulo", country: "Brazil", city: "São Paulo", brand: "Grand Hyatt", brandParent: "Hyatt", assignmentStatus: "Current", urbanOrResort: "Urban", managementStructure: "Brand-managed", sourceIds: ["src_c01_hyatt_01"] },
      { propertyName: "Hyatt Ziva Cancun", country: "Mexico", city: "Cancún", brand: "Hyatt Ziva", brandParent: "Hyatt", assignmentStatus: "Current", urbanOrResort: "Resort", allInclusive: true, managementStructure: "Brand-managed / owner-operator adjacency", sourceIds: ["src_c01_hyatt_01"], limitations: "Playa acquisition context — confirm contracting entity" },
    ],
    brandManagedCapability: [
      { brand: "Park Hyatt", brandParent: "Hyatt", relationshipType: "Brand Managed Capability", geography: "Global selective", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_hyatt_01"] },
      { brand: "Grand Hyatt", brandParent: "Hyatt", relationshipType: "Brand Managed Capability", geography: "Global selective", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_hyatt_01"] },
    ],
    structures: ["Brand-managed", "Franchise + Operator", "Owner-Operated"],
  },
  provisional_operator_sonesta: {
    sources: [
      { id: "src_c01_sonesta_01", title: "Sonesta Hotels", url: "https://www.sonesta.com", authority: "primary_authoritative" },
      { id: "src_c01_sonesta_02", title: "Sonesta extends MFA with GHL", url: "https://newsroom.sonesta.com/franchising/sonesta-extends-master-franchise-agreement-with-ghl-hoteles-through-2034/", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Royal Sonesta Boston", country: "United States", city: "Boston", brand: "Royal Sonesta", brandParent: "Sonesta", assignmentStatus: "Current", urbanOrResort: "Urban", managementStructure: "Brand-managed", sourceIds: ["src_c01_sonesta_01"] },
      { propertyName: "Sonesta Select examples (US portfolio)", country: "United States", city: "Various", brand: "Sonesta Select", brandParent: "Sonesta", assignmentStatus: "Current", urbanOrResort: "Urban", managementStructure: "Brand-managed / franchise mix", sourceIds: ["src_c01_sonesta_01"], limitations: "Aggregate portfolio claim — expand named hotels in apply wave" },
    ],
    brandManagedCapability: [
      { brand: "Royal Sonesta", brandParent: "Sonesta", relationshipType: "Brand Managed Capability", geography: "Americas", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_sonesta_01"] },
    ],
    structures: ["Brand-managed", "Franchise Only", "Franchise + Operator"],
  },
  provisional_operator_four_seasons: {
    sources: [
      { id: "src_c01_fs_01", title: "Four Seasons — About", url: "https://www.fourseasons.com/about_four_seasons/", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Four Seasons Hotel México City", country: "Mexico", city: "Mexico City", brand: "Four Seasons", brandParent: "Four Seasons", assignmentStatus: "Current", urbanOrResort: "Urban", chainScale: "Luxury", managementStructure: "Brand-managed", sourceIds: ["src_c01_fs_01"] },
      { propertyName: "Four Seasons Resort Costa Rica at Peninsula Papagayo", country: "Costa Rica", city: "Papagayo", brand: "Four Seasons", brandParent: "Four Seasons", assignmentStatus: "Current", urbanOrResort: "Resort", chainScale: "Luxury", managementStructure: "Brand-managed", sourceIds: ["src_c01_fs_01"] },
      { propertyName: "Four Seasons Hotel Bogotá", country: "Colombia", city: "Bogotá", brand: "Four Seasons", brandParent: "Four Seasons", assignmentStatus: "Current", urbanOrResort: "Urban", chainScale: "Luxury", managementStructure: "Brand-managed", sourceIds: ["src_c01_fs_01"] },
    ],
    brandManagedCapability: [
      { brand: "Four Seasons", brandParent: "Four Seasons", relationshipType: "Brand Managed Capability", geography: "Global", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_fs_01"], notes: "Primary model is managed hotels for owners" },
    ],
    structures: ["Brand-managed"],
  },
  provisional_operator_rosewood: {
    sources: [
      { id: "src_c01_rw_01", title: "Rosewood Hotels", url: "https://www.rosewoodhotels.com", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Rosewood São Paulo", country: "Brazil", city: "São Paulo", brand: "Rosewood", brandParent: "Rosewood Hotel Group", assignmentStatus: "Current", urbanOrResort: "Urban", chainScale: "Luxury", managementStructure: "Brand-managed", sourceIds: ["src_c01_rw_01"] },
      { propertyName: "Rosewood Mayakoba", country: "Mexico", city: "Playa del Carmen", brand: "Rosewood", brandParent: "Rosewood Hotel Group", assignmentStatus: "Current", urbanOrResort: "Resort", chainScale: "Luxury", managementStructure: "Brand-managed", brandedResidences: true, sourceIds: ["src_c01_rw_01"] },
    ],
    brandManagedCapability: [
      { brand: "Rosewood", brandParent: "Rosewood Hotel Group", relationshipType: "Brand Managed Capability", geography: "Global selective luxury", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_rw_01"] },
    ],
    structures: ["Brand-managed"],
  },
  provisional_operator_mandarin_oriental: {
    sources: [
      { id: "src_c01_mo_01", title: "Mandarin Oriental", url: "https://www.mandarinoriental.com", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Mandarin Oriental, Bodrum", country: "Turkey", city: "Bodrum", brand: "Mandarin Oriental", brandParent: "Mandarin Oriental", assignmentStatus: "Current", urbanOrResort: "Resort", chainScale: "Luxury", managementStructure: "Brand-managed / owned mix", sourceIds: ["src_c01_mo_01"] },
      { propertyName: "Mandarin Oriental, Miami", country: "United States", city: "Miami", brand: "Mandarin Oriental", brandParent: "Mandarin Oriental", assignmentStatus: "Current", urbanOrResort: "Urban", chainScale: "Luxury", managementStructure: "Brand-managed", sourceIds: ["src_c01_mo_01"] },
    ],
    brandManagedCapability: [
      { brand: "Mandarin Oriental", brandParent: "Mandarin Oriental", relationshipType: "Brand Managed Capability", geography: "Selective global", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_mo_01"] },
    ],
    structures: ["Brand-managed", "Owner-Operated"],
  },
  provisional_operator_radisson: {
    sources: [
      { id: "src_c01_rad_01", title: "Radisson Hotel Group", url: "https://www.radissonhotels.com", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Radisson Blu examples (Americas/Europe)", country: "Various", city: "Various", brand: "Radisson Blu", brandParent: "Radisson Hotel Group", assignmentStatus: "Current", managementStructure: "Franchise / managed mix", sourceIds: ["src_c01_rad_01"], limitations: "Named property expansion needed" },
    ],
    brandManagedCapability: [
      { brand: "Radisson Blu", brandParent: "Radisson Hotel Group", relationshipType: "Brand Managed Capability", geography: "Selective", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_rad_01"] },
    ],
    structures: ["Franchise Only", "Brand-managed", "Franchise + Operator"],
  },
  provisional_operator_melia: {
    sources: [
      { id: "src_c01_melia_01", title: "Meliá Hotels International", url: "https://www.melia.com", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "ME Cancún", country: "Mexico", city: "Cancún", brand: "ME by Meliá", brandParent: "Meliá", assignmentStatus: "Current", urbanOrResort: "Resort", managementStructure: "Brand-managed / owned", sourceIds: ["src_c01_melia_01"] },
      { propertyName: "Gran Meliá Palacio de Isora", country: "Spain", city: "Tenerife", brand: "Gran Meliá", brandParent: "Meliá", assignmentStatus: "Current", urbanOrResort: "Resort", managementStructure: "Brand-managed / owned", sourceIds: ["src_c01_melia_01"] },
      { propertyName: "Meliá Caribe Beach", country: "Dominican Republic", city: "Punta Cana", brand: "Meliá", brandParent: "Meliá", assignmentStatus: "Current", urbanOrResort: "Resort", allInclusive: true, managementStructure: "Brand-managed / owned", sourceIds: ["src_c01_melia_01"] },
    ],
    brandManagedCapability: [
      { brand: "Meliá", brandParent: "Meliá Hotels International", relationshipType: "Brand Managed Capability", geography: "Europe / CALA / selective global", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_melia_01"] },
    ],
    structures: ["Brand-managed", "Owner-Operated", "Franchise Only", "Lease"],
  },
  provisional_operator_auberge: {
    sources: [
      { id: "src_c01_aub_01", title: "Auberge Resorts Collection", url: "https://aubergeresorts.com", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Auberge Beach Residences & Spa Fort Lauderdale", country: "United States", city: "Fort Lauderdale", brand: "Auberge", brandParent: "Auberge", assignmentStatus: "Current", urbanOrResort: "Resort", brandedResidences: true, managementStructure: "Brand-managed", sourceIds: ["src_c01_aub_01"] },
      { propertyName: "Esperanza, Auberge Resorts Collection", country: "Mexico", city: "Los Cabos", brand: "Auberge", brandParent: "Auberge", assignmentStatus: "Current", urbanOrResort: "Resort", chainScale: "Luxury", managementStructure: "Brand-managed", sourceIds: ["src_c01_aub_01"] },
    ],
    brandManagedCapability: [
      { brand: "Auberge", brandParent: "Auberge Resorts Collection", relationshipType: "Brand Managed Capability", geography: "Americas selective", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_aub_01"] },
    ],
    structures: ["Brand-managed"],
  },
  provisional_operator_shangri_la: {
    sources: [
      { id: "src_c01_sl_01", title: "Shangri-La Group", url: "https://www.shangri-la.com", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Shangri-La Hotel, Singapore", country: "Singapore", city: "Singapore", brand: "Shangri-La", brandParent: "Shangri-La", assignmentStatus: "Current", urbanOrResort: "Urban", chainScale: "Luxury", managementStructure: "Owner-operated / brand-managed", sourceIds: ["src_c01_sl_01"] },
      { propertyName: "Shangri-La Barr Al Jissah", country: "Oman", city: "Muscat", brand: "Shangri-La", brandParent: "Shangri-La", assignmentStatus: "Current", urbanOrResort: "Resort", managementStructure: "Brand-managed", sourceIds: ["src_c01_sl_01"] },
    ],
    brandManagedCapability: [
      { brand: "Shangri-La", brandParent: "Shangri-La", relationshipType: "Brand Managed Capability", geography: "Asia / Middle East / selective", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_sl_01"] },
    ],
    structures: ["Owner-Operated", "Brand-managed"],
  },
  provisional_operator_barcelo: {
    sources: [
      { id: "src_c01_bar_01", title: "Barceló Hotel Group", url: "https://www.barcelo.com", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Barceló Maya Grand Resort", country: "Mexico", city: "Riviera Maya", brand: "Barceló", brandParent: "Barceló", assignmentStatus: "Current", urbanOrResort: "Resort", allInclusive: true, managementStructure: "Owner-operated", sourceIds: ["src_c01_bar_01"] },
      { propertyName: "Barceló Santo Domingo", country: "Dominican Republic", city: "Santo Domingo", brand: "Barceló", brandParent: "Barceló", assignmentStatus: "Current", urbanOrResort: "Urban", managementStructure: "Owner-operated / managed", sourceIds: ["src_c01_bar_01"] },
    ],
    brandManagedCapability: [
      { brand: "Barceló", brandParent: "Barceló Hotel Group", relationshipType: "Brand Managed Capability", geography: "Europe / CALA selective", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_bar_01"] },
    ],
    structures: ["Owner-Operated", "Brand-managed", "Lease"],
  },
  recGmiPhRt6hiayd9: {
    sources: [
      { id: "src_c01_mxm_01", title: "Managed by Marriott (MxM)", url: "https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Representative Marriott managed luxury/upper-upscale hotels (enterprise)", country: "Various", city: "Various", brand: "JW Marriott / Marriott Hotels / others", brandParent: "Marriott", assignmentStatus: "Current", managementStructure: "Brand-managed", sourceIds: ["src_c01_mxm_01"], limitations: "Enterprise program — expand named CALA examples in apply wave" },
    ],
    brandManagedCapability: [
      { brand: "JW Marriott", brandParent: "Marriott International", relationshipType: "Brand Managed Capability", geography: "Americas + selective global", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_mxm_01"] },
      { brand: "Marriott Hotels", brandParent: "Marriott International", relationshipType: "Brand Managed Capability", geography: "Selective by brand/market", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_mxm_01"] },
      { brand: "Courtyard by Marriott", brandParent: "Marriott International", relationshipType: "Brand Managed Capability", geography: "Limited relative to franchise", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_mxm_01"], notes: "Do not assume all Marriott flags are MxM-available equally" },
    ],
    structures: ["Brand-managed", "Franchise Only", "Franchise + Operator"],
  },
  rec3Uwxe6ovpiokuN: {
    sources: [
      { id: "src_c01_hil_01", title: "Hilton Development / Ownership", url: "https://www.hilton.com/en/corporate/", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Representative Hilton managed hotels (enterprise)", country: "Various", city: "Various", brand: "Hilton Hotels & Resorts / Conrad / Waldorf Astoria", brandParent: "Hilton", assignmentStatus: "Current", managementStructure: "Brand-managed", sourceIds: ["src_c01_hil_01"], limitations: "Expand named property list in apply wave" },
    ],
    brandManagedCapability: [
      { brand: "Conrad", brandParent: "Hilton", relationshipType: "Brand Managed Capability", geography: "Selective global", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_hil_01"] },
      { brand: "Waldorf Astoria", brandParent: "Hilton", relationshipType: "Brand Managed Capability", geography: "Selective global", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_hil_01"] },
      { brand: "Hampton by Hilton", brandParent: "Hilton", relationshipType: "Brand Managed Capability", geography: "Typically franchise-primary", thirdPartyOwnerAvailability: "Selective / limited", sourceIds: ["src_c01_hil_01"] },
    ],
    structures: ["Brand-managed", "Franchise Only", "Franchise + Operator"],
  },
  recF2WqLqNVyKGz9E: {
    sources: [
      { id: "src_c01_acc_01", title: "Accor Hotel Development", url: "https://group.accor.com/en/hotel-development", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "Representative Accor managed hotels", country: "Various", city: "Various", brand: "Sofitel / Fairmont / Novotel / others", brandParent: "Accor", assignmentStatus: "Current", managementStructure: "Brand-managed / franchise mix", sourceIds: ["src_c01_acc_01"], limitations: "Named property expansion needed" },
    ],
    brandManagedCapability: [
      { brand: "Sofitel", brandParent: "Accor", relationshipType: "Brand Managed Capability", geography: "Global selective", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_acc_01"] },
      { brand: "Fairmont", brandParent: "Accor", relationshipType: "Brand Managed Capability", geography: "Global selective", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_acc_01"] },
      { brand: "ibis", brandParent: "Accor", relationshipType: "Brand Managed Capability", geography: "Often franchise-heavy", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_acc_01"] },
    ],
    structures: ["Brand-managed", "Franchise Only", "Franchise + Operator", "Lease"],
  },
  rec7IXYQYpKMYsrDl: {
    sources: [
      { id: "src_c01_ihg_01", title: "IHG Development", url: "https://www.ihg.com/content/us/en/explore/development.html", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "InterContinental managed examples", country: "Various", city: "Various", brand: "InterContinental", brandParent: "IHG", assignmentStatus: "Current", managementStructure: "Brand-managed (subset)", sourceIds: ["src_c01_ihg_01"], limitations: "IHG franchise-primary overall" },
    ],
    brandManagedCapability: [
      { brand: "InterContinental", brandParent: "IHG", relationshipType: "Brand Managed Capability", geography: "Selective", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_ihg_01"] },
      { brand: "Holiday Inn Express", brandParent: "IHG", relationshipType: "Brand Managed Capability", geography: "Franchise-primary", thirdPartyOwnerAvailability: "Limited / rare", sourceIds: ["src_c01_ihg_01"] },
    ],
    structures: ["Franchise Only", "Brand-managed", "Franchise + Operator"],
  },
  rec8SrT3VjRkkYTxm: {
    sources: [
      { id: "src_c01_minor_01", title: "Minor Hotels", url: "https://www.minorhotels.com", authority: "primary_authoritative" },
    ],
    assignments: [
      { propertyName: "NH Collection / Anantara representative managed hotels", country: "Various", city: "Various", brand: "NH Collection / Anantara", brandParent: "Minor Hotels", assignmentStatus: "Current", managementStructure: "Brand-managed / owned mix", sourceIds: ["src_c01_minor_01"], limitations: "NH is brand scope under Minor — not separate Master" },
    ],
    brandManagedCapability: [
      { brand: "Anantara", brandParent: "Minor Hotels", relationshipType: "Brand Managed Capability", geography: "Selective global", thirdPartyOwnerAvailability: "Yes", sourceIds: ["src_c01_minor_01"] },
      { brand: "NH Collection", brandParent: "Minor Hotels", relationshipType: "Brand Managed Capability", geography: "Europe / LatAm selective", thirdPartyOwnerAvailability: "Selective", sourceIds: ["src_c01_minor_01"] },
    ],
    structures: ["Brand-managed", "Owner-Operated", "Franchise Only"],
  },
};

function mapCompToAssignment(comp, entityId) {
  const status = /historical/i.test(comp.developmentType || "")
    ? "Historical"
    : /announc|pipeline|upcoming/i.test(comp.developmentType || "")
      ? "Announced / Upcoming"
      : "Current";
  let developmentContext = "Unknown";
  const dt = `${comp.developmentType || ""} ${comp.whyComparable || ""}`;
  if (/new.?build|pre-?open/i.test(dt)) developmentContext = "New Build";
  else if (/conversion/i.test(dt)) developmentContext = "Conversion";
  else if (/reflag/i.test(dt)) developmentContext = "Reflag";
  else if (/reposition/i.test(dt)) developmentContext = "Repositioning";
  else if (/turnaround|distress/i.test(dt)) developmentContext = "Turnaround";
  else if (/renovat/i.test(dt)) developmentContext = "Renovation";
  else if (/acquisition|takeover|transition/i.test(dt)) developmentContext = "Acquisition Transition";
  else if (/open|operating|stabilized|managed/i.test(dt)) developmentContext = "Existing Operation / Takeover";

  return {
    assignmentId: `asg_c01_${entityId}_${(comp.propertyName || "x").slice(0, 40).replace(/\W+/g, "_").toLowerCase()}`,
    entityId,
    propertyName: comp.propertyName,
    canonicalPropertyName: comp.propertyName,
    country: comp.country || null,
    city: comp.city || null,
    region: null,
    brand: comp.brand || null,
    brandParent: null,
    keys: comp.keyCount || null,
    chainScale: comp.hotelSegment || comp.segment || null,
    segment: comp.hotelSegment || comp.segment || null,
    hotelType: comp.assetType || null,
    urbanOrResort: comp.urbanOrResort || null,
    fullServiceSelectService: null,
    extendedStay: null,
    allInclusive: null,
    developmentContext,
    operatingStructure: comp.operatingStructure || "Third-Party Management",
    assignmentStatus: status,
    assignmentStartDate: null,
    assignmentEndDate: null,
    ownerDeveloper: null,
    mixedUse: null,
    brandedResidences: null,
    meetingsConvention: null,
    fbComplexity: null,
    lastVerified: "2026-08-03",
    sourceIds: comp.sourceIds || [],
    evidenceClass: comp.verificationStatus === "Verified" ? "primary_authoritative" : "referenced",
    publicationClass: comp.verificationStatus === "Verified" ? "Auto-publish objective verified" : "Auto-publish with evidence qualification",
    conflictStatus: "None",
    limitations: comp.limitations || null,
    whyComparable: comp.whyComparable || null,
    comparabilityStrength: comp.comparabilityStrength || null,
  };
}

function main() {
  ensureDir(outRoot);
  ["assignments", "market-presence", "brand-relationships", "claims", "sources", "profile-payloads"].forEach((d) =>
    ensureDir(join(outRoot, d))
  );

  const entities = ENTITIES.map((e) => ({
    canonicalName: e.canonicalName,
    track: e.track,
    existingMasterId: e.id,
    provisionalEntityId: e.provisionalId,
    entityId: entityKey(e),
    aliases: e.aliases,
    parent: e.parent,
    website: e.website,
    operatingModel: e.operatingModel,
    managementAvailability: e.managementAvailability,
    existingOrProposedMaster: e.masterStatus === "existing" ? "Existing" : "Proposed",
    researchStatus: e.researchStatusHint || "In Progress",
    researchNote: e.researchNote || null,
  }));

  if (entities.length !== 27) throw new Error(`Expected 27 entities, got ${entities.length}`);
  writeJson(join(outRoot, "entities.json"), {
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    count: entities.length,
    track1: entities.filter((e) => e.track === 1).length,
    track2: entities.filter((e) => e.track === 2).length,
    existingMasters: entities.filter((e) => e.existingMasterId).length,
    provisional: entities.filter((e) => e.provisionalEntityId).length,
    entities,
  });

  // Load prior intel
  const cohorts = ["calibration-cohort", "wave-2-cohort", "wave-3-cohort"];
  const allComps = [];
  const allSources = [];
  const allBrandRels = [];
  const allClaims = [];
  const allGeo = [];
  for (const c of cohorts) {
    const base = join(root, "data/operator-intelligence", c);
    if (existsSync(join(base, "comparables.json"))) allComps.push(...readJson(join(base, "comparables.json")));
    if (existsSync(join(base, "sources.json"))) allSources.push(...readJson(join(base, "sources.json")));
    if (existsSync(join(base, "brand-relationships.json")))
      allBrandRels.push(...readJson(join(base, "brand-relationships.json")));
    if (existsSync(join(base, "claims.json"))) allClaims.push(...readJson(join(base, "claims.json")));
    if (existsSync(join(base, "geography.json"))) allGeo.push(...readJson(join(base, "geography.json")));
  }

  let air = { claims: [], presence: [], caseStudies: [] };
  const airPath = join(outRoot, "_raw/airtable-snapshot.json");
  if (existsSync(airPath)) air = readJson(airPath);

  const assignments = [];
  const brandRelsOut = [];
  const presenceOut = [];
  const claimsOut = [];
  const sourcesOut = new Map();

  function addSource(s) {
    const id = s.id || s.sourceId;
    if (!id) return;
    if (!sourcesOut.has(id)) {
      sourcesOut.set(id, {
        id,
        title: s.title || null,
        url: s.url || null,
        authority: s.authority || null,
        classification: "Existing / Reused",
        publisher: s.publisher || null,
      });
    }
  }

  for (const s of allSources) addSource(s);

  // From comparables
  for (const comp of allComps) {
    const eid = comp.operatorId;
    if (!entities.find((e) => e.entityId === eid)) continue;
    assignments.push(mapCompToAssignment(comp, eid));
  }

  // From case studies airtable
  for (const cs of air.caseStudies || []) {
    const op = (cs.fields.Operator || [])[0];
    if (!op || !entities.find((e) => e.entityId === op)) continue;
    assignments.push({
      assignmentId: `asg_c01_cs_${cs.id}`,
      entityId: op,
      propertyName: cs.fields.property_name,
      canonicalPropertyName: cs.fields.property_name,
      country: null,
      city: null,
      region: cs.fields.region || null,
      brand: cs.fields.branded_independent || null,
      brandParent: null,
      keys: null,
      chainScale: null,
      segment: null,
      hotelType: cs.fields.hotel_type || null,
      urbanOrResort: null,
      developmentContext: /conversion|reflag|turnaround|reposition|pre-?open/i.test(cs.fields.situation || "")
        ? cs.fields.situation
        : "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      lastVerified: null,
      sourceIds: [],
      evidenceClass: "case_study_row",
      publicationClass: "Auto-publish with evidence qualification",
      conflictStatus: "None",
      limitations: "Case Study narrative row — strengthen with official property source before apply",
      fromCaseStudyId: cs.id,
      whyComparable: cs.fields["Why Comparable"] || null,
      comparabilityStrength: cs.fields["Comparability Strength"] || null,
    });
  }

  // Prior brand relationships
  for (const br of allBrandRels) {
    if (!entities.find((e) => e.entityId === br.operatorId)) continue;
    brandRelsOut.push({
      brandRelationshipId: `br_c01_${br.operatorId}_${(br.brand || "x").replace(/\W+/g, "_")}`,
      entityId: br.operatorId,
      brand: br.brand,
      brandParent: br.brandParent || null,
      relationshipType: "Currently Operates",
      currentOrHistorical: "Current",
      geographyScope: br.geography || null,
      segmentScope: null,
      hotelTypeScope: null,
      thirdPartyOwnerAvailability: null,
      evidence: br.evidence || null,
      sourceIds: br.sourceIds || [],
      publicationStatus: "Publish With Evidence Label",
      conflictStatus: "None",
      limitations: br.limitations || "Do not infer project approval",
    });
  }

  // Airtable presence
  for (const p of air.presence || []) {
    const op = (p.fields.Operator || [])[0];
    if (!op) continue;
    presenceOut.push({
      presenceId: p.id,
      entityId: op,
      country: p.fields.Country,
      region: p.fields.Region || null,
      cityMetro: null,
      presenceType: p.fields["Market Presence Type"],
      currentOrHistorical: p.fields["Current / Historical"],
      verifiedAssignmentCount: null,
      evidence: p.fields["Source URLs"] || null,
      claimId: p.fields["Claim ID"] || null,
      lastVerified: p.fields["Verification Date"] || null,
      dryRunAction: "Existing — no change",
      publicationStatus: p.fields["Publication Status"] || null,
      conflictStatus: "None",
    });
  }

  // Claims from air + local
  for (const c of air.claims || []) {
    const op = (c.fields.Operator || [])[0];
    if (!op) continue;
    claimsOut.push({
      claimId: c.fields["Claim ID"] || c.id,
      entityId: op,
      claimCategory: c.fields["Claim Category"],
      subject: c.fields.Subject,
      predicate: c.fields.Predicate,
      rawValue: c.fields["Raw Value"],
      geographicScope: c.fields["Geographic Scope"],
      brandScope: c.fields["Brand Scope"],
      verificationStatus: c.fields["Verification Status"],
      publicationStatus: c.fields["Publication Status"],
      conflictStatus: c.fields["Conflict Status"] || "None",
      sourceUrls: c.fields["Source URLs"],
      dryRunAction: "Existing — no change",
    });
  }

  // Track 2 curated
  for (const [key, pack] of Object.entries(TRACK2_CURATED)) {
    for (const s of pack.sources || []) {
      addSource({ ...s, classification: "Proposed New PI Source" });
      sourcesOut.get(s.id).classification = "Proposed New PI Source";
    }
    for (const a of pack.assignments || []) {
      assignments.push({
        assignmentId: `asg_c01_${key}_${(a.propertyName || "x").slice(0, 40).replace(/\W+/g, "_").toLowerCase()}`,
        entityId: key,
        propertyName: a.propertyName,
        canonicalPropertyName: a.propertyName,
        country: a.country || null,
        city: a.city || null,
        brand: a.brand || null,
        brandParent: a.brandParent || null,
        keys: a.keys || null,
        chainScale: a.chainScale || null,
        urbanOrResort: a.urbanOrResort || null,
        allInclusive: a.allInclusive || null,
        brandedResidences: a.brandedResidences || null,
        developmentContext: a.developmentContext || "Existing Operation / Takeover",
        operatingStructure: a.managementStructure || null,
        assignmentStatus: a.assignmentStatus || "Current",
        lastVerified: "2026-08-10",
        sourceIds: a.sourceIds || [],
        evidenceClass: "primary_authoritative",
        publicationClass: a.limitations
          ? "Auto-publish with evidence qualification"
          : "Auto-publish objective verified",
        conflictStatus: "None",
        limitations: a.limitations || null,
      });
    }
    for (const b of pack.brandManagedCapability || []) {
      brandRelsOut.push({
        brandRelationshipId: `br_c01_bmc_${key}_${(b.brand || "x").replace(/\W+/g, "_")}`,
        entityId: key,
        brand: b.brand,
        brandParent: b.brandParent,
        relationshipType: "Brand Managed Capability",
        currentOrHistorical: "Current",
        geographyScope: b.geography || null,
        segmentScope: null,
        hotelTypeScope: null,
        thirdPartyOwnerAvailability: b.thirdPartyOwnerAvailability || null,
        evidence: b.notes || "Official development / brand platform",
        sourceIds: b.sourceIds || [],
        publicationStatus: "Publish With Evidence Label",
        conflictStatus: "None",
        limitations: "Not project-specific approval",
      });
    }
    // Derive presence countries from assignments
    const countries = new Set(
      (pack.assignments || []).map((a) => a.country).filter((c) => c && c !== "Various")
    );
    for (const country of countries) {
      presenceOut.push({
        presenceId: `mp_c01_prop_${key}_${country.replace(/\W+/g, "_")}`,
        entityId: key,
        country,
        region: null,
        cityMetro: null,
        presenceType: "Current Managed Property",
        currentOrHistorical: "Current",
        verifiedAssignmentCount: (pack.assignments || []).filter((a) => a.country === country).length,
        evidence: "Derived from assignment dry-run sources",
        lastVerified: "2026-08-10",
        dryRunAction: "Proposed new",
        conflictStatus: "None",
      });
    }
  }

  // Write per-entity files
  for (const e of entities) {
    const eid = e.entityId;
    const asg = assignments.filter((a) => a.entityId === eid);
    const br = brandRelsOut.filter((b) => b.entityId === eid);
    const mp = presenceOut.filter((p) => p.entityId === eid);
    const cl = claimsOut.filter((c) => c.entityId === eid);
    writeJson(join(outRoot, "assignments", `${eid}.json`), { entityId: eid, count: asg.length, assignments: asg });
    writeJson(join(outRoot, "brand-relationships", `${eid}.json`), {
      entityId: eid,
      count: br.length,
      brandRelationships: br,
    });
    writeJson(join(outRoot, "market-presence", `${eid}.json`), { entityId: eid, count: mp.length, marketPresence: mp });
    writeJson(join(outRoot, "claims", `${eid}.json`), { entityId: eid, count: cl.length, claims: cl });

    const profile = buildProfile(e, asg, br, mp, cl);
    writeJson(join(outRoot, "profile-payloads", `${eid}.json`), profile);
  }

  writeJson(join(outRoot, "sources", "sources.json"), {
    count: sourcesOut.size,
    sources: [...sourcesOut.values()],
  });
  writeJson(join(outRoot, "assignments", "_index.json"), {
    total: assignments.length,
    byEntity: Object.fromEntries(entities.map((e) => [e.entityId, assignments.filter((a) => a.entityId === e.entityId).length])),
  });
  writeJson(join(outRoot, "brand-relationships", "_index.json"), {
    total: brandRelsOut.length,
    brandManagedCapability: brandRelsOut.filter((b) => b.relationshipType === "Brand Managed Capability").length,
  });
  writeJson(join(outRoot, "market-presence", "_index.json"), {
    total: presenceOut.length,
    proposedNew: presenceOut.filter((p) => p.dryRunAction === "Proposed new").length,
    existing: presenceOut.filter((p) => String(p.dryRunAction || "").startsWith("Existing")).length,
  });
  writeJson(join(outRoot, "claims", "_index.json"), { total: claimsOut.length });

  writeJson(join(outRoot, "summary-metrics.json"), {
    generatedAt: new Date().toISOString(),
    entities: 27,
    track1: 12,
    track2: 15,
    existingMasters: entities.filter((e) => e.existingMasterId).length,
    provisional: entities.filter((e) => e.provisionalEntityId).length,
    assignments: assignments.length,
    brandRelationships: brandRelsOut.length,
    brandManagedCapabilityRows: brandRelsOut.filter((b) => b.relationshipType === "Brand Managed Capability").length,
    marketPresence: presenceOut.length,
    marketPresenceProposedNew: presenceOut.filter((p) => p.dryRunAction === "Proposed new").length,
    claims: claimsOut.length,
    sources: sourcesOut.size,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        entities: 27,
        assignments: assignments.length,
        brandRelationships: brandRelsOut.length,
        presence: presenceOut.length,
        claims: claimsOut.length,
        sources: sourcesOut.size,
      },
      null,
      2
    )
  );
}

function buildProfile(e, asg, br, mp, cl) {
  const countries = [...new Set(mp.map((p) => p.country).filter(Boolean))];
  const brands = [...new Set(br.map((b) => b.brand).filter(Boolean))];
  const classified = classifyExplorerReadiness({
    namedAssignmentCount: asg.length,
    distinctCountryCount: countries.length,
    distinctBrandNameCount: brands.length,
    track: e.track,
    hasBrandManagedCapability: br.some((b) => b.relationshipType === "Brand Managed Capability"),
    recordPurpose: null, // dry-run content class; Purpose gating applied in Airtable path
  });
  // Dry-run package historically reported content class without Purpose gating
  const usefulness = classified.contentClass;
  const explorerPublishable = usefulness === "Strong Profile" || usefulness === "Useful Profile";
  return {
    entityId: e.entityId,
    canonicalName: e.canonicalName,
    track: e.track,
    operatingModel: e.operatingModel,
    managementAvailability: e.managementAvailability,
    sections: {
      overview: {
        who: e.canonicalName,
        parent: e.parent,
        website: e.website,
        operatingModel: e.operatingModel,
        managementAvailability: e.managementAvailability,
      },
      operatingFootprint: { countries, presenceRows: mp.length },
      portfolioProfile: { assignmentCount: asg.length, sample: asg.slice(0, 5).map((a) => a.propertyName) },
      experience: {
        developmentContexts: [...new Set(asg.map((a) => a.developmentContext).filter(Boolean))],
        urbanResort: [...new Set(asg.map((a) => a.urbanOrResort).filter(Boolean))],
      },
      brandRelationships: { brands, rows: br.length, brandManagedCapability: br.filter((b) => b.relationshipType === "Brand Managed Capability").length },
      selectedAssignments: asg.slice(0, 8),
      operatingStructures: TRACK2_CURATED[e.entityId]?.structures || ["Third-Party Management"],
      differentiatingCapabilities: [],
      marketPresence: mp,
      recentMomentum: cl.filter((c) => /momentum|recent|expand/i.test(`${c.claimCategory} ${c.subject}`)),
      evidenceLastVerified: {
        sourceCount: [...new Set(asg.flatMap((a) => a.sourceIds || []))].length,
        note: "Dry-run local payload — not production UI",
      },
    },
    usefulness,
    explorerPublishable,
    readiness: {
      researchCompleteEnough: asg.length > 0 || mp.length > 0,
      explorerPublishable,
      strongExplorerProfile: usefulness === "Strong Profile",
    },
    fitDataReadinessDiagnostic: {
      status: strong ? "Fit Data Ready" : thin ? "Research Required" : "Conditional",
      note: "Diagnostic only — no scoring",
    },
  };
}

main();
