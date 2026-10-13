/**
 * Display label for the ADP property selector — same grammar on owner app and share.
 * Uses the shared Dealality property-display formatter (city/state/country).
 */
import { listPropertyProfiles } from "../data-model.js";
import {
  formatPropertyLocationLine,
  formatPropertySelectorLabel,
} from "../../dealality/property-display-label.js";

export function formatAdpPropertySelectorLabel(property) {
  if (!property) return "";
  return formatPropertySelectorLabel({
    name: property.name || property.displayName || property.propertyId || "",
    city: property.city,
    state: property.state,
    region: property.region,
    country: property.country,
  });
}

export function resolveAdpSharePropertyDisplay(propertyId) {
  const id = String(propertyId || "").trim();
  const hit = listPropertyProfiles().find((p) => p.propertyId === id) || null;
  if (!hit) {
    return {
      propertyId: id,
      name: id,
      city: "",
      state: "",
      country: "",
      locationLine: "",
      label: id,
    };
  }
  return {
    propertyId: hit.propertyId,
    name: hit.name,
    city: hit.city || "",
    state: hit.state || "",
    country: hit.country || "",
    locationLine: hit.locationLine || formatPropertyLocationLine(hit),
    label: hit.label || formatAdpPropertySelectorLabel(hit),
  };
}
