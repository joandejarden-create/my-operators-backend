/**
 * Display label for the ADP property selector — same grammar on owner app and share.
 */
import { listPropertyProfiles } from "../data-model.js";

export function formatAdpPropertySelectorLabel(property) {
  if (!property) return "";
  const name = property.name || property.propertyId || "";
  const city = property.city || "";
  const state = property.state || "";
  return name + " — " + city + ", " + state;
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
      label: id,
    };
  }
  return {
    propertyId: hit.propertyId,
    name: hit.name,
    city: hit.city || "",
    state: hit.state || "",
    label: formatAdpPropertySelectorLabel(hit),
  };
}
