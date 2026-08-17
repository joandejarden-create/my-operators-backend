/**
 * Country → administrative level for State / Region display field.
 * State / Region remains the normalized display; administrative_type is the underlying type.
 */

export const COUNTRY_ADMIN_LEVELS = Object.freeze({
  Mexico: { administrative_type: "State", display_field: "State / Region" },
  "Dominican Republic": { administrative_type: "Province", display_field: "State / Region" },
  "Costa Rica": { administrative_type: "Province", display_field: "State / Region" },
  Colombia: { administrative_type: "Department", display_field: "State / Region" },
  Brazil: { administrative_type: "State", display_field: "State / Region" },
  Argentina: { administrative_type: "Province", display_field: "State / Region" },
  Jamaica: { administrative_type: "Parish", display_field: "State / Region" },
  Barbados: { administrative_type: "Parish", display_field: "State / Region" },
  Panama: { administrative_type: "Province", display_field: "State / Region" },
  Peru: { administrative_type: "Department", display_field: "State / Region" },
  Chile: { administrative_type: "Region", display_field: "State / Region" },
  "Puerto Rico": { administrative_type: "Municipality", display_field: "State / Region" },
});

export function getAdminLevel(country) {
  return (
    COUNTRY_ADMIN_LEVELS[String(country || "").trim()] || {
      administrative_type: "Administrative Region",
      display_field: "State / Region",
    }
  );
}
