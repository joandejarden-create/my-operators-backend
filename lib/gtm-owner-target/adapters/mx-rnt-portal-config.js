/**
 * Mexico registry portal config (RNT — secondary / often unavailable).
 * Primary Mexico path: SIGER — see mx-siger-registry.js
 */
export const MX_RNT_PORTAL_OFFICIAL = "https://rnt-consulta.sectur.gob.mx/";
export const MX_RNT_PORTAL_TRAMITES = "https://rnt.sectur.gob.mx/";
export const MX_RNT_PORTAL_DEPRECATED = "https://rntsecturgob.com/";

export const MX_RNT_PORTAL_FALLBACKS = [
  MX_RNT_PORTAL_OFFICIAL,
  MX_RNT_PORTAL_TRAMITES,
];

/**
 * Active portal URL — only used when MX_RNT_LOOKUP_ENABLED=true.
 */
export function resolveMxRntPortalUrl() {
  if (!isMxRntLookupEnabled()) return null;
  const override = String(process.env.MX_RNT_PORTAL_URL || "").trim();
  if (override) return override;
  return MX_RNT_PORTAL_OFFICIAL;
}

export function isMxRntLookupEnabled() {
  const flag = String(process.env.MX_RNT_LOOKUP_ENABLED || "").trim().toLowerCase();
  return flag === "1" || flag === "true" || flag === "yes";
}

/**
 * Manual lookup guidance when tourism registry portals are down or slow.
 * @param {"direct_entity" | "rnt_bridge" | "opaque_spv"} bridgeStrategy
 */
export function mxRegistryFallbackSteps(bridgeStrategy) {
  if (bridgeStrategy === "direct_entity") {
    return [
      "PRIMARY (RNT down): Search SIGER (https://www.siger.gob.mx/) directly by CoStar True Owner razón social.",
      "Confirm RFC via SAT Validador.",
      "Extract representante legal from RPC folio or paid cert.",
    ];
  }
  return [
    "PRIMARY (RNT down): Search hotel website / Google Maps listing for operating razón social.",
    "Try SIGER with any entity name found on hotel site or invoice footer.",
    "Use SIEM open data (datos.gob.mx, SIEM dataset) as secondary establishment-name source.",
  ];
}
