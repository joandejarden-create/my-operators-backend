/**
 * Provider-independent trusted-secondary source host policy.
 * Trade / dataset hosts may corroborate but are never official SoT.
 */

export const CENSUS_SOURCE_TRUST_POLICY_VERSION =
  "census-source-trust-policy-v1";

/** Trusted secondary / verification only — never treat as official. */
export const TRUSTED_SECONDARY_HOSTS = Object.freeze([
  "travelweekly.com",
  "northstartravelmedia.com",
  "northstarmeetingsgroup.com",
  "hotelnewsnow.com",
  "costar.com",
  "hospitalitynet.org",
]);

/**
 * @param {string} host
 */
export function isTrustedSecondaryHost(host) {
  if (!host) return false;
  return TRUSTED_SECONDARY_HOSTS.some(
    (d) => host === d || host.endsWith(`.${d}`)
  );
}
