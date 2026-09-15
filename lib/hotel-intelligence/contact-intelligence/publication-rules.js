/**
 * Contact Intelligence V1 — server-side publication rules.
 * Never label inferred emails as officially attributed.
 * Never label corporate numbers as direct personal lines.
 */

import {
  ATTRIBUTION,
  CHANNEL_KIND,
  USAGE_RIGHTS,
  CONTACT_WRITE_GUARANTEES,
} from "./vocabulary.js";

/**
 * @param {object} channel
 * @returns {{ ok: boolean, publishable: object|null, violations: string[] }}
 */
export function applyPublicationRules(channel, { audience = "public" } = {}) {
  const violations = [];
  if (!channel || typeof channel !== "object") {
    return { ok: false, publishable: null, violations: ["channel_missing"] };
  }

  const attribution = String(channel.attribution || "").toUpperCase();
  const kind = String(channel.kind || "").toUpperCase();
  const usage = String(channel.usage_rights || USAGE_RIGHTS.PUBLIC_SAFE).toUpperCase();
  const displayLabel = String(channel.display_label || "").trim();

  if (attribution === ATTRIBUTION.INFERRED) {
    if (/official|verified person|attributed/i.test(displayLabel)) {
      violations.push("inferred_labeled_as_official");
    }
  }

  if (
    (kind === CHANNEL_KIND.SWITCHBOARD ||
      kind === CHANNEL_KIND.ORG_PHONE ||
      kind === CHANNEL_KIND.HOTEL_PHONE) &&
    attribution !== ATTRIBUTION.NAMED_PERSON
  ) {
    if (/direct (personal|mobile|line)|personal mobile|cell\b/i.test(displayLabel)) {
      violations.push("switchboard_labeled_as_direct_personal");
    }
  }

  if (audience === "public" && usage !== USAGE_RIGHTS.PUBLIC_SAFE) {
    violations.push("usage_not_public_safe");
  }

  if (audience === "public" && attribution === ATTRIBUTION.INFERRED) {
    violations.push("inferred_not_public");
  }

  if (violations.length) {
    return { ok: false, publishable: null, violations };
  }

  const publishable = {
    ...channel,
    attribution,
    kind,
    usage_rights: usage,
    publication: {
      audience,
      rules_version: "contact-publication-v1",
      write_guarantees: { ...CONTACT_WRITE_GUARANTEES },
    },
  };

  // Harden customer-facing labels
  if (attribution === ATTRIBUTION.INFERRED) {
    publishable.display_label = publishable.display_label || "Inferred (internal)";
    publishable.customer_caveat = "Inferred — not officially attributed.";
  }
  if (kind === CHANNEL_KIND.SWITCHBOARD || attribution === ATTRIBUTION.ORGANIZATION) {
    if (!publishable.display_label || /direct/i.test(publishable.display_label)) {
      publishable.display_label =
        kind === CHANNEL_KIND.SWITCHBOARD ? "Organization switchboard" : "Organization / property line";
    }
    publishable.customer_caveat =
      publishable.customer_caveat || "Corporate or property line — not a direct personal number.";
  }

  return { ok: true, publishable, violations: [] };
}

/**
 * Filter a contact package for a given audience.
 */
export function publishContactPackage(pkg, { audience = "public" } = {}) {
  const channels = [];
  const rejected = [];
  for (const ch of pkg?.channels || []) {
    const result = applyPublicationRules(ch, { audience });
    if (result.ok) channels.push(result.publishable);
    else rejected.push({ channel_id: ch?.channel_id || null, violations: result.violations });
  }

  const people = (pkg?.people || []).map((p) => {
    if (p.former_affiliation === true || String(p.affiliation_status || "").toUpperCase() === "FORMER") {
      rejected.push({ person_id: p.person_id, violations: ["former_affiliation"] });
      return null;
    }
    const outChannels = [];
    for (const ch of p.channels || []) {
      const result = applyPublicationRules(ch, { audience });
      if (result.ok) outChannels.push(result.publishable);
      else rejected.push({ person_id: p.person_id, violations: result.violations });
    }
    return { ...p, channels: outChannels };
  }).filter(Boolean);

  // Never publish inferred candidates or tenant-restricted payloads on public audience
  const inferred =
    audience === "public" ? [] : Array.isArray(pkg?.inferred_email_candidates) ? pkg.inferred_email_candidates : [];

  return {
    ...pkg,
    channels,
    people,
    inferred_email_candidates: inferred,
    publication: {
      audience,
      rejected_count: rejected.length,
      rejected: audience === "internal" ? rejected : rejected.map((r) => ({
        channel_id: r.channel_id || null,
        person_id: r.person_id || null,
        reason: "filtered_by_publication_rules",
      })),
      write_guarantees: { ...CONTACT_WRITE_GUARANTEES },
    },
  };
}
