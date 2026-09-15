/**
 * Contact Intelligence V1.2 — once-per-owner research cache.
 * Sibling hotels reuse org/person research only when ownership is evidenced
 * and role scope supports reuse.
 */

export function createOwnerResearchCache() {
  /** @type {Map<string, object>} */
  const byOwner = new Map();

  function key(ownerEntityId) {
    return String(ownerEntityId || "").trim() || null;
  }

  return {
    get(ownerEntityId) {
      const k = key(ownerEntityId);
      return k ? byOwner.get(k) || null : null;
    },
    set(ownerEntityId, payload) {
      const k = key(ownerEntityId);
      if (!k) return;
      byOwner.set(k, {
        ...payload,
        cached_at: new Date().toISOString(),
        reuse_count: 0,
      });
    },
    takeReuse(ownerEntityId) {
      const entry = this.get(ownerEntityId);
      if (!entry) return null;
      entry.reuse_count = (entry.reuse_count || 0) + 1;
      return {
        ...entry,
        reused: true,
        reuse_count: entry.reuse_count,
      };
    },
    size() {
      return byOwner.size;
    },
    entries() {
      return [...byOwner.entries()].map(([id, v]) => ({
        owner_entity_id: id,
        owner_display_name: v.owner_display_name || null,
        reuse_count: v.reuse_count || 0,
        has_org_phone_email: Boolean(v.has_org_phone_email),
        people_count: (v.people || []).length,
      }));
    },
  };
}
