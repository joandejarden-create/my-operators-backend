/**
 * Reject desk personas / webmaster-style identities that look like people.
 * WHO gate — functional mailboxes are OK as HOW, not as named stakeholders.
 */
export function isRejectedDeskPersona(contact = {}) {
  const name = String(contact.name || contact.fullName || "");
  const email = String(contact.email || "").toLowerCase();
  const role = String(contact.role || contact.title || "");
  if (/\b(webmaster|web[- ]?master|sysadmin|postmaster)\b/i.test(`${name} ${role}`)) {
    return { reject: true, reason: "WEBMASTER_DESK" };
  }
  if (/^(webmaster|postmaster|sysadmin|noreply|no-reply|donotreply)@/i.test(email)) {
    return { reject: true, reason: "WEBMASTER_MAILBOX" };
  }
  if (/ncifwebmaster|webadmin@/i.test(email)) {
    return { reject: true, reason: "WEBMASTER_MAILBOX" };
  }
  return { reject: false, reason: null };
}
