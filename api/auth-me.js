/**
 * GET /api/auth/me — Memberstack + Airtable user context (MVP auth test).
 */

export async function getAuthMe(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ ok: false, error: "method_not_allowed" });
  }

  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({ ok: false, error: "server_error", message: "User context missing." });
  }

  return res.json({
    ok: true,
    memberstack: {
      id: req.memberstackMemberId,
      email: req.memberstackEmail || u.email || null,
      verifiedVia: req.memberstackVerifiedVia || null,
    },
    airtable: {
      exists: true,
      userRecordId: u.userRecordId,
    },
    dealalityUser: {
      email: u.email,
      memberstackId: u.memberstackId,
      role: u.role,
      roleRaw: u.roleRaw || null,
      companyId: u.companyId,
      companyName: u.companyName,
      status: u.status,
      isAdmin: u.isAdmin,
    },
  });
}
