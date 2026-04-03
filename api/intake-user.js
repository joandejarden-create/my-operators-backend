import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

const F = {
  users: {
    table: "tbl6shiyz2wdUqE5F",
    uniqueWebflowId: "flddTfp7oLdcPwBIC", // Unique_Webflow_ID
    email: "fldBl7IXEscwkMhnZ",           // Email
    firstName: "fldG5nbAijQkUVSzr",       // First Name
    lastName: "fldV0g50iRB8J46Hh",        // Last Name
  },
};

export default async function userIntake(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const secret = req.headers["x-intake-secret"];
    if (!secret || secret !== process.env.INTAKE_SHARED_SECRET) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const { email, memberstackId, firstName, lastName } = req.body;
    if (!email || !memberstackId) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Look up user by Memberstack ID or email
    const users = await base(F.users.table)
      .select({ filterByFormula: `OR({${F.users.uniqueWebflowId}} = '${memberstackId}', {${F.users.email}} = '${email}')` })
      .firstPage();

    let user;
    if (users.length > 0) {
      user = await base(F.users.table).update(users[0].id, {
        [F.users.email]: email,
        [F.users.uniqueWebflowId]: memberstackId,
        [F.users.firstName]: firstName || "",
        [F.users.lastName]: lastName || "",
      });
    } else {
      user = await base(F.users.table).create({
        [F.users.email]: email,
        [F.users.uniqueWebflowId]: memberstackId,
        [F.users.firstName]: firstName || "",
        [F.users.lastName]: lastName || "",
      });
    }

    return res.json({ id: user.id });
  } catch (err) {
    console.error("Error in intake-user:", err);
    return res.status(500).json({ error: "Internal Server Error", details: err.message });
  }
}
