# Connect Zapier MCP to Cursor

This wires **Cursor** (the editor) to **Zapier MCP** so the AI can invoke the Zapier **tools** you allow (Airtable, Slack, Webflow actions, etc.). It is **not** part of the `deal-capture-proxy` Railway app—configuration lives in your **Zapier account** and **Cursor MCP settings**.

Official references:

- [Use Zapier MCP with your client](https://help.zapier.com/hc/en-us/articles/36265392843917-Use-Zapier-MCP-with-your-client) (includes Cursor-specific notes)
- [Zapier MCP home](https://mcp.zapier.com/)
- [Cursor MCP documentation](https://cursor.com/docs/mcp)

---

## 1. Zapier: create the MCP server

1. Open **[mcp.zapier.com](https://mcp.zapier.com/)** (or Zapier → **MCP servers**).
2. Create a **new MCP server**.
3. In the **MCP client** dropdown, choose **Cursor** so Zapier shows **Cursor-specific** connection steps.
4. Open the **Tools** (or equivalent) section and add only the actions you want the AI to run. Prefer a **minimal** set over **Add all tools**.
5. For each tool, pick the **Zapier app connection** (same accounts you use in Zaps).
6. Open the **Connect** tab and copy the **server URL** Zapier shows.

**Security:** Treat that URL like a password—anyone with it can run your enabled tools. Do not commit it to git. Prefer pasting into Cursor’s UI or `${env:…}` interpolation (see below).

**Quota:** Each **successful** tool call uses **two tasks** on your Zapier plan (per Zapier help). Failed calls do not consume tasks.

**Limitation:** Only **one** client should run tool calls through the same server URL at a time (Zapier’s note).

---

## 2. Cursor: register the server

Use **either** the Settings UI **or** an `mcp.json` file. Do **not** put the real URL in a committed file.

### Option A — Cursor Settings (simplest)

1. **Cursor Settings** → **Features** → **Model Context Protocol** (or search “MCP”).
2. **Add MCP server** (wording may vary).
3. Follow the field names Zapier’s **Connect** tab shows for **Cursor** (often a **remote URL** style server).

If Zapier opens an **OAuth** flow in the browser, complete it; if auth fails repeatedly, Zapier documents a possible **RFC 9728 OAuth** issue on some Cursor versions—**update Cursor** and retry ([Zapier help — Cursor](https://help.zapier.com/hc/en-us/articles/36265392843917-Use-Zapier-MCP-with-your-client)).

### Option B — `mcp.json` (URL from environment)

Cursor reads MCP config from:

- **Global:** `%USERPROFILE%\.cursor\mcp.json`
- **Project:** `.cursor\mcp.json` under the repo (team-shared tools—still avoid committing secrets)

Remote servers typically look like this ([Cursor docs](https://cursor.com/docs/mcp)):

```json
{
  "mcpServers": {
    "zapier": {
      "url": "${env:ZAPIER_MCP_SERVER_URL}"
    }
  }
}
```

1. Set a Windows **user** environment variable `ZAPIER_MCP_SERVER_URL` to the full URL from Zapier’s Connect tab (or use User Variables in System Properties).
2. Restart Cursor so it picks up the variable.
3. If Zapier’s instructions say to send a header (e.g. `Authorization`), add a `"headers"` block exactly as Zapier documents—do not guess.

If Zapier gives **OAuth client id/secret** instead of a single URL, use Cursor’s `"auth"` block as described under **Static OAuth for remote servers** in the [Cursor MCP doc](https://cursor.com/docs/mcp).

---

## 3. Verify in Cursor

1. Open **Output** → **MCP Logs** ([Cursor MCP debugging](https://cursor.com/docs/mcp)).
2. Start a chat and ask for something that uses a **narrow** tool you enabled (e.g. “List the fields on …” only if such a tool exists).
3. Approve the tool prompt when Cursor asks (unless you enable auto-run).

Use Zapier MCP **History** on the server to audit what ran.

---

## 4. What this does *not* do

- It does **not** edit your **Zaps** as YAML from Cursor; it runs **pre-approved Zapier actions as MCP tools**.
- It does **not** replace your **Railway** `deal-capture-proxy` APIs for Dealality product behavior—those stay as they are.

To **rotate** a leaked URL: Zapier MCP server → **Connect** → **Rotate token** (invalidates the old URL).
