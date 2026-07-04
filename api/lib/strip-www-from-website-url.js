/**
 * Strip a leading `www.` subdomain from the hostname only (path/query/hash unchanged).
 * Safe for hrefs: apex and www usually resolve to the same site.
 */
export function stripLeadingWwwFromWebsiteUrl(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return "";

  const hasScheme = /^[a-z][a-z0-9+.-]*:/i.test(s);

  try {
    const candidate = hasScheme ? s : `https://${s}`;
    const u = new URL(candidate);
    const strippedHost = u.hostname.replace(/^www\./i, "");
    if (strippedHost === u.hostname) {
      return s;
    }
    u.hostname = strippedHost;

    if (!hasScheme) {
      const hostPort = strippedHost + (u.port ? `:${u.port}` : "");
      const rest = `${u.pathname}${u.search}${u.hash}`;
      if (!rest || rest === "/") return hostPort;
      return `${hostPort}${rest}`;
    }

    // With scheme: URL.href adds "/" for bare origins — drop it if the input had none.
    let href = u.href;
    if (
      u.pathname === "/" &&
      !u.search &&
      !u.hash &&
      !s.endsWith("/")
    ) {
      href = href.replace(/\/$/, "");
    }
    return href;
  } catch {
    return s
      .replace(/^(https?:\/\/)www\./i, "$1")
      .replace(/^www\./i, "");
  }
}
