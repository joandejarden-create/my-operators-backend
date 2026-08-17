#!/usr/bin/env node
/**
 * Unit checks for lib/partner-intelligence/extract-source-text.js
 */
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

const tmpBrand = mkdtempSync(join(tmpdir(), "pi-extract-brand-"));
const tmpOperator = mkdtempSync(join(tmpdir(), "pi-extract-operator-"));
process.env.PARTNER_REFERENCE_ROOT = tmpBrand;
process.env.OPERATOR_REFERENCE_ROOT = tmpOperator;

const { parseHtmlDocument, readLocalSourceText } = await import(
  "../lib/partner-intelligence/extract-source-text.js"
);
const { resolveLocalSourceAbsolutePath } = await import(
  "../lib/partner-intelligence/reference-material-paths.js"
);

const SAMPLE_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Hotel Management Company | Hotel Equities</title>
  <meta name="description" content="Third party hotel management at Hotel Equities." />
  <style>.hidden { display: none; }</style>
  <script>window.tracking = "remove-me";</script>
</head>
<body>
  <nav>Skip navigation links</nav>
  <main>
    <h1>Hotel Equities</h1>
    <p>We deliver third party management for hotel owners across the Americas.</p>
  </main>
  <footer>Copyright Hotel Equities</footer>
  <noscript>Enable JavaScript</noscript>
</body>
</html>`;

// 1. Local HTML strips tags — no DOCTYPE or raw markup in text
{
  const doc = parseHtmlDocument(SAMPLE_HTML);
  assert.equal(doc.kind, "html");
  assert.ok(!doc.text.includes("<!DOCTYPE"), "DOCTYPE should not appear in plain text");
  assert.ok(!doc.text.includes("<meta"), "meta tags should not appear in plain text");
  assert.ok(!doc.text.includes("<h1>"), "HTML tags should not appear in plain text");
  assert.match(doc.title, /Hotel Equities/);
}

// 2. script/style/nav/footer/noscript content removed
{
  const doc = parseHtmlDocument(SAMPLE_HTML);
  assert.ok(!/remove-me/i.test(doc.text), "script body should be removed");
  assert.ok(!/\.hidden/i.test(doc.text), "style content should be removed");
  assert.ok(!/Skip navigation/i.test(doc.text), "nav boilerplate should be removed");
  assert.ok(!/Copyright Hotel Equities/i.test(doc.text), "footer boilerplate should be removed");
  assert.ok(!/Enable JavaScript/i.test(doc.text), "noscript should be removed");
}

// 3. Meaningful body text retained
{
  const doc = parseHtmlDocument(SAMPLE_HTML);
  assert.match(doc.text, /Hotel Equities/);
  assert.match(doc.text, /third party management/i);
  assert.match(doc.metaDescription, /Third party hotel management/i);
  assert.ok(doc.text.includes(doc.title));
  assert.ok(doc.text.includes(doc.metaDescription));
}

// 4. Whitespace collapsed
{
  const doc = parseHtmlDocument("<html><body><p>One   two\n\n three</p></body></html>");
  assert.ok(!/\s{2,}/.test(doc.text.replace(/\n\n/g, " ")), "runs of whitespace should collapse");
  assert.match(doc.text, /One two three/);
}

// 5. Brand Reference Material relative file still resolves
{
  const plainPath = "plain-sample.txt";
  const plainContent = "Plain text only.\nNo HTML parsing.\n";
  writeFileSync(join(tmpBrand, plainPath), plainContent, "utf8");
  const doc = readLocalSourceText(plainPath);
  assert.equal(doc.kind, "txt");
  assert.equal(doc.text, plainContent);
  assert.equal(doc.resolvedRootKind, "brand");
}

// 6. Local .html in Brand root uses cheerio path
{
  const htmlPath = "sample.html";
  writeFileSync(join(tmpBrand, htmlPath), SAMPLE_HTML, "utf8");
  const doc = readLocalSourceText(htmlPath);
  assert.equal(doc.kind, "html");
  assert.equal(doc.resolvedRootKind, "brand");
  assert.ok(!doc.text.includes("<!DOCTYPE"));
  assert.match(doc.text, /third party management/i);
}

// 7. Operator Reference Material relative file resolves when absent from Brand root
{
  const rel = "Hotel Equities CALA/HE CALA Marketing Presentation March 2026.pdf";
  const operatorDir = join(tmpOperator, "Hotel Equities CALA");
  mkdirSync(operatorDir, { recursive: true });
  const pdfStub = "%PDF-1.4 stub-not-real-pdf";
  writeFileSync(join(operatorDir, "HE CALA Marketing Presentation March 2026.pdf"), pdfStub, "utf8");

  const resolved = resolveLocalSourceAbsolutePath(rel);
  assert.equal(resolved.resolvedRootKind, "operator");
  assert.equal(resolved.resolvedRoot, tmpOperator);

  // readLocalSourceText will fail PDF parse on stub — verify resolution only via helper
  let readErr;
  try {
    readLocalSourceText(rel);
  } catch (e) {
    readErr = e;
  }
  assert.ok(readErr, "stub PDF should fail extraction");
  assert.ok(!/Local file not found/i.test(String(readErr.message)), "file should resolve before PDF parse");
}

// 8. Brand root wins when both roots contain the same relative path
{
  const rel = "Hotel Equities/website/dual-root.html";
  const brandDir = join(tmpBrand, "Hotel Equities", "website");
  const operatorDir = join(tmpOperator, "Hotel Equities", "website");
  mkdirSync(brandDir, { recursive: true });
  mkdirSync(operatorDir, { recursive: true });
  writeFileSync(join(brandDir, "dual-root.html"), "<html><body><p>from brand root</p></body></html>", "utf8");
  writeFileSync(join(operatorDir, "dual-root.html"), "<html><body><p>from operator root</p></body></html>", "utf8");

  const doc = readLocalSourceText(rel);
  assert.equal(doc.resolvedRootKind, "brand");
  assert.match(doc.text, /from brand root/);
  assert.ok(!/from operator root/i.test(doc.text));
}

// 9. Operator-only HTML resolves and parses
{
  const rel = "Hotel Equities CALA/website/CALA page.html";
  const operatorDir = join(tmpOperator, "Hotel Equities CALA", "website");
  mkdirSync(operatorDir, { recursive: true });
  writeFileSync(join(operatorDir, "CALA page.html"), SAMPLE_HTML, "utf8");

  const doc = readLocalSourceText(rel);
  assert.equal(doc.resolvedRootKind, "operator");
  assert.equal(doc.kind, "html");
  assert.match(doc.text, /third party management/i);
}

// 10. Missing file error lists both attempted roots
{
  const rel = "missing/nowhere.html";
  let err;
  try {
    resolveLocalSourceAbsolutePath(rel);
  } catch (e) {
    err = e;
  }
  assert.ok(err);
  assert.match(err.message, /Local file not found: missing\/nowhere\.html/);
  assert.match(err.message, /Brand Reference Material/i);
  assert.match(err.message, /Operator Reference Material/i);
}

rmSync(tmpBrand, { recursive: true, force: true });
rmSync(tmpOperator, { recursive: true, force: true });

console.log("test-partner-intelligence-extract-source-text: ok");
