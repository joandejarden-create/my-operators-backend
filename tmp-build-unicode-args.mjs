import fs from 'fs';

const content = fs.readFileSync('tmp-old-home-head-patched.txt', 'utf8');

// Escape for safe embedding: keep as JSON string with < > as unicode escapes
const unicodeEscaped = JSON.stringify(content)
  .replace(/</g, '\\u003c')
  .replace(/>/g, '\\u003e');

// unicodeEscaped is a full JSON string literal including surrounding quotes
const args = {
  actions: [
    {
      label: 'set_old_home_head',
      set_page_freeform_code: {
        page_id: '68108c2a063eeb5d1bd7ae90',
        location: 'head',
        content: '__CONTENT_PLACEHOLDER__',
      },
    },
  ],
  context:
    'Restores Old Home page HEAD freeform custom code with DashDark footer CSS appended before style close.',
};

// Build final JSON with unicode-escaped content spliced in
const shell = JSON.stringify(args);
const out = shell.replace(
  '"__CONTENT_PLACEHOLDER__"',
  unicodeEscaped
);

fs.writeFileSync('tmp-callmcp-args-unicode.json', out, 'utf8');

// Verify round-trip
const parsed = JSON.parse(out);
const c = parsed.actions[0].set_page_freeform_code.content;
console.log(
  JSON.stringify({
    outBytes: Buffer.byteLength(out),
    len: c.length,
    starts: c.startsWith('<link rel="preconnect"'),
    marker: c.includes('DashDark-style 4-col footer'),
    match: c === content,
  })
);
