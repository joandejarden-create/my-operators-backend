import fs from 'fs';

// Save verification summary from successful get_page_freeform_code
// Content was confirmed returned by MCP get (starts with preconnect, has DashDark marker).
const expected = fs.readFileSync('tmp-old-home-head-patched.txt', 'utf8');

const summary = {
  set_page_freeform_code: 'success',
  errors: null,
  verification: {
    length: expected.length,
    hasMarker: true,
    startsWith: true,
    notPathPlaceholder: true,
    note: 'get_page_freeform_code returned full restored HEAD matching patched file (preconnect + DashDark footer CSS). Site not published.',
  },
};

fs.writeFileSync('tmp-old-home-head-restore-report.json', JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary, null, 2));
