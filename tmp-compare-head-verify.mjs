import fs from 'fs';

const expected = fs.readFileSync('tmp-old-home-head-patched.txt', 'utf8');
const got = fs.readFileSync('tmp-old-home-head-get-verify.txt', 'utf8');

console.log(
  JSON.stringify(
    {
      setStatus: 'success',
      expectedLength: expected.length,
      gotLength: got.length,
      lengthsMatch: expected.length === got.length,
      hasMarker: got.includes('DashDark-style 4-col footer'),
      startsWith: got.startsWith('<link rel="preconnect"'),
      notPathPlaceholder: !got.includes('tmp-old-home-head-patched.txt'),
      exactMatch: expected === got,
    },
    null,
    2
  )
);
