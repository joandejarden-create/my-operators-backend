import fs from 'fs';

// Rebuild content from patched file and print as a single JSON string to stdout in 3 labeled chunks
const content = fs.readFileSync(
  'c:/Users/joand/OneDrive/Documents/deal-capture-proxy/tmp-old-home-head-patched.txt',
  'utf8'
);
const encoded = JSON.stringify(content);
const size = 9000;
for (let i = 0, off = 0; off < encoded.length; i++, off += size) {
  const chunk = encoded.slice(off, off + size);
  fs.writeFileSync(
    `c:/Users/joand/OneDrive/Documents/deal-capture-proxy/tmp-enc-${i}.txt`,
    chunk
  );
  console.log(`ENC_${i}_LEN=${chunk.length}`);
}
console.log(`ENC_TOTAL=${encoded.length}`);
console.log(`CONTENT_LEN=${content.length}`);
console.log(`HAS_MARKER=${content.includes('DashDark-style 4-col footer')}`);
