import "dotenv/config";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36";

async function probe(code) {
  const url = `https://all.accor.com/hotel/${code}/index.en.shtml`;
  const res = await fetch(url, { headers: { "User-Agent": UA }, redirect: "follow" });
  const html = await res.text();
  const title = (html.match(/<title[^>]*>([^<]+)/i) || [])[1] || "";
  const imgs = [...html.matchAll(/ahstatic\.com\/photos\/[^"'\\\s<>]+_p_1024x768/gi)].length;
  console.log(`${code}\t${res.status}\timgs1024=${imgs}\t${title.slice(0, 100)}`);
}

for (const c of ["A7L5", "B331", "B986", "6835", "B6V6", "9149", "6599", "B1Y6"]) {
  await probe(c);
}

// Fairmont SF alternate
for (const c of ["A7B5", "A052", "0521", "B052", "A5B2", "0571"]) {
  await probe(c);
}
