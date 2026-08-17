/** Temporary CDN / alternate-source probe for Lane 2 image gaps. */
async function get(url) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml,image/*,*/*",
    },
  });
  const text = await res.text();
  return { status: res.status, len: text.length, text, finalUrl: res.url };
}

function extract(text, re) {
  return [...new Set([...text.matchAll(re)].map((m) => m[1] || m[0]))].slice(0, 40);
}

const targets = [
  {
    id: "radisson-brand",
    url: "https://www.radissonhotels.com/en-us/brand/radisson-collection",
  },
  {
    id: "choice-radisson-collection",
    url: "https://www.choicehotels.com/radisson-collection",
  },
  {
    id: "accor-handwritten-brand",
    url: "https://all.accor.com/a/en/brands/handwritten-collection.html",
  },
  {
    id: "hilton-tapestry-brand",
    url: "https://www.hilton.com/en/brands/tapestry-collection/",
  },
  {
    id: "hilton-tapestry-alt",
    url: "https://www.hilton.com/en/tapestry/",
  },
];

for (const t of targets) {
  process.stdout.write(`${t.id}... `);
  try {
    const { status, len, text } = await get(t.url);
    const imgs = extract(
      text,
      /https?:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s<>]*)?/gi
    );
    const ah = extract(text, /https?:\/\/www\.ahstatic\.com\/photos\/[^"'\\\s<>]+/gi);
    const media = extract(text, /https?:\/\/media\.radissonhotels[^"'\\\s<>]+/gi);
    const hilton = extract(
      text,
      /https?:\/\/[^"'\\\s<>]*hilton[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s<>]*)?/gi
    );
    const ihg = extract(text, /https?:\/\/digital\.ihg\.com\/is\/image\/ihg\/[^"'\\\s<>]+/gi);
    const hotelCodes = extract(text, /hotel\/([A-Z0-9]{4})/gi);
    console.log(
      `status=${status} len=${len} imgs=${imgs.length} ah=${ah.length} media=${media.length} hilton=${hilton.length} ihg=${ihg.length} codes=${hotelCodes.slice(0, 8).join(",")}`
    );
    if (ah.length) console.log("  ah sample", ah.slice(0, 5));
    if (media.length) console.log("  media sample", media.slice(0, 5));
    if (hilton.length) console.log("  hilton sample", hilton.slice(0, 5));
    if (imgs.length && !ah.length && !media.length && !hilton.length) {
      console.log("  img sample", imgs.slice(0, 8));
    }
  } catch (err) {
    console.log(`ERR ${err.message}`);
  }
}
