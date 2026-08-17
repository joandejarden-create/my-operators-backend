const imgs = [
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518bded1ae16ef415eb_founder-joan-dejarden.png",
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69ad14c6a7716b55405234_testimonial-avatar-natalie.jpg",
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69ad21a26d598adc5289cd_testimonial-avatar-sarah.jpg",
];
const out = [];
for (const u of imgs) {
  const r = await fetch(u, { method: "HEAD" });
  out.push({ u: u.split("/").pop(), status: r.status, type: r.headers.get("content-type"), len: r.headers.get("content-length") });
}
console.log(JSON.stringify(out, null, 2));
