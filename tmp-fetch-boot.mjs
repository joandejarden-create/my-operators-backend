import https from "https";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve({ status: res.statusCode, body: d, headers: res.headers }));
      })
      .on("error", reject);
  });
}

const urls = [
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a%2F689e5ba67671442434f3ca35%2F6a6a23d33f4842a30976ec30%2Foldhomebootguardw19-1.0.0.js",
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a5d9abc7042b67c554dfb_old-home-hero-fit-boot.v20260729c.js",
];

for (const u of urls) {
  const { status, body } = await get(u);
  console.log("\n====", status, u.slice(-70), "len", body.length, "====");
  console.log(body.slice(0, 3500));
}
