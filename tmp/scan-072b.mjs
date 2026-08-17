import https from "https";
function get(url){return new Promise((resolve,reject)=>{https.get(url,res=>{if(res.statusCode>=300&&res.statusCode<400&&res.headers.location)return get(res.headers.location).then(resolve,reject);let d="";res.on("data",c=>d+=c);res.on("end",()=>resolve(d));}).on("error",reject);});}
const urls=[
"https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/css/mvp-deal-capture.webflow.shared.cabc49b77.css",
"https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css",
"https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d4d297aebfdd31871d780_dealality-old-home-section-type.v20260801a.css",
];
for (const u of urls) {
  const d = await get(u);
  console.log("\n==", u.split("/").pop(), d.length);
  const pats = [/rgba\(255,\s*255,\s*255,\s*0\.72\)/g, /255,255,255,\.72/g, /\.72\)/g];
  for (const p of pats) {
    const m = [...d.matchAll(p)].length;
    if (m) console.log(p, m);
  }
  // sample context around 0.72
  let idx = 0, n=0;
  while ((idx = d.indexOf("0.72", idx)) !== -1 && n < 5) {
    console.log(d.slice(Math.max(0,idx-60), idx+80).replace(/\s+/g," "));
    idx += 4; n++;
  }
}
