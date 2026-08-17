import fs from "fs";
import puppeteer from "puppeteer";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
const b64 = fs.readFileSync("tmp-wordmark.png").toString("base64");
await page.setContent(`<!doctype html><canvas id="c"></canvas><script>
const img = new Image();
img.onload = () => {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  canvas.width = img.width; canvas.height = img.height;
  ctx.drawImage(img,0,0);
  const { data, width, height } = ctx.getImageData(0,0,img.width,img.height);
  let minX=width,minY=height,maxX=0,maxY=0;
  for (let y=0;y<height;y++){
    for (let x=0;x<width;x++){
      const a=data[(y*width+x)*4+3];
      if(a>8){minX=Math.min(minX,x);minY=Math.min(minY,y);maxX=Math.max(maxX,x);maxY=Math.max(maxY,y);}
    }
  }
  const pad=28;
  const left=Math.max(0,minX-pad), top=Math.max(0,minY-pad);
  const w=Math.min(width-left,maxX-minX+1+pad*2), h=Math.min(height-top,maxY-minY+1+pad*2);
  const out=document.createElement('canvas');
  out.width=w; out.height=h;
  out.getContext('2d').drawImage(img,left,top,w,h,0,0,w,h);
  window.__crop = {left,top,w,h,dataUrl:out.toDataURL('image/png')};
};
img.src = 'data:image/png;base64,${b64}';
</script>`, { waitUntil: "load" });
await page.waitForFunction(() => window.__crop, { timeout: 30000 });
const crop = await page.evaluate(() => window.__crop);
const buf = Buffer.from(crop.dataUrl.split(",")[1], "base64");
fs.writeFileSync("tmp-wordmark-crop.png", buf);
console.log({ left: crop.left, top: crop.top, w: crop.w, h: crop.h, bytes: buf.length });
await browser.close();
