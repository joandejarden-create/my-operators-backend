import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";

/** URL -> hero image URL (from browser-assisted pass) */
const IMAGE_BY_SOURCE_URL = {
  "https://www.choicehotels.com/dominican-republic/santo-domingo/quality-inn-hotels/do002":
    "https://www.choicehotels.com/hoteldam/do/do003/images/480/DO003ExteriorTemp1.jpg",
  "https://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030":
    "https://www.choicehotels.com/hoteldam/cb/cb030/images/2048/CB030TerraceTemp001_1.jpg",
  "https://www.choicehotels.com/es-us/sonora/obregon/quality-inn-hotels/mx054?mc=llrsxxmx":
    "https://www.choicehotels.com/hoteldam/mx/mx054/images/1280/MX054ExteriorTemp01_1.jpg",
  "https://www.choicehotels.com/trinidad-and-tobago/scarborough/comfort-inn-hotels/tt005":
    "https://www.choicehotels.com/hoteldam/tt/tt005/images/1280/TT005Hexterior01_1.jpeg",
  "https://www.choicehotels.com/costa-rica/san-jose/quality-inn-hotels/cr010":
    "https://www.choicehotels.com/hoteldam/cr/cr013/images/480/CR013ExteriorTemp01_1.jpg",
  "https://www.choicehotels.com/costa-rica/san-jose/sleep-inn-hotels/cr013":
    "https://www.choicehotels.com/hoteldam/cr/cr013/images/1280/CR013ExteriorTemp01_1.jpg",
  "https://www.choicehotels.com/chile/vallenar/park-inn-hotels/cl011":
    "https://www.choicehotels.com/hoteldam/cl/cl011/images/1280/CL011Exterior1_1.JPG",
  "https://www.choicehotels.com/dominican-republic/juan-dolio-beach/ascend-hotels/do012":
    "https://www.choicehotels.com/hoteldam/do/do012/images/2048/do012exterior2_1.jpg",
  "https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb018":
    "https://www.choicehotels.com/hoteldam/cb/cb018/images/2048/CB018PoolCourtyard4_1.JPG",
  "https://www.choicehotels.com/aruba/palm-beach/radisson-blu-hotels/aw007":
    "https://www.choicehotels.com/hoteldam/aw/aw007/images/2048/AW007Exterior5_1.JPG",
  "https://www.choicehotels.com/honduras/french-harbour-roatan/clarion-hotels/hn011":
    "https://www.choicehotels.com/hoteldam/hn/hn011/images/1280/Exterior1.JPG",
  "https://www.choicehotels.com/mexico/cuajimalpa-de-morelos/sleep-inn-hotels/mx108":
    "https://www.choicehotels.com/hoteldam/mx/mx108/images/1280/MX108Exterior01_1.jpg",
  "https://www.choicehotels.com/argentina/rosario/radisson-red-hotels/aa024":
    "https://www.choicehotels.com/hoteldam/aa/aa024/images/1280/AA024ExteriorTemp1.jpg",
  "https://www.choicehotels.com/es-mx/panama/panama/radisson-hotels/pn018":
    "https://www.choicehotels.com/hoteldam/pn/pn018/images/1280/PN018AerialTemp1_1.jpg",
};

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

function firstChoiceUrl(body) {
  const m = String(body || "").match(/https?:\/\/[^\s)]+/i);
  return m ? m[0] : "";
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const rows = await base(TABLE)
    .select({
      filterByFormula: `{Slot Key} = "materials.caseStudy"`,
      maxRecords: 1000,
    })
    .all();

  let updated = 0;
  for (const r of rows) {
    const images = r.get("Image");
    if (Array.isArray(images) && images.length) continue;
    const body = String(r.get("Body") || "");
    const srcUrl = firstChoiceUrl(body);
    if (!srcUrl) continue;
    const imageUrl = IMAGE_BY_SOURCE_URL[srcUrl];
    if (!imageUrl) continue;
    const brand = String(r.get("Brand Name") || "").trim();
    const title = String(r.get("Title") || "").trim();
    console.log(`- ${brand}: attach "${title}"`);
    if (!dryRun) {
      await base(TABLE).update(r.id, { Image: [{ url: imageUrl }] });
    }
    updated++;
  }
  console.log(`${dryRun ? "Would update" : "Updated"} ${updated} row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

