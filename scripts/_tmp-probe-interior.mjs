const pids = ["flf21", "ncb10"];
const extra = [
  "GuestRoom1_1.jpg",
  "GuestRoom01_1.jpg",
  "Guestroom1_1.jpg",
  "Room1_1.jpg",
  "Suite1_1.jpg",
  "Kitchen1_1.jpg",
  "Kitchen01_1.jpg",
  "ExtendedStay1_1.jpg",
  "Bedroom1_1.jpg",
  "RoomView1_1.jpg",
  "GuestRoomTemp01_1.jpg",
  "GuestRoomTemp1.jpg",
  "GuestRoom1.jpg",
  "KitchenTemp01_1.jpg",
];

async function ok(url) {
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
    return res.ok && (res.headers.get("content-type") || "").includes("image") ? url : "";
  } catch {
    return "";
  }
}

for (const pid of pids) {
  const cc = pid.slice(0, 2);
  const P = pid.toUpperCase();
  console.log("---", pid);
  for (const name of extra) {
    for (const size of ["1280", "2048"]) {
      const url = `https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${P}${name}`;
      const hit = await ok(url);
      if (hit) console.log(hit);
      const url2 = `https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${name}`;
      const hit2 = await ok(url2);
      if (hit2) console.log(hit2);
    }
  }
}
