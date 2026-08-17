fetch("https://www.dealality.com/old-home?v=" + Date.now())
  .then((r) => r.text())
  .then((t) => {
    console.log({
      contactSection: /id=["']contact["']/.test(t),
      everyHotel: t.includes("Every hotel opportunity deserves"),
      leavingTable: t.includes("Leaving on the Table"),
    });
  });
