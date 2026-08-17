const query = `query {
  hotel(ctyhocn: "SJOCUQQ", language: "en") {
    name
    facilityOverview {
      shortDesc
      headline
      locationShortDesc
      hotelTeaserText
      directionsTo
    }
  }
}`;

const res = await fetch("https://www.hilton.com/graphql/customer", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 Chrome/124",
    Origin: "https://www.hilton.com",
    Referer: "https://www.hilton.com/en/hotels/sjocuqq-gran-hotel-costa-rica/",
  },
  body: JSON.stringify({ query }),
});
console.log(JSON.stringify(await res.json(), null, 2));
