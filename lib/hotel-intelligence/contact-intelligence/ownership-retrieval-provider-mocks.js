/**
 * Offline mock deps for retrieval-provider comparison — no network.
 */
export function createMockDiscoveryDeps() {
  return {
    contextDevSearch: async ({ query }) => {
      const hotelish = /Rendezvous Bay Hotel/i.test(query);
      const generic = /""/.test(query) || !/"[^"]+"/.test(query);
      if (generic) {
        return {
          ok: true,
          data: {
            results: [
              { title: "Investor.gov", url: "https://www.investor.gov/", description: "Investor resources" },
            ],
          },
        };
      }
      if (hotelish) {
        return {
          ok: true,
          data: {
            results: [
              {
                title: "Book Rendezvous Bay",
                url: "https://www.booking.com/hotel/ai/rendezvous.html",
                description: "Rates at Rendezvous Bay Hotel",
              },
              {
                title: "Anguilla tourism note",
                url: "https://example.com/anguilla-tourism",
                description: "Rendezvous Bay Hotel is a beachfront property",
              },
            ],
          },
        };
      }
      return {
        ok: true,
        data: {
          results: [
            {
              title: "Property listing",
              url: "https://example.com/listing",
              description: `${query} hotel listing`,
            },
          ],
        },
      };
    },
    contextDevScrapeMarkdown: async ({ url }) => {
      if (/booking\.com/i.test(url)) {
        return { ok: true, data: "Book your stay. Best rates. Cancel free." };
      }
      if (/example\.com\/anguilla/i.test(url)) {
        return {
          ok: true,
          data: "Rendezvous Bay Hotel is a beachfront property on Anguilla. No ownership disclosed.",
        };
      }
      return { ok: true, data: "Generic page without ownership language for the target hotel." };
    },
    parallelDiscover: async ({ hotel_seed }) => {
      if (/Rendezvous/i.test(hotel_seed.hotel_name)) {
        return {
          cost_usd: 0.1,
          ownership_claims: [
            {
              named_entity_or_person: "Jeremiah Gumbs",
              relationship: "HISTORICAL_OWNER",
              supporting_passage: "our Rendezvous Bay Hotel, owned by one Jeremiah Gumbs.",
              source_url: "https://www.chicagotribune.com/1996/09/29/back-to-anguilla/",
              publication_date: "1996-09-29",
              currentness: "HISTORICAL",
            },
          ],
          sources: [
            {
              url: "https://www.chicagotribune.com/1996/09/29/back-to-anguilla/",
              title: "Back to Anguilla",
              passage: "our Rendezvous Bay Hotel, owned by one Jeremiah Gumbs.",
            },
          ],
        };
      }
      if (/great house/i.test(hotel_seed.hotel_name)) {
        return {
          cost_usd: 0.1,
          ownership_claims: [
            {
              named_entity_or_person: "Mega REIT Portfolio Co",
              relationship: "PROPERTY_OWNER",
              supporting_passage: "Our portfolio includes luxury resorts worldwide.",
              source_url: "https://example.com/portfolio",
              currentness: "CURRENT_AS_OF_STATED_DATE",
            },
          ],
          sources: [
            {
              url: "https://example.com/portfolio",
              passage: "Our portfolio includes luxury resorts worldwide.",
            },
          ],
        };
      }
      return { cost_usd: 0.1, ownership_claims: [], sources: [] };
    },
  };
}
