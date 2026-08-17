#!/usr/bin/env node
import "../load-env.js";
import { getBrandPresenceHotelById } from "../api/brand-presence.js";

const req = { params: { recordId: "rec54jkFM0zveGu7P" } };
const res = {
  status(code) {
    this.code = code;
    return this;
  },
  json(body) {
    console.log(JSON.stringify({
      amenities: body.hotel?.amenities,
      amenitiesDisplayCount: body.hotel?.amenitiesDisplay?.length,
      amenitiesDisplay: body.hotel?.amenitiesDisplay?.map((x) => x.label),
    }, null, 2));
  },
};

await getBrandPresenceHotelById(req, res);
