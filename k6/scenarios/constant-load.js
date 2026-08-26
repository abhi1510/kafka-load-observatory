import { check } from "k6";

import { createPayload } from "../scripts/payload.js";
import { publish } from "../scripts/producer.js";

export const options = {
    vus: Number(__ENV.VUS || 20),
    duration: __ENV.DURATION || "2m",

    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<500"],
    },
};

export default function () {
    const response = publish(createPayload());

    check(response, {
        "status is 200": (r) => r.status === 200,
    });
}