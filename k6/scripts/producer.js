import http from "k6/http";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:5001";

export function publish(payload) {
    return http.post(
        `${BASE_URL}/events`,
        JSON.stringify(payload),
        {
            headers: {
                "Content-Type": "application/json",
            },
        }
    );
}