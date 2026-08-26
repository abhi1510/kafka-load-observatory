export function createPayload() {
    return {
        id: crypto.randomUUID(),
        timestamp: new Date().toISOString(),
        data: {
            source: "k6:klo",
            value: Math.floor(Math.random() * 100000),
        },
    };
}