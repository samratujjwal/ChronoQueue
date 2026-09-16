import { afterAll, describe, expect, it } from "vitest";
import { connection } from "./connection.js";

describe("redis connection", () => {
  it("is reachable", async () => {
    const pong = await connection.ping();
    expect(pong).toBe("PONG");
  });

  afterAll(async () => {
    await connection.quit();
  });
});
