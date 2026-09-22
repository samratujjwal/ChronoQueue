import { describe, expect, it, vi } from "vitest";
import { safeError, safely } from "./errors.js";

describe("safeError", () => {
  it("extracts name, message, and string code without the stack", () => {
    const error = new Error("boom");
    (error as { code?: string }).code = "ECONNREFUSED";
    const info = safeError(error);
    expect(info.name).toBe("Error");
    expect(info.message).toBe("boom");
    expect(info.code).toBe("ECONNREFUSED");
    expect("stack" in info).toBe(false);
  });

  it("truncates very long messages", () => {
    const info = safeError(new Error("x".repeat(1000)));
    expect(info.message.length).toBeLessThanOrEqual(501);
  });

  it("handles non-Error values", () => {
    expect(safeError("plain string").name).toBe("UnknownError");
    expect(safeError("plain string").message).toBe("plain string");
    expect(safeError(undefined).message).toBe("undefined");
  });

  it("ignores non-string codes", () => {
    const error = new Error("x") as Error & { code?: unknown };
    error.code = 42;
    expect(safeError(error).code).toBeUndefined();
  });
});

describe("safely", () => {
  it("swallows throws so observability never breaks processing", () => {
    const fn = vi.fn(() => {
      throw new Error("metrics backend exploded");
    });
    expect(() => safely(fn)).not.toThrow();
    expect(fn).toHaveBeenCalled();
  });
});
