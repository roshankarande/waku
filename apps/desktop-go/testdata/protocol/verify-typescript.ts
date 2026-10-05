import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { MAX_WIRE_MESSAGE_BYTES, PROTOCOL_VERSION } from "../../../../packages/waku-client/src/generated/constants.ts";
import type { ClientMessage } from "../../../../packages/waku-client/src/generated/ClientMessage.ts";
import type { ServerMessage } from "../../../../packages/waku-client/src/generated/ServerMessage.ts";

const fixtureRoot = fileURLToPath(new URL(".", import.meta.url));
const read = (name: string): unknown => JSON.parse(readFileSync(join(fixtureRoot, name), "utf8"));
const array = (name: string): Record<string, unknown>[] => {
  const value = read(name);
  if (!Array.isArray(value) || value.some((entry) => !entry || typeof entry !== "object" || Array.isArray(entry))) {
    throw new Error(`${name} is not an object fixture array`);
  }
  return value as Record<string, unknown>[];
};

if (PROTOCOL_VERSION !== 7 || MAX_WIRE_MESSAGE_BYTES !== 48 * 1024 * 1024) {
  throw new Error("generated protocol constants do not match protocol-v7 fixtures");
}

const hello: ClientMessage = {
  type: "hello",
  protocolVersion: PROTOCOL_VERSION,
  token: "fixture-token",
  clientId: "00000000-0000-4000-8000-000000000001",
  resumeFrom: [],
};
const serverHello: ServerMessage = { type: "hello", protocolVersion: PROTOCOL_VERSION, daemonVersion: "fixture" };
void hello;
void serverHello;

for (const [name, key] of [["commands-v7.json", "type"], ["responses-v7.json", "type"], ["events-v7.json", "kind"]] as const) {
  for (const [index, entry] of array(name).entries()) {
    if (typeof entry[key] !== "string" || entry[key] === "") {
      throw new Error(`${name}[${index}] has no ${key} discriminant`);
    }
  }
}

const boundaryText = readFileSync(join(fixtureRoot, "event-integer-boundaries.json"), "utf8");
for (const exact of ["9007199254740993", "18446744073709551615", "-9223372036854775808", "9223372036854775807"]) {
  if (!boundaryText.includes(exact)) throw new Error(`integer boundary ${exact} is missing`);
}

console.log("TypeScript protocol-v7 fixtures and generated bindings verified");
