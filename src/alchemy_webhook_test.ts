import { expect, test } from "bun:test";
import {
  ALCHEMY_FUTURE_TOLERANCE_MS,
  ALCHEMY_MAX_AGE_MS,
  ALCHEMY_NETWORK_CHAIN_IDS,
  MAX_ALCHEMY_ACTIVITIES,
  MAX_ALCHEMY_BODY_BYTES,
  verifyAlchemyWebhookDelivery,
} from "./alchemy_webhook.ts";
import { ChainWebhookError } from "./chain_events.ts";
import { CHAIN_CONFIGURATIONS } from "./payment_types.ts";
import { TRANSFER_TOPIC } from "./rpc.ts";

const NOW = new Date("2026-10-18T04:00:00.000Z");
const SIGNING_KEY = "a1b2c3d4e5f60718";
const WEBHOOK_ID = "wh_fake_base";
const EVENT_ID = "whevt_fake_event";
const PAYER = `0x${"ab".repeat(20)}`;
const TREASURY = `0x${"cd".repeat(20)}`;
const TRANSACTION_HASH = `0x${"ef".repeat(32)}`;
const BLOCK_HASH = `0x${"12".repeat(32)}`;
const AMOUNT = 900719925474099312345n;
const OPTIONS = {
  signingKey: SIGNING_KEY,
  webhookId: WEBHOOK_ID,
  chainId: 8453 as const,
  now: NOW,
};

test("signed Alchemy ERC20 logs normalize exact units, coordinates, and provider-neutral identity", async () => {
  const { body, signature } = await delivery();
  const verified = await verifyAlchemyWebhookDelivery(body, signature, OPTIONS);
  expect(verified).toEqual({
    provider: "alchemy",
    subscriptionId: WEBHOOK_ID,
    eventId: EVENT_ID,
    deliveryId: JSON.stringify(["alchemy", WEBHOOK_ID, EVENT_ID]),
    createdAt: NOW.toISOString(),
    bodySha256: hex(new Uint8Array(await crypto.subtle.digest("SHA-256", body))),
    transfers: [
      {
        chainId: 8453,
        transactionHash: TRANSACTION_HASH,
        logIndex: 2,
        blockNumber: 123456789n,
        blockHash: BLOCK_HASH,
        tokenAddress: CHAIN_CONFIGURATIONS[8453].tokenAddress.toLowerCase(),
        payerAddress: PAYER,
        recipientAddress: TREASURY,
        amountUnits: AMOUNT,
        removed: false,
      },
    ],
  });
});

test("key is UTF-8 text, signatures bind exact bytes, and verification precedes JSON parsing", async () => {
  const { body, signature } = await delivery();
  await rejected(body, null, "invalid_chain_webhook_signature", 401);
  await rejected(body, "0".repeat(64), "invalid_chain_webhook_signature", 401);
  await rejected(body, `0x${signature}`, "invalid_chain_webhook_signature", 401);
  await rejected(
    new TextEncoder().encode("invalid JSON"),
    signature,
    "invalid_chain_webhook_signature",
    401,
  );
  const spacedBody = new TextEncoder().encode(`${new TextDecoder().decode(body)}\n`);
  await rejected(spacedBody, signature, "invalid_chain_webhook_signature", 401);
  const hexDecodedKey = Uint8Array.from(SIGNING_KEY.match(/../g)!, (pair) =>
    Number.parseInt(pair, 16),
  );
  await rejected(body, await sign(body, hexDecodedKey), "invalid_chain_webhook_signature", 401);
  await expect(
    verifyAlchemyWebhookDelivery(body, signature.toUpperCase(), OPTIONS),
  ).resolves.toBeDefined();
});

test("payload can only use the configured webhook key and expected network", async () => {
  const input = payload();
  input.webhookId = "wh_another_key";
  let signed = await delivery(input);
  await rejected(signed.body, signed.signature, "chain_webhook_subscription_mismatch", 401);
  input.webhookId = WEBHOOK_ID;
  for (const network of ["ETH_MAINNET", "BASE_SEPOLIA", "__proto__"]) {
    input.event.network = network;
    signed = await delivery(input);
    await rejected(signed.body, signed.signature, "chain_webhook_network_mismatch");
  }
  for (const [network, chainId] of Object.entries(ALCHEMY_NETWORK_CHAIN_IDS)) {
    input.event.network = network;
    signed = await delivery(input);
    const result = await verifyAlchemyWebhookDelivery(signed.body, signed.signature, {
      ...OPTIONS,
      chainId,
    });
    expect(result.transfers[0].chainId).toBe(chainId);
  }
});

test("signed createdAt enforces application freshness at the old and future boundaries", async () => {
  for (const age of [ALCHEMY_MAX_AGE_MS, -ALCHEMY_FUTURE_TOLERANCE_MS]) {
    const input = payload();
    input.createdAt = new Date(NOW.getTime() - age).toISOString();
    const { body, signature } = await delivery(input);
    await expect(verifyAlchemyWebhookDelivery(body, signature, OPTIONS)).resolves.toBeDefined();
  }
  for (const age of [ALCHEMY_MAX_AGE_MS + 1, -ALCHEMY_FUTURE_TOLERANCE_MS - 1]) {
    const input = payload();
    input.createdAt = new Date(NOW.getTime() - age).toISOString();
    const { body, signature } = await delivery(input);
    await rejected(body, signature, "stale_chain_webhook", 401);
  }
});

test("the official nine-digit timestamp is signed verbatim and normalized to milliseconds", async () => {
  const input = payload();
  input.createdAt = "2024-01-23T07:42:26.411977228Z";
  const { body, signature } = await delivery(input);
  const verified = await verifyAlchemyWebhookDelivery(body, signature, {
    ...OPTIONS,
    now: new Date("2024-01-23T07:42:26.411Z"),
  });
  expect(verified.createdAt).toBe("2024-01-23T07:42:26.411Z");
  expect(verified.bodySha256).toBe(
    hex(new Uint8Array(await crypto.subtle.digest("SHA-256", body))),
  );
});

test("timestamps require a valid UTC date rather than permissive Date.parse input", async () => {
  for (const createdAt of [
    "yesterday",
    "2026-10-18",
    "2026-02-30T04:00:00.000Z",
    "2026-10-18T04:00:00+00:00",
    "2026-10-18T04:00:00.1234567890Z",
  ]) {
    const input = payload();
    input.createdAt = createdAt;
    const { body, signature } = await delivery(input);
    await rejected(body, signature, "invalid_chain_webhook_timestamp");
  }
});

test("same event identity preserves raw body hash changes and removed inclusion state", async () => {
  const input = payload();
  let signed = await delivery(input);
  const original = await verifyAlchemyWebhookDelivery(signed.body, signed.signature, OPTIONS);
  const retry = await verifyAlchemyWebhookDelivery(signed.body, signed.signature, OPTIONS);
  expect(retry).toEqual(original);
  input.event.activity[0].log.removed = true;
  signed = await delivery(input);
  const removed = await verifyAlchemyWebhookDelivery(signed.body, signed.signature, OPTIONS);
  expect(removed.deliveryId).toBe(original.deliveryId);
  expect(removed.bodySha256).not.toBe(original.bodySha256);
  expect(removed.transfers[0].removed).toBe(true);
  input.id = "whevt_another_event";
  signed = await delivery(input);
  expect(
    (await verifyAlchemyWebhookDelivery(signed.body, signed.signature, OPTIONS)).deliveryId,
  ).not.toBe(original.deliveryId);
});

test("the documented erc20 category uses the same canonical log decoding as token", async () => {
  const input = payload();
  input.event.activity[0].category = "erc20";
  const { body, signature } = await delivery(input);
  const verified = await verifyAlchemyWebhookDelivery(body, signature, OPTIONS);
  expect(verified.transfers[0].amountUnits).toBe(AMOUNT);
  expect(verified.transfers[0].transactionHash).toBe(TRANSACTION_HASH);
});

test("irrelevant native transfers, ERC721, and other token events do not become ERC20 observations", async () => {
  const input = payload();
  input.event.activity.push({ ...activity(), category: "external" });
  input.event.activity.push({ ...activity(), category: "erc721" });
  const nft = activity();
  nft.category = "erc20";
  nft.log.topics.push(`0x${"0".repeat(64)}`);
  input.event.activity.push(nft);
  const otherEvent = activity();
  otherEvent.log.topics[0] = `0x${"0".repeat(64)}`;
  input.event.activity.push(otherEvent);
  const { body, signature } = await delivery(input);
  expect((await verifyAlchemyWebhookDelivery(body, signature, OPTIONS)).transfers).toHaveLength(1);
});

test("malformed ERC20 coordinates, topics, removal state, and uint256 values are rejected", async () => {
  const mutations: Array<(input: ReturnType<typeof payload>) => void> = [
    (input) => {
      input.event.activity[0].log.blockHash = "0x1234";
    },
    (input) => {
      input.event.activity[0].log.transactionHash = "0x1234";
    },
    (input) => {
      input.event.activity[0].log.blockNumber = "0x00";
    },
    (input) => {
      input.event.activity[0].log.blockNumber = `0x1${"0".repeat(64)}`;
    },
    (input) => {
      input.event.activity[0].log.logIndex = "0x20000000000000";
    },
    (input) => {
      input.event.activity[0].log.logIndex = "2";
    },
    (input) => {
      input.event.activity[0].log.topics[1] = `0x1${"0".repeat(63)}`;
    },
    (input) => {
      input.event.activity[0].log.data = "0x1";
    },
    (input) => {
      input.event.activity[0].log.address = "0x1234";
    },
  ];
  for (const mutate of mutations) {
    const input = payload();
    mutate(input);
    const { body, signature } = await delivery(input);
    await expect(verifyAlchemyWebhookDelivery(body, signature, OPTIONS)).rejects.toBeInstanceOf(
      ChainWebhookError,
    );
  }
  const invalidRemoved = payload();
  const removedBody = new TextEncoder().encode(
    JSON.stringify(invalidRemoved).replace('"removed":false', '"removed":"false"'),
  );
  await rejected(removedBody, await sign(removedBody), "invalid_chain_log_removed");
});

test("signed malformed JSON and envelope types do not escape as untyped exceptions", async () => {
  for (const raw of ["{", "null", "[]", '"text"']) {
    const body = new TextEncoder().encode(raw);
    await expect(
      verifyAlchemyWebhookDelivery(body, await sign(body), OPTIONS),
    ).rejects.toBeInstanceOf(ChainWebhookError);
  }
  const invalidUtf8 = new Uint8Array([0xff]);
  await rejected(invalidUtf8, await sign(invalidUtf8), "invalid_chain_webhook_json");
  const input = payload();
  input.type = "NFT_ACTIVITY";
  const { body, signature } = await delivery(input);
  await rejected(body, signature, "unsupported_chain_webhook_type");
});

test("body and activity caps reject complete signed deliveries rather than silently truncating them", async () => {
  const large = new Uint8Array(MAX_ALCHEMY_BODY_BYTES + 1).fill(32);
  await rejected(large, await sign(large), "chain_webhook_body_too_large", 413);
  const input = payload();
  input.event.activity = Array.from({ length: MAX_ALCHEMY_ACTIVITIES + 1 }, activity);
  const { body, signature } = await delivery(input);
  expect(body.byteLength).toBeLessThan(MAX_ALCHEMY_BODY_BYTES);
  await rejected(body, signature, "chain_webhook_activity_limit", 413);
});

function payload() {
  return {
    webhookId: WEBHOOK_ID,
    id: EVENT_ID,
    createdAt: NOW.toISOString(),
    type: "ADDRESS_ACTIVITY",
    event: { network: "BASE_MAINNET", activity: [activity()] },
  };
}

function activity() {
  return {
    category: "token",
    value: 0.1,
    rawContract: { rawValue: "0x1", address: `0x${"00".repeat(20)}` },
    log: {
      address: CHAIN_CONFIGURATIONS[8453].tokenAddress,
      topics: [TRANSFER_TOPIC, addressTopic(PAYER), addressTopic(TREASURY)],
      data: `0x${AMOUNT.toString(16).padStart(64, "0")}`,
      transactionHash: TRANSACTION_HASH,
      blockHash: BLOCK_HASH,
      blockNumber: "0x75bcd15",
      logIndex: "0x2",
      removed: false,
    },
  };
}

async function delivery(input = payload()) {
  const body = new TextEncoder().encode(JSON.stringify(input));
  return { body, signature: await sign(body) };
}

async function sign(
  body: Uint8Array<ArrayBuffer>,
  keyBytes = new TextEncoder().encode(SIGNING_KEY),
) {
  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return hex(new Uint8Array(await crypto.subtle.sign("HMAC", key, body)));
}

async function rejected(body: Uint8Array, signature: string | null, code: string, status = 400) {
  await expect(verifyAlchemyWebhookDelivery(body, signature, OPTIONS)).rejects.toMatchObject({
    code,
    status,
  });
}

function addressTopic(address: string) {
  return `0x${"0".repeat(24)}${address.slice(2)}`;
}

function hex(bytes: Uint8Array) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
