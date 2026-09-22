#!/usr/bin/env python3
"""Validate resolved wallet workload results through the official MCP client.

Use a disposable active public-wallet run with at least two wallets. This
starts one transaction attempt and stops its workload, leaving the run active.
Development dependencies are the same as mcp_discovery_client.py.

    BBP_MCP_TOKEN=... python mcp_wallet_result_schema.py \
      --endpoint http://127.0.0.1:PORT/mcp --run-id TEST_RUN
"""

import argparse
import asyncio
from datetime import timedelta
import os
import uuid

import httpx
from jsonschema import Draft202012Validator
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


async def exercise(session, run_id):
    tools = {}
    cursor = None
    while True:
        page = await session.list_tools(cursor=cursor)
        tools.update((tool.name, tool) for tool in page.tools)
        cursor = page.nextCursor
        if cursor is None:
            break

    async def operation(name, arguments):
        Draft202012Validator(tools[name].inputSchema).validate(arguments)
        result = await session.call_tool(name, arguments)
        value = result.structuredContent
        while value["result_family"] == "operation":
            if value["state"] == "succeeded":
                value = value["terminal_result"]
                break
            if value["state"] in ("failed", "cancelled"):
                if (
                    name == "workload.stop"
                    and value["terminal_error"]["code"] == "workload_not_active"
                ):
                    return value["terminal_error"]
                raise AssertionError(value["terminal_error"])
            await asyncio.sleep(0.05)
            # ClientSession validates this complete operation envelope against
            # the advertised operation.get outputSchema, including its result.
            result = await session.call_tool(
                "operation.get", {"operation_id": value["operation_id"]}
            )
            value = result.structuredContent
        assert value["result_family"] != "error", value
        Draft202012Validator(tools[name].outputSchema).validate(value)
        return value

    identity = {"run_id": run_id, "workload_id": "schema-" + uuid.uuid4().hex}
    workload = {
        "type": "wallet_transactions",
        "strategy": "random_bruteforce",
        "retained_balance_percentage": 80,
        "transaction_count": 1,
        "amount": "0.01000000",
        "fee": "0.00001000",
        "timeout_sec": 5,
    }
    try:
        await operation("workload.start", {**identity, "workload": workload})
        result = await operation("workload.inspect", identity)
        resolved = result["configuration"]
        assert resolved["duration"] is None
        assert resolved["transaction_rate_millionths"] is None
        assert resolved["fee_reserve_satoshis"] > 0
        assert resolved["retained_balance_basis_points"] == 8000

        # Valid output must not broaden the strict input contract. Check the
        # nullable input and a computed field independently, so either would
        # catch accidental reuse of the result schema for workload.start.
        validator = Draft202012Validator(tools["workload.start"].inputSchema)
        for extra in ({"duration": None}, {"fee_reserve_satoshis": 1000}):
            assert not validator.is_valid(
                {**identity, "workload": {**workload, **extra}}
            ), extra
    finally:
        await operation("workload.stop", identity)
    print("Wallet workload results validate; strict input remains unchanged.")


async def run(args):
    async with httpx.AsyncClient(
        headers={"Authorization": "Bearer " + os.environ["BBP_MCP_TOKEN"]},
        timeout=30,
        trust_env=False,
    ) as http:
        async with streamable_http_client(args.endpoint, http_client=http) as streams:
            async with ClientSession(
                streams[0], streams[1], read_timeout_seconds=timedelta(seconds=30)
            ) as session:
                await session.initialize()
                async with asyncio.timeout(45):
                    await exercise(session, args.run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--run-id", required=True)
    asyncio.run(run(parser.parse_args()))
