#!/usr/bin/env python3
r"""Source-independent acceptance using the official MCP Python client.

Development dependencies only: ``pip install 'mcp>=1.30,<2' 'jsonschema>=4.23,<5'``.
An external orchestrator starts a fresh BBP editor and supplies its credentials:

    BBP_MCP_ENDPOINT=http://127.0.0.1:PORT/mcp BBP_MCP_TOKEN=... \
      python mcp_discovery_client.py \
        --case firo /absolute/path/to/firod discovery-firo 2 \
        --case bitcoin /absolute/path/to/bitcoind discovery-bitcoin 2 \
        --case monero /absolute/path/to/monerod discovery-monero 2

The client reads no BBP source, documentation, scenarios, credentials files, or
run artifacts. Scenario intent comes from arguments; contracts come from MCP.
It launches, observes, stops, and removes each run before starting the next.
JSON events go to stdout so the orchestrator can retain acceptance evidence.
"""

import argparse
import asyncio
import copy
from datetime import timedelta
import hashlib
import importlib.metadata
import json
import os
import sys
import time

import httpx
from jsonschema import Draft202012Validator, ValidationError
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from pydantic import AnyUrl
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def payload(document):
    """Application resources may wrap their data in an evidence envelope."""
    if isinstance(document, dict) and "family" in document and "data" in document:
        return document["data"]
    return document


class DiscoveryClient:
    def __init__(self, session, timeout):
        self.session = session
        self.timeout = timeout
        self.tools = {}
        self.resources = {}
        self.schemas = {}
        self.inputs = {}
        self.outputs = {}
        self.results = {}
        self.registry = Registry()  # No filesystem or network schema retrieval.
        self.validator_cache = {}
        self.calls = 0

    async def list_all(self, method, field):
        items = []
        cursor = None
        seen = set()
        while True:
            page = await method(cursor=cursor)
            items.extend(getattr(page, field))
            cursor = page.nextCursor
            if cursor is None:
                return items
            require(cursor not in seen, f"{field} pagination repeated a cursor")
            seen.add(cursor)

    async def resource(self, uri):
        require(uri in self.resources, f"Resource was not discovered: {uri}")
        started = time.monotonic()
        result = await self.session.read_resource(AnyUrl(uri))
        require(len(result.contents) == 1, f"Expected one JSON resource: {uri}")
        content = result.contents[0]
        require(str(content.uri) == uri, f"Resource URI changed: {uri}")
        require(
            content.mimeType == self.resources[uri].mimeType
            and content.mimeType in ("application/json", "application/schema+json"),
            f"Resource does not match its discovered JSON media type: {uri}",
        )
        require(hasattr(content, "text"), f"Expected text resource: {uri}")
        if uri.startswith("bbp:///schemas"):
            emit(
                "schema_resource_read", uri=uri,
                bytes=len(content.text.encode()),
                elapsed_ms=round((time.monotonic() - started) * 1000),
            )
        return payload(json.loads(content.text))

    def validator(self, schema):
        key = hashlib.sha256(json.dumps(schema, sort_keys=True).encode()).digest()
        if key in self.validator_cache:
            return self.validator_cache[key]
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, registry=self.registry)
        self.validator_cache[key] = validator
        return validator

    @staticmethod
    def validate(validator, value, context):
        error = next(validator.iter_errors(value), None)
        if error is not None:
            path = "/".join(str(part) for part in error.absolute_path) or "<root>"
            # Union failures otherwise print entire nested scenario schemas.
            details = error
            while details.context:
                details = min(details.context, key=lambda item: len(item.context))
            raise AssertionError(
                f"{context} violates discovered schema at {path}: "
                f"{details.message[:700]}"
            )

    async def discover(self):
        initialized = await self.session.initialize()
        require(initialized.capabilities.tools is not None, "No tools capability")
        require(
            initialized.capabilities.resources is not None,
            "No resources capability",
        )
        started = time.monotonic()
        tools = await self.list_all(self.session.list_tools, "tools")
        elapsed_ms = round((time.monotonic() - started) * 1000)
        self.tools = {tool.name: tool.model_dump(by_alias=True) for tool in tools}
        emit(
            "tools_discovered", count=len(tools), elapsed_ms=elapsed_ms,
            schema_bytes=sum(
                len(json.dumps(tool[key], separators=(",", ":")).encode())
                for tool in self.tools.values() for key in ("inputSchema", "outputSchema")
            ),
        )
        resources = await self.list_all(self.session.list_resources, "resources")
        self.resources = {str(resource.uri): resource for resource in resources}
        require(len(self.tools) == len(tools), "Duplicate tool names")
        require(len(self.resources) == len(resources), "Duplicate resource URIs")

        self.capabilities = await self.resource("bbp:///capabilities")
        bundle = await self.resource("bbp:///schemas")
        for uri in self.resources:
            if uri.startswith("bbp:///schemas/"):
                self.schemas[uri] = await self.resource(uri)
        require(self.schemas, "No reusable schema resources were discovered")
        self.registry = self.registry.with_resources(
            (uri, Resource.from_contents(schema, default_specification=DRAFT202012))
            for uri, schema in self.schemas.items()
        )
        emit("schema_validation_started", schemas=len(self.schemas), tools=len(self.tools))
        for uri, schema in self.schemas.items():
            self.validator(schema)
            emit("schema_validated", uri=uri)

        for name in ("scenario", "workload", "simulation_command"):
            uri = f"bbp:///schemas/{name}"
            require(uri in self.schemas, f"Missing reusable {name} schema")
            require(self.schemas[uri] == bundle[name], f"{name} schema differs")
        self.scenario_schema = self.schemas["bbp:///schemas/scenario"]
        self.scenario_validator = self.validator(self.scenario_schema)
        require(
            self.capabilities["scenario_schema"] == self.scenario_schema,
            "Capabilities disagree with the authoritative scenario resource",
        )
        require(
            {entry["name"] for entry in self.capabilities["operations"]}
            == set(self.tools),
            "Capabilities and tools/list advertise different operations",
        )
        require(
            set(bundle["operations"]) == set(self.tools),
            "Schema bundle and tools/list advertise different operations",
        )
        for name, tool in self.tools.items():
            require(bool(tool.get("description")), f"{name} has no description")
            require(tool.get("outputSchema") is not None, f"{name} has no output schema")
            require(
                bundle["operations"][name]
                == {"input": tool["inputSchema"], "output": tool["outputSchema"]},
                f"{name} differs between schemas and tools/list",
            )
            self.inputs[name] = self.validator(tool["inputSchema"])
            self.outputs[name] = self.validator(tool["outputSchema"])
            for value in (tool.get("_meta") or {}).values():
                if isinstance(value, str) and value.startswith("bbp:///schemas/"):
                    require(value in self.schemas, f"{name} links an undiscovered schema")
        for name, schema in bundle["result_schemas"].items():
            uri = f"bbp:///schemas/results/{name}"
            require(self.schemas.get(uri) == schema, f"{name} result schema differs")
            self.results[name] = self.validator(schema)
        require(
            set(self.results)
            == {entry["name"] for entry in self.capabilities["result_families"]},
            "Capabilities and reusable result schemas disagree",
        )
        require(await self.resource("bbp:///run_registry") == [], "Editor is not empty")
        emit(
            "discovered",
            sdk=importlib.metadata.version("mcp"),
            protocol=initialized.protocolVersion,
            tools=len(self.tools),
            resources=len(self.resources),
            schemas=len(self.schemas),
        )

    def validate_result(self, value, context):
        family = value.get("result_family")
        require(family in self.results, f"{context}: undiscovered result family {family}")
        self.validate(self.results[family], value, f"{context} result family")
        if "terminal_result" in value:
            self.validate_result(value["terminal_result"], f"{context} terminal")
        if "terminal_error" in value:
            self.validate_result(value["terminal_error"], f"{context} terminal error")

    async def call(self, name, arguments, invalid=False):
        require(name in self.tools, f"Tool was not discovered: {name}")
        if invalid:
            require(
                not self.inputs[name].is_valid(arguments),
                f"Negative probe is accepted by {name}'s input schema",
            )
        else:
            self.validate(self.inputs[name], arguments, f"{name} arguments")
        try:
            result = await self.session.call_tool(name, arguments)
        except RuntimeError as error:
            rejected = error.__context__
            if isinstance(rejected, ValidationError):
                while rejected.parent is not None:
                    rejected = rejected.parent
                pending = [rejected]
                leaves = []
                while pending:
                    item = pending.pop()
                    if item.context:
                        pending.extend(reversed(item.context))
                    else:
                        leaves.append({
                            "instance_path": list(item.absolute_path),
                            "schema_path": list(item.absolute_schema_path),
                            "message": item.message[:300],
                        })
                emit("sdk_output_schema_rejected", tool=name,
                     result=rejected.instance, errors=leaves)
            raise
        self.calls += 1
        require(isinstance(result.structuredContent, dict), f"{name}: no structured result")
        value = result.structuredContent
        self.validate(self.outputs[name], value, f"{name} output")
        self.validate_result(value, name)
        if result.isError:
            require(value["result_family"] == "error", f"{name}: untyped error")
            if not invalid:
                raise AssertionError(f"{name} failed: {json.dumps(value)}")
        return value

    async def operation(self, name, arguments, invalid=False):
        value = await self.call(name, arguments, invalid=invalid)
        deadline = time.monotonic() + self.timeout
        cancelling = False
        while value["result_family"] == "operation":
            state = value["state"]
            if state == "succeeded":
                terminal = value["terminal_result"]
                self.validate(self.outputs[name], terminal, f"{name} terminal output")
                return terminal
            if state in ("failed", "cancelled"):
                error = value["terminal_error"]
                if invalid:
                    return error
                raise AssertionError(f"{name} {state}: {json.dumps(error)}")
            if time.monotonic() >= deadline:
                if cancelling:
                    raise AssertionError(f"{name} cancellation did not settle")
                await self.call("operation.cancel", {"operation_id": value["operation_id"]})
                cancelling = True
                deadline = time.monotonic() + min(self.timeout, 30)
            await asyncio.sleep(0.2)
            value = await self.call("operation.get", {"operation_id": value["operation_id"]})
        return value

    def scenario(self, chain, binary, run_id, count, shared_network):
        require(chain in self.capabilities["chains"], f"Unsupported chain intent: {chain}")
        # These are user-intent fields, checked against contracts just discovered.
        # No scenario fixture, daemon alias table, or copied schema is used.
        scenario = {
            "run_id": run_id,
            "chain": chain,
            "chains": {chain: {"driver": chain, "default_binary": binary}},
            "block_production": {"enabled": False},
            "nodes": [
                {"id": f"n{index + 1}", "chain": chain, "role": "base"}
                for index in range(count)
            ],
        }
        if shared_network:
            scenario["isolated_network"] = False
        self.validate(self.scenario_validator, scenario, "Constructed scenario")
        return scenario

    async def negative_probe(self, scenario):
        invalid = copy.deepcopy(scenario)
        invalid["nodes"][0]["id"] = "invalid.node"
        require(
            not self.scenario_validator.is_valid(invalid),
            "Scenario schema admits a node ID containing a dot",
        )
        result = await self.operation("scenario.validate", {"scenario": invalid}, invalid=True)
        require(
            result["result_family"] == "error"
            or (result["result_family"] == "validation" and result["valid"] is False),
            "Runtime accepted the schema-invalid node ID",
        )
        emit("invalid_node_id_rejected", result=result)

    async def resolve_empty(self, run_id):
        scenario = {"run_id": run_id, "nodes": 0}
        self.validate(self.scenario_validator, scenario, "Empty inventory scenario")
        result = await self.operation("scenario.resolve", {"scenario": scenario})
        resolved = result["scenario"]
        schema = self.schemas["bbp:///schemas/resolved_scenario"]
        self.validate(self.validator(schema), resolved, "Resolved empty inventory")
        require(resolved["run_id"] == run_id, "Empty resolution changed run identity")
        require(resolved["nodes"] == 0, "Empty resolution introduced nodes")
        emit("empty_inventory_resolved", run_id=run_id)

    async def observe(self, run_id, chain, count):
        registry = await self.resource("bbp:///run_registry")
        matches = [entry for entry in registry if entry["run_id"] == run_id]
        require(len(matches) == 1 and matches[0]["state"] == "active", "Run is not active")
        deadline = time.monotonic() + min(self.timeout, 30)
        while True:
            nodes = await self.resource("bbp:///nodes")
            observed = nodes["nodes_summary"]
            metrics = [node.get("last_metrics", {}) for node in observed]
            if len(metrics) == count and all(
                metric.get("process_running") is True
                and isinstance(metric.get("pid"), int) and metric["pid"] > 0
                for metric in metrics
            ):
                break
            require(
                time.monotonic() < deadline,
                f"Run has no observed running daemon metrics: {json.dumps(observed)}",
            )
            await asyncio.sleep(0.2)
        result = await self.operation(
            "evidence.query",
            {"run_id": run_id, "families": ["lifecycle", "nodes", "processes"]},
        )
        require(result["items"], "Active run has no observed evidence")
        require(len(observed) == count, "Observed node count differs from intent")
        require(
            {node["node_id"] for node in observed} == {f"n{index + 1}" for index in range(count)},
            "Observed node identities differ from intent",
        )
        require(all(node.get("chain") == chain for node in observed), "Observed chain differs")
        emit("observed", run_id=run_id, chain=chain, nodes=observed)

    async def cleanup(self, run_id):
        registry = await self.resource("bbp:///run_registry")
        matches = [entry for entry in registry if entry["run_id"] == run_id]
        if not matches:
            return
        deadline = time.monotonic() + min(self.timeout, 30)
        while matches[0]["state"] == "stopping":
            require(time.monotonic() < deadline, "Run did not finish stopping before cleanup")
            await asyncio.sleep(0.2)
            registry = await self.resource("bbp:///run_registry")
            matches = [entry for entry in registry if entry["run_id"] == run_id]
            if not matches:
                return
        if matches[0]["state"] in ("starting", "active"):
            stopped = await self.operation("run.stop", {"run_id": run_id})
            require(stopped["state"] == "stopped", "run.stop did not stop the run")
            emit("stopped", run_id=run_id, result=stopped)
        cleaned = await self.operation(
            "run.clean", {"run_id": run_id, "remove_retained_artifacts": True}
        )
        require(cleaned["complete"] is True, "Cleanup did not complete")
        require(await self.resource("bbp:///run_registry") == [], "Cleanup left a registered run")
        emit("cleaned", run_id=run_id, result=cleaned)

    async def exercise(self, case, shared_network, negative):
        chain, binary, run_id, raw_count = case
        count = int(raw_count)
        require(count > 0, "Representative launches require at least one real node")
        scenario = self.scenario(chain, binary, run_id, count, shared_network)
        emit("constructed", chain=chain, scenario=scenario)
        validation = await self.operation("scenario.validate", {"scenario": scenario})
        require(validation["valid"] is True, f"Constructed scenario was rejected: {validation}")
        resolved = await self.operation("scenario.resolve", {"scenario": scenario})
        resolved_schema = self.schemas["bbp:///schemas/resolved_scenario"]
        self.validate(self.validator(resolved_schema), resolved["scenario"], "Resolved scenario")
        require(resolved["scenario"]["run_id"] == run_id, "Resolution changed run identity")
        if negative:
            await self.negative_probe(scenario)
        try:
            launched = await self.operation("run.launch", {"scenario": scenario})
            require(launched["run_id"] == run_id, "Launch changed run identity")
            require(launched["state"] == "active", "Launch did not become active")
            await self.observe(run_id, chain, count)
        except Exception as error:
            try:
                await self.cleanup(run_id)
            except Exception as cleanup_error:
                raise AssertionError(f"{error}; cleanup also failed: {cleanup_error}") from error
            raise
        else:
            await self.cleanup(run_id)
        emit("case_passed", chain=chain, run_id=run_id, nodes=count)


async def run(args):
    # Endpoint traffic is local; environment proxies must not intercept tokens.
    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {args.token}"},
        timeout=httpx.Timeout(args.timeout),
        trust_env=False,
    ) as http_client:
        async with streamable_http_client(args.endpoint, http_client=http_client) as streams:
            async with ClientSession(
                streams[0], streams[1], read_timeout_seconds=timedelta(seconds=args.timeout)
            ) as session:
                client = DiscoveryClient(session, args.timeout)
                await client.discover()
                await client.resolve_empty(args.case[0][2])
                for index, case in enumerate(args.case):
                    await client.exercise(case, args.shared_network, negative=index == 0)
                emit("acceptance_passed", cases=len(args.case), validated_tool_results=client.calls)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--endpoint", default=os.environ.get("BBP_MCP_ENDPOINT"))
    parser.add_argument(
        "--case", nargs=4, action="append", required=True,
        metavar=("CHAIN", "BINARY", "RUN_ID", "NODE_COUNT"),
    )
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--shared-network", action="store_true", help="Request non-isolated networking")
    args = parser.parse_args()
    args.token = os.environ.get("BBP_MCP_TOKEN")
    if not args.endpoint or not args.token:
        parser.error("Supply --endpoint/BBP_MCP_ENDPOINT and BBP_MCP_TOKEN")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        asyncio.run(run(args))
    except Exception as error:
        # Task groups wrap failures; retain the actionable leaf messages.
        def messages(item):
            nested = getattr(item, "exceptions", None)
            return [message for child in nested for message in messages(child)] if nested else [str(item)]

        emit("acceptance_failed", errors=messages(error))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
