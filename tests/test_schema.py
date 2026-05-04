import json
import os
import tempfile
import unittest

from luxai.magpie.schema import JsonRpcSchema, JsonRpcError, McpSchema
from tests.fake_rpc_transports import FakeRpcRequester, FakeRpcResponder


MCP_TOOLS = [
    {
        "name": "add",
        "description": "Add two numbers",
        "inputSchema": {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
        "outputSchema": {"type": "number"},
    },
    {
        "name": "greet",
        "description": "Say hello",
        "inputSchema": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        },
    },
]

MCP_TOOLS_JSON = json.dumps(MCP_TOOLS)


# ---------------------------------------------------
# JsonRpcSchema — basic dispatch
# ---------------------------------------------------

class TestJsonRpcSchemaDispatch(unittest.TestCase):

    def setUp(self):
        self.schema = JsonRpcSchema()

        @self.schema.method()
        def add(a: int, b: int) -> int:
            return a + b

        @self.schema.method("math.multiply")
        def multiply(a: int, b: int) -> int:
            return a * b

    def _req(self, method, params=None, req_id=1):
        r = {"jsonrpc": "2.0", "method": method, "id": req_id}
        if params is not None:
            r["params"] = params
        return r

    def _notif(self, method, params=None):
        r = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            r["params"] = params
        return r

    def test_named_params(self):
        resp = self.schema.dispatch(self._req("add", {"a": 3, "b": 4}))
        self.assertEqual(resp["result"], 7)

    def test_positional_params(self):
        resp = self.schema.dispatch(self._req("add", [10, 20]))
        self.assertEqual(resp["result"], 30)

    def test_explicit_name(self):
        resp = self.schema.dispatch(self._req("math.multiply", {"a": 3, "b": 5}))
        self.assertEqual(resp["result"], 15)

    def test_no_params(self):
        self.schema.register("ping", lambda: "pong")
        resp = self.schema.dispatch(self._req("ping"))
        self.assertEqual(resp["result"], "pong")

    def test_method_not_found(self):
        resp = self.schema.dispatch(self._req("unknown"))
        self.assertEqual(resp["error"]["code"], -32601)

    def test_invalid_params(self):
        resp = self.schema.dispatch(self._req("add", {"a": 1}))
        self.assertEqual(resp["error"]["code"], -32602)

    def test_internal_error(self):
        self.schema.register("boom", lambda: 1 / 0)
        resp = self.schema.dispatch(self._req("boom"))
        self.assertEqual(resp["error"]["code"], -32603)

    def test_invalid_request_not_dict(self):
        resp = self.schema.dispatch("not a dict")
        self.assertEqual(resp["error"]["code"], -32600)

    def test_notification_returns_none(self):
        self.schema.register("log", lambda msg: None)
        self.assertIsNone(self.schema.dispatch(self._notif("log", {"msg": "hi"})))

    def test_unknown_notification_returns_none(self):
        self.assertIsNone(self.schema.dispatch(self._notif("unknown.event")))

    def test_batch(self):
        batch = [self._req("add", {"a": 1, "b": 2}, 1), self._req("add", {"a": 10, "b": 20}, 2)]
        responses = self.schema.dispatch(batch)
        results = {r["id"]: r["result"] for r in responses}
        self.assertEqual(results[1], 3)
        self.assertEqual(results[2], 30)

    def test_batch_notification_excluded(self):
        self.schema.register("log", lambda msg="": None)
        batch = [self._req("add", {"a": 1, "b": 2}, 1), self._notif("log", {"msg": "x"})]
        self.assertEqual(len(self.schema.dispatch(batch)), 1)

    def test_responder_uses_schema(self):
        responder = FakeRpcResponder(
            recv_items=[({"jsonrpc": "2.0", "method": "add", "params": {"a": 5, "b": 6}, "id": 99}, "CTX")]
        )
        responder._schema = self.schema
        responder.handle_once(timeout=1.0)
        self.assertEqual(responder.send_calls[0][0]["result"], 11)

    def test_responder_no_send_for_notification(self):
        self.schema.register("log", lambda msg="": None)
        responder = FakeRpcResponder(
            recv_items=[({"jsonrpc": "2.0", "method": "log", "params": {"msg": "hi"}}, "CTX")]
        )
        responder._schema = self.schema
        responder.handle_once(timeout=1.0)
        self.assertEqual(len(responder.send_calls), 0)


# ---------------------------------------------------
# JsonRpcSchema — registration styles
# ---------------------------------------------------

class TestJsonRpcSchemaRegistration(unittest.TestCase):

    # --- register() with explicit input_schema ---

    def test_register_with_input_schema(self):
        schema = JsonRpcSchema()
        schema.register(
            name="face_look",
            description="Move robot eyes",
            input_schema={
                "type": "object",
                "properties": {"l_eye": {"type": "array"}, "r_eye": {"type": "array"}},
                "required": ["l_eye", "r_eye"],
            },
        )
        self.assertIn("face_look", schema._methods)
        entry = schema._methods["face_look"]
        self.assertEqual(entry["description"], "Move robot eyes")
        self.assertIn("l_eye", entry["input_schema"]["required"])

    def test_register_stub_no_func(self):
        schema = JsonRpcSchema()
        schema.register(
            name="move",
            input_schema={"type": "object", "properties": {"x": {"type": "number"}}, "required": ["x"]},
        )
        resp = schema.dispatch({"jsonrpc": "2.0", "method": "move", "params": {"x": 1.0}, "id": 1})
        self.assertEqual(resp["error"]["code"], -32601)
        self.assertIn("not implemented", resp["error"]["message"])

    def test_register_with_output_schema(self):
        schema = JsonRpcSchema()
        schema.register(
            name="add",
            input_schema={"type": "object", "properties": {"a": {"type": "number"}, "b": {"type": "number"}}, "required": ["a", "b"]},
            output_schema={"type": "number"},
        )
        self.assertEqual(schema._methods["add"]["output_schema"], {"type": "number"})

    def test_decorator_infers_output_schema(self):
        schema = JsonRpcSchema()

        @schema.method()
        def add(a: float, b: float) -> float:
            return a + b

        self.assertEqual(schema._methods["add"]["output_schema"], {"type": "number"})

    def test_decorator_no_return_annotation(self):
        schema = JsonRpcSchema()

        @schema.method()
        def log(msg: str):
            pass

        self.assertIsNone(schema._methods["log"]["output_schema"])

    # --- handler() decorator ---

    def test_handler_attaches_implementation(self):
        schema = JsonRpcSchema()
        schema.register(
            name="face_look",
            description="Move robot eyes",
            input_schema={
                "type": "object",
                "properties": {"l_eye": {"type": "array"}, "r_eye": {"type": "array"}},
                "required": ["l_eye", "r_eye"],
            },
        )

        @schema.handler("face_look")
        def handle_face_look(l_eye, r_eye):
            return {"ok": True, "l_eye": l_eye}

        resp = schema.dispatch({"jsonrpc": "2.0", "method": "face_look",
                                "params": {"l_eye": [10, 5], "r_eye": [-10, 5]}, "id": 1})
        self.assertEqual(resp["result"]["ok"], True)
        self.assertEqual(resp["result"]["l_eye"], [10, 5])

    def test_handler_unknown_method_raises(self):
        schema = JsonRpcSchema()
        with self.assertRaises(KeyError):
            @schema.handler("nonexistent")
            def fn(): ...

    def test_handler_sets_description_if_empty(self):
        schema = JsonRpcSchema()
        schema.register(name="move", input_schema={"type": "object"})

        @schema.handler("move")
        def handle_move():
            """Move the robot."""
            pass

        self.assertEqual(schema._methods["move"]["description"], "Move the robot.")

    # --- decorator ---

    def test_decorator_registers_and_dispatches(self):
        schema = JsonRpcSchema()

        @schema.method()
        def greet(name: str) -> str:
            """Say hello."""
            return f"Hello, {name}!"

        resp = schema.dispatch({"jsonrpc": "2.0", "method": "greet",
                                "params": {"name": "World"}, "id": 1})
        self.assertEqual(resp["result"], "Hello, World!")

    def test_decorator_stub_body_no_handler(self):
        schema = JsonRpcSchema()

        @schema.method()
        def face_look(l_eye: list, r_eye: list, duration: float = 0.0): ...

        self.assertIn("face_look", schema._methods)
        props = schema._methods["face_look"]["input_schema"]["properties"]
        self.assertEqual(props["l_eye"]["type"], "array")

    # --- wrap / unwrap ---

    def test_wrap_builds_envelope(self):
        schema = JsonRpcSchema()
        req = schema.wrap("add", {"a": 1, "b": 2})
        self.assertEqual(req["jsonrpc"], "2.0")
        self.assertEqual(req["method"], "add")
        self.assertIn("id", req)
        self.assertEqual(req["params"], {"a": 1, "b": 2})

    def test_unwrap_result(self):
        schema = JsonRpcSchema()
        result = schema.unwrap({"jsonrpc": "2.0", "result": 42, "id": 1})
        self.assertEqual(result, 42)

    def test_unwrap_raises_json_rpc_error(self):
        schema = JsonRpcSchema()
        with self.assertRaises(JsonRpcError) as ctx:
            schema.unwrap({"jsonrpc": "2.0", "error": {"code": -32601, "message": "Not found"}, "id": 1})
        self.assertEqual(ctx.exception.code, -32601)


# ---------------------------------------------------
# RpcRequester — schema proxy
# ---------------------------------------------------

class TestRpcRequesterProxy(unittest.TestCase):

    def _make_requester(self, response):
        schema = JsonRpcSchema()
        schema.register(
            name="face_look",
            input_schema={
                "type": "object",
                "properties": {"l_eye": {"type": "array"}, "r_eye": {"type": "array"}},
                "required": ["l_eye", "r_eye"],
            },
        )
        schema.register(
            name="add",
            input_schema={
                "type": "object",
                "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                "required": ["a", "b"],
            },
        )
        req = FakeRpcRequester(response=response)
        req._schema = schema
        return req

    def _json_rpc_response(self, result, req_id=1):
        return {"jsonrpc": "2.0", "result": result, "id": req_id}

    def test_proxy_call(self):
        client = self._make_requester(self._json_rpc_response({"moved": True}))
        result = client.face_look(l_eye=[10, 5], r_eye=[-10, 5])
        self.assertEqual(result, {"moved": True})

    def test_proxy_passes_method_name(self):
        client = self._make_requester(self._json_rpc_response(7))
        client.add(a=3, b=4)
        sent = client.calls[0][0]
        self.assertEqual(sent["method"], "add")
        self.assertEqual(sent["params"], {"a": 3, "b": 4})

    def test_proxy_timeout(self):
        client = self._make_requester(self._json_rpc_response(7))
        client.add(a=3, b=4, _timeout=5.0)
        self.assertEqual(client.calls[0][1], 5.0)

    def test_timeout_not_in_params(self):
        client = self._make_requester(self._json_rpc_response(7))
        client.add(a=3, b=4, _timeout=5.0)
        sent_params = client.calls[0][0].get("params", {})
        self.assertNotIn("_timeout", sent_params)

    def test_base_call_with_schema(self):
        client = self._make_requester(self._json_rpc_response(7))
        result = client.call("add", a=3, b=4)
        self.assertEqual(result, 7)

    def test_base_call_timeout_kwarg(self):
        client = self._make_requester(self._json_rpc_response(7))
        client.call("add", a=3, b=4, _timeout=2.0)
        self.assertEqual(client.calls[0][1], 2.0)

    def test_proxy_without_schema_raises_attribute_error(self):
        client = FakeRpcRequester(response=None)
        with self.assertRaises(AttributeError):
            client.face_look(l_eye=[0, 0], r_eye=[0, 0])

    def test_json_rpc_error_raised(self):
        client = self._make_requester(
            {"jsonrpc": "2.0", "error": {"code": -32601, "message": "Not found"}, "id": 1}
        )
        with self.assertRaises(JsonRpcError) as ctx:
            client.add(a=1, b=2)
        self.assertEqual(ctx.exception.code, -32601)


# ---------------------------------------------------
# McpSchema tests
# ---------------------------------------------------

class TestMcpSchema(unittest.TestCase):

    def setUp(self):
        self.schema = McpSchema(name="testbot", version="0.1.0")

        @self.schema.method()
        def move_motor(motor: str, angle: float) -> dict:
            """Move a robot motor to a specific angle."""
            return {"moved": motor, "angle": angle}

        self.schema.register(
            "robot.home",
            lambda: {"homed": True},
            description="Home all motors",
        )

    def _req(self, method, params=None, req_id=1):
        r = {"jsonrpc": "2.0", "method": method, "id": req_id}
        if params is not None:
            r["params"] = params
        return r

    def test_initialize(self):
        resp = self.schema.dispatch(self._req("initialize", {
            "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "test", "version": "0.0.1"},
        }))
        result = resp["result"]
        self.assertEqual(result["protocolVersion"], "2024-11-05")
        self.assertEqual(result["serverInfo"]["name"], "testbot")
        self.assertIn("tools", result["capabilities"])

    def test_initialized_notification_no_reply(self):
        self.assertIsNone(self.schema.dispatch({"jsonrpc": "2.0", "method": "notifications/initialized"}))

    def test_tools_list(self):
        resp = self.schema.dispatch(self._req("tools/list", {}))
        tool_names = {t["name"] for t in resp["result"]["tools"]}
        self.assertIn("move_motor", tool_names)
        self.assertIn("robot.home", tool_names)
        self.assertNotIn("initialize", tool_names)
        self.assertNotIn("tools/list", tool_names)

    def test_tools_list_has_description(self):
        resp = self.schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertIn("Move a robot motor", tools["move_motor"]["description"])
        self.assertEqual(tools["robot.home"]["description"], "Home all motors")

    def test_tools_list_has_input_schema(self):
        resp = self.schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertEqual(tools["move_motor"]["inputSchema"]["properties"]["motor"]["type"], "string")

    def test_decorator_output_schema_in_tools_list(self):
        resp = self.schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertEqual(tools["move_motor"]["outputSchema"], {"type": "object"})

    def test_tools_call_success(self):
        resp = self.schema.dispatch(self._req("tools/call", {
            "name": "move_motor", "arguments": {"motor": "shoulder", "angle": 1.57},
        }))
        self.assertFalse(resp["result"]["isError"])
        self.assertIn("shoulder", resp["result"]["content"][0]["text"])

    def test_tools_call_no_args(self):
        resp = self.schema.dispatch(self._req("tools/call", {"name": "robot.home"}))
        self.assertFalse(resp["result"]["isError"])

    def test_tools_call_unknown_tool(self):
        resp = self.schema.dispatch(self._req("tools/call", {"name": "nonexistent"}))
        self.assertTrue(resp["result"]["isError"])

    def test_tools_call_bad_args(self):
        resp = self.schema.dispatch(self._req("tools/call", {
            "name": "move_motor", "arguments": {"wrong_param": 99},
        }))
        self.assertTrue(resp["result"]["isError"])

    def test_ping(self):
        self.assertEqual(self.schema.dispatch(self._req("ping", {}))["result"], {})

    def test_register_object_output_schema_in_tools_list(self):
        schema = McpSchema()
        schema.register(
            name="status",
            description="Get status",
            input_schema={"type": "object"},
            output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}},
        )
        resp = schema.dispatch({"jsonrpc": "2.0", "method": "tools/list", "id": 1})
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertIn("status", tools)
        self.assertEqual(tools["status"]["outputSchema"]["type"], "object")

    def test_register_scalar_output_schema_not_in_tools_list(self):
        # MCP structuredContent must be a dict — scalar outputSchema must not be exposed
        schema = McpSchema()
        schema.register(
            name="scale",
            description="Scale a value",
            input_schema={"type": "object", "properties": {"value": {"type": "number"}}},
            output_schema={"type": "number"},
        )
        resp = schema.dispatch({"jsonrpc": "2.0", "method": "tools/list", "id": 1})
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertIn("scale", tools)
        self.assertNotIn("outputSchema", tools["scale"])

    def test_no_output_schema_omitted_from_tools_list(self):
        schema = McpSchema()
        schema.register(name="log", description="Log a message",
                        input_schema={"type": "object", "properties": {"msg": {"type": "string"}}})
        resp = schema.dispatch({"jsonrpc": "2.0", "method": "tools/list", "id": 1})
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertNotIn("outputSchema", tools["log"])


# ---------------------------------------------------
# McpSchema loading tests
# ---------------------------------------------------

class TestMcpSchemaLoading(unittest.TestCase):

    def _req(self, method, params=None, req_id=1):
        r = {"jsonrpc": "2.0", "method": method, "id": req_id}
        if params is not None:
            r["params"] = params
        return r

    # --- from_json_string ---

    def test_from_json_string_list(self):
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON)
        self.assertIn("add", schema._tools)
        self.assertIn("greet", schema._tools)

    def test_from_json_string_with_tools_key(self):
        schema = McpSchema.from_json_string(json.dumps({"tools": MCP_TOOLS}))
        self.assertIn("add", schema._tools)

    def test_from_json_string_server_metadata(self):
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON, name="testbot", version="0.2.0")
        resp = schema.dispatch(self._req("initialize", {}))
        info = resp["result"]["serverInfo"]
        self.assertEqual(info["name"], "testbot")
        self.assertEqual(info["version"], "0.2.0")

    def test_from_json_string_tools_list(self):
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON)
        resp = schema.dispatch(self._req("tools/list", {}))
        names = {t["name"] for t in resp["result"]["tools"]}
        self.assertEqual(names, {"add", "greet"})

    def test_from_json_string_preserves_description(self):
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON)
        resp = schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertEqual(tools["add"]["description"], "Add two numbers")

    def test_from_json_string_preserves_input_schema(self):
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON)
        resp = schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertEqual(tools["add"]["inputSchema"]["properties"]["a"]["type"], "number")
        self.assertIn("a", tools["add"]["inputSchema"]["required"])

    def test_from_json_string_scalar_output_schema_not_exposed(self):
        # add has outputSchema: {"type": "number"} which is scalar — must not appear in tools/list
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON)
        resp = schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertNotIn("outputSchema", tools["add"])
        self.assertNotIn("outputSchema", tools["greet"])

    def test_from_json_string_object_output_schema_exposed(self):
        tools_with_object_schema = [
            {
                "name": "get_info",
                "description": "Get info",
                "inputSchema": {"type": "object"},
                "outputSchema": {"type": "object", "properties": {"status": {"type": "string"}}},
            }
        ]
        schema = McpSchema.from_json_string(json.dumps(tools_with_object_schema))
        resp = schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertEqual(tools["get_info"]["outputSchema"]["type"], "object")

    def test_from_json_string_missing_name_raises(self):
        with self.assertRaises(ValueError):
            McpSchema.from_json_string('[{"description": "no name", "inputSchema": {}}]')

    def test_from_json_string_invalid_type_raises(self):
        with self.assertRaises(ValueError):
            McpSchema.from_json_string('"not a list or dict"')

    # --- from_json_file ---

    def test_from_json_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(MCP_TOOLS, f)
            path = f.name
        try:
            schema = McpSchema.from_json_file(path)
            self.assertIn("add", schema._tools)
            self.assertIn("greet", schema._tools)
        finally:
            os.unlink(path)

    def test_from_json_file_scalar_output_schema_not_in_tools(self):
        # Scalar outputSchema from JSON must not be stored in _tools (structuredContent constraint)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(MCP_TOOLS, f)
            path = f.name
        try:
            schema = McpSchema.from_json_file(path)
            self.assertNotIn("output_schema", schema._tools["add"])
        finally:
            os.unlink(path)

    # --- handler() after loading ---

    def test_handler_attaches_implementation(self):
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON)

        @schema.handler("add")
        def _add(a, b):
            return a + b

        resp = schema.dispatch(self._req("tools/call", {"name": "add", "arguments": {"a": 3, "b": 4}}))
        self.assertFalse(resp["result"]["isError"])
        self.assertIn("7", resp["result"]["content"][0]["text"])

    def test_tools_call_before_handler_is_error(self):
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON)
        resp = schema.dispatch(self._req("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}))
        self.assertTrue(resp["result"]["isError"])

    def test_builtins_not_exposed_as_tools(self):
        schema = McpSchema.from_json_string(MCP_TOOLS_JSON)
        names = {t["name"] for t in
                 schema.dispatch(self._req("tools/list", {}))["result"]["tools"]}
        self.assertNotIn("initialize", names)
        self.assertNotIn("tools/list", names)
        self.assertNotIn("ping", names)


# ---------------------------------------------------
# McpSchema structuredContent tests
# ---------------------------------------------------

class TestMcpStructuredContent(unittest.TestCase):
    """
    MCP structuredContent must be a dict (Record<string, unknown>).
    It is only included in tools/call responses when:
      - the tool has an object-type outputSchema
      - the actual result is a dict
    """

    def _req(self, method, params=None, req_id=1):
        r = {"jsonrpc": "2.0", "method": method, "id": req_id}
        if params is not None:
            r["params"] = params
        return r

    def test_dict_result_with_object_schema_includes_structured_content(self):
        schema = McpSchema()

        @schema.method()
        def get_status() -> dict:
            """Return status."""
            return {"ok": True, "code": 0}

        resp = schema.dispatch(self._req("tools/call", {"name": "get_status"}))
        result = resp["result"]
        self.assertFalse(result["isError"])
        self.assertIn("structuredContent", result)
        self.assertEqual(result["structuredContent"], {"ok": True, "code": 0})

    def test_scalar_result_never_includes_structured_content(self):
        schema = McpSchema()

        @schema.method()
        def add(a: float, b: float) -> float:
            return a + b

        resp = schema.dispatch(self._req("tools/call", {"name": "add", "arguments": {"a": 3, "b": 4}}))
        result = resp["result"]
        self.assertFalse(result["isError"])
        self.assertNotIn("structuredContent", result)
        self.assertIn("7", result["content"][0]["text"])

    def test_scalar_output_schema_not_exposed_in_tools_list(self):
        schema = McpSchema()

        @schema.method()
        def add(a: float, b: float) -> float:
            return a + b

        resp = schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertNotIn("outputSchema", tools["add"])

    def test_object_output_schema_exposed_in_tools_list(self):
        schema = McpSchema()

        @schema.method()
        def get_status() -> dict:
            """Return status."""
            return {"ok": True}

        resp = schema.dispatch(self._req("tools/list", {}))
        tools = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertIn("outputSchema", tools["get_status"])
        self.assertEqual(tools["get_status"]["outputSchema"], {"type": "object"})

    def test_no_output_schema_no_structured_content(self):
        schema = McpSchema()
        schema.register("echo", lambda: "pong", description="Echo")

        resp = schema.dispatch(self._req("tools/call", {"name": "echo"}))
        result = resp["result"]
        self.assertFalse(result["isError"])
        self.assertNotIn("structuredContent", result)

    def test_dict_result_without_output_schema_no_structured_content(self):
        # Even if the function returns a dict, structuredContent is only set when
        # the tool has an outputSchema (object type) declared.
        schema = McpSchema()
        schema.register("get_data", lambda: {"x": 1}, description="Get data")

        resp = schema.dispatch(self._req("tools/call", {"name": "get_data"}))
        result = resp["result"]
        self.assertFalse(result["isError"])
        self.assertNotIn("structuredContent", result)

    def test_structured_content_roundtrip_via_register_explicit_schema(self):
        schema = McpSchema()
        schema.register(
            name="get_info",
            func=lambda: {"name": "robot", "version": "1.0"},
            description="Get info",
            output_schema={"type": "object", "properties": {"name": {"type": "string"}, "version": {"type": "string"}}},
        )
        resp = schema.dispatch(self._req("tools/call", {"name": "get_info"}))
        result = resp["result"]
        self.assertFalse(result["isError"])
        self.assertIn("structuredContent", result)
        self.assertEqual(result["structuredContent"]["name"], "robot")


if __name__ == "__main__":
    unittest.main()
