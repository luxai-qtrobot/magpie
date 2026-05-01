import json
import tempfile
import unittest

from luxai.magpie.schema import JsonRpcSchema, JsonRpcError, McpSchema
from tests.fake_rpc_transports import FakeRpcRequester, FakeRpcResponder


ROBOT_API = {
    "face_look": {
        "description": "Move robot eyes to pixel offset from center",
        "params": {
            "l_eye":    {"type": "array",  "items": "integer", "required": True},
            "r_eye":    {"type": "array",  "items": "integer", "required": True},
            "duration": {"type": "number", "default": 0},
        },
        "returns": {"type": "boolean"},
    },
    "face_show_emotion": {
        "description": "Show a facial emotion",
        "params": {
            "emotion": {"type": "string", "required": True},
        },
    },
}


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

    # --- Way A: from_dict (custom IDL) ---

    def test_from_dict_defines_methods(self):
        schema = JsonRpcSchema.from_dict(ROBOT_API)
        self.assertIn("face_look", schema._methods)
        self.assertIn("face_show_emotion", schema._methods)

    def test_from_dict_no_handler_returns_not_implemented(self):
        schema = JsonRpcSchema.from_dict(ROBOT_API)
        resp = schema.dispatch({"jsonrpc": "2.0", "method": "face_look",
                                "params": {"l_eye": [0, 0], "r_eye": [0, 0]}, "id": 1})
        self.assertEqual(resp["error"]["code"], -32601)
        self.assertIn("not implemented", resp["error"]["message"])

    def test_from_dict_input_schema(self):
        schema = JsonRpcSchema.from_dict(ROBOT_API)
        entry = schema._methods["face_look"]
        props = entry["input_schema"]["properties"]
        self.assertEqual(props["l_eye"]["type"], "array")
        self.assertEqual(props["l_eye"]["items"], {"type": "integer"})
        self.assertIn("l_eye", entry["input_schema"]["required"])
        self.assertNotIn("duration", entry["input_schema"].get("required", []))

    def test_from_dict_description(self):
        schema = JsonRpcSchema.from_dict(ROBOT_API)
        self.assertIn("eyes", schema._methods["face_look"]["description"])

    # --- Way A: from_json_string / from_json_file ---

    def test_from_json_string(self):
        schema = JsonRpcSchema.from_json_string(json.dumps(ROBOT_API))
        self.assertIn("face_look", schema._methods)

    def test_from_json_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(ROBOT_API, f)
            path = f.name
        schema = JsonRpcSchema.from_json_file(path)
        self.assertIn("face_look", schema._methods)

    # --- handler() decorator ---

    def test_handler_attaches_implementation(self):
        schema = JsonRpcSchema.from_dict(ROBOT_API)

        @schema.handler("face_look")
        def handle_face_look(l_eye, r_eye, duration=0):
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

    def test_handler_updates_description(self):
        schema = JsonRpcSchema.from_dict(ROBOT_API)

        @schema.handler("face_look")
        def handle_face_look(l_eye, r_eye, duration=0):
            """Handles face look."""
            return True

        # original description preserved since from_dict already set it
        self.assertIn("eyes", schema._methods["face_look"]["description"])

    # --- Way B: register with params list ---

    def test_register_params_list(self):
        schema = JsonRpcSchema()
        schema.register("face_look", params=[("l_eye", list), ("r_eye", list), ("duration", float)])
        entry = schema._methods["face_look"]
        self.assertEqual(entry["input_schema"]["properties"]["l_eye"], {"type": "array"})
        self.assertIn("l_eye", entry["input_schema"]["required"])
        self.assertIn("r_eye", entry["input_schema"]["required"])
        self.assertIn("duration", entry["input_schema"]["required"])

    def test_register_no_func_no_handler_returns_not_implemented(self):
        schema = JsonRpcSchema()
        schema.register("face_look", params=[("l_eye", list), ("r_eye", list)])
        resp = schema.dispatch({"jsonrpc": "2.0", "method": "face_look",
                                "params": {"l_eye": [0, 0], "r_eye": [0, 0]}, "id": 1})
        self.assertEqual(resp["error"]["code"], -32601)

    # --- Way C: decorator ---

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
        self.assertIsNotNone(schema._methods["face_look"]["func"])
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
        """FakeRpcRequester with schema, pre-loaded with a canned response."""
        schema = JsonRpcSchema()
        schema.register("face_look", params=[("l_eye", list), ("r_eye", list)])
        schema.register("add", params=[("a", int), ("b", int)])
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
        self.assertEqual(client.calls[0][1], 5.0)  # timeout passed to transport

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


if __name__ == "__main__":
    unittest.main()
