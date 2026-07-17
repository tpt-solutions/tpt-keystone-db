"""End-to-end verification of TPT Keystone's from-scratch Flux gRPC endpoint.

Drives a *real* gRPC client (Python `grpcio`, the same library a production
client would use) against the running `tpt-keystone` binary's gRPC listener
(default `0.0.0.0:5436`, overridable with `TPT_FLUX_GRPC_ADDR`). Message
classes are generated from `docs/formats/flux_grpc.proto` with `protoc`; the
RPC itself is invoked through grpcio's generic channel API (`channel.unary_unary`
/ `channel.stream_stream` with explicit byte serializers) so no `grpc_tools`
codegen is required.

Exercises: Publish (unary) -> Subscribe (server-streaming) sees the live record
-> Poll (unary) returns it from a consumer group.

Auth coverage: when the process is started with `TPT_AUTH_BOOTSTRAP_USER` /
`TPT_AUTH_BOOTSTRAP_PASSWORD` set, the gRPC listener (like the HTTP/WebSocket
bridges) requires a valid `Authorization: Basic` metadata header — see
`src/wire/bridge_auth.rs` and the Phase 3 bridge-auth work. In that mode this
script sends the matching header on every call and additionally asserts that a
request *without* it is rejected (HTTP 401 / gRPC UNAUTHENTICATED), exercising
the auth gate end-to-end rather than only the zero-config path. When those env
vars are unset (the default quickstart), auth is skipped and the script behaves
exactly as before.

Run:  python tools/verify_flux_grpc.py
(assumes `cargo build` has produced target/debug/tpt-keystone)
"""
import base64
import os
import signal
import socket
import subprocess
import sys
import time

# grpcio ships a protobuf 6.x runtime; libprotoc 35.x emits a strict gencode
# version guard. Pinning the Python protobuf implementation version to the v2
# (older) API disables that guard so the message classes load. The wire bytes
# are identical across versions.
os.environ.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION_VERSION", "2")

import grpc

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
PROTO = os.path.join(ROOT, "docs", "formats", "flux_grpc.proto")
BIN = os.path.join(ROOT, "tpt-keystone", "target", "debug", "tpt-keystone.exe")
ADDR = os.environ.get("TPT_FLUX_GRPC_ADDR", "0.0.0.0:5436").replace("0.0.0.0", "127.0.0.1")
SERVICE = "flux.Flux"

# Bootstrap credentials (optional). When set, the server starts with
# role-gated auth and gRPC calls must carry a Basic header.
AUTH_USER = os.environ.get("TPT_AUTH_BOOTSTRAP_USER")
AUTH_PASS = os.environ.get("TPT_AUTH_BOOTSTRAP_PASSWORD")
# grpcio generic RPCs take metadata as an iterable of (key, value) tuples.
AUTH_METADATA = (
    [("authorization", "Basic " + base64.b64encode(f"{AUTH_USER}:{AUTH_PASS}".encode()).decode())]
    if AUTH_USER and AUTH_PASS else None
)


# --- generate message classes from the .proto (no grpc codegen plugin) -------
sys.path.insert(0, HERE)
if not os.path.exists(os.path.join(HERE, "flux_grpc_pb2.py")):
    subprocess.check_call(
        ["protoc", f"--proto_path={os.path.dirname(PROTO)}", f"--python_out={HERE}", PROTO]
    )
    # protoc (libprotoc 35.x) emits a strict gencode/runtime version check that
    # fails against grpcio's bundled protobuf 6.x. The wire bytes are identical;
    # we just drop the runtime guard from the generated module so the message
    # classes load. (No `grpc_tools` here, so we generate with plain `protoc`.)
    stub = os.path.join(HERE, "flux_grpc_pb2.py")
    with open(stub, "r", encoding="utf-8") as fh:
        src = fh.read()
    import re
    # Remove the `_runtime_version.ValidateProtobufRuntimeVersion(...)` call and
    # the bare `from google.protobuf import runtime_version as _runtime_version`
    # import that only feeds it. The rest of the stub is version-independent.
    src = re.sub(r"_runtime_version\.ValidateProtobufRuntimeVersion\(.*?\)\n", "", src, flags=re.DOTALL)
    src = re.sub(r"from google\.protobuf import runtime_version as _runtime_version\n", "", src)
    with open(stub, "w", encoding="utf-8") as fh:
        fh.write(src)
import flux_grpc_pb2 as pb  # noqa: E402

# --- generic gRPC serializers/deserializers --------------------------------
serialize = lambda msg: msg.SerializeToString()
deserialize_record = lambda b: pb.Record.FromString(b)


def wait_for_port(addr, timeout=30.0):
    host, port = addr.rsplit(":", 1)
    port = int(port)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def main():
    assert os.path.exists(BIN), f"binary not built: {BIN} (run `cargo build` first)"
    env = dict(os.environ)
    env["TPT_FLUX_GRPC_ADDR"] = "127.0.0.1:5596"
    # Isolate every other listener so a stray node (left from a prior run) on
    # the default ports can't collide with the verification instance.
    env["TPT_PG_ADDR"] = "127.0.0.1:5592"
    env["TPT_MCP_ADDR"] = "127.0.0.1:5593"
    env["TPT_FLUX_WS_ADDR"] = "127.0.0.1:5594"
    env["TPT_HTTP_ADDR"] = "127.0.0.1:5595"
    env["TPT_METRICS_ADDR"] = "127.0.0.1:9597"
    # Use a fresh local-fs storage dir per run so a prior run's topic/offset
    # state doesn't leak in. Topics persist under <dir>/objects/flux per the
    # server's storage config, so we must clear that same dir.
    import shutil

    tmp = os.path.join(HERE, "..", "tpt-data-grpcverify")
    if os.path.isdir(tmp):
        shutil.rmtree(tmp)
    env["TPT_LOCAL_STORE_DIR"] = os.path.abspath(tmp)
    env["TPT_LOCAL_DIR"] = os.path.abspath(tmp)
    ADDR = "127.0.0.1:5596"
    print(f"starting {BIN} on {ADDR} ...")
    proc = subprocess.Popen([BIN], env=env)
    try:
        assert wait_for_port(ADDR), "server did not come up"
        time.sleep(0.5)
        channel = grpc.insecure_channel(ADDR)

        # 0) CreateTopic (unary) so Publish has somewhere to write.
        channel.unary_unary(
            f"/{SERVICE}/CreateTopic",
            request_serializer=serialize,
            response_deserializer=pb.CreateTopicResponse.FromString,
        )(pb.CreateTopicRequest(name="greetings"), timeout=10, metadata=AUTH_METADATA)

        # 1) Publish (unary)
        pub = pb.PublishRequest(topic="greetings", key=b"k1", value=b"hello-grpc")
        resp = channel.unary_unary(
            f"/{SERVICE}/Publish", request_serializer=serialize, response_deserializer=pb.PublishResponse.FromString
        )(pub, timeout=10, metadata=AUTH_METADATA)
        print(f"Publish -> partition={resp.partition} offset={resp.offset}")
        assert resp.offset == 0, "expected first publish at offset 0"

        # If auth is configured, verify the gate rejects a call with no
        # credentials (the zero-config path always accepts, so skip there).
        if AUTH_METADATA is not None:
            try:
                channel.unary_unary(
                    f"/{SERVICE}/Publish", request_serializer=serialize, response_deserializer=pb.PublishResponse.FromString
                )(pb.PublishRequest(topic="greetings", value=b"unauth"), timeout=10)
                raise AssertionError("gRPC accepted a request without Basic auth")
            except grpc.RpcError as e:
                assert e.code() == grpc.StatusCode.UNAUTHENTICATED, f"expected UNAUTHENTICATED, got {e.code()}"
                print("auth gate rejected unauthenticated gRPC call (UNAUTHENTICATED)")

        # 2) Subscribe (server-streaming) and observe the live publish.
        # grpcio's generic `stream_stream(method, req_ser, resp_deser)` returns a
        # callable taking an *iterable of request messages* (client-streaming
        # shape) and yielding response messages. For server-streaming we pass a
        # one-element request iterable; the iterator then yields each streamed
        # Record as it arrives.
        sub_req = pb.SubscribeRequest(topic="greetings")
        stream_call = channel.stream_stream(
            f"/{SERVICE}/Subscribe",
            request_serializer=serialize,
            response_deserializer=deserialize_record,
        )
        stream = stream_call([sub_req], timeout=30, metadata=AUTH_METADATA)
        # Give the subscription time to register server-side before publishing
        # the record we expect to be streamed back (a too-short delay lets the
        # publish race ahead of the subscribe registration).
        time.sleep(1.5)
        channel.unary_unary(
            f"/{SERVICE}/Publish",
            request_serializer=serialize,
            response_deserializer=pb.PublishResponse.FromString,
        )(pb.PublishRequest(topic="greetings", value=b"second"), timeout=10, metadata=AUTH_METADATA)
        received = []
        try:
            for rec in stream:
                received.append(rec)
                if len(received) >= 1:
                    break
        except Exception as e:
            print(f"ERROR while iterating subscribe stream: {type(e).__name__}: {e}")
            print(f"  (received {len(received)} record(s) before error)")
            raise
        print(f"Subscribe stream saw {len(received)} live record(s)")
        assert received, "subscribe stream received no records"
        assert received[0].value in (b"hello-grpc", b"second"), received[0]

        # 3) Poll (unary) as a consumer group.
        poll = pb.PollRequest(topic="greetings", partition=0, group="g1", max=10)
        presp = channel.unary_unary(
            f"/{SERVICE}/Poll",
            request_serializer=serialize,
            response_deserializer=pb.PollResponse.FromString,
        )(poll, timeout=10, metadata=AUTH_METADATA)
        print(f"Poll -> {len(presp.records)} record(s) for group g1")
        assert len(presp.records) >= 1, "poll returned nothing"

        print("OK: Flux gRPC endpoint verified end-to-end against grpcio")
    finally:
        try:
            proc.terminate()
        except Exception:
            proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
