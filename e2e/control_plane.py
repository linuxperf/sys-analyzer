#!/usr/bin/env python3

import http.client
import json
import os
import re
import select
import signal
import socket
import socketserver
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from datetime import datetime, time as clock_time, timedelta, timezone
from pathlib import Path
from uuid import UUID


ROOT = Path(__file__).resolve().parents[1]
TIMEOUT = 10
UTC_SECONDS = r"^\d{2}:\d{2}:\d{2}$"


def parse_utc_clock(value):
    now = datetime.now(timezone.utc)
    timestamp = datetime.combine(now.date(), clock_time.fromisoformat(value), timezone.utc)
    # Test observations are recent and may cross midnight UTC.
    return timestamp - timedelta(days=1) if timestamp > now else timestamp


class ControlPlaneTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        build = subprocess.run(
            ["cargo", "build", "--locked", "--workspace", "--bins",
             "--message-format=json-render-diagnostics"],
            cwd=ROOT, stdout=subprocess.PIPE, check=True,
        )
        cls.binaries = {}
        for line in build.stdout.splitlines():
            message = json.loads(line)
            if message["reason"] == "compiler-artifact" and message["executable"]:
                cls.binaries[message["target"]["name"]] = message["executable"]

    def test_healthy_controller_serves_an_empty_list_over_http_1_1(self):
        rest_addr, _ = self.start_controller()
        connection = http.client.HTTPConnection(rest_addr, timeout=TIMEOUT)
        self.addCleanup(connection.close)
        connection.request("GET", "/health")
        response = connection.getresponse()
        self.assertEqual(response.version, 11)
        self.assertEqual(response.status, 200)
        self.assertEqual(json.load(response), {"status": "ok"})
        self.assertEqual(self.cli(rest_addr, "list", "--output", "json").stdout, "[]\n")

    def test_registers_an_agent_and_exposes_its_metadata(self):
        rest_addr, grpc_addr = self.start_controller()
        agent, log = self.start_agent(
            grpc_addr, "--agent-id", "节点甲乙丙", "--label", "test", "--metadata", "zone=local",
        )
        agent_id, _ = self.wait_for_registration(agent, log)
        self.assertEqual(agent_id, "节点甲乙丙")

        row, = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
        version_output = subprocess.check_output(
            [self.binaries["sys-analyzer-agent"], "--version"], encoding="utf-8", timeout=TIMEOUT,
        )
        self.assertEqual(row["agent_id"], agent_id)
        self.assertEqual(row["hostname"], socket.gethostname())
        self.assertEqual(row["pid"], agent.pid)
        self.assertEqual(row["version"], version_output.split()[1])
        self.assertEqual(row["labels"], ["test"])
        self.assertEqual(row["metadata"], {"zone": "local"})
        self.assertEqual(row["status"], "Online")
        self.assertRegex(row["registered_at"], UTC_SECONDS)

        _, table_row = self.cli(rest_addr, "list").stdout.splitlines()
        # Five wide characters occupy ten columns, followed by the column gap.
        self.assertTrue(table_row.startswith(f"{agent_id}  {row['hostname']}"), table_row)
        self.assertTrue(table_row.endswith('["test"]  {"zone":"local"}'), table_row)

    def test_concurrent_agents_get_distinct_ids_and_an_online_id_cannot_be_replaced(self):
        rest_addr, grpc_addr = self.start_controller()
        first = self.start_agent(grpc_addr, "--label", "first")
        second = self.start_agent(grpc_addr, "--label", "second")
        first_id, first_session = self.wait_for_registration(*first)
        second_id, second_session = self.wait_for_registration(*second)
        self.assertEqual(UUID(first_id).version, 4)
        self.assertEqual(UUID(second_id).version, 4)
        self.assertNotEqual(first_id, second_id)
        self.assertNotEqual(first_session, second_session)

        duplicate = subprocess.run(
            [self.binaries["sys-analyzer-agent"], "--controller", f"http://{grpc_addr}",
             "--agent-id", first_id, "--label", "replacement"],
            capture_output=True, encoding="utf-8", timeout=TIMEOUT,
        )
        self.assertNotEqual(duplicate.returncode, 0)
        self.assertIn("already online", duplicate.stderr)

        rows = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["agent_id"]: row["labels"] for row in rows},
                         {first_id: ["first"], second_id: ["second"]})

        output = self.cli("127.0.0.1:0", "--controller", f"http://{rest_addr}", "list")
        header, *table_rows = output.stdout.splitlines()
        columns = re.split(r" {2,}", header)
        self.assertEqual(columns, [
            "Agent ID", "Hostname", "PID", "Version", "Status", "Registered at (UTC)",
            "Last heartbeat (UTC)", "Labels", "Metadata",
        ])
        self.assertEqual(len(table_rows), len(rows))
        for row, table_row in zip(rows, table_rows):
            self.assertTrue(table_row.startswith(row["agent_id"]), table_row)
            self.assertIn(row["hostname"], table_row)
            self.assertIn("Online", table_row)
            values = re.split(r" {2,}", table_row)
            self.assertEqual(values[columns.index("Registered at (UTC)")], row["registered_at"])
            self.assertIn(json.dumps(row["labels"]), table_row)

    def test_unreachable_controller_exits_with_an_error(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            host, port = listener.getsockname()
            rest_addr = f"{host}:{port}"
            output = self.cli(rest_addr, "list", check=False)
        self.assertNotEqual(output.returncode, 0)
        self.assertEqual(output.stdout, "")
        self.assertIn(f"query Controller at http://{rest_addr}", output.stderr)
        self.assertIn("HTTP request failed", output.stderr)

    def test_agent_retries_when_started_before_the_controller(self):
        with socket.socket() as reserved_port:
            reserved_port.bind(("127.0.0.1", 0))
            host, port = reserved_port.getsockname()
            grpc_addr = f"{host}:{port}"
            agent, log = self.start_agent(grpc_addr, "--agent-id", "early-agent")
            self.wait_for_output(agent, log, r'^Registration for Agent "early-agent" .* will retry')
            retry_started_at = time.monotonic()

        _, (rest_addr, _) = self.spawn_controller(grpc_addr=grpc_addr)
        agent_id, _ = self.wait_for_registration(agent, log)
        self.assertGreaterEqual(time.monotonic() - retry_started_at, 2.5)
        row, = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
        self.assertEqual(row["agent_id"], agent_id)
        self.assertEqual(row["pid"], agent.pid)
        self.assertEqual(row["status"], "Online")

    def test_agent_retries_after_tcp_reset_during_registration_or_heartbeat(self):
        for stream_id, call in ((1, "Registration"), (3, "Heartbeat")):
            with self.subTest(call=call):
                rest_addr, grpc_addr = self.start_controller("--heartbeat-interval-ms", "100")
                proxy_addr, _ = self.start_grpc_reply_proxy(grpc_addr, stream_id, reset=True)
                agent, log = self.start_agent(proxy_addr, "--agent-id", "reset-agent")
                self.wait_for_output(agent, log, rf'^{call} for Agent "reset-agent" .*will retry')
                resumed_at = datetime.now(timezone.utc)
                self.wait_for_registration(agent, log)
                row = self.wait_for_heartbeat(rest_addr, resumed_at)
                self.assertEqual(row["pid"], agent.pid)
                self.assertEqual(row["status"], "Online")

    def test_agent_replays_registration_after_its_committed_reply_times_out(self):
        rest_addr, grpc_addr = self.start_controller("--heartbeat-interval-ms", "100")
        proxy_addr, release_reply = self.start_grpc_reply_proxy(grpc_addr, 1)
        agent, log = self.start_agent(proxy_addr, "--agent-id", "replay-agent")
        try:
            self.wait_for_output(agent, log, r'^Registration for Agent "replay-agent" .*will retry')
            original, = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
            self.assertEqual(original["status"], "Online")
            self.assertIsNone(original["last_heartbeat_at"])
        finally:
            release_reply.set()

        resumed_at = datetime.now(timezone.utc)
        self.wait_for_registration(agent, log)
        row = self.wait_for_heartbeat(rest_addr, resumed_at)
        self.assertEqual(row["registered_at"], original["registered_at"])
        self.assertEqual(row["pid"], agent.pid)

    def test_concurrent_agents_keep_reporting_heartbeats_to_the_cli(self):
        lease_timeout_ms = 1000
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "100", "--lease-timeout-ms", str(lease_timeout_ms),
        )
        processes = [self.start_agent(grpc_addr, "--agent-id", agent_id)
                     for agent_id in ("first", "second")]
        agent_ids = {self.wait_for_registration(*process)[0] for process in processes}

        initial_rows = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
        registrations = {row["agent_id"]: row["registered_at"] for row in initial_rows}
        self.assertEqual(registrations.keys(), agent_ids)
        last_seen = {agent_id: parse_utc_clock(timestamp)
                     for agent_id, timestamp in registrations.items()}
        initial_expiries = {agent_id: registered_at + timedelta(milliseconds=lease_timeout_ms)
                            for agent_id, registered_at in last_seen.items()}
        rounds = dict.fromkeys(agent_ids, 0)
        first_table = None
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            rows = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
            self.assertEqual({row["agent_id"]: row["registered_at"] for row in rows}, registrations)
            for row in rows:
                agent_id = row["agent_id"]
                self.assertEqual(row["status"], "Online")
                if row["last_heartbeat_at"] is not None:
                    self.assertRegex(row["last_heartbeat_at"], UTC_SECONDS)
                    heartbeat = parse_utc_clock(row["last_heartbeat_at"])
                    self.assertGreaterEqual(heartbeat, last_seen[agent_id])
                    if heartbeat > last_seen[agent_id]:
                        last_seen[agent_id] = heartbeat
                        rounds[agent_id] += 1
            if first_table is None and min(rounds.values()) >= 1:
                first_table = self.table_heartbeats(rest_addr)
                self.assertEqual(first_table.keys(), agent_ids)
            if first_table is not None and all(
                rounds[agent_id] >= 3
                and last_seen[agent_id] > max(initial_expiries[agent_id], first_table[agent_id])
                for agent_id in agent_ids
            ):
                break
            time.sleep(0.025)
        self.assertGreaterEqual(min(rounds.values()), 3, rounds)
        self.assertIsNotNone(first_table)
        final_table = self.table_heartbeats(rest_addr)
        self.assertEqual(final_table.keys(), agent_ids)
        for agent_id in agent_ids:
            self.assertGreater(last_seen[agent_id], initial_expiries[agent_id], agent_id)
            self.assertGreater(final_table[agent_id], first_table[agent_id], agent_id)
            self.assertGreaterEqual(final_table[agent_id], last_seen[agent_id], agent_id)

    def test_agent_resumes_heartbeats_after_a_timed_out_call(self):
        controller, (rest_addr, grpc_addr) = self.spawn_controller(
            "--heartbeat-interval-ms", "100", "--lease-timeout-ms", "15000",
        )
        agent, log = self.start_agent(grpc_addr, "--agent-id", "resilient")
        self.wait_for_registration(agent, log)
        initial, = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)

        controller.send_signal(signal.SIGSTOP)
        try:
            self.wait_for_output(agent, log, r'^Heartbeat for Agent "resilient" failed and will retry')
        finally:
            controller.send_signal(signal.SIGCONT)

        row = self.wait_for_heartbeat(rest_addr, datetime.now(timezone.utc))
        self.assertEqual(row["status"], "Online")
        self.assertEqual(row["registered_at"], initial["registered_at"])

    def wait_for_heartbeat(self, rest_addr, after):
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            row, = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
            heartbeat = row["last_heartbeat_at"]
            if heartbeat is not None and parse_utc_clock(heartbeat) > after:
                return row
            time.sleep(0.025)
        self.fail("Agent did not report a new heartbeat")

    def start_grpc_reply_proxy(self, grpc_addr, stream_id, *, reset=False):
        upstream_host, upstream_port = grpc_addr.split(":")
        upstream_addr = (upstream_host, int(upstream_port))
        fault_injected = threading.Event()
        release_reply = threading.Event()

        class Relay(socketserver.BaseRequestHandler):
            def handle(self):
                downstream = self.request
                try:
                    with socket.create_connection(upstream_addr, timeout=TIMEOUT) as upstream:
                        self.relay(downstream, upstream)
                except (ConnectionError, TimeoutError):
                    pass

            def relay(self, downstream, upstream):
                pending = bytearray()
                while readable := select.select([downstream, upstream], [], [], TIMEOUT)[0]:
                    if downstream in readable:
                        data = downstream.recv(65536)
                        if not data:
                            return
                        upstream.sendall(data)
                    if upstream not in readable:
                        continue
                    data = upstream.recv(65536)
                    if not data:
                        return
                    pending.extend(data)
                    # The HTTP/2 frame header gives the length and stream ID.
                    while len(pending) >= 9:
                        frame_end = 9 + int.from_bytes(pending[0:3], "big")
                        if len(pending) < frame_end:
                            break
                        reply_stream = int.from_bytes(pending[5:9], "big") & 0x7fffffff
                        if pending[3] == 0x01 and reply_stream == stream_id and not fault_injected.is_set():
                            fault_injected.set()
                            if reset:
                                downstream.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                                downstream.close()
                                return
                            if not release_reply.wait(TIMEOUT):
                                return
                        downstream.sendall(pending[:frame_end])
                        del pending[:frame_end]

        proxy = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Relay)
        proxy.daemon_threads = True
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        self.addCleanup(proxy.server_close)
        self.addCleanup(proxy.shutdown)
        self.addCleanup(release_reply.set)
        host, port = proxy.server_address
        return f"{host}:{port}", release_reply

    def table_heartbeats(self, rest_addr):
        header, *table_rows = self.cli(rest_addr, "list").stdout.splitlines()
        columns = re.split(r" {2,}", header)
        heartbeat_column = columns.index("Last heartbeat (UTC)")
        heartbeats = {}
        for table_row in table_rows:
            values = re.split(r" {2,}", table_row)
            self.assertRegex(values[heartbeat_column], UTC_SECONDS)
            heartbeats[values[0]] = parse_utc_clock(values[heartbeat_column])
        return heartbeats

    def start_controller(self, *args):
        _, addresses = self.spawn_controller(*args)
        return addresses

    def spawn_controller(self, *args, grpc_addr="127.0.0.1:0"):
        process, log = self.spawn(
            "sys-analyzer-controller", "--rest-addr", "127.0.0.1:0", "--grpc-addr", grpc_addr,
            *args,
        )
        addresses = self.wait_for_output(
            process, log, r"^Controller listening on REST (127\.0\.0\.1:\d+) and gRPC (127\.0\.0\.1:\d+)$",
        ).groups()
        return process, addresses

    def start_agent(self, grpc_addr, *args):
        env = os.environ.copy()
        env.pop("SYS_ANALYZER_AGENT_ID", None)
        return self.spawn("sys-analyzer-agent", "--controller", f"http://{grpc_addr}", *args, env=env)

    def wait_for_registration(self, process, log):
        agent_id, session = self.wait_for_output(
            process, log, r'^Agent "([^"]+)" registered with session ([0-9a-f-]+)$',
        ).groups()
        return agent_id, UUID(session)

    def spawn(self, binary, *args, env=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        log = Path(directory.name) / "process.log"
        with log.open("wb") as output:
            process = subprocess.Popen(
                [self.binaries[binary], *args], stdout=output, stderr=subprocess.STDOUT, env=env,
            )
        self.addCleanup(self.stop_process, process)
        return process, log

    def wait_for_output(self, process, log, pattern):
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            output = log.read_text(encoding="utf-8", errors="replace")
            match = re.search(pattern, output, re.MULTILINE)
            if match:
                return match
            if process.poll() is not None:
                break
            time.sleep(0.05)
        self.fail(f"Expected {pattern!r} from {process.args!r} (exit {process.poll()}):\n"
                  f"{log.read_text(encoding='utf-8', errors='replace')}")

    @staticmethod
    def stop_process(process):
        process.terminate()
        try:
            process.wait(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    def cli(self, rest_addr, *args, check=True):
        output = subprocess.run(
            [self.binaries["sys-analyzer-cli"], *args],
            env={**os.environ, "SYS_ANALYZER_REST_URL": f"http://{rest_addr}"},
            capture_output=True, encoding="utf-8", timeout=TIMEOUT,
        )
        if check:
            self.assertEqual(output.returncode, 0, output.stderr)
        return output


if __name__ == "__main__":
    unittest.main()
