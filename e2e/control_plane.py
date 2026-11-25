#!/usr/bin/env python3

import http.client
import json
import os
import re
import socket
import subprocess
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from uuid import UUID


ROOT = Path(__file__).resolve().parents[1]
TIMEOUT = 10


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
        self.assertGreater(datetime.fromisoformat(row["registered_at"].replace("Z", "+00:00")).timestamp(), 0)

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
        self.assertEqual(re.split(r" {2,}", header), [
            "Agent ID", "Hostname", "PID", "Version", "Status", "Registered at (UTC)",
            "Labels", "Metadata",
        ])
        self.assertEqual(len(table_rows), len(rows))
        for row, table_row in zip(rows, table_rows):
            self.assertTrue(table_row.startswith(row["agent_id"]), table_row)
            self.assertIn(row["hostname"], table_row)
            self.assertIn("Online", table_row)
            self.assertIn(row["registered_at"].replace("Z", "+00:00"), table_row)
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

    def start_controller(self):
        process, log = self.spawn(
            "sys-analyzer-controller", "--rest-addr", "127.0.0.1:0", "--grpc-addr", "127.0.0.1:0",
        )
        return self.wait_for_output(
            process, log, r"^Controller listening on REST (127\.0\.0\.1:\d+) and gRPC (127\.0\.0\.1:\d+)$",
        ).groups()

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
