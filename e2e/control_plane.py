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
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from uuid import UUID


ROOT = Path(__file__).resolve().parents[1]
TIMEOUT = 10
UTC_TIMESTAMP = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$"


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

    def test_stalled_agent_goes_offline_is_evicted_and_can_recover(self):
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "40", "--lease-timeout-ms", "300",
            "--offline-retention-ms", "1000", "--sweep-interval-ms", "20",
        )
        agent, log = self.start_agent(grpc_addr, "--agent-id", "stalled-agent")
        agent_id, original_session = self.wait_for_registration(agent, log)

        agent.send_signal(signal.SIGSTOP)
        try:
            self.wait_for_agent_status(rest_addr, agent_id, "Offline")
            self.wait_for_agent_status(rest_addr, agent_id, None)
        finally:
            agent.send_signal(signal.SIGCONT)

        self.wait_for_output(
            agent, log,
            rf'^Agent "stalled-agent" registered with session (?!{original_session}$)[0-9a-f-]+$',
        )
        recovered = self.wait_for_agent_status(rest_addr, agent_id, "Online")
        self.assertEqual(recovered["pid"], agent.pid)

    def test_stalled_agent_cannot_renew_a_replacement_agents_lease(self):
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "40", "--lease-timeout-ms", "1000",
            "--offline-retention-ms", "30000", "--sweep-interval-ms", "20",
        )
        first, first_log = self.start_agent(grpc_addr, "--agent-id", "reused-agent")
        agent_id, first_session = self.wait_for_registration(first, first_log)

        first.send_signal(signal.SIGSTOP)
        try:
            self.wait_for_agent_status(rest_addr, agent_id, "Offline")
            replacement, log = self.start_agent(grpc_addr, "--agent-id", agent_id)
            _, replacement_session = self.wait_for_registration(replacement, log)
        finally:
            first.send_signal(signal.SIGCONT)

        self.assertNotEqual(replacement_session, first_session)
        self.assertNotEqual(first.wait(timeout=TIMEOUT), 0)
        self.assertIn("already online", first_log.read_text(encoding="utf-8"))
        online = self.wait_for_heartbeat(rest_addr, int(time.time()) - 1)
        self.assertEqual(online["pid"], replacement.pid)

        replacement.kill()
        replacement.wait(timeout=TIMEOUT)
        offline = self.wait_for_agent_status(rest_addr, agent_id, "Offline")
        self.assertEqual(offline["pid"], replacement.pid)

    def test_controller_shuts_down_cleanly_on_sigint_and_sigterm(self):
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(stop_signal=stop_signal.name):
                controller, _ = self.spawn_controller()
                controller.send_signal(stop_signal)
                self.assertEqual(controller.wait(timeout=TIMEOUT), 0)

    def test_cli_shows_agent_detail_and_reports_a_missing_agent(self):
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "100", "--lease-timeout-ms", "10000",
        )
        connection = http.client.HTTPConnection(rest_addr, timeout=TIMEOUT)
        self.addCleanup(connection.close)
        for agent_id in ("rack/节点?worker=%", ".", ".."):
            with self.subTest(agent_id=agent_id):
                agent, log = self.start_agent(
                    grpc_addr, "--agent-id", agent_id, "--label", "detail", "--metadata", "zone=local",
                )
                self.wait_for_registration(agent, log)
                detail = json.loads(self.cli(rest_addr, "show", agent_id, "--output", "json").stdout)
                self.assertEqual(detail["agent_id"], agent_id)
                self.assertEqual(detail["status"], "Online")
                self.assertEqual(detail["hostname"], socket.gethostname())
                self.assertEqual(detail["pid"], agent.pid)
                self.assertEqual(detail["labels"], ["detail"])
                self.assertEqual(detail["metadata"], {"zone": "local"})
                self.assertEqual(detail["lease_expires_at"],
                                 (detail["last_heartbeat_at"] or detail["registered_at"]) + 10)
                table = self.cli(rest_addr, "show", agent_id).stdout
                self.assertIn("Lease expires at (UTC)", table)
                self.assertIn('{"zone":"local"}', table)

                connection.request("GET", f"/agents/{quote(agent_id, safe='')}")
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                rest_detail = json.load(response)
                self.assertEqual(rest_detail["agent_id"], agent_id)
                self.assertRegex(rest_detail["lease_expires_at"], UTC_TIMESTAMP)

        connection.request("GET", "/agents/missing-agent")
        response = connection.getresponse()
        self.assertEqual(response.status, 404)
        self.assertIn("missing-agent", json.load(response)["error"])
        missing = self.cli(rest_addr, "show", "missing-agent", check=False)
        self.assertNotEqual(missing.returncode, 0)
        self.assertEqual(missing.stdout, "")
        self.assertIn("missing-agent", missing.stderr)
        self.assertIn("404 Not Found", missing.stderr)

    def test_cli_filters_states_and_offline_agent_can_register_again(self):
        _, (rest_addr, grpc_addr) = self.spawn_controller(
            env={
                **os.environ,
                "SYS_ANALYZER_HEARTBEAT_INTERVAL_MS": "50",
                "SYS_ANALYZER_LEASE_TIMEOUT_MS": "500",
                "SYS_ANALYZER_OFFLINE_RETENTION_MS": "30000",
                "SYS_ANALYZER_SWEEP_INTERVAL_MS": "25",
            },
        )
        healthy = self.start_agent(grpc_addr, "--agent-id", "healthy-agent")
        self.wait_for_registration(*healthy)
        first, log = self.start_agent(
            grpc_addr, "--agent-id", "returning-agent", "--metadata", "zone=old",
        )
        agent_id, first_session = self.wait_for_registration(first, log)
        first.kill()
        first.wait(timeout=TIMEOUT)
        offline = self.wait_for_agent_status(rest_addr, agent_id, "Offline")
        self.assertEqual(offline["metadata"], {"zone": "old"})

        for status, expected_ids in (
            ("online", ["healthy-agent"]), ("Offline", [agent_id]), ("disabled", []),
        ):
            with self.subTest(status=status):
                rows = json.loads(self.cli(
                    rest_addr, "list", "--status", status, "--output", "json",
                ).stdout)
                self.assertEqual([row["agent_id"] for row in rows], expected_ids)

        connection = http.client.HTTPConnection(rest_addr, timeout=TIMEOUT)
        self.addCleanup(connection.close)
        connection.request("GET", "/agents?status=Offline")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        row, = json.load(response)
        self.assertEqual(row["agent_id"], agent_id)

        replacement, log = self.start_agent(
            grpc_addr, "--agent-id", agent_id, "--label", "new", "--metadata", "zone=new",
        )
        _, replacement_session = self.wait_for_registration(replacement, log)
        self.assertNotEqual(replacement_session, first_session)
        online = self.wait_for_agent_status(rest_addr, agent_id, "Online")
        self.assertEqual(online["pid"], replacement.pid)
        self.assertEqual(online["labels"], ["new"])
        self.assertEqual(online["metadata"], {"zone": "new"})

    def test_rest_reports_expired_leases_as_offline_before_the_next_sweep(self):
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "100", "--lease-timeout-ms", "1000",
            "--offline-retention-ms", "100", "--sweep-interval-ms", "60000",
        )
        agent, log = self.start_agent(grpc_addr, "--agent-id", "expired-agent")
        agent_id, _ = self.wait_for_registration(agent, log)
        agent.kill()
        agent.wait(timeout=TIMEOUT)

        connection = http.client.HTTPConnection(rest_addr, timeout=TIMEOUT)
        self.addCleanup(connection.close)

        def get_json(path):
            connection.request("GET", path)
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            return json.load(response)

        detail_path = f"/agents/{agent_id}"
        previous_detail = get_json(detail_path)
        expiry = datetime.fromisoformat(previous_detail["lease_expires_at"]).timestamp()
        time.sleep(max(0, expiry - time.time()) + 0.15)

        self.assertEqual(get_json(detail_path), {**previous_detail, "status": "Offline"})
        row, = get_json("/agents")
        self.assertEqual(row["status"], "Offline")
        self.assertEqual(row["agent_id"], agent_id)
        self.assertEqual(get_json("/agents?status=Offline"), [row])
        self.assertEqual(get_json("/agents?status=Online"), [])

    def wait_for_agent_status(self, rest_addr, agent_id, status):
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            rows = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
            row = next((row for row in rows if row["agent_id"] == agent_id), None)
            if (row["status"] if row is not None else None) == status:
                return row
            time.sleep(0.025)
        self.fail(f"Agent {agent_id!r} never reached status {status!r}")

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
        self.assertIs(type(row["registered_at"]), int)
        connection = http.client.HTTPConnection(rest_addr, timeout=TIMEOUT)
        self.addCleanup(connection.close)
        connection.request("GET", "/agents")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        registered, = json.load(response)
        self.assertRegex(registered["registered_at"], UTC_TIMESTAMP)
        self.assertEqual(row["registered_at"],
                         int(datetime.fromisoformat(registered["registered_at"]).timestamp()))

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
            self.assertEqual(values[columns.index("Registered at (UTC)")],
                             str(row["registered_at"]))
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

    def test_agent_deregisters_before_exiting_on_a_termination_signal(self):
        rest_addr, grpc_addr = self.start_controller("--lease-timeout-ms", "60000")

        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=shutdown_signal.name):
                agent_id = f"departing-agent-{shutdown_signal.name}"
                agent, log = self.start_agent(grpc_addr, "--agent-id", agent_id)
                self.wait_for_registration(agent, log)
                agent.send_signal(shutdown_signal)
                self.assertEqual(agent.wait(timeout=TIMEOUT), 0, log.read_text())
                agents = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
                self.assertNotIn(agent_id, {entry["agent_id"] for entry in agents})

    def test_agent_retries_deregistration_if_the_connection_resets_before_delivery(self):
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "30000", "--lease-timeout-ms", "60000",
        )
        # The second RPC on the connection is Deregister.
        proxy_addr, _ = self.start_grpc_fault_proxy(grpc_addr, 3, reset=True, intercept_request=True)
        agent, log = self.start_agent(proxy_addr, "--agent-id", "departing-agent")
        agent_id, _ = self.wait_for_registration(agent, log)
        self.wait_for_agent_status(rest_addr, agent_id, "Online")

        agent.send_signal(signal.SIGTERM)

        self.assertEqual(agent.wait(timeout=TIMEOUT), 0, log.read_text())
        self.assertEqual(json.loads(self.cli(rest_addr, "list", "--output", "json").stdout), [])

    def test_agent_shutdown_is_bounded_when_deregistration_gets_no_reply(self):
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "30000", "--lease-timeout-ms", "60000",
        )
        proxy_addr, _ = self.start_grpc_fault_proxy(grpc_addr, 3, hold=True, intercept_request=True)
        agent, log = self.start_agent(proxy_addr, "--agent-id", "departing-agent")
        agent_id, _ = self.wait_for_registration(agent, log)

        agent.send_signal(signal.SIGTERM)

        self.assertEqual(agent.wait(timeout=4), 0, log.read_text())
        self.assertIn("shutdown cleanup timed out", log.read_text())
        self.assertEqual(self.wait_for_agent_status(rest_addr, agent_id, "Online")["pid"], agent.pid)

    def test_disabling_an_online_agent_stops_it_and_rejects_its_id(self):
        rest_addr, grpc_addr = self.start_controller("--heartbeat-interval-ms", "100")
        agent_id = "rack/节点?worker=%"
        agent, log = self.start_agent(grpc_addr, "--agent-id", agent_id)
        self.wait_for_registration(agent, log)

        detail = json.loads(self.cli(rest_addr, "disable", agent_id, "--output", "json").stdout)
        self.assertEqual(detail["agent_id"], agent_id)
        self.assertEqual(detail["status"], "Disabled")
        self.assertEqual(agent.wait(timeout=TIMEOUT), 0, log.read_text())
        self.assertIn("Controller requested stop", log.read_text())

        replacement, replacement_log = self.start_agent(grpc_addr, "--agent-id", agent_id)
        self.assertNotEqual(replacement.wait(timeout=TIMEOUT), 0, replacement_log.read_text())
        self.assertIn("is disabled", replacement_log.read_text())
        rows = json.loads(self.cli(rest_addr, "list", "--status", "disabled",
                                   "--output", "json").stdout)
        self.assertEqual([row["agent_id"] for row in rows], [agent_id])

    def test_agent_can_rejoin_after_enable_and_old_deregistration(self):
        for enable_before_deregister in (False, True):
            with self.subTest(enable_before_deregister=enable_before_deregister):
                rest_addr, grpc_addr = self.start_controller("--lease-timeout-ms", "60000")
                proxy_addr, release_reply = self.start_grpc_fault_proxy(grpc_addr, 1, hold=True)
                agent_id = "returning-agent"
                agent, log = self.start_agent(proxy_addr, "--agent-id", agent_id)
                self.wait_for_agent_status(rest_addr, agent_id, "Online")

                connection = http.client.HTTPConnection(rest_addr, timeout=TIMEOUT)
                self.addCleanup(connection.close)
                connection.request("POST", f"/agents/{agent_id}/disable")
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.load(response)["status"], "Disabled")

                if enable_before_deregister:
                    self.assertIn("Offline", self.cli(rest_addr, "enable", agent_id).stdout)
                agent.send_signal(signal.SIGTERM)
                release_reply.set()
                self.assertEqual(agent.wait(timeout=TIMEOUT), 0, log.read_text())
                _, original_session = self.wait_for_registration(agent, log)

                status = "Offline" if enable_before_deregister else "Disabled"
                self.assertEqual(self.wait_for_agent_status(rest_addr, agent_id, status)["pid"],
                                 agent.pid)
                if not enable_before_deregister:
                    self.assertIn("Offline", self.cli(rest_addr, "enable", agent_id).stdout)

                replacement, replacement_log = self.start_agent(grpc_addr, "--agent-id", agent_id)
                _, new_session = self.wait_for_registration(replacement, replacement_log)
                self.assertNotEqual(new_session, original_session)
                self.assertEqual(self.wait_for_agent_status(rest_addr, agent_id, "Online")["pid"],
                                 replacement.pid)

    def test_removing_a_running_agent_deletes_its_record_and_it_can_register_again(self):
        rest_addr, grpc_addr = self.start_controller("--heartbeat-interval-ms", "100")
        proxy_addr, release_reply = self.start_grpc_fault_proxy(grpc_addr, 1, hold=True)
        agent_id = "."
        agent, log = self.start_agent(proxy_addr, "--agent-id", agent_id)
        self.wait_for_agent_status(rest_addr, agent_id, "Online")

        output = self.cli(rest_addr, "remove", agent_id, "--output", "json")
        self.assertEqual(json.loads(output.stdout), {"agent_id": agent_id, "removed": True})
        self.assertEqual(json.loads(self.cli(rest_addr, "list", "--output", "json").stdout), [])

        release_reply.set()
        _, first_session = self.wait_for_registration(agent, log)
        self.wait_for_output(
            agent, log,
            rf'^Agent "{re.escape(agent_id)}" registered with session (?!{first_session}$)[0-9a-f-]+$',
        )
        self.assertEqual(self.wait_for_agent_status(rest_addr, agent_id, "Online")["pid"],
                         agent.pid)
        self.assertIn("Removed", self.cli(rest_addr, "remove", agent_id).stdout)

    def test_disabled_offline_agent_survives_sweeping_until_it_is_enabled(self):
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "40", "--lease-timeout-ms", "300",
            "--offline-retention-ms", "1000", "--sweep-interval-ms", "20",
        )
        agent_id = "retained-agent"
        agent, log = self.start_agent(grpc_addr, "--agent-id", agent_id)
        self.wait_for_registration(agent, log)
        agent.kill()
        agent.wait(timeout=TIMEOUT)
        self.wait_for_agent_status(rest_addr, agent_id, "Offline")
        self.assertIn("Disabled", self.cli(rest_addr, "disable", agent_id).stdout)

        # A later registration must pass through Offline and be evicted while
        # the disabled record remains, even under the same retention settings.
        later, later_log = self.start_agent(grpc_addr, "--agent-id", "later-agent")
        self.wait_for_registration(later, later_log)
        later.kill()
        later.wait(timeout=TIMEOUT)
        self.wait_for_agent_status(rest_addr, "later-agent", None)
        self.assertEqual(self.wait_for_agent_status(rest_addr, agent_id, "Disabled")["pid"],
                         agent.pid)

        connection = http.client.HTTPConnection(rest_addr, timeout=TIMEOUT)
        self.addCleanup(connection.close)
        connection.request("POST", f"/agents/{agent_id}/enable")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(json.load(response)["status"], "Offline")
        self.wait_for_agent_status(rest_addr, agent_id, None)

    def test_management_rest_and_cli_errors_name_the_agent_and_state(self):
        rest_addr, grpc_addr = self.start_controller("--heartbeat-interval-ms", "100")
        agent_id = "managed-agent"
        agent, log = self.start_agent(grpc_addr, "--agent-id", agent_id)
        self.wait_for_registration(agent, log)
        connection = http.client.HTTPConnection(rest_addr, timeout=TIMEOUT)
        self.addCleanup(connection.close)

        def assert_error(command, status, reason):
            method, path = ("DELETE", f"/agents/{agent_id}") if command == "remove" else (
                "POST", f"/agents/{agent_id}/{command}",
            )
            connection.request(method, path)
            response = connection.getresponse()
            self.assertEqual(response.status, status)
            self.assertEqual(json.load(response), {"error": reason})
            output = self.cli(rest_addr, command, agent_id, "--output", "json", check=False)
            self.assertNotEqual(output.returncode, 0)
            self.assertEqual(output.stdout, "")
            self.assertIn(f"HTTP {status}", output.stderr)
            self.assertIn(reason, output.stderr)

        assert_error("enable", 409, f'Agent "{agent_id}" is not disabled')
        self.cli(rest_addr, "disable", agent_id)
        self.assertEqual(agent.wait(timeout=TIMEOUT), 0, log.read_text())
        assert_error("disable", 409, f'Agent "{agent_id}" is already disabled')

        connection.request("DELETE", f"/agents/{agent_id}")
        response = connection.getresponse()
        self.assertEqual(response.status, 204)
        self.assertEqual(response.read(), b"")
        for command in ("disable", "enable", "remove"):
            with self.subTest(command=command):
                assert_error(command, 404, f'Agent "{agent_id}" was not found')

    def test_agent_shutdown_deregisters_even_when_the_registration_reply_is_lost(self):
        rest_addr, grpc_addr = self.start_controller("--lease-timeout-ms", "60000")
        proxy_addr, release_reply = self.start_grpc_fault_proxy(
            grpc_addr, 1, reset=True, hold=True,
        )
        agent, log = self.start_agent(proxy_addr, "--agent-id", "departing-agent")

        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            agents = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
            if agents:
                self.assertEqual(agents[0]["agent_id"], "departing-agent")
                break
            time.sleep(0.025)
        else:
            self.fail("Controller did not register the Agent while the reply was held")

        agent.send_signal(signal.SIGTERM)
        release_reply.set()

        self.assertEqual(agent.wait(timeout=TIMEOUT), 0, log.read_text())
        self.assertEqual(json.loads(self.cli(rest_addr, "list", "--output", "json").stdout), [])

    def test_agent_retries_after_tcp_reset_during_registration_or_heartbeat(self):
        for stream_id, call in ((1, "Registration"), (3, "Heartbeat")):
            with self.subTest(call=call):
                rest_addr, grpc_addr = self.start_controller("--heartbeat-interval-ms", "100")
                proxy_addr, _ = self.start_grpc_fault_proxy(grpc_addr, stream_id, reset=True)
                agent, log = self.start_agent(proxy_addr, "--agent-id", "reset-agent")
                self.wait_for_output(agent, log, rf'^{call} for Agent "reset-agent" .*will retry')
                resumed_at = int(time.time())
                self.wait_for_registration(agent, log)
                row = self.wait_for_heartbeat(rest_addr, resumed_at)
                self.assertEqual(row["pid"], agent.pid)
                self.assertEqual(row["status"], "Online")

    def test_agent_replays_registration_after_its_committed_reply_times_out(self):
        rest_addr, grpc_addr = self.start_controller("--heartbeat-interval-ms", "100")
        proxy_addr, release_reply = self.start_grpc_fault_proxy(grpc_addr, 1)
        agent, log = self.start_agent(proxy_addr, "--agent-id", "replay-agent")
        try:
            self.wait_for_output(agent, log, r'^Registration for Agent "replay-agent" .*will retry')
            original, = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
            self.assertEqual(original["status"], "Online")
            self.assertIsNone(original["last_heartbeat_at"])
        finally:
            release_reply.set()

        resumed_at = int(time.time())
        self.wait_for_registration(agent, log)
        row = self.wait_for_heartbeat(rest_addr, resumed_at)
        self.assertEqual(row["registered_at"], original["registered_at"])
        self.assertEqual(row["pid"], agent.pid)

    def test_concurrent_agents_keep_reporting_heartbeats_to_the_cli(self):
        lease_timeout_seconds = 1
        rest_addr, grpc_addr = self.start_controller(
            "--heartbeat-interval-ms", "100", "--lease-timeout-ms", str(lease_timeout_seconds * 1000),
        )
        processes = [self.start_agent(grpc_addr, "--agent-id", agent_id)
                     for agent_id in ("first", "second")]
        agent_ids = {self.wait_for_registration(*process)[0] for process in processes}

        initial_rows = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
        registrations = {row["agent_id"]: row["registered_at"] for row in initial_rows}
        self.assertEqual(registrations.keys(), agent_ids)
        last_seen = registrations.copy()
        initial_expiries = {agent_id: registered_at + lease_timeout_seconds
                            for agent_id, registered_at in registrations.items()}
        rounds = dict.fromkeys(agent_ids, 0)
        first_table = None
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            rows = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
            self.assertEqual({row["agent_id"]: row["registered_at"] for row in rows}, registrations)
            for row in rows:
                agent_id = row["agent_id"]
                self.assertEqual(row["status"], "Online")
                heartbeat = row["last_heartbeat_at"]
                if heartbeat is not None:
                    self.assertIs(type(heartbeat), int)
                    self.assertGreaterEqual(heartbeat, last_seen[agent_id])
                    if heartbeat > last_seen[agent_id]:
                        last_seen[agent_id] = heartbeat
                        rounds[agent_id] += 1
            if first_table is None and min(rounds.values()) >= 1:
                first_table = self.table_heartbeats(rest_addr)
                self.assertEqual(first_table.keys(), agent_ids)
            if first_table is not None and all(
                rounds[agent_id] >= 3
                and last_seen[agent_id] > initial_expiries[agent_id]
                and last_seen[agent_id] > first_table[agent_id]
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

        row = self.wait_for_heartbeat(rest_addr, int(time.time()))
        self.assertEqual(row["status"], "Online")
        self.assertEqual(row["registered_at"], initial["registered_at"])

    def test_agent_reregisters_after_the_controller_restarts(self):
        controller, (rest_addr, grpc_addr) = self.spawn_controller("--heartbeat-interval-ms", "100")
        agent, log = self.start_agent(
            grpc_addr, "--agent-id", "restart-agent", "--label", "restart", "--metadata", "zone=local",
        )
        agent_id, original_session = self.wait_for_registration(agent, log)
        original = self.wait_for_heartbeat(rest_addr, int(time.time()))

        self.stop_process(controller)
        self.wait_for_output(agent, log, r'^Heartbeat for Agent "restart-agent" failed and will retry')
        restarted_at = int(time.time())
        _, (rest_addr, _) = self.spawn_controller("--heartbeat-interval-ms", "50", grpc_addr=grpc_addr)

        self.wait_for_output(
            agent, log,
            rf'^Agent "restart-agent" registered with session (?!{original_session}$)[0-9a-f-]+$',
        )
        row = self.wait_for_heartbeat(rest_addr, restarted_at)
        self.assertIsNone(agent.poll())
        self.assertEqual(row["agent_id"], agent_id)
        self.assertEqual(row["pid"], agent.pid)
        self.assertEqual(row["status"], "Online")
        self.assertEqual(row["labels"], original["labels"])
        self.assertEqual(row["metadata"], original["metadata"])
        self.assertGreater(row["registered_at"], original["registered_at"])

    def wait_for_heartbeat(self, rest_addr, after):
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            row, = json.loads(self.cli(rest_addr, "list", "--output", "json").stdout)
            heartbeat = row["last_heartbeat_at"]
            if heartbeat is not None and heartbeat > after:
                return row
            time.sleep(0.025)
        self.fail("Agent did not report a new heartbeat")

    def start_grpc_fault_proxy(self, grpc_addr, stream_id, *, reset=False, hold=False,
                               intercept_request=False):
        upstream_host, upstream_port = grpc_addr.split(":")
        upstream_addr = (upstream_host, int(upstream_port))
        fault_injected = threading.Event()
        release_frame = threading.Event()

        class Relay(socketserver.BaseRequestHandler):
            def handle(self):
                downstream = self.request
                try:
                    with socket.create_connection(upstream_addr, timeout=TIMEOUT) as upstream:
                        self.relay(downstream, upstream)
                except (ConnectionError, TimeoutError):
                    pass

            def relay(self, downstream, upstream):
                fault_source = downstream if intercept_request else upstream
                # The client sends 24 bytes before the first HTTP/2 frame.
                preface_length = 24 if intercept_request else 0
                pending = bytearray()
                while readable := select.select([downstream, upstream], [], [], TIMEOUT)[0]:
                    for source in readable:
                        target = upstream if source is downstream else downstream
                        data = source.recv(65536)
                        if not data:
                            return
                        if source is not fault_source:
                            target.sendall(data)
                            continue
                        pending.extend(data)
                        if preface_length:
                            if len(pending) < preface_length:
                                continue
                            target.sendall(pending[:preface_length])
                            del pending[:preface_length]
                            preface_length = 0
                        # The HTTP/2 frame header gives the length and stream ID.
                        while len(pending) >= 9:
                            frame_end = 9 + int.from_bytes(pending[0:3], "big")
                            if len(pending) < frame_end:
                                break
                            frame_stream = int.from_bytes(pending[5:9], "big") & 0x7fffffff
                            if pending[3] == 0x01 and frame_stream == stream_id and not fault_injected.is_set():
                                fault_injected.set()
                                if (not reset or hold) and not release_frame.wait(TIMEOUT):
                                    return
                                if reset:
                                    downstream.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                                    downstream.close()
                                    return
                            target.sendall(pending[:frame_end])
                            del pending[:frame_end]

        proxy = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Relay)
        proxy.daemon_threads = True
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        self.addCleanup(proxy.server_close)
        self.addCleanup(proxy.shutdown)
        self.addCleanup(release_frame.set)
        host, port = proxy.server_address
        return f"{host}:{port}", release_frame

    def table_heartbeats(self, rest_addr):
        header, *table_rows = self.cli(rest_addr, "list").stdout.splitlines()
        columns = re.split(r" {2,}", header)
        heartbeat_column = columns.index("Last heartbeat (UTC)")
        heartbeats = {}
        for table_row in table_rows:
            values = re.split(r" {2,}", table_row)
            heartbeats[values[0]] = int(values[heartbeat_column])
        return heartbeats

    def start_controller(self, *args):
        _, addresses = self.spawn_controller(*args)
        return addresses

    def spawn_controller(self, *args, grpc_addr="127.0.0.1:0", env=None):
        process, log = self.spawn(
            "sys-analyzer-controller", "--rest-addr", "127.0.0.1:0", "--grpc-addr", grpc_addr,
            *args, env=env,
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
