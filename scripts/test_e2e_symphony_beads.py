from __future__ import annotations

import importlib.util
import pathlib
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("e2e_symphony_beads.py")
e2e_symphony_beads = None

if MODULE_PATH.exists():
    spec = importlib.util.spec_from_file_location("e2e_symphony_beads", MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    e2e_symphony_beads = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(e2e_symphony_beads)


class SymphonyHarnessTest(unittest.TestCase):
    def test_script_exists(self) -> None:
        self.assertTrue(MODULE_PATH.exists(), f"expected harness at {MODULE_PATH}")

    def test_render_workflow_uses_beads_tracker_and_never_approval(self) -> None:
        self.assertIsNotNone(e2e_symphony_beads)
        workflow = e2e_symphony_beads.render_workflow(
            repo_path="/tmp/repo",
            rpc_endpoint="http://127.0.0.1:7777/rpc",
            workspace_root="/tmp/workspaces",
        )

        self.assertIn("tracker:", workflow)
        self.assertIn('  kind: "beads"', workflow)
        self.assertIn('  rpc_endpoint: "http://127.0.0.1:7777/rpc"', workflow)
        self.assertIn('  repo_path: "/tmp/repo"', workflow)
        self.assertIn('  root: "/tmp/workspaces"', workflow)
        self.assertIn("codex:", workflow)
        self.assertIn('  approval_policy: "never"', workflow)
        self.assertIn("  turn_sandbox_policy:", workflow)
        self.assertIn('    "networkAccess": true', workflow)
        self.assertIn("observability:", workflow)
        self.assertIn("  dashboard_enabled: false", workflow)

    def test_render_workflow_can_preserve_before_remove_hook(self) -> None:
        self.assertIsNotNone(e2e_symphony_beads)
        workflow = e2e_symphony_beads.render_workflow(
            repo_path="/tmp/repo",
            rpc_endpoint="http://127.0.0.1:7777/rpc",
            workspace_root="/tmp/workspaces",
            before_remove_hook="cp LIFECYCLE_PROOF.txt /tmp/proof.txt",
        )

        self.assertIn("hooks:", workflow)
        self.assertIn('  before_remove: "cp LIFECYCLE_PROOF.txt /tmp/proof.txt"', workflow)

    def test_lifecycle_prompt_uses_tracker_tools(self) -> None:
        self.assertIsNotNone(e2e_symphony_beads)
        prompt = e2e_symphony_beads.lifecycle_prompt(
            endpoint="http://127.0.0.1:7777/rpc",
            repo_path="/tmp/repo",
        )

        self.assertIn("tracker_create_issue", prompt)
        self.assertIn("tracker_add_blocker", prompt)
        self.assertIn("tracker_create_comment", prompt)
        self.assertIn("tracker_update_issue_state", prompt)
        self.assertNotIn("python3 - <<'PY'", prompt)

    def test_collect_observed_tool_fragments_reads_recent_events(self) -> None:
        self.assertIsNotNone(e2e_symphony_beads)
        observed = e2e_symphony_beads.collect_observed_tool_fragments(
            {
                "running": [
                    {
                        "last_message": "turn completed",
                        "recent_events": [
                            {
                                "message": "dynamic tool call requested (tracker_create_comment)"
                            },
                            {
                                "message": "dynamic tool call completed (tracker_update_issue_state)"
                            },
                        ],
                    }
                ]
            },
            {"tracker_create_comment", "tracker_update_issue_state"},
        )

        self.assertEqual(
            observed,
            {"tracker_create_comment", "tracker_update_issue_state"},
        )

    def test_collect_observed_tool_fragments_reads_recently_completed(self) -> None:
        self.assertIsNotNone(e2e_symphony_beads)
        observed = e2e_symphony_beads.collect_observed_tool_fragments(
            {
                "running": [],
                "recently_completed": [
                    {
                        "last_message": "turn completed",
                        "recent_events": [
                            {
                                "message": "dynamic tool call completed (tracker_update_issue_state)"
                            }
                        ],
                    }
                ],
            },
            {"tracker_update_issue_state"},
        )

        self.assertEqual(observed, {"tracker_update_issue_state"})


if __name__ == "__main__":
    unittest.main()
