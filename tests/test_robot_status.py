import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend import main
from backend.auto_trader import AutoTrader, RobotState
from backend.status import (
    STATUS_ANALYZING,
    STATUS_ERROR,
    STATUS_LOSS,
    STATUS_OPERATION_OPEN,
    STATUS_PENDING_RESULT,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STATUS_WAITING_ENTRY,
    STATUS_WAITING_NEXT_CANDLE_ENTRY,
    STATUS_WAITING_RESULT,
    STATUS_WIN,
    VALID_ROBOT_STATUSES,
    normalize_robot_status,
)


class RobotStatusContractTests(unittest.TestCase):
    def test_required_frontend_statuses_are_valid(self) -> None:
        for status in (
            STATUS_STOPPED,
            STATUS_RUNNING,
            STATUS_ANALYZING,
            STATUS_WAITING_ENTRY,
            STATUS_OPERATION_OPEN,
            STATUS_WAITING_RESULT,
            STATUS_WIN,
            STATUS_LOSS,
            STATUS_ERROR,
        ):
            with self.subTest(status=status):
                self.assertIn(status, VALID_ROBOT_STATUSES)

    def test_legacy_statuses_normalize_to_current_backend_states(self) -> None:
        self.assertEqual(normalize_robot_status("WAITING_ENTRY"), STATUS_WAITING_NEXT_CANDLE_ENTRY)
        self.assertEqual(normalize_robot_status("WAITING_ENTRY_WINDOW"), STATUS_WAITING_NEXT_CANDLE_ENTRY)
        self.assertEqual(normalize_robot_status("OPERATION_OPEN"), STATUS_PENDING_RESULT)
        self.assertEqual(normalize_robot_status("WAITING_RESULT"), STATUS_PENDING_RESULT)
        self.assertEqual(normalize_robot_status("UNKNOWN_STATUS"), STATUS_STOPPED)

    def test_build_robot_payload_always_returns_valid_status(self) -> None:
        cases = (
            (STATUS_STOPPED, STATUS_STOPPED),
            (STATUS_RUNNING, STATUS_RUNNING),
            (STATUS_ANALYZING, STATUS_ANALYZING),
            (STATUS_WAITING_ENTRY, STATUS_WAITING_NEXT_CANDLE_ENTRY),
            (STATUS_OPERATION_OPEN, STATUS_PENDING_RESULT),
            (STATUS_WAITING_RESULT, STATUS_PENDING_RESULT),
            (STATUS_WIN, STATUS_WIN),
            (STATUS_LOSS, STATUS_LOSS),
            (STATUS_ERROR, STATUS_ERROR),
            ("MISSING_CONSTANT", STATUS_STOPPED),
        )

        for raw_status, expected_status in cases:
            with self.subTest(status=raw_status):
                state = RobotState(status=raw_status)
                payload = main.build_robot_payload(state)
                self.assertEqual(payload["data"]["status"], expected_status)
                self.assertIn(payload["data"]["status"], VALID_ROBOT_STATUSES)


class RobotStateEndpointStatusFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.old_api_key = main.config.panel_api_key
        self.old_trader = main.auto_trader
        main.config.panel_api_key = "test-key"
        main.auto_trader = AutoTrader()
        self.client = TestClient(main.app)

    def tearDown(self) -> None:
        main.config.panel_api_key = self.old_api_key
        main.auto_trader = self.old_trader

    def test_robot_state_never_500s_when_state_impl_raises(self) -> None:
        user_id = "status-fallback-user"
        main.auto_trader.get(user_id).status = "MISSING_CONSTANT"

        with patch.object(
            main,
            "recover_sync_timeout_if_needed",
            side_effect=RuntimeError("unexpected state failure"),
        ):
            response = self.client.get(
                "/robot/state",
                headers={"x-api-key": "test-key", "x-user-id": user_id},
            )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertIn(payload["data"]["status"], VALID_ROBOT_STATUSES)
        self.assertEqual(payload["data"]["status"], STATUS_STOPPED)


if __name__ == "__main__":
    unittest.main()
