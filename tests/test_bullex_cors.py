import unittest

from fastapi.testclient import TestClient

from bullex_service import main


class BullexServiceCorsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)

    def test_cors_configuration_matches_expected_contract(self) -> None:
        middleware = next(
            middleware
            for middleware in main.app.user_middleware
            if middleware.cls.__name__ == "CORSMiddleware"
        )

        self.assertEqual(
            middleware.kwargs["allow_origins"],
            [
                "https://elcapobot.online",
                "https://www.elcapobot.online",
                "http://localhost:5173",
                "http://localhost:3000",
            ],
        )
        self.assertEqual(
            middleware.kwargs["allow_methods"],
            ["GET", "POST", "OPTIONS"],
        )
        self.assertEqual(
            middleware.kwargs["allow_headers"],
            ["x-api-key", "x-user-id", "content-type", "authorization"],
        )

    def test_options_internal_routes_allows_apex_origin(self) -> None:
        for path, method in (("/account", "GET"), ("/sessions/connect", "POST")):
            with self.subTest(path=path):
                response = self.client.options(
                    path,
                    headers={
                        "Origin": "https://elcapobot.online",
                        "Access-Control-Request-Method": method,
                        "Access-Control-Request-Headers": (
                            "x-api-key,x-user-id,content-type,authorization"
                        ),
                    },
                )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.headers.get("access-control-allow-origin"),
                    "https://elcapobot.online",
                )


if __name__ == "__main__":
    unittest.main()
