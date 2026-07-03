# BullEx CPU 100% validation checklist

Use this after deploying the bullex-service cache/timeout/backoff fix.

## Two-hour soak

- Start the robot and leave it running for at least 2 hours.
- Watch CPU with `docker stats bullex-service`.
- Expected: no CPU core remains pinned near 100% for minutes while RAM stays low.

## Assets cache

- Call `GET /bullex/assets` twice for the same `x-user-id` within 300 seconds.
- Expected logs: first call may show `[GET_INSTRUMENTS_START]` and `[GET_INSTRUMENTS_FINISH]`; second call should show `[INSTRUMENTS_CACHE_HIT]`.
- Expected: no repeated `get_instruments` run while the cache is valid.

## Timeout and stale cache

- If BullEx is slow or stuck, `get_instruments/read_assets` must stop waiting after 8 seconds.
- Expected logs: `[GET_INSTRUMENTS_TIMEOUT]`, then `[INSTRUMENTS_CACHE_STALE_USED]` when an old cache exists.
- Expected: the endpoint responds with cached assets when possible instead of hanging.

## Backoff

- After an instruments timeout or error, call `GET /bullex/assets` again immediately.
- Expected logs: `[INSTRUMENTS_BACKOFF_ACTIVE]`.
- Expected backoff sequence per user: 10 seconds, then 30 seconds, then 60 seconds.
- Expected: no immediate retry loop.

## Per-user lock

- Send concurrent `GET /bullex/assets` requests for the same `x-user-id`.
- Expected logs: `[READ_ASSETS_LOCK_WAIT]` and, when cache exists, `[READ_ASSETS_LOCK_REUSED]`.
- Expected: two `get_instruments` calls for the same user never run at the same time.

## Loop audit

- Confirm `bullexapi/stable_api.py:get_instruments` has a deadline and sleeps while waiting.
- Confirm `get_all_open_time` joins worker threads with a timeout.
- Expected: no `while True` or instruments wait loop related to assets runs without a sleep and a time limit.
