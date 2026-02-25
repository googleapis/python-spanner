import time
import asyncio
from google.api_core.exceptions import Aborted

async def _delay_until_retry(exc, deadline, attempts, default_retry_delay=None):
    from google.cloud.spanner_v1._helpers import _get_retry_delay
    delay = _get_retry_delay(exc, attempts, default_retry_delay)
    if time.time() + delay > deadline:
        raise exc
    await asyncio.sleep(delay)

async def _retry_on_aborted_exception(func, deadline, default_retry_delay=None):
    attempts = 0
    while True:
        try:
            attempts += 1
            return await func()
        except Aborted as exc:
            await _delay_until_retry(
                exc,
                deadline=deadline,
                attempts=attempts,
                default_retry_delay=default_retry_delay,
            )
            continue

async def _retry(
    func,
    retry_count=5,
    delay=2,
    allowed_exceptions=None,
    before_next_retry=None,
):
    retries = 0
    while True:
        try:
            return await func()
        except Exception as e:
            if allowed_exceptions and type(e) in allowed_exceptions:
                _check_err = allowed_exceptions.get(type(e))
                if callable(_check_err) and not _check_err(e):
                    raise e
            if retries >= retry_count:
                raise e
            if before_next_retry:
                before_next_retry(retries, delay)
            await asyncio.sleep(delay)
            retries += 1
