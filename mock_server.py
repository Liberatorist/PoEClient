import logging
from aiohttp import web
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timedelta

# Configuration
MAX_REQUESTS_PER_SECOND = 1000
RATE_LIMIT_PERIODS = [10, 30]  # seconds
MAX_HITS = [5, 10]
TIMEOUT_DURATIONS = [60, 300]  # seconds

# Track request timestamps for each token and endpoint
request_timestamps = defaultdict(lambda: defaultdict(deque))
timeout_expiry = defaultdict(lambda: defaultdict(
    lambda: [None for _ in range(len(RATE_LIMIT_PERIODS))]))


def num_requests_in_period(request_queue: deque[datetime], period: int):
    period_start = datetime.now() - timedelta(seconds=period)
    return len([timestamp for timestamp in request_queue if timestamp > period_start])


def get_state(request_queue: deque[datetime], period: int, timeout: int):
    return f'{num_requests_in_period(request_queue, period)}:{period}:{timeout}'


def get_states(request_queue: deque[datetime], periods: list[int], timeouts: list[int]):
    return ','.join([get_state(request_queue, period, timeout) for period, timeout in zip(periods, timeouts)])


def get_policy_header(max_hits: list[int], periods: list[int], timeouts: list[int]):
    return ','.join([f'{max_hits[i]}:{periods[i]}:{timeouts[i]}' for i in range(len(max_hits))])


async def handle_request(request):
    print(f"Received request at {datetime.now()}")
    token = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not token:
        return web.json_response({'error': 'Unauthorized'}, status=401)
    endpoint = request.match_info['endpoint']
    now = datetime.now()

    base_headers = {}

    # Clean old requests
    while request_timestamps[token][endpoint] and (now - request_timestamps[token][endpoint][0]) > timedelta(minutes=10):
        request_timestamps[token][endpoint].popleft()

    retry_after = [0 for _ in range(len(RATE_LIMIT_PERIODS))]

    # Check if currently in timeout
    if not any(timeout_expiry[token][endpoint]):
        for idx, (period, max_hits) in enumerate(zip(RATE_LIMIT_PERIODS, MAX_HITS)):
            if num_requests_in_period(request_timestamps[token][endpoint], period) >= max_hits:
                timeout_expiry[token][endpoint][idx] = now + \
                    timedelta(seconds=TIMEOUT_DURATIONS[idx])

    for idx, timestamp in enumerate(timeout_expiry[token][endpoint]):
        if timestamp and now > timestamp:
            timeout_expiry[token][endpoint][idx] = None
        retry_after[idx] = int(
            (timestamp - now).total_seconds()) if timestamp else 0
    print(timeout_expiry[token][endpoint])
    headers = {
        **base_headers,
        'X-Rate-Limit-Policy': 'test-policy',
        'X-Rate-Limit-Rules': 'Account',
        'X-Rate-Limit-Account': get_policy_header(MAX_HITS, RATE_LIMIT_PERIODS, TIMEOUT_DURATIONS),
        'X-Rate-Limit-Account-State': get_states(request_timestamps[token][endpoint], RATE_LIMIT_PERIODS, retry_after)
    }

    if max(retry_after) > 0:
        headers['Retry-After'] = str(max(retry_after))

    # Check if currently in timeout
    if max(retry_after) > 0:
        print(f"Rate limit timeout for token {token} on endpoint {
              endpoint}. Retry after {max(retry_after)} seconds.")
        return web.json_response({'error': 'Too Many Requests'}, status=429, headers=headers)

    # Check max requests per second
    period_start = now - timedelta(seconds=1)
    recent_requests = [timestamp for timestamp in request_timestamps[token]
                       [endpoint] if timestamp > period_start]
    if len(recent_requests) >= MAX_REQUESTS_PER_SECOND:
        print(f"Max requests per second exceeded for token {token} on endpoint {
              endpoint}. Retry after {retry_after} seconds.")
        return web.json_response({'error': 'Too Many Requests'}, status=429, headers=headers)

    # Track the request
    request_timestamps[token][endpoint].append(now)

    # Return a successful response
    print(f"Request successful for token {token} on endpoint {
          endpoint}. Current state: {headers['X-Rate-Limit-Account-State']}")
    await asyncio.sleep(1)  # Simulate processing time
    return web.json_response({'message': 'Request successful'}, headers=headers)


async def init_app():
    app = web.Application()
    app.router.add_get('/{endpoint}', handle_request)
    return app

if __name__ == '__main__':
    web.run_app(init_app(), port=8080)
