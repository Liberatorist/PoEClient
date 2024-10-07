import asyncio
from collections import defaultdict, deque
from time import time
from typing import Any, Iterator, NamedTuple, Optional

import aiohttp
from multidict import CIMultiDictProxy


class Policy:
    max_hits: int
    period: int

    def __init__(self, max_hits: int, period: int):
        self.max_hits = max_hits
        self.period = period

    def _current_hits(self, request_times: deque[float]) -> int:
        """Get the number of requests made within the policy period."""
        period_start = time() - self.period
        return sum(timestamp > period_start for timestamp in request_times)

    def is_violated(self, request_times: deque[float]) -> bool:
        """Check if the policy is violated based on the request times."""
        return self._current_hits(request_times) >= self.max_hits


class RequestKey(NamedTuple):
    token: str
    endpoint: str


class AsyncHttpClient:
    request_timestamps: dict[RequestKey, deque[float]]
    rate_limit_policies: dict[RequestKey, dict[str, list[Policy]]]
    base_url: str
    max_requests_per_second: float
    retry: bool
    user_agent: str

    def __init__(self, base_url: str, user_agent: str, max_requests_per_second: float = 10, retry: bool = True, raise_for_status: bool = True):
        self.base_url = base_url
        # 100 timestamps per queue should be enough since the ggg api endpoints we use only track at most 30 hits per period
        self.request_timestamps = defaultdict(lambda: deque(maxlen=100))
        self.rate_limit_policies = {}
        self.max_requests_per_second = max_requests_per_second
        self.retry = retry
        self.user_agent = user_agent
        self.raise_for_status = raise_for_status

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def _send_request(
            self,
            endpoint: str,
            token: Optional[str] = None,
            ignore_base_url: bool = False,
            **kwargs
    ) -> Any:
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]['User-Agent'] = self.user_agent
        if token:
            kwargs["headers"]['Authorization'] = f'Bearer {token}'
        else:
            token = "IP"  # rate limit by endpoint & ip is applied
        key = RequestKey(token, endpoint)
        if "method" not in kwargs:
            kwargs["method"] = "GET"

        await self._wait_until_request_allowed(key)
        url = endpoint if ignore_base_url else f"{self.base_url}/{endpoint}"
        self.request_timestamps[key].append(time())
        async with self.session.request(url=url, **kwargs) as response:
            self._adjust_policies(key, response.headers)
            if response.status == 429 and self.retry:
                retry_after = int(response.headers.get('Retry-After', 1))
                print(f"Rate limited. Retrying after {retry_after} seconds.")
                await asyncio.sleep(retry_after)
                return await self._send_request(key.endpoint, key.token, **kwargs)

            if response.status > 399 and self.raise_for_status:
                response.raise_for_status()

            try:
                return await response.json()
            except aiohttp.ContentTypeError as e:
                print(f"Failed to decode JSON: {e}")
                print(f"Response content: {await response.text()}")
                raise e

    async def _wait_until_request_allowed(self, key: RequestKey) -> None:
        """Wait until a request can be made based on the rate limit policies."""
        while not self._can_make_request(key):
            await asyncio.sleep(.1)

    def _adjust_policies(self, key: RequestKey, headers: CIMultiDictProxy[str]) -> None:
        """Update the rate limit policies based on the response headers."""
        if "dummy" in self.rate_limit_policies[key]:
            # Remove dummy policy
            del self.rate_limit_policies[key]["dummy"]

        now = time()
        timestamps = self.request_timestamps[key]
        new_policies = {}
        for rule in self._get_rules(headers):
            policies = []
            for policy, current_hits in self._parse_policies(rule, headers):
                policies.append(policy)
                # If the server has tracked more requests than the client, add artifical timestamps to the queue to match the server
                start_time = now - policy.period
                tracked_hits = sum(
                    timestamp > start_time for timestamp in timestamps)
                missing_hits = current_hits - tracked_hits
                timestamps.extend(now for _ in range(missing_hits))
            new_policies[rule] = policies
        self.rate_limit_policies[key] = new_policies

    def _can_make_request(self, key: RequestKey) -> bool:
        """Check if a request can be made based on the rate limit policies."""
        if self._ip_is_rate_limited():
            return False
        if key not in self.rate_limit_policies:
            # create a dummy policy to allow the first request, but disallow subsequent requests until the policy is known
            # this policy will be removed after the first response is received
            self.rate_limit_policies[key] = {
                "dummy": [Policy(1, 9999999)]
            }
            return True

        for policies in self.rate_limit_policies[key].values():
            for policy in policies:
                if policy.is_violated(self.request_timestamps[key]):
                    return False
        return True

    def _ip_is_rate_limited(self) -> bool:
        """Check if the client has exceeded the maximum number of requests per second."""
        start = time() - 1 / self.max_requests_per_second
        requests_time_period = sum(
            timestamp > start
            for queue in self.request_timestamps.values()
            for timestamp in queue
        )
        return requests_time_period >= 1

    @staticmethod
    def _get_rules(headers: CIMultiDictProxy[str]) -> list[str]:
        rules = headers.get('X-Rate-Limit-Rules')
        if not rules:
            return []
        return rules.split(',')

    @staticmethod
    def _parse_policies(rule: str, headers: CIMultiDictProxy[str]) -> Iterator[tuple[Policy, int]]:
        limit_header = f'X-Rate-Limit-{rule}'
        state_header = f'X-Rate-Limit-{rule}-State'
        limits = headers.get(limit_header)
        states = headers.get(state_header)
        if limits and states:
            for limit, state in zip(limits.split(','), states.split(',')):
                max_hits, period, _ = map(int, limit.split(':'))
                current_hits, _, _ = map(int, state.split(':'))
                yield Policy(max_hits, period), current_hits
