# PoEClient

Async client to run simultaneous requests against the Path of Exile API without triggering rate limiting

# Installation

```
pip install poe-async-client
```

# Example usage

```python
import asyncio
from poe_async_client.poe_client import PoEClient

async def fetch_stash_changes_simultaneously():
    client = PoEClient(
        max_requests_per_second=5,
        user_agent="OAuth myapp/1.0.0 (Contact: Liberatorist@gmail.com)"
    )
    async with client:
        token_response = await client.get_client_credentials(
            client_id="myapp",
            client_secret="super secret",
            scope="service:psapi"
        )
        token = token_response["access_token"]

        simultaneous_requests = [
            client.get_public_stashes(token=token)
            for _ in range(10)
        ]

        responses = await asyncio.gather(*simultaneous_requests)

```
