from .client import AsyncHttpClient
from .schema.client_schema import *


class PoEClient(AsyncHttpClient):

    # Requests per seconds are needed to avoid running into ggg's cloudflare ddos protection
    # I have not yet figured out what is safe to set this to. 20 results in triggering the protection.
    # It should also be noted that if this client is instantiated in different contexts, all maximum requests per seconds need to sum up to a safe value.

    def __init__(self, user_agent: str, max_requests_per_second: float = 10, retry: bool = True, raise_for_status: bool = True):
        super().__init__("https://www.pathofexile.com/api",
                         user_agent, max_requests_per_second, retry, raise_for_status)

    async def list_leagues(
            self,
            token: str,
            realm: Realm = "pc",
            type: str = "main",
            limit: int = 50,
            offset: int = 0
    ) -> ListLeaguesResponse:
        """Required scope: service:leagues"""
        return await self._send_request(
            endpoint="league",
            token=token,
            method="GET",
            params={
                "realm": realm,
                "type": type,
                "limit": limit,
                "offset": offset
            }
        )

    async def get_league(
            self,
            token: str,
            league: str,
            realm: Realm = "pc"
    ) -> GetLeagueResponse:
        """Required scope: service:leagues"""
        return await self._send_request(
            endpoint=f"league/{league}",
            token=token,
            method="GET",
            params={"realm": realm}
        )

    async def get_league_ladder(
            self,
            token: str,
            league: str,
            realm: Realm = "pc",
            sort: LadderSort = "xp",
            limit: int = 20,
            offset: int = 0
    ) -> GetLeagueLadderResponse:
        """Required scope: service:leagues:ladder"""
        return await self._send_request(
            endpoint=f"league/{league}/ladder",
            token=token,
            method="GET",
            params={
                "realm": realm,
                "sort": sort,
                "limit": limit,
                "offset": offset
            }
        )

    async def get_league_event_ladder(
            self,
            token: str,
            league: str,
            realm: Realm = "pc",
            limit: int = 20,
            offset: int = 0
    ) -> GetLeagueEventLadderResponse:
        return await self._send_request(
            endpoint=f"league/{league}/event-ladder",
            token=token,
            method="GET",
            params={
                "realm": realm,
                "limit": limit,
                "offset": offset
            }
        )

    async def get_pvp_matches(
            self,
            token: str,
            realm: Realm = "pc",
            type: PvPMatchType = "upcoming",
    ) -> GetPvPMatchesResponse:
        return await self._send_request(
            endpoint=f"pvp-match",
            token=token,
            method="GET",
            params={
                "realm": realm,
                "type": type,
            }
        )

    async def get_pvp_match(
            self,
            token: str,
            match: str,
            realm: Realm = "pc"
    ) -> GetPvPMatchResponse:
        return await self._send_request(
            endpoint=f"pvp-match/{match}",
            token=token,
            method="GET",
            params={"realm": realm}
        )

    async def get_pvp_match_ladder(
            self,
            token: str,
            match: str,
            realm: Realm = "pc",
            limit: int = 20,
            offset: int = 0
    ) -> GetPvPMatchLadderResponse:
        return await self._send_request(
            endpoint=f"pvp-match/{match}/ladder",
            token=token,
            method="GET",
            params={
                "realm": realm,
                "limit": limit,
                "offset": offset
            }
        )

    async def get_account_profile(
            self,
            token: str,
    ) -> GetAccountProfileResponse:
        return await self._send_request(
            endpoint=f"profile",
            token=token,
            method="GET"
        )

    async def get_account_leagues(
            self,
            token: str,
    ) -> ListLeaguesResponse:
        return await self._send_request(
            endpoint=f"account/leagues",
            token=token,
            method="GET"
        )

    async def list_characters(
            self,
            token: str,
    ) -> ListCharactersResponse:
        return await self._send_request(
            endpoint=f"character",
            token=token,
            method="GET"
        )

    async def get_character(
            self,
            token: str,
            character: str
    ) -> GetCharacterResponse:
        return await self._send_request(
            endpoint=f"character/{character}",
            token=token,
            method="GET"
        )

    async def list_account_stashes(
            self,
            token: str,
            league: str
    ) -> ListAccountStashesResponse:
        return await self._send_request(
            endpoint=f"stash/{league}",
            token=token,
            method="GET"
        )

    async def get_account_stash(
            self,
            token: str,
            league: str,
            stash_id: str,
            substash_id: Optional[str] = None
    ) -> GetAccountStashResponse:
        endpoint = f"stash/{league}/{stash_id}"
        if substash_id:
            endpoint += f"/{substash_id}"
        return await self._send_request(
            endpoint=endpoint,
            token=token,
            method="GET"
        )

    async def list_item_filters(
            self,
            token: str,
    ) -> ListItemFiltersResponse:
        return await self._send_request(
            endpoint=f"item-filter",
            token=token,
            method="GET",
        )

    async def get_item_filter(
            self,
            token: str,
            filter_id: str
    ) -> GetItemFilterResponse:
        return await self._send_request(
            endpoint=f"item-filter/{filter_id}",
            token=token,
            method="GET"
        )

    async def create_item_filter(
            self,
            token: str,
            body: CreateFilterBody,
            validate: str = "true"
    ) -> CreateItemFilterResponse:
        return await self._send_request(
            endpoint=f"item-filter",
            token=token,
            method="POST",
            data=body,
            headers={"Content-Type": "application/json"},
            params={"validate": validate}
        )

    async def update_item_filter(
            self,
            token: str,
            filter_id: str,
            body: UpdateFilterBody,
            validate: str = "true"
    ) -> UpdateItemFilterResponse:
        return await self._send_request(
            endpoint=f"item-filter/{filter_id}",
            token=token,
            method="POST",
            data=body,
            headers={"Content-Type": "application/json"},
            params={"validate": validate}
        )

    async def get_league_account(
            self,
            token: str,
            league: str
    ) -> GetLeagueAccountResponse:
        return await self._send_request(
            endpoint=f"league-account/{league}",
            token=token,
            method="GET"
        )

    async def list_guild_stashes(
            self,
            token: str,
            league: str
    ) -> ListGuildStashesResponse:
        return await self._send_request(
            endpoint=f"guild/stash/{league}",
            token=token,
            method="GET"
        )

    async def get_guild_stash(
            self,
            token: str,
            league: str,
            stash_id: str,
            substash_id: Optional[str] = None
    ) -> GetGuildStashResponse:
        endpoint = f"guild/stash/{league}/{stash_id}"
        if substash_id:
            endpoint += f"/{substash_id}"
        return await self._send_request(
            endpoint=endpoint,
            token=token,
            method="GET"
        )

    async def get_public_stashes(
            self,
            token: str,
            realm: Realm = "pc",
            id: Optional[str] = None
    ) -> GetPublicStashTabsResponse:
        url = "public-stash-tabs"
        params = {}
        if realm != "pc":
            url += "/" + realm
        if id:
            params = {"id": id}
        return await self._send_request(
            endpoint=url,
            token=token,
            method="GET",
            params=params
        )

    async def get_client_credentials(self, client_id: str, client_secret: str, scope: str) -> ClientCredentialsGrantResponse:
        return await self._send_request(
            endpoint="https://www.pathofexile.com/oauth/token",
            ignore_base_url=True,
            method="POST",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": scope
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )

    async def refresh_access_token(self, client_id: str, client_secret: str, refresh_token: str) -> RefreshTokenGrantResponse:
        return await self._send_request(
            endpoint="https://www.pathofexile.com/oauth/token",
            ignore_base_url=True,
            method="POST",
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
