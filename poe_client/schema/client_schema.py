from typing import Literal, NotRequired, Optional, TypedDict

from .schema import *

PvPMatchType = Literal["upcoming", "season", "team"]
LadderSort = Literal["xp", "depth", "depthsolo", "ancestor", "time", "score",
                     "class=scion", "class=marauder", "class=ranger", "class=witch",
                     "class=duelist", "class=templar", "class=shadow"]


class ListLeaguesResponse(TypedDict):
    leagues: list[League]


class GetLeagueResponse(TypedDict):
    league: Optional[League]


class Ladder(TypedDict):
    total: int
    cached_since: Optional[str]
    entries: list[LadderEntry]


class GetLeagueLadderResponse(TypedDict):
    league: League
    ladder: Ladder


class GetLeagueEventLadderResponse(TypedDict):
    league: League
    ladder: EventLadder


class GetPvPMatchesResponse(TypedDict):
    matches: list[PvPMatch]


class GetPvPMatchResponse(TypedDict):
    match: Optional[PvPMatch]


class GetPvPMatchLadderResponse(TypedDict):
    match: PvPMatch
    ladder: PvPMatchLadder


class GetAccountProfileResponse(TypedDict):
    uuid: str
    name: str
    realm: Optional[Realm]
    guild: Optional[Guild]
    twitch: Optional[Twitch]


class ListCharactersResponse(TypedDict):
    characters: list[MinimalCharacter]


class GetCharacterResponse(TypedDict):
    character: Optional[Character]


class ListAccountStashesResponse(TypedDict):
    stashes: list[PublicStashChange]


class GetAccountStashResponse(TypedDict):
    stash: Optional[PublicStashChange]


class ListItemFiltersResponse(TypedDict):
    filters: list[ItemFilter]


class GetItemFilterResponse(TypedDict):
    filter: Optional[ItemFilter]


class CreateItemFilterResponse(TypedDict):
    filter: ItemFilter


class UpdateItemFilterResponse(TypedDict):
    filter: ItemFilter
    error: Optional[Error]


class CreateFilterBody(TypedDict):
    filter_name: str
    realm: Realm
    description: NotRequired[str]
    version: NotRequired[str]
    type: NotRequired[Literal["Normal", "Ruthless"]]
    public: NotRequired[bool]
    filter: str


class UpdateFilterBody(TypedDict):
    filter_name: NotRequired[str]
    realm: NotRequired[Realm]
    description: NotRequired[str]
    version: NotRequired[str]
    type: NotRequired[Literal["Normal", "Ruthless"]]
    public: NotRequired[bool]
    filter: NotRequired[str]


class GetLeagueAccountResponse(TypedDict):
    league_account: LeagueAccount


class ListGuildStashesResponse(TypedDict):
    stashes: list[PublicStashChange]


class GetGuildStashResponse(TypedDict):
    stash: Optional[PublicStashChange]


class GetPublicStashTabsResponse(TypedDict):
    next_change_id: str
    stashes: list[PublicStashChange]


class ClientCredentialsGrantResponse(TypedDict):
    access_token: str
    expires_in: Optional[int]
    token_type: str
    username: str
    sub: str
    scope: str


class RefreshTokenGrantResponse(TypedDict):
    access_token: str
    expires_in: int
    token_type: str
    username: str
    sub: str
    scope: str
    refresh_token: str
