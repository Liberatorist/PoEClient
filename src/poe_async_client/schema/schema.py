from typing import Literal, NotRequired, Optional, TypedDict, Union

Realm = Literal["pc", "sony", "xbox"]


class LeagueRule(TypedDict):
    id: str
    name: str
    description: Optional[str]


class LeagueCategory(TypedDict):
    id: str
    current: NotRequired[Literal[True]]


class League(TypedDict):
    id: str
    realm: Optional[Realm]
    description: Optional[str]
    category: Optional[LeagueCategory]
    rules: Optional[list[LeagueRule]]
    registerAt: Optional[str]
    event: NotRequired[Literal[True]]
    url: Optional[str]
    startAt: Optional[str]
    endAt: Optional[str]
    timedEvent: NotRequired[Literal[True]]
    scoreEvent: NotRequired[Literal[True]]
    delveEvent: NotRequired[Literal[True]]
    ancestorEvent: NotRequired[Literal[True]]
    leagueEvent: NotRequired[Literal[True]]


class LadderEntryCharacterDepth(TypedDict):
    depth: Optional[int]


LadderEntryCharacter = TypedDict("LadderEntryCharacter", {
    "id": str,
    "name": str,
    "level": int,
    "class": str,
    "time": Optional[int],
    "score": Optional[int],
    "progress": Optional[dict],
    "experience": Optional[int],
    "depth": Optional[LadderEntryCharacterDepth]
})


class LadderEntry(TypedDict):
    rank: int
    dead: NotRequired[Literal[True]]
    retired: NotRequired[Literal[True]]
    ineligible: NotRequired[Literal[True]]
    public: NotRequired[Literal[True]]
    character: LadderEntryCharacter
    account: Optional['Account']


class PrivateLeague(TypedDict):
    name: str
    url: str


class EventLadderEntry(TypedDict):
    rank: int
    ineligible: NotRequired[Literal[True]]
    time: Optional[int]
    private_league: PrivateLeague


class AccountChallenges(TypedDict):
    set: str
    completed: int
    max: int


class AccountTwitchStream(TypedDict):
    name: str
    image: str
    status: str


class AccountTwitch(TypedDict):
    name: str
    stream: Optional[AccountTwitchStream]


class Guild(TypedDict):
    id: int
    name: str
    tag: str


class Account(TypedDict):
    name: str
    realm: Optional[Realm]
    guild: Optional[Guild]
    challenges: Optional[AccountChallenges]
    twitch: Optional[AccountTwitch]


class PvPLadderTeamMember(TypedDict):
    account: Account
    character: 'PvPCharacter'
    public: NotRequired[Literal[True]]


class PvPLadderTeamEntry(TypedDict):
    rank: int
    rating: Optional[int]
    points: Optional[int]
    games_played: Optional[int]
    cumulative_opponent_points: Optional[int]
    last_game_time: Optional[str]
    members: list[PvPLadderTeamMember]


class PvPMatch(TypedDict):
    id: str
    realm: Optional[Realm]
    startAt: Optional[str]
    endAt: Optional[str]
    url: Optional[str]
    description: str
    glickoRatings: bool
    pvp: bool
    style: Literal["Blitz", "Swiss", "Arena"]
    registerAt: Optional[str]
    complete: NotRequired[Literal[True]]
    upcoming: NotRequired[Literal[True]]
    inProgress: NotRequired[Literal[True]]


class PublicStashChange(TypedDict):
    id: str
    public: bool
    accountName: Optional[str]
    stash: Optional[str]
    lastCharacterName: NotRequired[str]
    stashType: str
    league: Optional[str]
    items: list["Item"]


class Ladder(TypedDict):
    total: int
    cached_since: Optional[str]
    entries: list[LadderEntry]


class EventLadder(TypedDict):
    total: int
    entries: list[EventLadderEntry]


class PvPLadder(TypedDict):
    total: int
    entries: list[PvPLadderTeamEntry]


class ProfileGuild(TypedDict):
    name: str
    tag: str


class ProfileTwitch(TypedDict):
    name: str
    stream: Optional[str]


class Profile(TypedDict):
    uuid: str
    name: str
    realm: Optional[Realm]
    guild: Optional[ProfileGuild]
    twitch: Optional[ProfileTwitch]


class ItemFilterValidation(TypedDict):
    valid: bool
    version: Optional[str]
    validated: Optional[str]


class ItemFilter(TypedDict):
    id: str
    filter_name: str
    realm: Realm
    description: str
    version: str
    type: str
    public: NotRequired[Literal[True]]
    filter: Optional[str]
    validation: Optional[ItemFilterValidation]


class Error(TypedDict):
    message: str
    code: int


class ItemSocket(TypedDict):
    group: int
    attr: Optional[str]
    sColour: Optional[str]


class ItemProperty(TypedDict):
    name: str
    values: list[list]
    displayMode: Optional[int]
    progress: Optional[float]
    type: Optional[int]
    suffix: Optional[str]


class CrucibleNode(TypedDict):
    # Define the structure of CrucibleNode if available
    pass


class ItemInfluences(TypedDict):
    elder: NotRequired[Literal[True]]
    shaper: NotRequired[Literal[True]]
    searing: NotRequired[Literal[True]]
    tangled: NotRequired[Literal[True]]


class ItemReward(TypedDict):
    label: str
    rewards: dict[str, int]


class ItemLogbookModFaction(TypedDict):
    id: Literal["Faction1", "Faction2", "Faction3", "Faction4"]
    name: str


class ItemLogbookMod(TypedDict):
    name: str
    faction: ItemLogbookModFaction
    mods: list[str]


class ItemUltimatumMod(TypedDict):
    type: str
    tier: int


class ItemIncubatedItem(TypedDict):
    name: str
    level: int
    progress: int
    total: int


class ItemScourged(TypedDict):
    tier: int
    level: Optional[int]
    progress: Optional[int]
    total: Optional[int]


class ItemCrucible(TypedDict):
    layout: str
    nodes: dict[str, CrucibleNode]


class ItemHybrid(TypedDict):
    isVaalGem: NotRequired[Literal[True]]
    baseTypeName: str
    properties: Optional[list[ItemProperty]]
    explicitMods: Optional[list[str]]
    secDescrText: Optional[str]


class ItemExtended(TypedDict):
    category: Optional[str]
    subcategories: Optional[list[str]]
    prefixes: Optional[int]
    suffixes: Optional[int]


class Item(TypedDict):
    verified: bool
    w: int
    h: int
    icon: str
    support: NotRequired[Literal[True]]
    stackSize: NotRequired[int]
    maxStackSize: NotRequired[int]
    stackSizeText: NotRequired[str]
    league: str
    id: str
    influences: NotRequired[ItemInfluences]
    elder: NotRequired[Literal[True]]
    shaper: NotRequired[Literal[True]]
    searing: NotRequired[Literal[True]]
    tangled: NotRequired[Literal[True]]
    abyssJewel: NotRequired[Literal[True]]
    delve: NotRequired[Literal[True]]
    fractured: NotRequired[Literal[True]]
    synthesised: NotRequired[Literal[True]]
    sockets: NotRequired[list[ItemSocket]]
    socketedItems: NotRequired[list['Item']]
    name: str
    typeLine: str
    baseType: str
    rarity: NotRequired[Literal["Normal", "Magic", "Rare", "Unique"]]
    identified: bool
    itemLevel: NotRequired[int]
    ilvl: int
    note: NotRequired[str]
    forum_note: NotRequired[str]
    lockedToCharacter: NotRequired[Literal[True]]
    lockedToAccount: NotRequired[Literal[True]]
    duplicated: NotRequired[Literal[True]]
    split: NotRequired[Literal[True]]
    corrupted: NotRequired[Literal[True]]
    unmodifiable: NotRequired[Literal[True]]
    cisRaceReward: NotRequired[Literal[True]]
    seaRaceReward: NotRequired[Literal[True]]
    thRaceReward: NotRequired[Literal[True]]
    properties: NotRequired[list[ItemProperty]]
    notableProperties: NotRequired[list[ItemProperty]]
    requirements: NotRequired[list[ItemProperty]]
    additionalProperties: NotRequired[list[ItemProperty]]
    nextLevelRequirements: NotRequired[list[ItemProperty]]
    talismanTier: NotRequired[int]
    rewards: NotRequired[list[ItemReward]]
    secDescrText: NotRequired[str]
    utilityMods: NotRequired[list[str]]
    logbookMods: NotRequired[list[ItemLogbookMod]]
    enchantMods: NotRequired[list[str]]
    scourgeMods: NotRequired[list[str]]
    implicitMods: NotRequired[list[str]]
    ultimatumMods: NotRequired[list[ItemUltimatumMod]]
    explicitMods: NotRequired[list[str]]
    craftedMods: NotRequired[list[str]]
    fracturedMods: NotRequired[list[str]]
    crucibleMods: NotRequired[list[str]]
    cosmeticMods: NotRequired[list[str]]
    veiledMods: NotRequired[list[str]]
    veiled: NotRequired[Literal[True]]
    descrText: NotRequired[str]
    flavourText: NotRequired[list[str]]
    flavourTextParsed: NotRequired[list[Union[dict, str]]]
    flavourTextNote: NotRequired[str]
    prophecyText: NotRequired[str]
    isRelic: NotRequired[Literal[True]]
    foilVariation: NotRequired[int]
    replica: NotRequired[Literal[True]]
    foreseeing: NotRequired[Literal[True]]
    incubatedItem: NotRequired[ItemIncubatedItem]
    scourged: NotRequired[ItemScourged]
    crucible: NotRequired[ItemCrucible]
    ruthless: NotRequired[Literal[True]]
    frameType: NotRequired[int]
    artFilename: NotRequired[str]
    hybrid: NotRequired[ItemHybrid]
    extended: NotRequired[ItemExtended]
    x: NotRequired[int]
    y: NotRequired[int]
    inventoryId: NotRequired[str]
    socket: NotRequired[int]
    colour: NotRequired[Literal["S", "D", "I", "G"]]


class Passives(TypedDict):
    hashes: list[int]
    hashes_ex: list[int]
    mastery_effects: dict[int, int]
    skill_overrides: dict[str, 'PassiveNode']
    bandit_choice: Optional[str]
    pantheon_major: Optional[str]
    pantheon_minor: Optional[str]
    jewel_data: dict[str, 'ItemJewelData']
    alternate_ascendancy: Optional[str]


class Metadata(TypedDict):
    version: str


class ItemJewelDataSubgraph(TypedDict):
    groups: dict[str, 'PassiveGroup']
    nodes: dict[str, 'PassiveNode']


class ItemJewelData(TypedDict):
    type: str
    radius: Optional[int]
    radiusMin: Optional[int]
    radiusVisual: Optional[str]
    subgraph: Optional[ItemJewelDataSubgraph]


class StashTabMetadata(TypedDict):
    public: NotRequired[Literal[True]]
    folder: NotRequired[Literal[True]]
    colour: Optional[str]


class StashTab(TypedDict):
    id: str
    parent: Optional[str]
    name: str
    type: str
    index: Optional[int]
    metadata: StashTabMetadata
    children: Optional[list['StashTab']]
    items: Optional[list[Item]]


class AtlasPassiveTree(TypedDict):
    name: str
    hashes: list[int]


class AtlasPassives(TypedDict):
    hashes: list[int]


class LeagueAccount(TypedDict):
    atlas_passives: NotRequired[AtlasPassives]
    atlas_passive_trees: list[AtlasPassiveTree]


class PassiveGroup(TypedDict):
    x: float
    y: float
    orbits: list[int]
    isProxy: NotRequired[Literal[True]]
    proxy: Optional[str]
    nodes: list[str]


class PassiveNodeMasteryEffect(TypedDict):
    effect: int
    stats: list[str]
    reminderText: Optional[list[str]]


class PassiveNodeExpansionJewel(TypedDict):
    size: Optional[int]
    index: Optional[int]
    proxy: Optional[int]
    parent: Optional[int]


PassiveNode = TypedDict('PassiveNode', {
    "skill": Optional[int],
    "name": Optional[str],
    "icon": Optional[str],
    "isKeystone": NotRequired[Literal[True]],
    "isNotable": NotRequired[Literal[True]],
    "isMastery": NotRequired[Literal[True]],
    "inactiveIcon": Optional[str],
    "activeIcon": Optional[str],
    "activeEffectImage": Optional[str],
    "masteryEffects": Optional[list[PassiveNodeMasteryEffect]],
    "isBlighted": NotRequired[Literal[True]],
    "isTattoo": NotRequired[Literal[True]],
    "isProxy": NotRequired[Literal[True]],
    "isJewelSocket": NotRequired[Literal[True]],
    "expansionJewel": Optional[PassiveNodeExpansionJewel],
    "recipe": Optional[list[str]],
    "grantedStrength": Optional[int],
    "grantedDexterity": Optional[int],
    "grantedIntelligence": Optional[int],
    "ascendancyName": Optional[str],
    "isAscendancyStart": NotRequired[Literal[True]],
    "isMultipleChoice": NotRequired[Literal[True]],
    "isMultipleChoiceOption": NotRequired[Literal[True]],
    "grantedPassivePoints": Optional[int],
    "stats": Optional[list[str]],
    "reminderText": Optional[list[str]],
    "flavourText": Optional[list[str]],
    "classStartIndex": Optional[int],
    "group": Optional[str],
    "orbit": Optional[int],
    "orbitIndex": Optional[int],
    "out": list[str],
    "in": list[str]
})

CrucibleNode = TypedDict('CrucibleNode', {
    "skill": Optional[int],
    "tier": Optional[int],
    "icon": Optional[str],
    "allocated": NotRequired[Literal[True]],
    "isNotable": NotRequired[Literal[True]],
    "isReward": NotRequired[Literal[True]],
    "stats": Optional[list[str]],
    "reminderText": Optional[list[str]],
    "orbit": Optional[int],
    "orbitIndex": Optional[int],
    "out": list[str],
    "in": list[str]
})

CrucibleNode = TypedDict('CrucibleNode', {
    "skill": Optional[int],
    "tier": Optional[int],
    "icon": Optional[str],
    "allocated": NotRequired[Literal[True]],
    "isNotable": NotRequired[Literal[True]],
    "isReward": NotRequired[Literal[True]],
    "stats": Optional[list[str]],
    "reminderText": Optional[list[str]],
    "orbit": Optional[int],
    "orbitIndex": Optional[int],
    "out": list[str],
    "in": list[str]
})

PvPCharacter = TypedDict('PvPCharacter', {
    "id": str,
    "name": str,
    "level": int,
    "class": str,
    "league": Optional[str],
    "score": Optional[int],
})


Character = TypedDict('Character', {
    "id": str,
    "name": str,
    "realm": Realm,
    "class": str,
    "league": Optional[str],
    "level": int,
    "experience": int,
    "ruthless": NotRequired[Literal[True]],
    "expired": NotRequired[Literal[True]],
    "deleted": NotRequired[Literal[True]],
    "current": NotRequired[Literal[True]],
    "equipment": list[Item],
    "inventory": list[Item],
    "rucksack": NotRequired[list[Item]],
    "jewels": list[Item],
    "passives": Passives,
    "metadata": Metadata
})

MinimalCharacter = TypedDict('MinimalCharacter', {
    "id": str,
    "name": str,
    "realm": Realm,
    "class": str,
    "league": Optional[str],
    "level": int,
    "experience": int,
    "ruthless": NotRequired[Literal[True]],
    "expired": NotRequired[Literal[True]],
    "deleted": NotRequired[Literal[True]],
    "current": NotRequired[Literal[True]],
})


class PvPMatchLadder(TypedDict):
    total: int
    entries: list[PvPLadderTeamEntry]


class Twitch(TypedDict):
    name: str
