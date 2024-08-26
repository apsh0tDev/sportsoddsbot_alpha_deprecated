#BetMGM
betmgm_url = f"https://sports.ny.betmgm.com/en/sports/api/widget/widgetdata?layoutSize=Large&page=SportLobby&sportId=5&widgetId=/mobilesports-v1.0/layout/layout_standards/modules/sportgrid&shouldIncludePayload=true"
betmgm_events = "https://sports.ny.betmgm.com/cds-api/bettingoffer/fixture-view?x-bwin-accessid=ZjVlNTEzYzAtMGUwNC00YTk1LTg4OGYtZDQ4ZGNhOWY4Mjc1&lang=en-us&country=US&userCountry=US&subdivision=US-XY&offerMapping=All&fixtureIds={id}&state=Latest&includePrecreatedBetBuilder=true&supportVirtual=false&isBettingInsightsEnabled=true&useRegionalisedConfiguration=true&includeRelatedFixtures=false&statisticsModes=All"
#365Scores
scores365_url = "https://webws.365scores.com/web/games/allscores/?appTypeId=5&langId=9&timezoneName=America/Caracas&userCountryId=18&sports=3&startDate={startDate}&endDate={endDate}&showOdds=true&onlyLiveGames=true&withTop=true"

#Unibet
unibet_url = "https://www.unibet.com/sportsbook-feeds/views/filter/tennis/all/matches?includeParticipants=true&useCombined=true"
unibet_matches = "https://eu-offering-api.kambicdn.com/offering/v2018/ub/betoffer/event/{id}.json?lang=en_GB&market=ZZ&client_id=2&channel_id=1&includeParticipants=true"

#Betfair
betfair_competitions_url = "https://api.au.pointsbet.com/api/v2/sports/tennis/competitions"
betfair_url = "https://api.au.pointsbet.com/api/v2/competitions/{competitionId}/events/featured?includeLive=true&page=1"

#Pointsbet
pointsbet_competitions_url = "https://api.rw.pointsbet.com/api/v2/sports/tennis/competitions"
pointsbet_url = "https://api.rw.pointsbet.com/api/v2/competitions/{competitionId}/events/featured?includeLive=true&page=1"
pointsbet_event_url = "https://api.rw.pointsbet.com/api/mes/v3/events/{eventId}"

#FaDuel
fanduel_live_url = f"https://sbapi.nj.sportsbook.fanduel.com/api/in-play?timezone=America%2FNew_York&eventTypeId=2&includeTabs=false&_ak=FhMFpcPWXMeyZxOx"
fanduel_url = f"https://sbapi.ny.sportsbook.fanduel.com/api/content-managed-page?page=SPORT&eventTypeId=2&_ak=FhMFpcPWXMeyZxOx&timezone=America%2FNew_York"
fanduel_event_url = "https://sbapi.ny.sportsbook.fanduel.com/api/event-page?_ak=FhMFpcPWXMeyZxOx&eventId={id}&tab={tab}&useCombinedTouchdownsVirtualMarket=true&usePulse=true&useQuickBets=true"

#Draftkings
draftkings_url = f"https://sportsbook-nash.draftkings.com/sites/US-SB/api/v4/featured/displaygroups/6/live?format=json"
draftkings_tournaments = "https://sportsbook-nash-usva.draftkings.com/sites/US-VA-SB/api/v5/eventgroups/{id}?format=json"
draftkings_markets = "https://sportsbook-nash-usva.draftkings.com/sites/US-VA-SB/api/v5/eventgroups/{tournament_id}/categories/{market_id}?format=json"

#Betrivers
betrivers_url = f"https://ny.betrivers.com/api/service/sportsbook/offering/listview/events?cageCode=212&type=live&pageSize=20&offset=0"
betrivers_event_url = "https://ny.betrivers.com/api/service/sportsbook/offering/listview/details?eventId={eventId}&cageCode=212"

available_markets = [
    "SET_ONE_WINNER",
    "SET_TWO_WINNER",
    "SET_THREE_WINNER",
    "MATCH_WINNER"
]

fanduel_tabs = [
    {"name" : "Popular", "case": "popular"},
    {"name" : "Point by Point", "case": "point-by-point"},
    {"name" : "Game Markets", "case": "game-markets"},
    {"name" : "Set Markets", "case": "set-markets"},
    {"name" : "Player Markets", "case" : "player-markets"}
]