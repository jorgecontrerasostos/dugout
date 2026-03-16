import statsapi
from rich import print
from urllib3.util.wait import select_wait_for_socket

# statsapi.get(endpoint, params, force=False)
phillies = statsapi.get('team', {'teamId':143})
print(phillies)

# Get all teams
teams = statsapi.lookup_team('')
print(teams)

# Get team by IDyu
team = statsapi.lookup_team('Red Sox')
print(team)

# Get roster
red_sox_roster = statsapi.roster(111)
print(red_sox_roster)

red_sox_roster_v2 = statsapi.get('team_roster', {'teamId': 111, 'rosterType':
    'active', 'hydrate': 'person'})
print(red_sox_roster_v2)

# Get player by name (it returns various players if the param is in their name
player = statsapi.lookup_player('Mayer')
print(player)

# Mayer stats
for season in [2024, 2025]:
    mayer_stats = statsapi.player_stat_data(
        691785,
        group="hitting",
        type="season",
        season=season
    )
    print(mayer_stats)

# Crochet Stats
crochet_stats = statsapi.player_stat_data(
    personId=676979,
    group="pitching",
    type="season",
    season=2025
)
batting_stat_line = mayer_stats['stats'][0]['stats']
pitching_stat_line = crochet_stats['stats'][0]['stats']
print(batting_stat_line.keys())
print(pitching_stat_line.keys())

# Standings
standings = statsapi.standings_data(season=2025)
print(standings)
