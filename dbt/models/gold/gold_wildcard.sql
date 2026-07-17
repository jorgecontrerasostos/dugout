select
    t.league_name,
    t.team_name,
    s.wins,
    s.losses,
    s.pct,
    case
        when s.wild_card_games_back = '-' then 0::varchar
    else s.wild_card_games_back
    end::float as wild_card_games_back,
    concat(s.home_wins, '-', s.home_losses) as home_split,
    concat(s.away_wins, '-', s.away_losses) as away_split,
    case
        when s.run_differential::int > 0 then concat('+', s.run_differential)
        -- not checking < 0 since it already has a hyphen from source (-7)
        else s.run_differential::varchar
    end::varchar as run_differential,
    s.streak
from {{ ref('silver_standings') }} as s
join {{ ref('silver_teams') }} as t
on t.team_id = s.team_id
order by t.league_name, s.wild_card_games_back, s.pct desc