--Question 5

--For all table we apply scd2


-- Create Battle Types Table
CREATE TABLE battle_types (
    battle_type_id SERIAL PRIMARY KEY,
    description TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE TABLE rules (
    rule_id SERIAL PRIMARY KEY,
    description TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create Maps Table
CREATE TABLE maps (
    map_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    map_type VARCHAR(100),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create Players Table
CREATE TABLE players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create Battles Table
CREATE TABLE battles (
    battle_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    battle_type_id INTEGER NOT NULL REFERENCES battle_types(battle_type_id),
    rule_id INTEGER NOT NULL REFERENCES rules(rule_id),
    map_id INTEGER NOT NULL REFERENCES maps(map_id),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    CHECK (end_time -  start_time <= INTERVAL '1 hour') --this is a constraint for a duration of the battle
);

-- Teams
CREATE TABLE teams (
    team_id SERIAL PRIMARY KEY,
    battle_id INTEGER NOT NULL REFERENCES battles(battle_id),
    team_name VARCHAR(255),
    result VARCHAR(50),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create Battle Participation Table
CREATE TABLE battle_participants (
    battle_id INTEGER NOT NULL REFERENCES battles(battle_id),
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    result VARCHAR(50),
    death_reason VARCHAR(255),
    destroyer_player_id INTEGER REFERENCES players(player_id),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (battle_id, player_id)
);

-- Create Player Performance Table
CREATE TABLE player_performance (
    battle_id INTEGER NOT NULL REFERENCES battles(battle_id),
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    damage_dealt INTEGER NOT NULL,
    damage_assisted INTEGER NOT NULL,
    damage_received INTEGER NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    performance_metrics JSONB, -- For flexible storage of additional metrics
    PRIMARY KEY (battle_id, player_id)
);

-- Create Player Economics Table
CREATE TABLE player_economics (
    battle_id INTEGER NOT NULL REFERENCES battles(battle_id),
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    currency_earned INTEGER NOT NULL,
    currency_spent INTEGER NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (battle_id, player_id)
);


-- Function to update the 'updated_at' column
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for the 'players' table
CREATE TRIGGER update_players_modtime
BEFORE UPDATE ON players
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

--This trigger function checks if the number of teams for a new or updated battle_id is exactly two,
-- and if not, it raises an exception.
CREATE FUNCTION ensure_two_teams_per_battle() RETURNS trigger AS $$
BEGIN
    IF (SELECT COUNT(*) FROM teams WHERE battle_id = NEW.battle_id) != 2 THEN
        RAISE EXCEPTION 'There must be exactly two teams per battle.';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_two_teams_per_battle BEFORE INSERT OR UPDATE ON teams
FOR EACH ROW EXECUTE FUNCTION ensure_two_teams_per_battle();



--Question 6
CREATE TABLE player_battle_settings (
   battle_id INTEGER NOT NULL REFERENCES battles(battle_id),
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    settings JSONB NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (battle_id, player_id)
);

-- Example of inserting data into the table
INSERT INTO player_battle_settings (battle_id, player_id, settings)
VALUES
(
    1,
    42,
    '{
        "show_vehicle_tier": true,
        "display_dog_tags": false,
        "display_actual_dog_tag": true,
        "enable_optics_effect": false,
        "show_in_postmortem": true,
        "enable_dynamic_camera": false,
        "smooth_camera_zoom": true,
        "enable_tactical_view": false,
        "enable_commander_camera": true,
        "horizontal_stabilization": false,
        "enable_x16_x25_zoom": true,
        "restrict_sniper_mode_toggle": true,
        "enable_automatic_hull_lock": false,
        "enable_server_reticle": true,
        "preferred_sniper_mode_zoom_level": "last_used",
        "show_vehicle_markers": true
    }'::JSONB
);

--Question 7


WITH battle_ranks AS (
    SELECT
        player_id,
        battle_id,
        result,
        LAG(result) OVER (PARTITION BY player_id ORDER BY battle_id) AS prev_result,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY battle_id) AS rn
    FROM
        battle_participants
),
result_change_flag AS (
    SELECT
        player_id,
        battle_id,
        result,
        CASE
            WHEN result = 'win' AND (prev_result IS NULL OR prev_result != 'win') THEN 1
            ELSE 0
        END AS win_flag,
        rn
    FROM battle_ranks
),
win_groups AS (
    SELECT
        player_id,
        battle_id,
        result,
        SUM(win_flag) OVER (PARTITION BY player_id ORDER BY rn) AS win_group
    FROM result_change_flag
),
win_steaks AS (
    SELECT
        player_id,
        win_group,
        COUNT(*) AS streak_length
    FROM win_groups
    WHERE result = 'win'
    GROUP BY player_id, win_group
),
max_win_streaks AS (
    SELECT
        player_id,
        MAX(streak_length) AS max_streak_length
    FROM win_steaks
    GROUP BY player_id
)
SELECT
    player_id,
    max_streak_length
FROM max_win_streaks
ORDER BY max_streak_length DESC, player_id;


--Question 8
WITH ordered_battles AS (
    SELECT
        bp.player_id,
        b.start_time::date AS battle_date,
        b.start_time,
        bp.result,
        pp.damage_dealt,
        -- Assuming a column kills exists for kill count:
        pp.kills,
        ROW_NUMBER() OVER (PARTITION BY bp.player_id, b.start_time::date, bp.result ORDER BY b.battle_id) AS result_rank
    FROM
        battle_participants bp
    INNER JOIN
        battles b ON bp.battle_id = b.battle_id
    INNER JOIN
        player_performance pp ON bp.battle_id = pp.battle_id AND bp.player_id = pp.player_id
),
damege_in_7th AS (
    SELECT
        player_id,
        battle_date,
        damage_dealt
    FROM
        ordered_battles
    WHERE
        result = 'win' AND result_rank = 7
),
kills_in_3rd_lose AS (
    SELECT
        player_id,
        battle_date,
        kills
    FROM
        ordered_battles
    WHERE
        result = 'loss' AND result_rank = 3
),
firsrt_draw_batlle AS (
    SELECT
        player_id,
        battle_date,
        start_time AS first_draw_time
    FROM
        ordered_battles
    WHERE
        result = 'draw' AND result_rank = 1
)
SELECT
    COALESCE(d.player_id, k.player_id, f.player_id) AS player_id,
    COALESCE(d.battle_date, k.battle_date, f.battle_date) AS battle_date,
    d.damage_dealt AS damage_in_7th_win,
    k.kills AS kills_in_3rd_loss,
    f.first_draw_time
FROM
    damege_in_7th d
FULL JOIN
    kills_in_3rd_lose k ON d.player_id = k.player_id AND d.battle_date = k.battle_date
FULL JOIN
    firsrt_draw_batlle f ON d.player_id = f.player_id AND d.battle_date = f.battle_date
ORDER BY
    battle_date, player_id;





