import psycopg2
from faker import Faker
import random
from datetime import timedelta
import configparser

# Initialize a config file
config = configparser.ConfigParser()
config.read('config.ini')

fake = Faker()

db_params = {
    'dbname': config.get("Database", "database"),
    'user': config.get("Database", "username"),
    'password': config.get("Database", "password"),
    'host':  config.get("Database", "localhost"),
    "port": config.get("Database", "port"),
}

# The number of records to generate for each table
NUM_PLAYERS = 100
NUM_BATTLES = 50
NUM_BATTLE_TYPES = 5
NUM_MAPS = 10
NUM_TEAMS = NUM_BATTLES * 2  # Assuming each battle has 2 teams
NUM_PARTICIPATIONS = 500
NUM_PERFORMANCES = 500

# SQL statements to insert the random data into the tables
SQL_INSERT_PLAYER = "INSERT INTO players (player_name) VALUES (%s);"
SQL_INSERT_BATTLE = "INSERT INTO battles (start_time, end_time, rule_id, battle_type_id, map_id) VALUES (%s, %s, %s, %s, %s);"
SQL_INSERT_TEAM = "INSERT INTO teams (battle_id, team_name, result) VALUES (%s, %s, %s);"
SQL_INSERT_PARTICIPATION = "INSERT INTO battle_participants (battle_id, player_id, team_id, death_reason, destroyer_player_id) VALUES (%s, %s, %s, %s, %s);"
SQL_INSERT_PERFORMANCE = "INSERT INTO player_performance (battle_id, player_id, damage_dealt, damage_assisted, damage_received) VALUES (%s, %s, %s, %s, %s);"

SQL_INSERT_BATTLE_TYPE = "INSERT INTO battle_types (description) VALUES (%s);"
SQL_INSERT_MAP = "INSERT INTO maps (name, map_type) VALUES (%s, %s);"


def populate_reference_tables(cur):
    # Populate battle_types table
    for i in range(NUM_BATTLE_TYPES):
        cur.execute(SQL_INSERT_BATTLE_TYPE, (f'Type {i + 1}',))

    # Populate maps table
    for i in range(NUM_MAPS):
        cur.execute(SQL_INSERT_MAP, (f'Map {i + 1}', f'Type {i % 3 + 1}'))


def generate_random_data(conn):
    cur = conn.cursor()

    populate_reference_tables(cur)

    #Insert random players
    for _ in range(NUM_PLAYERS):
        cur.execute(SQL_INSERT_PLAYER, (fake.user_name(),))

    # Insert random battles
    for _ in range(NUM_BATTLES):
        start_time = fake.date_time_this_decade()
        duration = start_time + timedelta(hours=1)
        battle_type_id = random.randint(1, 5)  # Assume we have 5 different battle types
        map_id = random.randint(1, 10)  # Assume we have 10 different maps
        rule_id = random.randint(1, 3) # Assume we have 3 different maps
        cur.execute(SQL_INSERT_BATTLE, (start_time, duration, rule_id, battle_type_id, map_id))

    # Insert random teams
    for battle_id in range(1, NUM_BATTLES + 1):
        for _ in range(2):  # Two teams per battle
            team_name = fake.word()
            result = random.choice(['win', 'loss'])
            cur.execute(SQL_INSERT_TEAM, (battle_id, team_name, result))

    # Insert random battle participations and performances
    for _ in range(NUM_PARTICIPATIONS):
        battle_id = random.randint(1, NUM_BATTLES)
        player_id = random.randint(1, NUM_PLAYERS)
        team_id = random.randint(1, NUM_TEAMS)
        death_reason = fake.sentence()
        destroyer_player_id = random.randint(1, NUM_PLAYERS) if random.choice([True, False]) else None
        damage_dealt = random.randint(0, 10000)
        damage_assisted = random.randint(0, 5000)
        damage_received = random.randint(0, 10000)

        cur.execute(SQL_INSERT_PARTICIPATION, (battle_id, player_id, team_id, death_reason, destroyer_player_id))
        # cur.execute(SQL_INSERT_PERFORMANCE, (battle_id, player_id, damage_dealt, damage_assisted, damage_received))

    # Commit changes
    conn.commit()
    # Close the cursor
    cur.close()


# Main script execution
try:
    # Connect to the database
    conn = psycopg2.connect(**db_params)

    # Generate random data
    generate_random_data(conn)

    # Close the connection
    conn.close()

    print("Random data generation completed successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
