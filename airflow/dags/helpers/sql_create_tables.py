# creates all the tables required for us to load our data

CREATE_ALL_TABLES = """
    DROP TABLE IF EXISTS fact_ports, fact_immigration, fact_demographics, fact_temperature,
                         dim_state, dim_datetime, dim_traveller_profile;

    CREATE TABLE IF NOT EXISTS fact_immigration (
        record_id               BIGINT     PRIMARY KEY,
        state_id                VARCHAR     NOT NULL,
        arrival_date            DATE        NOT NULL        distkey,
        profile_id              VARCHAR     NOT NULL,
        all_travellers          INTEGER,
        land_travellers         INTEGER,
        sea_travellers          INTEGER,
        air_travellers          INTEGER,
        business_travellers     INTEGER,
        pleasure_travellers     INTEGER,
        student_travellers      INTEGER
    );

    CREATE TABLE IF NOT EXISTS fact_ports (
        record_id               BIGINT     PRIMARY KEY,
        state_id                VARCHAR     NOT NULL,
        port_type               VARCHAR     NOT NULL,
        num_of_ports            INTEGER
    ) DISTSTYLE ALL;

    CREATE TABLE IF NOT EXISTS fact_demographics (
        state_id                VARCHAR     PRIMARY KEY,
        total_pop               INTEGER,
        male_pop                INTEGER,
        female_pop              INTEGER,
        veteran_pop             INTEGER,
        foreign_pop             INTEGER,
        hispanic_pop            INTEGER,
        white_pop               INTEGER,
        asian_pop               INTEGER,
        black_pop               INTEGER,
        native_pop              INTEGER,
        avg_hh_size             DECIMAL,
        total_hh                DECIMAL
    ) DISTSTYLE ALL;

    CREATE TABLE IF NOT EXISTS fact_temperature (
        record_id               BIGINT     PRIMARY KEY,
        state_id                VARCHAR     NOT NULL,
        year                    INTEGER     NOT NULL        distkey,
        observation_count       INTEGER,
        num_of_cities           INTEGER,
        min_temp                DECIMAL,
        max_temp                DECIMAL,
        avg_temp                DECIMAL,
        median_temp             DECIMAL
    );

    CREATE TABLE IF NOT EXISTS dim_datetime (
        timestamp               DATE        PRIMARY KEY,
        day                     INTEGER     NOT NULL,
        month                   INTEGER     NOT NULL,
        year                    INTEGER     NOT NULL,
        month_year              VARCHAR     NOT NULL,
        day_of_week             INTEGER
    ) DISTSTYLE ALL;

    CREATE TABLE IF NOT EXISTS staging_dim_traveller_profile (
        profile_id              VARCHAR,
        gender                  VARCHAR,
        age_category            VARCHAR,
        citizen_region          VARCHAR,
        citizen_global_region   VARCHAR
    );

    CREATE TABLE IF NOT EXISTS dim_traveller_profile (
        profile_id              VARCHAR     PRIMARY KEY,
        gender                  VARCHAR,
        age_category            VARCHAR,
        citizen_region          VARCHAR,
        citizen_global_region   VARCHAR
    ) DISTSTYLE ALL;

    CREATE TABLE IF NOT EXISTS dim_state (
        state_id                VARCHAR     PRIMARY KEY,
        state_name              VARCHAR
    ) DISTSTYLE ALL;
"""