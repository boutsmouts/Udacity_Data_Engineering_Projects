class SqlQueries:
    incidents_table_insert = ("""
        INSERT INTO incidents (
            collision_id,
            "timestamp",
            location_id,
            injuries,
            fatalities,
            cars_involved,
            causes
        )
        SELECT DISTINCT
            collision_id,
            TO_TIMESTAMP("date"+' '+"time", 'DD/MM/YYYY HH') AS "timestamp",
            MD5(cast(latitude AS varchar) || cast(longitude AS varchar)) AS location_id,
            injuries,
            fatalities,
            (CASE WHEN cause_vehicle_1 IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN cause_vehicle_2 IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN cause_vehicle_3 IS NOT NULL THEN 1 ELSE 0 END +
 		        CASE WHEN cause_vehicle_4 IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN cause_vehicle_5 IS NOT NULL THEN 1 ELSE 0 END) AS cars_involved,
            (COALESCE(cause_vehicle_1,'') + '|' +
                COALESCE(cause_vehicle_2,'') + '|' +
                COALESCE(cause_vehicle_3,'') + '|' +
                COALESCE(cause_vehicle_4,'') + '|' +
                COALESCE(cause_vehicle_5,'')) AS causes
        FROM staging_crashes
        WHERE (latitude IS NOT NULL)
        AND (longitude IS NOT NULL)
        AND (latitude != 0.0)
        AND (longitude != 0.0)
    """)

    locations_table_insert = ("""
        INSERT INTO locations (
            location_id,
            borough,
            zip,
            street,
            latitude,
            longitude
        )
        SELECT DISTINCT
            MD5(cast(latitude AS varchar) || cast(longitude AS varchar)) AS location_id,
            borough,
            zip,
            street,
            latitude,
            longitude
        FROM staging_crashes
        WHERE (latitude IS NOT NULL)
        AND (longitude IS NOT NULL)
        AND (latitude != 0.0)
        AND (longitude != 0.0)
    """)

    weather_table_insert = ("""
        INSERT INTO weather (
            "timestamp",
            temperature,
            humidity,
            pressure
        )
        SELECT DISTINCT
            TO_TIMESTAMP(staging_temperature."datetime", 'YYYY-MM-DD HH') AS "timestamp",
            staging_temperature."new york",
            staging_humidity."new york",
            staging_pressure."new york"
        FROM staging_temperature
        JOIN staging_humidity ON (staging_temperature."datetime" = staging_humidity."datetime")
        JOIN staging_pressure ON (staging_temperature."datetime" = staging_pressure."datetime")
    """)

    time_table_insert = ("""
        INSERT INTO "time" (
            "timestamp",
            "hour",
            "day",
            "month",
            "year",
            weekday
        )
        SELECT DISTINCT
            TO_TIMESTAMP("date"+' '+"time", 'DD/MM/YYYY HH') AS "timestamp",
            EXTRACT(hour FROM "timestamp"),
            EXTRACT(day FROM "timestamp"),
            EXTRACT(month FROM "timestamp"),
            EXTRACT(year FROM "timestamp"),
            EXTRACT(dayofweek FROM "timestamp")
        FROM staging_crashes
        WHERE (latitude IS NOT NULL)
        AND (longitude IS NOT NULL)
        AND (latitude != 0.0)
        AND (longitude != 0.0)
    """)
