CREATE TABLE IF NOT EXISTS public.staging_temperature (
	"datetime" varchar(256) NOT NULL,
	"New York" float,
	PRIMARY KEY ("datetime")
);

CREATE TABLE IF NOT EXISTS public.staging_humidity (
	"datetime" varchar(256) NOT NULL,
	"New York" float,
	PRIMARY KEY ("datetime")
);

CREATE TABLE IF NOT EXISTS public.staging_pressure (
	"datetime" varchar(256) NOT NULL,
	"New York" float,
	PRIMARY KEY ("datetime")
);

CREATE TABLE IF NOT EXISTS public.staging_crashes (
	collision_id float NOT NULL,
	"date" varchar(256),
	"time" varchar(256),
	borough varchar(256),
	zip varchar(256),
	street varchar(256),
	latitude float,
	longitude float,
	injuries float,
	fatalities float,
	cause_vehicle_1 varchar(256),
	cause_vehicle_2 varchar(256),
	cause_vehicle_3 varchar(256),
	cause_vehicle_4 varchar(256),
	cause_vehicle_5 varchar(256),
	PRIMARY KEY (collision_id)
);

CREATE TABLE IF NOT EXISTS public.incidents (
	collision_id float NOT NULL,
	"timestamp" timestamp,
	location_id varchar(256),
	injuries float,
	fatalities float,
	cars_involved int,
	causes varchar(256),
	PRIMARY KEY (collision_id)
);

CREATE TABLE IF NOT EXISTS public.weather (
	"timestamp" timestamp NOT NULL,
	temperature float,
	humidity float,
	pressure float,
	PRIMARY KEY ("timestamp")
);

CREATE TABLE IF NOT EXISTS public.locations (
	location_id varchar(256),
	borough varchar(256),
	zip varchar(256),
	street varchar(256),
	latitude float,
	longitude float,
	PRIMARY KEY (location_id)
);

CREATE TABLE IF NOT EXISTS public."time" (
	"timestamp" timestamp NOT NULL,
	"hour" int,
	"day" int,
	"month" varchar(256),
	"year" int,
	weekday varchar(256),
	PRIMARY KEY ("timestamp")
);
