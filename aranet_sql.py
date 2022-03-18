#insert_client_sql_query = """insert into public.clients(name_client, id_greenhouse) values ('%s', %s);"""

#insert_prod_unit_sql_query = """insert into public.production_unit(name_unit, id_gh_greenhouse) values ('%s', %s);"""

#insert_base_sql_query = """insert into public.bases(name_base, id_unit_production_unit) values ('%s', %s);"""

#insert_sensor_sql_query = """insert into public.sensors(name_of_sensor, "id_base_Bases") values ('%s', %s);"""

#insert_topic_sql_query = """insert into public.topics(name_of_topic, id_greenhouse) values ('%s', %s);"""

#insert_sensor_value = """
#	insert into public.sensor_values (qos, "timestamp", weight, id_topic_topics, "id_client_Clients",
#	weightraw, humidity, temperature, bec, dp, pec, vwc, voltage, current, ppfd, atmosphericpressure,
#	co2, "co2Abc", rssi, "time", battery, id_sensor_sensors)
#		VALUES (%s, '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

insert_client_sql_query = """insert into public.clients(name_client, id_greenhouse) values """

insert_prod_unit_sql_query = """insert into public.production_unit(name_unit, id_gh_greenhouse) values """

insert_base_sql_query = """insert into public.bases(name_base, id_unit_production_unit) values """

insert_sensor_sql_query = """insert into public.sensors(name_of_sensor, "id_base_Bases") values """

insert_topic_sql_query = """insert into public.topics(name_of_topic, id_greenhouse) values """

insert_sensor_value = """insert into public.sensor_values
(qos, timestamp, weight, id_topic_topics, "id_client_Clients", weightraw, humidity, temperature, bec, dp, pec, vwc, voltage, current, ppfd, atmosphericpressure, co2, "co2Abc", rssi, "time", battery, id_sensor_sensors)
VALUES"""

get_data_sql_query_from_src_day = """select * 
	from public.messages_json where timestamp::DATE  = '{day}' and id <= {max_id};"""

get_data_sql_query_from_src_day_max_id = """select * 
	from public.messages_json where timestamp::DATE  = '{day}' and  id > {id} and id <= {max_id};"""

get_data_sql_query_from_src_max_id = """select * 
	from public.messages_json where timestamp::TIME > '{min_timestamp}' and id <= {max_id} and timestamp::DATE  = '{day}';"""

get_data_production_unit_from_dst = """select * 
	from public.production_unit where public.production_unit.id_gh_greenhouse = {greenhouse};;"""

get_max_id_sql_query_from_src = """select max(id) 
	from public.messages_json where timestamp::DATE  = '{day}';"""

#get_max_id_sql_sensors_values_from_day_from_dst = """select max(id) from public.sensor_values where timestamp::DATE  = '{day}';"""
get_max_id_sql_sensors_values_from_day_from_dst = """select max(public.sensor_values.id) 
	from public.sensor_values, public.sensors, public.bases, public.production_unit
	where 
	public.sensor_values.timestamp::DATE  = '{day}' and
	public.sensor_values.id_sensor_sensors = public.sensors.id_sensor and
	public.sensors."id_base_Bases" = public.bases.id_base and
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_max_timestamp_sql_sensors_values_from_day_from_timestamp_from_dst = """select max(public.sensor_values."timestamp") 
	from public.sensor_values, public.sensors, public.bases, public.production_unit
	where 
	public.sensor_values.timestamp::DATE  = '{day}' and
	public.sensor_values.id_sensor_sensors = public.sensors.id_sensor and
	public.sensors."id_base_Bases" = public.bases.id_base and
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_max_id_values_sensor_sql_query_from_dst = """select max(public.sensor_values.id) 
	from public.sensor_values, public.sensors, public.bases, public.production_unit
	where 
	public.sensor_values.id_sensor_sensors = public.sensors.id_sensor and
	public.sensors."id_base_Bases" = public.bases.id_base and
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_min_id_sql_query_from_src = """select min(id) 
	from public.messages_json where timestamp::DATE  = '{day}';"""

get_min_id_sql_query_from_src_timestamp = """select max(id) 
	from public.messages_json where timestamp::DATE  = '{day}' and timestamp::TIME = '{min_timestamp}';"""

get_first_date_record_src = """ SELECT "timestamp"
	FROM public.messages_json
	order by timestamp asc
	limit 1;"""

get_sensor_sql_query_from_dst = """select distinct on (name_of_sensor) name_of_sensor, id_sensor, "id_base_Bases" 
	from public.sensors, public.bases, public.production_unit
	where 
	public.sensors."id_base_Bases" = public.bases.id_base and
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_client_sql_query_from_dst = """select distinct on (name_client) name_client, id_client
	from public.clients
	where
	public.clients.id_greenhouse = {greenhouse};"""

get_topic_sql_query_from_dst = """select distinct on (name_of_topic) name_of_topic, id_topic 
	from public.topics
	where
	public.topics.id_greenhouse = {greenhouse};"""

get_base_sql_query_from_dst = """select distinct on (name_base) name_base, id_base, id_unit_production_unit 
	from  public.bases, public.production_unit
	where 
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_production_unit_sql_query_from_dst = """select distinct on (name_unit) name_unit 
	from production_unit
	where
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_max_id_sensor_sql_query_from_dst = """select max(id_sensor) 
	from public.sensors, public.bases, public.production_unit
	where 
	public.sensors."id_base_Bases" = public.bases.id_base and
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_max_id_values_sensor_sql_query_from_dst = """select max(id) 
	from public.sensor_values, public.sensors, public.bases, public.production_unit
	where 
	public.sensor_values.id_sensor_sensors = public.sensors.id_sensor and
	public.sensors."id_base_Bases" = public.bases.id_base and
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_max_id_client_sql_query_from_dst = """select max(id_client) 
	from public.clients
	where
	public.clients.id_greenhouse = {greenhouse};"""

get_max_id_topic_sql_query_from_dst = """select max(id_topic) 
	from public.topics
	where
	public.topics.id_greenhouse = {greenhouse};"""

get_max_id_base_sql_query_from_dst = """select max(id_base) 
	from  public.bases, public.production_unit
	where 
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_max_id_production_unit_sql_query_from_dst = """select max(id_unit) 
	from public.production_unit
	where
	public.production_unit.id_gh_greenhouse = {greenhouse};"""

get_id_production_unit_from_name_sql_query_from_dst = """select id_unit 
	from public.production_unit 
	where
	public.production_unit.id_gh_greenhouse = {greenhouse} and
	name_unit = '{name}';"""

get_id_base_from_name_sql_query_from_dst = """select id_base 
	from  public.bases, public.production_unit
	where 
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse} and
	name_base = '{name}';"""

get_id_topic_from_name_sql_query_from_dst = """select id_topic 
	from public.topics 
	where
	public.topics.id_greenhouse = {greenhouse} and
	name_of_topic = '{name}';"""

get_id_sensor_from_name_sql_query_from_dst = """select id_sensor 
	from public.sensors, public.bases, public.production_unit
	where 
	public.sensors."id_base_Bases" = public.bases.id_base and
	public.bases.id_unit_production_unit = public.production_unit.id_unit and
	public.production_unit.id_gh_greenhouse = {greenhouse} and
	name_of_sensor = '{name}';"""

get_id_client_from_name_sql_query_from_dst = """select id_client, name_client
	from public.clients 
	where
	public.clients.id_greenhouse = {greenhouse} and
	name_client = '{name}';"""

get_dataset_from_psql_to_clickhouse_aranet = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, humidity, temperature, atmosphericpressure, co2, "co2Abc", ppfd, voltage, current, bec, dp, pec, vwc, weight, weightraw
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, humidity, temperature, atmosphericpressure, co2, "co2Abc", ppfd, voltage, current, bec, dp, pec, vwc, weight, weightraw
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet_humidity = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, humidity
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet_temperature = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, temperature
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet_atmosphericpressure = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, atmosphericpressure, co2, "co2Abc"
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet_ppfd = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, ppfd
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet_voltage = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, voltage
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet_current = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, current
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet_bec = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, bec, dp, pec, vwc
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""

get_dataset_from_psql_to_clickhouse_aranet_weight = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, weight, weightraw
	FROM public.sensor_values
    JOIN public.sensors
    ON public.sensor_values.id_sensor_sensors = public.sensors.id_sensor
    JOIN public.bases
    ON public.sensors."id_base_Bases" = public.bases.id_base
	JOIN public.production_unit
    ON public.bases.id_unit_production_unit = public.production_unit.id_unit
    JOIN public.greenhouse
    ON public.production_unit.id_gh_greenhouse = public.greenhouse.id_gh
	JOIN public.clients
    ON public.sensor_values."id_client_Clients" = public.clients.id_client
    JOIN public.topics
    ON public.sensor_values.id_topic_topics = public.topics.id_topic
    where public.production_unit.id_gh_greenhouse = {greenhouse}"""