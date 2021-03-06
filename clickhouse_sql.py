insert_aranet = """INSERT INTO aranet.aranet (*) VALUES """

insert_humidity = """INSERT INTO aranet.humidity (*) VALUES """

insert_temperature = """INSERT INTO aranet.temperature (*) VALUES """

insert_atmosphericpressure = """INSERT INTO aranet.atmosphericpressure (*) VALUES """

inert_ppfd = """INSERT INTO aranet.ppfd (*) VALUES """

insert_voltage = """INSERT INTO aranet.voltage (*) VALUES """

insert_current = """INSERT INTO aranet.current (*) VALUES """

insert_bec = """INSERT INTO aranet.bec (*) VALUES """

insert_weight = """INSERT INTO aranet.weight (*) VALUES """

join = """ 
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
    where 
    timestamp::DATE  = '{day}'
    and 
    id > {last_id} """

get_aranet = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, humidity, temperature, atmosphericpressure, co2, "co2Abc", ppfd, voltage, current, bec, dp, pec, vwc, weight, weightraw""" + join

get_humidity ="""SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, humidity""" + join + """ and humidity >= 0 order by id asc;"""
        

get_temperature = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, temperature""" + join + """ and temperature >= 0 order by id asc;"""

get_atmosphericpressure = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, atmosphericpressure, co2, "co2Abc" """ + join + """ and atmosphericpressure >= 0 and co2  >= 0 and "co2Abc" >= 0 order by id asc;"""

get_ppfd = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, ppfd""" + join + """and ppfd >= 0 order by id asc;"""

get_voltage = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, voltage""" + join + """and voltage >= 0 order by id asc;"""

get_current = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, current""" + join + """ and current >= 0 order by id asc;"""

get_bec = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, bec, dp, pec, vwc""" + join + """and bec >= 0 and dp >= 0 and pec >= 0 and vwc >= 0 order by id asc;"""

get_weight = """SELECT id, qos, timestamp, time, name_greenhouse, name_unit, name_base, name_of_sensor, name_client, name_of_topic, rssi, battery, weight, weightraw""" + join + """and weight >= 0 and weightraw >= 0 order by id asc;"""

get_last_id_aranet = """
select max(id) 
    from aranet.aranet where
    timestamp::DATE  = '{day}'"""

get_last_id_humidity = """
select max(id) 
    from aranet.humidity where
    timestamp::DATE  = '{day}'"""

get_last_id_temperature = """
select max(id) 
    from aranet.temperature where
    timestamp::DATE  = '{day}'"""

get_last_id_atmosphericpressure = """
select max(id) 
    from aranet.atmosphericpressure where
    timestamp::DATE  = '{day}'"""

get_last_id_ppfd = """
select max(id) 
    from aranet.ppfd where
    timestamp::DATE  = '{day}'"""

get_last_id_voltage = """
select max(id) 
    from aranet.voltage where
    timestamp::DATE  = '{day}'"""

get_last_id_current = """
select max(id) 
    from aranet.current where
    timestamp::DATE  = '{day}'"""

get_last_id_bec = """
select max(id) 
    from aranet.bec where
    timestamp::DATE  = '{day}'"""

get_last_id_weight = """
select max(id) 
    from aranet.weight where
    timestamp::DATE  = '{day}'"""

