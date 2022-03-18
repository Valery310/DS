"""
http://192.168.24.22:15672/#/
http://airflow.agrom.local:5555/dashboard
http://airflow.agrom.local/home
http://airflow.agrom.local:8889/lab

"""
import logging
import datetime
from datetime import datetime, timedelta
from aranet_sql import *
import sys
from alert import *
from aranet_class import sensor_value, production_unit, topic, clients, bases, sensor, AranetEncoder
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task

# default_args = {'start_date': date.today()}
#
error_head = "Aranet_TKP"
# Заголовок отправляемых сообщений об ошибках для идентификации проблемного дага.
src = PostgresHook(postgres_conn_id='postgres_aranet_tkp')
# Хук к базам источникам в комплексах
_dag_id = 'aranet_postgresql_tkp'
# Дата старта синхронизации. Логическая переменная, которая запускает синхронизацию с указанной даты.
_start_date = datetime(2022, 3, 2)
# Переменная, указывающая на уникальный ключ тепличного комплекса, который подставляется в запросы к бд
greenhouse = 3
# путь к каталогу хранения промежутточных файлов
path = "/var/share/tmp/aranet/tkp/"

log = logging.getLogger(__name__)
dst = PostgresHook(postgres_conn_id='postgres_dwh')
dst.get_conn().autocommit = True
cursor_src = src.get_conn().cursor()
cursor_dst = dst.get_conn().cursor()
current_max_id = 0
# [CONNECT Подключение к базам данных. Точка входа выполнения.]


def connect_to_psql(execution_date, **kwargs):
    ti = kwargs['ti']
    current_day = datetime.date(execution_date)
    current_day = current_day - timedelta(minutes=3)
    log.info(
        "Запуск копирования данных. Копируются данные за:" + str(current_day))
    log.info("Выполняется подключение к базам данных.")


# [CONNECT_SRC Подключение к базе данных источника.]
def connect_to_psql_src():
    log.info("Выполняется подключение к базе источнику")
    log.info("Подключение к базе источнику выполнено успешно!")
    #log.error("Соединение с БД назначения не было установлено!")
    # time.sleep(100)


# [CONNECT_DST Подключение к базе данных назначения.]
def connect_to_postgres_dst():
    log.info("Выполняется подключение к базе назначению")
    #log.info("Подключение к базе назначния выполнено успешно!")
    # time.sleep(100)
    # connect_to_postgres_dst()

# [get_max_id_src Получение последнего обработанного id запси из баз источника.]


def get_max_id_src():
    cursor_src.execute(get_max_id_sql_query_from_src)
    result = cursor_src.fetchone()[0]
    if result is None:
        return 0
    else:
        return result

# [EXTRACT_FROM_SRC Извлечение данных из БД источника.]


def extract_all_from_src(execution_date, **kwargs):
    import csv
    import os
    from time import sleep
    ti = kwargs['ti']
    #execution_date = datetime.date(execution_date)
    current_day = execution_date - timedelta(minutes=3)
    current_day = datetime.date(current_day)
    #execution_date = datetime.date(execution_date - timedelta(days=1))
    # ti.xcom_pull(key='execution_date')
    log.info(
        "Извлекаются данные за:" + str(current_day))
    log.info("Выполняется извлечение данных")
    # day = datetime.today()  # - timedelta(days=1)
    try:
        log.info("Создание каталога {path}extract/.".format(path=path))
        os.makedirs("{path}extract/".format(path=path), exist_ok=True)

        try:
            query_max_id = get_max_id_sql_query_from_src.format(
                day=current_day)
            log.info(query_max_id)
            cursor_src.execute(query_max_id)

            max_id = cursor_src.fetchone()[0]

            #ti.xcom_push(key='min_id', value=min_id)
            query_day_max_timestamp = get_max_timestamp_sql_sensors_values_from_day_from_timestamp_from_dst.format(
                day=current_day, greenhouse=greenhouse)
            log.info(query_day_max_timestamp)
            cursor_dst.execute(query_day_max_timestamp)

            day_max_id_timestamp = cursor_dst.fetchone()[0]

            #ti.xcom_push(key='max_id', value=max_id)
            if day_max_id_timestamp is None or day_max_id_timestamp == "":
                day_max_id_timestamp = '2021-01-01 00:00:00.000000'
            query_min_id = get_min_id_sql_query_from_src_timestamp.format(
                day=current_day, min_timestamp=day_max_id_timestamp)
            log.info(query_min_id)
            cursor_src.execute(query_min_id)

            min_id = cursor_src.fetchone()[0]

            #ti.xcom_push(key='day_max_id_timestamp', value=str(day_max_id_timestamp))
        except Exception as e:
            log.error(
                "Ошибка инициализации параметров: {err}".format(err=e))
            send_alert(e, error_head + sys._getframe().f_code.co_name)
            raise

        log.info("Последний используемый timestamp: %s" %
                 str(day_max_id_timestamp))

        log.info("Выполняется извлечение данных")
        try:

            if min_id is None or min_id == "":
                min_id = 0
            if max_id is None or max_id == "":
                max_id = sys.maxsize
            log.info("Последний используемый min_id: %s" %
                 str(min_id))
            query = get_data_sql_query_from_src_day_max_id.format(
                id=min_id, max_id=max_id, day=current_day)
            log.info(query)
            cursor_src.execute(query)
            log.info("Получен массив")
            rows = cursor_src.fetchall()
            log.info("Извлечено данных: {count}".format(count=len(rows)))
        except Exception as e:
            log.error(
                "Ошибка выполнения запроса к базе данных: {error}".format(error=e))
            send_alert(e, error_head + sys._getframe().f_code.co_name)
            raise
        try:
            file_path = "{path}extract/tmp_aranet_{date}.csv".format(
                path=path, date=str(current_day))
            log.info(
                "Сохраненние данных в формате csv в локальном хранилище в %s" % file_path)
            with open(file_path, 'w', newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows)
            csvfile.close()
        except Exception as s:
            log.error('Ошибка сохранения данных в csv: %s' % s)
            send_alert(e, error_head + sys._getframe().f_code.co_name)
            raise

    except Exception as e:
        log.error("Ошибка извлечения данных: {error}".format(error=e))
        send_alert(e, error_head + sys._getframe().f_code.co_name)
        raise

# [EXTRACT_FROM_SRC Трансформация данных.]


def read_extract_data(execution_date):
    import csv
    data = list()
    try:
        path_file = "{path}extract/tmp_aranet_{date}.csv".format(
            path=path, date=execution_date)
        log.info("Чтение из файла data: " + path_file)
        with open(path_file) as f:
            reader = csv.reader(f)
            data = list(reader)
            f.close()
        log.info("Длина массива data:" + str(len(data)))

    except Exception as e:
        log.error("Ошибка чтения данных из файла: {err}!".format(err=e))
        send_alert(e, error_head + sys._getframe().f_code.co_name)
    finally:
        return data


def transform(obj, dict_data, get_data_dst):
    current_list = list()
    list_difference = list()
    _type = str(type(obj))
    _type = _type[_type.find("__main__.")+9:_type.find("'>")]
    dict_data = set(dict_data)
    set_difference = list()
    log.info("Длина массива dict_data:" + str(len(dict_data)))
    log.info("Начало получения данных для справочника .")

    try:
        try:
            with dst.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(get_data_dst)
                    result = cur.fetchall()
                    log.info("Результат запроса:" + str(len(result)))
                    current_list = [row[0] for row in result]

        except Exception as e:
            log.warning(
                "Не удалось получить данные из базы данных назначения. Возможно, нет связи с базой или таблица еще пуста. {err}".format(err=e))
            send_alert(e, error_head + sys._getframe().f_code.co_name)
            current_list = None

        try:
            if current_list is None or len(current_list) == 0:
                list_difference = dict_data
            else:
                result_query_list = list()
                for item in current_list:
                    # print(item)
                    result_query_list.append(item)
                result_query_list = set(result_query_list)
                set_difference = dict_data - result_query_list
                log.info("Предварительный результат сравнения списков:" +
                         str(len(list_difference)))
                list_difference = list(set_difference)
        except Exception as e:
            log.error(
                "Ошибка сравнения справочников . Error: {err}", err=e)
            send_alert(e, error_head + sys._getframe().f_code.co_name)
            list_difference = None
        finally:
            log.info("Результат сравнения списков:" +
                     str(len(list_difference)))
            return list_difference

    except Exception as e:
        log.error(
            "Ошибка заполнения справочника {type_class}. Error: {err}", err=e, type_class=_type)
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def write_transform_data_to_json(obj, unit_list, execution_date):
    import json
    import os
    log.info("Создание каталога {path}transform/.".format(path=path))
    os.makedirs("{path}transform/".format(path=path), exist_ok=True)
    if unit_list is not None or len(unit_list) > 0:
        log.info("Длина массива unit_list:" + str(len(unit_list)))
        _type = str(type(obj))
        _type = _type[_type.find("__main__.")+9:_type.find("'>")]

        file_path = "{path}transform/tmp_{type_class}_{execution}.json".format(
            path=path, type_class=_type, execution=execution_date)
        log.info(
            "Сохраненние данных в формате json в локальном хранилище в {path}".format(path=file_path))

        try:
            with open(file_path, 'w') as outfile:
                json.dump(unit_list, outfile, cls=AranetEncoder)
                outfile.close()
        except Exception as e:
            log.error(
                "Ошибка сохранения справочника {type_class}. Error: {err}", err=e, type_class=_type)
            send_alert(e, error_head + sys._getframe().f_code.co_name)

    else:
        log.warning("Не данных для записи")


def transform_list_prod_unit(execution_date, **kwargs):
    #ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')
    execution_date = datetime.date(execution_date - timedelta(minutes=3))
    data = read_extract_data(execution_date)
    data_only_prod_unit = list()
    id_max = 0
    list_difference = list()
    unit_list = list()
    log.info("Начало получения данных для справочника производственных помещений.")
    log.info("Длина массива data:" + str(len(data)))
    try:
        for item in data:
            try:
                _p_unit = item[2]
                _p_unit = _p_unit[0:_p_unit.find("/")]
                data_only_prod_unit.append(_p_unit)
            except Exception as e:
                log.warning(
                    "Ошибка считывания данных из датасета. {err}".format(err=e))
            finally:
                continue
        # print(len(data_only_prod_unit))

        list_difference = transform(
            production_unit(1, "", 1), data_only_prod_unit, get_production_unit_sql_query_from_dst.format(greenhouse=greenhouse))
        # print(len(list_difference))

        try:
            if list_difference is not None and len(list_difference) > 0:
                for item in list_difference:
                    try:
                        id_max = id_max+1
                        unit_list.append(production_unit(
                            id_max, item, greenhouse))
                    except:
                        continue
            else:
                log.warn("Список пуст!")
        except Exception as e:
            log.error(
                "Ошибка создания списка json. Error: {err}", err=e)
            send_alert(e, error_head + sys._getframe().f_code.co_name)

        write_transform_data_to_json(production_unit(
            1, "", 1), unit_list, execution_date)
    except Exception as e:
        log.warning(
            "Не удалось выполнить трансформацию данных производственных помещений. {err}".format(err=e))
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def transform_list_topic(execution_date, **kwargs):
    ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')
    execution_date = datetime.date(execution_date - timedelta(minutes=3))
    data = read_extract_data(execution_date)
    data_only_topic = list()
    id_max = 0
    list_difference = list()
    unit_list = list()
    # print(len(data))
    log.info("Начало получения данных для справочника топиков.")

    try:
        for item in data:
            try:
                _topic = item[2]
                # _topic = _topic[_topic.find("/")+1: len(_topic)]
                # _topic = _topic[0:_topic.find("/")]
                data_only_topic.append(_topic)
            except Exception as e:
                log.warning(
                    "Ошибка считывания данных из датасета. {err}".format(err=e))
            finally:
                continue
        # print(len(data_only_topic))

        list_difference = transform(
            topic(1, ""), data_only_topic, get_topic_sql_query_from_dst.format(greenhouse=greenhouse))
        # print(len(list_difference))

        try:
            if list_difference is not None and len(list_difference) > 0:
                for item in list_difference:
                    try:
                        id_max = id_max+1
                        unit_list.append(topic(id_max, item))
                    except:
                        continue
            else:
                log.warn("Список пуст!")
        except Exception as e:
            log.error(
                "Ошибка создания списка json. Error: {err}", err=e)
            send_alert(e, error_head + sys._getframe().f_code.co_name)

        write_transform_data_to_json(topic(1, ""), unit_list, execution_date)
    except Exception as e:
        log.error(
            "Ошибка заполнения справочника топиков. Error: {err}", err=e)
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def transform_list_clients(execution_date, **kwargs):
    #ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')
    execution_date = datetime.date(execution_date - timedelta(minutes=3))
    data = read_extract_data(execution_date)
    id_max = 0
    list_difference = list()
    unit_list = list()
    print(len(data))
    data_only_client = list()
    log.info("Начало получения данных для справочника клиентов.")

    try:
        for item in data:
            try:
                client = item[4]
                data_only_client.append(client)
            except Exception as e:
                log.warning(
                    "Ошибка считывания данных из датасета. {err}".format(err=e))
            finally:
                continue
        print(len(data_only_client))

        list_difference = transform(
            clients(1, ""), data_only_client, get_client_sql_query_from_dst.format(greenhouse=greenhouse))
        print(len(list_difference))

        try:
            if list_difference is not None and len(list_difference) > 0:
                for item in list_difference:
                    try:
                        id_max = id_max+1
                        unit_list.append(clients(id_max, item))
                    except:
                        continue
            else:
                log.warn("Список пуст!")
        except Exception as e:
            log.error(
                "Ошибка создания списка json. Error: {err}", err=e)
            send_alert(e, error_head + sys._getframe().f_code.co_name)
        write_transform_data_to_json(clients(1, ""), unit_list, execution_date)
    except Exception as e:
        log.error(
            "Ошибка заполнения справочника клиентов. Error: {err}", err=e)
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def transform_list_bases(execution_date, **kwargs):
    #ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')
    execution_date = datetime.date(execution_date - timedelta(minutes=3))
    data = read_extract_data(execution_date)
    data_only_base = list()
    id_max = 0
    list_difference = list()
    unit_list = list()
    print(len(data))
    log.info("Начало получения данных для справочника баз.")

    try:
        for item in data:
            try:
                _base = item[4]
                data_only_base.append(_base)
            except Exception as e:
                log.warning(
                    "Ошибка считывания данных из датасета. {err}".format(err=e))
                send_alert(e, error_head + sys._getframe().f_code.co_name)
            finally:
                continue
        print(len(data_only_base))

        list_difference = transform(
            bases(1, "", 1), data_only_base, get_base_sql_query_from_dst.format(greenhouse=greenhouse))
        print("Итоговый список для записи" + str(len(list_difference)))
        try:
            if list_difference is not None and len(list_difference) > 0:
                with dst.get_conn() as conn:
                    with conn.cursor() as cur:
                        for item in list_difference:
                            try:
                                id_max = id_max+1
                                id_prod_unit = -1
                                _prod_unit = "Отсутствует"
                                for item_data in data:
                                    if item == item_data[4]:
                                        _prod_unit = item_data[2]
                                        _prod_unit = _prod_unit[0:_prod_unit.find(
                                            "/")]
                                query = get_id_production_unit_from_name_sql_query_from_dst.format(
                                    name=_prod_unit, greenhouse=greenhouse)
                                log.info(query)
                                cur.execute(query)
                                result = cur.fetchone()
                                print("id производственного помещения" +
                                      str(result[0]))
                                id_prod_unit = int(result[0])

                                unit_list.append(
                                    bases(id_max, item, id_prod_unit))
                            except Exception as e:
                                log.warning(
                                    "Ошибка итерации. Error: {err}", err=e)
                                continue
            else:
                log.warn("Список пуст!")
        except Exception as e:
            log.error(
                "Ошибка создания списка json. Error: {err}", err=e)
            send_alert(e, error_head + sys._getframe().f_code.co_name)

        write_transform_data_to_json(
            bases(1, "", 1), unit_list, execution_date)
    except Exception as e:
        log.error("Ошибка заполнения справочника баз. Error: {err}", err=e)
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def transform_list_sensors(execution_date, **kwargs):
    #ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')
    execution_date = datetime.date(execution_date - timedelta(minutes=3))
    data = read_extract_data(execution_date)
    id_max = 0
    list_difference = list()
    unit_list = list()
    # print(len(data))
    data_only_sensors = list()
    log.info("Начало получения данных для справочника датчиков.")

    try:
        for item in data:
            try:
                _sensor = item[2]
                _sensor = _sensor[_sensor.find(
                    "sensors/")+8:_sensor.find("/json")]
                data_only_sensors.append(_sensor)
            except Exception as e:
                log.warning(
                    "Ошибка считывания данных из датасета. {err}".format(err=e))
            finally:
                continue
        print("Результат анализа датасета: " + str(len(data_only_sensors)))

        list_difference = transform(
            sensor(1, "", 1), data_only_sensors, get_sensor_sql_query_from_dst.format(greenhouse=greenhouse))
        print(len(list_difference))

        try:
            if list_difference is not None and len(list_difference) > 0:
                with dst.get_conn() as conn:
                    with conn.cursor() as cur:
                        for item in list_difference:
                            try:
                                id_max = id_max+1
                                id_base = -1
                                _base = "Отсутствует"
                                for item_data in data:
                                    _sensor = item_data[2]
                                    _sensor = _sensor[_sensor.find(
                                        "sensors/")+8:_sensor.find("/json")]
                                    if item == _sensor:
                                        _base = item_data[4]

                                # for item in data:
                                    # if isinstance(item, bases) and item.base == _base:
                                    #   id_base = item.id_base
                                cur.execute(
                                    get_id_base_from_name_sql_query_from_dst.format(name=_base, greenhouse=greenhouse))
                                result = cur.fetchone()
                                print(result[0])
                                id_base = int(result[0])

                                unit_list.append(sensor(id_max, item, id_base))

                            except Exception as e:
                                log.warning(
                                    "Ошибка итерации. Error: {err}", err=e)
                                continue
            else:
                log.warn("Список пуст!")
        except Exception as e:
            log.error(
                "Ошибка создания списка json. Error: {err}", err=e)
            send_alert(e, error_head + sys._getframe().f_code.co_name)
        write_transform_data_to_json(
            sensor(1, "", 1), unit_list, execution_date)
    except Exception as e:
        log.error(
            "Ошибка заполнения справочника датчиков. Error: {err}", err=e)
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def transform_list_sensors_value(execution_date, **kwargs):
    import ast
    #ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')
    execution_date = datetime.date(execution_date - timedelta(minutes=3))
    data = read_extract_data(execution_date)
    data_only_value = list()
    id_max = 0
    # print(len(data))
    log.info("Начало получения данных для таблицы измеренеий.")
    list_clients = None
    list_sensors = None
    list_topics = None
    try:
        log.info("Загрузка существующих справочников.")
        with dst.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(get_client_sql_query_from_dst.format(
                    greenhouse=greenhouse))
                list_clients = cur.fetchall()
                log.info("Загрузка clients успешно завершена. Len: " +
                         str(len(list_clients)))

                cur.execute(get_topic_sql_query_from_dst.format(
                    greenhouse=greenhouse))
                list_topics = cur.fetchall()
                log.info("Загрузка topics успешно завершена. Len: " +
                         str(len(list_topics)))

                cur.execute(get_sensor_sql_query_from_dst.format(
                    greenhouse=greenhouse))
                list_sensors = cur.fetchall()
                log.info("Загрузка sensors успешно завершена. Len: " +
                         str(len(list_sensors)))

        log.info("Загрузка успешно завершена.")
    except Exception as e:
        log.error("Ошибка загрузки справочников: " + str(e))
        send_alert(e, error_head + sys._getframe().f_code.co_name)
    # raise Exception("Необходимо создать справочники!" Len: )

    try:
        with dst.get_conn() as conn:
            with conn.cursor() as cur:
                for item in data:
                    try:
                        id_max = id_max + 1
                        val = sensor_value(id_max)
                        _message = item[1]
                        try:
                            _value = ast.literal_eval(_message)
                        except:
                            log.warning(
                                "Ошибка преобразования массива в словарь" + str(e) + str(_message))
                            send_alert(e, error_head +
                                       sys._getframe().f_code.co_name)
                        _client = item[4]
                        # print("_client: " + _client)
                        val.id_client = -1
                        try:
                            for client in list_clients:
                                if client[0] == _client:
                                    val.id_client = int(client[1])
                                    break
                            if val.id_client == -1:
                                cur.execute(
                                    get_id_client_from_name_sql_query_from_dst.format(name=_client, greenhouse=greenhouse))
                                result = cur.fetchone()
                                val.id_client = int(result[0])
                            # print("id_client: " + str(val.id_client))
                        except Exception as e:
                            log.warning("id клиента " +
                                        str(_client) + "  не найден!" + str(e))
                            send_alert(e, error_head +
                                       sys._getframe().f_code.co_name)
                        finally:
                            if val.id_client == -1:
                                val.id_client = None

                        _topic = item[2]
                        # print("_topic: " + _topic)
                        val.id_topic = -1
                        try:
                            for topic in list_topics:
                                if topic[0] == _topic:
                                    val.id_topic = int(topic[1])
                                    break
                            if val.id_topic == -1:
                                log.info("Топик: " + str(_topic))
                                cur.execute(
                                    get_id_topic_from_name_sql_query_from_dst.format(name=_topic, greenhouse=greenhouse))
                                result = cur.fetchone()
                                val.id_topic = int(result[0])
                            # print("id_topic: " + str(val.id_topic))
                        except Exception as e:
                            log.warning("id топика " + str(_topic) +
                                        "  не найден!" + str(e))
                            send_alert(e, error_head +
                                       sys._getframe().f_code.co_name)
                        finally:
                            if val.id_topic == -1:
                                val.id_topic = None

                        _sensor = item[2]
                        _sensor = _sensor[_sensor.find(
                            "sensors/")+8:_sensor.find("/json")]
                        # print("_sensor: " + _sensor)
                        val.id_sensor = -1
                        try:
                            for sensor in list_sensors:
                                if sensor[0] == _sensor:
                                    val.id_sensor = int(sensor[1])
                                    break
                            if val.id_sensor == -1:
                                cur.execute(
                                    get_id_sensor_from_name_sql_query_from_dst.format(name=_sensor, greenhouse=greenhouse))
                                result = cur.fetchone()
                                val.id_sensor = int(result[0])
                            # print("id_sensor: " + str(val.id_sensor))
                        except Exception as e:
                            log.warning("id сенсора " +
                                        str(_sensor) + " не найден!" + str(e))
                            send_alert(e, error_head +
                                       sys._getframe().f_code.co_name)
                        finally:
                            if val.id_sensor == -1:
                                val.id_sensor = None

                        val.timestamp = item[5]
                        key = 'qos'
                        if key in _value:
                            val.qos = _value[key]
                            # print(val.qos)
                        key = 'weight'
                        if key in _value:
                            val.weight = _value[key]
                            # print(val.weight)
                        key = 'weightraw'
                        if key in _value:
                            val.weightraw = _value[key]
                            # print(val.weightraw)
                        key = 'humidity'
                        if key in _value:
                            val.humidity = _value[key]
                            # print(val.humidity)
                        key = 'temperature'
                        if key in _value:
                            val.temperature = _value[key]
                            # print(val.temperature)
                        key = 'bec'
                        if key in _value:
                            val.bec = _value[key]
                            # print(val.bec)
                        key = 'dp'
                        if key in _value:
                            val.dp = _value[key]
                            # print(val.dp)
                        key = 'pec'
                        if key in _value:
                            val.pec = _value[key]
                            # print(val.pec)
                        key = 'vwc'
                        if key in _value:
                            val.vwc = _value[key]
                            # print(val.vwc)
                        key = 'voltage'
                        if key in _value:
                            val.voltage = _value[key]
                            # print(val.voltage)
                        key = 'current'
                        if key in _value:
                            val.current = _value[key]
                            # print(val.current)
                        key = 'ppfd'
                        if key in _value:
                            val.ppfd = _value[key]
                            # print(val.ppfd)
                        key = 'atmosphericpressure'
                        if key in _value:
                            val.atmosphericpressure = _value[key]
                            # print(val.atmosphericpressure)
                        key = 'co2'
                        if key in _value:
                            val.co2 = _value[key]
                            # print(val.co2)
                        key = 'co2Abc'
                        if key in _value:
                            val.co2Abc = _value[key]
                            # print(val.co2Abc)
                        key = 'rssi'
                        if key in _value:
                            val.rssi = _value[key]
                            # print(val.rssi)
                        key = 'time'
                        if key in _value:
                            val.time = _value[key]
                            # print(val.time)
                        key = 'battery'
                        if key in _value:
                            val.battery = _value[key]
                            # print(val.battery)

                        # data_only_value.append(val)
                    except Exception as e:
                        log.warning(
                            "Ошибка считывания данных из датасета. {err}".format(err=e) + str(item))
                        send_alert(str(e) + str(item), error_head +
                                   sys._getframe().f_code.co_name)
                        # continue
                    finally:
                        data_only_value.append(val)

        log.info("Всего данных с датчиков: " + str(len(data_only_value)))

        write_transform_data_to_json(
            sensor_value(-1), data_only_value, execution_date)

    except Exception as e:
        log.error(
            "Ошибка заполнения таблицы измерений. Error: {err}".format(err=e))
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def finnaly_extract(execution_date, **kwargs):
    import os
    ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')
    execution_date = datetime.date(execution_date - timedelta(minutes=3))
    try:
        log.info("Создание каталога {path}extract/old/{execution_date}/.".format(
            path=path, execution_date=execution_date))
        os.makedirs("{path}extract/old/{execution_date}/".format(path=path,
                    execution_date=execution_date), exist_ok=True)
        log.info("Перенос файла в каталог {path}extract/old/{execution_date}/.".format(
            path=path, execution_date=execution_date))
        if os.path.exists('{path}extract/tmp_aranet_{day}.csv'.format(path=path, day=execution_date)):
            os.replace("{path}extract/tmp_aranet_{day}.csv".format(path=path, day=execution_date),
                       "{path}extract/old/{execution_date}/tmp_aranet_{day}.csv".format(path=path, day=execution_date, execution_date=execution_date))
    except Exception as e:
        log.error("Не удалось перенести файл! {err}".format(err=e))
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def finnaly_transform(execution_date, **kwargs):
    import os
    #ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')
    execution_date = datetime.date(execution_date - timedelta(minutes=3))
    try:
        log.info("Создание каталога {path}transform/old/{execution_date}/.".format(
            path=path, execution_date=execution_date))
        os.makedirs("{path}transform/old/{execution_date}/".format(path=path,
                    execution_date=execution_date), exist_ok=True)
        log.info(
            "Перенос файла в каталог {path}transform/old/.".format(path=path))
        if os.path.exists('{path}transform/tmp_aranet_class.sensor_value_{day}.json'.format(path=path, day=execution_date)):
            os.replace("{path}transform/tmp_aranet_class.sensor_value_{day}.json".format(path=path, day=execution_date),
                       "{path}transform/old/{execution_date}/tmp_aranet_class.sensor_value_{day}.json".format(path=path, day=execution_date, execution_date=execution_date))
        if os.path.exists('{path}transform/tmp_aranet_class.sensor_{day}.json'.format(path=path, day=execution_date)):
            os.replace("{path}transform/tmp_aranet_class.sensor_{day}.json".format(path=path, day=execution_date),
                       "{path}transform/old/{execution_date}/tmp_aranet_class.sensor_{day}.json".format(path=path, day=execution_date, execution_date=execution_date))
        if os.path.exists('{path}transform/tmp_aranet_class.bases_{day}.json'.format(path=path, day=execution_date)):
            os.replace("{path}transform/tmp_aranet_class.bases_{day}.json".format(path=path, day=execution_date),
                       "{path}transform/old/{execution_date}/tmp_aranet_class.bases_{day}.json".format(path=path, day=execution_date, execution_date=execution_date))
        if os.path.exists('{path}transform/tmp_aranet_class.topic_{day}.json'.format(path=path, day=execution_date)):
            os.replace("{path}transform/tmp_aranet_class.topic_{day}.json".format(path=path, day=execution_date),
                       "{path}transform/old/{execution_date}/tmp_aranet_class.topic_{day}.json".format(path=path, day=execution_date, execution_date=execution_date))
        if os.path.exists('{path}transform/tmp_aranet_class.production_unit_{day}.json'.format(path=path, day=execution_date)):
            os.replace("{path}transform/tmp_aranet_class.production_unit_{day}.json".format(path=path, day=execution_date),
                       "{path}transform/old/{execution_date}/tmp_aranet_class.production_unit_{day}.json".format(path=path, day=execution_date, execution_date=execution_date))
        if os.path.exists('{path}transform/tmp_aranet_class.clients_{day}.json'.format(path=path, day=execution_date)):
            os.replace("{path}transform/tmp_aranet_class.clients_{day}.json".format(path=path, day=execution_date),
                       "{path}transform/old/{execution_date}/tmp_aranet_class.clients_{day}.json".format(path=path, day=execution_date, execution_date=execution_date))
        clear_temp_file(execution_date)
    except Exception as e:
        log.error("Не удалось перенести файл! {err}".format(err=e))
        send_alert(e, error_head + sys._getframe().f_code.co_name)

def clear_temp_file(exec_date):
    import shutil
    try:
        log.info("Чистка каталогов после переноса данных.")
        shutil.rmtree(
            "{path}extract/old/{date}/".format(path=path, date=exec_date))
        shutil.rmtree(
            "{path}transform/old/{date}/".format(path=path, date=exec_date))
    except Exception as e:
        str_err = "Ошибка удаления каталога: " + str(e)
        log.error(str_err)
        send_alert(str_err, error_head + sys._getframe().f_code.co_name)
        raise

def read_transform_data(obj, execution_day):
    import json
    list_unit = list()
    execution_day = datetime.date(execution_day - timedelta(minutes=3))
    _type = str(type(obj))
    _type = _type[_type.find("__main__.")+9:_type.find("'>")]

    file_path = "{path}transform/tmp_{type_class}_{execution_day}.json".format(
        path=path, type_class=_type, execution_day=execution_day)
    log.info("Открытие файла: " + file_path)
    try:
        with open(file_path) as f:
            try:
                list_unit = json.loads(
                    f.read(), object_hook=type(obj).to_object)
            except Exception as e:
                log.error("Ошибка чтения файла: {err}!".format(err=e))
                send_alert(e, error_head + sys._getframe().f_code.co_name)
        log.info("Длина массива data:" + str(len(list_unit)))

    except Exception as e:
        log.error("Ошибка чтения данных из файла: {err}!".format(err=e))
        send_alert(e, error_head + sys._getframe().f_code.co_name)
    finally:
        return list_unit


def load_list_prod_unit(execution_date, **kwargs):
    list_unit = list()
    #ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')

    try:
        list_unit = read_transform_data(
            production_unit(1, "", 1), execution_date)
        log.info("Отправка данных в базу назначения.")
        if len(list_unit) > 0:
            with dst.get_conn() as conn:
                with conn.cursor() as cur:
                    query = insert_prod_unit_sql_query
                    for item in list_unit:
                        if isinstance(item, production_unit):
                            cur.execute(get_id_production_unit_from_name_sql_query_from_dst.format(
                                name=item.production_unit, greenhouse=greenhouse))
                            result = cur.fetchone()
                            if result is None or len(result) == 0:
                                query += "('{production_unit}', {greenhouse}), ".format(
                                    production_unit=item.production_unit, greenhouse=item.greenhouse)
                    query = query[:-2]
                    query += ";"
                    cur.execute(query)
                    conn.commit()

            log.info("Данные в справочник производственных помещений отправлены.")

        else:
            log.info("Нет новых данных.")
    except Exception as e:
        send_alert(e, error_head + sys._getframe().f_code.co_name)
        log.error(
            "Ошибка загрузки справочника производственных помещений. Error: {err}", err=e)


def load_list_bases(execution_date, **kwargs):
    list_unit = list()
    ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')

    try:
        list_unit = read_transform_data(bases(1, "", 1), execution_date)
        log.info("Отправка данных в базу назначения.")
        if len(list_unit) > 0:
            with dst.get_conn() as conn:
                with conn.cursor() as cur:
                    query = insert_base_sql_query
                    for item in list_unit:
                        if isinstance(item, bases):
                            cur.execute(get_id_base_from_name_sql_query_from_dst.format(
                                name=item.base, greenhouse=greenhouse))
                            result = cur.fetchone()
                            if result is None or len(result) == 0:
                                query += "('{base}', {id_production_unit}), ".format(
                                    base=item.base, id_production_unit=item.id_production_unit)
                    query = query[:-2]
                    query += ";"
                    cur.execute(query)
                    conn.commit()
            log.info("Данные в справочник базы отправлены.")
        else:
            log.info("Нет новых данных.")
    except Exception as e:
        send_alert(e, error_head + sys._getframe().f_code.co_name)
        log.error(
            "Ошибка загрузки справочника баз. Error: {err}", err=e)


def load_list_sensors(execution_date, **kwargs):
    list_unit = list()
    #ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')

    try:
        list_unit = read_transform_data(sensor(1, "", 1), execution_date)
        log.info("Отправка данных в базу назначения.")
        if len(list_unit) > 0:
            with dst.get_conn() as conn:
                with conn.cursor() as cur:
                    query = insert_sensor_sql_query
                    for item in list_unit:
                        if isinstance(item, sensor):
                            cur.execute(get_id_sensor_from_name_sql_query_from_dst.format(
                                name=item.sensor, greenhouse=greenhouse))
                            result = cur.fetchone()
                            if result is None or len(result) == 0:
                                query += "('{sensor}', {id_base}), ".format(
                                    sensor=item.sensor, id_base=item.id_base)
                    query = query[:-2]
                    query += ";"
                    cur.execute(query)
                    conn.commit()
                    # print(item)
            log.info("Данные в справочник сенсоров отправлены.")

            """if dst.get_conn().commit():
                log.log("Данные зафиксированы")
            else:
                log.log("Данные не зафиксированы")"""
        else:
            log.info("Нет новых данных.")
    except Exception as e:
        send_alert(e, error_head + sys._getframe().f_code.co_name)
        log.error(
            "Ошибка загрузки справочника сенсоров. Error: {err}", err=e)


def load_list_topic(execution_date, **kwargs):
    list_unit = list()
    ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')

    try:
        list_unit = read_transform_data(topic(1, ""), execution_date)
        log.info("Отправка данных в базу назначения.")
        if len(list_unit) > 0:
            with dst.get_conn() as conn:
                query = insert_topic_sql_query
                with conn.cursor() as cur:
                    for item in list_unit:
                        if isinstance(item, topic):
                            cur.execute(
                                get_id_topic_from_name_sql_query_from_dst.format(name=item.topic, greenhouse=greenhouse))
                            result = cur.fetchone()
                            if result is None or len(result) == 0:
                                # print(insert_topic_sql_query % (item.topic))
                                query += "('{topic}', {greenhouse}), ".format(
                                    topic=item.topic, greenhouse=greenhouse)
                    query = query[:-2]
                    query += ";"
                    cur.execute(query)
                    conn.commit()
                # print(item.topic)
            log.info("Данные в справочник топиков отправлены.")
        else:
            log.info("Нет новых данных.")
    except Exception as e:
        send_alert(e, error_head + sys._getframe().f_code.co_name)
        log.error(
            "Ошибка загрузки справочника топиков. Error: {err}", err=e)


def load_list_clients(execution_date, **kwargs):
    list_unit = list()
    ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')

    try:
        list_unit = read_transform_data(clients(1, ""), execution_date)
        log.info("Отправка данных в базу назначения.")
        if len(list_unit) > 0:
            with dst.get_conn() as conn:
                with conn.cursor() as cur:
                    query = insert_client_sql_query
                    for item in list_unit:
                        if isinstance(item, clients):
                            cur.execute(get_id_client_from_name_sql_query_from_dst.format(
                                name=item.client, greenhouse=greenhouse))
                            result = cur.fetchone()
                            if result is None or len(result) == 0:
                                query += "('{client}', {greenhouse}), ".format(
                                    client=item.client, greenhouse=greenhouse)
                    query = query[:-2]
                    query += ";"
                    cur.execute(query)
                    conn.commit()
                    # print(item)
            log.info("Данные в справочник клиентов отправлены.")
        else:
            log.info("Нет новых данных.")
    except Exception as e:
        log.error(
            "Ошибка загрузки справочника клиентов. Error: {err}", err=e)
        send_alert(e, error_head + sys._getframe().f_code.co_name)


def load_list_sensors_value(execution_date, **kwargs):
    list_unit = list()
    ti = kwargs['ti']
    # ti.xcom_pull(key='execution_date')

    try:
        list_unit = read_transform_data(sensor_value(-1), execution_date)
        log.info("Отправка данных в базу назначения.")
        if len(list_unit) > 0:
            with dst.get_conn() as conn:
                with conn.cursor() as cur:
                    #query = insert_sensor_value
                    for item in list_unit:
                        if isinstance(item, sensor_value):
                            # query = insert_sensor_value % (item.qos, item.timestamp, "{0:0.3f}".format(item.weight), item.id_topic, item.id_client, "{0:0.3f}".format(item.weightraw),  "{0:0.1f}".format(item.humidity), "{0:0.3f}".format(item.temperature), "{0:0.5f}".format(item.bec), "{0:0.6f}".format(item.dp),
                            # str(item.pec), str(item.vwc), str(item.voltage), str(item.current), str(item.ppfd), str(item.atmosphericpressure), str(item.co2), str(item.co2Abc), str(item.rssi), str(item.time), str(item.battery), str(item.id_sensor)))
                            # query = insert_sensor_value % (item.qos, item.timestamp, "{0:0.3f}".format(item.weight), item.id_topic, item.id_client, "{0:0.3f}".format(item.weightraw),  "{0:0.1f}".format(item.humidity), "{0:0.3f}".format(item.temperature), "{0:0.5f}".format(item.bec), "{0:0.6f}".format(item.dp),
                            #                               "{0:0.5f}".format(item.pec), "{0:0.3f}".format(item.vwc), "{0:0.3f}".format(item.voltage), "{0:0.6f}".format(item.current), "{0:0.1f}".format(item.ppfd), item.atmosphericpressure, item.co2, "{0:0.6f}".format(item.co2Abc), item.rssi, item.time, "{0:0.1f}".format(item.battery), item.id_sensor)
                            query = insert_sensor_value
                            query += """({qos}, '{timestamp}', {weight}, {id_topic}, {id_client}, {weightraw},  {humidity}, {temperature}, {bec}, {dp}, {pec}, {vwc}, {voltage}, {current}, {ppfd}, {atmosphericpressure}, {co2}, {co2Abc}, {rssi}, {time}, {battery}, {id_sensor}), """.format(qos=item.qos, timestamp=item.timestamp, weight=item.weight, id_topic=item.id_topic, id_client=item.id_client, weightraw=item.weightraw,  humidity=item.humidity, temperature=item.temperature, bec=item.bec, dp=item.dp,
                                                                                                                                                                                                                                                                                                  pec=item.pec, vwc=item.vwc, voltage=item.voltage, current=item.current, ppfd=item.ppfd, atmosphericpressure=item.atmosphericpressure, co2=item.co2, co2Abc=item.co2Abc, rssi=item.rssi, time=item.time, battery=item.battery, id_sensor=item.id_sensor)

                            query = query.replace('None', 'Null')
                            query = query[:-2]
                            query += ";"
                            cur.execute(query)
                            conn.commit()
            log.info("Данные датчиков отправлены.")
        else:
            log.info("Нет новых данных.")
    except Exception as e:
        log.error(
            "Ошибка загрузки данных датчиков. Error: {err}", err=e)
        send_alert(e, error_head + sys._getframe().f_code.co_name)

# [CLOSE_CONNECT_DST Закрытие соединения с базой назначения.]


def close_connection_dst(**kwargs):
    # ti = kwargs['ti']
    # dst_cursor = ti.xcom_pull(key='dst')
    cursor_dst.close
    # current_max_id = ti.xcom_pull(key='max_id')
    """if current_max_id is not None:
        write_max_id(current_max_id)"""
    log.info("Соединение с базой завершено.")


# [CLOSE_CONNECT_SRC Закрытие соединения с базой источником.]

def close_connection_src(**kwargs):
    # ti = kwargs['ti']
    # src_cursor = ti.xcom_pull(key='cursor')
    # log.info("Фиксация id выполнена.")
    cursor_src.close
    log.info("Соединение с базой завершено.")


with DAG(
    dag_id=_dag_id,
    schedule_interval=timedelta(minutes=1440),  # '0 1 * * *',
    start_date=_start_date,
    catchup=True,
    tags=['Aranet'],


) as dag:

    task_connect_to_postgres = PythonOperator(
        task_id="connect_to_postgres",
        python_callable=connect_to_psql,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_connect_to_postgres_src = PythonOperator(
        task_id="connect_to_postgres_src",
        python_callable=connect_to_psql_src,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_connect_to_postgres_dst = PythonOperator(
        task_id="connect_to_postgres_dst",
        python_callable=connect_to_postgres_dst,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_extract_all_from_src = PythonOperator(
        task_id="extract_all_from_src",
        python_callable=extract_all_from_src,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_transform_data_prod_unit = PythonOperator(
        task_id="transform_list_prod_unit",
        python_callable=transform_list_prod_unit,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_transform_data_base = PythonOperator(
        task_id="transform_list_bases",
        python_callable=transform_list_bases,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_transform_data_sensor = PythonOperator(
        task_id="transform_list_sensors",
        python_callable=transform_list_sensors,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_transform_data_value_sensor = PythonOperator(
        task_id="transform_list_sensors_value",
        python_callable=transform_list_sensors_value,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_transform_data_topic = PythonOperator(
        task_id="transform_list_topic",
        python_callable=transform_list_topic,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_transform_data_clients = PythonOperator(
        task_id="transform_list_clients",
        python_callable=transform_list_clients,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_load_data_sensors_value = PythonOperator(
        task_id="load_list_sensors_value",
        python_callable=load_list_sensors_value,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_load_data_base = PythonOperator(
        task_id="load_list_bases",
        python_callable=load_list_bases,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_load_data_sensor = PythonOperator(
        task_id="load_list_sensors",
        python_callable=load_list_sensors,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_load_data_prod_unit = PythonOperator(
        task_id="load_list_prod_unit",
        python_callable=load_list_prod_unit,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_load_data_topic = PythonOperator(
        task_id="load_list_topic",
        python_callable=load_list_topic,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_load_data_client = PythonOperator(
        task_id="load_list_clients",
        python_callable=load_list_clients,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_close_connection_dst = PythonOperator(
        task_id="close_connection_dst",
        python_callable=close_connection_dst,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_close_connection_src = PythonOperator(
        task_id="close_connection_src",
        python_callable=close_connection_src,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_finnaly_transform = PythonOperator(
        task_id="finnaly_transform",
        python_callable=finnaly_transform,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task_finnaly_extract = PythonOperator(
        task_id="finnaly_extract",
        python_callable=finnaly_extract,
        provide_context=True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

task_connect_to_postgres >> task_connect_to_postgres_src >> task_extract_all_from_src >> task_connect_to_postgres_dst
task_extract_all_from_src >> task_close_connection_src
task_connect_to_postgres_dst >> task_transform_data_prod_unit >> task_load_data_prod_unit >> task_transform_data_base >> task_load_data_base >> task_transform_data_sensor >> task_load_data_sensor >> task_transform_data_value_sensor
task_connect_to_postgres_dst >> task_transform_data_clients >> task_load_data_client >> task_transform_data_value_sensor
task_connect_to_postgres_dst >> task_transform_data_topic >> task_load_data_topic >> task_transform_data_value_sensor
task_transform_data_value_sensor >> task_load_data_sensors_value >> task_close_connection_dst
task_close_connection_dst >> task_finnaly_transform
task_close_connection_dst >> task_finnaly_extract
