#from os import curdir
#from xmlrpc.client import _DateTimeComparable, DateTime
import re
from aranet_sql import *
import datetime
from datetime import datetime, timedelta
import logging
import sys
from alert import *
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task
from clickhouse_aranet_dataset_class import aranet, atmosphericpressure, bec, current, humidity, ppfd, temperature, voltage, weight
from clickhouse_sql import *
from clickhouse_driver import Client
#from clickhouse import *

# Заголовок отправляемых сообщений об ошибках для идентификации проблемного дага.
error_head = "aranet_etl_to_clickhouse: "
# Хук БД источника
src = PostgresHook(postgres_conn_id='postgres_dwh')
# Подключение к ClickHouse
dst = Client(host='dwh.agrom.local', database='aranet',
             user='airflow', password='UwRskkh6')
#
_dag_id = 'aranet_clickhouse'
# Дата старта синхронизации. Логическая переменная, которая запускает синхронизацию с указанной даты.
_start_date = datetime(2021, 12, 17)
# Переменная, указывающая на уникальный ключ тепличного комплекса, который подставляется в запросы к бд
#greenhouse = 1
# Название тепличного комплекса, который подставляется в запросы к бд
#name_greenhouse = "ООО ТК \"Сосногорский\""
# путь к каталогу хранения промежутточных файлов
path = "/var/share/tmp/clickhouse/aranet/"
# Дельта времени между синхронизацией данных
minute_duration = 2

log = logging.getLogger(__name__)


def start(execution_date, **kwargs):
    log.info("Старт процесса переноса данных.")
    create_struct_folder(execution_date)


def create_struct_folder(exec_date):
    import os
    #exec_date = exec_date - timedelta(days=1)
    exec_date = datetime.date(exec_date - timedelta(minutes=minute_duration))
    try:
        log.info(
            "Создание каталога {path}extract/{date}/ и {path}transform/{date}/.".format(path=path, date=exec_date))
        os.makedirs("{path}/".format(path=path), exist_ok=True)
        os.makedirs("{path}extract/{date}/".format(path=path,
                    date=exec_date), exist_ok=True)
        os.makedirs("{path}transform/{date}/".format(path=path,
                    date=exec_date), exist_ok=True)
    except Exception as e:
        str_err = "Ошибка создания каталога: " + str(e)
        log.error(str_err)
        send_alert(str_err, error_head + sys._getframe().f_code.co_name)
        raise


def finnaly(execution_date, **kwargs):
    execution_date = datetime.date(
        execution_date - timedelta(minutes=minute_duration))
    clear_temp_file(execution_date)


def clear_temp_file(exec_date):
    import shutil
    try:
        log.info("Чистка каталогов после переноса данных.")
        shutil.rmtree(
            "{path}extract/{date}/".format(path=path, date=exec_date))
        shutil.rmtree(
            "{path}transform/{date}/".format(path=path, date=exec_date))
    except Exception as e:
        str_err = "Ошибка удаления каталога: " + str(e)
        log.error(str_err)
        send_alert(str_err, error_head + sys._getframe().f_code.co_name)
        raise

# EXTRACT


def extract(exec_date, obj, get_data_sql, get_last_id_sql):
    import csv
    last_id = 0
    result = list()
    exec_date = datetime.date(exec_date - timedelta(minutes=minute_duration))
    #exec_date = exec_date - timedelta(days=1)
    log.info("Старт выгрузки данных")
    try:
        with dst:
            log.info("Получение последнего id")
            # , name_greenhouse = name_greenhouse))
            result = dst.execute(get_last_id_sql.format(day=str(exec_date)))
            result = result[0]
            print("Результ: " + str(result[0]))
            if result is not None and result[0] > 0:
                last_id = int(result[0])
            log.info("Последний id: " + str(last_id))
    except Exception as e:
        str_err = "Ошибка получения посленего id:" + str(e)
        log.error(str_err)
        send_alert(str_err, error_head + sys._getframe().f_code.co_name)
        raise

    try:
        with src.get_conn() as conn:
            with conn.cursor() as cur:
                log.info("Получение данных")
                query = get_data_sql.format(last_id=last_id, day=str(
                    exec_date))  # greenhouse = greenhouse,
                print(query)
                cur.execute(query)
                result = cur.fetchall()
                log.info("Извлечено записей: " + str(len(result)))
    except Exception as e:
        str_err = "Ошибка получения данных:" + str(e)
        log.error(str_err)
        send_alert(str_err, error_head + sys._getframe().f_code.co_name)
        raise

    try:
        _type = str(type(obj))
        _type = _type[_type.find("__main__.")+9:_type.find("'>")]
        file_path = "{path}extract/{date}/tmp_aranet_{class_obj}.csv".format(
            path=path, date=exec_date, class_obj=_type)
        log.info(
            "Сохраненние данных в формате csv в локальном хранилище в %s" % file_path)
        with open(file_path, 'w', newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(result)
        csvfile.close()
        log.info("Запись в файл выполнена.")
    except Exception as e:
        str_err = "Ошибка сохранения данных в csv:" + str(e)
        log.error(str_err)
        send_alert(str_err, error_head + sys._getframe().f_code.co_name)
        raise


def extract_aranet(execution_date, **kwargs):
    log.info("Старт извлечения датасета Aranet.")
    extract(execution_date, aranet(), get_aranet, get_last_id_aranet)
    log.info("Извлечение датасета Aranet завершено.")


def extract_humidity(execution_date, **kwargs):
    log.info("Старт извлечения датасета Humidity.")
    extract(execution_date, humidity(), get_humidity, get_last_id_humidity)
    log.info("Извлечение датасета Humidity завершено.")


def extract_temperature(execution_date, **kwargs):
    log.info("Старт извлечения датасета Temperature.")
    extract(execution_date, temperature(),
            get_temperature, get_last_id_temperature)
    log.info("Извлечение датасета Temperature завершено.")


def extract_atmosphericpressure(execution_date, **kwargs):
    log.info("Старт извлечения датасета Atmosphericpressure.")
    extract(execution_date, atmosphericpressure(),
            get_atmosphericpressure, get_last_id_atmosphericpressure)
    log.info("Извлечение датасета Atmosphericpressure завершено.")


def extract_ppfd(execution_date, **kwargs):
    log.info("Старт извлечения датасета Ppfd.")
    extract(execution_date, ppfd(), get_ppfd, get_last_id_ppfd)
    log.info("Извлечение датасета Ppfd завершено.")


def extract_voltage(execution_date, **kwargs):
    log.info("Старт извлечения датасета Voltage.")
    extract(execution_date, voltage(), get_voltage, get_last_id_voltage)
    log.info("Извлечение датасета Voltage завершено.")


def extract_current(execution_date, **kwargs):
    log.info("Старт извлечения датасета Current.")
    extract(execution_date, current(), get_current, get_last_id_current)
    log.info("Извлечение датасета Current завершено.")


def extract_bec(execution_date, **kwargs):
    log.info("Старт извлечения датасета Bec.")
    extract(execution_date, bec(), get_bec, get_last_id_bec)
    log.info("Извлечение датасета Bec завершено.")


def extract_weight(execution_date, **kwargs):
    log.info("Старт извлечения датасета Weight.")
    extract(execution_date, weight(), get_weight, get_last_id_weight)
    log.info("Извлечение датасета Weight завершено.")

# TRANSFORM


def transform(exec_date, obj):
    log.info("Трансформация не требуется")


def transform_aranet(execution_date, **kwargs):
    log.info("Старт трансформации датасета Aranet.")
    transform(execution_date, aranet())
    log.info("Трансформация датасета Aranet завершена.")


def transform_humidity(execution_date, **kwargs):
    log.info("Старт трансформации датасета Humidity.")
    transform(execution_date, humidity())
    log.info("Трансформация датасета Humidity завершена.")


def transform_temperature(execution_date, **kwargs):
    log.info("Старт трансформации датасета Temperature.")
    transform(execution_date, temperature())
    log.info("Трансформация датасета Temperature завершена.")


def transform_atmosphericpressure(execution_date, **kwargs):
    log.info("Старт трансформации датасета Atmosphericpressure.")
    transform(execution_date, atmosphericpressure())
    log.info("Трансформация датасета Atmosphericpressure завершена.")


def transform_ppfd(execution_date, **kwargs):
    log.info("Старт трансформации датасета Ppfd.")
    transform(execution_date, ppfd())
    log.info("Трансформация датасета Ppfd завершена.")


def transform_voltage(execution_date, **kwargs):
    log.info("Старт трансформации датасета Voltage.")
    transform(execution_date, voltage())
    log.info("Трансформация датасета Voltage завершена.")


def transform_current(execution_date, **kwargs):
    log.info("Старт трансформации датасета Current.")
    transform(execution_date, current())
    log.info("Трансформация датасета Current завершена.")


def transform_bec(execution_date, **kwargs):
    log.info("Старт трансформации датасета Bec.")
    transform(execution_date, bec())
    log.info("Трансформация датасета Bec завершена.")


def transform_weight(execution_date, **kwargs):
    log.info("Старт трансформации датасета Weight.")
    transform(execution_date, weight())
    log.info("Трансформация датасета Weight завершена.")

# LOAD


def load(exec_date, obj):
    import csv
    #exec_date = exec_date - timedelta(days=1)
    exec_date = datetime.date(exec_date - timedelta(minutes=minute_duration))
    list_data = list()
    try:
        _type = str(type(obj))
        _type = _type[_type.find("__main__.")+9:_type.find("'>")]
        file_path = "{path}extract/{date}/tmp_aranet_{class_obj}.csv".format(
            path=path, date=exec_date, class_obj=_type)
        log.info("Чтения данных для отправки в CLickhouse: " + file_path)
        with open(file_path) as f:
            try:
                reader = csv.reader(f)
                list_data = list(reader)
                f.close()
            except Exception as e:
                str_err = "Ошибка чтения файла: {err}!".format(err=e)
                log.error(str_err)
                send_alert(str_err, error_head +
                           sys._getframe().f_code.co_name)
                raise
        log.info("Данные считаны. Длина массива data:" + str(len(list_data)))

    except Exception as e:
        str_err = "Ошибка чтения данных из файла: {err}!".format(err=e)
        log.error(str_err)
        send_alert(e, error_head + sys._getframe().f_code.co_name)
        raise
    finally:
        return list_data


def load_aranet(execution_date, **kwargs):
    list_unit = load(execution_date, aranet())
    log.info("Отправка данных в датасет Aranet.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = insert_aranet
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {humidity}, {temperature}, {atmosphericpressure}, {co2}, {co2Abc}, {ppfd}, {voltage}, {current}, {bec}, {dp}, {pec}, {vwc}, {weight}, {weightraw}), """.format(id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[
                    4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], humidity=item[12], temperature=item[13], atmosphericpressure=item[14], co2=item[15], co2Abc=item[16], ppfd=item[17], voltage=item[18], current=item[19], bec=item[20], dp=item[21], pec=item[22], vwc=item[23], weight=item[24], weightraw=item[25])
            try:
                query = query.replace('None', 'Null')
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Aranet отправлены.")
    else:
        log.info("Нет новых данных.")


def load_humidity(execution_date, **kwargs):
    list_unit = load(execution_date, humidity())
    log.info("Отправка данных в датасет Humidity.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = insert_humidity
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {humidity}), """.format(
                    id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], humidity=item[12])
            try:
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Humidity отправлены.")
    else:
        log.info("Нет новых данных.")


def load_temperature(execution_date, **kwargs):
    list_unit = load(execution_date, temperature())
    log.info("Отправка данных в датасет Temperature.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = insert_temperature
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {temperature}), """.format(
                    id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], temperature=item[12])
            try:
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Temperature отправлены.")
    else:
        log.info("Нет новых данных.")


def load_atmosphericpressure(execution_date, **kwargs):
    list_unit = load(execution_date, atmosphericpressure())
    log.info("Отправка данных в датасет Atmosphericpressure.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = insert_atmosphericpressure
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {atmosphericpressure}, {co2}, {co2Abc}), """.format(
                    id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], atmosphericpressure=item[12], co2=item[13], co2Abc=item[14])
            try:
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Atmosphericpressure отправлены.")
    else:
        log.info("Нет новых данных.")


def load_ppfd(execution_date, **kwargs):
    list_unit = load(execution_date, ppfd())
    log.info("Отправка данных в датасет Ppfd.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = inert_ppfd
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {ppfd}), """.format(
                    id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], ppfd=item[12])
            try:
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Ppfd отправлены.")
    else:
        log.info("Нет новых данных.")


def load_voltage(execution_date, **kwargs):
    list_unit = load(execution_date, voltage())
    log.info("Отправка данных в датасет Voltage.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = insert_voltage
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {voltage}), """.format(
                    id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], voltage=item[12])
            try:
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Voltage отправлены.")
    else:
        log.info("Нет новых данных.")


def load_current(execution_date, **kwargs):
    list_unit = load(execution_date, current())
    log.info("Отправка данных в датасет Current.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = insert_current
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {current}), """.format(
                    id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], current=item[12])
            try:
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Current отправлены.")
    else:
        log.info("Нет новых данных.")


def load_bec(execution_date, **kwargs):
    list_unit = load(execution_date, bec())
    log.info("Отправка данных в датасет Bec.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = insert_bec
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {bec}, {dp}, {pec}, {vwc}), """.format(
                    id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], bec=item[12], dp=item[13], pec=item[14], vwc=item[15])
            try:
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Bec отправлены.")
    else:
        log.info("Нет новых данных.")


def load_weight(execution_date, **kwargs):
    list_unit = load(execution_date, weight())
    log.info("Отправка данных в датасет Weight.")

    if list_unit is not None and len(list_unit) > 0:
        with dst:
            query = insert_weight
            for item in list_unit:
                query += """({id}, {qos}, '{timestamp}', {time}, '{name_greenhouse}', '{name_unit}', '{name_base}', '{name_sensor}', '{name_client}', '{name_topic}', {rssi}, {battery}, {weight}, {weightraw}), """.format(
                    id=item[0], qos=item[1], timestamp=item[2], time=item[3], name_greenhouse=item[4], name_unit=item[5], name_base=item[6], name_sensor=item[7], name_client=item[8], name_topic=item[9], rssi=item[10], battery=item[11], weight=item[12], weightraw=item[13])
            try:
                dst.execute(query)
            except Exception as e:
                str_err = "Ошибка отправки записи массива в Clickhouse: {err}| {item}!".format(
                    err=str(e), item=str(item))
                log.error(str_err)
                send_alert(e, error_head + sys._getframe().f_code.co_name)
                raise
        log.info("Данные в датасет Weight отправлены.")
    else:
        log.info("Нет новых данных.")


with DAG(
    dag_id=_dag_id,
    schedule_interval=timedelta(minutes=1440),  # '0 1 * * *',
    start_date=_start_date,
    catchup=True,
    tags=['Clickhouse'],


) as dag:

    task_connect_to_postgres = PythonOperator(
        task_id="start",
        python_callable=start,
        provide_context=True
    )
    task_extract_aranet = PythonOperator(
        task_id="extract_aranet",
        python_callable=extract_aranet,
        provide_context=True
    )
    task_extract_humidity = PythonOperator(
        task_id="extract_humidity",
        python_callable=extract_humidity,
        provide_context=True
    )
    task_extract_temperature = PythonOperator(
        task_id="extract_temperature",
        python_callable=extract_temperature,
        provide_context=True
    )
    task_extract_atmosphericpressure = PythonOperator(
        task_id="extract_atmosphericpressure",
        python_callable=extract_atmosphericpressure,
        provide_context=True
    )
    task_extract_ppfd = PythonOperator(
        task_id="extract_ppfd",
        python_callable=extract_ppfd,
        provide_context=True
    )
    task_extract_voltage = PythonOperator(
        task_id="extract_voltage",
        python_callable=extract_voltage,
        provide_context=True
    )
    task_extract_current = PythonOperator(
        task_id="extract_current",
        python_callable=extract_current,
        provide_context=True
    )
    task_extract_bec = PythonOperator(
        task_id="extract_bec",
        python_callable=extract_bec,
        provide_context=True
    )
    task_extract_weight = PythonOperator(
        task_id="extract_weight",
        python_callable=extract_weight,
        provide_context=True
    )
    task_transform_aranet = PythonOperator(
        task_id="transform_aranet",
        python_callable=transform_aranet,
        provide_context=True
    )
    task_transform_aranet_humidity = PythonOperator(
        task_id="transform_aranet_humidity",
        python_callable=transform_humidity,
        provide_context=True
    )
    task_transform_aranet_temperature = PythonOperator(
        task_id="transform_aranet_temperature",
        python_callable=transform_temperature,
        provide_context=True
    )
    task_transform_aranet_atmosphericpressure = PythonOperator(
        task_id="transform_aranet_atmosphericpressure",
        python_callable=transform_atmosphericpressure,
        provide_context=True
    )
    task_transform_aranet_ppfd = PythonOperator(
        task_id="transform_aranet_ppfd",
        python_callable=transform_ppfd,
        provide_context=True
    )
    task_transform_aranet_voltage = PythonOperator(
        task_id="transform_aranet_voltage",
        python_callable=transform_voltage,
        provide_context=True
    )
    task_transform_aranet_current = PythonOperator(
        task_id="transform_aranet_current",
        python_callable=transform_current,
        provide_context=True
    )

    task_transform_aranet_bec = PythonOperator(
        task_id="transform_aranet_bec",
        python_callable=transform_bec,
        provide_context=True
    )
    task_transform_aranet_weight = PythonOperator(
        task_id="transform_aranet_weight",
        python_callable=transform_weight,
        provide_context=True
    )
    task_load_aranet = PythonOperator(
        task_id="load_aranet",
        python_callable=load_aranet,
        provide_context=True
    )
    task_load_humidity = PythonOperator(
        task_id="load_humidity",
        python_callable=load_humidity,
        provide_context=True
    )
    task_load_temperature = PythonOperator(
        task_id="load_temperature",
        python_callable=load_temperature,
        provide_context=True
    )
    task_load_ppfd = PythonOperator(
        task_id="load_ppfd",
        python_callable=load_ppfd,
        provide_context=True
    )
    task_load_voltage = PythonOperator(
        task_id="load_voltage",
        python_callable=load_voltage,
        provide_context=True
    )
    task_load_current = PythonOperator(
        task_id="load_current",
        python_callable=load_current,
        provide_context=True
    )
    task_load_bec = PythonOperator(
        task_id="load_bec",
        python_callable=load_bec,
        provide_context=True
    )
    task_load_atmosphericpressure = PythonOperator(
        task_id="load_atmosphericpressure",
        python_callable=load_atmosphericpressure,
        provide_context=True
    )
    task_load_weight = PythonOperator(
        task_id="load_weight",
        python_callable=load_weight,
        provide_context=True
    )
    task_close = PythonOperator(
        task_id="finnaly",
        python_callable=finnaly,
        provide_context=True
    )

task_connect_to_postgres >> task_extract_aranet >> task_transform_aranet >> task_load_aranet >> task_close
task_connect_to_postgres >> task_extract_humidity >> task_transform_aranet_humidity >> task_load_humidity >> task_close
task_connect_to_postgres >> task_extract_temperature >> task_transform_aranet_temperature >> task_load_temperature >> task_close
task_connect_to_postgres >> task_extract_atmosphericpressure >> task_transform_aranet_atmosphericpressure >> task_load_atmosphericpressure >> task_close
task_connect_to_postgres >> task_extract_ppfd >> task_transform_aranet_ppfd >> task_load_ppfd >> task_close
task_connect_to_postgres >> task_extract_voltage >> task_transform_aranet_voltage >> task_load_voltage >> task_close
task_connect_to_postgres >> task_extract_current >> task_transform_aranet_current >> task_load_current >> task_close
task_connect_to_postgres >> task_extract_bec >> task_transform_aranet_bec >> task_load_bec >> task_close
task_connect_to_postgres >> task_extract_weight >> task_transform_aranet_weight >> task_load_weight >> task_close
