from datetime import datetime
import decimal
import json


class sensor_value:
    id: int = None,
    id_topic: int = None,
    id_client: int = None,
    id_sensor: int = None,
    qos: int = None,
    # timestamp: DateTime = None,
    timestamp: str = None,
    vwc: decimal = None,
    voltage: decimal = None,
    current: decimal = None,
    ppfd: decimal = None,
    atmosphericpressure: int = None,
    co2: int = None,
    co2Abc: decimal = None,
    rssi: int = None,
    time: int = None,
    battery: decimal = None,
    weight: decimal = None,
    weightraw: decimal = None,
    humidity: decimal = None,
    temperature: decimal = None,
    bec: decimal = None,
    dp: decimal = None,
    pec: decimal = None

    def __init__(self, id, qos=0, timestamp=None,
                 weight=None, weightraw=None, humidity=None, temperature=None,
                 bec=None, dp=None, pec=None, vwc=None, voltage=None, current=None,
                 ppfd=None, atmosphericpressure=None, co2=None, co2Abc=None,
                 rssi=None, _time=None, battery=None, id_client=None, id_topic=None, id_sensor=None):
        self.id = id
        self.qos = qos
        self.timestamp = timestamp
        self.weight = weight
        self.weightraw = weightraw
        self.humidity = humidity
        self.temperature = temperature
        self.bec = bec
        self.dp = dp
        self.pec = pec
        self.vwc = vwc
        self.voltage = voltage
        self.current = current
        self.ppfd = ppfd
        self.atmosphericpressure = atmosphericpressure
        self.co2 = co2
        self.co2Abc = co2Abc
        self.rssi = rssi
        self.time = _time
        self.battery = battery
        self.id_client = id_client
        self.id_topic = id_topic
        self.id_sensor = id_sensor

    @staticmethod
    def to_object(_value):
        """key = 'weight'
        if key in _value and _value[key] is not None:
            print(str(type(_value[key])) + ":" + str(_value[key]))
            _value[key] = "{0:0.3f}".format(Decimal(_value[key]))
        key = 'weightraw'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.3f}".format(Decimal(_value[key]))
            # print(val.weightraw)
        key = 'humidity'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.1f}".format(Decimal(_value[key]))
            # print(val.humidity)
        key = 'temperature'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.3f}".format(Decimal(_value[key]))
            # print(val.temperature)
        key = 'bec'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.5f}".format(Decimal(_value[key]))
            # print(val.bec)
        key = 'dp'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.6f}".format(Decimal(_value[key]))
            # print(val.dp)
        key = 'pec'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.5f}".format(Decimal(_value[key]))
            # print(val.pec)
        key = 'vwc'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.3f}".format(Decimal(_value[key]))
            # print(val.vwc)
        key = 'voltage'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.3f}".format(Decimal(_value[key]))
            # print(val.voltage)
        key = 'current'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.6f}".format(Decimal(_value[key]))
            # print(val.current)
        key = 'ppfd'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.1f}".format(Decimal(_value[key]))
            # print(val.ppfd)
        key = 'co2Abc'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.6f}".format(Decimal(_value[key]))
            # print(val.co2Abc)
        key = 'battery'
        if key in _value and _value[key] is not None:
            _value[key] = "{0:0.1f}".format(Decimal(_value[key]))"""

        inst = sensor_value(
            _value['id'], _value['qos'], _value['timestamp'], _value['weight'],
            _value['weightraw'], _value['humidity'],
            _value['temperature'], _value['bec'], _value['dp'], _value['pec'],
            _value['vwc'], _value['voltage'], _value['current'], _value['ppfd'],
            _value['atmosphericpressure'], _value['co2'], _value['co2Abc'],
            _value['rssi'], _value['time'], _value['battery'], _value['id_client'], _value['id_topic'], _value['id_sensor'])
        return inst

    def to_str(self):
        result = ""


class bases:
    id_base: int = None,
    base: str = None,
    id_production_unit: int = None,

    def __init__(self, id, name, id_prod):
        self.id_base = id
        self.base = name
        self.id_production_unit = id_prod

    @staticmethod
    def to_object(d):
        inst = bases(
            d['id_base'], d['base'], d['id_production_unit'])
        return inst


class clients:
    id_client: int = None,
    client: str = None,

    def __init__(self, id, name):
        self.id_client = id
        self.client = name

    @staticmethod
    def to_object(d):
        inst = clients(
            d['id_client'], d['client'])
        return inst


class production_unit:
    id_production_unit: int = None,
    production_unit: str = None,
    greenhouse: int = None,

    def __init__(self, id, name, id_gh):
        self.id_production_unit = id
        self.production_unit = name
        self.greenhouse = id_gh

    @staticmethod
    def to_object(d):
        inst = production_unit(
            d['id_production_unit'], d['production_unit'], d['greenhouse'])
        return inst


class messages_json:
    id: int
    message: json
    topic: str
    qos: int
    client: str
    timestamp: datetime

    def __init__(self, id, msg, tpc, qs, cl, ts):
        self.id = id
        self.message = msg
        self.topic = tpc
        self.qos = qs
        self.client = cl
        self.timestamp = datetime.fromtimestamp(ts, tz=None)

    def to_object(d):
        inst = messages_json(
            d['id'], d['message'], d['topic'], d['qos'], d['client'], d['timestamp'])
        return inst


class sensor:
    id_sensor: int = None,
    sensor: str = None,
    id_base: int = None,

    def __init__(self, id, name, id_base):
        self.id_sensor = id
        self.sensor = name
        self.id_base = id_base

    @staticmethod
    def to_object(d):
        inst = sensor(
            d['id_sensor'], d['sensor'], d['id_base'])
        return inst


class topic:
    id_topic: int = None,
    topic: str = None,

    def __init__(self, id, name):
        self.id_topic = id
        self.topic = name

    @staticmethod
    def to_object(d):
        inst = topic(
            d['id_topic'], d['topic'])
        return inst


class AranetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (production_unit, topic, sensor, messages_json, clients, bases, sensor_value)):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)
