import decimal
import json

class DataSets:
    id: int = None,
    timestamp: str = None,
    time: int = None,
    qos: int = None,
    topic: str = None,
    client: str = None,
    sensor: str = None,
    base: str = None,
    prod_unit: str = None,
    greenhouse: str = None,
    battery: decimal = None,
    rssi: int = None,

class aranet(DataSets):
    humidity: decimal = None
    temperature: decimal = None
    atmosphericpressure: decimal = None
    co2: decimal = None
    co2Abc: decimal = None
    ppfd: decimal = None
    voltage: decimal = None
    current: decimal = None
    bec: decimal = None
    dp: decimal = None
    pec: decimal = None
    vwc: decimal = None
    weight: decimal = None
    weightraw: decimal = None

    """def __init__(self, id, name, id_prod):
        self.id_base = id
        self.base = name
        self.id_production_unit = id_prod

    @staticmethod
    def to_object(_value):
        inst = aranet(
            _value['id'], _value['qos'], _value['timestamp'], _value['weight'],
            _value['weightraw'], _value['humidity'],
            _value['temperature'], _value['bec'], _value['dp'], _value['pec'],
            _value['vwc'], _value['voltage'], _value['current'], _value['ppfd'],
            _value['atmosphericpressure'], _value['co2'], _value['co2Abc'],
            _value['rssi'], _value['time'], _value['battery'], _value['id_client'], _value['id_topic'], _value['id_sensor'])
        return inst

    def to_str(self):
        result = ""
        """
    
class humidity(DataSets):
    humidity: decimal = None
   
class temperature(DataSets):
    temperature: decimal = None
  
class atmosphericpressure(DataSets):
    atmosphericpressure: decimal = None
    co2: decimal = None
    co2Abc: decimal = None
    
class ppfd(DataSets):
    ppfd: decimal = None
  
class voltage(DataSets):
    voltage: decimal = None
    
class current(DataSets):
    current: decimal = None
   
class bec(DataSets):
    bec: decimal = None
    dp: decimal = None
    pec: decimal = None
    vwc: decimal = None
    
class weight(DataSets):
    weight: decimal = None
    weightraw: decimal = None
   

class AranetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (aranet, humidity, temperature, atmosphericpressure, ppfd, voltage, current, bec, weight)):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)