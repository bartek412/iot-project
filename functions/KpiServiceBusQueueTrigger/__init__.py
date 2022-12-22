import logging
import json
import os
from azure.iot.hub import IoTHubRegistryManager

import azure.functions as func


def main(msg: func.ServiceBusMessage):
    msg = json.loads(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message'
                 )
    if msg['Good Count percentage'] < 0.9:
        device_id = msg['ConnectionDeviceId']
        registry_manager = IoTHubRegistryManager(os.getenv('CONN_STRING'))
        twin = registry_manager.get_twin(device_id)
        production_rate = twin.properties.as_dict()[
            'reported']['ProductionRate']
        patch = {
            "properties": {
                "desired": {"ProductionRate": production_rate}
            }
        }
        registry_manager.update_twin(device_id, patch)
