
import asyncio
import logging
import json
import time
import uuid
import os
import sys
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message, MethodResponse
from datetime import datetime
from asyncua import Client, ua
from dotenv import load_dotenv


TELEMETRY_DATA_NODES = ['ProductionStatus', 'WorkorderId', 'GoodCount',
                        'BadCount', 'Temperature']
TWIN_DATA_NODES = ['ProductionRate', 'DeviceError']


def load_settings():
    load_dotenv('settings.env')
    url_ua = os.getenv('URL_UA')
    try:
        device_id = sys.argv[1]
    except IndexError:
        print("You did not specify a device")
        sys.exit(1)
    conn_str = os.getenv(device_id)
    if not conn_str:
        print("Connection string for this device not found")
        sys.exit(1)
    return url_ua, device_id, conn_str


async def send_telemetry(device, data):
    logging.info("sending telemetry")
    msg = Message(json.dumps(data))
    msg.message_id = uuid.uuid4()
    msg.content_encoding = "utf-8"
    msg.content_type = "application/json"
    await device.send_message(msg)
    logging.info("done sending telemetry")


previous_error = 0


async def send_error_event(device, data):
    global previous_error

    if data['DeviceError'] > previous_error:
        logging.info("sending event")
        msg = Message(json.dumps(data))
        msg.message_id = uuid.uuid4()
        msg.content_encoding = "utf-8"
        msg.content_type = "application/json"
        msg.custom_properties['event'] = 'true'
        await device.send_message(msg)
        logging.info("done sending event")

        reported_properties = {
            'LastErrorDate': datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
        await device.patch_twin_reported_properties(reported_properties)
        previous_error = data['DeviceError']


async def get_data(parrent_node, data_nodes):
    result = {}
    for node_name in data_nodes:
        data_node = await parrent_node.get_child(node_name)
        data = await data_node.read_value()
        result[node_name] = data
    return result


async def update_reported_twin(device, data, twin):
    logging.info("Setting reported twin")
    new_twin = {}
    if data['ProductionRate'] != twin['reported']['ProductionRate']:
        new_twin['ProductionRate'] = data['ProductionRate']
    if data['DeviceError'] != twin['reported']['DeviceError']:
        new_twin['DeviceError'] = data['DeviceError']
    if new_twin:
        await device.patch_twin_reported_properties(data)


async def main():
    url_ua, device_id, conn_str = load_settings()
    device_client = IoTHubDeviceClient.create_from_connection_string(
        conn_str)

    # Connect the client.
    await device_client.connect()
    # Connect to ua server
    client = Client(url=url_ua)
    await client.connect()

    while True:
        async def set_production_rate_from_twin(patch):
            prod_node = await root_node.get_child('ProductionRate')
            await prod_node.write_value(ua.Variant(patch['ProductionRate'], ua.VariantType.Int32))

        root_node = client.get_node(f'ns=2;s=Device {device_id}')

        device_client.on_twin_desired_properties_patch_received = set_production_rate_from_twin

        data = await get_data(root_node, TELEMETRY_DATA_NODES)
        await send_telemetry(device_client, data)
        error_data = await get_data(root_node, ['DeviceError', 'WorkorderId'])
        await send_error_event(device_client, error_data)

        async def method_request_handler(method_request):
            try:
                if method_request.name == "EmergencyStop":
                    await root_node.call_method('EmergencyStop')
                    status = 200  # set return status code
                    payload = {"succeed": True}
                    logging.info("executed EmergencyStop")
                elif method_request.name == "ResetErrorStatus":
                    await root_node.call_method('ResetErrorStatus')
                    payload = {"succeed": True}
                    status = 200  # set return status code
                    logging.info("executed ResetErrorStatus")

                elif method_request.name == "MaintenanceDone":
                    reported_properties = {
                        'LastMaintenanceDate': datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
                    await device_client.patch_twin_reported_properties(reported_properties)
                    status = 200  # set return status code
                    payload = {"succeed": True}
                    logging.info("executed MaintenanceDone")
                else:
                    payload = {"MethodNotFound": True}
                    status = 404  # set return status code
                    logging.info("executed unknown method: " +
                                 method_request.name)
            except:
                status = 400
                payload = {"succeed": False}

            method_response = MethodResponse.create_from_method_request(
                method_request, status, payload)
            await device_client.send_method_response(method_response)

        device_client.on_method_request_received = method_request_handler

        twin = await device_client.get_twin()
        twin_data = await get_data(root_node, TWIN_DATA_NODES)

        await update_reported_twin(device_client, twin_data, twin)

        time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
