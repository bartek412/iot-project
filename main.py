
import asyncio
import logging
import json
import time
import uuid
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message, MethodResponse
from datetime import datetime
from asyncua import Client, ua


async def send_telemetry(device, data):
    logging.info("sending telemetry")
    msg = Message(json.dumps(data))
    msg.message_id = uuid.uuid4()
    msg.content_encoding = "utf-8"
    msg.content_type = "application/json"
    await device.send_message(msg)
    logging.info("done sending telemetry")


previous_error = 0


async def send_error_event(device, data, twin):
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


async def update_reported_twin(device, data):
    logging.info(f"Setting reported ProductionRate to {data}")
    await device.patch_twin_reported_properties(data)


telemetry_data_nodes = ['ProductionStatus', 'WorkorderId', 'GoodCount',
                        'BadCount', 'Temperature']
twin_data_nodes = ['ProductionRate', 'DeviceError']
conn_str = ''
url = "opc.tcp://localhost:4840/"


async def main():
    device_client = IoTHubDeviceClient.create_from_connection_string(
        conn_str)

    # Connect the client.
    await device_client.connect()
    # Connect to ua server
    client = Client(url=url)
    await client.connect()

    while True:

        root_node = client.get_node('ns=2;s=Device 1')
        data = await get_data(root_node, telemetry_data_nodes)
        twin = await device_client.get_twin()
        # send `messages_to_send` messages in parallel
        await send_telemetry(device_client, data)
        error_data = await get_data(root_node, ['DeviceError'])
        await send_error_event(device_client, error_data, twin)

        async def set_production_rate_from_twin(patch):
            logging.info(
                f"the data in the desired properties patch was: {patch}")
            data_node = await root_node.get_child('ProductionRate')
            await data_node.write_value(ua.root_Variant(patch['ProductionRate'], ua.root_nodeVariantType.Int32))

        async def method_request_handler(method_request):
            if method_request.name == "EmergencyStop":
                await root_node.call_method('EmergencyStop')
                status = 200  # set return status code
                logging.info("executed EmergencyStop")
            elif method_request.name == "ResetErrorStatus":
                await root_node.call_method('ResetErrorStatus')
                status = 200  # set return status code
                logging.info("executed ResetErrorStatus")

            elif method_request.name == "MaintenanceDone":
                reported_properties = {
                    'LastMaintenanceDate': datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
                await device_client.patch_twin_reported_properties(reported_properties)
                status = 200  # set return status code
                logging.info("executed MaintenanceDone")
            else:
                status = 400  # set return status code
                logging.info("executed unknown method: " + method_request.name)

            method_response = MethodResponse.create_from_method_request(
                method_request, status)
            await device_client.send_method_response(method_response)

        device_client.on_method_request_received = method_request_handler
        device_client.on_twin_desired_properties_patch_received = set_production_rate_from_twin
        twin_data = await get_data(root_node, twin_data_nodes)

        await update_reported_twin(device_client, twin_data)

        time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
