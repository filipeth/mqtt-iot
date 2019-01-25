# IoT device

IoT device (raspberry 3b+) that sends sensor data (temperature, humidity, pressure) and picture to a server using the MQTT protocol.

`aws_client.py` sends data to AWS IoT Core service, while `Client.py` sends data to a local MQTT broker.
- Edit the `mqtt_config.json` with the IP and port from the mosquitto broker

Energy Raspi 3b+
----
- 0.46A when running idle
- 0.47A reading and sending Sensor data
- 0.57A taking and sending pic
