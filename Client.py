import base64
import json
import paho.mqtt.client as mqtt
import picamera
import board
import busio
import adafruit_bme280
import datetime
import glob
import threading
from time import sleep
from socket import error as serror
import os


def on_connect(client, userdata, flags, rc):
    print("Connected " + str(rc))
    global connected_flag, disconnect_flag
    connected_flag = True
    disconnect_flag = False
    client.subscribe("{}/request/image".format(device_name))
    client.subscribe("{}/request/data".format(device_name))
    # check folder for files to send
    threading.Thread(target=resend_img).start()
    threading.Thread(target=resend_data).start()


def on_message(client, userdata, msg):
    if str(msg.topic).split('/')[-1] == 'image':
        print("Image requested by server.")
        take_pic(enable_timer=False)
    elif str(msg.topic).split('/')[-1] == 'data':
        print("Data requested by server.")
        read_sensor(enable_timer=False)


def resend_img():
    files = glob.glob("cache_img/*.jpg")
    if len(files) == 0:
        print("Folder empty. No images to be sent.")
    else:
        if connected_flag:
            print("Total of {} images to be sent.".format(len(files)))
            for file in files:
                captured_at = file.split('.')[0].split('_')[-1]
                print("Sending image taken at {}".format(captured_at))
                with open(file, 'rb') as img:
                    fileContent = img.read()
                    send_img(fileContent, captured_at)
                    os.unlink(file)


def resend_data():
    files = glob.glob("cache_data/*.json")
    if len(files) == 0:
        print("Folder empty. No sensor data to be sent.")
    else:
        if connected_flag:
            print("Total of {} sensor data to be sent.".format(len(files)))
            for file in files:
                captured_at = file.split('.')[0].split('_')[-1]
                print("Sending sensor data measured at {}".format(captured_at))
                with open(file) as file_to_send:
                    d = json.load(file_to_send)
                    send_data(d)
                    os.unlink(file)


def send_img(fileContent, captured_at):
    b64_bytes = base64.b64encode(fileContent)
    b64_string = b64_bytes.decode()
    # Publish JSON with data to MQTT with QOS=1
    payload = json.dumps({"image_data": b64_string,
                          "captured_at": captured_at})
    client.publish(topic="{}/image".format(device_name), payload=payload, qos=1)
    print('Image sent.')


def send_data(data):
    client.publish(topic='{}/sensor'.format(device_name), payload=data, qos=1)
    print("Data sent.")


# Add data aquired by sensor BME280 into a JSON and publish it
def read_sensor(enable_timer):
    if enable_timer:
        threading.Timer(sensor_time, read_sensor, args=[True]).start()
    temp = bme280.temperature
    press = bme280.pressure
    humidity = bme280.humidity
    captured_at = str(datetime.datetime.now().replace(second=0, microsecond=0))
    dic = {"temp": temp, "press": press, "humidity": humidity, "captured_at": captured_at}
    print("Temp: {} Pressure: {} Humidity: {}".format(temp, press, humidity))
    data_out = json.dumps(dic)
    if connected_flag:
        # Publish JSON
        print("Preparing data to send...")
        send_data(data_out)
    else:
        # save data in another folder for resending when the connection is back
        with open('cache_data/data_{}.json'.format(captured_at), 'w') as file:
            json.dump(data_out, file)
            print("No connection, data saved to resend later.")


# Add image into a JSON and publish it
def take_pic(enable_timer):
    if enable_timer:
        threading.Timer(pic_time, take_pic, args=[True]).start()
    try:
        camera = picamera.PiCamera()
        camera.resolution = (2592, 1944)
        camera.start_preview()
        sleep(1)
        camera.capture(image, quality=100)
        camera.stop_preview()
        pass
    finally:
        print("Picture captured.")
        captured_at = str(datetime.datetime.now().replace(second=0, microsecond=0))
        camera.close()

    with open(image, 'rb') as img:
        fileContent = img.read()
        if connected_flag:
            print("Preparing image to send...")
            send_img(fileContent, captured_at)
        else:
            # save image in another folder for resending when the connection is back
            print("No connection, saving image to resend later.")
            with open("cache_img/img_{}.jpg".format(captured_at), "wb") as to_save:
                to_save.write(fileContent)
                print("Image saved")


# Loads MQTT broker info to connect
with open('mqtt_config.json') as config:
    json_data = json.load(config)


pic_time = 600
sensor_time = 60
port = json_data.get("port")
broker = json_data.get("broker")
device_name = "iot{}".format(json_data.get("id"))

print("RUNNING Bloom-BoT")
# print("Sending picture every {}s and sensor data every {}s".format(pic_time, sensor_time))
connected_flag = False
client = mqtt.Client(client_id=device_name, clean_session=False)
client.on_connect = on_connect
client.on_message = on_message

# BME280 sensor
i2c = busio.I2C(board.SCL, board.SDA)
bme280 = adafruit_bme280.Adafruit_BME280_I2C(i2c)

# Open image to transmit via MQTT
image = 'picture.jpg'

# Create Timer in threads to send data
# Set timer to 5 min
take_pic(enable_timer=True)
read_sensor(enable_timer=True)

while not connected_flag:
    print('Connecting to broker on address {}...'.format(broker))
    try:
        client.connect(host=broker, port=port)
    except serror:
        print('Error. Trying again.')
    sleep(1)
    client.loop()


client.loop_forever()
