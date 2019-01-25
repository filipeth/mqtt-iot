import picamera
import json
import board
import busio
import adafruit_bme280
import logging
import argparse
import glob
import threading
import os
import time
import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import boto3
import paho.mqtt.client as mqtt
from AWSIoTPythonSDK.exception.AWSIoTExceptions import publishTimeoutException
from ssl import SSLError
from botocore.exceptions import ClientError
from socket import error as serror


# Define function to set flag when device is disconnected
def myOnOnlineCallback():
    global connected_flag
    connected_flag = True
    logger.info("Connected.")
    threading.Thread(target=resend_img, name="resend_images").start()


# Define function to set flag when device is disconnected
def myOnOfflineCallback():
    global connected_flag
    connected_flag = False
    logger.error("Disconnected from AWS IoT Core.")


# Publish data into topic with QOS=1
def send_img(file, captured_at):
    try:
        s3.upload_file(file, bucket_name, "{}/{}/{}.jpg".format(company_name, clientId, captured_at))
    except Exception as e:
        logger.error("Error in function send_img(). {}".format(e))
        raise
    else:
        logger.info('Image taken at {} uploaded to s3 {} bucket and folder {}.'.format(captured_at, bucket_name, clientId))


# Resend images that where not sent due to lack of connection
def resend_img():
    files = glob.glob("cache/*.jpg")
    if len(files) == 0:
        logger.info("Folder empty. No images to be sent.")
    else:
        logger.info("Total of {} images to be sent.".format(len(files)))
        for file in files:
            captured_at = file.split('.')[0].split('_')[1]
            logger.info("Trying to send image taken at {}".format(captured_at))
            try:
                send_img(file, captured_at)
            except Exception as e:
                logger.error("Error in function resend_img(). {}".format(e))
                logger.error("Not connected, will resend files later when connected.")
                break
            else:
                os.unlink(file)


# Add image into a JSON and publish it
def take_pic(enable_timer):
    if enable_timer:
        threading.Timer(pic_timer, take_pic, args=[True]).start()
    try:
        camera = picamera.PiCamera()
        camera.resolution = (2592, 1944)
        camera.start_preview()
        time.sleep(1)
        camera.capture(image, quality=100)
        camera.stop_preview()
    except Exception:
        logger.error("Error taking picture.")
    else:
        logger.info("Picture captured.")
        captured_at = str(datetime.datetime.now().replace(second=0, microsecond=0))
    finally:
        camera.close()

    logger.info("Preparing image to be upload into s3...")
    try:
        send_img(image, captured_at)
    except Exception as e:
        # save image in another folder for resending when the connection is back
        logger.error("Error in function take_pic(). {}".format(e))
        logger.error("No connection, saving image to resend later.")
        with open("cache/img_{}.jpg".format(captured_at), "wb") as to_save:
            with open(image, 'rb') as img:
                file_content = img.read()
            to_save.write(file_content)
            logger.info("Image saved.")


def read_sensor(enable_timer):
    if enable_timer:
        threading.Timer(sensor_timer, read_sensor, args=[True]).start()
    captured_at = str(datetime.datetime.now().replace(second=0, microsecond=0))
    temp = bme280.temperature
    press = bme280.pressure
    humidity = bme280.humidity
    dic = {"temp": temp, "press": press, "humidity": humidity, "captured_at": captured_at}
    data_out = json.dumps(dic)
    try:
        client.publish(topic='iot1/sensor', payload=data_out, qos=1)
        myAWSIoTMQTTClient.publish(topic=topic, payload=data_out, QoS=1)
    except Exception as error:
        logger.error("Storing data to send later. {}".format(error))
    else:
        logger.info("Sensor data published.")


def connect():
    global client
    client = mqtt.Client(client_id=clientId, clean_session=False)
    is_connected = False
    while not is_connected:
        print('Connecting to local broker on address {}...'.format("10.0.1.27"))
        try:
            client.connect(host="10.0.1.27", port=8883)
        except serror:
            print('Error. Trying again.')
            time.sleep(1)
            client.loop()
        else:
            print("Connected to local broker.")
            is_connected = True

    client.loop_forever()


# Read in command-line parameters
parser = argparse.ArgumentParser()
parser.add_argument("-e", "--endpoint", action="store", required=True, dest="host", help="Your AWS IoT custom endpoint")
parser.add_argument("-w", "--websocket", action="store_true", dest="useWebsocket", default=False,
                    help="Use MQTT over WebSocket")
parser.add_argument("-id", "--clientId", action="store", dest="clientId", required=True, help="Targeted client id")
parser.add_argument("-p", "--picTimer", action="store", dest="picTimer", type=int, default=600,
                    help="Send picture every time, in seconds.")
parser.add_argument("-s", "--sensorTimer", action="store", dest="sensorTimer", type=int, default=60,
                    help="Send sensor data every time, in seconds.")
parser.add_argument("-awsId", "--keyID", action="store", required=True, dest="aws_access_key_id", help="AWS Access Key id")
parser.add_argument("-awsSecret", "--secretKey", action="store", required=True, dest="aws_secret_access_key",
                    help="AWS Secret Access Key")
parser.add_argument("-b", "--bucket", action="store", required=True, dest="bucket_name",
                    help="AWS s3 bucket name")
parser.add_argument("-c", "--companyName", action="store", required=True, dest="company_name",
                    help="Company name")

args = parser.parse_args()
host = args.host
rootCAPath = "certificates/rootCA.pem"
clientId = args.clientId
company_name = args.company_name
topic = "{}/{}/sensor".format(company_name, clientId)
pic_timer = args.picTimer
sensor_timer = args.sensorTimer
aws_access_key_id = args.aws_access_key_id
aws_secret_access_key = args.aws_secret_access_key
bucket_name = args.bucket_name

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler("test.log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = None
myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId, useWebsocket=True)
myAWSIoTMQTTClient.configureEndpoint(host, 443)
myAWSIoTMQTTClient.configureCredentials(rootCAPath)

# Set aws access key
os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(10)  # 5 sec

# AWSIoTMQTTClient onOnline and onOffline configuration
myAWSIoTMQTTClient.onOnline = myOnOnlineCallback
myAWSIoTMQTTClient.onOffline = myOnOfflineCallback

# Create an S3 client
s3 = boto3.client('s3')

# BME280 sensor
i2c = busio.I2C(board.SCL, board.SDA)
bme280 = adafruit_bme280.Adafruit_BME280_I2C(i2c)


image = 'last_picture_taken.jpg'
# Start functions in threads
threading.Thread(target=take_pic, args=[True], name="camera").start()
threading.Thread(target=read_sensor, args=[True], name="sensor").start()

# Connect and subscribe to AWS IoT
connected_flag = False
while not connected_flag:
    logger.info("Connecting to AWS IoT core.")
    try:
        myAWSIoTMQTTClient.connect()
    except Exception as e:
        logger.error("Error connecting. {}".format(e))
        logger.error("Retrying in 5 seconds.")
        time.sleep(5)


# Connect to local MQTT broker
connect()

