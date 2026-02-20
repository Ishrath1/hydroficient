import random
import json
from datetime import datetime, timezone
import time
import paho.mqtt.client as mqtt
import threading


# Implementing the WaterSensor class.
class WaterSensor:
    """
    Simulates a Hydroficient HYDROLOGIC water sensor and publishes readings to an MQTT broker.

    Normal ranges:
    - pressure_upstream: 75-90 PSI
    - pressure_downstream: 70-85 PSI
    - flow_rate: 30-50 gallons/min

    Anomalies:
    - Leak: flow_rate > 80 gallons/min
    - Blockage: pressure_upstream > 90 PSI and pressure_downstream < 70 PSI
    - Stuck sensor: all readings are the same

    """

    def __init__(self, device_id,location, broker='localhost',port=1883):
        """
        Initialize the sensor with a device ID, location, and MQTT broker connection.
        """
        self.device_id = device_id
        self.location = location
        self.count = 0

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.connect(broker, port)
        self.client.loop_start()

        self.topic = f"hydroficient/grandmarina/sensors/{self.location}/readings"
        self.pressure_upstream_base = 82.0
        self.pressure_downstream_base = 76.0
        self.flow_rate_base = 40.0


    def get_reading(self):
        """
        Generate a normal sensor reading.
        """
        self.count += 1
        pressure_up = round(self.pressure_upstream_base + random.uniform(-2, 2),1)
        pressure_down = round(self.pressure_downstream_base + random.uniform(-2, 2),1)
        flow = round(self.flow_rate_base + random.uniform(-3, 3),1)

        return {
            "device_id": self.device_id,
            "location": self.location,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "counter": self.count,
            "pressure_upstream": pressure_up,
            "pressure_downstream":pressure_down,
            "flow_rate": flow
        }


    def publish_reading(self):
        """
        Publish a given reading to the MQTT broker.
        """
        reading = self.get_reading()
        self.client.publish(self.topic,json.dumps(reading))
        return reading
    
    def run_continuous(self, interval=2):
        """
        Continuously publish readings at a specified interval (in seconds).
        """
        print(f"Starting device: {self.device_id}")
        print(f"Location: {self.location}")
        print(f"Publishing to topic: {self.topic}")
        print("Interval between readings: {} seconds".format(interval))
        print('-' * 50)

        try:
            while True:
                reading = self.publish_reading()
                print(f"""[{reading['counter']}] Pressure {reading['pressure_upstream']}/{reading['pressure_downstream']} PSI,
                       Flow: {reading['flow_rate']} gal/min""")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nSensor stopped.")
            self.client.loop_stop()
            self.client.disconnect()


    def get_leak_reading(self):
        """
        Generate a reading simulating a water leak.

        Hint: A leak causes abnormally HIGH flow rate (80-120 gallons/min)
        """
        self.count += 1
        pressure_up = round(self.pressure_upstream_base + random.uniform(-2, 2),1)
        pressure_down = round(self.pressure_downstream_base + random.uniform(-2, 2),1)
        flow = round(self.flow_rate_base + random.uniform(40,80),1)

        return {
            "device_id": self.device_id,
            "counter": self.count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pressure_upstream": pressure_up,
            "pressure_downstream":pressure_down,
            "flow_rate": flow
        }

    def get_blockage_reading(self):
        """
        Generate a reading simulating a pipe blockage.

        Hint: A blockage causes HIGH upstream pressure and LOW downstream pressure
        """
        self.count += 1
        pressure_up = round(self.pressure_upstream_base + random.uniform(8,20),1)
        pressure_down = round(self.pressure_downstream_base + random.uniform(-50, -20),1)
        flow = round(self.flow_rate_base + random.uniform(-3, 3),1)

        return {
            "device_id": self.device_id,
            "counter": self.count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pressure_upstream": pressure_up,
            "pressure_downstream":pressure_down,
            "flow_rate": flow
        }

    def get_stuck_reading(self):
        """
        Generate a reading simulating a stuck/malfunctioning sensor.

        Hint: A stuck sensor reports the SAME value for all readings
        """
        self.count += 1
        pressure_up = round(self.pressure_upstream_base + random.uniform(-2, 2),1)

        return {
            "device_id": self.device_id,
            "counter": self.count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pressure_upstream": pressure_up,
            "pressure_downstream":pressure_up,
            "flow_rate": pressure_up
        }

    
# Runing the three hydrologic sensors.

def run_sensor(device_id,location,interval):
    sensor=WaterSensor(device_id,location)
    sensor.run_continuous(interval)

devices = [
    {"device_id": "GM-HYDROLOGIC-01", "location":"main-building"},
    {"device_id": "GM-HYDROLOGIC-02", "location":"pool-wing"},
    {"device_id": "GM-HYDROLOGIC-03", "location":"main-kitchen"},
]

threads = []
for d in devices:
    t = threading.Thread(target=run_sensor, args=(d["device_id"], d["location"], 2),daemon=True)
    t.start()
    threads.append(t)

print("All sensors running. Press Ctrl+C to stop.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping all sensors.")
    