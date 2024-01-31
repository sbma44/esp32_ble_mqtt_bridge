# Sensor Logging

This repo provides a systemd daemon that consumes MQTT messages from my local server and sticks them into an in-memory SQLite database that periodically backs itself up to disk..

Once a day, a CSV and JSON of the median temps (by 5-minute window) is constructed and uploaded to S3. A simple API is also provided for querying the data in the SQLite database.

The system runs on a Raspberry Pi; much of the system design is motivated by a desire to minimize writes to flash memory.

The `arduino/` directory contains sketches powering some of the ad-hoc ESP8266-based sensor modules that collect and send data to the MQTT server, as well as `esp32_ble_mqtt_bridge` which provides code for an ESP32 to collect BLE advertisements containing temperature and humidity measurements from Xiaomi temperature sensors running [custom firmware](https://github.com/pvvx/ATC_MiThermometer), then send those measurements to the MQTT server.

## Testing

`python3 -m test.test_database_handler`