#include <PubSubClient.h>

#include <WiFi.h>

#include "credentials.h"
// define WLAN_SSID "MyNetwork"
// define WLAN_PASS "MyPassword"

#define MQTT_SERVER      "192.168.1.2"
#define MQTT_SERVERPORT  1883                   // use 8883 for SSL

WiFiClient espClient;
PubSubClient client(espClient);

#include <BLEDevice.h>
#include <BLEUtils.h>
#include <BLEScan.h>
#include <BLEAdvertisedDevice.h>

int scanTime = 5; //In seconds
BLEScan* pBLEScan;

#define NAME_CACHE_SIZE 10
uint8_t nameCache_addr[NAME_CACHE_SIZE * 3];
char nameCache_name[NAME_CACHE_SIZE][25];
unsigned long nameCache_t[NAME_CACHE_SIZE];

int cacheIndex(bool oldest, uint8_t b0, uint8_t b1, uint8_t b2) {
  if (!oldest) {
    for (uint8_t i = 0; i < NAME_CACHE_SIZE; i++) {
      if ((nameCache_addr[(i*3)] == b0) && (nameCache_addr[(i*3)+1] == b1) && (nameCache_addr[(i*3)+2] == b2))
        return i;
    }
    return -1;
  }
  else {
    unsigned long n = millis();
    uint8_t m = 0;
    for(uint8_t i = 0; i < NAME_CACHE_SIZE; i++) {
      if (nameCache_t[i] < n ) {
        n = nameCache_t[i];
        m = i;
      }
    }
    return m;
  }
}

class MyAdvertisedDeviceCallbacks: public BLEAdvertisedDeviceCallbacks {
    void onResult(BLEAdvertisedDevice advertisedDevice) {      
      esp_bd_addr_t *m_address;
      m_address = advertisedDevice.getAddress().getNative();
      if ((*(*m_address) == 0xa4) && (*(*m_address + 1) == 0xc1) && (*(*m_address + 2) == 0x38)) {
        std::string strServiceData = advertisedDevice.getServiceData();
        Serial.printf("Advertised Device: %s \n", advertisedDevice.toString().c_str());  

        char deviceName[25];
        strcpy(deviceName, advertisedDevice.getName().c_str());
        uint8_t ci = cacheIndex(false, *(*m_address), *(*m_address+1), *(*m_address+2));
        if (strlen(deviceName) == 0) {
          Serial.println("nameless device!");
          if (ci > 0) {
            strcpy(deviceName, nameCache_name[ci]);
          }
        }
        else {
          if (ci < 0) {
            ci = cacheIndex(true, 0, 0, 0);
            strcpy(nameCache_name[ci], deviceName);
            nameCache_addr[(ci*3)] = *(*m_address);
            nameCache_addr[(ci*3) + 1] = *(*m_address + 1);
            nameCache_addr[(ci*3) + 2] = *(*m_address + 2);
            nameCache_t[ci] = millis();
          }
        }
        
        // temperature
        float celsius = (float) (((256 * ((uint8_t) strServiceData[7])) + ((uint8_t) strServiceData[6])) / 100.0);

        // humidity
        float humidity = (float) (((256 * ((uint8_t) strServiceData[9])) + ((uint8_t) strServiceData[8])) / 100.0);

        Serial.printf("Celsius %0.2f Humidity %0.1f\n", celsius, humidity);
        
        if (client.connected()) {
          Serial.println("publishing...");
          char topic[128] = "xiaomi_mijia/";          
          strcat(topic, deviceName);
          strcat(topic, "/temperature");
          client.publish(topic, String(celsius, 2).c_str());

          topic[0] = '\0';
          strcat(topic, "xiaomi_mijia/");
          strcat(topic, deviceName);
          strcat(topic, "/humidity");
          client.publish(topic, String(humidity, 1).c_str());          
        }
        else {
          Serial.println("...but not connected");
        }
      }
    }
};

void setup() {
  Serial.begin(115200);

  // initialize device name cache
  for(uint8_t i = 0; i < NAME_CACHE_SIZE; i++) {
    nameCache_t[i] = millis();
    for(uint8_t j = 0; j < 3; j++) {
      nameCache_addr[(i*3)+j] = 0;
    }
  }

  WiFi.begin(WLAN_SSID, WLAN_PASS);
  client.setServer(MQTT_SERVER, MQTT_SERVERPORT); //connecting to mqtt server
  client.setCallback(callback);
  //delay(5000);
  connectmqtt();
  
  BLEDevice::init("");
  pBLEScan = BLEDevice::getScan(); //create new scan
  pBLEScan->setAdvertisedDeviceCallbacks(new MyAdvertisedDeviceCallbacks());
  pBLEScan->setActiveScan(true); //active scan uses more power, but get results faster
  pBLEScan->setInterval(100);
  pBLEScan->setWindow(99);  // less or equal setInterval value
}

void loop() {
  // mqtt
  if (!client.connected())
  {
    reconnect();
  }
  
  BLEScanResults foundDevices = pBLEScan->start(scanTime, false);
  //Serial.print("Devices found: ");
  //Serial.println(foundDevices.getCount());
  //Serial.println("Scan done!");
  pBLEScan->clearResults();   // delete results fromBLEScan buffer to release memory 

  // handle mqtt messages
  client.loop();

  delay(10000);
}

void callback(char* topic, byte* payload, unsigned int length) {   //callback includes topic and payload ( from which (topic) the payload is comming)
  //client.publish("outTopic", "LED turned OFF");
}

void reconnect() {
  while (!client.connected()) {
    //Serial.println("Attempting MQTT connection...");
    if (client.connect("ESP32_clientID")) {
      //Serial.println("connected");
      // Once connected, publish an announcement...
      //client.publish("outTopic", "Nodemcu connected to MQTT");
      // ... and resubscribe
      //client.subscribe("inTopic");

    } else {
      /*
      
      Serial.print("failed, rc=");
      Serial.print(client.state());
      Serial.println(" try again in 5 seconds");
      // Wait 5 seconds before retrying
      */
      delay(5000);
    }
  }
}

void connectmqtt()
{
  client.connect("ESP32_clientID");  // ESP will connect to mqtt broker with clientID
  {
    //Serial.println("connected to MQTT");
    // Once connected, publish an announcement...

    // ... and resubscribe
    //client.subscribe("inTopic"); //topic=Demo
    //client.publish("outTopic",  "connected to MQTT");

    if (!client.connected())
    {
      reconnect();
    }
  }
}
