#include <PubSubClient.h>
#include <SoftwareSerial.h>
#include <ESP8266WiFi.h>

#include "credentials.h"
// define WLAN_SSID "MyNetwork"
// define WLAN_PASS "MyPassword"

#define MQTT_SERVER      "192.168.1.2"
#define MQTT_SERVERPORT  1883                   // use 8883 for SSL

#define CO2_TX D1
#define CO2_RX D2

WiFiClient espClient;
PubSubClient client(espClient);
SoftwareSerial SerialCO2(CO2_RX, CO2_TX); // RX, TX

const uint8_t cmd[9] = {0xFF,0x01,0x86,0x00,0x00,0x00,0x00,0x00,0x79};

void askCO2() {
  for (int i=0; i<9; i++) {
    SerialCO2.write(cmd[i]);
  }
}

int getCO2() {
  uint8_t response[9];
  int ppm = -1;
  
  while(SerialCO2.available() > 9) {
    SerialCO2.read();
  }
  
  if (SerialCO2.available() == 9) {
    for(int i=0; i < 9; i++) {
      response[i] = SerialCO2.read();
    }
  
    int responseHigh = (int) response[2];
    int responseLow = (int) response[3];
    ppm = (responseHigh << 8) + responseLow;
  }

  return ppm;
}


void setup() {
  WiFi.begin(WLAN_SSID, WLAN_PASS);
  client.setServer(MQTT_SERVER, MQTT_SERVERPORT); //connecting to mqtt server
  client.setCallback(callback);

  connectmqtt();
  Serial.begin(115200);
  SerialCO2.begin(9600);
}

unsigned long last = 0;
bool askCycle = true;
void loop() {
  // mqtt
  if (!client.connected())
  {
    reconnect();
  }
  
  if ((millis() < last) || (millis() - last > 5000)) {  
    if (askCycle) {
      askCO2();      
    }
    else {
      int ppm = getCO2();    
      if ((ppm > 0) && (ppm < 60000)) {
        Serial.println(ppm);
        client.publish("co2/office/co2_ppm", String(ppm).c_str());
      }  
    }
    askCycle = !askCycle;
    
    last = millis(); 
  }

  client.loop();
}


void callback(char* topic, byte* payload, unsigned int length) {   //callback includes topic and payload ( from which (topic) the payload is comming)
  //client.publish("outTopic", "LED turned OFF");
}

void reconnect() {
  while (!client.connected()) {
    Serial.println("Attempting MQTT reconnection...");
    if (client.connect(String("ESP8266_" + WiFi.macAddress()).c_str())) {
      Serial.println("MQTT connected");
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
  client.connect(String("ESP8266_" + WiFi.macAddress()).c_str());  // ESP will connect to mqtt broker with clientID
  {
    Serial.println("MQTT connected");
    
    if (!client.connected())
    {
      reconnect();
    }
  }
}
