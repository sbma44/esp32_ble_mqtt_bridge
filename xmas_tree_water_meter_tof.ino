#include <Statistical.h>

#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include "credentials.h"
WiFiClient espClient;
PubSubClient client(espClient);

#include <Wire.h>
#include "Adafruit_VL6180X.h"
Adafruit_VL6180X vl = Adafruit_VL6180X();

#define MAX_SAMPLES 255
uint8_t samples[MAX_SAMPLES + 1];
uint8_t sample_count = 0;

// define WLAN_SSID "MyNetwork"
// define WLAN_PASS "MyPassword"

#define MQTT_SERVER      "192.168.1.2"
#define MQTT_SERVERPORT  1883                   // use 8883 for SSL


void setup() {
  Serial.begin(115200);
  while (!Serial) { // wait for serial port to open on native usb devices
    delay(1);
  }
  
  pinMode(D4, OUTPUT);
  digitalWrite(D4, HIGH); 

  Serial.println("Adafruit VL6180x test!");
  if (! vl.begin()) {
    Serial.println("Failed to find sensor");
    while (1);
  }
  Serial.println("Sensor found!");

  WiFi.begin(WLAN_SSID, WLAN_PASS);
  client.setServer(MQTT_SERVER, MQTT_SERVERPORT); //connecting to mqtt server
  client.setCallback(callback);

  connectmqtt();
}

unsigned long last = 0;
void loop() {
  // mqtt
  if (!client.connected())
  {
    reconnect();
  }

  if(1) {
  //if ((millis() < last) || (millis() - last > 250)) {
    uint8_t range = vl.readRange();
    uint8_t status = vl.readRangeStatus();
    
    if (status == VL6180X_ERROR_NONE) {
      Serial.print("Range: "); Serial.println(range);
      samples[sample_count] = range;
      
      if (sample_count == MAX_SAMPLES) {
        digitalWrite(D4, LOW);
        delay(20);
        digitalWrite(D4, HIGH);   
        
        Array_Stats<uint8_t> Data_Array(samples, sizeof(samples) / sizeof(samples[0])); 
        client.publish("xmas/tree/water/raw", String(Data_Array.Quartile(2)).c_str());
    
        Serial.print("### published median value ");
        Serial.println(Data_Array.Quartile(2));
        sample_count = 0;
      }
      else {
        sample_count = sample_count + 1;
      }
    }
    else if  ((status >= VL6180X_ERROR_SYSERR_1) && (status <= VL6180X_ERROR_SYSERR_5)) {
      Serial.println("System error");
    }
    else if (status == VL6180X_ERROR_ECEFAIL) {
      Serial.println("ECE failure");
    }
    else if (status == VL6180X_ERROR_NOCONVERGE) {
      Serial.println("No convergence");
    }
    else if (status == VL6180X_ERROR_RANGEIGNORE) {
      Serial.println("Ignoring range");
    }
    else if (status == VL6180X_ERROR_SNR) {
      Serial.println("Signal/Noise error");
    }
    else if (status == VL6180X_ERROR_RAWUFLOW) {
      Serial.println("Raw reading underflow");
    }
    else if (status == VL6180X_ERROR_RAWOFLOW) {
      Serial.println("Raw reading overflow");
    }
    else if (status == VL6180X_ERROR_RANGEUFLOW) {
      Serial.println("Range reading underflow");
    }
    else if (status == VL6180X_ERROR_RANGEOFLOW) {
      Serial.println("Range reading overflow");
    }

    last = millis();
  }
  
  client.loop();
}


void callback(char* topic, byte* payload, unsigned int length) {   //callback includes topic and payload ( from which (topic) the payload is comming)
  //client.publish("outTopic", "LED turned OFF");
}

void reconnect() {
  while (!client.connected()) {
    Serial.println("Attempting MQTT connection...");
    if (client.connect("ESP8266_xmastree")) {
      Serial.println("connected");      
    } else {      
      delay(5000);
    }
  }
}

void connectmqtt()
{
  client.connect("ESP8266_xmastree");  // ESP will connect to mqtt broker with clientID
  {
    if (!client.connected())
    {
      reconnect();
    }
  }
}
