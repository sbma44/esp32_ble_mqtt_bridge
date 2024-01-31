#include <Adafruit_PM25AQI.h>

#include <PubSubClient.h>
#include <SoftwareSerial.h>
#include <ESP8266WiFi.h>
#include "Adafruit_PM25AQI.h"

#include "credentials.h"
// define WLAN_SSID "MyNetwork"
// define WLAN_PASS "MyPassword"

#define MQTT_SERVER      "192.168.1.2"
#define MQTT_SERVERPORT  1883                   // use 8883 for SSL

#define SOFT_TX D5
#define SOFT_RX D4

String ROOM = "kitchen";

WiFiClient espClient;
PubSubClient client(espClient);
SoftwareSerial SoftSerial(SOFT_RX, SOFT_TX); // RX, TX
Adafruit_PM25AQI aqi = Adafruit_PM25AQI();

void setup() {
  WiFi.begin(WLAN_SSID, WLAN_PASS);
  client.setServer(MQTT_SERVER, MQTT_SERVERPORT); //connecting to mqtt server
  client.setCallback(callback);

  connectmqtt();
  Serial.begin(115200);
  SoftSerial.begin(9600);

  if (! aqi.begin_UART(&SoftSerial)) { // connect to the sensor over software serial 
    Serial.println("Could not find PM 2.5 sensor!");
    while (1) delay(10);
  }

  Serial.println("PM25 found!");
}

uint16_t pm10_standard,  ///< Standard PM1.0
  pm25_standard,       ///< Standard PM2.5
  pm100_standard;      ///< Standard PM10.0
uint16_t pm10_env,       ///< Environmental PM1.0
  pm25_env,            ///< Environmental PM2.5
  pm100_env;           ///< Environmental PM10.0
uint16_t particles_03um, ///< 0.3um Particle Count
  particles_05um,      ///< 0.5um Particle Count
  particles_10um,      ///< 1.0um Particle Count
  particles_25um,      ///< 2.5um Particle Count
  particles_50um,      ///< 5.0um Particle Count
  particles_100um;  
unsigned long last = 0;
bool updated = false;
void loop() {
  // mqtt
  if (!client.connected())
  {
    reconnect();
  }

  PM25_AQI_Data data;
  if (aqi.read(&data)) {
    pm25_standard = data.pm25_standard;
    pm100_standard = data.pm100_standard;
    pm25_env = data.pm25_env;
    pm100_env = data.pm100_env;
    particles_03um = data.particles_03um;
    particles_05um = data.particles_05um;
    particles_10um = data.particles_10um;
    particles_25um = data.particles_25um;
    particles_50um = data.particles_50um;
    particles_100um = data.particles_100um;
    updated = true;

    Serial.println();
    Serial.println(F("---------------------------------------"));
    Serial.println(F("Concentration Units (standard)"));
    Serial.println(F("---------------------------------------"));
    Serial.print(F("PM 1.0: ")); Serial.print(data.pm10_standard);
    Serial.print(F("\t\tPM 2.5: ")); Serial.print(data.pm25_standard);
    Serial.print(F("\t\tPM 10: ")); Serial.println(data.pm100_standard);
    Serial.println(F("Concentration Units (environmental)"));
    Serial.println(F("---------------------------------------"));
    Serial.print(F("PM 1.0: ")); Serial.print(data.pm10_env);
    Serial.print(F("\t\tPM 2.5: ")); Serial.print(data.pm25_env);
    Serial.print(F("\t\tPM 10: ")); Serial.println(data.pm100_env);
    Serial.println(F("---------------------------------------"));
    Serial.print(F("Particles > 0.3um / 0.1L air:")); Serial.println(data.particles_03um);
    Serial.print(F("Particles > 0.5um / 0.1L air:")); Serial.println(data.particles_05um);
    Serial.print(F("Particles > 1.0um / 0.1L air:")); Serial.println(data.particles_10um);
    Serial.print(F("Particles > 2.5um / 0.1L air:")); Serial.println(data.particles_25um);
    Serial.print(F("Particles > 5.0um / 0.1L air:")); Serial.println(data.particles_50um);
    Serial.print(F("Particles > 10 um / 0.1L air:")); Serial.println(data.particles_100um);
    Serial.println(F("---------------------------------------"));
  }
  
  if (updated && (millis() < last) || (millis() - last > 10000)) {  
    client.publish(String("aq/" + ROOM + "/pm25_standard").c_str(), String(pm25_standard).c_str());
    client.publish(String("aq/" + ROOM + "/pm100_standard").c_str(), String(pm100_standard).c_str());
    client.publish(String("aq/" + ROOM + "/pm25_env").c_str(), String(pm25_env).c_str());
    client.publish(String("aq/" + ROOM + "/pm100_env").c_str(), String(pm100_env).c_str());
    client.publish(String("aq/" + ROOM + "/particles_03um").c_str(), String(particles_03um).c_str());
    client.publish(String("aq/" + ROOM + "/particles_05um").c_str(), String(particles_05um).c_str());
    client.publish(String("aq/" + ROOM + "/particles_10um").c_str(), String(particles_10um).c_str());
    client.publish(String("aq/" + ROOM + "/particles_25um").c_str(), String(particles_25um).c_str());
    client.publish(String("aq/" + ROOM + "/particles_50um").c_str(), String(particles_50um).c_str());
    client.publish(String("aq/" + ROOM + "/particles_100um").c_str(), String(particles_100um).c_str());
    updated = false;
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
