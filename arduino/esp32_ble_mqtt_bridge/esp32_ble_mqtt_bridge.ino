#include "credentials.h"
// define WLAN_SSID "MyNetwork"
// define WLAN_PASS "MyPassword"

#include <esp_task_wdt.h> // watchdog timer

// wifi & MQTT connectivity
#include <WiFi.h>
#include <PubSubClient.h>
WiFiClient espClient;
PubSubClient mqtt_client(espClient);
char mqtt_client_id[19];

#define MQTT_SERVER      "192.168.1.2"
#define MQTT_SERVERPORT  1883                   // use 8883 for SSL

// bluetooth low-energy
#include <BLEDevice.h>
#include <BLEUtils.h>
#include <BLEScan.h>
#include <BLEAdvertisedDevice.h>
int scanTime = 5; // seconds
BLEScan* pBLEScan;

// parallel cache structures
// microcontrollers are resource-constrained environments
// it is not that I don't know C well
// definitely not that
#define NAME_CACHE_SIZE 10
uint8_t nameCache_addr[NAME_CACHE_SIZE * 3];
char nameCache_name[NAME_CACHE_SIZE][25];
int nameCache_temp[NAME_CACHE_SIZE];
int nameCache_humidity[NAME_CACHE_SIZE];
unsigned long nameCache_t[NAME_CACHE_SIZE];

// LilyGo ESP32 w/ OLED--nice, but not the only thing I run this code on
#define ENABLE_OLED_DISPLAY
#ifdef ENABLE_OLED_DISPLAY
  #include <Adafruit_GFX.h>    // Core graphics library
  #include <Adafruit_ST7789.h> // Hardware-specific library for ST7789
  #include <SPI.h>

  // pinouts from https://github.com/Xinyuan-LilyGO/TTGO-T-Display
  #define TFT_MOSI 19
  #define TFT_SCLK 18
  #define TFT_CS 5
  #define TFT_DC 16
  #define TFT_RST 23
  #define TFT_BL 4

  Adafruit_ST7789 tft = Adafruit_ST7789(TFT_CS, TFT_DC, TFT_MOSI, TFT_SCLK, TFT_RST);

  void setupDisplay() {
    pinMode(TFT_BL, OUTPUT);      // TTGO T-Display enable Backlight pin 4
    digitalWrite(TFT_BL, HIGH);   // T-Display turn on Backlight
    tft.init(135, 240);           // Initialize ST7789 240x135
    tft.cp437(true);
    tft.fillScreen(ST77XX_BLACK);
    tft.setRotation(1);
  }

  void padText(char text[], int amt, bool padLeft) {
    if(padLeft) {
      for(int i=0; i<(amt - strlen(text)); i++) {
        tft.print(' ');
      }
    }

    tft.print(text);

    if(!padLeft) {
      for(int i=0; i<(amt - strlen(text)); i++) {
        tft.print(' ');
      }
    }
  }

  void refreshDisplay() {
    tft.setTextWrap(false);
    tft.setCursor(0, 0);

    // iterate through our cache structures and print out device names and the
    // time since they were observed, ordering by most recently seen
    int totalDisplayed = 0;
    long lastMaxThreshold = 0;
    int curMaxIndex = -1;
    unsigned long curMax = 0;
    unsigned long now = millis();
    do {
      curMaxIndex = -1;

      for(int i=0; i<NAME_CACHE_SIZE; i++) {
        if ((nameCache_t[i] > curMax) && ((lastMaxThreshold == 0) || (nameCache_t[i] < lastMaxThreshold))) {
          curMaxIndex = i;
          curMax = nameCache_t[i];
        }
      }

      if ((curMaxIndex > -1) && (strlen(nameCache_name[curMaxIndex]) > 0)) {
        lastMaxThreshold = curMax;
        tft.setTextSize(2);
        tft.setTextColor(ST77XX_WHITE, ST77XX_BLACK);
        char shortname[7];
        strncpy(shortname, &nameCache_name[curMaxIndex][2], 6);
        shortname[6] = '\0';
        padText(shortname, 6, false);

        // latency
        if((now - curMax) < 180000) {
          // less than 3m
          tft.setTextColor(ST77XX_GREEN, ST77XX_BLACK);
        }
        else if((now - curMax) < 360000) {
          // less than 6m
          tft.setTextColor(ST77XX_YELLOW, ST77XX_BLACK);
        }
        else {
          tft.setTextColor(ST77XX_RED, ST77XX_BLACK);
        }
        char seconds[6];
        itoa(min(9999, int(floor((now - curMax) / 1000))), seconds, 10);
        seconds[5] = '\0';
        padText(seconds, 5, true);
        tft.print("s ");


        // temperature
        if ((nameCache_temp[curMaxIndex] >= 72) && (nameCache_temp[curMaxIndex] <= 78)) {
          tft.setTextColor(ST77XX_GREEN, ST77XX_BLACK);
        }
        else if (nameCache_temp[curMaxIndex] < 72) {
          tft.setTextColor(ST77XX_CYAN, ST77XX_BLACK);
        }
        else {
          tft.setTextColor(ST77XX_RED, ST77XX_BLACK);
        }
        char temp[3];
        itoa(nameCache_temp[curMaxIndex], temp, 10);
        temp[2] = '\0';
        padText(temp, 2, true);
        tft.print("F ");

        // humidity
        if ((nameCache_humidity[curMaxIndex] >= 25) && (nameCache_humidity[curMaxIndex] <= 55)) {
          tft.setTextColor(ST77XX_GREEN, ST77XX_BLACK);
        }
        else if ((nameCache_humidity[curMaxIndex] >= 15) && (nameCache_humidity[curMaxIndex] <= 65)) {
          tft.setTextColor(ST77XX_YELLOW, ST77XX_BLACK);
        }
        else {
          tft.setTextColor(ST77XX_RED, ST77XX_BLACK);
        }
        char humidity[3];
        itoa(nameCache_humidity[curMaxIndex], humidity, 10);
        humidity[2] = '\0';
        padText(humidity, 2, true);
        tft.println('%');

        totalDisplayed = totalDisplayed + 1;
        curMax = 0;
      }
      else {
        Serial.println("### did not find a candidate to display");
      }
    } while((curMaxIndex > -1) && (totalDisplayed < NAME_CACHE_SIZE));
  }
#else
  void setupDisplay() { return; }
  void refreshDisplay() { return; }
#endif

// look up cache location based on 3 LSB of BLE MAC
int cacheIndex(bool oldest, uint8_t b0, uint8_t b1, uint8_t b2) {
  if (!oldest) {
    for (int i = 0; i < NAME_CACHE_SIZE; i++) {
      if ((nameCache_addr[(i*3)] == b0) && (nameCache_addr[(i*3)+1] == b1) && (nameCache_addr[(i*3)+2] == b2))
        return i;
    }
    return -1;
  }
  else {
    long n = millis();
    int m = 0;
    for(int i = 0; i < NAME_CACHE_SIZE; i++) {
      if (nameCache_t[i] < n) {
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
        int ci = cacheIndex(false, *(*m_address + 3), *(*m_address + 4), *(*m_address + 5));

        if (strlen(deviceName) == 0) {
          Serial.println("nameless device!");
          if (ci >= 0) {
            strcpy(deviceName, nameCache_name[ci]);
          }
        }
        else {
          // not all BLE advertisements carry the device name
          // we stash names here so they can be applied to nameless ads based on MAC
          if (ci < 0) {
            ci = cacheIndex(true, 0, 0, 0);
            strncpy(nameCache_name[ci], deviceName, 25);
            nameCache_name[ci][24] = '\0'; // ensure final byte is null
            nameCache_addr[(ci*3)] = *(*m_address + 3);
            nameCache_addr[(ci*3) + 1] = *(*m_address + 4);
            nameCache_addr[(ci*3) + 2] = *(*m_address + 5);
          }
        }

        // temperature
        float celsius = (float) (((256 * ((uint8_t) strServiceData[7])) + ((uint8_t) strServiceData[6])) / 100.0);

        // humidity
        float humidity = (float) (((256 * ((uint8_t) strServiceData[9])) + ((uint8_t) strServiceData[8])) / 100.0);

        // store this as latest observation (used for OLED display)
        if (ci >= 0) {
          nameCache_t[ci] = millis();
          nameCache_temp[ci] = (int) ((celsius * 1.8) + 32);
          nameCache_humidity[ci] = int(humidity);
        }

        Serial.printf("Celsius %0.2f Humidity %0.1f\n", celsius, humidity);

        // publish data to MQTT server
        if (mqtt_client.connected()) {
          Serial.println("publishing...");
          char topic[128] = "xiaomi_mijia/";
          strcat(topic, deviceName);
          strcat(topic, "/temperature");
          mqtt_client.publish(topic, String(celsius, 2).c_str());

          topic[0] = '\0';
          strcat(topic, "xiaomi_mijia/");
          strcat(topic, deviceName);
          strcat(topic, "/humidity");
          mqtt_client.publish(topic, String(humidity, 1).c_str());

          // reset watchdog timer -- failing to do this for 900s will restart the uC
          esp_task_wdt_reset();
        }
        else {
          Serial.println("...but not connected");
        }
      }
    }
};

void setup() {
  Serial.begin(115200);

  // set MQTT ID
  uint64_t chipid = ESP.getEfuseMac(); // The chip ID is essentially its MAC address(length: 6 bytes).
  uint16_t chip = (uint16_t)(chipid >> 32);
  snprintf(mqtt_client_id, 19, "ESP32-%04X%08X", chip, (uint32_t)chipid);


  // initialize esp32 watchdog to 900s
  // handles wifi disassociation, among other things
  esp_task_wdt_init(900, true); //enable panic so ESP32 restarts on disconnect
  esp_task_wdt_add(NULL); //add current thread to WDT watch

  // initialize OLED, if present
  setupDisplay();

  // initialize device name cache
  for(uint8_t i = 0; i < NAME_CACHE_SIZE; i++) {
    nameCache_t[i] = millis();
    for(uint8_t j = 0; j < 3; j++) {
      nameCache_addr[(i*3)+j] = 0;
    }
  }
  // connect to wifi
  WiFi.begin(WLAN_SSID, WLAN_PASS);

  // connect to MQTT server
  mqtt_client.setServer(MQTT_SERVER, MQTT_SERVERPORT);
  mqtt_client.setCallback(mqtt_callback);
  connect_mqtt();

  // begin BLE device scan for advertisements
  BLEDevice::init("");
  pBLEScan = BLEDevice::getScan(); //create new scan
  pBLEScan->setAdvertisedDeviceCallbacks(new MyAdvertisedDeviceCallbacks());
  pBLEScan->setActiveScan(true); //active scan uses more power, but get results faster
  pBLEScan->setInterval(100);
  pBLEScan->setWindow(99);  // less or equal setInterval value
}

void loop() {
  mqtt_reconnect(); // returns immediately if connected

  BLEScanResults foundDevices = pBLEScan->start(scanTime, false);

  //Serial.print("Devices found: ");
  //Serial.println(foundDevices.getCount());
  //Serial.println("Scan done!");

  pBLEScan->clearResults();   // delete results from BLEScan buffer to release memory

  // handle mqtt messages
  mqtt_client.loop();

  refreshDisplay();

  // wait for the scan & resulting processing to complete before starting a new one
  delay((scanTime * 2) * 1000);
}

// unused, we only publish data, not watch for it
void mqtt_callback(char* topic, byte* payload, unsigned int length) {   // callback includes topic and payload ( from which (topic) the payload is comming)
  //mqtt_client.publish("outTopic", "LED turned OFF");
}

void mqtt_reconnect() {
  while (!mqtt_client.connected()) {
    if (mqtt_client.connect(mqtt_client_id)) {
      Serial.print("connected as ");
      Serial.println(mqtt_client_id);
    }
    else {
      // Wait 5 seconds before retrying
      delay(5000);
    }
  }
}

void connect_mqtt()
{
  mqtt_client.connect(mqtt_client_id);

  if (!mqtt_client.connected())
  {
    mqtt_reconnect();
  }
}