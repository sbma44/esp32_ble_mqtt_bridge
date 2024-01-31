from setuptools import setup

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
   name='sensor_logging',
   version='1.0',
   description='Consumes MQTT messages, storing them in an in-memory sqlite db with periodic uploads to S3',
   license="MIT",
   long_description=long_description,
   author='Tom Lee',
   author_email='thomas.j.lee@gmail.com',
   url="https://github.com/sbma44/sensor_logging",
   packages=['sensor_logging'],
   install_requires=['paho-mqtt', 'redis', 'boto3', 'troposphere']
)