"""Contains functionality related to Weather"""
import logging

import json

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("WEATHER: consuming weather process_message")
#         weather_json = json.loads(message.value())
        weather_json = json.loads(json.dumps(message.value()))
        self.temperature = weather_json.get("temperature")
        self.status = weather_json.get("status")
        logger.info("consuming weather process_message ", weather_json)
