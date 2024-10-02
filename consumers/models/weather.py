"""Contains functionality related to Weather"""
import json
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        # Process incoming weather messages. Set the temperature and status.
        logger.info("weather message polled")
        try:
            print("topic", message.topic())
            message = message.value()
            print("message_value", message)

            self.temperature = message.get('temperature')
            self.status = message.get('status')

        except Exception as e:
            logger.error(f"error processing weather message, value: {message}, error: {e}")

