from weather.utils import get_weather_api
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')

class WeatherAPI:
    # def store_weather_info(self, api_key, city_name, file_name=WEATHER_DATA_FILE):
    def get_weather_info(self, city_name):
        """Stores weather data for specified city at the desired file path(JSON format)."""
        return get_weather_api(api_key=WEATHER_API_KEY, city_name=city_name)