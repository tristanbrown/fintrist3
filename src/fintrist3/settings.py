"""
This is the config file for fintrist, containing various parameters the user may
wish to modify.
"""
import os
from dotenv import load_dotenv

load_dotenv()

class ConfigObj():
    APIKEY_AV = os.getenv('APIKEY_AV')
    APIKEY_TIINGO = os.getenv('APIKEY_TIINGO')
    APIKEY_IEX = os.getenv('APIKEY_IEX')
    TZ = os.getenv('TIMEZONE') or 'UTC'

Config = ConfigObj()
