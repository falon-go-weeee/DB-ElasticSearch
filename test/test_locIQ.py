import requests
import time
import logging
from functools import wraps
from app.logger import timed

LOCATIONIQ_API_KEY = 'pk.bc7ccb19ed88617f050998f1b303b2a1'

@timed
def get_geocode(address):
    url = f'https://us1.locationiq.com/v1/search.php?key={LOCATIONIQ_API_KEY}&q={address}&format=json'
    response = requests.get(url)
    
    if response.status_code == 200:
        results = response.json()
        return results
    else:
        print(f"Error: {response.status_code}")
        return None

address = '1 Embarcadero street San Francisco'
geocode_json = get_geocode(address)
print(geocode_json)