import requests
from logger import timed
from test_geo import geocode

# LOCATIONIQ_API_KEY = 'pk.bc7ccb19ed88617f050998f1b303b2a1'
LOCATIONIQ_API_KEY = 'pk.d0a993464896fcbd5d3a8f46b9382883'

@timed
def complete_address(address):
    url = f'https://us1.locationiq.com/v1/search.php?key={LOCATIONIQ_API_KEY}&q={address}&format=json'
    response = requests.get(url)
    res = []
    if response.status_code == 200:
        try:
            results = response.json()
            for add in results:
                res.append(add['display_name'])
            print(res)

            return res
        except:
            return geocode(address)
    else:
        # print(f"Error: {response.status_code}")
        return geocode(address)

if __name__=="__main__":
    res = complete_address("600 E 7TH STREET, 90021")
    print(res)
