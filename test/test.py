import requests
from requests.structures import CaseInsensitiveDict

def geocode(address):
    key = 'bc39da4703e14cf5bb6110f0419e4cbd'
    url = f"https://api.geoapify.com/v1/geocode/search?text={address}&apiKey={key}"

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"

    resp = requests.get(url, headers=headers).json()
    ls = []
    for address in resp['features']:
        ls.append(address['properties']['formatted'])

    return ls

if __name__=='__main__':
    geocode('600 E 7TH STREET, 90021')