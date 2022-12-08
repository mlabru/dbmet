import json
import requests

# site DECEA
DS_SITE = "http://10.32.40.53/"
DS_URL_ICAO = DS_SITE + "api2/diagnostico/lista?dti={}&dtf={}&local={}"


ls_url = DS_URL_ICAO.format("2021-01-08 13:30", "2021-01-08 13:30", 2)

# make request
l_response = requests.request("GET", ls_url,
                              headers=ldct_header,
                              data=ldct_payload,
                              verify=False)
