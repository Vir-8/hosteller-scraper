import requests
import json
import pandas as pd
from retry import retry
from datetime import datetime, timedelta
import concurrent.futures
import datetime as dt
import os

# Load configuration from config.json
with open("config.json") as config_file:
    config = json.load(config_file)

MAX_RETRIES = config["max_retries"]
RETRY_DELAY_SECONDS = config["retry_delay_seconds"]
MAX_WORKERS = config["max_num_of_threads"]

DATA_URL = config["graphql_url"]
DATA_FOR_DAYS = config["duration_days"]

BASE_HEADER = config["headers"][0]["base_header"]
DATA_HEADER = config["headers"][0]["data_header"]


room_data_dict = {}
timestamp = 0


@retry(
    requests.exceptions.RequestException, tries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS
)
def get_all_hostels():
    # Define directories and path to store raw data
    global timestamp
    timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    os.makedirs(os.path.join("raw", f"{timestamp}", "hostels"), exist_ok=True)
    raw_data_file_path = os.path.join("raw", f"{timestamp}", "hostels", "hostels.json")

    # Reqest data
    response = requests.request("GET", config["get_hostels_url"], headers=BASE_HEADER)

    # Store raw data
    with open(raw_data_file_path, "w") as json_file:
        json.dump(response.json(), json_file, indent=4)

    return response.json()["pageProps"]["hostels"]


@retry(
    requests.exceptions.RequestException, tries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS
)
def get_hostel_details(hostel):
    # Get Hostel ID and name

    # Get the hostel slug for details request
    slug = hostel["url"][len("/hostels/") :]

    # Define directories and path to store raw data
    os.makedirs(
        os.path.join("raw", f"{timestamp}", "hostels", f"{slug}"), exist_ok=True
    )
    raw_data_file_path = os.path.join(
        "raw", f"{timestamp}", "hostels", f"{slug}", f"{slug}.json"
    )

    response = requests.request(
        "GET",
        f"{config['hostel_id_url']}{slug}.json?hostelURL={slug}",
        headers=BASE_HEADER,
    )

    # Store raw data
    with open(raw_data_file_path, "w") as json_file:
        json.dump(response.json(), json_file, indent=4)

    hostel_id = response.json()["pageProps"]["hostelDetails"]["_id"]
    hostel_name = response.json()["pageProps"]["hostelDetails"]["name"]
    try:
        generate_session(hostel_id, hostel_name, slug)
    except Exception as e:
        print("Skipping.. Error generating session: ", e)


@retry(
    requests.exceptions.RequestException, tries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS
)
def generate_session(hostel_id, hostel_name, slug):
    # Generate session to get hostel data

    # Define directories and path to store raw data
    os.makedirs(
        os.path.join("raw", f"{timestamp}", "hostels", f"{slug}"), exist_ok=True
    )
    raw_data_file_path = os.path.join(
        "raw", f"{timestamp}", "hostels", f"{slug}", "session.json"
    )

    # Define checkin and checkout dates for payload
    checkIn = datetime.today().strftime("%Y-%m-%d")
    checkOut = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    payload = f'{{"query":"mutation postSearchedData($checkinDate: String!, $checkoutDate: String!, $searchType: searchType!, $hostelId: ID, $tripPackageId: ID, $workationPackageId: ID) {{\\n Search(\\n hostelId: $hostelId\\n checkinDate: $checkinDate\\n checkoutDate: $checkoutDate\\n searchType: $searchType\\n tripPackageId: $tripPackageId\\n workationPackageId: $workationPackageId\\n ) {{\\n sessionId\\n hostelId\\n }}\\n}}","variables":{{"hostelId":"{hostel_id}","checkinDate":"{checkIn}","checkoutDate":"{checkOut}","searchType":"Hostel"}}}}'

    response = requests.request("POST", DATA_URL, headers=DATA_HEADER, data=payload)

    # Store raw data
    with open(raw_data_file_path, "w") as json_file:
        json.dump(response.json(), json_file, indent=4)

    session_id = response.json()["data"]["Search"]["sessionId"]
    print("Session ID: " + session_id)

    try:
        get_room_details(session_id, hostel_name, slug)
    except Exception as e:
        print("Skipping.. Error fetching room details: ", e)


@retry(
    requests.exceptions.RequestException, tries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS
)
def get_room_details(session_id, hostel_name, slug):
    # Get details for each room of a hostel

    # Define directories and path to store raw data
    os.makedirs(
        os.path.join("raw", f"{timestamp}", "hostels", f"{slug}"), exist_ok=True
    )
    raw_data_file_path = os.path.join(
        "raw", f"{timestamp}", "hostels", f"{slug}", "room_details.json"
    )

    payload = f'{{"query":"mutation SearchBySessionNew($sessionId: String!) {{\\n SearchBySessionNew(sessionId: $sessionId) {{\\n searchResults\\n }}\\n}}","variables":{{"sessionId":"{session_id}"}}}}'

    response = requests.request("POST", DATA_URL, headers=DATA_HEADER, data=payload)
    rooms = response.json()["data"]["SearchBySessionNew"]["searchResults"]

    # Store raw data
    with open(raw_data_file_path, "w") as json_file:
        json.dump(response.json(), json_file, indent=4)

    for room in rooms:
        room_id = room["roomUniqueId"]
        room_name = room["roomName"]

        try:
            get_availability_info(hostel_name, room_name, session_id, room_id, slug)
        except Exception as e:
            print("Skipping.. Error fetching availability details: ", e)


@retry(
    requests.exceptions.RequestException, tries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS
)
def get_availability_info(hostel_name, room_name, session_id, room_id, slug):
    # Get availability info including dates, units and prices

    # Define directories and path to store raw data
    os.makedirs(
        os.path.join("raw", f"{timestamp}", "hostels", f"{slug}", "availability"),
        exist_ok=True,
    )
    raw_data_file_path = os.path.join(
        "raw", f"{timestamp}", "hostels", f"{slug}", "availability", f"{room_id}.json"
    )

    payload = f'{{"query":"mutation availability($searchFilter: searchFilter) {{\\n availability(searchFilter: $searchFilter) {{\\n searchResults\\n roomAvailableMessage\\n }}\\n}}","variables":{{"searchFilter":{{"sessionId":"{session_id}","roomUniqueId":"{room_id}","page":1,"limit":{DATA_FOR_DAYS}}}}}}}'

    response = requests.request("POST", DATA_URL, headers=DATA_HEADER, data=payload)
    data = response.json()["data"]["availability"]["searchResults"]

    # Store raw data
    with open(raw_data_file_path, "w") as json_file:
        json.dump(response.json(), json_file, indent=4)

    for day in data:
        date = day["date"]
        unit = day["unit"]
        price = day["price"]

        if hostel_name not in room_data_dict:
            room_data_dict[hostel_name] = []

        room_data_dict[hostel_name].append(
            {
                "Hostel Name": hostel_name,
                "Room Name": room_name,
                "Date": date,
                "Unit": unit,
                "Price": price,
            }
        )


def main():
    hostels = get_all_hostels()

    # Get hostel details concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_hostel_details, hostel) for hostel in hostels]
    concurrent.futures.wait(futures)

    room_data_df = pd.concat(
        [pd.DataFrame(value) for value in room_data_dict.values()], ignore_index=True
    )

    room_data_df.to_csv("room_data.csv", index=False)


main()
