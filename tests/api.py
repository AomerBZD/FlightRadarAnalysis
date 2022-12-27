from pprint import pformat

from FlightRadar24.api import FlightRadar24API
from FlightRadar24.flight import Flight

def flatten_dict(d: dict, parent_key: str = '', sep: str ='_') -> dict:
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _recursive_cast(obj, type_, default_):
    for k, v in obj.items():
        if isinstance(v, dict):
            obj[k] = _recursive_cast(v, type_, default_)
        elif isinstance(v, list):
            obj[k] = [_recursive_cast(_, type_, default_) for _ in v]
        else:
            try:
                obj[k] = type_(v)
            except (ValueError, TypeError):
                obj[k] = default_
    return obj


def flight_details_cleaning(flight_detail):
    # retreive historical and images data
    flight_detail.pop("aircraft_history")
    flight_detail.pop("aircraft_images")
    # remove temporarly time details information
    time_details = flight_detail.pop("time_details")
    trail = flight_detail.pop('trail')
    # convert to string all remaining fields
    flight_detail = _recursive_cast(flight_detail, str, "N/A")
    # time details linearization
    time_details = _recursive_cast(flatten_dict(time_details, "time_details"), int, 0)
    flight_detail.update(time_details)
    # trail data conversion
    trail_keys = ["alt", "hd", "lat", "lng", "spd", "ts"]
    flight_detail.update({"trail_" + k: [_[k] for _ in trail] for k in trail_keys})
    return flight_detail


if __name__ == "__main__":
    fr_api = FlightRadar24API()
    flights = fr_api.get_flights()
    details = fr_api.get_flight_details(flights[-1].id)    
    flights[-1].set_flight_details(details)
    flight_detail = flight_details_cleaning(flights[-1].__dict__)
    print(pformat(flight_detail))

