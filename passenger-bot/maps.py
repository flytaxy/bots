import os
import requests
import polyline as pl
import folium
from geopy.geocoders import GoogleV3
from dotenv import load_dotenv

load_dotenv()
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")


def build_route(start_coords, destination_address):
    """Будує маршрут, повертає (координати призначення, дистанцію, шлях до PNG карти)"""
    geolocator = GoogleV3(api_key=GOOGLE_MAPS_API_KEY)
    location = geolocator.geocode(destination_address)

    if not location:
        return None, None, None

    dest_coords = (location.latitude, location.longitude)

    directions_url = f"https://maps.googleapis.com/maps/api/directions/json?origin={start_coords[0]},{start_coords[1]}&destination={dest_coords[0]},{dest_coords[1]}&key={GOOGLE_MAPS_API_KEY}&language=uk"
    response = requests.get(directions_url).json()

    if not response["routes"]:
        return None, None, None

    distance_text = response["routes"][0]["legs"][0]["distance"]["text"]
    distance_km = float(distance_text.replace(" км", "").replace(",", ".").strip())

    # Полілінія маршруту
    polyline_data = response["routes"][0]["overview_polyline"]["points"]
    points = pl.decode(polyline_data)

    # Створюємо folium карту
    m = folium.Map(location=start_coords, zoom_start=13)
    folium.Marker(
        start_coords, tooltip="Старт", icon=folium.Icon(color="green")
    ).add_to(m)
    folium.Marker(
        dest_coords, tooltip="Призначення", icon=folium.Icon(color="red")
    ).add_to(m)
    folium.PolyLine(points, color="blue", weight=5).add_to(m)

    m.save("route_map.html")

    # PNG через Static Maps API
    static_map_url = (
        f"https://maps.googleapis.com/maps/api/staticmap?"
        f"size=600x400&path=enc:{polyline_data}"
        f"&markers=color:green|{start_coords[0]},{start_coords[1]}"
        f"&markers=color:red|{dest_coords[0]},{dest_coords[1]}"
        f"&key={GOOGLE_MAPS_API_KEY}"
    )
    img_data = requests.get(static_map_url).content
    map_file = "route_map.png"
    with open(map_file, "wb") as f:
        f.write(img_data)

    return dest_coords, distance_km, map_file
