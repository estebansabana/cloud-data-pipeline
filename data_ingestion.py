import os
import sys
import requests
import pandas as pd
import folium
from dotenv import load_dotenv

# Load environment variables securely from .env
load_dotenv()
api_key = os.getenv("FOURSQUARE_API_KEY")

if not api_key:
    sys.exit("Error: FOURSQUARE_API_KEY not found in the .env file.")

# Foursquare Places API endpoint
URL = "https://places-api.foursquare.com/places/search"

# Headers required by the Places API (Bearer token + version header)
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {api_key.strip()}",
    "X-Places-Api-Version": "2025-06-17",
}

# Search parameters: restaurants near Chía, Cundinamarca
params = {
    "query": "restaurants",
    "ll": "4.85876,-74.05866",
    "limit": 50,
}

# --- Request to the API ---
try:
    response = requests.get(URL, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"Query successful: {len(data.get('results', []))} places found.")
except requests.exceptions.RequestException as e:
    sys.exit(f"Error: the request to the Foursquare API failed: {e}")

# --- Transform the JSON response into a table ---
try:
    df = pd.json_normalize(data["results"])
    df_clean = df[["name", "location.address", "categories",
                   "latitude", "longitude", "distance"]]
except (KeyError, ValueError) as e:
    sys.exit(f"Error: unexpected response structure from the API: {e}")

print(df_clean.head())

# Make sure the output folder exists
os.makedirs("./output", exist_ok=True)

# Save the raw dataset locally
df_clean.to_csv("./output/restaurants_raw.csv", index=False)
print("File './output/restaurants_raw.csv' generated successfully.")

# --- Geospatial visualization (optional) ---
try:
    m = folium.Map(location=[4.85876, -74.05866], zoom_start=15)
    for _, row in df_clean.iterrows():
        folium.Marker([row["latitude"], row["longitude"]],
                      popup=row["name"]).add_to(m)
    m.save("./output/map_restaurants_chia.html")
    print("Map './output/map_restaurants_chia.html' generated successfully.")
except Exception as e:
    print(f"Warning: the map could not be generated: {e}")