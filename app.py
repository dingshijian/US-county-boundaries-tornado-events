import os
import json
import requests
import pandas as pd
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import geopandas as gpd
from shapely.ops import unary_union

# ---- SET FILE PATHS ----
county_geojson_file = "gz_2010_us_050_00_20m.json"  # Must be in your project folder
csv_path = "us-weather-events-1980-2024.csv"

# ---- GOOGLE DRIVE LARGE FILE HANDLING ----
file_id = "1WDsm4qBNcGg8MOskRcLvSRGRU41Ef6rX"
gdrive_url = f"https://drive.google.com/uc?export=download&id={file_id}"

def download_large_file_from_gdrive(destination):
    """Downloads a large Google Drive file in chunks."""
    session = requests.Session()
    response = session.get(gdrive_url, stream=True)
    
    if response.status_code == 200:
        print(f"Downloading {destination} from Google Drive...")
        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)
        print("✅ Download complete.")
    else:
        print(f"❌ Failed to download CSV. Status code: {response.status_code}")
        exit(1)

# 🔹 Download only if the file does not exist
if not os.path.exists(csv_path):
    download_large_file_from_gdrive(csv_path)

# ✅ Read the CSV File (Use Low Memory Mode for Large Files)
df_tornado = pd.read_csv(csv_path, low_memory=False)

print("📌 Columns in the CSV:", df_tornado.columns.tolist())
print("📏 File size:", os.path.getsize(csv_path), "bytes")
print("📊 Unique EVENT_TYPE values:", df_tornado["EVENT_TYPE"].unique())
print("🛠 Data type of EVENT_TYPE:", df_tornado["EVENT_TYPE"].dtype)

# ---- PROCESSING THE DATA ----
df_tornado = df_tornado[df_tornado["EVENT_TYPE"] == "Tornado"]
df_tornado = df_tornado.dropna(subset=["BEGIN_LAT", "BEGIN_LON"])

# Convert coordinates to numeric
df_tornado["BEGIN_LAT"] = pd.to_numeric(df_tornado["BEGIN_LAT"], errors="coerce")
df_tornado["BEGIN_LON"] = pd.to_numeric(df_tornado["BEGIN_LON"], errors="coerce")

# Extract year from "BEGIN_DATE_TIME"
df_tornado["YEAR"] = pd.to_datetime(df_tornado["BEGIN_DATE_TIME"], errors='coerce').dt.year

# Assign default F0 if "TOR_F_SCALE" is missing
if "TOR_F_SCALE" not in df_tornado.columns:
    df_tornado["TOR_F_SCALE"] = "F0"

# ---- FUJITA SCALE MAPPING ----
f_scale_mapping = {
    "F0": {"size": 6,  "color": "lightgreen"},
    "F1": {"size": 8,  "color": "green"},
    "F2": {"size": 10, "color": "yellowgreen"},
    "F3": {"size": 12, "color": "orange"},
    "F4": {"size": 14, "color": "orangered"},
    "F5": {"size": 16, "color": "red"}
}

# ---- LOAD COUNTY GEOJSON ----
if not os.path.exists(county_geojson_file):
    raise FileNotFoundError(f"County GeoJSON file not found: {county_geojson_file}")

with open(county_geojson_file, 'r', encoding='utf-8', errors='replace') as f:
    county_data = json.load(f)

df_counties = gpd.GeoDataFrame.from_features(county_data["features"], crs="EPSG:4326")
df_counties["geometry"] = df_counties["geometry"].simplify(tolerance=0.01, preserve_topology=True)

# ---- EXTRACT BOUNDARIES FOR MAP ----
all_boundaries = unary_union(df_counties.geometry.boundary)
combined_lats, combined_lons = [], []
if hasattr(all_boundaries, "geoms"):
    for line in all_boundaries.geoms:
        coords = list(line.coords)
        combined_lats.extend([pt[1] for pt in coords] + [None])
        combined_lons.extend([pt[0] for pt in coords] + [None])
elif all_boundaries.geom_type == "LineString":
    coords = list(all_boundaries.coords)
    combined_lats = [pt[1] for pt in coords]
    combined_lons = [pt[0] for pt in coords]

# ---- GEO LAYOUT SETTINGS ----
geo_layout = dict(
    scope='usa',
    projection=dict(type='albers usa'),
    showland=True,
    landcolor="rgb(217, 217, 217)",
    subunitcolor="rgb(255, 255, 255)",
    countrycolor="rgb(255, 255, 255)",
    lakecolor="rgb(255, 255, 255)"
)

# ---- FUNCTION TO CREATE FIGURE ----
def create_figure(selected_year):
    df_year = df_tornado[df_tornado["YEAR"] == selected_year]
    
    fig = go.Figure()

    # County boundaries trace
    fig.add_trace(go.Scattergeo(
        lat=combined_lats,
        lon=combined_lons,
        mode="lines",
        line=dict(width=2, color="black"),
        name="County Boundaries"
    ))

    # Tornado markers per Fujita scale
    for f_scale, props in f_scale_mapping.items():
        subset = df_year[df_year["TOR_F_SCALE"] == f_scale]
        if not subset.empty:
            fig.add_trace(go.Scattergeo(
                lat=subset["BEGIN_LAT"],
                lon=subset["BEGIN_LON"],
                mode="markers",
                marker=dict(
                    size=props["size"],
                    color=props["color"],
                    opacity=0.8
                ),
                name=f"Tornado {f_scale}",
                text=subset["BEGIN_DATE_TIME"],
                hoverinfo="text+name"
            ))

    fig.update_layout(
        title=f"Tornado Events - {selected_year}",
        geo=geo_layout,
        margin={"r": 0, "t": 40, "l": 0, "b": 0}
    )
    return fig

# ---- DASH APP SETUP ----
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("US Tornado Dashboard", style={"textAlign": "center"}),
    
    html.Div([
        html.Label("Select Year:"),
        dcc.Dropdown(
            id="year-dropdown",
            options=[{"label": str(year), "value": year} for year in range(1980, 2025)],
            value=1980,
            clearable=False,
            style={"width": "150px", "margin": "0 auto"}
        )
    ], style={"textAlign": "center", "padding": "10px"}),

    dcc.Graph(id="graph", figure=create_figure(1980))
])

@app.callback(
    Output("graph", "figure"),
    Input("year-dropdown", "value")
)
def update_figure(selected_year):
    return create_figure(selected_year)

# ---- DEPLOY ON RENDER ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))  # Use PORT from Render
    app.run_server(debug=True, host='0.0.0.0', port=port)
    server = app.server  # Required for Render deployment