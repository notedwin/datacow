import base64
import glob
import json
import math

import folium
import pandas as pd
from branca.element import IFrame
from folium.plugins import Draw, MarkerCluster
from PIL import Image


def get_geotagging(exif):
    if not exif:
        # raise ValueError("No EXIF metadata found")
        return None
    else:
        # gps_keys = {2: "GPSLatitude", 4: "GPSLongitude"}
        (deg, minutes, seconds) = exif[2]
        direction = exif[1]
        lat = (float(deg) + float(minutes) / 60 + float(seconds) / (60 * 60)) * (
            -1 if direction in ["W", "S"] else 1
        )

        (deg, minutes, seconds) = exif[4]
        direction = exif[3]
        lon = (float(deg) + float(minutes) / 60 + float(seconds) / (60 * 60)) * (
            -1 if direction in ["W", "S"] else 1
        )
        return lat, lon


def create_place_markers():
    gt = pd.read_csv("data/saved_places_gt.csv")
    name2id = pd.read_csv("data/places_id.csv")
    google_info = pd.read_csv("data/info.csv")

    gt_w_id = pd.merge(gt, name2id, on="place", how="inner")
    juan_big_table = pd.merge(gt_w_id, google_info, on="place_id", how="inner").to_dict(
        orient="records"
    )

    restaurants = folium.FeatureGroup(name="restaurants")
    marker_cluster = MarkerCluster()
    # url = "https://leafletjs.com/examples/custom-icons/{}".format
    # icon_image = url("star.png")

    # star = folium.CustomIcon(icon_image, icon_size=(38, 95), icon_anchor=(22, 94))

    uniq = set()
    for x in juan_big_table:
        # print(x)
        name = x.get("name", "NULL")
        rating = x.get("rating", 0)
        ed_rating = x.get("ed_rating", 0)
        ed_notes = x.get("note", "")

        ab_rating = x.get("ab_rating", 0)
        ab_notes = x.get("ab_note", "")

        num_ratings = x.get("user_ratings_total", 0)

        type_ = x.get("types", "")
        url = x.get("url", "")
        web = x.get("website", "")
        price = x.get("price_level", "")
        price = int(price) if not math.isnan(price) else None

        num_visited = x.get("times_visited", 0)
        geo = (x.get("lat", 0), x.get("lon", 0))

        ty = sorted(json.loads(type_.replace("'", '"')))
        ty = list(
            filter(
                lambda x: x not in ["establishment", "point_of_interest", "food"], ty
            )
        )

        for t in ty:
            uniq.add(t)

        html = f"""
        <div style="background-color: #ccc; font-size: 1rem; color: #222; line-height: 1.5; padding: 5px">
            <h3>{name}</h3>
            <a target=" _blank" rel="noopener noreferrer" href="{web}"> visit {name}'s website</a>
            <br />
            <b>Thing 1's rating: {ed_rating}/5</b>
            <br />
            <b>Thing 2's rating: {ab_rating}/5</b>
            <br />
            <b>Visited: {num_visited} times.</b>
            <br />
            <p>
                Edwin's notes: {ed_notes}
                <br />
                Abril's notes: {ab_notes}
            </p>
            
            
            <a target="_blank" rel="noopener noreferrer" href="{url}"> Google Info</a>
            <br />
            {
                "<b>Price Level: " + "$" * int(price) + "</b>"
                if price
                else ""
            }
            <br />
            <b>{num_ratings} ratings averaging rating of {rating}</b>
            <br />
            <a>type of place: {type_}</a>
        </div>

        """

        iframe = IFrame(html=html, width=300, height=400)
        popup = folium.Popup(iframe, max_width=500)

        if "restaurant" not in ty and "cafe" not in ty:
            print(f"place: {name}, type: {ty}")

        folium.Marker(
            geo,
            popup=popup,
            icon=folium.Icon(prefix="fa", color="beige", icon="star")
            if ed_rating >= 4
            else None,
        ).add_to(marker_cluster)

    return restaurants.add_child(marker_cluster)


def create_image_markers():
    images = folium.FeatureGroup(name="images")
    image_cluster = MarkerCluster()
    for filename in glob.glob("assets/images/*.jpg"):
        encoded = base64.b64encode(open(filename, "rb").read())
        html = '<img src="data:image/JPEG;base64,{}">'.format
        popup = folium.Popup(html(encoded.decode("UTF-8")))

        im = Image.open(filename)
        exif_dict = im.getexif().get_ifd(0x8825)
        gps_coord = get_geotagging(exif_dict)

        image_cluster.add_child(
            folium.Marker(
                gps_coord,
                icon=folium.CustomIcon(
                    filename,
                    icon_size=(40, 60),
                ),
                popup=popup,
            )
        )
    return images.add_child(image_cluster)


def create_nh():
    neighborhoods = folium.FeatureGroup(name="neighborhoods", show=False)
    hood_labels = pd.read_csv("data/hoods.csv")

    for x in hood_labels.to_dict(orient="records"):
        name = x.get("Name")
        lat = x.get("Latitude")
        lon = x.get("Longitude")
        votes = x.get("Votes", 0)

        popup = f"""
            <div style="font-weight: bold">
            {name}
            <div/>
        """

        folium.Marker(
            (lat, lon),
            icon=folium.DivIcon(
                icon_size=(100, 36),
                icon_anchor=(0, 0),
                html=f'<div style="font-size: 1.5rem;color:#eee">{name} votes: {votes}</div>',
            ),
            popup=popup,
        ).add_to(neighborhoods)

    return neighborhoods


def create_boundaries():
    nh_bounds = folium.FeatureGroup(name="neighborhood_boundaries", show=False)
    popup = folium.GeoJsonPopup(
        fields=["name"],
        aliases=["Neighborhood"],
        localize=True,
        labels=True,
        style="background-color: yellow;",
    )

    tooltip = folium.GeoJsonTooltip(
        fields=["name"],
        aliases=["Neighborhood"],
        localize=True,
        sticky=True,
        labels=True,
        style="""
            background-color: #F0EFEF;
            border: 2px solid black;
            border-radius: 3px;
            box-shadow: 3px;
        """,
        max_width=800,
    )

    folium.GeoJson(
        data=(open("data/chi.geojson", "r").read()),
        name="geojson",
        zoom_on_click=True,
        style_function=lambda feature: {
            "fillColor": "#ff6666",
            "color": "#CCC",
            "weight": 2,
            "dashArray": "5, 5",
            "fillOpacity": 0.2,
        },
        tooltip=tooltip,
        popup=popup,
    ).add_to(nh_bounds)
    return nh_bounds


# def create_routes():
#     routes = folium.FeatureGroup(name="routes")
#     gpx_path = "./data/apple_health_export/workout-routes/"
#     gpx_files = glob.glob(os.path.join(gpx_path, "*.gpx"))

#     for file_idx, gpx_file in enumerate(gpx_files):
#         gpx = gpxpy.parse(open(gpx_file, "r"))
#         points = []
#         for track in gpx.tracks:
#             for segment in track.segments:
#                 step = 50
#                 for point in segment.points[::step]:
#                     points.append(tuple([point.latitude, point.longitude]))

#         folium.PolyLine(
#             points, color="red", weight=5, opacity=0.55
#         ).add_to(routes)
#     return routes


if __name__ == "__main__":
    map = folium.Map(
        location=[41.8846976, -87.647283],
        tiles="https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png",
        attr="""&copy; <a target="_blank" rel="noopener noreferrer" href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>""",
        zoom_start=14,
        prefer_canvas=True,
    )
    map.add_child(create_place_markers())
    map.add_child(create_image_markers())
    map.add_child(create_nh())
    map.add_child(create_boundaries())
    # map.add_child(create_routes())
    folium.LayerControl(overlay=False, collapsed=False).add_to(map)
    Draw(export=True).add_to(map)
    map.save("output/map.html")
