import tkinter as tk
from tkinter import *
from tkinter import ttk
import os
import folium
from time_dji_mvp import *

class CustomNodeDisplay:
    def __init__(self, city, lat, lon):
        self.city = city
        self.lat = lat
        self.lon = lon
    def __eq__(self, other):
        return (self.city == other.city) and (self.lat == other.lat) and (self.lon == other.lon)
    def __hash__(self):
        return hash((self.city, self.lat, self.lon))

def display_cities_in_map(graph, MAP):
    tmp = set()
    for edge in graph:
        frm = edge[0]
        to = edge[1]
        dist = edge[2]
        tmp.add(CustomNodeDisplay(frm.city, frm.lat, frm.lon))        
    for el in tmp:   
        folium.Marker([el.lat.real, el.lon.real], popup=el.city, icon=folium.Icon(color='red', 
                    icon_color='yellow',icon = 'cloud')).add_to(MAP)

def display_path_in_map(path, MAP):
    points = []
    for node in path:        
        points.append([node.lat.real, node.lat.real])
        folium.Marker([node.lat.real, node.lat.real], icon= folium.Icon(color='red', 
                  icon_color='yellow',icon = 'cloud')).add_to(MAP)
    folium.PolyLine(points).add_to(MAP)





if __name__ == "__main__":

    latitude = 9.6302
    longitude = -84.2542
    MAP = folium.Map(location=[latitude, longitude], zoom_start=8)
    costa_rica_map = 'costa_rica_map.html'

    os.chdir('.')
    FROM_BAGACES = CustomNode(city='Bagaces',lat=10.522919, lon=-85.254421, day='monday', time_dep='05:15', family = None)

    json_parser = JsonParser('./costa_rica_test', departure_slot = FROM_BAGACES)
    display_cities_in_map(json_parser.graph, MAP)

    TO_PALMARES = CustomNode(city='Palmares',lat=10.0674032568169, lon=-84.439651966095, day='', time_dep='', family = None)

    TO_LIBERIA = CustomNode(city='Liberia',lat=-14.0, lon=-140, day='', time_dep='', family = None)

    DEBUG_LIBERIATO = CustomNode(city='Liberia', lat=Decimal('10.6293285846986'),
     lon=Decimal('85.4429429769516'), day='monday', time_dep='12:15', family = None)

 #   fr = from_to_debug(FROM_BAGACES, DEBUG_LIBERIATO, json_parser.graph, rosetta=True)
    final_result = from_to_all(FROM_BAGACES, TO_LIBERIA, json_parser.graph, rosetta=True, debug=DEBUG_LIBERIATO)
    final_result2 = from_to_optimized(FROM_BAGACES, TO_LIBERIA, json_parser.graph, rosetta=True)

    #display_path_in_map(final_result, MAP)
    MAP.save(costa_rica_map)

    ttt = True





