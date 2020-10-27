import os
import json
import heapq
from collections import defaultdict
from decimal import Decimal
import hashlib

from collections import deque, namedtuple

week_list = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']


inf = float('inf')
Edge = namedtuple('Edge', 'start, end, cost')

def make_edge(start, end, cost=1):
    return Edge(start, end, cost)

class Graph:
    def __init__(self, edges):
        # let's check that the data is right
        wrong_edges = [i for i in edges if len(i) not in [2, 3]]
        if wrong_edges:
            raise ValueError('Wrong edges data: {}'.format(wrong_edges))
        self.edges = [make_edge(*edge) for edge in edges]

    @property
    def vertices(self):
        return set(
            sum(
                ([edge.start, edge.end] for edge in self.edges), []
            )
        )

    @property
    def neighbours(self):
        neighbours = {vertex: set() for vertex in self.vertices}
        for edge in self.edges:
            neighbours[edge.start].add((edge.end, edge.cost))

        return neighbours

    def dijkstra(self, source, dest):
        assert source in self.vertices, 'Such source node doesn\'t exist'
        distances = {vertex: inf for vertex in self.vertices}
        previous_vertices = {
            vertex: None for vertex in self.vertices
        }
        distances[source] = 0
        vertices = self.vertices.copy()

        while vertices:
            current_vertex = min(
                vertices, key=lambda vertex: distances[vertex])
            vertices.remove(current_vertex)
            if distances[current_vertex] == inf:
                break
            for neighbour, cost in self.neighbours[current_vertex]:
                alternative_route = distances[current_vertex] + cost
                if alternative_route < distances[neighbour]:
                    distances[neighbour] = alternative_route
                    previous_vertices[neighbour] = current_vertex

        path, current_vertex = deque(), dest
        while previous_vertices[current_vertex] is not None:
            path.appendleft(current_vertex)
            current_vertex = previous_vertices[current_vertex]
        if path:
            path.appendleft(current_vertex)
        return path

def compute_dij_path_total_distance(path):
    ret = 0
    if (len(path) <2):
        return inf
    for i in range(len(path) - 1):
        ret += compute_distance(path[i], path[i + 1])
    return ret


def print_dij_path(path):
    for node in path:
        print(node.asdict())

def get_stops_file_name(frm, to):
    return 'from_'+frm+'_to_'+to+'_stops.json'

def time_to_mins(time_str):
    return int(time_str[:-3]) * 60 + int(time_str[-2:])

class CustomNode:

    def __init__(self, city, lon, lat, day, time_dep, wait_node = ''):
        self.city = city
        if lon is None:
            self.lon = Decimal(0.0)
        else:
            self.lon = Decimal(lon)
        if lat is None: 
            self.lat = Decimal(0.0)
        else:
            self.lat = Decimal(lat)
        self.day = day
        self.time_dep = time_dep
        self.wait_node = wait_node

    def asdict(self):
        return {'city': self.city, 'lon' : self.lon, 'lat': self.lat, 'day' : self.day, 'time': self.time_dep, 'wait_node': self.wait_node}
    def __hash__(self):
        return hash((self.city, self.lon, self.lat, self.day, self.time_dep, self.wait_node))
    def __eq__(self, other):
        #to study
        return (self.__class__ == other.__class__) and (self.wait_node == other.wait_node and self.city == other.city and self.lon == other.lon and self.lat == other.lat and self.day == other.day and self.time_dep== other.time_dep )

def compute_distance(from_node, to_node):  
        now = time_to_mins(from_node.time_dep)
        now_day_idx = week_list.index(from_node.day)
        next_day_idx = week_list.index(to_node.day)
        nextt = time_to_mins(to_node.time_dep)
        wait_duration = 0
        # DIRTY
        if next_day_idx < now_day_idx:
            wait_duration = (len(week_list) - now_day_idx + next_day_idx) * time_to_mins('24:00') + nextt - now
        elif next_day_idx > now_day_idx:
            wait_duration = (next_day_idx - now_day_idx) * time_to_mins('24:00') + nextt - now 
        elif next_day_idx == now_day_idx:
            if  nextt > now:
                wait_duration = nextt - now
            else:
                wait_duration = time_to_mins('24:00') * len(week_list) + nextt - now
        return wait_duration

class CustomEdge:
    def __init__(self, from_node, to_node):
        self.from_node = from_node
        self.to_node = to_node
        value = compute_distance(self.from_node, self.to_node) #include wait duration between i and t
        self.dist = value
    
    def __hash__(self):
        return hash((self.from_node, self.to_node, self.dist))
    def __eq__(self, other):
        return (self.from_node == other.from_node) and (self.to_node == other.to_node) and (self.dist == other.dist)

class JsonParser:

    def __init__(self, collection_root_path):
        self.graph  = []
        self.edges_per_main_node = defaultdict(lambda:set())
      
        cities = [ f.path for f in os.scandir(collection_root_path) if f.is_dir() ]
        for city in cities:
            direct_connections = [ f.name for f in os.scandir(city) if f.is_dir() ]
            for direct_connection in direct_connections:
                city_name = city.rsplit('\\', 1)[-1]
                p = city + '\\' + direct_connection
                json_name = '\\' + get_stops_file_name(city_name, direct_connection)
                with open(p+json_name) as json_file:
                    stops_ids = json.load(json_file)
                    self.__process_stops_ids_to_graph(city_name, direct_connection, stops_ids)

    def print_graph(self):
        for edge in self.graph:
            print(edge[0].asdict())
            print(edge[1].asdict())
            print(edge[2])
            print('****')



    def __process_stops_ids_to_graph(self, city_name, direct_connection, stops_ids):
        for skey, scontent in stops_ids.items():
            flon = scontent.get('lon')
            flat = scontent.get('lat')
            schedule = scontent.get('schedule')
            for day, times in schedule.items():
                for time in times:
                    from_node = CustomNode(city=city_name, lon=flon, lat=flat, day=day, time_dep=time.get('time_dep'))
                    to_node = CustomNode(city=direct_connection, lon=time.get('to_lon'), lat = time.get('to_lat'), day = time.get('date_arr'), time_dep=time.get('time_arr'))
                    f_t_edge = CustomEdge(from_node, to_node)
                    self.graph.append((from_node, to_node, f_t_edge.dist))
                    self.edges_per_main_node[(city_name, direct_connection)].add(f_t_edge)

        # not optimal
        cross = True
        pair = None
        pair = [pair for pair in self.edges_per_main_node.keys() if pair[0] == direct_connection]
        if len(pair) == 0:
            pair = [pair for pair in self.edges_per_main_node.keys() if pair[1] == city_name]
            cross = False

        if len(pair) > 1:
            return ValueError('Something went wrong with the data structure and the hashed IDs')
        if len(pair) == 0:
            return
        else:
            end_nodes_current_city = set()  
            front_nodes_current_city = set()
            for edge in self.edges_per_main_node.get(pair[0]):
                if cross:
                    end_nodes_current_city.add(edge.from_node)
                else:
                    end_nodes_current_city.add(edge.to_node)
            for edge in self.edges_per_main_node.get((city_name, direct_connection)):
                if cross:
                    front_nodes_current_city.add(edge.to_node)
                else:
                    front_nodes_current_city.add(edge.from_node)

            for end_node in end_nodes_current_city:
                mindist = inf
                n = None
                for front_node in front_nodes_current_city:
                    n = front_node
                    n.wait_node = 'True'
                    if cross:
                        dist = compute_distance(front_node, end_node)
                        self.graph.append((n,  end_node,  dist))

                    else:
                        dist = compute_distance(end_node, front_node)
                        self.graph.append((end_node,  n,  dist))

def from_to(from_city, to_city, graph_without_random_start):
    #1. get list of "from city" nodes,  use the from_city node to find the "shortest " distance accross the list of the from city nodes
    # run dij on all possibilities with 3. ?
    #2. add these edges to the graph
    #3. list "to city" nodes, run dij against all of them with 1. ???

    from_city_available_nodes = set()
    to_city_available_nodes = set()

    for edge in graph_without_random_start:
        from_node = edge[0]
        to_node = edge[1]
        # insert thresholds / push criterias here ? so that we consider only the most relevants
        if from_node.city == from_city.city:
            from_city_available_nodes.add(from_node)
        if to_node.city == to_city.city:
            to_city_available_nodes.add(to_node)
          

    edges_cpy = graph_without_random_start.copy()

    min_dij_path_len = inf
    min_dij_path = deque()
    for from_city_node in from_city_available_nodes:
        edges_cpy.append((from_city, from_city_node, compute_distance(from_city, from_city_node)))
        #can probably optimize as all the core of the tree wont change, only the start leaves + branc and end leaves
        #to optimize, we could precompute on all the end nodes ?? and cache that?? or is recomputation more efficient
    tmp_graph = Graph(edges_cpy)
    for to_city_node in to_city_available_nodes:
        tmp_path = tmp_graph.dijkstra(from_city, to_city_node)
        tmp_dij_path_len = compute_dij_path_total_distance(tmp_path)
        if tmp_dij_path_len < min_dij_path_len:
            min_dij_path_len = tmp_dij_path_len
            min_dij_path = tmp_path
    return min_dij_path


if __name__ == '__main__':

    json_parser = JsonParser('./costa_rica_test')

    B = CustomNode(city='B',lon=0.0, lat=0.0, day='monday', time_dep='01:30')
    D = CustomNode(city='D',lon=0.0, lat=0.0, day='monday', time_dep='03:00')
    C = CustomNode(city='C',lon=0.0, lat=0.0, day='monday', time_dep='02:15')
    A = CustomNode(city='A',lon=0.0, lat=0.0, day='monday', time_dep='01:00')

    A2 = CustomNode(city='A',lon=-1.0, lat=-1.0, day='monday', time_dep='00:03')
    D2 = CustomNode(city='D',lon=-1.0, lat=-1.0, day='', time_dep='')

    final_result = from_to(A2, D2, json_parser.graph)

    json_parser.print_graph()
    print('-----------------\n')
    print(compute_dij_path_total_distance(final_result))
    print_dij_path(final_result)    