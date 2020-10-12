import os
import json
import heapq
from collections import defaultdict
from decimal import Decimal

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

    def get_node_pairs(self, n1, n2, both_ends=True):
        if both_ends:
            node_pairs = [[n1, n2], [n2, n1]]
        else:
            node_pairs = [[n1, n2]]
        return node_pairs

    def remove_edge(self, n1, n2, both_ends=True):
        node_pairs = self.get_node_pairs(n1, n2, both_ends)
        edges = self.edges[:]
        for edge in edges:
            if [edge.start, edge.end] in node_pairs:
                self.edges.remove(edge)

    def add_edge(self, n1, n2, cost=1, both_ends=True):
        node_pairs = self.get_node_pairs(n1, n2, both_ends)
        for edge in self.edges:
            if [edge.start, edge.end] in node_pairs:
                return ValueError('Edge {} {} already exists'.format(n1, n2))

        self.edges.append(Edge(start=n1, end=n2, cost=cost))
        if both_ends:
            self.edges.append(Edge(start=n2, end=n1, cost=cost))

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




def get_stops_file_name(frm, to):
    return 'from_'+frm+'_to_'+to+'_stops.json'



def time_to_mins(time_str):
    return int(time_str[:-3]) * 60 + int(time_str[-2:])

class CustomNode:
    def __init__(self, city, lon, lat, day, time_dep):
        self.city = city
        self.lon = Decimal(lon)
        self.lat = Decimal(lat)
        self.day = day
        self.time_dep = time_dep
    def __hash__(self):
        return hash((self.city, self.lon, self.lat, self.day, self.time_dep))
    def __eq__(self, other):
        #to study
        return (self.__class__ == other.__class__) and (self.city == other.city and self.lon == other.lon and self.lat == other.lat and self.day == other.day and self.time_dep== other.time_dep )
    
class CustomEdge:
    def __init__(self, from_node, to_node):
        self.from_node = from_node
        self.to_node = to_node
        self.dist = self.__compute_distance(self.from_node, self.to_node) #include wait duration between i and t
    
    def __hash__(self):
        return hash((self.from_node, self.to_node, self.dist))

    def __compute_distance(self, from_node, to_node):
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

class JsonParser:
    def __init__(self, collection_root_path):
       # self.graph = Graph()
        self.g  = []
        self.edges = []
        self.stacknode1 = []
        self.stacknode2 = []

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

    def __process_stops_ids_to_graph(self, city_name, direct_connection, stops_ids):
        
        for skey, scontent in stops_ids.items():
            flon = scontent.get('lon')
            flat = scontent.get('lat')
            schedule = scontent.get('schedule')
            for day, times in schedule.items():
                for time in times:
                    from_node = CustomNode(city=city_name, lon=flon, lat=flat, day=day, time_dep=time.get('time_dep'))
                    # 2 passes for the to_node and edge, to_node gets completed on second roung, edge gets the waiting time added as the to_node is completed ???????
                    to_node = CustomNode(city=direct_connection, lon=time.get('to_lon'), lat = time.get('to_lat'), day = time.get('date_arr'), time_dep=time.get('time_arr'))
                    f_t_edge = CustomEdge(from_node, to_node)
                    self.edges.append(f_t_edge)
                    self.g.append((from_node, to_node, f_t_edge.dist))
                    if from_node not in self.stacknode1:
                        self.stacknode1.append(from_node)
                    else:
                        tt = True
                    if to_node not in self.stacknode2:
                        self.stacknode2.append(to_node)
                    else:
                        tt = True

                   # self.graph.add_node(from_node)
                   # self.graph.add_node(to_node)
                   # self.graph.add_edge(f_t_edge)

class CustomDijkstra:
    def __init__(self, graph):
        pass
    def calculate_distances(self, graph, starting_vertex):
        distances = {vertex: float('infinity') for vertex in graph}
        distances[starting_vertex] = 0

        pq = [(0, starting_vertex)]
        while len(pq) > 0:
            current_distance, current_vertex = heapq.heappop(pq)

            # Nodes can get added to the priority queue multiple times. We only
            # process a vertex the first time we remove it from the priority queue.
            if current_distance > distances[current_vertex]:
                continue

            for neighbor, weight in graph[current_vertex].items():
                distance = current_distance + weight

                # Only consider this new path if it's better than any path we've
                # already found.
                if distance < distances[neighbor]:
                    distances[neighbor] = distance
                    heapq.heappush(pq, (distance, neighbor))

        return distances

if __name__ == '__main__':
#  t1 = CustomNode('a', 'a', 'a', 'a', 'a')
#  t2 = CustomNode('a', 'a', 'a', 'a', 'a')
#
#  ss = set()
#  ss.add(CustomNode('a', 'a', 'a', 'a', 'a'))
#  ss.add(CustomNode('a', 'a', 'a', 'a', 'a'))
#  ss.add(t1)

    json_parser = JsonParser('./costa_rica_test')

    graph2 = Graph([
       ("a", "b", 7),  ("a", "c", 9),  ("a", "f", 14), ("b", "c", 10),
       ("b", "d", 15), ("c", "d", 11), ("c", "f", 2),  ("d", "e", 6),
       ("e", "f", 9)])

    GRAPH = Graph(json_parser.g)
    
    print(GRAPH.dijkstra(json_parser.edges[1].from_node, json_parser.edges[1].to_node))
   # print(graph2.dijkstra('a', 'e'))

  
    test= True


    