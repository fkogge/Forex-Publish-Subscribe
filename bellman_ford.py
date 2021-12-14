"""
Implements a directed weighted graph, via a dictionary, and detects shortest
paths and negative cycles via the Bellman-Ford algorithm.

Link to Bellman-Ford:
- https://en.wikipedia.org/wiki/Bellman%E2%80%93Ford_algorithm

Author: Francis Kogge
Date: 10/26/2021
Course: CPSC 5520 (Distributed Systems)
"""


class BellmanFordGraph(object):
    """
    Creates a graph, implemented via a dictionary. Detects shortest paths
    and negative cycles using the Bellman Ford algorithm. Edges contain two
    data elements: the edge's weight/cost, and a timestamp of when the edge was
    added.
    """

    def __init__(self):
        """
        Initializes the graph, where the graph is a dictionary of:
        - Key: vertex
        - Value: dictionary of vertices with a tuple of edge weight and
                 timestamp as its value
        """
        self.graph = {}

    def add_edge(self, from_vertex, to_vertex, weight, time):
        """
        Adds an edge between the two given vertices to the graph.
        :param from_vertex: vertex from which edge is outgoing
        :param to_vertex: vertex where edge goes to
        :param weight: edge weight
        :param time: timestamp of when the edge is added
        """
        if from_vertex not in self.graph:
            self.graph[from_vertex] = {}
        if to_vertex not in self.graph:
            self.graph[to_vertex] = {}

        self.graph[from_vertex][to_vertex] = weight

    def remove_edge(self, from_vertex, to_vertex):
        """
        Removes an edge from the graph.
        :param from_vertex: vertex to remove edge
        :param to_vertex: connected to from_vertex
        """
        del self.graph[from_vertex][to_vertex]

    def get_edge_weight(self, from_vertex, to_vertex):
        """
        Returns the weight of the given edge.
        :param from_vertex: vertex of the outgoing edge
        :param to_vertex: vertex that the other vertex is connected to
        :return: edge weight
        """
        return self.graph[from_vertex][to_vertex]

    def has_edge(self, from_vertex, to_vertex):
        """
        Returns whether an edge exists between the two given vertices.
        :param from_vertex: vertex from which there is presumably an outgoing
                            edge
        :param to_vertex: vertex that from_vertex may or may not be connected to
        :return: True if edge exists, False if not
        """
        if from_vertex not in self.graph or to_vertex not in self.graph:
            return False

        return to_vertex in self.graph[from_vertex]

    def shortest_paths(self, start_vertex, tolerance: float = 0):
        """
        Find the shortest paths (sum of edge weights) from start_vertex to
        every other vertex. Also detect if there are negative cycles and
        report one of them. Edges may be negative.

        For relaxation and cycle detection, we use tolerance. Only
        relaxations resulting in an improvement greater than tolerance are
        considered. For negative cycle detection, if the sum of weights is
        greater than -tolerance it is not reported as a negative cycle. This
        is useful when circuits are expected to be close to zero.

        :param start_vertex: start of all paths
        :param tolerance: only if a path is more than tolerance better will
              it be relaxed
        :param tolerance: floating point comparison tolerance
        :return: distance, predecessor, negative_cycle
            distance: dictionary keyed by vertex of shortest distance from
                      start_vertex to that vertex
            predecessor: dictionary keyed by vertex of previous vertex in
                         shortest path from start_vertex
            negative_cycle: None if no negative cycle, otherwise an edge,
                            (u,v), in one such cycle
        """
        distance = {}
        predecessor = {}

        for vertex in self.graph:
            distance[vertex] = float('inf')
            predecessor[vertex] = None

        distance[start_vertex] = 0  # Distance to start vertex is always 0

        # Relax edges 1 less time than the number of vertices
        for i in range(len(self.graph) - 1):
            for u, edges in self.graph.items():
                for v, weight in edges.items():
                    relaxed_dist = distance[u] + weight
                    if self.can_relax(relaxed_dist, distance[v], tolerance):
                        # Shorter distance found so relax edge and record
                        # predecessor vertex
                        distance[v] = relaxed_dist
                        predecessor[v] = u

        # Negative cycle detection
        for u, edges in self.graph.items():
            for v, weight in edges.items():
                relaxed_dist = distance[u] + weight
                if self.can_relax(relaxed_dist, distance[v], tolerance):
                    # Returning any negative edge to indicate there is a
                    # negative cycle
                    return distance, predecessor, (u, v)

        return distance, predecessor, None

    @staticmethod
    def can_relax(relaxed_dist, orig_dist, tolerance):
        """
        Returns whether there is a shorter distance to an edge, meaning it
        can be relaxed. Adds a tolerance too because floating point
        comparisons can be imprecise.
        :param relaxed_dist: relaxed edge distance
        :param orig_dist: original edge distance
        :param tolerance: floating point comparison tolerance
        :return: True if can be relaxed, false if cannot be relaxed
        """
        return relaxed_dist + tolerance < orig_dist
