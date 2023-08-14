import sys

from collections import namedtuple

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from random import randrange
from typing import NamedTuple, Callable, TypeVar, Tuple, List

from pyspark.sql.dataframe import DataFrame, Column


class Graph(NamedTuple):
    """The `Graph` class is the central class to express graphs"""

    Edge = namedtuple('Edge', ['src', 'dst', 'weight'])

    edges: List['Graph.Edge']
    vertices: List[Tuple[int]]


@dataclass
class Scope:
    """The `Scope` class abstarcts away inward, outward and all neighbours of a vertex"""
    class Direction(Enum):
        IN = "IN"
        OUT = "OUT"
        INOUT = "INOUT"

    inward: DataFrame
    outward: DataFrame
    all_nbr: DataFrame

    def __init__(self, edges: DataFrame):
        def __wrapper(id: str, nbr: str, dir: 'Scope.Direction'):
            return (
                edges_df
                    .withColumnRenamed(id, "id")
                    .withColumnRenamed(nbr, "nbr")
                    .withColumn("dir", F.lit(dir))
                    .select("id", "nbr", "weight", "dir")
            )
        self.inward = __wrapper("dst", "src", Scope.Direction.IN)
        self.outward = __wrapper("src", "dst", Scope.Direction.OUT)
        self.all_nbr = self.outward.union(self.inward)

    def __call__(self, dir: 'Scope.Direction') -> DataFrame:
        match dir:
            case Scope.Direction.IN:
                return self.inward
            case Scope.Direction.OUT:
                return self.outward
            case Scope.Direction.INOUT:
                return self.all_nbr
            case _:
                raise ValueError(f"Unknown {dir}")

    def __iter__(self):
        yield self.inward
        yield self.outward
        yield self.all_nbr


def graph(disjoint_sets_number: int = 1000, disjoint_set_vertices: int = 500, disjoint_set_edges: int = 1000) -> Graph:
    """Generates a graph
        Parameters:
            disjoint_sets_number (int) : total number of disjoint sets in the graph
            disjoint_set_elements (int) : total number of vertices in a disjoint set in the graph
            disjoint_set_edges (int) : total number of edges in a disjoint set in the graph

        Returns:
            Graph (Graph): a graph according to the specifications
    """
    start = 0
    end = disjoint_set_vertices - 1
    
    disjoint_set_step = max(disjoint_set_vertices, 1000)
    edges = []
    for x in range(disjoint_sets_number):
        edges += [ Graph.Edge(x, x + 1, randrange(10)) for x in range(start, end) ] + [ Graph.Edge(randrange(start, end), randrange(start, end), randrange(10)) for _ in range(disjoint_set_edges) ]
        start += disjoint_set_step
        end += disjoint_set_step

    vertices = set()

    for x, y, _ in edges:
        vertices.add((x,))
        vertices.add((y,))

    return Graph(edges, vertices)


def small_graph() -> Graph:
    """Generates a small graph for tests"""
    verties = [ (x,) for x in range(1, 7) ]
    edges = [Graph.Edge(1, 2, 1), Graph.Edge(1, 3, 5), Graph.Edge(2, 3, 1), Graph.Edge(3, 4, 1), Graph.Edge(4, 5, 1), Graph.Edge(6, 6, 1)]

    return Graph(edges, verties)


def to_dag(graph: Graph) -> Graph:
    """Turns a graph into a directed asyclic graph"""
    edges, vertices = graph
    edges = list(filter(lambda e: e.src < e.dst, edges))

    return Graph(edges, vertices)


V = TypeVar("V")
def timer(handler: Callable[[], V]):
    """Timer"""
    start = datetime.now()
    result = handler()
    print(datetime.now() - start)
    return result
