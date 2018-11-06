from copy import deepcopy
import json
from collections import deque, OrderedDict


class DAGValidationError(Exception):
    pass


class DAG(object):
    """ Directed acyclic graph implementation. """

    def __init__(self):
        """ Construct a new DAG with no nodes. """
        self.reset()

    def node_exists(self, node_addr, graph=None):
        if not graph:
            graph = self.graph

        if node_addr in graph and 'placeholder' not in graph[node_addr]:
            return True
        return False

    def is_valid_node(self, node_addr, node_data, graph=None):
        if not graph:
            graph = self.graph

        test_graph = deepcopy(graph)
        test_graph[node_addr] = node_data
        for node in node_data['data']['prev_nodes']:
            if node not in test_graph:
                test_graph[node] = {'data': {'prev_nodes': []},
                                    'placeholder': True}
        is_valid, message = self.validate(test_graph)
        if is_valid:
            return True
        else:
            return False

    def add_node(self, node_addr, node_data, graph=None):
        """ Add a node if it does not exist yet, or error out. """
        if not graph:
            graph = self.graph

        if self.node_exists(node_addr, graph=graph):
            raise KeyError('Node {} already exists'.format(node_addr))

        if self.is_valid_node(node_addr, node_data, graph=graph):
            graph[node_addr] = node_data

            for node in node_data['data']['prev_nodes']:
                if node not in graph:
                    graph[node] = {'data': {'prev_nodes': []},
                                   'placeholder': True}
        else:
            raise DAGValidationError()

    def add_node_if_valid(self, node_addr, node_data, graph=None):
        try:
            self.add_node(node_addr, node_data, graph=graph)
        except (KeyError, DAGValidationError):
            return False

    def remove_node(self, node_addr, graph=None):
        """ Deletes this node and all edges referencing it. """
        if not graph:
            graph = self.graph
        if node_addr not in graph:
            raise KeyError('node %s does not exist' % node_addr)
        graph.pop(node_addr)

        for node in graph:
            if node_addr in graph[node]['data']['prev_nodes']:
                graph[node]['data']['prev_nodes'].remove(node_addr)

    def remove_node_if_exists(self, node_name, graph=None):
        try:
            self.remove_node(node_name, graph=graph)
        except KeyError:
            pass

    def upstream(self, node, graph=None):
        """ Returns a list of all nodes that have edges towards this node """
        if graph is None:
            graph = self.graph
        return [
            key for key in graph if node in graph[key]['data']['prev_nodes']
        ]

    def downstream(self, node, graph=None):
        """ Returns a list of all nodes this node has edges towards. """
        if graph is None:
            graph = self.graph
        if node not in graph:
            raise KeyError('node %s is not in graph' % node)
        return graph[node]['data']['prev_nodes']

    def all_downstreams(self, node, graph=None):
        """Returns a list of all nodes ultimately downstream
        of the given node in the dependency graph, in
        topological order."""
        if graph is None:
            graph = self.graph
        nodes = [node]
        nodes_seen = set()
        i = 0
        while i < len(nodes):
            downstreams = self.downstream(nodes[i], graph)
            for downstream_node in downstreams:
                if downstream_node not in nodes_seen:
                    nodes_seen.add(downstream_node)
                    nodes.append(downstream_node)
            i += 1
        return list(
            filter(
                lambda node: node in nodes_seen,
                self.topological_sort(graph=graph)
            )
        )

    def all_upstreams(self, node, graph=None):
        if graph is None:
            graph = self.graph

        upstream = self.upstream(node)
        nodes = set([node])

        while upstream:
            nodes = nodes.union(set(upstream))
            new_upstream = set()
            for node in upstream:
                new_upstream = new_upstream.union(self.upstream(node))
            upstream = new_upstream

        return list(
            filter(
                lambda node: node in nodes,
                self.topological_sort(graph=graph)
            )
        )

    def tails(self, graph=None, include_placeholders=True):
        """ Return a list of all leaves (nodes with no downstreams) """
        if graph is None:
            graph = self.graph

        if include_placeholders:
            return [
                key for key in graph if not graph[key]['data']['prev_nodes']
            ]

        return [
            key for key in graph
            if not graph[key]['data']['prev_nodes'] and
            'placeholder' not in graph[key]
        ]

    def placeholders(self, graph=None):
        if graph is None:
            graph = self.graph

        return [
            node for node in self.topological_sort()
            if 'placeholder' in graph[node]
        ]

    def reset(self):
        """ Restore the graph to an empty state. """
        self.graph = OrderedDict()

    def heads(self, graph=None):
        """ Returns a list of all nodes in the graph with no dependencies. """
        if graph is None:
            graph = self.graph

        # dependent_nodes = set(
        #     node for dependents in graph.values()
        #     for node in dependents['data']['prev_nodes']
        # )
        dependent_nodes = []
        for dependents in graph.values():
            for node in dependents['data']['prev_nodes']:
                dependent_nodes.append(node)
        dependent_nodes = set(dependent_nodes)
        return [node for node in graph.keys() if node not in dependent_nodes]

    def validate(self, graph=None):
        """ Returns (Boolean, message) of whether DAG is valid. """
        graph = graph if graph is not None else self.graph
        if self.size(graph) != 0:
            if len(self.heads(graph)) == 0:
                return (False, 'no independent nodes detected')
            try:
                self.topological_sort(graph)
            except ValueError:
                return (False, 'failed topological sort')
        return (True, 'valid')

    def topological_sort(self, graph=None):
        """ Returns a topological ordering of the DAG.

        Raises an error if this is not possible (graph is not valid).
        """
        if graph is None:
            graph = self.graph

        in_degree = {}
        for node in graph:
            in_degree[node] = 0

        for node in graph:
            for prev_node in graph[node]['data']['prev_nodes']:
                in_degree[prev_node] += 1

        queue = deque()
        for node in in_degree:
            if in_degree[node] == 0:
                queue.appendleft(node)

        sorted_nodes = []
        while queue:
            node = queue.pop()
            sorted_nodes.append(node)
            for prev_node in graph[node]['data']['prev_nodes']:
                in_degree[prev_node] -= 1
                if in_degree[prev_node] == 0:
                    queue.appendleft(prev_node)

        if len(sorted_nodes) == len(graph):
            return sorted_nodes
        else:
            raise ValueError('graph is not acyclic')

    def size(self, graph=None, include_placeholders=True):
        if graph is None:
            graph = self.graph

        if not include_placeholders:
            graph = [
                node for node in graph if 'placeholder' not in graph[node]
            ]

        return len(graph)

    def to_json(self):
        return json.dumps(self.graph)

    def from_json(self, graph_json):
        graph = json.loads(self.graph)

        if self.validate(graph):
            self.graph = graph

    def visualize(self):
        _nodes = self.topological_sort()
        heads = self.heads()
        head_downstreams = list(set([item for node in heads for item in self.downstream(node)]))  # NOQA
        tails = self.tails()
        nodes = []
        for index, node in enumerate(_nodes):
            node_data = {
                'id': node,
                'index': index
            }
            if node in tails:
                node_data['type'] = 'tail'
            elif node in heads:
                node_data['type'] = 'head'
            elif node in head_downstreams:
                node_data['type'] = 'head_candidate'
            else:
                node_data['type'] = 'middle'
            nodes.append(node_data)
        links = []

        for node in _nodes:
            for link in self.graph[node]['data']['prev_nodes']:
                links.append({'source': node, 'target': link})

        result = {
            'nodes': nodes,
            'links': links
        }
        print(json.dumps(result))


if __name__ == '__main__':
    dag = DAG()
    dag.add_node(1, {'data': {'prev_nodes': []}})
    dag.add_node(3, {'data': {'prev_nodes': [1, 2]}})
    dag.add_node(4, {'data': {'prev_nodes': [2, 3]}})
    dag.add_node(5, {'data': {'prev_nodes': [3, 4]}})
    dag.add_node(6, {'data': {'prev_nodes': [1, 2]}})
    print(dag.all_upstreams(4))
    print(dag.all_downstreams(4))
    print(dag.validate())
    print(dag.topological_sort())
    print(dag.heads())
