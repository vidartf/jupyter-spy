#

"""Processing tools for Jupyter message logs"""


import json
from collections import defaultdict

import jupyter_client



class Node:
    def __init__(self, obj, parent):
        self.obj = obj
        self.parent = parent
        self.children = set()


class TreeSorter:
    def __init__(self):
        self.node_map = {}
        self._unresolved_nodes = {}

    def add_entry(self, obj, node_id, parent_id):
        try:
            parent = self.node_map[parent_id]
        except KeyError:
            parent = Node(None, None)
            self._unresolved_nodes[parent_id] = parent

        try:
            node = self._unresolved_nodes.pop(node_id)
            node.parent = parent
            node.obj = obj
        except KeyError:
            node = Node(obj, parent)
        self.node_map[node_id] = node
        parent.children.add(node)

    @property
    def root_nodes(self):
        return tuple(self._unresolved_nodes.values())


class Processor:
    def __init__(self, logfile):
        self.logfile = logfile
        with open(self.logfile, encoding='utf8') as f:
            self._content = json.load(f)
        self._map = None
        self._tree = None

    def free_caches(self):
        self._map = None
        self._tree = None

    @property
    def entries(self):
        yield from self._content

    @property
    def count(self):
        return len(self._content)

    @property
    def map(self):
        if self._map is None:
            self._map = {e['msg_id']: e for e in self.entries}
        return self._map

    @property
    def tree(self):
        if self._tree is None:
            self._tree = TreeSorter()
            for msg_id, entry in self.map.items():
                parent_id = entry['parent_header']['msg_id']
                self._tree.add_entry(entry, msg_id, parent_id)
        return self._tree


    @staticmethod
    def stat_node(node):
        if node.obj is None:
            msg_type = 'unknown'
        else:
            msg_type = node.obj['msg_type']
        child_types = defaultdict(int)
        for child in node.children:
            child_type, grandchild_types = Processor.stat_node(child)
            child_types[child_type] += 1
            for gc_type, gc_type_count in grandchild_types.items():
                child_types[gc_type] += gc_type_count
        return msg_type, child_types

    def msg_types(self):
        types = defaultdict(int)
        for entry in self.entries:
            types[entry['msg_type']] += 1
        return dict(types)
