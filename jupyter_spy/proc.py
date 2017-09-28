#

"""Processing tools for Jupyter message logs"""


import json
from collections import defaultdict


class Node:
    """Helper class for a tree node.

    The `obj` attribute holds the actual object that the
    node represents.
    """
    def __init__(self, obj, parent):
        self.obj = obj
        self.parent = parent
        self.children = set()


class TreeSorter:
    """Helper class for sorting messages into a tree.

    The messages can be added in any order.
    """

    def __init__(self):
        self.node_map = {}
        self._unresolved_nodes = {}

    def add_entry(self, obj, node_id, parent_id):
        """Add a message to the tree.

        The given ids will be used to identify the newly
        create node and its parent node.
        """
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
        """Any nodes whos parent is missing or unresolved."""
        return tuple(self._unresolved_nodes.values())


class Processor:
    """Log entries processor class.

    This class will load the entries from a log file captured with Spy
    and has some utility functions for working efficiently with these.
    """

    def __init__(self, logfile):
        self.logfile = logfile
        with open(self.logfile, encoding='utf8') as f:
            self._content = json.load(f)
        self._map = None
        self._tree = None

    def free_caches(self):
        """Clears the internal caches used by certain properties

        Clear these if the messages has been changed in any way,
        or if the cache values have become corrupted in some
        other way.
        """
        self._map = None
        self._tree = None

    @property
    def entries(self):
        """An iterator to the logged messages"""
        yield from self._content

    @property
    def count(self):
        """The number of loaded messages"""
        return len(self._content)

    @property
    def map(self):
        """A map for looking up messages by their ID.

        The map is cached to avoid recalculating the map,
        which means the returned map should not be modified.
        """
        if self._map is None:
            self._map = {e['msg_id']: e for e in self.entries}
        return self._map

    @property
    def tree(self):
        """A tree for inspecting message hierarchies.

        The messages are put in hierarchical nodes according
        to their parent message ids.

        The tree is cached to avoid recalculation,
        which means the returned thee should not be modified.
        """
        if self._tree is None:
            self._tree = TreeSorter()
            for msg_id, entry in self.map.items():
                parent_id = entry['parent_header']['msg_id']
                self._tree.add_entry(entry, msg_id, parent_id)
        return self._tree


    @staticmethod
    def stat_node(node):
        """Return the type stat of a message tree node and its children.

        Returns a two-tuple, where the first value is the type of the node
        itself. The second is a dict of all child types and their count.
        Note: This includes all children, not just direct ones.
        """
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
        """Statistics on the message types loaded.

        Returns a dict with the message types loaded, and
        their respective count.
        """
        types = defaultdict(int)
        for entry in self.entries:
            types[entry['msg_type']] += 1
        return dict(types)
