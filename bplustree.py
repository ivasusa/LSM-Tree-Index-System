from bisect import bisect_left, bisect_right

class LeafNode:
    def __init__(self, order):
        self.order = order
        self.keys = []
        self.values = []  
        self.next = None

    def insert(self, key, value):
        pos = bisect_left(self.keys, key)
        
        if pos < len(self.keys) and self.keys[pos] == key:
            self.values[pos].append(value)
        else:
            self.keys.insert(pos, key)
            self.values.insert(pos, [value])

    def split(self):
        middle = len(self.keys) // 2
        
        new_node = LeafNode(self.order)
        new_node.keys = self.keys[middle:]
        new_node.values = self.values[middle:]
        self.keys = self.keys[:middle]
        self.values = self.values[:middle]
        new_node.next = self.next
        self.next = new_node
        
        return new_node.keys[0], new_node

    def search(self, key):
        pos = bisect_left(self.keys, key)
        if pos < len(self.keys) and self.keys[pos] == key:
            return list(self.values[pos])
        return []

    def items(self):
        for key, vals in zip(self.keys, self.values):
            yield key, list(vals)

class InternalNode:
    def __init__(self, order):
        self.order = order
        self.keys = []
        self.children = []

    def split(self):
        middle = len(self.keys) // 2
        separator = self.keys[middle]
        
        new_node = InternalNode(self.order)
        new_node.keys = self.keys[middle+1:]
        new_node.children = self.children[middle+1:]
        
        self.keys = self.keys[:middle]
        self.children = self.children[:middle+1]
        
        return separator, new_node

class BPlusTree:
    def __init__(self, order=32):
        self.order = max(3, order)
        self.root = LeafNode(self.order)
        self.count = 0

    def insert(self, key, value):
        root = self.root
        if isinstance(root, LeafNode):
            root.insert(key, value)
            self.count += 1
            if len(root.keys) > self.order - 1:
                sep_key, right_node = root.split()
                
                new_root = InternalNode(self.order)
                new_root.keys = [sep_key]
                new_root.children = [root, right_node]
                self.root = new_root
        else:
            path_to_leaf = []
            current = root
            while not isinstance(current, LeafNode):
                path_to_leaf.append(current)
                pos = bisect_right(current.keys, key)
                current = current.children[pos]
            
            current.insert(key, value)
            self.count += 1
            
            if len(current.keys) > self.order - 1:
                sep_key, right_node = current.split()
                for parent in reversed(path_to_leaf):
                    pos = bisect_right(parent.keys, sep_key)
                    parent.keys.insert(pos, sep_key)
                    parent.children.insert(pos+1, right_node)
                    
                    if len(parent.keys) > self.order - 1:
                        sep_key, right_node = parent.split()
                        continue
                    else:
                        break
                else:
                    new_root = InternalNode(self.order)
                    new_root.keys = [sep_key]
                    new_root.children = [self.root, right_node]
                    self.root = new_root

    def search(self, key):
        current = self.root
        
        while isinstance(current, InternalNode):
            pos = bisect_right(current.keys, key)
            current = current.children[pos]
            
        return current.search(key)

    def items(self):
        current = self.root
        while isinstance(current, InternalNode):
            current = current.children[0]
        while current:
            for key, vals in zip(current.keys, current.values):
                yield key, list(vals)
            current = current.next

    def size(self):
        return self.count

    def clear(self):
        self.root = LeafNode(self.order)
        self.count = 0
