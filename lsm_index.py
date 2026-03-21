import itertools
from bplustree import BPlusTree

class LSMIndex:
    def __init__(self, column_name, c0_capacity=1000):
        self.column = column_name
        self.levels = [BPlusTree()]
        self.capacities = [c0_capacity, c0_capacity * 3, c0_capacity * 9, c0_capacity * 27]
        self.seq_counter = 0

    def level_sizes(self):
        return [level.size() for level in self.levels]

    def get_next_seq(self):
        self.seq_counter += 1
        return self.seq_counter

    def make_sure_level_exists(self, level_num):
        while level_num >= len(self.levels):
            self.levels.append(BPlusTree())

    def insert(self, key, record_id, deleted=False):
        seq = self.get_next_seq()
        self.make_sure_level_exists(0)
        level0 = self.levels[0]
        
        if level0.size() >= self.capacities[0]:
            self.merge_level(0)
            level0 = self.levels[0]
            
        level0.insert(key, (record_id, deleted, seq))

    def get_all_from_level(self, level_num):
        all_entries = []
        for key, values in self.levels[level_num].items():
            for value in values:
                record_id, deleted, seq = value
                all_entries.append((key, record_id, deleted, seq))
        return all_entries

    def merge_two_lists(self, list1, list2):
        temp = []
        i = j = 0
        
        while i < len(list1) and j < len(list2):
            if list1[i][0] < list2[j][0]:
                temp.append(list1[i])
                i += 1
            else:
                temp.append(list2[j])
                j += 1
                
        while i < len(list1):
            temp.append(list1[i])
            i += 1
            
        while j < len(list2):
            temp.append(list2[j])
            j += 1

        final_result = []
        for key, group in itertools.groupby(temp, key=lambda entry: entry[0]):
            latest_by_id = {}
            for (_, record_id, deleted, seq) in group:
                existing = latest_by_id.get(record_id)
                if (existing is None) or (seq > existing[1]):
                    latest_by_id[record_id] = (deleted, seq)
                    
            for record_id, (deleted, seq) in sorted(latest_by_id.items()):
                if deleted:
                    continue
                final_result.append((key, record_id, False, seq))
                
        return final_result

    def merge_level(self, level_num):
        if level_num + 1 >= len(self.capacities):
            return  

        self.make_sure_level_exists(level_num + 1)
        current_level_data = self.get_all_from_level(level_num)
        next_level_data = self.get_all_from_level(level_num + 1)
        merged_data = self.merge_two_lists(current_level_data, next_level_data)

        self.levels[level_num] = BPlusTree()

        capacity_next = self.capacities[level_num + 1]
        stay_data = merged_data[:capacity_next]
        overflow_data = merged_data[capacity_next:]

        new_tree = BPlusTree()
        for key, record_id, deleted, seq in stay_data:
            new_tree.insert(key, (record_id, deleted, seq))
        self.levels[level_num + 1] = new_tree

        if overflow_data:
            self.make_sure_level_exists(level_num + 2)
            next_next_data = self.get_all_from_level(level_num + 2)
            merged_overflow = self.merge_two_lists(overflow_data, next_next_data)

            self.levels[level_num + 2] = BPlusTree()
            for key, record_id, deleted, seq in merged_overflow:
                self.levels[level_num + 2].insert(key, (record_id, deleted, seq))

            if self.levels[level_num + 2].size() >= self.capacities[level_num + 2]:
                self.merge_level(level_num + 2)


    def find_all_matches(self, key):
        all_matches = []
        for level in self.levels:
            if level.size() == 0:
                continue
            matches = level.search(key)
            for (record_id, deleted, seq) in matches:
                all_matches.append((key, record_id, deleted, seq))
        return all_matches

    def search(self, key):
        all_matches = self.find_all_matches(key)
        latest_version = {}
        for k, record_id, deleted, seq in all_matches:
            existing = latest_version.get(record_id)
            if (existing is None) or (seq > existing[1]):
                latest_version[record_id] = (deleted, seq)
        active_records = set()
        for record_id, (deleted, seq) in latest_version.items():
            if not deleted:
                active_records.add(record_id)
                
        return active_records
