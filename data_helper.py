from lsm_index import LSMIndex

class DataHelper:
    def __init__(self, fact_table):
        self.table = fact_table
        self.indexes = {}  

    def create_index(self, column):
        if column not in self.table.schema:
            raise ValueError(f"Column {column} not in schema")
        
        new_index = LSMIndex(column)
        for row_id, row in self.table.scan():
            value = row.get(column)
            if value is None:
                continue
            is_deleted = bool(row.get('deleted', False))
            new_index.insert(value, row_id, deleted=is_deleted)
        
        self.indexes[column] = new_index
        return new_index

    def insert(self, new_row):
        self.table.insert(new_row)
        row_id = int(new_row['ID'])
        for column, index in self.indexes.items():
            value = new_row.get(column)
            if value is None:
                continue
            is_deleted = bool(new_row.get('deleted', False))
            index.insert(value, row_id, deleted=is_deleted)

    def delete(self, row_id):
        row = self.table.get(row_id)
        if row is None:
            return
        self.table.delete(row_id)
        for column, index in self.indexes.items():
            value = row.get(column)
            if value is None:
                continue
            index.insert(value, int(row_id), deleted=True)

    def find_using_indexes(self, conditions):
        result_sets = []
        for column, value in conditions:
            index = self.indexes.get(column)
            if index is None:
                return None  
            found = index.search(value)
            result_sets.append(found)
        
        return result_sets

    def search(self, conditions, combine='AND', use_index=True):
        if use_index and conditions:
            sets = self.find_using_indexes(conditions)
            if sets is None:
                use_index = False

        result_ids = set()
        
        if use_index and conditions:
            if combine == 'AND':
                result_ids = set.intersection(*sets) if sets else set()
            else: 
                result_ids = set.union(*sets) if sets else set()
        else:
            for row_id, row in self.table.scan():
                if row.get('deleted'):
                    continue
                
                condition_results = []
                for column, value in conditions:
                    condition_results.append(row.get(column) == value)
                
                if not conditions:
                    matches = True
                elif combine == 'AND':
                    matches = all(condition_results)
                else: 
                    matches = any(condition_results)
                
                if matches:
                    result_ids.add(row_id)
        final_results = []
        for row_id in sorted(result_ids):
            row = self.table.get(row_id)
            if row and not row.get('deleted'):
                final_results.append(row)
        
        return final_results

    def aggregate(self, rows, agg_functions):
        result = {}
        
        for column, functions in agg_functions.items():
            numbers = []
            for row in rows:
                value = row.get(column)
                if value is not None and value != '':
                    numbers.append(float(value))
            
            column_result = {}
            for function in functions:
                if function == 'min':
                    column_result['min'] = min(numbers) if numbers else None
                elif function == 'max':
                    column_result['max'] = max(numbers) if numbers else None
                elif function == 'sum':
                    column_result['sum'] = sum(numbers) if numbers else 0
                elif function == 'count':
                    column_result['count'] = len(numbers)
                elif function == 'avg':
                    if numbers:
                        column_result['avg'] = sum(numbers) / len(numbers)
                    else:
                        column_result['avg'] = None
                else:
                    raise ValueError()
            
            result[column] = column_result
        
        return result
