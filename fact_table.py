import csv

class FactTable:
    def __init__(self, schema, csv_path=None):
        self.schema = schema
        self.rows = {}
        self.csv_path = csv_path

    @classmethod
    def from_csv(cls, path):
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            schema = reader.fieldnames
            table = cls(schema, csv_path=path)
            for row in reader:
                if 'deleted' in row:
                    row['deleted'] = bool(int(row['deleted']))
                else:
                    row['deleted'] = False
                    
                id_val = int(row['ID'])
                table.rows[id_val] = row
        return table

    def _save_to_csv(self):
        if not self.csv_path:
            return
        with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.schema)
            writer.writeheader()
            for id_val in sorted(self.rows.keys()):
                row_data = dict(self.rows[id_val])
                if 'deleted' in row_data:
                    row_data['deleted'] = '1' if row_data['deleted'] else '0'
                else:
                    row_data['deleted'] = '0'
                csv_row = {}
                for col in self.schema:
                    val = row_data.get(col)
                    csv_row[col] = '' if val is None else str(val)
                    
                writer.writerow(csv_row)

    def insert(self, row):
        id_val = int(row['ID'])
        if 'deleted' in row:
            row['deleted'] = bool(int(row['deleted']))
        else:
            row['deleted'] = False
            
        self.rows[id_val] = row
        if self.csv_path:
            self._save_to_csv()

    def delete(self, id_val):
        id_val = int(id_val)
        if id_val in self.rows:
            self.rows[id_val]['deleted'] = True
            if self.csv_path:
                self._save_to_csv()

    def get(self, id_val):
        return self.rows.get(int(id_val))

    def scan(self):
        for id_val, row in self.rows.items():
            yield id_val, row
