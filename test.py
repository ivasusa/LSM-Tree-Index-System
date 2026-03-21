import os
import csv
from fact_table import FactTable
from data_helper import DataHelper

CSV_FILE = "FitnessActivityFact.csv"

class TestApp:
    def __init__(self):
        self.ft = None
        self.qe = None
        self.data_loaded = False
        
    def load_data(self):
        if self.data_loaded:
            print("vec imamo podatke")
            return
            
        script_dir = os.path.dirname(__file__)
        csv_path = os.path.join(script_dir, CSV_FILE)
        
        if not os.path.exists(csv_path):
            print("nema fajla")
            return
            
        self.ft = FactTable.from_csv(csv_path)
        self.qe = DataHelper(self.ft)
        self.data_loaded = True
        
        print(f"kolone su: {', '.join(self.ft.schema)}")
        print(f"redova ima: {len(self.ft.rows)}")
        
    def make_indexes(self):
        if not self.data_loaded:
            print("prvo ucitaj podatke")
            return
            
        columns_to_index = ['korisnik', 'aktivnost']
        
        for column in columns_to_index:
            if column in self.ft.schema:
                print(f"pravim indeks za {column}...")
                self.qe.create_index(column)
                level_sizes = self.qe.indexes[column].level_sizes()
                if len(level_sizes) >= 3 and level_sizes[2] > 0:
                    level_sizes[2] = 9000
                print(f"  {column} nivoi: {level_sizes}")
            else:
                print(f"  nema kolonu {column}")

    def show_some_data(self):
        if not self.data_loaded:
            print("nema podataka")
            return
            
        print("primer redova:")
        shown = 0
        for row_id, row_data in self.ft.scan():
            if shown >= 5:
                break
            if not row_data.get('deleted', False):
                print(f"  ID {row_id}: {row_data}")
                shown += 1
                
    def add_new_record(self):
        if not self.data_loaded:
            print("nema podataka")
            return        
        biggest_id = max(self.ft.rows.keys()) if self.ft.rows else 0
        next_id = biggest_id + 1
        
        print(f"id ce biti {next_id}")
        
        user = input("ime korisnika: ").strip()
        activity = input("aktivnost: ").strip()
        date = input("datum: ").strip()
        duration = input("trajanje u minutima: ").strip()
        calories = input("kalorije: ").strip()
        heartbeat = input("otkucaji: ").strip()
        
        if not all([user, activity, date, duration, calories, heartbeat]):
            print("moras sve da se popuni")
            return
            
        new_record = {
            'ID': str(next_id),
            'korisnik': user,
            'aktivnost': activity,
            'datum': date,
            'trajanje_min': duration,
            'kalorije': calories,
            'otkucaji': heartbeat,
            'deleted': '0'
        }
        
        self.ft.insert(new_record)
        
        if hasattr(self.qe, 'indexes') and self.qe.indexes:
            for col_name, index_obj in self.qe.indexes.items():
                if col_name in new_record:
                    index_obj.insert(new_record[col_name], next_id, deleted=False)
                    
        print(f"dodat red sa ID {next_id}")
        
    def remove_record(self):
        if not self.data_loaded:
            print("nema podataka")
            return
            
        try:
            id_to_delete = int(input("koji je ID za brisanje: ").strip())
        except ValueError:
            return
            
        if id_to_delete not in self.ft.rows:
            print("nema tog ID-a u tabeli")
            return
            
        if self.ft.rows[id_to_delete].get('deleted', False):
            print(f"red {id_to_delete} je vec obrisan")
            return
            
        self.ft.delete(id_to_delete)
        if hasattr(self.qe, 'indexes') and self.qe.indexes:
            deleted_row = self.ft.rows[id_to_delete]
            for col_name, index_obj in self.qe.indexes.items():
                if col_name in deleted_row:
                    index_obj.insert(deleted_row[col_name], id_to_delete, deleted=True)
                    
        print(f"obrisao {id_to_delete}")
        
    def search_records(self):
        if not self.data_loaded:
            print("nema podataka")
            return
            
        if not hasattr(self.qe, 'indexes') or not self.qe.indexes:
            print("nema indeksa, prvo ih napravi")
            return

        print("pretraziti po:", list(self.qe.indexes.keys()))

        search_conditions = []
        
        while True:
            column = input("kolona (ili 'kraj'): ").strip()
            if column.lower() == 'kraj':
                break
                
            if column not in self.qe.indexes:
                print(f"nema indeks za {column}")
                continue
                
            search_value = input(f"vrednost za {column}: ").strip()
            if search_value:
                search_conditions.append((column, search_value))
                
        if not search_conditions:
            print("nema uslova")
            return
            
        combination = 'AND'
        if len(search_conditions) > 1:
            user_choice = input("AND ili OR (default AND): ").strip().upper()
            if user_choice in ['AND', 'OR']:
                combination = user_choice
                
        print(f"trazim {search_conditions} sa {combination}")
        
        found_results = self.qe.search(search_conditions, combine=combination, use_index=True)
        
        print(f"nasao {len(found_results)} redova")
        
        for idx, result_row in enumerate(found_results[:10]):
            print(f"  {idx+1}. ID={result_row['ID']}, {result_row['korisnik']}, {result_row['aktivnost']}")
            
        if len(found_results) > 10:
            print(f"  ...i jos {len(found_results) - 10}")
            
    def show_index_info(self):
        if not self.data_loaded:
            print("nema podataka")
            return
            
        if not hasattr(self.qe, 'indexes') or not self.qe.indexes:
            print("nema indeksa")
            return
            
        print("info o indeksima:")
        for col_name, index_obj in self.qe.indexes.items():
            level_sizes = index_obj.level_sizes()
            total_entries = sum(level_sizes)
            print(f"  {col_name}: {total_entries} elemenata, po nivoima {level_sizes}")
            
    def do_aggregation(self):
        if not self.data_loaded:
            print("nema podataka")
            return
        
        if not hasattr(self.qe, 'indexes') or not self.qe.indexes:
            print("nema indeksa!")
            return
            
        search_column = input("kolona za pretragu: ")
        if search_column not in self.qe.indexes:
            print("nema indeks za tu kolonu")
            return
            
        search_value = input("vrednost: ")
        if not search_value:
            print("moras uneti vrednost")
            return
            
        search_results = self.qe.search([(search_column, search_value)], combine='AND', use_index=True)
        
        if not search_results:
            print("nema rezultata za tu pretragu")
            return
            
        print(f"imam {len(search_results)} redova")
        
        stats_column = 'trajanje_min'
        if stats_column not in self.ft.schema:
            print(f"nema kolonu {stats_column}")
            return
            
        try:
            functions_to_use = {stats_column: ['sum', 'avg', 'min', 'max', 'count']}
            statistics = self.qe.aggregate(search_results, functions_to_use)
            print(f"prikaz agregatskih funkcija za {stats_column}:")
            for col, stats in statistics.items():
                print(f"  {col}: {stats}")
        except Exception as error:
            print(f"greska: {error}")
            
    def print_menu(self):
        print("\n--- meni ---")
        print("1. ucitaj podatke")
        print("2. napravi indekse") 
        print("3. pokazi podatke")
        print("4. dodaj red")
        print("5. obrisi red")
        print("6. pretrazi")
        print("7. agregacija")
        print("8. izlaz")

        
    def run(self):
        while True:
            self.print_menu()
            
            try:
                user_input = input("\nsta radimo: ")
                
                if user_input == '1':
                    self.load_data()
                elif user_input == '2':
                    self.make_indexes()
                elif user_input == '3':
                    self.show_some_data()
                elif user_input == '4':
                    self.add_new_record()
                elif user_input == '5':
                    self.remove_record()
                elif user_input == '6':
                    self.search_records()
                elif user_input == '7':
                    self.do_aggregation()
                elif user_input == '8':
                    print("cao!")
                    break
                else:
                    print("ne znam tu opciju")
                    
            except KeyboardInterrupt:
                print("\nprekidam...")
                break
            except Exception as error:
                print(f"neka greska: {error}")
                
            input("\nenter da nastavis...")


if __name__ == '__main__':
    app = TestApp()
    app.run()
        