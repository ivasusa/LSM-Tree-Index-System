import os
import csv
from fact_table import FactTable
from data_helper import DataHelper


CSV = 'FitnessActivityFact.csv'


def main():
    here = os.path.dirname(__file__)
    path = os.path.join(here, CSV)
    print(f" Učitavanje CSV fajla: {CSV}")
    ft = FactTable.from_csv(path)
    print(f" - Detektovane kolone: {', '.join(ft.schema)}")
    print(f" - Učitan broj redova: {len(ft.rows)}")

    qe = DataHelper(ft)
    print("Kreiranje LSM indeksa")
    qe.create_index('korisnik')
    qe.create_index('aktivnost')
    print(" - Indeksi kreirani: korisnik, aktivnost")
    print(f" - Veličine nivoa [korisnik]: {qe.indexes['korisnik'].level_sizes()}")
    print(f" - Veličine nivoa [aktivnost]: {qe.indexes['aktivnost'].level_sizes()}")

    print("Ubacivanje primeraka u FactTable i indekse")
    new_rows = [
        {'ID': '10001', 'korisnik': 'Marko Markić', 'aktivnost': 'Vožnja', 'datum': '2025-06-01', 'trajanje_min': '45', 'kalorije': '400', 'otkucaji': '140', 'deleted': '0'},
        {'ID': '10002', 'korisnik': 'Tanja Simić', 'aktivnost': 'Plivanje', 'datum': '2025-06-02', 'trajanje_min': '30', 'kalorije': '300', 'otkucaji': '130', 'deleted': '0'},
        {'ID': '12985', 'korisnik': 'Iva Susa', 'aktivnost': 'Plivanje', 'datum': '2025-06-02', 'trajanje_min': '30', 'kalorije': '300', 'otkucaji': '130', 'deleted': '0'}

    ]
    for r in new_rows:
        qe.insert(r)
        print(f" - Ubacen red ID={r['ID']} korisnik={r['korisnik']} aktivnost={r['aktivnost']}")

    print("Brisanje reda ID=12984")
    qe.delete(12984)
    print("Test brisanja reda ID=12984 ")
    test_id = 12984
    row_before = ft.get(test_id)
    if row_before:
        print(f" - Pre: ID={test_id} deleted={row_before.get('deleted')}")
    else:
        print(f" - Pre: Red sa ID={test_id} nije pronađen u FactTable")
    qe.delete(test_id)
    row_after = ft.get(test_id)
    if row_after:
        print(f" - Posle: ID={test_id} deleted={row_after.get('deleted')}")
    else:
        print(f" - Posle: Red sa ID={test_id} nije pronađen u FactTable")
    with open(path, newline='', encoding='utf-8') as f:
        for line in f:
            if line.startswith(f"{test_id},"):
                print(f" - U CSV fajlu: {line.strip()}")
                break

    print("Pretraga (korišćenje indeksa)")
    cond_and = [('korisnik', 'Tanja Simić'), ('aktivnost', 'Plivanje')]
    print(f" - AND uslovi pretrage: {cond_and}")

    idx_results = qe.search(cond_and, combine='AND', use_index=True)
    print(f"   > Pronađeno {len(idx_results)} redova:")
    for row in idx_results:
        print(f"     ID={row['ID']}, korisnik={row['korisnik']}, aktivnost={row['aktivnost']}, datum={row['datum']}, trajanje={row['trajanje_min']}min")

    cond_or = [('korisnik', 'Tanja Simić'), ('aktivnost', 'Trčanje')]
    print(f" - OR uslovi pretrage: {cond_or}")
    idx_results = qe.search(cond_or, combine='OR', use_index=True)
    print(f"   > Pronađeno {len(idx_results)} redova:")
    for i, row in enumerate(idx_results[:10]):
        print(f"     ID={row['ID']}, korisnik={row['korisnik']}, aktivnost={row['aktivnost']}, datum={row['datum']}, trajanje={row['trajanje_min']}min")
    if len(idx_results) > 10:
        print(f"     ... i još {len(idx_results) - 10} redova")

    print(" Agregacija 'trajanje_min' za AND ")
    aggs = {'trajanje_min': ['sum', 'avg']}
    rows_for_agg = qe.search(cond_and, combine='AND', use_index=True)
    agg_idx = qe.aggregate(rows_for_agg, aggs)
    print(f" - Agregacija (na {len(rows_for_agg)} redova): {agg_idx}")

    print("Testic završen.")


if __name__ == '__main__':
    main()
