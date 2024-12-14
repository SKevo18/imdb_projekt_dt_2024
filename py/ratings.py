#!/usr/bin/env python3
from random import randint
from titles import ROOT, nahodny_timestamp

CESTA = ROOT / "title.ratings.tsv.old"

if __name__ == "__main__":
    spracovane = 0
    nova_cesta = CESTA.with_stem("title.ratings").with_suffix(".tsv")
    with CESTA.open() as vstup, nova_cesta.open("w") as vystup:
        vstup.readline()
        vystup.write("tconst\trating\ttimestamp\n")
        for riadok in vstup:
            tconst, priemerne_hodnotenie, _ = riadok.strip().split("\t")
            priemerne_hodnotenie = float(priemerne_hodnotenie)
            pokusy = 0
            hodnotenia = []
            while pokusy < 500000:
                pocet_hodnoteni = randint(5, 10)
                hodnotenia_temp = [float(randint(0, 10)) for _ in range(pocet_hodnoteni - 1)]
                suma_hodnoteni = sum(hodnotenia_temp)
                posledne_hodnotenie = priemerne_hodnotenie * pocet_hodnoteni - suma_hodnoteni
                if posledne_hodnotenie.is_integer() and 0 <= posledne_hodnotenie <= 10:
                    hodnotenia_temp.append(float(posledne_hodnotenie))
                    vypocitany_priemer = sum(hodnotenia_temp) / pocet_hodnoteni
                    assert vypocitany_priemer == priemerne_hodnotenie, f"{vypocitany_priemer} != {priemerne_hodnotenie}"
                    hodnotenia = hodnotenia_temp
                    break
                pokusy += 1
            if not hodnotenia:
                print(f"preskočiť {tconst}")
                hodnotenia = [float(priemerne_hodnotenie)]
            vystup.writelines(
                f"{tconst}\t{hodnotenie}\t{nahodny_timestamp('2004')}\n"
                for hodnotenie in hodnotenia
            )
            spracovane += 1
            print(spracovane)
    print(spracovane)
