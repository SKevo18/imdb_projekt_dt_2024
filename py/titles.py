from random import randint
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent
CESTA = ROOT / "title.basics.tsv.old"


def nahodny_datum(rok: str, rok_koniec: int | None = None) -> str:
    r = int(rok)
    if rok_koniec is not None:
        r = randint(r, rok_koniec)

    mesiac = randint(1, 12)
    if mesiac == 2:
        den = randint(1, 28)
    elif mesiac in [4, 6, 9, 11]:
        den = randint(1, 30)
    else:
        den = randint(1, 31)
    return f"{r}-{mesiac:02d}-{den:02d}"


def nahodny_cas() -> str:
    hodina = randint(0, 23)
    minuta = randint(0, 59)
    sekunda = randint(0, 59)

    return f"{hodina:02d}:{minuta:02d}:{sekunda:02d}"


def nahodny_timestamp(rok: str, rok_koniec: int | None = None) -> str:
    aktualny_rok = datetime.now().year
    if rok_koniec is None:
        rok_koniec = aktualny_rok
    else:
        rok_koniec = min(rok_koniec, aktualny_rok)

    rok_zaciatok = max(int(rok), aktualny_rok - 10)
    if rok_zaciatok > rok_koniec:
        rok_zaciatok, rok_koniec = rok_koniec, rok_zaciatok

    return f"{nahodny_datum(str(randint(rok_zaciatok, rok_koniec)))}T{nahodny_cas()}"


if __name__ == "__main__":
    spracovane = 0
    with CESTA.open() as i:
        i.readline()
        with (CESTA.with_stem("title.basics").with_suffix(".tsv")).open("w") as o:
            o.write(
                "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\tlastUpdate\n"
            )
            for r in i:
                (
                    tconst,
                    titleType,
                    primaryTitle,
                    originalTitle,
                    isAdult,
                    startYear,
                    endYear,
                    runtimeMinutes,
                    genres,
                ) = r.strip().split("	")

                if startYear == "\\N":
                    continue
                if endYear != "\\N" and int(endYear) < int(startYear):
                    continue

                startDate = nahodny_datum(startYear)
                endDate = nahodny_datum(endYear) if endYear != "\\N" else "\\N"
                lastUpdate = nahodny_timestamp(
                    startYear, int(endYear) if endYear != "\\N" else datetime.now().year
                )

                o.write(
                    f"{tconst}\t{titleType}\t{primaryTitle}\t{originalTitle}\t{isAdult}\t{startDate}\t{endDate}\t{runtimeMinutes}\t{genres}\t{lastUpdate}\n"
                )
                spracovane += 1

    print(spracovane)
