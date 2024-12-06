<!-- markdownlint-disable MD033 -->

# Spracovanie a analýza databázy filmov IMDb

## Popis projektu

Môj projekt sa zameriava na spracovanie [IMDb datasetu](https://developer.imdb.com/non-commercial-datasets/) pomocou ETL procesu v rámci hviezdicovej schémy na platforme [Snowflake](https://www.snowflake.com/).

## Zdrojový dataset

Platforma IMDb poskytuje časť svojich dát prostredníctvom datasetov pre verejnosť alebo akademické účely. Tieto dáta sa denne aktualizujú a najnovšia verzia je dostupná k stiahnutiu [**tu**](https://datasets.imdbws.com/).

V rámci môjho projektu analyzujem verziu datasetu k **6. decembru 2024**.

**Entitno-relačný diagram datasetu** s vizualizáciou vzťahov naprieč všetkými súbormi v datasete by v tradičnej entitno-relačnej schéme vyzeral nasledovne:

<p align="center">
<img alt="ERD diagram surových dát" src="original_erd.png"/>
<b>Obrázok 1:</b> ERD diagram surových dát
</p>

**Všeobecné vlastnosti datasetov:**

- Každý dataset je v komprimovanom formáte (gzip) a samotné dáta sú v TSV formáte (kde oddelovače hodnôt predstavujú tabulátory - `\t`);
- Kódovanie textu je UTF-8;
- Prvý riadok každého súboru obsahuje hlavičku so zoznamom stĺpcov;
- Hodnota `\N` predstavuje chýbajúcu alebo neznámu hodnotu (`NULL`);
- Polia (napr.: `types`, `attributes`) môžu obsahovať jeden alebo viac reťazcov, ktoré sú oddelené čiarkou;

### Súbory a ich význam

- `title.akas.tsv.gz`: obsahuje záznamy o alternatívnych, medzinárodných a lokálnych názvoch titulov, keďže názvy filmov sú obvykle prekladané do viacerých jazykov;
- `title.basics.tsv.gz`: obsahuje základné informácie o každom titule v datasete (titul môže predstavovať napr.: jeden film alebo seriál);
- `title.crew.tsv.gz`: informácie o filmových a televíznych tvorcoch, konkrétne o režiséroch (`directors`) a scenáristoch (`writers`);
- `title.episode.tsv.gz`: týka sa epizód seriálov; prepája epizódy (tituly) so seriálom, ktorého sú súčasťou (t. j. s nadradeným titulom);
- `title.principals.tsv.gz`: informácie o hlavných osobách spojených s titulom (herci, režiséri, kameramani, atď.), pričom uvádza ich roly alebo postavy, ktoré hrali;
- `title.ratings.tsv.gz`: obsahuje hodnotenia titulov na základe hlasovania používateľov IMDb;
- `name.basics.tsv.gz`: opisuje jednotlivé osoby (hercov, režisérov, scenáristov, atď.) v databáze;

## Hviezdicová schéma

Dáta som transformoval na hviezdicovú schému, ktorá je znázornená nižšie:

<p align="center">
<img alt="ERD diagram hviezdicovej schémy" src="star_schema.png"/>
<b>Obrázok 2:</b> ERD diagram hviezdicovej schémy
</p>

## Odkazy

- [GitHub repozitár](https://github.com/SKevo18/imdb_projekt_dt_2024)
- [Zdrojové datasety](https://datasets.imdbws.com/)
- [Snowflake](https://www.snowflake.com/)
- ER diagramy boli vytvorené v programe [MySQLWorkbench](https://www.mysql.com/products/workbench/)

**Autor projektu:** Kevin Svitač, FPVaI UKF 2024
