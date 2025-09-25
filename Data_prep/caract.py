import pandas as pd
from pathlib import Path
from pyproj import Transformer

base = Path(__file__).resolve().parent.parent
for i in range(2005,2019): 
    df = pd.read_csv(base / f'Data/caract-{i}.csv', encoding='latin1', on_bad_lines='skip')
    globals()[f"df_{i}"] = df
for i in range(2019,2024): 
    df = pd.read_csv(base / f'Data/caract-{i}.csv',sep = ';', encoding='latin1')
    globals()[f"df_{i}"] = df

df_2009 = pd.read_csv(base / f'Data/caract-2009.csv',sep = "\t", encoding='latin1', on_bad_lines='skip')

columns = []
for i in range(2005, 2024):
    columns.append(globals()[f"df_{i}"].columns.tolist())

df_complet = pd.DataFrame(columns=columns[0])

for i in range(2005, 2019):
    df_complet = pd.concat([df_complet, globals()[f"df_{i}"]], axis=0)

df_complet['an'] = df_complet['an'].astype(int)
df_complet['an'] = df_complet['an'].map(lambda x: x + 2000)

df_complet["hrmn"] = df_complet["hrmn"].apply(lambda x: f"{int(x):04d}")
df_complet["hrmn"] = df_complet["hrmn"].str[:2] + ":" + df_complet["hrmn"].str[2:]

cols_to_int = [col for col in df_complet.columns if col not in ["adr", "gps","hrmn","dep","com","lat","long"]]
df_complet[cols_to_int] = df_complet[cols_to_int].apply(pd.to_numeric, downcast="integer", errors="coerce")



transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

def convert_coords(row):
    try:
        x = float(row["long"])
        y = float(row["lat"])
        lon, lat = transformer.transform(x, y)
        return pd.Series({"lat": lat, "long": lon})
    except Exception:
        return pd.Series({"lat": None, "long": None})

df_complet[["lat", "long"]] = df_complet.apply(convert_coords, axis=1)

df_2019 = globals()["df_2019"]
df_2020 = globals()["df_2020"]
df_2021 = globals()["df_2021"]
df_2022 = globals()["df_2022"]
df_2023 = globals()["df_2023"]

df_end = pd.concat([df_complet, df_2019, df_2020, df_2021, df_2022, df_2023], axis=0)

df_end["date"] = pd.to_datetime(
    df_end["an"].astype(str) + "-" +
    df_end["mois"].astype(str).str.zfill(2) + "-" +
    df_end["jour"].astype(str).str.zfill(2),
    format="%Y-%m-%d"
)
df_end["weekday"] = df_end["date"].dt.weekday + 1
base = Path(__file__).resolve().parent.parent
print(base / "Data_clean/caract-2005-2023.csv")
df_end.to_csv("Data_clean/caract-2005-2023.csv", index=False)