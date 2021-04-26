# ------------------------------------------------------------------------------ #
# Processes RKI data and creates JSON
# @Author:        Matthias Wetzka
# ------------------------------------------------------------------------------ #
import ujson
import pandas as pd
import covid19_inference as cov19
import numpy as np
from IPython.display import display

""" # Load data
"""

# Load data with cov19npis module
rki = cov19.data_retrieval.RKI()
#rki.download_all_available_data(force_download=True)
rki.download_all_available_data()

# Load population data
population = pd.read_csv(
    "../data/population_rki_age_groups.csv", encoding="cp1252",
)

pop_rki_aligned = population.set_index(["ags", "Region", "NUTS3"])

# Raw geo_file
with open("../data/population_landkreise.json") as json_file:
    population_landkreise = ujson.load(json_file)

pop_rki_aligned["unbekannt"] = pop_rki_aligned.sum(axis=1) * 0.01  # 1% not known


""" # Create data dict
"""

ags = ["A00-A04", "A05-A14", "A15-A34", "A35-A59", "A60-A79", "A80+", "unknown"]


# 09564 Nürnberg
#rki.
#df.loc[label]

# New cases with rolling 7 day sum:
data_rki = rki.data
datemax = rki.data["date"].max()
index = pd.date_range(rki.data["date"].min(), rki.data["date"].max())

data_rki = data_rki.set_index(["IdLandkreis", "Altersgruppe", "date",])
data_rki = data_rki.groupby(["IdLandkreis", "Altersgruppe", "date",]).sum()["confirmed"]
#display(data_rki)

#display(data_rki)

#for lk_id in data_rki.index.get_level_values(level="IdLandkreis").unique():
#    print(lk_id)

# rolling window

data_rki = data_rki.groupby(level=[0, 1]).apply(
    lambda x: x.reset_index(level=[0, 1], drop=True).reindex(index)
)
data_rki.index = data_rki.index.set_names("date", level=-1)
data_rki = data_rki.fillna(0)
#display(data_rki)

cases_7_day_sum = data_rki.rolling(7).sum()
cases_7_day_sum = cases_7_day_sum.fillna(0)

#lk_id = 9564
#cases_7_day_sum = cases_7_day_sum.xs(lk_id, level="IdLandkreis")

data = {}

for lk_id in cases_7_day_sum.index.get_level_values(level="IdLandkreis").unique():
    data_lk = cases_7_day_sum.xs(lk_id, level="IdLandkreis")
    data[lk_id] = []
    print(f"processing landkreis {lk_id}")

    #data = []
    #for idx in index:
    for idx in data_lk.index.get_level_values(level="date").unique():
        cases_at_date = data_lk.xs(idx, level="date")
    
        handle = idx.strftime('%Y-%m-%d')
        row = {}
        row["date"] = handle
        for ag in ags:
            try:
                #if lk_id == 9564 and idx == datemax:
                #    display(cases_at_date[ag])

                # TODO round floats for json
                row[ag] = (cases_at_date[ag]
                    * 100000
                    / pop_rki_aligned[ag].xs(lk_id, level="ags").values[0]
                )
            except:
                row[ag] = 0
        data[lk_id].append(row)

#display(data)
print("writing data")
with open("../data/data_latest.json", "w") as outfile:
    ujson.dump(data, outfile)
