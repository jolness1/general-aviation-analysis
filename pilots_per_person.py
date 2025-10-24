import pandas as pd
import os

INPUT_DIR = 'inputs'
PILOTS_FILE = os.path.join(INPUT_DIR, 'pilots-by-state.csv')
POP_FILE = os.path.join(INPUT_DIR, 'state-level-census-2020.csv')
OUT_DIR = os.path.join('outputs', 'pilots')

os.makedirs(OUT_DIR, exist_ok=True)

# pilots data
if not os.path.exists(PILOTS_FILE):
    raise SystemExit(f"Missing pilots file: {PILOTS_FILE}")

pilots = pd.read_csv(PILOTS_FILE)
# normalize column names
pilots.columns = [c.strip() for c in pilots.columns]

# population data
if not os.path.exists(POP_FILE):
    raise SystemExit(f"Missing population file: {POP_FILE}")
pop = pd.read_csv(POP_FILE)
pop.columns = [c.strip() for c in pop.columns]
if 'AREA' in pop.columns and 'POPULATION' in pop.columns:
    pop = pop.rename(columns={'AREA': 'state', 'POPULATION': 'population'})
else:
    raise SystemExit('Population file missing expected columns AREA and POPULATION')
# coerce population
pop['population'] = pop['population'].astype(str).str.replace(',', '').str.strip()
pop['population'] = pd.to_numeric(pop['population'], errors='coerce')

# merge on state (left join pilots -> pop)
merged = pilots.merge(pop[['state', 'population']], left_on='state', right_on='state', how='left')

# columns to compute per-100k
cols = {
    'totalPilots': 'totalPilotPer100k',
    'students': 'studentPilotPer100k',
    'private': 'privatePilotPer100k',
    'misc': 'miscPilotPer100k',
    'flightInstructor': 'flightInstructorPer100k',
}

# coerce numeric columns
for c in cols.keys():
    if c in merged.columns:
        merged[c] = pd.to_numeric(merged[c], errors='coerce').fillna(0).astype(int)
    else:
        merged[c] = 0

# helper per100k
def per100k(count, pop):
    try:
        if pd.isna(pop) or pop == 0:
            return float('nan')
        return round(float(count) / float(pop) * 100000.0, 4)
    except Exception:
        return float('nan')

# compute rates
for src_col, out_col in cols.items():
    merged[out_col] = merged.apply(lambda r: per100k(r[src_col], r['population']), axis=1)

out_cols = ['state', 'totalPilotPer100k', 'studentPilotPer100k', 'privatePilotPer100k', 'miscPilotPer100k', 'flightInstructorPer100k']

out_df = pd.DataFrame({
    'state': merged['state'],
    'totalPilotPer100k': merged['totalPilotPer100k'],
    'studentPilotPer100k': merged['studentPilotPer100k'],
    'privatePilotPer100k': merged['privatePilotPer100k'],
    'miscPilotPer100k': merged['miscPilotPer100k'],
    'flightInstructorPer100k': merged['flightInstructorPer100k'],
})

for col in out_df.columns:
    if col not in out_df or out_df[col].isnull().all():
        # attempt to populate from merged
        if col in merged.columns:
            out_df[col] = merged[col]

# write files sorted by each metric descending
metrics = {
    'totalPilotPer100k': 'totalPilotPer100k.csv',
    'studentPilotPer100k': 'studentPilotPer100k.csv',
    'privatePilotPer100k': 'privatePilotPer100k.csv',
    'miscPilotPer100k': 'miscPilotPer100k.csv',
    'flightInstructorPer100k': 'flightInstructorPer100k.csv',
}

for metric, fname in metrics.items():
    sorted_df = out_df.sort_values(metric, ascending=False)
    out_path = os.path.join(OUT_DIR, fname)
    sorted_df.to_csv(out_path, index=False)
    print(f"Wrote {out_path}")

print('Done')
