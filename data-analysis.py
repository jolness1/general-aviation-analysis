import pandas as pd
import os

# grab main input file
INPUT_DIR = 'inputs'
OS_INPUT_FILE = os.path.join(INPUT_DIR, 'general-aviation-accidents-2012-2021.csv')
df = pd.read_csv(OS_INPUT_FILE)

# clean column names just in case
df.columns = [c.strip() for c in df.columns]

# check injury level column exists
if 'InjuryLevel' not in df.columns:
    raise SystemExit('Input CSV missing InjuryLevel column')

# deal with NaN values as unknown
df['InjuryLevel'] = df['InjuryLevel'].fillna('Unknown').astype(str).str.strip()

# fatal vs non-fatal classification
def classify_injury(s: str) -> str:
    low = s.lower()
    if 'non' in low:
        return 'non-fatal'
    if 'fatal' in low:
        return 'fatal'
    return 'unknown'

df['InjuryCategory'] = df['InjuryLevel'].apply(classify_injury)
df['IsFatal'] = df['InjuryCategory'] == 'fatal'

for col in ['FatalInjuries', 'SeriousInjuries']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    else:
        # if missing data, create column filled with zeros
        df[col] = 0

# replace missing states with 'Unknown' so they are grouped 
# (international flights it looks like: not sure why they are in NTSB data)
df['StateOrRegion'] = df.get('StateOrRegion').fillna('Unknown')

# per-state counts
# count crashes where InjuryCategory indicates fatal vs non-fatal
fatal_counts = (
    df[df['InjuryCategory'] == 'fatal']
    .groupby('StateOrRegion')
    .size()
    .reset_index(name='fatalCrashes')
)
nonfatal_counts = (
    df[df['InjuryCategory'] == 'non-fatal']
    .groupby('StateOrRegion')
    .size()
    .reset_index(name='nonFatalCrashes')
)

fatal_with_reported_inj = (
    df[(df['InjuryCategory'] == 'fatal') & (df['FatalInjuries'] > 0)]
    .groupby('StateOrRegion')
    .size()
    .reset_index(name='fatalCrashesWithReportedFatalInjuries')
)

injury_sums = (
    df.groupby('StateOrRegion')[['FatalInjuries', 'SeriousInjuries']]
    .sum()
    .reset_index()
)

# merge it all
accidents_by_state = (
    fatal_counts
    .merge(nonfatal_counts, on='StateOrRegion', how='outer')
    .merge(injury_sums, on='StateOrRegion', how='left')
    .merge(fatal_with_reported_inj, on='StateOrRegion', how='left')
    .fillna(0)
)

# ensure integer
for col in ['fatalCrashes', 'nonFatalCrashes', 'FatalInjuries', 'SeriousInjuries', 'fatalCrashesWithReportedFatalInjuries']:
    if col in accidents_by_state.columns:
        accidents_by_state[col] = accidents_by_state[col].astype(int)
    else:
        accidents_by_state[col] = 0

# rename column to `state` for output since we just care about that
accidents_by_state = accidents_by_state.rename(columns={'StateOrRegion': 'state'})

# load 2020 population data
pop_file = os.path.join(INPUT_DIR, 'state-level-census-2020.csv')
if os.path.exists(pop_file):
    pop = pd.read_csv(pop_file)
    pop.columns = [c.strip() for c in pop.columns]
    if 'AREA' in pop.columns and 'POPULATION' in pop.columns:
        pop = pop.rename(columns={'AREA': 'state', 'POPULATION': 'population'})
        pop['population'] = pop['population'].astype(str).str.replace(',', '').str.strip()
        pop['population'] = pd.to_numeric(pop['population'], errors='coerce')
    else:
        pop = None
else:
    pop = None

# per-100k calculation
def per100k(crashes, pop):
    try:
        if pd.isna(pop) or pop == 0:
            return float('nan')
        return float(crashes) / float(pop) * 100000.0
    except Exception:
        return float('nan')

accidents_by_state['totalCrashes'] = accidents_by_state['fatalCrashes'] + accidents_by_state['nonFatalCrashes']
if pop is not None:
    accidents_by_state = accidents_by_state.merge(pop[['state', 'population']], on='state', how='left')
else:
    accidents_by_state['population'] = pd.NA

# per-100k rates for fatal/non-fatal/total
accidents_by_state['fatalCrashesPer100kResidents'] = accidents_by_state.apply(lambda r: per100k(r['fatalCrashes'], r['population']), axis=1)
accidents_by_state['nonFatalCrashesPer100kResidents'] = accidents_by_state.apply(lambda r: per100k(r['nonFatalCrashes'], r['population']), axis=1)
accidents_by_state['totalCrashesPer100kResidents'] = accidents_by_state.apply(lambda r: per100k(r['totalCrashes'], r['population']), axis=1)

# Round rates 4 decimal places (keeps small values separated but shortens the output a bit)
for col in ['fatalCrashesPer100kResidents', 'nonFatalCrashesPer100kResidents', 'totalCrashesPer100kResidents']:
    if col in accidents_by_state.columns:
        accidents_by_state[col] = accidents_by_state[col].round(4)

# rename injury columns because of their random leading capital letters
out_df = accidents_by_state.copy()
out_df = out_df.rename(columns={'FatalInjuries': 'fatalInjuries', 'SeriousInjuries': 'seriousInjuries'})
output_columns = ['state', 'fatalCrashes', 'nonFatalCrashes', 'totalCrashes', 'fatalInjuries', 'seriousInjuries', 'fatalCrashesWithReportedFatalInjuries', 'population', 'fatalCrashesPer100kResidents', 'nonFatalCrashesPer100kResidents', 'totalCrashesPer100kResidents']

# fatal accidents by state
fatal_sorted = out_df.sort_values('fatalCrashes', ascending=False)
# output directories
OUT_BASE = 'outputs'
OUT_BY_STATE = os.path.join(OUT_BASE, 'by-state')
OUT_STATE_LIST = os.path.join(OUT_BASE, 'state-list')
OUT_RATE = os.path.join(OUT_BASE, 'accident-rates')
os.makedirs(OUT_BY_STATE, exist_ok=True)
os.makedirs(OUT_STATE_LIST, exist_ok=True)
os.makedirs(OUT_RATE, exist_ok=True)

fatal_out_path = os.path.join(OUT_STATE_LIST, 'fatal-accidents-by-state.csv')
fatal_sorted.to_csv(fatal_out_path, columns=output_columns, index=False)
print(f"Created {fatal_out_path}")

# non-fatal accidents by state
non_fatal_sorted = out_df.sort_values('nonFatalCrashes', ascending=False)
nonfatal_out_path = os.path.join(OUT_STATE_LIST, 'non-fatal-accidents-by-state.csv')
non_fatal_sorted.to_csv(nonfatal_out_path, columns=output_columns, index=False)

# rate sorted files
fatal_rate_sorted = out_df.sort_values('fatalCrashesPer100kResidents', ascending=False)
fatal_rate_out = os.path.join(OUT_RATE, 'fatal-accidents-rate.csv')
fatal_rate_sorted.to_csv(fatal_rate_out, columns=output_columns, index=False)

nonfatal_rate_sorted = out_df.sort_values('nonFatalCrashesPer100kResidents', ascending=False)
nonfatal_rate_out = os.path.join(OUT_RATE, 'non-fatal-accidents-rate.csv')
nonfatal_rate_sorted.to_csv(nonfatal_rate_out, columns=output_columns, index=False)
print(f"Created {nonfatal_out_path}")

total_rate_sorted = out_df.sort_values('totalCrashesPer100kResidents', ascending=False)
total_rate_out = os.path.join(OUT_RATE, 'total-accidents-rate.csv')
total_rate_sorted.to_csv(total_rate_out, columns=output_columns, index=False)

total_by_state_sorted = out_df.sort_values('totalCrashes', ascending=False)
total_by_state_out = os.path.join(OUT_STATE_LIST, 'total-accidents-by-state.csv')
total_by_state_sorted.to_csv(total_by_state_out, columns=output_columns, index=False)
print(f"Created {total_by_state_out}")

# state by state .csv files with crashes (ie Montana.csv, Texas.csv, etc)
os.makedirs('by-state', exist_ok=True)

unique_states = df['StateOrRegion'].unique()
for state in unique_states:
    if pd.isna(state):
        continue
    state_data = df[df['StateOrRegion'] == state]
    # replace special chars in filename
    safe_filename = str(state).replace(' ', '-').replace('/', '-').replace("\\", '-')
    state_data.to_csv(os.path.join(OUT_BY_STATE, f'{safe_filename}.csv'), index=False)

print(f"Created {len(unique_states)} state files in by-state/ directory")
print("Analysis complete!")
