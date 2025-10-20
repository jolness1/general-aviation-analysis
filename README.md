# General aviation analysis 
## Q: Does Montana experience crashes at a higher rate than other states?
Quick analysis of crash rates in general aviation

### Basic Findings:

- Montana ranks 3rd for fatal & overall crashes and 4th for non-fatal _by population_. Which probably isn't the best metric but gave us a baseline answer to "is there any chance of this being worth digging up flight hour or takeoff/landing data from FAA?" Answer: Yes but data probably ends up showing that numbers are close because there tends to be more general aviation (GA) flight hours/resident in rural states and states with large forests/fire seasons. Montana checks both boxes on that. 

### Project structure:
- `inputs/` - contains the 2020 census and GA data for 2012-2021
- `outputs/` - contains: 
    - `accident-rates/` - total/fatal/non-fatal rate per 100k residents sorted .csv files
    - `by-state/` — state by state data (ie all crashes for Montana are in `Montana.csv`)
    - `state-list/` - tota/fatal/non-fatal sorted by absolute numbers (least useful thing in the output — just for auditing)

### FAQ
#### Q: Do I want to use this? 
##### A: Probably not. Just a quick test to see if the data was worth looking in to further (maybe — seems that accident rates per/100k are skewed due to more GA activity per resident)

#### Q: I'm ignoring you, how do I use this anyways?
##### A: If you insist — set up a venv and install dependencies from requirements.txt (or just use the global dependencies if you want). Then run `python3 data-analysis.py` in the command line. 


### _Notes_:
- Data from [NTSB general aviation dashboard](https://www.ntsb.gov/safety/data/Pages/GeneralAviationDashboard.aspx)
- Again, this probably isn't useful for you
- MIT [License](/LICENSE) 
