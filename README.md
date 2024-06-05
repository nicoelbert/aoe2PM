# aoe2PM - Age of Empires II Object Centric Process Mining

## Overview
`aoe2PM` is a Python package dedicated to facilitating object centric process mining in the context of real-time strategy (RTS) games, specifically Age of Empires II. The package is designed to convert `.aoe2record` files, which contain game replay data, into OCEL2.0-standard event logs that can be used for process mining analyses. This enables detailed examination of player strategies and interactions within the game environment.

## Installation

Before using `aoe2PM`, ensure Python is installed along with the necessary dependencies:
- `mgz`
- `pandas`
- `pm4py`
- `sqlite3`

You can install these packages via pip:
```bash

pip install mgz pandas pm4py
```



## Features
  ```python
`aoe2PM` offers the following function to process Age of Empires II record files:
```
### `exportOCEL_fromRecordfile(match_ids: list, recordfile_path: str = './data/recordfiles/', goal: int = 10000000000, db_path: str = './out/aoe_data_ocel.sqlite', masterdata_path: str = './masterdata/')`
Extracts essential match data from a list of `.aoe2record` files and exporting it to a `.sql`file in the OCEL2.0 stanard in a specified directory. 

#### Parameters

- **`match_ids` (list)**: 
  A list of integers representing the match IDs. These IDs are used to locate and process the corresponding `.aoe2record` files.

- **`recordfile_path` (str, optional)**: 
  The path to the directory where the `.aoe2record` files are stored. Default is `'./data/recordfiles/'`.

- **`goal` (int, optional)**: 
  The maximum number of matches to process. Default is a very large number (`10000000000`), effectively meaning all matches in `match_ids` will be processed unless a different goal is set.

- **`db_path` (str, optional)**: 
  The path to the output SQLite database file where the OCEL 2.0 data will be stored. Default is `'./out/aoe_data_ocel.sqlite'`.

- **`masterdata_path` (str, optional)**: 
  The path to the directory containing master data files required for processing, such as `building_actions.json` and `base_build_times.json`. Default is `'./masterdata/'`.



## Usage

To use the `exportOCEL_fromRecordfile` function from the `aoe2PM` module, follow these steps:

1. Import the necessary module.
2. Define the paths for the record files, database output, and master data.
3. Load a list of match IDs from the record files in the specified directory.
4. Extract the match IDs from the filenames.
5. Pass the list of match IDs to the `exportOCEL_fromRecordfile` function with the desired parameters.
6. The `.sql` file will be exported to the specified database path.

### Example code is available in `demo_notebook.ipynb`. Example data can be found in `./data`.

```python
from aoe2PM import aoeOCEL
import os

# Define the paths for the record files, database output, and master data
recordfile_path = './data/recordfiles/'
db_path = './out/aoe_data_ocel.sqlite'
masterdata_path = './data/masterdata/'

# Load a list of match ids from the recordfiles in the data folder
recordfiles = os.listdir(recordfile_path)

# Extract the match id from the filename
match_ids = [int(file.split('_')[-1].split('.')[0]) for file in recordfiles]

# Pass the list of match ids to the exportOCEL_fromRecordfile function - Set goal to 100 to only export the first 100 matches
aoeOCEL.exportOCEL_fromRecordfile(match_ids, goal=100, db_path=db_path, recordfile_path=recordfile_path)

# .sql file is exported to db_path
```
