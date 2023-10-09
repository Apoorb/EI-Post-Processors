# 1. Save a list of MOVES pollutants
# 2. Save a list of EI types
# 3. Save a list of road types
# 4. Save a table of EI type + Process map
# 5. Save a table of TDM road type and MOVES road type
# 6. Save a table of default analysis areas and counties

from ttionroadei.utils import connect_to_server_db
from ttionroadei.settings import MOVES4_Default_DB

connect_to_server_db(MOVES4_Default_DB)
