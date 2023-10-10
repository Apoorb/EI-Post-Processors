# 1. Save a list of MOVES pollutants
# 2. Save a list of EI types
# 3. Save a list of road types
# 4. Save a table of EI type + Process map
# 5. Save a table of TDM road type and MOVES road type
# 6. Save a table of default analysis areas and counties

import pandas as pd
from ttionroadei.utils import connect_to_server_db
from ttionroadei.settings import MOVES4_Default_DB, MOVES4_Default_TAB, INV_TYPE

conn = connect_to_server_db(database_nm=MOVES4_Default_DB, port=3308)
emisprc = pd.read_sql(
    "SELECT processID, processName, shortName  FROM emissionprocess WHERE isAffectedByOnroad = 1",
    conn,
)
pollutants = pd.read_sql(
    "SELECT pollutantID, pollutantName, shortName, NEIPollutantCode, energyOrMass FROM pollutant WHERE isAffectedByOnroad = 1",
    conn,
)
conn.close()

# TODO: Change to CG's DB
fi_tdm_hpms_rdtype = (
    r"E:\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - FY2024_Utility_Development\Resources\Input Data"
    r"\Road Type Mapping\RoadType_Designation.csv"
)
tdm_hpms_rdtype = pd.read_csv(fi_tdm_hpms_rdtype)
#######################################################################################

# Default definition of EMS EIs
####################
ems_prcs_EMStup = INV_TYPE["EMS"]["processId"]
emisprc_EMS = emisprc.loc[emisprc.processID.isin(ems_prcs_EMStup)]

# Default definition of Refuelling (RF) EIs
###################
ems_prcs_RFtup = INV_TYPE["RF"]["processId"]
emisprc_RF = emisprc.loc[emisprc.processID.isin(ems_prcs_RFtup)]

# Default definition of Total energy consumption (TEC) EIs
###################
ems_prcs_TECtup = INV_TYPE["TEC"]["pollutantId"]
pollutants_TEC = pollutants.loc[pollutants.pollutantID.isin(ems_prcs_TECtup)]
