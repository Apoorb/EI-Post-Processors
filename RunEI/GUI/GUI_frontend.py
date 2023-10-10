"""
This is a mockup of the inputs needed from the user for the post processor to work.
I (AB) am using folders from the current (Oct 10, 2023 MOVES 3) utilities.
This needs to be revised based on how the GUI gets redesigned.

"""
from pathlib import Path

import pandas as pd
import logging as lg

from ttionroadei.csvxmlpostprc.csvgen import checkinputsmain
from ttionroadei.utils import log, profile, ts
from ttionroadei.settings import logs_folder, base_units

# 1. SELECT INPUTS
########################################################################################

# 0. Orient to the data directory
ei_folder = ""
# TODO: Change to MOVES 4 utilities structure. I am using old utilities. I am hard
#  coding. Handle this through a config file or setting.py. Ideally concatenate the paths
#  based on the `ei_folder` path.

log_dir = r"C:\Users\a-bibeka\PycharmProjects\EI Post Processors\logs"
onroadei_dir = Path(
    r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24\2020AERR_hgb_mvs31"
    r"\2020AERR_hgb_mvs31_p\HGB\2020\swkd\Outputs\Emission_output"
)
onroadei_fi = onroadei_dir.joinpath("emission_output_VMT.txt")
offroadei_dir = Path(
    r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24\2020AERR_hgb_mvs31"
    r"\2020AERR_hgb_mvs31_p\HGB\2020\swkd\Outputs\Emission_output"
)
offroadei_fis = {
    "APU": offroadei_dir.joinpath("emission_output_APU.txt"),
    "ONI": offroadei_dir.joinpath("emission_output_ONI.txt"),
    "SHEI": offroadei_dir.joinpath("emission_output_SHEI.txt"),
    "SHP": offroadei_dir.joinpath("emission_output_SHP.txt"),
    "Starts": offroadei_dir.joinpath("emission_output_Starts.txt"),
}
transvmt_dir = Path(
    r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24\2020AERR_hgb_mvs31"
    r"\2020AERR_hgb_mvs31_p\HGB\2020\swkd\Outputs\Summarized_output"
)
transvmtvht_fi = transvmt_dir.joinpath("VMT_st_ft_Summary.txt")
offroadact_dir = Path(
    r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24\2020AERR_hgb_mvs31"
    r"\2020AERR_hgb_mvs31_p\HGB\2020\swkd\Outputs\Activity_output"
)
offroadei_fis = {
    "AdjSHP": offroadei_dir.joinpath("Adjusted_SHP.txt"),
    "SHP": offroadei_dir.joinpath("SHP.txt"),
    "ONI": offroadei_dir.joinpath("ONI.txt"),
    "APU_SHEI": offroadei_dir.joinpath("Hotelling_Hours.txt"),
    "Starts": offroadei_dir.joinpath("Start.txt"),
}

# 1. Based on the inventory type chosen for the GUI for the emission calc (previous module),
# give users dropdown.
# TODO: Based on the options chosen for earlier modules in the GUI. Give users a
#  dropdown based on it.
# TODO: Based on the inventory type chosen, get the detailed table on associated
#  processes from MOVES default database.  Have a default mapping, but give users to
#  define their own
EI_dropdown = ["EMS", "RF", "TEC"]
EI_selected = [
    "EMS",
]

# 2. Based on the analysis areas (assuming we can have multiple analysis areas) chosen
# for the GUI for the emission calc, give users a dropdown.
# TODO: Get the table on analysis areas from CG's DB, and the options chosen for
#  earlier modules in the GUI.
#  Have a custom option for user to define an areas based on 254 counties.
#  Give users a dropdown based on it.
area_dropdown = ["HGB", "TX"]
area_selected = [
    "HGB",
]
# File the associated counties from previous modules.
counties_dropdown = [48201, 48039, 48157, 48473, 48339, 48291, 48071, 48167]
counties_selected = counties_dropdown
# 3. Based on the analysis years (assuming we can have multiple analysis years) chosen
# for the GUI for the emission calc, give users a dropdown.
# TODO: Based on the analysis years and seasons chosen for earlier modules in the GUI. Give users a
#  dropdown.
year_dropdown = [2020, 2026]
year_selected = [
    2020,
]
season_dropdown = [
    "s",
]
daytype_dropdown = [
    "wk",
]
season_selected = season_dropdown
daytype_selected = daytype_dropdown
# 4. Based on the pollutants included in the MRS, need to generate a file on
# pollutants before
# , give users a dropdown.
# TODO: Parse MRS or an intermediate file on pollutants from the previous module runs.
# TODO: Based on the pollutant chosen, get the detailed table on pollutants from MOVES
#  default database
pollutants_mrs = ["CO", "NOx", "PM10"]

# 5. Ask the user for any MOVES pollutants that are to be combined.
# TODO: Give a dropdown to select pollutants to be combined.
# TODO: Give user option to rename any MOVES pollutants that need to be renames for NEI?
pollutants_dropdown = pollutants_mrs
pollutants_selected = [
    "CO",
]
# TODO: Give user default option for output units.
#  Read the units of emission module output.
input_units = {
    "mass": "Grams",
    "energy": "J",
    "distance": "Miles",
}  # Read from previous module!
output_units = base_units
conversion_factor = {"mass": 1, "energy": 1, "distance": 1}

# 6. Based on the area chosen, run code in the backend to get the road-type mapping.
# TODO: Ask the user the road type info needed (MOVES or TDM or HPMS?). Use defaults?
# TODO: Read the road type information from previous module runs.
# TODO: Give option to provide custom mapping of road types based on following columns:
#  areaTypeId, areaType, roadTypeId, roadType, mvSroadTypeId, mvSroadType
fi_temp_tdm_hpms_rdtype = (
    r"E:\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - FY2024_Utility_Development\Resources\Input Data"
    r"\Road Type Mapping\RoadType_Designation.csv"
)
temp_tdm_hpms_rdtype = pd.read_csv(fi_temp_tdm_hpms_rdtype)
temp_tdm_hpms_rdtype = temp_tdm_hpms_rdtype.rename(
    columns={
        "Area": "area",
        "TDM_FunctionClass_Code": "funcClassID",
        "FunctionClass": "funcClass",
        "TDM_AreaType_Code": "areaTypeID",
        "AreaType": "areaType",
        "MOVES_RoadTypeID": "mvsRoadTypeID",
        "MOVES_RoadType": "mvsRoadType",
    }
)

# The TDM road type for selected area
# TODO: add error checking to see if the area exisits in the mapping file.
area_selected = [
    "ATX",
]
try:
    temp_tdm_hpms_rdtype1 = temp_tdm_hpms_rdtype[lambda df: df.area.isin(area_selected)]
    if len(temp_tdm_hpms_rdtype1) == 0:
        raise ValueError
except ValueError as verr:
    log(f"Area type not in the road type file. {verr}", level=lg.ERROR)

use_tdm_area_rdtype = True
if not use_tdm_area_rdtype:
    use_hpms_area_rdtype = False
    if not use_hpms_area_rdtype:
        use_custom_area_rdtype = True
        user_defined_area_rdtype = pd.DataFrame()

# 7. Specific the options that need to be generated:
# a) Detailed CSV files
# b) Aggregated and Pivoted CSV files
# TODO: Ask the user for the type of aggregation and pivot.
# c) XML file
# TODO: Ask the user for XML header details.
gendetailedcsvfiles = True
genaggpivfiles = True
genxmlfile = True

if genxmlfile:
    which_years = ()
    which_pollutants = ()

    document_id = ""
    document_title = ""
    author_name = ""
    user_identifier = ""
    organization_name = ""
    program_system_code = ""
    comment = ""
    creation_datetime = ""
    submission_type = "QA"
    data_category = ""
    data_flow_name = ""
    model = ""
    model_version = ""
    submittal_comment = ""


def checkparams():
    ...


# 2. Run GUI
########################################################################################
def runGUI():
    if gendetailedcsvfiles:
        ...
    if genaggpivfiles:
        ...
    if genxmlfile:
        ...


if __name__ == "__main__":
    runGUI()
