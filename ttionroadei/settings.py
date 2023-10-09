"""
Global settings that can be configured by the user.
"""
import logging as lg

log_console = True
log_file = True
log_filename = "osmnx"
log_level = lg.INFO
log_name = "OSMnx"
logs_folder = ".././logs"
EI_Types = ("EMS", "RF", "TEC")
MOVES4_Default_DB = "movesdb20230615"
MOVES4_Default_TAB = {
    "activitytype": "Activity type IDs and description",
    "emissionprocess": "Emission process IDs, abbreviation, and fullname",
    "fueltype": "Fuel type",
    "pollutant": "Pollutant IDs, abbreviation, and fullname",
    "roadtype": "MOVES road types",
    "scc": "EPA NEI's SCC code and its decomposition",
    "sourceusetype": "Source use type IDs and fullname",
    "sourceusetype": "Source use type IDs, abbreviation, and fullname",
}
sccNei = ("22", "fuelTypeId", "sourceUseTypeId", "00", "80")

csvxml_act = (
    # ToDo: Connect to CG's input DB
    "area",  # Analysis area as defined in CG's DB or custom defined by user using GUI.
    "County",
    "FIPS",
    "year",
    "season",
    "dayType",
    "hour",
    "sccNei",
    "roadTypeId",
    "roadType",
    "mvSroadTypeId",
    "mvSroadType",
    "sourceUseTypeId",
    "sourceUseType",
    "fuelTypeId",
    "fuelType",
    "actTypeId",
    "actTypeAbb",
    "actType",
    "act",
    "unit",
)

csvxml_ei = (
    "area",  # Analysis area as defined in CG's DB or custom defined by user using GUI.
    "County",
    "FIPS",
    "year",
    "season",
    "dayType",
    "hour",
    "sccNei",
    "roadTypeId",
    "roadType",
    "mvSroadTypeId",
    "mvSroadType",
    "sourceUseTypeId",
    "sourceUseType",
    "fuelTypeId",
    "fuelType",
    "eisPollutantCode",  # Some EIS codes are combination of multiple MOVES pollutants.
    "pollutantId" "pollutant",
    "emission",
    "unit",
)

csvxml_comb = {
    "area",
    "County",
    "FIPS",
    "year",
    "season",
    "dayType",
    "hour",
    "sccNei",
    "roadTypeId",
    "roadType",
    "mvSroadTypeId",
    "mvSroadType",
    "sourceUseTypeId",
    "sourceUseType",
    "fuelTypeId",
    "fuelType",
    "eisPollutantCode",
    "pollutantId" "pollutant",
    "emission",
    "emissionUnit",
    "actTypeId",
    "actTypeAbb",
    "actType",
    "act",
    "actUnit",
}

csvxml_aggpiv_opts = {
    "aggByRdSutFt": "Keep base scenario details, road, source use, and fuel type "
    "categories. "
    "Remove details of hour",
    "aggByHrSutFt": "Keep base scenario details (Year, County, Season, DayType), hour, source "
    "use and fuel type categories. Remove details of road type.",
    "aggByHrRd": "Keep base scenario details, hour, and road type categories. "
    "Remove details of source use and fuel type",
    "aggByHr": "Keep base scenario details and hour categories. Remove details of "
    "source use, fuel, and road type",
    "aggByBaseScenario": "Keep base scenario details. Remove details of hour, source use, "
    "fuel, and road type",
}

csvxml_xmlheader = {}
