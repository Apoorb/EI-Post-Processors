"""
This is a mockup of the inputs needed from the user for the post processor to work.
I (AB) am using folders from the current (Oct 10, 2023 MOVES 3) utilities.
This needs to be revised based on how the GUI gets redesigned.

"""
from pathlib import Path
import yaml
import pandas as pd
import logging as lg
import pkg_resources
import yaml

from ttionroadei.utils import _add_handler, settings, get_labels, unit_converter
from ttionroadei.csvxmlpostprc.csvxmlgen import CsvXmlGen


class PostProcessorGUI:
    def __init__(self, ei_base_dir=None, log_dir=r"./logs"):
        ##### Paths ####################################################################
        # TODO: Change to MOVES 4 utilities structure. I am using old utilities. I am
        #  hard coding. Handle this through a config file or setting.py. Ideally
        #  concatenate the paths based on the `ei_base_dir` path.
        self.pollutant_map_codes_nei_selected = dict()
        self.pollutant_codes_nei_dropdown = list()
        self.pollutant_codes_nei_selected = list()
        self.labels = dict()
        self.ei_base_dir = ""
        self.out_dir = ""
        self.out_dir_pp = ""
        self.ei_fis_EMS = dict()
        self.ei_fis_RF = dict()
        self.ei_fis_TEC = dict()
        self.transvmtvht_fi = ""
        self.offroadact_dir = ""
        self.act_fis = dict()
        self.fi_temp_tdm_hpms_rdtype = ""
        self.output_yaml_file = ""
        self.onroadei_dir = ""
        self.ei_base_dir = None
        self.log_dir = log_dir
        self.logger = lg.getLogger(name=__file__)
        self.logger = _add_handler(dir=self.log_dir, logger=self.logger)
        ##### Parameters ###############################################################
        self.ei_dropdown = tuple()
        self.ei_selected = tuple()
        self.area_dropdown = tuple()
        self.area_selected = str
        self.counties_dropdown = tuple()
        self.counties_selected = tuple()
        self.year_selected = tuple()
        self.year_dropdown = tuple()
        self.season_dropdown = tuple()
        self.season_selected = tuple()
        self.daytype_dropdown = tuple()
        self.daytype_selected = tuple()
        self.pollutant_codes_dropdown = tuple()
        # TODO: Need a mapping file to MOVES pollutantIDs
        self.pollutant_map = dict()
        self.pollutant_codes_selected = tuple()
        self.pollutant_map_codes_selected = dict()
        self.use_tdm_area_rdtype = True
        self.use_hpms_area_rdtype = False
        self.tdm_hpms_rdtype = pd.DataFrame()
        self.tdm_hpms_rdtype_flt = pd.DataFrame()
        self.input_units = dict()
        self.output_units = dict()
        self.conversion_factor = pd.DataFrame()

        # TODO: Ask the user for XML header details.
        self.gendetailedcsvfiles = True
        self.genaggpivfiles = True
        self.genxmlfile = True
        ##### XML Fields ###############################################################
        self.document_id = ""
        self.document_title = ""
        self.author_name = ""
        self.user_identifier = ""
        self.organization_name = ""
        self.program_system_code = ""
        self.comment = ""
        self.creation_datetime = ""
        self.submission_type = "QA"
        self.data_category = ""
        self.data_flow_name = ""
        self.model = ""
        self.model_version = ""
        self.submittal_comment = ""
        if self.genxmlfile:
            self.which_years = ()
            self.which_pollutants = ()

    def set_paths(self):
        self.ei_dir = Path(
            r"E:\Texas A&M Transportation Institute\HMP - TCEQ Projects - FY2022_HGB2026_"
            r"\_Tasks\Task 3\ad19hgb26_mvs33_w\ad19hgb26_mvs33_p\HGB\2026\p1fr\Outputs\Emission_output"
        )
        self.ei_fis_EMS = {
            "OnRoad": self.ei_dir.joinpath("emission_output_VMT.txt"),
            "APU": self.ei_dir.joinpath("emission_output_APU.txt"),
            "ONI": self.ei_dir.joinpath("emission_output_ONI.txt"),
            "SHEI": self.ei_dir.joinpath("emission_output_SHEI.txt"),
            "SHP": self.ei_dir.joinpath("emission_output_SHP.txt"),
            "Starts": self.ei_dir.joinpath("emission_output_Starts.txt"),
        }
        self.ei_fis_RF = {
            cat: file_path.parent.joinpath("RF_" + file_path.name)
            for cat, file_path in self.ei_fis_EMS.items()
            if cat != "SHP"
        }
        self.ei_fis_TEC = {
            cat: file_path.parent.joinpath("TEC_" + file_path.name)
            for cat, file_path in self.ei_fis_EMS.items()
            if cat != "SHP"
        }
        self.transvmtvht_fi = Path(self.ei_dir).parent.joinpath(
            "Summarized_output", "VMT_st_ft_Summary.txt"
        )
        self.offroadact_dir = Path(self.ei_dir).parent.joinpath("Activity_output")
        self.act_fis = {
            "OnRoad": self.transvmtvht_fi,
            "AdjSHP": self.offroadact_dir.joinpath("Adjusted_SHP.txt"),
            "TotSHP": self.offroadact_dir.joinpath("SHP.txt"),
            "ONI": self.offroadact_dir.joinpath("ONI.txt"),
            "APU_SHEI": self.offroadact_dir.joinpath("Hotelling_Hours.txt"),
            "Starts": self.offroadact_dir.joinpath("Start.txt"),
        }
        # TODO: Read from CG's Database.
        self.fi_temp_tdm_hpms_rdtype = (
            r"E:\Texas A&M Transportation Institute"
            r"\HMP - TCEQ Projects - FY2024_Utility_Development\Resources\Input Data"
            r"\Road Type Mapping\RoadType_Designation.csv"
        )
        self.output_yaml_file = Path(self.log_dir).joinpath(
            "postProcessorSelection.yaml"
        )
        self.out_dir = Path(r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24")
        self.out_dir_pp = self.out_dir.joinpath("Summary")
        self.out_dir_pp.mkdir(exist_ok=True)

    def provide_options(self):
        # 1. Based on the inventory type chosen for the GUI for the emission calc (
        # previous module),
        # give users dropdown.
        # TODO: Based on the options chosen for earlier modules in the GUI. Give users a
        #  dropdown based on it.
        # TODO: Based on the inventory type chosen, get the detailed table on associated
        #  processes from MOVES default database.  Have a default mapping, but give
        #  users to
        #  define their own
        # 2. Based on the analysis areas (assuming we can have multiple analysis
        # areas) chosen
        # for the GUI for the emission calc, give users a dropdown.
        # TODO: Get the table on analysis areas from CG's DB, and the options chosen for
        #  earlier modules in the GUI.
        #  Have a custom option for user to define an areas based on 254 counties.
        #  Give users a dropdown based on it.
        # 3. Based on the analysis years (assuming we can have multiple analysis
        # years) chosen
        # for the GUI for the emission calc, give users a dropdown.
        # TODO: Based on the analysis years and seasons chosen for earlier modules in
        #  the GUI. Give users a dropdown.
        # 4. Based on the pollutants included in the MRS, need to generate a file on
        # pollutants before
        # , give users a dropdown.
        # TODO: Parse MRS or an intermediate file on pollutants from the previous
        #  module runs.
        # TODO: Based on the pollutant chosen, get the detailed table on pollutants
        #  from MOVES default database
        # 5. Ask the user for any MOVES pollutants that are to be combined.
        # TODO: Give a dropdown to select pollutants to be combined.
        # TODO: Give user option to rename any MOVES pollutants that need to be
        #  renames for NEI?
        self.ei_dropdown = ["EMS", "RF", "TEC"]
        self.area_dropdown = ["HGB", "TX"]
        # File the associated counties from previous modules.
        self.counties_dropdown = [
            48201,
            48039,
            48157,
            48473,
            48339,
            48291,
            48071,
            48167,
        ]
        self.year_dropdown = [2020, 2026]
        self.season_dropdown = [
            "p1",
        ]
        self.daytype_dropdown = [
            "fr",
        ]
        self.pollutant_codes_dropdown = ["CO", "NOx", "PM10"]
        self.pollutant_codes_nei_dropdown = ["CO", "NOx", "PM10"]
        # ToDo: Need a mapping file for pollutant codes to pollutants
        # FixMe: How are me going to get this inputs?
        #   - NEI pollutants?
        #   - MOVES pollutants?
        #   - Custom pollutants?
        self.pollutant_map = {
            "CO": [2],
            "NOx": [3],
            "PM10": [100, 106, 107],
            "TEC": [91],  # Should always be selected for TEC EIs
        }
        # ToDo: Choose from the following options
        settings["valid_units"]
        # TODO: Read the units of emission module output. This is constant. Depends on the MOVES output settings.
        self.input_units = {
            "mass": "grams",
            "energy": "Kilojoules",
        }  # Read from previous module!
        self.output_units = {
            "mass": "pound",
            "energy": "Kilojoules",
        }  # Fixme change this
        mass_confactor = unit_converter(
            in_unit=self.input_units["mass"], out_unit=self.output_units["mass"]
        )
        energy_confactor = unit_converter(
            in_unit=self.input_units["energy"], out_unit=self.output_units["energy"]
        )

        self.conversion_factor = pd.DataFrame(
            {
                "input_units": [self.input_units["mass"], self.input_units["energy"]],
                "output_units": [
                    self.output_units["mass"],
                    self.output_units["energy"],
                ],
                "confactor": [mass_confactor, energy_confactor],
            }
        )
        # TODO: Use MOVES units combinations

    def set_param(self):
        self.ei_selected = ["EMS", "RF", "TEC"]
        self.area_selected = "HGB"
        self.counties_selected = [
            48201,
            48039,
            48157,
        ]
        self.year_selected = [
            2026,
        ]
        self.season_selected = ("p1",)
        self.daytype_selected = ("fr",)
        self.pollutant_codes_selected = ["CO", "NOx", "TEC"]
        self.pollutant_map_codes_selected = {
            k: v
            for k, v in self.pollutant_map.items()
            if k in self.pollutant_codes_selected
        }
        # ToDo NEI pollutants need to be a subset of `self.pollutant_codes_selected`
        self.pollutant_codes_nei_selected = ["CO", "NOx"]
        self.pollutant_map_codes_nei_selected = {
            k: v
            for k, v in self.pollutant_map.items()
            if k in self.pollutant_codes_nei_selected
        }
        # 6. Based on the area chosen, run code in the backend to get the road-type
        # mapping.
        # TODO: Ask the user the road type info needed (MOVES or TDM or HPMS?). Use
        #  defaults?
        # TODO: Read the road type information from previous module runs.
        # TODO: Give option to provide custom mapping of road types based on
        #  following columns:
        #  areaTypeId, areaType, roadTypeId, roadType, mvSroadTypeId, mvSroadType
        self._get_roadtype()
        self.labels = get_labels(
            database_nm=settings.get("MOVES4_Default_DB"),
            user="moves",
            password="moves",
            host="127.0.0.1",
            port=3308,
        )

        # 7. Specific the options that need to be generated:
        # a) Detailed CSV files
        # b) Aggregated and Pivoted CSV files
        # TODO: Ask the user for the type of aggregation and pivot.
        # c) XML file

    def _get_roadtype(self):
        # TODO: add error checking to see if the area exisits in the mapping file.
        self.use_tdm_area_rdtype = True
        self.use_hpms_area_rdtype = False
        if self.use_tdm_area_rdtype or self.use_hpms_area_rdtype:
            self.tdm_hpms_rdtype = pd.read_csv(self.fi_temp_tdm_hpms_rdtype)
            self.tdm_hpms_rdtype = self.tdm_hpms_rdtype.drop(columns=["MOVES_RoadType"])
            self.tdm_hpms_rdtype = self.tdm_hpms_rdtype.rename(
                columns={
                    "Area": "area",
                    "TDM_FunctionClass_Code": "funcClassID",
                    "FunctionClass": "funcClass",
                    "TDM_AreaType_Code": "areaTypeID",
                    "AreaType": "areaType",
                    "MOVES_RoadTypeID": "mvsRoadTypeID",
                }
            )
        area_sel = ""
        if self.use_tdm_area_rdtype:
            area_sel = self.area_selected
        elif self.use_hpms_area_rdtype:
            area_sel = "VLink"
        else:
            area_sel = ""
        try:
            self.tdm_hpms_rdtype_flt = self.tdm_hpms_rdtype[
                lambda df: df.area == area_sel
            ].reset_index(drop=True)
            if (area_sel != "") and (len(self.tdm_hpms_rdtype_flt) == 0):
                raise ValueError("Empty raod type mapping file.")
        except ValueError as verr:
            self.logger.error(msg=f"Area type not in the road type file. {verr}")
        off_net = pd.DataFrame(
            {
                "area": [self.tdm_hpms_rdtype_flt.loc[0, "area"]],
                "funcClassID": [-99],
                "areaTypeID": [-99],
                "areaType": ["N/A"],
                "funcClass": ["Off-Network"],
                "mvsRoadTypeID": [1],
            }
        )
        self.tdm_hpms_rdtype_flt = pd.concat(
            [self.tdm_hpms_rdtype_flt, off_net]
        ).reset_index(drop=True)

    def save_params(self):
        # Define a dictionary to hold all the variables
        self.check_params()
        variables_dict = {
            "ei_selected": self.ei_selected,
            "area_selected": self.area_selected,
            "counties_selected": self.counties_selected,
            "year_selected": self.year_selected,
            "season_selected": self.season_selected,
            "daytype_selected": self.daytype_selected,
            "pollutant_map_codes_selected": self.pollutant_map_codes_selected,
            "pollutant_map_codes_nei_selected": self.pollutant_map_codes_nei_selected,
            "input_units": self.input_units,
            "output_units": self.output_units.copy(),
            "conversion_factor": self.conversion_factor,
            "fi_temp_tdm_hpms_rdtype": str(self.fi_temp_tdm_hpms_rdtype),
            "use_tdm_area_rdtype": self.use_tdm_area_rdtype,
            "gendetailedcsvfiles": self.gendetailedcsvfiles,
            "genaggpivfiles": self.genaggpivfiles,
            "genxmlfile": self.genxmlfile,
            "ei_base_dir": self.ei_base_dir,
            "log_dir": str(self.log_dir),
            "ei_dir": str(self.ei_dir),
            "summary_dir": str(self.out_dir_pp),
            "ei_fis_EMS": {key: str(value) for key, value in self.ei_fis_EMS.items()},
            "ei_fis_RF": {key: str(value) for key, value in self.ei_fis_RF.items()},
            "ei_fis_TEC": {key: str(value) for key, value in self.ei_fis_TEC.items()},
            "act_fis": {key: str(value) for key, value in self.act_fis.items()},
        }
        # Define the output YAML file path

        # Save the variables as YAML
        with open(self.output_yaml_file, "w") as yaml_file:
            yaml.dump(
                variables_dict, yaml_file, default_flow_style=False, sort_keys=False
            )
        self.logger.info(msg=f"Variables saved to {str(self.output_yaml_file)}")

    def get_param(self, param):
        ...

    def check_params(self):
        ...

    def run_pp(self):
        csvxmlgen = CsvXmlGen(self)
        csvxmlgen.paramqc()
        if self.gendetailedcsvfiles:
            act_emis_dict = csvxmlgen.detailedcsvgen()
        if self.genaggpivfiles:
            year = 1
            scenario = 1
            area = 1
            csvxmlgen.aggxlsxgen(self.pollutant_map_codes_nei_selected)
        if self.genxmlfile:
            csvxmlgen.aggsccneigen()


if __name__ == "__main__":
    ppgui = PostProcessorGUI()
    ppgui.set_paths()
    ppgui.provide_options()
    ppgui.set_param()
    ppgui.check_params()
    ppgui.save_params()
    ppgui.run_pp()
    ppgui.get_param()
