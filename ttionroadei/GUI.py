"""
This is a mockup of the inputs needed from the user for the post processor to work.
I (AB) am using folders from the current (Oct 10, 2023 MOVES 3) utilities.
This needs to be revised based on how the GUI gets redesigned.

"""
from pathlib import Path
import yaml
import pandas as pd
import logging as lg
from ttionroadei.utils import _add_handler
from ttionroadei.csvxmlpostprc.csvgen import filegenmain


class PostProcessorGUI:
    def __init__(self, ei_folder=None, log_dir=r"./logs"):
        ##### Paths ####################################################################
        # TODO: Change to MOVES 4 utilities structure. I am using old utilities. I am
        #  hard coding. Handle this through a config file or setting.py. Ideally
        #  concatenate the paths based on the `ei_folder` path.
        self.onroadei_fi = ""
        self.offroadei_dir = ""
        self.transvmt_dir = ""
        self.transvmtvht_fi = ""
        self.offroadact_dir = ""
        self.offroadei_fis = dict()
        self.offroadact_fis = dict()
        self.fi_temp_tdm_hpms_rdtype = ""
        self.output_yaml_file = ""
        self.onroadei_dir = ""
        self.ei_folder = None
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
        self.pollutants_dropdown = tuple()
        self.pollutants_selected = tuple()
        self.use_tdm_area_rdtype = True
        self.use_custom_area_rdtype = False
        self.use_hpms_area_rdtype = False
        self.temp_tdm_hpms_rdtype = pd.DataFrame()
        self.temp_tdm_hpms_rdtype_flt = pd.DataFrame()
        self.user_defined_area_rdtype = pd.DataFrame()
        self.conversion_factor = dict()
        self.input_units = dict()
        self.output_units = dict()
        self.pollutants_mrs = dict()

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
            "s",
        ]
        self.daytype_dropdown = [
            "wk",
        ]
        self.pollutants_mrs = ["CO", "NOx", "PM10"]
        self.pollutants_dropdown = self.pollutants_mrs
        # TODO: Give user default option for output units.
        #  Read the units of emission module output.
        self.input_units = {
            "mass": "Grams",
            "energy": "J",
            "distance": "Miles",
        }  # Read from previous module!
        self.output_units = self.input_units
        self.conversion_factor = {"mass": 1, "energy": 1, "distance": 1}

    def set_param(self):
        self.onroadei_dir = Path(
            r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24"
            r"\2020AERR_hgb_mvs31"
            r"\2020AERR_hgb_mvs31_p\HGB\2020\swkd\Outputs\Emission_output"
        )
        self.onroadei_fi = self.onroadei_dir.joinpath("emission_output_VMT.txt")
        self.offroadei_dir = Path(
            r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24"
            r"\2020AERR_hgb_mvs31"
            r"\2020AERR_hgb_mvs31_p\HGB\2020\swkd\Outputs\Emission_output"
        )
        self.offroadei_fis = {
            "APU": self.offroadei_dir.joinpath("emission_output_APU.txt"),
            "ONI": self.offroadei_dir.joinpath("emission_output_ONI.txt"),
            "SHEI": self.offroadei_dir.joinpath("emission_output_SHEI.txt"),
            "SHP": self.offroadei_dir.joinpath("emission_output_SHP.txt"),
            "Starts": self.offroadei_dir.joinpath("emission_output_Starts.txt"),
        }
        self.transvmt_dir = Path(
            r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24"
            r"\2020AERR_hgb_mvs31"
            r"\2020AERR_hgb_mvs31_p\HGB\2020\swkd\Outputs\Summarized_output"
        )
        self.transvmtvht_fi = self.transvmt_dir.joinpath("VMT_st_ft_Summary.txt")
        self.offroadact_dir = Path(
            r"C:\Users\a-bibeka\Documents\Projects_Temp\Utilities_FY24"
            r"\2020AERR_hgb_mvs31"
            r"\2020AERR_hgb_mvs31_p\HGB\2020\swkd\Outputs\Activity_output"
        )
        self.offroadact_fis = {
            "AdjSHP": self.offroadei_dir.joinpath("Adjusted_SHP.txt"),
            "SHP": self.offroadei_dir.joinpath("SHP.txt"),
            "ONI": self.offroadei_dir.joinpath("ONI.txt"),
            "APU_SHEI": self.offroadei_dir.joinpath("Hotelling_Hours.txt"),
            "Starts": self.offroadei_dir.joinpath("Start.txt"),
        }
        self.fi_temp_tdm_hpms_rdtype = (
            r"E:\Texas A&M Transportation Institute"
            r"\HMP - TCEQ Projects - FY2024_Utility_Development\Resources\Input Data"
            r"\Road Type Mapping\RoadType_Designation.csv"
        )
        self.output_yaml_file = Path(self.log_dir).joinpath(
            "postProcessorSelection.yaml"
        )
        self.ei_selected = [
            "EMS",
        ]
        self.area_selected = "HGB"
        self.counties_selected = self.counties_dropdown
        self.year_selected = [
            2020,
        ]
        self.season_selected = self.season_dropdown
        self.daytype_selected = self.daytype_dropdown
        self.pollutants_selected = [
            "CO",
        ]
        # 6. Based on the area chosen, run code in the backend to get the road-type
        # mapping.
        # TODO: Ask the user the road type info needed (MOVES or TDM or HPMS?). Use
        #  defaults?
        # TODO: Read the road type information from previous module runs.
        # TODO: Give option to provide custom mapping of road types based on
        #  following columns:
        #  areaTypeId, areaType, roadTypeId, roadType, mvSroadTypeId, mvSroadType
        self._get_roadtype()
        # 7. Specific the options that need to be generated:
        # a) Detailed CSV files
        # b) Aggregated and Pivoted CSV files
        # TODO: Ask the user for the type of aggregation and pivot.
        # c) XML file

    def _get_roadtype(self):
        # TODO: add error checking to see if the area exisits in the mapping file.
        self.use_tdm_area_rdtype = True
        self.use_hpms_area_rdtype = False
        self.use_moves_area_rdtype = False
        if self.use_tdm_area_rdtype or self.use_hpms_area_rdtype:
            self.temp_tdm_hpms_rdtype = pd.read_csv(self.fi_temp_tdm_hpms_rdtype)
            self.temp_tdm_hpms_rdtype = self.temp_tdm_hpms_rdtype.drop(
                columns=["MOVES_RoadType"]
            )
            self.temp_tdm_hpms_rdtype = self.temp_tdm_hpms_rdtype.rename(
                columns={
                    "Area": "area",
                    "TDM_FunctionClass_Code": "funcClassID",
                    "FunctionClass": "funcClass",
                    "TDM_AreaType_Code": "areaTypeID",
                    "AreaType": "areaType",
                    "MOVES_RoadTypeID": "mvsRoadTypeID",
                }
            )
        if self.use_tdm_area_rdtype:
            try:
                self.temp_tdm_hpms_rdtype_flt = self.temp_tdm_hpms_rdtype[
                    lambda df: df.area == self.area_selected
                ]
                if len(self.temp_tdm_hpms_rdtype_flt) == 0:
                    raise ValueError("Empty raod type mapping file.")
            except ValueError as verr:
                self.logger.error(msg=f"Area type not in the road type file. {verr}")

        if self.use_hpms_area_rdtype:
            self.temp_tdm_hpms_rdtype_flt = self.temp_tdm_hpms_rdtype[
                lambda df: df.area == "VLink"
            ]

        if self.use_moves_area_rdtype:
            ...
            # TODO: do something

    def save_param(self):
        # Define a dictionary to hold all the variables
        self.check_params()
        variables_dict = {
            "ei_folder": self.ei_folder,
            "log_dir": str(self.log_dir),
            "onroadei_dir": str(self.onroadei_dir),
            "onroadei_fi": str(self.onroadei_fi),
            "offroadei_dir": str(self.offroadei_dir),
            "offroadei_fis": {
                key: str(value) for key, value in self.offroadei_fis.items()
            },
            "offroadact_dir": str(self.offroadact_dir),
            "offroadact_fis": {
                key: str(value) for key, value in self.offroadact_fis.items()
            },
            "transvmt_dir": str(self.transvmt_dir),
            "transvmtvht_fi": str(self.transvmtvht_fi),
            "ei_selected": self.ei_selected,
            "area_selected": self.area_selected,
            "counties_selected": self.counties_selected,
            "year_selected": self.year_selected,
            "season_selected": self.season_selected,
            "daytype_selected": self.daytype_selected,
            "pollutants_mrs": self.pollutants_mrs,
            "pollutants_selected": self.pollutants_selected,
            "input_units": self.input_units,
            "output_units": self.output_units.copy(),
            "conversion_factor": self.conversion_factor,
            "fi_temp_tdm_hpms_rdtype": str(self.fi_temp_tdm_hpms_rdtype),
            "use_tdm_area_rdtype": self.use_tdm_area_rdtype,
            "gendetailedcsvfiles": self.gendetailedcsvfiles,
            "genaggpivfiles": self.genaggpivfiles,
            "genxmlfile": self.genxmlfile,
        }
        # Define the output YAML file path

        # Save the variables as YAML
        with open(self.output_yaml_file, "w") as yaml_file:
            yaml.dump(variables_dict, yaml_file, default_flow_style=False)
        self.logger.info(msg=f"Variables saved to {str(self.output_yaml_file)}")

    def get_param(self, param):
        ...

    def check_params(self):
        ...

    def run_pp(self):
        filegenmain(
            gendetailedcsvfiles=self.gendetailedcsvfiles,
            genaggpivfiles=self.genaggpivfiles,
            genxmlfile=self.genxmlfile,
            params=ppgui,
        )


if __name__ == "__main__":
    ppgui = PostProcessorGUI()
    ppgui.provide_options()
    ppgui.set_param()
    ppgui.check_params()
    ppgui.save_param()
    ppgui.run_pp()
