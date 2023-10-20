"""
This is a mockup of the inputs needed from the user for the post processor to work.
I (AB) am using folders from the current (Oct 10, 2023 MOVES 3) utilities.
This needs to be revised based on how the GUI gets redesigned.

"""
from pathlib import Path
import pandas as pd
import logging as lg
import yaml

from ttionroadei.utils import (
    _add_handler,
    settings,
    get_labels,
    unit_converter,
    delete_old_log_files,
)
from ttionroadei.csvxmlpostprc.csvxmlgen import CsvXmlGen
from ttionroadei.csvxmlpostprc.xmlgen import XMLGenerator


class PostProcessorGUI:
    """
    PostProcessorGUI is a class for setting up and running the post-processing of emissions
    data from MOVES. This class is responsible for configuring the post-processing
    parameters and generating detailed CSV files, aggregated and pivoted CSV files, and
    XML files for emissions data based on user input and pre-defined settings. It
    provides methods for setting paths, specifying input options, saving parameters,
    and running the post-processing workflow.

    Parameters
    ----------
    ei_base_dir : str or None, optional
        The base directory for MOVES input data. If not provided, the class will use
        default settings.
    log_dir : str, optional
        The directory for log files. Default is "./logs".

    Attributes
    ----------
    log_dir : str
        The directory for log files.
    logger : logging.Logger
        The logger for recording information and errors during the post-processing.
    labels : dict
        A dictionary containing labels for emissions data.
    ei_base_dir : str
        The base directory for MOVES input data.
    out_dir : str
        The output directory for post-processed data.
    out_dir_pp : pathlib.Path
        The output directory for post-processed data as a Path object.
    ei_fis_EMS : dict
        A dictionary containing file paths for different emissions data categories.
    ei_fis_RF : dict
        A dictionary containing file paths for roadway fractions data.
    ei_fis_TEC : dict
        A dictionary containing file paths for total emissions data.
    transvmtvht_fi : str
        The file path for VMT and VHT data.
    offroadact_dir : str
        The directory containing off-road activity data.
    act_fis : dict
        A dictionary containing file paths for different off-road activity data categories.
    fi_temp_tdm_hpms_rdtype : str
        The file path for road type mapping data.
    output_yaml_file : str
        The file path for saving post-processing parameters as a YAML file.
    onroadei_dir : str
        The directory containing on-road emissions data.

    Methods
    -------
    set_paths()
        Set the file and directory paths for input and output data.
    provide_csv_options()
        Specify the input options for post-processing of CSV files.
    set_csv_param()
        Set the post-processing parameters for CSV file generation.
    provide_xml_options()
        Specify the input options for post-processing of XML files.
    set_xml_param()
        Set the post-processing parameters for XML file generation.
    save_params()
        Save the post-processing parameters as a YAML file.
    check_params()
        Check the validity of the provided parameters.
    run_pp()
        Execute the post-processing workflow, including generating CSV and XML files.

    Example Usage
    -------------
    ppgui = PostProcessorGUI()
    ppgui.set_paths()
    ppgui.provide_csv_options()
    ppgui.set_csv_param()
    ppgui.provide_xml_options()
    ppgui.set_xml_param()
    ppgui.check_params()
    ppgui.save_params()
    ppgui.run_pp()
    """

    def __init__(self, ei_base_dir=None, log_dir=r"./logs"):
        ##### Paths ####################################################################
        # TODO: Change to MOVES 4 utilities structure. I am using old utilities. I am
        #  hard coding. Handle this through a config file or setting.py. Ideally
        #  concatenate the paths based on the `ei_base_dir` path.
        self.log_dir = log_dir
        self.logger = lg.getLogger(name=__file__)
        self.logger = _add_handler(dir=self.log_dir, logger=self.logger)
        self.logger.info(
            ".........................Post-processing initialed........................."
        )
        delete_old_log_files(log_directory=log_dir, max_age_in_days=30)
        self.labels = dict()
        self.ei_base_dir = ""
        self.out_dir = ""
        self.out_dir_pp = Path()
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
        ##### Parameters ###############################################################
        self.ei_dropdown = tuple()
        self.eis_selected = tuple()
        self.area_dropdown = tuple()
        self.area_selected = str
        self.FIPS_dropdown = tuple()
        self.FIPSs_selected = tuple()
        self.years_selected = tuple()
        self.year_dropdown = tuple()
        self.season_dropdown = tuple()
        self.seasons_selected = tuple()
        self.daytype_dropdown = tuple()
        self.daytypes_selected = tuple()
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

        # TODO: Ask the user for XML Header details.
        self.gendetailedcsvfiles = True
        self.genaggpivfiles = True
        self.genxmlfile = True
        ##### XML Fields ###############################################################
        self.xml_pollutant_codes_dropdown = list()
        self.xml_year_dropdown = list()
        self.xml_season_dropdown = list()
        self.xml_daytype_dropdown = list()
        self.xml_pollutant_codes_selected = list()
        self.xml_year_selected = int()
        self.xml_season_selected = ""
        self.xml_daytype_selected = ""
        self.xml_data = {"Header": {}, "Payload": {}}

    def set_paths(self):
        """
        Set the file and directory paths for input and output data.

        This method initializes paths to directories and files used in the
        post-processing workflow, such as input data directories and output directories.
        It also configures the logging directory and logger for recording information
        and errors.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.ei_dir = Path(
            r"E:\Texas A&M Transportation Institute\HMP - TCEQ Projects - "
            r"FY2022_HGB2026_"
            r"\_Tasks\Task 3\ad19hgb26_mvs33_w\ad19hgb26_mvs33_p\HGB\2026\p1fr"
            r"\Outputs\Emission_output"
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

    def _get_roadtype(self):
        """Retrieve and process road type data from a mapping file."""
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

    def provide_csv_options(self):
        """
        Specify the input options for post-processing of CSV files.

        This method allows the developer to specify options for post-processing CSV files,
        including the choice of emissions data categories, analysis areas, counties,
        years, seasons, day types, and pollutant codes.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
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
        self.FIPS_dropdown = [
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
        self.pollutant_codes_dropdown = ["CO", "NOx", "PM10", "TEC", "PM25"]
        # ToDo: Need a mapping file for pollutant codes to pollutants
        # FixMe: How are me going to get this inputs?
        #   - NEI pollutants?
        #   - MOVES pollutants?
        #   - Custom pollutants?
        self.pollutant_map = {
            "CO": [2],
            "NOx": [3],
            "PM10": [100, 106, 107],
            "PM25": [110, 116, 117],
            "TEC": [91],  # Should always be selected for TEC EIs
        }
        # ToDo: Choose from the following options
        settings["valid_units"]
        # TODO: Read the units of emission module output. This is constant. Depends
        #  on the MOVES output settings.
        self.input_units = {
            "mass": "grams",
            "energy": "Kilojoules",
        }  # Read from previous module!
        self.output_units = {
            "mass": "short_ton",
            "energy": "MBTU",
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
        # TODO: Use MOVES/ CG's units names

    def set_csv_param(self):
        """
        Set the post-processing parameters for CSV file generation.

        This method sets the post-processing parameters for generating detailed CSV
        files based on the specified options. It includes the selection of emissions
        data categories, areas, counties, years, seasons, day types, and pollutant codes.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.eis_selected = ["EMS", "TEC"]  #  "RF",
        self.area_selected = "HGB"
        self.FIPSs_selected = [
            48201,
            48039,
            48157,
        ]
        self.years_selected = [
            2026,
        ]
        self.seasons_selected = ("p1",)
        self.daytypes_selected = ("fr",)
        self.pollutant_codes_selected = ["CO", "NOx", "PM10", "PM25"]  # , "TEC"
        if "TEC" in self.eis_selected:
            if "TEC" not in self.pollutant_codes_selected:
                self.pollutant_codes_selected.append("TEC")
        self.pollutant_map_codes_selected = {
            k: v
            for k, v in self.pollutant_map.items()
            if k in self.pollutant_codes_selected
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

    def provide_xml_options(self):
        """
        Specify the input options for post-processing of XML files.

        This method allows the user to specify options for post-processing XML files,
        including the choice of pollutant codes, years, seasons, and day types for
        generating the XML output.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.xml_pollutant_codes_dropdown = ["CO", "NOx", "PM10", "PM25"]
        self.xml_year_dropdown = self.years_selected
        self.xml_season_dropdown = self.seasons_selected
        self.xml_daytype_dropdown = self.daytypes_selected

    def set_xml_param(self):
        """
        Set the post-processing parameters for XML file generation.

        This method sets the post-processing parameters for generating XML files based
        on the specified options. It includes the selection of pollutant codes, years,
        seasons, day types, and other XML header information.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.xml_pollutant_codes_selected = ["CO", "NOx", "PM10", "PM25"]
        self.xml_year_selected = 2026
        self.xml_season_selected = "p1"
        self.xml_daytype_selected = "fr"
        self.xml_data["Header"]["id"] = "HGB_20"
        self.xml_data["Header"]["AuthorName"] = "Mogwai Turner"
        self.xml_data["Header"][
            "OrganizationName"
        ] = "Texas Commission on Environmental Quality"
        self.xml_data["Header"]["DocumentTitle"] = "EIS"
        self.xml_data["Header"]["CreationDateTime"] = "2022-04-22T14:32:31"
        self.xml_data["Header"]["Comment"] = (
            "AERR MOVES 3.0.3-based 2020 annual on-road inventories for 254 Texas Counties"
            " produced by TTI; SCCs and pollutant codes per EPA 2020 NEI instructions"
        )

        self.xml_data["Header"]["DataFlowName"] = "CERS_V2"
        self.xml_data["Header"]["Properties"] = {
            "SubmissionType": "QA",
            "DataCategory": "Onroad",
        }
        self.xml_data["Payload"]["UserIdentifier"] = "XMTURN02"
        self.xml_data["Payload"]["ProgramSystemCode"] = "TXCEQ"
        self.xml_data["Payload"]["EmissionsYear"] = f"{self.xml_year_selected}"
        self.xml_data["Payload"]["Model"] = "MOVES"
        self.xml_data["Payload"]["ModelVersion"] = "MOVES3.0.3"
        self.xml_data["Payload"][
            "SubmittalComment"
        ] = "AERR MOVES 3.0.3-based 2020 annual on-road inventories for 254 Texas Counties"
        self.xml_data["Payload"]["ReportingPeriod"] = "O3D"
        self.xml_data["Payload"]["CalculationParameterTypeCode"] = "I"

    def save_params(self):
        """
        Save the post-processing parameters as a YAML file.

        This method saves the post-processing parameters and configuration as a YAML
        file for future reference and reproducibility.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        # Define a dictionary to hold all the variables
        self.check_params()
        variables_dict = {
            "eis_selected": self.eis_selected,
            "area_selected": self.area_selected,
            "counties_selected": self.FIPSs_selected,
            "year_selected": self.years_selected,
            "season_selected": self.seasons_selected,
            "daytype_selected": self.daytypes_selected,
            "pollutant_map_codes_selected": self.pollutant_map_codes_selected,
            "xml_pollutant_codes_selected": self.xml_pollutant_codes_selected,
            "input_units": self.input_units,
            "output_units": self.output_units.copy(),
            "conversion_factor": self.conversion_factor.to_dict(),
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
            "xml_data": self.xml_data,
        }
        # Define the output YAML file path

        # Save the variables as YAML
        with open(self.output_yaml_file, "w") as yaml_file:
            yaml.dump(
                variables_dict, yaml_file, default_flow_style=False, sort_keys=False
            )
        self.logger.info(msg=f"Variables saved to {str(self.output_yaml_file)}")

    def check_params(self):
        """
        Check the validity of the provided parameters.

        This method performs checks to ensure that the provided post-processing
        parameters are valid and consistent. It helps identify potential issues or
        errors in the parameter settings.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        ...

    def process_detailed_csv(self, csvxmlgen, act_out_fi, emis_out_fi):
        """
        Process and combine main module data to develop detailed data and save it as CSV files.

        Parameters
        ----------
        csvxmlgen : CsvXmlGen
            An instance of the CsvXmlGen class for generating CSV and XML files.
        act_out_fi : str
            Path to the output file for detailed activity data.
        emis_out_fi : str
            Path to the output file for detailed emission data.

        Returns
        -------
        dict
            A dictionary containing detailed activity and emission data.
        """
        self.logger.info(
            "Processing and combining main module data to develop detailed data..."
        )
        act_emis_dict = csvxmlgen.detailedcsvgen()
        act_emis_dict["act"].to_csv(act_out_fi, index=False)
        act_emis_dict["emis"].to_csv(emis_out_fi, index=False)
        self.logger.info(
            f"Saved detailed activity and emission data to {str(act_out_fi)} and {str(emis_out_fi)}, respectively."
        )
        return act_emis_dict

    def load_detailed_csv_data(self):
        """
        Load detailed activity and emission data from CSV files.

        Parameters
        ----------
        None

        Returns
        -------
        dict
            A dictionary containing detailed activity and emission data.
        """
        try:
            act_emis_dict = {
                "act": pd.read_csv(self.out_dir_pp.joinpath("activityDetailed.csv")),
                "emis": pd.read_csv(self.out_dir_pp.joinpath("emissionDetailed.csv")),
            }
        except:
            self.logger.error("Generate detailed CSV files to prepare aggregate files!")
            raise
        return act_emis_dict

    def process_aggregate_tables(self, csvxmlgen, act_emis_dict, agg_tab_out_fi):
        """
        Aggregate detailed activity data to develop aggregate tables and save them as
        Excel files.

        Parameters
        ----------
        csvxmlgen : CsvXmlGen
            An instance of the CsvXmlGen class for generating CSV and XML files.
        act_emis_dict : dict
            A dictionary containing detailed activity and emission data.
        agg_tab_out_fi : str
            Path to the output file for aggregate tables.

        Returns
        -------
        None
        """
        self.logger.info(
            "Aggregating detailed activity to develop aggregate tables table..."
        )
        agg_act_emis_dict = csvxmlgen.aggxlsxgen(act_emis_dict)
        with pd.ExcelWriter(agg_tab_out_fi, engine="xlsxwriter") as writer:
            for key, val in agg_act_emis_dict.items():
                val["emis"].to_excel(writer, sheet_name=f"{key}_emis", index=False)
                val["act"].to_excel(writer, sheet_name=f"{key}_act", index=False)
        self.logger.info(f"Saved aggregate tables to {str(agg_tab_out_fi)}.")

    def process_xml_files(self, csvxmlgen, act_emis_dict, xmlscc_csv_out_fi):
        """
        Process and combine detailed activity and emission data to develop XML staging
        table and save it as a CSV file. Then, use the staging table to generate an XML
        file.

        Parameters
        ----------
        csvxmlgen : CsvXmlGen
            An instance of the CsvXmlGen class for generating CSV and XML files.
        act_emis_dict : dict
            A dictionary containing detailed activity and emission data.
        xmlscc_csv_out_fi : str
            Path to the output file for the XML staging table in CSV format.

        Returns
        -------
        None
        """
        self.logger.info(
            "Processing and combining detailed activity and emission data to develop XML staging table..."
        )
        xml_pols = self.xml_pollutant_codes_selected
        xmlscc_df = csvxmlgen.aggsccgen(
            act_emis_dict=act_emis_dict,
            nei_pols=xml_pols,
            xml_year_selected=self.xml_year_selected,
            xml_season_selected=self.xml_season_selected,
            xml_daytype_selected=self.xml_daytype_selected,
        )
        xmlscc_df.to_csv(xmlscc_csv_out_fi, index=False)
        self.logger.info(f"Saved XML staging table to {str(xmlscc_csv_out_fi)}.")

        self.logger.info("Using Metadata and XML staging table to develop XML...")
        xmlscc_df = pd.read_csv(xmlscc_csv_out_fi)
        xmlscc_df_filt = xmlscc_df.loc[
            lambda df: (df.year == self.xml_year_selected)
            & (df.season == self.xml_season_selected)
            & (df.dayType == self.xml_daytype_selected)
        ]
        self.xml_data["Payload"]["Location"] = xmlscc_df_filt
        xml_fi_name = f"{self.area_selected}{self.xml_year_selected}{self.xml_season_selected}{self.xml_daytype_selected}.xml"
        xmlscc_out_fi = self.out_dir_pp.joinpath(xml_fi_name)
        xmlgen_obj = XMLGenerator(self.xml_data)
        tree = xmlgen_obj.generate_xml()
        tree.write(
            xmlscc_out_fi, pretty_print=True, xml_declaration=True, encoding="utf-8"
        )
        self.logger.info(f"Saved XML to {str(xmlscc_out_fi)}.")

    def run_pp(self):
        """
        Execute the post-processing workflow, including generating CSV and XML files.

        This method executes the complete post-processing workflow, which includes
        generating detailed CSV files, aggregated and pivoted CSV files, and XML files
        for emissions data based on the specified parameters and options.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        csvxmlgen = CsvXmlGen(self)
        act_out_fi = self.out_dir_pp.joinpath("activityDetailed.csv")
        emis_out_fi = self.out_dir_pp.joinpath("emissionDetailed.csv")
        xmlscc_csv_out_fi = self.out_dir_pp.joinpath("xmlSCCStagingTable.csv")
        agg_tab_out_fi = self.out_dir_pp.joinpath("aggregateTable.xlsx")
        if self.gendetailedcsvfiles:
            act_emis_dict = self.process_detailed_csv(
                csvxmlgen, act_out_fi, emis_out_fi
            )
        else:
            act_emis_dict = self.load_detailed_csv_data()

        if self.genaggpivfiles:
            self.process_aggregate_tables(csvxmlgen, act_emis_dict, agg_tab_out_fi)

        if self.genxmlfile:
            self.process_xml_files(csvxmlgen, act_emis_dict, xmlscc_csv_out_fi)

        self.logger.info("Post-processing ended")


if __name__ == "__main__":
    ppgui = PostProcessorGUI()
    ppgui.set_paths()
    ppgui.provide_csv_options()
    ppgui.set_csv_param()
    ppgui.provide_xml_options()
    ppgui.set_xml_param()
    ppgui.check_params()
    ppgui.save_params()
    ppgui.run_pp()
