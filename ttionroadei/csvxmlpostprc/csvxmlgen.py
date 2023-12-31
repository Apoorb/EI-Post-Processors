"""
Generate detailed and summarized csv files
Created on: 10/17/2023
Created by: Apoorb
"""
import pandas as pd
import logging as lg
from ttionroadei.utils import _add_handler, settings


class CsvXmlGen:
    """
    This class is responsible for processing emissions data and activity data, and
    aggregating them into CSV, xlsx, and XML files. It provides methods for performing data
    quality checks, adding labels to the data, generating detailed CSV files, aggregating
    data, and creating XML files for various scenarios.

    Attributes
    ----------
    gui_obj: object
        An instance of the PostProcessorGUI class, which provides input parameters and
        data sources for post-processing.
    logger: logging.Logger
        A logger for recording information and errors during the data processing.
    settings : dict
        Configuration settings for data processing.
    labels : dict
        Labels for various data categories.
    area_selected : list
        The selected analysis areas for data processing.
    years_selected : list
        The selected years for data processing.
    seasons_selected : list
        The selected seasons for data processing.
    daytypes_selected : list
        The selected day types for data processing.
    FIPSs_selected : list
        The selected FIPS codes for data processing.
    outpollutants : pd.DataFrame
        DataFrame containing selected pollutant codes and mapping to MOVES pollutantIDs.
    EIs_selected : list
        The selected emissions data categories.
    act_fis : dict
        File paths for off-road activity data.
    ei_fis : dict
        File paths for emissions data.
    conversion_factor : pd.DataFrame
        Conversion factors for emissions data units.
    area_rdtype_df : pd.DataFrame
        Dataframe containing area+road type mapping to MOVES.
    sccfun : function
        A lambda function for generating SCC (Source Classification Code) from data.
    sutFtfun : function
        A lambda function for generating SUT-FT labels from data.

    Methods
    -------
    paramqc()
        Perform parameter quality checks to ensure data processing parameters are valid.
    detailedcsvgen(dev_w_mvs3=True)
        Generate detailed CSV files from activity and emissions data.
    aggxlsxgen(act_emis_dict)
        Aggregate detailed activity and emissions data and generate summary Excel files.
    aggsccgen(
        act_emis_dict,
        xml_pols_selected,
        xml_year_selected,
        xml_season_selected,
        xml_daytype_selected,
    )
        Aggregate emissions data to NEI SCCs and return the result as a DataFrame.
    _emisprc()
        Process emissions data and filter it based on selected parameters.
    _actprc()
        Process emissions data and filter it based on selected parameters.
    act_add_labs(df_)
        Add labels to activity data and return the result as a DataFrame.
    emis_add_labs(df_)
        Add labels to emissions data and return the result as a DataFrame.
    """

    def __init__(self, gui_obj):
        self.logger = lg.getLogger(name=__file__)
        self.logger = _add_handler(dir=gui_obj.log_dir, logger=self.logger)
        self.settings = settings
        self.labels = gui_obj.labels
        self.area_selected = gui_obj.area_selected
        self.years_selected = gui_obj.years_selected
        self.seasons_selected = gui_obj.seasons_selected
        self.daytypes_selected = gui_obj.daytypes_selected
        self.FIPSs_selected = gui_obj.FIPSs_selected
        self.outpollutants = (
            pd.DataFrame.from_dict(gui_obj.pollutant_map_codes_selected, orient="index")
            .stack()
            .reset_index()
            .drop(columns="level_1")
        )
        self.outpollutants.columns = ["pollutantCode", "pollutantID"]
        self.EIs_selected = gui_obj.EIs_selected
        self.act_fis = gui_obj.act_fis
        self.ei_fis = {}
        for ei in self.EIs_selected:
            self.ei_fis[ei] = gui_obj.__getattribute__(f"ei_fis_{ei}")
        self.conversion_factor = gui_obj.conversion_factor
        self.area_rdtype_df = gui_obj.tdm_hpms_rdtype_flt
        self.sccfun = (
            lambda df: "22"
            + df.fuelTypeID.astype(str).str.zfill(2)
            + df.sourceUseTypeID.astype(str)
            + "0080"
        )
        self.sutFtfun = lambda df: df.sutLab + "_" + df.ftLab

    def qc_input_units_and_conversion(self, _emis_tmp1):
        try:
            if set() != set(_emis_tmp1.emissionunits.unique()) - set(
                self.conversion_factor.input_units.values
            ):
                raise ValueError(
                    "The input units selected through the GUI do not match units in the utilities output."
                )
        except ValueError as verr:
            self.logger.error(msg=f"{verr}")
            raise

    def _emisprc(self, dev_w_mvs3):
        """
        This method processes emissions data, applies filters, and formats it for
        further processing. It reads emissions data files for different EI
        categories, filters them based on selected parameters such as FIPS codes,
        and renames columns for consistency.

        Parameters
        ----------
        dev_w_mvs3 : bool
            A flag indicating whether the data is from MOVES3 or MOVES4 utilities.

        Returns
        -------
        pd.DataFrame
            Processed emissions data containing pollutant emissions by category and
            attributes.
        """
        emis_filter_rename_dict = self.settings["emis_rename"]
        emis_id_cols = set(self.settings["csvxml_ei"]["idx"]) - set(
            ["pollutantCode", "actTypeABB"]
        )
        # FixMe: Update after MOVES 4 main utilities are ready.
        if dev_w_mvs3:
            emis_id_cols = [
                i
                for i in emis_id_cols
                if i not in ("area", "dayType", "season", "year")
            ]
        ls_df = []
        for ei in self.ei_fis.keys():
            if ei not in self.EIs_selected:
                continue
            for cat, path in self.ei_fis[ei].items():
                df = pd.read_csv(path, sep="\t")
                # FixMe: the revised output from Chaoyi might handle this
                df["EIType"] = ei
                df1 = (
                    df.filter(items=emis_filter_rename_dict.keys())
                    .rename(columns=emis_filter_rename_dict)
                    .loc[
                        lambda df: (df.FIPS.isin(self.FIPSs_selected))
                        # FixMe: Add the following columns and filters for MOVES 4 utilities
                        # & (df.area == self.area_selected)
                        # & (df.year.isin(self.year_selected))
                        # & (df.season.isin(self.season_selected))
                        # & (df.dayType.isin(self.dayType_selected))
                    ]
                )
                df1 = df1.rename(columns={"emission": cat})
                if cat in ["SHP", "ONI", "APU", "SHEI", "Starts"]:
                    df1[["funcClassID", "areaTypeID"]] = -99
                if cat in ["APU", "SHEI"]:
                    df1[["sourceUseTypeID"]] = 62
                    df1[["fuelTypeID"]] = 2
                df2 = df1.melt(
                    id_vars=emis_id_cols, var_name="actTypeABB", value_name="emission"
                )
                ls_df.append(df2)
        _emis_tmp = pd.concat(ls_df)
        _emis_tmp1 = self.outpollutants.merge(_emis_tmp, on="pollutantID", how="left")
        self.qc_input_units_and_conversion(_emis_tmp1)
        _emis = (
            _emis_tmp1.rename(columns={"emissionunits": "input_units"})
            .merge(self.conversion_factor, on="input_units")
            .assign(emission=lambda df: df.emission * df.confactor)
            .drop(columns=["input_units", "confactor"])
            .rename(columns={"output_units": "emissionunits"})
        )
        return _emis

    def _actprc(self, dev_w_mvs3):
        """
        This method processes activity data, applies filters, and formats it
        for further processing. It reads activity data files, filters them based on
        selected parameters such as FIPS codes, and renames columns for consistency.

        Parameters
        ----------
        dev_w_mvs3 : bool
            A flag indicating whether the data is from MOVES3 or MOVES4 utilities.

        Returns
        -------
        pd.DataFrame
            Processed off-road activity data containing activity values by category
            and attributes.
        """
        act_filter_rename_dict = self.settings["act_rename"]
        act_id_cols = set(self.settings["csvxml_act"]["idx"]) - set(
            ["actTypeABB", "activityunits"]
        )
        # FixMe: Update after MOVES 4 main utilities are ready.
        if dev_w_mvs3:
            act_id_cols = [
                i for i in act_id_cols if i not in ("area", "dayType", "season", "year")
            ]
        ls_df = []
        for cat, path in self.act_fis.items():
            if cat == "TotSHP":
                # Note: removing total SHP. It is a combination of AdjSHP and ONI.
                continue
            df = pd.read_csv(path, sep="\t")
            df1 = (
                df.filter(items=act_filter_rename_dict.keys())
                .rename(columns=act_filter_rename_dict)
                .loc[
                    lambda df: (df.FIPS.isin(self.FIPSs_selected))
                    # FixMe: Add the following columns and filters for MOVES 4 utilities
                    # & (df.area == self.area_selected)
                    # & (df.year.isin(self.year_selected))
                    # & (df.season.isin(self.season_selected))
                    # & (df.dayType.isin(self.dayType_selected))
                ]
            )
            if cat in ["AdjSHP", "ONI", "APU_SHEI", "Starts"]:
                df1[["funcClassID", "areaTypeID"]] = -99
            if cat in [
                "APU_SHEI",
            ]:
                df1[["sourceUseTypeID"]] = 62
                df1[["fuelTypeID"]] = 2
            df2 = df1.melt(
                id_vars=act_id_cols, var_name="actTypeABB", value_name="activity"
            )
            ls_df.append(df2)
        _act = pd.concat(ls_df).assign(
            activityunits=lambda df: df.actTypeABB.map(self.settings["activityunits"])
        )
        return _act

    def act_add_labs(self, df_):
        """
        This method adds labels to the activity data and merges it with other relevant
        data, such as area labels, road type labels, and more.

        Parameters
        ----------
        df_ : pd.DataFrame
            The activity data to which labels are added.

        Returns
        -------
        pd.DataFrame
            Activity data with added labels and merged data.
        """
        columns = [
            item for sublist in self.settings["csvxml_act"].values() for item in sublist
        ]
        return (
            df_.merge(
                self.area_rdtype_df,
                on=["area", "funcClassID", "areaTypeID"],
                how="left",
            )
            .merge(self.labels["moves_roadtypes"], on="mvsRoadTypeID")
            .merge(self.labels["moves_sut"], on="sourceUseTypeID")
            .merge(self.labels["moves_ft"], on="fuelTypeID")
            .merge(self.labels["act_lab"], on="actTypeABB")
            .merge(self.labels["county"], on="FIPS")
            .assign(
                sccNEI=lambda df: self.sccfun(df),
                sutFtLabel=lambda df: self.sutFtfun(df),
            )
            .filter(items=columns)
        )

    def emis_add_labs(self, df_):
        """
        This method adds labels to the emissions data and merges it with other relevant
        data, such as area labels, road type labels, pollutant labels, and more.

        Parameters
        ----------
        df_ : pd.DataFrame
            The emissions data to which labels are added.

        Returns
        -------
        pd.DataFrame
            Emissions data with added labels and merged data.
        """
        columns = [
            item for sublist in self.settings["csvxml_ei"].values() for item in sublist
        ]
        return (
            df_.merge(
                self.labels["emisprc"].drop(columns="processName"),
                on="processID",
                how="left",
            )
            .merge(
                self.area_rdtype_df,
                on=["area", "funcClassID", "areaTypeID"],
                how="left",
            )
            .merge(self.labels["moves_roadtypes"], on="mvsRoadTypeID")
            .merge(self.labels["moves_sut"], on="sourceUseTypeID")
            .merge(self.labels["moves_ft"], on="fuelTypeID")
            .merge(self.labels["county"], on="FIPS")
            .merge(self.labels["pollutants"], on="pollutantID")
            .assign(
                sccNEI=lambda df: self.sccfun(df),
                sutFtLabel=lambda df: self.sutFtfun(df),
            )
            .filter(items=columns)
        )

    def qc_areardtype(self, emis_out, act_out):
        data_areardtype = (
            emis_out[["areaTypeID", "funcClassID"]]
            .drop_duplicates()
            .reset_index(drop=True)
            .assign(type="data")
        )
        mapping_used = self.area_rdtype_df[["areaTypeID", "funcClassID"]].assign(
            type="mapping"
        )
        test = data_areardtype.merge(
            mapping_used, on=["areaTypeID", "funcClassID"], how="left"
        )
        try:
            assert test.type_y.isna().sum() == 0
        except AssertionError as aserr:
            self.logger.error(
                "Incorrect road type and area type mapping used. "
                "Switch from TDM to HPMS or vice-versa in `self.use_tdm_area_rdtype` in `GUI.py`"
            )
            raise

    def detailedcsvgen(self, dev_w_mvs3=True):
        """
        This method generates detailed CSV files for activity and emissions data,
        applying various processing steps and adding labels.

        Parameters
        ----------
        dev_w_mvs3 : bool, optional
            A flag indicating whether the data is from MOVES3 or MOVES4 utilities
            (default is True).

        Returns
        -------
        dict
            A dictionary containing detailed CSV data for activities and emissions.
        """
        self.logger.info(msg="ETL activity data from the main modules...")
        act_df = self._actprc(dev_w_mvs3=dev_w_mvs3)
        if dev_w_mvs3:
            act_df = act_df.assign(
                dayType=self.daytypes_selected * len(act_df),
                season=self.seasons_selected * len(act_df),
                year=self.years_selected * len(act_df),
                area=[self.area_selected] * len(act_df),
            )
        act_out = self.act_add_labs(act_df)
        self.logger.info(msg="Processed activity data.")
        self.logger.info(msg="ETL emission data from the main modules...")
        emis_df = self._emisprc(dev_w_mvs3=dev_w_mvs3)
        if dev_w_mvs3:
            emis_df = emis_df.assign(
                dayType=self.daytypes_selected * len(emis_df),
                season=self.seasons_selected * len(emis_df),
                year=self.years_selected * len(emis_df),
                area=[self.area_selected] * len(emis_df),
            )
        self.qc_areardtype(emis_df, act_df)
        emis_out = self.emis_add_labs(emis_df)
        self.logger.info(msg="Processed emission data.")
        return {"act": act_out, "emis": emis_out}

    def aggxlsxgen(self, act_emis_dict):
        """
        Generate aggregated Excel file for activity and emissions data. This method
        aggregates detailed activity and emissions data by different indices.

        Parameters
        ----------
        act_emis_dict : dict
            A dictionary containing detailed activity and emissions data.

        Returns
        -------
        dict
            A dictionary containing aggregated Excel data for activities and emissions.
        """
        self.logger.info(msg="Aggregating detailed activity and emission data...")
        emis_scenario_cols = [
            "EIType",
            "area",
            "FIPS",
            "year",
            "season",
            "dayType",
        ]
        act_scenario_cols = emis_scenario_cols.copy()
        act_scenario_cols.pop(0)
        keep_act = act_scenario_cols + [
            "hour",
            "sutFtLabel",
            "mvsRoadLab",
            "actTypeABB",
            "activityunits",
            "activity",
        ]
        keep_emis = emis_scenario_cols + [
            "pollutantCode",
            "hour",
            "sutFtLabel",
            "mvsRoadLab",
            "actTypeABB",
            "emissionunits",
            "emission",
        ]
        order_act = act_scenario_cols + [
            "hour",
            "sourceUseTypeID",
            "fuelTypeID",
            "mvsRoadTypeID" "actTypeABB",
            "activityunits",
        ]
        order_emis = emis_scenario_cols + [
            "pollutantCode",
            "hour",
            "sourceUseTypeID",
            "fuelTypeID",
            "mvsRoadTypeID" "actTypeABB",
            "emissionunits",
            "mvsRoadLab",
            "emission",
        ]
        act_idx = self.settings["csvxml_act"]["idx"]
        emis_idx = self.settings["csvxml_ei"]["idx"]
        aggdfs = {}
        for aggtype, val in self.settings["xlsxxml_aggpiv_opts"].items():
            remove = val["remove"]
            add = val["add"]
            act_idx1 = set(act_idx) - set(remove) | set(add)
            emis_idx1 = set(emis_idx) - set(remove) | set(add)
            agg_act = (
                act_emis_dict["act"]
                .loc[lambda df: df.actTypeABB != "Speed"]
                .groupby(list(act_idx1), as_index=False)
                .activity.sum()
            )
            act_sort_cols = [i for i in order_act if i in agg_act.columns]
            agg_act = (
                agg_act.sort_values(act_sort_cols)
                .reset_index(drop=True)
                .filter(items=keep_act)
            )
            agg_emis = (
                act_emis_dict["emis"]
                .groupby(list(emis_idx1), as_index=False)
                .emission.sum()
            )
            emis_sort_cols = [i for i in order_emis if i in agg_emis.columns]
            agg_emis = (
                agg_emis.sort_values(emis_sort_cols)
                .reset_index(drop=True)
                .filter(items=keep_emis)
            )
            aggdfs[aggtype] = {
                "act": agg_act,
                "emis": agg_emis,
            }
        self.logger.info(msg="Aggregated detailed activity and emission data.")
        return aggdfs

    def aggsccgen(
        self,
        act_emis_dict,
        xml_pols_selected,
        xml_year_selected,
        xml_season_selected,
        xml_daytype_selected,
    ):
        """
        This method aggregates detailed activity and emissions data to NEI SCCs for
        specific parameters.

        Parameters
        ----------
        act_emis_dict : dict
            A dictionary containing detailed activity and emissions data.
        xml_pols_selected : list
            A list of pollutant codes for NEI.
        xml_year_selected : int
            The selected year for NEI data.
        xml_season_selected : int
            The selected season for NEI data.
        xml_daytype_selected : int
            The selected day type for NEI data.

        Returns
        -------
        pd.DataFrame
            Aggregated data for NEI SCCs (Source Classification Codes).
        """
        self.logger.info(
            msg="Aggregating detailed activity and emission data to NEI SCCs."
        )
        scc_emis_df = (
            act_emis_dict["emis"]
            .loc[
                lambda df: (df.pollutantCode.isin(xml_pols_selected))
                & (df.year == xml_year_selected)
                & (df.season == xml_season_selected)
                & (df.dayType == xml_daytype_selected)
            ]
            .assign(
                sccNEI=lambda df: self.sccfun(df),
            )
        )
        agg_emis_scc = scc_emis_df.groupby(
            [
                "area",
                "year",
                "season",
                "dayType",
                "FIPS",
                "sccNEI",
                "pollutantCode",
                "emissionunits",
            ],
            as_index=False,
        ).emission.sum()
        scc_act_df = (
            act_emis_dict["act"]
            .loc[
                lambda df: (df.actTypeABB == "VMT")
                & (df.year == xml_year_selected)
                & (df.season == xml_season_selected)
                & (df.dayType == xml_daytype_selected)
            ]
            .assign(
                sccNEI=lambda df: self.sccfun(df), E6MILE=lambda df: df.activity / 1e6
            )
        )
        agg_act_scc = scc_act_df.groupby(
            ["area", "year", "season", "dayType", "FIPS", "sccNEI"]
        ).E6MILE.sum()
        df_nei_scc = agg_emis_scc.merge(
            agg_act_scc,
            on=["area", "year", "season", "dayType", "FIPS", "sccNEI"],
            how="left",
        )
        df_nei_scc["E6MILE"] = df_nei_scc.E6MILE.fillna(0)
        self.logger.info(
            msg="Aggregated detailed activity and emission data to NEI SCC."
        )
        return df_nei_scc
