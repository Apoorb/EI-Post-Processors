"""


"""
from pathlib import Path
import yaml
import pandas as pd
import logging as lg
import ttionroadei
from ttionroadei.utils import _add_handler, settings


class CsvXmlGen:
    def __init__(self, gui_obj):
        self.logger = lg.getLogger(name=__file__)
        self.logger = _add_handler(dir=gui_obj.log_dir, logger=self.logger)
        self.settings = settings
        self.labels = gui_obj.labels
        self.area_selected = gui_obj.area_selected
        self.year_selected = gui_obj.year_selected
        self.season_selected = gui_obj.season_selected
        self.dayType_selected = gui_obj.daytype_selected
        self.FIPS_selected = gui_obj.counties_selected
        self.outpollutant = pd.DataFrame.from_dict(
            gui_obj.pollutant_map_codes_selected, orient="index"
        ).reset_index()
        self.outpollutant.columns = ["pollutantCode", "pollutantID"]
        self.ei_selected = gui_obj.ei_selected
        self.act_fis = gui_obj.act_fis
        self.ei_fis = {}
        for ei in self.ei_selected:
            self.ei_fis[ei] = gui_obj.__getattribute__(f"ei_fis_{ei}")
        self.output_units = gui_obj.output_units
        self.conversion_factor = gui_obj.conversion_factor
        self.area_rdtype_df = gui_obj.tdm_hpms_rdtype_flt
        self.sccfun = (
            lambda df: "22"
            + df.fuelTypeID.astype(str).str.zfill(2)
            + df.sourceUseTypeID.astype(str)
            + "0080"
        )
        self.sutFtfun = lambda df: df.sutLab + "_" + df.ftLab
        self.act_df = pd.DataFrame()
        self.emis_df = pd.DataFrame()

    def paramqc(self):
        a = 1
        ...

    def _emisprc(self, test_w_mvs3):
        emis_rename_dict = {
            "dayType": "dayType",
            "season": "season",
            "year": "year",
            "EIType": "EIType",
            "County": "FIPS",
            "Hour": "hour",
            "hour": "hour",
            "Roadtype": "funcClassID",
            "Areatype": "areaTypeID",
            "Soucetype": "sourceUseTypeID",
            "SUT": "sourceUseTypeID",
            "Fueltype": "fuelTypeID",
            "Pollutant": "pollutantID",
            "Process": "processID",
            "Emission": "emission",
            "Unit": "emissionunits",
            "VMT_Emission": "VMT",
            "VMT_RF_Emission": "VMT",
            "VMT_TEC_Emission": "VMT",
            "Adjusted SHP Emission": "AdjSHP",
            "ONI Emission": "ONI",
            "ONI RF Emission": "ONI",
            "ONI TEC Emission": "ONI",
            "RF Emission": "emission",
            "Start Emission": "Starts",
            "Start RF Emission": "Starts",
            "Start TEC Emission": "Starts",
            "TEC Emission": "emission",
        }

        emis_id_cols = [
            "dayType",
            "season",
            "year",
            "EIType",
            "FIPS",
            "hour",
            "funcClassID",
            "areaTypeID",
            "sourceUseTypeID",
            "fuelTypeID",
            "pollutantID",
            "processID",
            "emissionunits",
        ]

        # FixMe: Update after MOVES 4 main utilities are ready.
        if test_w_mvs3:
            emis_id_cols = [i for i in emis_id_cols if i not in ("dayType", "season", "year")]
        ls_df = []
        for ei in self.ei_fis.keys():
            for cat, path in self.ei_fis[ei].items():
                df = pd.read_csv(path, sep="\t")
                df["EIType"] = ei
                df1 = (
                    df.filter(items=emis_rename_dict.keys())
                    .rename(columns=emis_rename_dict)
                    .loc[
                        lambda df: (df.FIPS.isin(self.FIPS_selected))
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
        _emis = pd.concat(ls_df)
        _emis1 = _emis.merge(
            self.area_rdtype_df, on=["funcClassID", "areaTypeID"], how="left"
        )
        return _emis1

    def _actprc(self, test_w_mvs3):
        act_rename_dict = {
            "dayType": "dayType",
            "season": "season",
            "year": "year",
            "County": "FIPS",
            "Hour": "hour",
            "Roadtype": "funcClassID",
            "Areatype": "areaTypeID",
            "Soucetype": "sourceUseTypeID",
            "SUT": "sourceUseTypeID",
            "Fueltype": "fuelTypeID",
            "Starts Calculated": "Starts",
            "Adjust SHP Calculated": "AdjSHP",
            "ONI Calculated": "ONI",
            "SHEI Calculated": "SHEI",
            "APU Calculated": "APU",
            "VMT Calculated": "VMT",
            "VHT Calculated": "VHT",
            "Speed": "Speed",
        }
        act_id_cols = [
            "dayType",
            "season",
            "year",
            "FIPS",
            "hour",
            "funcClassID",
            "areaTypeID",
            "sourceUseTypeID",
            "fuelTypeID",
        ]
        # FixMe: Update after MOVES 4 main utilities are ready.
        if test_w_mvs3:
            act_id_cols = [i for i in act_id_cols if i not in ("dayType", "season", "year")]
        ls_df = []
        for cat, path in self.act_fis.items():
            if cat == "TotSHP":
                # Note: removing total SHP. It is a combination of AdjSHP and ONI.
                continue
            df = pd.read_csv(path, sep="\t")
            df1 = (
                df.filter(items=act_rename_dict.keys())
                .rename(columns=act_rename_dict)
                .loc[
                    lambda df: (df.FIPS.isin(self.FIPS_selected))
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
        _act = pd.concat(ls_df)
        _act["activityunits"] = _act.actTypeABB.map(self.settings["activities"])
        _act1 = _act.merge(
            self.area_rdtype_df, on=["funcClassID", "areaTypeID"], how="left"
        )
        return _act1

    def _actqc(self):
        ...

    def _emisqc(self):
        ...

    def _outputsqc(self):
        ...

    def act_add_labs(self, df):
        return (
            (
                df.merge(self.labels["moves_roadtypes"], on="mvsRoadTypeID")
                .merge(self.labels["moves_sut"], on="sourceUseTypeID")
                .merge(self.labels["moves_ft"], on="fuelTypeID")
                .merge(self.labels["act_lab"], on="actTypeABB")
                .merge(self.labels["county"], on="FIPS")
            )
            .assign(
                sccNEI=lambda df: self.sccfun(df),
                sutFtLabel=lambda df: self.sutFtfun(df),
            )
            .filter(items=self.settings["csvxml_act"])
        )

    def emis_add_labs(self, df):
        return (
            df.merge(
                self.labels["emisprc"].drop(columns="processName"),
                on="processID",
                how="left",
            )
            .merge(self.labels["moves_roadtypes"], on="mvsRoadTypeID")
            .merge(self.labels["moves_sut"], on="sourceUseTypeID")
            .merge(self.labels["moves_ft"], on="fuelTypeID")
            .merge(self.labels["county"], on="FIPS")
            .assign(
                sccNEI=lambda df: self.sccfun(df),
                sutFtLabel=lambda df: self.sutFtfun(df),
            )
            .filter(items=self.settings["csvxml_ei"])
        )

    def detailedcsvgen(self, test_w_mvs3=True):
        self.act_df = self._actprc(test_w_mvs3=test_w_mvs3)
        emis = self._emisprc(test_w_mvs3=test_w_mvs3)
        if test_w_mvs3:
            self.act_df = self.act_df.assign(
                dayType=self.dayType_selected * len(self.act_df),
                season=self.season_selected * len(self.act_df),
                year=self.year_selected * len(self.act_df),
            )
            emis = emis.assign(
                dayType=self.dayType_selected * len(emis),
                season=self.season_selected * len(emis),
                year=self.year_selected * len(emis),
            )
        self.emis_df = self.outpollutant.merge(
            emis, on="pollutantID", how="left"
        ).merge(self.labels["pollutants"], on="pollutantID")
        act_out = self.act_add_labs(self.act_df)
        emis_out = self.emis_add_labs(self.emis_df)
        # ToDo: Add test function:
        set(self.settings["csvxml_ei"]).symmetric_difference(set(self.emis_df.columns))
        set(self.settings["csvxml_act"]).symmetric_difference(set(self.act_df.columns))

        return {"act": act_out, "emis": emis_out}

    def aggsccneigen(self):

        ...

    def aggxlsxgen(self):
        ...
