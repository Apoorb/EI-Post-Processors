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

    def paramqc(self):
        a = 1
        ...

    def _emisprc(self, dev_w_mvs3):
        emis_filter_rename_dict = self.settings["emis_rename"]
        emis_id_cols = set(self.settings["csvxml_ei"]["idx"]) - set(
            [
                "pollutantCode",
            ]
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
            for cat, path in self.ei_fis[ei].items():
                df = pd.read_csv(path, sep="\t")
                # FixMe: the revised output from Chaoyi might handle this
                df["EIType"] = ei
                df1 = (
                    df.filter(items=emis_filter_rename_dict.keys())
                    .rename(columns=emis_filter_rename_dict)
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
        _emis_tmp = pd.concat(ls_df)
        _emis_tmp1 = self.outpollutant.merge(_emis_tmp, on="pollutantID", how="left")
        try:
            if set(_emis_tmp1.emissionunits.unique()) != set(
                self.conversion_factor.input_units.values
            ):
                raise ValueError(
                    "The input units selected through the GUI do not match units in the utilities output."
                )
            _emis = (
                _emis_tmp1.rename(columns={"emissionunits": "input_units"})
                .merge(self.conversion_factor, on="input_units")
                .assign(emission=lambda df: df.emission * df.confactor)
                .drop(columns=["input_units", "confactor"])
                .rename(columns={"output_units": "emissionunits"})
            )
        except ValueError as verr:
            self.logger.error(msg=f"{verr}")
            raise
        return _emis

    def _actprc(self, dev_w_mvs3):
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
        _act = pd.concat(ls_df).assign(
            activityunits=lambda df: df.actTypeABB.map(self.settings["activityunits"])
        )
        return _act

    def _actqc(self):
        # ToDo: Add test function:
        set(self.settings["csvxml_act"]).symmetric_difference(set(self.act_df.columns))
        ...

    def _emisqc(self):
        # ToDo: Add test function:
        set(self.settings["csvxml_ei"]).symmetric_difference(set(self.emis_df.columns))
        ...

    def _outputsqc(self):
        # ToDo: Add test function:
        set(self.settings["csvxml_act"]).symmetric_difference(set(self.act_df.columns))
        set(self.settings["csvxml_ei"]).symmetric_difference(set(self.emis_df.columns))

        ...

    def act_add_labs(self, df_):
        columns = [
            item for sublist in self.settings["csvxml_act"].values() for item in sublist
        ]
        return (
            df_.merge(self.area_rdtype_df, on=["funcClassID", "areaTypeID"], how="left")
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
        columns = [
            item for sublist in self.settings["csvxml_ei"].values() for item in sublist
        ]
        return (
            df_.merge(
                self.labels["emisprc"].drop(columns="processName"),
                on="processID",
                how="left",
            )
            .merge(self.area_rdtype_df, on=["funcClassID", "areaTypeID"], how="left")
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

    def detailedcsvgen(self, dev_w_mvs3=True):
        act_df = self._actprc(dev_w_mvs3=dev_w_mvs3)
        emis_df = self._emisprc(dev_w_mvs3=dev_w_mvs3)
        if dev_w_mvs3:
            act_df = act_df.assign(
                dayType=self.dayType_selected * len(act_df),
                season=self.season_selected * len(act_df),
                year=self.year_selected * len(act_df),
            )
            emis_df = emis_df.assign(
                dayType=self.dayType_selected * len(emis_df),
                season=self.season_selected * len(emis_df),
                year=self.year_selected * len(emis_df),
            )
        act_out = self.act_add_labs(act_df)
        emis_out = self.emis_add_labs(emis_df)
        return {"act": act_out, "emis": emis_out}

    def aggxlsxgen(self, act_emis_dict):
        a = 1
        ...

    def aggsccgen(self, act_emis_dict, nei_pols, year, season, daytype):
        # ToDo: Move to xmlgen module
        scc_emis_df = (
            act_emis_dict["emis"]
            .loc[
                lambda df: (df.year == year)
                & (df.season == season)
                & (df.dayType == daytype)
                & (df.pollutantCode.isin(nei_pols))
            ]
            .assign(
                sccNEI=lambda df: self.sccfun(df),
            )
        )
        agg_emis_scc = scc_emis_df.groupby(
            ["FIPS", "sccNEI", "pollutantCode", "emissionunits"], as_index=False
        ).emission.sum()
        scc_act_df = (
            act_emis_dict["act"]
            .loc[lambda df: df.actTypeABB == "VMT"]
            .loc[
                lambda df: (df.year == year)
                & (df.season == season)
                & (df.dayType == daytype)
            ]
            .assign(
                sccNEI=lambda df: self.sccfun(df), E6MILE=lambda df: df.activity / 1e6
            )
        )
        agg_act_scc = scc_act_df.groupby(
            ["FIPS", "sccNEI"], as_index=False
        ).E6MILE.sum()
        df_nei_scc = agg_emis_scc.merge(agg_act_scc, on=["FIPS", "sccNEI"], how="left")
        return df_nei_scc
