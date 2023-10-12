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
        self.mvs4default_tables = gui_obj.mvs4default_tables
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
        self.summary_dir = gui_obj.out_dir_pp

    def paramqc(self):
        a = 1
        ...

    def _emisprc(self):
        rename_dict = {
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

        id_cols = [
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
        ls_df = []
        for ei in self.ei_fis.keys():
            for cat, path in self.ei_fis[ei].items():
                df = pd.read_csv(path, sep="\t")
                df["EIType"] = ei
                df1 = (
                    df.filter(items=rename_dict.keys())
                    .rename(columns=rename_dict)
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
                    id_vars=id_cols, var_name="actTypeABB", value_name="emission"
                )
                ls_df.append(df2)
        _emis = pd.concat(ls_df)
        _emis1 = _emis.merge(
            self.area_rdtype_df, on=["funcClassID", "areaTypeID"], how="left"
        )
        return _emis1

    def _actprc(self):
        rename_dict = {
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
        id_cols = [
            "FIPS",
            "hour",
            "funcClassID",
            "areaTypeID",
            "sourceUseTypeID",
            "fuelTypeID",
        ]
        ls_df = []
        for cat, path in self.act_fis.items():
            if cat == "TotSHP":
                # Note: removing total SHP. It is a combination of AdjSHP and ONI.
                continue
            df = pd.read_csv(path, sep="\t")
            df1 = (
                df.filter(items=rename_dict.keys())
                .rename(columns=rename_dict)
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
                id_vars=id_cols, var_name="actTypeABB", value_name="activity"
            )
            ls_df.append(df2)
        _act = pd.concat(ls_df)
        _act["activityunits"] = _act.actTypeABB.map(self.settings["activities"])
        _act1 = _act.merge(
            self.area_rdtype_df, on=["funcClassID", "areaTypeID"], how="left"
        )
        return _act1

    def _actemisconnector(self, act, emis):
        ...

    def _actqc(self):
        ...

    def _emisqc(self):
        ...

    def _outputsqc(self):
        ...

    def detailedcsvgen(self):
        act = self._actprc()
        emis = self._emisprc()
        a = 1
        emisprc = self.mvs4default_tables["emisprc"].drop(columns="processName")
        moves_roadtypes = self.mvs4default_tables["moves_roadtypes"].drop(columns="mvsRoadType")
        moves_sut = self.mvs4default_tables["moves_sut"].drop(columns="sourceTypeName")
        moves_ft = self.mvs4default_tables["moves_ft"].drop(columns="fuelTypeDesc")

        act1 = act.merge(moves_roadtypes, on="mvsRoadTypeID"
                          ).merge(moves_sut, on="sourceUseTypeID"
                                  ).merge(moves_ft, on="fuelTypeID")

        emis1 = emis.merge(emisprc, on="processID", how="left"
                  ).merge(moves_roadtypes, on="mvsRoadTypeID"
                          ).merge(moves_sut, on="sourceUseTypeID"
                                  ).merge(moves_ft, on="fuelTypeID")

        # self._actemisconnector(act, emis)

    def aggsccneigen(self):
        ...

    def aggxlsxgen(self):
        ...
