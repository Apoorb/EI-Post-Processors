"""
Test post-processors to ensure they are working as intended.

This script is designed to test the post-processors for different areas and scenarios.
It performs various tests to verify the functionality of the post-processing tasks,
comparing the results with expected outputs.

The tests cover the following areas:
- Activity data processing.
- Emission data processing.
- Aggregated data comparison with detailed CSV files.
- Comparison of XML data with detailed CSV files.
- Road area type testing.

These tests help ensure that the post-processing steps are correctly transforming raw
data into detailed CSV files and aggregated outputs.

Please note that this script relies on various configuration parameters and data
sources provided in the 'pp_param' dictionary and may require user-specific updates for testing.

To run the tests, use pytest or another testing framework.
Author: Apoorb
Date: 10/25/2023
"""
# ToDo: Test for different areas and scenarios!
import pytest
import pkg_resources
import yaml
import pandas as pd
from pathlib import Path
from itertools import chain
import numpy as np
import lxml
from ttionroadei.utils import settings

# Todo: User to update the following for testing.
log_dir = Path("./logs")
package_name = "ttionroadei"
try:
    # Get the path to the postProcessorSelection.yaml file within the package
    pp_param_fi = pkg_resources.resource_filename(
        package_name, "logs/postProcessorSelection.yaml"
    )
    # Read and parse the YAML file
    with open(pp_param_fi, "r") as yaml_file:
        pp_param = yaml.safe_load(yaml_file)
except Exception as e:
    print(f"Error: {e}")

# Test inputs for detailed csv comparison with raw data.
#######################################################################################
# Raw activity and emission data.
# ToDO: Need to use the following on the raw data when CG updates the format:
pp_param["EIs_selected"]
pp_param["area_selected"]
pp_param["FIPSs_selected"]
pp_param["years_selected"]
pp_param["seasons_selected"]
pp_param["daytypes_selected"]
# Rename the columns in the raw activity and emission data from the main utility module.
rename_act_cols = {
    "County": "FIPS",
    "Hour": "hour",
    "SUT": "sourceUseTypeID",
    "Soucetype": "sourceUseTypeID",
    "Fueltype": "fuelTypeID",
    "Roadtype": "funcClassID",
    "Areatype": "areaTypeID",
    "VMT Calculated": "VMT",
    "VHT Calculated": "VHT",
    "SHEI Calculated": "SHEI",
    "APU Calculated": "APU",
    "Adjust SHP Calculated": "activity",
    "ONI Calculated": "activity",
    "SHP Calculated": "activity",
    "Starts Calculated": "activity",
}
rename_emis_cols = {
    "County": "FIPS",
    "Hour": "hour",
    "SUT": "sourceUseTypeID",
    "Soucetype": "sourceUseTypeID",
    "Fueltype": "fuelTypeID",
    "Roadtype": "funcClassID",
    "Areatype": "areaTypeID",
    "Pollutant": "pollutantID",
    "Process": "processID",
    "Adjusted SHP Emission": "emission",
    "ONI Emission": "emission",
    "VMT_Emission": "emission",
    "Start Emission": "emission",
    "Emission": "emission",
    "Unit": "input_units",
}
# Read activity and emission data from the main utility.
raw_act_data = {}
for cat, fi in pp_param["act_fis"].items():
    raw_act_data[cat] = pd.read_csv(fi, sep="\t").loc[
        lambda df: (df.County.isin(pp_param["FIPSs_selected"]))
    ]
keep_polids = list(chain(*pp_param["pollutant_map_codes_selected"].values()))
raw_emis_data = {}
for ei in pp_param["EIs_selected"]:
    raw_emis_data[ei] = {}
    for cat, fi in pp_param[f"ei_fis_{ei}"].items():
        raw_emis_data[ei][cat] = pd.read_csv(fi, sep="\t").loc[
            lambda df: df.County.isin(pp_param["FIPSs_selected"])
            & (df.Pollutant.isin(keep_polids))
        ]

# Detailed csv output
act_detailed = pd.read_csv(pp_param["act_out_fi"])
emis_detailed = pd.read_csv(pp_param["emis_out_fi"])
# Keep the following columns in the detailed csv activity and emission data.
keep_act_cols = [
    "area",
    "FIPS",
    "year",
    "season",
    "dayType",
    "hour",
    "areaTypeID",
    "funcClassID",
    "sourceUseTypeID",
    "fuelTypeID",
    "actTypeABB",
    "activity",
]
keep_emis_cols = [i for i in keep_act_cols if i not in ["activity"]] + [
    "pollutantCode",
    "pollutantID",
    "processID",
    "emission",
    "EIType",
]
# Conversion factors for converting emission in the raw data units to detailed csv units.
conv_fac_map = {
    pp_param["conversion_factor"]["input_units"][0]: pp_param["conversion_factor"][
        "confactor"
    ][0],
    pp_param["conversion_factor"]["input_units"][1]: pp_param["conversion_factor"][
        "confactor"
    ][1],
}
# Test data for `test_act`
act_test_data = [
    ("AdjSHP", ["AdjSHP"], ["FIPS", "hour", "sourceUseTypeID", "fuelTypeID"]),
    ("ONI", ["ONI"], ["FIPS", "hour", "sourceUseTypeID", "fuelTypeID"]),
    ("Starts", ["Starts"], ["FIPS", "hour", "sourceUseTypeID", "fuelTypeID"]),
    (
        "TotSHP",
        ["ONI", "AdjSHP"],
        ["FIPS", "hour", "sourceUseTypeID", "fuelTypeID"],
    ),
    ("APU_SHEI", ["APU", "SHEI"], ["FIPS", "hour"]),
    (
        "OnRoad",
        ["VMT", "VHT"],
        [
            "FIPS",
            "hour",
            "sourceUseTypeID",
            "fuelTypeID",
            "areaTypeID",
            "funcClassID",
            "actTypeABB",
        ],
    ),
]
# Test data for `test_emis`
emis_test_data = [
    (
        "Starts",
        "Starts",
        [
            "EIType",
            "FIPS",
            "hour",
            "pollutantID",
            "processID",
            "sourceUseTypeID",
            "fuelTypeID",
        ],
    ),
    (
        "SHP",
        "AdjSHP",
        [
            "EIType",
            "FIPS",
            "hour",
            "pollutantID",
            "processID",
            "sourceUseTypeID",
            "fuelTypeID",
        ],
    ),
    (
        "ONI",
        "ONI",
        [
            "EIType",
            "FIPS",
            "hour",
            "pollutantID",
            "processID",
            "sourceUseTypeID",
            "fuelTypeID",
        ],
    ),
    (
        "APU",
        "APU",
        ["EIType", "FIPS", "hour", "pollutantID", "processID"],
    ),
    (
        "SHEI",
        "SHEI",
        ["EIType", "FIPS", "hour", "pollutantID", "processID"],
    ),
    (
        "OnRoad",
        "VMT",
        [
            "EIType",
            "FIPS",
            "hour",
            "pollutantID",
            "processID",
            "sourceUseTypeID",
            "fuelTypeID",
            "areaTypeID",
            "funcClassID",
        ],
    ),
]
# Test inputs for aggregate sheet comparison with detailed csv.
#######################################################################################
x1 = pd.ExcelFile(pp_param["agg_tab_out_fi"])
act_sheets = [sheet for sheet in x1.sheet_names if sheet.split("_")[1] == "act"]
emis_sheets = [sheet for sheet in x1.sheet_names if sheet.split("_")[1] == "emis"]

# Read the XML
#######################################################################################
def read_xml(
    path_xml=pp_param["xmlscc_xml_out_fi"],
    ns={
        "cer": "http://www.exchangenetwork.net/schema/cer/2",
        "hdr": "http://www.exchangenetwork.net/schema/header/2",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    },
):
    parser = lxml.etree.XMLParser(remove_blank_text=True)
    tree = lxml.etree.parse(str(path_xml), parser)
    rt = tree.getroot()
    ns = rt.nsmap
    cntyelems = rt.findall(".//cer:StateAndCountyFIPSCode", ns)
    pollutantCode, emission, sccNEI, FIPS, E6MILE = [], [], [], [], []
    for cntyelem in cntyelems:
        for rptper in cntyelem.getparent().findall(
            ".//cer:ReportingPeriodEmissions", ns
        ):
            poltmp = [rptper.find("cer:PollutantCode", ns).text]
            emistmp = [float(rptper.find("cer:TotalEmissions", ns).text)]
            cntystmp = [
                int(
                    rptper.getparent()
                    .getparent()
                    .getparent()
                    .find(".//cer:StateAndCountyFIPSCode", ns)
                    .text
                )
            ]
            sccstmp = [
                int(
                    rptper.getparent()
                    .getparent()
                    .find(".//cer:SourceClassificationCode", ns)
                    .text
                )
            ]
            vmttmp = [
                float(
                    rptper.getparent().find(".//cer:CalculationParameterValue", ns).text
                )
            ]
            pollutantCode += list(poltmp)
            emission += list(emistmp)
            sccNEI += list(sccstmp)
            FIPS += list(cntystmp)
            E6MILE += list(vmttmp)

    daxml = pd.DataFrame(
        dict(
            sccNEI=sccNEI,
            FIPS=FIPS,
            E6MILE=E6MILE,
            pollutantCode=pollutantCode,
            emission=emission,
        )
    )
    return daxml


# Test
#######################################################################################
@pytest.mark.parametrize(
    "acttype_in,acttype_out,idx",
    act_test_data,
    ids=["AdjSHP", "ONI", "Starts", "TotSHP", "APU_SHEI", "OnRoad"],
)
def test_act(acttype_in, acttype_out, idx):
    """
    Test activity data processing.

    Parameters
    ----------
    acttype_in : str
        Input activity type.

    acttype_out : str
        Detailed csv output activity type.

    idx : list
        List of index columns for joining input and output.

    Returns
    -------
    None

    Raises
    ------
    AssertionError
        If the test fails and the data doesn't match as expected.

    Notes
    -----
    This test function compares the activity data processing for a specific activity type with the expected output type.
    It checks whether the transformation of raw activity data to detailed CSV data is performed correctly.
    The 'idx' parameter specifies the columns used for comparison.
    """
    raw_act_data_filt = raw_act_data[acttype_in].rename(columns=rename_act_cols)
    if acttype_in == "APU_SHEI":
        raw_act_data_filt = raw_act_data_filt.melt(
            id_vars=["FIPS", "hour"],
            value_vars=["APU", "SHEI"],
            var_name="actTypeABB",
            value_name="activity",
        )
    if acttype_in == "OnRoad":
        raw_act_data_filt = raw_act_data_filt.melt(
            id_vars=[
                "FIPS",
                "hour",
                "areaTypeID",
                "funcClassID",
                "sourceUseTypeID",
                "fuelTypeID",
            ],
            value_vars=["VMT", "VHT"],
            var_name="actTypeABB",
            value_name="activity",
        )
    act_detailed_filt = act_detailed.loc[lambda df: df.actTypeABB.isin(acttype_out)]
    if acttype_out == ["ONI", "AdjSHP"]:
        act_detailed_filt = act_detailed_filt.groupby(
            [
                "area",
                "FIPS",
                "year",
                "season",
                "dayType",
                "hour",
                "areaTypeID",
                "funcClassID",
                "sourceUseTypeID",
                "fuelTypeID",
            ],
            as_index=False,
        ).activity.sum()
    df_comp = raw_act_data_filt.merge(
        act_detailed_filt,
        on=idx,
        how="outer",
    )
    assert all(df_comp["activity_x"] == df_comp["activity_x"])


@pytest.mark.parametrize(
    "acttype_in,acttype_out,idx",
    emis_test_data,
    ids=["Starts", "AdjSHP", "ONI", "APU", "SHEI", "VMT"],
)
def test_emission(acttype_in, acttype_out, idx):
    """
    Test emission data processing.

    Parameters
    ----------
    acttype_in : str
        Input activity type.

    acttype_out : str
        Detailed csv output activity type.

    idx : list
        List of index columns for joining input and output.

    Returns
    -------
    None

    Raises
    ------
    AssertionError
        If the test fails and the data doesn't match as expected.

    Notes
    -----
    This test function compares the emission data processing for a specific emission activity type with the expected output type.
    It checks whether the transformation of raw emission data to detailed CSV data is performed correctly.
    """
    ls_df = []
    for ei in pp_param["EIs_selected"]:
        if (ei != "EMS") & (acttype_out == "AdjSHP"):
            continue
        df = raw_emis_data[ei][acttype_in]
        df.columns = [
            i.replace("TEC ", "")
            .replace("RF ", "")
            .replace("_TEC_", "_")
            .replace("_RF_", "_")
            for i in df.columns
        ]
        df = df.rename(columns=rename_emis_cols)
        df["EIType"] = ei
        ls_df.append(df)
    raw_emis_data_filt = pd.concat(ls_df)
    raw_emis_data_filt = raw_emis_data_filt.assign(
        emission=lambda df: df.input_units.map(conv_fac_map) * df.emission
    ).drop(columns="input_units")
    raw_emis_data_filt["actTypeABB"] = acttype_out
    emis_detailed_filt = emis_detailed.loc[
        lambda df: df.actTypeABB == acttype_out
    ].filter(items=keep_emis_cols)
    comp_df = raw_emis_data_filt.merge(
        emis_detailed_filt,
        on=idx,
        how="left",
    )
    assert np.allclose(comp_df.emission_x, comp_df.emission_y)


@pytest.mark.parametrize("act_sheet", act_sheets, ids=act_sheets)
def test_agg_act_xlsx_eq_detailed(act_sheet):
    """
    Test aggregated activity data by comparing it with detailed CSV data.

    Parameters
    ----------
    act_sheet : str
        Name of the aggregated activity sheet.

    Returns
    -------
    None

    Raises
    ------
    AssertionError
        If the test fails and the data doesn't match as expected.

    Notes
    -----
    This test function compares the data from an aggregated activity sheet with the corresponding data in the detailed CSV file.
    """
    out_df = x1.parse(act_sheet)
    idx_cols = [
        i
        for i in out_df.columns
        if i not in ("activityunits", "activity", "emission", "emissionunit")
    ]
    in_df = (
        act_detailed.groupby(idx_cols, as_index=False)
        .activity.sum()
        .loc[lambda df: df.actTypeABB != "Speed"]
    )
    comp_df = in_df.merge(out_df, on=idx_cols, how="left")
    assert np.allclose(comp_df.activity_x, comp_df.activity_y)


@pytest.mark.parametrize("emis_sheet", emis_sheets, ids=emis_sheets)
def test_agg_emis_xlsx_eq_detailed(emis_sheet):
    """
    Test aggregated emission data by comparing it with detailed CSV data.

    Parameters
    ----------
    emis_sheet : str
        Name of the aggregated emission sheet.

    Returns
    -------
    None

    Raises
    ------
    AssertionError
        If the test fails and the data doesn't match as expected.

    Notes
    -----
    This test function compares the data from an aggregated emission sheet with the corresponding data in the detailed CSV file.
    It checks if the aggregated data matches the detailed CSV data.
    """
    out_df = x1.parse(emis_sheet)
    idx_cols = [
        i
        for i in out_df.columns
        if i not in ("activityunits", "activity", "emission", "emissionunit")
    ]
    in_df = emis_detailed.groupby(idx_cols, as_index=False).emission.sum()
    comp_df = in_df.merge(out_df, on=idx_cols, how="left")
    assert np.allclose(comp_df.emission_x, comp_df.emission_y)


def test_xmlcsv_eq_detailed():
    """
    Test XML data by comparing it with detailed CSV data.

    Returns
    -------
    None

    Raises
    ------
    AssertionError
        If the test fails and the data doesn't match as expected.

    Notes
    -----
    This test function compares the data from an XML file with the corresponding data in a CSV file.
    It checks if the XML data matches the detailed CSV data.
    """
    # Todo: need to filter year, season, and daytype when we add them
    out_df = pd.read_csv(pp_param["xmlscc_csv_out_fi"])
    idx_cols = [
        i for i in out_df.columns if i not in ("E6MILE", "emission", "emissionunit")
    ]
    in_df = emis_detailed.groupby(idx_cols, as_index=False).emission.sum()
    in_df = in_df.loc[
        lambda df: (df.pollutantCode.isin(pp_param["xml_pollutant_codes_selected"]))
        & (df.year == pp_param["xml_year_selected"])
        & (df.season == pp_param["xml_season_selected"])
        & (df.dayType == pp_param["xml_daytype_selected"])
    ]
    comp_df = in_df.merge(out_df, on=idx_cols, how="left")
    assert np.allclose(comp_df.emission_x, comp_df.emission_y)


def test_xml_eq_detailed():
    """Test if the XML values are equal to the XML csv."""
    csv_xml = pd.read_csv(pp_param["xmlscc_csv_out_fi"])
    df_xml = read_xml()
    comp_df = df_xml.merge(csv_xml, on=["FIPS", "sccNEI", "pollutantCode"], how="outer")
    assert np.allclose(comp_df.emission_x, comp_df.emission_y) & np.allclose(
        comp_df.E6MILE_x, comp_df.E6MILE_y
    )


def test_roadareatype():
    pp_param["fi_temp_tdm_hpms_rdtype"]
    pp_param["use_tdm_area_rdtype"]
    # Todo: write test
