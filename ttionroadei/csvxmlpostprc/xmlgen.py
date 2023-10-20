"""
XML generation code
"""
from lxml import etree
import pandas as pd


def create_element(parent, namespace, element_name, text=None):
    element = etree.Element(etree.QName(namespace, element_name))
    if text is not None:
        element.text = text
    if parent is not None:
        parent.append(element)
    return element


def create_header_element(xml_data, namespace):
    header = create_element(None, namespace["hdr"], "Header")

    header_elements = {
        "AuthorName": "AuthorName",
        "OrganizationName": "OrganizationName",
        "DocumentTitle": "DocumentTitle",
        "CreationDateTime": "CreationDateTime",
        "Comment": "Comment",
        "DataFlowName": "DataFlowName",
    }

    for key, element_name in header_elements.items():
        create_element(header, namespace["hdr"], element_name, xml_data["Header"][key])

    for prop_name, prop_value in xml_data["Header"]["Properties"].items():
        property_elem = create_element(header, namespace["hdr"], "Property")
        create_element(property_elem, namespace["hdr"], "PropertyName", prop_name)
        create_element(property_elem, namespace["hdr"], "PropertyValue", prop_value)

    return header


def create_payload_element(xml_data, namespace):
    payload = create_element(None, namespace["hdr"], "Payload")
    cers = create_element(payload, namespace["cer"], "CERS")

    payload_elements = {
        "UserIdentifier": "UserIdentifier",
        "ProgramSystemCode": "ProgramSystemCode",
        "EmissionsYear": "EmissionsYear",
        "Model": "Model",
        "ModelVersion": "ModelVersion",
        "SubmittalComment": "SubmittalComment",
    }

    for key, element_name in payload_elements.items():
        create_element(cers, namespace["cer"], element_name, xml_data["Payload"][key])

    grp = xml_data["Payload"]["Location"].groupby(["FIPS"])
    for FIPS, df_FIPS in grp:
        grp_sccNEI = df_FIPS.groupby(["sccNEI"])
        location = create_element(cers, namespace["cer"], "Location")
        create_element(
            location, namespace["cer"], "StateAndCountyFIPSCode", str(FIPS[0])
        )
        for SCC, df_sccNEI in grp_sccNEI:
            location_emissions_process = create_location_emissions_process_element(
                df_sccNEI, SCC, namespace
            )
            location.append(location_emissions_process)

    return payload


def create_location_emissions_process_element(data, SCC, namespace):
    location_emissions_process = create_element(
        None, namespace["cer"], "LocationEmissionsProcess"
    )

    create_element(
        location_emissions_process,
        namespace["cer"],
        "SourceClassificationCode",
        str(SCC[0]),
    )
    reporting_period = create_element(
        location_emissions_process, namespace["cer"], "ReportingPeriod"
    )
    create_element(reporting_period, namespace["cer"], "ReportingPeriodTypeCode", "O3D")
    create_element(
        reporting_period, namespace["cer"], "CalculationParameterTypeCode", "I"
    )
    create_element(
        reporting_period,
        namespace["cer"],
        "CalculationParameterValue",
        str(data.iloc[0]["E6MILE"]),
    )
    create_element(
        reporting_period,
        namespace["cer"],
        "CalculationParameterUnitofMeasure",
        "E6MILE",
    )
    calculation_material_code = create_element(
        reporting_period, namespace["cer"], "CalculationMaterialCode"
    )
    if str(SCC[0])[3] == "1":
        calculation_material_code.text = "127"
    elif str(SCC[0])[3] == "2":
        calculation_material_code.text = "44"
    else:
        raise ValueError("SCC[3] can only be 1 (gas) or 2 (diesel) for MOVES3.")

    for idx, row in data.iterrows():
        reporting_period_emissions = create_element(
            reporting_period, namespace["cer"], "ReportingPeriodEmissions"
        )
        create_element(
            reporting_period_emissions,
            namespace["cer"],
            "PollutantCode",
            row["pollutantCode"],
        )
        create_element(
            reporting_period_emissions,
            namespace["cer"],
            "TotalEmissions",
            str(row["emission"]),
        )
        create_element(
            reporting_period_emissions,
            namespace["cer"],
            "EmissionsUnitofMeasureCode",
            row["emissionunits"],
        )

    return location_emissions_process


def xmlgen(xml_data):
    namespace = {
        "hdr": "http://www.exchangenetwork.net/schema/header/2",
        "cer": "http://www.exchangenetwork.net/schema/cer/1",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    }

    root = etree.Element(etree.QName(namespace["hdr"], "Document"), nsmap=namespace)
    root.set(
        etree.QName(namespace["xsi"], "schemaLocation"),
        "http://www.exchangenetwork.net/schema/cer/1/index.xsd",
    )
    root.set("id", xml_data["Header"]["id"])

    header_element = create_header_element(xml_data, namespace)
    payload_element = create_payload_element(xml_data, namespace)
    root.append(header_element)
    root.append(payload_element)
    xml_string = etree.tostring(
        root, pretty_print=True, encoding="utf-8", xml_declaration=True
    ).decode()

    print(xml_string)
    output_file = "output.xml"
    with open(output_file, "w", encoding="utf-8") as file:
        file.write(xml_string)


if __name__ == "__main__":
    ...
