from lxml import etree
import pandas as pd


def create_header_element(xml_data, namespace):
    header = etree.Element(etree.QName(namespace["hdr"], "Header"))

    elements = {
        "AuthorName": "AuthorName",
        "OrganizationName": "OrganizationName",
        "DocumentTitle": "DocumentTitle",
        "CreationDateTime": "CreationDateTime",
        "Comment": "Comment",
        "DataFlowName": "DataFlowName",
    }

    for key, element_name in elements.items():
        element = etree.SubElement(header, etree.QName(namespace["hdr"], element_name))
        element.text = xml_data["Header"][key]

    for prop_name, prop_value in xml_data["Header"]["Properties"].items():
        property_elem = etree.SubElement(
            header, etree.QName(namespace["hdr"], "Property")
        )
        property_name = etree.SubElement(
            property_elem, etree.QName(namespace["hdr"], "PropertyName")
        )
        property_name.text = prop_name
        property_value = etree.SubElement(
            property_elem, etree.QName(namespace["hdr"], "PropertyValue")
        )
        property_value.text = prop_value

    return header


def create_payload_element(xml_data, namespace):
    payload = etree.Element(
        etree.QName(namespace["hdr"], "Payload"), operation="refresh"
    )
    cers = etree.SubElement(payload, etree.QName(namespace["cer"], "CERS"))

    elements = {
        "UserIdentifier": "UserIdentifier",
        "ProgramSystemCode": "ProgramSystemCode",
        "EmissionsYear": "EmissionsYear",
        "Model": "Model",
        "ModelVersion": "ModelVersion",
        "SubmittalComment": "SubmittalComment",
    }

    for key, element_name in elements.items():
        element = etree.SubElement(cers, etree.QName(namespace["cer"], element_name))
        element.text = xml_data["Payload"][key]
    grp = xml_data["Payload"]["Location"].groupby(["FIPS"])
    for FIPS, df_FIPS in grp:
        grp_sccNEI = df_FIPS.groupby(["sccNEI"])
        location = etree.SubElement(cers, etree.QName(namespace["cer"], "Location"))
        state_and_county_fips_code = etree.SubElement(
            location, etree.QName(namespace["cer"], "StateAndCountyFIPSCode")
        )
        state_and_county_fips_code.text = f"{FIPS[0]}"
        for SCC, df_sccNEI in grp_sccNEI:
            location_emissions_process = create_location_emissions_process_element(
                df_sccNEI, SCC, namespace
            )
            location.append(location_emissions_process)
    return payload


def create_location_emissions_process_element(data, SCC, namespace):
    location_emissions_process = etree.Element(
        etree.QName(namespace["cer"], "LocationEmissionsProcess")
    )

    source_classification_code = etree.SubElement(
        location_emissions_process,
        etree.QName(namespace["cer"], "SourceClassificationCode"),
    )
    source_classification_code.text = f"{SCC[0]}"

    reporting_period = etree.SubElement(
        location_emissions_process, etree.QName(namespace["cer"], "ReportingPeriod")
    )
    reporting_period_type_code = etree.SubElement(
        reporting_period, etree.QName(namespace["cer"], "ReportingPeriodTypeCode")
    )
    reporting_period_type_code.text = "O3D"
    calculation_parameter_type_code = etree.SubElement(
        reporting_period, etree.QName(namespace["cer"], "CalculationParameterTypeCode")
    )
    calculation_parameter_type_code.text = "I"
    calculation_parameter_value = etree.SubElement(
        reporting_period, etree.QName(namespace["cer"], "CalculationParameterValue")
    )
    calculation_parameter_value.text = str(data.iloc[0]["E6MILE"])
    calculation_parameter_unit_of_measure = etree.SubElement(
        reporting_period,
        etree.QName(namespace["cer"], "CalculationParameterUnitofMeasure"),
    )
    calculation_parameter_unit_of_measure.text = "E6MILE"

    calculation_material_code = etree.SubElement(
        reporting_period, etree.QName(namespace["cer"], "CalculationMaterialCode")
    )
    if str(SCC[0])[3] == "1":
        calculationmaterialcode = "127"
    elif str(SCC[0])[3] == "2":
        calculationmaterialcode = "44"
    else:
        raise ValueError("SCC[3] can only be 1 (gas) or 2 (diesel) for MOVES3.")
    calculation_material_code.text = calculationmaterialcode

    for idx, row in data.iterrows():
        reporting_period_emissions = etree.SubElement(
            reporting_period, etree.QName(namespace["cer"], "ReportingPeriodEmissions")
        )

        pollutant_code = etree.SubElement(
            reporting_period_emissions, etree.QName(namespace["cer"], "PollutantCode")
        )
        pollutant_code.text = row["pollutantCode"]

        total_emissions = etree.SubElement(
            reporting_period_emissions, etree.QName(namespace["cer"], "TotalEmissions")
        )
        total_emissions.text = str(row["emission"])

        emissions_unit_of_measure_code = etree.SubElement(
            reporting_period_emissions,
            etree.QName(namespace["cer"], "EmissionsUnitofMeasureCode"),
        )
        emissions_unit_of_measure_code.text = row["emissionunits"]

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
