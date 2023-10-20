from lxml import etree


class XMLGenerator:
    def __init__(self, xml_data):
        self.xml_data = xml_data
        self.namespace = {
            "hdr": "http://www.exchangenetwork.net/schema/header/2",
            "cer": "http://www.exchangenetwork.net/schema/cer/1",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        }

    def create_element(self, parent, namespace, element_name, text=None):
        element = etree.Element(etree.QName(namespace, element_name))
        if text is not None:
            element.text = text
        if parent is not None:
            parent.append(element)
        return element

    def create_header_element(self):
        header = self.create_element(None, self.namespace["hdr"], "Header")

        header_elements = {
            "AuthorName": "AuthorName",
            "OrganizationName": "OrganizationName",
            "DocumentTitle": "DocumentTitle",
            "CreationDateTime": "CreationDateTime",
            "Comment": "Comment",
            "DataFlowName": "DataFlowName",
        }

        for key, element_name in header_elements.items():
            self.create_element(
                header,
                self.namespace["hdr"],
                element_name,
                self.xml_data["Header"][key],
            )

        for prop_name, prop_value in self.xml_data["Header"]["Properties"].items():
            property_elem = self.create_element(
                header, self.namespace["hdr"], "Property"
            )
            self.create_element(
                property_elem, self.namespace["hdr"], "PropertyName", prop_name
            )
            self.create_element(
                property_elem, self.namespace["hdr"], "PropertyValue", prop_value
            )

        return header

    def create_payload_element(self):
        payload = self.create_element(None, self.namespace["hdr"], "Payload")
        cers = self.create_element(payload, self.namespace["cer"], "CERS")

        payload_elements = {
            "UserIdentifier": "UserIdentifier",
            "ProgramSystemCode": "ProgramSystemCode",
            "EmissionsYear": "EmissionsYear",
            "Model": "Model",
            "ModelVersion": "ModelVersion",
            "SubmittalComment": "SubmittalComment",
        }

        for key, element_name in payload_elements.items():
            self.create_element(
                cers, self.namespace["cer"], element_name, self.xml_data["Payload"][key]
            )

        grp = self.xml_data["Payload"]["Location"].groupby(["FIPS"])
        for FIPS, df_FIPS in grp:
            grp_sccNEI = df_FIPS.groupby(["sccNEI"])
            location = self.create_element(cers, self.namespace["cer"], "Location")
            self.create_element(
                location, self.namespace["cer"], "StateAndCountyFIPSCode", str(FIPS[0])
            )
            for SCC, df_sccNEI in grp_sccNEI:
                location_emissions_process = (
                    self.create_location_emissions_process_element(df_sccNEI, SCC)
                )
                location.append(location_emissions_process)

        return payload

    def create_location_emissions_process_element(self, data, SCC):
        location_emissions_process = self.create_element(
            None, self.namespace["cer"], "LocationEmissionsProcess"
        )

        self.create_element(
            location_emissions_process,
            self.namespace["cer"],
            "SourceClassificationCode",
            str(SCC[0]),
        )
        reporting_period = self.create_element(
            location_emissions_process, self.namespace["cer"], "ReportingPeriod"
        )
        self.create_element(
            reporting_period,
            self.namespace["cer"],
            "ReportingPeriodTypeCode",
            self.xml_data["Payload"]["ReportingPeriod"],
        )
        self.create_element(
            reporting_period,
            self.namespace["cer"],
            "CalculationParameterTypeCode",
            self.xml_data["Payload"]["CalculationParameterTypeCode"],
        )
        self.create_element(
            reporting_period,
            self.namespace["cer"],
            "CalculationParameterValue",
            str(data.iloc[0]["E6MILE"]),
        )
        self.create_element(
            reporting_period,
            self.namespace["cer"],
            "CalculationParameterUnitofMeasure",
            "E6MILE",
        )
        calculation_material_code = self.create_element(
            reporting_period, self.namespace["cer"], "CalculationMaterialCode"
        )
        if str(SCC[0])[3] == "1":
            calculation_material_code.text = "127"
        elif str(SCC[0])[3] == "2":
            calculation_material_code.text = "44"
        else:
            raise ValueError("SCC[3] can only be 1 (gas) or 2 (diesel) for MOVES3.")

        for idx, row in data.iterrows():
            reporting_period_emissions = self.create_element(
                reporting_period, self.namespace["cer"], "ReportingPeriodEmissions"
            )
            self.create_element(
                reporting_period_emissions,
                self.namespace["cer"],
                "PollutantCode",
                row["pollutantCode"],
            )
            self.create_element(
                reporting_period_emissions,
                self.namespace["cer"],
                "TotalEmissions",
                str(row["emission"]),
            )
            self.create_element(
                reporting_period_emissions,
                self.namespace["cer"],
                "EmissionsUnitofMeasureCode",
                row["emissionunits"],
            )

        return location_emissions_process

    def generate_xml(self):
        root = etree.Element(
            etree.QName(self.namespace["hdr"], "Document"), nsmap=self.namespace
        )
        root.set(
            etree.QName(self.namespace["xsi"], "schemaLocation"),
            "http://www.exchangenetwork.net/schema/cer/1/index.xsd",
        )
        root.set("id", self.xml_data["Header"]["id"])
        stylesheet_pi = etree.ProcessingInstruction(
            "xml-stylesheet", 'type="text/xsl" href="CER_StyleSheet.xslt"'
        )
        root.addprevious(stylesheet_pi)
        header_element = self.create_header_element()
        payload_element = self.create_payload_element()
        root.append(header_element)
        root.append(payload_element)
        tree = etree.ElementTree(root)
        xml_string = etree.tostring(
            root, pretty_print=True, encoding="utf-8", xml_declaration=True
        ).decode()
        return tree


if __name__ == "__main__":
    xml_data = {
        "Header": {
            # Define your header data here
        },
        "Payload": {
            # Define your payload data here
        },
    }

    xml_generator = XMLGenerator(xml_data)
    generated_xml = xml_generator.generate_xml()
    print(generated_xml)
