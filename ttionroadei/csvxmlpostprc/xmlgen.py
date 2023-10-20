"""
XML generation code.
"""
from lxml import etree


class XMLGenerator:
    """
    XMLGenerator class for generating XML documents using the lxml library.

    Parameters
    ----------
    xml_data : dict
        A dictionary containing the data for generating the XML document.

    Attributes
    ----------
    xml_data : dict
        The input data for creating the XML document.
    namespace : dict
        A dictionary that defines XML namespaces used in the document.

    Methods
    -------
    create_element(parent, namespace, element_name, text=None)
        Create an XML element with an optional text value and append it to the parent element.

    create_header_element()
        Create the XML header element based on the provided data.

    create_payload_element()
        Create the XML payload element based on the provided data.

    create_location_emissions_process_element(data, SCC)
        Create an XML element for a location emissions process based on the provided data.

    generate_xml()
        Generate the complete XML document based on the input data and return it as an ElementTree object.

    Example Usage
    -------------
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
    """

    def __init__(self, xml_data):
        """
        Initialize an XMLGenerator instance.

        Parameters
        ----------
        xml_data : dict
            A dictionary containing the data for generating the XML document.
        """
        self.xml_data = xml_data
        self.namespace = {
            "hdr": "http://www.exchangenetwork.net/schema/header/2",
            "cer": "http://www.exchangenetwork.net/schema/cer/1",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        }

    def create_element(self, parent, namespace, element_name, text=None):
        """
        Create an XML element and append it to the parent element if provided.

        Parameters
        ----------
        parent : Element or None
            The parent element to which the new element will be appended. If None, the element is not appended to any parent.
        namespace : str
            The XML namespace for the element.
        element_name : str
            The name of the element to be created.
        text : str or None, optional
            The text value to be set for the element. Default is None.

        Returns
        -------
        Element
            The created XML element.
        """
        element = etree.Element(etree.QName(namespace, element_name))
        if text is not None:
            element.text = text
        if parent is not None:
            parent.append(element)
        return element

    def create_header_element(self):
        """
        Create the XML header element based on the provided data.

        Returns
        -------
        Element
            The XML header element.
        """
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
        """
        Create the XML payload element based on the provided data.

        Returns
        -------
        Element
            The XML payload element.
        """
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
        """
        Create an XML element for a location emissions process based on the provided data.

        Parameters
        ----------
        data : DataFrame
            The data containing emissions process information.
        SCC : str
            The Source Classification Code.

        Returns
        -------
        Element
            The XML element for the location emissions process.
        """
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
        """
        Generate the complete XML document based on the input data.

        Returns
        -------
        ElementTree
            The complete XML document as an ElementTree object.
        """
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
