import sys
import yaml

import pkg_resources
import yaml

# Specify the name of your package (replace 'your_package_name' with the actual
# package name)
package_name = "ttionroadei"

try:
    # Get the path to the settings.yaml file within the package
    settings_yaml_path = pkg_resources.resource_filename(package_name, "settings.yaml")

    # Read and parse the YAML file
    with open(settings_yaml_path, "r") as yaml_file:
        data = yaml.safe_load(yaml_file)

    # Access and use the data from settings.yaml as needed
    print(data)

except Exception as e:
    print(f"Error: {e}")


# Access specific values from the parsed YAML data
log_console = data.get("log_console")
log_file = data.get("log_file")
log_filename = data.get("log_filename")
log_name = data.get("log_name")
INV_TYPE = data.get("INV_TYPE")
base_units = data.get("base_units")
MOVES4_Default_DB = data.get("MOVES4_Default_DB")
MOVES4_Default_TAB = data.get("MOVES4_Default_TAB")
sccNei = data.get("sccNei")
csvxml_act = data.get("csvxml_act")
csvxml_ei = data.get("csvxml_ei")
csvxml_comb = data.get("csvxml_comb")
aggRows = data.get("aggRows")
csvxml_aggpiv_opts = data.get("csvxml_aggpiv_opts")

# You can now work with these variables in your Python code
# For example, you can print the values to see their content
print("log_console:", log_console)
print("log_file:", log_file)
print("log_filename:", log_filename)
print("log_level:", log_level)
print("log_name:", log_name)
print("INV_TYPE:", INV_TYPE)
print("base_units:", base_units)
print("MOVES4_Default_DB:", MOVES4_Default_DB)
# ... and so on for other variables
