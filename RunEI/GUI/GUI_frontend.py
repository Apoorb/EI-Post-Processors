"""


"""

# 0. Orient to the data directory
data_folder = ""

# 1. Based on the inventory type chosen for the GUI for the emission calc (previous module),
# give users dropdown.
# TODO: Based on the options chosen for earlier modules in the GUI. Give users a
#  dropdown based on it.
# TODO: Based on the inventory type chosen, get the detailed table on associated
#  processes from MOVES default database.  Have a default mapping, but give users to
#  define their own

# 2. Based on the analysis areas (assuming we can have multiple analysis areas) chosen
# for the GUI for the emission calc, give users a dropdown.
# TODO: Get the table on analysis areas from CG's DB, and the options chosen for
#  earlier modules in the GUI.
#  Have a custom option for user to define an areas based on 254 counties.
#  Give users a dropdown based on it.

# 3. Based on the analysis years (assuming we can have multiple analysis years) chosen
# for the GUI for the emission calc, give users a dropdown.
# TODO: Based on the options chosen for earlier modules in the GUI. Give users a
#  dropdown based on it.


# 4. Based on the pollutants included in the MRS, need to generate a file on
# pollutants before
# , give users a dropdown.
# TODO: Parse MRS or an intermediate file on pollutants from the previous module runs.
# TODO: Based on the pollutant chosen, get the detailed table on pollutants from MOVES
#  default database

# 5. Ask the user for any MOVES pollutants that are to be combined.
# TODO: Give a dropdown to select pollutants to be combined.
# TODO: Give user option to rename any MOVES pollutants that need to be renames for NEI?

# 6. Based on the area chosen, run code in the backend to get the road-type mapping.
# TODO: Ask the user the road type info needed (MOVES or TDM or HPMS?)
# TODO: Give option to provide custom mapping of road types.

# 7. Specific the options that need to be generated:
# a) Detailed CSV files
# b) Aggregated and Pivoted CSV files
# TODO: Ask the user for the type of aggregation and pivot.
# c) XML file
# TODO: Ask the user for XML header details.


if __name__ == "__main__":
    ...
