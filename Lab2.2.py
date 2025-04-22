
import yaml
import arcpy
import logging
import os
from etl.GSheetsEtl import GSheetsEtl

def setup():
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
        logging.basicConfig(filename=f"{config_dict.get('workspace_dir')}wnv.log",
                            filemode="w", level=logging.DEBUG)
    return config_dict

def bufferFunct (shapefile, bufferDistance):
    outputName = f"{shapefile}_buffer"

    if arcpy.Exists(outputName):
        print(f"Output {outputName} already exists. Deleting it...")
        arcpy.Delete_management(outputName)

    arcpy.analysis.Buffer(shapefile, outputName, bufferDistance)
    print(f"buffer executed for {shapefile}")
    print(f"buffered shapefile: {outputName}")
    return

def intersectFunct (infeatures):
    layerName = ("intersect")
        #input(f"Name the intersect layer: "))
    arcpy.analysis.Intersect(infeatures, layerName, "ONLY_FID")
    print(f"{layerName} created")
    return layerName

# ETL Function
def etl():
    print("Starting ETL Process...")
    etl_instance = GSheetsEtl(config_dict)
    etl_instance.process()

def exportMap ():
    aprx = arcpy.mp.ArcGISProject(f"{config_dict.get('workspace_dir')}WestNileOutbreak.aprx")
    lyt = aprx.listLayouts()[0]
    sub_title = input("Enter a subtitle for the map layout: ")
    for el in lyt.listElements():
        print(el.name)
        if "Title" in el.name:
            el.text = el.text + sub_title
    lyt.exportToPDF(f"{config_dict.get('workspace_dir')}Layouts\WestNileOutbreakLayout.pdf")

if __name__ == "__main__":

    global config_dict
    config_dict = setup()
    print(config_dict)
    logging.info("Starting West Nile Virus Simulation")

    # create folder for layouts
    layout_folder = rf"{config_dict.get('workspace_dir')}Layouts"
    os.makedirs(layout_folder, exist_ok=True)

    logging.debug("Entering ETL Method")
    #etl()
    logging.debug("Exiting ETL Method")

    workspace = f"{config_dict.get('workspace_dir')}WestNileOutbreak.gdb" #updated to yaml
    arcpy.env.workspace = workspace
    arcpy.env.overwriteOutput = True

    fcs = arcpy.ListFeatureClasses()
    logging.info(fcs)
    #print(fcs)

    logging.debug("Entering Buffer Method")
    bufferList = ["Wetlands", "OSMP_Properties", "Mosquito_Larval_Sites", "Lakes_and_Reservoirs", "avoid_points"]


    for i in bufferList:
        logging.info(f"buffer:{i}")
        #print(f"buffer:{i}")
        #bufferDistance = input("Enter the buffer distance:") #set manually
        bufferFunct(i, "1500 feet")
    logging.debug("Exiting Buffer Method")

    logging.debug("Entering Intersect Method")
    intersectLyr = intersectFunct(["Wetlands_buffer", "OSMP_Properties_buffer", "Mosquito_Larval_Sites_buffer", "Lakes_and_Reservoirs_buffer"])
    logging.debug("Exiting Intersect Method")

    #Too many features. Must Dissolve
    logging.info("Running Dissolve")
    logging.debug("Entering Dissolve Method")
    #print("Running Dissolve")
    arcpy.management.Dissolve(
        in_features="intersect",
        out_feature_class="Intersect_Dissolve",
        dissolve_field="FID_Wetlands_buffer",
        statistics_fields=None,
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES",
        concatenation_separator=""
    )
    logging.debug("Exiting Dissolve Method")

    #erase
    logging.debug("Entering Erase Method")
    #print("Running Erase")
    arcpy.analysis.Erase("Intersect_Dissolve", "avoid_points_buffer", "spray_area")
    logging.debug("Exiting Erase Method")
    #print("Erase Executed")

    logging.debug("Entering Spatial Join Method")
    arcpy.analysis.SpatialJoin("Addresses", "spray_area", "addresses_intersect_join")
    join_lyr = "addresses_intersect_join"
    logging.info(f"{join_lyr} created")
    #print(f"{join_lyr} created")
    logging.debug("Exiting Spatial Join Method")

    proj_path = config_dict.get('workspace_dir')
    aprx = arcpy.mp.ArcGISProject(rf"{proj_path}\\WestNileOutbreak.aprx")

    logging.debug("Entering Add Data Method")
    map_doc = aprx.listMaps()[0]
    #failed to add data. Possible credentials issue.
    map_doc.addDataFromPath(rf"{config_dict.get('workspace_dir')}WestNileOutbreak.gdb\\{join_lyr}")
    logging.info(f"{join_lyr} added to aprx")
    #print(f"{join_lyr} added to aprx")
    aprx.save()
    logging.debug("Exiting Add Data Method")

    logging.debug("Entering Count Records Method")
    column_name = "OID_1"
    count_not_null = 0

    with arcpy.da.SearchCursor(join_lyr, [column_name]) as cursor:
        for row in cursor:
            if row[0] is not None:
                count_not_null += 1

    logging.info(f"Number of records where {column_name} is not null: {count_not_null}")
    #print(f"Number of records where {column_name} is not null: {count_not_null}")
    logging.debug("Exiting Count Records Method")

    #exportMap
    exportMap()