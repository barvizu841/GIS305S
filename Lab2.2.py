
import yaml
import arcpy
from etl.GSheetsEtl import GSheetsEtl

def setup():
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
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

if __name__ == "__main__":
    global config_dict
    config_dict = setup()
    print(config_dict)

    #etl()

    workspace = f"{config_dict.get('workspace_dir')}WestNileOutbreak.gdb" #updated to yaml
    arcpy.env.workspace = workspace
    arcpy.env.overwriteOutput = True

    fcs = arcpy.ListFeatureClasses()
    print(fcs)

    bufferList = ["Wetlands", "OSMP_Properties", "Mosquito_Larval_Sites", "Lakes_and_Reservoirs", "avoid_points"]

    for i in bufferList:
        print(f"buffer:{i}")
        #bufferDistance = input("Enter the buffer distance:") #set manually
        bufferFunct(i, "1500 feet")

    intersectLyr = intersectFunct(["Wetlands_buffer", "OSMP_Properties_buffer", "Mosquito_Larval_Sites_buffer", "Lakes_and_Reservoirs_buffer"])

    #Too many features. Must Dissolve
    print("Running Dissolve")
    arcpy.management.Dissolve(
        in_features="intersect",
        out_feature_class="Intersect_Dissolve",
        dissolve_field="FID_Wetlands_buffer",
        statistics_fields=None,
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES",
        concatenation_separator=""
    )

    #erase
    print("Running Erase")
    arcpy.analysis.Erase("Intersect_Dissolve", "avoid_points_buffer", "spray_area")
    print("Erase Executed")

    arcpy.analysis.SpatialJoin("Addresses", "spray_area", "addresses_intersect_join")
    join_lyr = "addresses_intersect_join"
    print(f"{join_lyr} created")

    proj_path = config_dict.get('workspace_dir')
    aprx = arcpy.mp.ArcGISProject(rf"{proj_path}\\WestNileOutbreak.aprx")

    map_doc = aprx.listMaps()[0]
    #failed to add data. Possible credentials issue.
    map_doc.addDataFromPath(rf"{config_dict.get('workspace_dir')}WestNileOutbreak.gdb\\{join_lyr}")
    print(f"{join_lyr} added to aprx")
    aprx.save()

    column_name = "OID_1"
    count_not_null = 0

    with arcpy.da.SearchCursor(join_lyr, [column_name]) as cursor:
        for row in cursor:
            if row[0] is not None:
                count_not_null += 1


    print(f"Number of records where {column_name} is not null: {count_not_null}")
