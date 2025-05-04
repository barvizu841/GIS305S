import arcpy
import requests
import csv
from etl.SpatialEtl import SpatialEtl

class GSheetsEtl(SpatialEtl):
    '''
    GSheetsEtl performs an extract, transform, and load process using a URL to a google spreadsheet.
    The spreadsheet must contain an address and zipcode column.

    Parameters:
        config_dict (dictionary): A dictionary containging a remote_url key to the google spreadsheet
        and a web geocoding service.

    '''
    config_dict = None

    def __init__(self, config_dict):
        super().__init__(config_dict)

    def extract(self):
        '''
        Extracting data from a google spreadsheet and save it as a local file
        '''

        print("Extracting addresses from google form spreadsheet")

        r = requests.get(self.config_dict.get('remote_url')) #updated to yaml
        r.encoding = "utf-8"
        data = r.text
        with open(f"{self.config_dict.get('proj_dir')}addresses.csv", "w") as output_file:
            output_file.write(data) #updated to yaml

    def transform(self):
        '''
        Transforming data by georeferencing the addresses
        '''
        print("Add City, State")

        transformed_file = open(f"{self.config_dict.get('proj_dir')}new_addresses.csv", "w") #updated to yaml
        transformed_file.write("X, Y, Type\n")
        with open(f"{self.config_dict.get('proj_dir')}addresses.csv", "r") as partial_file: #updated to yaml
            csv_dict = csv.DictReader(partial_file, delimiter=',')
            for row in csv_dict:
                address = row["Street Address"] + "Boulder CO"
                print(address)
                geocode_url = self.config_dict.get('geocoder_prefix_ref') + address + \
                              self.config_dict.get('geocoder_suffix_ref') #upadted to yaml
                r = requests.get(geocode_url)

                resp_dict = r.json()
                x = resp_dict['result']['addressMatches'][0]['coordinates']['x']
                y = resp_dict['result']['addressMatches'][0]['coordinates']['y']
                transformed_file.write(f"{x}, {y},Residential\n")

        transformed_file.close()

    def load(self):
        '''
        Loading data into the ArcGIS Pro project
        '''
        workspace = f"{self.config_dict.get('workspace_dir')}WestNileOutbreak.gdb" #updated to yaml
        arcpy.env.workspace = workspace
        arcpy.env.overwriteOutput = True

        in_table = f"{self.config_dict.get('proj_dir')}new_addresses.csv" #updated to yaml
        out_feature_class = "avoid_points"
        x_coords = "X"
        y_coords = "Y"

        arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)

        print(arcpy.GetCount_management(out_feature_class))

    def process(self):
        self.extract()
        self.transform()
        self.load()
        
