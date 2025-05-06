class SpatialEtl:

    def __init__(self, config_dict):
        self.config_dict = config_dict
    '''
    Extract data from google survey
    '''
    def extract(self):
        print(f"Extracting data from {self.config_dict.get('remote_url')} to {self.config_dict.get('project_dir')}")
    '''
    Transform data by geocoding addresses
    '''
    def transform(self):
        print(f"Transforming {self.config_dict.get('data_format')}")
    '''
    Load data into the ArcGIS Pro project
    '''
    def load(self):
        print(f"Loading data into {self.config_dict.get('workspace_dir')}WestNileOutbreak.gdb")

    '''
    def __init__(self, remote, local_dir, data_format, destination):
        self.remote = remote
        self.local_dir = local_dir
        self.data_format = data_format
        self.destination = destination

    def extract(self):
        print(f"Extracting data from {self.remote} to {self.local_dir}")

    def transform(self):
        print(f"Transforming {self.data_format}")

    def load(self):
        print(f"Loading data into {self.destination}")
    '''


