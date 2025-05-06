This code completes an analysis within ArcGIS Pro of areas that should be sprayed for mosquitos near Boulder, Colorado.
This process combines wetlands locations, mosquito larva sites, and lakes and reservoirs to identify likely areas of high mosquito populations.
Then, the code uses the extract, transform, and load method to take addresses from a google survey and geocode them. These addresses represent individuals sensitive to mosquito spray.
A buffer is created around these addresses and these areas are then removed from the spray area.
Addresses within the spray area are then identified using a spatial join as addresses that should be notified of spraying activity.

To run this code, adjust file paths within the yaml file for your unique file paths.
