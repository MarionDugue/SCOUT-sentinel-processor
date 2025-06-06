# sentinel2/utils.py

def compute_ndvi(image):
    ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI")
    return image.addBands(ndvi).select("NDVI")
