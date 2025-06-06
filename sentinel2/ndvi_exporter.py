#!/usr/bin/env python3
"""
NDVI Exporter Module: exports daily-averaged, AOI-clipped Sentinel-2 NDVI GeoTIFFs
for an AOI and date range, filtering on max cloud cover.
"""

import os
import io
import re
import urllib.request
import zipfile

import ee
import geopandas as gpd
from shapely.geometry import Polygon


import logging

# Logger for this module
logger = logging.getLogger(__name__)


def load_aoi_geom(path, layer=None):
    """Load AOI polygon geometry from vector file (GeoPackage, GeoJSON, Shapefile)."""
    gdf = gpd.read_file(path, layer=layer) if layer else gpd.read_file(path)
    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])]
    if gdf.empty:
        raise ValueError(f"No valid Polygon/MultiPolygon in {path}")

    geom = gdf.geometry.iloc[0]
    poly = geom if isinstance(geom, Polygon) else list(geom.geoms)[0]
    raw_coords = list(poly.exterior.coords)
    ring = [[float(coord[0]), float(coord[1])] for coord in raw_coords]

    if ring[0] != ring[-1]:
        ring.append(ring[0])

    return ee.Geometry.Polygon([ring])


def calculate_ndvi(img):
    """Compute and attach an 'ndvi' band to a Sentinel-2 image."""
    ndvi = img.normalizedDifference(["B8", "B4"]).rename("ndvi")
    return img.addBands(ndvi)


def attach_cloud_metadata(pair, geom, thresh):
    """Attach cloud percentage metadata within AOI to Sentinel-2 image."""
    s2 = ee.Image(pair.get("primary"))
    cp = ee.Image(pair.get("secondary")).select("probability")
    cloud_mask = cp.gt(thresh)
    frac = cloud_mask.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=geom, scale=10, maxPixels=1e9
    ).get("probability")
    return s2.set(
        {
            "cloud_pct_roi": ee.Number(frac).multiply(100),
            "system:time_start": s2.get("system:time_start"),
        }
    )


def export_ndvi(
    aoi_path,
    start_date,
    end_date,
    output_dir,
    cloud_threshold,
    s2_sr_collection,
    s2_cp_collection,
    layer=None,
):
    """Export daily-averaged Sentinel-2 NDVI GeoTIFFs clipped to AOI."""

    try:
        ee.Initialize()

        geom = load_aoi_geom(aoi_path, layer)
        region_geojson = geom.getInfo()

        raw_name = layer or os.path.splitext(os.path.basename(aoi_path))[0]
        aoi_name = re.sub(r"[^A-Za-z0-9_-]", "_", raw_name)

        s2_sr = (
            ee.ImageCollection(s2_sr_collection)
            .filterDate(start_date, end_date)
            .filterBounds(geom)
            .map(calculate_ndvi)
            .map(lambda img: img.clip(geom))
        )

        s2_cp = (
            ee.ImageCollection(s2_cp_collection)
            .filterDate(start_date, end_date)
            .filterBounds(geom)
        )

        index_filter = ee.Filter.equals(
            leftField="system:index", rightField="system:index"
        )
        joined = ee.Join.inner().apply(
            primary=s2_sr, secondary=s2_cp, condition=index_filter
        )

        paired = ee.ImageCollection(
            joined.map(lambda f: attach_cloud_metadata(f, geom, cloud_threshold))
        )

        good = paired.filter(ee.Filter.lte("cloud_pct_roi", cloud_threshold))

        with_date = good.map(
            lambda img: img.set(
                "date_str", ee.Date(img.get("system:time_start")).format("YYYY-MM-dd")
            )
        )
        distinct_dates = with_date.aggregate_array("date_str").distinct()

        def make_daily(d):
            d = ee.String(d)
            imgs = with_date.filter(ee.Filter.eq("date_str", d)).select("ndvi")
            daily_mean = imgs.mean().rename("ndvi").clip(geom)
            daily_mean = daily_mean.updateMask(daily_mean.neq(0))
            return daily_mean.set(
                {"date_str": d, "system:time_start": ee.Date(d).millis()}
            )

        daily_imgs = ee.ImageCollection(distinct_dates.map(make_daily))

        os.makedirs(output_dir, exist_ok=True)
        count = daily_imgs.size().getInfo()
        print(f"Exporting {count} daily‐averaged NDVI image(s)…")

        def export_one(img):
            date = ee.Date(img.get("system:time_start")).format("YYYY-MM-dd").getInfo()
            name = f"NDVI_{date}_{aoi_name}"
            out_tif = os.path.join(output_dir, f"{name}.tif")

            ndvi_masked = img.select("ndvi")
            url = ndvi_masked.getDownloadURL(
                {"scale": 10, "region": region_geojson, "fileFormat": "GEO_TIFF"}
            )

            print(f"  → downloading {name}.tif …", end="", flush=True)
            resp = urllib.request.urlopen(url)
            data = resp.read()

            if data[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(data)) as z:
                    member = z.namelist()[0]
                    with open(out_tif, "wb") as f:
                        f.write(z.read(member))
            else:
                with open(out_tif, "wb") as f:
                    f.write(data)

            print(" done")

        imgs = daily_imgs.toList(count)
        for i in range(count):
            export_one(ee.Image(imgs.get(i)))
    except Exception as err:
        logger.error(f"Error during NDVI export: {err}", exc_info=True)
        raise
