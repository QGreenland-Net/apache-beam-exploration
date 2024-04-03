from dataclasses import dataclass

import fsspec
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


INPUT_URL = "https://arcticdata.io/metacat/d1/mn/v2/object/urn%3Auuid%3A31162eb9-7e3b-4b88-948f-f4c99f13a83f"
OUTPUT_PATH = "data/foo.gpkg"

# This object lets us set various options for our pipeline, such as the pipeline
# runner that will execute our pipeline and any runner-specific configuration
# required by the chosen runner.
beam_options = PipelineOptions(
    runner='DirectRunner',
    project='seal_tags',
    # TODO: do we have to specify this? Could be generated automatically from
    # `project` + unique key?
    # job_name='seal_tags_job',
    temp_location='./seal_tags-tmp',
)

def write_csv(pcoll):
    import pandas as pd
    import geopandas

    local_fs = pcoll # <OpenFile 'https://arcticdata.io/metacat/d1/mn/v2/object/urn%3Auuid%3A31162eb9-7e3b-4b88-948f-f4c99f13a83f'>

    with local_fs as f:
        df = pd.read_csv(f)
        geom = geopandas.points_from_xy(df.Longitude, df.Latitude)
        geo_df = geopandas.GeoDataFrame(df, geometry=geom)
        geo_df.to_file(OUTPUT_PATH, crs="EPSG:4326")

    return OUTPUT_PATH

@dataclass
class WriteCSV(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(write_csv)


def open_with_fsspec(fname):
    return fsspec.open(fname, mode="rb")

@dataclass
class OpenURLWithFSSpec(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(open_with_fsspec)

recipe = (
    beam.Create([INPUT_URL])
    | OpenURLWithFSSpec()
    | WriteCSV()
)

with beam.Pipeline(options=beam_options) as p:
    (p | recipe)
