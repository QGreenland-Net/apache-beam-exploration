from dataclasses import dataclass
import subprocess
from pathlib import Path

import fsspec
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


INPUT_URL = "https://arcticdata.io/metacat/d1/mn/v2/object/urn%3Auuid%3A31162eb9-7e3b-4b88-948f-f4c99f13a83f"
OUTPUT_PATH = "data/foo.gpkg"
CACHE_DIR = Path("./cache/")

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

def csv_to_gpkg_with_ogr2ogr(pcoll):
    # The pcoll is a Path to a local file.
    input_file = pcoll

    result = subprocess.run(
        f"ogr2ogr -a_srs 'EPSG:4326' -oo X_POSSIBLE_NAMES=Longitude -oo Y_POSSIBLE_NAMES=Latitude {OUTPUT_PATH} {input_file}",
        shell=True,
        executable="/bin/bash",
        capture_output=True,
    )

    if result.returncode != 0:
        stdout = str(result.stdout, encoding="utf8")
        stderr = str(result.stderr, encoding="utf8")
        output = f"===STDOUT===\n{stdout}\n\n===STDERRR===\n{stderr}"
        raise RuntimeError(
            f"Subprocess failed with output:\n\n{output}",
        )

    return OUTPUT_PATH

@dataclass
class WriteGpkg(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(csv_to_gpkg_with_ogr2ogr)


def fetch_data(fname):
    cached_data_path = CACHE_DIR / "local.csv"
    # TODO: there must be an easier way to cache/fetch remote data w/ fsspec? I
    # tried using the `cache_storage` option but I'm not sure how to trigger the
    # write.  Also looked at this, but unclear how to use:
    # https://filesystem-spec.readthedocs.io/en/latest/api.html?highlight=get#fsspec.spec.AbstractFileSystem.get
    with fsspec.open(fname, mode="rb") as remote_file:
        data = remote_file.read()
        with open(cached_data_path, 'wb') as local_file:
            local_file.write(data)

    return cached_data_path

@dataclass
class FetchDataWithFSSpec(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(fetch_data)

recipe = (
    beam.Create([INPUT_URL])
    | FetchDataWithFSSpec()
    | WriteGpkg()
)

with beam.Pipeline(options=beam_options) as p:
    (p | recipe)
