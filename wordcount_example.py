"""Minimal wordcount example using apache beam.

https://beam.apache.org/get-started/wordcount-example/
"""
import re

from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam



output_dir = 'data'
# TODO: Will Beam accept pathlib.Paths?
input_file = f'{output_dir}/words.txt'
output_path = f'{output_dir}/wordcounts.txt'


class WordCountOptions(PipelineOptions):
    """Accept runtime Pipeline options."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', dest='input', help='File to read in')
        parser.add_argument('--output', dest='output', help='File to write out')


# This object lets us set various options for our pipeline, such as the pipeline
# runner that will execute our pipeline and any runner-specific configuration
# required by the chosen runner.
pipeline_options = WordCountOptions(
    runner='DirectRunner',
    project='wordcount',
    job_name='unique_wordcount_job_name',
    # This doesn't appear to be created. Maybe not used by any of our PTransform
    # steps.
    temp_location='./words-tmp',
)

# The Pipeline object builds up the graph of transformations to be executed,
# associated with that particular pipeline.
with beam.Pipeline(options=pipeline_options) as p:
    (p
    # A text file Read transform is applied to the Pipeline object itself, and
    # produces a PCollection as output. Each element in the output PCollection
    # represents one line of text from the input file.
    | beam.io.ReadFromText(input_file)
    # This transform splits the lines in PCollection<String>, where each element is
    # an individual word. The Flatmap transform is a simplified version of ParDo.
    | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
    # The SDK-provided Count transform is a generic transform that takes a
    # PCollection of any type, and returns a PCollection of key/value pairs. Each
    # key represents a unique element from the input collection, and each value
    # represents the number of times that key appeared in the input collection.
    | beam.combiners.Count.PerElement()
    # The next transform formats each of the key/value pairs of unique words and
    # occurrence counts into a printable string suitable for writing to an output
    # file.
    | beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
    # A text file write transform. This transform takes the final PCollection of
    # formatted Strings as input and writes each element to an output text
    # file. Each element in the input PCollection represents one line of text in the
    # resulting output file.
    | beam.io.WriteToText(output_path))
