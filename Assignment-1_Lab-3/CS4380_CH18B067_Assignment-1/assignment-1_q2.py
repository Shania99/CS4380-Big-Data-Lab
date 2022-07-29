import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'graphite-byte-260703'
google_cloud_options.job_name = 'lab3q2'
google_cloud_options.region = "us-central1"
google_cloud_options.temp_location="gs://bdl22_ch18b067/temp/"
options.view_as(StandardOptions).runner = 'DataflowRunner'
p= beam.Pipeline(options=options)
lines = p | 'Read' >> beam.io.ReadFromText( 'gs://bdl2022/lines_big.txt' ) |'Find Words' >> beam.Map(lambda line: len(line.split()))| 'Convert to List' >> beam.combiners.ToList() |'Find Average'>> beam.Map(lambda x : sum(x) /len(x) ) | 'Write' >> beam.io.WriteToText( 'gs://bdl22_ch18b067/outputs/assignment-1_q2_outputs.txt')
result = p.run()

