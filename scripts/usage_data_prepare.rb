#!/usr/bin/ruby
require 'json'
require 'time'

#
# Transforms usage JSON file's jobs element in to big query friendly records
#
def transform
  dest = File.open(ARGV[1], 'w', :external_encoding=>'utf-8'))
  File.open(ARGV[0]).each do|line|
    if block_given?
      line = yield line
    end
    dest.puts line
  end
  dest.close
end

#
# Transforms each json record's jobs element in to bigquery friendly json structure
#
# {'jobs': {'hudson-matrix-MatrixProject':2}} is converted to
#
# {'jobs': [{'type':'hudson_matrix_MatrixProject', 'count':2}]}
#
filter = lambda do |line|
  usage = JSON.parse(line)
  jobs = []
  if usage['jobs']
    usage['jobs'].each do |k,v|
      jobs << {'type'=>k.gsub('-','_'), 'count'=>v}
    end
  end

  # convert to UTC timestamp that bigquery understands
  if usage['timestamp']
    usage['timestamp'] = Time.strptime(usage['timestamp'], "%d/%b/%Y:%T %Z").utc
  end
  usage['jobs'] = jobs
  usage.to_json
end

def usage
    puts "Usage: SOURCE_FILE DESTINATION_FILE"
end

if ARGV.size < 2
    usage
    exit 1
end

transform &filter