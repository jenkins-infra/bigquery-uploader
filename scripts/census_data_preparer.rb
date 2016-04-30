#!/usr/bin/ruby
require 'json'
require 'time'
require 'zlib'

class CensusFile
  attr_reader :timestamp
  attr_reader :full_name
  attr_reader :path
  attr_reader :name

  def initialize(path)
    @path = path
    @full_name = File.basename(path)
    x = @full_name.split('.')
    @name = x[x.size - 2]
    @timestamp = Time.strptime(@name, "%Y%m%d")
  end

  #
  # Transforms usage JSON file's jobs element in to big query friendly records
  #
  def transform
    dest = File.open(@name, 'w', :external_encoding=>'utf-8')
    uncompress_name = "#{@name}.raw"
    uncompress(uncompress_name)
    File.open(uncompress_name, 'r').each do|line|
      if block_given?
        line = yield line
      end
      dest.puts line
    end
    dest.close
  end

  private
  def uncompress(n)
    ufile = File.open(n, 'w', :external_encoding=>'utf-8')
    Zlib::GzipReader.open(@path) do |gz|
      ufile.puts gz.read
    end
    ufile.close
  end
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

def sort(path, descending=true)
  files = []
  Dir["#{path}/*.gz"].each do |f|
    files << CensusFile.new(f)
  end
  result = files.sort do |l,r|
    if descending
        r.timestamp <=> l.timestamp
    else
        l.timestamp <=> r.timestamp
    end
  end
  result
end


def usage
    puts "Usage: path_census_dir"
end

if ARGV.size < 1
    usage
    exit 1
end

def upload f
    system "java -jar ./target/bigquery-uploader-1.0-SNAPSHOT-all.jar \
                        -projectId jenkins-user-stats \
                        -datasetId jenkinsstats \
                        -tableId jenkins_usage \
                        -bqFile  #{f.name}  \
                        -schemaFile ./schema/usage-schema.json \
                        -credentialFile ./gapipk.json \
                        -uploadType census \
                        -createTable"
end

path = ARGV.shift
order = ARGV.shift

if order.nil?
  order = true
elsif order == 'false'
  order = false
else
  order = true
end

# latest_file=sort(path, order).first
# latest_file.transform(&filter)
# upload f

count = 0
sort(path, order).each do |f|
    puts f.inspect
    f.transform(&filter)
    upload f
    count = count.next
    break if count == 2
end


