#!/usr/bin/ruby
require 'json'

#
# Transforms extension JSON file in to big query friend records
#
def transform
  dest = File.open(ARGV[1], 'w')
  f = File.read(ARGV[0], :external_encoding=>'utf-8')
  data = JSON.parse(f)
  if data['artifacts']
    data['artifacts'].each do |k,v|
        dest.puts v.to_json.gsub('-','_')
    end
  end
  dest.close
end


def usage
    puts "Usage: SOURCE_FILE DESTINATION_FILE"
end

if ARGV.size < 2
    usage
    exit 1
end

transform