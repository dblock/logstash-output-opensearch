#!/usr/bin/env ruby
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../lib'))

require 'bundler/setup'
require 'logstash-core'
require 'logstash/plugin_mixins/ecs_compatibility_support'
require 'logstash/outputs/opensearch'
require 'logstash/codecs/plain'
require 'logstash/instrument/namespaced_null_metric'
require 'cabin'

java_import 'java.util.concurrent.Callable'
java_import 'java.util.concurrent.FutureTask'
java_import 'java.util.concurrent.LinkedBlockingQueue'
java_import 'java.util.concurrent.ThreadPoolExecutor'
java_import 'java.util.concurrent.TimeUnit'

puts "Running on #{RUBY_PLATFORM} ..."

host = 'https://search-dblock-test-opensearch-21-tu5gqrjd4vg4qazjsu6bps5zsy.us-west-2.es.amazonaws.com:443'

auth = {
    "type" => 'aws_iam',
    "aws_access_key_id" => ENV['AWS_ACCESS_KEY_ID'],
    "aws_secret_access_key" => ENV['AWS_SECRET_ACCESS_KEY'],
    "session_token" => ENV['AWS_SESSION_TOKEN'],
    "region" => ENV['AWS_REGION']
}

index_name = "my-index"

settings = {
    "hosts" => host,
    "index" => index_name,
    # "ecs_compatibility" => "disabled",
    "auth_type" => auth
}

config = LogStash::Outputs::OpenSearch.new(settings)
pp config.register

# chars = (0..9).to_a + ('A'..'z').to_a + ('!'..'?').to_a
data = JSON.load(File.read(File.join(File.dirname(__FILE__), 'data.json')))
p data.size

events = data.map do |row|
    LogStash::Event.new(row)
end

# events = [
#     LogStash::Event.new("message" => "d\u2019approbations")
# ]

config.multi_receive(events)

options = {
  hosts: [
    URI(host)
  ],
  logger: Cabin::Channel.get,
  http_compression: true,
  target_bulk_bytes: 1,
  auth_type: auth
}

client = LogStash::Outputs::OpenSearch::HttpClient.new(options)
pp client.get('/')

# # pp client.post('/my-index/_doc/1', {}, LogStash::Json.dump('=>' => '=>'))
# pp client.bulk(   
#     [
#         [
#             "index", {:_id=>nil, :_index=>"my-index", :routing=>nil}, {"message"=>"value", "@version"=>"1", "@timestamp"=>"tt"}
#         ]
#     ]    
# )

sleep 3

count = client.get("/#{index_name}/_count")["count"]
puts "There are #{count} documents in #{index_name}."

pp client.delete("/#{index_name}")
