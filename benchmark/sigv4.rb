#!/usr/bin/env ruby
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../lib'))

require 'bundler/setup'
require 'logstash-core'
require 'logstash/outputs/opensearch'
require 'cabin'

java_import 'java.util.concurrent.Callable'
java_import 'java.util.concurrent.FutureTask'
java_import 'java.util.concurrent.LinkedBlockingQueue'
java_import 'java.util.concurrent.ThreadPoolExecutor'
java_import 'java.util.concurrent.TimeUnit'

puts "Running on #{RUBY_PLATFORM} ..."

class MakeRequest
  include Callable

  def initialize(client, iters)
    @client = client
    @iters = iters
  end

  def call
    begin
      @iters.times do
        @client.get('/')
      end
      STDOUT.write '.'
    rescue LogStash::Outputs::OpenSearch::HttpClient::Pool::BadResponseCodeError
      STDOUT.write 'x'
    end
  end
end

executor = ThreadPoolExecutor.new(
  4, # core_pool_treads
  4, # max_pool_threads
  60, # keep_alive_time
  TimeUnit::SECONDS,
  LinkedBlockingQueue.new
)

num_tests = 10
num_threads = 10
num_iters = 100

total_time = 0.0

options = {
    hosts: [
      URI('https://search-dblock-test-opensearch-21-tu5gqrjd4vg4qazjsu6bps5zsy.us-west-2.es.amazonaws.com')
    ],
    logger: Cabin::Channel.get,
    auth_type: {
        "type" => 'aws_iam',
        "aws_access_key_id" => ENV['AWS_ACCESS_KEY_ID'],
        "aws_secret_access_key" => ENV['AWS_SECRET_ACCESS_KEY'],
        "session_token" => ENV['AWS_SESSION_TOKEN'],
        "region" => ENV['AWS_REGION']
    }
}

client = LogStash::Outputs::OpenSearch::HttpClient.new(options)

num_tests.times do |i|
  tasks = []

  t_0 = Time.now
  num_threads.times do
    task = FutureTask.new(MakeRequest.new(client, num_iters))
    executor.execute(task)
    tasks << task
  end

  tasks.each do |t|
    t.get
  end
  t_1 = Time.now

  time_ms = (t_1-t_0) * 1000.0
  puts "TEST #{i}: Time elapsed = #{time_ms}ms"
  total_time +=  time_ms
end
executor.shutdown()

puts "Average completion time: #{total_time/num_tests}"