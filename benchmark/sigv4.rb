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

  def initialize(iters)
    @iters = iters
  end

  def call
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
     
    @iters.times do |iter|
      begin
        client = LogStash::Outputs::OpenSearch::HttpClient.new(options)
        client.get('/')
        STDOUT.write '.'
      rescue LogStash::Outputs::OpenSearch::HttpClient::Pool::BadResponseCodeError => e
        puts "#{e.response_code}: #{e.response_body}"
        STDOUT.write 'x'
      end
    end
  end
end

logger = Cabin::Channel.get
logger.level = :debug
l = Logger.new(STDOUT)
l.level = Logger::DEBUG
logger.subscribe(l)

java::lang.System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
java::lang.System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
java::lang.System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire", "debug");
java::lang.System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.commons.httpclient", "debug");


executor = ThreadPoolExecutor.new(
  4, # core_pool_treads
  4, # max_pool_threads
  60, # keep_alive_time
  TimeUnit::SECONDS,
  LinkedBlockingQueue.new
)

num_tests = 1
num_threads = 1
num_iters = 1

total_time = 0.0


num_tests.times do |i|
  tasks = []

  t_0 = Time.now
  num_threads.times do
    task = FutureTask.new(MakeRequest.new(num_iters))
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