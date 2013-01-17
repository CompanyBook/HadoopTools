require 'optparse'
include Java
require 'java'


HBASE_HOME = '/Users/birger/source/hbase/current/'
HADOOP_HOME = '/Users/birger/source/hadoop/current/'

HBASE_CONF_DIR = HBASE_HOME + 'conf'
BIN='bin/'
LIB='lib/'
SERVICE_NAME = 'hbase-regionserver'

[HADOOP_HOME, HADOOP_HOME+LIB, HADOOP_HOME+BIN, HBASE_HOME, HBASE_HOME+LIB, HBASE_HOME+BIN].each { |dir|
  Dir[dir+"\*.jar"].each { |jar|
    require jar
  }
}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobClient
#import org.apache.hadoop.mapreduce.JobStatus
import org.apache.hadoop.mapred.JobStatus
import java.net.InetSocketAddress
import org.apache.hadoop.mapreduce.JobContext

#Class for publishing mapreduce job notifications to Flowdock
class JobPublisher
  attr_reader :client, :config, :debug
  @@URL = 'https://api.flowdock.com/v1/messages/team_inbox/'

  def initialize(jobtracker = 'localhost', port =8021, key='' , debug = false)

    @debug = debug
    @known_jobs = []
    @key = key
    @jobtracker = jobtracker
    @config = HBaseConfiguration.create()
    @client = JobClient.new(InetSocketAddress.new(@jobtracker, port), @config)
    @running = false
  end

  def job_report(job)
    format = "%.2f"
    "#{job.jobId} complete: #{job.isJobComplete()}\nsetup: #{ format % job.setupProgress()}\nmap: #{format % job.mapProgress()}\nreduce: #{format % job.reduceProgress()}\ncleanup: #{format % job.cleanupProgress()}\n"
  end

  def run
    @known_jobs = client.jobsToComplete().collect { |job| job }
    puts "starting...."

    @running = true

    while (@running)
      current_jobs = client.jobsToComplete()
      completed = @known_jobs.collect { |a| a.jobId } - current_jobs.collect { |a| a.jobId }

      completed.each { |id|
        completed_job = @known_jobs.find { |a| a.jobId == id }
        publish completed_job
      }

      new_jobs = current_jobs.collect { |a| a.jobId } - @known_jobs.collect { |a| a.jobId }

      new_jobs.each { |id|
        new_job = current_jobs.find { |a| a.jobId == id }
        publish(new_job, 'New Job')
      }
      @known_jobs = current_jobs

      sleep(30) unless @debug
    end

  end

  def thread_run
    @t1=Thread.new { run() }
  end

  def stop
    @running = false
    @t1.join()
  end


  def publish(job, status = job.isJobComplete() ? "Done" :"Running")
    data = {
        "source" => "Hadoop",
        "from_address" => "#{job.username}@companybook.no",
        "from_name" => @jobtracker,
        "subject" => "#{job.jobId} Status:#{status}",
        "content" => job_report(job),
        "link" => "http://#{@jobtracker}:50030/jobdetails.jsp?jobid=#{job.jobId}"

    }.collect { |k, v| "#{k}=#{v}" }.join("&")

    if !@debug
      result = %x{curl --data "#{data}" #{@@URL+@key}}
      puts result
    else
      puts "DEBUG --data " + data + " #{@@URL+@key}"
    end
  end
end

jp = JobPublisher.new('jobtracker.companybook.no', 8021, 'ccdcaacd197d0cd8597b9fcc5fd304be' ,  (%x{hostname}.include? 'Birger'))

if jp.debug
  jp.run
else
  jp.thread_run
end

