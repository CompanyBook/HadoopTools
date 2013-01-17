require 'optparse'
include Java
require 'java'

HBASE_HOME = '/Users/birger/source/hbase/current/'
HADOOP_HOME = '/Users/birger/source/hadoop/current/'

HBASE_CONF_DIR = HBASE_HOME + 'conf'
BIN='bin/'
LIB='lib/'
SERVICE_NAME = 'hbase-regionserver'

[HBASE_HOME,HBASE_HOME+LIB, HBASE_HOME+BIN, HADOOP_HOME, HADOOP_HOME+LIB, HADOOP_HOME+BIN ].each { |dir|
    Dir[dir+"\*.jar"].each { |jar|
      require jar
    }
}


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.util.Bytes


class HbaseRestarter

  attr_reader :config, :admin, :connection, :status, :previous_region_server

  @@NULL_BYTE = Bytes.toBytes('')
  @@USER = ''

  def initialize
    @config = getConfiguration()
    @admin = HBaseAdmin.new(@config)
    @status = @admin.getClusterStatus()
    @connection = HConnectionManager.getConnection(@config)
    @previous_region_server = @@NULL_BYTE
  end

  def to_bytes(val)
    Bytes.toBytes(val)
  end

  def destroy
    HConnectionManager.deleteConnection(@config, true)
  end


  def getConfiguration()
    config = HBaseConfiguration.create()
    config.setInt("hbase.client.prefetch.limit", 1)
    # Make a config that retries at short intervals many times
    config.setInt("hbase.client.pause", 500)
    config.setInt("hbase.client.retries.number", 100)
    return config
  end

  def region_count(server)
    get_regions(server).length
  end

  def move_region(region, destination = @@NULL_BYTE)

    if region.class == String || region.class == Java::JavaLang::String
      region_name = to_bytes region
    else
      region_name = region.getEncodedNameAsBytes()
    end

    if destination.class == String
      destination = to_bytes destination
    end

    @admin.move(region_name, destination)
  end

  #move all regions off a regionserver
  def move_regions(server)
    if server.class == String
      server =  region_servers.find { |a| a.get_server_name.include? server }
    end
    #puts "moving regions on RS:#{server.to_s}"
    max_try = 10

    r_count = region_count(server)
    while (r_count !=0 && max_try > 0) do

      get_regions(server).each { |region|
        move_region(region, @previous_region_server)
      }

      sleep 1
      current_count = region_count server

      while (r_count > current_count)
        r_count= current_count
        sleep(3)
      end
    end
    @previous_region_server = Bytes.toBytes(server.getServerName())

    return max_try > 0
  end

  def get_regions(server)
    #puts "getting regions on RS:#{server}"
    region_server = @connection.getHRegionConnection(server.getHostname(), server.getPort())
    region_server.getOnlineRegions()
  end

  def region_server_process_id(server)

    get_pid = "ps aux | grep org.apache.hadoop.hbase.regionserver.HRegionServer | grep #{server.getPort()} | awk '{print $2}' | tail +2 "
    response = %x(#{get_pid})
    response.strip
  end


  def cmd(command, host=nil)
    if host
      "ssh #{user}@host ''"
    else
      command
    end
  end

  def restart_region_server(server, command='date', user='hbase')
    host = server.class == String ? server : server.hostname
    ssh = "ssh -oStrictHostKeyChecking=no #{user}@#{host} #{command}"


    result = %x(#{ssh})

    #%x{sudo ssh -o StrictHostKeyChecking=false -i /var/run/hbase/.ssh/id_dsa hbase@datanode14.companybook.no}
    #port = server.getPort()
    #
    #remote = false
    #puts "restarting RS:#{server}"
    #
    ##stop RS
    #pid = region_server_process_id(server)
    #
    #while (pid != region_server_process_id(server)) do
    #  puts "killing server:#{server} #{pid}"
    #  %x(kill -0 #{pid})
    #  sleep(1)
    #end
    #
    #
    ##start RS
    #args = "-D hbase.regionserver.port=#{port} -D hbase.regionserver.info.port=#{port +100}"
    #%x(#{HBASE_HOME}/#{BIN}/hbase-daemon.sh START #{args} )
    #puts "starting RS with port #{port}"
    #

    #
    #puts region_server_process_id server
    #
    stop_cmd = "ps aux | grep org.apache.hadoop.hbase.regionserver.HRegionServer | grep #{server.getPort()} | awk '{print $2}' | tail -n 1"
    start_cmd = "service hbase-regionserver start "


    #
    #
    #if(remote)
    #  stop_cmd = "ssh hbase@#{server.getHostname()} '#{stop_cmd}'"
    #else
    #  result = /stop_cmd/
    #
    #end
    #
    #
    #
    #puts stop_cmd
    #puts start_cmd
    ##puts  %x( )
    true
  end

  def region_servers()
    @status.getServers().sort{|a,b| a.hostname <=> b.hostname }
  end

  def run_balancer(enable)
    puts "balancer: #{enable}"
    @admin.balanceSwitch(enable)
  end

  #restarts all region servers gracefully
  def restart_cluster
    #stop balancing hbase tables in order to restart nodes
    run_balancer(false)

    region_servers.each { |region_server|
      unless move_regions(region_server) then
        raise "unable to move regions #{region_server}"
      end

      unless restart_region_server(region_server) then
        raise "unable to restart RS #{region_server}"
      end
    }

    run_balancer(true)
  end

  #utility method for moving all regions off a regionserver by hostname (practical in hbase shell)
  def empty_region_server_by_host_name(host_name)
    rs = region_servers.find { |s| s.hostname.include? host_name }
    if rs
      @previous_region_server = Bytes.toBytes('')
      move_regions(rs)
    end
  end

end

region = 'a342add96a2d0e6a8f41259ee7aa8d62'
hr = HbaseRestarter.new
puts hr.region_servers
#rs = hr.region_servers.find { |a| a.get_server_name.include? '17' }
1
