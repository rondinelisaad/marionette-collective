#!/usr/bin/env ruby

require 'mcollective'

module MCollective
  module Logger
    class Schedule_logger<Console_logger
      def log(level, from, msg)
        return if @known_levels.index(level) < @known_levels.index(:info)

        if @known_levels.index(level) > @known_levels.index(:info)
          STDERR.puts(msg)
        else
          STDOUT.puts(msg)
        end
      end
    end
  end
end

class NoopConnector
  def send(*args)
    true
  end

  def method_missing(*args)
    true
  end
end

def load_agent(agent)
  classname = "MCollective::Agent::#{agent.capitalize}"

  MCollective::PluginManager.loadclass(classname)
  MCollective::PluginManager << {:type => "#{agent}_agent", :class => classname}
end

def setup(configfile)
  logger = MCollective::Logger::Schedule_logger.new
  MCollective::Log.configure(logger)

  config = MCollective::Config.instance
  config.loadconfig(configfile)

  MCollective::PluginManager.delete("connector_plugin")

  # stub the connector with a noop one
  MCollective::PluginManager << {:type => "connector_plugin", :class => "NoopConnector"}

  MCollective::Data.load_data_sources
end

def call(msgfile, results_file)
  msg = YAML.load(File.read(msgfile))["msg"]

  agent_name = msg[:agent]

  msg[:body][:data].delete(:mcollective_schedule)

  load_agent(agent_name)

  result = MCollective::PluginManager["#{agent_name}_agent"].handlemsg(msg, MCollective::PluginManager["connector_plugin"])

  MCollective::Util.atomic_file(results_file) do |f|
    f.puts YAML.dump(result)
  end
end

config = ARGV.shift
msg = ARGV.shift
results = ARGV.shift

setup(config)
call(msg, results)
