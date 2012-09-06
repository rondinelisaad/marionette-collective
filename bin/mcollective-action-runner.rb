#!/usr/bin/env ruby

require 'mcollective'
require 'rb-inotify'
require 'rufus/scheduler'

Log = MCollective::Log

def run_command(id, command)
  # see http://stackoverflow.com/questions/1740308/create-a-daemon-with-double-fork-in-ruby
  raise 'First fork failed' if (pid = fork) == -1
  return unless pid.nil?

  # rufus does Kernel#exit when you stop it, doh
  begin
    @rufus.stop
    @notifier.stop
  rescue Exception
  end

  Process.setsid

  raise 'Second fork failed' if (pid = fork) == -1
  exit! unless pid.nil?

  File.open(File.join(@basedir, "actions", "#{id}.pid"), "w") {|f| f.puts Process.pid}

  Dir.chdir '/'
  File.umask 0000

  STDIN.reopen("/dev/null")
  STDOUT.reopen(File.join(@basedir, "logs", "#{id}.stdout"), "a")
  STDERR.reopen(File.join(@basedir, "logs", "#{id}.stderr"), "a")

  File.unlink(File.join(@basedir, "actions", "#{id}.action")) rescue nil

  exec(command)
end

def add_action(action)
  Log.warn("Processing %s as a new action" % action)

  request_id = /^([a-z0-9]{32})\.action/.match(action)[1]
  request_file = File.join(@basedir, "actions", action)
  request_lines = File.readlines(request_file)
  request_time = Integer(request_lines.first)
  request_command = request_lines.last

  if request_time > Time.now.to_i
    @rufus.find_by_tag(request_id).each do |action|
      Log.warn("Unscheduling rufus job %s" % action.job_id)
      action.unschedule
    end

    @rufus.at request_time, :tags => request_id, :blocking => true do
      Log.warn("Running action %s from file %s" % [request_id, request_file])
      Log.warn("Running: %s" % request_command)

      begin
        run_command(request_id, request_command)
      rescue
        Log.error("Failed to run command: %s: %s" % [e.class, e.to_s])
      end
    end
  else
    Log.warn("%s is an old job, removing" % request_id)
    File.unlink(request_file)
  end
rescue Exception => e
  Log.error("Could not run job: %s: %s" % [e.class, e.to_s])
end

def delete_action(action)
  Log.warn("Processing %s as a deleted action" % action)

  request_id = /^([a-z0-9]{32})\.action/.match(action)[1]
  request_file = File.join(@basedir, "actions", action)

  @rufus.find_by_tag(request_id).each do |action|
    Log.warn("Unscheduling rufus job %s" % action.job_id)
    action.unschedule
  end
rescue Exception => e
  Log.error("Could not delete job: %s: %s" % [e.class, e.to_s])
end

def load_all_actions
  Dir.entries((File.join(@basedir, "actions"))).grep(/^[a-z0-9]{32}\.action$/).each do |action|
    add_action(action)
  end
end

def clear_pids
  Dir.entries((File.join(@basedir, "actions"))).grep(/^[a-z0-9]{32}\.pid$/).each do |pid|
    pidfile = File.join(@basedir, "actions", pid)
    pid = File.readlines(pidfile).first.chomp

    unless File.exist?("/proc/#{pid}")
      Log.info("Deleting pid file for completed command")
      File.unlink(pidfile)
    end
  end
end

MCollective::Applications.load_config

@basedir = "/tmp/actions"
@notifier = INotify::Notifier.new
@rufus = Rufus::Scheduler.start_new

def @rufus.handle_exception(job, exception)
  puts "job #{job.job_id} caught exception '#{exception}'"
end

@rufus.every("10s") do
  clear_pids
end

@rufus.every("30s") do
  Log.info("Still alive")
end

load_all_actions

@notifier.watch("/tmp/actions/actions", :move, :close_write, :delete) do |event|
  next unless event.name =~ /action$/

  if event.flags.include?(:moved_to)
    add_action(event.name)

  elsif event.flags.include?(:delete)
    delete_action(event.name)

  else
    Log.warn("Unknown event flags %s received for file %s" % [event.flags.inspect, event.name])
  end
end

@notifier.run
