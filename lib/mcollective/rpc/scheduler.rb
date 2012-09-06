module MCollective
  module RPC
    class Scheduler
      def initialize(options={})
        @request = options.fetch(:request, nil)
        @basedir = options.fetch(:basedir, "/tmp/actions")

        setup_dirs
      end

      def delete!(id=nil)
        id ||= @request.uniqid

        raise "Please provide an ID to delete" unless id

        Log.debug("Removing scheduled action %s" % id)

        File.unlink(action_file(id)) rescue nil
        File.unlink(request_file(id)) rescue nil
        File.unlink(results_file(id)) rescue nil
      end

      def load_results(reply)
        raise "Can only load results for status requests" unless @request.status_request?

        stored_result = YAML.load(File.read(results_file(@request.schedule_status)))

        reply.data = stored_result[:data]
        reply.statuscode = stored_result[:statuscode]
        reply.statusmsg = stored_result[:statusmsg]
      end

      def scheduled_actions
        Dir.entries(File.join(@basedir, "actions")).map do |file|
          if file =~ /(.+?)\.action$/
            {:requestid => $1, :time => Time.at(Integer(File.readlines(File.join(@basedir, "actions", file)).first)).to_s}
          else
            nil
          end
        end.compact
      end

      def schedule_action
        raise "No request to schedule" unless @request

        if Integer(@request.schedule) < Time.now.to_i
          raise "Request %s can not be scheduled it is in the past" % @request.uniqid
        end

        Log.debug("Scheduling a call to %s at %s" % [@request.action, @request.schedule])

        Util.atomic_file(action_file) do |f|
          f.puts @request.schedule
          f.puts "/home/rip/work/github/marionette-collective/bin/run-action.rb ~/.mcollective %s %s" % [request_file, results_file]
        end

        Util.atomic_file(request_file) {|f| f.write(@request.to_job.to_yaml) }

        Util.atomic_file(results_file) {|f| }
      end

      def setup_dirs
        ["actions", "requests", "results", "logs"].each do |dir|
          dir = File.join(@basedir, dir)
          FileUtils.mkdir_p(dir) unless File.directory?(dir)
        end
      end

      def results_file(id=nil)
        id ||= @request.uniqid

        File.join(@basedir, "results", "#{id}.yaml")
      end

      def action_file(id=nil)
        id ||= @request.uniqid

        File.join(@basedir, "actions", "#{id}.action")
      end

      def request_file(id=nil)
        id ||= @request.uniqid

        File.join(@basedir, "requests", "#{id}.yaml")
      end
    end
  end
end
