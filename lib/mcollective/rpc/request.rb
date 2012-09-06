module MCollective
  module RPC
    # Simple class to manage compliant requests for MCollective::RPC agents
    class Request
      attr_accessor :time, :action, :data, :sender, :agent, :uniqid, :caller, :ddl, :schedule, :schedule_status
      attr_reader :msg

      def initialize(msg, ddl)
        @msg = msg
        @time = msg[:msgtime]
        @action = msg[:body][:action]
        @data = msg[:body][:data]
        @sender = msg[:senderid]
        @agent = msg[:body][:agent]
        @uniqid = msg[:requestid]
        @caller = msg[:callerid] || "unknown"
        @schedule = @data[:mcollective_schedule]
        @schedule_status = @data[:mcollective_schedule_status]
        @ddl = ddl
      end

      # If data is a hash, quick helper to get access to it's include? method
      # else returns false
      def include?(key)
        return false unless @data.is_a?(Hash)
        return @data.include?(key)
      end

      # If no :process_results is specified always respond else respond
      # based on the supplied property
      def should_respond?
        @data.fetch(:process_results, true)
      end

      def scheduled?
        !!@schedule
      end

      def status_request?
        !!@schedule_status
      end

      # If data is a hash, gives easy access to its members, else returns nil
      def [](key)
        return nil unless @data.is_a?(Hash)
        return @data[key]
      end

      def to_hash
        {:agent => @agent,
         :action => @action,
         :data => @data}
      end

      # Validate the request against the DDL
      def validate!
        @ddl.validate_rpc_request(@action, @data)
      end

      def to_json
        to_hash.merge!({:sender   => @sender,
                        :callerid => @callerid,
                        :uniqid   => @uniqid}).to_json
      end

      def to_job
        {"job" => uniqid,
         "schedule" => schedule,
         "msg" => msg}
      end
    end
  end
end
