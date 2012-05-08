module MCollective
  module RPC
    # The main component of the Simple RPC client system, this wraps around MCollective::Client
    # and just brings in a lot of convention and standard approached.
    class Client
      attr_accessor :discovery_timeout, :timeout, :verbose, :filter, :config, :progress, :ttl, :reply_to
      attr_reader :client, :stats, :ddl, :agent, :limit_targets, :limit_method, :output_format, :batch_size, :batch_sleep_time, :batch_mode

      @@initial_options = nil

      # Creates a stub for a remote agent, you can pass in an options array in the flags
      # which will then be used else it will just create a default options array with
      # filtering enabled based on the standard command line use.
      #
      #   rpc = RPC::Client.new("rpctest", :configfile => "client.cfg", :options => options)
      #
      # You typically would not call this directly you'd use MCollective::RPC#rpcclient instead
      # which is a wrapper around this that can be used as a Mixin
      def initialize(agent, flags = {})
        if flags.include?(:options)
          initial_options = flags[:options]

        elsif @@initial_options
          initial_options = Marshal.load(@@initial_options)

        else
          oparser = MCollective::Optionparser.new({:verbose => false, :progress_bar => true, :mcollective_limit_targets => false, :batch_size => nil, :batch_sleep_time => 1}, "filter")

          initial_options = oparser.parse do |parser, opts|
            if block_given?
              yield(parser, opts)
            end

            Helpers.add_simplerpc_options(parser, opts)
          end

          @@initial_options = Marshal.dump(initial_options)
        end

        @stats = Stats.new
        @agent = agent
        @discovery_timeout = initial_options[:disctimeout] || 2
        @timeout = initial_options[:timeout] || 5
        @verbose = initial_options[:verbose]
        @filter = initial_options[:filter]
        @config = initial_options[:config]
        @discovered_agents = nil
        @progress = initial_options[:progress_bar]
        @limit_targets = initial_options[:mcollective_limit_targets]
        @limit_method = Config.instance.rpclimitmethod
        @output_format = initial_options[:output_format] || :console
        @force_direct_request = false
        @reply_to = initial_options[:reply_to]

        @batch_size = Integer(initial_options[:batch_size] || 0)
        @batch_sleep_time = Float(initial_options[:batch_sleep_time] || 1)
        @batch_mode = @batch_size > 0

        agent_filter agent

        @client = MCollective::Client.new(@config)
        @client.options = initial_options

        @collective = @client.collective
        @ttl = initial_options[:ttl] || Config.instance.ttl

        # if we can find a DDL for the service override
        # the timeout of the client so we always magically
        # wait appropriate amounts of time.
        #
        # We add the discovery timeout to the ddl supplied
        # timeout as the discovery timeout tends to be tuned
        # for local network conditions and fact source speed
        # which would other wise not be accounted for and
        # some results might get missed.
        #
        # We do this only if the timeout is the default 5
        # seconds, so that users cli overrides will still
        # get applied
        begin
          @ddl = DDL.new(agent)
          @timeout = @ddl.meta[:timeout] + @discovery_timeout if @timeout == 5
        rescue Exception => e
          Log.debug("Could not find DDL: #{e}")
          @ddl = nil
        end

        # allows stderr and stdout to be overridden for testing
        # but also for web apps that might not want a bunch of stuff
        # generated to actual file handles
        if initial_options[:stderr]
          @stderr = initial_options[:stderr]
        else
          @stderr = STDERR
          @stderr.sync = true
        end

        if initial_options[:stdout]
          @stdout = initial_options[:stdout]
        else
          @stdout = STDOUT
          @stdout.sync = true
        end
      end

      # Disconnects cleanly from the middleware
      def disconnect
        @client.disconnect
      end

      # Returns help for an agent if a DDL was found
      def help(template)
        if @ddl
          @ddl.help(template)
        else
          return "Can't find DDL for agent '#{@agent}'"
        end
      end

      # Creates a suitable request hash for the SimpleRPC agent.
      #
      # You'd use this if you ever wanted to take care of sending
      # requests on your own - perhaps via Client#sendreq if you
      # didn't care for responses.
      #
      # In that case you can just do:
      #
      #   msg = your_rpc.new_request("some_action", :foo => :bar)
      #   filter = your_rpc.filter
      #
      #   your_rpc.client.sendreq(msg, msg[:agent], filter)
      #
      # This will send a SimpleRPC request to the action some_action
      # with arguments :foo = :bar, it will return immediately and
      # you will have no indication at all if the request was receieved or not
      #
      # Clearly the use of this technique should be limited and done only
      # if your code requires such a thing
      def new_request(action, data)
        callerid = PluginManager["security_plugin"].callerid

        raise 'callerid received from security plugin is not valid' unless PluginManager["security_plugin"].valid_callerid?(callerid)

        {:agent  => @agent,
         :action => action,
         :caller => callerid,
         :data   => data}
      end

      # Magic handler to invoke remote methods
      #
      # Once the stub is created using the constructor or the RPC#rpcclient helper you can
      # call remote actions easily:
      #
      #   ret = rpc.echo(:msg => "hello world")
      #
      # This will call the 'echo' action of the 'rpctest' agent and return the result as an array,
      # the array will be a simplified result set from the usual full MCollective::Client#req with
      # additional error codes and error text:
      #
      # {
      #   :sender => "remote.box.com",
      #   :statuscode => 0,
      #   :statusmsg => "OK",
      #   :data => "hello world"
      # }
      #
      # If :statuscode is 0 then everything went find, if it's 1 then you supplied the correct arguments etc
      # but the request could not be completed, you'll find a human parsable reason in :statusmsg then.
      #
      # Codes 2 to 5 maps directly to UnknownRPCAction, MissingRPCData, InvalidRPCData and UnknownRPCError
      # see below for a description of those, in each case :statusmsg would be the reason for failure.
      #
      # To get access to the full result of the MCollective::Client#req calls you can pass in a block:
      #
      #   rpc.echo(:msg => "hello world") do |resp|
      #      pp resp
      #   end
      #
      # In this case resp will the result from MCollective::Client#req.  Instead of returning simple
      # text and codes as above you'll also need to handle the following exceptions:
      #
      # UnknownRPCAction - There is no matching action on the agent
      # MissingRPCData - You did not supply all the needed parameters for the action
      # InvalidRPCData - The data you did supply did not pass validation
      # UnknownRPCError - Some other error prevented the agent from running
      #
      # During calls a progress indicator will be shown of how many results we've received against
      # how many nodes were discovered, you can disable this by setting progress to false:
      #
      #   rpc.progress = false
      #
      # This supports a 2nd mode where it will send the SimpleRPC request and never handle the
      # responses.  It's a bit like UDP, it sends the request with the filter attached and you
      # only get back the requestid, you have no indication about results.
      #
      # You can invoke this using:
      #
      #   puts rpc.echo(:process_results => false)
      #
      # This will output just the request id.
      #
      # Batched processing is supported:
      #
      #   printrpc rpc.ping(:batch_size => 5)
      #
      # This will do everything exactly as normal but communicate to only 5
      # agents at a time
      def method_missing(method_name, *args, &block)
        # set args to an empty hash if nothings given
        args = args[0]
        args = {} if args.nil?

        action = method_name.to_s

        @stats.reset

        @ddl.validate_rpc_request(action, args) if @ddl

        # if a global batch size is set just use that else set it
        # in the case that it was passed as an argument
        batch_mode = args.include?(:batch_size) || @batch_mode
        batch_size = args.delete(:batch_size) || @batch_size
        batch_sleep_time = args.delete(:batch_sleep_time) || @batch_sleep_time

        # if we were given a batch_size argument thats 0 and batch_mode was
        # determined to be on via global options etc this will allow a batch_size
        # of 0 to disable or batch_mode for this call only
        batch_mode = (batch_mode && Integer(batch_size) > 0)

        # Handle single target requests by doing discovery and picking
        # a random node.  Then do a custom request specifying a filter
        # that will only match the one node.
        if @limit_targets
          target_nodes = pick_nodes_from_discovered(@limit_targets)
          Log.debug("Picked #{target_nodes.join(',')} as limited target(s)")

          custom_request(action, args, target_nodes, {"identity" => /^(#{target_nodes.join('|')})$/}, &block)
        elsif batch_mode
          call_agent_batched(action, args, options, batch_size, batch_sleep_time, &block)
        else
          call_agent(action, args, options, :auto, &block)
        end
      end

      # Constructs custom requests with custom filters and discovery data
      # the idea is that this would be used in web applications where you
      # might be using a cached copy of data provided by a registration agent
      # to figure out on your own what nodes will be responding and what your
      # filter would be.
      #
      # This will help you essentially short circuit the traditional cycle of:
      #
      # mc discover / call / wait for discovered nodes
      #
      # by doing discovery however you like, contructing a filter and a list of
      # nodes you expect responses from.
      #
      # Other than that it will work exactly like a normal call, blocks will behave
      # the same way, stats will be handled the same way etcetc
      #
      # If you just wanted to contact one machine for example with a client that
      # already has other filter options setup you can do:
      #
      # puppet.custom_request("runonce", {}, ["your.box.com"], {:identity => "your.box.com"})
      #
      # This will do runonce action on just 'your.box.com', no discovery will be
      # done and after receiving just one response it will stop waiting for responses
      #
      # If direct_addressing is enabled in the config file you can provide an empty
      # hash as a filter, this will force that request to be a directly addressed
      # request which technically does not need filters.  If you try to use this
      # mode with direct addressing disabled an exception will be raise
      def custom_request(action, args, expected_agents, filter = {}, &block)
        @ddl.validate_rpc_request(action, args) if @ddl

        if filter == {} && !Config.instance.direct_addressing
          raise "Attempted to do a filterless custom_request without direct_addressing enabled, preventing unexpected call to all nodes"
        end

        @stats.reset

        custom_filter = Util.empty_filter
        custom_options = options.clone

        # merge the supplied filter with the standard empty one
        # we could just use the merge method but I want to be sure
        # we dont merge in stuff that isnt actually valid
        ["identity", "fact", "agent", "cf_class", "compound"].each do |ftype|
          if filter.include?(ftype)
            custom_filter[ftype] = [filter[ftype], custom_filter[ftype]].flatten
          end
        end

        # ensure that all filters at least restrict the call to the agent we're a proxy for
        custom_filter["agent"] << @agent unless custom_filter["agent"].include?(@agent)
        custom_options[:filter] = custom_filter

        # Fake out the stats discovery would have put there
        @stats.discovered_agents([expected_agents].flatten)

        # Handle fire and forget requests
        #
        # If a specific reply-to was set then from the client perspective this should
        # be a fire and forget request too since no response will ever reach us - it
        # will go to the reply-to destination
        if args[:process_results] == false || @reply_to
          return fire_and_forget_request(action, args, custom_filter)
        end

        # Now do a call pretty much exactly like in method_missing except with our own
        # options and discovery magic
        if block_given?
          call_agent(action, args, custom_options, [expected_agents].flatten) do |r|
            block.call(r)
          end
        else
          call_agent(action, args, custom_options, [expected_agents].flatten)
        end
      end

      # Sets the class filter
      def class_filter(klass)
        @filter["cf_class"] << klass
        @filter["cf_class"].compact!
        reset
      end

      # Sets the fact filter
      def fact_filter(fact, value=nil, operator="=")
        return if fact.nil?
        return if fact == false

        if value.nil?
          parsed = Util.parse_fact_string(fact)
          @filter["fact"] << parsed unless parsed == false
        else
          parsed = Util.parse_fact_string("#{fact}#{operator}#{value}")
          @filter["fact"] << parsed unless parsed == false
        end

        @filter["fact"].compact!
        reset
      end

      # Sets the agent filter
      def agent_filter(agent)
        @filter["agent"] << agent
        @filter["agent"].compact!
        reset
      end

      # Sets the identity filter
      def identity_filter(identity)
        @filter["identity"] << identity
        @filter["identity"].compact!
        reset
      end

      # Set a compound filter
      def compound_filter(filter)
        @filter["compound"] <<  Util.create_compound_callstack(filter)
        reset
      end

      # Resets various internal parts of the class, most importantly it clears
      # out the cached discovery
      def reset
        @discovered_agents = nil
      end

      # Reet the filter to an empty one
      def reset_filter
        @filter = Util.empty_filter
        agent_filter @agent
      end

      # Does discovery based on the filters set, if a discovery was
      # previously done return that else do a new discovery.
      #
      # Alternatively if identity filters are given and none of them are
      # regular expressions then just use the provided data as discovered
      # data, avoiding discovery
      #
      # Discovery can be forced if direct_addressing is enabled by passing
      # in an array of nodes with :nodes or JSON data like those produced
      # by mcollective RPC JSON output using :json
      #
      # Will show a message indicating its doing discovery if running
      # verbose or if the :verbose flag is passed in.
      #
      # Use reset to force a new discovery
      def discover(flags={})
        flags.keys.each do |key|
          raise "Unknown option #{key} passed to discover" unless [:verbose, :hosts, :nodes, :json].include?(key)
        end

        flags.include?(:verbose) ? verbose = flags[:verbose] : verbose = @verbose

        verbose = false unless @output_format == :console

        # flags[:nodes] and flags[:hosts] are the same thing, we should never have
        # allowed :hosts as that was inconsistent with the established terminology
        flags[:nodes] = flags.delete(:hosts) if flags.include?(:hosts)

        reset if flags[:nodes] || flags[:json]

        unless @discovered_agents
          # if either hosts or JSON is supplied try to figure out discovery data from there
          # if direct_addressing is not enabled this is a critical error as the user might
          # not have supplied filters so raise an exception
          if flags[:nodes] || flags[:json]
            raise "Can only supply discovery data if direct_addressing is enabled" unless Config.instance.direct_addressing

            hosts = []

            if flags[:nodes]
              hosts = Helpers.extract_hosts_from_array(flags[:nodes])
            elsif flags[:json]
              hosts = Helpers.extract_hosts_from_json(flags[:json])
            end

            raise "Could not find any hosts in discovery data provided" if hosts.empty?

            @discovered_agents = hosts
            @force_direct_request = true

          # if an identity filter is supplied and it is all strings no regex we can use that
          # as discovery data, technically the identity filter is then redundant if we are
          # in direct addressing mode and we could empty it out but this use case should
          # only really be for a few -I's on the CLI
          #
          # For safety we leave the filter in place for now, that way we can support this
          # enhancement also in broadcast mode
          elsif options[:filter]["identity"].size > 0
            regex_filters = options[:filter]["identity"].select{|i| i.match("^\/")}.size

            if regex_filters == 0
              @discovered_agents = options[:filter]["identity"].clone
              @force_direct_request = true if Config.instance.direct_addressing
            end
          end
        end

        # All else fails we do it the hard way using a traditional broadcast
        unless @discovered_agents
          @stats.time_discovery :start

          actual_timeout = options[:disctimeout] + @client.timeout_for_compound_filter(options[:filter]["compound"])
          @stderr.print("Determining the amount of hosts matching filter for %d seconds .... " % actual_timeout) if verbose

          # if the requested limit is a pure number and not a percent
          # and if we're configured to use the first found hosts as the
          # limit method then pass in the limit thus minimizing the amount
          # of work we do in the discover phase and speeding it up significantly
          if @limit_method == :first and @limit_targets.is_a?(Fixnum)
            @discovered_agents = @client.discover(@filter, options[:disctimeout], @limit_targets)
          else
            @discovered_agents = @client.discover(@filter, options[:disctimeout])
          end

          @force_direct_request = false
          @stderr.puts(@discovered_agents.size) if verbose

          @stats.time_discovery :end
        end

        @stats.discovered_agents(@discovered_agents)
        RPC.discovered(@discovered_agents)

        @discovered_agents
      end

      # Provides a normal options hash like you would get from
      # Optionparser
      def options
        {:disctimeout => @discovery_timeout,
         :timeout => @timeout,
         :verbose => @verbose,
         :filter => @filter,
         :collective => @collective,
         :output_format => @output_format,
         :ttl => @ttl,
         :config => @config}
      end

      # Sets the collective we are communicating with
      def collective=(c)
        @collective = c
        @client.options[:collective] = c
      end

      # Sets and sanity checks the limit_targets variable
      # used to restrict how many nodes we'll target
      def limit_targets=(limit)
        if limit.is_a?(String)
          raise "Invalid limit specified: #{limit} valid limits are /^\d+%*$/" unless limit =~ /^\d+%*$/

          begin
            @limit_targets = Integer(limit)
          rescue
            @limit_targets = limit
          end
        else
          @limit_targets = Integer(limit)
        end
      end

      # Sets and sanity check the limit_method variable
      # used to determine how to limit targets if limit_targets is set
      def limit_method=(method)
        method = method.to_sym unless method.is_a?(Symbol)

        raise "Unknown limit method #{method} must be :random or :first" unless [:random, :first].include?(method)

        @limit_method = method
      end

      # Sets the batch size, if the size is set to 0 that will disable batch mode
      def batch_size=(limit)
        raise "Can only set batch size if direct addressing is supported" unless Config.instance.direct_addressing

        @batch_size = Integer(limit)
        @batch_mode = @batch_size > 0
      end

      def batch_sleep_time=(time)
        raise "Can only set batch sleep time if direct addressing is supported" unless Config.instance.direct_addressing

        @batch_sleep_time = Float(time)
      end

      private
      # Pick a number of nodes from the discovered nodes
      #
      # The count should be a string that can be either
      # just a number or a percentage like 10%
      #
      # It will select nodes from the discovered list based
      # on the rpclimitmethod configuration option which can
      # be either :first or anything else
      #
      #   - :first would be a simple way to do a distance based
      #     selection
      #   - anything else will just pick one at random
      def pick_nodes_from_discovered(count)
        if count =~ /%$/
          pct = (discover.size * (count.to_f / 100)).to_i
          pct == 0 ? count = 1 : count = pct
        else
          count = count.to_i
        end

        return discover if discover.size <= count

        result = []

        if @limit_method == :first
          return discover[0, count]
        else
          count.times do
            rnd = rand(discover.size)
            result << discover[rnd]
            discover.delete_at(rnd)
          end
        end

        [result].flatten
      end

      # for requests that do not care for results just
      # return the request id and don't do any of the
      # response processing.
      #
      # We send the :process_results flag with to the
      # nodes so they can make decisions based on that.
      #
      # Should only be called via method_missing
      def fire_and_forget_request(action, args, filter=nil)
        @ddl.validate_rpc_request(action, args) if @ddl

        req = new_request(action.to_s, args)

        filter = options[:filter] unless filter

        message = Message.new(req, nil, {:agent => @agent, :type => :request, :collective => @collective, :filter => filter, :options => options})
        message.reply_to = @reply_to if @reply_to

        return @client.sendreq(message, nil)
      end

      # Calls an agent in a way very similar to call_agent but it supports batching
      # the queries to the network.
      #
      # The result sets, stats, block handling etc is all exactly like you would expect
      # from normal call_agent.
      #
      # This is used by method_missing and works only with direct addressing mode
      def call_agent_batched(action, args, opts, batch_size, sleep_time, &block)
        raise "Batched requests requires direct addressing" unless Config.instance.direct_addressing
        raise "Cannot bypass result processing for batched requests" if args[:process_results] == false

        batch_size = Integer(batch_size)
        sleep_time = Float(sleep_time)

        Log.debug("Calling #{agent}##{action} in batches of #{batch_size} with sleep time of #{sleep_time}")

        @force_direct_request = true

        discovered = discover
        result = []
        respcount = 0

        if discovered.size > 0
          req = new_request(action.to_s, args)

          if @progress && !block_given?
            twirl = Progress.new
            @stdout.puts
            @stdout.print twirl.twirl(respcount, discovered.size)
          end

          discovered.in_groups_of(batch_size) do |hosts, last_batch|
            message = Message.new(req, nil, {:agent => @agent, :type => :direct_request, :collective => @collective, :filter => opts[:filter], :options => opts})
            message.discovered_hosts = hosts.clone.compact

            @client.req(message) do |resp|
              respcount += 1

              if block_given?
                process_results_with_block(action, resp, block)
              else
                @stdout.print twirl.twirl(respcount, discovered.size) if @progress

                result << process_results_without_block(resp, action)
              end
            end

            @stats.noresponsefrom.concat @client.stats[:noresponsefrom]
            @stats.responses += @client.stats[:responses]
            @stats.blocktime += @client.stats[:blocktime] + sleep_time
            @stats.totaltime += @client.stats[:totaltime]
            @stats.discoverytime += @client.stats[:discoverytime]

            sleep sleep_time unless last_batch
          end
        else
          @stderr.print("\nNo request sent, we did not discover any nodes.")
        end

        @stats.finish_request

        RPC.stats(@stats)

        @stdout.print("\n") if @progress

        if block_given?
          return stats
        else
          return [result].flatten
        end
      end

      # Handles traditional calls to the remote agents with full stats
      # blocks, non blocks and everything else supported.
      #
      # Other methods of calling the nodes can reuse this code by
      # for example specifying custom options and discovery data
      def call_agent(action, args, opts, disc=:auto, &block)
        # Handle fire and forget requests and make sure
        # the :process_results value is set appropriately
        #
        # specific reply-to requests should be treated like
        # fire and forget since the client will never get
        # the responses
        if args[:process_results] == false || @reply_to
          return fire_and_forget_request(action, args)
        else
          args[:process_results] = true
        end

        # Do discovery when no specific discovery array is given
        #
        # If an array is given set the force_direct_request hint that
        # will tell the message object to be a direct request one
        if disc == :auto
          discovered = discover
        else
          @force_direct_request = true if Config.instance.direct_addressing
          discovered = disc
        end

        req = new_request(action.to_s, args)

        message = Message.new(req, nil, {:agent => @agent, :type => :request, :collective => @collective, :filter => opts[:filter], :options => opts})
        message.discovered_hosts = discovered.clone
        message.type = :direct_request if @force_direct_request

        result = []
        respcount = 0

        if discovered.size > 0
          if @progress && !block_given?
            twirl = Progress.new
            @stdout.puts
            @stdout.print twirl.twirl(respcount, discovered.size)
          end

          @client.req(message) do |resp|
            respcount += 1

            if block_given?
              process_results_with_block(action, resp, block)
            else
              @stdout.print twirl.twirl(respcount, discovered.size) if @progress

              result << process_results_without_block(resp, action)
            end
          end

          @stats.client_stats = @client.stats
        else
          @stderr.print("\nNo request sent, we did not discover any nodes.")
        end

        @stats.finish_request

        RPC.stats(@stats)

        @stdout.print("\n\n") if @progress

        if block_given?
          return stats
        else
          return [result].flatten
        end
      end

      # Handles result sets that has no block associated, sets fails and ok
      # in the stats object and return a hash of the response to send to the
      # caller
      def process_results_without_block(resp, action)
        @stats.node_responded(resp[:senderid])

        if resp[:body][:statuscode] == 0 || resp[:body][:statuscode] == 1
          @stats.ok if resp[:body][:statuscode] == 0
          @stats.fail if resp[:body][:statuscode] == 1

          return Result.new(@agent, action, {:sender => resp[:senderid], :statuscode => resp[:body][:statuscode],
                              :statusmsg => resp[:body][:statusmsg], :data => resp[:body][:data]})
        else
          @stats.fail

          return Result.new(@agent, action, {:sender => resp[:senderid], :statuscode => resp[:body][:statuscode],
                              :statusmsg => resp[:body][:statusmsg], :data => nil})
        end
      end

      # process client requests by calling a block on each result
      # in this mode we do not do anything fancy with the result
      # objects and we raise exceptions if there are problems with
      # the data
      def process_results_with_block(action, resp, block)
        @stats.node_responded(resp[:senderid])

        if resp[:body][:statuscode] == 0 || resp[:body][:statuscode] == 1
          @stats.time_block_execution :start

          case block.arity
          when 1
            block.call(resp)
          when 2
            rpcresp = Result.new(@agent, action, {:sender => resp[:senderid], :statuscode => resp[:body][:statuscode],
                                   :statusmsg => resp[:body][:statusmsg], :data => resp[:body][:data]})
            block.call(resp, rpcresp)
          end

          @stats.time_block_execution :end
        else
          case resp[:body][:statuscode]
          when 2
            raise UnknownRPCAction, resp[:body][:statusmsg]
          when 3
            raise MissingRPCData, resp[:body][:statusmsg]
          when 4
            raise InvalidRPCData, resp[:body][:statusmsg]
          when 5
            raise UnknownRPCError, resp[:body][:statusmsg]
          end
        end
      end
    end
  end
end
