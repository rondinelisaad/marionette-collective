#!/usr/bin/env rspec

require 'spec_helper'

module MCollective
  module RPC
    describe Client do
      before do
        @coreclient = mock
        @discoverer = mock
        ddl = stub

        ddl.stubs(:meta).returns({:timeout => 2})

        @discoverer.stubs(:force_direct_mode?).returns(false)
        @discoverer.stubs(:ddl).returns(ddl)
        @discoverer.stubs(:discovery_method).returns("mc")
        @discoverer.stubs(:force_discovery_method_by_filter).returns(false)

        @coreclient.stubs("options=")
        @coreclient.stubs(:collective).returns("mcollective")
        @coreclient.stubs(:timeout_for_compound_filter).returns(0)
        @coreclient.stubs(:discoverer).returns(@discoverer)

        Config.instance.stubs(:loadconfig).with("/nonexisting").returns(true)
        MCollective::Client.stubs(:new).returns(@coreclient)
        Config.instance.stubs(:direct_addressing).returns(true)

        @stderr = StringIO.new
        @stdout = StringIO.new

        @client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
      end

      describe "#discovery_method=" do
        it "should set the method" do
          @client.discovery_method = "rspec"
          @client.discovery_method.should == "rspec"
        end

        it "should set initial options if provided" do
          client = Client.new("rspec", {:options => {:discovery_options => ["rspec"], :filter => Util.empty_filter, :config => "/nonexisting"}})
          client.discovery_method = "rspec"
          client.discovery_method.should == "rspec"
          client.discovery_options.should == ["rspec"]
        end

        it "should clear the options if none are given initially" do
          @client.discovery_options = ["rspec"]
          @client.discovery_method = "rspec"
          @client.discovery_options.should == []
        end

        it "should set the client options" do
          @client.expects(:options).returns("rspec")
          @client.client.expects(:options=).with("rspec")
          @client.discovery_method = "rspec"
        end

        it "should adjust discovery timeout for the new method" do
          @client.expects(:discovery_timeout).once.returns(1)
          @client.discovery_method = "rspec"
          @client.instance_variable_get("@discovery_timeout").should == 1
        end

        it "should reset the rpc client" do
          @client.expects(:reset)
          @client.discovery_method = "rspec"
        end
      end

      describe "#discovery_options=" do
        it "should flatten the options array" do
          @client.discovery_options = "foo"
          @client.discovery_options.should == ["foo"]
        end
      end

      describe "#discovery_timeout" do
        it "should favour the initial options supplied timeout" do
          client = Client.new("rspec", {:options => {:disctimeout => 3, :filter => Util.empty_filter, :config => "/nonexisting"}})
          client.discovery_timeout.should == 3
        end

        it "should return the DDL data if no specific options are supplied" do
          client = Client.new("rspec", {:options => {:disctimeout => nil, :filter => Util.empty_filter, :config => "/nonexisting"}})
          client.discovery_timeout.should == 2
        end
      end

      describe "#limit_method" do
        it "should force strings to symbols" do
          @client.limit_method = "first"
          @client.limit_method.should == :first
        end

        it "should only allow valid methods" do
          @client.limit_method = :first
          @client.limit_method.should == :first
          @client.limit_method = :random
          @client.limit_method.should == :random

          expect { @client.limit_method = :fail }.to raise_error(/Unknown/)
          expect { @client.limit_method = "fail" }.to raise_error(/Unknown/)
        end
      end

      describe "#method_missing" do
        it "should reset the stats" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.stubs(:call_agent)

          Stats.any_instance.expects(:reset).once
          client.foo
        end

        it "should validate the request against the ddl" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          client.stubs(:call_agent)

          ddl = mock
          ddl.expects(:validate_rpc_request).with("rspec", {:arg => :val}).raises("validation failed")
          client.instance_variable_set("@ddl", ddl)

          expect { client.rspec(:arg => :val) }.to raise_error("validation failed")
        end

        it "should support limited targets" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.limit_targets = 10

          client.expects(:pick_nodes_from_discovered).with(10).returns(["one", "two"])
          client.expects(:custom_request).with("foo", {}, ["one", "two"], {"identity" => /^(one|two)$/}).once

          client.foo
        end

        describe "batch mode" do
          before do
            Config.instance.stubs(:direct_addressing).returns(true)
            @client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          end

          it "should support global batch_size" do
            @client.batch_size = 10
            @client.expects(:call_agent_batched).with("rspec", {}, @client.options, 10, 1)
            @client.rspec
          end

          it "should support custom batch_size" do
            @client.expects(:call_agent_batched).with("rspec", {}, @client.options, 10, 1)
            @client.rspec :batch_size => 10
          end

          it "should allow supplied batch_size override global one" do
            @client.batch_size = 10
            @client.expects(:call_agent_batched).with("rspec", {}, @client.options, 20, 1)
            @client.rspec :batch_size => 20
          end

          it "should support global batch_sleep_time" do
            @client.batch_size = 10
            @client.batch_sleep_time = 20
            @client.expects(:call_agent_batched).with("rspec", {}, @client.options, 10, 20)
            @client.rspec
          end

          it "should support custom batch_sleep_time" do
            @client.batch_size = 10
            @client.expects(:call_agent_batched).with("rspec", {}, @client.options, 10, 20)
            @client.rspec :batch_sleep_time => 20
          end

          it "should allow supplied batch_sleep_time override global one" do
            @client.batch_size = 10
            @client.batch_sleep_time = 10
            @client.expects(:call_agent_batched).with("rspec", {}, @client.options, 10, 20)
            @client.rspec :batch_sleep_time => 20
          end
        end

        it "should support normal calls" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          client.expects(:call_agent).with("foo", {}, client.options, :auto).once

          client.foo
        end
      end

      describe "#limit_targets=" do
        before do
          client = stub
          discoverer = stub
          ddl = stub

          ddl.stubs(:meta).returns({:timeout => 2})

          discoverer.stubs(:force_direct_mode?).returns(false)
          discoverer.stubs(:ddl).returns(ddl)
          discoverer.stubs(:discovery_method).returns("mc")

          client.stubs("options=")
          client.stubs(:collective).returns("mcollective")
          client.stubs(:discoverer).returns(discoverer)

          Config.instance.stubs(:loadconfig).with("/nonexisting").returns(true)
          MCollective::Client.expects(:new).returns(client)
          Config.instance.stubs(:direct_addressing).returns(true)

          @client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
        end

        it "should support percentages" do
          @client.limit_targets = "10%"
          @client.limit_targets.should == "10%"
        end

        it "should support integers" do
          @client.limit_targets = 10
          @client.limit_targets.should == 10
          @client.limit_targets = "20"
          @client.limit_targets.should == 20
          @client.limit_targets = 1.1
          @client.limit_targets.should == 1
          @client.limit_targets = 1.7
          @client.limit_targets.should == 1
        end

        it "should not invalid limits to be set" do
          expect { @client.limit_targets = "a" }.to raise_error(/Invalid/)
          expect { @client.limit_targets = "%1" }.to raise_error(/Invalid/)
          expect { @client.limit_targets = "1.1" }.to raise_error(/Invalid/)
        end
      end

      describe "#call_agent_batched" do
        before do
          @client = stub
          @discoverer = stub
          @ddl = stub

          @ddl.stubs(:meta).returns({:timeout => 2})

          @discoverer.stubs(:force_direct_mode?).returns(false)
          @discoverer.stubs(:ddl).returns(@ddl)
          @discoverer.stubs(:discovery_method).returns("mc")

          @client.stubs("options=")
          @client.stubs(:collective).returns("mcollective")
          @client.stubs(:discoverer).returns(@discoverer)

          Config.instance.stubs(:loadconfig).with("/nonexisting").returns(true)
          MCollective::Client.expects(:new).returns(@client)
          Config.instance.stubs(:direct_addressing).returns(true)
        end

        it "should require direct addressing" do
          Config.instance.stubs(:direct_addressing).returns(false)
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          expect {
            client.send(:call_agent_batched, "foo", {}, {}, 1, 1)
          }.to raise_error("Batched requests requires direct addressing")
        end

        it "should require that all results be processed" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          expect {
            client.send(:call_agent_batched, "foo", {:process_results => false}, {}, 1, 1)
          }.to raise_error("Cannot bypass result processing for batched requests")
        end

        it "should only accept integer batch sizes" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          expect {
            client.send(:call_agent_batched, "foo", {}, {}, "foo", 1)
          }.to raise_error(/invalid value for Integer/)
        end

        it "should only accept float sleep times" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          expect {
            client.send(:call_agent_batched, "foo", {}, {}, 1, "foo")
          }.to raise_error(/invalid value for Float/)
        end

        it "should batch hosts in the correct size" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :stderr => StringIO.new}})

          client.expects(:new_request).returns("req")

          discovered = mock
          discovered.stubs(:size).returns(1)
          discovered.expects(:in_groups_of).with(10).raises("spec pass")

          client.instance_variable_set("@client", @coreclient)
          @coreclient.stubs(:discover).returns(discovered)
          @coreclient.stubs(:timeout_for_compound_filter).returns(0)

          expect { client.send(:call_agent_batched, "foo", {}, {}, 10, 1) }.to raise_error("spec pass")
        end

        it "should force direct requests" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :stderr => StringIO.new}})

          Message.expects(:new).with('req', nil, {:type => :direct_request, :agent => 'foo', :filter => nil, :options => {}, :collective => 'mcollective'}).raises("spec pass")
          client.expects(:new_request).returns("req")

          client.instance_variable_set("@client", @coreclient)
          @coreclient.stubs(:discover).returns(["test"])
          @coreclient.stubs(:timeout_for_compound_filter).returns(0)

          expect { client.send(:call_agent_batched, "foo", {}, {}, 1, 1) }.to raise_error("spec pass")
        end

        it "should process blocks correctly" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :stderr => StringIO.new}})

          msg = mock
          msg.expects(:discovered_hosts=).times(10)

          stats = {:noresponsefrom => [], :responses => 0, :blocktime => 0, :totaltime => 0, :discoverytime => 0}

          Message.expects(:new).with('req', nil, {:type => :direct_request, :agent => 'foo', :filter => nil, :options => {}, :collective => 'mcollective'}).returns(msg).times(10)
          client.expects(:new_request).returns("req")
          client.expects(:sleep).with(1.0).times(9)

          client.instance_variable_set("@client", @coreclient)
          @coreclient.stubs(:discover).returns([1,2,3,4,5,6,7,8,9,0])
          @coreclient.expects(:req).with(msg).yields("result").times(10)
          @coreclient.stubs(:stats).returns stats
          @coreclient.stubs(:timeout_for_compound_filter).returns(0)

          client.expects(:process_results_with_block).with("foo", "result", instance_of(Proc)).times(10)

          result = client.send(:call_agent_batched, "foo", {}, {}, 1, 1) { }
          result.class.should == Stats
        end

        it "should return an array of results in array mode" do
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :stderr => StringIO.new}})
          client.instance_variable_set("@client", @coreclient)

          msg = mock
          msg.expects(:discovered_hosts=).times(10)

          stats = {:noresponsefrom => [], :responses => 0, :blocktime => 0, :totaltime => 0, :discoverytime => 0}

          Progress.expects(:new).never

          Message.expects(:new).with('req', nil, {:type => :direct_request, :agent => 'foo', :filter => nil, :options => {}, :collective => 'mcollective'}).returns(msg).times(10)
          client.expects(:new_request).returns("req")
          client.expects(:sleep).with(1.0).times(9)

          @coreclient.stubs(:discover).returns([1,2,3,4,5,6,7,8,9,0])
          @coreclient.expects(:req).with(msg).yields("result").times(10)
          @coreclient.stubs(:stats).returns stats
          @coreclient.stubs(:timeout_for_compound_filter).returns(0)

          client.expects(:process_results_without_block).with("result", "foo").returns("rspec").times(10)

          client.send(:call_agent_batched, "foo", {}, {}, 1, 1).should == ["rspec", "rspec", "rspec", "rspec", "rspec", "rspec", "rspec", "rspec", "rspec", "rspec"]
        end
      end

      describe "#batch_sleep_time=" do
        it "should correctly set the sleep" do
          Config.instance.stubs(:direct_addressing).returns(true)

          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.batch_sleep_time = 5
          client.batch_sleep_time.should == 5
        end

        it "should only allow batch sleep to be set for direct addressing capable clients" do
          Config.instance.stubs(:direct_addressing).returns(false)
          Config.instance.stubs(:loadconfig).with("/nonexisting").returns(true)
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          expect { client.batch_sleep_time = 5 }.to raise_error("Can only set batch sleep time if direct addressing is supported")
        end
      end

      describe "#batch_size=" do
        it "should correctly set the size" do
          Config.instance.stubs(:direct_addressing).returns(true)

          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.batch_mode.should == false
          client.batch_size = 5
          client.batch_size.should == 5
          client.batch_mode.should == true
        end

        it "should only allow batch size to be set for direct addressing capable clients" do
          Config.instance.stubs(:loadconfig).with("/nonexisting").returns(true)
          Config.instance.stubs(:direct_addressing).returns(false)
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          expect { client.batch_size = 5 }.to raise_error("Can only set batch size if direct addressing is supported")
        end

        it "should support disabling batch mode when supplied a batch size of 0" do
          Config.instance.stubs(:direct_addressing).returns(true)

          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.batch_size = 5
          client.batch_mode.should == true
          client.batch_size = 0
          client.batch_mode.should == false
        end
      end

      describe "#discover" do
        it "should not accept invalid flags" do
          Config.instance.stubs(:direct_addressing).returns(true)
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})

          expect { client.discover(:rspec => :rspec) }.to raise_error("Unknown option rspec passed to discover")
        end

        it "should reset when :json, :hosts or :nodes are provided" do
          Config.instance.stubs(:direct_addressing).returns(true)
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.expects(:reset).times(3)
          client.discover(:hosts => ["one"])
          client.discover(:nodes => ["one"])
          client.discover(:json => ["one"])
        end

        it "should only allow discovery data in direct addressing mode" do
          Config.instance.stubs(:direct_addressing).returns(false)
          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.expects(:reset).once

          expect {
            client.discover(:nodes => ["one"])
          }.to raise_error("Can only supply discovery data if direct_addressing is enabled")
        end

        it "should parse :nodes and :hosts and force direct requests" do
          Config.instance.stubs(:direct_addressing).returns(true)
          Helpers.expects(:extract_hosts_from_array).with(["one"]).returns(["one"]).twice

          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.discover(:nodes => ["one"]).should == ["one"]
          client.discover(:hosts => ["one"]).should == ["one"]
          client.instance_variable_get("@force_direct_request").should == true
          client.instance_variable_get("@discovered_agents").should == ["one"]
        end

        it "should parse :json and force direct requests" do
          Config.instance.stubs(:direct_addressing).returns(true)
          Helpers.expects(:extract_hosts_from_json).with('["one"]').returns(["one"]).once

          client = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting"}})
          client.discover(:json => '["one"]').should == ["one"]
          client.instance_variable_get("@force_direct_request").should == true
          client.instance_variable_get("@discovered_agents").should == ["one"]
        end

        it "should force direct mode for non regex identity filters" do
          Config.instance.stubs(:direct_addressing).returns(true)

          client = Client.new("foo", {:options => {:filter => {"identity" => ["foo"], "agent" => []}, :config => "/nonexisting"}})
          client.discover
          client.instance_variable_get("@discovered_agents").should == ["foo"]
          client.instance_variable_get("@force_direct_request").should == true
        end

        it "should not set direct mode if its disabled" do
          Config.instance.stubs(:direct_addressing).returns(false)

          client = Client.new("foo", {:options => {:filter => {"identity" => ["foo"], "agent" => []}, :config => "/nonexisting"}})

          client.discover
          client.instance_variable_get("@force_direct_request").should == false
          client.instance_variable_get("@discovered_agents").should == ["foo"]
        end

        it "should not set direct mode for regex identities" do
          Config.instance.stubs(:direct_addressing).returns(false)

          rpcclient = Client.new("foo", {:options => {:filter => {"identity" => ["/foo/"], "agent" => []}, :config => "/nonexisting"}})

          rpcclient.client.expects(:discover).with({'identity' => ['/foo/'], 'agent' => ['foo']}, 2).once.returns(["foo"])

          rpcclient.discover
          rpcclient.instance_variable_get("@force_direct_request").should == false
          rpcclient.instance_variable_get("@discovered_agents").should == ["foo"]
        end

        it "should print status to stderr if in verbose mode" do
          @stderr.expects(:print).with("Discovering hosts using the mc method for 2 second(s) .... ")
          @stderr.expects(:puts).with(1)

          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => true, :disctimeout => 2, :stderr => @stderr, :stdout => @stdout}})

          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2).returns(["foo"])

          rpcclient.discover
        end

        it "should not print status to stderr if in nonverbose mode" do
          @stderr.expects(:print).never
          @stderr.expects(:puts).never

          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => false, :disctimeout => 2, :stderr => @stderr, :stdout => @stdout}})
          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2).returns(["foo"])

          rpcclient.discover
        end

        it "should record the start and end times" do
          Stats.any_instance.expects(:time_discovery).with(:start)
          Stats.any_instance.expects(:time_discovery).with(:end)

          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => false, :disctimeout => 2}})
          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2).returns(["foo"])

          rpcclient.discover
        end

        it "should discover using limits in :first rpclimit mode given a number" do
          Config.instance.stubs(:rpclimitmethod).returns(:first)
          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => false, :disctimeout => 2}})
          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2, 1).returns(["foo"])

          rpcclient.limit_targets = 1

          rpcclient.discover
        end

        it "should not discover using limits in :first rpclimit mode given a string" do
          Config.instance.stubs(:rpclimitmethod).returns(:first)
          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => false, :disctimeout => 2}})
          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2).returns(["foo"])
          rpcclient.limit_targets = "10%"

          rpcclient.discover
        end

        it "should not discover using limits when not in :first mode" do
          Config.instance.stubs(:rpclimitmethod).returns(:random)

          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => false, :disctimeout => 2}})
          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2).returns(["foo"])

          rpcclient.limit_targets = 1
          rpcclient.discover
        end

        it "should ensure force_direct mode is false when doing traditional discovery" do
          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => false, :disctimeout => 2}})
          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2).returns(["foo"])

          rpcclient.instance_variable_set("@force_direct_request", true)
          rpcclient.discover
          rpcclient.instance_variable_get("@force_direct_request").should == false
        end

        it "should store discovered nodes in stats" do
          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => false, :disctimeout => 2}})
          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2).returns(["foo"])

          rpcclient.discover
          rpcclient.stats.discovered_nodes.should == ["foo"]
        end

        it "should save discovered nodes in RPC" do
          rpcclient = Client.new("foo", {:options => {:filter => Util.empty_filter, :config => "/nonexisting", :verbose => false, :disctimeout => 2}})
          rpcclient.client.expects(:discover).with({'identity' => [], 'compound' => [], 'fact' => [], 'agent' => ['foo'], 'cf_class' => []}, 2).returns(["foo"])

          RPC.expects(:discovered).with(["foo"]).once
          rpcclient.discover
        end
      end
    end
  end
end
