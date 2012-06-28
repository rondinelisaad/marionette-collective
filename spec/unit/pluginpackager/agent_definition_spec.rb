#!/usr/bin/env rspec

require 'spec_helper'

module MCollective
  module PluginPackager
    describe AgentDefinition do
      before :each do
        PluginPackager.expects(:get_metadata).once.returns({:name => "foo"})
      end

      describe "#initialize" do
        it "should replace spaces in the package name with dashes" do
          agent = AgentDefinition.new(".", "test package", nil, nil, nil, nil, [], {}, "agent")
          agent.metadata[:name].should == "test-package"
        end

        it "should set dependencies if present" do
          agent = AgentDefinition.new(".", "test-package", nil, nil, nil, nil, ["foo"], {}, "agent")
          agent.dependencies.should == ["foo"]
        end

        it "should set mc server, client and common dependencies" do
          agent = AgentDefinition.new(".", "test-package", nil, nil, nil, nil, [], {:server => "pe-mcollective"}, "agent")
          agent.mcserver.should == "pe-mcollective"
          agent.mcclient.should == "mcollective-client"
          agent.mccommon.should == "mcollective-common"
        end
      end

      describe "#identify_packages" do
        it "should attempt to identify all agent packages" do
          AgentDefinition.any_instance.expects(:common).once.returns(:check)
          AgentDefinition.any_instance.expects(:agent).once.returns(:check)
          AgentDefinition.any_instance.expects(:client).once.returns(:check)

          agent = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          agent.packagedata[:common].should == :check
          agent.packagedata[:agent].should == :check
          agent.packagedata[:client].should == :check
        end
      end

      describe "#agent" do
        before do
          AgentDefinition.any_instance.expects(:client).once
        end

        it "should not populate the agent files if the agent directory is empty" do
          AgentDefinition.any_instance.expects(:common).returns(nil)
          PluginPackager.expects(:check_dir_present).returns(false)
          agent = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          agent.packagedata[:agent].should == nil
        end

        it "should populate the agent files if the agent directory is present and not empty" do
          AgentDefinition.any_instance.expects(:common).returns(nil)
          PluginPackager.expects(:check_dir_present).with("tmpdir/agent").returns(true)
          File.stubs(:join).returns("tmpdir")
          File.stubs(:join).with(".", "agent").returns("tmpdir/agent")
          File.stubs(:join).with(".", "aggregate").returns("tmpdir/aggregate")
          Dir.stubs(:glob).returns(["file.rb", "implementation.rb"])

          agent = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          agent.packagedata[:agent][:files].should == ["file.rb", "implementation.rb"]
        end

        it "should add common package as dependency if present" do
          AgentDefinition.any_instance.expects(:common).returns({:files=> ["test.rb"]})
          PluginPackager.expects(:check_dir_present).with("tmpdir/agent").returns(true)
          File.stubs(:join).returns("/tmp")
          File.stubs(:join).with(".", "agent").returns("tmpdir/agent")
          File.stubs(:join).with(".", "aggregate").returns("tmpdir/aggregate")
          Dir.stubs(:glob).returns([])

          agent = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          agent.packagedata[:agent][:dependencies].should == ["mcollective", "mcollective-foo-common"]
        end
      end

      describe "#common" do
        before do
          AgentDefinition.any_instance.expects(:agent)
          AgentDefinition.any_instance.expects(:client)
        end

        it "should not populate the commong files if the util directory is empty" do
          PluginPackager.expects(:check_dir_present).returns(false)
          common = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          common.packagedata[:common].should == nil
        end

        it "should populate the common files if the common directory is present and not empty" do
          PluginPackager.expects(:check_dir_present).returns(true)
          File.stubs(:join).returns("tmpdir")
          Dir.stubs(:glob).returns(["file.rb"])
          common = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          common.packagedata[:common][:files].should == ["file.rb"]
        end
      end

      describe "#client" do
        before do
          AgentDefinition.any_instance.expects(:agent).returns(nil)
          File.expects(:join).with(".", "application").returns("clientdir")
          File.expects(:join).with(".", "bin").returns("bindir")
          File.expects(:join).with(".", "agent").returns("agentdir")
          File.expects(:join).with(".", "aggregate").returns("aggregatedir")
        end

        it "should populate client files if all directories are present" do
          AgentDefinition.any_instance.expects(:common).returns(nil)
          PluginPackager.expects(:check_dir_present).times(4).returns(true)
          File.expects(:join).with("clientdir", "*").returns("clientdir/*")
          File.expects(:join).with("bindir", "*").returns("bindir/*")
          File.expects(:join).with("agentdir", "*.ddl").returns("agentdir/*.ddl")
          File.expects(:join).with("aggregatedir", "*").returns("aggregatedir/*")
          Dir.expects(:glob).with("clientdir/*").returns(["client.rb"])
          Dir.expects(:glob).with("bindir/*").returns(["bin.rb"])
          Dir.expects(:glob).with("agentdir/*.ddl").returns(["agent.ddl"])
          Dir.expects(:glob).with("aggregatedir/*").returns(["aggregate.rb"])

          client = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          client.packagedata[:client][:files].should == ["client.rb", "bin.rb", "agent.ddl", "aggregate.rb"]
        end

        it "should not populate client files if directories are not present" do
          AgentDefinition.any_instance.expects(:common).returns(nil)
          PluginPackager.expects(:check_dir_present).times(4).returns(false)

          client = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          client.packagedata[:client].should == nil
        end

        it "should add common package as dependency if present" do
          AgentDefinition.any_instance.expects(:common).returns("common")
          PluginPackager.expects(:check_dir_present).times(4).returns(true)
          File.expects(:join).with("clientdir", "*").returns("clientdir/*")
          File.expects(:join).with("bindir", "*").returns("bindir/*")
          File.expects(:join).with("agentdir", "*.ddl").returns("agentdir/*.ddl")
          File.expects(:join).with("aggregatedir", "*").returns("aggregatedir/*")
          Dir.expects(:glob).with("clientdir/*").returns(["client.rb"])
          Dir.expects(:glob).with("bindir/*").returns(["bin.rb"])
          Dir.expects(:glob).with("agentdir/*.ddl").returns(["agent.ddl"])
          Dir.expects(:glob).with("aggregatedir/*").returns(["aggregate.rb"])

          client = AgentDefinition.new(".", nil, nil, nil, nil, nil, [], {}, "agent")
          client.packagedata[:client][:dependencies].should == ["mcollective-client", "mcollective-foo-common"]
        end
      end
    end
  end
end
