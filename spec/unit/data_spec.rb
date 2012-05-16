#!/usr/bin/env rspec

require 'spec_helper'

module MCollective
  describe Data do
    describe "#load_data_sources" do
      it "should use the pluginmanager to load data sources" do
        PluginManager.expects(:find_and_load).with("data").returns([])
        Data.load_data_sources
      end

      it "should remove plugins that should not be active on this node" do
        PluginManager.expects(:find_and_load).with("data").returns(["rspec_data"])
        PluginManager.expects(:grep).returns(["rspec_data"])
        PluginManager.expects(:delete).with("rspec_data")

        ddl = mock
        ddl.stubs(:meta).returns({:timeout => 1})
        DDL.stubs(:new).returns(ddl)
        Data::Base.expects(:activate?).returns(false)
        PluginManager.expects("[]").with("rspec_data").returns(Data::Base.new)
        Data.load_data_sources
      end
    end

    describe "#[]" do
      it "should return the correct plugin" do
        PluginManager.expects("[]").with("rspec_data").times(4)
        Data["Rspec"]
        Data["rspec"]
        Data["rspec_data"]
        Data["rspec_Data"]
      end
    end

    describe "#method_missing" do
      it "should raise errors for unknown plugins" do
        PluginManager.expects("include?").with("rspec_data").returns(false)
        expect { Data.rspec_data }.to raise_error(NoMethodError)
      end

      it "should do a lookup on the right plugin" do
        rspec_data = mock
        rspec_data.expects(:lookup).returns("rspec")

        PluginManager.expects("include?").with("rspec_data").returns(true)
        PluginManager.expects("[]").with("rspec_data").returns(rspec_data)

        Data.rspec_data("rspec").should == "rspec"
      end
    end

    describe "#ddl_validate" do
      before do
        @ddl = mock
        @ddl.expects(:meta).returns({:name => "rspec test"})
      end

      it "should ensure the ddl has a dataquery" do
        @ddl.expects(:entities).returns({})
        expect { Data.ddl_validate(@ddl, "rspec") }.to raise_error("No dataquery has been defined in the DDL for data plugin rspec test")
      end

      it "should ensure the ddl has an input" do
        @ddl.expects(:entities).returns({:data => {:input => {}, :output => {}}})
        expect { Data.ddl_validate(@ddl, "rspec") }.to raise_error("No :query input has been defined in the DDL for data plugin rspec test")
      end

      it "should ensure the ddl has output" do
        @ddl.expects(:entities).returns({:data => {:input => {:query => {}}, :output => {}}})
        expect { Data.ddl_validate(@ddl, "rspec") }.to raise_error("No output has been defined in the DDL for data plugin rspec test")
      end

      it "should validate the argument" do
        @ddl.expects(:entities).returns({:data => {:input => {:query => {}}, :output => {:test => {}}}})
        @ddl.expects(:validate_input_argument).returns("rspec validated")
        Data.ddl_validate(@ddl, "rspec").should == "rspec validated"
      end
    end
  end
end
