#!/usr/bin/env rspec

require 'spec_helper'

module MCollective
  describe Cache do
    before do
      Cache.instance_variable_set("@locks_mutex", Mutex.new)
      Cache.instance_variable_set("@cache_locks", {})
      Cache.instance_variable_set("@cache", {})
    end

    describe "#setup" do
      it "should correctly setup a new cache" do
        Cache.setup("rspec1", 300)
        Cache.instance_variable_get("@cache")["rspec1"].should == {:max_age => 300.0}

        Cache.setup("rspec2")
        Cache.instance_variable_get("@cache")["rspec2"].should == {:max_age => 300.0}
      end
    end

    describe "#delete!" do
      it "should delete the cache and return true" do
        Cache.setup("rspec")
        Cache.delete!("rspec").should == true
      end
    end

    describe "#write" do
      it "should detect unknown caches" do
        expect { Cache.write("rspec", :key, :val) }.to raise_error("No cache called 'rspec'")
      end

      it "should write to the cache" do
        time = Time.now
        Time.expects(:now).returns(time)

        Cache.setup("rspec")
        Cache.write("rspec", :key, :val).should == :val

        cached = Cache.instance_variable_get("@cache")["rspec"]
        cached[:key][:value].should == :val
        cached[:key][:create_time].should == time
      end
    end

    describe "#read" do
      it "should read a written entry correctly" do
        Cache.setup("rspec")
        Cache.write("rspec", :key, :val)
        Cache.read("rspec", :key).should == :val
      end

      it "should raise on expired reads" do
        Cache.setup("rspec")
        Cache.write("rspec", :key, :val)

        Cache.expects(:valid?).returns(false)

        expect { Cache.read("rspec", :key) }.to raise_error(/has expired/)
      end
    end

    describe "#valid?" do
      it "should return true for valid" do
        Cache.setup("rspec", 300)
        Cache.write("rspec", :key, :val)
        Cache.valid?("rspec", :key).should == true
      end

      it "should return false for invalid" do
        Cache.setup("rspec", 300)
        Cache.write("rspec", :key, :val)

        time = Time.now + 600
        Time.expects(:now).returns(time)

        Cache.valid?("rspec", :key).should == false
      end
    end

    describe "#invalid?" do
      it "should invert valid?" do
        Cache.setup("rspec", 300)
        Cache.write("rspec", :key, :val)
        Cache.expects(:valid?).returns(true)
        Cache.invalid?("rspec", :key).should == false
      end
    end

    describe "#synchronize" do
      it "should use the correct mutex" do
        Cache.setup("rspec")

        locks = Cache.instance_variable_get("@cache_locks")
        rspec_lock = locks["rspec"]
        rspec_lock.expects(:synchronize).yields

        locks.expects("[]").with("rspec").returns(rspec_lock)

        ran = 0
        Cache.synchronize("rspec") do
          ran = 1
        end

        ran.should == 1
      end
    end
  end
end
