#! /usr/bin/env rspec

require 'spec_helper'

module MCollective
  module Matcher
    describe 'scanner' do
      it "should identify a '(' token" do
        scanner = Scanner.new("(")
        token = scanner.get_token
        token.should == ["(", "("]
      end

      it "should identify a ')' token" do
        scanner = Scanner.new(")")
        token = scanner.get_token
        token.should == [")", ")"]
      end

      it "should identify a 'and' token" do
        scanner = Scanner.new("and ")
        token = scanner.get_token
        token.should == ["and", "and"]
      end

      it "should identify a 'or' token" do
        scanner = Scanner.new("or ")
        token = scanner.get_token
        token.should == ["or", "or"]
      end

      it "should identify a 'not' token" do
        scanner = Scanner.new("not ")
        token = scanner.get_token
        token.should == ["not", "not"]
      end

      it "should identify a '!' token" do
        scanner = Scanner.new("!")
        token = scanner.get_token
        token.should == ["not", "not"]
      end

      it "should identify a fact statement token" do
        scanner = Scanner.new("foo=bar")
        token = scanner.get_token
        token.should == ["statement", "foo=bar"]
      end

      it "should identify a fact statement token" do
        scanner = Scanner.new("foo=bar")
        token = scanner.get_token
        token.should == ["statement", "foo=bar"]
      end

      it "should identify a class statement token" do
        scanner = Scanner.new("/class/")
        token = scanner.get_token
        token.should == ["statement", "/class/"]
      end

      it "should identify a function statement token with a dot value" do
        scanner = Scanner.new("foo('bar').baz")
        token = scanner.get_token
        token.should == ["fstatement", "foo('bar').baz"]
      end

      it "should identify a function statement token without a dot value" do
        scanner = Scanner.new("foo('bar')")
        token = scanner.get_token
        token.should == ["fstatement", "foo('bar')"]
      end

      it "should identify a function statement with multiple parameters" do
        scanner = Scanner.new("foo('bar','baz')")
        token = scanner.get_token
        token.should == ["fstatement", "foo('bar','baz')"]
      end

      it "should identify a bad token when a function is missing its end bracket" do
        scanner = Scanner.new("foo(")
        token = scanner.get_token
        token.should == ["bad_token", [0,3]]
      end

      it "should identify a bad token when there is a regex before a comparison operator" do
        scanner = Scanner.new("/foo/=bar")
        token = scanner.get_token
        token.should == ["bad_token", [0,8]]
      end

      it "should identify a bad token where there is a forward slash before a comparison operator" do
        scanner = Scanner.new("/foo=bar")
        token = scanner.get_token
        token.should == ["bad_token", [0,7]]
      end

      it "should identify a bad token where there is only one forward slash after a comparison operator" do
        scanner = Scanner.new("foo=/bar")
        token = scanner.get_token
        token.should == ["bad_token", [0,7]]
      end

      it "should identify a bad token where function parameters are not in single quotes" do
        scanner = Scanner.new("foo(bar)")
        token = scanner.get_token
        token.should == ["bad_token", [0,7]]
      end

      it "should identify a bad token where there are non alphanumerical or underscore chars in the dot value" do
        scanner = Scanner.new("foo('bar').val-ue")
        token = scanner.get_token
        token.should == ["bad_token", [0,16]]
      end

      it "should identify a bad token where there are chained dot values" do
        scanner = Scanner.new("foo('bar').a.b")
        token = scanner.get_token
        token.should == ["bad_token", [0,13]]
      end

      it "should identify bad tokens where function parameters are not comma seperated" do
        scanner = Scanner.new("foo('a' 'b')")
        token = scanner.get_token
        token.should == ["bad_token", [0,11]]
      end
    end
  end
end
