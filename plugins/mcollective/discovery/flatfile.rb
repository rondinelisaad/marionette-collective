# discovers against a flatfile instead of the traditional network discovery
#
#  - the path to the file is hardcoded now
#  - as its a file of identities, only identity filters are supported
#
# woot.
module MCollective
  class Discovery
    class Flatfile
      def self.discover(filter, timeout, limit=0, client=nil)
        unless client.options[:discovery_options].empty?
          file = client.options[:discovery_options].first
        else
          raise "The flatfile discovery method needs a path to a text file"
        end

        discovered = []

        hosts = File.readlines(file).map{|l| l.chomp}

        # this plugin only supports identity filters, do regex matches etc against
        # the list found in the flatfile
        unless filter["identity"].empty?
          filter["identity"].each do |identity|
            identity = Regexp.new(identity.gsub("\/", "")) if identity.match("^/")

            if identity.is_a?(Regexp)
              discovered = hosts.grep(identity)
            elsif hosts.include?(identity)
              discovered << identity
            end
          end
        else
          discovered = hosts
        end

        if limit > 0
          return discovered.shuffle[0,limit]
        else
          return discovered.sort
        end
      end
    end
  end
end
