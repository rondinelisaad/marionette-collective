module MCollective
  module Cache
    # protects access to @cache_locks and top level @cache
    @locks_mutex = Mutex.new

    # stores a mutex per named cache
    @cache_locks = {}

    # the named caches protected by items in @cache_locks
    @cache = {}

    def self.setup(cache_name, ttl=300)
      @locks_mutex.synchronize do
        break if @cache_locks.include?(cache_name)

        @cache_locks[cache_name] = Mutex.new

        @cache_locks[cache_name].synchronize do
          @cache[cache_name] = {:max_age => Float(ttl)}
        end
      end

      true
    end

    def self.has_cache?(cache_name)
      @locks_mutex.synchronize do
        @cache.include?(cache_name)
      end
    end

    def self.delete!(cache_name)
      @locks_mutex.synchronize do
        raise("No cache called '%s'" % cache_name) unless @cache_locks.include?(cache_name)

        @cache_locks.delete(cache_name)
        @cache.delete(cache_name)
      end

      true
    end

    def self.write(cache_name, key, value)
      raise("No cache called '%s'" % cache_name) unless @cache.include?(cache_name)

      @cache_locks[cache_name].synchronize do
        @cache[cache_name][key] ||= {}
        @cache[cache_name][key][:create_time] = Time.now
        @cache[cache_name][key][:value] = value
      end

      value
    end

    def self.read(cache_name, key)
      raise("No cache called '%s'" % cache_name) unless @cache.include?(cache_name)

      unless valid?(cache_name, key)
        Log.debug("Cache expired on '%s' key '%s'" % [cache_name, key])
        raise("Cache for item '%s' on cache '%s' has expired" % [key, cache_name])
      end

      Log.debug("Cache hit on '%s' key '%s'" % [cache_name, key])

      @cache_locks[cache_name].synchronize do
        @cache[cache_name][key][:value]
      end
    end

    def self.valid?(cache_name, key)
      raise("No cache called '%s'" % cache_name) unless @cache.include?(cache_name)

      @cache_locks[cache_name].synchronize do
        unless @cache[cache_name].include?(key)
          Log.debug("Cache miss on '%s' key '%s'" % [cache_name, key])
          raise("No item called '%s' for cache '%s'" % [key, cache_name])
        end

        (Time.now - @cache[cache_name][key][:create_time]) < @cache[cache_name][:max_age]
      end
    end

    def self.invalid?(cache_name, key)
      !valid?(cache_name, key)
    end

    def self.synchronize(cache_name)
      raise("No cache called '%s'" % cache_name) unless @cache.include?(cache_name)

      raise ("No block supplied to synchronize") unless block_given?

      @cache_locks[cache_name].synchronize do
        yield
      end
    end

    def self.invalidate!(cache_name, key)
      raise("No cache called '%s'" % cache_name) unless @cache.include?(cache_name)

      @cache_locks[cache_name].synchronize do
        return false unless @cache[cache_name].include?(key)

        @cache[cache_name].delete(key)
      end
    end
  end
end
