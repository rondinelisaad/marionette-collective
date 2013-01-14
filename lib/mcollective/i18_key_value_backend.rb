module MCollective
    class I18n_KeyValue_Backend
      module Implementation
        attr_accessor :store

        include I18n::Backend::Base, I18n::Backend::Flatten

        def initialize(store)
          @store = store
        end

        def store_translations(locale, data, options = {})
          escape = options.fetch(:escape, true)
          flatten_translations(locale, data, escape, false).each do |key, value|
            key = "#{locale}.#{key}"

            @store[key] = value
          end
        end

        def available_locales
          locales = @store.keys.map { |k| k =~ /\./; $` }
          locales.uniq!
          locales.compact!
          locales.map! { |k| k.to_sym }
          locales
        end

      protected

        def lookup(locale, key, scope = [], options = {})
          key   = normalize_flat_keys(locale, key, scope, options[:separator])
          value = @store["#{locale}.#{key}"]
          value.is_a?(Hash) ? value.deep_symbolize_keys : value
        end
      end

      include Implementation
    end
end
