module EmbeddedMongo
  module Util
    def self.stringify_hash!(hash)
      raise ArgumentError.new("Argument is not a hash: #{hash.inspect}") unless hash.kind_of?(Hash)
      stringify!(hash)
    end

    def self.deep_clone(obj)
      # TODO: come up with something less hackish
      Marshal.load(Marshal.dump(obj))
    end

    private

    def self.stringify!(struct)
      case struct
      when Hash
        struct.keys.each do |key|
          struct[key.to_s] = stringify!(struct.delete(key))
        end
        struct
      when Array
        struct.each_with_index { |entry, i| struct[i] = stringify!(entry) }
      when Symbol
        struct.to_s
      when String, Numeric, BSON::ObjectId, Regexp, TrueClass, FalseClass, nil
        struct
      else
        raise "Cannot stringify #{struct.inspect}"
      end
    end
  end
end
