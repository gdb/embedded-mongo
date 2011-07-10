module EmbeddedMongo
  module Util
    # TODO: make this recursive / stringify values
    def self.stringify_hash!(hash)
      raise ArgumentError.new("Argument is not a hash: #{hash.inspect}") unless hash.kind_of?(Hash)
      stringify!(hash)
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
      when String, Integer, BSON::ObjectId, nil
        struct
      else
        raise "Cannot stringify #{struct.inspect}"
      end
    end
  end
end
