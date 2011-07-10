module EmbeddedMongo
  module Util
    def self.stringify_keys!(hash)
      hash.keys.each do |key|
        hash[key.to_s] = hash.delete(key)
      end
      hash
    end
  end
end
