module EmbeddedMongo
  class DB < Mongo::DB
    def command(selector, opts={})
      puts "COMMAND: #{selector.inspect}, opts: #{opts.inspect}"
    end

    # verbatim
    def collection(name, opts={})
      if strict? && !collection_names.include?(name)
        raise Mongo::MongoDBError, "Collection #{name} doesn't exist. Currently in strict mode."
      else
        opts[:safe] = opts.fetch(:safe, @safe)
        opts.merge!(:pk => @pk_factory) unless opts[:pk]
        Collection.new(name, self, opts)
      end
    end
    alias_method :[], :collection

    # verbatim
    def collections
      collection_names.map do |name|
        Collection.new(name, self)
      end
    end
  end
end
