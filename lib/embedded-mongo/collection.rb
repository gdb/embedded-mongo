module EmbeddedMongo
  class Collection < Mongo::Collection
    def insert_documents(documents, collection_name=@name, check_keys=true, safe=false)
      # TODO: do something with check_keys / safe
      EmbeddedMongo.log.debug("insert_documents: #{documents.inspect}, #{collection_name.inspect}, #{check_keys.inspect}, #{safe.inspect}")
      @connection.request(:insert_documents, @db.name, collection_name, documents)
    end

    def update(selector, document, opts={})
      EmbeddedMongo.log.debug("update: #{selector.inspect}, #{document.inspect}, #{opts.inspect}")
      opts = { :safe => @safe }.merge(opts)
      @connection.request(:update, @db.name, @name, selector, document, opts)
    end

    def remove(selector={}, opts={})
      opts = { :safe => @safe }.merge(opts)
      @connection.request(:remove, @db.name, @name, selector, opts)
    end

    # verbatim
    def find(selector={}, opts={})
      fields = opts.delete(:fields)
      fields = ["_id"] if fields && fields.empty?
      skip   = opts.delete(:skip) || skip || 0
      limit  = opts.delete(:limit) || 0
      sort   = opts.delete(:sort)
      hint   = opts.delete(:hint)
      snapshot   = opts.delete(:snapshot)
      batch_size = opts.delete(:batch_size)
      timeout    = (opts.delete(:timeout) == false) ? false : true
      transformer = opts.delete(:transformer)

      if timeout == false && !block_given?
        raise ArgumentError, "Collection#find must be invoked with a block when timeout is disabled."
      end

      if hint
        hint = normalize_hint_fields(hint)
      else
        hint = @hint        # assumed to be normalized already
      end

      raise RuntimeError, "Unknown options [#{opts.inspect}]" unless opts.empty?

      cursor = Cursor.new(self, {
        :selector    => selector, 
        :fields      => fields, 
        :skip        => skip, 
        :limit       => limit,
        :order       => sort, 
        :hint        => hint, 
        :snapshot    => snapshot, 
        :timeout     => timeout, 
        :batch_size  => batch_size,
        :transformer => transformer,
      })

      if block_given?
        yield cursor
        cursor.close
        nil
      else
        cursor
      end
    end
  end
end
