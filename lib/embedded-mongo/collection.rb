module EmbeddedMongo
  class Collection < Mongo::Collection
    # In recent upgrades to the mongo libary (~1.12), the internals of the insert method
    # were changed so that it no longer calls the insert_documents below. This override
    # is replicating the behavior from mongo 1.8 so that existing unit tests work
    # after upgrading to a newer mongo framework.
    def insert(doc_or_docs, opts={})
      doc_or_docs = [doc_or_docs] unless doc_or_docs.is_a?(Array)
      doc_or_docs.collect! {|doc| BSON::ObjectId.create_pk(doc)}
      result = insert_documents(doc_or_docs, @name, true, false, opts)
      result.size > 1 ? result : result.first
    end

    def insert_documents(documents, collection_name=@name, check_keys=true, safe=false, opts={})
      # TODO: do something with check_keys / safe
      EmbeddedMongo.log.debug("insert_documents: #{documents.inspect}, #{collection_name.inspect}, #{check_keys.inspect}, #{safe.inspect}, #{opts.inspect}")
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
