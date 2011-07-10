module EmbeddedMongo::Backend
  class Manager
    def initialize(spec)
      @spec = spec
      @dbs = {}
    end

    def insert_documents(db_name, collection_name, documents)
      documents.each { |doc| EmbeddedMongo::Util.stringify_keys!(doc) }
      EmbeddedMongo.log.info("INSERT: #{db_name.inspect} #{collection_name.inspect} #{documents.inspect}")
      collection = get_collection(db_name, collection_name)
      collection.insert_documents(documents)
    end

    def find(db_name, collection_name, selector)
      EmbeddedMongo::Util.stringify_keys!(selector)
      EmbeddedMongo.log.info("FIND: #{db_name.inspect} #{collection_name.inspect} #{selector.inspect}")
      if collection_name == '$cmd'
        db = get_db(db_name)
        return db.run_command(selector)
      end
      collection = get_collection(db_name, collection_name)
      collection.find(selector)
    end

    def update(db_name, collection_name, selector, update, opts)
      EmbeddedMongo::Util.stringify_keys!(selector)
      EmbeddedMongo::Util.stringify_keys!(update)
      EmbeddedMongo.log.info("FIND: #{db_name.inspect} #{collection_name.inspect} #{selector.inspect} #{update.inspect} #{opts.inspect}")
      collection = get_collection(db_name, collection_name)
      collection.update(selector, update, opts)
    end

    def remove(db_name, collection_name, selector, opts)
      EmbeddedMongo::Util.stringify_keys!(selector)
      EmbeddedMongo.log.info("REMOVE: #{db_name.inspect} #{collection_name.inspect} #{selector.inspect} #{opts.inspect}")
      collection = get_collection(db_name, collection_name)
      collection.remove(selector, opts)
    end

    # Management functions
    def drop_db(db_name)
      @dbs.delete(db_name)
    end

    private

    def get_db(db_name)
      @dbs[db_name] ||= DB.new(self, db_name)
    end

    def get_collection(db_name, collection_name)
      db = get_db(db_name)
      collection = db.get_collection(collection_name)
      collection
    end
  end
end
