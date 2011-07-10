module EmbeddedMongo::Backend
  class Manager
    def initialize(spec)
      @spec = spec
      @dbs = {}
    end

    def insert_documents(db_name, collection_name, documents)
      EmbeddedMongo.log.info("INSERT: #{db_name.inspect} #{collection_name.inspect} #{documents.inspect}")
      collection = get_collection(db_name, collection_name)
      collection.insert_documents(documents)
    end

    def find(db_name, collection_name, selector)
      EmbeddedMongo.log.info("FIND: #{db_name.inspect} #{collection_name.inspect} #{selector.inspect}")
      collection = get_collection(db_name, collection_name)
      collection.find(selector)
    end

    def update(db_name, collection_name, selector, update, opts)
      EmbeddedMongo.log.info("FIND: #{db_name.inspect} #{collection_name.inspect} #{selector.inspect} #{update.inspect} #{opts.inspect}")
      collection = get_collection(db_name, collection_name)
      collection.update(selector, update, opts)
    end

    private

    def get_db(db_name)
      @dbs[db_name] ||= DB.new(db_name)
    end

    def get_collection(db_name, collection_name)
      db = get_db(db_name)
      collection = db.get_collection(collection_name)
      collection
    end
  end
end
