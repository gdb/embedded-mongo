module EmbeddedMongo::Backend
  class DB
    def initialize(name)
      @name = name
      @collections = {}
    end

    def get_collection(collection_name)
      @collections[collection_name] ||= Collection.new(collection_name)
    end
  end
end
