module EmbeddedMongo::Backend
  class DB
    def initialize(manager, name)
      raise ArgumentError.new("Invalid collection name #{name.inspect}") if name['.'] or name['$']
      @manager = manager
      @name = name
      @collections = {}
    end

    def run_command(cmd)
      if cmd['dropDatabase']
        @manager.drop_db($name)
        [{ 'dropped' => @name, 'ok' => 1.0 }]
      else
        raise NotImplementedError.new("Unrecognized command #{cmd.inspect}")
      end
    end

    def get_collection(collection_name)
      @collections[collection_name] ||= Collection.new(self, collection_name)
    end
  end
end
