module EmbeddedMongo::Backend
  class DB
    attr_reader :collections, :name

    def initialize(manager, name)
      raise ArgumentError.new("Invalid collection name #{name.inspect}") if name['.'] or name['$']
      @manager = manager
      @name = name
      @collections = {}

      set_last_error({})
    end

    def run_command(cmd)
      if cmd['dropDatabase']
        @manager.drop_db(@name)
        [{
           'dropped' => @name,
           'ok' => 1.0
         }]
      elsif collection_name = cmd['drop']
        @collections.delete(collection_name)
        [{
           'ok' => 1.0
         }]
      elsif collection_name = cmd['create']
        get_collection(collection_name)
        [{
           'ok' => 1.0
         }]
      elsif cmd['buildinfo']
        raise "Command #{cmd.inspect} only allowed for admin database" unless @name == 'admin'
        # {"ok"=>1, "debug"=>false, "bits"=>64, "versionArray"=>[2, 0, 4, 0], "version"=>"2.0.4", "maxBsonObjectSize"=>16777216, "sysInfo"=>"Linux yellow 2.6.24-29-server #1 SMP Tue Oct 11 15:57:27 UTC 2011 x86_64 BOOST_LIB_VERSION=1_46_1", "gitVersion"=>"nogitversion"}
        [{
           'version' => '2.0.4.0', # sure, this seems like a good version
           'gitVersion' => 'nogitversion',
           'sysInfo' => 'fake sysinfo',
           'bits' => 64,
           'debug' => false,
           'ok' => 1.0
         }]
      elsif cmd['getlasterror']
        [@last_error]
      elsif cmd.has_key?('$eval')
        raise NotImplementedError.new('Does not currently support $eval commands')
      else
        raise NotImplementedError.new("Unrecognized command #{cmd.inspect}")
      end
    end

    def set_last_error(opts)
      @last_error = {
        'err' => nil,
        'n' => 0,
        'ok' => 1.0
      }.merge(opts)
    end

    # TODO: don't think we always create a collection
    def get_collection(collection_name)
      @collections[collection_name] ||= Collection.new(self, collection_name)
    end
  end
end
