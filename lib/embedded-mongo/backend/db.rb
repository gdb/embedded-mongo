module EmbeddedMongo::Backend
  class DB
    def initialize(manager, name)
      raise ArgumentError.new("Invalid collection name #{name.inspect}") if name['.'] or name['$']
      @manager = manager
      @name = name
      @collections = {}

      @last_error = nil
      @n = 0
    end

    def run_command(cmd)
      if cmd['dropDatabase']
        @manager.drop_db($name)
        [{
           'dropped' => @name,
           'ok' => 1.0
         }]
      elsif cmd['buildinfo']
        raise "Command #{cmd.inspect} only allowed for admin database" unless @name == 'admin'
        # {"version"=>"1.6.3", "gitVersion"=>"nogitversion", "sysInfo"=>"Linux allspice 2.6.24-28-server #1 SMP Wed Aug 18 21:17:51 UTC 2010 x86_64 BOOST_LIB_VERSION=1_42", "bits"=>64, "debug"=>false, "ok"=>1.0}
        [{
           'version' => '0.0.1',
           'gitVersion' => 'nogitversion',
           'sysInfo' => 'fake sysinfo',
           'bits' => 64,
           'debug' => false,
           'ok' => 1.0
         }]
      elsif cmd['getlasterror']
        # TODO: populate @last_error / @n
        [{
           'err' => @last_error,
           'n' => @n,
           'ok' => 1.0
         }]
      elsif cmd.has_key?('$eval')
        raise NotImplementedError.new('Does not currently support $eval commands')
      else
        raise NotImplementedError.new("Unrecognized command #{cmd.inspect}")
      end
    end

    def get_collection(collection_name)
      @collections[collection_name] ||= Collection.new(self, collection_name)
    end
  end
end
