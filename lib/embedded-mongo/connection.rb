module EmbeddedMongo
  class Connection < Mongo::Connection
    # mock methods
    def request(method, *args)
      @backend.send(method, *args)
    end

    def connect
      EmbeddedMongo.log.debug "Connecting to #{@host_to_try.inspect}"
      @backend = Backend.connect_backend(@host_to_try)
    end

    def send_message(operation, message, log_message=nil)
      EmbeddedMongo.log.debug "Calling send_message with: #{operation.inspect}, #{message.inspect}, #{log_message.inspect}"
      raise "send_message"
    end

    def receive_message(operation, message, log_message=nil, socket=nil, command=false)
      EmbeddedMongo.log.debug "Calling receive_message with: #{operation.inspect}, #{message.inspect}, #{log_message.inspect}, #{command.inspect}"
      raise "receive_message"
    end

    # verbatim
    def db(db_name, opts={})
      DB.new(db_name, self, opts)
    end

    # verbatim
    def [](db_name)
      DB.new(db_name, self, :safe => @safe)
    end
  end
end
