module EmbeddedMongo
  class Connection < Mongo::Connection
    # mock methods
    def request(method, *args)
      @backend.send(method, *args)
    end

    def connect
      puts "Connecting to #{@host_to_try.inspect}"
      @backend = Backend.connect_backend(@host_to_try)
    end

    def send_message(operation, message, log_message=nil)
      puts "Calling send_message with: #{operation.inspect}, #{message.inspect}, #{log_message.inspect}"
      raise "send_message"
    end

    def receive_message(operation, message, log_message=nil, socket=nil, command=false)
      puts "Calling receive_message with: #{operation.inspect}, #{message.inspect}, #{log_message.inspect}, #{command.inspect}"
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
