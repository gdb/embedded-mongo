module EmbeddedMongo
  class Cursor < Mongo::Cursor
    def refresh
      send_initial_query
    end

    def send_initial_query
      if @query_run
        false
      else
        results = @connection.request(:find, @db.name, @collection.name, selector, :limit => @limit)
        @returned += results.length
        @cache += results
        @query_run = true
      end
    end

    def count(skip_and_limit=false)
      raise NotImplementedError.new if skip_and_limit
      send_initial_query
      @cache.length
    end
  end
end
