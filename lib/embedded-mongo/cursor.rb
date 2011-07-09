module EmbeddedMongo
  class Cursor < Mongo::Cursor
    def refresh
      send_initial_query
    end

    def send_initial_query
      if @query_run
        false
      else
        results = @connection.request(:query, @db.name, @collection.name, selector)
        @returned += results.length
        @cache += results
        @query_run = true
      end
    end
  end
end
