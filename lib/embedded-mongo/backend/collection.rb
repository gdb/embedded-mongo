# TODO: make hash/symbol agnostic

module EmbeddedMongo::Backend
  class Collection
    class DuplicateKeyError < StandardError; end

    def initialize(name)
      @name = name
      @data = []
    end

    def insert_documents(documents)
      documents.each { |doc| insert(doc) }
      documents.map { |doc| doc[:_id] }
    end

    def query(query)
      results = []
      @data.each do |doc|
        results << doc if query_match?(query, doc)
      end
      EmbeddedMongo.log.info("Query has #{results.length} matches")
      results
    end

    private

    def check_id(doc)
      id = doc[:_id]
      raise unless id
    end

    def check_duplicate_key(doc)
      raise DuplicateKeyError if @data.any? { |other| doc[:_id] == other[:_id] }
    end

    def insert(doc)
      begin
        check_id(doc)
        check_duplicate_key(doc)
      rescue DuplicateKeyError
        $stderr.puts "Duplicate key error: #{id}"
        return
      end

      @data << doc
    end

    def query_match?(query, doc)
      query.all? { |k, v| doc[k] == v }
    end
  end
end
