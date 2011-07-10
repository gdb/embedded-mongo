module EmbeddedMongo::Backend
  class Collection
    class DuplicateKeyError < StandardError; end

    def initialize(name)
      @name = name
      @data = []
    end

    def insert_documents(documents)
      documents.each { |doc| EmbeddedMongo::Util.stringify_keys!(doc) }

      documents.each { |doc| insert(doc) }
      documents.map { |doc| doc['_id'] }
    end

    def find(selector)
      EmbeddedMongo::Util.stringify_keys!(selector)

      results = []
      @data.each do |doc|
        results << doc if selector_match?(selector, doc)
      end
      EmbeddedMongo.log.info("Query has #{results.length} matches")
      results
    end

    private

    def check_id(doc)
      id = doc['_id']
      raise NotImplementedError.new("#{doc.inspect} has no '_id' attribute") unless id
    end

    def check_duplicate_key(doc)
      raise DuplicateKeyError if @data.any? { |other| doc['_id'] == other['_id'] }
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

    def selector_match?(selector, doc)
      selector.all? { |k, v| doc[k] == v }
    end
  end
end
