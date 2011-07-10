# TODO: deep clone upon insert

module EmbeddedMongo::Backend
  class Collection
    class DuplicateKeyError < StandardError; end

    def initialize(db, name)
      raise ArgumentError.new("Invalid collection name #{name.inspect}") if name['.'] or name['$']
      @db = db
      @name = name
      @data = []
    end

    def insert_documents(documents)
      documents.each { |doc| insert(doc) }
      documents.map { |doc| doc['_id'] }
    end

    def find(selector)
      results = []
      @data.each do |doc|
        results << doc if selector_match?(selector, doc)
      end
      EmbeddedMongo.log.info("Query has #{results.length} matches")
      results
    end

    def update(selector, update, opts)

      @data.each do |doc|
        next unless selector_match?(selector, doc)
        apply_update!(update, doc)
        break unless opts[:multi]
      end
    end

    def remove(selector={}, opts={})
      @data.reject! { |doc| selector_match?(selector, doc) }
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
        EmbeddedMongo.log.info("Duplicate key error: #{id}")
        return
      end

      @data << doc
    end

    def selector_match?(selector, doc)
      selector.all? { |k, v| doc[k] == v }
    end

    def apply_update!(update, doc)
      EmbeddedMongo.log.info("Applying update: #{update.inspect} to #{doc.inspect}")
      id = doc['_id']
      doc.clear
      update.each do |k, v|
        doc[k] = v
      end
      doc['_id'] ||= id
    end
  end
end
