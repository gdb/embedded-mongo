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

    def find(selector, opts)
      limit = opts.delete(:limit)
      raise ArgumentError.new("Unrecognized opts: #{opts.inspect}") unless opts.empty?

      results = []
      @data.each do |doc|
        if selector_match?(selector, doc)
          results << doc
          break if limit > 0 and results.length >= limit
        end
      end
      EmbeddedMongo.log.info("Query has #{results.length} matches")
      results
    end

    def update(selector, update, opts)
      # TODO: return value?
      multi = opts.delete(:multi)
      upsert = opts.delete(:upsert)
      safe = opts.delete(:safe) # TODO: do something with this
      raise ArgumentError.new("Unrecognized opts: #{opts.inspect}") unless opts.empty?

      n = 0
      @data.each do |doc|
        next unless selector_match?(selector, doc)
        apply_update!(update, doc)
        n += 1
        break unless multi
      end

      if n == 0 and upsert
        insert(update)
        @db.set_last_error({ 'updatedExisting' => false, 'upserted' => update['_id'], 'n' => 1 })
      else
        @db.set_last_error({ 'updatedExisting' => n > 0, 'n' => n })
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
      raise NotImplementedError.new('Does not current support $where queries') if selector.has_key?('$where')
      selector.all? { |k, v| partial_match?(v, doc[k]) }
    end

    def partial_match?(partial_selector, value)
      EmbeddedMongo.log.debug("partial_match? #{partial_selector.inspect} #{value.inspect}")
      case partial_selector
      when Array, Numeric, String, BSON::ObjectId, Boolean, nil
        partial_selector == value
      when Hash
        if partial_selector.all? { |k, v| !k.start_with?('$') }
          partial_selector == value
        else
          raise NotImplementedError.new("Cannot mix $ directives with non: #{partial_selector.inspect}") if partial_selector.any? { |k, v| !k.start_with?('$') }
          partial_selector.all? do |k, v|
            directive_match?(k, v, value)
         end
        end
      else
        raise "Unsupported selector #{partial_selector.inspect}"
      end
    end

    def directive_match?(directive_key, directive_value, value)
      case directive_key
      when '$lt'
        raise NotImplementedError.new("Only implemented for numeric directive values: #{directive_value.inspect}") unless directive_value.kind_of?(Numeric)
        value and value < directive_value
      when '$gt'
        raise NotImplementedError.new("Only implemented for numeric directive values: #{directive_value.inspect}") unless directive_value.kind_of?(Numeric)
        value and value > directive_value
      when '$gte'
        raise NotImplementedError.new("Only implemented for numeric directive values: #{directive_value.inspect}") unless directive_value.kind_of?(Numeric)
        value and value >= directive_value
      when '$lte'
        raise NotImplementedError.new("Only implemented for numeric directive values: #{directive_value.inspect}") unless directive_value.kind_of?(Numeric)
        value and value <= directive_value
      when '$in'
        raise NotImplementedError.new("Only implemented for arrays: #{directive_value.inspect}") unless directive_value.kind_of?(Array)
        directive_value.include?(value)
      when '$ne'
        directive_value != value
      else
        raise NotImplementedError.new("Have yet to implement: #{directive_key}")
        # raise Mongo::OperationFailure.new("invalid operator: #{directive_key}")
      end
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
