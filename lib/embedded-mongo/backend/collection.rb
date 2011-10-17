module EmbeddedMongo::Backend
  class Collection
    class DuplicateKeyError < StandardError; end

    def initialize(db, name)
      # TODO: system.namespaces
      @db = db
      @name = name
      @data = []

      if name == 'system.namespaces'
        # mark as system?
      elsif name['.'] or name['$']
        raise ArgumentError.new("Invalid collection name #{name.inspect}")
      end
    end

    def insert_documents(documents)
      documents.each { |doc| insert(EmbeddedMongo::Util.deep_clone(doc)) }
      documents.map { |doc| doc['_id'] }
    end

    def find(selector, opts)
      limit = opts.delete(:limit)
      sort = opts.delete(:sort)
      raise ArgumentError.new("Unrecognized opts: #{opts.inspect}") unless opts.empty?

      results = []
      data.each do |doc|
        if selector_match?(selector, doc)
          results << doc
          break if limit > 0 and results.length >= limit
        end
      end

      EmbeddedMongo.log.debug("Query has #{results.length} matches")
      if sort
        case sort
        when String
          sort = [[sort]]
        when Array
          sort = [sort] unless sort.first.kind_of?(Array)
        else
          raise Mongo::InvalidSortValueError.new("invalid sort type: #{sort.inspect}")
        end
        results.sort! { |x, y| sort_cmp(sort, x, y) }
      end
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
        selector = EmbeddedMongo::Util.deep_clone(selector)
        apply_update!(update, selector)
        insert(selector)
        @db.set_last_error({ 'updatedExisting' => false, 'upserted' => update['_id'], 'n' => 1 })
      else
        @db.set_last_error({ 'updatedExisting' => n > 0, 'n' => n })
      end
    end

    def remove(selector={}, opts={})
      @data.reject! { |doc| selector_match?(selector, doc) }
    end

    private

    def data
      if @name == 'system.namespaces'
        @db.collections.keys.map { |name| { 'name' => "#{@db.name}.#{name}" } }
      else
        @data
      end
    end

    def check_id(doc)
      id = doc['_id']
      raise NotImplementedError.new("#{doc.inspect} has no '_id' attribute") unless id
    end

    def check_duplicate_key(doc)
      raise DuplicateKeyError if @data.any? { |other| doc['_id'] == other['_id'] }
    end

    # Make sure to clone at call sites
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

    def sort_cmp(sort, x, y)
      sort.each do |spec|
        case spec
        when String
          field = spec
          direction = nil
        when Array
          field, direction = spec
        else
          raise Mongo::InvalidSortValueError.new("invalid sort directive: #{spec.inspect}")
        end

        x_val = x[field.to_s]
        y_val = y[field.to_s]
        if direction.to_s == 'ascending' or direction.to_s == 'asc' or (direction.kind_of?(Numeric) and direction > 0) or direction.nil?
          if x_val.kind_of?(Numeric) and y_val.kind_of?(Numeric)
            cmp = x_val <=> y_val
          elsif x_val.kind_of?(Numeric)
            cmp = 1
          elsif y_val.kind_of?(Numeric)
            cmp = -1
          else
            cmp = 0
          end
          return cmp if cmp != 0
        elsif direction.to_s == 'descending' or direction.to_s == 'desc' or (direction.kind_of?(Numeric) and direction < 0)
          if x_val.kind_of?(Numeric) and y_val.kind_of?(Numeric)
            cmp = y_val <=> x_val
          elsif x_val.kind_of?(Numeric)
            cmp = -1
          elsif y_val.kind_of?(Numeric)
            cmp = 1
          else
            cmp = 0
          end
          return cmp if cmp != 0
        else
          raise NotImplementedError.new("Unrecognized sort [field, direction] = [#{field.inspect}, #{direction.inspect}] (full spec #{sort.inspect}")
        end
      end
      0
    end

    def selector_match?(selector, doc)
      raise NotImplementedError.new('Does not current support $where queries') if selector.has_key?('$where')
      selector.all? { |k, v| partial_match?(v, doc[k]) }
    end

    def partial_match?(partial_selector, value)
      EmbeddedMongo.log.debug("partial_match? #{partial_selector.inspect} #{value.inspect}")
      case partial_selector
      when Array, Numeric, String, BSON::ObjectId, TrueClass, FalseClass, Time, nil
        partial_selector == value
      when Hash
        if no_directive?(partial_selector)
          partial_selector == value
        else
          raise NotImplementedError.new("Cannot mix $ directives with non: #{partial_selector.inspect}") if has_non_directive?(partial_selector)
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
      if no_directive?(update)
        doc.clear
        update.each { |k, v| doc[k] = v }
      else
        # TODO: should set_last_error to {"err"=>"Modifiers and non-modifiers cannot be mixed", "code"=>10154, "n"=>0, "ok"=>1.0}
        raise NotImplementedError.new("Modifiers and non-modifiers cannot be mixed #{update.inspect}") if has_non_directive?(update)
        update.each do |directive_key, directive_value|
          apply_update_directive!(directive_key, directive_value, doc)
        end
      end
      doc['_id'] ||= id
    end

    def apply_update_directive!(directive_key, directive_value, doc)
      case directive_key
      when '$set'
        directive_value.each { |k, v| deep_doc,key = deep( doc, k, true ); deep_doc[key]=v }
      when '$unset'
        directive_value.each { |k,v| deep_doc,key = deep( doc, k ); (deep_doc.delete(key) rescue nil); nil }
      when '$push', '$pop', '$addToSet'
        directive_value.each do |k,v|
          deep_doc,key = deep( doc, k, true )
          raise Mongo::OperationFailure, 'Not An Array' if deep_doc.has_key?(key) and !deep_doc[key].is_a?(Array)
          deep_doc[key] = [] unless deep_doc.has_key?(key)
          case directive_key
          when '$push' then deep_doc[key].push(v)
          when '$pop'
            if v > 0
              deep_doc[key].pop
            else
              deep_doc[key].shift
            end
          when '$addToSet'
            deep_doc[key].push(v) unless deep_doc[key].include?(v)
          end
        end
      else
        raise NotImplementedError.new("Have yet to implement updating: #{directive_key}")
      end
    end

    # enable the use of multipart keys e.g. 'stats.1984-04-14'
    # returns a tuple: [deep_doc,singlepert_key]
    # given:   [doc, 'stats.1984-04-14']
    # returns: [doc['stats'], '1984-04-14']
    def deep(doc,key, create=false)
      key = key.split('.') unless key.is_a?(Array)
      while key.size > 1
        current_key = key.shift
        unless doc.has_key?(current_key)
          if create then doc[current_key] = Hash.new
          else return nil
          end
        end
        raise Mongo::OperationFailure, 'Cannot descend into an existing object that is not a hash' unless doc[current_key].is_a?(Hash)
        doc = doc[current_key]
      end
      return [doc, key.last]
    end

    def has_non_directive?(document)
      document.any? { |k, v| !k.start_with?('$') }
    end

    def no_directive?(document)
      document.all? { |k, v| !k.start_with?('$') }
    end
  end
end
