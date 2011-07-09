require 'rubygems'
require 'test/unit'

require File.join(File.dirname(__FILE__), '../../lib/embedded-mongo')

class InterfaceTest < Test::Unit::TestCase
  def setup
    @conn = EmbeddedMongo::Connection.new
  end

  def test_insert_and_find
    test_db = @conn['test']
    foo_collection = test_db['foo']
    id = foo_collection.insert({})

    cursor = foo_collection.find({ :_id => id })
    assert_equal(1, cursor.count)
  end
end
