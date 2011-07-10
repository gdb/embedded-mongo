require 'rubygems'
require 'test/unit'

require File.join(File.dirname(__FILE__), '../../lib/embedded-mongo')

class InterfaceTest < Test::Unit::TestCase
  def setup
    @conn = EmbeddedMongo::Connection.new
    @test_db = @conn['test']
    @foo_collection = @test_db['foo']
  end

  def test_insert_and_find
    id = @foo_collection.insert({ 'bar' => 'baz' })

    cursor = @foo_collection.find({ '_id' => id })
    assert_equal(1, cursor.count)
    assert_equal({ '_id' => id, 'bar' => 'baz'}, cursor.first)
  end
end
