require 'rubygems'
require 'test/unit'

require File.join(File.dirname(__FILE__), '../../lib/embedded-mongo')

class InterfaceTest < Test::Unit::TestCase
  def setup
    # Tests should pass with either of the following lines
    @conn = EmbeddedMongo::Connection.new
    # @conn = Mongo::Connection.new
    @test_db = @conn['test']
    @foo_collection = @test_db['foo']
  end

  def test_insert_and_find
    id = @foo_collection.insert({ 'bar' => 'baz' })

    cursor = @foo_collection.find({ '_id' => id })
    assert_equal(1, cursor.count)
    assert_equal({ '_id' => id, 'bar' => 'baz'}, cursor.first)
  end

  def test_insert_update_and_find
    id = @foo_collection.insert({ 'zombie' => 'baz' })
    @foo_collection.update({ 'zombie' => 'baz' }, { 'test' => 'tar' })

    cursor = @foo_collection.find({ '_id' => id })
    assert_equal(1, cursor.count)
    assert_equal({ '_id' => id, 'test' => 'tar'}, cursor.first)
  end

  def test_changing_ids
    id = @foo_collection.insert({ 'zing' => 'zong' })
    @foo_collection.update({ '_id' => id }, { '_id' => 'other_id' })

    cursor = @foo_collection.find({ '_id' => id })
    assert_equal(0, cursor.count)
    cursor = @foo_collection.find({ '_id' => 'other_id' })
    assert_equal(1, cursor.count)
    assert_equal({ '_id' => 'other_id' }, cursor.first)
  end
end
