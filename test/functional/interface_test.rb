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
    @foo_collection.remove
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

  def test_update_upsert_record_not_present
    selector = {'schubar'=>'mubar'}
    @foo_collection.update(
      selector,
      {'$set'=>{'baz'=>'bingo'}},
      :upsert => true
      )
    cursor = @foo_collection.find(selector)
    assert_equal 1, cursor.count
    entry = cursor.first
    assert_equal 'bingo', (entry['baz'] rescue nil)
  end

  def test_update_upsert_record_present
    selector = {'fubar'=>'rubar'}
    @foo_collection.insert(
      selector.merge('tweedle'=>'dee')
      )
    @foo_collection.update(
      selector,
      {'$set'=>{'baz'=>'bingo'}},
      :upsert => true
      )
    cursor = @foo_collection.find(selector)
    assert_equal 1, cursor.count
    entry = cursor.first
    assert_equal 'bingo', (entry['baz'] rescue nil), 'failed to set new value'
    assert_equal 'dee', (entry['tweedle'] rescue nil), 'overwrote unrelated value in record'
  end

  def test_update_increment_record_field
    selector = {'fubar'=>'rubar'}
    @foo_collection.insert(
      selector.merge('baz'=>1)
      )
    @foo_collection.update(
      selector,
      {'$inc'=>{'baz'=>2}}
      )
    cursor = @foo_collection.find(selector)
    assert_equal 1, cursor.count
    entry = cursor.first
    assert_equal 3, (entry['baz'] rescue nil), 'failed to increment value'
  end

  def test_update_increment_record_field_with_incorrect_type
    selector = {'fubar'=>'rubar'}
    @foo_collection.insert(
      selector.merge('baz'=>'not an integer')
      )

    assert_raise(Mongo::OperationFailure) do
      @foo_collection.update(
        selector,
        {'$inc'=>{'baz'=>2}}
        )
    end
  end

  def test_update_upsert_record_with_id
    @foo_collection.update(
      {'foo' => 'bart','_id'=>0xdeadbeef},
      {'$set'=>{'baz'=>'bingo'}},
      :upsert => true
      )
    cursor = @foo_collection.find({ 'foo' => 'bart' })
    assert_equal 1, cursor.count
    entry = cursor.first
    assert_equal 0xdeadbeef, (entry['_id'] rescue nil), 'overwrote id'
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

  def test_remove
    id1 = @foo_collection.insert({ 'lime' => 'limit' })
    id2 = @foo_collection.insert({ 'lemon' => 'limit' })
    @foo_collection.remove({ 'lime' => 'limit' })

    cursor = @foo_collection.find({ '_id' => id1 })
    assert_equal(0, cursor.count)
    cursor = @foo_collection.find({ '_id' => id2 })
    assert_equal(1, cursor.count)
    assert_equal({ '_id' => id2, 'lemon' => 'limit' }, cursor.first)
  end

  def test_sort
    @foo_collection.insert({ 'a' => 10 })
    @foo_collection.insert({ 'a' => 20 })
    @foo_collection.insert({ 'b' => 'foo' })

    cursor1 = @foo_collection.find.sort([['a', 'asc']])
    res1 = cursor1.to_a
    assert_equal(nil, res1[0]['a'])
    assert_equal(10, res1[1]['a'])
    assert_equal(20, res1[2]['a'])

    cursor2 = @foo_collection.find.sort([['a', 'desc']])
    res2 = cursor2.to_a
    assert_equal(20, res2[0]['a'])
    assert_equal(10, res2[1]['a'])
    assert_equal(nil, res2[2]['a'])
  end

  def test_or_support
    @foo_collection.insert({ 'e' => 10 })
    @foo_collection.insert({ 'e' => 20 })
    @foo_collection.insert({ 'e' => 30 })

    cursor1 = @foo_collection.find(:$or=>[{'e'=>10}, {'e'=>20}])
    res1 = cursor1.to_a
    assert_equal(2, res1.length)
    p res1
  end

  def test_and_support
    @foo_collection.insert({ 'e' => 10 })
    @foo_collection.insert({ 'e' => 20 })
    @foo_collection.insert({ 'e' => 30 })

    cursor1 = @foo_collection.find({:$or=>[{'e'=>10}, {'e'=>20}], :$and=>[{'e'=>10}]})
    res1 = cursor1.to_a
    assert_equal(1, res1.length)
  end

  def test_for_in
    @foo_collection.insert({'q'=>['der', 'dor']})
    @foo_collection.insert({'q'=>['dar', 'dot']})
    @foo_collection.insert({'q'=>['dot']})


    cursor1 = @foo_collection.find({'q'=>'dot'})
    res1 = cursor1.to_a
    assert_equal(2, res1.length)
  end

end
