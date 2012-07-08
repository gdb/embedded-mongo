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

  def test_update_set
    @foo_collection.insert({'a'=>20,'b'=>40})
    @foo_collection.update({'a'=>20},{
      '$set'=>{'c'=>60}
    })
    assert_contained ({'a'=>20,'b'=>40,'c'=>60}), @foo_collection.find_one({'a'=>20})

    @foo_collection.update({'a'=>20},{
      '$set'=>{'b'=>100}
    })
    
    assert_contained ({'a'=>20,'b'=>100,'c'=>60}), @foo_collection.find_one({'a'=>20})

    @foo_collection.update({'a'=>20},{
      '$set'=>{'stats.today'=>100}
    })
    assert_contained ({'stats'=>{'today'=>100}}), @foo_collection.find_one({'a'=>20})
  end

  def test_update_unset
    @foo_collection.insert({'a'=>20,'b'=>40,'c'=>60,'d'=>{'e'=>30,'f'=>57}})
    @foo_collection.update({'a'=>20},{
      '$unset'=>{'b'=>1}
    })

    assert_not_nil @foo_collection.find_one({'a'=>20})
    assert_not_contained ({'b'=>40}), @foo_collection.find_one({'a'=>20})
    assert_contained ({'a'=>20,'c'=>60}), @foo_collection.find_one({'a'=>20})

    @foo_collection.update({'a'=>20},{
      '$unset'=>{'d.e'=>1}
    })
    assert_not_contained ({'d'=>{'e'=>30}}), @foo_collection.find_one({'a'=>20})
    assert_contained ({'d'=>{'f'=>57}}), @foo_collection.find_one({'a'=>20})
  end

  def test_update_push
    @foo_collection.insert({'a'=>20,'c'=>60})
    @foo_collection.update({'a'=>20},{
      '$push'=>{'b'=>5}
    })
    assert_contained ({'a'=>20,'b'=>[5]}), @foo_collection.find_one({'a'=>20})
    @foo_collection.update({'a'=>20},{
      '$push'=>{'b'=>10}
    })
    assert_contained ({'a'=>20,'b'=>[5,10]}), @foo_collection.find_one({'a'=>20})
    assert_raise Mongo::OperationFailure do
      @foo_collection.update({'a'=>20},{
        '$push'=>{'c'=>10}
      })
    end

    @foo_collection.update({'a'=>20},{
      '$push'=>{'g.h.i'=>5}
    })
    assert_contained ({'g'=>{'h'=>{'i'=>[5]}}}), @foo_collection.find_one({'a'=>20})
    @foo_collection.update({'a'=>20},{
      '$push'=>{'g.h.i'=>4}
    })
    assert_contained ({'g'=>{'h'=>{'i'=>[5,4]}}}), @foo_collection.find_one({'a'=>20})

  end

  def test_update_pop
    @foo_collection.insert({'a'=>20,'b'=>12,'c'=>[5,10,15,20]})
    @foo_collection.update({'a'=>20},{
      '$pop'=>{'c'=>1}
    })
    assert_contained ({'a'=>20,'c'=>[5,10,15]}), @foo_collection.find_one({'a'=>20})
    @foo_collection.update({'a'=>20},{
      '$pop'=>{'c'=>-1}
    })
    assert_contained ({'a'=>20,'c'=>[10,15]}), @foo_collection.find_one({'a'=>20})

    assert_raise Mongo::OperationFailure do
      @foo_collection.update({'a'=>20},{
        '$pop'=>{'b'=>1}
      })
    end
  end

  def test_update_add_to_set
    @foo_collection.insert({'a'=>20,'b'=>12,'c'=>[5,10,15,20]})
    @foo_collection.update({'a'=>20},{
      '$addToSet'=>{'c'=>25}
    })
    assert_contained ({'a'=>20,'c'=>[5,10,15,20,25]}), @foo_collection.find_one({'a'=>20})
    
    @foo_collection.update({'a'=>20},{
      '$addToSet'=>{'c'=>5}
    })
    assert_contained ({'a'=>20,'c'=>[5,10,15,20,25]}), @foo_collection.find_one({'a'=>20})

  end

  # ensure that hsh1 is contained in hsh2
  def assert_contained hsh1, hsh2, additional_message=nil
    contained, msg = catch :contained do
        hsh1.each do |k,v|
          throw :contained, [false,"missing key <#{k}>"] unless hsh2.has_key? k
          unless v == hsh2[k]
            throw :contained, [false,"mismatch entry for <#{k}>:\nexpect:<#{v.inspect}>\nactual:<#{hsh2[k].inspect}>"]
          end
        end
        throw :contained, [true]
      end
    assert contained, [additional_message,msg].compact.join("\n")
  end

  def assert_not_contained hsh1, hsh2, msg=nil
    fail_message = [msg,"expected was contained in actual:\ne:<#{hsh1.inspect}>\na:<#{hsh2.inspect}>"].compact.join("\n")
    assert_raise Test::Unit::AssertionFailedError, fail_message do
      assert_contained hsh1, hsh2
    end
  end
end
