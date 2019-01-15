describe Strongman do
  it "can resolve single value" do
    loader = Strongman.new do |ids|
      ids.map {|id| "awesome #{id}"}
    end

    one = loader.load(1)

    expect(one.value).to eq("awesome 1")
  end

  it "can resolve two values one separately" do
    loader = Strongman.new do |ids|
      ids.map {|id| "awesome #{id}"}
    end

    one = loader.load(1)
    two = loader.load(2)

    expect(one.value).to eq("awesome 1")
    expect(two.value).to eq("awesome 2")
  end

  it "can resolve multiple values" do
    loader = Strongman.new do |ids|
      ids.map {|id| "awesome #{id}"}
    end

    promise = loader.load_many([1, 2])

    one, two = promise.value

    expect(one).to eq("awesome 1")
    expect(two).to eq("awesome 2")
  end

  it "runs loader just one time, even for multiple values" do
    loader = Strongman.new do |ids|
      ids.map {|_id| ids}
    end

    one = loader.load(1)
    two = loader.load(2)

    expect(one.value).to eq([1, 2])
    expect(two.value).to eq([1, 2])
  end

  it "runs loader just one time, even for mixed access values" do
    loader = Strongman.new do |ids|
      ids.map {|_id| ids}
    end

    first = loader.load_many([1, 2])
    loader.load(3)

    expect(first.value[0]).to eq([1, 2, 3])
    expect(first.value[1]).to eq([1, 2, 3])
  end

  it "can return a hash instead of an array" do
    loader = Strongman.new do |ids|
      Hash[ids.zip(ids.map {|id| id + 10})]
    end

    first = loader.load_many([1, 2])
    second = loader.load(3)

    expect(first.value[0]).to eq(11)
    expect(first.value[1]).to eq(12)
    expect(second.value).to eq(13)
  end

  it "does not run if no need to" do
    calls = 0
    loader = Strongman.new do |ids|
      calls += 1
      Hash[ids.zip(ids.map {|_id| ids})]
    end

    loader.load_many([1, 2])
    loader.load(3)

    expect(calls).to eq(0)
  end

  it "works even if loader resolves to a promise executed out of order" do
    promise = Concurrent::Promises.resolvable_future

    loader = Strongman.new do |ids|
      ids.map do |id|
        promise.then do |value|
          value + id + 40
        end
      end
    end

    plus_fourty = loader.load(2)
    promise.fulfill(100)

    expect(plus_fourty.value).to eq(142)
  end

  it "works even if loader returns a promise executed out of order" do
    promise = Concurrent::Promises.resolvable_future

    loader = Strongman.new do |ids|
      promise.then do |value|
        ids.map do |id|
          value + id + 40
        end
      end
    end

    plus_fourty = loader.load(2)
    promise.fulfill(100)

    expect(plus_fourty.value).to eq(142)
  end

  it "works if promise is passed as an argument to dataloader" do
    promise = Concurrent::Promises.resolvable_future

    loader = Strongman.new do |promises|
      promises.map do |p|
        p.then do |value|
          value + 40
        end
      end
    end

    plus_fourty = loader.load(promise)
    promise.fulfill(100)

    expect(plus_fourty.value).to eq(140)
  end

  it "can depend on other loaders" do
    data_loader = Strongman.new do |ids|
      ids.map {|_id| ids}
    end

    data_transformer = Strongman.new do |ids|
      data_loader.load_many(ids).then do |records|
        records.map(&:count)
      end
    end

    parent_data = data_loader.load(3)
    transformer_one = data_transformer.load(1)
    transformer_two = data_transformer.load(2)

    expect(transformer_one.value!).to eq(3)
    expect(transformer_two.value!).to eq(3)
    expect(parent_data.value!).to eq([3, 1, 2])
  end

  it "does not run what it does not need to when chaining" do
    data_loader = Strongman.new do |ids|
      ids.map {|_id| ids}
    end

    data_transformer = Strongman.new do |ids|
      data_loader.load_many(ids).then do |records|
        records.map(&:count)
      end
    end

    one = data_transformer.load(1)
    two = data_transformer.load(2)
    three = data_loader.load(3)

    expect(three.value!).to eq([3])
    expect(one.value!).to eq(2)
    expect(two.value!).to eq(2)
  end

  it "supports loading out of order when chaining" do
    data_loader = Strongman.new do |ids|
      ids.map {|_id| ids}
    end

    data_transformer = data_loader.chain do |parent, ids|
      parent.load_many(ids).then do |records|
        records.map(&:count)
      end
    end

    three = data_loader.load(3)
    one = data_transformer.load(1)
    two = data_transformer.load(2)

    expect(three.value!).to eq([3, 1, 2])
    expect(one.value!).to eq(3)
    expect(two.value!).to eq(3)
  end

  it "caches values for each key" do
    calls = 0

    data_loader = Strongman.new do |ids|
      calls += 1
      ids.map {|id| id}
    end

    one = data_loader.load(1)
    two = data_loader.load(2)

    expect(one.value!).to be(1)
    expect(two.value!).to be(2)

    one2 = data_loader.load(1)
    two2 = data_loader.load(2)

    expect(one2.value!).to be(1)
    expect(two2.value!).to be(2)

    expect(calls).to be(1)
  end

  it "uses cache for load_many as well (per-item)" do
    calls = 0
    data_loader = Strongman.new do |ids|
      calls += 1
      ids.map {|_id| ids}
    end

    2.times do
      one = data_loader.load_many([1, 2])
      two = data_loader.load_many([2, 3])

      expect(one.value![0]).to eq([1, 2, 3])
      expect(one.value![1]).to eq([1, 2, 3])
      expect(two.value![0]).to eq([1, 2, 3])
      expect(two.value![1]).to eq([1, 2, 3])
    end

    expect(calls).to eq(1)
  end

  it "can resolve in complex cases" do
    loads = []

    loader = Strongman.new(name: "loader 1") do |ids|
      loads.push(["loader", ids])
      ids.map {|id| {name: "bar #{id}"}}
    end

    loader2 = loader.chain(name: "loader 2") do |parent, ids|
      loads.push(["loader2", ids])

      parent.load_many(ids).then do |records|
        Hash[ids.zip(records.map {|r| r[:name]})]
      end
    end

    one = loader.load(0)
    two = loader.load_many([1, 2])
    three = loader.load_many([2, 3])
    four = loader2.load_many([2, 3, 5])

    loader3 = loader2.chain(name: "loader 3") do |parent, ids|
      loads.push(["loader3", ids])

      parent.load_many(ids).then do |names|
        Hash[ids.zip(names.map {|name| "foo #{name}"})]
      end
    end

    five = loader3.load_many([2, 3, 5, 7])

    expect(five.value!).to eq(["foo bar 2", "foo bar 3", "foo bar 5", "foo bar 7"])
    expect(four.value!).to eq(["bar 2", "bar 3", "bar 5"])
    expect(three.value!).to eq([{name: "bar 2"}, {name: "bar 3"}])
    expect(two.value!).to eq([{name: "bar 1"}, {name: "bar 2"}])
    expect(one.value!).to eq(name: "bar 0")

    expect(loads).to eq([
                          ["loader3", [2, 3, 5, 7]],
                          ["loader2", [2, 3, 5, 7]],
                          ["loader", [0, 1, 2, 3, 5, 7]]
                        ])
  end

  it 'can be passed a primed cache' do
    cache = Concurrent::Map.new
    cache[0] = 42

    data_loader = Strongman.new(cache: cache) do |ids|
      ids.map {|id| id}
    end

    expect(data_loader.load(0).value!).to eq(42)
  end

  it 'can be passed a primed cache with promises' do
    cache = Concurrent::Map.new
    cache[0] = Concurrent::Promises.future {42}

    data_loader = Strongman.new(cache: cache) do |ids|
      ids.map {|id| id}
    end

    expect(data_loader.load(0).value).to eq(42)
  end

  it 'can be passed custom cache' do
    class Cache
      def compute_if_absent(key)
        42
      end
    end

    data_loader = Strongman.new(cache: Cache.new) do |ids|
      ids.map {|id| id}
    end

    expect(data_loader.load(0).value).to eq(42)
  end

  it 'can disable the cache' do
    data_loader = Strongman.new(cache: nil) do |ids|
      ids.map {|id| ids}
    end

    one = data_loader.load(0)
    two = data_loader.load(0)

    expect(one.value).to eq([0, 0])
  end

  it 'can reset the cache' do
    data_loader = Strongman.new do |ids|
      ids.map {|id| ids}
    end

    one = data_loader.load(0).value

    data_loader.cache = Concurrent::Map.new
    data_loader.cache[0] = 42

    one_again = data_loader.load(0)

    expect(one_again.value!).to eq(42)
  end

  it 'raises an TypeError if keys passed to load_many are not array' do
    data_loader = Strongman.new do |ids|
      ids.map {|id| ids}
    end

    expect {
      data_loader.load_many("foo")
    }.to raise_error(TypeError, "#load_many must be called with an Array, but got: String")
  end

  it 'raises an TypeError if keys passed to load is nil' do
    data_loader = Strongman.new do |ids|
      ids.map {|id| ids}
    end

    expect {
      data_loader.load(nil)
    }.to raise_error(TypeError, "#load must be called with a key, but got: nil")
  end

  it 'raises an TypeError if batch loader returns something else than Array or Hash' do
    data_loader = Strongman.new do |ids|
      "value"
    end

    expect {
      data_loader.load(42).value!
    }.to raise_error(TypeError, "Batch loader must return an Array or Hash, but returned: String")
  end

  it 'raises an error if dataloader returns array of different size' do
    data_loader = Strongman.new do |ids|
      [1, 2]
    end

    expect {
      data_loader.load(42).value!
    }.to raise_error(StandardError, /Batch loader must be instantiated with function that returns Array or Hash of the same size as provided to it Array of keys/)
  end

  it 'raises an error if dataloader returns hash of different size' do
    data_loader = Strongman.new do |ids|
      {
        1 => 2,
        3 => 4
      }
    end

    expect {
      data_loader.load(42).value!
    }.to raise_error(StandardError, /Batch loader must be instantiated with function that returns Array or Hash of the same size as provided to it Array of keys/)
  end

  it 'raises an error if not passed a dataloader' do
    expect {
      data_loader = Strongman.new
    }.to raise_error(StandardError, "Dataloader must be constructed with a block which accepts Array and returns either Array or Hash of the same size (or Promise)")
  end

  it 'raises an error if dataloader does not return value for given key' do
    data_loader = Strongman.new do |ids|
      {
        1 => 2
      }
    end

    expect {
      data_loader.load(42).value!
    }.to raise_error(StandardError, "Batch loader didn't resolve a key: 42. Resolved keys: [1]")
  end

  it 'can disable batching by setting max_batch_size = 1' do
    loader = Strongman.new(max_batch_size: 1) do |ids|
      ids.map {|id| ids}
    end

    one = loader.load(1)
    two = loader.load(2)

    expect(one.value!).to eq([1])
    expect(two.value!).to eq([2])
  end

  it 'can force grouped batching by setting max_batch_size' do
    loader = Strongman.new(max_batch_size: 2) do |ids|
      ids.map {|id| ids}
    end

    one = loader.load(1)
    two = loader.load(2)
    three = loader.load(3)

    expect(one.value!).to eq([1, 2])
    expect(three.value!).to eq([3])
  end

  it 'returns the same promise when called two times' do
    loader = Strongman.new do |ids|
      ids.map {|id| ids}
    end

    one = loader.load(0)
    two = loader.load(0)

    expect(one).to be(two)
  end

  it 'accepts an interceptor chain' do
    accumulator = Concurrent::Array.new

    interceptor = -> (next_interceptor) {
      -> (ids) {
        accumulator << "before"
        result = next_interceptor.call(ids)
        accumulator << result
        accumulator << "after"
        result
      }
    }

    loader = Strongman.new(interceptor: interceptor) do |ids|
      ids.map {|id| ids}
    end

    one = loader.load(1)

    expect(one.value!).to eq([1])
    expect(accumulator.to_a).to eq(["before", [[1]], "after"])
  end
end
