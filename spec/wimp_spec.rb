describe Wimp do
  it "can resolve single value" do
    loader = Wimp.new do |ids|
      ids.map {|id| "awesome #{id}"}
    end

    one = loader.load(1)

    expect(one.value).to eq("awesome 1")
  end

  it "can resolve two values one separately" do
    loader = Wimp.new do |ids|
      ids.map {|id| "awesome #{id}"}
    end

    one = loader.load(1)
    two = loader.load(2)

    expect(one.value).to eq("awesome 1")
    expect(two.value).to eq("awesome 2")
  end

  it "can resolve multiple values" do
    loader = Wimp.new do |ids|
      ids.map {|id| "awesome #{id}"}
    end

    promise = loader.load_many([1, 2])

    one, two = promise.value

    expect(one).to eq("awesome 1")
    expect(two).to eq("awesome 2")
  end

  it "runs loader just one time, even for multiple values" do
    loader = Wimp.new do |ids|
      ids.map {|_id| ids}
    end

    one = loader.load(1)
    two = loader.load(2)

    expect(one.value).to eq([1, 2])
    expect(two.value).to eq([1, 2])
  end

  it "runs loader just one time, even for mixed access values" do
    loader = Wimp.new do |ids|
      ids.map {|_id| ids}
    end

    first = loader.load_many([1, 2])
    loader.load(3)

    expect(first.value[0]).to eq([1, 2, 3])
    expect(first.value[1]).to eq([1, 2, 3])
  end

  it "can return a hash instead of an array" do
    loader = Wimp.new do |ids|
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
    loader = Wimp.new do |ids|
      calls += 1
      Hash[ids.zip(ids.map {|_id| ids})]
    end

    loader.load_many([1, 2])
    loader.load(3)

    expect(calls).to eq(0)
  end

  it "caches values for each key" do
    calls = 0

    data_loader = Wimp.new do |ids|
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
    data_loader = Wimp.new do |ids|
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

  it 'can be passed a primed cache' do
    cache = Concurrent::Map.new
    cache[0] = 42

    data_loader = Wimp.new(cache: cache) do |ids|
      ids.map {|id| id}
    end

    expect(data_loader.load(0).value!).to eq(42)
  end

  it 'can be passed custom cache' do
    class Cache
      def compute_if_absent(key)
        42
      end
    end

    data_loader = Wimp.new(cache: Cache.new) do |ids|
      ids.map {|id| id}
    end

    expect(data_loader.load(0).value).to eq(42)
  end

  it 'can disable the cache' do
    data_loader = Wimp.new(cache: nil) do |ids|
      ids.map {|id| ids}
    end

    one = data_loader.load(0)
    two = data_loader.load(0)

    expect(one.value).to eq([0, 0])
  end

  it 'can reset the cache' do
    data_loader = Wimp.new do |ids|
      ids.map {|id| ids}
    end

    one = data_loader.load(0).value

    data_loader.cache = Concurrent::Map.new
    data_loader.cache[0] = 42

    one_again = data_loader.load(0)

    expect(one_again.value!).to eq(42)
  end

  it 'raises an TypeError if keys passed to load_many are not array' do
    data_loader = Wimp.new do |ids|
      ids.map {|id| ids}
    end

    expect {
      data_loader.load_many("foo")
    }.to raise_error(TypeError, "#load_many must be called with an Array, but got: String")
  end

  it 'raises an TypeError if keys passed to load is nil' do
    data_loader = Wimp.new do |ids|
      ids.map {|id| ids}
    end

    expect {
      data_loader.load(nil)
    }.to raise_error(TypeError, "#load must be called with a key, but got: nil")
  end

  it 'raises an TypeError if batch loader returns something else than Array or Hash' do
    data_loader = Wimp.new do |ids|
      "value"
    end

    expect {
      data_loader.load(42).value!
    }.to raise_error(TypeError, "Batch loader must return an Array or Hash, but returned: String")
  end

  it 'raises an error if dataloader returns array of different size' do
    data_loader = Wimp.new do |ids|
      [1, 2]
    end

    expect {
      data_loader.load(42).value!
    }.to raise_error(StandardError, /Batch loader must be instantiated with function that returns Array or Hash of the same size as provided to it Array of keys/)
  end

  it 'raises an error if dataloader returns hash of different size' do
    data_loader = Wimp.new do |ids|
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
      data_loader = Wimp.new
    }.to raise_error(StandardError, "Dataloader must be constructed with a block which accepts Array and returns either Array or Hash of the same size (or Promise)")
  end

  it 'raises an error if dataloader does not return value for given key' do
    data_loader = Wimp.new do |ids|
      {
        1 => 2
      }
    end

    expect {
      data_loader.load(42).value!
    }.to raise_error(StandardError, "Batch loader didn't resolve a key: 42. Resolved keys: [1]")
  end

  it 'returns the same promise when called two times' do
    loader = Wimp.new do |ids|
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

    loader = Wimp.new(interceptor: interceptor) do |ids|
      ids.map {|id| ids}
    end

    one = loader.load(1)

    expect(one.value!).to eq([1])
    expect(accumulator.to_a).to eq(["before", [[1]], "after"])
  end
end
