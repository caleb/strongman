require "concurrent"
require "awesome_print"

class Strongman
  class NoCache
    def compute_if_absent(_key)
      yield
    end
  end

  class Batch
    attr_accessor :parent
    attr_accessor :name
    attr_accessor :lock
    attr_accessor :fulfilled
    attr_accessor :fulfilling

    def initialize(loader_block, name: nil, parent: nil, max_batch_size: Float::INFINITY)
      @name = name
      @queue = Concurrent::Array.new
      @promise = Concurrent::Promises.resolvable_future
      @loader_block = loader_block
      @lock = Concurrent::ReadWriteLock.new
      @parent = parent
      @children = Concurrent::Array.new
      @fulfilling = Concurrent::AtomicBoolean.new(false)
      @fulfilled = Concurrent::AtomicBoolean.new(false)
      @max_batch_size = max_batch_size

      @parent.children << self if @parent

      @root = nil
      @batch_chain = nil
    end

    def fulfilled?
      root.fulfilled.true?
    end

    def fulfilling?
      root.fulfilling.true?
    end

    def needs_fulfilling?
      !fulfilled? && !fulfilling?
    end

    def queue(key)
      @queue << key

      future = @promise.then do |results|
        unless results.key?(key)
          raise StandardError, "Batch loader didn't resolve a key: #{key}. Resolved keys: #{results.keys}"
        end

        result = results[key]

        if result.is_a?(Concurrent::Promises::Future)
          result
        else
          Concurrent::Promises.resolvable_future.fulfill(result)
        end
      end.flat

      #
      # If our queue is full, fulfill immediately and return the bare future
      #
      if @queue.size >= @max_batch_size
        root.fulfill_hierarchy

        future
      else
        #
        # If the queue is not full, create a delayed future that fulfills when the value is requested and chains
        # to the inner future
        #
        Concurrent::Promises.delay do
          # with_lock do
          root.fulfill_hierarchy if root.needs_fulfilling?
          # end

          future
        end.flat
      end
    end

    def mark_fulfilled!
      root.fulfilled.make_true
      self
    end

    def mark_fulfilling!
      root.fulfilling.make_true
      self
    end

    def mark_not_fulfilling!
      root.fulfilling.make_false
      self
    end

    def with_lock
      root.lock.with_write_lock do
        yield
      end
    end

    def root
      if @root
        @root
      else
        find_top = -> (batch) {
          if batch.parent
            find_top.(batch.parent)
          else
            batch
          end
        }

        @root = find_top.(self)
      end
    end

    def batch_chain
      if @batch_chain
        @batch_chain
      else
        @batch_chain = Concurrent::Array.new

        add_children = -> (batch) {
          @batch_chain << batch
          if batch.children.size > 0
            batch.children.flat_map(&add_children)
          end
        }
        add_children.(root)

        @batch_chain
      end
    end

    def fulfill_hierarchy
      raise Error.new("Only run #fulfill_hierarchy on root batches") if @parent

      with_lock do
        return if fulfilled?

        mark_fulfilling!
        batch_chain.reverse.each(&:fulfill!)
      ensure
        mark_fulfilled!
        mark_not_fulfilling!
      end
    end

    def fulfill!
      results = @loader_block.call(@queue)

      if results.is_a?(Concurrent::Promises::Future)
        # if the strongman loader block returns a promise (e.g. if the block uses another loader),
        # make sure to touch it to kick off any delayed effects before chaining
        results.touch.then do |inner_results|
          @promise.fulfill(normalize_results(inner_results))
        end.flat
      else
        @promise.fulfill(normalize_results(results))
      end

      self
    end

    def children
      @children ||= Concurrent::Array.new
    end

    private

    def normalize_results(results)
      unless results.is_a?(Array) || results.is_a?(Hash)
        raise TypeError, "Batch loader must return an Array or Hash, but returned: #{results.class.name}"
      end

      if @queue.size != results.size
        raise StandardError, "Batch loader must be instantiated with function that returns Array or Hash " \
          "of the same size as provided to it Array of keys" \
          "\n\nProvided keys:\n#{@queue}" \
          "\n\nReturned values:\n#{results}"
      end

      if results.is_a?(Array)
        Hash[@queue.zip(results)]
      else
        results.is_a?(Hash)
        results
      end
    end
  end

  attr_accessor :cache

  def initialize(**options, &block)
    unless block_given?
      raise TypeError, "Dataloader must be constructed with a block which accepts " \
        "Array and returns either Array or Hash of the same size (or Promise)"
    end

    @name = options.delete(:name)
    @parent = options.delete(:parent)
    @cache = if options.has_key?(:cache)
               options.delete(:cache) || NoCache.new
             else
               Concurrent::Map.new
             end
    @max_batch_size = options.fetch(:max_batch_size, Float::INFINITY)

    @interceptor = options.delete(:interceptor) || -> (n) {
      -> (ids) {
        n.call(ids)
      }
    }

    if @parent
      @interceptor = @interceptor.call(-> (n) {
        -> (ids) {
          n.call(@parent, ids)
        }
      })
    end

    @loader_block = @interceptor.call(block)
  end

  def depends_on(**options, &block)
    Strongman.new(**options, parent: self, &block)
  end

  def load(key)
    if key.nil?
      raise TypeError, "#load must be called with a key, but got: nil"
    end

    result = retrieve_from_cache(key) do
      batch.queue(key)
    end

    if result.is_a?(Concurrent::Promises::Future)
      result
    else
      Concurrent::Promises.future {result}
    end
  end

  def load_many(keys)
    unless keys.is_a?(Array)
      raise TypeError, "#load_many must be called with an Array, but got: #{keys.class.name}"
    end

    promises = keys.map(&method(:load))
    Concurrent::Promises.zip_futures(*promises).then {|*results| results}
  end

  def batch
    if @batch.nil? || @batch.fulfilled?
      @batch = Batch.new(@loader_block, name: @name, parent: @parent&.batch, max_batch_size: @max_batch_size)
    else
      @batch
    end
  end

  def retrieve_from_cache(key)
    @cache.compute_if_absent(key) do
      yield
    end
  end
end
