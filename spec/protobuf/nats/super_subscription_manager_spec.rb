require "spec_helper"
require "thread"

describe ::Protobuf::Nats::SuperSubscriptionManager do
  let(:nats_client) { ::FakeNatsClient.new }
  let(:callback) { proc { |data, reply, subject| } }
  subject { described_class.new(nats_client, &callback) }

  after do
    # Ensure the thread is killed after each test
    subject.shutdown(0.1)
  end

  describe "#initialize" do
    it "starts a pending queue handler thread" do
      handler_thread = subject.instance_variable_get(:@pending_queue_handler)
      expect(handler_thread).to be_a(Thread)
      expect(handler_thread.alive?).to be(true)
    end
  end

  describe "message processing" do
    it "processes messages from the queue and invokes the callback" do
      message_data = "message_data"
      message_reply = "message_reply"
      message_subject = "message_subject"
      message = double(:data => message_data, :reply => message_reply, :subject => message_subject)
      
      mutex = Mutex.new
      cond = ConditionVariable.new
      
      # Expect the callback to be called with the message contents
      expect(callback).to receive(:call).with(message_data, message_reply, message_subject) do
        mutex.synchronize { cond.signal }
      end

      # Push a message to the queue and wait for it to be processed
      pending_queue = subject.instance_variable_get(:@pending_queue)
      pending_queue.push(message)
      
      # Wait for the callback to signal
      mutex.synchronize { cond.wait(mutex, 1) }
    end
  end

  describe "#queue_subscribe" do
    it "subscribes to a nats queue" do
      fake_subscription = nats_client.subscribe("test.sub")
      expect(nats_client).to receive(:subscribe).with("my.queue.name", :queue => "my.queue.name").and_return(fake_subscription)
      subject.queue_subscribe("my.queue.name")
    end

    it "shovels messages from old queue to the new one" do
      # Create a subscription with a message already in its queue
      subscription = nats_client.subscribe("my.queue.name")
      subscription.pending_queue.push("belated_message")
      
      # Stub the nats client to return this subscription
      allow(nats_client).to receive(:subscribe).and_return(subscription)
      
      subject.queue_subscribe("my.queue.name")

      # The main pending queue should have received the message
      pending_queue = subject.instance_variable_get(:@pending_queue)
      expect(pending_queue.pop).to eq("belated_message")
    end
  end

  describe "error handling" do
    it "logs per-message errors and continues" do
      mutex = Mutex.new
      cond = ConditionVariable.new

      # Setup a callback that will raise an error
      exploding_callback = proc { raise "Boom!" }
      manager = described_class.new(nats_client, &exploding_callback)

      # Mock the logger on the manager instance we are testing
      logger = ::Logger.new(nil)
      allow(manager).to receive(:logger).and_return(logger)
      expect(logger).to receive(:error).with(/failed to process message/i) do
        mutex.synchronize { cond.signal }
      end

      # Push a message that will trigger the error
      pending_queue = manager.instance_variable_get(:@pending_queue)
      pending_queue.push(double(:data => "d", :reply => "r", :subject => "s"))

      # Wait for the logger to be called
      mutex.synchronize { cond.wait(mutex, 1) }

      # The thread should still be alive
      handler_thread = manager.instance_variable_get(:@pending_queue_handler)
      expect(handler_thread.alive?).to be(true)
      
      manager.shutdown(0.1)
    end
  end

  describe "#shutdown" do
    it "stops the handler thread" do
      handler_thread = subject.instance_variable_get(:@pending_queue_handler)
      expect(handler_thread.alive?).to be(true)
      
      subject.shutdown
      
      expect(handler_thread.join(1)).to eq(handler_thread)
      expect(handler_thread.alive?).to be(false)
    end
  end

  describe "#unsubscribe_all" do
    it "unsubscribes from all subscriptions" do
      sub1 = nats_client.subscribe("test.1")
      sub2 = nats_client.subscribe("test.2")
      
      allow(nats_client).to receive(:subscribe).and_return(sub1, sub2)
      
      subject.queue_subscribe("test.1")
      subject.queue_subscribe("test.2")

      expect(sub1).to receive(:unsubscribe)
      expect(sub2).to receive(:unsubscribe)

      subject.unsubscribe_all
    end
  end
end
