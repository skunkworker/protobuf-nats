require "securerandom"
require "thread"
require "nats/client" # Using the real NATS::Msg for accuracy

class FakeNatsClient
  attr_reader :subscriptions, :published_messages

  def initialize(options = {})
    @inbox_base = options[:inbox] || "_INBOX.FAKE"
    @inbox_id = 0
    @subscriptions = {}
    @replies = []
    @published_messages = []
  end

  def connect(*)
    # No-op
  end

  def new_inbox
    @inbox_id += 1
    "#{@inbox_base}.#{@inbox_id}"
  end

  # This is the trigger. When the SUT calls publish, we send our fake replies.
  def publish(subject, data, reply_to = nil)
    @published_messages << { :subject => subject, :data => data, :reply_to => reply_to }
    return unless reply_to

    # Find the subscriber that is listening for this reply.
    matching_subject = subscriptions.keys.find do |subscribed_subject|
      next unless subscribed_subject.include?("*")
      regex = Regexp.new("^" + subscribed_subject.gsub("*", "[^.]+") + "$")
      regex.match?(reply_to)
    end
    return unless matching_subject
    subscription = subscriptions[matching_subject][:subscription]
    return unless subscription.pending_queue

    # Deliver all pre-configured replies to the subscriber's queue.
    @replies.each do |reply_data|
      message = NATS::Msg.new(:subject => reply_to, :data => reply_data)
      subscription.pending_queue.push(message)
    end
  end

  def flush
    # No-op
  end

  def subscribe(subject, _args = {}, &block)
    sub = ::NATS::Subscription.new
    sub.pending_queue = ::SizedQueue.new(1024)
    subscriptions[subject] = { :subscription => sub }
    sub
  end

  def unsubscribe(*)
    # No-op
  end

  # Test setup method: tell the fake what to reply with.
  def will_reply_with(*messages)
    @replies.push(*messages)

    puts "@replies: #{@replies}"
  end

  # DEPRECATED: This is kept temporarily but should be removed.
  def schedule_messages(messages)
    @replies.push(*messages.map(&:data))
  end
end
