# frozen_string_literal: true

require("google/cloud/pubsub")

class Pubsub
  def self.listen!
    new.listen!
  end

  def self.publish!(message)
    new.publish!(message)
  end

  def publish!(message)
    puts "[Pubsub] Publishing #{message}"
    topic.publish(message)
  end

  def listen!
    puts "Listening to #{subscription.name}"

    subscriber = subscription.listen do |received_message|
      data = received_message.message.attributes
      handle_message(data)
      received_message.acknowledge!
    end

    subscriber.on_error do |exception|
      puts "[Pubsub] Exception: #{exception.class} #{exception.message}"
    end

    at_exit do
      subscriber.stop!(10)
    end

    subscriber.start
  end

  private

  def handle_message(data)
    puts "[Pubsub] Received (ID: #{data["id"]})"

    start = Time.current
    sleep(data["execution_time"].to_f)
    execution_time = (Time.current - start).round(2)
    puts "[Pubsub] Finished processing (ID: #{data["id"]}) - #{execution_time}s"
  end

  def topic
    @topic ||= client.topic(topic_name) || client.create_topic(topic_name)
  end

  def subscription
    @subscription ||= client.subscription(subscription_name) || topic.create_subscription(subscription_name)
  end

  def topic_name
    config[:topic_name]
  end

  def subscription_name
    config[:subscription_name]
  end

  def config
    Rails.application.config_for(:pubsub)
  end

  # Create a new client.
  # @return [Google::Cloud::PubSub]
  def client
    @client ||= Google::Cloud::PubSub.new(
      project_id: config[:project_id],
      emulator_host: config[:emulator_host]
    )
  end
end
