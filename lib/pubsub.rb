# frozen_string_literal: true

require("google/cloud/pubsub")

class Pubsub
  def self.listen!
    new.listen!
  end

  def self.publish!(message, attributes = {})
    new.publish!(message, attributes)
  end

  def publish!(message, attributes = {})
    topic.publish(message, attributes)
  end

  def listen!
    puts("Listening to #{subscription.name}")

    subscriber = subscription.listen do |received_message|
      serialized_job, _job_args, attributes = parse_received_message(received_message)

      # puts("[Pubsub] Received (ID: #{_job_args["id"]})")
      received_message.acknowledge!

      execute_at = attributes.fetch("execute_at", nil)
      if execute_at.present? && (execute_at.to_f - Time.current.to_f).positive?
        # puts("[Pubsub] Rotating (ID: #{_job_args["id"]})")
        Pubsub.publish!(serialized_job.to_json, attributes)
      else
        ActiveJob::Base.execute(serialized_job)
      end
    end

    subscriber.on_error do |exception|
      puts("[Pubsub] Exception: #{exception.class} #{exception.message}")
    end

    at_exit { subscriber.stop!(10) }

    subscriber.start
  end

  private

  def parse_received_message(received_message)
    data = received_message.message.data

    job = JSON.parse(data)
    job_args = job.dig("arguments", 0)
    attributes = received_message.message.attributes.stringify_keys

    [job, job_args, attributes]
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
