# frozen_string_literal: true

class PubsubJob < ApplicationJob
  retry_on(StandardError, wait: 5.minutes, attempts: 4)

  def perform(data)
    data = data.stringify_keys
    Rails.logger.info("[PubsubJob] Perform Now: #{data}")

    raise(StandardError) if rand < 0.2 # fail 20% of the time

    sleep(data["execution_time"].to_f)
    puts("[Pubsub] Finished processing (ID: #{data["id"]}) - #{data["execution_time"]}s")
  end
end
