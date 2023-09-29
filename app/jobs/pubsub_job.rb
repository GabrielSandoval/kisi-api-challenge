# frozen_string_literal: true

class PubsubJob < ApplicationJob
  queue_as :default
  retry_on StandardError, wait: 5.minutes, attempts: 3

  self.queue_adapter = :pubsub

  def perform(data)
    Rails.logger.info "[PubsubJob] Perform Now: #{data}"

    raise StandardError if rand < 0.2  # fail 20% of the time

    Pubsub.publish!(data)
  end

  before_enqueue do |job|
    # Before Enqueueing:
    # If it's already the 4th time, meaning the first,
    # second, and third try failed. Then push this
    # to :morgue queue
    if job.executions >= 2 && job.queue_name != :morgue
      job.queue_name = :morgue
    end
  end
end
