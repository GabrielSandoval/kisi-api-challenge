# frozen_string_literal: true

class PubsubJob < ApplicationJob
  class CustomError < RuntimeError; end

  queue_as :default
  retry_on CustomError, wait: 5.minutes, attempts: 4

  self.queue_adapter = :pubsub

  def perform(data)
    Rails.logger.info "[PubsubJob] Perform Now: #{data}"

    Pubsub.publish!(data)
  end

  before_enqueue do |job|
    # Before Enqueueing:
    # If it's already the 4th time, meaning the first,
    # second, and third try failed. Then push this
    # to :morgue queue
    if job.executions >= 3 && job.queue_name != :morgue
      job.queue_name = :morgue
    end
  end
end
