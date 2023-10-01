# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    class PubsubAdapter
      # Enqueue a job to be performed.
      # @param [ActiveJob::Base] job The job to be performed.
      def enqueue(job)
        Rails.logger.info("[PubsubAdapter] Enqueue: #{job.arguments[0]}")
        Base.execute(job.serialize)
      end

      # Enqueue a job to be performed at a certain time.
      # @param [ActiveJob::Base] job The job to be performed.
      # @param [Float] timestamp The time to perform the job.
      def enqueue_at(job, timestamp)
        Rails.logger.info("[PubsubAdapter] Enqueue At: #{job.arguments[0]} - #{Time.at(timestamp)}")
        delay = timestamp - Time.current.to_f

        if delay.positive?
          Concurrent::ScheduledTask.execute(delay) { Base.execute(job.serialize) }
        else
          Base.execute(job.serialize)
        end
      end
    end
  end
end
