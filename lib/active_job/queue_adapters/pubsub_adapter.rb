# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    class PubsubAdapter
      # Enqueue a job to be performed.
      # @param [ActiveJob::Base] job The job to be performed.
      def enqueue(job)
        data = job.arguments[0]
        Rails.logger.info "[PubsubAdapter] Enqueue: #{data}"

        Pubsub.publish!(data)
      end

      # Enqueue a job to be performed at a certain time.
      # @param [ActiveJob::Base] job The job to be performed.
      # @param [Float] timestamp The time to perform the job.
      def enqueue_at(job, timestamp)
        raise NotImplementedError, "Check gcloud scheduler jobs create pubsub"
      end
    end
  end
end
