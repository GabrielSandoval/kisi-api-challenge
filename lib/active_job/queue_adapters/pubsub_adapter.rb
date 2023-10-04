# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    class PubsubAdapter
      # Enqueue a job to be performed.
      # @param [ActiveJob::Base] job The job to be performed.
      def enqueue(job)
        # Rails.logger.info("[PubsubAdapter] Enqueue: #{job.arguments[0]}")
        Pubsub.publish!(job.serialize.to_json)
      end

      # Enqueue a job to be performed at a certain time.
      # @param [ActiveJob::Base] job The job to be performed.
      # @param [Float] timestamp The time to perform the job.
      def enqueue_at(job, timestamp)
        data = job.arguments[0].stringify_keys
        # puts("[PubsubAdapter] Enqueue At: #{data} - #{timestamp}")

        data[:execute_at] = timestamp
        job.arguments[0] = data

        puts("[PubsubAdapter] FAILED (ID: #{data["id"]}) - retrying at #{timestamp}")
        Pubsub.publish!(job.serialize.to_json, data)
      end
    end
  end
end
