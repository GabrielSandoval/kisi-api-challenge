# frozen_string_literal: true

namespace(:publisher) do
  desc("Enqueues jobs to GCP")
  task(run: :environment) do
    puts("Enqueueing jobs...")

    50.times do |i|
      PubsubJob.perform_later({
        id: i,
        execution_time: rand(0..5.0).round(2),
        failure: rand < 0.2, # 20% will fail
      })
    end
  end
end
