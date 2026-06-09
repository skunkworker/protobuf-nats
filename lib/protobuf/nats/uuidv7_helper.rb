module Protobuf
  module Nats
    class UUIDv7Helper
      # Extract the Unix timestamp (in seconds) from a UUIDv7 string
      # Returns nil if the UUID cannot be parsed
      #
      # @param uuid [String] A UUIDv7 string (e.g., "01234567-89ab-7def-0123-456789abcdef")
      # @return [Time, nil] The timestamp embedded in the UUID, or nil if parsing fails
      def self.extract_timestamp(uuid)
        return nil unless uuid.is_a?(String)

        # UUIDv7 format: first 48 bits (12 hex chars) are Unix timestamp in milliseconds
        # Remove dashes and extract the timestamp portion
        uuid_bytes = uuid.gsub('-', '')
        return nil if uuid_bytes.length < 12

        timestamp_ms = uuid_bytes[0...12].to_i(16)
        Time.at(timestamp_ms / 1000.0)
      rescue => e
        nil
      end

      # Calculate the age of a UUIDv7 in seconds
      # Returns nil if the UUID cannot be parsed
      #
      # @param uuid [String] A UUIDv7 string
      # @param current_time [Time] The time to compare against (defaults to Time.now)
      # @return [Float, nil] The age in seconds, or nil if parsing fails
      def self.age_in_seconds(uuid, current_time: Time.now)
        timestamp = extract_timestamp(uuid)
        return nil unless timestamp

        current_time - timestamp
      end
    end
  end
end
