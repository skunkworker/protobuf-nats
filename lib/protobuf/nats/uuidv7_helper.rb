module Protobuf
  module Nats
    class UUIDv7Helper
      # Strict RFC 9562 UUIDv7 shape, matching .generate's output. A
      # non-UUID token read as a timestamp gives callers a garbage age.
      UUIDV7_REGEX = /\A\h{8}-\h{4}-7\h{3}-\h{4}-\h{12}\z/

      # Same shape without dashes. extract_timestamp accepts this form too.
      UUIDV7_COMPACT_REGEX = /\A\h{12}7\h{3}\h{4}\h{12}\z/

      # Generates a UUIDv7 without a CSPRNG. Callers only need the 48-bit
      # ms timestamp prefix plus enough randomness for uniqueness.
      # SecureRandom's gen_random dominated per-request CPU and garbage
      # (measured ~6.8us/op, 4 GC-triggering allocations); a per-thread
      # non-cryptographic Random halves both. Layout still matches RFC 9562.
      #
      # @return [String] a UUIDv7 string (e.g. "01234567-89ab-7def-8123-456789abcdef")
      def self.generate
        unix_ts_ms = ::Process.clock_gettime(::Process::CLOCK_REALTIME, :millisecond) & 0xffffffffffff
        rng = (::Thread.current[:pb_nats_uuid_rng] ||= ::Random.new)
        format(
          "%08x-%04x-%04x-%04x-%04x%08x",
          (unix_ts_ms >> 16) & 0xffffffff,   # high 32 bits of ms timestamp
          unix_ts_ms & 0xffff,               # low 16 bits of ms timestamp
          (0x7000 | rng.rand(0x1000)),       # version 7 + 12 random bits
          (0x8000 | rng.rand(0x4000)),       # RFC 4122 variant + 14 random bits
          rng.rand(0x10000),                 # 16 random bits
          rng.rand(0x100000000)              # 32 random bits
        )
      end

      # Extracts the Unix timestamp (seconds) from a UUIDv7 string.
      #
      # Validates the whole token. String#to_i(16) stops at the first
      # non-hex character and returns 0 instead of raising. A non-UUID
      # token used to parse as epoch 0, reporting a ~56-year age into the
      # client.unexpected_message gauge.
      #
      # @param uuid [String] a UUIDv7 string
      # @return [Time, nil] the embedded timestamp, or nil if parsing fails
      def self.extract_timestamp(uuid)
        return nil unless uuid.is_a?(String)
        return nil unless uuid.match?(UUIDV7_REGEX) || uuid.match?(UUIDV7_COMPACT_REGEX)

        # First 48 bits (12 hex chars) are the Unix timestamp in ms.
        uuid_bytes = uuid.tr('-', '')

        timestamp_ms = uuid_bytes[0, 12].to_i(16)
        Time.at(timestamp_ms / 1000.0)
      rescue
        nil
      end

      # Calculates the age of a UUIDv7 in seconds.
      #
      # @param uuid [String] a UUIDv7 string
      # @param current_time [Time] time to compare against (default: now)
      # @return [Float, nil] the age in seconds, or nil if parsing fails
      def self.age_in_seconds(uuid, current_time: Time.now)
        timestamp = extract_timestamp(uuid)
        return nil unless timestamp

        current_time - timestamp
      end

      # Age (integer ms) of a strictly-validated UUIDv7 token, or nil for a
      # non-UUIDv7 token. Allocation-light; runs per message on server intake.
      def self.age_ms(token)
        return nil unless token.is_a?(String) && token.match?(UUIDV7_REGEX)
        unix_ts_ms = (token[0, 8].to_i(16) << 16) | token[9, 4].to_i(16)
        ::Process.clock_gettime(::Process::CLOCK_REALTIME, :millisecond) - unix_ts_ms
      end
    end
  end
end
