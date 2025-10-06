require 'thread'
require 'set'
require 'tempfile'
require 'stringio'
module Aws
  module S3
    # @api private
    class MultipartStreamUploader
      # api private
      DEFAULT_PART_SIZE = 5 * 1024 * 1024 # 5MB

      # api private
      DEFAULT_THREAD_COUNT = 10

      # api private
      TEMPFILE_PREFIX = 'aws-sdk-s3-upload_stream'.freeze

      # @api private
      CREATE_OPTIONS = Set.new(Client.api.operation(:create_multipart_upload).input.shape.member_names)

      # @api private
      UPLOAD_PART_OPTIONS = Set.new(Client.api.operation(:upload_part).input.shape.member_names)

      class MultipartStreamMemoryBuffer < StringIO
        def close
        end

        def unlink
        end
      end

      # @option options [Client] :client
      def initialize(bucket, key, client: Client.new, part_size: DEFAULT_PART_SIZE, thread_count: DEFAULT_THREAD_COUNT, tempfile: false,  **options)
        @bucket = bucket
        @key = key
        @client = client
        @tempfile = tempfile
        @part_size = part_size
        @thread_count = thread_count
        @options = options
      end

      # @return [Client]
      attr_reader :client

      # @return [void]
      def upload(&block)
        upload_id = @client.create_multipart_upload(@options.slice(*CREATE_OPTIONS).merge(bucket: @bucket, key: @key)).upload_id
        begin
          parts = upload_parts(upload_id, &block)
          @client.complete_multipart_upload(bucket: @bucket, key: @key, upload_id:, multipart_upload: { parts: })
        rescue StandardError
          @client.abort_multipart_upload(bucket: @bucket, key: @key, upload_id:)
          raise
        end
      end

      private

      def temporary_buffer_pipe
        io = @tempfile ? Tempfile.new(TEMPFILE_PREFIX) : MultipartStreamMemoryBuffer.new
        io.binmode
        io
      end

      def upload_parts(upload_id, &block)
        thread_parts = IO.pipe do |read_pipe, write_pipe|
          threads = upload_in_threads(read_pipe, upload_id)
          block.call(write_pipe)
          write_pipe.close
          threads.map(&:value)
        end
        thread_parts.flatten(1).sort_by { |part| part[:part_number] }
      end

      def upload_part(body, part_number, upload_id)
        @client.upload_part(@options.slice(*UPLOAD_PART_OPTIONS).merge(upload_id:, bucket: @bucket, key: @key, body:, part_number:))
      end

      def upload_in_threads(read_pipe, upload_id)
        part_number_counter = 0
        schedule_in_threads_with_mutex do |read_mutex|
          Enumerator.new do |yielder|
            loop do
              bytes_copied = 0
              body, part_number = read_mutex.synchronize do
                temp_io = temporary_buffer_pipe
                bytes_copied = IO.copy_stream(read_pipe, temp_io, @part_size)
                temp_io.rewind
                temp_io.truncate(bytes_copied)
                if bytes_copied.zero?
                  temp_io.close
                  temp_io.unlink
                else
                  [temp_io, part_number_counter += 1]
                end
              end
              break if bytes_copied.zero?
              response = upload_part(body, part_number, upload_id)
              yielder << {etag: response.etag, part_number:}
            ensure
              body&.close
              body&.unlink
            end
          end.to_a
        end
      end

      def schedule_in_threads_with_mutex
        mutex = Mutex.new
        @thread_count.times.map do
          thread = Thread.new do
            Thread.current.abort_on_exception = true
            yield(mutex)
          end
        end
      end
    end
  end
end
