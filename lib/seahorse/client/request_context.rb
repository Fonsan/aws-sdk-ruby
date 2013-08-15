# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

require 'stringio'

module Seahorse
  module Client
    class RequestContext

      # @option options [String] :operation_name (nil)
      # @option options [Hash] :params ({})
      # @option options [Configuration] :config (nil)
      # @option options [Http::Request] :http_request (Http::Request.new)
      # @option options [Http::Response] :http_response (Http::Response.new)
      #   and #rewind.
      def initialize(options = {})
        @operation_name = options[:operation_name]
        @params = options[:params] || {}
        @config = options[:config]
        @http_request = options[:http_request] || Http::Request.new
        @http_response = options[:http_response] || Http::Response.new
        @metadata = {}
      end

      # @return [String] Name of the API operation called.
      attr_accessor :operation_name

      def operation
        @operation ||= config.api.operations[operation_name]
      end

      # @return [Hash] The hash of request parameters.
      attr_accessor :params

      # @return [Configuration] The client configuration.
      attr_accessor :config

      # @return [Http::Request]
      attr_accessor :http_request

      # @return [Http::Response]
      attr_accessor :http_response

      # Returns the metadata for the given `key`.
      # @param [Symbol] key
      # @return [Object]
      def [](key)
        @metadata[key]
      end

      # Sets the request context metadata for the given `key`.  Request metadata
      # useful for handlers that need to keep state on the request, without
      # sending that data with the request over HTTP.
      # @param [Symbol] key
      # @param [Object] value
      def []=(key, value)
        @metadata[key] = value
      end

    end
  end
end
