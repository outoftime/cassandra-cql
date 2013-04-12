=begin
Copyright 2011 Inside Systems, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
=end

module CassandraCQL
  module Error
    class InvalidRequestException < Exception; end
  end

  class Database
    attr_reader :connection, :schema, :keyspace

    def initialize(servers, options={}, thrift_client_options={})
      @options = {
        :keyspace => 'system'
      }.merge(options)

      @thrift_client_options = {
        :exception_class_overrides => CassandraCQL::Thrift::InvalidRequestException,
        :connect_timeout => 5
      }.merge(thrift_client_options)

      @keyspace = @options[:keyspace]
      @cql_version = @options[:cql_version]
      @use_cql3_query =
        (@cql_version.nil? || @cql_version.split('.').first.to_i >= 3) &&
        CassandraCQL::Thrift::Client.method_defined?(:execute_cql3_query)

      if @use_cql3_query
        self.consistency = @options.fetch(:consistency, :quorum)
      end

      @servers = servers
      connect!
    end

    def use_cql3_query?
      @use_cql3_query
    end

    def connect!
      @connection = ThriftClient.new(CassandraCQL::Thrift::Client, @servers, @thrift_client_options)

      if @options[:username] and @options[:password]
        login!(@options[:username], @options[:password])
      end

      obj = self
      @connection.add_callback(:post_connect) do
        if @connection.describe_version >= '19.35.0' && (!@cql_version || @cql_version >= '3.0.0')
          @use_cql3_query = true
        elsif @cql_version
          @use_cql3_query = false
          @connection.set_cql_version(@cql_version)
        else
          @use_cql3_query = false
        end
        @connection.login(@auth_request) if @auth_request
        execute("USE #{@keyspace}")
      end
      @connection.connect!
    end

    def disconnect!
      @connection.disconnect! if active?
    end

    def active?
      # TODO: This should be replaced with a CQL call that doesn't exist yet
      @connection.describe_version
      true
    rescue Exception
      false
    end
    alias_method :ping, :active?

    def reset!
      disconnect!
      connect!
    end
    alias_method :reconnect!, :reset!

    def statement_class
      return @statement_class if @statement_class

      version_module = 'V' + CassandraCQL.CASSANDRA_VERSION.gsub('.', '')
      return @statement_class = CassandraCQL.const_get(version_module).const_get(:Statement)
    end

    def prepare(statement, options={}, &block)
      stmt = statement_class.new(self, statement)
      if block_given?
        yield stmt
      else
        stmt
      end
    end

    def execute(statement, *bind_vars)
      result = statement_class.new(self, statement).execute(bind_vars)
      if block_given?
        yield result
      else
        result
      end
    rescue CassandraCQL::Thrift::InvalidRequestException
      raise Error::InvalidRequestException.new($!.why)
    end

    def execute_cql_query(cql, compression=CassandraCQL::Thrift::Compression::NONE)
      if use_cql3_query?
        @connection.execute_cql3_query(cql, compression, consistency)
      else
        @connection.execute_cql_query(cql, compression)
      end
    rescue CassandraCQL::Thrift::InvalidRequestException
      raise Error::InvalidRequestException.new($!.why)
    end

    def keyspace=(ks)
      @keyspace = (ks.nil? ? nil : ks.to_s)
    end

    def keyspaces
      # TODO: This should be replaced with a CQL call that doesn't exist yet
      @connection.describe_keyspaces.map { |keyspace| Schema.new(keyspace) }
    end

    def schema
      # TODO: This should be replaced with a CQL call that doesn't exist yet
      Schema.new(@connection.describe_keyspace(@keyspace))
    end

    def login!(username, password)
      request = CassandraCQL::Thrift::AuthenticationRequest.new
      request.credentials = {'username' => username, 'password' => password}
      ret = @connection.login(request)
      # To avoid a double login on the initial connect, we set
      # @auth_request after the first successful login.
      @auth_request = request
      ret
    end

    def with_consistency(level)
      previous_level = @consistency_name
      self.consistency = level
      yield
      self.consistency = previous_level
    end

    def consistency
      @consistency ||= get_consistency(:quorum)
    end

    def consistency=(level)
      @consistency_name = level
      @consistency = get_consistency(level)
    end

    private

    def get_consistency(level)
      level_const = level.to_s.upcase

      if CassandraCQL::Thrift::ConsistencyLevel.const_defined?(level_const)
        CassandraCQL::Thrift::ConsistencyLevel.const_get(level_const)
      else
        raise ArgumentError.new("Invalid consistency level")
      end
    end
  end
end
