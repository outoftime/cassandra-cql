require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "Database" do
  before do
    @connection = setup_cassandra_connection
  end

  describe "reset!" do
    it "should create a new connection" do
      @connection.should_receive(:connect!)
      @connection.reset!
    end
  end

  describe "login!" do
    it "should call login! on connection" do
      creds = { 'username' => 'myuser', 'password' => 'mypass' }
      @connection.connection.should_receive(:login) do |auth|
        auth.credentials.should eq(creds)
      end
      @connection.login!(creds['username'], creds['password'])
    end
  end

  describe "setting the consistency level" do
    context "with CQL3", :cql_version => "3.0.0" do
      it "should return the current consistency" do
        @connection.consistency.should == CassandraCQL::Thrift::ConsistencyLevel::QUORUM
      end

      it "should change the consistency" do
        @connection.consistency = :all
        @connection.consistency.should == CassandraCQL::Thrift::ConsistencyLevel::ALL
      end

      it "should change the consistency within a block" do
        @connection.with_consistency(:one) do
          @connection.consistency.should == CassandraCQL::Thrift::ConsistencyLevel::ONE
        end

        @connection.consistency.should == CassandraCQL::Thrift::ConsistencyLevel::QUORUM
      end

      it "should raise and error if invalid consistency is used" do
        expect { @connection.consistency = :foo }.to raise_error(ArgumentError)
      end
    end
  end
end
