/*
  Copyright (c) 2014-2016 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "constants.hpp"
#include "serialization.hpp"
#include "token_metadata_impl.hpp"

#include "third_party/mt19937_64/mt19937_64.hpp"

#include <boost/test/unit_test.hpp>

#include <limits>
#include <sstream>
#include <string>
#include <stdarg.h>

#define CASS_PROTOCOL_VERSION 3

class BufferBuilder {
public:
  char* data() const {
    return const_cast<char*>(buffer_.data());
  }

  size_t size() const {
    return buffer_.size();
  }

  template<class T>
  void append(T value) {
    std::string buffer(size_of(value), 0);
    encode(&buffer[0], value);
    buffer_.append(buffer);
  }

  template<class T>
  void append_value(T value) {
    append<int32_t>(size_of(value));
    append<T>(value);
  }

  void append_string(const std::string& str) {
    append<uint16_t>(str.size());
    append(str);
  }

  template<class T>
  void encode_at(size_t index, T value) {
    assert(index < buffer_.size() && index + size_of(value) < buffer_.size());
    encode(&buffer_[index], value);
  }

private:
  static size_t size_of(uint16_t value) {
    return sizeof(int16_t);
  }

  static size_t size_of(int32_t value) {
    return sizeof(int32_t);
  }

  static size_t size_of(int64_t value) {
    return sizeof(int64_t);
  }

  static size_t size_of(const std::string& value) {
    return value.size();
  }

  static void encode(char* buf, uint16_t value) {
    cass::encode_uint16(buf, value);
  }

  static void encode(char* buf, int32_t value) {
    cass::encode_int32(buf, value);
  }

  static void encode(char* buf, int64_t value) {
    cass::encode_int64(buf, value);
  }

  static void encode(char* buf, const std::string& value) {
    std::copy(value.begin(), value.end(), buf);
  }

private:
  std::string buffer_;
};

typedef std::map<std::string, std::string> ReplicationMap;

struct ColumnMetadata {
  ColumnMetadata(const std::string& name, const cass::DataType::ConstPtr& data_type)
    : name(name)
    , data_type(data_type) { }
  std::string name;
  cass::DataType::ConstPtr data_type;
};

typedef std::vector<ColumnMetadata> ColumnMetadataVec;

class RowResultResponseBuilder : public BufferBuilder {
public:
  RowResultResponseBuilder(const ColumnMetadataVec& column_metadata)
    : row_count_(0) {
    append<cass_int32_t>(CASS_RESULT_KIND_ROWS); // Kind
    append<cass_int32_t>(CASS_RESULT_FLAG_GLOBAL_TABLESPEC); // Flags
    append<cass_int32_t>(column_metadata.size()); // Column count
    append_string("keyspace");
    append_string("table");

    for (ColumnMetadataVec::const_iterator i = column_metadata.begin(),
          end = column_metadata.end(); i != end; ++i) {
      append_column_metadata(*i);
    }

    row_count_index_ = size();
    append<cass_int32_t>(0); // Row count (updated later)
  }

  void append_keyspace_row_v3(const std::string& keyspace_name,
                              const ReplicationMap& replication) {
    append_value<std::string>(keyspace_name);

    size_t size = sizeof(int32_t);
    for (ReplicationMap::const_iterator i = replication.begin(),
         end = replication.end(); i != end; ++i) {
      size += sizeof(int32_t) + i->first.size();
      size += sizeof(int32_t) + i->second.size();
    }

    append<cass_int32_t>(size);
    append<cass_int32_t>(replication.size()); // Element count
    for (ReplicationMap::const_iterator i = replication.begin(),
         end = replication.end(); i != end; ++i) {
      append_value<std::string>(i->first);
      append_value<std::string>(i->second);
    }

    ++row_count_;
  }

  void append_keyspace_row_v2(const std::string& keyspace_name,
                              const std::string& strategy_class,
                              const std::string& strategy_options) {
    append_value<std::string>(keyspace_name);
    append_value<std::string>(strategy_class);
    append_value<std::string>(strategy_options);

    ++row_count_;
  }

  void append_column_metadata(const ColumnMetadata& metadata) {
    append_string(metadata.name);
    append_data_type(metadata.data_type);
  }

  void append_data_type(const cass::DataType::ConstPtr& data_type) {
    append<uint16_t>(data_type->value_type());

    switch (data_type->value_type()) {
      case CASS_VALUE_TYPE_LIST:
      case CASS_VALUE_TYPE_SET:
        append_data_type(cass::CollectionType::ConstPtr(data_type)->types()[0]);
        break;
      case CASS_VALUE_TYPE_MAP:
        append_data_type(cass::CollectionType::ConstPtr(data_type)->types()[0]);
        append_data_type(cass::CollectionType::ConstPtr(data_type)->types()[1]);
        break;
      case CASS_VALUE_TYPE_TUPLE:
      case CASS_VALUE_TYPE_UDT:
        assert(false && "Tuples and UDTs are not supported");
        break;
      default:
        break;
    }
  }

  void finish() {
    encode_at(row_count_index_, row_count_);
  }

private:
  size_t row_count_index_;
  int32_t row_count_;
};

class TokenCollectionBuilder : public BufferBuilder {
public:
  TokenCollectionBuilder()
    : count_(0) {
    append<cass_int32_t>(0); // Element count (updated later)
  }

  void append_murmur3_token(cass::Murmur3Partitioner::Token token) {
    std::stringstream ss;
    ss << token;
    append_value<std::string>(ss.str());
    ++count_;
  }

  void append_md5_token(cass::RandomPartitioner::Token token) {
    ++count_;
  }

  void append_byte_ordered_token(cass::ByteOrderedPartitioner::Token token) {
    //append_value<cass::ByteOrderedPartitioner::Token>(token);
    ++count_;
  }

  void finish() {
    encode_at(0, count_);
  }

private:
  //static size_t size_of(cass::ByteOrderedPartitioner::Token value) {
  //  return value.size();
  //}

  //static void encode(char* buf, cass::ByteOrderedPartitioner::Token value) {
  //  memcpy(buf, value.data(), value.size());
  //}

  private:
  int32_t count_;
};

void add_keyspace_network_topology(const std::string& keyspace_name,
                                   ReplicationMap& replication,
                                   cass::TokenMetadata* token_metadata) {

  cass::DataType::ConstPtr varchar_data_type(new cass::DataType(CASS_VALUE_TYPE_VARCHAR));

  ColumnMetadataVec column_metadata;
  column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
  column_metadata.push_back(ColumnMetadata("replication", cass::CollectionType::map(varchar_data_type, varchar_data_type, true)));
  RowResultResponseBuilder builder(column_metadata);

  replication["class"] = CASS_NETWORK_TOPOLOGY_STRATEGY;
  builder.append_keyspace_row_v3(keyspace_name, replication);
  builder.finish();

  cass::ResultResponse result;
  result.decode(CASS_PROTOCOL_VERSION, builder.data(), builder.size());
  result.decode_first_row();
  token_metadata->add_keyspaces(cass::VersionNumber(3, 0, 0), &result);
}

void add_murmur3_host(const cass::Host::Ptr& host,
                      MT19937_64& rng,
                      size_t num_tokens,
                      cass::TokenMetadata* token_metadata) {
  TokenCollectionBuilder builder;
  for (size_t i = 0;  i < num_tokens; ++i) {
    builder.append_murmur3_token(rng());
  }
  builder.finish();


  cass::CollectionType::ConstPtr data_type(cass::CollectionType::list(cass::DataType::ConstPtr(new cass::DataType(CASS_VALUE_TYPE_VARINT)), false));
  cass::Value value(CASS_PROTOCOL_VERSION, data_type, builder.data(), builder.size());
  token_metadata->add_host(host, &value);
}

cass::Host::Ptr create_host(const std::string& address,
                            const std::string& rack,
                            const std::string& dc) {
  cass::Host::Ptr host(new cass::Host(cass::Address(address, 4092), false));
  host->set_rack_and_dc(rack, dc);
  return host;
}

BOOST_AUTO_TEST_SUITE(token_metadata)

BOOST_AUTO_TEST_CASE(murmur3)
{
  cass::ScopedPtr<cass::TokenMetadata> token_metadata(cass::TokenMetadata::from_partitioner(CASS_MURMUR3_PARTITIONER));
  MT19937_64 rng;

  uint64_t start = uv_hrtime();
  char buf[128];
  for (int i = 0; i < 10; ++i) {
    sprintf(buf, "127.0.%d.%d", i / 255, (i % 255 + 1));
    add_murmur3_host(create_host(buf, "rack1", "dc1"), rng, 256, token_metadata.get());
  }
  //add_murmur3_host(create_host("127.0.0.2", "rack1", "dc1"), rng, 2048, token_metadata.get());
  //add_murmur3_host(create_host("127.0.0.3", "rack1", "dc1"), rng, 2048, token_metadata.get());
  //add_murmur3_host(create_host("127.0.0.4", "rack1", "dc1"), rng, 2048, token_metadata.get());
  //add_murmur3_host(create_host("127.0.0.5", "rack1", "dc1"), rng, 2048, token_metadata.get());
  //add_murmur3_host(create_host("127.0.0.6", "rack1", "dc1"), rng, 2048, token_metadata.get());

  ReplicationMap replication;
  replication["dc1"] = "3";
  //replication["dc2"] = "3"; // TODO: This causes havok if the no hosts with dc2 exist
  add_keyspace_network_topology("ks1", replication, token_metadata.get());

  token_metadata->build();
  uint64_t elapsed = uv_hrtime() - start;

  // HERE
  //token_metadata->get_replicas("ks1", )

  printf("Elapsed: %f ms\n", (double)elapsed / (1000.0 * 1000.0));
}

BOOST_AUTO_TEST_SUITE_END()
