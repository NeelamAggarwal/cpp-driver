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

#ifndef __CASS_TOKEN_METADATA_IMPL_HPP_INCLUDED__
#define __CASS_TOKEN_METADATA_IMPL_HPP_INCLUDED__

#include "collection_iterator.hpp"
#include "map_iterator.hpp"
#include "result_iterator.hpp"
#include "result_response.hpp"
#include "row.hpp"
#include "string_ref.hpp"
#include "token_metadata.hpp"
#include "value.hpp"

#include "third_party/rapidjson/rapidjson/document.h"

#include <sparsehash/dense_hash_map>

#include <assert.h>
#include <algorithm>
#include <queue>
#include <vector>

#define CASS_NETWORK_TOPOLOGY_STRATEGY "NetworkTopologyStrategy"
#define CASS_SIMPLE_STRATEGY           "SimpleStrategy"

namespace  std {

template<>
struct hash<cass::Host::Ptr> {
  std::size_t operator()(const cass::Host::Ptr& host) const {
    if (!host) return 0;
    return hash(host->address());
  }
  std::hash<cass::Address> hash;
};

template<>
struct equal_to<cass::Host::Ptr> {
  bool operator()(const cass::Host::Ptr& lhs, const cass::Host::Ptr& rhs) const {
    if (lhs == rhs) {
      return true;
    }
    if (!lhs || !rhs) {
      return false;
    }
    return lhs->address() == rhs->address();
  }
};

} // namespace std

namespace cass {

class IdGenerator {
public:
  typedef sparsehash::dense_hash_map<std::string, uint32_t> IdMap;

  IdGenerator() {
    ids_.set_empty_key(std::string());
  }

  uint32_t get(const std::string& key) {
    if (key.empty()) {
      return 0;
    }

    IdMap::const_iterator i = ids_.find(key);
    if (i != ids_.end()) {
      return i->second;
    }

    uint32_t id = ids_.size() + 1;
    ids_[key] = id;
    return id;
  }

private:
  IdMap ids_;
};

struct Murmur3Partitioner {
  typedef int64_t Token;

  static Token get_token(const StringRef& str);
  static Token hash(const StringRef& str);
};

struct RandomPartitioner {
  struct Token {
    uint64_t hi;
    uint64_t lo;

    bool operator<(const Token& other) const {
      return hi < other.hi || lo < other.lo;
    }

    bool operator==(const Token& other) const {
      return hi == other.hi && lo == other.lo;
    }
  };

  static Token get_token(const StringRef& str);
  static Token hash(const StringRef& str);
};

class ByteOrderedPartitioner {
public:
  typedef std::vector<uint8_t> Token;

  static Token get_token(const StringRef& str);
  static Token hash(const StringRef& str);
};

class RackSet : public sparsehash::dense_hash_set<uint32_t> {
public:
  RackSet() {
    set_empty_key(0);
    set_deleted_key(CASS_UINT32_MAX);
  }
};

class DcRackMap : public sparsehash::dense_hash_map<uint32_t, RackSet> {
public:
  DcRackMap() {
    set_empty_key(0);
    set_deleted_key(CASS_UINT32_MAX);
  }
};

class ReplicationFactorMap : public sparsehash::dense_hash_map<uint32_t, size_t> {
public:
  ReplicationFactorMap() {
    set_empty_key(0);
  }
};

template <class Partitioner>
class ReplicationStrategy {
public:
  typedef typename Partitioner::Token Token;

  typedef std::pair<Token, Host::Ptr> TokenHost;
  typedef std::vector<TokenHost> TokenHostVec;

  typedef std::pair<Token, CopyOnWriteHostVec> TokenReplicas;
  typedef std::vector<TokenReplicas> TokenReplicasVec;

  typedef std::deque<typename TokenHostVec::const_iterator> TokenHostQueue; // TODO(mpenick): Make this not a std::deque<>

  class SkippedEndpoints {
  public:
    typedef typename TokenHostVec::const_iterator Iterator;

    SkippedEndpoints()
      : pos_(0) { }

    void clear() {
      pos_ = 0;
      skipped_.clear();
    }

    bool empty() const {
      return skipped_.empty() || pos_ == skipped_.size();
    }

    void push_back(Iterator it) {
      skipped_.push_back(it);
    }

    Iterator pop_front() {
      assert(!empty());
      return skipped_[pos_++];
    }

  private:
    size_t pos_;
    std::vector<Iterator> skipped_;
  };

  class DcSkippedEndpointsMap : public sparsehash::dense_hash_map<uint32_t, SkippedEndpoints> {
  public:
    DcSkippedEndpointsMap() {
      sparsehash::dense_hash_map<uint32_t, SkippedEndpoints>::set_empty_key(0);
    }
  };

  enum Type {
    NETWORK_TOPOLOGY_STRATEGY,
    SIMPLE_STRATEGY,
    NON_REPLICATED
  };

  ReplicationStrategy()
    : type_(NON_REPLICATED)
    , total_replication_factor_(0) { }

  void init(IdGenerator& dc_ids,
            const VersionNumber& cassandra_version,
            const Row* row);

  bool operator!=(const ReplicationStrategy& other) const {
    return type_ != other.type_ ||
                    replication_factors_ != other.replication_factors_;
  }

  void build_replicas(const TokenHostVec& tokens, const DcRackMap& dc_racks,
                      TokenReplicasVec& result) const;

private:
  void build_replicas_network_topology(const TokenHostVec& tokens, const DcRackMap& dc_racks,
                                       TokenReplicasVec& result) const;
  void build_replicas_simple(const TokenHostVec& tokens, const DcRackMap& dc_racks,
                             TokenReplicasVec& result) const ;
  void build_replicas_non_replicated(const TokenHostVec& tokens, const DcRackMap& dc_racks,
                                     TokenReplicasVec& result) const;

private:
  Type type_;
  size_t total_replication_factor_;
  ReplicationFactorMap replication_factors_;
};

template <class Partitioner>
void ReplicationStrategy<Partitioner>::init(IdGenerator& dc_ids,
                                            const VersionNumber& cassandra_version,
                                            const Row* row) {
  StringRef strategy_class;

  if (cassandra_version >= VersionNumber(3, 0, 0)) {
    const Value* value = row->get_by_name("replication");
    if (value &&  value->is_map() &&
        is_string_type(value->primary_value_type()) &&
        is_string_type(value->secondary_value_type())) {
      MapIterator iterator(value);
      while (iterator.next()) {
        std::string key(iterator.key()->to_string());
        if (key == "class") {
          strategy_class = iterator.value()->to_string_ref();
        } else {
          std::string value(iterator.value()->to_string());
          size_t replication_factor = strtoul(value.c_str(), NULL, 10);
          if (replication_factor > 0) {
            if (key == "replication_factor"){
              total_replication_factor_ = replication_factor;
            } else {
              total_replication_factor_ += replication_factor;
              replication_factors_[dc_ids.get(key)] =  replication_factor;
            }
          } else {
            LOG_WARN("Replication factor of 0 for option %s", key.c_str());
          }
        }
      }
    }
  } else {
    const Value* value;
    value = row->get_by_name("strategy_class");
    if (value && is_string_type(value->value_type())) {
      strategy_class = value->to_string_ref();
    }

    value = row->get_by_name("strategy_options");

    int32_t buffer_size = value->size();
    ScopedPtr<char[]> buf(new char[buffer_size + 1]);
    memcpy(buf.get(), value->data(), buffer_size);
    buf[buffer_size] = '\0';

    rapidjson::Document d;
    d.ParseInsitu(buf.get());

    if (!d.HasParseError() && d.IsObject()) {
      for (rapidjson::Value::ConstMemberIterator i = d.MemberBegin(); i != d.MemberEnd(); ++i) {
        std::string key(i->name.GetString(), i->name.GetStringLength());
        std::string value(i->value.GetString(), i->value.GetStringLength());
        size_t replication_factor = strtoul(value.c_str(), NULL, 10);
        if (replication_factor > 0) {
          if (key == "replication_factor") {
            total_replication_factor_ = replication_factor;
          } else {
            total_replication_factor_ += replication_factor;
            replication_factors_[dc_ids.get(key)] =  replication_factor;
          }
        } else {
          LOG_WARN("Replication factor of 0 for option %s", key.c_str());
        }
      }
    }
  }

  if (ends_with(strategy_class, CASS_NETWORK_TOPOLOGY_STRATEGY)) {
    type_ = NETWORK_TOPOLOGY_STRATEGY;
  } else if (ends_with(strategy_class, CASS_SIMPLE_STRATEGY)) {
    type_ = SIMPLE_STRATEGY;
  }
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas(const TokenHostVec& tokens, const DcRackMap& dc_racks,
                                                      TokenReplicasVec& result) const {
  result.clear();
  result.reserve(tokens.size());

  switch (type_) {
    case NETWORK_TOPOLOGY_STRATEGY:
      build_replicas_network_topology(tokens, dc_racks, result);
      break;
    case SIMPLE_STRATEGY:
      build_replicas_simple(tokens, dc_racks, result);
      break;
    default:
      build_replicas_non_replicated(tokens, dc_racks, result);
      break;
  }
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas_network_topology(const TokenHostVec& tokens, const DcRackMap& dc_racks,
                                                                       TokenReplicasVec& result) const {
  if (replication_factors_.empty()) {
    return;
  }

  DcRackMap dc_racks_observed;
  dc_racks_observed.resize(dc_racks.size());

  DcSkippedEndpointsMap dc_skipped_endpoints;
  dc_skipped_endpoints.resize(dc_skipped_endpoints.size());

  ReplicationFactorMap replica_counts;
  replica_counts.resize(replication_factors_.size());

  const size_t num_replicas = total_replication_factor_;
  const size_t num_tokens = tokens.size();

  TokenHostQueue replicas;
  typename TokenHostVec::const_iterator it = tokens.begin();

  for (typename TokenHostVec::const_iterator i = tokens.begin(), end = tokens.end(); i != end; ++i) {
    Token token = i->first;
    //printf("This token: %lld\n", token);

    for (typename TokenHostQueue::const_iterator j = replicas.begin(), end = replicas.end(); j != end;) {
      //printf("Last token: %lld This token: %lld\n", (int64_t)(*j)->first, (int64_t)token);
      if ((*j)->first < token) {
        const SharedRefPtr<Host>& host = (*j)->second;
        uint32_t dc = host->dc_id();
        uint32_t rack = host->rack_id();
        size_t& replica_count_this_dc = replica_counts[dc];
        if (replica_count_this_dc > 0) {
          --replica_count_this_dc;
        }
        dc_racks_observed[dc].erase(rack);
        ++j;
        replicas.pop_front();
      } else {
        ++j;
      }
    }

    //printf("Queue size: %zu\n", replicas.size());

    for (size_t count = 0; count < num_tokens && replicas.size() < num_replicas; ++count) {
      typename TokenHostVec::const_iterator  curr_it = it;
      uint32_t dc = it->second->dc_id();
      uint32_t rack = it->second->rack_id();

      ++it;
      if (it == tokens.end()) {
        it = tokens.begin();
      }

      if (dc == 0) {
        continue;
      }

      size_t replication_factor;
      {
        ReplicationFactorMap::const_iterator r = replication_factors_.find(dc);
        if (r == replication_factors_.end()) {
          continue;
        }
        replication_factor = r->second;
      }

      size_t& replica_count_this_dc = replica_counts[dc];
      if (replica_count_this_dc >= replication_factor) {
        continue;
      }

      size_t rack_count_this_dc;
      {
        DcRackMap::const_iterator r = dc_racks.find(dc);
        if (r == dc_racks.end()) {
          continue;
        }
        rack_count_this_dc = r->second.size();
      }


      RackSet& racks_observed_this_dc = dc_racks_observed[dc];

      if (rack == 0 || racks_observed_this_dc.size() == rack_count_this_dc) {
        ++replica_count_this_dc;
        replicas.push_back(curr_it);
      } else {
        SkippedEndpoints& skipped_endpoints_this_dc = dc_skipped_endpoints[dc];
        if (racks_observed_this_dc.count(rack) > 0) {
          skipped_endpoints_this_dc.push_back(curr_it);
        } else {
          ++replica_count_this_dc;
          replicas.push_back(curr_it);
          racks_observed_this_dc.insert(rack);

          if (racks_observed_this_dc.size() == rack_count_this_dc) {
            while (!skipped_endpoints_this_dc.empty() && replica_count_this_dc < replication_factor) {
              ++replica_count_this_dc;
              replicas.push_back(skipped_endpoints_this_dc.pop_front());
            }
          }
        }
      }
    }

    CopyOnWriteHostVec hosts(new HostVec());
    hosts->reserve(num_replicas);
    for (typename TokenHostQueue::const_iterator j = replicas.begin(), end = replicas.end(); j != end; ++j) {
      hosts->push_back(i->second);
    }
    result.push_back(TokenReplicas(token, hosts));
  }

#if 0
  for (typename TokenHostVec::const_iterator end = tokens.end(); i != end;) {
    CopyOnWriteHostVec replicas(new HostVec());
    replicas->reserve(num_replicas);

    dc_racks_observed.clear();
    dc_skipped_endpoints.clear();
    replica_counts.clear();

    typename TokenHostVec::const_iterator j = i;
    for (size_t index = 0; index < tokens.size() && replicas->size() < num_replicas; ++index) {
      const SharedRefPtr<Host>& host = j->second;
      uint32_t dc = host->dc_id();

      ++j;
      if (j == tokens.end()) {
        j = tokens.begin();
      }

      ++count;

      if (dc == 0) {
        continue;
      }

      size_t replication_factor;
      {
        ReplicationFactorMap::const_iterator r = replication_factors_.find(dc);
        if (r == replication_factors_.end()) {
          continue;
        }
        replication_factor = r->second;
      }

      size_t& replica_count_this_dc = replica_counts[dc];
      if (replica_count_this_dc >= replication_factor) {
        continue;
      }

      size_t rack_count_this_dc;
      {
        DcRackMap::const_iterator r = dc_racks.find(dc);
        if (r == dc_racks.end()) {
          continue;
        }
        rack_count_this_dc = r->second.size();
      }


      RackSet& racks_observed_this_dc = dc_racks_observed[dc];
      uint32_t rack = host->rack_id();

      if (rack == 0 || racks_observed_this_dc.size() == rack_count_this_dc) {
        ++replica_count_this_dc;
        replicas->push_back(host);
      } else {
        SkippedEndpoints& skipped_endpoints_this_dc = dc_skipped_endpoints[dc];
        if (racks_observed_this_dc.count(rack) > 0) {
          skipped_endpoints_this_dc.push_back(index);
        } else {
          ++replica_count_this_dc;
          replicas->push_back(host);
          racks_observed_this_dc.insert(rack);

          if (racks_observed_this_dc.size() == rack_count_this_dc) {
            while (!skipped_endpoints_this_dc.empty() && replica_count_this_dc < replication_factor) {
              ++replica_count_this_dc;
              replicas->push_back(tokens[skipped_endpoints_this_dc.pop_front()].second);
            }
          }
        }
      }
    }
    result.push_back(TokenReplicas(i->first, replicas));
  }
#endif
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas_simple(const TokenHostVec& tokens, const DcRackMap& not_used,
                                                             TokenReplicasVec& result) const {
  if (replication_factors_.empty()) {
    return;
  }
  size_t num_replicas = std::min<size_t>(total_replication_factor_, tokens.size());
  for (typename TokenHostVec::const_iterator i = tokens.begin(), end = tokens.end(); i != end; ++i) {
    CopyOnWriteHostVec replicas(new HostVec());
    typename TokenHostVec::const_iterator j = i;
    do {
      replicas->push_back(j->second);
      ++j;
      if (j == tokens.end()) {
        j = tokens.begin();
      }
    } while (replicas->size() < num_replicas);
    result.push_back(TokenReplicas(i->first, replicas));
  }
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas_non_replicated(const TokenHostVec& tokens, const DcRackMap& not_used,
                                                                     TokenReplicasVec& result) const {
  for (typename TokenHostVec::const_iterator i = tokens.begin(); i != tokens.end(); ++i) {
    CopyOnWriteHostVec replicas(new HostVec(1, i->second));
    result.push_back(TokenReplicas(i->first, replicas));
  }
}

template <class Partitioner>
class TokenMetadataImpl : public TokenMetadata {
public:
  typedef typename Partitioner::Token Token;

  typedef std::pair<Token, Host::Ptr> TokenHost;
  typedef std::vector<TokenHost> TokenHostVec;

  typedef std::pair<Token, CopyOnWriteHostVec> TokenReplicas;
  typedef std::vector<TokenReplicas> TokenReplicasVec;

  typedef sparsehash::dense_hash_set<Host::Ptr> HostMap;

  typedef sparsehash::dense_hash_map<std::string, TokenReplicasVec> KeyspaceReplicaMap;
  typedef ReplicationStrategy<Partitioner> ReplicationStrategy;
  typedef sparsehash::dense_hash_map<std::string, ReplicationStrategy> KeyspaceStrategyMap;

  struct RemoveTokenHostIf {
    RemoveTokenHostIf(const Host::Ptr& host)
      : host(host) { }

    bool operator()(const TokenHost& token) const {
      return token.second->address() == host->address();
    }

    const Host::Ptr host;
  };

  static const CopyOnWriteHostVec NO_REPLICAS;

  TokenMetadataImpl() {
    hosts_.set_empty_key(Host::Ptr());
    hosts_.set_deleted_key(Host::Ptr(new Host(Address("0.0.0.0", 0), false)));
    replicas_.set_empty_key(std::string());
    replicas_.set_deleted_key(std::string(1, '\0'));
    strategies_.set_empty_key(std::string());
    strategies_.set_deleted_key(std::string(1, '\0'));
  }

  virtual void add_host(const Host::Ptr& host, const Value* tokens);
  virtual void update_host(const Host::Ptr& host, const Value* tokens);
  virtual void remove_host(const Host::Ptr& host);

  virtual void add_keyspaces(const VersionNumber& cassandra_version, ResultResponse* result);
  virtual void update_keyspaces(const VersionNumber& cassandra_version, ResultResponse* result);
  virtual void drop_keyspace(const std::string& keyspace_name);

  virtual void build();

  virtual const CopyOnWriteHostVec& get_replicas(const std::string& keyspace_name,
                                                 const std::string& routing_key) const;

private:
  void internal_update_keyspace(const VersionNumber& cassandra_version,
                                ResultResponse* result,
                                bool should_build_replicas);
  void internal_remove_host(const Host::Ptr& host);
  void update_host_ids(const Host::Ptr& host);
  void build_dc_racks();
  void build_replicas();

  TokenHostVec tokens_;
  HostMap hosts_;
  DcRackMap dc_racks_;
  KeyspaceReplicaMap replicas_;
  KeyspaceStrategyMap strategies_;
  IdGenerator rack_ids_;
  IdGenerator dc_ids_;
};

// TODO(mpenick): Does CopyOnWritePtr need a bool operator?
template <class Partitioner>
const CopyOnWriteHostVec TokenMetadataImpl<Partitioner>::NO_REPLICAS(NULL);

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::add_host(const Host::Ptr& host, const Value* tokens) {
  update_host_ids(host);
  hosts_.insert(host);

  CollectionIterator iterator(tokens);
  while (iterator.next()) {
    Token token = Partitioner::get_token(iterator.value()->to_string_ref());
    tokens_.push_back(TokenHost(token, host));
  }
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::update_host(const Host::Ptr& host, const Value* tokens) {
  internal_remove_host(host);

  update_host_ids(host);
  hosts_.insert(host);

  TokenHostVec new_tokens;
  CollectionIterator iterator(tokens);
  while (iterator.next()) {
    Token token = Partitioner::get_token(iterator.value()->to_string_ref());
    new_tokens.push_back(TokenHost(token, host));
  }

  std::sort(new_tokens.begin(), new_tokens.end());

  tokens_.resize(tokens_.size() + new_tokens.size());
  std::merge(tokens_.begin(), tokens_.end(),
             new_tokens.begin(), new_tokens.end(),
             tokens_.begin());

  build_replicas();
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::remove_host(const Host::Ptr& host) {
  hosts_.erase(host);
  internal_remove_host(host);
  build_replicas();
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::add_keyspaces(const VersionNumber& cassandra_version,
                                                   ResultResponse* result) {
  internal_update_keyspace(cassandra_version, result, false);
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::update_keyspaces(const VersionNumber& cassandra_version,
                                                      ResultResponse* result) {
  internal_update_keyspace(cassandra_version, result, true);
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::drop_keyspace(const std::string& keyspace_name) {
  replicas_.erase(keyspace_name);
  strategies_.erase(keyspace_name);
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::build() {
  std::sort(tokens_.begin(), tokens_.end());
  build_replicas();
}

template <class Partitioner>
const CopyOnWriteHostVec& TokenMetadataImpl<Partitioner>::get_replicas(const std::string& keyspace_name,
                                                                       const std::string& routing_key) const {
  return NO_REPLICAS;
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::internal_update_keyspace(const VersionNumber& cassandra_version,
                                                              ResultResponse* result,
                                                              bool should_build_replicas) {
  ResultIterator rows(result);

  while (rows.next()) {
    std::string keyspace_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &keyspace_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name'");
      continue;
    }

    ReplicationStrategy strategy;

    strategy.init(dc_ids_, cassandra_version, row);

    typename KeyspaceStrategyMap::iterator i = strategies_.find(keyspace_name);
    if (i == strategies_.end() || i->second != strategy) {
      if (i == strategies_.end()) {
        strategies_[keyspace_name] = strategy;
      } else {
        i->second = strategy;
      }
      if (should_build_replicas) {
        build_dc_racks();
        strategy.build_replicas(tokens_, dc_racks_, replicas_[keyspace_name]);
      }
    }
  }
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::internal_remove_host(const Host::Ptr& host) {
  typename TokenHostVec::iterator last = std::remove_copy_if(tokens_.begin(), tokens_.end(),
                                                             tokens_.end(),
                                                             RemoveTokenHostIf(host));
  tokens_.resize(last - tokens_.begin());
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::build_dc_racks() {
  dc_racks_.clear();
  for (HostMap::const_iterator i = hosts_.begin(), end = hosts_.end();
       i != end; ++i) {
    uint32_t dc = (*i)->dc_id();
    uint32_t rack = (*i)->rack_id();
    if (dc != 0 && rack != 0) {
      dc_racks_[dc].insert(rack);
    }
  }
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::update_host_ids(const Host::Ptr& host) {
  host->set_rack_and_dc_ids(rack_ids_.get(host->rack()), dc_ids_.get(host->dc()));
}

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::build_replicas() {
  build_dc_racks();
  for (typename KeyspaceStrategyMap::const_iterator i = strategies_.begin(),
       end = strategies_.end();
       i != end; ++i) {
    const std::string& keyspace_name = i->first;
    const ReplicationStrategy& strategy = i->second;
    strategy.build_replicas(tokens_, dc_racks_, replicas_[keyspace_name]);
  }
}

} // namespace cass

#endif
