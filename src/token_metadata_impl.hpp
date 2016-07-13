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

#include <algorithm>
#include <queue>
#include <vector>

#define CASS_NETWORK_TOPOLOGY_STRATEGY "NetworkTopologyStrategy"
#define CASS_SIMPLE_STRATEGY           "SimpleStrategy"

namespace cass {

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

template <class Partitioner>
class ReplicationStrategy {
public:
  typedef typename Partitioner::Token Token;

  typedef std::pair<Token, Host::Ptr> TokenHost;
  typedef std::vector<TokenHost> TokenHostVec;

  typedef std::pair<Token, CopyOnWriteHostVec> TokenReplicas;
  typedef std::vector<TokenReplicas> TokenReplicasVec;

  typedef sparsehash::dense_hash_set<Host::Ptr> HostMap;

  typedef std::pair<std::string, size_t> ReplicationFactor;
  typedef std::vector<ReplicationFactor> ReplicationFactorVec;

  enum Type {
    NETWORK_TOPOLOGY_STRATEGY,
    SIMPLE_STRATEGY,
    NON_REPLICATED
  };

  ReplicationStrategy()
    : type_(NON_REPLICATED) { }

  void init(const VersionNumber& cassandra_version,
            const Row* row);

  bool operator!=(const ReplicationStrategy& other) const {
    return type_ != other.type_ ||
                    replication_factors_ != other.replication_factors_;
  }

  void build_replicas(const TokenHostVec& tokens, const HostMap& hosts, TokenReplicasVec& result) const;

private:
  void build_replicas_network_topology(const TokenHostVec& tokens, const HostMap& hosts, TokenReplicasVec& result) const;
  void build_replicas_simple(const TokenHostVec& tokens, const HostMap& hosts, TokenReplicasVec& result) const ;
  void build_replicas_non_replicated(const TokenHostVec& tokens, const HostMap& hosts, TokenReplicasVec& result) const;

private:
  Type type_;
  ReplicationFactorVec replication_factors_;
};

template <class Partitioner>
void ReplicationStrategy<Partitioner>::init(const VersionNumber& cassandra_version,
                                            const Row* row) {
  StringRef strategy_class;

  if (cassandra_version >= VersionNumber(3, 0, 0)) {
    const Value* value = row->get_by_name("replication");
    if (value &&  value->is_map() &&
        is_string_type(value->primary_value_type()) &&
        is_string_type(value->secondary_value_type())) {
      MapIterator iterator(value);
      while (iterator.next()) {
        if (iterator.key()->to_string_ref() == "class") {
          strategy_class = iterator.value()->to_string_ref();
        } else {
          std::string key(iterator.key()->to_string());
          std::string value(iterator.value()->to_string());
          size_t replication_factor = strtoul(value.c_str(), NULL, 10);
          if (replication_factor > 0) {
            replication_factors_.push_back(ReplicationFactor(key, replication_factor));
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
          replication_factors_.push_back(ReplicationFactor(key, replication_factor));
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

  std::sort(replication_factors_.begin(), replication_factors_.end());
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas(const TokenHostVec& tokens, const HostMap& hosts, TokenReplicasVec& result) const {
  result.clear();
  result.reserve(tokens.size());

  switch (type_) {
    case NETWORK_TOPOLOGY_STRATEGY:
      build_replicas_network_topology(tokens, hosts, result);
      break;
    case SIMPLE_STRATEGY:
      build_replicas_simple(tokens, hosts, result);
      break;
    default:
      build_replicas_non_replicated(tokens, hosts, result);
      break;
  }
}

template <class Partitioner>
void ReplicationStrategy<Partitioner>::build_replicas_network_topology(const TokenHostVec& tokens, const HostMap& hosts, TokenReplicasVec& result) const {
  if (replication_factors_.empty()) {
    return;
  }

#if 0
  ReplicationFactorVec replica_counts;

  struct Datacenter {
    Datacenter()
      : num_racks(0)
      , num_observed_racks(0) { }
    size_t num_racks;
    size_t num_observed_racks;
    std::queue<Host::Ptr> skipped_endpoints;
  };

  typedef std::vector<std::pair<std::string, bool> > RacksVisitedVec;
  typedef std::vector<std::pair<std::string, Datacenter> DatacenterVec;

  RacksVisitedVec racks_visited;
  DatacenterVec datacenters;

  for (TokenHostVec::const_iterator i = tokens.begin(), end = tokens.end(); i != end; ++i) {
    std::map<std::string, std::set<std::string> > racks_observed;
    std::map<std::string, std::list<SharedRefPtr<Host> > > skipped_endpoints;

    CopyOnWriteHostVec replicas(new HostVec());
    TokenHostVec::const_iterator j = i;
    for (size_t count = 0; count < tokens.size() && replica_counts != replication_factors_; ++count) {
      const SharedRefPtr<Host>& host = j->second;
      const std::string& dc = host->dc();

      ++j;
      if (j == tokens.end()) {
        j = tokens.begin();
      }

      ReplicationFactorVec::const_iterator rf = std::find_if(replic)
      DCReplicaCountMap::const_iterator rf_it =  replication_factors_.find(dc);
      if (dc.empty() || rf_it == replication_factors_.end()) {
        continue;
      }

      const size_t rf = rf_it->second;
      size_t& replica_count_this_dc = replica_counts[dc];
      if (replica_count_this_dc >= rf) {
        continue;
      }

      const size_t rack_count_this_dc = racks[dc].size();
      std::set<std::string>& racks_observed_this_dc = racks_observed[dc];
      const std::string& rack = host->rack();

      if (rack.empty() || racks_observed_this_dc.size() == rack_count_this_dc) {
        ++replica_count_this_dc;
        replicas->push_back(host);
      } else {
        if (racks_observed_this_dc.count(rack) > 0) {
          skipped_endpoints[dc].push_back(host);
        } else {
          ++replica_count_this_dc;
          replicas->push_back(host);
          racks_observed_this_dc.insert(rack);

          if (racks_observed_this_dc.size() == rack_count_this_dc) {
            std::list<SharedRefPtr<Host> >& skipped_endpoints_this_dc = skipped_endpoints[dc];
            while (!skipped_endpoints_this_dc.empty() && replica_count_this_dc < rf) {
              ++replica_count_this_dc;
              replicas->push_back(skipped_endpoints_this_dc.front());
              skipped_endpoints_this_dc.pop_front();
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
void ReplicationStrategy<Partitioner>::build_replicas_simple(const TokenHostVec& tokens, const HostMap& hosts, TokenReplicasVec& result) const {
  if (replication_factors_.empty()) {
    return;
  }
  size_t replication_factor = replication_factors_[0].second;
  size_t num_replicas = std::min<size_t>(replication_factor, tokens.size());
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
void ReplicationStrategy<Partitioner>::build_replicas_non_replicated(const TokenHostVec& tokens, const HostMap& hosts, TokenReplicasVec& result) const {
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

  typedef sparsehash::dense_hash_map<std::string, TokenReplicasVec> KeyspaceReplicaMap;
  typedef ReplicationStrategy<Partitioner> ReplicationStrategy;
  typedef sparsehash::dense_hash_map<std::string, ReplicationStrategy> KeyspaceStrategyMap;

  typedef sparsehash::dense_hash_set<Host::Ptr> HostMap;

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
    replicas_.set_empty_key("");
    strategies_.set_empty_key("");
    hosts_.set_empty_key(Host::Ptr());
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
  void build_replicas();

  TokenHostVec tokens_;
  HostMap hosts_;
  KeyspaceReplicaMap replicas_;
  KeyspaceStrategyMap strategies_;
};

// TODO(mpenick): Does CopyOnWritePtr need a bool operator?
template <class Partitioner>
const CopyOnWriteHostVec TokenMetadataImpl<Partitioner>::NO_REPLICAS(NULL);

template <class Partitioner>
void TokenMetadataImpl<Partitioner>::add_host(const Host::Ptr& host, const Value* tokens) {
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

    strategy.init(cassandra_version, row);

    typename KeyspaceStrategyMap::iterator i = strategies_.find(keyspace_name);
    if (i == strategies_.end() || i->second != strategy) {
      if (i == strategies_.end()) {
        strategies_[keyspace_name] = strategy;
      } else {
        i->second = strategy;
      }
      if (should_build_replicas) {
        strategy.build_replicas(tokens_, hosts_, replicas_[keyspace_name]);
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
void TokenMetadataImpl<Partitioner>::build_replicas() {
  for (typename KeyspaceStrategyMap::const_iterator i = strategies_.begin(),
       end = strategies_.end();
       i != end; ++i) {
    const std::string& keyspace_name = i->first;
    const ReplicationStrategy& strategy = i->second;
    strategy.build_replicas(tokens_, hosts_, replicas_[keyspace_name]);
  }
}

} // namespace cass

#endif
