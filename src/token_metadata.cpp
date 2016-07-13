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

#include "token_metadata.hpp"

#include "token_metadata_impl.hpp"

#define CASS_MURMUR3_PARTITIONER      "Murmur3Partitioner"
#define CASS_RANDOM_PARTITIONER       "RandomPartitioner"
#define CASS_BYTE_ORDERED_PARTITIONER "ByteOrderedPartitioner"

namespace cass {

TokenMetadata* TokenMetadata::from_partitioner(const std::string& partitioner) {
  if (ends_with(partitioner, CASS_MURMUR3_PARTITIONER)) {
    return new TokenMetadataImpl<Murmur3Partitioner>();
  } else if (ends_with(partitioner, CASS_RANDOM_PARTITIONER)) {
    return new TokenMetadataImpl<RandomPartitioner>();
  } else if (ends_with(partitioner, CASS_BYTE_ORDERED_PARTITIONER)) {
    return new TokenMetadataImpl<ByteOrderedPartitioner>();
  } else {
    LOG_WARN("Unsupported partitioner class '%s'", partitioner.c_str());
    return NULL;
  }
}

} // namespace cass
