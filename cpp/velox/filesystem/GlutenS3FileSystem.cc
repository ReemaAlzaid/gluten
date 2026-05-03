/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "filesystem/GlutenS3FileSystem.h"

#include <folly/Synchronized.h>
#include <glog/logging.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/dwio/common/FileSink.h"

namespace gluten {

namespace velox = facebook::velox;
namespace filesystems = facebook::velox::filesystems;

namespace {

using FileSystemMap = folly::Synchronized<
    std::unordered_map<std::string, std::shared_ptr<filesystems::FileSystem>>>;
using FileSystemGenerator = std::function<std::shared_ptr<filesystems::FileSystem>(
    std::shared_ptr<const velox::config::ConfigBase>,
    std::string_view)>;
using FileSinkGenerator =
    std::function<std::unique_ptr<velox::dwio::common::FileSink>(
        const std::string&,
        const velox::dwio::common::FileSink::Options&)>;

FileSystemMap& glutenS3FileSystems() {
  static FileSystemMap instances;
  return instances;
}

filesystems::CacheKeyFn glutenS3CacheKeyFunc;

std::shared_ptr<filesystems::FileSystem> glutenS3FileSystemGenerator(
    std::shared_ptr<const velox::config::ConfigBase> properties,
    std::string_view s3Path) {
  std::string cacheKey;
  std::string bucketName;
  std::string key;
  filesystems::getBucketAndKeyFromPath(
      filesystems::getPath(s3Path), bucketName, key);
  if (glutenS3CacheKeyFunc) {
    cacheKey = glutenS3CacheKeyFunc(properties, s3Path);
  } else {
    cacheKey = filesystems::S3Config::cacheKey(bucketName, properties);
  }

  auto fs = glutenS3FileSystems().withRLock(
      [&](auto& instanceMap) -> std::shared_ptr<filesystems::FileSystem> {
        auto iterator = instanceMap.find(cacheKey);
        if (iterator != instanceMap.end()) {
          LOG(WARNING) << "[GlutenS3FileSystem] cache hit for S3 path: "
                       << s3Path;
          return iterator->second;
        }
        return nullptr;
      });
  if (fs != nullptr) {
    return fs;
  }

  return glutenS3FileSystems().withWLock(
      [&](auto& instanceMap) -> std::shared_ptr<filesystems::FileSystem> {
        auto iterator = instanceMap.find(cacheKey);
        if (iterator != instanceMap.end()) {
          LOG(WARNING) << "[GlutenS3FileSystem] cache hit for S3 path: "
                       << s3Path;
          return iterator->second;
        }

        auto logLevel = properties->get(
            filesystems::S3Config::kS3LogLevel, std::string("FATAL"));
        std::optional<std::string> logLocation =
            static_cast<std::optional<std::string>>(
                properties->get<std::string>(
                    filesystems::S3Config::kS3LogLocation));
        filesystems::initializeS3(logLevel, logLocation);
        LOG(WARNING) << "[GlutenS3FileSystem] creating filesystem for bucket: "
                     << bucketName;
        auto fs = std::make_shared<GlutenS3FileSystem>(bucketName, properties);
        instanceMap.insert({cacheKey, fs});
        return fs;
      });
}

std::unique_ptr<velox::dwio::common::FileSink> glutenS3WriteFileSinkGenerator(
    const std::string& fileURI,
    const velox::dwio::common::FileSink::Options& options) {
  if (filesystems::isS3File(fileURI)) {
    LOG(WARNING) << "[GlutenS3FileSystem] creating WriteFileSink for path: "
                 << fileURI;
    auto fileSystem =
        filesystems::getFileSystem(fileURI, options.connectorProperties);
    return std::make_unique<velox::dwio::common::WriteFileSink>(
        fileSystem->openFileForWrite(fileURI, {{}, options.pool, std::nullopt}),
        fileURI,
        options.metricLogger,
        options.stats,
        options.fileSystemStats);
  }
  return nullptr;
}

} // namespace

std::unique_ptr<velox::WriteFile> GlutenS3FileSystem::openFileForWrite(
    std::string_view s3Path,
    const filesystems::FileOptions& options) {
  LOG(WARNING) << "[GlutenS3FileSystem] openFileForWrite delegating to Velox "
                  "S3 for path: "
               << s3Path;
  return filesystems::S3FileSystem::openFileForWrite(s3Path, options);
}

void registerGlutenS3FileSystem(filesystems::CacheKeyFn cacheKeyFunc) {
  glutenS3FileSystems().withWLock([&](auto& instanceMap) {
    if (instanceMap.empty()) {
      LOG(WARNING) << "[GlutenS3FileSystem] registering";
      glutenS3CacheKeyFunc = cacheKeyFunc;
      filesystems::registerFileSystem(
          filesystems::isS3File,
          FileSystemGenerator(glutenS3FileSystemGenerator));
      velox::dwio::common::FileSink::registerFactory(
          FileSinkGenerator(glutenS3WriteFileSinkGenerator));
    }
  });
}

void finalizeGlutenS3FileSystem() {
  LOG(WARNING) << "[GlutenS3FileSystem] finalizing";
  bool singleUseCount = true;
  glutenS3FileSystems().withWLock([&](auto& instanceMap) {
    for (const auto& entry : instanceMap) {
      singleUseCount &= (entry.second.use_count() == 1);
    }
    VELOX_CHECK(
        singleUseCount, "Cannot finalize GlutenS3FileSystem while in use");
    instanceMap.clear();
  });

  filesystems::finalizeS3();
}

} // namespace gluten
