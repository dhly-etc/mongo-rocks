/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/modules/rocks/src/rocks_parameters_gen.h"
#include "rocks_util.h"

#include "mongo/db/json.h"
#include "mongo/logger/parse_log_component_settings.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"
#include "rocks_global_options.h"

#include <rocksdb/cache.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/version.h>

namespace mongo {

    namespace {
        Status RocksRateLimiterServerParameterSet(int newNum, const std::string& name,
                                                  RocksEngine* engine) {
            if (newNum <= 0) {
                return Status(ErrorCodes::BadValue, str::stream() << name << " has to be > 0");
            }
            log() << "RocksDB: changing rate limiter to " << newNum << "MB/s";
            engine->setMaxWriteMBPerSec(newNum);

            return Status::OK();
        }
    }  // namespace

    void RocksRateLimiterServerParameter::append(OperationContext* opCtx, BSONObjBuilder& b,
                                                 const std::string& name) {
        b.append(name, _data->getMaxWriteMBPerSec());
    }

    Status RocksRateLimiterServerParameter::set(const BSONElement& newValueElement) {
        if (!newValueElement.isNumber()) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
        }
        return RocksRateLimiterServerParameterSet(newValueElement.numberInt(), name(), _data);
    }

    Status RocksRateLimiterServerParameter::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) return status;
        return RocksRateLimiterServerParameterSet(num, name(), _data);
    }

    void RocksBackupServerParameter::append(OperationContext* opCtx, BSONObjBuilder& b,
                                            const std::string& name) {
        b.append(name, "");
    }

    Status RocksBackupServerParameter::set(const BSONElement& newValueElement) {
        auto str = newValueElement.str();
        if (str.size() == 0) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a string");
        }
        return setFromString(str);
    }

    Status RocksBackupServerParameter::setFromString(const std::string& str) {
        return _data->backup(str);
    }

    void RocksCompactServerParameter::append(OperationContext* opCtx, BSONObjBuilder& b,
                                             const std::string& name) {
        b.append(name, "");
    }

    Status RocksCompactServerParameter::set(const BSONElement& newValueElement) {
        return setFromString("");
    }

    Status RocksCompactServerParameter::setFromString(const std::string& str) {
        _data->getCompactionScheduler()->compactAll();
        return Status::OK();
    }

    namespace {
        Status RocksCacheSizeParameterSet(int newNum, const std::string& name,
                                          RocksEngine* engine) {
            if (newNum <= 0) {
                return Status(ErrorCodes::BadValue, str::stream() << name << " has to be > 0");
            }
            log() << "RocksDB: changing block cache size to " << newNum << "GB";
            const long long bytesInGB = 1024 * 1024 * 1024LL;
            size_t newSizeInBytes = static_cast<size_t>(newNum * bytesInGB);
            engine->getBlockCache()->SetCapacity(newSizeInBytes);

            return Status::OK();
        }
    }  // namespace

    void RocksCacheSizeParameter::append(OperationContext* opCtx, BSONObjBuilder& b,
                                         const std::string& name) {
        const long long bytesInGB = 1024 * 1024 * 1024LL;
        long long cacheSizeInGB = _data->getBlockCache()->GetCapacity() / bytesInGB;
        b.append(name, cacheSizeInGB);
    }

    Status RocksCacheSizeParameter::set(const BSONElement& newValueElement) {
        if (!newValueElement.isNumber()) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
        }
        return RocksCacheSizeParameterSet(newValueElement.numberInt(), name(), _data);
    }

    Status RocksCacheSizeParameter::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) return status;
        return RocksCacheSizeParameterSet(num, name(), _data);
    }

    void RocksOptionsParameter::append(OperationContext* opCtx, BSONObjBuilder& b,
                                       const std::string& name) {
        std::string columnOptions;
        std::string dbOptions;
        std::string fullOptionsStr;
        rocksdb::Options fullOptions = _data->getDB()->GetOptions();
        rocksdb::Status s = GetStringFromColumnFamilyOptions(&columnOptions, fullOptions);
        if (!s.ok()) {  // If we failed, append the error for the user to see.
            b.append(name, s.ToString());
            return;
        }

        fullOptionsStr.append(columnOptions);

        s = GetStringFromDBOptions(&dbOptions, fullOptions);
        if (!s.ok()) {  // If we failed, append the error for the user to see.
            b.append(name, s.ToString());
            return;
        }

        fullOptionsStr.append(dbOptions);

        b.append(name, fullOptionsStr);
    }

    Status RocksOptionsParameter::set(const BSONElement& newValueElement) {
        // In case the BSON element is not a string, the conversion will fail,
        // raising an exception catched by the outer layer.
        // Which will generate an error message that looks like this:
        // wrong type for field (rocksdbOptions) 3 != 2
        return setFromString(newValueElement.String());
    }

    Status RocksOptionsParameter::setFromString(const std::string& str) {
        log() << "RocksDB: Attempting to apply settings: " << str;
        std::set<std::string> supported_db_options = {"db_write_buffer_size", "delayed_write_rate",
                                                      "max_background_jobs", "max_total_wal_size"};

        std::set<std::string> supported_cf_options = {"max_write_buffer_number",
                                                      "disable_auto_compactions",
                                                      "level0_slowdown_writes_trigger",
                                                      "level0_stop_writes_trigger",
                                                      "soft_pending_compaction_bytes_limit",
                                                      "hard_pending_compaction_bytes_limit"};
        std::unordered_map<std::string, std::string> optionsMap;
        rocksdb::Status s = rocksdb::StringToMap(str, &optionsMap);
        if (!s.ok()) {
            return Status(ErrorCodes::BadValue, s.ToString());
        }
        for (const auto& v : optionsMap) {
            if (supported_db_options.find(v.first) != supported_db_options.end()) {
                s = _data->getDB()->SetDBOptions({v});
            } else if (supported_cf_options.find(v.first) != supported_cf_options.end()) {
                s = _data->getDB()->SetOptions({v});
            } else {
                return Status(ErrorCodes::BadValue, str::stream() << "unknown param: " << v.first);
            }
        }
        if (!s.ok()) {
            return Status(ErrorCodes::BadValue, s.ToString());
        }

        return Status::OK();
    }

    void RocksdbMaxConflictCheckSizeParameter::append(OperationContext* opCtx, BSONObjBuilder& b,
                                                      const std::string& name) {
        b << name << rocksGlobalOptions.maxConflictCheckSizeMB;
    }

    Status RocksdbMaxConflictCheckSizeParameter::set(const BSONElement& newValueElement) {
        return setFromString(newValueElement.toString(false));
    }

    Status RocksdbMaxConflictCheckSizeParameter::setFromString(const std::string& str) {
        std::string trimStr;
        size_t pos = str.find('.');
        if (pos != std::string::npos) {
            trimStr = str.substr(0, pos);
        }
        int newValue;
        Status status = parseNumberFromString(trimStr, &newValue);
        if (!status.isOK()) {
            return status;
        }
        rocksGlobalOptions.maxConflictCheckSizeMB = newValue;
        _data->getDB()->SetMaxConflictBytes(newValue * 1024 * 1024);
        return Status::OK();
    }
}  // namespace mongo
