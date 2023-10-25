/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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

#include "rocks_record_store.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <algorithm>
#include <memory>
#include <mutex>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/modules/rocks/src/rocks_parameters_gen.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/record_id_helpers.h"
#include "mongo/db/server_recovery.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/platform/endian.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/str.h"
#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_durability_manager.h"
#include "rocks_engine.h"
#include "rocks_oplog_manager.h"
#include "rocks_prepare_conflict.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {

    using namespace fmt::literals;
    using std::string;
    using std::unique_ptr;

    static int64_t cappedMaxSizeSlackFromSize(int64_t cappedMaxSize) {
        return std::min(cappedMaxSize / 10, int64_t(16 * 1024 * 1024));
    }

    MONGO_FAIL_POINT_DEFINE(RocksWriteConflictException);
    MONGO_FAIL_POINT_DEFINE(RocksWriteConflictExceptionForReads);

    namespace {
        AtomicWord<std::uint32_t> minSSTFileCountReserved{4};
    }
    ExportedMinSSTFileCountReservedParameter::ExportedMinSSTFileCountReservedParameter(
        StringData name, ServerParameterType spt)
        : ServerParameter(name, spt), _data(&minSSTFileCountReserved) {}

    void ExportedMinSSTFileCountReservedParameter::append(OperationContext* opCtx,
                                                          BSONObjBuilder* b, StringData name,
                                                          const boost::optional<TenantId>&) {
        b->append(name, static_cast<long long>(_data->load()));
    }

    Status ExportedMinSSTFileCountReservedParameter::setFromString(
        StringData str, const boost::optional<TenantId>&) {
        int num = 0;
        Status status = NumberParser{}(str, &num);
        if (!status.isOK()) {
            return status;
        }
        if (num < 1 || num > (1000 * 1000)) {
            return Status(ErrorCodes::BadValue,
                          "minSSTFileCountReserved must be between 1 and 1 million, inclusive");
        }
        _data->store(static_cast<uint32_t>(num));
        return Status::OK();
    }

    RocksRecordStore::RocksRecordStore(RocksEngine* engine, rocksdb::ColumnFamilyHandle* cf,
                                       OperationContext* opCtx, Params params)
        : RecordStore(params.uuid, params.ident, params.isCapped),
          _engine(engine),
          _db(engine->getDB()),
          _cf(cf),
          _oplogManager(params.nss.isOplog() ? engine->getOplogManager() : nullptr),
          _counterManager(engine->getCounterManager()),
          _compactionScheduler(engine->getCompactionScheduler()),
          _prefix(params.prefix),
          _isCapped(params.isCapped),
          _cappedMaxSize(params.cappedMaxSize),
          _cappedMaxSizeSlack(cappedMaxSizeSlackFromSize(params.cappedMaxSize)),
          _cappedMaxDocs(params.cappedMaxDocs),
          _cappedDeleteCheckCount(0),
          _keyFormat(params.keyFormat),
          _isOplog(params.nss.isOplog()),
          _ident(params.ident),
          _dataSizeKey(std::string("\0\0\0\0", 4) + "datasize-" + params.ident),
          _numRecordsKey(std::string("\0\0\0\0", 4) + "numrecords-" + params.ident),
          _cappedOldestKey(params.nss.isOplog()
                               ? std::string("\0\0\0\0", 4) + "cappedOldestKey-" + params.ident
                               : ""),
          _shuttingDown(false),
          _tracksSizeAdjustments(params.tracksSizeAdjustments) {
        LOGV2_DEBUG(0, 2, "Opening collection", "namespace"_attr = params.nss,
                    "prefix"_attr = rocksdb::Slice(_prefix).ToString(true));

        if (_isCapped) {
            invariant(_cappedMaxSize > 0);
            invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
        } else {
            invariant(_cappedMaxSize == -1);
            invariant(_cappedMaxDocs == -1);
        }

        _loadCountFromCountManager(opCtx);
        // Get next id
        auto txn = std::unique_ptr<rocksdb::TOTransaction>(
            _db->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TOTransactionOptions()));
        invariant(txn);
        std::unique_ptr<RocksIterator> iter(
            RocksRecoveryUnit::NewIteratorWithTxn(txn.get(), _cf, _prefix));
        // first check if the collection is empty
        iter->SeekPrefix("");
        bool emptyCollection = !iter->Valid();
        if (!emptyCollection) {
            // if it's not empty, find next RecordId
            _nextIdNum.store(_getLargestKey(opCtx, iter.get()).getLong() + 1);
        } else {
            // Need to start at 1 so we are always higher than RecordId::min()
            _nextIdNum.store(1);
            _dataSize.store(0);
            _numRecords.store(0);
            if (!_isOplog) {
                _counterManager->updateCounter(_numRecordsKey, 0);
                _counterManager->updateCounter(_dataSizeKey, 0);
                sizeRecoveryState(getGlobalServiceContext())
                    .markCollectionAsAlwaysNeedsSizeAdjustment(_ident);
            }
        }

        _hasBackgroundThread = RocksEngine::initRsOplogBackgroundThread(params.nss);
        invariant(_isOplog == (_oplogManager != nullptr));
        invariant(_isOplog == NamespaceString::oplog(_cf->GetName()));
        if (_isOplog) {
            _engine->startOplogManager(opCtx, this);
        }
    }

    RocksRecordStore::~RocksRecordStore() {
        {
            stdx::lock_guard<stdx::timed_mutex> lk(_cappedDeleterMutex);
            _shuttingDown = true;
        }

        LOGV2_DEBUG(0, 1, "~RocksRecordStore", "ident"_attr = getIdent());

        invariant(_isOplog == (_oplogManager != nullptr));
        if (_isOplog) {
            _engine->haltOplogManager();
        }
    }

    void RocksRecordStore::_loadCountFromCountManager(OperationContext* opCtx) {
        if (_isOplog) {
            long long v = _counterManager->loadCounter(_cappedOldestKey);
            if (v > 0) {
                _cappedOldestKeyHint = RecordId(_counterManager->loadCounter(_cappedOldestKey));
            }
            return;
        }
        _numRecords.store(_counterManager->loadCounter(_numRecordsKey));
        _dataSize.store(_counterManager->loadCounter(_dataSizeKey));
        if (_dataSize.load() < 0) {
            _dataSize.store(0);
        }
        if (_numRecords.load() < 0) {
            _numRecords.store(0);
        }
    }

    NamespaceString RocksRecordStore::ns(OperationContext* opCtx) const {
        if (!_uuid) {
            return NamespaceString::kEmpty;
        }

        return CollectionCatalog::get(opCtx)
            ->lookupNSSByUUID(opCtx, *_uuid)
            .value_or(NamespaceString::kEmpty);
    }

    KeyFormat RocksRecordStore::keyFormat() const { return _keyFormat; }

    int64_t RocksRecordStore::storageSize(OperationContext* opCtx, BSONObjBuilder* extraInfo,
                                          int infoLevel) const {
        long long size = _dataSize.load();
        if (_isOplog) {
            size = dataSize(opCtx);
        }
        // We need to make it multiple of 256 to make
        // jstests/concurrency/fsm_workloads/convert_to_capped_collection.js happy
        return static_cast<int64_t>(std::max(size & (~255), static_cast<long long>(256)));
    }

    void RocksRecordStore::doDeleteRecord(OperationContext* opCtx, const RecordId& dl) {
        invariant(!_isOplog);
        std::string key(_makePrefixedKey(_prefix, dl, _keyFormat));

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto txn = ru->getTransaction();
        invariant(txn);

        std::string oldValue;
        auto status =
            rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(_cf, key, &oldValue); });
        invariantRocksOK(status);
        int oldLength = oldValue.size();

        invariantRocksOK(ROCKS_OP_CHECK(txn->Delete(_cf, key)));
        _changeNumRecords(opCtx, -1);
        _increaseDataSize(opCtx, -oldLength);
    }

    long long RocksRecordStore::dataSize(OperationContext* opCtx) const {
        if (_isOplog) {
            uint64_t curSizeAllMemTables = 0, liveSSTTotalSize = 0;
            invariant(_db->GetRootDB()->GetIntProperty(
                _cf, rocksdb::Slice("rocksdb.cur-size-all-mem-tables"), &curSizeAllMemTables));
            invariant(_db->GetRootDB()->GetIntProperty(
                _cf, rocksdb::Slice("rocksdb.live-sst-files-size"), &liveSSTTotalSize));
            return curSizeAllMemTables + liveSSTTotalSize;
        }
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        return _dataSize.load(std::memory_order_relaxed) + ru->getDeltaCounter(_dataSizeKey);
    }

    long long RocksRecordStore::numRecords(OperationContext* opCtx) const {
        if (_isOplog) {
            uint64_t cntMem = 0, cntImm = 0;
            int64_t cntSST = 0;
            invariant(_db->GetRootDB()->GetIntProperty(
                _cf, rocksdb::Slice("rocksdb.num-entries-active-mem-table"), &cntMem));
            invariant(_db->GetRootDB()->GetIntProperty(
                _cf, rocksdb::Slice("rocksdb.num-entries-imm-mem-tables"), &cntImm));

            std::vector<rocksdb::LiveFileMetaData> allFiles;
            _db->GetRootDB()->GetLiveFilesMetaData(&allFiles);
            for (const auto& f : allFiles) {
                if (!NamespaceString::oplog(f.column_family_name)) {
                    continue;
                }
                invariant(f.num_entries >= f.num_deletions);
                cntSST += f.num_entries - f.num_deletions;
            }
            return cntMem + cntImm + cntSST;
        }
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        return _numRecords.load(std::memory_order_relaxed) + ru->getDeltaCounter(_numRecordsKey);
    }

    bool RocksRecordStore::cappedAndNeedDelete(long long dataSizeDelta,
                                               long long numRecordsDelta) const {
        invariant(_isCapped);

        if (_dataSize.load() + dataSizeDelta > _cappedMaxSize) return true;

        if ((_cappedMaxDocs != -1) && (_numRecords.load() + numRecordsDelta > _cappedMaxDocs))
            return true;

        return false;
    }

    void RocksRecordStore::reclaimOplog(OperationContext* opCtx) {
        auto persistedTimestamp = _engine->getLastStableRecoveryTimestamp()
                                      ? *_engine->getLastStableRecoveryTimestamp()
                                      : Timestamp::min();
        std::vector<rocksdb::LiveFileMetaData> allFiles, oplogFiles, pendingDelFiles;
        ssize_t oplogTotalBytes = 0, pendingDelSize = 0;
        std::string maxDelKey;
        _db->GetRootDB()->GetLiveFilesMetaData(&allFiles);
        for (const auto& f : allFiles) {
            if (!NamespaceString::oplog(f.column_family_name)) {
                continue;
            }
            auto largestKey = rocksdb::Slice(f.largestkey);
            largestKey.remove_suffix(sizeof(rocksdb::RocksTimeStamp));
            auto largestTs = _prefixedKeyToTimestamp(largestKey);
            if (largestTs > persistedTimestamp) {
                continue;
            }
            oplogFiles.push_back(f);
            oplogTotalBytes += f.size;
        }
        std::sort(oplogFiles.begin(), oplogFiles.end(),
                  [&](const rocksdb::LiveFileMetaData& a, const rocksdb::LiveFileMetaData& b) {
                      return a.smallestkey < b.smallestkey;
                  });
        for (const auto& f : oplogFiles) {
            if (oplogTotalBytes - pendingDelSize > _cappedMaxSize + static_cast<ssize_t>(f.size)) {
                pendingDelSize += f.size;
                pendingDelFiles.push_back(f);
                auto largestKey = rocksdb::Slice(f.largestkey);
                largestKey.remove_suffix(sizeof(rocksdb::RocksTimeStamp));
                if (largestKey.compare(rocksdb::Slice(maxDelKey)) > 0) {
                    maxDelKey = largestKey.ToString();
                }
            }
        }
        if (pendingDelFiles.size() < static_cast<uint32_t>(minSSTFileCountReserved.load())) {
            return;
        }
        {
            // update _cappedOldestKeyHint
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
            invariant(ru);
            std::unique_ptr<RocksIterator> iterator(ru->NewIterator(_cf, _prefix, _isOplog));
            rocksdb::Slice maxDelKeyWithoutPrefix(maxDelKey.data(), maxDelKey.size());
            maxDelKeyWithoutPrefix.remove_prefix(_prefix.size());
            rocksPrepareConflictRetry(opCtx, [&] {
                iterator->Seek(rocksdb::Slice(maxDelKeyWithoutPrefix));
                return iterator->status();
            });
            if (!iterator->Valid()) {
                LOGV2_WARNING(0, "reclaimOplog oplog seekto failed",
                              "key"_attr = _prefixedKeyToTimestamp(maxDelKey),
                              "error"_attr = rocksToMongoStatus(iterator->status()));
                return;
            }
            rocksPrepareConflictRetry(opCtx, [&] {
                iterator->Next();
                return iterator->status();
            });
            if (!iterator->Valid()) {
                if (!iterator->status().ok()) {
                    LOGV2_WARNING(0, "reclaimOplog oplog lookup next",
                                  "key"_attr = _prefixedKeyToTimestamp(maxDelKey),
                                  "error"_attr = rocksToMongoStatus(iterator->status()));
                }
                return;
            }
            LOGV2(0, "reclaimOplog update _cappedOldestKeyHint",
                  "from"_attr = Timestamp(_cappedOldestKeyHint.getLong()),
                  "to"_attr = Timestamp(_makeRecordId(iterator->key(), _keyFormat).getLong()),
                  "maxDelKey"_attr = _prefixedKeyToTimestamp(maxDelKey));
            // TODO(deyukong): we should persist _cappedOldestKeyHint because
            // there is a small chance that DeleteFilesInRange successes but CompactRange fails
            // and then crashes and restarts. In this case, there is a hole at the front of oplogs.
            // Currently, we just log this error.
            _cappedOldestKeyHint =
                std::max(_cappedOldestKeyHint, _makeRecordId(iterator->key(), _keyFormat));

            invariant(!_cappedOldestKey.empty());
            _counterManager->updateCounter(_cappedOldestKey, _cappedOldestKeyHint.getLong());
            _counterManager->sync();
            LOGV2(0, "save _cappedOldestKeyHint", "value"_attr = _cappedOldestKeyHint);
            {
                // for test
                _loadCountFromCountManager(opCtx);
            }
        }
        auto s = _compactionScheduler->compactOplog(_cf, _prefix, maxDelKey);

        if (s.isOK()) {
            LOGV2(0, "reclaimOplog success", "to"_attr = _prefixedKeyToTimestamp(maxDelKey));
        } else {
            LOGV2(0, "reclaimOplog fail", "to"_attr = _prefixedKeyToTimestamp(maxDelKey),
                  "error"_attr = s);
        }
    }

    Status RocksRecordStore::doInsertRecords(OperationContext* opCtx, std::vector<Record>* records,
                                             const std::vector<Timestamp>& timestamps) {
        auto insertRecord = [this](OperationContext* opCtx, const Record& record,
                                   Timestamp timestamp) {
            auto data = record.data.data();
            auto len = record.data.size();

            dassert(opCtx->lockState()->isWriteLocked());
            if (_isCapped && len > _cappedMaxSize) {
                return StatusWith<RecordId>(ErrorCodes::BadValue,
                                            "object to insert exceeds cappedMaxSize");
            }

            RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
            invariant(ru);
            auto txn = ru->getTransaction();
            invariant(txn);

            RecordId loc;
            if (_isOplog) {
                StatusWith<RecordId> status = record_id_helpers::extractKeyOptime(data, len);
                if (!status.isOK()) {
                    return status;
                }
                loc = status.getValue();
            } else {
                switch (_keyFormat) {
                    case KeyFormat::Long:
                        loc = _nextId();
                        break;
                    case KeyFormat::String:
                        loc = record.id;
                        break;
                }
            }

            Timestamp ts;
            if (timestamp.isNull() && _isOplog) {
                ts = Timestamp(loc.getLong());
                opCtx->recoveryUnit()->setOrderedCommit(false);
            } else {
                ts = timestamp;
            }
            if (!ts.isNull()) {
                auto s = opCtx->recoveryUnit()->setTimestamp(ts);
                invariant(s.isOK(), s.reason());
            }
            invariantRocksOK(ROCKS_OP_CHECK(txn->Put(
                _cf, _makePrefixedKey(_prefix, loc, _keyFormat), rocksdb::Slice(data, len))));

            _changeNumRecords(opCtx, 1);
            _increaseDataSize(opCtx, len);

            return StatusWith<RecordId>(loc);
        };

        int index = 0;
        for (auto& record : *records) {
            StatusWith<RecordId> res = insertRecord(opCtx, record, timestamps[index++]);
            if (!res.isOK()) return res.getStatus();

            record.id = res.getValue();
        }
        return Status::OK();
    }

    Status RocksRecordStore::doUpdateRecord(OperationContext* opCtx, const RecordId& loc,
                                            const char* data, int len) {
        std::string key(_makePrefixedKey(_prefix, loc, _keyFormat));

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto txn = ru->getTransaction();
        invariant(txn);

        std::string old_value;
        auto status =
            rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(_cf, key, &old_value); });
        invariantRocksOK(status);

        int old_length = old_value.size();

        invariantRocksOK(ROCKS_OP_CHECK(txn->Put(_cf, key, rocksdb::Slice(data, len))));

        _increaseDataSize(opCtx, len - old_length);

        return Status::OK();
    }

    bool RocksRecordStore::updateWithDamagesSupported() const { return false; }

    StatusWith<RecordData> RocksRecordStore::doUpdateWithDamages(
        OperationContext* opCtx, const RecordId& loc, const RecordData& oldRec,
        const char* damageSource, const mutablebson::DamageVector& damages) {
        MONGO_UNREACHABLE;
    }

    void RocksRecordStore::printRecordMetadata(OperationContext* opCtx, const RecordId& recordId,
                                               std::set<Timestamp>* recordTimestamps) const {
        MONGO_UNIMPLEMENTED;  // TODO
    }

    std::unique_ptr<SeekableRecordCursor> RocksRecordStore::getCursor(OperationContext* opCtx,
                                                                      bool forward) const {
        RecordId startIterator;
        if (_isOplog && forward) {
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
            // If we already have a snapshot we don't know what it can see, unless we know no
            // one else could be writing (because we hold an exclusive lock).
            // TODO re-enable or remove
            // invariant(!ru->inActiveTxn() || opCtx->lockState()->isCollectionLockedForMode(
            //                                     NamespaceString(ns(opCtx)), MODE_X));

            startIterator = _cappedOldestKeyHint;
            ru->setIsOplogReader();
        }

        return std::make_unique<Cursor>(opCtx, _db, _cf, _prefix, _keyFormat, forward, _isCapped,
                                        _isOplog, startIterator);
    }

    Status RocksRecordStore::doTruncate(OperationContext* opCtx) {
        // We can't use getCursor() here because we need to ignore the visibility of records (i.e.
        // we need to delete all records, regardless of visibility)
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        std::unique_ptr<RocksIterator> iterator(ru->NewIterator(_cf, _prefix, _isOplog));
        if (!_isOplog) {
            for (rocksPrepareConflictRetry(opCtx,
                                           [&] {
                                               iterator->SeekToFirst();
                                               return iterator->status();
                                           });
                 iterator->Valid(); rocksPrepareConflictRetry(opCtx, [&] {
                     iterator->Next();
                     return iterator->status();
                 })) {
                deleteRecord(opCtx, _makeRecordId(iterator->key(), _keyFormat));
            }
        } else {
            iterator->SeekToFirst();
            if (iterator->Valid()) {
                const bool inclusive = true;
                cappedTruncateAfter(opCtx, _makeRecordId(iterator->key(), _keyFormat), inclusive,
                                    nullptr);
            }
        }

        return rocksToMongoStatus(iterator->status());
    }

    Status RocksRecordStore::doRangeTruncate(OperationContext* opCtx, const RecordId& minRecordId,
                                             const RecordId& maxRecordId, int64_t hintDataSizeDiff,
                                             int64_t hintNumRecordsDiff) {
        MONGO_UNIMPLEMENTED;  // TODO
    }

    Status RocksRecordStore::doCompact(OperationContext* opCtx,
                                       boost::optional<int64_t> freeSpaceTargetMB) {
        std::string beginString(_makePrefixedKey(_prefix, RecordId(), _keyFormat));
        std::string endString(_makePrefixedKey(_prefix, RecordId::maxLong(), _keyFormat));
        rocksdb::Slice beginRange(beginString);
        rocksdb::Slice endRange(endString);
        // TODO(wolfkdy): support it
        return Status(ErrorCodes::InvalidOptions,
                      "not supported, use rocksdbCompact server paramter instead");
        // return rocksToMongoStatus(_db->CompactRange(&beginRange, &endRange));
    }

    void RocksRecordStore::validate(OperationContext* opCtx, bool full, ValidateResults* results) {
        // NOTE(cuixin): SERVER-38886 Refactor RecordStore::validate implementations
        // should not do any work, the code is move to _genericRecordStoreValidate
    }

    void RocksRecordStore::appendNumericCustomStats(OperationContext* opCtx, BSONObjBuilder* result,
                                                    double scale) const {
        result->appendBool("capped", _isCapped);
        if (_isCapped) {
            result->appendNumber("max", static_cast<long long>(_cappedMaxDocs));
            result->appendNumber("maxSize", static_cast<long long>(_cappedMaxSize / scale));
        }
    }

    Status RocksRecordStore::oplogDiskLocRegisterImpl(OperationContext* opCtx,
                                                      const Timestamp& opTime, bool orderedCommit) {
        invariant(_isOplog);
        StatusWith<RecordId> record = record_id_helpers::keyForOptime(opTime, KeyFormat::Long);

        opCtx->recoveryUnit()->setOrderedCommit(orderedCommit);
        if (!orderedCommit) {
            // This labels the current transaction with a timestamp.
            // This is required for oplog visibility to work correctly, as RocksDB uses the
            // transaction list to determine where there are holes in the oplog.
            return opCtx->recoveryUnit()->setTimestamp(opTime);
        }
        invariant(_oplogManager);
        _oplogManager->setOplogReadTimestamp(opTime);
        return Status::OK();
    }

    void RocksRecordStore::waitForAllEarlierOplogWritesToBeVisibleImpl(
        OperationContext* opCtx) const {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        invariant(_oplogManager);
        if (_oplogManager->isRunning()) {
            _oplogManager->waitForAllEarlierOplogWritesToBeVisible(this, opCtx);
        }
    }

    RecordId RocksRecordStore::getLargestKey(OperationContext* opCtx) const {
        auto txn = std::unique_ptr<rocksdb::TOTransaction>(
            _db->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TOTransactionOptions()));
        invariant(txn);
        std::unique_ptr<RocksIterator> iter(
            RocksRecoveryUnit::NewIteratorWithTxn(txn.get(), _cf, _prefix));
        iter->SeekPrefix("");
        return iter->Valid() ? _getLargestKey(opCtx, iter.get()) : RecordId{};
    }

    RecordId RocksRecordStore::_getLargestKey(OperationContext* opCtx, RocksIterator* iter) const {
        rocksPrepareConflictRetry(opCtx, [&] {
            iter->SeekToLast();
            return iter->status();
        });
        dassert(iter->Valid());
        rocksdb::Slice lastSlice = iter->key();
        return _makeRecordId(lastSlice, _keyFormat);
    }

    void RocksRecordStore::reserveRecordIds(OperationContext* opCtx, std::vector<RecordId>* out,
                                            size_t nRecords) {
        auto id = _nextIdNum.fetchAndAdd(nRecords);
        for (size_t i = 0; i < nRecords; ++i) {
            out->push_back(RecordId{id++});
        }
    }

    void RocksRecordStore::updateStatsAfterRepair(OperationContext* opCtx, long long numRecords,
                                                  long long dataSize) {
        sizeRecoveryState(getGlobalServiceContext())
            .markCollectionAsAlwaysNeedsSizeAdjustment(_ident);
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        ru->resetDeltaCounters();
        if (!_isOplog) {
            _numRecords.store(numRecords);
            _dataSize.store(dataSize);
            _counterManager->updateCounter(_numRecordsKey, numRecords);
            _counterManager->updateCounter(_dataSizeKey, dataSize);
        }
    }

    void RocksRecordStore::doCappedTruncateAfter(OperationContext* opCtx, const RecordId& end,
                                                 bool inclusive,
                                                 const AboutToDeleteRecordCallback& aboutToDelete) {
        // Only log messages at a lower level here for testing.
        int logLevel = getTestCommandsEnabled() ? 0 : 2;

        std::unique_ptr<SeekableRecordCursor> cursor = getCursor(opCtx, true);
        LOGV2_DEBUG(0, logLevel, "Truncating capped collection in RocksDB record store",
                    logAttrs(ns(opCtx)), "inclusive"_attr = inclusive);

        auto record = cursor->seekExact(end);
        invariant(record, str::stream() << "Failed to seek to the record located at " << end);

        int64_t recordsRemoved = 0;
        RecordId lastKeptId;
        RecordId firstRemovedId;
        RecordId lastRemovedId;

        if (inclusive) {
            std::unique_ptr<SeekableRecordCursor> reverseCursor = getCursor(opCtx, false);
            invariant(reverseCursor->seekExact(end));
            auto prev = reverseCursor->next();
            if (prev) {
                lastKeptId = prev->id;
            } else {
                invariant(_isOplog);
                lastKeptId = RecordId();
            }
            firstRemovedId = end;
            LOGV2(0, "lastKeptId", "lastKeptId"_attr = lastKeptId);
        } else {
            // If not deleting the record located at 'end', then advance the cursor to the first
            // record
            // that is being deleted.
            record = cursor->next();
            if (!record) {
                LOGV2_DEBUG(0, logLevel, "No records to delete for truncation");
                return;  // No records to delete.
            }
            lastKeptId = end;
            firstRemovedId = record->id;
            LOGV2(0, "firstRemovedId", "firstRemovedId"_attr = Timestamp(lastKeptId.getLong()));
        }

        WriteUnitOfWork wuow(opCtx);
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        // NOTE(wolfkdy): truncate del should have no commit-ts
        invariant(ru->getCommitTimestamp() == Timestamp());
        {
            stdx::lock_guard<Latch> cappedCallbackLock(_cappedCallbackMutex);
            do {
                if (!_isOplog) {
                    deleteRecord(opCtx, record->id);
                }
                recordsRemoved++;
                lastRemovedId = record->id;
                LOGV2_DEBUG(0, logLevel, "Record id to delete for truncation", logAttrs(ns(opCtx)),
                            "recordId"_attr = record->id,
                            "timestamp"_attr = Timestamp(record->id.getLong()));
            } while ((record = cursor->next()));
        }
        wuow.commit();
        // Compute the number and associated sizes of the records to delete.
        // Truncate the collection starting from the record located at 'firstRemovedId' to the end
        // of
        // the collection.
        LOGV2(0, "Truncating collection", logAttrs(ns(opCtx)), "fromRecordId"_attr = firstRemovedId,
              "fromTimestamp"_attr = Timestamp(firstRemovedId.getLong()),
              "toRecordId"_attr = lastRemovedId,
              "toTimestamp"_attr = Timestamp(lastRemovedId.getLong()),
              "numRemoved"_attr = recordsRemoved);

        if (_isOplog) {
            int retryCnt = 0;
            Timestamp truncTs(lastKeptId.getLong());
            {
                auto alterClient = opCtx->getServiceContext()->getService()->makeClient(
                    "reconstruct-check-oplog-removed");
                AlternativeClientRegion acr(alterClient);
                const auto tmpOpCtx = cc().makeOperationContext();
                /* TODO(wolfkdy): RocksHarnessHelper did not register global rocksEngine
                 * so RocksRecoveryUnit wont be set atomitly by
                 * StorageClientObserver::onCreateOperationContext remove this line below when this
                 * issue is fixed
                 */
                tmpOpCtx->setRecoveryUnit(std::unique_ptr<RecoveryUnit>(_engine->newRecoveryUnit()),
                                          WriteUnitOfWork::RecoveryUnitState::kNotInUnitOfWork);
                auto checkRemovedOK = [&] {
                    RocksRecoveryUnit::getRocksRecoveryUnit(tmpOpCtx.get())->abandonSnapshot();
                    std::unique_ptr<SeekableRecordCursor> cursor1 = getCursor(tmpOpCtx.get(), true);
                    auto rocksCursor = dynamic_cast<RocksRecordStore::Cursor*>(cursor1.get());
                    auto record = rocksCursor->seekToLast();

                    Timestamp lastTs = record ? Timestamp(record->id.getLong()) : Timestamp();
                    invariant(lastTs >= truncTs);
                    LOGV2_DEBUG(0, logLevel, "lastTs, truncTs", "lastTs"_attr = lastTs,
                                "truncTs"_attr = truncTs);
                    return (lastTs == truncTs);
                };
                while (true) {
                    auto s = _compactionScheduler->compactOplog(
                        _cf, _makePrefixedKey(_prefix, firstRemovedId, _keyFormat),
                        _makePrefixedKey(_prefix, lastRemovedId, _keyFormat));
                    invariant(s.isOK());
                    invariant(retryCnt++ < 10);
                    if (checkRemovedOK()) {
                        break;
                    }
                    LOGV2_DEBUG(0, logLevel, "retryCnt", "retryCnt"_attr = retryCnt);
                }
            }
            if (_isOplog) {
                // Immediately rewind visibility to our truncation point, to prevent new
                // transactions from appearing.
                LOGV2_DEBUG(0, logLevel, "Rewinding oplog visibility point after truncation",
                            "timestamp"_attr = truncTs);

                if (!serverGlobalParams.enableMajorityReadConcern &&
                    _engine->getOldestTimestamp() > truncTs) {
                    // If majority read concern is disabled, we must set the oldest timestamp along
                    // with the commit timestamp. Otherwise, the commit timestamp might be set
                    // behind the oldest timestamp.
                    const bool force = true;
                    _engine->setOldestTimestamp(truncTs, force);
                } else {
                    const bool force = false;
                    invariantRocksOK(_engine->getDB()->SetTimeStamp(
                        rocksdb::TimeStampType::kCommitted,
                        rocksdb::RocksTimeStamp(truncTs.asULL()), force));
                }

                _oplogManager->setOplogReadTimestamp(truncTs);
                LOGV2_DEBUG(0, 1, "Truncation new read timestamp", "timestamp"_attr = truncTs);
            }
        }
    }

    RecordId RocksRecordStore::_nextId() {
        invariant(!_isOplog);
        return RecordId(_nextIdNum.fetchAndAdd(1));
    }

    rocksdb::Slice RocksRecordStore::_makeKey(const RecordId& loc, KeyFormat keyFormat,
                                              int64_t* storage) {
        const char* data;
        size_t size;

        switch (keyFormat) {
            case KeyFormat::Long:
                *storage = endian::nativeToBig(loc.getLong());
                data = reinterpret_cast<const char*>(storage);
                size = sizeof(*storage);
                break;
            case KeyFormat::String:
                auto str = loc.getStr();
                data = str.rawData();
                size = str.size();
                break;
        }

        return rocksdb::Slice(data, size);
    }

    std::string RocksRecordStore::_makePrefixedKey(const std::string& prefix, const RecordId& loc,
                                                   KeyFormat keyFormat) {
        int64_t storage;
        auto encodedLoc = _makeKey(loc, keyFormat, &storage);
        std::string key(prefix);
        key.append(encodedLoc.data(), encodedLoc.size());
        return key;
    }

    Timestamp RocksRecordStore::_prefixedKeyToTimestamp(const rocksdb::Slice& key) const {
        rocksdb::Slice slice(key);
        slice.remove_prefix(_prefix.size());
        return Timestamp(_makeRecordId(slice, _keyFormat).getLong());
    }

    Timestamp RocksRecordStore::_prefixedKeyToTimestamp(const std::string& key) const {
        return _prefixedKeyToTimestamp(rocksdb::Slice(key));
    }

    RecordId RocksRecordStore::_makeRecordId(const rocksdb::Slice& slice, KeyFormat keyFormat) {
        switch (keyFormat) {
            case KeyFormat::Long:
                invariant(slice.size() == sizeof(int64_t));
                return RecordId{
                    endian::bigToNative(*reinterpret_cast<const int64_t*>(slice.data()))};
            case KeyFormat::String:
                return RecordId{slice.data(), static_cast<int32_t>(slice.size())};
        }

        MONGO_UNREACHABLE;
    }

    bool RocksRecordStore::findRecord(OperationContext* opCtx, const RecordId& loc,
                                      RecordData* out) const {
        RecordData rd = _getDataFor(_cf, _prefix, opCtx, loc, _keyFormat);
        if (rd.data() == NULL) return false;
        *out = rd;
        return true;
    }

    RecordData RocksRecordStore::_getDataFor(rocksdb::ColumnFamilyHandle* cf,
                                             const std::string& prefix, OperationContext* opCtx,
                                             const RecordId& loc, KeyFormat keyFormat) {
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);

        std::string valueStorage;
        auto key = _makePrefixedKey(prefix, loc, keyFormat);
        auto status =
            rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(cf, key, &valueStorage); });
        if (status.IsNotFound()) {
            return RecordData(nullptr, 0);
        }
        invariantRocksOK(status);

        SharedBuffer data = SharedBuffer::allocate(valueStorage.size());
        memcpy(data.get(), valueStorage.data(), valueStorage.size());
        return RecordData(data, valueStorage.size());
    }

    void RocksRecordStore::_changeNumRecords(OperationContext* opCtx, int64_t amount) {
        if (!_tracksSizeAdjustments) {
            return;
        }

        if (!sizeRecoveryState(getGlobalServiceContext()).collectionNeedsSizeAdjustment(_ident)) {
            return;
        }

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        ru->incrementCounter(_numRecordsKey, &_numRecords, amount);
    }

    void RocksRecordStore::_increaseDataSize(OperationContext* opCtx, int64_t amount) {
        if (!_tracksSizeAdjustments) {
            return;
        }
        if (!sizeRecoveryState(getGlobalServiceContext()).collectionNeedsSizeAdjustment(_ident)) {
            return;
        }
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        ru->incrementCounter(_dataSizeKey, &_dataSize, amount);
    }

    // --------

    RocksRecordStore::Cursor::Cursor(OperationContext* opCtx, rocksdb::TOTransactionDB* db,
                                     rocksdb::ColumnFamilyHandle* cf, std::string prefix,
                                     KeyFormat keyFormat, bool forward, bool isCapped, bool isOplog,
                                     RecordId startIterator)
        : _opCtx(opCtx),
          _db(db),
          _cf(cf),
          _prefix(std::move(prefix)),
          _keyFormat(keyFormat),
          _forward(forward),
          _isCapped(isCapped),
          _isOplog(isOplog) {
        if (_isOplog) {
            _oplogVisibleTs =
                RocksRecoveryUnit::getRocksRecoveryUnit(opCtx)->getOplogVisibilityTs();
        }
        if (!startIterator.isNull()) {
            // This is a hack to speed up first/last record retrieval from the oplog
            _needFirstSeek = false;
            _lastLoc = startIterator;
            iterator();
            _skipNextAdvance = true;
            _eof = false;
        }
    }

    // requires !_eof
    void RocksRecordStore::Cursor::positionIterator() {
        _skipNextAdvance = false;
        int64_t locStorage;
        auto seekTarget = RocksRecordStore::_makeKey(_lastLoc, _keyFormat, &locStorage);
        if (!_iterator->Valid() || _iterator->key() != seekTarget) {
            rocksPrepareConflictRetry(_opCtx, [&] {
                _iterator->Seek(seekTarget);
                return _iterator->status();
            });
            if (!_iterator->Valid()) {
                invariantRocksOK(_iterator->status());
            }
        }

        if (_forward) {
            // If _skipNextAdvance is true we landed after where we were. Return our new location on
            // the next call to next().
            _skipNextAdvance =
                !_iterator->Valid() || _lastLoc != _makeRecordId(_iterator->key(), _keyFormat);
        } else {
            // Seek() lands on or after the key, while reverse cursors need to land on or before.
            if (!_iterator->Valid()) {
                // Nothing left on or after.
                rocksPrepareConflictRetry(_opCtx, [&] {
                    _iterator->SeekToLast();
                    return _iterator->status();
                });
                invariantRocksOK(_iterator->status());
                _skipNextAdvance = true;
            } else {
                if (_lastLoc != _makeRecordId(_iterator->key(), _keyFormat)) {
                    // Landed after. This is true: iterator->key() > _lastLoc
                    // Since iterator is valid and Seek() landed after key,
                    // iterator will still be valid after we call Prev().
                    _skipNextAdvance = true;
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        _iterator->Prev();
                        return _iterator->status();
                    });
                }
            }
        }
        // _lastLoc != _makeRecordId(_iterator->key()) indicates that the record _lastLoc was
        // deleted. In this case, mark _eof only if the collection is capped.
        _eof = !_iterator->Valid() ||
               (_isCapped && _lastLoc != _makeRecordId(_iterator->key(), _keyFormat));
    }

    rocksdb::Iterator* RocksRecordStore::Cursor::iterator() {
        if (_iterator.get() != nullptr) {
            return _iterator.get();
        }
        _iterator.reset(RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(
            _cf, _prefix, _isOplog /* isOplog */));
        if (!_needFirstSeek) {
            positionIterator();
        }
        return _iterator.get();
    }

    boost::optional<Record> RocksRecordStore::Cursor::next() {
        if (_eof) {
            return {};
        }

        auto iter = iterator();
        // ignore _eof

        if (!_skipNextAdvance) {
            if (_needFirstSeek) {
                _needFirstSeek = false;
                if (_forward) {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->SeekToFirst();
                        return iter->status();
                    });
                } else {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->SeekToLast();
                        return iter->status();
                    });
                }
            } else {
                if (_forward) {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->Next();
                        return iter->status();
                    });
                } else {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->Prev();
                        return iter->status();
                    });
                }
            }
        }
        _skipNextAdvance = false;

        return curr();
    }

    boost::optional<Record> RocksRecordStore::Cursor::seekExact(const RecordId& id) {
        if (_oplogVisibleTs && id.getLong() > *_oplogVisibleTs) {
            _eof = true;
            return {};
        }
        _needFirstSeek = false;
        _skipNextAdvance = false;
        _iterator.reset();

        auto key = _makePrefixedKey(_prefix, id, _keyFormat);
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
        auto status =
            rocksPrepareConflictRetry(_opCtx, [&] { return ru->Get(_cf, key, &_seekExactResult); });

        if (status.IsNotFound()) {
            _eof = true;
            return {};
        }
        invariantRocksOK(status);

        _eof = false;
        _lastLoc = id;

        return {{_lastLoc, {_seekExactResult.data(), static_cast<int>(_seekExactResult.size())}}};
    }

    boost::optional<Record> RocksRecordStore::Cursor::seekNear(const RecordId& start) {
        auto id = start;
        if (_oplogVisibleTs && id.getLong() > *_oplogVisibleTs) {
            id = RecordId{*_oplogVisibleTs};
        }
        _needFirstSeek = false;
        _skipNextAdvance = false;
        _iterator.reset();

        int64_t storage;
        auto key = _makeKey(start, _keyFormat, &storage);
        auto it =
            RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(_cf, _prefix, _isOplog);

        auto status = rocksPrepareConflictRetry(_opCtx, [&] {
            if (_forward) {
                it->SeekForPrev(key);
            } else {
                it->Seek(key);
            }
            return it->status();
        });

        if (!it->Valid()) {
            status = rocksPrepareConflictRetry(_opCtx, [&] {
                if (_forward) {
                    it->Seek(key);
                } else {
                    it->SeekForPrev(key);
                }
                return it->status();
            });
        } else {
            invariantRocksOK(status);
        }

        if (!it->Valid()) {
            _eof = true;
            return {};
        }

        invariant(it->Valid());
        invariantRocksOK(status);

        _eof = false;
        _lastLoc = RocksRecordStore::_makeRecordId(it->key(), _keyFormat);

        auto value = it->value();
        return {{_lastLoc, {value.data(), static_cast<int>(value.size())}}};
    }

    boost::optional<Record> RocksRecordStore::Cursor::seekToLast() {
        _needFirstSeek = false;
        _skipNextAdvance = false;
        // do not support backwoard
        invariant(_forward);
        auto iter = iterator();
        rocksPrepareConflictRetry(_opCtx, [&] {
            iter->SeekToLast();
            return iter->status();
        });
        return curr();
    }

    void RocksRecordStore::Cursor::save() {
        try {
            if (_iterator) {
                _iterator.reset();
            }
            _oplogVisibleTs = boost::none;
        } catch (const WriteConflictException&) {
            // Ignore since this is only called when we are about to kill our transaction
            // anyway.
        }
    }

    void RocksRecordStore::Cursor::saveUnpositioned() {
        save();
        _eof = true;
    }

    bool RocksRecordStore::Cursor::restore(bool tolerateCappedRepositioning) {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
        if (_isOplog && _forward) {
            ru->setIsOplogReader();
            _oplogVisibleTs = ru->getOplogVisibilityTs();
        }
        if (!_iterator.get()) {
            _iterator.reset(ru->NewIterator(_cf, _prefix, _isOplog));
        }
        _skipNextAdvance = false;

        if (_eof) return true;
        if (_needFirstSeek) return true;

        positionIterator();
        // Return false if the collection is capped and we reached an EOF. Otherwise return true.
        if (_isOplog) {
            invariant(_isCapped);
        }
        return _isCapped && _eof ? false : true;
    }

    void RocksRecordStore::Cursor::detachFromOperationContext() {
        _opCtx = nullptr;
        _iterator.reset();
    }

    void RocksRecordStore::Cursor::reattachToOperationContext(OperationContext* opCtx) {
        _opCtx = opCtx;
        // iterator recreated in restore()
    }

    void RocksRecordStore::Cursor::setSaveStorageCursorOnDetachFromOperationContext(bool) {
        MONGO_UNIMPLEMENTED;  // TODO
    }

    boost::optional<Record> RocksRecordStore::Cursor::curr() {
        if (!_iterator->Valid()) {
            invariantRocksOK(_iterator->status());
            _eof = true;
            return {};
        }
        if (_oplogVisibleTs &&
            _makeRecordId(_iterator->key(), _keyFormat).getLong() > *_oplogVisibleTs) {
            _eof = true;
            return {};
        }
        _eof = false;
        _lastLoc = _makeRecordId(_iterator->key(), _keyFormat);

        auto dataSlice = _iterator->value();
        return {{_lastLoc, {dataSlice.data(), static_cast<int>(dataSlice.size())}}};
    }
}  // namespace mongo
