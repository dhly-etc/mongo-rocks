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

#include "rocks_engine.h"

#include <rocksdb/advanced_cache.h>
#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/comparator.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/version.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <mutex>

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/modules/rocks/src/rocks_parameters_gen.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_recovery.h"
#include "mongo/db/snapshot_window_options_gen.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/concurrency/semaphore_ticketholder.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/exit.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/processinfo.h"
#include "mongo_rate_limiter_checker.h"
#include "rocks_counter_manager.h"
#include "rocks_global_options.h"
#include "rocks_index.h"
#include "rocks_record_store.h"
#include "rocks_record_store_oplog_thread.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#define ROCKS_ERR(a)                      \
    do {                                  \
        s = (a);                          \
        if (!s.ok()) {                    \
            return rocksToMongoStatus(s); \
        }                                 \
    } while (0)

namespace mongo {

    class RocksEngine::RocksJournalFlusher : public BackgroundJob {
    public:
        explicit RocksJournalFlusher(RocksDurabilityManager* durabilityManager)
            : BackgroundJob(false /* deleteSelf */), _durabilityManager(durabilityManager) {}

        virtual std::string name() const { return "RocksJournalFlusher"; }

        virtual void run() override {
            ThreadClient tc(name(), getGlobalServiceContext()->getService());
            LOGV2_DEBUG(0, 1, "starting thread", "name"_attr = name());

            while (!_shuttingDown.load()) {
                try {
                    auto opCtx = tc->getOperationContext();
                    _durabilityManager->waitUntilDurable(opCtx, false);
                } catch (const AssertionException& e) {
                    invariant(e.code() == ErrorCodes::ShutdownInProgress);
                }

                int ms = storageGlobalParams.journalCommitIntervalMs.load();
                if (!ms) {
                    ms = 100;
                }

                MONGO_IDLE_THREAD_BLOCK;
                sleepmillis(ms);
            }
            LOGV2_DEBUG(0, 1, "stopping thread", "name"_attr = name());
        }

        void shutdown() {
            _shuttingDown.store(true);
            wait();
        }

    private:
        RocksDurabilityManager* _durabilityManager;  // not owned
        std::atomic<bool> _shuttingDown{false};      // NOLINT
    };

    namespace {
        std::set<NamespaceString> _backgroundThreadNamespaces;
        Mutex _backgroundThreadMutex;
        rocksdb::TOComparator comparator;
        rocksdb::TOComparator comparatorFake(0);
        bool isNsEnableTimestamp(const StringData& ns) {
            if (ns == "local.replset.minvalid" || ns == "local.oplog.rs") {
                return true;
            }
            if (ns.startsWith("local.")) {
                return false;
            }
            if (ns == "_mdb_catalog") {
                return false;
            }
            return true;
        }

    }  // namespace

    // first four bytes are the default prefix 0
    const std::string RocksEngine::kMetadataPrefix("\0\0\0\0metadata-", 13);
    const std::string RocksEngine::kStablePrefix("\0\0\0\0stableTs-", 13);

    const int RocksEngine::kDefaultJournalDelayMillis = 100;

    RocksEngine::RocksEngine(const std::string& path, bool durable, int formatVersion,
                             bool readOnly)
        : _path(path),
          _durable(durable),
          _formatVersion(formatVersion),
          _maxPrefix(0),
          _keepDataHistory(serverGlobalParams.enableMajorityReadConcern),
          _stableTimestamp(0),
          _initialDataTimestamp(0) {
        {  // create block cache
            uint64_t cacheSizeGB = rocksGlobalOptions.cacheSizeGB;
            if (cacheSizeGB == 0) {
                ProcessInfo pi;
                unsigned long long memSizeMB = pi.getMemSizeMB();
                if (memSizeMB > 0) {
                    // reserve 1GB for system and binaries, and use 30% of the rest
                    double cacheMB = (memSizeMB - 1024) * 0.5;
                    cacheSizeGB = static_cast<uint64_t>(cacheMB / 1024);
                }
                if (cacheSizeGB < 1) {
                    cacheSizeGB = 1;
                }
            }
            _blockCache = rocksdb::NewLRUCache(cacheSizeGB * 1024 * 1024 * 1024LL, 6);
        }
        _maxWriteMBPerSec = rocksGlobalOptions.maxWriteMBPerSec;
        _rateLimiter.reset(
            rocksdb::NewGenericRateLimiter(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024));
        if (rocksGlobalOptions.counters) {
            _statistics = rocksdb::CreateDBStatistics();
        }

        LOGV2(0, "clusterRole", "role"_attr = serverGlobalParams.clusterRole);

        // used in building options for the db
        _compactionScheduler = std::make_shared<RocksCompactionScheduler>();

        // Until the Replication layer installs a real callback, prevent truncating the oplog.
        setOldestActiveTransactionTimestampCallback(
            [](Timestamp) { return StatusWith(boost::make_optional(Timestamp::min())); });

        // TODO(wolfkdy): support readOnly mode
        invariant(!readOnly);

        // open DB
        _initDatabase();

        _counterManager.reset(new RocksCounterManager(_db.get(), _defaultCf.get(),
                                                      rocksGlobalOptions.crashSafeCounters));

        // open iterator
        auto txn = std::unique_ptr<rocksdb::TOTransaction>(
            _db->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TOTransactionOptions()));
        // metadata is write no-timestamped, so read no-timestamped
        rocksdb::ReadOptions readOpts;
        auto iter =
            std::unique_ptr<rocksdb::Iterator>(txn->GetIterator(readOpts, _defaultCf.get()));

        // find maxPrefix
        iter->SeekToLast();
        if (iter->Valid()) {
            // otherwise the DB is empty, so we just keep it at 0
            bool ok = extractPrefix(iter->key(), &_maxPrefix);
            // this is DB corruption here
            invariant(ok);
        }

        // Log ident to prefix map. also update _maxPrefix if there's any prefix bigger than
        // current _maxPrefix. Here we have no need to check conflict state since we'are
        // bootstraping and there wouldn't be any Prepares.
        {
            for (iter->Seek(kMetadataPrefix);
                 iter->Valid() && iter->key().starts_with(kMetadataPrefix); iter->Next()) {
                invariantRocksOK(iter->status());
                rocksdb::Slice ident(iter->key());
                ident.remove_prefix(kMetadataPrefix.size());
                // this could throw DBException, which then means DB corruption. We just let it fly
                // to the caller
                BSONObj identConfig(iter->value().data());
                BSONElement element = identConfig.getField("prefix");

                if (element.eoo() || !element.isNumber()) {
                    LOGV2(0, "Mongo metadata in RocksDB database is corrupted.");
                    invariant(false);
                }
                uint32_t identPrefix = static_cast<uint32_t>(element.numberInt());

                _identMap[StringData(ident.data(), ident.size())] = identConfig.getOwned();

                _maxPrefix = std::max(_maxPrefix, identPrefix);
            }
        }

        // just to be extra sure. we need this if last collection is oplog -- in that case we
        // reserve prefix+1 for oplog key tracker
        ++_maxPrefix;

        // start compaction thread and load dropped prefixes
        _compactionScheduler->start(_db.get(), _defaultCf.get());
        auto maxDroppedPrefix = _compactionScheduler->loadDroppedPrefixes(
            iter.get(), {_defaultCf.get(), _oplogCf.get()});
        _maxPrefix = std::max(_maxPrefix, maxDroppedPrefix);

        _durabilityManager.reset(
            new RocksDurabilityManager(_db.get(), _durable, _defaultCf.get(), _oplogCf.get()));
        _oplogManager.reset(new RocksOplogManager(_db.get(), this, _durabilityManager.get()));

        rocksdb::RocksTimeStamp ts(0);
        auto status = _db->QueryTimeStamp(rocksdb::TimeStampType::kStable, &ts);
        if (!status.IsNotFound()) {
            invariant(status.ok(), status.ToString());
            _recoveryTimestamp = Timestamp(ts);
            if (!_recoveryTimestamp.isNull()) {
                setInitialDataTimestamp(_recoveryTimestamp);
                setStableTimestamp(_recoveryTimestamp, false);
                LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kStorageRecovery},
                                    "Rocksdb recoveryTimestamp",
                                    "timestamp"_attr = _recoveryTimestamp);
            }
        }

        if (_durable) {
            _journalFlusher = std::make_unique<RocksJournalFlusher>(_durabilityManager.get());
            _journalFlusher->go();
        }
    }

    RocksEngine::~RocksEngine() { cleanShutdown(); }

    void RocksEngine::_initDatabase() {
        // open DB
        rocksdb::TOTransactionDB* db = nullptr;
        rocksdb::Status s;

        const bool newDB = [&]() {
            const auto path = boost::filesystem::path(_path) / "CURRENT";
            return !boost::filesystem::exists(path);
        }();
        if (newDB) {
            // init manifest so list column families will not fail when db is empty.
            invariantRocksOK(rocksdb::TOTransactionDB::Open(
                _options(false /* isOplog */, false /* trimHistory */),
                rocksdb::TOTransactionDBOptions(rocksGlobalOptions.maxConflictCheckSizeMB), _path,
                kStablePrefix, &db));
            invariantRocksOK(db->Close());
        }

        const bool hasOplog = [&]() {
            std::vector<std::string> columnFamilies;
            s = rocksdb::DB::ListColumnFamilies(
                _options(false /* isOplog */, false /* trimHIstory */), _path, &columnFamilies);
            invariantRocksOK(s);

            auto it = std::find(columnFamilies.begin(), columnFamilies.end(),
                                NamespaceString::kRsOplogNamespace.ns().toString());
            return (it != columnFamilies.end());
        }();

        // init oplog columnfamily if not exists.
        if (!hasOplog) {
            rocksdb::ColumnFamilyHandle* cf = nullptr;
            invariantRocksOK(rocksdb::TOTransactionDB::Open(
                _options(false /* isOplog */, false /* trimHistory */),
                rocksdb::TOTransactionDBOptions(rocksGlobalOptions.maxConflictCheckSizeMB), _path,
                kStablePrefix, &db));
            invariantRocksOK(
                db->CreateColumnFamily(_options(true /* isOplog */, false /* trimHistory */),
                                       NamespaceString::kRsOplogNamespace.ns().toString(), &cf));
            invariantRocksOK(db->DestroyColumnFamilyHandle(cf));
            invariantRocksOK(db->Close());
            LOGV2(0, "init oplog cf success");
        }

        std::vector<rocksdb::ColumnFamilyHandle*> cfs;

        std::vector<rocksdb::ColumnFamilyDescriptor> open_cfds = {
            {rocksdb::kDefaultColumnFamilyName,
             _options(false /* isOplog */, false /* trimHistory */)},
            {NamespaceString::kRsOplogNamespace.ns().toString(),
             _options(true /* isOplog */, false /* trimHistory */)}};

        const bool trimHistory = true;
        std::vector<rocksdb::ColumnFamilyDescriptor> trim_cfds = {
            {rocksdb::kDefaultColumnFamilyName, _options(false /* isOplog */, trimHistory)},
            {NamespaceString::kRsOplogNamespace.ns().toString(),
             _options(true /* isOplog */, trimHistory)}};

        const std::string trimTs = "";

        s = rocksdb::TOTransactionDB::Open(
            _options(false /* isOplog */, false /* trimHistory */),
            rocksdb::TOTransactionDBOptions(rocksGlobalOptions.maxConflictCheckSizeMB), _path,
            open_cfds, &cfs, trim_cfds, trimHistory, kStablePrefix, &db);

        invariantRocksOK(s);
        invariant(cfs.size() == 2);
        invariant(cfs[0]->GetName() == rocksdb::kDefaultColumnFamilyName);
        invariant(cfs[1]->GetName() == NamespaceString::kRsOplogNamespace.ns());
        _db.reset(db);
        _defaultCf.reset(cfs[0]);
        _oplogCf.reset(cfs[1]);
    }

    std::map<int, std::vector<uint64_t>> RocksEngine::getDefaultCFNumEntries() const {
        std::map<int, std::vector<uint64_t>> numEntriesMap;

        std::vector<rocksdb::LiveFileMetaData> allFiles;
        _db->GetRootDB()->GetLiveFilesMetaData(&allFiles);
        for (const auto& f : allFiles) {
            if (NamespaceString::oplog(f.column_family_name)) {
                continue;
            }

            if (numEntriesMap.find(f.level) == numEntriesMap.end()) {
                numEntriesMap[f.level] = std::vector<uint64_t>(2, 0);
            }

            numEntriesMap[f.level][0] += f.num_entries;
            numEntriesMap[f.level][1] += f.num_deletions;
        }

        return numEntriesMap;
    }

    int64_t RocksEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
        stdx::lock_guard<Latch> lk(_identObjectMapMutex);

        auto indexIter = _identIndexMap.find(ident);
        if (indexIter != _identIndexMap.end()) {
            return static_cast<int64_t>(indexIter->second->getSpaceUsedBytes(opCtx));
        }
        auto collectionIter = _identCollectionMap.find(ident);
        if (collectionIter != _identCollectionMap.end()) {
            return collectionIter->second->storageSize(opCtx);
        }

        // this can only happen if collection or index exists, but it's not opened (i.e.
        // getRecordStore or getSortedDataInterface are not called)
        return 1;
    }

    void RocksEngine::flushAllFiles(OperationContext* opCtx, bool) {
        LOGV2_DEBUG(0, 1, "RocksEngine::flushAllFiles");
        _counterManager->sync();
        _durabilityManager->waitUntilDurable(opCtx, true);
    }

    Status RocksEngine::beginBackup(OperationContext* opCtx) {
        _counterManager->sync();
        return rocksToMongoStatus(_db->PauseBackgroundWork());
    }

    void RocksEngine::endBackup(OperationContext* opCtx) { _db->ContinueBackgroundWork(); }

    void RocksEngine::setOldestActiveTransactionTimestampCallback(
        StorageEngine::OldestActiveTransactionTimestampCallback callback) {
        stdx::lock_guard<Latch> lk(_oldestActiveTransactionTimestampCallbackMutex);
        _oldestActiveTransactionTimestampCallback = std::move(callback);
    };

    RecoveryUnit* RocksEngine::newRecoveryUnit() { return new RocksRecoveryUnit(_durable, this); }

    Status RocksEngine::createRecordStore(OperationContext* opCtx, const NamespaceString& nss,
                                          StringData ident, const CollectionOptions& options,
                                          KeyFormat keyFormat) {
        if (options.clusteredIndex) {
            // A clustered collection requires both CollectionOptions.clusteredIndex and
            // KeyFormat::String. For a clustered record store that is not associated with a
            // clustered collection KeyFormat::String is sufficient.
            uassert(6144100,
                    "RecordStore with CollectionOptions.clusteredIndex requires KeyFormat::String",
                    keyFormat == KeyFormat::String);
        }

        BSONObjBuilder configBuilder;
        auto s = _createIdent(ident, &configBuilder);
        if (s.isOK() && nss.isOplog()) {
            _oplogIdent = ident.toString();
            // oplog needs two prefixes, so we also reserve the next one
            uint64_t oplogTrackerPrefix = 0;
            {
                stdx::lock_guard<Latch> lk(_identMapMutex);
                oplogTrackerPrefix = ++_maxPrefix;
            }
            // we also need to write out the new prefix to the database. this is just an
            // optimization
            std::string encodedPrefix(encodePrefix(oplogTrackerPrefix));
            auto txn = _db->makeTxn();
            rocksdb::Status s;
            ROCKS_ERR(txn->Put(encodedPrefix, rocksdb::Slice()));
            ROCKS_ERR(txn->Commit());
        }
        return s;
    }

    std::unique_ptr<RecordStore> RocksEngine::getRecordStore(OperationContext* opCtx,
                                                             const NamespaceString& nss,
                                                             StringData ident,
                                                             const CollectionOptions& options) {
        rocksdb::ColumnFamilyHandle* cf = nullptr;
        if (nss.isOplog()) {
            _oplogIdent = ident.toString();
            cf = _oplogCf.get();
        } else {
            cf = _defaultCf.get();
        }

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        RocksRecordStore::Params params;
        params.nss = nss;
        params.uuid = options.uuid;
        params.ident = ident.toString();
        params.prefix = prefix;
        params.isCapped = options.capped;
        params.cappedMaxSize =
            params.isCapped ? (options.cappedSize ? options.cappedSize : 4096) : -1;
        params.cappedMaxDocs =
            params.isCapped ? (options.cappedMaxDocs ? options.cappedMaxDocs : -1) : -1;
        params.keyFormat = options.clusteredIndex ? KeyFormat::String : KeyFormat::Long;
        params.tracksSizeAdjustments = true;
        if (params.nss.isReplicated()) {
            rocksdb::TOTransaction::enableTimestamp(params.prefix);
        }
        std::unique_ptr<RocksRecordStore> recordStore = std::make_unique<RocksRecordStore>(
            this, cf, opCtx, params, std::make_unique<RocksRecordStoreOplogThread>());

        {
            stdx::lock_guard<Latch> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = recordStore.get();
        }

        return std::move(recordStore);
    }

    Status RocksEngine::createSortedDataInterface(OperationContext* opCtx,
                                                  const NamespaceString& nss,
                                                  const CollectionOptions& collOptions,
                                                  StringData ident, const IndexDescriptor* desc) {
        BSONObjBuilder configBuilder;
        // let index add its own config things
        RocksIndexBase::generateConfig(&configBuilder, _formatVersion, desc->version());
        return _createIdent(ident, &configBuilder);
    }

    Status RocksEngine::createColumnStore(OperationContext* opCtx, const NamespaceString& ns,
                                          const CollectionOptions& collOptions, StringData ident,
                                          const IndexDescriptor* desc) {
        // TODO
        MONGO_UNIMPLEMENTED;
    }

    std::unique_ptr<SortedDataInterface> RocksEngine::getSortedDataInterface(
        OperationContext* opCtx, const NamespaceString& nss, const CollectionOptions& collOptions,
        StringData ident, const IndexDescriptor* desc) {
        invariant(collOptions.uuid);

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        // oplog have no indexes
        invariant(!nss.isOplog());

        if (isNsEnableTimestamp(prefix)) {
            rocksdb::TOTransaction::enableTimestamp(prefix);
        }

        std::unique_ptr<RocksIndexBase> index;
        if (desc->unique()) {
            index = std::make_unique<RocksUniqueIndex>(
                _db.get(), _defaultCf.get(), prefix, *collOptions.uuid, ident.toString(),
                Ordering::make(desc->keyPattern()),
                collOptions.clusteredIndex ? KeyFormat::String : KeyFormat::Long, std::move(config),
                nss, desc->indexName(), desc->keyPattern(), desc->isPartial(), desc->isIdIndex());
        } else {
            auto si = std::make_unique<RocksStandardIndex>(
                _db.get(), _defaultCf.get(), prefix, *collOptions.uuid, ident.toString(),
                Ordering::make(desc->keyPattern()),
                collOptions.clusteredIndex ? KeyFormat::String : KeyFormat::Long, std::move(config),
                nss, desc->indexName(), desc->keyPattern());
            if (rocksGlobalOptions.singleDeleteIndex) {
                si->enableSingleDelete();
            }
            index = std::move(si);
        }
        {
            stdx::lock_guard<Latch> lk(_identObjectMapMutex);
            _identIndexMap[ident] = index.get();
        }
        return index;
    }

    std::unique_ptr<ColumnStore> RocksEngine::getColumnStore(OperationContext* opCtx,
                                                             const NamespaceString& nss,
                                                             const CollectionOptions& collOptions,
                                                             StringData ident,
                                                             const IndexDescriptor*) {
        // TODO
        MONGO_UNIMPLEMENTED;
    }

    // TODO(wolfkdy); this interface is not fully reviewed
    std::unique_ptr<RecordStore> RocksEngine::makeTemporaryRecordStore(OperationContext* opCtx,
                                                                       StringData ident,
                                                                       KeyFormat keyFormat) {
        BSONObjBuilder configBuilder;
        auto s = _createIdent(ident, &configBuilder);
        if (!s.isOK()) {
            return nullptr;
        }
        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        RocksRecordStore::Params params;
        params.nss = NamespaceString::kEmpty;
        params.ident = ident.toString();
        params.prefix = prefix;
        params.isCapped = false;
        params.cappedMaxSize = -1;
        params.cappedMaxDocs = -1;
        params.keyFormat = keyFormat;
        params.tracksSizeAdjustments = false;

        std::unique_ptr<RocksRecordStore> recordStore = std::make_unique<RocksRecordStore>(
            this, _defaultCf.get(), opCtx, params, std::make_unique<RocksRecordStoreOplogThread>());

        {
            stdx::lock_guard<Latch> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = recordStore.get();
        }

        return std::move(recordStore);
    }

    Status RocksEngine::dropSortedDataInterface(OperationContext* opCtx, StringData ident) {
        // TODO
        MONGO_UNIMPLEMENTED;
    }

    // cannot be rolled back
    Status RocksEngine::dropIdent(RecoveryUnit* ru, StringData ident,
                                  const StorageEngine::DropIdentCallback& onDrop) {
        auto config = _tryGetIdentConfig(ident);
        // happens rarely when dropped prefix markers are persisted but metadata changes
        // are lost due to system crash on standalone with default acknowledgement behavior
        if (config.isEmpty()) {
            LOGV2(0, "Cannot find ident to drop, ignoring", "ident"_attr = ident);
            return Status::OK();
        }

        rocksdb::WriteOptions writeOptions;
        writeOptions.sync = true;
        rocksdb::TOTransactionOptions txnOptions;
        std::unique_ptr<rocksdb::TOTransaction> txn(
            _db->BeginTransaction(writeOptions, txnOptions));

        auto status = txn->Delete(_defaultCf.get(), kMetadataPrefix + ident.toString());
        if (!status.ok()) {
            LOGV2(0, "dropIdent error", "status"_attr = status.ToString());
            txn->Rollback();
            return rocksToMongoStatus(status);
        }

        // calculate which prefixes we need to drop
        std::vector<std::string> prefixesToDrop;
        prefixesToDrop.push_back(_extractPrefix(config));
        if (_oplogIdent == ident.toString()) {
            // if we're dropping oplog, we also need to drop keys from RocksOplogKeyTracker (they
            // are stored at prefix+1)
            prefixesToDrop.push_back(rocksGetNextPrefix(prefixesToDrop[0]));
        }

        auto cf = (_oplogIdent == ident.toString()) ? _oplogCf.get() : _defaultCf.get();
        // we need to make sure this is on disk before starting to delete data in compactions
        auto s = _compactionScheduler->dropPrefixesAtomic(cf, prefixesToDrop, txn.get(), config);

        if (s.isOK()) {
            // remove from map
            stdx::lock_guard<Latch> lk(_identMapMutex);
            _identMap.erase(ident);
        }

        if (onDrop) {
            onDrop();
        }

        return s;
    }

    void RocksEngine::dropIdentForImport(OperationContext* opCtx, StringData ident) {
        // TODO
        MONGO_UNIMPLEMENTED;
    }

    bool RocksEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
        stdx::lock_guard<Latch> lk(_identMapMutex);
        return _identMap.find(ident) != _identMap.end();
    }

    std::vector<std::string> RocksEngine::getAllIdents(OperationContext* opCtx) const {
        std::vector<std::string> indents;
        stdx::lock_guard<Latch> lk(_identMapMutex);
        for (auto& entry : _identMap) {
            indents.push_back(entry.first);
        }
        return indents;
    }

    void RocksEngine::cleanShutdown() {
        if (_journalFlusher) {
            _journalFlusher->shutdown();
            _journalFlusher.reset();
        }
        _durabilityManager.reset();
        // TODO determine if this should just be removed or if we need to replace it with something
        // _snapshotManager.dropAllSnapshots();
        _counterManager->sync();
        _counterManager.reset();
        _compactionScheduler->stop();
        _compactionScheduler.reset();
        _defaultCf.reset();
        _oplogCf.reset();
        _db.reset();
    }

    void RocksEngine::setJournalListener(JournalListener* jl) {
        _durabilityManager->setJournalListener(jl);
    }

    void RocksEngine::setOldestTimestampFromStable() {
        Timestamp stableTimestamp(_stableTimestamp.load());

        // TODO(wolfkdy): impl failpoint RocksSetOldestTSToStableTS

        // Calculate what the oldest_timestamp should be from the stable_timestamp. The oldest
        // timestamp should lag behind stable by 'targetSnapshotHistoryWindowInSeconds' to create a
        // window of available snapshots. If the lag window is not yet large enough, we will not
        // update/forward the oldest_timestamp yet and instead return early.
        Timestamp newOldestTimestamp = _calculateHistoryLagFromStableTimestamp(stableTimestamp);
        if (newOldestTimestamp.isNull()) {
            return;
        }

        setOldestTimestamp(newOldestTimestamp, false);
    }

    Timestamp RocksEngine::getAllDurableTimestamp() const {
        return Timestamp(_oplogManager->fetchAllDurableValue());
    }

    boost::optional<Timestamp> RocksEngine::getOplogNeededForCrashRecovery() const {
        return boost::none;
    }

    size_t RocksEngine::getBlockCacheUsage() const { return _blockCache->GetUsage(); }

    Timestamp RocksEngine::getStableTimestamp() const { return Timestamp(_stableTimestamp.load()); }
    Timestamp RocksEngine::getOldestTimestamp() const { return Timestamp(_oldestTimestamp.load()); }
    Timestamp RocksEngine::getCheckpointTimestamp() const {
        return Timestamp(_lastStableCheckpointTimestamp.load());
    }

    void RocksEngine::setPinnedOplogTimestamp(const Timestamp& pinnedTimestamp) {
        // TODO
        MONGO_UNIMPLEMENTED;
    }

    void RocksEngine::dump() const {
        // TODO
        MONGO_UNIMPLEMENTED;
    }

    void RocksEngine::setMaxWriteMBPerSec(int maxWriteMBPerSec) {
        _maxWriteMBPerSec = maxWriteMBPerSec;
        _rateLimiter->SetBytesPerSecond(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024);
    }

    Status RocksEngine::backup(const std::string& path) {
        rocksdb::Checkpoint* checkpoint;
        auto s = rocksdb::Checkpoint::Create(_db.get(), &checkpoint);
        if (s.ok()) {
            s = checkpoint->CreateCheckpoint(path);
        }
        delete checkpoint;
        return rocksToMongoStatus(s);
    }

    Status RocksEngine::_createIdent(StringData ident, BSONObjBuilder* configBuilder) {
        BSONObj config;
        uint32_t prefix = 0;
        {
            stdx::lock_guard<Latch> lk(_identMapMutex);
            if (_identMap.find(ident) != _identMap.end()) {
                // already exists
                return Status::OK();
            }

            prefix = ++_maxPrefix;
            configBuilder->append("prefix", static_cast<int32_t>(prefix));

            config = configBuilder->obj();
            _identMap[ident] = config.copy();
        }

        BSONObjBuilder builder;

        auto txn = _db->makeTxn();
        rocksdb::Status s;
        ROCKS_ERR(txn->Put(kMetadataPrefix + ident.toString(),
                           rocksdb::Slice(config.objdata(), config.objsize())));
        ROCKS_ERR(txn->Commit());
        // As an optimization, add a key <prefix> to the DB
        std::string encodedPrefix(encodePrefix(prefix));
        auto txn1 = _db->makeTxn();
        ROCKS_ERR(txn1->Put(encodedPrefix, rocksdb::Slice()));
        ROCKS_ERR(txn1->Commit());
        return rocksToMongoStatus(s);
    }

    BSONObj RocksEngine::_getIdentConfig(StringData ident) {
        stdx::lock_guard<Latch> lk(_identMapMutex);
        auto identIter = _identMap.find(ident);
        invariant(identIter != _identMap.end());
        return identIter->second.copy();
    }

    BSONObj RocksEngine::_tryGetIdentConfig(StringData ident) {
        stdx::lock_guard<Latch> lk(_identMapMutex);
        auto identIter = _identMap.find(ident);
        const bool identFound = (identIter != _identMap.end());
        return identFound ? identIter->second.copy() : BSONObj();
    }

    std::string RocksEngine::_extractPrefix(const BSONObj& config) {
        return encodePrefix(config.getField("prefix").numberInt());
    }

    rocksdb::Options RocksEngine::_options(bool isOplog, bool trimHistory) const {
        // default options
        rocksdb::Options options;
        options.rate_limiter = _rateLimiter;
        rocksdb::BlockBasedTableOptions table_options;
        table_options.block_cache = _blockCache;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.block_size = 16 * 1024;  // 16KB
        table_options.format_version = 2;

        if (isOplog && trimHistory) {
            options.comparator = &comparatorFake;
            options.disable_auto_compactions = true;
        } else {
            options.comparator = &comparator;
        }
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        options.write_buffer_size = rocksGlobalOptions.writeBufferSize;
        options.max_write_buffer_number = rocksGlobalOptions.maxWriteBufferNumber;
        options.max_background_jobs = rocksGlobalOptions.maxBackgroundJobs;
        options.max_total_wal_size = rocksGlobalOptions.maxTotalWalSize;
        options.db_write_buffer_size = rocksGlobalOptions.dbWriteBufferSize;
        options.num_levels = rocksGlobalOptions.numLevels;

        options.delayed_write_rate = rocksGlobalOptions.delayedWriteRate;
        options.level0_file_num_compaction_trigger =
            rocksGlobalOptions.level0FileNumCompactionTrigger;
        options.level0_slowdown_writes_trigger = rocksGlobalOptions.level0SlowdownWritesTrigger;
        options.level0_stop_writes_trigger = rocksGlobalOptions.level0StopWritesTrigger;
        options.soft_pending_compaction_bytes_limit =
            static_cast<unsigned long long>(rocksGlobalOptions.softPendingCompactionMBLimit) *
            1024 * 1024;
        options.hard_pending_compaction_bytes_limit =
            static_cast<unsigned long long>(rocksGlobalOptions.hardPendingCompactionMBLimit) *
            1024 * 1024;
        options.target_file_size_base = 64 * 1024 * 1024;  // 64MB
        options.level_compaction_dynamic_level_bytes = true;
        options.max_bytes_for_level_base = rocksGlobalOptions.maxBytesForLevelBase;
        // This means there is no limit on open files. Make sure to always set ulimit so that it can
        // keep all RocksDB files opened.
        options.max_open_files = -1;
        options.optimize_filters_for_hits = true;
        options.compaction_filter_factory.reset(
            _compactionScheduler->createCompactionFilterFactory());
        options.enable_thread_tracking = true;
        // Enable concurrent memtable
        options.allow_concurrent_memtable_write = true;
        options.enable_write_thread_adaptive_yield = true;

        options.compression_per_level.resize(3);
        options.compression_per_level[0] = rocksdb::kNoCompression;
        options.compression_per_level[1] = rocksdb::kNoCompression;
        if (isOplog) {
            // NOTE(deyukong): with kNoCompression, storageSize precisely matches non-compressed
            // userdata size. In this way, oplog capping will be timely.
            options.compression_per_level[2] = rocksdb::kNoCompression;
        } else {
            if (rocksGlobalOptions.compression == "snappy") {
                options.compression_per_level[2] = rocksdb::kSnappyCompression;
            } else if (rocksGlobalOptions.compression == "zlib") {
                options.compression_per_level[2] = rocksdb::kZlibCompression;
            } else if (rocksGlobalOptions.compression == "none") {
                options.compression_per_level[2] = rocksdb::kNoCompression;
            } else if (rocksGlobalOptions.compression == "lz4") {
                options.compression_per_level[2] = rocksdb::kLZ4Compression;
            } else if (rocksGlobalOptions.compression == "lz4hc") {
                options.compression_per_level[2] = rocksdb::kLZ4HCCompression;
            } else {
                LOGV2(0, "Unknown compression, will use default (snappy)");
                options.compression_per_level[2] = rocksdb::kSnappyCompression;
            }
        }
        options.info_log = std::shared_ptr<rocksdb::Logger>(new MongoRocksLogger());
        options.statistics = _statistics;

        // create the DB if it's not already present
        options.create_if_missing = true;
        // options.wal_dir = _path + "/journal";

        // allow override
        if (!rocksGlobalOptions.configString.empty()) {
            rocksdb::Options base_options(options);
            auto s = rocksdb::GetOptionsFromString(base_options, rocksGlobalOptions.configString,
                                                   &options);
            if (!s.ok()) {
                LOGV2(0, "Invalid rocksdbConfigString",
                      "config"_attr = redact(rocksGlobalOptions.configString));
                invariantRocksOK(s);
            }
        }

        return options;
    }

    namespace {

        MONGO_FAIL_POINT_DEFINE(RocksPreserveSnapshotHistoryIndefinitely);
        MONGO_FAIL_POINT_DEFINE(RocksSetOldestTSToStableTS);

    }  // namespace

    Timestamp RocksEngine::getInitialDataTimestamp() const {
        return Timestamp(_initialDataTimestamp.load());
    }

    void RocksEngine::setStableTimestamp(Timestamp stableTimestamp, bool force) {
        if (!_keepDataHistory || stableTimestamp.isNull()) {
            return;
        }

        _stableTimestamp.store(stableTimestamp.asULL());
        invariantRocksOK(_db->SetTimeStamp(rocksdb::TimeStampType::kStable,
                                           rocksdb::RocksTimeStamp(_stableTimestamp.load()),
                                           force));

        const Timestamp initialDataTimestamp(_initialDataTimestamp.load());

        // cases:
        //
        // First, initialDataTimestamp is Timestamp(0, 1) -> (i.e: during initial sync).
        //
        // Second, enableMajorityReadConcern is false. In this case, we are not tracking a
        // stable timestamp.
        //
        // Third, stableTimestamp < initialDataTimestamp:
        //
        // Fourth, stableTimestamp >= initialDataTimestamp: Take stable checkpoint. Steady
        // state case.
        if (initialDataTimestamp.asULL() <= 1) {
            ;
        } else if (!_keepDataHistory) {
            // Ensure '_lastStableCheckpointTimestamp' is set such that oplog truncation may
            // take place entirely based on the oplog size.
            _lastStableCheckpointTimestamp.store(std::numeric_limits<uint64_t>::max());
        } else if (stableTimestamp < initialDataTimestamp) {
            LOGV2_DEBUG_OPTIONS(
                0, 2, logv2::LogOptions{logv2::LogComponent::kStorageRecovery},
                "Stable timestamp is behind the initial data timestamp, skipping a checkpoint",
                "stable_timestamp"_attr = stableTimestamp.toString(),
                "initial_data_timestamp"_attr = initialDataTimestamp.toString());
        } else {
            LOGV2_DEBUG_OPTIONS(0, 2, logv2::LogOptions{logv2::LogComponent::kStorageRecovery},
                                "Performing stable checkpoint",
                                "stable_timestamp"_attr = stableTimestamp);

            // Publish the checkpoint time after the checkpoint becomes durable.
            _lastStableCheckpointTimestamp.store(stableTimestamp.asULL());
        }

        setOldestTimestamp(stableTimestamp, force);
    }

    void RocksEngine::setInitialDataTimestamp(Timestamp initialDataTimestamp) {
        _initialDataTimestamp.store(initialDataTimestamp.asULL());
    }

    // TODO(wolfkdy): in 4.0.3, setOldestTimestamp considers oplogReadTimestamp
    // it disappears in mongo4.2, find why it happens
    void RocksEngine::setOldestTimestamp(Timestamp oldestTimestamp, bool force) {
        // Set the oldest timestamp to the stable timestamp to ensure that there is no lag window
        // between the two.
        if (RocksSetOldestTSToStableTS.shouldFail()) {
            force = false;
        }
        if (RocksPreserveSnapshotHistoryIndefinitely.shouldFail()) {
            return;
        }

        rocksdb::RocksTimeStamp ts(oldestTimestamp.asULL());

        if (force) {
            {
                rocksdb::ShouldNotCheckOldestTsBlock shouldNotCheckOldestTsblock(&comparator, ts);
                invariantRocksOK(_db->SetTimeStamp(rocksdb::TimeStampType::kOldest, ts, force));
            }
            invariantRocksOK(_db->SetTimeStamp(rocksdb::TimeStampType::kCommitted, ts, force));
            _oldestTimestamp.store(oldestTimestamp.asULL());
            LOGV2_DEBUG(0, 2, "oldest_timestamp and commit_timestamp force set",
                        "timestamp"_attr = oldestTimestamp);
        } else {
            invariantRocksOK(_db->SetTimeStamp(rocksdb::TimeStampType::kOldest, ts, force));
            if (_oldestTimestamp.load() < oldestTimestamp.asULL()) {
                _oldestTimestamp.store(oldestTimestamp.asULL());
            }
            LOGV2_DEBUG(0, 2, "oldest_timestamp set", "timestamp"_attr = oldestTimestamp);
        }
    }

    Timestamp RocksEngine::_calculateHistoryLagFromStableTimestamp(Timestamp stableTimestamp) {
        // The oldest_timestamp should lag behind the stable_timestamp by
        // 'targetSnapshotHistoryWindowInSeconds' seconds.

        if (stableTimestamp.getSecs() <
            static_cast<unsigned>(minSnapshotHistoryWindowInSeconds.load())) {
            // The history window is larger than the timestamp history thus far. We must wait for
            // the history to reach the window size before moving oldest_timestamp forward.
            return Timestamp();
        }

        Timestamp calculatedOldestTimestamp(
            stableTimestamp.getSecs() - minSnapshotHistoryWindowInSeconds.load(),
            stableTimestamp.getInc());

        if (calculatedOldestTimestamp.asULL() <= _oldestTimestamp.load()) {
            // The stable_timestamp is not far enough ahead of the oldest_timestamp for the
            // oldest_timestamp to be moved forward: the window is still too small.
            return Timestamp();
        }

        return calculatedOldestTimestamp;
    }

    bool RocksEngine::supportsRecoverToStableTimestamp() const { return _keepDataHistory; }

    bool RocksEngine::supportsRecoveryTimestamp() const { return true; }

    bool RocksEngine::canRecoverToStableTimestamp() const {
        static const std::uint64_t allowUnstableCheckpointsSentinel =
            static_cast<std::uint64_t>(Timestamp::kAllowUnstableCheckpointsSentinel.asULL());
        const std::uint64_t initialDataTimestamp = _initialDataTimestamp.load();
        // Illegal to be called when the dataset is incomplete.
        invariant(initialDataTimestamp > allowUnstableCheckpointsSentinel);
        return _stableTimestamp.load() >= initialDataTimestamp;
    }

    StatusWith<Timestamp> RocksEngine::recoverToStableTimestamp(OperationContext* opCtx) {
        if (!supportsRecoverToStableTimestamp()) {
            LOGV2_FATAL(0, "Rocksdb is configured to not support recover to a stable timestamp");
            fassertFailed(ErrorCodes::InternalError);
        }

        if (!canRecoverToStableTimestamp()) {
            Timestamp stableTS = getStableTimestamp();
            Timestamp initialDataTS = getInitialDataTimestamp();
            return Status(
                ErrorCodes::UnrecoverableRollbackError,
                str::stream()
                    << "No stable timestamp available to recover to. Initial data timestamp: "
                    << initialDataTS.toString() << ", Stable timestamp: " << stableTS.toString());
        }

        invariant(!_oplogManager->isRunning());
        LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
                            "RocksEngine::RecoverToStableTimestamp oplogManager is halt.");

        _oplogManager->init(nullptr /* rocksdb::TOTransactionDB* */,
                            nullptr /* RocksDurabilityManager* */);

        LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
                            "RocksEngine::RecoverToStableTimestamp syncing size storer to disk.");
        _counterManager->sync();
        _counterManager.reset();
        LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
                            "RocksEngine::RecoverToStableTimestamp shutting down counterManager");

        if (_journalFlusher) {
            _journalFlusher->shutdown();
            _journalFlusher.reset();
        }
        LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
                            "RocksEngine::RecoverToStableTimestamp shutting down journal");

        _durabilityManager.reset();
        LOGV2_DEBUG_OPTIONS(
            0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
            "RocksEngine::RecoverToStableTimestamp shutting down durabilityManager");

        _compactionScheduler->stop();
        LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
                            "RocksEngine::RecoverToStableTimestamp stop _compactionScheduler");

        // close db
        _defaultCf.reset();
        _oplogCf.reset();
        _db.reset();
        LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
                            "RocksEngine::RecoverToStableTimestamp shutting down rocksdb");

        // compactionScheduler should be create before initDatabase, because
        // options.compaction_filter_factory need compactionScheduler
        _compactionScheduler = std::make_shared<RocksCompactionScheduler>();
        LOGV2_DEBUG_OPTIONS(
            0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
            "RocksEngine::RecoverToStableTimestamp shutting down _compactionScheduler");

        _initDatabase();
        LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
                            "RocksEngine::RecoverToStableTimestamp open rocksdb");

        _compactionScheduler->start(_db.get(), _defaultCf.get());

        _durabilityManager.reset(
            new RocksDurabilityManager(_db.get(), _durable, _defaultCf.get(), _oplogCf.get()));

        _oplogManager->init(_db.get(), _durabilityManager.get());

        const auto stableTimestamp = getStableTimestamp();
        const auto initialDataTimestamp = getInitialDataTimestamp();

        LOGV2_DEBUG_OPTIONS(0, 0, logv2::LogOptions{logv2::LogComponent::kReplicationRollback},
                            "Rolling back to the stable timestamp.",
                            "stable_timestamp"_attr = stableTimestamp,
                            "initial_data_timestamp"_attr = initialDataTimestamp);

        setInitialDataTimestamp(initialDataTimestamp);
        setStableTimestamp(stableTimestamp, false);

        if (_durable) {
            _journalFlusher = std::make_unique<RocksJournalFlusher>(_durabilityManager.get());
            _journalFlusher->go();
        }
        _counterManager.reset(new RocksCounterManager(_db.get(), _defaultCf.get(),
                                                      rocksGlobalOptions.crashSafeCounters));
        opCtx->setRecoveryUnit(std::unique_ptr<RecoveryUnit>(newRecoveryUnit()),
                               WriteUnitOfWork::RecoveryUnitState::kNotInUnitOfWork);

        // oplogManager will be oppend by oplog record store is open, no need open here

        return {stableTimestamp};
    }

    boost::optional<Timestamp> RocksEngine::getRecoveryTimestamp() const {
        if (!supportsRecoveryTimestamp()) {
            LOGV2_FATAL(0, "RocksDB is configured to not support providing a recovery timestamp");
            fassertFailed(ErrorCodes::InternalError);
        }

        if (_recoveryTimestamp.isNull()) {
            return boost::none;
        }

        return _recoveryTimestamp;
    }

    /**
     * Returns a timestamp value that is at or before the last checkpoint. Everything before
     * this
     * value is guaranteed to be persisted on disk and replication recovery will not need to
     * replay documents with an earlier time.
     */
    boost::optional<Timestamp> RocksEngine::getLastStableRecoveryTimestamp() const {
        if (!supportsRecoverToStableTimestamp()) {
            LOGV2_FATAL(0, "Rocksdb is configured to not support recover to a stable timestamp");
            fassertFailed(ErrorCodes::InternalError);
        }

        const auto ret = _lastStableCheckpointTimestamp.load();
        if (ret) {
            return Timestamp(ret);
        }

        if (!_recoveryTimestamp.isNull()) {
            return _recoveryTimestamp;
        }

        return boost::none;
    }

    bool RocksEngine::supportsReadConcernSnapshot() const { return true; }

    bool RocksEngine::supportsReadConcernMajority() const { return _keepDataHistory; }

    void RocksEngine::startOplogManager(OperationContext* opCtx,
                                        RocksRecordStore* oplogRecordStore) {
        stdx::lock_guard<Latch> lock(_oplogManagerMutex);
        if (_oplogManagerCount == 0) _oplogManager->start(opCtx, oplogRecordStore);
        _oplogManagerCount++;
    }

    void RocksEngine::haltOplogManager() {
        stdx::unique_lock<Latch> lock(_oplogManagerMutex);
        invariant(_oplogManagerCount > 0);
        _oplogManagerCount--;
        if (_oplogManagerCount == 0) {
            _oplogManager->halt();
        }
    }
}  // namespace mongo
