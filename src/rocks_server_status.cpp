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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <sstream>

#include "mongo/platform/basic.h"

#include "rocks_server_status.h"

#include <rocksdb/db.h>
#include <rocksdb/statistics.h>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

#include "rocks_engine.h"
#include "rocks_recovery_unit.h"

namespace mongo {
    using std::string;

    namespace {
        std::string PrettyPrintBytes(size_t bytes) {
            if (bytes < (16 << 10)) {
                return std::to_string(bytes) + "B";
            } else if (bytes < (16 << 20)) {
                return std::to_string(bytes >> 10) + "KB";
            } else if (bytes < (16LL << 30)) {
                return std::to_string(bytes >> 20) + "MB";
            } else {
                return std::to_string(bytes >> 30) + "GB";
            }
        }
    }  // namespace

    RocksServerStatusSection::RocksServerStatusSection(RocksEngine* engine)
        : ServerStatusSection("rocksdb"), _engine(engine) {}

    bool RocksServerStatusSection::includeByDefault() const { return true; }

    BSONObj RocksServerStatusSection::generateSection(OperationContext* opCtx,
                                                      const BSONElement& configElement) const {
        Lock::GlobalLock lk(opCtx, LockMode::MODE_IS);

        BSONObjBuilder bob;

        generatePropertiesSection(&bob);
        generateThreadStatusSection(&bob);
        generateCountersSection(&bob);
        generateTxnStatsSection(&bob);
        generateOplogDelStatsSection(&bob);
        generateCompactSchedulerSection(&bob);
        generateDefaultCFEntriesNumSection(&bob);

        RocksEngine::appendGlobalStats(bob);

        return bob.obj();
    }

    void RocksServerStatusSection::generateDefaultCFEntriesNumSection(BSONObjBuilder* bob) const {
        auto defaultCFNumEntries = _engine->getDefaultCFNumEntries();

        BSONObjBuilder objBuilder;
        for (auto& numVec : defaultCFNumEntries) {
            BSONObjBuilder ob;
            ob.append("num-entries", static_cast<long long>(numVec.second[0]));
            ob.append("num-deletions", static_cast<long long>(numVec.second[1]));
            objBuilder.append("L" + std::to_string(numVec.first), ob.obj());
        }
        bob->append("file-num-entries", objBuilder.obj());
    }

    void RocksServerStatusSection::generatePropertiesSection(BSONObjBuilder* bob) const {
        // if the second is true, that means that we pass the value through PrettyPrintBytes
        std::vector<std::pair<std::string, bool>> properties = {
            {"stats", false},
            {"num-immutable-mem-table", false},
            {"mem-table-flush-pending", false},
            {"compaction-pending", false},
            {"background-errors", false},
            {"cur-size-active-mem-table", true},
            {"cur-size-all-mem-tables", true},
            {"num-entries-active-mem-table", false},
            {"num-entries-imm-mem-tables", false},
            {"estimate-table-readers-mem", true},
            {"num-snapshots", false},
            {"oldest-snapshot-time", false},
            {"num-live-versions", false}};
        auto getProperties = [&](const std::pair<std::string, bool>& property,
                                 const std::string& prefix) {
            std::string statsString;
            if (prefix == "oplogcf-") {
                if (!_engine->getDB()->GetProperty(_engine->getOplogCFHandle(),
                                                   "rocksdb." + property.first, &statsString)) {
                    statsString = "<error> unable to retrieve statistics by oplogCF";
                    bob->append(property.first, statsString);
                    return;
                }
            } else {
                if (!_engine->getDB()->GetProperty("rocksdb." + property.first, &statsString)) {
                    statsString = "<error> unable to retrieve statistics";
                    bob->append(property.first, statsString);
                    return;
                }
            }
            if (property.first == "stats") {
                // special casing because we want to turn it into array
                BSONArrayBuilder a;
                std::stringstream ss(statsString);
                std::string line;
                while (std::getline(ss, line)) {
                    a.append(line);
                }
                bob->appendArray(prefix + property.first, a.arr());
            } else if (property.second) {
                bob->append(prefix + property.first, PrettyPrintBytes(std::stoll(statsString)));
            } else {
                bob->append(prefix + property.first, statsString);
            }
        };

        for (auto const& property : properties) {
            getProperties(property, "");
            getProperties(property, "oplogcf-");
        }

        bob->append("total-live-recovery-units", RocksRecoveryUnit::getTotalLiveRecoveryUnits());
        bob->append("block-cache-usage", PrettyPrintBytes(_engine->getBlockCacheUsage()));
    }

    void RocksServerStatusSection::generateThreadStatusSection(BSONObjBuilder* bob) const {
        std::vector<rocksdb::ThreadStatus> threadList;
        auto s = rocksdb::Env::Default()->GetThreadList(&threadList);
        if (s.ok()) {
            BSONArrayBuilder threadStatus;
            for (auto& ts : threadList) {
                if (ts.operation_type == rocksdb::ThreadStatus::OP_UNKNOWN ||
                    ts.thread_type == rocksdb::ThreadStatus::USER) {
                    continue;
                }
                BSONObjBuilder threadObjBuilder;
                threadObjBuilder.append("type",
                                        rocksdb::ThreadStatus::GetOperationName(ts.operation_type));
                threadObjBuilder.append(
                    "time_elapsed", rocksdb::ThreadStatus::MicrosToString(ts.op_elapsed_micros));
                auto op_properties = rocksdb::ThreadStatus::InterpretOperationProperties(
                    ts.operation_type, ts.op_properties);

                const std::vector<std::pair<std::string, std::string>> properties(
                    {{"JobID", "job_id"},
                     {"BaseInputLevel", "input_level"},
                     {"OutputLevel", "output_level"},
                     {"IsManual", "manual"}});
                for (const auto& prop : properties) {
                    auto itr = op_properties.find(prop.first);
                    if (itr != op_properties.end()) {
                        threadObjBuilder.append(prop.second, static_cast<int>(itr->second));
                    }
                }

                const std::vector<std::pair<std::string, std::string>> byte_properties(
                    {{"BytesRead", "bytes_read"},
                     {"BytesWritten", "bytes_written"},
                     {"TotalInputBytes", "total_bytes"}});
                for (const auto& prop : byte_properties) {
                    auto itr = op_properties.find(prop.first);
                    if (itr != op_properties.end()) {
                        threadObjBuilder.append(prop.second,
                                                PrettyPrintBytes(static_cast<size_t>(itr->second)));
                    }
                }

                const std::vector<std::pair<std::string, std::string>> speed_properties(
                    {{"BytesRead", "read_throughput"}, {"BytesWritten", "write_throughput"}});
                for (const auto& prop : speed_properties) {
                    auto itr = op_properties.find(prop.first);
                    if (itr != op_properties.end()) {
                        size_t speed = (itr->second * 1000 * 1000) /
                                       static_cast<size_t>(
                                           (ts.op_elapsed_micros == 0) ? 1 : ts.op_elapsed_micros);
                        threadObjBuilder.append(
                            prop.second, PrettyPrintBytes(static_cast<size_t>(speed)) + "/s");
                    }
                }

                threadStatus.append(threadObjBuilder.obj());
            }
            bob->appendArray("thread-status", threadStatus.arr());
        }
    }

    void RocksServerStatusSection::generateCountersSection(BSONObjBuilder* bob) const {
        // add counters
        auto stats = _engine->getStatistics();
        if (stats) {
            BSONObjBuilder countersObjBuilder;
            const std::vector<std::pair<rocksdb::Tickers, std::string>> counterNameMap = {
                {rocksdb::NUMBER_KEYS_WRITTEN, "num-keys-written"},
                {rocksdb::NUMBER_KEYS_READ, "num-keys-read"},
                {rocksdb::NUMBER_DB_SEEK, "num-seeks"},
                {rocksdb::NUMBER_DB_NEXT, "num-forward-iterations"},
                {rocksdb::NUMBER_DB_PREV, "num-backward-iterations"},
                {rocksdb::BLOCK_CACHE_MISS, "block-cache-misses"},
                {rocksdb::BLOCK_CACHE_HIT, "block-cache-hits"},
                {rocksdb::BLOOM_FILTER_USEFUL, "bloom-filter-useful"},
                {rocksdb::BYTES_WRITTEN, "bytes-written"},
                {rocksdb::BYTES_READ, "bytes-read-point-lookup"},
                {rocksdb::ITER_BYTES_READ, "bytes-read-iteration"},
                {rocksdb::FLUSH_WRITE_BYTES, "flush-bytes-written"},
                {rocksdb::COMPACT_READ_BYTES, "compaction-bytes-read"},
                {rocksdb::COMPACT_WRITE_BYTES, "compaction-bytes-written"}};

            for (const auto& counter_name : counterNameMap) {
                countersObjBuilder.append(
                    counter_name.second,
                    static_cast<long long>(stats->getTickerCount(counter_name.first)));
            }

            bob->append("counters", countersObjBuilder.obj());
        }
    }

    void RocksServerStatusSection::generateTxnStatsSection(BSONObjBuilder* bob) const {
        //  transaction stats
        rocksdb::TOTransactionStat txnStat;
        memset(&txnStat, 0, sizeof txnStat);
        rocksdb::TOTransactionDB* db = _engine->getDB();
        invariant(db->Stat(&txnStat).ok());
        BSONObjBuilder txnObjBuilder;
        txnObjBuilder.append("max-conflict-bytes",
                             static_cast<long long>(txnStat.max_conflict_bytes));
        txnObjBuilder.append("cur-conflict-bytes",
                             static_cast<long long>(txnStat.cur_conflict_bytes));
        txnObjBuilder.append("uncommitted-keys", static_cast<long long>(txnStat.uk_num));
        txnObjBuilder.append("committed-keys", static_cast<long long>(txnStat.ck_num));
        txnObjBuilder.append("alive-txn-num", static_cast<long long>(txnStat.alive_txns_num));
        txnObjBuilder.append("read-queue-num", static_cast<long long>(txnStat.read_q_num));
        txnObjBuilder.append("commit-queue-num", static_cast<long long>(txnStat.commit_q_num));
        txnObjBuilder.append("oldest-timestamp", static_cast<long long>(txnStat.oldest_ts));
        txnObjBuilder.append("min-read-timestamp", static_cast<long long>(txnStat.min_read_ts));
        txnObjBuilder.append("max-commit-timestamp", static_cast<long long>(txnStat.max_commit_ts));
        txnObjBuilder.append("committed-max-txnid",
                             static_cast<long long>(txnStat.committed_max_txnid));
        txnObjBuilder.append("min-uncommit-ts", static_cast<long long>(txnStat.min_uncommit_ts));
        txnObjBuilder.append("update-max-commit-ts-times",
                             static_cast<long long>(txnStat.update_max_commit_ts_times));
        txnObjBuilder.append("update-max-commit-ts-retries",
                             static_cast<long long>(txnStat.update_max_commit_ts_retries));
        txnObjBuilder.append("txn-commits", static_cast<long long>(txnStat.txn_commits));
        txnObjBuilder.append("txn-aborts", static_cast<long long>(txnStat.txn_aborts));
        txnObjBuilder.append("commit-without-ts-times",
                             static_cast<long long>(txnStat.commit_without_ts_times));
        txnObjBuilder.append("read-without-ts-times",
                             static_cast<long long>(txnStat.read_without_ts_times));
        txnObjBuilder.append("read-with-ts-times",
                             static_cast<long long>(txnStat.read_with_ts_times));
        txnObjBuilder.append("read-queue-walk-len-sum",
                             static_cast<long long>(txnStat.read_q_walk_len_sum));
        txnObjBuilder.append("read-queue-walk-times",
                             static_cast<long long>(txnStat.read_q_walk_times));
        txnObjBuilder.append("commit-queue-walk-len-sum",
                             static_cast<long long>(txnStat.commit_q_walk_len_sum));
        txnObjBuilder.append("commit-queue-walk-times",
                             static_cast<long long>(txnStat.commit_q_walk_times));
        bob->append("transaction-stats", txnObjBuilder.obj());
    }

    void RocksServerStatusSection::generateOplogDelStatsSection(BSONObjBuilder* bob) const {
        // oplog compact delete stats
        BSONObjBuilder oplogDelBuilder;
        auto oplogStats = _engine->getCompactionScheduler()->getOplogDelCompactStats();
        oplogDelBuilder.append("oplog-deleted-entries",
                               static_cast<long long>(oplogStats.oplogEntriesDeleted));
        oplogDelBuilder.append("oplog-deleted-size",
                               static_cast<long long>(oplogStats.oplogSizeDeleted));
        oplogDelBuilder.append("oplog-compact-skip-entries",
                               static_cast<long long>(oplogStats.oplogCompactSkip));
        oplogDelBuilder.append("opLog-compact-keep-entries",
                               static_cast<long long>(oplogStats.oplogCompactKeep));
        bob->append("oplog-compact-stats", oplogDelBuilder.obj());
    }
    void RocksServerStatusSection::generateCompactSchedulerSection(BSONObjBuilder* bob) const {
        // compaction scheduler stats
        // TODO(wolfkdy): use jstests and failPoints to test primary/secondary status after dropIndex
        // and dropCollection, test prefixes draining before and after mongod reboot
        BSONObjBuilder bb;
        bool large = false;
        auto droppedPrefixes = _engine->getCompactionScheduler()->getDroppedPrefixes();
        {
            BSONArrayBuilder a;
            for (auto p : droppedPrefixes) {
                a.append(
                    BSON("prefix" << static_cast<long long>(p.first) << "debug-info" << p.second));
                if (a.len() > 1024 * 1024 * 15) {
                    large = true;
                    break;
                }
            }
            bb.appendArray("dropped-prefixes", a.arr());
        }
        auto obj = bb.obj();
        if (large) {
            log() << "status is over 15MB";
        }
        bob->append("compaction-scheduler", obj);
    }
}  // namespace mongo
