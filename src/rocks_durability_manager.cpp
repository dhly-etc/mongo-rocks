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

#include "rocks_durability_manager.h"

#include <rocksdb/db.h>

#include "mongo/db/storage/journal_listener.h"
#include "rocks_util.h"

namespace mongo {
    RocksDurabilityManager::RocksDurabilityManager(rocksdb::DB* db, bool durable,
                                                   rocksdb::ColumnFamilyHandle* defaultCf,
                                                   rocksdb::ColumnFamilyHandle* oplogCf)
        : _db(db),
          _durable(durable),
          _defaultCf(defaultCf),
          _oplogCf(oplogCf),
          _journalListener(nullptr) {}

    void RocksDurabilityManager::setJournalListener(JournalListener* jl) {
        stdx::unique_lock<Latch> lk(_journalListenerMutex);
        _journalListener = jl;
    }

    // TODO(cuixin): rtt should modify waitUntilDurable
    void RocksDurabilityManager::waitUntilDurable(OperationContext* opCtx, bool forceFlush) {
        uint32_t start = _lastSyncTime.load();
        // Do the remainder in a critical section that ensures only a single thread at a time
        // will attempt to synchronize.
        stdx::unique_lock<Latch> lk(_lastSyncMutex);
        uint32_t current = _lastSyncTime.loadRelaxed();  // synchronized with writes through mutex
        if (current != start) {
            // Someone else synced already since we read lastSyncTime, so we're done!
            return;
        }
        _lastSyncTime.store(current + 1);

        stdx::unique_lock<Latch> jlk(_journalListenerMutex);
        if (_journalListener) {
            JournalListener::Token token = _journalListener->getToken(opCtx);
            if (!_durable || forceFlush) {
                invariantRocksOK(_db->Flush(rocksdb::FlushOptions(), {_defaultCf, _oplogCf}));
            } else {
                invariantRocksOK(_db->SyncWAL());
            }
            _journalListener->onDurable(token);
        }
    }

    void RocksDurabilityManager::waitUntilPreparedUnitOfWorkCommitsOrAborts(
        OperationContext* opCtx, std::uint64_t lastCount) {
        invariant(opCtx);
        stdx::unique_lock<Latch> lk(_prepareCommittedOrAbortedMutex);
        if (lastCount == _prepareCommitOrAbortCounter.loadRelaxed()) {
            opCtx->waitForConditionOrInterrupt(_prepareCommittedOrAbortedCond, lk, [&] {
                return _prepareCommitOrAbortCounter.loadRelaxed() > lastCount;
            });
        }
    }

    void RocksDurabilityManager::notifyPreparedUnitOfWorkHasCommittedOrAborted() {
        stdx::unique_lock<Latch> lk(_prepareCommittedOrAbortedMutex);
        _prepareCommitOrAbortCounter.fetchAndAdd(1);
        _prepareCommittedOrAbortedCond.notify_all();
    }
}  // namespace mongo
