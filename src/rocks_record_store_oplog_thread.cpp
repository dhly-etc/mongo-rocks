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

#include "rocks_record_store_oplog_thread.h"

#include "mongo/db/catalog_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/util/exit.h"
#include "rocks_record_store.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {
    namespace {

        /**
         * Returns whether any oplog records were deleted.
         */
        bool deleteExcessDocuments() {
            if (!getGlobalServiceContext()->getStorageEngine()) {
                LOGV2_DEBUG(0, 1, "no global storage engine yet");
                return false;
            }
            const auto opCtx = cc().makeOperationContext();

            try {
                // A Global IX lock should be good enough to protect the oplog truncation from
                // interruptions such as restartCatalog. Database lock or collection lock is not
                // needed. This improves concurrency if oplog truncation takes long time.
                Lock::GlobalLock lk(opCtx.get(), MODE_IX);

                RocksRecordStore* rs = nullptr;
                {
                    // Release the database lock right away because we don't want to
                    // block other operations on the local database and given the
                    // fact that oplog collection is so special, Global IX lock can
                    // make sure the collection exists.
                    AutoGetOplog oplog{opCtx.get(), OplogAccessMode::kWrite};
                    if (!oplog.getCollection()) {
                        LOGV2_DEBUG(0, 2, "no oplog");
                        return false;
                    }
                    rs = checked_cast<RocksRecordStore*>(oplog.getCollection()->getRecordStore());
                }
                rs->reclaimOplog(opCtx.get());
                return true;
            } catch (const ExceptionForCat<ErrorCategory::Interruption>&) {
                return false;
            } catch (const std::exception& e) {
                LOGV2_FATAL_NOTRACE(0, "error in RocksRecordStoreThread",
                                    "error"_attr = redact(e.what()));
            } catch (...) {
                LOGV2_FATAL_NOTRACE(0, "unknown error in RocksRecordStoreThread");
            }
            MONGO_UNREACHABLE;
        }

    }  // namespace

    std::string RocksRecordStoreOplogThread::name() const {
        return std::string("RocksRecordStoreThread-for-") + NamespaceString::kRsOplogNamespace.ns();
    }

    void RocksRecordStoreOplogThread::run() {
        ThreadClient tc(name(), getGlobalServiceContext()->getService());

        while (!globalInShutdownDeprecated()) {
            bool removed = deleteExcessDocuments();
            LOGV2_DEBUG(0, 2, "RocksRecordStoreThread deleted", "removed"_attr = removed);
            if (!removed) {
                // If we removed 0 documents, sleep a bit in case we're on a laptop
                // or something to be nice.
                sleepmillis(1000);
            } else {
                // wake up every 100ms
                sleepmillis(100);
            }
        }

        LOGV2(0, "shutting down");
    }

}  // namespace mongo
