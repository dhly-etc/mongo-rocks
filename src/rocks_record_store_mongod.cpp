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

#include <mutex>
#include <set>

#include "mongo/base/checked_cast.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session/session_txn_record_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/background.h"
#include "mongo/util/exit.h"
#include "rocks_engine.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {

    namespace {
        Timestamp getoldestPrepareTs(OperationContext* opCtx) {
            auto alterClient =
                opCtx->getServiceContext()->getService()->makeClient("get-oldest-prepared-txn");
            AlternativeClientRegion acr(alterClient);
            const auto tmpOpCtx = cc().makeOperationContext();
            tmpOpCtx->recoveryUnit()->setTimestampReadSource(
                RecoveryUnit::ReadSource::kNoTimestamp);
            DBDirectClient client(tmpOpCtx.get());

            FindCommandRequest find{NamespaceString::kSessionTransactionsTableNamespace};
            find.setFilter(BSON("txnState"
                                << "kPrepared"));
            find.setSort(BSON("lastWriteOpTime" << 1));

            auto c = client.find(find);
            if (c->more()) {
                auto raw = c->next();
                SessionTxnRecord record =
                    SessionTxnRecord::parse(IDLParserContext("init prepared txns"), raw);
                return record.getLastWriteOpTime().getTimestamp();
            }
            return Timestamp::max();
        }

        std::set<NamespaceString> _backgroundThreadNamespaces;
        Mutex _backgroundThreadMutex;

        class RocksRecordStoreThread : public BackgroundJob {
        public:
            RocksRecordStoreThread(const NamespaceString& ns)
                : BackgroundJob(true /* deleteSelf */), _ns(ns) {
                _name = std::string("RocksRecordStoreThread-for-") + _ns.toStringForResourceId();
            }

            virtual std::string name() const { return _name; }

            /**
             * @return if any oplog records are deleted.
             */
            bool _deleteExcessDocuments() {
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
                            LOGV2_DEBUG(0, 2, "no collection", logAttrs(_ns));
                            return false;
                        }
                        rs = checked_cast<RocksRecordStore*>(
                            oplog.getCollection()->getRecordStore());
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
                MONGO_UNREACHABLE
            }

            virtual void run() {
                ThreadClient tc(_name, getGlobalServiceContext()->getService());

                while (!globalInShutdownDeprecated()) {
                    bool removed = _deleteExcessDocuments();
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

        private:
            NamespaceString _ns;
            std::string _name;
        };

    }  // namespace

    // static
    bool RocksEngine::initRsOplogBackgroundThread(const NamespaceString& nss) {
        if (!nss.isOplog()) {
            return false;
        }

        // TODO readOnly?
        if (storageGlobalParams.repair) {
            LOGV2_DEBUG(0, 1,
                        "not starting RocksRecordStoreThread for because we are either in repair "
                        "or read-only mode",
                        "nss"_attr = nss);
            return false;
        }

        stdx::lock_guard<Latch> lock(_backgroundThreadMutex);
        if (_backgroundThreadNamespaces.count(nss)) {
            LOGV2(0, "RocksRecordStoreThread already started", logAttrs(nss));
        } else {
            LOGV2(0, "Starting RocksRecordStoreThread", logAttrs(nss));
            BackgroundJob* backgroundThread = new RocksRecordStoreThread(nss);
            backgroundThread->go();
            _backgroundThreadNamespaces.insert(nss);
        }
        return true;
    }

}  // namespace mongo
