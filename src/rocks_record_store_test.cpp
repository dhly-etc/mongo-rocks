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

#include "rocks_record_store.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include <boost/filesystem/operations.hpp>
#include <memory>
#include <vector>

#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/json.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_db.h"
#include "mongo/db/repl/repl_settings.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/kv/kv_engine_test_harness.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/scopeguard.h"
#include "rocks_compaction_scheduler.h"
#include "rocks_oplog_manager.h"
#include "rocks_recovery_unit.h"
#include "rocks_snapshot_manager.h"

namespace mongo {

    class RocksHarnessHelper final : public RecordStoreHarnessHelper {
    public:
        RocksHarnessHelper(RecordStoreHarnessHelper::Options options)
            : _dbpath("rocks_test"),
              _engine(_dbpath.path(), true /* durable */, 3 /* kRocksFormatVersion */,
                      false /* readOnly */) {
            repl::ReplicationCoordinator::set(serviceContext(),
                                              std::make_unique<repl::ReplicationCoordinatorMock>(
                                                  serviceContext(), repl::ReplSettings()));
        }

        virtual ~RocksHarnessHelper() {}

        std::unique_ptr<RecordStore> newRecordStore() override {
            return newRecordStore("a.b", CollectionOptions{});
        }

        std::unique_ptr<RecordStore> newRecordStore(
            const std::string& ns, const CollectionOptions& options,
            KeyFormat keyFormat = KeyFormat::Long) override {
            auto opCtx = newOperationContext();

            RocksRecordStore::Params params;
            params.nss = NamespaceString::createNamespaceString_forTest(ns);
            params.ident = "1";
            params.prefix = "prefix";
            params.isCapped = options.capped;
            params.cappedMaxSize = options.cappedSize;
            params.cappedMaxDocs = options.cappedMaxDocs;

            return std::make_unique<RocksRecordStore>(&_engine, _engine.getCf_ForTest(ns),
                                                      opCtx.get(), params);
        }

        std::unique_ptr<RecordStore> newOplogRecordStore() override {
            auto opCtx = newOperationContext();

            RocksRecordStore::Params params;
            params.nss = NamespaceString::kRsOplogNamespace;
            params.ident = "1";
            params.prefix = "prefix";
            params.isCapped = true;
            params.cappedMaxSize = 1024 * 1024 * 1024;

            auto rs = std::make_unique<RocksRecordStore>(
                &_engine, _engine.getCf_ForTest(params.nss.toStringForResourceId()), opCtx.get(),
                params);
            _engine.startOplogManager(opCtx.get(), rs.get());
            return rs;
        }

        KVEngine* getEngine() override { return &_engine; }

        std::unique_ptr<RecoveryUnit> newRecoveryUnit() {
            return std::unique_ptr<RecoveryUnit>(_engine.newRecoveryUnit());
        }

    private:
        unittest::TempDir _dbpath;
        ClockSourceMock _cs;

        RocksEngine _engine;
    };

    MONGO_INITIALIZER(RegisterRecordStoreHarnessFactory)(InitializerContext* const) {
        mongo::registerRecordStoreHarnessHelperFactory(
            [](RecordStoreHarnessHelper::Options options) {
                return std::make_unique<RocksHarnessHelper>(options);
            });
    }

    TEST(RocksRecordStoreTest, CounterManager1) {
        auto harnessHelper = newRecordStoreHarnessHelper();
        std::unique_ptr<RecordStore> rs(harnessHelper->newRecordStore());

        int N = 12;

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
            {
                WriteUnitOfWork uow(opCtx.get());
                for (int i = 0; i < N; i++) {
                    StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, Timestamp());
                    ASSERT_OK(res.getStatus());
                }
                uow.commit();
            }
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
            ASSERT_EQUALS(N, rs->numRecords(opCtx.get()));
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
            rs = harnessHelper->newRecordStore();
            ASSERT_EQUALS(N, rs->numRecords(opCtx.get()));
        }
        rs.reset(nullptr);  // this has to be deleted before ss
    }

}  // namespace mongo
