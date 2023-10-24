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

#include "rocks_index.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include <boost/filesystem/operations.hpp>
#include <string>

#include "mongo/base/init.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/storage/sorted_data_interface_test_harness.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "rocks_engine.h"
#include "rocks_recovery_unit.h"
#include "rocks_snapshot_manager.h"

namespace mongo {
    namespace {

        using std::string;

        class RocksIndexHarness final : public SortedDataInterfaceHarnessHelper {
        public:
            RocksIndexHarness()
                : _order(Ordering::make(BSONObj())),
                  _dbpath("rocks_test"),
                  _engine(_dbpath.path(), true /* durable */, 3 /* kRocksFormatVersion */,
                          false /* readOnly */) {}

            virtual ~RocksIndexHarness() {}

            std::unique_ptr<SortedDataInterface> newSortedDataInterface(bool unique, bool partial,
                                                                        KeyFormat keyFormat) {
                BSONObjBuilder configBuilder;
                RocksIndexBase::generateConfig(&configBuilder, 3,
                                               IndexDescriptor::IndexVersion::kV2);
                if (unique) {
                    return std::make_unique<RocksUniqueIndex>(
                        _engine.getDB(), _engine.getDefaultCf_ForTest(), "prefix", UUID::gen(),
                        "ident", _order, configBuilder.obj(),
                        NamespaceString::createNamespaceString_forTest("test.rocks"), "testIndex",
                        BSONObj(), partial);
                } else {
                    return std::make_unique<RocksStandardIndex>(
                        _engine.getDB(), _engine.getDefaultCf_ForTest(), "prefix", UUID::gen(),
                        "ident", _order, configBuilder.obj());
                }
            }

            std::unique_ptr<SortedDataInterface> newIdIndexSortedDataInterface() {
                BSONObjBuilder configBuilder;
                RocksIndexBase::generateConfig(&configBuilder, 3,
                                               IndexDescriptor::IndexVersion::kV2);

                return std::make_unique<RocksUniqueIndex>(
                    _engine.getDB(), _engine.getDefaultCf_ForTest(), "prefix", UUID::gen(), "ident",
                    _order, configBuilder.obj(),
                    NamespaceString::createNamespaceString_forTest("test.rocks"), "_id_",
                    BSON("_id" << 1), false, true);
            }

            std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
                return std::make_unique<RocksRecoveryUnit>(true /* durale */, &_engine);
            }

        private:
            Ordering _order;
            unittest::TempDir _dbpath;
            RocksEngine _engine;
        };

        MONGO_INITIALIZER(RegisterSortedDataInterfaceHarnessFactory)(InitializerContext* const) {
            registerSortedDataInterfaceHarnessHelperFactory(
                [] { return std::make_unique<RocksIndexHarness>(); });
        }

    }  // namespace
}  // namespace mongo
