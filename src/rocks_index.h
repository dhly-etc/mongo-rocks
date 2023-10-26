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

#include <rocksdb/db.h>

#include <atomic>
#include <string>

#include "mongo/bson/ordering.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/sorted_data_interface.h"

#pragma once

namespace rocksdb {
    class DB;
    class ColumnFamilyHandle;
}  // namespace rocksdb

namespace mongo {

    class RocksRecoveryUnit;

    class RocksIndexBase : public SortedDataInterface {
        RocksIndexBase(const RocksIndexBase&) = delete;
        RocksIndexBase& operator=(const RocksIndexBase&) = delete;

    public:
        RocksIndexBase(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, std::string prefix,
                       const UUID& uuid, std::string ident, Ordering order, KeyFormat keyFormat,
                       const BSONObj& config, NamespaceString ns, std::string indexName,
                       const BSONObj& keyPattern);

        boost::optional<RecordId> findLoc(OperationContext* opCtx,
                                          const key_string::Value& keyString) const override;

        IndexValidateResults validate(OperationContext* opCtx, bool full) const override;

        bool appendCustomStats(OperationContext* /* opCtx */, BSONObjBuilder* /* output */,
                               double /* scale */) const override {
            // nothing to say here, really
            return false;
        }

        bool isEmpty(OperationContext* opCtx) override;

        long long getSpaceUsedBytes(OperationContext* opCtx) const override;

        long long getFreeStorageBytes(OperationContext* opCtx) const override;

        void printIndexEntryMetadata(OperationContext* opCtx,
                                     const key_string::Value& keyString) const override;

        int64_t numEntries(OperationContext* opCtx) const override;

        Status initAsEmpty(OperationContext* opCtx) override;

        static void generateConfig(BSONObjBuilder* configBuilder, int formatVersion,
                                   IndexDescriptor::IndexVersion descVersion);

    protected:
        static std::string _makePrefixedKey(const std::string& prefix,
                                            const key_string::Value& encodedKey);

        rocksdb::DB* _db;  // not owned

        rocksdb::ColumnFamilyHandle* _cf;  // not owned

        // Each key in the index is prefixed with _prefix
        std::string _prefix;
        std::string _ident;

        // very approximate index storage size
        std::atomic<long long> _indexStorageSize;

        NamespaceString _ns;
        std::string _indexName;
        const BSONObj _keyPattern;

        class StandardBulkBuilder;
        class UniqueBulkBuilder;
        friend class UniqueBulkBuilder;
    };

    class RocksUniqueIndex : public RocksIndexBase {
    public:
        RocksUniqueIndex(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, std::string prefix,
                         const UUID& uuid, std::string ident, Ordering order, KeyFormat keyFormat,
                         const BSONObj& config, NamespaceString ns, std::string indexName,
                         const BSONObj& keyPattern, bool partial = false, bool isIdIdx = false);

        Status insert(OperationContext* opCtx, const key_string::Value& keyString, bool dupsAllowed,
                      IncludeDuplicateRecordId includeDuplicateRecordId =
                          IncludeDuplicateRecordId::kOff) override;

        void unindex(OperationContext* opCtx, const key_string::Value& keyString,
                     bool dupsAllowed) override;

        std::unique_ptr<Cursor> newCursor(OperationContext* opCtx,
                                          bool isForward = true) const override;

        Status dupKeyCheck(OperationContext* opCtx, const key_string::Value& keyString) override;

        std::unique_ptr<SortedDataBuilderInterface> makeBulkBuilder(OperationContext* opCtx,
                                                                    bool dupsAllowed) override;

        void insertWithRecordIdInValue_forTest(OperationContext* opCtx,
                                               const key_string::Value& keyString,
                                               RecordId rid) override;

    private:
        Status _insert(OperationContext* opCtx, const BSONObj& key, const RecordId& loc,
                       bool dupsAllowed);

        Status _insertIntoIdIndex(OperationContext* opCtx, const BSONObj& key, const RecordId& loc,
                                  bool dupsAllowed);

        void _unindexFromIdIndex(OperationContext* opCtx, const BSONObj& key, const RecordId& loc,
                                 bool dupsAllowed);

        void _unindex(OperationContext* opCtx, const BSONObj& key, const RecordId& loc,
                      bool dupsAllowed);

        bool _keyExistsTimestampSafe(OperationContext* opCtx, const key_string::Value& prefixedKey);

        const bool _partial;
        const bool _isIdIndex;
    };

    class RocksStandardIndex : public RocksIndexBase {
    public:
        RocksStandardIndex(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, std::string prefix,
                           const UUID& uuid, std::string ident, Ordering order, KeyFormat keyFormat,
                           const BSONObj& config, NamespaceString ns, std::string indexName,
                           const BSONObj& keyPattern);

        Status insert(OperationContext* opCtx, const key_string::Value& keyString, bool dupsAllowed,
                      IncludeDuplicateRecordId includeDuplicateRecordId =
                          IncludeDuplicateRecordId::kOff) override;

        void unindex(OperationContext* opCtx, const key_string::Value& keyString,
                     bool dupsAllowed) override;

        std::unique_ptr<Cursor> newCursor(OperationContext* opCtx,
                                          bool isForward = true) const override;

        Status dupKeyCheck(OperationContext* opCtx, const key_string::Value& keyString) override {
            // dupKeyCheck shouldn't be called for non-unique indexes
            invariant(false);
            return Status::OK();
        }

        std::unique_ptr<SortedDataBuilderInterface> makeBulkBuilder(OperationContext* opCtx,
                                                                    bool dupsAllowed) override;

        void insertWithRecordIdInValue_forTest(OperationContext* opCtx,
                                               const key_string::Value& keyString,
                                               RecordId rid) override;

        void enableSingleDelete() { useSingleDelete = true; }

    private:
        bool useSingleDelete;
    };

}  // namespace mongo
