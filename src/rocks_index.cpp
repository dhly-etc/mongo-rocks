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

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/str.h"
#include "rocks_engine.h"
#include "rocks_prepare_conflict.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {

    using std::string;
    using std::stringstream;
    using std::vector;
    namespace {
        static const int kKeyStringV0Version = 0;
        static const int kKeyStringV1Version = 1;
        static const int kMinimumIndexVersion = kKeyStringV0Version;
        static const int kMaximumIndexVersion = kKeyStringV1Version;
        static const std::string emptyItem("");

        bool hasFieldNames(const BSONObj& obj) {
            BSONForEach(e, obj) {
                if (e.fieldName()[0]) return true;
            }
            return false;
        }

        /**
         * Strips the field names from a BSON object
         */
        BSONObj stripFieldNames(const BSONObj& obj) {
            if (!hasFieldNames(obj)) return obj;

            BSONObjBuilder b;
            BSONForEach(e, obj) { b.appendAs(e, StringData()); }
            return b.obj();
        }

        string dupKeyError(const BSONObj& key, const std::string& collectionNamespace,
                           const std::string& indexName) {
            stringstream ss;
            ss << "E11000 duplicate key error";
            ss << " collection: " << collectionNamespace;
            ss << " index: " << indexName;
            ss << " dup key: " << key.toString();
            return ss.str();
        }

        RecordId decodeRecordIdAtEnd(const char* buffer, size_t size, KeyFormat keyFormat) {
            switch (keyFormat) {
                case KeyFormat::Long:
                    return key_string::decodeRecordIdLongAtEnd(buffer, size);
                case KeyFormat::String:
                    return key_string::decodeRecordIdStrAtEnd(buffer, size);
            }
            MONGO_UNREACHABLE;
        }

        size_t sizeWithoutRecordIdAtEnd(const char* buffer, size_t size, KeyFormat keyFormat) {
            switch (keyFormat) {
                case KeyFormat::Long:
                    return key_string::sizeWithoutRecordIdLongAtEnd(buffer, size);
                case KeyFormat::String:
                    return key_string::sizeWithoutRecordIdStrAtEnd(buffer, size);
            }
            MONGO_UNREACHABLE;
        }
    }  // namespace

    /// RocksIndexBase

    IndexValidateResults RocksIndexBase::validate(OperationContext* opCtx, bool full) const {
        MONGO_UNIMPLEMENTED;  // TODO
    }

    Status RocksIndexBase::initAsEmpty(OperationContext* opCtx) {
        // no-op
        return Status::OK();
    }

    std::string RocksIndexBase::_makePrefixedKey(const std::string& prefix,
                                                 const key_string::Value& encodedKey) {
        std::string key(prefix);
        key.append(encodedKey.getBuffer(), encodedKey.getSize());
        return key;
    }
    /**
     * Bulk builds a non-unique index.
     */
    class RocksIndexBase::StandardBulkBuilder : public SortedDataBuilderInterface {
    public:
        StandardBulkBuilder(RocksStandardIndex* index, OperationContext* opCtx)
            : _index(index), _opCtx(opCtx) {}

        Status addKey(const key_string::Value& key) override {
            return _index->insert(_opCtx, key, true);
        }

    private:
        RocksStandardIndex* _index;
        OperationContext* _opCtx;
    };

    /**
     * Bulk builds a unique index.
     *
     * In order to support unique indexes in dupsAllowed mode this class only does an actual insert
     * after it sees a key after the one we are trying to insert. This allows us to gather up all
     * duplicate locs and insert them all together. This is necessary since bulk cursors can only
     * append data.
     */
    class RocksIndexBase::UniqueBulkBuilder : public SortedDataBuilderInterface {
    public:
        UniqueBulkBuilder(rocksdb::ColumnFamilyHandle* cf, std::string prefix, Ordering ordering,
                          key_string::Version keyStringVersion, NamespaceString ns,
                          std::string indexName, OperationContext* opCtx, bool dupsAllowed,
                          const BSONObj& keyPattern, bool isIdIndex, KeyFormat keyFormat)
            : _cf(cf),
              _prefix(std::move(prefix)),
              _ordering(ordering),
              _keyStringVersion(keyStringVersion),
              _ns(std::move(ns)),
              _indexName(std::move(indexName)),
              _opCtx(opCtx),
              _dupsAllowed(dupsAllowed),
              _keyString(keyStringVersion),
              _keyPattern(keyPattern),
              _isIdIndex(isIdIndex),
              _keyFormat(keyFormat) {}

        Status addKey(const key_string::Value& keyString) override {
            auto key = key_string::toBson(
                keyString.getBuffer(),
                sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize(), _keyFormat),
                _ordering, keyString.getTypeBits());
            auto loc = decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize(), _keyFormat);
            if (_isIdIndex) {
                return addKeyTimestampUnsafe(key, loc);
            } else {
                return addKeyTimestampSafe(key, loc);
            }
        }

        Status addKeyTimestampSafe(const BSONObj& newKey, const RecordId& loc) {
            // Do a duplicate check, but only if dups aren't allowed.
            if (!_dupsAllowed) {
                const int cmp = newKey.woCompare(_previousKey, _ordering);
                if (cmp == 0) {
                    // Duplicate found!
                    return buildDupKeyErrorStatus(newKey, _ns, _indexName, _keyPattern, {});
                } else {
                    // _previousKey.isEmpty() is only true on the first call to addKey().
                    // newKey must be > the last key
                    invariant(_previousKey.isEmpty() || cmp > 0);
                }
            }

            _keyString.resetToKey(newKey, _ordering, loc);
            std::string prefixedKey(
                RocksIndexBase::_makePrefixedKey(_prefix, _keyString.getValueCopy()));
            std::string valueItem = _keyString.getTypeBits().isAllZeros()
                                        ? emptyItem
                                        : std::string(_keyString.getTypeBits().getBuffer(),
                                                      _keyString.getTypeBits().getSize());

            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
            invariant(ru);
            auto transaction = ru->getTransaction();
            invariant(transaction);
            invariantRocksOK(transaction->Put(_cf, prefixedKey, valueItem));

            // Don't copy the key again if dups are allowed.
            if (!_dupsAllowed) {
                _previousKey = newKey.getOwned();
            }
            return Status::OK();
        }

        Status addKeyTimestampUnsafe(const BSONObj& newKey, const RecordId& loc) {
            const int cmp = newKey.woCompare(_previousKey, _ordering);
            if (cmp != 0) {
                if (!_previousKey.isEmpty()) {  // _previousKey.isEmpty() is only true on the first
                                                // call to addKey().
                    invariant(cmp > 0);         // newKey must be > the last key
                    // We are done with dups of the last key so we can insert it now.
                    doInsert();
                }
                invariant(_records.empty());
            } else {
                // Dup found!
                if (!_dupsAllowed) {
                    return buildDupKeyErrorStatus(newKey, _ns, _indexName, _keyPattern, {});
                }

                // If we get here, we are in the weird mode where dups are allowed on a unique
                // index, so add ourselves to the list of duplicate locs. This also replaces the
                // _previousKey which is correct since any dups seen later are likely to be newer.
            }

            _previousKey = newKey.getOwned();
            _keyString.resetToKey(_previousKey, _ordering);
            _records.push_back(std::make_pair(loc, _keyString.getTypeBits()));

            return Status::OK();
        }

    private:
        void doInsert() {
            invariant(!_records.empty());

            key_string::Builder value(_keyStringVersion);
            for (size_t i = 0; i < _records.size(); i++) {
                value.appendRecordId(_records[i].first);
                // When there is only one record, we can omit AllZeros TypeBits. Otherwise they need
                // to be included.
                if (!(_records[i].second.isAllZeros() && _records.size() == 1)) {
                    value.appendTypeBits(_records[i].second);
                }
            }

            std::string prefixedKey(
                RocksIndexBase::_makePrefixedKey(_prefix, _keyString.getValueCopy()));
            rocksdb::Slice valueSlice(value.getBuffer(), value.getSize());

            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
            invariant(ru);
            auto transaction = ru->getTransaction();
            invariant(transaction);
            invariantRocksOK(transaction->Put(_cf, prefixedKey, valueSlice));

            _records.clear();
        }

        rocksdb::ColumnFamilyHandle* _cf;  // not owned
        std::string _prefix;
        Ordering _ordering;
        const key_string::Version _keyStringVersion;
        NamespaceString _ns;
        std::string _indexName;
        OperationContext* _opCtx;
        const bool _dupsAllowed;
        BSONObj _previousKey;
        key_string::Builder _keyString;
        BSONObj _keyPattern;
        const bool _isIdIndex;
        std::vector<std::pair<RecordId, key_string::TypeBits>> _records;
        KeyFormat _keyFormat;
    };

    namespace {
        /**
         * Functionality shared by both unique and standard index
         */
        class RocksCursorBase : public SortedDataInterface::Cursor {
        public:
            RocksCursorBase(OperationContext* opCtx, rocksdb::DB* db,
                            rocksdb::ColumnFamilyHandle* cf, std::string prefix, bool forward,
                            Ordering order, key_string::Version keyStringVersion,
                            KeyFormat keyFormat)
                : _db(db),
                  _cf(cf),
                  _prefix(prefix),
                  _forward(forward),
                  _order(order),
                  _keyFormat(keyFormat),
                  _keyStringVersion(keyStringVersion),
                  _key(keyStringVersion),
                  _typeBits(keyStringVersion),
                  _opCtx(opCtx) {}

            boost::optional<IndexKeyEntry> next(
                KeyInclusion keyInclusion = KeyInclusion::kInclude) override {
                // Advance on a cursor at the end is a no-op
                if (_eof) {
                    return {};
                }
                if (!_lastMoveWasRestore) {
                    advanceCursor();
                }
                updatePosition();
                return curr(keyInclusion);
            }

            boost::optional<KeyStringEntry> nextKeyString() override {
                // Advance on a cursor at the end is a no-op
                if (_eof) {
                    return {};
                }
                if (!_lastMoveWasRestore) {
                    advanceCursor();
                }
                updatePosition();
                return getKeyStringEntry();
            }

            void setEndPosition(const BSONObj& key, bool inclusive) override {
                if (key.isEmpty()) {
                    // This means scan to end of index.
                    _endPosition.reset();
                    return;
                }

                // NOTE: this uses the opposite rules as a normal seek because a forward scan should
                // end after the key if inclusive and before if exclusive.
                const auto discriminator = _forward == inclusive
                                               ? key_string::Discriminator::kExclusiveAfter
                                               : key_string::Discriminator::kExclusiveBefore;
                _endPosition = std::make_unique<key_string::Builder>(_keyStringVersion);
                _endPosition->resetToKey(stripFieldNames(key), _order, discriminator);
            }

            boost::optional<KeyStringEntry> seekForKeyString(
                const key_string::Value& keyString) override {
                _query = keyString;
                seekCursor(_query);
                updatePosition();
                return getKeyStringEntry();
            }

            boost::optional<IndexKeyEntry> seek(
                const key_string::Value& keyString,
                KeyInclusion keyInclusion = KeyInclusion::kInclude) override {
                _query = keyString;
                seekCursor(_query);
                updatePosition();
                return curr(keyInclusion);
            }

            void save() override {
                try {
                    if (_iterator) {
                        _iterator.reset();
                    }
                } catch (const WriteConflictException&) {
                }
                if (!_lastMoveWasRestore) {
                    _savedEOF = _eof;
                }
            }

            void saveUnpositioned() override {
                save();
                _savedEOF = true;
            }

            void restore() override {
                if (!_iterator) {
                    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
                    invariant(ru);
                    _iterator.reset(ru->NewIterator(_cf, _prefix));
                    invariant(_iterator);
                }

                // TODO(cuixin): rocks need an interface let iterator get txn
                // invariant(ru->getTransaction() == _iterator->getSession());

                if (!_savedEOF) {
                    _lastMoveWasRestore = !seekCursor(_key.getValueCopy());
                }
            }

            void detachFromOperationContext() final {
                _opCtx = nullptr;
                _iterator.reset();
            }

            void reattachToOperationContext(OperationContext* opCtx) final {
                _opCtx = opCtx;
                // iterator recreated in restore()
            }

            void setSaveStorageCursorOnDetachFromOperationContext(bool) final {
                MONGO_UNIMPLEMENTED;  // TODO
            }

        protected:
            // Called after _key has been filled in. Must not throw WriteConflictException.
            virtual void updateLocAndTypeBits() {
                _loc = decodeRecordIdAtEnd(_key.getBuffer(), _key.getSize(), _keyFormat);
                BufReader br(_valueSlice().data(), _valueSlice().size());
                _typeBits.resetFromBuffer(&br);
            }

            boost::optional<IndexKeyEntry> curr(KeyInclusion keyInclusion) const {
                if (_eof) {
                    return {};
                }

                BSONObj bson;
                if (keyInclusion == KeyInclusion::kInclude) {
                    bson = key_string::toBson(_key.getBuffer(), _key.getSize(), _order, _typeBits);
                }

                return {{std::move(bson), _loc}};
            }

            boost::optional<KeyStringEntry> getKeyStringEntry() {
                if (_eof) {
                    return {};
                }

                return {{_key.getValueCopy(), _loc}};
            }

            void advanceCursor() {
                if (_eof) {
                    return;
                }
                if (_iterator.get() == nullptr) {
                    _iterator.reset(
                        RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(_cf, _prefix));
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        _iterator->SeekPrefix(rocksdb::Slice(_key.getBuffer(), _key.getSize()));
                        return _iterator->status();
                    });
                    // advanceCursor() should only ever be called in states where the above seek
                    // will succeed in finding the exact key
                    invariant(_iterator->Valid());
                }
                rocksPrepareConflictRetry(_opCtx, [&] {
                    _forward ? _iterator->Next() : _iterator->Prev();
                    return _iterator->status();
                });
                _updateOnIteratorValidity();
            }

            // Seeks to query. Returns true on exact match.
            bool seekCursor(const key_string::Value& query) {
                auto* iter = iterator();
                const rocksdb::Slice keySlice(query.getBuffer(), query.getSize());
                rocksPrepareConflictRetry(_opCtx, [&] {
                    if (_forward) {
                        iter->Seek(keySlice);
                    } else {
                        iter->SeekForPrev(keySlice);
                    }
                    return iter->status();
                });
                if (!_updateOnIteratorValidity()) {
                    return false;
                }

                if (iter->key() == keySlice) {
                    return true;
                }

                auto found = iter->key();
                int cmp = std::memcmp(found.data(), keySlice.data(),
                                      std::min(found.size(), keySlice.size()));

                // Make sure we land on a matching key (after/before for forward/reverse).
                // If this operation is ignoring prepared updates and search_near() lands on a key
                // that compares lower than the search key (for a forward cursor), calling next() is
                // not guaranteed to return a key that compares greater than the search key. This is
                // because ignoring prepare conflicts does not provide snapshot isolation and the
                // call to next() may land on a newly-committed prepared entry. We must advance our
                // cursor until we find a key that compares greater than the search key. The same
                // principle applies to reverse cursors. See SERVER-56839.
                const bool enforcingPrepareConflicts =
                    _opCtx->recoveryUnit()->getPrepareConflictBehavior() ==
                    PrepareConflictBehavior::kEnforce;
                rocksdb::Slice curKey;
                while (_forward ? cmp < 0 : cmp > 0) {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        if (_forward) {
                            iter->Next();
                        } else {
                            iter->Prev();
                        }
                        return iter->status();
                    });

                    if (!_updateOnIteratorValidity()) {
                        break;
                    }

                    if (!kDebugBuild && enforcingPrepareConflicts) {
                        break;
                    }

                    rocksdb::Slice key;
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        key = iter->key();
                        return iter->status();
                    });

                    cmp = std::memcmp(curKey.data(), keySlice.data(),
                                      std::min(curKey.size(), keySlice.size()));

                    if (enforcingPrepareConflicts) {
                        // If we are enforcing prepare conflicts, calling next() or prev() must
                        // always give us a key that compares, respectively, greater than or less
                        // than our search key. An exact match is also possible in the case of _id
                        // indexes, because the recordid is not a part of the key.
                        dassert(_forward ? cmp >= 0 : cmp <= 0);
                    }
                }

                return false;
            }

            void updatePosition() {
                _lastMoveWasRestore = false;
                if (_eof) {
                    _loc = RecordId();
                    return;
                }

                if (_iterator.get() == nullptr) {
                    // _iterator is out of position because we just did a seekExact
                    _key.resetFromBuffer(_query.getBuffer(), _query.getSize());
                } else {
                    auto key = _iterator->key();
                    _key.resetFromBuffer(key.data(), key.size());
                }

                if (_endPosition) {
                    int cmp = _key.compare(*_endPosition);
                    if (_forward ? cmp > 0 : cmp < 0) {
                        _eof = true;
                        return;
                    }
                }

                updateLocAndTypeBits();
            }

            // ensure that _iterator is initialized and return a pointer to it
            RocksIterator* iterator() {
                if (_iterator.get() == nullptr) {
                    _iterator.reset(
                        RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(_cf, _prefix));
                }
                return _iterator.get();
            }

            // Update _eof based on _iterator->Valid() and return _iterator->Valid()
            bool _updateOnIteratorValidity() {
                if (_iterator->Valid()) {
                    _eof = false;
                    return true;
                } else {
                    _eof = true;
                    invariantRocksOK(_iterator->status());
                    return false;
                }
            }

            rocksdb::Slice _valueSlice() {
                if (_iterator.get() == nullptr) {
                    return rocksdb::Slice(_value);
                }
                return rocksdb::Slice(_iterator->value());
            }

            rocksdb::DB* _db;                  // not owned
            rocksdb::ColumnFamilyHandle* _cf;  // not owned
            std::string _prefix;
            std::unique_ptr<RocksIterator> _iterator;
            const bool _forward;
            bool _lastMoveWasRestore = false;
            Ordering _order;
            KeyFormat _keyFormat;

            // These are for storing savePosition/restorePosition state
            bool _savedEOF = false;
            RecordId _savedRecordId;

            key_string::Version _keyStringVersion;
            key_string::Builder _key;
            key_string::TypeBits _typeBits;
            RecordId _loc;

            key_string::Value _query;

            std::unique_ptr<key_string::Builder> _endPosition;

            bool _eof = false;
            OperationContext* _opCtx;

            // stores the value associated with the latest call to seekExact()
            std::string _value;
        };

        class RocksStandardCursor final : public RocksCursorBase {
        public:
            RocksStandardCursor(OperationContext* opCtx, rocksdb::DB* db,
                                rocksdb::ColumnFamilyHandle* cf, std::string prefix, bool forward,
                                Ordering order, key_string::Version keyStringVersion,
                                KeyFormat keyFormat)
                : RocksCursorBase(opCtx, db, cf, prefix, forward, order, keyStringVersion,
                                  keyFormat) {
                iterator();
            }

            bool isRecordIdAtEndOfKeyString() const override { return true; }
        };

        class RocksUniqueCursor final : public RocksCursorBase {
        public:
            RocksUniqueCursor(OperationContext* opCtx, rocksdb::DB* db,
                              rocksdb::ColumnFamilyHandle* cf, std::string prefix, bool forward,
                              Ordering order, key_string::Version keyStringVersion,
                              KeyFormat keyFormat, std::string indexName, bool isIdIndex)
                : RocksCursorBase(opCtx, db, cf, prefix, forward, order, keyStringVersion,
                                  keyFormat),
                  _indexName(std::move(indexName)),
                  _isIdIndex(isIdIndex) {}

            bool isRecordIdAtEndOfKeyString() const override {
                return _key.getSize() !=
                       key_string::getKeySize(_key.getBuffer(), _key.getSize(), _order, _typeBits);
            }

            void restore() override {
                RocksCursorBase::restore();

                auto it = iterator();
                if (!_lastMoveWasRestore || !it->Valid()) {
                    return;
                }

                auto keySize =
                    key_string::getKeySize(_key.getBuffer(), _key.getSize(), _order, _typeBits);

                // This check is only to avoid returning the same key again after a restore. Keys
                // shorter than _key cannot have "prefix key" same as _key. Therefore we care only
                // about the keys with size greater than or equal to that of the _key.
                auto itKey = it->key();
                if (itKey.size() >= keySize &&
                    std::memcmp(_key.getBuffer(), itKey.data(), keySize) == 0) {
                    _lastMoveWasRestore = false;
                }
            }

            void updateLocAndTypeBits() {
                // _id indexes remain at the old format
                if (!_isIdIndex) {
                    RocksCursorBase::updateLocAndTypeBits();
                    return;
                }
                BufReader br(_valueSlice().data(), _valueSlice().size());
                _loc = key_string::decodeRecordIdLong(&br);
                _typeBits.resetFromBuffer(&br);

                if (!br.atEof()) {
                    LOGV2_FATAL(28609, "Unique index cursor seeing multiple records",
                                "key"_attr = redact(curr(KeyInclusion::kInclude)->key),
                                "index"_attr = _indexName);
                    fassertFailed(28609);
                }
            }

        private:
            std::string _indexName;
            const bool _isIdIndex;
        };
    }  // namespace

    /// RocksIndexBase
    RocksIndexBase::RocksIndexBase(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                                   std::string prefix, const UUID& uuid, std::string ident,
                                   Ordering order, KeyFormat keyFormat, const BSONObj& config,
                                   NamespaceString ns, std::string indexName,
                                   const BSONObj& keyPattern)
        : SortedDataInterface(
              ident,
              [&] {
                  int indexFormatVersion = 0;  // default
                  if (config.hasField("index_format_version")) {
                      indexFormatVersion = config.getField("index_format_version").numberInt();
                  }

                  if (indexFormatVersion < kMinimumIndexVersion ||
                      indexFormatVersion > kMaximumIndexVersion) {
                      Status indexVersionStatus(
                          ErrorCodes::UnsupportedFormat,
                          "Unrecognized index format -- you might want to upgrade MongoDB");
                      fassertFailedWithStatusNoTrace(ErrorCodes::InternalError, indexVersionStatus);
                  }

                  return indexFormatVersion;
              }() >= kKeyStringV1Version
                  ? key_string::Version::V1
                  : key_string::Version::V0,
              order, keyFormat),
          _db(db),
          _cf(cf),
          _prefix(prefix),
          _ident(std::move(ident)),
          _ns(std::move(ns)),
          _indexName(std::move(indexName)),
          _keyPattern(keyPattern) {
        uint64_t storageSize;
        std::string nextPrefix = rocksGetNextPrefix(_prefix);
        rocksdb::Range wholeRange(_prefix, nextPrefix);
        _db->GetApproximateSizes(&wholeRange, 1, &storageSize);
        _indexStorageSize.store(static_cast<long long>(storageSize), std::memory_order_relaxed);
    }

    boost::optional<RecordId> RocksIndexBase::findLoc(OperationContext* opCtx,
                                                      const key_string::Value& key) const {
        dassert(key_string::decodeDiscriminator(key.getBuffer(), key.getSize(), getOrdering(),
                                                key.getTypeBits()) ==
                key_string::Discriminator::kInclusive);

        auto cursor = newCursor(opCtx);
        auto ksEntry = cursor->seekForKeyString(key);
        if (!ksEntry) {
            return boost::none;
        }

        if (key_string::compare(
                ksEntry->keyString.getBuffer(), key.getBuffer(),
                sizeWithoutRecordIdAtEnd(ksEntry->keyString.getBuffer(),
                                         ksEntry->keyString.getSize(), _rsKeyFormat),
                key.getSize()) == 0) {
            return ksEntry->loc;
        }
        return boost::none;
    }

    bool RocksIndexBase::isEmpty(OperationContext* opCtx) {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        std::unique_ptr<rocksdb::Iterator> it(ru->NewIterator(_cf, _prefix));

        auto s = rocksPrepareConflictRetry(opCtx, [&] {
            it->SeekToFirst();
            return it->status();
        });
        return !it->Valid();
    }

    long long RocksIndexBase::getSpaceUsedBytes(OperationContext* opCtx) const {
        // There might be some bytes in the WAL that we don't count here. Some
        // tests depend on the fact that non-empty indexes have non-zero sizes
        return static_cast<long long>(
            std::max(_indexStorageSize.load(std::memory_order_relaxed), static_cast<long long>(1)));
    }

    long long RocksIndexBase::getFreeStorageBytes(OperationContext* opCtx) const {
        MONGO_UNIMPLEMENTED;  // TODO
    }

    void RocksIndexBase::printIndexEntryMetadata(OperationContext* opCtx,
                                                 const key_string::Value& keyString) const {
        MONGO_UNIMPLEMENTED;  // TODO
    }

    int64_t RocksIndexBase::numEntries(OperationContext* opCtx) const {
        std::unique_ptr<SortedDataInterface::Cursor> cursor(newCursor(opCtx, 1));

        int64_t numEntries = 0;
        for (auto entry = cursor->seek(key_string::Value{}, Cursor::KeyInclusion::kExclude); entry;
             entry = cursor->next(Cursor::KeyInclusion::kExclude)) {
            ++numEntries;
        }

        return numEntries;
    }

    void RocksIndexBase::generateConfig(BSONObjBuilder* configBuilder, int formatVersion,
                                        IndexDescriptor::IndexVersion descVersion) {
        if (formatVersion >= 3 && descVersion >= IndexDescriptor::IndexVersion::kV2) {
            configBuilder->append("index_format_version",
                                  static_cast<int32_t>(kMaximumIndexVersion));
        } else {
            // keep it backwards compatible
            configBuilder->append("index_format_version",
                                  static_cast<int32_t>(kMinimumIndexVersion));
        }
    }

    /// RocksUniqueIndex

    RocksUniqueIndex::RocksUniqueIndex(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                                       std::string prefix, const UUID& uuid, std::string ident,
                                       Ordering order, KeyFormat keyFormat, const BSONObj& config,
                                       NamespaceString ns, std::string indexName,
                                       const BSONObj& keyPattern, bool partial, bool isIdIdx)
        : RocksIndexBase(db, cf, prefix, uuid, ident, order, keyFormat, config, std::move(ns),
                         std::move(indexName), keyPattern),
          _partial(partial),
          _isIdIndex(isIdIdx) {}

    std::unique_ptr<SortedDataInterface::Cursor> RocksUniqueIndex::newCursor(
        OperationContext* opCtx, bool forward) const {
        return std::make_unique<RocksUniqueCursor>(opCtx, _db, _cf, _prefix, forward, _ordering,
                                                   _keyStringVersion, _rsKeyFormat, _indexName,
                                                   _isIdIndex);
    }

    std::unique_ptr<SortedDataBuilderInterface> RocksUniqueIndex::makeBulkBuilder(
        OperationContext* opCtx, bool dupsAllowed) {
        return std::make_unique<RocksIndexBase::UniqueBulkBuilder>(
            _cf, _prefix, _ordering, _keyStringVersion, _ns, _indexName, opCtx, dupsAllowed,
            _keyPattern, _isIdIndex, _rsKeyFormat);
    }

    Status RocksUniqueIndex::insert(OperationContext* opCtx, const key_string::Value& keyString,
                                    bool dupsAllowed,
                                    IncludeDuplicateRecordId includeDuplicateRecordId) {
        auto key = key_string::toBson(
            keyString.getBuffer(),
            sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize(), _rsKeyFormat),
            _ordering, keyString.getTypeBits());
        auto loc = decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize(), _rsKeyFormat);
        return _isIdIndex ? _insertIntoIdIndex(opCtx, key, loc, dupsAllowed)
                          : _insert(opCtx, key, loc, dupsAllowed);
    }

    std::unique_ptr<RocksIterator> RocksUniqueIndex::_keyExists(OperationContext* opCtx,
                                                                const key_string::Value& key) {
        std::unique_ptr<RocksIterator> iter;
        iter.reset(RocksRecoveryUnit::getRocksRecoveryUnit(opCtx)->NewIterator(_cf, _prefix));
        auto s = rocksPrepareConflictRetry(opCtx, [&] {
            iter->SeekPrefix(rocksdb::Slice(key.getBuffer(), key.getSize()));
            return iter->status();
        });
        return iter->Valid() ? std::move(iter) : nullptr;
    }

    Status RocksUniqueIndex::_insert(OperationContext* opCtx, const BSONObj& key,
                                     const RecordId& loc, bool dupsAllowed) {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        invariant(ru->getTransaction());
        if (!dupsAllowed) {
            auto encodedKey = key_string::Builder{_keyStringVersion, key, _ordering}.getValueCopy();
            const std::string prefixedKey(RocksIndexBase::_makePrefixedKey(_prefix, encodedKey));
            invariantRocksOK(ru->getTransaction()->GetForUpdate(_cf, prefixedKey));
            if (auto it = _keyExists(opCtx, encodedKey)) {
                auto itKey = it->key();
                return decodeRecordIdAtEnd(itKey.data(), itKey.size(), _rsKeyFormat) != loc
                           ? buildDupKeyErrorStatus(key, _ns, _indexName, _keyPattern, {})
                           : Status::OK();
            }
        }
        auto tableKey = key_string::Builder{_keyStringVersion, key, _ordering, loc}.getValueCopy();
        const std::string prefixedKey(RocksIndexBase::_makePrefixedKey(_prefix, tableKey));
        std::string valueItem =
            tableKey.getTypeBits().isAllZeros()
                ? emptyItem
                : std::string(tableKey.getTypeBits().getBuffer(), tableKey.getTypeBits().getSize());
        invariantRocksOK(ROCKS_OP_CHECK(ru->getTransaction()->Put(_cf, prefixedKey, valueItem)));
        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);

        return Status::OK();
    }

    Status RocksUniqueIndex::_insertIntoIdIndex(OperationContext* opCtx, const BSONObj& key,
                                                const RecordId& loc, bool dupsAllowed) {
        dassert(opCtx->lockState()->isWriteLocked());
        invariant(loc.isValid());
        dassert(!hasFieldNames(key));
        invariant(!dupsAllowed);

        auto encodedKey = key_string::Builder{_keyStringVersion, key, _ordering}.getValueCopy();
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        invariant(ru->getTransaction());
        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);

        std::string currentValue;
        auto getStatus = rocksPrepareConflictRetry(
            opCtx, [&] { return ru->Get(_cf, prefixedKey, &currentValue); });

        if (getStatus.ok()) {
            return buildDupKeyErrorStatus(key, _ns, _indexName, _keyPattern, {});
        }

        if (!getStatus.IsNotFound()) {
            return rocksToMongoStatus(getStatus);
        }

        // nothing here. just insert the value
        key_string::Builder value(_keyStringVersion, loc);
        if (!encodedKey.getTypeBits().isAllZeros()) {
            value.appendTypeBits(encodedKey.getTypeBits());
        }
        rocksdb::Slice valueSlice(value.getBuffer(), value.getSize());
        invariantRocksOK(ROCKS_OP_CHECK(ru->getTransaction()->Put(_cf, prefixedKey, valueSlice)));
        return Status::OK();
    }

    void RocksUniqueIndex::_unindex(OperationContext* opCtx, const BSONObj& key,
                                    const RecordId& loc, bool dupsAllowed) {
        auto encodedKey =
            key_string::Builder(_keyStringVersion, key, _ordering, loc).getValueCopy();
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto transaction = ru->getTransaction();
        invariant(transaction);

        invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
        _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);
    }

    void RocksUniqueIndex::unindex(OperationContext* opCtx, const key_string::Value& keyString,
                                   bool dupsAllowed) {
        auto key = key_string::toBson(
            keyString.getBuffer(),
            sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize(), _rsKeyFormat),
            _ordering, keyString.getTypeBits());
        auto loc = decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize(), _rsKeyFormat);
        if (_isIdIndex) {
            _unindexFromIdIndex(opCtx, key, loc, dupsAllowed);
        } else {
            _unindex(opCtx, key, loc, dupsAllowed);
        }
    }

    void RocksUniqueIndex::_unindexFromIdIndex(OperationContext* opCtx, const BSONObj& key,
                                               const RecordId& loc, bool dupsAllowed) {
        auto encodedKey = key_string::Builder{_keyStringVersion, key, _ordering}.getValueCopy();
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto transaction = ru->getTransaction();
        invariant(transaction);

        auto triggerWriteConflictAtPoint = [&]() {
            // NOTE(wolfkdy): can only be called when a Get returns NOT_FOUND, to avoid SI's
            // write skew. this function has the semantics of GetForUpdate.
            // DO NOT use this if you dont know if the exists or not.
            invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
        };

        if (!dupsAllowed) {
            std::string tmpVal;
            if (_partial) {
                // Check that the record id matches.  We may be called to unindex records that are
                // not present in the index due to the partial filter expression.
                auto s = rocksPrepareConflictRetry(
                    opCtx, [&] { return ru->Get(_cf, prefixedKey, &tmpVal); });
                if (s.IsNotFound()) {
                    // NOTE(wolfkdy): SERVER-28546
                    triggerWriteConflictAtPoint();
                    return;
                }
                invariantRocksOK(s);
                BufReader br(tmpVal.data(), tmpVal.size());
                invariant(br.remaining());
                if (key_string::decodeRecordIdLong(&br) != loc) {
                    return;
                }
                // Ensure there aren't any other values in here.
                key_string::TypeBits::fromBuffer(_keyStringVersion, &br);
                invariant(!br.remaining());
            }
            invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
            _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                        std::memory_order_relaxed);
            return;
        }

        // dups are allowed, so we have to deal with a vector of RecordIds.
        std::string currentValue;
        auto getStatus = rocksPrepareConflictRetry(
            opCtx, [&] { return ru->Get(_cf, prefixedKey, &currentValue); });
        if (getStatus.IsNotFound()) {
            // NOTE(wolfkdy): SERVER-28546
            triggerWriteConflictAtPoint();
            return;
        }
        invariantRocksOK(getStatus);

        bool foundLoc = false;
        std::vector<std::pair<RecordId, key_string::TypeBits>> records;

        BufReader br(currentValue.data(), currentValue.size());
        while (br.remaining()) {
            RecordId locInIndex = key_string::decodeRecordIdLong(&br);
            key_string::TypeBits typeBits =
                key_string::TypeBits::fromBuffer(_keyStringVersion, &br);

            if (loc == locInIndex) {
                if (records.empty() && !br.remaining()) {
                    // This is the common case: we are removing the only loc for this key.
                    // Remove the whole entry.
                    invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
                    _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                                std::memory_order_relaxed);
                    return;
                }

                foundLoc = true;
                continue;
            }

            records.push_back(std::make_pair(locInIndex, typeBits));
        }

        if (!foundLoc) {
            LOGV2_WARNING(0, "Record id not found in index", "recordId"_attr = loc,
                          "key"_attr = redact(key));
            return;  // nothing to do
        }

        // Put other locs for this key back in the index.
        key_string::Builder newValue(_keyStringVersion);
        invariant(!records.empty());
        for (size_t i = 0; i < records.size(); i++) {
            newValue.appendRecordId(records[i].first);
            // When there is only one record, we can omit AllZeros TypeBits. Otherwise they need
            // to be included.
            if (!(records[i].second.isAllZeros() && records.size() == 1)) {
                newValue.appendTypeBits(records[i].second);
            }
        }

        rocksdb::Slice newValueSlice(newValue.getBuffer(), newValue.getSize());
        invariantRocksOK(ROCKS_OP_CHECK(transaction->Put(_cf, prefixedKey, newValueSlice)));
        _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);
    }

    Status RocksUniqueIndex::dupKeyCheck(OperationContext* opCtx,
                                         const key_string::Value& keyString) {
        invariant(!_isIdIndex);

        auto it = _keyExists(opCtx, keyString);
        if (!it) {
            return Status::OK();
        }

        rocksPrepareConflictRetry(opCtx, [&] {
            it->Next();
            return it->status();
        });

        if (!it->Valid()) {
            return Status::OK();
        }

        auto key = it->key();
        return key.size() != keyString.getSize() ||
                       std::memcmp(key.data(), keyString.getBuffer(), key.size()) != 0
                   ? buildDupKeyErrorStatus(keyString, _ns, _indexName, _keyPattern, {}, _ordering)
                   : Status::OK();
    }

    void RocksUniqueIndex::insertWithRecordIdInValue_forTest(OperationContext* opCtx,
                                                             const key_string::Value& keyString,
                                                             RecordId rid) {
        MONGO_UNIMPLEMENTED;  // TODO
    }

    /// RocksStandardIndex
    RocksStandardIndex::RocksStandardIndex(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                                           std::string prefix, const UUID& uuid, std::string ident,
                                           Ordering order, KeyFormat keyFormat,
                                           const BSONObj& config, NamespaceString ns,
                                           std::string indexName, const BSONObj& keyPattern)
        : RocksIndexBase(db, cf, prefix, uuid, ident, order, keyFormat, config, std::move(ns),
                         std::move(indexName), keyPattern),
          useSingleDelete(false) {}

    Status RocksStandardIndex::insert(OperationContext* opCtx, const key_string::Value& keyString,
                                      bool dupsAllowed,
                                      IncludeDuplicateRecordId includeDuplicateRecordId) {
        dassert(opCtx->lockState()->isWriteLocked());

        if (!dupsAllowed) {
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
            invariant(ru);
            invariant(ru->getTransaction());

            key_string::Builder encodedKeyBuilder{_keyStringVersion, _ordering};
            encodedKeyBuilder.resetFromBuffer(
                keyString.getBuffer(),
                sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize(), _rsKeyFormat));
            auto encodedKey = encodedKeyBuilder.getValueCopy();

            const std::string prefixedKey(RocksIndexBase::_makePrefixedKey(_prefix, encodedKey));
            invariantRocksOK(ru->getTransaction()->GetForUpdate(_cf, prefixedKey));

            auto keyExists = [&] {
                std::unique_ptr<RocksIterator> iter;
                iter.reset(
                    RocksRecoveryUnit::getRocksRecoveryUnit(opCtx)->NewIterator(_cf, _prefix));
                auto s = rocksPrepareConflictRetry(opCtx, [&] {
                    iter->SeekPrefix(rocksdb::Slice(encodedKey.getBuffer(), encodedKey.getSize()));
                    return iter->status();
                });
                return iter->Valid();
            }();

            if (keyExists) {
                return buildDupKeyErrorStatus(keyString, _ns, _indexName, _keyPattern, {},
                                              _ordering);
            }
        }

        std::string prefixedKey(_makePrefixedKey(_prefix, keyString));
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto transaction = ru->getTransaction();

        rocksdb::Slice value;
        if (!keyString.getTypeBits().isAllZeros()) {
            value =
                rocksdb::Slice(reinterpret_cast<const char*>(keyString.getTypeBits().getBuffer()),
                               keyString.getTypeBits().getSize());
        }

        invariantRocksOK(ROCKS_OP_CHECK(transaction->Put(_cf, prefixedKey, value)));
        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);

        return Status::OK();
    }

    void RocksStandardIndex::unindex(OperationContext* opCtx, const key_string::Value& keyString,
                                     bool dupsAllowed) {
        invariant(dupsAllowed);
        dassert(opCtx->lockState()->isWriteLocked());

        std::string prefixedKey(_makePrefixedKey(_prefix, keyString));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto transaction = ru->getTransaction();
        invariant(transaction);

        if (useSingleDelete) {
            LOGV2_WARNING(0, "mongoRocks4.0+ nolonger supports singleDelete, fallback to Delete");
        }
        invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
        _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);
    }

    std::unique_ptr<SortedDataInterface::Cursor> RocksStandardIndex::newCursor(
        OperationContext* opCtx, bool forward) const {
        return std::make_unique<RocksStandardCursor>(opCtx, _db, _cf, _prefix, forward, _ordering,
                                                     _keyStringVersion, _rsKeyFormat);
    }

    std::unique_ptr<SortedDataBuilderInterface> RocksStandardIndex::makeBulkBuilder(
        OperationContext* opCtx, bool dupsAllowed) {
        invariant(dupsAllowed);
        return std::make_unique<RocksIndexBase::StandardBulkBuilder>(this, opCtx);
    }

    void RocksStandardIndex::insertWithRecordIdInValue_forTest(OperationContext* opCtx,
                                                               const key_string::Value& keyString,
                                                               RecordId rid) {
        MONGO_UNIMPLEMENTED;  // TODO
    }

}  // namespace mongo
