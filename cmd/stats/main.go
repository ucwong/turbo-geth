package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"
)

func main() {
	//testMigrate()
	storageFormatDiff2()
	//collectStorageNumOfDuplicate()
}

func testMigrate() {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}

	//err=db.DeleteBucket(dbutils.AccountsHistoryIndexBucket)
	//if err!=nil {
	//	log.Fatal("delete AccountsHistoryIndexBucket", err)
	//}
	//err = db.DeleteBucket(dbutils.StorageHistoryIndexBucket)
	//if err!=nil {
	//	log.Fatal("delete StorageHistoryIndexBucket", err)
	//}

	//bn:=uint64(9254301)
	numOfBatch := 0
	accIndex := make(map[string]*ethdb.HistoryIndexBytes, 0)
	storageIndex := make(map[string]*ethdb.HistoryIndexBytes, 0)
	accChangeset := make(map[uint64][]byte, 0)
	storageChangeset := make(map[uint64][]byte, 0)

	var k, v []byte
	var done bool
	for !done {
		numOfBatch = 0
		err := db.DB().Update(func(tx *bolt.Tx) error {
			var csBucket *bolt.Bucket = tx.Bucket(dbutils.ChangeSetBucket)
			c := csBucket.Cursor()

			if k == nil {
				k, v = c.First()
			} else {
				k, v = c.Seek(k)
				k, v = c.Next()
			}

			for ; k != nil; k, v = c.Next() {

				ts, bucket := dbutils.DecodeTimestamp(k)
				fmt.Println(ts, string(bucket))

				cs, err := dbutils.DecodeChangeSet(v)
				if err != nil {
					fmt.Println(ts, string(bucket), err)
					return err
				}

				switch {
				case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
					for _, v := range cs.Changes {
						index, ok := accIndex[string(v.Key)]
						if !ok {
							indexBytes, err := db.Get(dbutils.AccountsHistoryIndexBucket, v.Key)
							if err != nil && err != ethdb.ErrKeyNotFound {
								log.Fatal(err)
							}
							index = ethdb.WrapHistoryIndex(indexBytes)
						}
						index.Append(ts)
						accIndex[string(v.Key)] = index
					}
					vCopy := make([]byte, len(v))
					copy(vCopy, v)
					accChangeset[ts] = vCopy
				case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
					cs2 := &changeset.ChangeSet{
						Changes: make([]changeset.Change, len(cs.Changes)),
					}
					for i, v := range cs.Changes {
						cs2.Changes[i] = changeset.Change{
							Key:   cs.Changes[i].Key,
							Value: cs.Changes[i].Value,
						}

						//fill storage index
						index, ok := storageIndex[string(v.Key)]
						if !ok {
							indexBytes, err := db.Get(dbutils.StorageHistoryIndexBucket, v.Key)
							if err != nil && err != ethdb.ErrKeyNotFound {
								return err
							}
							index = ethdb.WrapHistoryIndex(indexBytes)
						}
						index.Append(ts)
						storageIndex[string(v.Key)] = index
					}

					expCsEnc2, err := changeset.EncodeStorageDict(cs2)
					if err != nil {
						fmt.Println(ts, string(bucket), err)
						return err
					}
					storageChangeset[ts] = expCsEnc2

				default:
					fmt.Println(string(k), "------------------------------")
				}

				if numOfBatch > 100 {
					commTime := time.Now()

					fmt.Println("Start update")
					if len(accIndex) > 0 {
						accIndexBucket, err := tx.CreateBucketIfNotExists(dbutils.AccountsHistoryIndexBucket, false)
						if err != nil {
							return err
						}

						for i := range accIndex {
							err = accIndexBucket.Put([]byte(i), *accIndex[i])
							if err != nil {
								return err
							}
						}
					}
					if len(storageIndex) > 0 {
						storageIndexBucket, err := tx.CreateBucketIfNotExists(dbutils.StorageHistoryIndexBucket, false)
						if err != nil {
							return err
						}

						for i := range storageIndex {
							err = storageIndexBucket.Put([]byte(i), *storageIndex[i])
							if err != nil {
								return err
							}
						}
					}

					if len(storageChangeset) > 0 {
						storageCSBucket, err := tx.CreateBucketIfNotExists(dbutils.StorageChangeSetBucket, false)
						if err != nil {
							return err
						}

						for i := range storageChangeset {
							err = storageCSBucket.Put(dbutils.EncodeTimestamp(i), storageChangeset[i])
							if err != nil {
								return err
							}
						}
					}

					if len(accChangeset) > 0 {
						accCSBucket, err := tx.CreateBucketIfNotExists(dbutils.AccountChangeSetBucket, false)
						if err != nil {
							return err
						}

						for i := range accChangeset {
							err = accCSBucket.Put(dbutils.EncodeTimestamp(i), accChangeset[i])
							if err != nil {
								return err
							}
						}
 					}
					fmt.Println("Commit", ts, time.Now().Sub(commTime), time.Now().Sub(startTime))
					if err != nil {
						log.Fatal("err on update", err, ts)
					}
					accIndex = make(map[string]*ethdb.HistoryIndexBytes, 0)
					storageIndex = make(map[string]*ethdb.HistoryIndexBytes, 0)
					accChangeset = make(map[uint64][]byte, 0)
					storageChangeset = make(map[uint64][]byte, 0)
					break
				} else {
					numOfBatch++
				}

			}

			if k == nil {
				done = true
			}
			k = common.CopyBytes(k)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	/*	chb := tx.Bucket(dbutils.ChangeSetBucket)
			err = chb.ForEach(func(k, v []byte) error {
				ts, bucket := dbutils.DecodeTimestamp(k)
				fmt.Println(ts, string(bucket))

				cs, err := dbutils.DecodeChangeSet(v)
				if err != nil {
					fmt.Println(ts, string(bucket), err)
					return err
				}

				switch {
				case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
					for _, v := range cs.Changes {
						index, ok := accIndex[string(v.Key)]
						if !ok {
							indexBytes, err := db.Get(dbutils.AccountsHistoryIndexBucket, v.Key)
							if err != nil && err != ethdb.ErrKeyNotFound {
								log.Fatal(err)
							}
							index = ethdb.WrapHistoryIndex(indexBytes)
						}
						index.Append(ts)
						accIndex[string(v.Key)] = index
					}
					vCopy := make([]byte, len(v))
					copy(vCopy, v)
					accChangeset[ts] = vCopy
				case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
					cs2 := &changeset.ChangeSet{
						Changes: make([]changeset.Change, len(cs.Changes)),
					}
					for i, v := range cs.Changes {
						cs2.Changes[i] = changeset.Change{
							Key:   cs.Changes[i].Key,
							Value: cs.Changes[i].Value,
						}

						//fill storage index
						index, ok := storageIndex[string(v.Key)]
						if !ok {
							indexBytes, err := db.Get(dbutils.StorageHistoryIndexBucket, v.Key)
							if err != nil && err != ethdb.ErrKeyNotFound {
								return err
							}
							index = ethdb.WrapHistoryIndex(indexBytes)
						}
						index.Append(ts)
						storageIndex[string(v.Key)] = index
					}

					expCsEnc2, err := changeset.EncodeStorageDict(cs2)
					if err != nil {
						fmt.Println(ts, string(bucket), err)
						return err
					}
					storageChangeset[ts] = expCsEnc2

				default:
					fmt.Println(string(k), "------------------------------")
				}

				if numOfBatch > 100 {
					commTime := time.Now()

					fmt.Println("Start update")
					err = db.DB().Update(func(tx *bolt.Tx) error {
						if len(accIndex) > 0 {
							accIndexBucket, err := tx.CreateBucketIfNotExists(dbutils.AccountsHistoryIndexBucket, false)
							if err != nil {
								return err
							}

							for i := range accIndex {
								err = accIndexBucket.Put([]byte(i), *accIndex[i])
								if err != nil {
									return err
								}
							}
						}
						if len(storageIndex) > 0 {
							storageIndexBucket, err := tx.CreateBucketIfNotExists(dbutils.StorageHistoryIndexBucket, false)
							if err != nil {
								return err
							}

							for i := range storageIndex {
								err = storageIndexBucket.Put([]byte(i), *storageIndex[i])
								if err != nil {
									return err
								}
							}
						}

						if len(storageChangeset) > 0 {
							storageCSBucket, err := tx.CreateBucketIfNotExists(dbutils.StorageChangeSetBucket, false)
							if err != nil {
								return err
							}

							for i := range storageChangeset {
								err = storageCSBucket.Put(dbutils.EncodeTimestamp(i), storageChangeset[i])
								if err != nil {
									return err
								}
							}
						}

						if len(accChangeset) > 0 {
							accCSBucket, err := tx.CreateBucketIfNotExists(dbutils.AccountChangeSetBucket, false)
							if err != nil {
								return err
							}

							for i := range accChangeset {
								err = accCSBucket.Put(dbutils.EncodeTimestamp(i), accChangeset[i])
								if err != nil {
									return err
								}
							}
						}
						return nil
					})
					fmt.Println("Commit", ts, time.Now().Sub(commTime), time.Now().Sub(startTime))
					if err != nil {
						log.Fatal("err on update", err, ts)
					}
					accIndex = make(map[string]*ethdb.HistoryIndexBytes, 0)
					storageIndex = make(map[string]*ethdb.HistoryIndexBytes, 0)
					accChangeset = make(map[uint64][]byte, 0)
					storageChangeset = make(map[uint64][]byte, 0)
					numOfBatch=0
				} else {
					numOfBatch++
				}
				return nil
			})
			if err != nil && err != stopErr {
				log.Println(err)
				return err
			}

			return nil
		})
	*/
	if err != nil {
		log.Fatal(err)
	}

	//mut:=db.NewBatch()
	//numOfBlocksToCommit:=0
	//err = db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
	//	return true, nil
	//})
	//if err!=nil {
	//	log.Fatal(err)
	//}

}

func historyIndexCalculation4() {
	first := time.Now()

	var currentSize, expSize uint64
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}

	db2, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/storage_history_index4")
	if err != nil {
		log.Fatal(err)
	}
	i := 0
	m := make(map[string]*ethdb.HistoryIndexBytes)
	numOfBlocksToCommit := 0
	err = db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		ts, bucket := dbutils.DecodeTimestamp(k)
		fmt.Println(ts, string(bucket))
		if ts < 4738645 {
			return true, nil
		}
		switch {
		case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
			return true, nil
		case bytes.Equal(dbutils.StorageHistoryBucket, bucket):

			cs, err := dbutils.DecodeChangeSet(v)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}

			for _, v := range cs.Changes {
				index, ok := m[string(v.Key)]
				if !ok {
					indexBytes, err := db2.Get(dbutils.StorageHistoryBucket, v.Key)
					if err != nil && err != ethdb.ErrKeyNotFound {
						log.Fatal(err)
					}
					index = ethdb.WrapHistoryIndex(indexBytes)
				}
				index.Append(ts)
				m[string(v.Key)] = index
			}
		default:
			fmt.Println(string(k), "------------------------------")
		}
		i++
		if i%10000 == 0 {
			fmt.Println("time", time.Now().Sub(first))
		}
		if numOfBlocksToCommit > 100 {
			dbc := db2.NewBatch()
			numOfBlocksToCommit = 0
			for i := range m {
				err = dbc.Put(dbutils.StorageHistoryBucket, []byte(i), *m[i])
				if err != nil {
					log.Fatal(err)
				}
			}
			_, err := dbc.Commit()
			if err != nil {
				log.Fatal(err)
			}
			m = make(map[string]*ethdb.HistoryIndexBytes, 0)
		} else {
			numOfBlocksToCommit++
		}
		return true, nil
	})
	if err != nil {
		log.Println("err", err)
	}
	fmt.Println("Current size", currentSize)
	fmt.Println("Exp size", expSize)
}

func storageFormatDiff() {
	var currentSize, expSize uint64
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}

	fst, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/changesets_storage_diff_stats.txt")
	defer fst.Close()
	if err != nil {
		log.Fatal(err)
	}
	csvStorage := csv.NewWriter(fst)
	err = csvStorage.Write([]string{
		"block",
		"cs_size",
		"exp_cs_size",
		"changes",
		"len_of_vals",
		"num_of_removes",
		"numOfDuplicatedAddHashes",
		"numOfDuplicatedKeys",
	})
	if err != nil {
		log.Fatal(err)
	}

	err = db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		ts, bucket := dbutils.DecodeTimestamp(k)
		fmt.Println(ts, string(bucket))
		cs, err := dbutils.DecodeChangeSet(v)
		if err != nil {
			fmt.Println(ts, string(bucket), err)
			return false, err
		}

		switch {
		case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
			return true, nil
		case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
			currentSize += uint64(len(v))
			blockLength := strconv.Itoa(len(v))

			cs2 := &changeset.ChangeSet{
				Changes: make([]changeset.Change, len(cs.Changes)),
			}
			for i := range cs.Changes {
				cs2.Changes[i] = changeset.Change{
					Key:   cs.Changes[i].Key,
					Value: cs.Changes[i].Value,
				}
			}
			expCsEnc, err := changeset.EncodeStorage(cs2)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}

			newBlockLength := strconv.Itoa(len(expCsEnc))
			expSize += uint64(len(expCsEnc))
			numOfChangessByBlock := strconv.Itoa(len(cs.Changes))

			addrs := make(map[common.Hash]int, 0)
			keys := make(map[common.Hash]int, 0)
			lenOfVal := uint64(0)
			numOfEmptyElements := uint64(0)
			for _, v := range cs.Changes {
				addrHash := common.BytesToHash(v.Key[0:common.HashLength])
				key := common.BytesToHash(v.Key[common.HashLength+8 : 2*common.HashLength+8])
				addrs[addrHash] += 1
				keys[key] += 1
				if len(v.Value) == 0 {
					numOfEmptyElements++
				} else {
					lenOfVal += uint64(len(v.Value))
				}
			}
			numOfDuplicatedAddHashes := 0
			for _, v := range addrs {
				if v > 1 {
					numOfDuplicatedAddHashes += v
				}
			}
			numOfDuplicatedKeys := 0
			for _, v := range keys {
				if v > 1 {
					numOfDuplicatedKeys += v
				}
			}
			var avgLenOfVal uint64
			numOfNoneZeroElements := (uint64(len(cs.Changes)) - numOfEmptyElements)
			if numOfNoneZeroElements > 0 {
				avgLenOfVal = lenOfVal / numOfNoneZeroElements
			}

			err = csvStorage.Write([]string{
				strconv.FormatUint(ts, 10),
				blockLength,
				newBlockLength,
				numOfChangessByBlock,
				strconv.FormatUint(avgLenOfVal, 10),
				strconv.FormatUint(numOfEmptyElements, 10),
				strconv.Itoa(numOfDuplicatedAddHashes),
				strconv.Itoa(numOfDuplicatedKeys),
			})

		default:
			fmt.Println(string(k), "------------------------------")
		}
		return true, nil
	})
	if err != nil {
		log.Println("err", err)
	}
	fmt.Println("Current size", currentSize)
	fmt.Println("Exp size", expSize)

}
func storageFormatDiff2() {
	var currentSize, expSize, expSizeDict, expSizeDictNew uint64
	var expErrors, expDictErrors uint64
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}

	fst, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/changesets_storage_encode_size.txt")
	defer fst.Close()
	if err != nil {
		log.Fatal(err)
	}

	csvStorage := csv.NewWriter(fst)
	err = csvStorage.Write([]string{
		"block",
		"cs_size",
		"exp_cs_size",
		"dict_size",
		"new_size",
	})



	err = db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		ts, bucket := dbutils.DecodeTimestamp(k)
		fmt.Println(ts, string(bucket))

		switch {
		case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
			return true, nil
		case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
			cs, err := dbutils.DecodeChangeSet(v)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}

			cs2 := &changeset.ChangeSet{
				Changes: make([]changeset.Change, len(cs.Changes)),
			}
			for i := range cs.Changes {
				cs2.Changes[i] = changeset.Change{
					Key:   cs.Changes[i].Key,
					Value: cs.Changes[i].Value,
				}
			}
			expCsEnc2, err := changeset.EncodeStorageDict(cs2)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}

			csTestDict, err := changeset.DecodeStorageDict(expCsEnc2)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}
			if reflect.DeepEqual(csTestDict, cs) {
				fmt.Println("DICT not equal", ts)
				expDictErrors++
			}
			expCsEncDictNew, err := changeset.EncodeStorageDict2(cs2)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}

			expCsEnc, err := changeset.EncodeStorage(cs2)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}
			csTest, err := changeset.DecodeStorage(expCsEnc)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}
			if reflect.DeepEqual(csTest, cs) {
				fmt.Println("EXP not equal", ts)
				expErrors++
			}

			currentSize += uint64(len(v))
			expSize += uint64(len(expCsEnc))
			expSizeDict += uint64(len(expCsEnc2))
			expSizeDictNew += uint64(len(expCsEncDictNew))
			csvStorage.Write([]string{
				strconv.FormatUint(ts,10),
				strconv.Itoa(len(v)),
				strconv.Itoa(len(expCsEnc)),
				strconv.Itoa(len(expCsEnc2)),
				strconv.Itoa(len(expCsEncDictNew)),
			})

		default:
			fmt.Println(string(k), "------------------------------")
		}
		return true, nil
	})
	if err != nil {
		log.Println("err", err)
	}
	fmt.Println("Current size", currentSize)
	fmt.Println("Exp size", expSize)
	fmt.Println("Exp errors", expErrors)
	fmt.Println("Dict size", expSizeDict)
	fmt.Println("Dict errors", expDictErrors)

}

func collectChangesetCsv() {
	var contractsLength, addressesLength uint64
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}
	fac, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/changesets_acc_stats.txt")
	defer fac.Close()
	if err != nil {
		log.Fatal(err)
	}
	csvAcc := csv.NewWriter(fac)

	fst, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/changesets_storage_stats.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer fst.Close()
	csvStorage := csv.NewWriter(fst)
	db2, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/stats")
	if err != nil {
		log.Fatal(err)
	}
	i := uint64(0)
	db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		ts, bucket := dbutils.DecodeTimestamp(k)
		fmt.Println(ts, string(bucket))
		cs, err := dbutils.DecodeChangeSet(v)
		if err != nil {
			fmt.Println(ts, string(bucket), err)
			return false, err
		}
		switch {
		case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
			addressesLength += uint64(len(v))
			blockLength := strconv.Itoa(len(v))
			numOfChangessByBlock := strconv.Itoa(len(cs.Changes))
			allAccLen := 0
			for _, v := range cs.Changes {
				addr := common.BytesToHash(v.Key)
				b, err := db2.Get(dbutils.AccountsBucket, addr.Bytes())
				if err != nil && err != ethdb.ErrKeyNotFound {
					log.Fatal(err)
				}
				var count uint64
				if len(b) == 0 {
					b = make([]byte, 8)
				} else {
					count = binary.LittleEndian.Uint64(b)
				}
				binary.LittleEndian.PutUint64(b, count+1)
				allAccLen += len(v.Value)
			}
			avgAccLen := allAccLen / len(cs.Changes)
			err = csvAcc.Write([]string{
				strconv.FormatUint(ts, 10),
				blockLength,
				numOfChangessByBlock,
				strconv.Itoa(avgAccLen),
			})
			if err != nil {
				log.Fatal(err)
			}
		case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
			contractsLength += uint64(len(v))
			blockLength := strconv.Itoa(len(v))
			numOfChangessByBlock := strconv.Itoa(len(cs.Changes))

			addrs := make(map[common.Hash]int, 0)
			keys := make(map[common.Hash]int, 0)
			lenOfVal := uint64(0)
			for _, v := range cs.Changes {
				addrHash := common.BytesToHash(v.Key[0:common.HashLength])
				key := common.BytesToHash(v.Key[common.HashLength+8 : 2*common.HashLength+8])
				addrs[addrHash] += 1
				keys[key] += 1

				b, err := db2.Get(dbutils.StorageBucket, addrHash.Bytes())
				if err != nil && err != ethdb.ErrKeyNotFound {
					log.Fatal(err)
				}
				var count uint64
				if len(b) == 0 {
					b = make([]byte, 8)
				} else {
					count = binary.LittleEndian.Uint64(b)
				}
				binary.LittleEndian.PutUint64(b, count+1)

				b, err = db2.Get(dbutils.StorageHistoryBucket, key.Bytes())
				if err != nil && err != ethdb.ErrKeyNotFound {
					log.Fatal(err)
				}
				count = 0
				if len(b) == 0 {
					b = make([]byte, 8)
				} else {
					count = binary.LittleEndian.Uint64(b)
				}
				binary.LittleEndian.PutUint64(b, count+1)
				lenOfVal += uint64(len(v.Value))
			}
			numOfDuplicatedAddHashes := 0
			for _, v := range addrs {
				if v > 1 {
					numOfDuplicatedAddHashes += v
				}
			}
			numOfDuplicatedKeys := 0
			for _, v := range keys {
				if v > 1 {
					numOfDuplicatedKeys += v
				}
			}

			avgLenOfVal := lenOfVal / uint64(len(cs.Changes))
			err = csvStorage.Write([]string{
				strconv.FormatUint(ts, 10),
				blockLength,
				numOfChangessByBlock,
				strconv.FormatUint(avgLenOfVal, 10),
				strconv.Itoa(numOfDuplicatedAddHashes),
				strconv.Itoa(numOfDuplicatedKeys),
			})

		default:
			fmt.Println(string(k), "------------------------------")
		}
		i++
		return true, nil
	})
	if err != nil {
		log.Println("err", err)
	}
	fmt.Println("HAT len", addressesLength)
	fmt.Println("HST len", contractsLength)

}

func collectStorageNumOfDuplicate() {
	var storageChangesetLength, addressesLength uint64
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}

	fst, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/changesets_storage_stats.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer fst.Close()
	csvStorage := csv.NewWriter(fst)
	csvStorage.Write([]string{
		"blocknum",
		"blockLength",
		"numOfChangesByBlock",
		"numFrom2to3OfDuplicatedAddHashes",
		"numFrom4to5OfDuplicatedAddHashes",
		"num5to10OfDuplicatedAddHashes",
		"num10to20OfDuplicatedAddHashes",
		"num20to50OfDuplicatedAddHashes",
		"numMore50OfDuplicatedAddHashes",
		"numFrom2to3OfDuplicatedKeys",
		"numFrom4to5OfDuplicatedKeys",
		"num5to10OfDuplicatedKeys",
		"num10to20OfDuplicatedKeys",
		"num20to50OfDuplicatedKeys",
		"numMore50OfDuplicatedKeys",
		"uniqueaddrhashes",
		"uniquekeys",
		"avgLenOfVal",
	})

	i := uint64(0)
	db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		ts, bucket := dbutils.DecodeTimestamp(k)
		fmt.Println(ts, string(bucket))
		switch {
		case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
			return true, nil
		case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
			cs, err := dbutils.DecodeChangeSet(v)
			if err != nil {
				fmt.Println(ts, string(bucket), err)
				return false, err
			}

			storageChangesetLength += uint64(len(v))
			blockLength := strconv.Itoa(len(v))
			numOfChangessByBlock := strconv.Itoa(len(cs.Changes))

			addrs := make(map[common.Hash]int, 0)
			keys := make(map[common.Hash]int, 0)
			lenOfVal := uint64(0)
			numOfNoneEmpty := 0
			for _, v := range cs.Changes {
				addrHash := common.BytesToHash(v.Key[0:common.HashLength])
				key := common.BytesToHash(v.Key[common.HashLength+8 : 2*common.HashLength+8])
				addrs[addrHash] += 1
				keys[key] += 1
				if len(v.Value) > 0 {
					numOfNoneEmpty++
					lenOfVal += uint64(len(v.Value))
				}
			}

			numFrom2to3OfDuplicatedAddHashes := 0
			numFrom4to5OfDuplicatedAddHashes := 0
			num5to10OfDuplicatedAddHashes := 0
			num10to20OfDuplicatedAddHashes := 0
			num20to50OfDuplicatedAddHashes := 0
			numMore50OfDuplicatedAddHashes := 0
			//numMore250OfDuplicatedAddHashes := 0
			for _, v := range addrs {
				switch {
				case v > 1 && v <= 3:
					numFrom2to3OfDuplicatedAddHashes++
				case v > 3 && v <= 5:
					numFrom4to5OfDuplicatedAddHashes++
				case v > 5 && v <= 10:
					num5to10OfDuplicatedAddHashes++
				case v > 10 && v <= 20:
					num10to20OfDuplicatedAddHashes++
				case v > 20 && v <= 50:
					num20to50OfDuplicatedAddHashes++
				case v > 50 && v < 250:
					numMore50OfDuplicatedAddHashes++
				}
			}
			numFrom2to3OfDuplicatedKeys := 0
			numFrom4to5OfDuplicatedKeys := 0
			num5to10OfDuplicatedKeys := 0
			num10to20OfDuplicatedKeys := 0
			num20to50OfDuplicatedKeys := 0
			numMore50OfDuplicatedKeys := 0
			for _, v := range keys {
				switch {
				case v > 1 && v <= 3:
					numFrom2to3OfDuplicatedKeys++
				case v > 3 && v <= 5:
					numFrom4to5OfDuplicatedKeys++
				case v > 5 && v <= 10:
					num5to10OfDuplicatedKeys++
				case v > 10 && v <= 20:
					num10to20OfDuplicatedKeys++
				case v > 20 && v <= 50:
					num20to50OfDuplicatedKeys++
				case v > 50:
					numMore50OfDuplicatedKeys++
				}
			}

			avgLenOfVal := uint64(0)
			if numOfNoneEmpty > 0 {
				avgLenOfVal = lenOfVal / uint64(numOfNoneEmpty)
			}

			err = csvStorage.Write([]string{
				strconv.FormatUint(ts, 10),
				blockLength,
				numOfChangessByBlock,
				strconv.Itoa(numFrom2to3OfDuplicatedAddHashes),
				strconv.Itoa(numFrom4to5OfDuplicatedAddHashes),
				strconv.Itoa(num5to10OfDuplicatedAddHashes),
				strconv.Itoa(num10to20OfDuplicatedAddHashes),
				strconv.Itoa(num20to50OfDuplicatedAddHashes),
				strconv.Itoa(numMore50OfDuplicatedAddHashes),
				strconv.Itoa(numFrom2to3OfDuplicatedKeys),
				strconv.Itoa(numFrom4to5OfDuplicatedKeys),
				strconv.Itoa(num5to10OfDuplicatedKeys),
				strconv.Itoa(num10to20OfDuplicatedKeys),
				strconv.Itoa(num20to50OfDuplicatedKeys),
				strconv.Itoa(numMore50OfDuplicatedKeys),
				strconv.Itoa(len(addrs)),
				strconv.Itoa(len(keys)),
				strconv.FormatUint(avgLenOfVal, 10),
			})

		default:
			fmt.Println(string(k), "------------------------------")
		}
		i++
		return true, nil
	})
	if err != nil {
		log.Println("err", err)
	}
	fmt.Println("HAT len", addressesLength)
	fmt.Println("HST len", storageChangesetLength)

}

/*
Current size 75989100632
Exp size 65702267087
Exp errors 0
Dict size 56629648083
Dict errors 0

*/
