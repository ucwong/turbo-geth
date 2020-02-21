package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ugorji/go/codec"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"
)

func main() {
	//testMigrate()
	//migrateAccountIndexes()
	//migrateStorageIndexes()
	storageFormatDiff3()
	//migragteCompressionOfBlocks()
	//copyCodeContracts()
	//checkCompressionOfBlocks()
	//testMigrate()
	//storageFormatDiff2()
	//collectStorageNumOfDuplicate()
}

func testMigrate() {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}


	numOfBatch := 0
	//accChangeset := make(map[uint64][]byte, 0)
	//storageChangeset := make(map[uint64][]byte, 0)

	var k, v []byte
	var done bool
	for !done {
		numOfBatch = 0
		//accIndex = make(map[string]*ethdb.HistoryIndexBytes, 0)
		//storageIndex = make(map[string]*ethdb.HistoryIndexBytes, 0)

		err := db.DB().Update(func(tx *bolt.Tx) error {
			var csBucket *bolt.Bucket = tx.Bucket(dbutils.ChangeSetBucket)
			accIndexBucket, err := tx.CreateBucketIfNotExists(dbutils.AccountsHistoryIndexBucket, false)
			if err != nil {
				return err
			}
			storageIndexBucket, err := tx.CreateBucketIfNotExists(dbutils.StorageHistoryIndexBucket, false)
			if err != nil {
				return err
			}


			accIndex := make(map[string]*ethdb.HistoryIndexBytes, 0)
			storageIndex := make(map[string]*ethdb.HistoryIndexBytes, 0)

			cursor := csBucket.Cursor()

			if k == nil {
				fmt.Println("first")
				k, v = cursor.First()
			} else {
				fmt.Println("Seek")
				k, v = cursor.Seek(k)
				k, v = cursor.Next()
			}
			fmt.Println("for",)

			for ; k != nil;  {

				ts, bucket := dbutils.DecodeTimestamp(k)
				fmt.Println(ts, string(bucket))

				cs, err := dbutils.DecodeChangeSet(v)
				if err != nil {
					fmt.Println(ts, string(bucket), err)
					return err
				}
				fmt.Println("switch")
				switch {
				case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
					for _, v := range cs.Changes {
						index, ok := accIndex[string(v.Key)]
						if !ok {
							indexBytes, _ := db.Get(dbutils.AccountsHistoryIndexBucket, v.Key)
							index = ethdb.WrapHistoryIndex(indexBytes)
						}
						index.Append(ts)
						accIndex[string(v.Key)] = index
					}
					//vCopy := make([]byte, len(v))
					//copy(vCopy, v)
					//accChangeset[ts] = vCopy
				case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
					//cs2 := &changeset.ChangeSet{
					//	Changes: make([]changeset.Change, len(cs.Changes)),
					//}
					for _, v := range cs.Changes {
						//cs2.Changes[i] = changeset.Change{
						//	Key:   cs.Changes[i].Key,
						//	Value: cs.Changes[i].Value,
						//}

						//fill storage index
						index, ok := storageIndex[string(v.Key)]
						if !ok {
							indexBytes,_:=db.Get(dbutils.StorageHistoryIndexBucket, v.Key)
							//indexBytes, _ := storageIndexBucket.Get(v.Key)
							index = ethdb.WrapHistoryIndex(indexBytes)
						}
						index.Append(ts)
						storageIndex[string(v.Key)] = index
					}

					//expCsEnc2, err := changeset.EncodeStorageDict(cs2)
					//if err != nil {
					//	fmt.Println(ts, string(bucket), err)
					//	return err
					//}
					//storageChangeset[ts] = expCsEnc2

				default:
					fmt.Println(string(k), "------------------------------")
				}
				fmt.Println("next")

				k, v = cursor.Next()
				if numOfBatch > 10000 || k==nil {
					commTime := time.Now()

					fmt.Println("Start update")
					if len(accIndex) > 0 {
						tuples:=make([][]byte, 0, len(accIndex))
						for i := range accIndex {
							tuples=append(tuples, []byte(i),*accIndex[i])
							//err = accIndexBucket.Put([]byte(i),*accIndex[i])
							//if err!=nil {
							//	return err
							//}
						}
						err=accIndexBucket.MultiPut(tuples...)
						if err != nil {
							return err
						}
					}
					if len(storageIndex) > 0 {
						tuples:=make([][]byte, 0, len(storageIndex))
						for i := range storageIndex {
							//err = storageIndexBucket.Put([]byte(i), *storageIndex[i])
							//if err != nil {
							//	return err
							//}
							tuples=append(tuples, []byte(i), *storageIndex[i])
						}
						err = storageIndexBucket.MultiPut(tuples...)
						if err != nil {
							return err
						}

						//for i := range storageIndex {
						//	err = storageIndexBucket.Put([]byte(i), *storageIndex[i])
						//	if err != nil {
						//		return err
						//	}
						//}
					}

					//if len(storageChangeset) > 0 {
					//	storageCSBucket, err := tx.CreateBucketIfNotExists(dbutils.StorageChangeSetBucket, false)
					//	if err != nil {
					//		return err
					//	}
					//
					//	for i := range storageChangeset {
					//		err = storageCSBucket.Put(dbutils.EncodeTimestamp(i), storageChangeset[i])
					//		if err != nil {
					//			return err
					//		}
					//	}
					//}
					//
					//if len(accChangeset) > 0 {
					//	accCSBucket, err := tx.CreateBucketIfNotExists(dbutils.AccountChangeSetBucket, false)
					//	if err != nil {
					//		return err
					//	}
					//
					//	for i := range accChangeset {
					//		err = accCSBucket.Put(dbutils.EncodeTimestamp(i), accChangeset[i])
					//		if err != nil {
					//			return err
					//		}
					//	}
 					//}
					fmt.Println("Commit", ts, time.Now().Sub(commTime), time.Now().Sub(startTime))
					//if err != nil {
					//	log.Fatal("err on update", err, ts)
					//}
					//accChangeset = make(map[uint64][]byte, 0)
					//storageChangeset = make(map[uint64][]byte, 0)
					break
				} else {
					numOfBatch++
				}
				fmt.Println("++")
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
func storageFormatDiff3() {
	var currentSize, expSizeDict uint64
	var  expDictErrors uint64
	db, err := ethdb.NewBoltDatabase("/home/b00ris/chaindata")
	if err != nil {
		log.Fatal(err)
	}
	mp:=make(map[uint64]uint64,0)
	defer spew.Dump(mp)
	errs:=0
	err = db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		ts, bucket := dbutils.DecodeTimestamp(k)
		fmt.Println(ts, string(bucket), errs)

		switch {
		case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
			return true, nil
		case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
			cs, err := dbutils.DecodeChangeSet(v)
			if err != nil {
				fmt.Println(ts, "dbutils.DecodeChangeSet", string(bucket), err)
				errs++
				return false, err
			}

			cs2 := &changeset.ChangeSet{
				Changes: make([]changeset.Change, len(cs.Changes)),
			}
			for i := range cs.Changes {
				mp[binary.LittleEndian.Uint64(cs.Changes[i].Key[common.HashLength:common.HashLength+common.IncarnationLength])]++
				cs2.Changes[i] = changeset.Change{
					Key:   cs.Changes[i].Key,
					Value: cs.Changes[i].Value,
				}
			}
			encDict, err := changeset.EncodeStorageDict3(cs2)
			if err != nil {
				fmt.Println(ts, "EncodeStorageDict3", string(bucket), err)
				return false, err
			}

			csTestDict, err := changeset.DecodeStorageDict3(encDict)
			if err != nil {
				fmt.Println(v)
				fmt.Println(ts, "DecodeStorageDict3", string(bucket), err)
				return false, err
			}
			if reflect.DeepEqual(csTestDict, cs) {
				fmt.Println("DICT not equal", ts)
				expDictErrors++
			}

			currentSize += uint64(len(v))
			expSizeDict += uint64(len(encDict))
		default:
			fmt.Println(string(k), "------------------------------")
		}
		return true, nil
	})
	if err != nil {
		log.Println("err", err)
	}
	fmt.Println("Current size", currentSize)
	fmt.Println("Dict size", expSizeDict)
	fmt.Println("Dict errors", expDictErrors)
}
func calculateSizeOfAccounts() {
	var currentSize, expSizeDict uint64
	var  expDictErrors uint64
	db, err := ethdb.NewBoltDatabase("/home/b00ris/	chaindata")
	if err != nil {
		log.Fatal(err)
	}
	mp:=make(map[uint64]uint64,0)
	defer spew.Dump(mp)
	errs:=0
	err = db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		ts, bucket := dbutils.DecodeTimestamp(k)
		fmt.Println(ts, string(bucket), errs)

		switch {
		case bytes.Equal(dbutils.AccountsHistoryBucket, bucket):
			return true, nil
		case bytes.Equal(dbutils.StorageHistoryBucket, bucket):
			cs, err := dbutils.DecodeChangeSet(v)
			if err != nil {
				fmt.Println(ts, "dbutils.DecodeChangeSet", string(bucket), err)
				errs++
				return false, err
			}

			cs2 := &changeset.ChangeSet{
				Changes: make([]changeset.Change, len(cs.Changes)),
			}
			for i := range cs.Changes {
				mp[binary.LittleEndian.Uint64(cs.Changes[i].Key[common.HashLength:common.HashLength+common.IncarnationLength])]++
				cs2.Changes[i] = changeset.Change{
					Key:   cs.Changes[i].Key,
					Value: cs.Changes[i].Value,
				}
			}
			encDict, err := changeset.EncodeStorageDict3(cs2)
			if err != nil {
				fmt.Println(ts, "EncodeStorageDict3", string(bucket), err)
				return false, err
			}

			csTestDict, err := changeset.DecodeStorageDict3(encDict)
			if err != nil {
				fmt.Println(v)
				fmt.Println(ts, "DecodeStorageDict3", string(bucket), err)
				return false, err
			}
			if reflect.DeepEqual(csTestDict, cs) {
				fmt.Println("DICT not equal", ts)
				expDictErrors++
			}

			currentSize += uint64(len(v))
			expSizeDict += uint64(len(encDict))
		default:
			fmt.Println(string(k), "------------------------------")
		}
		return true, nil
	})
	if err != nil {
		log.Println("err", err)
	}
	fmt.Println("Current size", currentSize)
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

func checkCompressionOfBlocks()  {
		var rlpSize, gzipRlpSize, gzipRlp2Size uint64
		db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
		if err != nil {
			log.Fatal(err)
		}

		fst, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/blocks_encode_size.csv")
		defer fst.Close()
		if err != nil {
			log.Fatal(err)
		}

		csvStorage := csv.NewWriter(fst)
		err = csvStorage.Write([]string{
			"block",
			"rlp",
			"gzip_rlp",
			"gzip_rlp2",
		})


		var handle codec.CborHandle
		handle.WriterBufferSize = 64 * 1024
		err = db.Walk(dbutils.BlockBodyPrefix, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			block:=binary.BigEndian.Uint64(k[0:8])
			fmt.Println(block)

			rlpLen:=len(v)

			var rlpBuf bytes.Buffer
			zrlp := gzip.NewWriter(&rlpBuf)
			_, err = zrlp.Write(v)
			if err!=nil {
				return false, err
			}
			zrlp.Close()
			gzRlpLen:=len(rlpBuf.Bytes())

			var rlpBuf2 bytes.Buffer
			zrlp2,_ := gzip.NewWriterLevel(&rlpBuf2,1)
			_, err = zrlp2.Write(v)
			if err!=nil {
				return false, err
			}
			zrlp2.Close()
			gzRlpLen2:=len(rlpBuf2.Bytes())




			rlpSize+=uint64(rlpLen)
			gzipRlpSize+=uint64(gzRlpLen)
			gzipRlp2Size+=uint64(gzRlpLen2)
			fmt.Println("stats", rlpSize, gzipRlpSize, gzipRlp2Size)


			err = csvStorage.Write([]string{
				strconv.FormatUint(binary.BigEndian.Uint64(k[0:8]), 10),
				strconv.Itoa(rlpLen),
				strconv.Itoa(gzRlpLen),
				strconv.Itoa(gzRlpLen2),

			})
			if err!=nil {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			log.Println("err", err)
		}
		fmt.Println("rlp size      ", rlpSize)
		fmt.Println("gzip rlp size ", gzipRlpSize)
		fmt.Println("gzip rlp2 size", gzipRlp2Size)
}

func migragteCompressionOfBlocks()  {
		db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
		if err != nil {
			log.Fatal(err)
		}

		i:=0
		batchSize:=1000
	ts:=time.Now()
	fmt.Println("storage index migration")
	tuples:=make([][]byte, 0, 11000)
	numOfBatch := 0

	j:=uint64(0)
	var k, v []byte
	var done bool
	for !done {
		numOfBatch = 0
		err := db.DB().Update(func(tx *bolt.Tx) error {
			var blocksBucket *bolt.Bucket = tx.Bucket(dbutils.BlockBodyPrefix)
			//var blocksBucket2 *bolt.Bucket = tx.Bucket(dbutils.BlockBodyPrefixCompressed)
			c := blocksBucket.Cursor()

			if k == nil {
				k, v = c.First()
			} else {
				k, v = c.Seek(k)
				k, v = c.Next()
			}


			for ; k != nil; k, v = c.Next() {
				j++
				block:=binary.BigEndian.Uint64(k[0:8])
				fmt.Println(block)

				var rlpBuf bytes.Buffer
				zrlp := gzip.NewWriter(&rlpBuf)
				_, err = zrlp.Write(v)
				if err!=nil {
					log.Fatal(err)
				}
				zrlp.Close()

				tuples=append(tuples, k, rlpBuf.Bytes())

				if numOfBatch > 10000 {
					commTime := time.Now()

					fmt.Println("Start update")
					if len(tuples) > 0 {
						//err = blocksBucket2.MultiPut(tuples...)
						//if err!=nil {
						//	log.Fatal(err, k)
						//}
						tuples=make([][]byte, 0,11000)
					}
					fmt.Println("Commit", j, time.Now().Sub(commTime), time.Now().Sub(ts))
					break
				} else {
					numOfBatch++
				}

			}

			if k == nil {
				done = true
				if len(tuples)>0 {
					err = blocksBucket.MultiPut(tuples...)
					if err!=nil {
						log.Fatal(err, k)
					}
				}
			}
			k = common.CopyBytes(k)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Storage migration success")

	err = db.Walk(dbutils.BlockBodyPrefix, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			block:=binary.BigEndian.Uint64(k[0:8])
			fmt.Println(block)

			var rlpBuf bytes.Buffer
			zrlp := gzip.NewWriter(&rlpBuf)
			_, err = zrlp.Write(v)
			if err!=nil {
				return false, err
			}
			zrlp.Close()

			tuples = append(tuples, dbutils.BlockBodyPrefixCompressed, k, rlpBuf.Bytes())

			if i>batchSize {
				_,err:=db.MultiPut(tuples...)
				if err!=nil {
					return false, err
				}
				i=0
				tuples=make([][]byte,0, 3500)
			} else {
				i++
			}
			return true, nil
		})
		if err != nil {
			log.Println("err", err)
		}
		if len(tuples)>0 {
			_,err:=db.MultiPut(tuples...)
			if err!=nil {
				log.Println(err)
			}

		}
}

func migrateAccountIndexes()  {
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}
	ts:=time.Now()
	fmt.Println("AccountsHistoryIndexBucket migration")
	tuples:=make([][]byte, 0, 11000)



	numOfBatch := 0

	j:=uint64(0)
	var k, v []byte
	var done bool
	for !done {
		numOfBatch = 0
		err := db.DB().Update(func(tx *bolt.Tx) error {
			var accIndex *bolt.Bucket = tx.Bucket(dbutils.AccountsHistoryIndexBucket)
			c := accIndex.Cursor()

			if k == nil {
				k, v = c.First()
			} else {
				k, v = c.Seek(k)
				k, v = c.Next()
			}

			decodeWithoutPanic:= func(v []byte) (res []byte){
				defer func() {
					err:=recover()
					if err!=nil {
						res = v
					}
				}()

				vals,err:=ethdb.WrapHistoryIndex(v).Decode()
				if err!=nil {
					panic(err)
				}
				hi:=dbutils.NewHistoryIndex()
				for i:=range vals {
					hi.Append(vals[i])
				}

				return *hi
			}

			for ; k != nil; k, v = c.Next() {
				j++
				nw:=decodeWithoutPanic(v)
				tuples=append(tuples, k, nw)

				if numOfBatch > 10000 {
					commTime := time.Now()

					fmt.Println("Start update")
					if len(tuples) > 0 {
						err = accIndex.MultiPut(tuples...)
						if err!=nil {
							log.Fatal(err, k)
						}
						tuples=make([][]byte, 0, 11000)
					}
					fmt.Println("Commit", j, time.Now().Sub(commTime), time.Now().Sub(ts))
					if err != nil {
						log.Fatal("err on update", err, ts)
					}
					break
				} else {
					numOfBatch++
				}

			}

			if k == nil {
				done = true
				if len(tuples)>0 {
					err = accIndex.MultiPut(tuples...)
					if err!=nil {
						log.Fatal(err, k)
					}
				}
			}
			k = common.CopyBytes(k)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Account migration success")
}


func migrateStorageIndexes()  {
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}
	ts:=time.Now()
	fmt.Println("storage index migration")
	tuples:=make([][]byte, 0, 11000)



	numOfBatch := 0

	j:=uint64(0)
	var k, v []byte
	var done bool
	for !done {
		numOfBatch = 0
		err := db.DB().Update(func(tx *bolt.Tx) error {
			var storageIndex *bolt.Bucket = tx.Bucket(dbutils.StorageHistoryIndexBucket)
			c := storageIndex.Cursor()

			if k == nil {
				k, v = c.First()
			} else {
				k, v = c.Seek(k)
				k, v = c.Next()
			}

			decodeWithoutPanic:= func(v []byte) (res []byte){
				defer func() {
					err:=recover()
					if err!=nil {
						res = v
					}
				}()

				vals,err:=ethdb.WrapHistoryIndex(v).Decode()
				if err!=nil {
					panic(err)
				}
				hi:=dbutils.NewHistoryIndex()
				for i:=range vals {
					hi.Append(vals[i])
				}

				return *hi
			}

			for ; k != nil; k, v = c.Next() {
				j++
				nw:=decodeWithoutPanic(v)
				tuples=append(tuples, k, nw)

				if numOfBatch > 10000 {
					commTime := time.Now()

					fmt.Println("Start update")
					if len(tuples) > 0 {
						err = storageIndex.MultiPut(tuples...)
						if err!=nil {
							log.Fatal(err, k)
						}
						tuples=make([][]byte, 0,11000)
					}
					fmt.Println("Commit", j, time.Now().Sub(commTime), time.Now().Sub(ts))
					break
				} else {
					numOfBatch++
				}

			}

			if k == nil {
				done = true
				if len(tuples)>0 {
					err = storageIndex.MultiPut(tuples...)
					if err!=nil {
						log.Fatal(err, k)
					}
				}
			}
			k = common.CopyBytes(k)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Storage migration success")

}


func copyCodeContracts()  {
		db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
		if err != nil {
			log.Fatal(err)
		}


		db2, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/contract_codes")
		if err != nil {
			log.Fatal(err)
		}

		err = db.Walk(dbutils.CodeBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			err:=db2.Put(dbutils.CodeBucket, k,v)
			if err!=nil {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			log.Println("err", err)
		}
}

/*

Current size 75989100632
Dict size 43257146833
Dict errors 0
(map[uint64]uint64) (len=2) {
 (uint64) 18374686479671623679: (uint64) 935166687,
 (uint64) 18302628885633695743: (uint64) 12
}

Current size 75989100632
Exp size 65702267087
Exp errors 0
Dict size 56629648083
Dict errors 0



rlp size       126317329945
gzip rlp size  84384714811
gzip rlp2 size 88306259484

*/
