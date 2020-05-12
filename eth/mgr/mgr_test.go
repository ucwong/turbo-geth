package mgr_test

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/eth/mgr"
	"github.com/stretchr/testify/require"
)

func TestScheduleProperties(t *testing.T) {
	require := require.New(t)
	stateSize := uint64(123456)
	block := uint64(11)

	var prevEpoch mgr.Tick
	var prevSlice mgr.StateSizeSlice

	var sizeFromSlicesAccumulator uint64
	var sizeFromEpochsAccumulator uint64
	schedule := mgr.NewStateSchedule(stateSize, block, block+mgr.BlocksPerCycle+100)
	for i := range schedule.Ticks {
		epoch := schedule.Ticks[i]
		for j := range epoch.StateSizeSlices {
			ss := epoch.StateSizeSlices[j]
			sizeFromSlicesAccumulator += ss.ToSize - ss.FromSize

			// props
			if prevEpoch.Number < epoch.Number { // because epochs are cycled
				require.Less(ss.FromSize, ss.ToSize)
				require.LessOrEqual(prevSlice.ToSize, ss.FromSize)
			}

			prevSlice = ss
		}
		sizeFromEpochsAccumulator += epoch.ToSize - epoch.FromSize

		// props
		if prevEpoch.Number < epoch.Number {
			require.LessOrEqual(block, epoch.FromBlock)
			require.Less(epoch.FromBlock, epoch.ToBlock)
			require.Less(prevEpoch.ToBlock, epoch.FromBlock)

			require.LessOrEqual(epoch.ToSize, stateSize)
			require.Less(epoch.FromSize, epoch.ToSize)
			require.Less(prevEpoch.ToSize, epoch.FromSize)
		}

		prevEpoch = epoch

	}

}
