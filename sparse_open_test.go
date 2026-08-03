package bbolt

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/aperturerobotics/bbolt/internal/common"
	"github.com/aperturerobotics/bbolt/internal/freelist"
)

const (
	sparseLogicalBytesEnv      = "BBOLT_SPARSE_LOGICAL_BYTES"
	sparseFreePagesEnv         = "BBOLT_SPARSE_FREE_PAGES"
	sparseFreeStrideEnv        = "BBOLT_SPARSE_FREE_STRIDE"
	sparseFreelistTypeEnv      = "BBOLT_SPARSE_FREELIST_TYPE"
	sparseExpectedLogicalEnv   = "BBOLT_SPARSE_EXPECTED_LOGICAL_BYTES"
	sparseExpectedPageSizeEnv  = "BBOLT_SPARSE_EXPECTED_PAGE_SIZE"
	sparseExpectedFreePagesEnv = "BBOLT_SPARSE_EXPECTED_FREE_PAGES"
	sparseRoleEnv              = "BBOLT_SPARSE_OPEN_ROLE"
	sparsePathEnv              = "BBOLT_SPARSE_OPEN_PATH"
	sparseRoleChild            = "child"
)

var (
	sparseBucket = []byte("sparse-open-bucket")
	sparseKey    = []byte("sparse-open-key")
	sparseValue  = []byte("sparse-open-value")
)

// TestSparseFreelistFreshProcessOpen measures opening a synthesized sparse
// database with a synced freelist. It is deliberately opt-in because the
// requested logical sizes are ordinarily 100 GB or larger.
func TestSparseFreelistFreshProcessOpen(t *testing.T) {
	if os.Getenv(sparseRoleEnv) == sparseRoleChild {
		sparseFreelistChild(t)
		return
	}

	logicalText := os.Getenv(sparseLogicalBytesEnv)
	freeText := os.Getenv(sparseFreePagesEnv)
	if logicalText == "" || freeText == "" {
		t.Skip("set BBOLT_SPARSE_LOGICAL_BYTES and BBOLT_SPARSE_FREE_PAGES to run")
	}
	if runtime.GOOS != "darwin" {
		t.Skip("sparse allocation validation is implemented only on Darwin")
	}
	logicalBytes, err := strconv.ParseUint(logicalText, 10, 64)
	if err != nil || logicalBytes == 0 || logicalBytes > math.MaxInt64 {
		t.Fatalf("invalid %s=%q", sparseLogicalBytesEnv, logicalText)
	}
	requestedFreePages, err := strconv.ParseUint(freeText, 10, 64)
	if err != nil || requestedFreePages > math.MaxInt64 {
		t.Fatalf("invalid %s=%q", sparseFreePagesEnv, freeText)
	}
	freeStride := uint64(1)
	if strideText := os.Getenv(sparseFreeStrideEnv); strideText != "" {
		freeStride, err = strconv.ParseUint(strideText, 10, 64)
		if err != nil || freeStride == 0 || freeStride > math.MaxInt64 {
			t.Fatalf("invalid %s=%q", sparseFreeStrideEnv, strideText)
		}
	}
	freelistType := FreelistMapType
	switch typeText := os.Getenv(sparseFreelistTypeEnv); typeText {
	case "", string(FreelistMapType):
	case string(FreelistArrayType):
		freelistType = FreelistArrayType
	default:
		t.Fatalf("invalid %s=%q", sparseFreelistTypeEnv, typeText)
	}

	path := filepath.Join(t.TempDir(), "sparse-open.db")
	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(sparseBucket)
		if err != nil {
			return err
		}
		return bucket.Put(sparseKey, sparseValue)
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	activeMeta := *db.meta()
	pageSizeBytes := db.pageSize
	pageSize := uint64(pageSizeBytes)
	root := *activeMeta.RootBucket()
	oldHWM := uint64(activeMeta.Pgid())
	if pageSize == 0 || logicalBytes%pageSize != 0 {
		_ = db.Close()
		t.Fatalf("logical bytes %d are not aligned to page size %d", logicalBytes, pageSize)
	}
	targetHWM := logicalBytes / pageSize
	if targetHWM <= oldHWM || targetHWM > uint64(^common.Pgid(0)) {
		_ = db.Close()
		t.Fatalf("target HWM %d is outside (%d, %d]", targetHWM, oldHWM, uint64(^common.Pgid(0)))
	}
	if root.RootPage() >= common.Pgid(targetHWM) {
		_ = db.Close()
		t.Fatalf("root page %d is not below target HWM %d", root.RootPage(), targetHWM)
	}
	if !activeMeta.IsFreelistPersisted() {
		_ = db.Close()
		t.Fatal("seed database did not persist a freelist")
	}

	freelistPage := db.page(activeMeta.Freelist())
	originalIDs := append(common.Pgids(nil), freelistPage.FreelistPageIds()...)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for i, id := range originalIDs {
		if id <= 1 || uint64(id) >= oldHWM || (i > 0 && originalIDs[i-1] >= id) {
			t.Fatalf("invalid original freelist id %d at index %d (old HWM %d)", id, i, oldHWM)
		}
	}

	// Keep the rewritten freelist in a fresh low extent beginning at the old
	// HWM. Added IDs begin after that extent, so none can name storage pages.
	if requestedFreePages > uint64(math.MaxInt64)-uint64(len(originalIDs)) {
		t.Fatal("requested freelist cardinality overflows int64")
	}
	totalFreePages := uint64(len(originalIDs)) + requestedFreePages
	pgidBytes := uint64(unsafe.Sizeof(common.Pgid(0)))
	freelistBytes := uint64(common.PageHeaderSize)
	if totalFreePages >= 0xFFFF {
		if freelistBytes > ^uint64(0)-pgidBytes {
			t.Fatal("freelist size overflows uint64")
		}
		freelistBytes += pgidBytes
	}
	if totalFreePages > (^uint64(0)-freelistBytes)/pgidBytes {
		t.Fatal("freelist size overflows uint64")
	}
	freelistBytes += totalFreePages * pgidBytes
	if freelistBytes > ^uint64(0)-pageSize+1 {
		t.Fatal("freelist page count overflows uint64")
	}
	storagePages := (freelistBytes + pageSize - 1) / pageSize
	storageStart := oldHWM
	if storagePages == 0 || storagePages > ^uint64(0)-storageStart {
		t.Fatal("freelist storage page ID overflows pgid")
	}
	addedStart := storageStart + storagePages
	if addedStart > targetHWM {
		t.Fatalf("freelist storage does not fit: storage=%d start=%d target=%d", storagePages, storageStart, targetHWM)
	}
	if requestedFreePages != 0 {
		lastOffset := requestedFreePages - 1
		if addedStart >= targetHWM || lastOffset > (targetHWM-1-addedStart)/freeStride {
			t.Fatalf("free pages do not fit: start=%d target=%d requested=%d stride=%d", addedStart, targetHWM, requestedFreePages, freeStride)
		}
	}
	if storageStart > uint64(^common.Pgid(0)) || storagePages > uint64(^common.Pgid(0))-storageStart {
		t.Fatal("freelist storage page ID overflows pgid")
	}
	if storagePages > uint64(^uint32(0)) || storagePages > (uint64(^uint(0)>>1)/uint64(pageSizeBytes)) {
		t.Fatal("freelist storage extent is too large to materialize")
	}

	allIDs := make(common.Pgids, 0, totalFreePages)
	allIDs = append(allIDs, originalIDs...)
	for i := uint64(0); i < requestedFreePages; i++ {
		allIDs = append(allIDs, common.Pgid(addedStart+i*freeStride))
	}
	slices.Sort(allIDs)
	if uint64(len(allIDs)) != totalFreePages {
		t.Fatalf("freelist cardinality changed: got %d want %d", len(allIDs), totalFreePages)
	}
	for i := 1; i < len(allIDs); i++ {
		if allIDs[i] == allIDs[i-1] {
			t.Fatalf("duplicate synthesized freelist ID %d", allIDs[i])
		}
	}
	for _, id := range allIDs {
		if uint64(id) >= targetHWM || (uint64(id) >= storageStart && uint64(id) < addedStart) {
			t.Fatalf("synthesized ID %d overlaps storage or target HWM", id)
		}
	}

	// Preserve the original low pages, then rewrite only the freelist extent,
	// metadata pages, and sparse logical EOF.
	metaPages := make([][]byte, 2)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range metaPages {
		metaPages[i] = make([]byte, pageSizeBytes)
		if _, err := file.ReadAt(metaPages[i], int64(i)*int64(pageSizeBytes)); err != nil {
			_ = file.Close()
			t.Fatalf("read meta page %d: %v", i, err)
		}
	}
	if err := file.Truncate(int64(logicalBytes)); err != nil {
		_ = file.Close()
		t.Fatalf("sparse truncate: %v", err)
	}

	serialized := make([]byte, int(storagePages)*pageSizeBytes)
	serializedPage := common.LoadPage(serialized)
	serializedPage.SetId(common.Pgid(storageStart))
	serializedPage.SetOverflow(uint32(storagePages - 1))
	newFreelist := freelist.NewArrayFreelist()
	newFreelist.Init(allIDs)
	newFreelist.Write(serializedPage)
	if _, err := file.WriteAt(serialized, int64(storageStart)*int64(pageSizeBytes)); err != nil {
		_ = file.Close()
		t.Fatalf("write synthesized freelist: %v", err)
	}

	for i, metaBytes := range metaPages {
		m := common.LoadPageMeta(metaBytes)
		m.SetRootBucket(root)
		m.SetFreelist(common.Pgid(storageStart))
		m.SetPgid(common.Pgid(targetHWM))
		m.SetChecksum(m.Sum64())
		if _, err := file.WriteAt(metaBytes, int64(i)*int64(pageSizeBytes)); err != nil {
			_ = file.Close()
			t.Fatalf("write meta page %d: %v", i, err)
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatalf("sync synthesized fixture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if info, err := os.Stat(path); err != nil || uint64(info.Size()) != logicalBytes {
		t.Fatalf("logical fixture size mismatch: size=%v err=%v want=%d", info, err, logicalBytes)
	}
	physical, err := sparsePhysicalBytes(path)
	if err != nil {
		t.Fatal(err)
	}
	if physical >= logicalBytes/100 {
		t.Fatalf("Darwin sparse fixture is not sparse enough: physical=%d logical=%d", physical, logicalBytes)
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestSparseFreelistFreshProcessOpen$", "-test.v")
	cmd.Env = append(os.Environ(),
		sparseRoleEnv+"="+sparseRoleChild,
		sparsePathEnv+"="+path,
		sparseFreeStrideEnv+"="+strconv.FormatUint(freeStride, 10),
		sparseFreelistTypeEnv+"="+string(freelistType),
		sparseExpectedLogicalEnv+"="+strconv.FormatUint(logicalBytes, 10),
		sparseExpectedPageSizeEnv+"="+strconv.FormatUint(pageSize, 10),
		sparseExpectedFreePagesEnv+"="+strconv.FormatUint(totalFreePages, 10),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	start := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var childResult string
	var output strings.Builder
	var elapsed time.Duration
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		output.WriteString(line)
		output.WriteByte('\n')
		if childResult == "" && strings.HasPrefix(line, "BBOLT_SPARSE_OPEN_RESULT ") {
			childResult = line
			elapsed = time.Since(start)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("sparse child failed: %v\n%s%s", err, output.String(), stderr.String())
	}
	if childResult == "" {
		t.Fatalf("sparse child emitted no machine-readable result:\n%s%s", output.String(), stderr.String())
	}
	fmt.Println(childResult)
	fmt.Printf("BBOLT_SPARSE_PARENT_RESULT logical_bytes=%d page_size=%d free_pages=%d free_stride=%d freelist_type=%s launch_read_elapsed_ns=%d\n", logicalBytes, pageSize, totalFreePages, freeStride, freelistType, elapsed.Nanoseconds())
}

func sparseFreelistChild(t *testing.T) {
	path := os.Getenv(sparsePathEnv)
	if path == "" {
		t.Fatal("missing sparse child path")
	}
	freelistType := FreelistType(os.Getenv(sparseFreelistTypeEnv))
	if freelistType != FreelistMapType && freelistType != FreelistArrayType {
		t.Fatalf("invalid child freelist type %q", freelistType)
	}
	start := time.Now()
	db, err := Open(path, 0600, &Options{FreelistType: freelistType})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		bucket := tx.Bucket(sparseBucket)
		if bucket == nil {
			return fmt.Errorf("missing bucket")
		}
		if got := bucket.Get(sparseKey); !bytes.Equal(got, sparseValue) {
			return fmt.Errorf("unexpected value %q", got)
		}
		return nil
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	info, err := os.Stat(path)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	expectedLogical, err := strconv.ParseUint(os.Getenv(sparseExpectedLogicalEnv), 10, 64)
	if err != nil || uint64(info.Size()) != expectedLogical {
		_ = db.Close()
		t.Fatalf("logical bytes = %d, want %d: %v", info.Size(), expectedLogical, err)
	}
	expectedPageSize, err := strconv.ParseUint(os.Getenv(sparseExpectedPageSizeEnv), 10, 64)
	if err != nil || uint64(db.pageSize) != expectedPageSize {
		_ = db.Close()
		t.Fatalf("page size = %d, want %d: %v", db.pageSize, expectedPageSize, err)
	}
	expectedFreePages, err := strconv.ParseUint(os.Getenv(sparseExpectedFreePagesEnv), 10, 64)
	if err != nil || uint64(db.freelist.FreeCount()) != expectedFreePages {
		_ = db.Close()
		t.Fatalf("free pages = %d, want %d: %v", db.freelist.FreeCount(), expectedFreePages, err)
	}
	fmt.Printf("BBOLT_SPARSE_OPEN_RESULT logical_bytes=%d page_size=%d free_pages=%d free_stride=%s freelist_type=%s open_read_elapsed_ns=%d alloc_bytes=%d heap_alloc_bytes=%d heap_sys_bytes=%d\n", info.Size(), db.pageSize, db.freelist.FreeCount(), os.Getenv(sparseFreeStrideEnv), freelistType, elapsed.Nanoseconds(), mem.Alloc, mem.HeapAlloc, mem.HeapSys)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func sparsePhysicalBytes(path string) (uint64, error) {
	if runtime.GOOS != "darwin" {
		return 0, fmt.Errorf("sparse allocation validation is unavailable on %s", runtime.GOOS)
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	sys := reflect.Indirect(reflect.ValueOf(info.Sys()))
	blocks := sys.FieldByName("Blocks")
	if !blocks.IsValid() {
		return 0, fmt.Errorf("Darwin stat data has no block count")
	}
	var n uint64
	switch blocks.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n = blocks.Uint()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if blocks.Int() < 0 {
			return 0, fmt.Errorf("negative Darwin block count")
		}
		n = uint64(blocks.Int())
	default:
		return 0, fmt.Errorf("unsupported Darwin block count kind %s", blocks.Kind())
	}
	return n * 512, nil
}
