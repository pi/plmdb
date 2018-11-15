// synchronized with commit 223c56076b2961f6518fcb98cd84cc9f60bb67a5
unit lmdb;

{$ifdef fpc}{$H+}{$endif}

interface

uses SysUtils;

type EOutOfMemory = class(Exception);

{$ifdef testing}
procedure test_midl;
{$endif}

const
  MDB_VERSION_MAJOR	= 0;
  MDB_VERSION_MINOR	= 9;
  MDB_VERSION_PATCH	= 70;
  MDB_VERSION_FULL = (MDB_VERSION_MAJOR shl 24) or (MDB_VERSION_MINOR shl 16) or MDB_VERSION_PATCH;
  MDB_VERSION_DATE = 'December 19, 2015';

function MDB_VERSION_STRING: string;

type
  mdb_mode_t = integer;
  mdb_size_t = {$ifdef fpc}SizeUint{$else}cardinal{$endif};
  mdb_filehandle_t = THANDLE;
  MDB_val =  record
    mv_size: mdb_size_t;
    mv_data: pointer;
  end;
  PMDB_val = ^MDB_val;
  MDB_cmp_func = function(a, b: PMDB_val): integer;
  MDB_rel_func = procedure(item: PMDB_val; oldptr, newptr, relctx: pointer);

const
  MDB_SIZE_MAX = high(mdb_size_t);

const // Environment Flags
  MDB_FIXEDMAP	  = $0000001;
  MDB_NOSUBDIR	  = $0004000;
  MDB_NOSYNC		  = $0010000;
  MDB_RDONLY		  = $0020000;
  MDB_NOMETASYNC  = $0040000;
  MDB_WRITEMAP		= $0080000;
  MDB_MAPASYNC		= $0100000;
  MDB_NOTLS		    = $0200000;
  MDB_NOLOCK		  = $0400000;
  MDB_NORDAHEAD	  = $0800000;
  MDB_NOMEMINIT	  = $1000000;
  MDB_PREVMETA	  = $2000000;

const // Database Flags
  MDB_REVERSEKEY  = $02;
  MDB_DUPSORT		  = $04;
  MDB_INTEGERKEY	= $08;
  MDB_DUPFIXED	  = $10;
  MDB_INTEGERDUP	= $20;
  MDB_REVERSEDUP	= $40;
  MDB_CREATE		  = $40000;

const // Write Flags
  MDB_NOOVERWRITE	= $00010;
  MDB_NODUPDATA	  = $00020;
  MDB_CURRENT	    = $00040;
  MDB_RESERVE	    = $10000;
  MDB_APPEND	    = $20000;
  MDB_APPENDDUP	  = $40000;
  MDB_MULTIPLE	  = $80000;

const // Copy Flags
  MDB_CP_COMPACT	= $01;

type // Cursor Get operations.
  MDB_cursor_op = (
    MDB_FIRST,
    MDB_FIRST_DUP,
    MDB_GET_BOTH,
    MDB_GET_BOTH_RANGE,
    MDB_GET_CURRENT,
    MDB_GET_MULTIPLE,
    MDB_LAST,
    MDB_LAST_DUP,
    MDB_NEXT,
    MDB_NEXT_DUP,
    MDB_NEXT_MULTIPLE,
    MDB_NEXT_NODUP,
    MDB_PREV,
    MDB_PREV_DUP,
    MDB_PREV_NODUP,
    MDB_SET,
    MDB_SET_KEY,
    MDB_SET_RANGE,
    MDB_PREV_MULTIPLE
  );

const // Return Codes
  MDB_SUCCESS	          = 0;
  MDB_KEYEXIST	        = (-30799);
  MDB_NOTFOUND	        = (-30798);
  MDB_PAGE_NOTFOUND     = (-30797);
  MDB_CORRUPTED	        = (-30796);
  MDB_PANIC		          = (-30795);
  MDB_VERSION_MISMATCH  = (-30794);
  MDB_INVALID	          = (-30793);
  MDB_MAP_FULL	        = (-30792);
  MDB_DBS_FULL	        = (-30791);
  MDB_READERS_FULL	    = (-30790);
  MDB_TLS_FULL	        = (-30789);
  MDB_TXN_FULL	        = (-30788);
  MDB_CURSOR_FULL	      = (-30787);
  MDB_PAGE_FULL	        = (-30786);
  MDB_MAP_RESIZED	      = (-30785);
  MDB_INCOMPATIBLE	    = (-30784);
  MDB_BAD_RSLOT		      = (-30783);
  MDB_BAD_TXN			      = (-30782);
  MDB_BAD_VALSIZE		    = (-30781);
  MDB_BAD_DBI		        = (-30780);
  MDB_PROBLEM		        = (-30779);
  MDB_LAST_ERRCODE      = MDB_PROBLEM;

type
  PMDB_stats = ^MDB_stats;
  MDB_stats = record
    ms_psize: mdb_size_t;
    ms_depth: mdb_size_t;
    ms_branch_pages: mdb_size_t;
    ms_leaf_pages: mdb_size_t;
    ms_overflow_pages: mdb_size_t;
    ms_entries: mdb_size_t;
  end;
  PMDB_envinfo = ^MDB_envinfo;
  MDB_envinfo = record
    me_mapaddr: pointer;
    me_mapsize: mdb_size_t;
    me_last_pgno: mdb_size_t;
    me_last_txnid: mdb_size_t;
    me_maxreaders: cardinal;
    me_numreaders: cardinal;
  end;

function mdb_version(out major, minor, patch: integer): string;
function mdb_strerror(err: integer): string;

type
  TEnv = class;
  TDbi = class;
  TTxn = class;
  TCursor = class;

  MDB_assert_func = function(env: TEnv; const msg: string): integer;
  MDB_msg_func = function(const msg: string; ctx: pointer): integer;

  TEnv = class
  protected
    fUserCtx: pointer;
  public
    constructor Create; // replaces mdb_env_create
    destructor Destroy; override; // replaces mdb_env_close

    procedure Open(const path: string; flags: cardinal; mode: mdb_mode_t);
    procedure Copy(const path: string);
    procedure CopyFd(fd: mdb_filehandle_t);
    procedure Copy2(const path: string; flags: cardinal);
    procedure CopyFd2(fd: mdb_filehandle_t; flags: cardinal);
    procedure GetStat(out stat: MDB_stats);
    procedure GetInfo(out stat: MDB_envinfo);
    procedure Sync(force: boolean);
    procedure SetFlags(flags: cardinal; onoff: boolean);
    function GetFlags: cardinal;
    function GetPath: string;
    function GetFd: mdb_filehandle_t;
    procedure SetMapsize(size: mdb_size_t);
    procedure SetMaxreaders(readers: cardinal);
    procedure GetMaxreaders(out readers: cardinal);
    procedure SetMaxdbs(dbs: cardinal);
    function GetMaxKeySize: integer;
    procedure SetUserCtx(ctx: pointer);
    function GetUserCtx: pointer;
    procedure SetAssert(func: MDB_assert_func);

    function BeginTxn(parent: TTxn; flags: cardinal): TTxn; // mdb_txn_begin

    procedure ReaderList(func: MDB_msg_func; ctx: pointer);

    procedure ReaderCheck(out dead: integer);
  end;

  TTxn = class
  public
    constructor Create(anEnv: TEnv; aParent: TTxn; flags: cardinal); // mdb_txn_begin
    destructor Destroy; override; // mdb_txn_abort

    function Env: TEnv;
    function ID: mdb_size_t;
    function Commit: integer;
    procedure Reset;
    function ReNew: integer;
  end;

  TDbi = class
  private
    fEnv: TEnv;
  public
    constructor Create(txn: TTxn; const name: string; flags: cardinal); // mdb_dbi_open
    destructor Destroy; override; // mdb_dbi_close

    function GetStat(txn: TTxn; out stat: MDB_stats): integer;
    function GetFlags(txn: TTxn; out flags: cardinal): integer;
    function Drop(txn: TTxn; del: boolean): integer;
    function SetCompare(txn: TTxn; cmp: MDB_cmp_func): integer;
    function SetDupSort(txn: TTxn; cmp: MDB_cmp_func): integer;
    function SetRelFunc(txn: TTxn; rel: MDB_rel_func): integer;
    function SetRelCtx(txn: TTxn; ctx: pointer): integer;
    function Get(txn: TTxn; const key: MDB_val; out data: MDB_val): integer;
    function Put(txn: TTxn; const key, data: MDB_val; flags: cardinal): integer;
    function Del(txn: TTxn; const key, data: MDB_val): integer;

    function OpenCursor(txn: TTxn; out cursor: TCursor): integer;

    function Cmp(txn: TTxn; const a, b: MDB_val): integer; // mdb_cmp
    function DCmp(txn: TTxn; const a, b: MDB_val): integer; // mdb_dcmp
  end;

  TCursor = class
  public
    constructor Create(aTxn: TTxn; aDbi: TDbi);
    destructor Destroy; override; // mdb_cursor_close

    function ReNew(aTxn: TTxn): integer;
    function GetTxn: TTxn; // mdb_cursor_txn
    function GetDbi: TDbi;
    function Get(out key, data: MDB_val; op: MDB_cursor_op): integer;
    function Put(const key, data: MDB_val; flags: cardinal): integer;
    function Del(flags: cardinal ): integer;
    function ValCount(out count: mdb_size_t): integer; // mdb_cursor_count
  end;

implementation

{$ifdef windows}
uses windows
;
{$endif}

{ internal types }
type
  pgno_t = MDB_ID;
  txnid_t = MDB_ID;
  indx_t  = word;
  mdb_hash_t = uint64;

const
  P_INVALID	 = not pgno_t(0);
  DEFAULT_MAPSIZE	= 1048576;
  DEFAULT_READERS	= 126;
  CACHELINE	= 64;

{$ifdef windows}
type
  NTSTATUS = integer;

function NtCreateSection(
  out SectionHandle:      THANDLE;
  DesiredAccess:          ACCESS_MASK;
  ObjectAttributes:       pointer;
  var MaximumSize:        LARGE_INTEGER;
  SectionPageProtection:  ULONG;
  AllocationAttributes:   ULONG;
  FileHandle:             THANDLE
): NTSTATUS; stdcall; external 'ntdll.dll';

const // inherit disposition
  ViewShare = 1;
  ViewUnmap = 2;

function NtMapViewOfSection(
    SectionHandle:      THANDLE;
    ProcessHandle:      THANDLE;
    BaseAddress:        PPointer;
    ZeroBits:           PtrUint;
    CommitSize:         SizeUint;
    var SectionOffset:  LARGE_INTEGER;
    var ViewSize:       SizeUint;
    InheritDisposition: integer;
    AllocationType:     ULONG;
    Win32Protect:       ULONG
): NTSTATUS; stdcall; external 'ntdll.dll';

function NtClose(Handle: THandle): NTSTATUS; stdcall; external 'ntdll.dll';

function TranslateNtStatusToWinError(st: integer): integer;
var
  o: TOverlapped;
  br: cardinal;
begin
  FillChar(o, sizeof(o), 0);
  o.Internal := st;
  br := 0;
  GetOverlappedResult(0, o, br, false);
  result := GetLastError();
end;
{$endif}

procedure CheckOOM(p: pointer);
begin
  if p = nil then
    raise EOutOfMemory.Create('PLMDB: Out of memory');
end;

{ MIDL }

type
  MDB_ID = mdb_size_t;
  MDB_IDL = record
    Cap: mdb_size_t;
    Count: mdb_size_t;
    List: array [1..1] of MDB_ID;
  end;
  PMDB_IDL = ^MDB_IDL;

const
  MDB_IDL_LOGN  = 16;
  MDB_IDL_DB_SIZE = 1 shl MDB_IDL_LOGN;
  MDB_IDL_UM_SIZE = (1 shl (MDB_IDL_LOGN+1));

  MDB_IDL_DB_MAX = (MDB_IDL_DB_SIZE-1);
  MDB_IDL_UM_MAX = (MDB_IDL_UM_SIZE-1);

function mdb_midl_search(const ids: MDB_IDL; id: MDB_ID): cardinal;
var
  base: cardinal;
  pivot: cardinal;
  val: integer;
  n: cardinal;
begin
  base := 0;
  result := 1;
  val := 0;
  n := ids.Count;

  while n > 0 do
  begin
    pivot := n shr 1;
    result := base + pivot + 1;

    if ids.List[result] = id then
      exit;

    if ids.List[result] < id then
    begin
      n := pivot;
      val := -1;
    end
    else
    begin
      base := result;
      dec(n, pivot + 1);
      val := 1;
    end;
  end;

  if val > 0 then
    inc(result);
end;

procedure mdb_midl_alloc(out pmidl: PMDB_IDL; num: cardinal);
begin
  GetMem(pmidl, sizeof(MDB_IDL) + num*sizeof(MDB_ID));
  CheckOOM(pmidl);
  pmidl^.Cap := num;
  pmidl^.Count := 0;
end;

procedure mdb_midl_shrink(var pmidl: PMDB_IDL);
begin
  if pmidl^.Cap > MDB_IDL_UM_MAX then
  begin
    ReallocMem(pmidl, sizeof(MDB_IDL) + MDB_IDL_UM_MAX * sizeof(MDB_ID));
    CheckOOM(pmidl);
    pmidl^.Cap := MDB_IDL_UM_MAX;
  end;
end;

procedure mdb_midl_grow(var pmidl: PMDB_IDL; num: cardinal);
begin
  ReallocMem(pmidl, sizeof(MDB_IDL) + (pmidl^.Cap + num) * sizeof(MDB_ID));
  CheckOOM(pmidl);
  inc(pmidl^.Cap, num);
end;

procedure mdb_midl_need(var pmidl: PMDB_IDL; num: cardinal);
begin
  if (pmidl^.Count + num) > pmidl^.Cap then
  begin
    inc(num, pmidl^.Count);
    num := (num + num div 4 + (256 + 2)) and cardinal(-256);
    mdb_midl_grow(pmidl, pmidl^.Cap - num);
  end;
end;

procedure mdb_midl_append(var pmidl: PMDB_IDL; id: MDB_ID);
begin
  if pmidl^.Count = pmidl^.Cap then
    mdb_midl_grow(pmidl, MDB_IDL_UM_MAX);
  inc(pmidl^.Count); // ONE BASED!
  pmidl^.List[pmidl^.Count] := id;
end;

procedure mdb_midl_append_list(var pmidl: PMDB_IDL; const src: MDB_IDL);
begin
  mdb_midl_need(pmidl, src.Count);
  Move(src.List[1], pmidl^.List[pmidl^.Count + 1], src.Count * sizeof(MDB_IDL));
  inc(pmidl^.Count, src.Count);
end;

procedure mdb_midl_append_range(var pmidl: PMDB_IDL; id: MDB_ID; n: cardinal);
var
  i: cardinal;
begin
  if pmidl^.Count + n > pmidl^.Cap then
    mdb_midl_grow(pmidl, n or MDB_IDL_UM_MAX);
  inc(pmidl^.Count, n);
  for i := pmidl^.Count downto pmidl^.Count - n + 1 do
  begin
    pmidl^.List[i] := id;
    inc(id);
  end;
end;

procedure mdb_midl_xmerge(pmidl: PMDB_IDL; merge: PMDB_IDL);
var
  old_id, merge_id, i, j, k, total: MDB_ID;
begin
  i := merge^.Count;
  j := pmidl^.Count;
  k := i + j;
  total := k;
  old_id := pmidl^.List[j];
  while i > 0 do
  begin
    merge_id := merge^.List[i];
    dec(i);
    while old_id < merge_id do
    begin
      pmidl^.List[k] := old_id;
      dec(k);
      dec(j);
      old_id := pmidl^.List[j];
    end;
    pmidl^.List[k] := merge_id;
    dec(k);
  end;
  pmidl^.Count := total;
end;

(*
#define SMALL	8
#define	MIDL_SWAP(a,b)	{ itmp=(a); (a)=(b); (b)=itmp; }

void
mdb_midl_sort( MDB_IDL ids )
{
  /* Max possible depth of int-indexed tree * 2 items/level */
  int istack[sizeof(int)*CHAR_BIT * 2];
  int i,j,k,l,ir,jstack;
  MDB_ID a, itmp;

  ir = (int)ids[0];
  l = 1;
  jstack = 0;
  for(;;) {
    if (ir - l < SMALL) {	/* Insertion sort */
      for (j=l+1;j<=ir;j++) {
        a = ids[j];
        for (i=j-1;i>=1;i--) {
          if (ids[i] >= a) break;
          ids[i+1] = ids[i];
        }
        ids[i+1] = a;
      }
      if (jstack == 0) break;
      ir = istack[jstack--];
      l = istack[jstack--];
    } else {
      k = (l + ir) >> 1;	/* Choose median of left, center, right */
      MIDL_SWAP(ids[k], ids[l+1]);
      if (ids[l] < ids[ir]) {
        MIDL_SWAP(ids[l], ids[ir]);
      }
      if (ids[l+1] < ids[ir]) {
        MIDL_SWAP(ids[l+1], ids[ir]);
      }
      if (ids[l] < ids[l+1]) {
        MIDL_SWAP(ids[l], ids[l+1]);
      }
      i = l+1;
      j = ir;
      a = ids[l+1];
      for(;;) {
        do i++; while(ids[i] > a);
        do j--; while(ids[j] < a);
        if (j < i) break;
        MIDL_SWAP(ids[i],ids[j]);
      }
      ids[l+1] = ids[j];
      ids[j] = a;
      jstack += 2;
      if (ir-i+1 >= j-l) {
        istack[jstack] = ir;
        istack[jstack-1] = i;
        ir = j-1;
      } else {
        istack[jstack] = j-1;
        istack[jstack-1] = l;
        l = i;
      }
    }
  }
}
*)
procedure mdb_midl_sort(ids: PMDB_IDL);
const
  SMALL = 8;
  procedure MIDL_SWAP(var a, b: MDB_ID);
  var
    tmp: MDB_ID;
  begin
    tmp := a;
    a := b;
    b := tmp;
  end;
var
  istack: array [0..8 * 2 * sizeof(integer) - 1] of integer;
  i, j, k, l, ir, jstack: integer;
  a: MDB_ID;
begin
  ir := ids^.Count;
  l := 1;
  jstack := 0;
  while true do
  begin
    if (ir - l < SMALL) then
    begin	// Insertion sort
      for j := l + 1  to ir do
      begin
        a := ids^.List[j];
        for i := j - 1 downto 1 do
        begin
          if ids^.List[i] >= a then
            break;
          ids^.List[i + 1] := ids^.List[i];
        end;
        ids^.List[i + 1] := a;
      end;
      if (jstack = 0) then
        break;
      ir := istack[jstack];
      dec(jstack);
      l := istack[jstack];
      dec(jstack);
    end
    else
    begin
      k := (l + ir) shr 1;	// Choose median of left, center, right
      MIDL_SWAP(ids^.List[k], ids^.List[l + 1]);
      if (ids^.List[l] < ids^.List[ir]) then
        MIDL_SWAP(ids^.List[l], ids^.List[ir]);
      if (ids^.List[l + 1] < ids^.List[ir]) then
        MIDL_SWAP(ids^.List[l+1], ids^.List[ir]);
      if (ids^.List[l] < ids^.List[l+1]) then
        MIDL_SWAP(ids^.List[l], ids^.List[l+1]);
      i := l + 1;
      j := ir;
      a := ids^.List[l + 1];
      while true do
      begin
        repeat inc(i) until ids^.List[i] <= a;
        repeat dec(j) until ids^.List[j] >= a;
        if (j < i) then
          break;
        MIDL_SWAP(ids^.List[i], ids^.List[j]);
      end;
      ids^.List[l + 1] := ids^.List[j];
      ids^.List[j] := a;
      inc(jstack, 2);
      if (ir - i + 1) >= (j - l) then
      begin
        istack[jstack] := ir;
        istack[jstack - 1] := i;
        ir := j - 1;
      end
      else
      begin
        istack[jstack] := j - 1;
        istack[jstack - 1] := l;
        l := i;
      end;
    end;
  end;
end;

{ MDB }

function MDB_VERSION_STRING: string;
begin
  result := Format('LMDB %d.%d.%d: (%s)', [MDB_VERSION_MAJOR, MDB_VERSION_MINOR, MDB_VERSION_PATCH, MDB_VERSION_DATE]);
end;

function mdb_version(out major, minor, patch: integer): string;
begin
  major := MDB_VERSION_MAJOR;
  minor := MDB_VERSION_MINOR;
  patch := MDB_VERSION_PATCH;
  result := MDB_VERSION_STRING
end;

function mdb_strerror(err: integer): string;
begin
  // todo
end;

end.
