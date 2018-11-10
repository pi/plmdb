// synchronized with commit 223c56076b2961f6518fcb98cd84cc9f60bb67a5
unit lmdb;

interface

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
  MDB_dbi = cardinal;
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
  MDB_NOOVERWRITE	= $10;
  MDB_NODUPDATA	  = $20;
  MDB_CURRENT	    = $40;
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

const // TEMP TEMP
  ENOMEM = 100;

type
  MDB_stats = record
	  ms_psize: cardinal;
    ms_depth: cardinal;
    ms_branch_pages: mdb_size_t;
    ms_leaf_pages: mdb_size_t;
    ms_overflow_pages: mdb_size_t;
    ms_entries: mdb_size_t;
  end;
  PMDB_stats = ^MDB_stats;

  MDB_envinfo = record
    me_mapaddr: pointer;
	  me_mapsize: mdb_size_t;
    me_last_pgno: mdb_size_t;
	  me_last_txnid: mdb_size_t;
	  me_maxreaders: cardinal;
    me_numreaders: cardinal;
  end;
  PMDB_envinfo = ^MDB_envinfo;

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
  public
    constructor Create; // replaces mdb_env_create
    destructor Destroy; // replaces mdb_env_close

    function open(const path: string; flags: cardinal; mode: mdb_mode_t): integer;
    function copy(const path: string): integer;
    function copyfd(fd: mdb_filehandle_t): integer;
    function copy2(const path: string; flags: cardinal): integer;
    function copyfd2(fd: mdb_filehandle_t; flags: cardinal): integer;
    function get_stat(out stat: MDB_stats): integer;
    function info(out stat: MDB_envinfo): integer;
    function sync(force: boolean): integer;
    function set_flags(flags: cardinal; onoff: boolean): integer;
    function get_flags(out flags: cardinal): integer;
    function get_path(out path: string): integer;
    function get_fd(out fd: mdb_filehandle_t): integer;
    function set_mapsize(size: mdb_size_t): integer;
    function set_maxreaders(readers: cardinal): integer;
    function get_maxreaders(out readers: cardinal): integer;
    function set_maxdbs(dbs: cardinal): integer;
    function get_maxkeysize: integer;
    function set_userctx(ctx: pointer): integer;
    function get_userctx: pointer;
    function set_assert(func: MDB_assert_func): integer;

    function BeginTxn(parent: TTxn; flags: cardinal; out txn: TTxn): integer; // mdb_txn_begin

    function reader_list(func: MDB_msg_func; ctx: pointer): integer;

    function reader_check(out dead: integer): integer;
  end;

  TTxn = class
  public
    constructor Create(anEnv: TEnv; aParent: TTxn; flags: cardinal); // mdb_txn_begin
    destructor Destroy; // mdb_txn_abort

    function env: TEnv;
    function id: mdb_size_t;
    function commit: integer;
    procedure reset;
    function renew: integer;
  end;

  TDbi = class
  private
    fEnv: TEnv;
  public
    constructor Create(txn: TTxn; const name: string; flags: cardinal); // mdb_dbi_open
    destructor Destroy; // mdb_dbi_close

    function get_stat(txn: TTxn; out stat: MDB_stats): integer;
    function get_flags(txn: TTxn; out flags: cardinal): integer;
    function drop(txn: TTxn; del: boolean): integer;
    function set_compare(txn: TTxn; cmp: MDB_cmp_func): integer;
    function set_dupsort(txn: TTxn; cmp: MDB_cmp_func): integer;
    function set_relfunc(txn: TTxn; rel: MDB_rel_func): integer;
    function set_relctx(txn: TTxn; ctx: pointer): integer;
    function get(txn: TTxn; const key: MDB_val; out data: MDB_val): integer;
    function put(txn: TTxn; const key, data: MDB_val; flags: cardinal): integer;
    function del(txn: TTxn; const key, data: MDB_val): integer;

    function cursor_open(txn: TTxn; out cursor: TCursor): integer;

    function cmp(txn: TTxn; const a, b: MDB_val): integer; // mdb_cmp
    function dcmp(txn: TTxn; const a, b: MDB_val): integer; // mdb_dcmp
  end;

  TCursor = class
  public
    destructor Destroy; override; // mdb_cursor_close

    function renew(txn: TTxn): integer;
    function get_txn: TTxn; // mdb_cursor_txn
    function get_dbi: TDbi;
    function get(out key, data: MDB_val; op: MDB_cursor_op): integer;
    function put(const key, data: MDB_val; flags: cardinal): integer;
    function del(flags: cardinal ): integer;
    function val_count(out count: mdb_size_t): integer; // mdb_cursor_count
  end;

implementation

uses
    SysUtils
;

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

function mdb_midl_alloc(out pmidl: PMDB_IDL; num: cardinal): integer;
begin
  GetMem(pmidl, sizeof(MDB_IDL) + num*sizeof(MDB_ID));
  if pmidl = nil then
    result := ENOMEM
  else
  begin
    result := 0;
    pmidl^.Cap := num;
    pmidl^.Count := 0;
  end;
end;

function mdb_midl_shrink(var pmidl: PMDB_IDL): integer;
begin
  if pmidl^.Cap > MDB_IDL_UM_MAX then
  begin
    ReallocMem(pmidl, sizeof(MDB_IDL) + MDB_IDL_UM_MAX * sizeof(MDB_ID));
    if pmidl = nil then
      result := ENOMEM
    else
    begin
      pmidl^.Cap := MDB_IDL_UM_MAX;
      result := 0;
    end;
  end;
end;

function mdb_midl_grow(var pmidl: PMDB_IDL; num: cardinal): integer;
begin
  ReallocMem(pmidl, sizeof(MDB_IDL) + (pmidl^.Cap + num) * sizeof(MDB_ID));
  if pmidl = nil then
    result := ENOMEM
  else
  begin
    inc(pmidl^.Cap, num);
    result := 0;
  end;
end;

function mdb_midl_need(var pmidl: PMDB_IDL; num: cardinal): integer;
var
  new: cardinal;
begin
  result := 0;
  if (pmidl^.Count + num) > pmidl^.Cap then
  begin
    inc(num, pmidl^.Count);
		num := (num + num div 4 + (256 + 2)) and cardinal(-256);
    result := mdb_midl_grow(pmidl, pmidl^.Cap - num);
  end;
end;

function mdb_midl_append(var pmidl: PMDB_IDL; id: MDB_ID): integer;
begin
  if pmidl^.Count = pmidl^.Cap then
  begin
    result := mdb_midl_grow(pmidl, MDB_IDL_UM_MAX);
    if result <> 0 then
       exit;
  end;
  inc(pmidl^.Count); // ONE BASED!
  pmidl^.List[pmidl^.Count] := id;
  result := 0;
end;

function mdb_midl_append_list(var pmidl: PMDB_IDL; const src: MDB_IDL): integer;
begin
  result := mdb_midl_need(pmidl, src.Count);
  if result = 0 then
  begin
    Move(src.List[1], pmidl^.List[pmidl^.Count + 1], src.Count * sizeof(MDB_IDL));
    inc(pmidl^.Count, src.Count);
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
