#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/hash.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "nodes/pg_list.h"
#include "access/xact.h"
#include "executor/executor.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void    _PG_init(void);
void 	_PG_fini(void);
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;



typedef struct Checkdestroy_info
{
	bool isJump ;//是否跳过执行
	bool isopen;//表是否被打开
	bool isdelete;
}Checkdestroy_info; 

static Checkdestroy_info *check = NULL;
//guc
static bool pg_checkdestroy_work = true;

static void pgcheckdestroy_post_parse_analyze_hook(ParseState *pstate, Query *query);
static void pg_check_ExecutorFinish(QueryDesc *queryDesc);
static void pg_check_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_check_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count,bool execute_once);
static void pg_check_ExecutorEnd(QueryDesc *queryDesc);
static void pg_check_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                    ProcessUtilityContext context, ParamListInfo params,
                    QueryEnvironment *queryEnv,
                    DestReceiver *dest, char *completionTag);

void    _PG_init(void);
void	_PG_fini(void);
bool getModifyTableState(QueryDesc *queryDesc, int eflags);
bool check_drop_truncate(Node *node);


static void 
build_checkinfo()
{
	if(!check)
	{
		check->isJump = false;
		check->isopen = false;
		check->isdelete = false;
	}
}

//处理无效quals
bool
getModifyTableState(QueryDesc *queryDesc, int eflags)
{

	bool result = false;
	MemoryContext oldcontext;
	ListCell *oid_list;
	ListCell   *l;
	Relation rel = NULL;
	PlannedStmt *plannedstmt = queryDesc->plannedstmt;
        List     *rangeTable = plannedstmt->rtable;
	EState *estate = CreateExecutorState();

	/* Do permissions checks */
	ExecCheckRTPerms(rangeTable, true);

	foreach(oid_list,queryDesc->plannedstmt->relationOids)
	{
		Oid relid = lfirst_oid(oid_list);
		rel = RelationIdGetRelation(relid);

		heap_close(rel,NoLock);
	}


	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_param_list_info = queryDesc->params;

	if (queryDesc->plannedstmt->nParamExec > 0)
		estate->es_param_exec_vals = (ParamExecData *)
			palloc0(queryDesc->plannedstmt->nParamExec * sizeof(ParamExecData));


	estate->es_sourceText = queryDesc->sourceText;
	estate->es_queryEnv = queryDesc->queryEnv;

	estate->es_output_cid = GetCurrentCommandId(true);

	//estate->es_snapshot = RegisterSnapshot(queryDesc->snapshot);
        //estate->es_crosscheck_snapshot = RegisterSnapshot(queryDesc->crosscheck_snapshot);
        estate->es_top_eflags = eflags;
        estate->es_instrument = queryDesc->instrument_options;


	estate->es_range_table = rangeTable;
	estate->es_plannedstmt = plannedstmt;

	if (plannedstmt->resultRelations)
	{
		List       *resultRelations = plannedstmt->resultRelations;
		int            numResultRelations = list_length(resultRelations);
		ResultRelInfo *resultRelInfos;
		ResultRelInfo *resultRelInfo;

		resultRelInfos = (ResultRelInfo *)
			palloc(numResultRelations * sizeof(ResultRelInfo));
		resultRelInfo = resultRelInfos;
		foreach(l, resultRelations)
		{
			Index        resultRelationIndex = lfirst_int(l);
			Oid            resultRelationOid;
			Relation    resultRelation;

			resultRelationOid = getrelid(resultRelationIndex, rangeTable);
			resultRelation = heap_open(resultRelationOid, RowExclusiveLock);

			InitResultRelInfo(resultRelInfo,
					resultRelation,
					resultRelationIndex,
					NULL,
					estate->es_instrument);
			resultRelInfo++;
			check->isopen = true;

			/* need close relation*/
			heap_close(resultRelation,RowExclusiveLock);
		}
		estate->es_result_relations = resultRelInfos;
		estate->es_num_result_relations = numResultRelations;
		/* es_result_relation_info is NULL except when within ModifyTable */
		estate->es_result_relation_info = NULL;

	}
	else
	{
		estate->es_result_relations = NULL;
		estate->es_num_result_relations = 0;
		estate->es_result_relation_info = NULL;
	}

	estate->es_result_relation_info = NULL;
	estate->es_root_result_relations = NULL;
	estate->es_num_root_result_relations = 0;

	estate->es_rowMarks = NIL;
	

	estate->es_tupleTable = NIL;
	estate->es_trig_tuple_slot = NULL;
	estate->es_trig_oldtup_slot = NULL;
	estate->es_trig_newtup_slot = NULL;

	estate->es_epqTuple = NULL;
	estate->es_epqTupleSet = NULL;
	estate->es_epqScanDone = NULL;	
	estate->es_processed = 0;

	PlanState *planstate =  ExecInitNode(queryDesc->plannedstmt->planTree,estate,eflags);
	DecrTupleDescRefCount(rel->rd_att);
	heap_close(rel,NoLock);

	MemoryContextSwitchTo(oldcontext);
	
	queryDesc->planstate = planstate;
	queryDesc->estate = estate;

	ModifyTableState *node = castNode(ModifyTableState, planstate);

	PlanState  *subplanstate ;
	if(node !=NULL)
		subplanstate = node->mt_plans[node->mt_whichplan];


	if(subplanstate != NULL && subplanstate->plan->qual == NULL)
	{
		check->isJump = true;
		result = true;
	}


	return result;
}


/* check drop & truncate table*/
bool 
check_drop_truncate(Node *node)
{
	if(nodeTag(node) == T_DropStmt || nodeTag(node) == T_TruncateStmt)
		return true;
	else
		return false;
}


static void
pgcheckdestroy_post_parse_analyze_hook(ParseState *pstate, Query *query)
{

	check = (Checkdestroy_info*)palloc0(sizeof(Checkdestroy_info));
	build_checkinfo();

	if(prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate,query);

	if(!pg_checkdestroy_work)	
		return;

	if(query->commandType == CMD_DELETE)
	{
		check->isdelete = true;
	
		if(query->jointree->quals == NULL)
		{
			check->isJump = true;
		}

	}

	//truncate table && drop 
	if(query->utilityStmt != NULL && check_drop_truncate(query->utilityStmt) )
	{
		check->isJump = true;
	}

	return;
}

static void
pg_check_ExecutorStart(QueryDesc *queryDesc, int eflags)
{

	if (prev_ExecutorStart)
	{
                prev_ExecutorStart(queryDesc, eflags);
       	}
	/* if delete need check quals*/
	 else if(!check->isJump)
	{	
		if(check->isdelete){
			if (!getModifyTableState(queryDesc,eflags))	
				standard_ExecutorStart(queryDesc, eflags);
		} else
			standard_ExecutorStart(queryDesc, eflags);

	}
	else
	{
		EState *estate = CreateExecutorState();
		estate->es_processed = 0;
		queryDesc->estate = estate;

	}

		
	
	if(queryDesc->plannedstmt->queryId != 0)
	{
		if (queryDesc->totaltime == NULL)
                {
                        MemoryContext oldcxt;

                        oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
                        queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
                        MemoryContextSwitchTo(oldcxt);
                }
	}

}

static void
pg_check_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count,bool execute_once)
{
	PG_TRY();
	{
                if (prev_ExecutorRun)
		{
                        prev_ExecutorRun(queryDesc, direction, count,execute_once);
              	 }
		 else if(!check->isJump)
                        standard_ExecutorRun(queryDesc, direction, count,execute_once);
       }
        PG_CATCH();
        {
                PG_RE_THROW();
        }
        PG_END_TRY();
}


static void
pg_check_ExecutorFinish(QueryDesc *queryDesc)
{
	PG_TRY();
        {
                if (prev_ExecutorFinish)
		{
                        prev_ExecutorFinish(queryDesc);
              	}
		else if(!check->isJump)
                        standard_ExecutorFinish(queryDesc);
        }
        PG_CATCH();
        {
                PG_RE_THROW();
        }
        PG_END_TRY();
}


static void
pg_check_ExecutorEnd(QueryDesc *queryDesc)
{

	 uint32          queryId = queryDesc->plannedstmt->queryId;
	//ListCell *oid_list;
	if(queryId != 0 && queryDesc->totaltime)
	{
		 InstrEndLoop(queryDesc->totaltime);
	}

	if (prev_ExecutorEnd)
	{
                prev_ExecutorEnd(queryDesc);
       	}
	 else if(!check->isJump) 
                standard_ExecutorEnd(queryDesc);

	
	check->isJump = false;
	check->isopen = false;
	check->isdelete = false;
}

static void pg_check_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                    ProcessUtilityContext context, ParamListInfo params,
                    QueryEnvironment *queryEnv,
                    DestReceiver *dest, char *completionTag)
{
	PG_TRY();
	{
		if (prev_ProcessUtility)
		{
			prev_ProcessUtility(pstmt, queryString,
					context, params, queryEnv,
					dest, completionTag);
		}
		else if(!check->isJump)
			standard_ProcessUtility(pstmt, queryString,
					context, params, queryEnv,
					dest, completionTag);

	}
	PG_CATCH();
	{
		check->isJump = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	check->isJump = false;
}


void	_PG_init(void)
{

	DefineCustomBoolVariable("pg_checkdestroy.work",
                "pg_checkdestroy work.",
                NULL,
                 &pg_checkdestroy_work,
                true,
                PGC_USERSET,
                0,
                NULL,
                NULL,
                NULL);

	elog(LOG,"%s\n","This is pg_checkdrop");
	if (!process_shared_preload_libraries_in_progress)
                return;	

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgcheckdestroy_post_parse_analyze_hook;
	
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_check_ExecutorStart;

	prev_ExecutorRun = ExecutorRun_hook;
        ExecutorRun_hook = pg_check_ExecutorRun;

	prev_ExecutorFinish = ExecutorFinish_hook;
        ExecutorFinish_hook = pg_check_ExecutorFinish;

	prev_ExecutorEnd = ExecutorEnd_hook;
        ExecutorEnd_hook = pg_check_ExecutorEnd;

	prev_ProcessUtility = ProcessUtility_hook;
    	ProcessUtility_hook = pg_check_ProcessUtility;
}

void
_PG_fini(void)
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
        ExecutorStart_hook = prev_ExecutorStart;
        ExecutorRun_hook = prev_ExecutorRun;
        ExecutorFinish_hook = prev_ExecutorFinish;
        ExecutorEnd_hook = prev_ExecutorEnd;
	ProcessUtility_hook = prev_ProcessUtility;
}
