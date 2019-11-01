/*  DTSMETHODS.h -- Local definitions of DTS methods.
*/

int dts_initDTS (void *data);		/* Admin Methods		*/
int dts_shutdownDTS (void *data);
int dts_abortDTS (void *data);
int dts_nodeStat (void *data);
int dts_submitLogs (void *data);
int dts_getQLog (void *data);
int dts_eraseQLog (void *data);

int dts_startQueue (void *data);	/* Queue Methods		*/
int dts_pauseQueue (void *data);
int dts_shutdownQueue (void *data);
int dts_pokeQueue (void *data);
int dts_stopQueue (void *data);
int dts_listQueue (void *data);
int dts_flushQueue (void *data);
int dts_restartQueue (void *data);
int dts_addToQueue (void *data);
int dts_removeFromQueue (void *data);
int dts_getQueueStat (void *data);
int dts_setQueueStat (void *data);
int dts_setQueueCount (void *data);
int dts_getQueueCount (void *data);
int dts_setQueueDir (void *data);
int dts_getQueueDir (void *data);
int dts_setQueueCmd (void *data);
int dts_getQueueCmd (void *data);
int dts_getCopyDir (void *data);
int dts_execCmd (void *data);
int dts_printQueueCfg (void *data);

int dts_queueAccept (void *data);
int dts_queueDest (void *data);
int dts_queueSrc (void *data);
int dts_queueValid (void *data);
int dts_queueSetControl (void *data);
int dts_queueComplete (void *data);
int dts_queueRelease (void *data);
int dts_queueUpdateStats (void *data);

int dts_Access (void *data);		/* File Utility Methods		*/
int dts_Cat (void *data);
int dts_Checksum (void *data);
int dts_Copy (void *data);
int dts_Cfg (void *data);
int dts_Cwd (void *data);
int dts_CheckDir (void *data);
int dts_Chmod (void *data);
int dts_Delete (void *data);
int dts_Dir (void *data);
int dts_DestDir (void *data);
int dts_DiskUsed (void *data);
int dts_DiskFree (void *data);
int dts_Echo (void *data);
int dts_FSize (void *data);
int dts_FMode (void *data);
int dts_FTime (void *data);
int dts_Mkdir (void *data);
int dts_Ping (void *data);
int dts_PingSleep (void *data);
int dts_PingStr (void *data);
int dts_PingArray (void *data);
int dts_remotePing (void *data);
int dts_Rename (void *data);
int dts_SetRoot (void *data);
int dts_SetDbg (void *data);
int dts_UnsetDbg (void *data);
int dts_Touch (void *data);

int dts_Read (void *data);		/* Low-level I/O Methods	*/
int dts_Write (void *data);
int dts_Prealloc (void *data);
int dts_Stat (void *data);
int dts_StatVal (void *data);

int dts_List (void *data);
int dts_Set (void *data);
int dts_Get (void *data);

int dts_initTransfer (void *data);
int dts_doTransfer (void *data);
int dts_endTransfer (void *data);
int dts_cancelTransfer (void *data);

int dts_monAttach (void *data); 	/* Console Methods 		*/
int dts_monConsole (void *data);
int dts_monDetach (void *data);

int dts_testFault (void *data);		/* Test Methods			*/

int dts_psHandler (void *data);		/* Async method handlers	*/
int dts_nullHandler (void *data);

